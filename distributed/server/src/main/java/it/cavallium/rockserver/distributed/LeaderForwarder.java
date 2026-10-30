package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the current leader and forwards requests to its Rockserver API.
 *
 * <p>The API address of a member comes from this node's peer list. A member added to the group after this node
 * started is missing from it: its address is then asked to the member itself, through {@link TermService} on its
 * Raft address, which the Raft configuration carries.</p>
 */
final class LeaderForwarder implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderForwarder.class);
	private static final long POLL_MS = 20;

	/** Connections to each peer, used in turn: one connection has a single event loop thread. */
	private static final int CONNECTIONS_PER_PEER = 4;
	/** A peer whose API port does not accept a connection this fast is treated as down. */
	private static final int PROBE_TIMEOUT_MS = 500;
	/** After a failed connection to a peer, requests to it fail at once for this long instead of reconnecting. */
	private static final long DOWN_BACKOFF_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

	/** The leader could not be connected to: the request was not sent, so it can always be retried. */
	static final class NotConnectedException extends RocksDBRetryException {
		NotConnectedException() {}
	}

	/** One connection to a peer, as handed out for one request. */
	record Target(String peerId, String key, CompletableFuture<GrpcConnection> handle, GrpcConnection connection) {}

	private final NodeConfig config;
	/**
	 * Connections by peer id and slot; a failed lookup or handshake is removed, so that the next request tries again,
	 * and so are all of a peer's connections once a call to it fails in transport ({@link #invalidate}).
	 */
	private final ConcurrentHashMap<String, CompletableFuture<GrpcConnection>> connections = new ConcurrentHashMap<>();
	private final java.util.concurrent.atomic.AtomicInteger nextSlot = new java.util.concurrent.atomic.AtomicInteger();
	/** Until when each peer is considered down after a failed connection, in {@link System#nanoTime()} units. */
	private final ConcurrentHashMap<String, Long> downUntil = new ConcurrentHashMap<>();
	/** Closes connections, never on a thread of the connection itself (its close waits for its own executor). */
	private final ExecutorService closer = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
			.name("rockserver-leader-close-", 0)
			.factory());
	private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
			Thread.ofPlatform().daemon().name("rockserver-leader-wait").factory());
	/** Runs the blocking connection handshakes. */
	private final ExecutorService connector = Executors.newThreadPerTaskExecutor(Thread.ofVirtual()
			.name("rockserver-leader-connect-", 0)
			.factory());
	private volatile boolean closed;

	LeaderForwarder(NodeConfig config) {
		this.config = config;
	}

	/**
	 * Completes with the id of a known leader: another node, or this node once it is a ready leader. Fails with a
	 * retryable {@code SERVER_OVERLOADED} if no leader is known within the request timeout.
	 */
	CompletableFuture<String> leader(RockserverStateMachine stateMachine) {
		var result = new CompletableFuture<String>();
		long deadline = System.nanoTime() + config.requestTimeout().toNanos();
		poll(stateMachine, result, deadline);
		return result;
	}

	private void poll(RockserverStateMachine stateMachine, CompletableFuture<String> result, long deadline) {
		var division = stateMachine.division();
		if (division != null) {
			var info = division.getInfo();
			var leaderId = info.getLeaderId();
			if (leaderId != null) {
				var leader = leaderId.toString();
				if (!leader.equals(config.nodeId()) || (info.isLeader() && info.isLeaderReady())) {
					result.complete(leader);
					return;
				}
			}
		}
		if (System.nanoTime() - deadline > 0) {
			result.completeExceptionally(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"No Raft leader available"));
			return;
		}
		try {
			timer.schedule(() -> poll(stateMachine, result, deadline), POLL_MS, TimeUnit.MILLISECONDS);
		} catch (RuntimeException ex) {
			result.completeExceptionally(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "Node is closing"));
		}
	}

	/**
	 * A connection to {@code peerId}. Fails with {@link NotConnectedException} if the peer cannot be connected to,
	 * or was not a moment ago.
	 */
	CompletableFuture<Target> target(String peerId, RockserverStateMachine stateMachine) {
		var down = downUntil.get(peerId);
		if (down != null && System.nanoTime() - down < 0) {
			return CompletableFuture.failedFuture(new NotConnectedException());
		}
		var key = peerId + "#" + Math.floorMod(nextSlot.getAndIncrement(), CONNECTIONS_PER_PEER);
		var handle = connections.computeIfAbsent(key, _ -> connect(peerId, stateMachine));
		return handle.handle((connection, error) -> {
			if (error != null) {
				connections.remove(key, handle);
				downUntil.put(peerId, System.nanoTime() + DOWN_BACKOFF_NANOS);
				LOG.debug("{}: cannot reach the API of {}", config.nodeId(), peerId, error);
				throw new CompletionException(new NotConnectedException());
			}
			downUntil.remove(peerId);
			return new Target(peerId, key, handle, connection);
		});
	}

	/**
	 * Forgets the connection of {@code target} after a call on it failed in transport, so that the next request to
	 * that slot reconnects, and closes it in the background once its other calls end. Connections made since, and
	 * the other slots, are left alone: they fail on their own if the peer is gone.
	 */
	void invalidate(Target target) {
		if (connections.remove(target.key(), target.handle())) {
			try {
				closer.execute(target.connection()::close);
			} catch (java.util.concurrent.RejectedExecutionException closing) {
				Thread.ofVirtual().start(target.connection()::close);
			}
		}
	}

	private CompletableFuture<GrpcConnection> connect(String peerId, RockserverStateMachine stateMachine) {
		if (closed) {
			return CompletableFuture.failedFuture(new IOException("Node is closing"));
		}
		var known = config.peers().stream().filter(p -> p.id().equals(peerId)).findFirst();
		var address = known.isPresent()
				? CompletableFuture.completedFuture(new HostAndPort(known.get().apiHost(), known.get().apiPort()))
				: askApiAddress(peerId, stateMachine);
		return address.thenApplyAsync(api -> {
			LOG.debug("{}: connecting to {} at {}:{}", config.nodeId(), peerId, api.host(), api.port());
			probe(api);
			// Retried here, possibly at another leader, never by gRPC against a peer that may be gone.
			return GrpcConnection.forHostAndPortWithoutRetry(config.nodeId() + "->" + peerId, api);
		}, connector);
	}

	/** The handshake retries a refused connection for seconds: fail at once on a peer that is down instead. */
	private static void probe(HostAndPort address) {
		try (var socket = new java.net.Socket()) {
			socket.connect(new java.net.InetSocketAddress(address.host(), address.port()), PROBE_TIMEOUT_MS);
		} catch (IOException e) {
			throw new java.io.UncheckedIOException(e);
		}
	}


	/** Asks a member that is not in this node's peer list for its API address, at its Raft address. */
	private CompletableFuture<HostAndPort> askApiAddress(String peerId, RockserverStateMachine stateMachine) {
		var raftAddress = raftAddress(peerId, stateMachine);
		if (raftAddress == null) {
			return CompletableFuture.failedFuture(new IOException("Peer " + peerId + " is not a member"));
		}
		var channel = NettyChannelBuilder.forTarget(raftAddress).usePlaintext().build();
		return TermService.apiAddress(channel, config.requestTimeout()).whenComplete((address, error) -> {
			channel.shutdown();
			if (address != null) {
				LOG.info("{}: member {} at {} serves its API at {}:{}", config.nodeId(), peerId, raftAddress,
						address.host(), address.port());
			}
		});
	}

	private static @Nullable String raftAddress(String peerId, RockserverStateMachine stateMachine) {
		var division = stateMachine.division();
		if (division == null) {
			return null;
		}
		var conf = division.getRaftConf();
		for (var peers : List.of(conf.getCurrentPeers(), conf.getPreviousPeers())) {
			for (RaftPeer peer : peers) {
				if (peer.getId().toString().equals(peerId)) {
					return peer.getAddress();
				}
			}
		}
		return null;
	}

	@Override
	public void close() throws IOException {
		closed = true;
		timer.shutdownNow();
		connector.shutdown();
		closer.shutdown();
		IOException failure = null;
		for (var pending : connections.values()) {
			if (!pending.isDone()) {
				// A handshake still in progress closes its connection when it completes.
				pending.thenAccept(GrpcConnection::close);
				continue;
			}
			if (pending.isCompletedExceptionally()) {
				continue;
			}
			try {
				pending.join().close();
			} catch (RuntimeException ex) {
				if (failure == null) {
					failure = new IOException("Failed to close forwarding connections", ex);
				} else {
					failure.addSuppressed(ex);
				}
			}
		}
		connections.clear();
		if (failure != null) {
			throw failure;
		}
	}
}
