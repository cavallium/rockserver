package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.GrpcClientTransport;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.RaftPeer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds the current leader and forwards requests to its Rockserver API.
 *
 * <p>The API address of a member comes from this node's peer list. A member added to the group after this node
 * started is missing from it: its address is then asked to the member itself, through {@link TermService} on its
 * Raft address, which the Raft configuration carries.</p>
 *
 * <p>A request that finds no leader waits for one: {@link #leaderChanged()}, called on every leader change and when
 * this node becomes a ready leader, wakes it at once. A slow poll stays behind as a safety net.</p>
 */
final class LeaderForwarder implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderForwarder.class);
	/**
	 * Checks again this often while waiting for a leader. Every change is notified, and a request registers before its
	 * last check, so no wake-up is lost; the poll only enforces the deadline and covers a notification Ratis might skip.
	 */
	private static final long POLL_MS = 250;

	/**
	 * Connections to each peer, used in turn. They share {@link #transport}, which has this many event loop threads:
	 * Netty hands each new connection the next thread, so the connections to one peer, made one after another, get a
	 * thread each, as when every connection had its own.
	 */
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
	private final AtomicInteger nextSlot = new AtomicInteger();
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
	/** {@link #leader} results still waiting for a leader, with their check; failed by {@link #close()}. */
	private final ConcurrentHashMap<CompletableFuture<String>, Runnable> pendingLeaders = new ConcurrentHashMap<>();
	/** API addresses learned from members missing from this node's peer list, by peer id. */
	private final ConcurrentHashMap<String, HostAndPort> learnedAddresses = new ConcurrentHashMap<>();
	/**
	 * API addresses that members of this node's peer list told about themselves: they win over the list, which may
	 * be wrong on this node (see {@link #listedApiAddress}). An entry is only ever replaced by a newer answer.
	 */
	private final ConcurrentHashMap<String, HostAndPort> answeredAddresses = new ConcurrentHashMap<>();
	/**
	 * Members of {@link #answeredAddresses} that a connection failed to since they answered: they are asked again
	 * before the next connection, and their old answer is kept, for connections and leader hints, until a new one
	 * replaces it (R18-2, R18 review finding 1).
	 */
	private final Set<String> staleAnswers = ConcurrentHashMap.newKeySet();
	/** The API addresses of this node's peer list, by peer id, built once. */
	private final Map<String, HostAndPort> listedAddresses;
	/** Raised whenever {@link #apiAddress} may return something else for some peer; see {@link #addressVersion()}. */
	private final AtomicLong addressVersion = new AtomicLong();
	/** The event loop threads and callback executor of every forwarding connection; closed after them. */
	private final GrpcClientTransport transport;
	/** The channels to the Raft ports of the other members, for the address lookups. */
	private final PeerChannels peerChannels;
	/** Whether {@link #peerChannels} is this instance's own, closed with it, rather than the node's shared one. */
	private final boolean ownPeerChannels;
	private volatile boolean closed;

	LeaderForwarder(NodeConfig config) {
		this(config, new PeerChannels(), true);
	}

	/** @param peerChannels the node's shared channels to the other members' Raft ports; the caller closes them */
	LeaderForwarder(NodeConfig config, PeerChannels peerChannels) {
		this(config, peerChannels, false);
	}

	private LeaderForwarder(NodeConfig config, PeerChannels peerChannels, boolean ownPeerChannels) {
		this.config = config;
		this.peerChannels = peerChannels;
		this.ownPeerChannels = ownPeerChannels;
		var listed = new HashMap<String, HostAndPort>();
		for (var peer : config.peers()) {
			listed.put(peer.id(), new HostAndPort(peer.apiHost(), peer.apiPort()));
		}
		this.listedAddresses = Map.copyOf(listed);
		this.transport = GrpcClientTransport.create(config.nodeId() + "-forward", CONNECTIONS_PER_PEER);
	}

	/**
	 * Completes with the id of a known leader: another node, or this node once it is a ready leader. Fails with a
	 * retryable {@code SERVER_OVERLOADED} if no leader is known within the request timeout.
	 */
	CompletableFuture<String> leader(RockserverStateMachine stateMachine) {
		var result = new CompletableFuture<String>();
		long deadline = System.nanoTime() + config.requestTimeout().toNanos();
		if (check(stateMachine, result, deadline)) {
			return result;
		}
		// Woken by leaderChanged(), or by the next poll, which waits in the timer whose queue close() drops.
		pendingLeaders.put(result, () -> check(stateMachine, result, deadline));
		result.whenComplete((_, _) -> pendingLeaders.remove(result));
		// Checked again once registered: a change that Ratis reported between the first check and the registration
		// found nothing to wake, but it is visible now. Ratis updates the leader before it notifies, so no change can
		// fall between this check and the registration.
		if (check(stateMachine, result, deadline)) {
			return result;
		}
		schedule(stateMachine, result, deadline);
		if (closed) {
			result.completeExceptionally(closing());
		}
		return result;
	}

	/**
	 * Checks again every request waiting for a leader, off the calling thread (Ratis' own, which must not run the
	 * requests' continuations). Called on every leader change, and when this node becomes a ready leader.
	 */
	void leaderChanged() {
		if (pendingLeaders.isEmpty()) {
			return;
		}
		try {
			connector.execute(() -> pendingLeaders.values().forEach(Runnable::run));
		} catch (RejectedExecutionException closing) {
			// close() fails them.
		}
	}

	/**
	 * The API address of {@code peerId} as this node knows it: from its peer list, or learned from the member itself;
	 * null if unknown yet.
	 */
	@Nullable HostAndPort apiAddress(String peerId) {
		var answered = answeredAddresses.get(peerId);
		if (answered != null) {
			return answered;
		}
		var listed = listedAddresses.get(peerId);
		if (listed != null) {
			return listed;
		}
		return learnedAddresses.get(peerId);
	}

	/**
	 * A counter that changes whenever {@link #apiAddress} may return another address for some peer, so that a caller
	 * on a hot path (the leader hint of every response) can cache its result: read it before {@link #apiAddress}.
	 */
	long addressVersion() {
		return addressVersion.get();
	}

	/** Records {@code address} in {@code addresses}; true, and the version raised, if it changed the entry. */
	private boolean record(ConcurrentHashMap<String, HostAndPort> addresses, String peerId, HostAndPort address) {
		var previous = addresses.put(peerId, address);
		if (address.equals(previous)) {
			return false;
		}
		addressVersion.incrementAndGet();
		return true;
	}

	private static RocksDBException closing() {
		return RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "Node is closing");
	}

	/** Completes {@code result} if a leader is known or the deadline passed; true if it is done. */
	private boolean check(RockserverStateMachine stateMachine, CompletableFuture<String> result, long deadline) {
		if (result.isDone()) {
			return true;
		}
		var division = stateMachine.division();
		if (division != null) {
			var info = division.getInfo();
			var leaderId = info.getLeaderId();
			if (leaderId != null) {
				var leader = leaderId.toString();
				if (!leader.equals(config.nodeId()) || (info.isLeader() && info.isLeaderReady())) {
					result.complete(leader);
					return true;
				}
			}
		}
		if (System.nanoTime() - deadline > 0) {
			result.completeExceptionally(RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
					"No Raft leader available"));
			return true;
		}
		return false;
	}

	private void schedule(RockserverStateMachine stateMachine, CompletableFuture<String> result, long deadline) {
		long delay = Math.max(1, Math.min(TimeUnit.MILLISECONDS.toNanos(POLL_MS), deadline - System.nanoTime() + 1));
		try {
			timer.schedule(() -> {
				if (!check(stateMachine, result, deadline)) {
					schedule(stateMachine, result, deadline);
				}
			}, delay, TimeUnit.NANOSECONDS);
		} catch (RuntimeException ex) {
			result.completeExceptionally(closing());
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
				// The member may serve its API elsewhere now (restarted with another port while this node kept running):
				// it is asked again at once, and before the next connection, instead of the old port being tried until
				// this node restarts (R18-2). Its old answer stays until a new one replaces it: after a one-off failure at
				// the same port it is still right, and the peer list may not be (R18 review finding 1).
				var listedPeer = listedPeer(peerId);
				if (listedPeer != null && answeredAddresses.containsKey(peerId)) {
					staleAnswers.add(peerId);
					listedApiAddress(listedPeer);
				}
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
			} catch (RejectedExecutionException closing) {
				Thread.ofVirtual().start(target.connection()::close);
			}
		}
	}

	private CompletableFuture<GrpcConnection> connect(String peerId, RockserverStateMachine stateMachine) {
		if (closed) {
			return CompletableFuture.failedFuture(new IOException("Node is closing"));
		}
		var known = listedPeer(peerId);
		var address = known != null ? listedApiAddress(known) : askApiAddress(peerId, stateMachine);
		return address.thenApplyAsync(api -> {
			LOG.debug("{}: connecting to {} at {}:{}", config.nodeId(), peerId, api.host(), api.port());
			probe(api);
			// Retried here, possibly at another leader, never by gRPC against a peer that may be gone.
			return GrpcConnection.forHostAndPortWithoutRetry(config.nodeId() + "->" + peerId, api, transport);
		}, connector);
	}

	private @Nullable PeerConfig listedPeer(String peerId) {
		for (var peer : config.peers()) {
			if (peer.id().equals(peerId)) {
				return peer;
			}
		}
		return null;
	}

	/** The handshake retries a refused connection for seconds: fail at once on a peer that is down instead. */
	private static void probe(HostAndPort address) {
		try (var socket = new Socket()) {
			socket.connect(new InetSocketAddress(address.host(), address.port()), PROBE_TIMEOUT_MS);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}


	/**
	 * The API address of a member of this node's peer list, checked once against what the member answers at its Raft
	 * address: a node started with wrong API ports for its peers would otherwise fail every call that it forwards. The
	 * answer wins, and a difference is logged. A member that does not answer within {@link #PROBE_TIMEOUT_MS} is likely
	 * down: its last answer, or else the listed address, is used, and it is asked again at the next connection. An
	 * answer is used without asking again until a connection to the member fails ({@link #target}): then the member is
	 * asked again, and a member that moved its API port is found at the new one (R18-2), while the old answer is kept
	 * until the new one replaces it (R18 review finding 1).
	 */
	private CompletableFuture<HostAndPort> listedApiAddress(PeerConfig peer) {
		var listed = listedAddresses.get(peer.id());
		var answered = answeredAddresses.get(peer.id());
		if (answered != null && !staleAnswers.contains(peer.id())) {
			return CompletableFuture.completedFuture(answered);
		}
		var channel = peerChannels.get(peer.raftAddress());
		return TermService.apiAddress(channel, Duration.ofMillis(PROBE_TIMEOUT_MS)).handle((address, error) -> {
			if (error != null) {
				LOG.debug("{}: member {} did not tell its API address", config.nodeId(), peer.id(), error);
				var last = answeredAddresses.get(peer.id());
				return last != null ? last : listed;
			}
			staleAnswers.remove(peer.id());
			var previous = answeredAddresses.get(peer.id());
			if (record(answeredAddresses, peer.id(), address)) {
				if (previous != null) {
					LOG.info("{}: member {} serves its API at {}:{} now, no longer at {}:{}", config.nodeId(), peer.id(),
							address.host(), address.port(), previous.host(), previous.port());
				} else if (!address.equals(listed)) {
					LOG.error("{}: member {} serves its API at {}:{}, but the peer list of this node says {}:{}: using the"
							+ " member's. Fix --peer {}", config.nodeId(), peer.id(), address.host(), address.port(),
							listed.host(), listed.port(), peer.format());
				}
			}
			return address;
		});
	}

	/** Asks a member that is not in this node's peer list for its API address, at its Raft address. */
	private CompletableFuture<HostAndPort> askApiAddress(String peerId, RockserverStateMachine stateMachine) {
		var raftAddress = raftAddress(peerId, stateMachine);
		if (raftAddress == null) {
			return CompletableFuture.failedFuture(new IOException("Peer " + peerId + " is not a member"));
		}
		var channel = peerChannels.get(raftAddress);
		return TermService.apiAddress(channel, config.requestTimeout()).whenComplete((address, error) -> {
			if (address != null && record(learnedAddresses, peerId, address)) {
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
		// The timer drops the next polls of pending leader() calls: fail those calls instead.
		timer.shutdownNow();
		for (var pending : pendingLeaders.keySet()) {
			pending.completeExceptionally(closing());
		}
		connector.shutdown();
		closer.shutdown();
		if (ownPeerChannels) {
			peerChannels.close();
		}
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
		// Handshakes still in progress fail on the stopped threads and close their connections.
		transport.close();
		if (failure != null) {
			throw failure;
		}
	}
}
