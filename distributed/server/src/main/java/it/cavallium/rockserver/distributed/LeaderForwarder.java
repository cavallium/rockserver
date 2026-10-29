package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Finds the current leader and forwards requests to its Rockserver API. */
final class LeaderForwarder implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderForwarder.class);
	private static final long POLL_MS = 20;

	private final NodeConfig config;
	private final ConcurrentHashMap<String, GrpcConnection> connections = new ConcurrentHashMap<>();
	private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(
			Thread.ofPlatform().daemon().name("rockserver-leader-wait").factory());

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

	/** The API of {@code peerId}, bound to {@code context}. */
	RocksDBAsyncAPI api(String peerId, RequestContext context) {
		var connection = connections.computeIfAbsent(peerId, id -> {
			var peer = config.peer(id);
			LOG.debug("{}: connecting to {} at {}:{}", config.nodeId(), id, peer.apiHost(), peer.apiPort());
			return GrpcConnection.forHostAndPort(config.nodeId() + "->" + id,
					new HostAndPort(peer.apiHost(), peer.apiPort()));
		});
		return connection.getAsyncApi(context);
	}

	@Override
	public void close() throws IOException {
		timer.shutdownNow();
		IOException failure = null;
		for (GrpcConnection connection : connections.values()) {
			try {
				connection.close();
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
