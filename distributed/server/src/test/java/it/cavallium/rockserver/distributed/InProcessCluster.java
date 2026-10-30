package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.jetbrains.annotations.Nullable;

/**
 * Several {@link ReplicatedNode}s in this JVM, on loopback with real Raft and API sockets. Nodes can be halted and
 * restarted with their data directories. Network faults need separate processes: see {@code ProcessCluster}.
 */
final class InProcessCluster implements AutoCloseable {

	/** Rockserver config with a string-append merge operator on every column, identical on every node. */
	static final String MERGE_CONFIG = """
			database: {
			  global: {
			    fallback-column-options: {
			      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
			    }
			  }
			}
			""";

	private final Path root;
	private final Map<String, NodeConfig> configs = new LinkedHashMap<>();
	private final Map<String, ReplicatedNode> running = new LinkedHashMap<>();

	InProcessCluster(Path root, int size, UnaryOperator<NodeConfig> customize) throws IOException {
		this.root = root;
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, MERGE_CONFIG);
		var peers = new ArrayList<PeerConfig>();
		for (int i = 0; i < size; i++) {
			peers.add(new PeerConfig("n" + i, "127.0.0.1", freePort(), "127.0.0.1", freePort()));
		}
		for (PeerConfig peer : peers) {
			var config = NodeConfig.defaults(peer.id(), peers, root.resolve(peer.id()))
					.withRockserverConfig(rockserverConfig)
					.withBindHosts("127.0.0.1", "127.0.0.1")
					.withElectionTimeouts(Duration.ofMillis(400), Duration.ofMillis(800))
					.withRequestTimeout(Duration.ofSeconds(10))
					.withPipelinedUpdates(ProcessCluster.PIPELINED_UPDATES);
			configs.put(peer.id(), customize.apply(config));
		}
	}

	/** Starts every node; if one fails to start, stops the others, so that a failed test leaks no running node. */
	InProcessCluster startAll() throws IOException {
		try {
			for (String id : configs.keySet()) {
				start(id);
			}
		} catch (IOException | RuntimeException ex) {
			try {
				close();
			} catch (Exception closeError) {
				ex.addSuppressed(closeError);
			}
			throw ex;
		}
		return this;
	}

	ReplicatedNode start(String id) throws IOException {
		if (running.containsKey(id)) {
			throw new IllegalStateException(id + " is running");
		}
		var node = ReplicatedNode.start(configs.get(id));
		running.put(id, node);
		return node;
	}

	/** Changes how a stopped node starts next. */
	void reconfigure(String id, UnaryOperator<NodeConfig> change) {
		if (running.containsKey(id)) {
			throw new IllegalStateException(id + " is running");
		}
		configs.computeIfPresent(id, (_, config) -> change.apply(config));
	}

	/** Graceful stop (leadership hand-off). */
	void stop(String id) throws IOException {
		var node = running.remove(id);
		if (node != null) {
			node.close();
		}
	}

	/** Abrupt stop: no hand-off. */
	void halt(String id) throws IOException {
		var node = running.remove(id);
		if (node != null) {
			node.halt();
		}
	}

	List<String> ids() {
		return List.copyOf(configs.keySet());
	}

	NodeConfig config(String id) {
		return configs.get(id);
	}

	ReplicatedNode node(String id) {
		var node = running.get(id);
		if (node == null) {
			throw new IllegalStateException(id + " is not running");
		}
		return node;
	}

	List<ReplicatedNode> runningNodes() {
		return List.copyOf(running.values());
	}

	/** Waits until exactly one running node is the leader and every running node agrees on it. */
	ReplicatedNode awaitLeader(Duration timeout) {
		await(timeout, () -> leaderOrNull() != null);
		return leaderOrNull();
	}

	private @Nullable ReplicatedNode leaderOrNull() {
		ReplicatedNode leader = null;
		for (ReplicatedNode node : running.values()) {
			if (node.isLeader()) {
				if (leader != null) {
					return null;
				}
				leader = node;
			}
		}
		if (leader == null) {
			return null;
		}
		for (ReplicatedNode node : running.values()) {
			if (!leader.config().nodeId().equals(node.leaderId())) {
				return null;
			}
		}
		return leader;
	}

	ReplicatedNode anyFollower() {
		var leader = awaitLeader(Duration.ofSeconds(20));
		return running.values().stream().filter(n -> n != leader).findFirst().orElseThrow();
	}

	/** Waits until every running node applied at least the leader's applied index at call time. */
	void awaitConverged(Duration timeout) {
		long target = awaitLeader(timeout).appliedIndex();
		await(timeout, () -> running.values().stream().allMatch(n -> n.appliedIndex() >= target));
	}

	static void await(Duration timeout, BooleanSupplier condition) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("Condition not met within " + timeout);
			}
			try {
				Thread.sleep(20);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			}
		}
	}

	static int freePort() {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	Path root() {
		return root;
	}

	@Override
	public void close() throws IOException {
		IOException failure = null;
		for (String id : List.copyOf(running.keySet())) {
			try {
				halt(id);
			} catch (IOException ex) {
				if (failure == null) {
					failure = ex;
				} else {
					failure.addSuppressed(ex);
				}
			}
		}
		if (failure != null) {
			throw failure;
		}
	}
}
