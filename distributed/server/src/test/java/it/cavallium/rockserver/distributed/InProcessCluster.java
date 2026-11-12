package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
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
	private final UnaryOperator<NodeConfig> customize;
	private final String host;

	InProcessCluster(Path root, int size, UnaryOperator<NodeConfig> customize) throws IOException {
		this(root, size, "127.0.0.1", customize);
	}

	/** {@code host}: the address every node binds and is named by, as written in the peer list ({@code [::1]}). */
	InProcessCluster(Path root, int size, String host, UnaryOperator<NodeConfig> customize) throws IOException {
		this.root = root;
		this.host = host;
		this.customize = customize;
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, MERGE_CONFIG);
		var peers = new ArrayList<PeerConfig>();
		// Not ports from the system: those are ephemeral, and the same one can come back twice, or go to a connection of
		// another process between here and the bind. Ratis then ends the JVM, with every test of the run.
		var ports = ProcessCluster.freePorts(2 * size, List.of());
		for (int i = 0; i < size; i++) {
			peers.add(new PeerConfig("n" + i, host, ports.get(2 * i), host, ports.get(2 * i + 1)));
		}
		for (PeerConfig peer : peers) {
			configs.put(peer.id(), customize.apply(baseConfig(peer.id(), peers)));
		}
	}

	private NodeConfig baseConfig(String id, List<PeerConfig> peers) {
		return NodeConfig.defaults(id, peers, root.resolve(id))
				.withRockserverConfig(root.resolve("rockserver.conf"))
				.withBindHosts(host, host)
				.withElectionTimeouts(Duration.ofMillis(400), Duration.ofMillis(800))
				.withRequestTimeout(Duration.ofSeconds(10))
				.withPipelinedUpdates(ProcessCluster.PIPELINED_UPDATES);
	}

	/**
	 * Starts a new node {@code id} with {@code --join}, on ports of its own, with the peer list of the existing nodes
	 * plus itself; the existing nodes' configurations do not change. It waits for an add-peer: see {@link #raftPeers}.
	 */
	ReplicatedNode join(String id) throws IOException {
		addJoining(id);
		return start(id);
	}

	/**
	 * The configuration of {@link #join}, without the start. R9-9: the peers are every configured node (each one's own
	 * entry, so a node that joined before is in it too), and the ports are none of theirs: a stopped node does not hold
	 * its ports, but takes them again when it restarts.
	 */
	NodeConfig addJoining(String id) throws IOException {
		if (configs.containsKey(id)) {
			throw new IllegalStateException(id + " exists");
		}
		var taken = new HashSet<Integer>();
		var peers = new ArrayList<PeerConfig>();
		for (var config : configs.values()) {
			var self = config.self();
			peers.add(self);
			taken.add(self.raftPort());
			taken.add(self.apiPort());
		}
		// Free now, but possibly a stopped node's: asked for enough that two are left without those.
		var ports = ProcessCluster.freePorts(2 + taken.size(), List.of()).stream().filter(port -> !taken.contains(port))
				.limit(2).toList();
		peers.add(new PeerConfig(id, host, ports.get(0), host, ports.get(1)));
		var config = customize.apply(baseConfig(id, peers)).withJoin(true);
		configs.put(id, config);
		return config;
	}

	/** The Raft peers of {@code ids}, at the addresses of their configurations, for {@code admin().setConfiguration}. */
	List<org.apache.ratis.protocol.RaftPeer> raftPeers(List<String> ids) {
		return ids.stream().map(id -> configs.get(id).self())
				.map(peer -> org.apache.ratis.protocol.RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress())
						.build())
				.toList();
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
		var leader = awaitLeader(timeout);
		// Its log, not what it applied: a new leader may still be applying what the previous one committed.
		var last = leader.stateMachine().division().getRaftLog().getLastEntryTermIndex();
		long target = Math.max(leader.appliedIndex(), last != null ? last.getIndex() : -1);
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
