package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Design scenario 6: a deploy restarts every node in turn, under continuous load, twice through the whole cluster.
 *
 * <p>Each restart is graceful: SIGTERM, so a leader hands its leadership to a follower before closing, then a
 * start of the same node, and a wait until it has applied everything the leader had committed. Writers keep doing
 * idempotent puts of increasing versions (one key per version, so a retried put cannot undo a later one) and
 * readers keep reading versions that were acknowledged before their read began, all through {@link ClusterClient}s
 * that retry retryable failures on other nodes.</p>
 *
 * <p>The test prints, for every restart, the longest window in which client operations failed and had to be
 * retried, the longest pause in acknowledged writes and the slowest write and read. Reads that reach a node as it
 * closes fail over only after their attempt timeout, because the core client retries them on the same node; writes
 * fail over at once, so the write pause measures the cluster's own unavailability.</p>
 *
 * <p>At the end the cluster is stopped a node at a time, the follower first, so the leader stops with one follower
 * down; then every replica's database must hold exactly the acknowledged puts.</p>
 */
@Timeout(300)
class RollingRestartTest {

	private static final int NODES = 3;
	private static final int ROUNDS = 2;
	private static final int WRITERS = 3;
	private static final int READERS = 3;
	private static final Duration ELECTION_TIMEOUT_MIN = Duration.ofMillis(400);
	private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofMillis(800);
	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms",
			ELECTION_TIMEOUT_MIN.toMillis() + ":" + ELECTION_TIMEOUT_MAX.toMillis());
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext SETUP_CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	/** Bounds one attempt: a node that is closing can hold a read until this timeout before it fails over. */
	private static final RequestContext OP_CONTEXT = RequestContext.latency(Duration.ofSeconds(2));
	/** Long enough to ride out an election timeout or two; an operation still failing after it is a test failure. */
	private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(30);
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration CATCH_UP_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration LEADER_TIMEOUT = Duration.ofSeconds(20);
	/** Acknowledged puts every writer must add between two restarts, so that each restart starts under load. */
	private static final int PUTS_BETWEEN_RESTARTS = 20;
	/**
	 * Bound of every client operation during a graceful restart, retries included. A hand-off costs one Raft
	 * round trip plus the client's failover (a retry round pauses 100 ms); the worst expected case is an attempt
	 * that waits for its whole {@link #OP_CONTEXT} timeout (2 s) on a node that is closing, then succeeds on the
	 * next node. Failing over by election instead would add 400-800 ms per election here, and a proposal waiting
	 * for the node's request timeout (10 s) or a stuck shutdown would exceed this bound.
	 */
	private static final Duration MAX_OP_LATENCY = Duration.ofSeconds(5);
	/**
	 * Bound of the longest time without an acknowledged write during a graceful restart. Writes are not retried by
	 * the core client on the same node, so this is the cluster's own unavailability: a leader restart costs one
	 * hand-off (a few hundred ms here), a follower restart almost nothing. Waiting out an election timeout or a
	 * proposal timeout instead would show up as pauses of about a second or more.
	 */
	private static final Duration MAX_WRITE_PAUSE = Duration.ofSeconds(2);
	/**
	 * Bound of the graceful stop of a leader whose followers are all down. It has nobody to hand off to, so it
	 * should stop about as fast as a follower (about a second here); every hand-off attempt to a stopped peer would
	 * cost the 5 s hand-off timeout, with writes refused meanwhile.
	 */
	private static final Duration MAX_LONE_STOP = Duration.ofSeconds(8);
	/** Operations slower than this are listed in the report. */
	private static final Duration SLOW_OP = Duration.ofMillis(500);
	private static final String HAND_OFF_LOG = "leadership transferred to";

	@TempDir
	Path root;

	@Test
	void gracefulRollingRestartsUnderLoadFailNoOperationAndLoseNoWrite() throws Exception {
		try (var cluster = ProcessCluster.start(root, NODES, SERVER_ARGS); var raft = new RaftProbe(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			raft.awaitLeader(cluster.nodeIds(), 0, LEADER_TIMEOUT);
			long columnId;
			try (var setup = new ClusterClient(cluster.apiAddresses(), RETRY_TIMEOUT, true)) {
				var column = setup.createColumn("versions", SCHEMA, SETUP_CONTEXT);
				assertTrue(column.acknowledged(), () -> "createColumn failed: " + column.error());
				columnId = column.value();
			}

			var restarts = new ArrayList<Restart>();
			Load load = new Load(cluster, columnId);
			try {
				load.start();
				for (int round = 1; round <= ROUNDS; round++) {
					for (var id : cluster.nodeIds()) {
						load.awaitProgress(PUTS_BETWEEN_RESTARTS);
						restarts.add(restart(cluster, raft, id, round));
					}
				}
				load.awaitProgress(PUTS_BETWEEN_RESTARTS);
			} finally {
				load.stop();
			}
			var ops = load.ops();
			System.out.println(report(restarts, ops));

			assertEquals(List.of(), load.failures(), "operations that failed despite retries");
			assertEquals(List.of(), load.staleReads(), "reads that missed an acknowledged put");
			for (var restart : restarts) {
				var slowest = restart.slowest(ops);
				assertTrue(slowest.compareTo(MAX_OP_LATENCY) < 0, "restart of node " + restart.node + " (round "
						+ restart.round + ") held an operation for " + slowest);
				var pause = restart.writePause(ops);
				assertTrue(pause.compareTo(MAX_WRITE_PAUSE) < 0, "restart of node " + restart.node + " (round "
						+ restart.round + ") stopped all writes for " + pause);
				if (restart.wasLeader) {
					assertTrue(restart.handedOff, "leader " + restart.node + " stopped without handing off leadership "
							+ "(round " + restart.round + ")\n" + restart.stopLog);
				}
			}
			assertTrue(restarts.stream().filter(restart -> restart.wasLeader).count() >= ROUNDS,
					"every round should have restarted the leader at least once: " + restarts);

			// Every acknowledged put is visible through the cluster...
			try (var client = new ClusterClient(cluster.apiAddresses(), RETRY_TIMEOUT, false)) {
				for (int writer = 0; writer < WRITERS; writer++) {
					long version = load.lastAcknowledged(writer);
					var read = client.get(columnId, key(writer, version), OP_CONTEXT);
					assertEquals(version, version(read.valueOrThrow()), "last acknowledged put of writer " + writer);
				}
			}
			// ...and, once every replica applied the whole log, stored identically on every replica. Stopped
			// gracefully: by default RocksDB buffers its WAL for a few seconds, so a SIGKILL may roll a replica back
			// (Raft replays those entries at its next start, but the database files alone would differ).
			raft.awaitAllApplied(cluster.nodeIds(), CATCH_UP_TIMEOUT);
			stopOneByOne(cluster, raft);
			var expected = new TreeMap<String, String>();
			for (int writer = 0; writer < WRITERS; writer++) {
				for (long version = 1; version <= load.lastAcknowledged(writer); version++) {
					expected.put(hex(key(writer, version).keys()[0]), hex(value(version)));
				}
			}
			for (var id : cluster.nodeIds()) {
				var replica = replicaContents(cluster, id);
				assertEquals(List.of("versions"), List.copyOf(replica.keySet()), "user columns of replica " + id);
				assertEquals("", difference(expected, replica.get("versions")),
						"replica " + id + " against the acknowledged puts");
			}
		}
	}

	/**
	 * Stops the cluster gracefully, a node at a time, the way a deploy meets a node that is already down: first the
	 * follower that the leader lists first, then the leader, which must still hand off to the follower that is up,
	 * then that last node, which has nobody to hand off to and must not wait for the stopped ones.
	 */
	private static void stopOneByOne(ProcessCluster cluster, RaftProbe raft) throws IOException {
		var leader = raft.awaitLeader(cluster.nodeIds(), 0, LEADER_TIMEOUT).id();
		var followers = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();
		var down = followers.getFirst();
		var survivor = followers.getLast();
		var times = new StringBuilder("Stopped the cluster one node at a time, ms:");

		long stopping = System.nanoTime();
		cluster.stop(down);
		times.append(" follower ").append(down).append('=').append(millis(System.nanoTime() - stopping));
		// The leader has noticed that the follower is gone.
		raft.awaitUnresponsive(leader, down, LEADER_TIMEOUT);

		long logStart = Files.size(cluster.stderrLog(leader));
		stopping = System.nanoTime();
		cluster.stop(leader);
		times.append(", leader ").append(leader).append('=').append(millis(System.nanoTime() - stopping));
		var leaderLog = logSince(cluster.stderrLog(leader), logStart);

		stopping = System.nanoTime();
		cluster.stop(survivor);
		var lastStop = Duration.ofNanos(System.nanoTime() - stopping);
		times.append(", last node ").append(survivor).append('=').append(lastStop.toMillis());
		System.out.println(times);

		assertTrue(leaderLog.contains(HAND_OFF_LOG + " " + survivor), "leader " + leader + " with follower " + down
				+ " down should hand off to " + survivor + "\n" + leaderLog);
		assertTrue(lastStop.compareTo(MAX_LONE_STOP) < 0, "node " + survivor + " took " + lastStop
				+ " to stop with no other node up");
	}

	private static String logSince(Path log, long offset) throws IOException {
		var bytes = Files.readAllBytes(log);
		return new String(bytes, (int) offset, bytes.length - (int) offset, StandardCharsets.UTF_8);
	}

	/** Missing, unexpected and different keys, at most a few of each; empty if the contents are equal. */
	private static String difference(Map<String, String> expected, Map<String, String> actual) {
		var missing = expected.keySet().stream().filter(key -> !actual.containsKey(key)).toList();
		var unexpected = actual.keySet().stream().filter(key -> !expected.containsKey(key)).toList();
		var different = expected.keySet().stream()
				.filter(key -> actual.containsKey(key) && !actual.get(key).equals(expected.get(key)))
				.toList();
		var result = new StringBuilder();
		for (var part : List.of(Map.entry("missing", missing), Map.entry("unexpected", unexpected),
				Map.entry("different", different))) {
			if (!part.getValue().isEmpty()) {
				result.append(part.getValue().size()).append(' ').append(part.getKey()).append(" keys, e.g. ")
						.append(part.getValue().subList(0, Math.min(5, part.getValue().size()))).append("; ");
			}
		}
		return result.toString();
	}

	/** Stops a node gracefully, starts it again and waits until it has applied what the leader had committed. */
	private static Restart restart(ProcessCluster cluster, RaftProbe raft, String id, int round) throws IOException {
		var leaderBefore = raft.leader(cluster.nodeIds());
		boolean wasLeader = leaderBefore.map(leader -> leader.id().equals(id)).orElse(false);
		long term = leaderBefore.map(Leader::term).orElse(0L);
		var log = cluster.stderrLog(id);
		long logStart = Files.size(log);

		long stopping = System.nanoTime();
		cluster.stop(id);
		long stopped = System.nanoTime();
		var stopLog = logSince(log, logStart);
		var others = cluster.nodeIds().stream().filter(other -> !other.equals(id)).toList();
		var leaderAfter = raft.awaitLeader(others, wasLeader ? term + 1 : term, LEADER_TIMEOUT);
		long leaderKnown = System.nanoTime();

		cluster.start(id);
		cluster.waitReady(id, READY_TIMEOUT);
		long ready = System.nanoTime();
		raft.awaitCaughtUp(id, cluster.nodeIds(), CATCH_UP_TIMEOUT);
		long caughtUp = System.nanoTime();
		return new Restart(id, round, wasLeader, leaderAfter.id(), stopLog.contains(HAND_OFF_LOG), stopLog,
				stopping, stopped, leaderKnown, ready, caughtUp);
	}

	/** Every user column of a stopped node's database: column name to sorted hex key to hex value. */
	private static Map<String, Map<String, String>> replicaContents(ProcessCluster cluster, String id)
			throws IOException {
		var contents = new TreeMap<String, Map<String, String>>();
		try (var db = new EmbeddedConnection(cluster.dataDir(id).resolve("db"), "replica-" + id, null)) {
			var api = db.getSyncApi(SETUP_CONTEXT);
			for (var column : api.getAllColumnDefinitions().keySet()) {
				if (column.equals(NodeStore.META_COLUMN)) {
					continue;
				}
				var entries = new TreeMap<String, String>();
				long columnId = api.getColumnId(column);
				try (Stream<KV> range = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
					range.forEach(kv -> entries.put(hex(kv.keys().keys()), hex(kv.value())));
				}
				contents.put(column, entries);
			}
		}
		return contents;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Load

	/**
	 * One client operation, as the test saw it.
	 *
	 * @param attempts attempts {@link ClusterClient} made; more than one means earlier attempts failed
	 * @param node the node of the last attempt
	 */
	private record Op(boolean write, long startNanos, long endNanos, int attempts, boolean acknowledged,
			String node) {

		long latencyNanos() {
			return endNanos - startNanos;
		}

		boolean overlaps(long fromNanos, long toNanos) {
			return startNanos <= toNanos && endNanos >= fromNanos;
		}
	}

	/** Writer and reader threads, each with its own {@link ClusterClient} starting on a different node. */
	private static final class Load {

		private final ProcessCluster cluster;
		private final long columnId;
		private final AtomicLongArray acknowledged = new AtomicLongArray(WRITERS);
		private final ConcurrentLinkedQueue<Op> ops = new ConcurrentLinkedQueue<>();
		private final List<String> failures = Collections.synchronizedList(new ArrayList<>());
		private final List<String> staleReads = Collections.synchronizedList(new ArrayList<>());
		private final List<Thread> threads = new ArrayList<>();
		private final List<ClusterClient> clients = new ArrayList<>();
		private volatile boolean running = true;

		Load(ProcessCluster cluster, long columnId) {
			this.cluster = cluster;
			this.columnId = columnId;
		}

		void start() {
			for (int writer = 0; writer < WRITERS; writer++) {
				int id = writer;
				var client = client(writer);
				threads.add(Thread.ofPlatform().name("writer-" + writer).start(() -> write(id, client)));
			}
			// Readers read acknowledged versions only: start them once every writer has one.
			awaitProgress(1);
			for (int reader = 0; reader < READERS; reader++) {
				int id = reader;
				var client = client(WRITERS + reader);
				threads.add(Thread.ofPlatform().name("reader-" + reader).start(() -> read(id, client)));
			}
		}

		/** Waits until every writer acknowledged {@code puts} more puts, failing if the load stalls. */
		void awaitProgress(int puts) {
			var targets = new long[WRITERS];
			for (int writer = 0; writer < WRITERS; writer++) {
				targets[writer] = acknowledged.get(writer) + puts;
			}
			await(() -> {
				for (int writer = 0; writer < WRITERS; writer++) {
					if (acknowledged.get(writer) < targets[writer]) {
						return !failures.isEmpty();
					}
				}
				return true;
			}, RETRY_TIMEOUT.plusSeconds(30), "load to make progress");
			assertEquals(List.of(), failures, "operations that failed despite retries");
		}

		void stop() throws InterruptedException {
			running = false;
			for (var thread : threads) {
				thread.join(RETRY_TIMEOUT.plusSeconds(30));
				assertFalse(thread.isAlive(), thread.getName() + " did not stop");
			}
			clients.forEach(ClusterClient::close);
		}

		long lastAcknowledged(int writer) {
			return acknowledged.get(writer);
		}

		List<Op> ops() {
			return List.copyOf(ops);
		}

		List<String> failures() {
			return List.copyOf(failures);
		}

		List<String> staleReads() {
			return List.copyOf(staleReads);
		}

		/** Client whose failover order starts at node {@code index % NODES}. */
		private ClusterClient client(int index) {
			var ids = cluster.nodeIds();
			var addresses = new LinkedHashMap<String, HostAndPort>();
			for (int i = 0; i < ids.size(); i++) {
				var id = ids.get((index + i) % ids.size());
				addresses.put(id, cluster.apiAddress(id));
			}
			// Puts of one key per version are idempotent: retrying an ambiguous one is safe.
			var client = new ClusterClient(addresses, RETRY_TIMEOUT, true);
			clients.add(client);
			return client;
		}

		private void write(int writer, ClusterClient client) {
			for (long version = 1; running; version++) {
				long start = System.nanoTime();
				var result = client.put(columnId, key(writer, version), value(version), OP_CONTEXT);
				ops.add(new Op(true, start, System.nanoTime(), result.attempts(), result.acknowledged(), result.node()));
				if (!result.acknowledged()) {
					failures.add("writer " + writer + " put of version " + version + ": " + result.status() + " after "
							+ result.attempts() + " attempts, last on node " + result.node() + ": " + result.error());
					return;
				}
				acknowledged.set(writer, version);
			}
		}

		private void read(int reader, ClusterClient client) {
			var random = ThreadLocalRandom.current();
			while (running) {
				int writer = random.nextInt(WRITERS);
				long latest = acknowledged.get(writer);
				// Half of the reads ask for the latest acknowledged put, which a stale replica would miss first.
				long version = random.nextBoolean() ? latest : random.nextLong(1, latest + 1);
				long start = System.nanoTime();
				var result = client.get(columnId, key(writer, version), OP_CONTEXT);
				ops.add(new Op(false, start, System.nanoTime(), result.attempts(), result.acknowledged(), result.node()));
				if (!result.acknowledged()) {
					failures.add("reader " + reader + " get of writer " + writer + " version " + version + ": failed after "
							+ result.attempts() + " attempts, last on node " + result.node() + ": " + result.error());
					return;
				}
				if (result.value() == null || version(result.value()) != version) {
					staleReads.add("reader " + reader + " read writer " + writer + " version " + version + " on node "
							+ result.node() + ", acknowledged before the read, as "
							+ (result.value() == null ? "missing" : "version " + version(result.value())));
				}
			}
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Restarts and measurements

	/** Timeline of one restart, in {@link System#nanoTime()}. */
	private record Restart(String node, int round, boolean wasLeader, String leaderAfter, boolean handedOff,
			String stopLog, long stopping, long stopped, long leaderKnown, long ready, long caughtUp) {

		/** The slowest operation that ran while the node was down or catching up. */
		Duration slowest(List<Op> ops) {
			return Duration.ofNanos(during(ops).mapToLong(Op::latencyNanos).max().orElse(0));
		}

		/** The slowest write ({@code true}) or read that ran while the node was down or catching up. */
		Duration slowest(List<Op> ops, boolean write) {
			return Duration.ofNanos(during(ops).filter(op -> op.write() == write).mapToLong(Op::latencyNanos).max()
					.orElse(0));
		}

		/** Operations slower than {@code threshold}, slowest first. */
		List<Op> slowerThan(List<Op> ops, Duration threshold) {
			return during(ops).filter(op -> op.latencyNanos() > threshold.toNanos())
					.sorted(Comparator.comparingLong(Op::latencyNanos).reversed())
					.toList();
		}

		/** Longest interval covered by operations that needed more than one attempt. */
		Duration failureWindow(List<Op> ops) {
			var retried = during(ops).filter(op -> op.attempts() > 1)
					.sorted(Comparator.comparingLong(Op::startNanos))
					.toList();
			long longest = 0;
			long from = 0;
			long to = Long.MIN_VALUE;
			for (var op : retried) {
				if (op.startNanos() > to) {
					from = op.startNanos();
					to = op.endNanos();
				} else {
					to = Math.max(to, op.endNanos());
				}
				longest = Math.max(longest, to - from);
			}
			return Duration.ofNanos(longest);
		}

		/** Longest time without an acknowledged write, from the SIGTERM until the node caught up. */
		Duration writePause(List<Op> ops) {
			var ends = ops.stream()
					.filter(op -> op.write() && op.acknowledged() && op.endNanos() >= stopping && op.endNanos() <= caughtUp)
					.mapToLong(Op::endNanos)
					.sorted()
					.toArray();
			long longest = 0;
			long previous = stopping;
			for (long end : ends) {
				longest = Math.max(longest, end - previous);
				previous = end;
			}
			return Duration.ofNanos(Math.max(longest, caughtUp - previous));
		}

		long retried(List<Op> ops) {
			return during(ops).filter(op -> op.attempts() > 1).count();
		}

		long count(List<Op> ops) {
			return during(ops).count();
		}

		private Stream<Op> during(List<Op> ops) {
			return ops.stream().filter(op -> op.overlaps(stopping, caughtUp));
		}

		@Override
		public String toString() {
			return "node " + node + " round " + round + (wasLeader ? " (leader)" : "");
		}
	}

	private static String report(List<Restart> restarts, List<Op> ops) {
		var report = new StringBuilder("Rolling restart: ").append(ops.size()).append(" operations, ")
				.append(ops.stream().filter(Op::write).count()).append(" writes\n");
		var format = "%-5s %-4s %-6s %-10s %6s %7s %6s %8s %7s %7s %7s %7s %10s%n";
		report.append(String.format(format, "round", "node", "role", "leader->", "stop", "elected", "ready",
				"caughtUp", "failWin", "wrPause", "slowWr", "slowRd", "retried"));
		var slow = new StringBuilder();
		for (var restart : restarts) {
			report.append(String.format(format, restart.round, restart.node, restart.wasLeader ? "leader" : "follow",
					restart.wasLeader ? (restart.handedOff ? "handoff " : "ELECTED ") + restart.leaderAfter : "-",
					millis(restart.stopped - restart.stopping), millis(restart.leaderKnown - restart.stopping),
					millis(restart.ready - restart.stopping), millis(restart.caughtUp - restart.stopping),
					millis(restart.failureWindow(ops).toNanos()), millis(restart.writePause(ops).toNanos()),
					millis(restart.slowest(ops, true).toNanos()), millis(restart.slowest(ops, false).toNanos()),
					restart.retried(ops) + "/" + restart.count(ops)));
			for (var op : restart.slowerThan(ops, SLOW_OP)) {
				slow.append("  ").append(restart).append(": ").append(op.write() ? "write" : "read").append(" took ")
						.append(millis(op.latencyNanos())).append(" ms in ").append(op.attempts())
						.append(" attempts, from ").append(millis(op.startNanos() - restart.stopping))
						.append(" ms after the SIGTERM, last attempt on node ").append(op.node()).append('\n');
			}
		}
		report.append("Times in ms from the SIGTERM; failWin: longest window covered by operations that failed at "
				+ "least once; wrPause: longest time without an acknowledged write; retried: retried/all operations.");
		if (!slow.isEmpty()) {
			report.append("\nOperations slower than ").append(SLOW_OP.toMillis()).append(" ms:\n").append(slow);
		}
		return report.toString();
	}

	private static String millis(long nanos) {
		return Long.toString(TimeUnit.NANOSECONDS.toMillis(nanos));
	}

	// ---------------------------------------------------------------------------------------------------------
	// Raft state of the nodes

	private record Leader(String id, long term) {}

	/** Queries the nodes' Raft group info directly on their Raft ports, without retries. */
	private static final class RaftProbe implements AutoCloseable {

		private static final Duration POLL = Duration.ofMillis(20);

		private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));
		private final RaftClient client;

		RaftProbe(ProcessCluster cluster) {
			var peers = cluster.nodeIds().stream().map(id -> {
				var address = cluster.raftAddress(id);
				return RaftPeer.newBuilder().setId(id).setAddress(address.host() + ":" + address.port()).build();
			}).toList();
			var properties = new RaftProperties();
			RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
			this.client = RaftClient.newBuilder()
					.setClientId(ClientId.randomId())
					.setRaftGroup(RaftGroup.valueOf(groupId, peers))
					.setProperties(properties)
					.setRetryPolicy(RetryPolicies.noRetry())
					.build();
		}

		Optional<GroupInfoReply> info(String id) {
			try {
				return Optional.of(client.getGroupManagementApi(RaftPeerId.valueOf(id)).info(groupId));
			} catch (IOException | RuntimeException unreachable) {
				return Optional.empty();
			}
		}

		/** The leader with the highest term among {@code ids}, if any of them reports being leader. */
		Optional<Leader> leader(List<String> ids) {
			Leader best = null;
			for (var id : ids) {
				var info = info(id);
				if (info.isPresent() && info.get().getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
					long term = info.get().getRoleInfoProto().getLeaderInfo().getTerm();
					if (best == null || term > best.term()) {
						best = new Leader(id, term);
					}
				}
			}
			return Optional.ofNullable(best);
		}

		/** Waits for a leader among {@code ids} with at least {@code minTerm}. */
		Leader awaitLeader(List<String> ids, long minTerm, Duration timeout) {
			var found = new Leader[1];
			await(() -> {
				var leader = leader(ids);
				leader.filter(l -> l.term() >= minTerm).ifPresent(l -> found[0] = l);
				return found[0] != null;
			}, timeout, "a leader among " + ids + " with term >= " + minTerm);
			return found[0];
		}

		/** Waits until node {@code id} applied every entry the leader had committed when the wait began. */
		void awaitCaughtUp(String id, List<String> ids, Duration timeout) {
			var leader = awaitLeader(ids, 0, timeout);
			long committed = info(leader.id()).map(info -> info.getLogInfoProto().getCommitted().getIndex())
					.orElseThrow(() -> new IllegalStateException("Leader " + leader.id() + " went away"));
			await(() -> applied(id) >= committed, timeout, "node " + id + " to apply index " + committed);
		}

		/** Waits until every node applied the leader's whole log (call without load). */
		void awaitAllApplied(List<String> ids, Duration timeout) {
			await(() -> {
				var leader = leader(ids);
				if (leader.isEmpty()) {
					return false;
				}
				var info = info(leader.get().id());
				if (info.isEmpty()) {
					return false;
				}
				long last = info.get().getLogInfoProto().getLastEntry().getIndex();
				return ids.stream().allMatch(id -> applied(id) >= last);
			}, timeout, "every node to apply the whole log");
		}

		/** Waits until {@code leader} has had no answer from {@code follower} for longer than an election timeout. */
		void awaitUnresponsive(String leader, String follower, Duration timeout) {
			await(() -> info(leader).stream()
					.flatMap(info -> info.getRoleInfoProto().getLeaderInfo().getFollowerInfoList().stream())
					.anyMatch(rpc -> rpc.getId().getId().toStringUtf8().equals(follower)
							&& rpc.getLastRpcElapsedTimeMs() > ELECTION_TIMEOUT_MAX.toMillis()),
					timeout, "leader " + leader + " to notice that " + follower + " is down");
		}

		private long applied(String id) {
			return info(id).map(info -> info.getLogInfoProto().getApplied().getIndex()).orElse(-1L);
		}

		@Override
		public void close() throws IOException {
			client.close();
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Helpers

	/** Bounded wait for a condition, polled every 20 ms. */
	private static void await(BooleanSupplier condition, Duration timeout, String what) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("Timed out after " + timeout + " waiting for " + what);
			}
			try {
				Thread.sleep(RaftProbe.POLL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError("Interrupted while waiting for " + what, e);
			}
		}
	}

	private static Keys key(int writer, long version) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(((long) writer << 48) | version).array()));
	}

	private static Buf value(long version) {
		return Buf.wrap(("v" + version).getBytes(StandardCharsets.UTF_8));
	}

	private static long version(Buf value) {
		return Long.parseLong(new String(value.toByteArray(), StandardCharsets.UTF_8).substring(1));
	}

	private static String hex(Buf... parts) {
		var hex = new StringBuilder();
		for (var part : parts) {
			hex.append(part == null ? "null" : HexFormat.of().formatHex(part.toByteArray())).append('/');
		}
		return hex.toString();
	}
}
