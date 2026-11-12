package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Cold restarts of a three-process cluster: every node is SIGKILLed at once under concurrent write load, then the
 * whole cluster is started again from its data directories.
 *
 * <p>The load is merges with unique tokens (so a write applied twice or lost shows up as a token seen twice or
 * missing), puts of unique keys, blocks of puts followed by a {@code deleteRange} over part of the block, and one
 * {@code createColumn} under load. After every restart the test checks, through the cluster, that every
 * acknowledged write is present exactly once, that writes with an unknown outcome are present at most once, that
 * writes known to have failed are absent, and that each writer's acknowledged merges keep their order. At the end,
 * once every node has applied the whole log, every node is stopped and the replicas are compared byte for byte,
 * offline.</p>
 *
 * <p>Losing the un-synced RocksDB WAL tail, in two ways. With a positive {@code delay-wal-flush-duration} (5 seconds
 * by default, 1 second here) RocksDB buffers its WAL inside the process (manual WAL flush), so a SIGKILL drops the
 * writes since the last WAL flush from the local database, together with their applied index, while older writes
 * survive: the replicas reopen behind the Raft log (the reopened positions are printed) and must replay it without
 * applying an entry twice. And after each kill the test cuts the newest WAL file of one node at a random byte, like
 * a power loss that kept only a prefix of what was written without fsync, which usually tears the last record: the
 * replica must open the prefix and replay the rest.</p>
 *
 * <p>Not covered, as it needs a fault-injecting file system or machines whose power can be cut: losing data that was
 * fsynced (storage that acknowledges an fsync of the Raft log, a snapshot or an SST without persisting it), a power
 * loss that keeps later un-synced blocks but not earlier ones (the cut only models losing a suffix), and un-synced
 * loss in the other files of the data directory (Raft log segments are fsynced before an entry counts, RocksDB
 * syncs its SST files, MANIFEST and closed WALs itself).</p>
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class ColdRestartTest {

	private static final int CYCLES = 5;
	private static final int MERGE_WRITERS = 3;
	private static final int MERGE_KEYS = 16;
	/** Keys per block of the range writer; the first {@link #RANGE_DELETED} of them are then deleted by range. */
	private static final int RANGE_BLOCK_KEYS = 20;
	private static final int RANGE_DELETED = 10;
	/** The put writer creates the late column after this many puts. */
	private static final int LATE_COLUMN_AFTER_PUTS = 30;

	private static final String MERGES = "merges";
	private static final String PUTS = "puts";
	private static final String RANGES = "ranges";
	private static final String LATE = "late";
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	/** Bounds one attempt; longer than the nodes' request timeout, so the node reports its own failures. */
	private static final RequestContext REQUEST = RequestContext.batch(Duration.ofSeconds(8));
	/** How long one client call keeps failing over: long enough to wait through a whole-cluster restart. */
	private static final Duration RETRY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	/** From starting the processes until the cluster acknowledges a write and serves a read. */
	private static final Duration SERVE_BOUND = Duration.ofSeconds(30);
	private static final Duration LOAD_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration WRITER_STOP_TIMEOUT = Duration.ofSeconds(90);
	/**
	 * From the end of the load until every node has applied everything the leader committed, before the offline
	 * comparison. A follower learns that an entry is committed from the leader's next request, at the latest the next
	 * heartbeat (half the smallest election timeout: 200 ms here); the node that was down in the two-of-three scenario
	 * first has to receive and apply the {@link #CATCH_UP_WRITES} it missed. Measured: see the R66 entry of
	 * design.md.
	 */
	private static final Duration CONVERGENCE_TIMEOUT = Duration.ofSeconds(30);
	/** A member's line of {@code admin status}: id, role, applied index, commit index. */
	private static final Pattern STATUS_LINE =
			Pattern.compile("^(\\S+): (\\w+), applied index (\\d+), commit index (\\d+)");
	/**
	 * Acknowledged writes while one node is down in the two-of-three scenario, over the snapshot interval. The others
	 * keep the log it misses: Ratis purges only closed log segments, which hold 64 MB, so it catches up from the log;
	 * whether it installed a snapshot instead is printed.
	 */
	private static final int CATCH_UP_WRITES = 800;

	/**
	 * The merge operator of {@link InProcessCluster#MERGE_CONFIG}, and RocksDB's WAL written to the operating system
	 * only once a second (manual WAL flush), as with the default {@code delay-wal-flush-duration} of 5 seconds but
	 * often enough that a cycle of load usually has both a flushed prefix and a lost tail.
	 */
	private static final String ROCKSERVER_CONFIG = """
			database: {
			  global: {
			    delay-wal-flush-duration: PT1S
			    fallback-column-options: {
			      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
			    }
			  }
			}
			""";

	private static final Pattern WAL_FILE = Pattern.compile("\\d+\\.log");
	private static final long WAL_CUT_MAX_BYTES = 64 * 1024;
	private static final Pattern LEADING = Pattern.compile(": leading term (\\d+)$");
	private static final Pattern NOT_LEADING = Pattern.compile(": no longer leading term (\\d+)$");
	private static final Pattern REOPENED = Pattern.compile(": database open, applied position (.+)$");

	@TempDir
	Path root;

	@Test
	void everyAcknowledgedWriteSurvivesRepeatedColdRestarts() throws Exception {
		try (var cluster = startCluster(); var workload = new Workload(cluster)) {
			workload.createColumns();
			var report = new ArrayList<String>();
			for (int cycle = 1; cycle <= CYCLES; cycle++) {
				int threshold = ThreadLocalRandom.current().nextInt(150, 400);
				workload.start();
				workload.awaitAcknowledged(threshold);
				cluster.killAll();
				workload.stop();
				var cut = cutWalTail(cluster, cycle - 1);
				long restarted = System.nanoTime();
				for (var id : cluster.nodeIds()) {
					cluster.start(id);
				}
				cluster.waitReady(READY_TIMEOUT);
				var ready = Duration.ofNanos(System.nanoTime() - restarted);
				var serving = awaitServing(cluster, workload, restarted);
				workload.join();
				workload.verify("after cold restart " + cycle);
				report.add("cycle " + cycle + ": killed after " + workload.cycleAcknowledged()
						+ " acknowledged ops, all READY " + ready.toMillis() + " ms, serving " + serving.toMillis()
						+ " ms after start; node " + cut.node() + " lost the last " + cut.bytes() + " bytes of its WAL;"
						+ " reopened at " + reopenedPositions(cluster));
			}
			report.add(workload.summary());
			report.forEach(line -> System.out.println("ColdRestartTest: " + line));
			assertReplicasIdentical(cluster, workload);
		}
	}

	@Test
	void twoNodesServeAfterAColdRestartAndTheThirdConverges() throws Exception {
		try (var cluster = startCluster(); var workload = new Workload(cluster)) {
			workload.createColumns();
			workload.start();
			workload.awaitAcknowledged(300);
			String left = lastLeader(cluster);
			cluster.killAll();
			workload.stop();
			var first = cluster.nodeIds().stream().filter(id -> !id.equals(left)).toList();
			var cut = cutWalTail(cluster, cluster.nodeIds().indexOf(first.getFirst()));
			long restarted = System.nanoTime();
			for (var id : first) {
				cluster.start(id);
			}
			cluster.waitReady(READY_TIMEOUT);
			var serving = awaitServing(cluster, workload, restarted);
			workload.join();
			workload.verify("with nodes " + first + " restarted, " + left + " still down");

			// More load while the third node is down, which it has to catch up with when it returns.
			workload.start();
			workload.awaitAcknowledged(CATCH_UP_WRITES);
			workload.stop();
			workload.join();
			workload.verify("after more writes on nodes " + first);

			long returned = System.nanoTime();
			cluster.start(left);
			cluster.waitReady(left, READY_TIMEOUT);
			var rejoined = awaitServing(cluster, workload, returned);
			workload.start();
			workload.awaitAcknowledged(100);
			workload.stop();
			workload.join();
			workload.verify("after node " + left + " returned");
			System.out.println("ColdRestartTest: two of three (" + first + ", leader " + left + " left down, node "
					+ cut.node() + " lost the last " + cut.bytes() + " bytes of its WAL) serving " + serving.toMillis()
					+ " ms after start; node " + left + " serving " + rejoined.toMillis()
					+ " ms after its start, back for " + Duration.ofNanos(System.nanoTime() - returned).toMillis()
					+ " ms before the comparison, installed a snapshot: " + installedSnapshot(left) + "; "
					+ workload.summary());
			assertReplicasIdentical(cluster, workload);
		}
	}

	private ProcessCluster startCluster() throws IOException {
		Files.writeString(rockserverConfig(), ROCKSERVER_CONFIG);
		var cluster = ProcessCluster.start(root, 3, List.of("--rockserver-config", rockserverConfig().toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000",
				"--snapshot-every", "400"));
		try {
			cluster.waitReady(READY_TIMEOUT);
		} catch (RuntimeException e) {
			cluster.close();
			throw e;
		}
		return cluster;
	}

	private Path rockserverConfig() {
		return root.resolve("rockserver.conf");
	}

	/**
	 * Waits until the cluster acknowledges a write, then until every running node returns it from a linearizable
	 * read (a replica that failed to replay its log stops serving), with new clients so that no reconnection backoff
	 * of the load clients is counted. Fails unless all of it happens within {@link #SERVE_BOUND} of
	 * {@code startedNanos}; returns the time until the write was acknowledged.
	 */
	private static Duration awaitServing(ProcessCluster cluster, Workload workload, long startedNanos) {
		long deadline = startedNanos + SERVE_BOUND.toNanos();
		PutOp probe = null;
		try (var client = new ClusterClient(cluster.apiAddresses(), SERVE_BOUND, false)) {
			while (probe == null && System.nanoTime() - deadline < 0) {
				var attempt = workload.put(client, PUTS);
				probe = attempt.status() == OpStatus.OK ? attempt : null;
			}
		}
		if (probe == null) {
			return fail("The cluster did not acknowledge a write within " + SERVE_BOUND + " of its restart\n"
					+ allLogTails(cluster));
		}
		var acknowledged = Duration.ofNanos(System.nanoTime() - startedNanos);
		for (var id : cluster.nodeIds()) {
			if (!cluster.isRunning(id)) {
				continue;
			}
			var remaining = Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
			try (var direct = new ClusterClient(Map.of(id, cluster.apiAddress(id)), remaining, false)) {
				var read = direct.get(workload.columnId(PUTS), key(probe.key()), REQUEST);
				if (!read.acknowledged()) {
					fail("Node " + id + " does not serve within " + SERVE_BOUND + " of the restart: " + read.error()
							+ "\n" + allLogTails(cluster));
				}
				assertEquals(probe.value(), text(read.value()), "The acknowledged probe write read through node " + id);
			}
		}
		return acknowledged;
	}

	private static String allLogTails(ProcessCluster cluster) {
		return cluster.nodeIds().stream().map(cluster::logTail).reduce("", String::concat);
	}

	/**
	 * Waits until every node has applied the whole log ({@link #awaitConverged}), then stops every node gracefully and
	 * compares their databases offline: same columns with the same ids, same keys and values. The applied positions
	 * may still differ, by entries without content: a leader that stops hands leadership over, and the new leader's
	 * first entry reaches only the nodes still running.
	 *
	 * <p>Without the wait the comparison raced the leader's next heartbeat (R66): {@code verify} reads through the
	 * leader, and a follower stopped right after it had the last entries in its log without knowing them committed, so
	 * it stopped a few entries behind. Starting the cluster again for another try raced the same way, as each try
	 * wrote a probe ({@link #awaitServing}) and stopped the nodes within milliseconds of its acknowledgement.</p>
	 */
	private void assertReplicasIdentical(ProcessCluster cluster, Workload workload) throws Exception {
		var converged = awaitConverged(cluster);
		var exitCodes = new LinkedHashMap<String, Integer>();
		for (var id : cluster.nodeIds()) {
			cluster.stop(id);
			exitCodes.put(id, cluster.exitCode(id));
		}
		var replicas = new LinkedHashMap<String, Replica>();
		for (var id : cluster.nodeIds()) {
			replicas.put(id, readReplica(cluster.dataDir(id).resolve("db"), id));
		}
		var reference = replicas.values().iterator().next();
		boolean identical = replicas.values().stream()
				.allMatch(replica -> replica.content.equals(reference.content));
		if (!identical) {
			fail("Replicas differ, although every node had applied index " + converged.index() + " "
					+ converged.elapsed().toMillis() + " ms after the load: " + describeDifferences(replicas)
					+ "\nstatus before the stop:\n" + converged.status()
					+ "exit codes of the graceful stop (143 is SIGTERM): " + exitCodes + "\n" + allLogTails(cluster));
		}
		for (var replica : replicas.entrySet()) {
			assertTrue(replica.getValue().applied.getIndex() >= converged.index(), "Node " + replica.getKey()
					+ " had applied index " + converged.index() + " while running, and its database holds "
					+ replica.getValue().applied + " after a graceful stop (exit code " + exitCodes.get(replica.getKey())
					+ ")\n" + cluster.logTail(replica.getKey()));
		}
		assertEquals(workload.columnId(LATE), reference.columnIds.get(LATE), "late column id");
		System.out.println("ColdRestartTest: replicas identical at applied positions "
				+ replicas.values().stream().map(replica -> replica.applied.toString()).toList()
				+ ", " + reference.size() + " keys; every node had applied index " + converged.index() + " "
				+ converged.elapsed().toMillis() + " ms after the load");
	}

	/** The index every node had applied, how long after the call, and the {@code status} output that showed it. */
	private record Converged(long index, Duration elapsed, String status) {}

	/**
	 * Waits, with no load running, until {@code admin status} shows twice in a row one leader and the same applied
	 * and commit index on every node: nothing is left to commit or to apply, so replicas stopped now hold the same
	 * entries. Fails after {@link #CONVERGENCE_TIMEOUT} with the last status (role, applied and commit index of each
	 * node, or why it did not answer) and the nodes' log tails.
	 */
	private static Converged awaitConverged(ProcessCluster cluster) throws Exception {
		long started = System.nanoTime();
		Long previous = null;
		while (true) {
			var result = cluster.adminResult(cluster.nodeIds(), "status");
			var index = result.exitCode() == 0 ? convergedIndex(cluster.nodeIds(), result.output()) : null;
			var elapsed = Duration.ofNanos(System.nanoTime() - started);
			if (index != null && index.equals(previous)) {
				return new Converged(index, elapsed, memberLines(cluster.nodeIds(), result.output()));
			}
			if (elapsed.compareTo(CONVERGENCE_TIMEOUT) > 0) {
				var running = new TreeMap<String, String>();
				for (var id : cluster.nodeIds()) {
					running.put(id, cluster.isRunning(id) ? "running" : "exited with " + cluster.exitCode(id));
				}
				return fail("The nodes did not reach the same applied index within " + CONVERGENCE_TIMEOUT
						+ " without load; processes " + running + "; last status (exit code " + result.exitCode() + "):\n"
						+ memberLines(cluster.nodeIds(), result.output()) + allLogTails(cluster));
			}
			previous = index;
			Thread.sleep(50);
		}
	}

	/** The lines of {@code status} about the members {@code ids}, without their commit infos; all of it if none. */
	private static String memberLines(List<String> ids, String status) {
		var lines = status.lines()
				.filter(line -> ids.stream().anyMatch(id -> line.startsWith(id + ": ")))
				.map(line -> line.replaceFirst(", commit infos .*$", "") + "\n")
				.collect(Collectors.joining());
		return lines.isEmpty() ? status : lines;
	}

	/**
	 * The index that every one of {@code ids} has both applied and committed according to {@code status}, null if
	 * they differ, a node did not answer, or there is not exactly one leader.
	 */
	static Long convergedIndex(List<String> ids, String status) {
		var indexes = new HashMap<String, Long>();
		int leaders = 0;
		for (var line : status.lines().toList()) {
			var matcher = STATUS_LINE.matcher(line);
			if (!matcher.find() || !ids.contains(matcher.group(1))) {
				continue;
			}
			if (!matcher.group(3).equals(matcher.group(4)) || indexes.put(matcher.group(1),
					Long.parseLong(matcher.group(3))) != null) {
				return null;
			}
			if (matcher.group(2).equals("LEADER")) {
				leaders++;
			}
		}
		var distinct = new HashSet<>(indexes.values());
		return leaders == 1 && indexes.size() == ids.size() && distinct.size() == 1 ? distinct.iterator().next() : null;
	}

	/** One replica's database: user column ids and contents (hex keys and values), and its applied position. */
	private record Replica(Map<String, Long> columnIds, Map<String, Map<String, String>> content, TermIndex applied) {
		int size() {
			return content.values().stream().mapToInt(Map::size).sum();
		}
	}

	private Replica readReplica(Path dbDir, String id) throws IOException {
		var hex = HexFormat.of();
		try (var connection = new EmbeddedConnection(dbDir, "replica-" + id, rockserverConfig())) {
			var api = connection.getSyncApi(RequestContext.batch(Duration.ofMinutes(1)));
			var columnIds = new TreeMap<String, Long>();
			var content = new TreeMap<String, Map<String, String>>();
			TermIndex applied = null;
			for (var name : api.getAllColumnDefinitions().keySet()) {
				long columnId = api.getColumnId(name);
				if (name.equals(NodeStore.META_COLUMN)) {
					var value = api.get(0, columnId, NodeStore.key(NodeStore.APPLIED), RequestType.<Buf>current());
					applied = value == null ? TermIndex.INITIAL_VALUE : NodeStore.decodeTermIndex(value);
					continue;
				}
				columnIds.put(name, columnId);
				var entries = new TreeMap<String, String>();
				try (var stream = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
					stream.forEach(kv -> entries.put(hex.formatHex(kv.keys().keys()[0].toByteArray()),
							kv.value() == null ? "" : hex.formatHex(kv.value().toByteArray())));
				}
				content.put(name, entries);
			}
			assertNotNull(applied, "replica " + id + " has no " + NodeStore.META_COLUMN + " column");
			// The column ids are part of the replicated state too.
			content.put("(column ids)", new TreeMap<>(columnIds.entrySet().stream()
					.collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().toString()))));
			return new Replica(columnIds, content, applied);
		}
	}

	private static String describeDifferences(Map<String, Replica> replicas) {
		var text = new StringBuilder();
		var ids = List.copyOf(replicas.keySet());
		var reference = replicas.get(ids.getFirst());
		for (var id : ids) {
			text.append("\n  node ").append(id).append(": applied ").append(replicas.get(id).applied)
					.append(", ").append(replicas.get(id).size()).append(" keys");
		}
		for (var id : ids.subList(1, ids.size())) {
			var other = replicas.get(id);
			var columns = new TreeSet<String>(reference.content.keySet());
			columns.addAll(other.content.keySet());
			for (var column : columns) {
				var a = reference.content.getOrDefault(column, Map.of());
				var b = other.content.getOrDefault(column, Map.of());
				var keys = new TreeSet<String>(a.keySet());
				keys.addAll(b.keySet());
				int shown = 0;
				for (var key : keys) {
					if (!Objects.equals(a.get(key), b.get(key)) && shown++ < 5) {
						text.append("\n  ").append(column).append(" key ").append(key).append(": node ")
								.append(ids.getFirst()).append('=').append(a.get(key)).append(", node ").append(id)
								.append('=').append(b.get(key));
					}
				}
			}
		}
		return text.toString();
	}

	/**
	 * The node that led the latest term, from the nodes' own logs since their last start, or the first node if no
	 * node logged its leadership.
	 */
	private String lastLeader(ProcessCluster cluster) throws IOException {
		String leader = cluster.nodeIds().getFirst();
		long leaderTerm = -1;
		for (var id : cluster.nodeIds()) {
			long led = -1;
			for (var line : linesSinceLastStart(id)) {
				var leading = LEADING.matcher(line);
				if (leading.find()) {
					led = Math.max(led, Long.parseLong(leading.group(1)));
				}
				var notLeading = NOT_LEADING.matcher(line);
				if (notLeading.find() && Long.parseLong(notLeading.group(1)) == led) {
					led = -1;
				}
			}
			if (led > leaderTerm) {
				leader = id;
				leaderTerm = led;
			}
		}
		return leader;
	}

	/**
	 * Cuts the newest RocksDB WAL file of a stopped node at a random byte of its last {@link #WAL_CUT_MAX_BYTES},
	 * like a power loss that kept only a prefix of what RocksDB wrote without fsync, so that the last record is
	 * usually torn. Takes the first node, in id order from the {@code turn}-th one, whose newest WAL file is not empty
	 * (it is empty right after a flush, as when a snapshot was taken).
	 */
	private static WalCut cutWalTail(ProcessCluster cluster, int turn) throws IOException {
		var ids = cluster.nodeIds();
		for (int i = 0; i < ids.size(); i++) {
			var id = ids.get((turn + i) % ids.size());
			Path newest;
			try (var files = Files.walk(cluster.dataDir(id).resolve("db"))) {
				newest = files.filter(Files::isRegularFile)
						.filter(path -> WAL_FILE.matcher(path.getFileName().toString()).matches())
						.max(Comparator.comparingLong(ColdRestartTest::walNumber))
						.orElseThrow(() -> new AssertionError("Node " + id + " has no WAL file"));
			}
			long size = Files.size(newest);
			if (size > 0) {
				long length = ThreadLocalRandom.current().nextLong(Math.max(0, size - WAL_CUT_MAX_BYTES), size);
				try (var channel = FileChannel.open(newest, StandardOpenOption.WRITE)) {
					channel.truncate(length);
				}
				return new WalCut(id, size - length);
			}
		}
		return new WalCut(ids.get(turn % ids.size()), 0);
	}

	private record WalCut(String node, long bytes) {}

	private static long walNumber(Path wal) {
		var name = wal.getFileName().toString();
		return Long.parseLong(name.substring(0, name.length() - ".log".length()));
	}

	/** True if the node logged that it installed a snapshot from the leader since its last start. */
	private boolean installedSnapshot(String id) throws IOException {
		return linesSinceLastStart(id).stream().anyMatch(line -> line.endsWith(": installing snapshot"));
	}

	/** The applied position each node logged when it reopened its database at its last start. */
	private Map<String, String> reopenedPositions(ProcessCluster cluster) throws IOException {
		var positions = new TreeMap<String, String>();
		for (var id : cluster.nodeIds()) {
			for (var line : linesSinceLastStart(id)) {
				var matcher = REOPENED.matcher(line);
				if (matcher.find()) {
					positions.put(id, matcher.group(1));
				}
			}
		}
		return positions;
	}

	/** The node's log (stderr, where the nodes log) after the harness marker of its last start. */
	private List<String> linesSinceLastStart(String id) throws IOException {
		var log = root.resolve(Network.hostname(id)).resolve("stderr.log");
		var lines = Files.readAllLines(log, StandardCharsets.UTF_8);
		int start = 0;
		for (int i = 0; i < lines.size(); i++) {
			if (lines.get(i).startsWith("---- harness: start #")) {
				start = i + 1;
			}
		}
		return lines.subList(start, lines.size());
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static long decodeKey(Keys keys) {
		return ByteBuffer.wrap(keys.keys()[0].toByteArray()).getLong();
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static @Nullable String text(@Nullable Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	/** A merge of {@code token} into {@code key} by merge writer {@code writer}. */
	private record MergeOp(int writer, long key, String token, OpStatus status) {}

	private record PutOp(String column, long key, String value, OpStatus status) {}

	/** Puts of one block of the range writer, then a deleteRange of its first keys (null if never sent). */
	private record RangeBlock(long start, List<PutOp> puts, @Nullable OpStatus deleteStatus) {}

	/**
	 * Concurrent writers against the cluster, recording the outcome of every operation, and the checks of what the
	 * cluster holds against those outcomes. Writers run between {@link #start} and {@link #stop}; the records and
	 * the checks cover every cycle so far.
	 */
	private static final class Workload implements AutoCloseable {

		private final ClusterClient client;
		private final Map<String, Long> columns = new ConcurrentHashMap<>();
		private final Queue<MergeOp> merges = new ConcurrentLinkedQueue<>();
		private final Queue<PutOp> puts = new ConcurrentLinkedQueue<>();
		private final Queue<RangeBlock> ranges = new ConcurrentLinkedQueue<>();
		private final Queue<Throwable> failures = new ConcurrentLinkedQueue<>();
		private final AtomicLong tokens = new AtomicLong();
		private final AtomicLong putKeys = new AtomicLong();
		private final AtomicLong rangeBlocks = new AtomicLong();
		private final AtomicInteger acknowledged = new AtomicInteger();
		private final List<Thread> threads = new ArrayList<>();
		private volatile boolean running;
		private int cycleAcknowledged;

		Workload(ProcessCluster cluster) {
			this.client = new ClusterClient(cluster.apiAddresses(), RETRY_TIMEOUT, false);
		}

		void createColumns() {
			for (var name : List.of(MERGES, PUTS, RANGES)) {
				columns.put(name, createColumn(name));
			}
		}

		/** Creating a column that exists with the same schema returns its id, so retrying after any failure is safe. */
		private long createColumn(String name) {
			long deadline = System.nanoTime() + RETRY_TIMEOUT.toNanos();
			while (true) {
				var result = client.createColumn(name, SCHEMA, REQUEST);
				if (result.acknowledged()) {
					return result.value();
				}
				if (System.nanoTime() - deadline > 0) {
					throw new AssertionError("Could not create column " + name, result.error());
				}
			}
		}

		long columnId(String name) {
			var id = columns.get(name);
			assertNotNull(id, "column " + name + " was never created");
			return id;
		}

		void start() {
			running = true;
			acknowledged.set(0);
			for (int w = 0; w < MERGE_WRITERS; w++) {
				int writer = w;
				threads.add(Thread.ofPlatform().name("merge-writer-" + w)
						.start(() -> guarded(() -> mergeWriter(writer))));
			}
			threads.add(Thread.ofPlatform().name("put-writer").start(() -> guarded(this::putWriter)));
			threads.add(Thread.ofPlatform().name("range-writer").start(() -> guarded(this::rangeWriter)));
		}

		/** Waits until {@code count} operations were acknowledged since {@link #start} (and the late column exists). */
		void awaitAcknowledged(int count) {
			InProcessCluster.await(LOAD_TIMEOUT, () -> {
				assertTrue(failures.isEmpty(), () -> "A writer failed: " + failures.peek());
				return acknowledged.get() >= count && columns.containsKey(LATE);
			});
		}

		/** Asks the writers to stop after their current operation. */
		void stop() {
			running = false;
		}

		void join() throws InterruptedException {
			for (var thread : threads) {
				if (!thread.join(WRITER_STOP_TIMEOUT)) {
					fail("Writer " + thread.getName() + " did not stop within " + WRITER_STOP_TIMEOUT);
				}
			}
			threads.clear();
			cycleAcknowledged = acknowledged.get();
			if (!failures.isEmpty()) {
				var error = new AssertionError("A writer failed", failures.peek());
				failures.forEach(error::addSuppressed);
				throw error;
			}
		}

		int cycleAcknowledged() {
			return cycleAcknowledged;
		}

		private void guarded(Runnable body) {
			try {
				body.run();
			} catch (Throwable e) {
				failures.add(e);
			}
		}

		private void mergeWriter(int writer) {
			long column = columnId(MERGES);
			while (running) {
				long key = ThreadLocalRandom.current().nextInt(MERGE_KEYS);
				var token = "w" + writer + "-" + tokens.incrementAndGet();
				var result = client.merge(column, key(key), text(token), REQUEST);
				merges.add(new MergeOp(writer, key, token, result.status()));
				count(result.status());
			}
		}

		private void putWriter() {
			int written = 0;
			while (running) {
				if (!columns.containsKey(LATE) && ++written > LATE_COLUMN_AFTER_PUTS) {
					columns.put(LATE, createColumn(LATE));
					continue;
				}
				var column = columns.containsKey(LATE) && putKeys.get() % 2 == 1 ? LATE : PUTS;
				count(put(client, column).status());
			}
		}

		private void rangeWriter() {
			while (running) {
				long start = rangeBlocks.getAndIncrement() * RANGE_BLOCK_KEYS;
				var blockPuts = new ArrayList<PutOp>();
				OpStatus deleteStatus = null;
				for (int i = 0; i < RANGE_BLOCK_KEYS && running; i++) {
					var put = put(client, RANGES, start + i);
					blockPuts.add(put);
					count(put.status());
				}
				if (blockPuts.size() == RANGE_BLOCK_KEYS) {
					long column = columnId(RANGES);
					var result = client.write(REQUEST, api -> {
						api.deleteRange(column, key(start), key(start + RANGE_DELETED));
						return null;
					});
					deleteStatus = result.status();
					count(deleteStatus);
				}
				ranges.add(new RangeBlock(start, List.copyOf(blockPuts), deleteStatus));
			}
		}

		private void count(OpStatus status) {
			if (status == OpStatus.OK) {
				acknowledged.incrementAndGet();
			}
		}

		/** Puts a new unique key into {@code column} through {@code client} and records the outcome. */
		PutOp put(ClusterClient client, String column) {
			return put(client, column, putKeys.incrementAndGet());
		}

		private PutOp put(ClusterClient client, String column, long key) {
			var value = column + "-" + key;
			var result = client.put(columnId(column), key(key), text(value), REQUEST);
			var op = new PutOp(column, key, value, result.status());
			if (!column.equals(RANGES)) {
				puts.add(op);
			}
			return op;
		}

		/** Checks what the cluster holds against the outcome of every operation so far. */
		void verify(String when) {
			var problems = new ArrayList<String>();
			verifyMerges(read(MERGES), problems);
			verifyPuts(problems);
			verifyRanges(read(RANGES), problems);
			var late = client.getColumnId(LATE, REQUEST);
			if (!late.acknowledged() || late.value() != columnId(LATE)) {
				problems.add("column " + LATE + " has id " + late.value() + " (" + late.error() + "), created as "
						+ columnId(LATE));
			}
			if (!problems.isEmpty()) {
				fail(when + ": " + problems.size() + " problems, first ones:\n  "
						+ String.join("\n  ", problems.subList(0, Math.min(20, problems.size()))));
			}
		}

		private Map<Long, String> read(String column) {
			long columnId = columnId(column);
			var result = client.read(REQUEST, api -> {
				try (var stream = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
					return stream.toList();
				}
			});
			var values = new HashMap<Long, String>();
			for (KV kv : result.valueOrThrow()) {
				values.put(decodeKey(kv.keys()), text(kv.value()));
			}
			return values;
		}

		private void verifyMerges(Map<Long, String> values, List<String> problems) {
			// Where each token is, and how often.
			var positions = new HashMap<String, Integer>();
			var keys = new HashMap<String, Long>();
			var counts = new HashMap<String, Integer>();
			values.forEach((key, value) -> {
				var list = value.split(",");
				for (int i = 0; i < list.length; i++) {
					counts.merge(list[i], 1, Integer::sum);
					positions.put(list[i], i);
					keys.put(list[i], key);
				}
			});
			var lastPosition = new HashMap<String, Integer>();
			var recorded = new HashMap<String, MergeOp>();
			for (var op : merges) {
				recorded.put(op.token(), op);
				int count = counts.getOrDefault(op.token(), 0);
				boolean allowed = switch (op.status()) {
					case OK -> count == 1;
					case UNKNOWN -> count <= 1;
					case FAILED -> count == 0;
				};
				if (!allowed) {
					problems.add("merge " + op + " is present " + count + " times");
				}
				if (count > 0 && keys.get(op.token()) != op.key()) {
					problems.add("merge " + op + " is in key " + keys.get(op.token()));
				}
				if (op.status() == OpStatus.OK && count == 1) {
					// A writer's acknowledged merges into one key are applied in the order it sent them.
					var writerKey = op.writer() + "/" + op.key();
					int position = positions.get(op.token());
					var previous = lastPosition.put(writerKey, position);
					if (previous != null && previous >= position) {
						problems.add("merge " + op + " is at position " + position + ", before an earlier acknowledged"
								+ " merge of the same writer at " + previous);
					}
				}
			}
			counts.keySet().stream().filter(token -> !recorded.containsKey(token))
					.forEach(token -> problems.add("token " + token + " was never written"));
		}

		private void verifyPuts(List<String> problems) {
			var byColumn = new HashMap<String, Map<Long, String>>();
			for (var column : List.of(PUTS, LATE)) {
				byColumn.put(column, read(column));
			}
			var expectedKeys = new HashMap<String, Set<Long>>();
			for (var op : puts) {
				var values = byColumn.get(op.column());
				expectedKeys.computeIfAbsent(op.column(), _ -> new HashSet<>()).add(op.key());
				checkPut(op, values.get(op.key()), op.status() == OpStatus.OK, op.status() == OpStatus.FAILED,
						problems);
			}
			byColumn.forEach((column, values) -> values.keySet().stream()
					.filter(key -> !expectedKeys.getOrDefault(column, Set.of()).contains(key))
					.forEach(key -> problems.add(column + " key " + key + " was never written")));
		}

		private void verifyRanges(Map<Long, String> values, List<String> problems) {
			var written = new HashSet<Long>();
			for (var block : ranges) {
				for (var op : block.puts()) {
					written.add(op.key());
					var actual = values.get(op.key());
					if (op.key() - block.start() >= RANGE_DELETED) {
						checkPut(op, actual, op.status() == OpStatus.OK, op.status() == OpStatus.FAILED, problems);
						continue;
					}
					// An acknowledged put was applied before the deleteRange was sent; one with an unknown outcome
					// may be applied after it.
					var delete = block.deleteStatus();
					boolean mustBePresent = op.status() == OpStatus.OK && (delete == null || delete == OpStatus.FAILED);
					boolean mustBeAbsent = op.status() == OpStatus.FAILED
							|| (op.status() == OpStatus.OK && delete == OpStatus.OK);
					checkPut(op, actual, mustBePresent, mustBeAbsent, problems);
				}
			}
			values.keySet().stream().filter(key -> !written.contains(key))
					.forEach(key -> problems.add(RANGES + " key " + key + " was never written"));
		}

		private static void checkPut(PutOp op, @Nullable String actual, boolean mustBePresent, boolean mustBeAbsent,
				List<String> problems) {
			if (actual == null ? mustBePresent : mustBeAbsent || !actual.equals(op.value())) {
				problems.add("put " + op + " reads " + actual);
			}
		}

		String summary() {
			var statuses = new TreeMap<String, Integer>();
			merges.forEach(op -> statuses.merge("merge " + op.status(), 1, Integer::sum));
			puts.forEach(op -> statuses.merge("put " + op.status(), 1, Integer::sum));
			ranges.forEach(block -> {
				block.puts().forEach(op -> statuses.merge("range put " + op.status(), 1, Integer::sum));
				if (block.deleteStatus() != null) {
					statuses.merge("deleteRange " + block.deleteStatus(), 1, Integer::sum);
				}
			});
			return "operations by outcome " + statuses;
		}

		@Override
		public void close() throws InterruptedException {
			running = false;
			for (var thread : threads) {
				thread.join(WRITER_STOP_TIMEOUT);
			}
			client.close();
		}
	}
}
