package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.apache.ratis.thirdparty.io.grpc.Status;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests of the cluster operations' pieces: entries, wire answers, digests, metrics export, monitor helpers. */
class ReplicaOpsTest {

	@TempDir
	Path temp;

	@Test
	void maintenanceEntriesRoundTrip() {
		for (LogEntry entry : List.of(new LogEntry.ClusterVersion(Capabilities.SUPPORTED), new LogEntry.Checksum(),
				new LogEntry.Backup("nightly-2026.11.06_a", 0x1234_5678_9abcL))) {
			byte[] encoded = LogEntryCodec.encode(entry);
			var decoded = LogEntryCodec.decode(encoded);
			assertEquals(entry, decoded);
			assertArrayEquals(encoded, LogEntryCodec.encode(decoded));
			assertEquals(LogEntryCodec.encodeEntryBody(entry).length, LogEntryCodec.entrySize(entry));
			assertFalse(Applier.isBatchable(entry), "applied alone, at one position");
		}
	}

	@Test
	void backupNames() {
		assertTrue(ReplicaOps.validBackupName("a"));
		assertTrue(ReplicaOps.validBackupName("2026-11-06.full_1"));
		for (String bad : new String[] {"", ".hidden", "../x", "a/b", "a b", "x".repeat(65), null}) {
			assertFalse(ReplicaOps.validBackupName(bad), String.valueOf(bad));
		}
		assertThrows(IllegalArgumentException.class, () -> new LogEntry.Backup("../etc", 1));
	}

	@Test
	void resultsRoundTrip() {
		for (var result : List.of(new ReplicaOps.Result(ReplicaOps.State.DONE, 42, 7, "abcdef"),
				new ReplicaOps.Result(ReplicaOps.State.PENDING, -1, 0, ""),
				new ReplicaOps.Result(ReplicaOps.State.FAILED, 3, 0, "java.io.IOException: disk full é"))) {
			assertEquals(result, ReplicaOps.decodeResult(ReplicaOps.encodeResult(result)));
		}
	}

	private static ReplicaOps.Result done(String digest) {
		return new ReplicaOps.Result(ReplicaOps.State.DONE, 9, 3, digest);
	}

	/** {@code admin checksum} says OK only with a digest from every member, all equal. */
	@Test
	void checksumVerdict() {
		var members = List.of("n0", "n1", "n2");
		var none = new ReplicaOps.Result(ReplicaOps.State.NONE, 9, 0, "");
		var failed = new ReplicaOps.Result(ReplicaOps.State.FAILED, 9, 0, "closed");
		assertNull(ReplicatedServerMain.checksumVerdict(9, members, Map.of("n0", done("a"), "n1", done("a"), "n2", done("a")),
				false));
		assertEquals(1, ReplicatedServerMain.checksumVerdict(9, members,
				Map.of("n0", done("a"), "n1", done("b"), "n2", done("a")), false).exitCode());
		// Missing, failed, skipped, or no digest at all: incomplete, never OK.
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, ReplicatedServerMain.checksumVerdict(9, members,
				Map.of("n0", done("a"), "n1", done("a")), false).exitCode());
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, ReplicatedServerMain.checksumVerdict(9, members,
				Map.of("n0", done("a"), "n1", failed, "n2", done("a")), false).exitCode());
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, ReplicatedServerMain.checksumVerdict(9, members, Map.of(), false)
				.exitCode());
		var skipped = Map.of("n0", done("a"), "n1", none, "n2", done("a"));
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE,
				ReplicatedServerMain.checksumVerdict(9, members, skipped, false).exitCode());
		assertNull(ReplicatedServerMain.checksumVerdict(9, members, skipped, true));
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, ReplicatedServerMain.checksumVerdict(9, members,
				Map.of("n0", none, "n1", none, "n2", none), true).exitCode(), "no digest at all");
	}

	/** {@code admin backup} says OK only if every member wrote it. */
	@Test
	void backupVerdict() {
		var members = List.of("n0", "n1", "n2");
		var failed = new ReplicaOps.Result(ReplicaOps.State.FAILED, 9, 0, "disk full");
		assertNull(ReplicatedServerMain.backupVerdict(members, Map.of("n0", done("d"), "n1", done("d"), "n2", done("d"))));
		assertEquals(1, ReplicatedServerMain.backupVerdict(members,
				Map.of("n0", done("d"), "n1", failed, "n2", done("d"))).exitCode());
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, ReplicatedServerMain.backupVerdict(members,
				Map.of("n0", done("d"), "n2", done("d"))).exitCode());
	}

	/**
	 * Only an answer that proves nothing was proposed lets admin propose a checksum again: a member's own marked
	 * UNAVAILABLE (also an older member's "The node is starting") or NOT_FOUND, not a bare UNAVAILABLE of a transport
	 * that broke mid-call (R40-1).
	 */
	@Test
	void retriedOnlyWhenNothingWasProposed() {
		assertTrue(ReplicatedServerMain.notProposed(Status.UNAVAILABLE.withDescription(ReplicaOps.NOT_PROPOSED + "x")));
		assertTrue(ReplicatedServerMain.notProposed(Status.UNAVAILABLE.withDescription("The node is starting")));
		assertTrue(ReplicatedServerMain.notProposed(Status.NOT_FOUND.withDescription("No group x")));
		assertFalse(ReplicatedServerMain.notProposed(Status.UNAVAILABLE));
		assertFalse(ReplicatedServerMain.notProposed(Status.UNAVAILABLE.withDescription("Network closed for unknown reason")));
		// What a caller sees of a member whose connection is reset mid-call, and of one whose server runs shutdownNow
		// (RST_STREAM CANCEL): Review of R40, 1 to 3.
		assertFalse(ReplicatedServerMain.notProposed(Status.UNAVAILABLE.withDescription("io exception")));
		assertFalse(ReplicatedServerMain.notProposed(
				Status.CANCELLED.withDescription("RST_STREAM closed stream. HTTP/2 error code: CANCEL")));
		assertFalse(ReplicatedServerMain.notProposed(Status.ABORTED.withDescription(ReplicaOps.NOT_PROPOSED + "x")));
		assertFalse(ReplicatedServerMain.notProposed(Status.DEADLINE_EXCEEDED));
	}

	/**
	 * R40-1: a node that is starting (no proposer yet) refuses a proposal with UNAVAILABLE and a description that marks
	 * it as its own proof that nothing was proposed, so that {@code admin} tells it apart from a transport UNAVAILABLE.
	 */
	@Test
	void aStartingNodeMarksItsRefusalAsNotProposed() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.clusterVersion(Capabilities.SUPPORTED);
			var error = assertThrows(CompletionException.class,
					() -> ops.answer(ReplicaOps.request(ReplicaOps.OP_PROPOSE_CHECKSUM, new byte[0])).join());
			var status = Status.fromThrowable(error.getCause());
			assertEquals(Status.Code.UNAVAILABLE, status.getCode(), status.toString());
			assertTrue(ReplicatedServerMain.notProposed(status), status.toString());
			assertTrue(status.getDescription().startsWith("Not proposed: "), status.toString());
		}
	}

	/** add-peer refuses a node lacking a bit that the cluster uses, or that every member supports (about to be used). */
	@Test
	void addedNodeNeedsCurrentAndPendingCapabilities() {
		long old = Capabilities.PIPELINED_UPDATES | Capabilities.CLIENT_SESSIONS;
		assertEquals(0, ReplicatedServerMain.missingCapabilities(0, 0, 0));
		assertEquals(Capabilities.CHECKSUM | Capabilities.BACKUP | Capabilities.CHECKSUM_SCHEMAS,
				ReplicatedServerMain.missingCapabilities(0, Capabilities.SUPPORTED, old), "a raise is pending");
		assertEquals(Capabilities.CLIENT_SESSIONS,
				ReplicatedServerMain.missingCapabilities(Capabilities.CLIENT_SESSIONS, 0, 0));
		assertEquals(0, ReplicatedServerMain.missingCapabilities(old, old, Capabilities.SUPPORTED));
	}

	@Test
	void capabilities() {
		assertEquals("none", Capabilities.describe(0));
		assertEquals("pipelined-updates,client-sessions,checksum,backup,checksum-schemas",
				Capabilities.describe(Capabilities.SUPPORTED));
		assertTrue(Capabilities.describe(1L << 40).contains("0x"));
		assertTrue(Capabilities.has(Capabilities.GENESIS, Capabilities.CLIENT_SESSIONS));
		assertFalse(Capabilities.has(Capabilities.CHECKSUM, Capabilities.CHECKSUM | Capabilities.BACKUP));
	}

	/** Entries whose bytes could be cut differently must not digest the same. */
	@Test
	void digestSeparatesKeyPartsAndValues() throws Exception {
		var a = digest(new KV(new Keys(buf("ab"), buf("c")), buf("d")));
		var b = digest(new KV(new Keys(buf("a"), buf("bc")), buf("d")));
		var c = digest(new KV(new Keys(buf("ab"), buf("c")), null));
		var d = digest(new KV(new Keys(buf("ab"), buf("c")), buf("")));
		assertNotEquals(a, b);
		assertNotEquals(c, d);
		assertEquals(a, digest(new KV(new Keys(buf("ab"), buf("c")), buf("d"))));
	}

	private static String digest(KV kv) throws Exception {
		var digest = MessageDigest.getInstance("SHA-256");
		ReplicaOps.digestEntry(digest, kv);
		return HexFormat.of().formatHex(digest.digest());
	}

	private static Buf buf(String text) {
		return Buf.wrap(text.getBytes(StandardCharsets.UTF_8));
	}

	@Test
	void metricsAreExportedCumulativeWithTheirKind() {
		var metrics = new NodeMetrics("n7");
		try {
			var apply = metrics.duration("apply");
			apply.record(TimeUnit.MILLISECONDS.toNanos(3));
			var registry = new SimpleMeterRegistry();
			metrics.bind(registry);
			apply.record(TimeUnit.MILLISECONDS.toNanos(5));
			var batch = metrics.amount("writes.batchSize");
			batch.record(10);
			batch.record(20);
			metrics.gauge("leader.lockedKeys", () -> 4);
			metrics.exportedGauge("raft.term", () -> 9);

			FunctionTimer timer = registry.find("rockserver.replication.duration").tag("name", "apply").functionTimer();
			assertEquals(2, timer.count());
			assertEquals(8, timer.totalTime(TimeUnit.MILLISECONDS), 0.001);
			assertEquals("apply", timer.getId().getTag("kind"));
			assertEquals("n7", timer.getId().getTag("node"));
			FunctionCounter count = registry.find("rockserver.replication.count").tag("name", "writes.batchSize")
					.functionCounter();
			FunctionCounter amount = registry.find("rockserver.replication.amount").tag("name", "writes.batchSize")
					.functionCounter();
			assertEquals(2, count.count());
			assertEquals(30, amount.count());
			assertEquals("client", count.getId().getTag("kind"));
			Gauge locked = registry.find("rockserver.replication.leader.lockedKeys").gauge();
			assertEquals(4, locked.value());
			assertEquals(9, registry.find("rockserver.replication.raft.term").tag("node", "n7").gauge().value());

			// A new database, a new registry: everything moves along.
			var reopened = new SimpleMeterRegistry();
			metrics.bind(reopened);
			assertEquals(2, reopened.find("rockserver.replication.duration").tag("name", "apply").functionTimer().count());
			assertEquals(9, reopened.find("rockserver.replication.raft.term").gauge().value());

			// A database whose meters are tagged with a configured node name: the replication meters follow it.
			var named = new SimpleMeterRegistry();
			metrics.bind(named, "host-7");
			assertEquals("host-7", named.find("rockserver.replication.duration").tag("name", "apply").functionTimer()
					.getId().getTag("node"));
			assertEquals(9, named.find("rockserver.replication.raft.term").tag("node", "host-7").gauge().value());
		} finally {
			metrics.close();
		}
	}

	/** A blank {@code node-name} (the default config's) means unset: the node id tags the core meters. */
	@Test
	void blankNodeNameMeansTheNodeId() throws Exception {
		Path blank = temp.resolve("blank.conf");
		Files.writeString(blank, "database.metrics.node-name = \"\"\n");
		try (var store = NodeStore.open(temp.resolve("db1"), blank, "n-blank")) {
			assertEquals("n-blank", store.metricsNodeName("fallback"));
		}
		Path named = temp.resolve("named.conf");
		Files.writeString(named, "database.metrics.node-name = host-1\n");
		try (var store = NodeStore.open(temp.resolve("db2"), named, "n-named")) {
			assertEquals("host-1", store.metricsNodeName("fallback"));
		}
	}

	@Test
	void kinds() {
		assertEquals("apply", NodeMetrics.kindOf("apply.commit"));
		assertEquals("apply", NodeMetrics.kindOf("snapshot"));
		assertEquals("apply", NodeMetrics.kindOf("raft.appendToApply"));
		assertEquals("raft", NodeMetrics.kindOf("raft.admit"));
		assertEquals("raft", NodeMetrics.kindOf("lag.unapplied"));
		assertEquals("raft", NodeMetrics.kindOf("leader.stall.stepdowns"));
		assertEquals("client", NodeMetrics.kindOf("read.barrier"));
		assertEquals("client", NodeMetrics.kindOf("forward"));
	}

	@Test
	void lagSamplePeriod() {
		var previous = System.getProperty(NodeMetrics.LAG_SAMPLE_PROPERTY);
		try {
			System.clearProperty(NodeMetrics.LAG_SAMPLE_PROPERTY);
			assertEquals(10, NodeMetrics.lagSamplePeriodMs());
			System.setProperty(NodeMetrics.LAG_SAMPLE_PROPERTY, "0");
			assertEquals(0, NodeMetrics.lagSamplePeriodMs());
			System.setProperty(NodeMetrics.LAG_SAMPLE_PROPERTY, "nope");
			assertEquals(10, NodeMetrics.lagSamplePeriodMs());
		} finally {
			if (previous == null) {
				System.clearProperty(NodeMetrics.LAG_SAMPLE_PROPERTY);
			} else {
				System.setProperty(NodeMetrics.LAG_SAMPLE_PROPERTY, previous);
			}
		}
	}

	@Test
	void raftLogBytesCountsSegmentsOnly() throws Exception {
		Files.write(temp.resolve("log_0-99"), new byte[100]);
		Files.write(temp.resolve("log_inprogress_100"), new byte[50]);
		Files.write(temp.resolve("raft-meta"), new byte[1000]);
		assertEquals(150, NodeMonitor.raftLogBytes(temp));
		assertTrue(Double.isNaN(NodeMonitor.raftLogBytes(temp.resolve("missing"))));
	}

	@Test
	void alarmLogsAtStartRepeatsAndEnds() {
		var alarm = new NodeMonitor.Alarm("x");
		assertFalse(alarm.cleared());
		assertTrue(alarm.raised(1_000));
		assertFalse(alarm.raised(2_000));
		assertTrue(alarm.raised(61_000));
		assertTrue(alarm.cleared());
		assertFalse(alarm.cleared());
	}

	/**
	 * R53-3: a line of {@code replica-ops.log} cut inside a multi-byte character (an append cut by a full disk or a
	 * power loss, with a non-ASCII data directory in a {@code DONE} line) is skipped, and the lines around it still load.
	 * Before, {@code Files.readAllLines} threw {@code MalformedInputException} and no line loaded, at every start.
	 */
	@Test
	void aResultLineCutInsideACharacterIsSkippedAndTheOthersLoad() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		byte[] cut = "è".getBytes(StandardCharsets.UTF_8);
		var log = new java.io.ByteArrayOutputStream();
		log.writeBytes("2026-11-01T10:00:00Z checksum 7 DONE 3 abc\n".getBytes(StandardCharsets.UTF_8));
		log.writeBytes("2026-11-01T10:01:00Z backup 8 DONE b1 /data/caff".getBytes(StandardCharsets.UTF_8));
		log.write(cut[0]);
		log.writeBytes("\n".getBytes(StandardCharsets.UTF_8));
		log.writeBytes("2026-11-01T10:02:00Z checksum 9 DONE 4 def\n".getBytes(StandardCharsets.UTF_8));
		log.writeBytes("2026-11-01T10:03:00Z backup 10 DONE b2 /data/caffè\n".getBytes(StandardCharsets.UTF_8));
		Files.write(temp.resolve("replica-ops.log"), log.toByteArray());

		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 9L), ops.checksumResults().stream().map(ReplicaOps.Result::index).toList());
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 7, 3, "abc"), ops.checksumResult(7));
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 9, 4, "def"), ops.checksumResult(9));
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("b2", 10).state());
			assertEquals("/data/caffè", ops.backupResult("b2", 10).detail());
		}
	}

	/**
	 * R53-3 review: a last line torn before its {@code \n} (an append cut by a full disk or a power loss) costs only its
	 * own result. Before, the first result written after the restart was appended to the same physical line, and that
	 * merged line was rejected at the next start too (here {@code State.valueOf("DO2026-...")}).
	 */
	@Test
	void aResultWrittenAfterATornLastLineStartsOnALineOfItsOwn() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Files.writeString(temp.resolve("replica-ops.log"), "2026-11-01T10:00:00Z checksum 7 DONE 3 abc\n"
				+ "2026-11-01T10:01:00Z checksum 8 DO", StandardCharsets.UTF_8);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L), ops.checksumResults().stream().map(ReplicaOps.Result::index).toList());
			ops.persist("checksum 11 DONE 5 ghi");
			ops.persist("checksum 12 DONE 6 jkl");
		}
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 11L, 12L), ops.checksumResults().stream().map(ReplicaOps.Result::index).toList());
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 11, 5, "ghi"), ops.checksumResult(11));
		}
	}

	private static final String DIGEST = "0123456789abcdef".repeat(4);

	private static List<Long> checksumIndexes(ReplicaOps ops) {
		return ops.checksumResults().stream().map(ReplicaOps.Result::index).toList();
	}

	/**
	 * R54-1: an append cut short while the node runs (a full disk, then space freed), inside the state of its line or
	 * inside its digest, costs only its own result. Before, the flag that makes the next append start on a line of its
	 * own was set only at the start, so the next result was glued onto the fragment: at the next start the merged line
	 * was rejected (a bad state), or loaded as the cut result with a digest that held the next line, and the next
	 * result was lost either way.
	 */
	@Test
	void anAppendCutWhileTheNodeRunsCostsOnlyItsOwnResult() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.persist("checksum 7 DONE 3 " + DIGEST);
			ops.cutNextAppendForTesting = record -> record.indexOf(" DONE ") + 3;
			ops.persist("checksum 8 DONE 3 " + DIGEST);
			ops.persist("checksum 11 DONE 5 " + DIGEST);
			ops.cutNextAppendForTesting = record -> record.indexOf(DIGEST) + 10;
			ops.persist("checksum 12 DONE 3 " + DIGEST);
			ops.persist("checksum 13 DONE 6 " + DIGEST);
		}
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 11L, 13L), checksumIndexes(ops));
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 11, 5, DIGEST), ops.checksumResult(11));
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 13, 6, DIGEST), ops.checksumResult(13));
		}
	}

	/**
	 * R54-2: a last line torn inside its last field (a checksum's digest, a backup's directory) by a power loss or a
	 * full disk does not load: it was taken as a {@code DONE} result with the cut digest, which a peer's comparison
	 * reports as REPLICAS DIVERGED, or a {@code DONE} backup at a directory that does not exist. It does not load at a
	 * later start either, once more results were written after it.
	 */
	@Test
	void aTornLastLineCutInsideItsLastFieldDoesNotLoad() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Files.writeString(temp.resolve("replica-ops.log"), "2026-11-01T10:00:00Z checksum 7 DONE 3 " + DIGEST + "\n"
				+ "2026-11-01T10:00:30Z backup 9 DONE b1 /data/backups/b1\n"
				+ "2026-11-01T10:01:00Z checksum 8 DONE 3 " + DIGEST.substring(0, 20), StandardCharsets.UTF_8);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L), checksumIndexes(ops));
			assertEquals("/data/backups/b1", ops.backupResult("b1", 9).detail());
			ops.persist("checksum 11 DONE 5 " + DIGEST);
		}
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 11L), checksumIndexes(ops));
		}

		Files.writeString(temp.resolve("replica-ops.log"), "2026-11-01T10:00:00Z backup 9 DONE b1 /data/backups/b1\n"
				+ "2026-11-01T10:01:00Z backup 10 DONE b2 /data/back", StandardCharsets.UTF_8);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("b1", 9).state());
			assertEquals(List.of(9L), ops.backupIndexesForTesting());
		}
	}

	/**
	 * R54 review 2: a last line that lost only its {@code \n} (a power loss or a full disk cut the append right before
	 * it) but whose check verifies is whole, so it loads, and the next append starts on a line of its own; a last line
	 * whose check fails or is cut is still not loaded.
	 */
	@Test
	void aCheckedLastLineThatLostOnlyItsNewlineLoads() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		var file = temp.resolve("replica-ops.log");
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.persist("checksum 7 DONE 3 " + DIGEST);
			ops.persist("checksum 8 DONE 3 " + DIGEST);
		}
		var bytes = Files.readAllBytes(file);
		Files.write(file, java.util.Arrays.copyOf(bytes, bytes.length - 1));
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 8L), checksumIndexes(ops));
			ops.persist("checksum 9 DONE 4 " + DIGEST);
		}
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 8L, 9L), checksumIndexes(ops));
		}
		bytes = Files.readAllBytes(file);
		Files.write(file, java.util.Arrays.copyOf(bytes, bytes.length - 2));
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(7L, 8L), checksumIndexes(ops));
		}
	}

	/**
	 * R54-2: a line this version wrote ends with a check of its content, so a line cut anywhere (a cut that a later
	 * append made a middle line, a page of a power loss) or changed is rejected wherever it is in the file, while the
	 * lines around it load. Lines of an older version, which have no check, still load before the first checked line.
	 */
	@Test
	void aCutOrChangedLineIsRejectedWhereverItIs() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		var file = temp.resolve("replica-ops.log");
		Files.writeString(file, "2026-11-01T10:00:00Z checksum 6 DONE 2 " + DIGEST + "\n", StandardCharsets.UTF_8);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.persist("checksum 7 DONE 3 " + DIGEST);
		}
		Files.writeString(file, "2026-11-01T10:01:00Z checksum 10 DONE 4 " + DIGEST.substring(0, 9) + "\n"
				+ "2026-11-01T10:01:30Z checksum 14 DONE 4 " + DIGEST + " #00000000\n", StandardCharsets.UTF_8,
				StandardOpenOption.APPEND);
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.persist("checksum 12 DONE 5 " + DIGEST);
		}
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			assertEquals(List.of(6L, 7L, 12L), checksumIndexes(ops));
			assertEquals(new ReplicaOps.Result(ReplicaOps.State.DONE, 7, 3, DIGEST), ops.checksumResult(7));
		}
	}

	/**
	 * R6-5: a periodic checksum whose proposal failed is tried again after {@link ReplicaOps#PROPOSAL_RETRY}, not a full
	 * interval later; and while no check is known, the interval counts from the first start of the node, kept in a file,
	 * not from each start.
	 */
	@Test
	void periodicChecksumRetriesAndKeepsItsScheduleAcrossRestarts() throws Exception {
		var every = Duration.ofHours(1);
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		long start;
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.clusterVersion(Capabilities.SUPPORTED);
			ops.checksumEveryForTesting(every);
			long now = System.currentTimeMillis();
			assertFalse(ops.periodicChecksumDue(now));
			start = ops.scheduleStartMillisForTesting();
			assertTrue(start > 0 && start <= now);
			long due = start + every.toMillis();
			assertFalse(ops.periodicChecksumDue(due - 1));
			assertTrue(ops.periodicChecksumDue(due));
			assertFalse(ops.periodicChecksumDue(due + 1), "proposed already");
			var notApplied = new CompletionException(RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED, "x"));
			assertTrue(ops.periodicChecksumFailed(due, notApplied));
			long retry = ReplicaOps.PROPOSAL_RETRY.toMillis();
			assertFalse(ops.periodicChecksumDue(due + retry - 1));
			assertTrue(ops.periodicChecksumDue(due + retry), "tried again after a failure");
			// A failure of an older proposal does not move the newer one.
			assertTrue(ops.periodicChecksumFailed(due, notApplied));
			assertFalse(ops.periodicChecksumDue(due + 2 * retry));
			// An unknown outcome (the entry may still commit and apply late): no early retry, one interval as before.
			long second = due + retry;
			var unknown = new CompletionException(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "x"));
			assertFalse(ops.periodicChecksumFailed(second, unknown));
			assertFalse(ops.periodicChecksumDue(second + retry), "no retry after an unknown outcome");
			assertFalse(ops.periodicChecksumFailed(second, new IllegalStateException("closing")));
			assertFalse(ops.periodicChecksumDue(second + every.toMillis() - 1));
			assertTrue(ops.periodicChecksumDue(second + every.toMillis()), "due one interval after the unknown one");
		}
		Thread.sleep(20);
		// A restart before the first check: the schedule goes on from the first start.
		try (var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.clusterVersion(Capabilities.SUPPORTED);
			ops.checksumEveryForTesting(every);
			assertTrue(ops.periodicChecksumDue(start + every.toMillis()), "due one interval after the first start");
			assertEquals(start, ops.scheduleStartMillisForTesting());
		}
	}

	/**
	 * R26-2: a power loss right after a backup's rename can leave {@code backup.properties} empty (it was not synced
	 * before the rename). When the entry is applied again, that file says no index and no request: it is the trace of an
	 * interrupted attempt, which is done again, like a backup without properties, not a backup "at index -1" that makes
	 * this one FAILED.
	 */
	@Test
	void aBackupWhosePropertiesAreEmptyIsTakenAgain() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Path dir = ReplicaOps.backupsDir(config.dataDir()).resolve("b1");
		Files.createDirectories(dir.resolve(ReplicaOps.BACKUP_DB_DIR));
		Files.writeString(dir.resolve(ReplicaOps.BACKUP_PROPERTIES), "");
		try (var store = NodeStore.open(temp.resolve("store"), null, "n0");
				var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.backup("b1", 5, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 10), store);
			var result = ops.backupResult("b1", 10);
			assertEquals(ReplicaOps.State.DONE, result.state(), result.detail());
		}
		var properties = ReplicaOps.readBackupProperties(dir);
		assertEquals("10", properties.getProperty("index"));
		assertEquals("5", properties.getProperty("request"));
		assertTrue(Files.isRegularFile(dir.resolve(ReplicaOps.BACKUP_DB_DIR).resolve("CURRENT")));
	}

	/**
	 * R29-1: a backup that fails on a node removes what it wrote. A checkpoint that fails after its live files are in
	 * {@code db.tmp} (ENOSPC on the MANIFEST or WAL copy) left that directory, hard-linking every live SST and blob file
	 * of the moment, until a backup of the same name ran again, which a date-named one never does.
	 */
	@Test
	void aBackupWhoseCheckpointFailsLeavesNothingBehind() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Path dir = ReplicaOps.backupsDir(config.dataDir()).resolve("nightly");
		try (var store = NodeStore.open(temp.resolve("store"), null, "n0");
				var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			NodeStore.checkpointBuiltForTesting = building -> {
				throw new java.io.IOException("No space left on device (test)");
			};
			try {
				ops.backup("nightly", 5, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 10), store);
			} finally {
				NodeStore.checkpointBuiltForTesting = null;
			}
			var result = ops.backupResult("nightly", 10);
			assertEquals(ReplicaOps.State.FAILED, result.state(), result.detail());
		}
		assertFalse(Files.exists(dir.resolve(ReplicaOps.BACKUP_DB_DIR + ".tmp")), "the checkpoint's building directory");
		assertFalse(Files.exists(dir), "the failed backup's directory");
	}

	/**
	 * R29-1: a backup whose checkpoint is complete but whose {@code backup.properties} cannot be written leaves no
	 * database without properties behind (it also hard-links the live files). An existing backup of the same name at
	 * another index is refused and kept.
	 */
	@Test
	void aBackupWhosePropertiesCannotBeWrittenLeavesNoDatabase() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Path dir = ReplicaOps.backupsDir(config.dataDir()).resolve("nightly");
		try (var store = NodeStore.open(temp.resolve("store"), null, "n0");
				var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			// The properties' temporary file cannot be made: a directory with something in it is in its place.
			NodeStore.checkpointBuiltForTesting = building -> Files.createDirectories(
					building.resolveSibling(ReplicaOps.BACKUP_PROPERTIES + ".tmp").resolve("blocker"));
			try {
				ops.backup("nightly", 5, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 10), store);
			} finally {
				NodeStore.checkpointBuiltForTesting = null;
			}
			var result = ops.backupResult("nightly", 10);
			assertEquals(ReplicaOps.State.FAILED, result.state(), result.detail());
			assertFalse(Files.exists(dir), "a database without backup.properties");

			// A backup that exists already is not touched by the refusal of another one with its name.
			ops.backup("nightly", 6, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 20), store);
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("nightly", 20).state());
			ops.backup("nightly", 7, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 30), store);
			assertEquals(ReplicaOps.State.FAILED, ops.backupResult("nightly", 30).state());
		}
		assertEquals("20", ReplicaOps.readBackupProperties(dir).getProperty("index"));
		assertTrue(Files.isRegularFile(dir.resolve(ReplicaOps.BACKUP_DB_DIR).resolve("CURRENT")));
	}

	/**
	 * R41-2: a backup fsyncs the directories whose new entries it made, {@code backups/} (entry {@code <name>}) and the
	 * data directory (entry {@code backups}, at the first backup), before it records DONE: otherwise a power loss can
	 * take back a backup that admin reported written.
	 */
	@Test
	void aBackupSyncsTheDirectoriesItCreatesEntriesIn() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Path dataDir = config.dataDir().toAbsolutePath().normalize();
		Path backups = ReplicaOps.backupsDir(dataDir);
		var synced = java.util.Collections.synchronizedList(new java.util.ArrayList<Path>());
		try (var store = NodeStore.open(temp.resolve("store"), null, "n0");
				var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			NodeStore.syncedForTesting = synced::add;
			try {
				ops.backup("nightly", 5, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 10), store);
			} finally {
				NodeStore.syncedForTesting = null;
			}
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("nightly", 10).state());
		}
		assertTrue(synced.contains(backups), "backups/ not fsynced: " + synced);
		assertTrue(synced.contains(dataDir), "the data directory not fsynced: " + synced);
		// After the entry was made in it: its last fsync comes after the backup's own directory's first one.
		assertTrue(synced.lastIndexOf(backups) > synced.indexOf(backups.resolve("nightly")), synced.toString());
	}

	/**
	 * R41-3: the same backup request proposed twice (admin's first answer was cut, so it proposed the same request id
	 * through another member). Node a applied the first entry (index 100) and has the backup taken there; node b installed
	 * a snapshot past 100 and never applied it. At the second entry (200) every copy of the request must still be the
	 * one at 100: a reports DONE with the copy at 100 (and says so), b writes no copy at 200 and reports that it holds
	 * none, instead of two copies of one name at different log positions, all reported DONE at 200.
	 */
	@Test
	void aBackupProposedAgainKeepsTheFirstPositionOnEveryNode() throws Exception {
		var peers = List.of(new PeerConfig("a", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("b", "127.0.0.1", 3, "127.0.0.1", 4));
		var configA = NodeConfig.defaults("a", peers, temp.resolve("a"));
		var configB = NodeConfig.defaults("b", peers, temp.resolve("b"));
		long requestId = 77;
		var first = new Request(5, 1, 1, new LogEntry.Backup("nightly", requestId));
		var again = new Request(6, 1, 1, new LogEntry.Backup("nightly", requestId));
		var i1 = org.apache.ratis.server.protocol.TermIndex.valueOf(1, 100);
		var i2 = org.apache.ratis.server.protocol.TermIndex.valueOf(1, 200);
		Path dirA = ReplicaOps.backupsDir(configA.dataDir()).resolve("nightly");
		Path dirB = ReplicaOps.backupsDir(configB.dataDir()).resolve("nightly");
		Path storeB = temp.resolve("b-store");
		try (var store = NodeStore.open(temp.resolve("a-store"), null, "a");
				var ops = new ReplicaOps(configA, null, new NodeMetrics("a"))) {
			var applier = new Applier(store);
			applier.maintenance(ops);
			applier.apply(first, i1);
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("nightly", 100).state());
			// b's database: a snapshot of the leader past the first entry. backups/ is local, so b has no copy.
			store.checkpoint(storeB);
			applier.apply(again, i2);
			var result = ops.backupResult("nightly", 200);
			assertEquals(ReplicaOps.State.DONE, result.state(), result.detail());
			assertTrue(result.detail().contains("100"), "a does not say its copy is at 100: " + result.detail());
		}
		assertEquals("100", ReplicaOps.readBackupProperties(dirA).getProperty("index"));
		try (var store = NodeStore.open(storeB, null, "b");
				var ops = new ReplicaOps(configB, null, new NodeMetrics("b"))) {
			var applier = new Applier(store);
			applier.maintenance(ops);
			applier.apply(again, i2);
			var result = ops.backupResult("nightly", 200);
			assertEquals(ReplicaOps.State.FAILED, result.state(), "b took a copy at 200: " + result.detail());
			assertTrue(result.detail().contains("100"), result.detail());
		}
		assertFalse(Files.exists(dirB), "a copy of the request at another log position");
	}

	/**
	 * Review of R41, 3 (mixed versions): a node holds the copy of a request at index 100 but takes 200 for the request's
	 * first entry (its database came from a snapshot of an older leader, which records no first index, or it took the
	 * copy with an older version). Its copy is not at the request's first entry as this node knows it, and another node
	 * may hold a copy at 200: FAILED naming both indexes, not DONE, so that admin does not print OK over copies of one
	 * name at different positions. With the first index it knows (100), the same copy is DONE.
	 */
	@Test
	void aCopyOfTheRequestAtAnotherIndexThanItsFirstIsFailed() throws Exception {
		var config = NodeConfig.defaults("n0", List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2)), temp);
		Path dir = ReplicaOps.backupsDir(config.dataDir()).resolve("nightly");
		try (var store = NodeStore.open(temp.resolve("store"), null, "n0");
				var ops = new ReplicaOps(config, null, new NodeMetrics("n0"))) {
			ops.backup("nightly", 77, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 100), store);
			assertEquals(ReplicaOps.State.DONE, ops.backupResult("nightly", 100).state());

			ops.backup("nightly", 77, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 200), 200, store);
			var result = ops.backupResult("nightly", 200);
			assertEquals(ReplicaOps.State.FAILED, result.state(), result.detail());
			assertTrue(result.detail().contains("100") && result.detail().contains("200"), result.detail());

			ops.backup("nightly", 77, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 300), 100, store);
			result = ops.backupResult("nightly", 300);
			assertEquals(ReplicaOps.State.DONE, result.state(), result.detail());
		}
		assertEquals("100", ReplicaOps.readBackupProperties(dir).getProperty("index"), "the copy is kept");
	}
}
