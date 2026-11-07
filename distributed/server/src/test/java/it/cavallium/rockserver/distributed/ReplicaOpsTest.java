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

	/** Only an answer that proves nothing was proposed lets admin propose a checksum again. */
	@Test
	void retriedOnlyWhenNothingWasProposed() {
		assertTrue(ReplicatedServerMain.notProposed(Status.Code.UNAVAILABLE));
		assertTrue(ReplicatedServerMain.notProposed(Status.Code.NOT_FOUND));
		assertFalse(ReplicatedServerMain.notProposed(Status.Code.ABORTED));
		assertFalse(ReplicatedServerMain.notProposed(Status.Code.DEADLINE_EXCEEDED));
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
}
