package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.FunctionTimer;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.List;
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
				new LogEntry.Backup("nightly-2026.11.06_a"))) {
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
		assertThrows(IllegalArgumentException.class, () -> new LogEntry.Backup("../etc"));
	}

	@Test
	void resultsRoundTrip() {
		for (var result : List.of(new ReplicaOps.Result(ReplicaOps.State.DONE, 42, 7, "abcdef"),
				new ReplicaOps.Result(ReplicaOps.State.PENDING, -1, 0, ""),
				new ReplicaOps.Result(ReplicaOps.State.FAILED, 3, 0, "java.io.IOException: disk full é"))) {
			assertEquals(result, ReplicaOps.decodeResult(ReplicaOps.encodeResult(result)));
		}
	}

	@Test
	void capabilities() {
		assertEquals("none", Capabilities.describe(0));
		assertEquals("pipelined-updates,client-sessions,checksum,backup", Capabilities.describe(Capabilities.SUPPORTED));
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
		return Buf.wrap(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
		} finally {
			metrics.close();
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
}
