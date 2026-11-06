package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Cross-replica checksums on a real 3-node cluster: a divergence is reported, schemas count, the schedule survives a leader change. */
class ChecksumTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private ReplicatedNode start() throws Exception {
		cluster = new InProcessCluster(temp, 3, config -> config).startAll();
		return cluster.awaitLeader(TIMEOUT);
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
	}

	private long checksum(ReplicatedNode through) throws Exception {
		long index = (Long) through.proposer().propose(new LogEntry.Checksum()).get(20, TimeUnit.SECONDS);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				node.stateMachine().ops().checksumResult(index).state() == ReplicaOps.State.DONE));
		return index;
	}

	/**
	 * R3-G3-2: a row written on one replica only, past the log, makes its digest differ; every replica reports the
	 * divergence (the error "REPLICAS DIVERGED", the metric and the gauge of the index).
	 */
	@Test
	void aRowOnOneReplicaOnlyIsReportedAsADivergence() throws Exception {
		var leader = start();
		long column = api(leader).createColumn("c", SCHEMA);
		for (long i = 0; i < 50; i++) {
			api(leader).put(0, column, key(i), value("v" + i), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		var follower = cluster.anyFollower();
		follower.stateMachine().store().db().put(0, column, key(1_000), value("only here"), RequestType.none());

		long index = checksum(leader);
		var own = follower.stateMachine().ops().checksumResult(index);
		var other = leader.stateMachine().ops().checksumResult(index);
		assertEquals(51, own.keys());
		assertEquals(50, other.keys());
		assertNotEquals(own.detail(), other.detail());
		// Each replica asks the others for their digest of the position, again every 10 s while one is pending.
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().ops().lastMismatchIndex() == index));
		for (var node : cluster.runningNodes()) {
			assertTrue(node.metrics().amount("checksum.mismatches").count() >= 1, node.config().nodeId());
		}
	}

	/** CR2-9: replicas with the same data but a column of another schema (here its merge operator) digest differently. */
	@Test
	void aColumnWithAnotherSchemaOnOneReplicaIsADivergence() throws Exception {
		var leader = start();
		api(leader).createColumn("s", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var follower = cluster.anyFollower();
		var db = follower.stateMachine().store().db();
		// Past the log: the column deleted and created again, empty and with the same name, under another schema.
		db.deleteColumn(db.getColumnId("s"));
		db.createColumn("s", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true, "other-operator", 1L));
		assertTrue(Capabilities.has(leader.stateMachine().ops().clusterVersion(), Capabilities.CHECKSUM_SCHEMAS));

		long index = checksum(leader);
		var own = follower.stateMachine().ops().checksumResult(index);
		var other = leader.stateMachine().ops().checksumResult(index);
		assertEquals(0, own.keys());
		assertEquals(0, other.keys());
		assertNotEquals(own.detail(), other.detail(), "the schema is part of the digest");
		assertNotEquals(ReplicaOps.describeSchema(SCHEMA), ReplicaOps.describeSchema(
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true, "other-operator", 1L)));
	}

	/**
	 * CR2-2: the interval counts from the last check that the replicas applied, not from when the current leader took
	 * over: with leadership moving more often than the interval, checks still run. Here the leader changes halfway
	 * through an interval of 6 s; the next check comes 6 s after the last one, not 6 s after the change.
	 */
	@Test
	void thePeriodicScheduleSurvivesALeaderChange() throws Exception {
		var every = Duration.ofSeconds(6);
		start();
		for (var node : cluster.runningNodes()) {
			node.stateMachine().ops().checksumEveryForTesting(every);
		}
		// The first one, one interval after the start (no check known yet).
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> !node.stateMachine().ops().checksumResults().isEmpty()));
		long firstApplied = System.nanoTime();
		long first = cluster.runningNodes().getFirst().stateMachine().ops().checksumResults().getLast().index();

		Thread.sleep(every.toMillis() / 2);
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		cluster.stop(oldLeader.config().nodeId());
		var newLeader = cluster.awaitLeader(TIMEOUT);
		assertNotEquals(oldLeader.config().nodeId(), newLeader.config().nodeId());

		InProcessCluster.await(TIMEOUT, () -> newLeader.stateMachine().ops().checksumResults().getLast().index() > first);
		long after = System.nanoTime() - firstApplied;
		System.out.println("ChecksumTest: next periodic check " + Duration.ofNanos(after).toMillis() + " ms after the first,"
				+ " leader changed at " + every.toMillis() / 2 + " ms");
		// Before, the new leader counted from its takeover: at least 9 s.
		assertTrue(after < every.toNanos() + TimeUnit.MILLISECONDS.toNanos(1_500), "late: " + Duration.ofNanos(after));
		assertTrue(after > every.toNanos() - TimeUnit.MILLISECONDS.toNanos(1_000), "early: " + Duration.ofNanos(after));
	}

	/**
	 * CR2-2: applying a checksum entry again (a restart that replays it after a lost WAL tail, or entries after the
	 * snapshot) keeps the time this replica first applied it, from its results file: the schedule is not restarted at
	 * the replay. Here the recorded time is moved three hours back while the node is down, and the replay is the apply
	 * of the same position.
	 */
	@Test
	void replayingAChecksumEntryKeepsItsFirstApplyTime() throws Exception {
		var leader = start();
		long index = checksum(leader);
		var follower = cluster.anyFollower();
		var id = follower.config().nodeId();
		cluster.stop(id);
		var results = cluster.config(id).dataDir().resolve("replica-ops.log");
		long old = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3);
		var lines = Files.readAllLines(results).stream()
				.map(line -> line.contains(" checksum " + index + " ")
						? Instant.ofEpochMilli(old) + line.substring(line.indexOf(' ')) : line)
				.toList();
		Files.write(results, lines);

		var restarted = cluster.start(id);
		var ops = restarted.stateMachine().ops();
		assertEquals(old, ops.lastChecksumMillisForTesting(), "loaded from the results file");
		cluster.awaitConverged(TIMEOUT);
		var term = restarted.stateMachine().division().getInfo().getCurrentTerm();
		ops.checksum(TermIndex.valueOf(term, index), restarted.stateMachine().store());
		InProcessCluster.await(TIMEOUT, () -> ops.checksumResult(index).state() == ReplicaOps.State.DONE);
		assertEquals(old, ops.lastChecksumMillisForTesting(), "a replay does not move the schedule");

		// Its new lines (written at the replay) do not move it at the next start either.
		cluster.stop(id);
		assertEquals(old, cluster.start(id).stateMachine().ops().lastChecksumMillisForTesting());
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(String text) {
		return Buf.wrap(text.getBytes(StandardCharsets.UTF_8));
	}
}
