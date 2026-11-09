package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.ratis.server.raftlog.RaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Snapshots are taken by applied log bytes too, not only every {@code snapshotEveryEntries} entries. */
class SnapshotByBytesTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aFewLargeEntriesTriggerASnapshot() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(1_000_000)
				.withSnapshotEveryBytes(1 << 20)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (var node : cluster.runningNodes()) {
			assertNull(node.stateMachine().getLatestSnapshot());
		}
		var value = Buf.wrap(new byte[256 * 1024]);
		for (long i = 0; i < 8; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())), value,
					RequestType.none());
		}
		// 2 MiB in 8 entries, far from a million: every replica takes a snapshot anyway.
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().getLatestSnapshot() != null));
	}

	/**
	 * The count starts from the log applied past the snapshot, not from 0 at every start: restarts more frequent than
	 * {@code snapshotEveryBytes} of writes would otherwise never let it reach the threshold.
	 */
	@Test
	void theCountSurvivesARestart() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(1_000_000)
				.withSnapshotEveryBytes(1 << 20)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		var value = Buf.wrap(new byte[256 * 1024]);
		// 768 KiB: below the threshold.
		for (long i = 0; i < 3; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())), value,
					RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		for (var id : cluster.ids()) {
			cluster.stop(id);
		}
		for (var id : cluster.ids()) {
			cluster.start(id);
		}
		for (var node : cluster.runningNodes()) {
			assertNull(node.stateMachine().getLatestSnapshot());
			long seeded = node.stateMachine().bytesSinceSnapshotForTesting();
			assertTrue(seeded >= 3 * 256 * 1024, node.config().nodeId() + " counts " + seeded + " bytes");
		}
		// Once Ratis replayed the log up to the database's position, those entries are not counted twice: the count
		// stays below the threshold, and no snapshot is taken.
		cluster.awaitLeader(TIMEOUT);
		cluster.awaitConverged(TIMEOUT);
		Thread.sleep(500);
		for (var node : cluster.runningNodes()) {
			long counted = node.stateMachine().bytesSinceSnapshotForTesting();
			assertTrue(counted < (1 << 20), node.config().nodeId() + " counts " + counted + " bytes after the replay");
			assertNull(node.stateMachine().getLatestSnapshot(), node.config().nodeId() + " took a snapshot");
		}
		leader = cluster.awaitLeader(TIMEOUT);
		api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		// 512 KiB more: 1.25 MiB since the latest snapshot (none), only 512 KiB since the restart.
		for (long i = 3; i < 5; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())), value,
					RequestType.none());
		}
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().getLatestSnapshot() != null));
	}

	/**
	 * R48-1: after a restart, Ratis' StateMachineUpdater starts at the log's snapshot index and hands the log over again
	 * only once a commit arrives; the database is already further. A snapshot asked meanwhile (here before any commit,
	 * with one node of three up; in a cluster, a snapshot request while the replay has not reached the database's
	 * position) must not return the database's index: Ratis compares it with its own applied index, logs
	 * "Bug in StateMachine", and purges nothing, while the byte count was reset and a checkpoint made. It answers "no
	 * snapshot taken" instead, keeping the count, until Ratis has handed over every entry up to the database's
	 * position; then it takes one as usual.
	 */
	@Test
	void noSnapshotPastWhatRatisHandedOverAfterARestart() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(1_000_000)
				.withSnapshotEveryBytes(1 << 20)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		var value = Buf.wrap(new byte[256 * 1024]);
		for (long i = 0; i < 3; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())), value,
					RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		for (var id : cluster.ids()) {
			cluster.stop(id);
		}
		var first = cluster.ids().getFirst();
		var stateMachine = cluster.start(first).stateMachine();
		long databaseApplied = stateMachine.appliedIndex().get();
		assertTrue(databaseApplied > stateMachine.handedOverForTesting(),
				"applied " + databaseApplied + ", handed over " + stateMachine.handedOverForTesting());
		long counted = stateMachine.bytesSinceSnapshotForTesting();

		assertEquals(RaftLog.INVALID_LOG_INDEX, stateMachine.takeSnapshot(), "no snapshot past what Ratis applied");
		assertNull(stateMachine.getLatestSnapshot(), "no checkpoint was made");
		assertEquals(counted, stateMachine.bytesSinceSnapshotForTesting(), "the count is kept");

		for (var id : cluster.ids()) {
			if (!id.equals(first)) {
				cluster.start(id);
			}
		}
		cluster.awaitLeader(TIMEOUT);
		cluster.awaitConverged(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= databaseApplied);
		long taken = stateMachine.takeSnapshot();
		assertTrue(taken >= databaseApplied && taken <= stateMachine.handedOverForTesting(),
				"snapshot at " + taken + ", database at " + databaseApplied);
		assertEquals(taken, stateMachine.getLatestSnapshot().getIndex());
	}

	@Test
	void logBytesBetweenCountsTheSegmentsInTheRange() throws Exception {
		var dir = Files.createDirectory(temp.resolve("log"));
		Files.write(dir.resolve("log_0-9"), new byte[1000]);
		Files.write(dir.resolve("log_10-19"), new byte[2000]);
		Files.write(dir.resolve("log_inprogress_20"), new byte[500]);
		Files.write(dir.resolve("raft-meta"), new byte[7000]);
		// Whole segments past the snapshot, the open one up to the applied index.
		assertEquals(2500, RockserverStateMachine.logBytesBetween(dir, 9, 25));
		// Half of the first, all of the second.
		assertEquals(500 + 2000, RockserverStateMachine.logBytesBetween(dir, 4, 19));
		// No snapshot: from the start.
		assertEquals(3000, RockserverStateMachine.logBytesBetween(dir, -1, 19));
		assertEquals(0, RockserverStateMachine.logBytesBetween(dir, 19, 19));
		assertEquals(0, RockserverStateMachine.logBytesBetween(dir.resolve("missing"), -1, 19));
	}
}
