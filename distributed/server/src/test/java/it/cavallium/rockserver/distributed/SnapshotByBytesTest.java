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
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
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
	 * only as commits arrive; the database is already further. A snapshot taken while the replay has not reached the
	 * database's position (the byte trigger from a replayed group, or {@code admin snapshot}, with the commit index
	 * below that position) must not return the database's index: Ratis compares it with its own applied index, logs
	 * "Bug in StateMachine", and purges nothing, while the byte count was reset and a checkpoint made. It answers "no
	 * snapshot taken" instead, keeping the count, until Ratis has handed over every entry up to the database's
	 * position; then it takes one as usual. Ratis' creation gap does not cover the window: Ratis 3.3.1 compares the
	 * state machine's last applied term-index, which {@code openStore} sets to the database's position, with the latest
	 * snapshot, so right after a start the gap is the database's position minus the snapshot's, and a request goes
	 * through before anything is replayed. The test sends one through Ratis ({@code snapshotManagementAsync}, as
	 * {@code admin snapshot} does) to one node of three restarted alone, so that nothing is committed, then calls
	 * {@code takeSnapshot} directly as Ratis' updater does.
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

		var division = Objects.requireNonNull(stateMachine.division());
		var reply = division.getRaftServer().snapshotManagementAsync(SnapshotManagementRequest.newCreate(
				ClientId.randomId(), division.getId(), division.getGroup().getGroupId(), 1, 20_000))
				.get(30, TimeUnit.SECONDS);
		assertTrue(reply.getLogIndex() < databaseApplied, "the request through Ratis took a snapshot at "
				+ reply.getLogIndex() + ", past what Ratis handed over (" + stateMachine.handedOverForTesting() + ")");
		assertNull(stateMachine.getLatestSnapshot(), "no checkpoint was made through Ratis");
		assertEquals(counted, stateMachine.bytesSinceSnapshotForTesting(), "the count is kept through Ratis");

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
