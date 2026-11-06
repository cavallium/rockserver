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
