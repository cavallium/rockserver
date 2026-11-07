package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R25-W: the threads that pause the API on a hung disk ({@code gateApi}) and step a stalled leader down
 * ({@code watchRaft}) keep running while what a hung disk wedges is wedged: the monitor of the snapshot storage, which
 * the state machine updater holds for a whole checkpoint or snapshot install, and the write lock of the Raft log, which
 * Ratis holds while an append waits for a log worker that stopped writing. Before round 25 both ran on one maintenance
 * thread with the node monitor, which reads the snapshot directory under the storage monitor and the log under its
 * read lock: one hung disk parked that thread for good, and the API was never paused, nor the leader stepped down.
 */
class WatchdogThreadsTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
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

	/** Three nodes that each hold a snapshot, so that the node monitor reads its directory; returns the leader. */
	private ReplicatedNode startWithSnapshots() throws Exception {
		cluster = new InProcessCluster(Files.createDirectory(temp.resolve("cluster")), 3,
				c -> c.withRequestTimeout(Duration.ofSeconds(2)).withSnapshotEveryEntries(16)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(CONTEXT);
		long column = api.createColumn("c", SCHEMA);
		for (int i = 0; i < 40; i++) {
			api.put(0, column, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().getLatestSnapshot() != null));
		return leader;
	}

	/**
	 * A follower whose snapshot storage monitor is held (as by a checkpoint that waits for a hung disk) still pauses its
	 * API once its disk probe hangs.
	 */
	@Test
	@Timeout(180)
	void aHeldSnapshotStorageMonitorDoesNotStopTheApiPause() throws Exception {
		startWithSnapshots();
		var victim = cluster.anyFollower();
		var storage = victim.stateMachine().getStateMachineStorage();
		var release = new CountDownLatch(1);
		var holder = holdMonitor(storage, release);
		var hang = new CountDownLatch(1);
		var key = victim.config().dataDir().toAbsolutePath().normalize();
		try {
			// A refresh of the node monitor runs every second: by now one waits for the storage monitor.
			Thread.sleep(1_500);
			DiskProbe.HANGS_FOR_TESTING.put(key, hang);
			InProcessCluster.await(TIMEOUT, () -> !listens(victim));
			assertApiGateNotBlocked(victim);
		} finally {
			hang.countDown();
			DiskProbe.HANGS_FOR_TESTING.remove(key);
			release.countDown();
			holder.join();
		}
		InProcessCluster.await(TIMEOUT, () -> listens(victim));
	}

	/**
	 * A follower whose Raft log write lock is held (Ratis' append waiting for a stopped log worker) still pauses its API
	 * once its disk probe hangs.
	 */
	@Test
	@Timeout(180)
	void aHeldRaftLogWriteLockDoesNotStopTheApiPause() throws Exception {
		LogWorkerGate.checkContract();
		startWithSnapshots();
		var victim = cluster.anyFollower();
		var release = new CountDownLatch(1);
		var holder = LogWorkerGate.holdWriteLock(victim.stateMachine().division().getRaftLog(), release);
		var hang = new CountDownLatch(1);
		var key = victim.config().dataDir().toAbsolutePath().normalize();
		try {
			Thread.sleep(1_500);
			DiskProbe.HANGS_FOR_TESTING.put(key, hang);
			InProcessCluster.await(TIMEOUT, () -> !listens(victim));
			assertApiGateNotBlocked(victim);
		} finally {
			hang.countDown();
			DiskProbe.HANGS_FOR_TESTING.remove(key);
			release.countDown();
			holder.join();
		}
		InProcessCluster.await(TIMEOUT, () -> listens(victim));
	}

	/** A leader whose log stopped steps down while its snapshot storage monitor is held. */
	@Test
	@Timeout(180)
	void aLeaderWhoseLogStoppedStepsDownWhileItsStorageMonitorIsHeld() throws Exception {
		var oldLeader = startWithSnapshots();
		var release = new CountDownLatch(1);
		var holder = holdMonitor(oldLeader.stateMachine().getStateMachineStorage(), release);
		var disk = new CountDownLatch(1);
		try {
			Thread.sleep(1_500);
			LogWorkerGate.block(oldLeader.stateMachine().division().getRaftLog(), disk);
			var column = oldLeader.connection().getSyncApi(CONTEXT).getColumnId("c");
			oldLeader.connection().getAsyncApi(CONTEXT).putAsync(0, column, key(1_000), Buf.wrap(new byte[] {2}),
					RequestType.none());
			InProcessCluster.await(Duration.ofSeconds(60), () -> cluster.runningNodes().stream()
					.anyMatch(node -> node != oldLeader && node.isLeader()));
			assertTrue(oldLeader.metrics().amount("leader.log.stepdowns").count() >= 1, "not stepped down by the watchdog");
		} finally {
			disk.countDown();
			release.countDown();
			holder.join();
		}
	}

	/** Holds the monitor of {@code lock} on a thread of its own until {@code release} opens; returns once held. */
	private static Thread holdMonitor(Object lock, CountDownLatch release) throws InterruptedException {
		var held = new CountDownLatch(1);
		var holder = Thread.ofPlatform().daemon().name("storage-monitor-holder").start(() -> {
			synchronized (lock) {
				held.countDown();
				try {
					release.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
		held.await();
		return holder;
	}

	/** The API gate runs on a thread of its own, which nothing here blocks. */
	private static void assertApiGateNotBlocked(ReplicatedNode node) {
		var name = "rockserver-" + node.config().nodeId() + "-api-gate";
		var gate = Thread.getAllStackTraces().keySet().stream().filter(thread -> thread.getName().equals(name))
				.findFirst().orElseThrow(() -> new AssertionError("no thread " + name));
		assertNotEquals(Thread.State.BLOCKED, gate.getState(), name);
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static boolean listens(ReplicatedNode node) {
		var self = node.config().self();
		try (var socket = new Socket()) {
			socket.connect(new InetSocketAddress(self.apiHost(), self.apiPort()), 1_000);
			return true;
		} catch (IOException refused) {
			return false;
		}
	}
}
