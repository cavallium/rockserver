package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * RT-23 / RT-64 (scenario 12 of the design): the disk of one node fails under a running 3-node process cluster, with
 * the fault-injecting shim of {@link FaultFs} preloaded into every node: from one moment on, every write (ENOSPC, a
 * full disk) or every fsync and fdatasync (EIO, a disk that fails on flush) under that node's data directory fails.
 * Ratis' log and RocksDB both see it.
 *
 * <p>Expected, for the leader and for a follower: the node stops as a failed replica, with exit code 3 (see "Local
 * failures" in the design); the other two keep acknowledging writes; once the space or the disk is back, the node
 * starts again (after {@code repair-log} if the failed write left a torn tail in its open log segment, which is safe
 * here: that entry's fsync failed, so this node never acknowledged it) and catches up; every acknowledged write is
 * readable and the replicas are identical. A write is acknowledged only after a majority fsynced it, so a node whose
 * fsync failed cannot have counted towards one.</p>
 *
 * <p>DD-7h: {@link #aSigtermRacingAReplicaFailureExitsWithThree} sends SIGTERM to a follower while its disk failure
 * stops it, and expects exit code 3, not the 0 of a graceful stop.</p>
 *
 * <p>Multi-process: part of the heavy suites. Skipped without {@code gcc}.</p>
 */
@Timeout(600)
class DiskFaultTest {

	private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(60);
	/**
	 * Lines a node's log shows once a disk failure made it a failed node: Ratis' log worker closing the division, the
	 * replica, the Raft close. Not the bare errno text: a graceful stop that flushes onto the failed disk prints it too,
	 * and that stop exits 0 (see DD-7h in the design document).
	 */
	private static final List<String> FAILURE_MARKERS = List.of("replica failed to", "Raft closed", "hit exception");
	private static final int SIGNAL_ROUNDS = Integer.getInteger("rockserver.test.diskFaultSignalRounds", 6);

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;

	@AfterEach
	void tearDown() {
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	/** Every node with the shim loaded, each with its own trigger next to (not inside) its data directory. */
	private Path start() throws Exception {
		var library = FaultFs.build(root.resolve("faultfs"));
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000",
				"--snapshot-every", "2000"));
		for (var id : cluster.members()) {
			cluster.setEnvironment(id, FaultFs.environment(library, cluster.dataDir(id), trigger(id)));
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		return rockserverConfig;
	}

	private Path trigger(String id) {
		return cluster.nodeDir(id).resolve("disk-fault");
	}

	@ParameterizedTest(name = "{0} on the {1}")
	@CsvSource({"ENOSPC, WRITE, leader", "ENOSPC, WRITE, follower", "EIO, SYNC, leader", "EIO, SYNC, follower",
			"EIO, ALL, follower"})
	void aFailedDiskStopsItsNodeAndLosesNoAcknowledgedWrite(FaultFs.Errno errno, FaultFs.Ops ops, String role)
			throws Exception {
		var rockserverConfig = start();
		var support = new ScenarioSupport(cluster);
		long column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 256).start();
		try {
			workload.awaitMore(200, PHASE_TIMEOUT, "before the fault");
			var leader = support.awaitLeader(PHASE_TIMEOUT);
			var victim = role.equals("leader") ? leader
					: cluster.members().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
			FaultFs.trigger(trigger(victim), errno, ops);
			long faultAt = System.nanoTime();
			int code = cluster.awaitExit(victim, EXIT_TIMEOUT);
			System.out.println("Node " + victim + " (" + role + ") exited with " + code + " after "
					+ Duration.ofNanos(System.nanoTime() - faultAt).toMillis() + " ms of " + errno + " " + ops);
			assertEquals(3, code, () -> "a node whose disk fails stops as a failed replica\n" + cluster.logTail(victim));

			// The other two serve on.
			long exitedAt = System.nanoTime();
			ScenarioSupport.await(PHASE_TIMEOUT, () -> workload.lastAcknowledgedNanos() > exitedAt,
					"no write acknowledged without node " + victim);
			workload.awaitMore(100, PHASE_TIMEOUT, "without node " + victim);

			// The space comes back: the node starts again and catches up.
			FaultFs.clear(trigger(victim));
			restartAfterFault(victim);
			support.awaitCaughtUp(victim);
			workload.awaitMore(100, PHASE_TIMEOUT, "after node " + victim + " came back");
		} finally {
			workload.stop();
		}
		support.awaitQuiescent();
		var writes = workload.writes();
		int unknownApplied = ScenarioSupport.checkWrites(client, column, writes);
		System.out.println("Writes " + writes.size() + ", unknown outcomes that applied: " + unknownApplied);
		support.checkReplicasIdentical(rockserverConfig);
	}

	/**
	 * DD-7h: a SIGTERM that races a replica failure exits with 3. A follower's disk starts failing (EIO on every write and
	 * sync) while it is frozen with SIGSTOP, so that the failure starts right after SIGCONT; the SIGTERM follows either
	 * as soon as the node's log shows the failure, or after a random 0-30 ms. The
	 * exit code must be 3 whenever the failure showed in the node's log before the signal; at least one round must have
	 * signalled a node that was failing and still alive.
	 */
	@Test
	void aSigtermRacingAReplicaFailureExitsWithThree() throws Exception {
		long seed = Long.getLong("rockserver.test.seed", System.nanoTime());
		System.out.println("DiskFaultTest seed: " + seed + " (-Drockserver.test.seed=" + seed + ")");
		var random = new Random(seed);
		var rockserverConfig = start();
		var support = new ScenarioSupport(cluster);
		long column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 256).start();
		int raced = 0;
		var rounds = new ArrayList<String>();
		try {
			workload.awaitMore(200, PHASE_TIMEOUT, "before the first round");
			for (int round = 1; round <= SIGNAL_ROUNDS; round++) {
				var leader = support.awaitLeader(PHASE_TIMEOUT);
				var victim = cluster.members().stream().filter(id -> !id.equals(leader)).toList()
						.get(random.nextInt(2));
				boolean waitForFailure = round % 2 == 1;
				int delayMillis = random.nextInt(31);
				cluster.pause(victim);
				FaultFs.trigger(trigger(victim), FaultFs.Errno.EIO, FaultFs.Ops.ALL);
				cluster.resume(victim);
				if (waitForFailure) {
					long deadline = System.nanoTime() + EXIT_TIMEOUT.toNanos();
					while (!failureShown(victim) && cluster.isRunning(victim) && System.nanoTime() < deadline) {
						Thread.sleep(1);
					}
				} else {
					Thread.sleep(delayMillis);
				}
				boolean aliveAtSignal = cluster.isRunning(victim);
				boolean failingAtSignal = failureShown(victim);
				cluster.stop(victim);
				int code = cluster.exitCode(victim);
				boolean failed = failureShown(victim);
				var line = "round " + round + ": node " + victim + (waitForFailure ? " signalled on its failure"
						: " signalled after " + delayMillis + " ms") + ", alive " + aliveAtSignal + ", failing "
						+ failingAtSignal + ", failure in its log " + failed + ", exit code " + code;
				System.out.println(line);
				rounds.add(line);
				// Only a failure that showed before the signal makes it the failure's stop. One that first shows during the
				// graceful stop (the stop's own flush hits the disk) may exit 0: see DD-7h in the design document.
				if (failingAtSignal) {
					assertEquals(3, code, () -> line + "\n" + cluster.logTail(victim));
				}
				if (aliveAtSignal && failingAtSignal && code == 3) {
					raced++;
				}
				FaultFs.clear(trigger(victim));
				restartAfterFault(victim);
				support.awaitCaughtUp(victim);
				workload.awaitMore(50, PHASE_TIMEOUT, "after " + line);
			}
		} finally {
			workload.stop();
		}
		System.out.println(String.join("\n", rounds));
		assertTrue(raced > 0, "no round signalled a failing node that was still alive: the race was not exercised");
		support.awaitQuiescent();
		ScenarioSupport.checkWrites(client, column, workload.writes());
		support.checkReplicasIdentical(rockserverConfig);
	}

	/** Whether the node's output since its last start shows that the disk failure reached it. */
	private boolean failureShown(String id) {
		var output = cluster.output(id);
		// Both logs carry a marker line at every start; the stderr one is the last.
		int start = output.lastIndexOf("---- harness: start #");
		var current = start >= 0 ? output.substring(start) : output;
		return FAILURE_MARKERS.stream().anyMatch(current::contains);
	}

	/**
	 * Starts a node whose disk failed. If it stops before {@code READY} (Ratis refuses an open log segment with a torn
	 * tail), runs {@code repair-log} once, which is safe after a failed write: the entry whose write or fsync failed was
	 * never acknowledged by this node, and then starts it again.
	 */
	private void restartAfterFault(String id) throws Exception {
		cluster.start(id);
		try {
			cluster.waitReady(id, READY_TIMEOUT);
			return;
		} catch (IllegalStateException notReady) {
			if (cluster.isRunning(id)) {
				throw notReady;
			}
			System.out.println("Node " + id + " did not start after its disk fault: " + notReady.getMessage());
		}
		var repair = cluster.repairLog(id);
		System.out.println("repair-log of node " + id + " (exit " + repair.exitCode() + "):\n" + repair.output());
		assertEquals(0, repair.exitCode(), repair.output());
		cluster.start(id);
		cluster.waitReady(id, READY_TIMEOUT);
	}
}
