package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.CrashPoints.Point;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scenario 7 of the design: an existing member that lags behind past the log that the leader still holds catches up
 * through a snapshot install, also when the follower or the leader is killed in the middle of it.
 *
 * <p>The leader of this cluster keeps its log for a follower that lags ({@code RaftSettings}: the log is purged only
 * up to what every member committed), so a lagging member needs an install only from a leader that did not track
 * it: a former follower, which purged its own log up to its snapshots. The tests build that case: follower {@code F}
 * is cut off (a partitioned link) while the others take snapshots and the other follower {@code G} purges its log;
 * leadership then moves to {@code G}, and the link heals. {@code F} must install a snapshot from
 * {@code G}. A slow link (once {@code LinkProxy} can throttle, see RT-4) would give the same lag without a cut.</p>
 */
@Timeout(600)
class LaggingInstallTest {

	private static final String SNAPSHOT_INSTALLED = "installing snapshot";
	private static final long SNAPSHOT_EVERY = 100;
	/** Ratis purges in steps of 1024 entries ({@code RaftSettings}): G's log must have moved well past that. */
	private static final long LAG_ENTRIES = 1024 + 4 * SNAPSHOT_EVERY;
	private static final Duration CRASH_TIMEOUT = Duration.ofSeconds(60);

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;
	private ScenarioSupport support;
	private Path rockserverConfig;
	private long column;
	private ScenarioSupport.UniqueWrites workload;

	@AfterEach
	void tearDown() throws Exception {
		if (workload != null) {
			try {
				workload.stop();
			} catch (AssertionError | Exception ignored) {
				// Reported by the test itself.
			}
		}
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	/** The three roles of a test: the leader, the follower that lags, the follower that purges and then leads. */
	private record Roles(String leader, String lagging, String purging) {}

	private void finish(Roles roles) throws Exception {
		for (var id : cluster.nodeIds()) {
			if (!cluster.isRunning(id)) {
				cluster.start(id);
				cluster.waitReady(id, READY_TIMEOUT);
			}
		}
		for (var id : cluster.nodeIds()) {
			support.awaitCaughtUp(id);
		}
		workload.awaitMore(100, PHASE_TIMEOUT, "at the end");
		workload.stop();
		var writes = workload.writes();
		workload = null;
		support.awaitQuiescent();
		ScenarioSupport.checkWrites(client, column, writes);
		assertTrue(cluster.output(roles.lagging()).contains(SNAPSHOT_INSTALLED),
				"the lagging member caught up without a snapshot install: the scenario did not happen");
		support.checkReplicasIdentical(rockserverConfig);
	}

	@Test
	void aLaggingMemberCatchesUpThroughAnInstall() throws Exception {
		var roles = startLagging();
		cluster.network().heal();
		finish(roles);
	}

	@Test
	void aLaggingMemberKilledAfterReceivingTheSnapshotInstallsItAfterItsRestart() throws Exception {
		killedDuringInstall(Point.INSTALL_RECEIVED);
	}

	@Test
	void aLaggingMemberKilledWhileRestoringTheSnapshotRestartsAndCatchesUp() throws Exception {
		killedDuringInstall(Point.INSTALL_RESTORING);
	}

	private void killedDuringInstall(Point point) throws Exception {
		var roles = startLagging();
		CrashPointMain.arm(cluster.dataDir(roles.lagging()), point, 0);
		ScenarioSupport.await(Duration.ofSeconds(10), () -> CrashPointMain.armed(cluster.dataDir(roles.lagging())),
				"the lagging node did not arm");
		cluster.network().heal();
		support.awaitCrash(roles.lagging(), point, CRASH_TIMEOUT);
		System.out.println("Lagging node " + roles.lagging() + " halted at " + point);
		finish(roles);
	}

	/**
	 * The leader that sends the snapshot is killed as soon as the lagging node starts receiving it (its first
	 * received file appears). The remaining two elect a leader, which resends a snapshot or the log; the killed node
	 * restarts. Whether the kill fell inside the transfer depends on timing, and is printed.
	 */
	@Test
	void theLeaderKilledDuringAnInstallLeavesTheLaggingMemberToCatchUpFromTheNextLeader() throws Exception {
		var roles = startLagging();
		var receiving = installDir(cluster.dataDir(roles.lagging()));
		cluster.network().heal();
		long start = System.nanoTime();
		boolean caught = false;
		while (System.nanoTime() - start < CRASH_TIMEOUT.toNanos()) {
			if (receivingFiles(receiving)) {
				caught = !cluster.output(roles.lagging()).contains(SNAPSHOT_INSTALLED);
				break;
			}
			Thread.onSpinWait();
		}
		cluster.kill(roles.purging());
		System.out.println("Killed the sending leader " + roles.purging() + (caught ? " during" : " after")
				+ " the transfer to " + roles.lagging());
		// The old leader and the lagging node elect a leader without the killed one.
		support.awaitLeader(PHASE_TIMEOUT);
		finish(roles);
	}

	/**
	 * Cuts the lagging follower off, writes until the purging follower took a snapshot well past it (purging its own
	 * log), then hands leadership to the purging follower. The lagging one is still cut off.
	 */
	private Roles startLagging() throws Exception {
		rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000",
				"--snapshot-every", String.valueOf(SNAPSHOT_EVERY)), CrashPointMain.class.getName());
		for (var id : cluster.members()) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		support = new ScenarioSupport(cluster);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		// Values of 1 KiB: the snapshot is several MiB, so its transfer takes a moment.
		workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 1024).start();
		workload.awaitMore(100, PHASE_TIMEOUT, "before the lag");
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		var followers = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();
		var roles = new Roles(leader, followers.get(0), followers.get(1));
		long lagFrom = support.applied(roles.lagging());
		cluster.network().isolate(roles.lagging());
		ScenarioSupport.await(Duration.ofMinutes(3),
				() -> support.snapshotIndex(roles.purging()) > lagFrom + LAG_ENTRIES,
				"the purging follower took no snapshot far enough past the lagging one");
		var transfer = cluster.adminResult(List.of(roles.leader(), roles.purging()), "transfer-leader",
				roles.purging());
		assertTrue(transfer.exitCode() == 0, transfer.output());
		ScenarioSupport.await(PHASE_TIMEOUT, () -> roles.purging().equals(support.leaderOrNull()),
				"the purging follower did not take over");
		workload.awaitMore(50, PHASE_TIMEOUT, "under the purging follower");
		System.out.println("Lagging " + roles.lagging() + " from index " + lagFrom + "; leader now " + roles.purging()
				+ " with its snapshot at " + support.snapshotIndex(roles.purging()));
		return roles;
	}

	/** Where Ratis writes a snapshot it receives: {@code tmp/} of the state machine directory. */
	private static Path installDir(Path dataDir) {
		return dataDir.resolve("raft");
	}

	/** True once a directory of a snapshot being received exists under {@code raftDir}. */
	private static boolean receivingFiles(Path raftDir) {
		try (Stream<Path> paths = Files.walk(raftDir, 4)) {
			return paths.anyMatch(path -> path.getParent() != null && path.getParent().getFileName() != null
					&& path.getParent().getFileName().toString().equals("tmp")
					&& path.getFileName().toString().startsWith("snapshot-"));
		} catch (IOException | java.io.UncheckedIOException e) {
			return false;
		}
	}
}
