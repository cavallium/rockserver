package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.CrashPoints.Point;
import java.io.IOException;
import java.io.UncheckedIOException;
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
 * <p>With the production settings this cannot happen: the log is purged only up to what every member committed
 * ({@code RaftSettings}), and that holds on the followers as well, since the leader sends every member's commit index
 * to each of them. A lagging member then always catches up from the log. These tests start every node with the test
 * setting {@link RaftSettings#PURGE_UP_TO_SNAPSHOT} (through {@link CrashPointMain#purgeUpToSnapshot}), which purges up
 * to each snapshot: follower {@code F} is cut off (a partitioned link) while the others write and snapshot, and the
 * link heals only once the leader's log starts past {@code F}'s last entry, checked on disk before the heal. The
 * install path that runs is the production one; only the reason the log is gone differs (in production: a new member,
 * or a member whose log the leader no longer has after a repair). A slow link (once {@code LinkProxy} can throttle,
 * see RT-4) would give the same lag without a cut.</p>
 */
@Timeout(600)
class LaggingInstallTest {

	private static final String SNAPSHOT_INSTALLED = "installing snapshot";
	private static final long SNAPSHOT_EVERY = 100;
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

	/** The roles of a test: the leader (which purges its log and sends the snapshot) and the follower that lags. */
	private record Roles(String leader, String lagging) {}

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
	 * received file appears). The remaining two elect a leader, which resends a snapshot (its log is purged too); the
	 * killed node restarts. Whether the kill fell inside the transfer depends on timing, and is printed.
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
		cluster.kill(roles.leader());
		System.out.println("Killed the sending leader " + roles.leader() + (caught ? " during" : " after")
				+ " the transfer to " + roles.lagging());
		// The other follower and the lagging node elect a leader without the killed one.
		support.awaitLeader(PHASE_TIMEOUT);
		finish(roles);
	}

	/**
	 * Cuts the lagging follower off and writes until the leader's log starts past the lagging follower's last entry
	 * (the leader purged it up to a snapshot), so that only an install can catch it up. The lagging one is still cut
	 * off when this returns.
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
			CrashPointMain.purgeUpToSnapshot(cluster.dataDir(id));
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
		var lagging = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		var roles = new Roles(leader, lagging);
		cluster.network().isolate(roles.lagging());
		// The test reaches the nodes directly, not through the cut links. Nothing reaches the lagging node any more.
		long[] laggingLast = {-1};
		ScenarioSupport.await(PHASE_TIMEOUT, () -> {
			var info = support.groupInfos().get(roles.lagging());
			laggingLast[0] = info == null ? -1 : info.getLogInfoProto().getLastEntry().getIndex();
			return laggingLast[0] >= 0;
		}, "the lagging node does not answer");
		ScenarioSupport.await(Duration.ofMinutes(3), () -> logStart(cluster.dataDir(roles.leader())) > laggingLast[0] + 1,
				"the leader did not purge its log past the lagging node's last entry " + laggingLast[0]);
		assertTrue(roles.leader().equals(support.leaderOrNull()), "the leader changed while the lagging node was cut off");
		System.out.println("Lagging " + roles.lagging() + " with its last entry at " + laggingLast[0] + "; leader "
				+ roles.leader() + " has its log from " + logStart(cluster.dataDir(roles.leader()))
				+ " and its snapshot at " + support.snapshotIndex(roles.leader()));
		return roles;
	}

	/**
	 * The first index of the node's Raft log, from the names of its segment files ({@code log_<start>-<end>},
	 * {@code log_inprogress_<start>}), or -1 if there is none or the directory cannot be read.
	 */
	static long logStart(Path dataDir) {
		try (Stream<Path> paths = Files.walk(dataDir.resolve("raft"), 4)) {
			return paths.filter(path -> path.getParent() != null && path.getParent().getFileName() != null
							&& path.getParent().getFileName().toString().equals("current"))
					.map(path -> path.getFileName().toString())
					.filter(name -> name.startsWith("log_"))
					.mapToLong(name -> {
						var rest = name.startsWith("log_inprogress_") ? name.substring("log_inprogress_".length())
								: name.substring("log_".length());
						int dash = rest.indexOf('-');
						return Long.parseLong(dash < 0 ? rest : rest.substring(0, dash));
					})
					.min().orElse(-1);
		} catch (IOException | UncheckedIOException | NumberFormatException e) {
			return -1;
		}
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
		} catch (IOException | UncheckedIOException e) {
			return false;
		}
	}
}
