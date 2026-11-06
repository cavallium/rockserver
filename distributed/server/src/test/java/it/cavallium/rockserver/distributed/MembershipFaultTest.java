package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.distributed.CrashPoints.Point;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scenario 8 of the design: a membership change under faults. Node 4 joins a 3-node cluster under writes through
 * {@code add-peer}, and a node halts or is killed while the change is in its joint configuration (old and new members
 * together):
 * <ul>
 *   <li>the leader, or node 4, halted where it applies the committed joint configuration (the final one not yet);</li>
 *   <li>the leader, or node 4, killed while the joint configuration is appended and not committed. The test holds that
 *   state on purpose: the two old followers are paused (SIGSTOP) before {@code add-peer}, so the leader catches node 4
 *   up and appends the joint configuration, which needs a majority of the old members and cannot get one; the kill
 *   comes once the node's own configuration shows the joint one, then the followers resume.</li>
 * </ul>
 * The configuration must end as a plain one (never stuck in the joint phase), with node 4 in it whenever the joint
 * configuration was committed, no acknowledged write may be lost, and every member must end with the same data.
 */
@Timeout(600)
class MembershipFaultTest {

	private static final Duration CRASH_TIMEOUT = Duration.ofSeconds(90);
	private static final List<String> ORIGINALS = List.of("1", "2", "3");
	private static final List<String> ALL = List.of("1", "2", "3", "4");
	/**
	 * Election timeouts of the tests that pause both old followers: the leader steps down once a majority has not
	 * answered for the longest one, and before that {@code add-peer} must start its JVM, wait out the paused members'
	 * answers (3 s) and reach the leader.
	 */
	private static final String SLOW_ELECTIONS = "15000:20000";

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

	private void start() throws Exception {
		start("400:800");
	}

	private void start(String electionTimeouts) throws Exception {
		rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 1, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--snapshot-every", "200",
				"--election-timeout-ms", electionTimeouts,
				"--request-timeout-ms", "5000"), CrashPointMain.class.getName());
		for (var id : ORIGINALS) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		support = new ScenarioSupport(cluster);
		var originals = new LinkedHashMap<String, Utils.HostAndPort>();
		ORIGINALS.forEach(id -> originals.put(id, cluster.apiAddress(id)));
		client = new ClusterClient(originals, CLIENT_RETRY_TIMEOUT, false);
		column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		workload = new ScenarioSupport.UniqueWrites(originals, column, 4, 256).start();
		workload.awaitMore(200, PHASE_TIMEOUT, "before the change");
		cluster.setMembers(ALL);
		cluster.start("4", "--join");
		cluster.waitReady("4", READY_TIMEOUT);
	}

	/** Runs {@code add-peer 4} in the background: the command may fail when its leader dies. */
	private CompletableFuture<ProcessCluster.CommandResult> addPeerAsync() {
		return addPeerAsync(ORIGINALS);
	}

	/**
	 * Runs {@code add-peer 4} in the background with the members listed in this order (the Ratis client of the command
	 * tries the first one first).
	 */
	private CompletableFuture<ProcessCluster.CommandResult> addPeerAsync(List<String> members) {
		return CompletableFuture.supplyAsync(() -> {
			try {
				return cluster.adminResult(members, "add-peer", cluster.peer("4").format());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
	}

	@Test
	void theLeaderHaltedAfterTheJointConfigurationCommittedLeavesNode4AMember() throws Exception {
		start();
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		armAndWait(leader, Point.JOINT_CONFIGURATION_APPLIED);
		var command = addPeerAsync();
		support.awaitCrash(leader, Point.JOINT_CONFIGURATION_APPLIED, CRASH_TIMEOUT);
		System.out.println("Leader " + leader + " halted in the joint configuration; add-peer: " + outcome(command));
		disarmOthers(leader);
		awaitPlainConfiguration(Set.copyOf(ALL));
		cluster.start(leader);
		cluster.waitReady(leader, READY_TIMEOUT);
		finish(ALL);
	}

	@Test
	void theJoiningNodeHaltedInTheJointConfigurationRejoinsAfterItsRestart() throws Exception {
		start();
		armAndWait("4", Point.JOINT_CONFIGURATION_APPLIED);
		var command = addPeerAsync();
		support.awaitCrash("4", Point.JOINT_CONFIGURATION_APPLIED, CRASH_TIMEOUT);
		System.out.println("Node 4 halted in the joint configuration; add-peer: " + outcome(command));
		// 1, 2 and 3 are a majority of both configurations: the change completes without node 4.
		awaitPlainConfiguration(Set.copyOf(ALL));
		cluster.start("4", "--join");
		cluster.waitReady("4", READY_TIMEOUT);
		finish(ALL);
	}

	/**
	 * The leader is killed (SIGKILL) while the joint configuration is appended in its log and in node 4's, and not
	 * committed. The old followers never received it. Whichever node wins next (node 4 may, as its log is the longest)
	 * the configuration ends plain, old or new, and node 4 can be added (again) afterwards.
	 */
	@Test
	void theLeaderKilledWithTheJointConfigurationAppendedButNotCommittedLeavesAPlainConfiguration() throws Exception {
		start(SLOW_ELECTIONS);
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		var followers = ORIGINALS.stream().filter(id -> !id.equals(leader)).toList();
		var command = holdJointConfigurationUncommitted(leader, followers, false);
		cluster.kill(leader);
		followers.forEach(cluster::resume);
		System.out.println("Killed leader " + leader + " with the joint configuration uncommitted; add-peer: "
				+ outcome(command));
		var members = awaitPlainConfiguration(null);
		System.out.println("Configuration after the kill: " + members);
		cluster.start(leader);
		cluster.waitReady(leader, READY_TIMEOUT);
		if (!members.contains("4")) {
			var again = cluster.adminResult(ORIGINALS, "add-peer", cluster.peer("4").format());
			assertTrue(again.exitCode() == 0, again.output());
		}
		awaitPlainConfiguration(Set.copyOf(ALL));
		finish(ALL);
	}

	/**
	 * Node 4 is killed (SIGKILL) once it holds the appended, uncommitted joint configuration; then the old followers
	 * resume, and 1, 2 and 3, a majority of both configurations, complete the change without it. It rejoins after a
	 * restart.
	 */
	@Test
	void theJoiningNodeKilledWithTheJointConfigurationAppendedButNotCommittedRejoinsAfterItsRestart() throws Exception {
		start(SLOW_ELECTIONS);
		var leader = support.awaitLeader(PHASE_TIMEOUT);
		var followers = ORIGINALS.stream().filter(id -> !id.equals(leader)).toList();
		var command = holdJointConfigurationUncommitted(leader, followers, true);
		cluster.kill("4");
		followers.forEach(cluster::resume);
		System.out.println("Killed node 4 with the joint configuration uncommitted; add-peer: " + outcome(command));
		var members = awaitPlainConfiguration(null);
		System.out.println("Configuration after the kill: " + members);
		cluster.start("4", "--join");
		cluster.waitReady("4", READY_TIMEOUT);
		if (!members.contains("4")) {
			var again = cluster.adminResult(ORIGINALS, "add-peer", cluster.peer("4").format());
			assertTrue(again.exitCode() == 0, again.output());
		}
		awaitPlainConfiguration(Set.copyOf(ALL));
		finish(ALL);
	}

	/**
	 * Pauses the old followers and starts {@code add-peer 4}: the leader catches node 4 up and appends the joint
	 * configuration, which cannot commit without one of the paused followers. Returns the running command once the
	 * leader's configuration (and node 4's too, if {@code onNode4}) is the joint one. The followers stay paused.
	 */
	private CompletableFuture<ProcessCluster.CommandResult> holdJointConfigurationUncommitted(String leader,
			List<String> followers, boolean onNode4) {
		followers.forEach(cluster::pause);
		var order = new ArrayList<String>();
		order.add(leader);
		order.addAll(followers);
		var command = addPeerAsync(order);
		ScenarioSupport.await(Duration.ofSeconds(15), () -> isJoint(leader) && (!onNode4 || isJoint("4")),
				"the joint configuration was not appended" + (onNode4 ? " on the leader and on node 4" : " on the leader")
						+ " while the old followers were paused");
		// Still the leader of the same configuration: a step-down could only follow 15 s without a majority.
		var info = support.groupInfo(leader);
		assertTrue(info != null && info.getRoleInfoProto().getRole() == RaftProtos.RaftPeerRole.LEADER,
				"the leader stepped down before the kill");
		return command;
	}

	/** Whether the node's current configuration (appended, committed or not) is a joint one. */
	private boolean isJoint(String id) {
		var info = support.groupInfo(id);
		return info != null && info.getConf().map(conf -> conf.getOldPeersCount() > 0).orElse(false);
	}

	private void armAndWait(String id, Point point) throws Exception {
		CrashPointMain.arm(cluster.dataDir(id), point, 0);
		ScenarioSupport.await(Duration.ofSeconds(10), () -> CrashPointMain.armed(cluster.dataDir(id)),
				"node " + id + " did not arm");
	}

	/** Every node applies the joint configuration too: only the chosen one may halt there. */
	private void disarmOthers(String halted) throws Exception {
		for (var id : ALL) {
			if (!id.equals(halted) && cluster.isRunning(id)) {
				CrashPointMain.disarm(cluster.dataDir(id));
			}
		}
	}

	private static String outcome(CompletableFuture<ProcessCluster.CommandResult> command) {
		try {
			var result = command.get(CRASH_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			var output = result.output().strip();
			return "exit " + result.exitCode() + (output.isEmpty() ? "" : ": "
					+ output.lines().reduce((a, b) -> b).orElse(""));
		} catch (Exception e) {
			return "no result: " + e;
		}
	}

	/**
	 * Waits until the leader's configuration is a plain one (no previous members) and, if {@code expected} is not
	 * null, has exactly those members. Returns the members.
	 */
	private Set<String> awaitPlainConfiguration(@Nullable Set<String> expected) {
		@SuppressWarnings("unchecked")
		Set<String>[] members = new Set[1];
		ScenarioSupport.await(PHASE_TIMEOUT, () -> {
			var leader = support.leaderOrNull();
			if (leader == null) {
				return false;
			}
			var info = support.groupInfos().get(leader);
			RaftConfigurationProto conf = info == null ? null : info.getConf().orElse(null);
			if (conf == null || conf.getOldPeersCount() > 0) {
				return false;
			}
			members[0] = ids(conf.getPeersList());
			return expected == null || expected.equals(members[0]);
		}, "the configuration did not become plain" + (expected == null ? "" : " with members " + expected));
		return members[0];
	}

	private static Set<String> ids(List<RaftPeerProto> peers) {
		return peers.stream().map(peer -> peer.getId().toString(StandardCharsets.UTF_8))
				.collect(Collectors.toCollection(TreeSet::new));
	}

	private void finish(List<String> members) throws Exception {
		for (var id : members) {
			support.awaitCaughtUp(id);
		}
		workload.awaitMore(100, PHASE_TIMEOUT, "at the end");
		workload.stop();
		var writes = workload.writes();
		workload = null;
		support.awaitQuiescent();
		ScenarioSupport.checkWrites(client, column, writes);
		support.checkReplicasIdentical(rockserverConfig);
	}
}
