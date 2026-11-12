package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.distributed.CrashPoints.Point;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scenario 1 of the design, with each kill aimed at a step of the protocol instead of a random moment: a node of a
 * 3-node process cluster under concurrent writes halts at a {@link CrashPoints crash point} (as a SIGKILL there would
 * stop it): before the leader appends a proposal, between the commit and the apply of a group on the leader, between
 * that apply and its replies, as a node wins an election, in the middle of a snapshot (checkpoint built, not
 * published) and after a snapshot was published but before Ratis learned of it. The point, the node and how many passes
 * of the point to let go first are drawn from a seeded random generator, for many rounds. A separate case stops the
 * leader with entries appended to its log and not replicated, by cutting its links first.
 *
 * <p>After every round writes must resume and the node must restart and catch up; at the end every acknowledged
 * write is readable, no write that failed as "not applied" took effect, and the replicas hold the same data.</p>
 *
 * <p>Seed and rounds: {@code -Drockserver.test.seed=<long>} and {@code -Drockserver.test.protocolKillRounds=<n>}
 * (default 12).</p>
 */
@Timeout(900)
@Tag("slow")
class ProtocolPointKillTest {

	private static final int ROUNDS = Integer.getInteger("rockserver.test.protocolKillRounds", 12);
	private static final Duration CRASH_TIMEOUT = Duration.ofSeconds(60);
	/** Points reached on the leader by the write path. */
	private static final List<Point> LEADER_POINTS = List.of(Point.BEFORE_APPEND, Point.BEFORE_APPLY,
			Point.AFTER_APPLY_BEFORE_REPLY);
	/** Points reached by any node. */
	private static final List<Point> ANY_NODE_POINTS = List.of(Point.BEFORE_APPLY, Point.AFTER_APPLY_BEFORE_REPLY,
			Point.SNAPSHOT_BUILT, Point.SNAPSHOT_TAKEN);

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;
	/** A client of one node only, with few retries: see the isolated leader's test. */
	private ClusterClient leaderClient;
	private final List<Thread> stranded = new ArrayList<>();

	private synchronized ClusterClient toLeader(String leader) {
		if (leaderClient == null) {
			leaderClient = new ClusterClient(Map.of(leader, cluster.apiAddress(leader)), Duration.ofSeconds(2), false);
		}
		return leaderClient;
	}

	@AfterEach
	void tearDown() {
		if (leaderClient != null) {
			leaderClient.close();
		}
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	private Path start() throws Exception {
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000",
				// Snapshots every few seconds of the workload, so that the snapshot points are reached.
				"--snapshot-every", "300"), CrashPointMain.class.getName());
		for (var id : cluster.members()) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		return rockserverConfig;
	}

	@Test
	void killsAtProtocolPointsLoseNoAcknowledgedWrite() throws Exception {
		long seed = Long.getLong("rockserver.test.seed", System.nanoTime());
		System.out.println("ProtocolPointKillTest seed: " + seed + " (-Drockserver.test.seed=" + seed + ")");
		var random = new Random(seed);
		var rockserverConfig = start();
		var support = new ScenarioSupport(cluster);
		long column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 64).start();
		var done = new ArrayList<String>();
		try {
			workload.awaitMore(100, PHASE_TIMEOUT, "before the first round");
			for (int round = 1; round <= ROUNDS; round++) {
				var leader = support.awaitLeader(PHASE_TIMEOUT);
				String victim;
				Point point;
				int skip;
				switch (random.nextInt(3)) {
					case 0 -> {
						victim = leader;
						point = LEADER_POINTS.get(random.nextInt(LEADER_POINTS.size()));
						skip = random.nextInt(40);
					}
					case 1 -> {
						victim = cluster.nodeIds().get(random.nextInt(cluster.nodeIds().size()));
						point = ANY_NODE_POINTS.get(random.nextInt(ANY_NODE_POINTS.size()));
						skip = point == Point.SNAPSHOT_BUILT || point == Point.SNAPSHOT_TAKEN ? 0 : random.nextInt(40);
					}
					default -> {
						victim = null;
						point = Point.ELECTED;
						skip = 0;
					}
				}
				if (point == Point.ELECTED) {
					victim = electionRound(support, leader);
				} else {
					CrashPointMain.arm(cluster.dataDir(victim), point, skip);
					support.awaitCrash(victim, point, CRASH_TIMEOUT);
				}
				long crashedAt = System.nanoTime();
				var line = "round " + round + ": " + victim + " halted at " + point + " (skip " + skip + ", leader was "
						+ leader + ")";
				System.out.println(line);
				done.add(line);
				// The two others serve on, whatever the point was.
				ScenarioSupport.await(PHASE_TIMEOUT, () -> workload.lastAcknowledgedNanos() > crashedAt,
						"no write acknowledged after " + line);
				cluster.start(victim);
				cluster.waitReady(victim, READY_TIMEOUT);
				support.awaitCaughtUp(victim);
				workload.awaitMore(50, PHASE_TIMEOUT, "after " + line);
			}
		} finally {
			workload.stop();
		}
		support.awaitQuiescent();
		var writes = workload.writes();
		int unknownApplied = ScenarioSupport.checkWrites(client, column, writes);
		System.out.println("Rounds: " + done + "; writes " + writes.size() + ", unknown outcomes that applied: "
				+ unknownApplied);
		support.checkReplicasIdentical(rockserverConfig);
		assertLogsMatch();
	}

	/**
	 * R3-G4b-3: with every node stopped, Raft's log matching holds on disk: wherever two logs hold the same index, they
	 * hold the same term. A restarted node that kept an entry the cluster replaced (an unreplicated tail of a killed
	 * leader not truncated) fails this. Returns each node's index-to-term map.
	 */
	private Map<String, TreeMap<Long, Long>> assertLogsMatch() throws IOException {
		var logs = new LinkedHashMap<String, TreeMap<Long, Long>>();
		for (var id : cluster.nodeIds()) {
			logs.put(id, LogTerms.read(cluster.dataDir(id)));
		}
		for (var a : logs.entrySet()) {
			for (var b : logs.entrySet()) {
				if (a.getKey().compareTo(b.getKey()) < 0) {
					long index = LogTerms.firstDisagreement(a.getValue(), b.getValue());
					assertEquals(-1, index, () -> "the logs of " + a.getKey() + " and " + b.getKey() + " disagree at index "
							+ index + ": term " + a.getValue().get(index) + " and " + b.getValue().get(index));
				}
			}
		}
		return logs;
	}

	/**
	 * Arms {@link Point#ELECTED} on both followers and kills the leader: whichever follower wins halts as it learns
	 * it won. The old leader restarts at once, so that the remaining follower and it form a majority. Returns the node
	 * that halted; the other follower is disarmed.
	 */
	private String electionRound(ScenarioSupport support, String leader) throws Exception {
		var followers = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();
		for (var follower : followers) {
			CrashPointMain.arm(cluster.dataDir(follower), Point.ELECTED, 0);
		}
		ScenarioSupport.await(Duration.ofSeconds(10), () -> followers.stream()
				.allMatch(id -> CrashPointMain.armed(cluster.dataDir(id))), "the followers did not arm");
		cluster.kill(leader);
		String[] halted = new String[1];
		ScenarioSupport.await(CRASH_TIMEOUT, () -> {
			for (var follower : followers) {
				if (!cluster.isRunning(follower)) {
					halted[0] = follower;
					return true;
				}
			}
			return false;
		}, "no follower won the election");
		support.awaitCrash(halted[0], Point.ELECTED, Duration.ofSeconds(5));
		var other = followers.stream().filter(id -> !id.equals(halted[0])).findFirst().orElseThrow();
		CrashPointMain.disarm(cluster.dataDir(other));
		cluster.start(leader);
		cluster.waitReady(leader, READY_TIMEOUT);
		return halted[0];
	}

	/**
	 * The leader appends entries that never reach a follower: its links are cut while clients write to it directly,
	 * then it is killed. The followers elect a leader without those entries, and the old leader, restarted, drops them
	 * from its log. None of those writes was acknowledged; the others stay.
	 */
	@Test
	void aLeaderKilledWithAppendedButUnreplicatedEntriesLosesOnlyUnacknowledgedWrites() throws Exception {
		var rockserverConfig = start();
		var support = new ScenarioSupport(cluster);
		long column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 64).start();
		String oldLeader;
		long oldTerm;
		long replicatedUpTo;
		long appendedUpTo;
		try {
			workload.awaitMore(100, PHASE_TIMEOUT, "before the fault");
			var leader = support.awaitLeader(PHASE_TIMEOUT);
			var followers = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();
			long leaderTerm = support.lastEntryTerm(leader);
			// Connected before the cut: the handshake of a new connection could outlast the leader's last moments.
			assertEquals(column, toLeader(leader).getColumnId("data", BATCH_CONTEXT).valueOrThrow());
			cluster.network().isolate(leader);
			// Two writes of the test's own, straight to the leader, right after the cut (R62-9): it appends them while it
			// still leads (it misses its majority only after an election timeout, and appends nothing once it stepped
			// down) and can send them to nobody. Without them its log was ahead of the followers' only if a write of the
			// workload happened to be between the leader's append and its send at the cut: each writer waits for its
			// write in flight, which reached the followers, so none sends another one in time, and the wait below failed
			// in one run of four.
			for (int i = 0; i < 2; i++) {
				long key = -1 - i;
				stranded.add(Thread.ofPlatform().daemon().name("stranded-write-" + i).start(
						() -> toLeader(leader).put(column, ScenarioSupport.key(key), Buf.wrap(new byte[] {1}), BATCH_CONTEXT)));
			}
			// The followers' copies of this leader's log can no longer grow (the test reaches every node directly). Read
			// their last entries once what was in flight had time to land, and before an election (at least 400 ms) can
			// add entries of a new term; then wait for the leader to append past both: those entries of its term exist on
			// the leader only.
			Thread.sleep(100);
			var infos = support.groupInfos(followers);
			assertTrue(infos.size() == followers.size(), "the followers do not answer");
			long followersLast = -1;
			for (var entry : infos.entrySet()) {
				var last = entry.getValue().getLogInfoProto().getLastEntry();
				assertTrue(last.getTerm() == leaderTerm, "follower " + entry.getKey() + " is already in term "
						+ last.getTerm() + ", not " + leaderTerm + ": the precondition cannot be checked");
				followersLast = Math.max(followersLast, last.getIndex());
			}
			long followersLastOfTerm = followersLast;
			// Clients reach the leader directly: their writes are appended there, and go nowhere else.
			ScenarioSupport.await(Duration.ofSeconds(5), () -> {
				var info = support.groupInfo(leader);
				return info != null && info.getLogInfoProto().getLastEntry().getIndex() > followersLastOfTerm;
			}, "the isolated leader appended nothing past the followers' last entry " + followersLastOfTerm);
			long leaderLast = support.groupInfo(leader).getLogInfoProto().getLastEntry().getIndex();
			cluster.kill(leader);
			oldLeader = leader;
			oldTerm = leaderTerm;
			replicatedUpTo = followersLastOfTerm;
			appendedUpTo = leaderLast;
			cluster.network().heal();
			var newLeader = support.awaitLeader(PHASE_TIMEOUT);
			assertTrue(Set.copyOf(followers).contains(newLeader), newLeader);
			System.out.println("Killed " + leader + " with its log at " + leaderLast + ", the followers' logs at "
					+ followersLastOfTerm + " (term " + leaderTerm + ")");
			long healedAt = System.nanoTime();
			ScenarioSupport.await(PHASE_TIMEOUT, () -> workload.lastAcknowledgedNanos() > healedAt,
					"no write acknowledged by the new leader");
			cluster.start(leader);
			cluster.waitReady(leader, READY_TIMEOUT);
			support.awaitCaughtUp(leader);
			workload.awaitMore(100, PHASE_TIMEOUT, "after the old leader rejoined");
		} finally {
			workload.stop();
		}
		for (var thread : stranded) {
			thread.join(Duration.ofSeconds(60));
			assertTrue(!thread.isAlive(), "a write sent to the isolated leader is still waiting");
		}
		support.awaitQuiescent();
		ScenarioSupport.checkWrites(client, column, workload.writes());
		support.checkReplicasIdentical(rockserverConfig);
		// R3-G4b-3, the truncation itself: the entries only the old leader had appended are gone from its log, replaced
		// by the new leader's (a later term), wherever its log still holds those indices.
		var logs = assertLogsMatch();
		var oldLog = logs.get(oldLeader);
		int replaced = 0;
		for (long index = replicatedUpTo + 1; index <= appendedUpTo; index++) {
			var term = oldLog.get(index);
			if (term != null) {
				long at = index;
				assertTrue(term > oldTerm, () -> "node " + oldLeader + " still holds its unreplicated entry " + at
						+ " of term " + oldTerm);
				replaced++;
			}
		}
		System.out.println("Old leader " + oldLeader + ": entries " + (replicatedUpTo + 1) + ".." + appendedUpTo
				+ " of term " + oldTerm + " replaced, " + replaced + " of them still in its log with a later term");
	}
}
