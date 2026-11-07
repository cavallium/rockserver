package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Membership changes on a multi-process cluster, through the {@code admin} command line: permanent loss and
 * replacement of a node, and clusters seeded from an existing single-node database.
 *
 * <p>A new member catches up by snapshot install only once the leader's log no longer starts at its first entry,
 * so these tests write enough data to fill a Raft log segment and pass the purge gap
 * ({@link #fillUntilLogsArePurged}).</p>
 */
@Timeout(600)
class MembershipTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration TIMEOUT = Duration.ofSeconds(90);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final List<String> SERVER_ARGS = List.of("--snapshot-every", "200",
			"--election-timeout-ms", "400:800",
			"--request-timeout-ms", "5000");
	/** Large values fill a 64 MB Raft log segment in about a thousand entries, the purge gap. */
	private static final int FILLER_VALUE_BYTES = 64 * 1024;
	private static final int MAX_FILLER_KEYS = 4_000;
	private static final int WRITERS = 8;
	private static final Duration RETRY_PAUSE = Duration.ofMillis(100);
	private static final String JOIN = "--join";
	/** Longer than any election timeout of {@link #SERVER_ARGS}: the time a new node waits for its {@code add-peer}. */
	private static final Duration WAIT_BEFORE_ADD_PEER = Duration.ofSeconds(5);
	private static final String SNAPSHOT_INSTALLED = "installing snapshot";
	private static final String OUTSIDE = "this node is outside the cluster";
	private static final String REPLICA_FAILED = "replica failed";
	private static final String SEEDED_BUT_EMPTY = "The cluster was seeded with an existing database, but this"
			+ " replica's database is empty";

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
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

	/**
	 * A member is lost with its disk; a new node with a new id and empty data replaces it through add-peer and
	 * remove-peer, catches up by snapshot install, and the cluster then survives the loss of another original member.
	 * What is lost is the leader, and the new node waits for its add-peer longer than an election timeout: a leader of
	 * a later term closes the Raft server of a node outside its configuration that asks for a vote, so a new node must
	 * not ask. It restarts during the wait, and again later, without the option.
	 */
	@Test
	void lostNodeIsReplacedByANewNodeThatCatchesUpFromASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		long column = client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		int keys = fillUntilLogsArePurged(column, 0, List.of("1", "2", "3"));
		var originals = List.of("1", "2", "3");
		assertOk(cluster.admin(originals, "transfer-leader", "2"));
		assertOk(cluster.admin(originals, "transfer-leader", "1"));
		InProcessCluster.await(TIMEOUT, () -> {
			try {
				return leaderOf(originals).equals("1");
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});

		cluster.kill("1");
		deleteRecursively(cluster.dataDir("1"));
		cluster.setMembers(List.of("2", "3", "4"));
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		Thread.sleep(WAIT_BEFORE_ADD_PEER);
		cluster.kill("4");
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		Thread.sleep(WAIT_BEFORE_ADD_PEER);

		long addStart = System.nanoTime();
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		var addTime = Duration.ofNanos(System.nanoTime() - addStart);
		assertOk(cluster.admin(List.of("1", "2", "3", "4"), "remove-peer", "1"));
		var membershipTime = Duration.ofNanos(System.nanoTime() - addStart);
		var output4 = cluster.output("4");
		assertTrue(output4.contains(SNAPSHOT_INSTALLED), "node 4 did not catch up by snapshot install");
		assertFalse(output4.contains(REPLICA_FAILED), "node 4 failed:\n" + cluster.logTail("4"));
		System.out.printf("MEASURE replacement: %d keys of %d bytes, add-peer (catch-up) %d ms,"
				+ " add-peer + remove-peer %d ms%n", keys, FILLER_VALUE_BYTES, addTime.toMillis(),
				membershipTime.toMillis());

		for (int i = keys; i < keys + 100; i++) {
			client.put(column, key(i), fillerValue(i), CONTEXT).valueOrThrow();
		}
		keys += 100;

		// With a configuration of its own the node needs the option no more. A majority of {2, 3, 4} needs it later on.
		cluster.stop("4");
		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);
		assertArrayEquals(fillerValue(keys - 1).toByteArray(), readThrough("4", column, key(keys - 1)).toByteArray());

		// Another original member is lost: the new membership {2, 3, 4} keeps a majority.
		cluster.kill("2");
		long failoverStart = System.nanoTime();
		client.put(column, key(keys), fillerValue(keys), CONTEXT).valueOrThrow();
		System.out.printf("MEASURE first write after losing node 2: %d ms%n",
				Duration.ofNanos(System.nanoTime() - failoverStart).toMillis());
		keys++;
		for (int i = 0; i < keys; i += 37) {
			assertArrayEquals(fillerValue(i).toByteArray(), client.get(column, key(i), CONTEXT).valueOrThrow()
					.toByteArray(), "key " + i);
		}

		// Whichever survivor leads, the other one serves through it.
		var survivors = List.of("3", "4");
		awaitAppliedEverywhere(survivors, column, key(keys - 1), fillerValue(keys - 1));
		stopAll(survivors);
		assertReplicasEqual(survivors, Map.of("data", (long) keys));
	}

	/**
	 * A cluster seeded with a copy of an existing database serves its data; a member added later with an empty
	 * database receives the data as a snapshot.
	 */
	@Test
	void seededClusterServesTheSeedAndANewMemberReceivesItAsASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var seed = createSeed();
		for (var id : List.of("1", "2", "3")) {
			copyRecursively(seed.dir, cluster.dataDir(id).resolve("db"));
		}
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		assertSeedServed(seed, List.of("1", "2", "3"));
		assertTrue(cluster.output(leaderOf(List.of("1", "2", "3"))).contains("(seeded: true)"),
				"the cluster was not initialized as seeded");

		long filler = client.createColumn("filler", SCHEMA, CONTEXT).valueOrThrow();
		int keys = fillUntilLogsArePurged(filler, 0, List.of("1", "2", "3"));

		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		long addStart = System.nanoTime();
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		System.out.printf("MEASURE seeded join: add-peer (catch-up) %d ms after %d filler keys%n",
				Duration.ofNanos(System.nanoTime() - addStart).toMillis(), keys);
		var output4 = cluster.output("4");
		assertTrue(output4.contains(SNAPSHOT_INSTALLED), "node 4 did not receive a snapshot");
		assertFalse(output4.contains(REPLICA_FAILED), "node 4 failed:\n" + cluster.logTail("4"));

		// The option stays harmless on a node that is a member, and the new member serves the seed from its own database
		// once it leads.
		var members = List.of("1", "2", "3", "4");
		cluster.stop("4");
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		// A transfer needs the follower's match index, which only a reply to an entry moves: the reply to the last entry may
		// have gone with the stop.
		client.put(filler, key(keys - 1), fillerValue(keys - 1), CONTEXT).valueOrThrow();
		assertOk(cluster.admin(members, "transfer-leader", "4"));
		assertSeedServed(seed, List.of("4"));
		awaitAppliedEverywhere(members, filler, key(keys - 1), fillerValue(keys - 1));
		stopAll(members);
		var expected = new TreeMap<String, Long>(seed.keysByColumn);
		expected.put("filler", (long) keys);
		assertReplicasEqual(members, expected);
	}

	/**
	 * A member with an empty database added to a seeded cluster right after it started must get the seed from the
	 * snapshot the leader takes after genesis, never by replaying a log that lacks the seed. (If it ever did replay
	 * the genesis entry, it would stop with a clear error instead of diverging.)
	 */
	@Test
	void emptyMemberJoiningASeededClusterRightAwayGetsTheSeedFromASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var seed = createSeed();
		for (var id : List.of("1", "2", "3")) {
			copyRecursively(seed.dir, cluster.dataDir(id).resolve("db"));
		}
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		assertSeedServed(seed, List.of("1", "2", "3"));

		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		InProcessCluster.await(TIMEOUT, () -> cluster.output("4").contains(SNAPSHOT_INSTALLED));
		assertFalse(cluster.output("4").contains(SEEDED_BUT_EMPTY), "node 4 replayed the seeded genesis entry");

		var column = client.getColumnId("seeded", CONTEXT).valueOrThrow();
		client.put(column, key(1_000_000), text("after"), CONTEXT).valueOrThrow();
		var members = List.of("1", "2", "3", "4");
		awaitAppliedEverywhere(members, column, key(1_000_000), text("after"));
		stopAll(members);
		var expected = new TreeMap<String, Long>(seed.keysByColumn);
		expected.merge("seeded", 1L, Long::sum);
		assertReplicasEqual(members, expected);
	}

	/** A running node that a {@code remove-peer} removes closes Raft by itself, and must not stay up as a shell. */
	@Test
	void aNodeRemovedWhileRunningExits() throws Exception {
		cluster = ProcessCluster.create(root, 3, 0, serverArgs());
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();

		var removed = List.of("1", "2", "3").stream()
				.filter(id -> {
					try {
						return !id.equals(leaderOf(List.of("1", "2", "3")));
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				})
				.findFirst()
				.orElseThrow();
		assertOk(cluster.admin(List.of("1", "2", "3"), "remove-peer", removed));
		// 3 when Ratis closes its Raft server (NOT_IN_CONF), 4 when its check while it hears no leader comes first (R11).
		int code = cluster.awaitExit(removed, TIMEOUT);
		assertTrue(code == 3 || code == 4, () -> code + "\n" + cluster.logTail(removed));
	}

	/**
	 * Whichever node leads when a node of a seeded cluster is lost, the documented procedure works: every replica took a
	 * snapshot of the seed, so a new member never replays a log that does not hold it. What is lost is the leader, so the
	 * node that leads afterwards is a follower of the time of genesis.
	 */
	@Test
	void aNodeOfASeededClusterIsReplacedWhicheverNodeLeads() throws Exception {
		var seed = startSeededCluster();
		var originals = List.of("1", "2", "3");
		long column = client.getColumnId("seeded", CONTEXT).valueOrThrow();
		client.put(column, key(1_000_000), text("after"), CONTEXT).valueOrThrow();

		var lost = leaderOf(originals);
		cluster.kill(lost);
		deleteRecursively(cluster.dataDir(lost));
		var members = new ArrayList<>(originals);
		members.remove(lost);
		members.add("4");
		cluster.setMembers(members);
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);

		assertOk(cluster.admin(originals, "add-peer", cluster.peer("4").format()));
		assertOk(cluster.admin(List.of("1", "2", "3", "4"), "remove-peer", lost));
		assertTrue(cluster.output("4").contains(SNAPSHOT_INSTALLED), "node 4 did not receive a snapshot");
		assertFalse(cluster.output("4").contains(REPLICA_FAILED), () -> "node 4 failed:\n" + cluster.logTail("4"));
		client.put(column, key(1_000_001), text("later"), CONTEXT).valueOrThrow();
		awaitAppliedEverywhere(members, column, key(1_000_001), text("later"));
		assertSeedServed(seed, List.of("4"));
		stopAll(members);
		var expected = new TreeMap<String, Long>(seed.keysByColumn);
		expected.merge("seeded", 2L, Long::sum);
		assertReplicasEqual(members, expected);
	}

	/**
	 * A lost node is replaced by remove-peer of the lost node, then add-peer of the new one: two of the three members
	 * answer before, during and after both changes, so a failure of the new node cannot stop the cluster. The one-step
	 * replacement (the lost node left out of {@code --peer}, the new one in) changes two voters: {@code admin} refuses
	 * it, with exit code 1, before it proposes anything (round 14).
	 */
	@ParameterizedTest(name = "seeded: {0}, the leader is lost: {1}")
	@CsvSource({"false,true", "false,false", "true,true", "true,false"})
	void aLostNodeIsReplacedRemoveThenAdd(boolean seeded, boolean leaderLost) throws Exception {
		var originals = List.of("1", "2", "3");
		long column;
		if (seeded) {
			startSeededCluster();
			column = client.getColumnId("seeded", CONTEXT).valueOrThrow();
		} else {
			cluster = ProcessCluster.create(root, 3, 1, serverArgs());
			startAll(originals);
			client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
			column = client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		}
		client.put(column, key(2_000_000), text("before"), CONTEXT).valueOrThrow();
		var leader = leaderOf(originals);
		var lost = leaderLost ? leader : originals.stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
		cluster.kill(lost);
		deleteRecursively(cluster.dataDir(lost));
		var survivors = originals.stream().filter(id -> !id.equals(lost)).toList();
		var members = new ArrayList<>(survivors);
		members.add("4");
		cluster.setMembers(members);
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);

		var oneStep = cluster.adminResult(survivors, "add-peer", cluster.peer("4").format());
		assertEquals(1, oneStep.exitCode(), oneStep.output());
		assertTrue(oneStep.output().contains(VoterChange.MULTI_VOTER_REFUSAL), oneStep.output());
		for (var survivor : survivors) {
			assertFalse(cluster.output(survivor).contains("receive setConfiguration"), survivor);
		}

		assertOk(cluster.admin(originals, "remove-peer", lost));
		assertOk(cluster.admin(survivors, "add-peer", cluster.peer("4").format()));
		assertFalse(cluster.output("4").contains(REPLICA_FAILED), () -> "node 4 failed:\n" + cluster.logTail("4"));
		var status = cluster.admin(members, "status");
		assertFalse(status.contains("unreachable"), status);

		// Node 4 counts now: with it stopped the other two still commit, and so do they with one of them stopped instead.
		client.put(column, key(2_000_001), text("after"), CONTEXT).valueOrThrow();
		cluster.stop(survivors.getFirst());
		client.put(column, key(2_000_002), text("later"), CONTEXT).valueOrThrow();
		cluster.start(survivors.getFirst());
		cluster.waitReady(survivors.getFirst(), READY_TIMEOUT);
		client.put(column, key(2_000_003), text("end"), CONTEXT).valueOrThrow();
		awaitAppliedEverywhere(members, column, key(2_000_003), text("end"));
		stopAll(members);
		var dumps = new TreeMap<String, Long>();
		dump(cluster.dataDir(members.getFirst()).resolve("db")).forEach((name, dump) -> dumps.put(name, dump.keys()));
		assertReplicasEqual(members, dumps);
	}

	/**
	 * A planned replacement: the node to replace is the leader and it runs. It closes Raft and exits when it is out. In
	 * two steps, add-peer then remove-peer, one voter each: {@code admin} refuses a change of two voters at once
	 * (round 14), which is unsafe while the old one still votes (design: "Replacing a node").
	 */
	@Test
	void aRunningLeaderIsReplacedInTwoSteps() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var originals = List.of("1", "2", "3");
		startAll(originals);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		long column = client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		client.put(column, key(1), text("before"), CONTEXT).valueOrThrow();
		var leader = leaderOf(originals);
		var others = originals.stream().filter(id -> !id.equals(leader)).toList();
		var members = new ArrayList<>(others);
		members.add("4");
		cluster.setMembers(members);
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);

		assertOk(cluster.admin(originals, "add-peer", cluster.peer("4").format()));
		var all = new ArrayList<>(originals);
		all.add("4");
		assertOk(cluster.admin(all, "remove-peer", leader));
		// 3 when Ratis closes its Raft server (NOT_IN_CONF), 4 when its check while it hears no leader comes first (R11).
		int code = cluster.awaitExit(leader, TIMEOUT);
		assertTrue(code == 3 || code == 4, () -> code + "\n" + cluster.logTail(leader));
		client.put(column, key(2), text("after"), CONTEXT).valueOrThrow();
		awaitAppliedEverywhere(members, column, key(2), text("after"));
		stopAll(members);
		assertReplicasEqual(members, Map.of("data", 2L));
	}

	/**
	 * A node that forgot {@code --join} and lists only itself elected itself and took writes: {@code add-peer} refuses it,
	 * and also under a new attempt with the node stopped and started again.
	 */
	@Test
	void addPeerRefusesANodeWithALogOfItsOwn() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var members = List.of("1", "2", "3");
		startAll(members);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		long column = client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		client.put(column, key(1), text("real"), CONTEXT).valueOrThrow();

		cluster.setMembers(List.of("4"));
		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> {
			try {
				return leaderOf(List.of("4")).equals("4");
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
		InProcessCluster.await(TIMEOUT, () -> {
			try {
				return cluster.admin(List.of("4"), "status").contains("commitIndex: 1");
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		});
		var result = cluster.adminResult(members, "add-peer", cluster.peer("4").format());
		assertEquals(1, result.exitCode(), result.output());
		assertTrue(result.output().contains("log of its own"), result.output());
		var status = cluster.admin(members, "status");
		assertFalse(status.contains("id: \"4\""), status);
	}

	/**
	 * A start without {@code --join} leaves the peer list of that start in the data directory, and Ratis then uses it
	 * whatever the next start says: {@code --join} must not look as if it helped. {@code add-peer} refuses such a node too:
	 * it may run just then, in the middle of its exits, and it would become a member.
	 */
	@Test
	void joinIsRefusedAfterAStartWithoutIt() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var originals = List.of("1", "2", "3");
		startAll(originals);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();

		var members = List.of("1", "2", "3", "4");
		cluster.setMembers(members);
		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);
		var result = cluster.adminResult(originals, "add-peer", cluster.peer("4").format());
		assertEquals(1, result.exitCode(), result.output());
		assertTrue(result.output().contains("without --join"), result.output());
		if (cluster.isRunning("4")) {
			cluster.kill("4");
		}

		cluster.start("4", JOIN);
		assertEquals(1, cluster.awaitExit("4", TIMEOUT), () -> cluster.logTail("4"));
		assertTrue(cluster.output("4").contains("Wipe the data directory"), () -> cluster.logTail("4"));

		deleteRecursively(cluster.dataDir("4"));
		cluster.start("4", JOIN);
		cluster.waitReady("4", READY_TIMEOUT);
		assertOk(cluster.admin(originals, "add-peer", cluster.peer("4").format()));
	}

	/** It would never end, and flood the leader with requests: a node that is not there is told at once. */
	@Test
	void addPeerFailsAtOnceWithoutItsNode() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var members = List.of("1", "2", "3");
		startAll(members);
		var result = cluster.adminResult(members, "add-peer", cluster.peer("4").format());
		assertEquals(1, result.exitCode(), result.output());
		assertTrue(result.output().contains("does not answer"), result.output());
		assertFalse(cluster.output(leaderOf(members)).contains("receive setConfiguration"));
	}

	/** Without a majority no change can end. The command ends at its limit, with an error. */
	@Test
	void aChangeWithoutAMajorityEndsAtItsTimeLimit() throws Exception {
		cluster = ProcessCluster.create(root, 3, 0, serverArgs());
		var members = List.of("1", "2", "3");
		startAll(members);
		cluster.kill("2");
		cluster.kill("3");
		long start = System.nanoTime();
		var result = cluster.adminResult(members, "--timeout-ms", "3000", "remove-peer", "3");
		var elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertEquals(1, result.exitCode(), result.output());
		assertTrue(result.output().contains("FAILED"), result.output());
		assertTrue(elapsed.compareTo(Duration.ofSeconds(30)) < 0, () -> "took " + elapsed);
	}

	@Test
	void aMemberTakesASnapshotOnRequest() throws Exception {
		cluster = ProcessCluster.create(root, 3, 0, serverArgs());
		var members = List.of("1", "2", "3");
		startAll(members);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		assertOk(cluster.admin(members, "snapshot", "2"));
		assertTrue(cluster.output("2").contains("snapshot taken at"), () -> cluster.logTail("2"));
		assertFalse(cluster.output("3").contains("snapshot taken at"), () -> cluster.logTail("3"));
	}

	/**
	 * DD-30 / R4-6 / R11: a removed node that a supervisor starts again, while the members are up, exits with 4 before
	 * it serves anything, whether its log holds the configuration that removed it or the members tell it.
	 */
	@Test
	void aRemovedNodeThatRestartsExitsWithFour() throws Exception {
		cluster = ProcessCluster.create(root, 3, 0, serverArgs());
		var members = List.of("1", "2", "3");
		startAll(members);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		var removed = members.stream().filter(id -> {
			try {
				return !id.equals(leaderOf(members));
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}).findFirst().orElseThrow();
		assertOk(cluster.admin(members, "remove-peer", removed));
		int code = cluster.awaitExit(removed, TIMEOUT);
		assertTrue(code == 3 || code == 4, () -> code + "\n" + cluster.logTail(removed));

		cluster.start(removed);
		assertEquals(4, cluster.awaitExit(removed, TIMEOUT), () -> cluster.logTail(removed));
		var output = cluster.output(removed);
		assertTrue(output.contains("removed from the cluster"), () -> cluster.logTail(removed));
		assertEquals(1, output.lines().filter(line -> line.strip().equals("READY " + removed)).count(),
				() -> "READY only from its first start\n" + cluster.logTail(removed));
		// The other two serve on without it.
		client.put(client.createColumn("after", SCHEMA, CONTEXT).valueOrThrow(), key(1), text("x"), CONTEXT)
				.valueOrThrow();
	}

	/** Nothing starts an election in such a cluster. */
	@Test
	void aNewClusterOfNodesWithJoinWarns() throws Exception {
		cluster = ProcessCluster.create(root, 3, 0, serverArgs());
		for (var id : List.of("1", "2", "3")) {
			cluster.start(id, JOIN);
		}
		cluster.waitReady(READY_TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.output("1").contains(OUTSIDE));
		assertTrue(cluster.output("1").contains("nothing elects a leader"), () -> cluster.logTail("1"));
	}

	// ---- cluster helpers ----

	/** Three nodes that start from a copy of a database: returns once every replica has a snapshot of it. */
	private Seed startSeededCluster() throws IOException {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var seed = createSeed();
		var originals = List.of("1", "2", "3");
		for (var id : originals) {
			copyRecursively(seed.dir, cluster.dataDir(id).resolve("db"));
		}
		startAll(originals);
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		assertSeedServed(seed, originals);
		InProcessCluster.await(TIMEOUT, () -> originals.stream().allMatch(id -> cluster.output(id).contains("snapshot taken at")));
		return seed;
	}

	private List<String> serverArgs() throws IOException {
		var config = root.resolve("rockserver.conf");
		Files.writeString(config, InProcessCluster.MERGE_CONFIG);
		var args = new ArrayList<>(SERVER_ARGS);
		args.add("--rockserver-config");
		args.add(config.toString());
		return args;
	}

	private Path rockserverConfig() {
		return root.resolve("rockserver.conf");
	}

	private void startAll(List<String> ids) throws IOException {
		for (var id : ids) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
	}

	/**
	 * Stops the nodes gracefully, so that their databases hold everything they applied: replicas apply entries
	 * without the RocksDB WAL, so after a kill the files are a consistent but older prefix, which the node completes
	 * from its Raft log only when it starts again.
	 */
	private void stopAll(List<String> ids) {
		for (var id : ids) {
			cluster.stop(id);
		}
	}

	private static void assertOk(String adminOutput) {
		assertTrue(adminOutput.lines().anyMatch(line -> line.strip().equals("OK")), adminOutput);
	}

	/** The leader according to {@code admin status}, asked through {@code members}. */
	private String leaderOf(List<String> members) throws IOException {
		var status = cluster.admin(members, "status");
		return status.lines()
				.filter(line -> line.contains(": LEADER"))
				.map(line -> line.substring(0, line.indexOf(':')))
				.findFirst()
				.orElseThrow(() -> new AssertionError("No leader in status:\n" + status));
	}

	/**
	 * Makes each member lead in turn and reads {@code key} through it: a leader serves a read only after applying
	 * every committed entry, so afterwards every member's database holds all the user writes made so far. Reading
	 * through each other member at the same time checks that followers reach every leader, including members that
	 * joined after them.
	 */
	private void awaitAppliedEverywhere(List<String> members, long column, Keys key, Buf expected)
			throws IOException {
		for (var leader : members) {
			assertOk(cluster.admin(members, "transfer-leader", leader));
			InProcessCluster.await(TIMEOUT, () -> {
				try {
					return leaderOf(members).equals(leader);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			for (var via : members) {
				var value = readThrough(via, column, key);
				assertNotNull(value, "read through " + via + " with leader " + leader);
				assertArrayEquals(expected.toByteArray(), value.toByteArray(),
						"read through " + via + " with leader " + leader);
			}
			assertEquals(leader, leaderOf(members), "leadership moved during the reads");
		}
	}

	/**
	 * Reads {@code key} through node {@code via} only, retrying retryable failures (a follower that did not learn
	 * of a new leader yet) within the timeout; any other failure fails the test at once.
	 */
	private Buf readThrough(String via, long column, Keys key) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			try {
				return client.api(via, CONTEXT).get(0, column, key, RequestType.current());
			} catch (RuntimeException e) {
				if (!ClusterClient.isRetryable(e) || System.nanoTime() - deadline > 0) {
					throw new AssertionError("Read through node " + via + " failed", e);
				}
			}
			try {
				// Retry pause only: the loop's outcome does not depend on its length.
				Thread.sleep(RETRY_PAUSE);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			}
		}
	}

	/**
	 * Writes keys from {@code firstKey} up, in rounds, until the Raft log of every member starts after index 0,
	 * so that a member added from now on needs a snapshot. Returns the next unused key.
	 */
	private int fillUntilLogsArePurged(long column, int firstKey, List<String> members) throws Exception {
		int next = firstKey;
		long start = System.nanoTime();
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
			while (!members.stream().allMatch(this::logPurged)) {
				if (next - firstKey >= MAX_FILLER_KEYS) {
					throw new AssertionError("Raft logs not purged after " + MAX_FILLER_KEYS + " writes: "
							+ members.stream().map(id -> id + "=" + logFiles(id)).toList());
				}
				var round = new ArrayList<Future<?>>();
				for (int w = 0; w < WRITERS; w++) {
					int writer = w;
					int base = next;
					round.add(executor.submit(() -> {
						for (int i = base + writer; i < base + 200; i += WRITERS) {
							client.put(column, key(i), fillerValue(i), CONTEXT).valueOrThrow();
						}
					}));
				}
				for (var future : round) {
					try {
						future.get();
					} catch (ExecutionException e) {
						throw new AssertionError("Filler write failed", e.getCause());
					}
				}
				next += 200;
			}
		}
		System.out.printf("MEASURE filler: %d keys of %d bytes until the logs were purged, %d ms%n", next - firstKey,
				FILLER_VALUE_BYTES, Duration.ofNanos(System.nanoTime() - start).toMillis());
		return next;
	}

	/** True if the node's Raft log no longer holds index 0: some prefix was purged after a snapshot. */
	private boolean logPurged(String id) {
		var files = logFiles(id);
		return !files.isEmpty() && files.stream().noneMatch(name -> name.equals("log_inprogress_0")
				|| name.startsWith("log_0-"));
	}

	private List<String> logFiles(String id) {
		var dir = cluster.dataDir(id).resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("current");
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(path -> path.getFileName().toString()).filter(name -> name.startsWith("log_")).sorted()
					.toList();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	// ---- seed ----

	private record Seed(Path dir, long columnA, long columnB, Map<String, Long> keysByColumn) {}

	private static final int SEED_KEYS = 500;

	/** A single-node Rockserver database with two columns, written through the embedded API. */
	private Seed createSeed() throws IOException {
		var dir = root.resolve("seed");
		long a;
		long b;
		try (var connection = new EmbeddedConnection(dir, "seed", rockserverConfig())) {
			var db = connection.getEmbeddedDB();
			a = db.createColumn("seeded", SCHEMA);
			b = db.createColumn("seeded-merges", SCHEMA);
			for (int i = 0; i < SEED_KEYS; i++) {
				db.put(0, a, key(i), text("seed-" + i), RequestType.none());
			}
			for (int i = 0; i < 20; i++) {
				db.merge(0, b, key(i % 5), text("m" + i), RequestType.none());
			}
		}
		return new Seed(dir, a, b, Map.of("seeded", (long) SEED_KEYS, "seeded-merges", 5L));
	}

	/** Every seeded key reads back through each of {@code via}, with the seed's column ids. */
	private void assertSeedServed(Seed seed, List<String> via) {
		for (var id : via) {
			var api = client.api(id, CONTEXT);
			assertEquals(seed.columnA, api.getColumnId("seeded"), "column id through " + id);
			assertEquals(seed.columnB, api.getColumnId("seeded-merges"), "column id through " + id);
			for (int i = 0; i < SEED_KEYS; i += 7) {
				assertEquals("seed-" + i, text(api.get(0, seed.columnA, key(i), RequestType.current())),
						"key " + i + " through " + id);
			}
			assertEquals("m0,m5,m10,m15", text(api.get(0, seed.columnB, key(0), RequestType.current())));
		}
	}

	// ---- replica comparison ----

	private record ColumnDump(long id, long keys, String sha256) {}

	/** Every user column of a stopped node's database: id, key count and a digest of its contents in key order. */
	private Map<String, ColumnDump> dump(Path dbDir) throws IOException {
		var result = new TreeMap<String, ColumnDump>();
		try (var connection = new EmbeddedConnection(dbDir, "dump", rockserverConfig())) {
			var db = connection.getEmbeddedDB();
			for (var name : db.getAllColumnDefinitions().keySet()) {
				if (name.equals(NodeStore.META_COLUMN)) {
					continue;
				}
				long id = db.getColumnId(name);
				var digest = sha256();
				long count = 0;
				try (Stream<KV> range = db.getRange(0, id, null, null, false, RequestType.<KV>allInRange())) {
					for (var kv : (Iterable<KV>) range::iterator) {
						for (var part : kv.keys().keys()) {
							update(digest, part.toByteArray());
						}
						update(digest, kv.value() == null ? new byte[0] : kv.value().toByteArray());
						count++;
					}
				}
				result.put(name, new ColumnDump(id, count, HexFormat.of().formatHex(digest.digest())));
			}
		}
		return result;
	}

	/** Every replica holds identical user data, with the expected number of keys per column. */
	private void assertReplicasEqual(List<String> ids, Map<String, Long> expectedKeys) throws IOException {
		Map<String, ColumnDump> first = null;
		for (var id : ids) {
			var dump = dump(cluster.dataDir(id).resolve("db"));
			var counts = new TreeMap<String, Long>();
			dump.forEach((name, column) -> counts.put(name, column.keys()));
			assertEquals(new TreeMap<>(expectedKeys), counts, "key counts of node " + id);
			if (first == null) {
				first = dump;
			} else {
				assertEquals(first, dump, "node " + id + " differs from node " + ids.getFirst());
			}
		}
	}

	private static MessageDigest sha256() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	/** Length-prefixed, so that different splits of the same bytes digest differently. */
	private static void update(MessageDigest digest, byte[] bytes) {
		digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
		digest.update(bytes);
	}

	// ---- data ----

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	/** Incompressible value of a filler key, reproducible from the key. */
	private static Buf fillerValue(long key) {
		var bytes = new byte[FILLER_VALUE_BYTES];
		new SplittableRandom(key).nextBytes(bytes);
		return Buf.wrap(bytes);
	}

	// ---- files ----

	private static void copyRecursively(Path source, Path target) throws IOException {
		try (Stream<Path> paths = Files.walk(source)) {
			for (var path : (Iterable<Path>) paths::iterator) {
				var copy = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(copy);
				} else {
					Files.copy(path, copy);
				}
			}
		}
	}

	private static void deleteRecursively(Path path) throws IOException {
		if (!Files.exists(path)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(path)) {
			for (var entry : paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
				Files.delete(entry);
			}
		}
	}
}
