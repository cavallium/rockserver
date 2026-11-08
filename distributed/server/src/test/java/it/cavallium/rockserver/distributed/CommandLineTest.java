package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3-G2b-3: the operator commands of {@link ReplicatedServerMain} end to end, run in this JVM through {@code main} with
 * the exit replaced ({@link ReplicatedServerMain#exiter}), so each run reports its exit code and output: {@code admin
 * checksum} and {@code admin backup} against a running in-process cluster, then {@code export-seed} of a stopped
 * replica and {@code restore-backup} of the backup into an empty data directory, which then starts as a seeded node and
 * serves the data. Also the refusals: wrong arguments (exit 2), a target that exists, a restore into a used directory
 * (exit 1), and a newer cluster's database or backup (exit 5).
 */
class CommandLineTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final int KEYS = 100;

	@TempDir
	Path temp;
	private InProcessCluster cluster;
	private InProcessCluster restored;
	private IntConsumer previousExiter;

	/** Thrown by the replaced exit: an Error, so that no {@code catch (RuntimeException)} of a command swallows it. */
	private static final class Exited extends Error {
		final int code;

		Exited(int code) {
			super("exit " + code, null, false, false);
			this.code = code;
		}
	}

	/** Exit code (0 when the command returned), what it printed, and what it threw instead of exiting, if anything. */
	private record Run(int code, String out, String err, @Nullable Throwable thrown) {}

	@BeforeEach
	void replaceExit() {
		previousExiter = ReplicatedServerMain.exiter;
		ReplicatedServerMain.exiter = code -> {
			throw new Exited(code);
		};
	}

	@AfterEach
	void tearDown() throws Exception {
		ReplicatedServerMain.exiter = previousExiter;
		if (cluster != null) {
			cluster.close();
		}
		if (restored != null) {
			restored.close();
		}
	}

	private static synchronized Run run(String... args) {
		var out = new ByteArrayOutputStream();
		var err = new ByteArrayOutputStream();
		var oldOut = System.out;
		var oldErr = System.err;
		System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
		System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
		int code = 0;
		Throwable thrown = null;
		try {
			ReplicatedServerMain.main(args);
		} catch (Exited exited) {
			code = exited.code;
		} catch (Throwable e) {
			// An uncaught exception ends a real JVM with 1.
			code = 1;
			thrown = e;
		} finally {
			System.setOut(oldOut);
			System.setErr(oldErr);
		}
		var run = new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8), thrown);
		System.out.println("$ " + String.join(" ", args) + "\n-> " + code + "\n" + run.out() + run.err()
				+ (thrown != null ? thrown + "\n" : ""));
		return run;
	}

	@Test
	void adminChecksumAndBackupThenExportSeedAndRestoreBackup() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("old")), 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		for (long i = 0; i < KEYS; i++) {
			api.put(0, column, key(i), value("v" + i), RequestType.none());
		}
		var admin = new ArrayList<String>(List.of("admin"));
		for (var peer : leader.config().peers()) {
			admin.add("--peer");
			admin.add(peer.format());
		}
		admin.addAll(List.of("--timeout-ms", "30000"));

		var checksum = run(with(admin, "checksum"));
		assertEquals(0, checksum.code(), checksum.toString());
		assertTrue(checksum.out().contains("checksum at log index"), checksum.out());
		assertTrue(checksum.out().strip().endsWith("OK"), checksum.out());

		var backup = run(with(admin, "backup", "b1"));
		assertEquals(0, backup.code(), backup.toString());
		assertTrue(backup.out().contains("backup b1 at log index"), backup.out());
		assertTrue(backup.out().strip().endsWith("OK"), backup.out());

		var badName = run(with(admin, "backup", "../escape"));
		assertEquals(2, badName.code(), badName.toString());
		var extraArgument = run(with(admin, "checksum", "now"));
		assertEquals(2, extraArgument.code(), extraArgument.toString());
		var unknown = run(with(admin, "frobnicate"));
		assertEquals(2, unknown.code(), unknown.toString());

		var follower = cluster.anyFollower();
		var followerDir = follower.config().dataDir();
		var backupDir = ReplicaOps.backupsDir(followerDir).resolve("b1");
		var rockserverConfig = cluster.root().resolve("rockserver.conf");
		cluster.close();
		cluster = null;

		// export-seed of a stopped replica.
		var seed = temp.resolve("seed");
		var export = run("export-seed", "--data-dir", followerDir.toString(), "--target", seed.toString(),
				"--rockserver-config", rockserverConfig.toString());
		assertEquals(0, export.code(), export.toString());
		assertTrue(export.out().contains("Exported the database of cluster"), export.out());
		assertTrue(Files.isDirectory(seed));
		var again = run("export-seed", "--data-dir", followerDir.toString(), "--target", seed.toString(),
				"--rockserver-config", rockserverConfig.toString());
		assertEquals(1, again.code(), again.toString());
		assertInstanceOf(IOException.class, again.thrown());
		assertTrue(again.thrown().getMessage().contains("already exists"), again.thrown().getMessage());
		var missingTarget = run("export-seed", "--data-dir", followerDir.toString());
		assertEquals(2, missingTarget.code(), missingTarget.toString());
		assertTrue(missingTarget.err().contains("missing --target"), missingTarget.err());

		// restore-backup into the first node of a new single-node cluster, which then serves the data.
		var newRoot = Files.createDirectories(temp.resolve("new"));
		var target = newRoot.resolve("n0");
		var restore = run("restore-backup", "--backup", backupDir.toString(), "--data-dir", target.toString(),
				"--rockserver-config", rockserverConfig.toString());
		assertEquals(0, restore.code(), restore.toString());
		assertTrue(restore.out().startsWith("OK: "), restore.out());
		var intoUsed = run("restore-backup", "--backup", backupDir.toString(), "--data-dir", target.toString(),
				"--rockserver-config", rockserverConfig.toString());
		assertEquals(1, intoUsed.code(), intoUsed.toString());
		assertTrue(intoUsed.out().contains("FAILED"), intoUsed.out());
		var unknownOption = run("restore-backup", "--backup", backupDir.toString(), "--data-dir", target.toString(),
				"--colour", "blue");
		assertEquals(2, unknownOption.code(), unknownOption.toString());

		restored = new InProcessCluster(newRoot, 1, c -> c).startAll();
		var newLeader = restored.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> newLeader.stateMachine().store().clusterId() != null);
		var newApi = newLeader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long newColumn = newLeader.stateMachine().store().db().getColumnId("c");
		for (long i = 0; i < KEYS; i++) {
			var read = newApi.get(0, newColumn, key(i), RequestType.current());
			assertEquals("v" + i, new String(read.toByteArray(), StandardCharsets.UTF_8));
		}
	}

	/**
	 * Round 14: {@code admin} reads the leader's configuration and refuses a change of more than one voter before it
	 * proposes anything. The one-step replacement of a lost node ({@code --peer} lists the survivors and the new node) is
	 * refused with the two-step advice; remove-peer of the lost node, then add-peer of the new one, go through, and the
	 * new node counts. A re-run of a change that succeeded prints OK; a stale {@code --peer} with remove-peer is refused.
	 */
	@Test
	void adminRefusesAChangeOfMoreThanOneVoter() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("guard")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var lost = cluster.anyFollower().config().nodeId();
		var survivors = new ArrayList<>(List.of("n0", "n1", "n2"));
		survivors.remove(lost);
		cluster.stop(lost);
		cluster.join("n3");
		var newPeer = cluster.config("n3").self().format();

		var oneStep = run(admin(plus(survivors, "n3"), "add-peer", newPeer));
		assertEquals(1, oneStep.code(), oneStep.toString());
		assertTrue(oneStep.out().contains(VoterChange.MULTI_VOTER_REFUSAL), oneStep.out());
		assertTrue(oneStep.out().contains("remove-peer " + lost) && oneStep.out().contains("add-peer n3"),
				oneStep.out());
		assertVoters(List.of("n0", "n1", "n2"));

		var remove = run(admin(List.of("n0", "n1", "n2"), "remove-peer", lost));
		assertEquals(0, remove.code(), remove.toString());
		assertTrue(remove.out().strip().endsWith("OK"), remove.out());
		assertVoters(survivors);
		var add = run(admin(survivors, "add-peer", newPeer));
		assertEquals(0, add.code(), add.toString());
		assertVoters(plus(survivors, "n3"));
		// Right away, as a re-run after a timeout would be: n3 may not have applied the genesis yet, which looks like a
		// log of its own. add-peer reads the leader's configuration first and answers OK for a voter at that address,
		// without asking n3 (it must never tell to wipe a voter).
		var again = run(admin(survivors, "add-peer", newPeer));
		assertEquals(0, again.code(), again.toString());
		assertTrue(again.out().strip().endsWith("OK"), again.out());
		assertTrue(again.out().contains("already a voter"), again.out());

		// A stale --peer: it lists the lost node again, and removes a survivor.
		var stale = run(admin(List.of("n0", "n1", "n2"), "remove-peer", survivors.getFirst()));
		assertEquals(1, stale.code(), stale.toString());
		assertTrue(stale.out().contains(VoterChange.MULTI_VOTER_REFUSAL), stale.out());
		assertVoters(plus(survivors, "n3"));

		// n3 counts: with it stopped the two survivors still commit.
		var api = leader().connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), value("v1"), RequestType.none());
		cluster.stop("n3");
		// A re-run with the voter n3 down: still OK, from the leader's configuration alone.
		var whileDown = run(admin(survivors, "add-peer", newPeer));
		assertEquals(0, whileDown.code(), whileDown.toString());
		assertTrue(whileDown.out().contains("already a voter"), whileDown.out());
		var leader = leader();
		leader.connection().getSyncApi(RequestContext.batch(TIMEOUT)).put(0, column, key(2), value("v2"),
				RequestType.none());
	}

	/**
	 * Review of round 14: a refusal by Ratis itself that no re-read can change ({@code changes a majority set}: two of
	 * three voters replaced, which Ratis refuses whatever the flag) fails at once with Ratis' message, instead of
	 * reading the configuration again until the time limit.
	 */
	@Test
	void aRefusalThatNoReReadChangesFailsAtOnce() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("majority")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		cluster.join("n3");
		cluster.join("n4");
		long start = System.nanoTime();
		var replace = run(with(List.of(admin(List.of("n0", "n1", "n3", "n4"), "remove-peer", "n1")),
				ReplicatedServerMain.UNSAFE_MULTI_VOTER_CHANGE));
		var elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertEquals(1, replace.code(), replace.toString());
		assertTrue(replace.out().contains("changes a majority set"), replace.out());
		assertTrue(!replace.out().contains("kept changing"), replace.out());
		assertTrue(elapsed.compareTo(Duration.ofSeconds(15)) < 0, () -> "took " + elapsed + ": " + replace.out());
		assertVoters(List.of("n0", "n1", "n2"));
	}

	private String[] admin(List<String> peerIds, String... command) {
		var args = new ArrayList<String>(List.of("admin"));
		for (var id : peerIds) {
			args.add("--peer");
			args.add(cluster.config(id).self().format());
		}
		args.addAll(List.of("--timeout-ms", "30000"));
		args.addAll(List.of(command));
		return args.toArray(String[]::new);
	}

	/** The leader ({@link InProcessCluster#awaitLeader} also waits for a --join node to know it, which it does not). */
	private ReplicatedNode leader() {
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(ReplicatedNode::isLeader));
		return cluster.runningNodes().stream().filter(ReplicatedNode::isLeader).findFirst().orElseThrow();
	}

	private void assertVoters(List<String> expected) {
		var conf = leader().stateMachine().division().getRaftConf();
		assertEquals(expected.stream().sorted().toList(),
				conf.getCurrentPeers().stream().map(peer -> peer.getId().toString()).sorted().toList());
		assertTrue(conf.getPreviousPeers().isEmpty(), () -> "a change is under way: " + conf);
	}

	private static List<String> plus(List<String> ids, String... more) {
		var all = new ArrayList<>(ids);
		all.addAll(List.of(more));
		return all;
	}

	/**
	 * R27-1: {@code admin status} asks the members at once, each once with a time limit of its own, and prints them in
	 * the {@code --peer} order. A dead member listed first (a closed port, and a socket that accepts but never answers)
	 * no longer holds the others for the whole admin time limit, which the runbooks' "see the status" relies on.
	 */
	@Test
	void statusAnswersAtOnceWhenMembersAreDown() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("status")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		int closedPort;
		try (var probe = new java.net.ServerSocket(0)) {
			closedPort = probe.getLocalPort();
		}
		try (var silent = new java.net.ServerSocket(0)) {
			var args = new ArrayList<String>(List.of("admin",
					"--peer", "d1=127.0.0.1:" + silent.getLocalPort() + "/127.0.0.1:" + silent.getLocalPort(),
					"--peer", "d2=127.0.0.1:" + closedPort + "/127.0.0.1:" + closedPort));
			for (var id : List.of("n0", "n1", "n2")) {
				args.add("--peer");
				args.add(cluster.config(id).self().format());
			}
			// A long admin time limit, as the default (10 minutes) is: status must not wait for it.
			args.addAll(List.of("--timeout-ms", "60000", "status"));
			long start = System.nanoTime();
			var status = run(args.toArray(String[]::new));
			long tookMs = (System.nanoTime() - start) / 1_000_000;
			assertEquals(0, status.code(), status.toString());
			assertTrue(tookMs < 15_000, "status took " + tookMs + " ms");
			// One entry per member, each starting with "<id>: " (the commit infos of an answer span several lines).
			var order = List.of("d1", "d2", "n0", "n1", "n2");
			var lines = status.out().strip().lines()
					.filter(line -> order.stream().anyMatch(id -> line.startsWith(id + ": ")))
					.toList();
			assertEquals(5, lines.size(), status.out());
			for (int i = 0; i < order.size(); i++) {
				assertTrue(lines.get(i).startsWith(order.get(i) + ": "), status.out());
			}
			assertTrue(lines.get(0).contains("unreachable") && lines.get(1).contains("unreachable"), status.out());
			assertTrue(lines.subList(2, 5).stream().anyMatch(line -> line.contains("LEADER")), status.out());
			assertTrue(lines.subList(2, 5).stream().noneMatch(line -> line.contains("unreachable")), status.out());
		}
	}

	/**
	 * R28-1: {@code admin checksum} and {@code backup} propose through the first member that takes the request, but a
	 * member listed first that accepts connections and never answers (a frozen JVM, SIGSTOP) no longer holds the proposal
	 * for the whole admin time limit: it is asked whether it answers at all, for a short time, and skipped. The healthy
	 * members get the proposal and give their results; the silent one has none, so both end incomplete (exit 4).
	 * Before, the proposal waited on it until the time limit and then failed, with no member asked.
	 */
	@Test
	void aSilentMemberListedFirstDoesNotHoldAProposalForTheTimeLimit() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("silent")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		// Every member applied the cluster version: the proposal goes to the member that answers first, and one that
		// has not applied it yet refuses backup and checksum.
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.BACKUP)));
		try (var silent = new java.net.ServerSocket(0)) {
			var args = new ArrayList<String>(List.of("admin",
					"--peer", "d1=127.0.0.1:" + silent.getLocalPort() + "/127.0.0.1:" + silent.getLocalPort()));
			for (var id : List.of("n0", "n1", "n2")) {
				args.add("--peer");
				args.add(cluster.config(id).self().format());
			}
			args.addAll(List.of("--timeout-ms", "15000"));

			var backup = run(with(args, "backup", "b2"));
			assertTrue(backup.out().contains("backup b2 at log index"), backup.toString());
			for (var id : List.of("n0", "n1", "n2")) {
				assertTrue(backup.out().contains(id + ": DONE"), backup.toString());
			}
			assertTrue(backup.out().contains("d1: no result within the time limit"), backup.toString());
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, backup.code(), backup.toString());

			var checksum = run(with(args, "checksum"));
			assertTrue(checksum.out().contains("checksum at log index"), checksum.toString());
			for (var id : List.of("n0", "n1", "n2")) {
				assertTrue(checksum.out().contains(id + ": DONE"), checksum.toString());
			}
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, checksum.code(), checksum.toString());

			// R28 review 1: with a time limit no longer than the question's own 3 s, the silent member asked first
			// used up the whole limit, and the command ended "no member took the request" without asking the healthy
			// ones; it also ran up to 3 s past the limit. The members are asked at once, within the time left, for the
			// proposal and (R29-3) for the results. The bound is the limit plus 2 s, room for the admin's own setup and
			// the channels' closing on a loaded host (R29 review 3); the old code overran by the silent member's 3 s.
			var shortLimit = new ArrayList<>(args.subList(0, args.size() - 1));
			shortLimit.add("2500");
			long start = System.nanoTime();
			var shortBackup = run(with(shortLimit, "backup", "b4"));
			long backupMillis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(shortBackup.out().contains("backup b4 at log index"), shortBackup.toString());
			assertTrue(backupMillis < 4_500, "backup took " + backupMillis + " ms with --timeout-ms 2500");
			start = System.nanoTime();
			var shortChecksum = run(with(shortLimit, "checksum"));
			long checksumMillis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(shortChecksum.out().contains("checksum at log index"), shortChecksum.toString());
			assertTrue(checksumMillis < 4_500, "checksum took " + checksumMillis + " ms with --timeout-ms 2500");
		}
	}

	/**
	 * R28 review 2: a proposal that a member took waits for that member's answer as long as the admin time limit allows
	 * when it is not idempotent ({@code checksum}): cutting it short leaves open whether the entry commits (exit 4, and
	 * "run it again" would start a second full-database digest), and the member bounds it itself with its own
	 * {@code --request-timeout-ms}, which the command does not know. Only an idempotent one ({@code backup}, by its
	 * request id) is cut after {@link ReplicatedServerMain#PROPOSE_ANSWER_TIMEOUT} and goes to the next member.
	 */
	@Test
	void onlyAnIdempotentProposalIsCutShortPerMember() {
		var hour = Duration.ofHours(1);
		assertEquals(hour, ReplicatedServerMain.proposalWait(false, hour));
		assertEquals(ReplicatedServerMain.PROPOSE_ANSWER_TIMEOUT, ReplicatedServerMain.proposalWait(true, hour));
		var second = Duration.ofSeconds(1);
		assertEquals(second, ReplicatedServerMain.proposalWait(true, second));
		assertEquals(second, ReplicatedServerMain.proposalWait(false, second));
	}

	/**
	 * R28-2: a {@code checksum} or {@code backup} that no member took within the time limit compared or wrote nothing:
	 * it is incomplete (exit 4, "run it again"), not a divergence or a failed backup (exit 1). Before, both exited with
	 * 1, and checksum's answer from a silent member ("does not tell whether the operation was proposed") did too.
	 */
	@Test
	void aProposalThatNoMemberTookIsIncomplete() throws Exception {
		int closedPort;
		try (var probe = new java.net.ServerSocket(0)) {
			closedPort = probe.getLocalPort();
		}
		try (var silent = new java.net.ServerSocket(0)) {
			var args = List.of("admin",
					"--peer", "d1=127.0.0.1:" + silent.getLocalPort() + "/127.0.0.1:" + silent.getLocalPort(),
					"--peer", "d2=127.0.0.1:" + closedPort + "/127.0.0.1:" + closedPort,
					"--timeout-ms", "5000");
			var checksum = run(with(args, "checksum"));
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, checksum.code(), checksum.toString());
			assertTrue(checksum.out().contains("FAILED: "), checksum.toString());
			var backup = run(with(args, "backup", "b3"));
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, backup.code(), backup.toString());
			assertTrue(backup.out().contains("FAILED: "), backup.toString());
		}
	}

	/**
	 * R28-3: {@code admin} refuses an option it does not know (a misspelled {@code --timout-ms} ran with the 10-minute
	 * default, and swallowed the next word) and an option of another command ({@code --allow-skipped} is checksum's,
 * {@code --unsafe-multi-voter-change} add-peer's and remove-peer's), and
	 * a malformed value ({@code --timeout-ms}, {@code --peer}, {@code --group-id}, the index of backup-status) is wrong
	 * usage too: exit 2 with the usage, not a stack trace with exit 1.
	 */
	@Test
	void adminRefusesUnknownOptionsAndMalformedValuesWithTwo() {
		var peer = List.of("admin", "--peer", "d1=127.0.0.1:1/127.0.0.1:1");
		var misspelled = run(with(peer, "--timout-ms", "3000", "status"));
		assertEquals(2, misspelled.code(), misspelled.toString());
		assertTrue(misspelled.err().contains("unknown option --timout-ms"), misspelled.err());
		var otherCommands = run(with(peer, "--allow-skipped", "status"));
		assertEquals(2, otherCommands.code(), otherCommands.toString());
		assertTrue(otherCommands.err().contains("--allow-skipped"), otherCommands.err());
		// R28 review 5: add-peer's and remove-peer's flag is refused with the other commands too.
		var unsafeElsewhere = run(with(peer, "--unsafe-multi-voter-change", "status"));
		assertEquals(2, unsafeElsewhere.code(), unsafeElsewhere.toString());
		assertTrue(unsafeElsewhere.err().contains("--unsafe-multi-voter-change"), unsafeElsewhere.err());
		var badTimeout = run(with(peer, "--timeout-ms", "soon", "status"));
		assertEquals(2, badTimeout.code(), badTimeout.toString());
		var badGroup = run(with(peer, "--group-id", "nope", "status"));
		assertEquals(2, badGroup.code(), badGroup.toString());
		var badPeer = run("admin", "--peer", "d1=nowhere", "status");
		assertEquals(2, badPeer.code(), badPeer.toString());
		var badIndex = run(with(peer, "--timeout-ms", "1000",
				"backup-status", "b1", "latest"));
		assertEquals(2, badIndex.code(), badIndex.toString());
		for (var refused : List.of(misspelled, otherCommands, unsafeElsewhere, badTimeout, badGroup, badPeer, badIndex)) {
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Usage:"), refused.err());
		}
	}

	/**
	 * R28-L1: {@code admin status} asks the members at once (R27-1). Four members listed first that accept connections
	 * and never answer cost one question time (3 s) together; asked one after another, as before R27-1, they cost about
	 * 2 s each, so the healthy members listed after them came late.
	 */
	@Test
	void statusDoesNotAskSilentMembersOneAfterAnother() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("status4")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var silent = new ArrayList<java.net.ServerSocket>();
		try {
			var args = new ArrayList<String>(List.of("admin"));
			for (int i = 1; i <= 4; i++) {
				var socket = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
				silent.add(socket);
				args.add("--peer");
				args.add("d" + i + "=127.0.0.1:" + socket.getLocalPort() + "/127.0.0.1:" + socket.getLocalPort());
			}
			for (var id : List.of("n0", "n1", "n2")) {
				args.add("--peer");
				args.add(cluster.config(id).self().format());
			}
			args.addAll(List.of("--timeout-ms", "60000", "status"));
			long start = System.nanoTime();
			var status = run(args.toArray(String[]::new));
			long tookMs = (System.nanoTime() - start) / 1_000_000;
			System.out.println("status took " + tookMs + " ms");
			assertEquals(0, status.code(), status.toString());
			var lines = status.out().strip().lines().filter(line -> line.matches("^[dn][0-9]: .*")).toList();
			assertEquals(7, lines.size(), status.out());
			for (int i = 0; i < 4; i++) {
				assertTrue(lines.get(i).startsWith("d" + (i + 1) + ": unreachable"), status.out());
			}
			assertTrue(lines.subList(4, 7).stream().anyMatch(line -> line.contains("LEADER")), status.out());
			// One question time (3 s) and the closing of the client, well under 4 x 2 s.
			assertTrue(tookMs < 6_000, "status took " + tookMs + " ms");
		} finally {
			for (var socket : silent) {
				socket.close();
			}
		}
	}

	/**
	 * R29-3: after the proposal, {@code checksum} and {@code backup} ask every member for its result at once, each
	 * within the time left, so silent members listed first (frozen with their ports open) no longer cost 3 s each per
	 * round, one after another, and the command ends within {@code --timeout-ms} (plus the closing). Before, three of
	 * them made a 4 s command take more than 9 s.
	 */
	@Test
	void resultsAreAskedOfAllMembersAtOnceWithinTheTimeLimit() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("results")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.BACKUP)));
		var silent = new ArrayList<java.net.ServerSocket>();
		try {
			var args = new ArrayList<String>(List.of("admin"));
			for (int i = 1; i <= 3; i++) {
				var socket = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
				silent.add(socket);
				args.add("--peer");
				args.add("d" + i + "=127.0.0.1:" + socket.getLocalPort() + "/127.0.0.1:" + socket.getLocalPort());
			}
			for (var id : List.of("n0", "n1", "n2")) {
				args.add("--peer");
				args.add(cluster.config(id).self().format());
			}
			args.addAll(List.of("--timeout-ms", "4000"));
			for (var command : List.of(List.of("backup", "b6"), List.of("checksum"))) {
				long start = System.nanoTime();
				var result = run(with(args, command.toArray(String[]::new)));
				long tookMs = (System.nanoTime() - start) / 1_000_000;
				assertTrue(result.out().contains(" at log index "), result.toString());
				for (var id : List.of("n0", "n1", "n2")) {
					assertTrue(result.out().contains(id + ": DONE"), result.toString());
				}
				for (var id : List.of("d1", "d2", "d3")) {
					assertTrue(result.out().contains(id + ": no result within the time limit"), result.toString());
				}
				assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, result.code(), result.toString());
				// The limit plus 2 s (R29 review 3): the old code took more than 9 s, 3 s per silent member.
				assertTrue(tookMs < 6_000, command + " took " + tookMs + " ms with --timeout-ms 4000");
			}
		} finally {
			for (var socket : silent) {
				socket.close();
			}
		}
	}

	/**
	 * A member that answers the capabilities question at once and then never answers anything else, as a leader on a
	 * hung disk would: it keeps what it was asked.
	 */
	private static final class HangingMember implements AutoCloseable {
		final List<byte[]> held = new CopyOnWriteArrayList<>();
		final Server server;

		HangingMember() throws IOException {
			ServerCalls.UnaryMethod<byte[], byte[]> ops = (request, response) -> {
				// A 16-byte group id, then the operation byte and its payload.
				var rest = java.util.Arrays.copyOfRange(request, 16, request.length);
				if (rest.length > 0 && rest[0] == ReplicaOps.OP_CAPABILITIES) {
					response.onNext(ByteBuffer.allocate(2 * Long.BYTES).putLong(Capabilities.SUPPORTED)
							.putLong(Capabilities.SUPPORTED).array());
					response.onCompleted();
				} else {
					held.add(rest);
				}
			};
			server = NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0))
					.addService(ServerServiceDefinition.builder(TermService.SERVICE_NAME)
							.addMethod(TermService.OPS, ServerCalls.asyncUnaryCall(ops)).build())
					.build()
					.start();
		}

		String peer(String id) {
			return id + "=127.0.0.1:" + server.getPort() + "/127.0.0.1:" + server.getPort();
		}

		long count(byte op) {
			return held.stream().filter(request -> request[0] == op).count();
		}

		@Override
		public void close() throws InterruptedException {
			server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	/**
	 * A member that answers the capabilities question after {@code delay} (so that the real members take the
	 * proposal) and answers every other question with a byte that names no result state, as a member of another or
	 * mismatched version might.
	 */
	private static final class GarbledMember implements AutoCloseable {
		final Server server;

		GarbledMember(Duration delay) throws IOException {
			ServerCalls.UnaryMethod<byte[], byte[]> ops = (request, response) -> {
				var rest = java.util.Arrays.copyOfRange(request, 16, request.length);
				if (rest.length > 0 && rest[0] == ReplicaOps.OP_CAPABILITIES) {
					try {
						Thread.sleep(delay);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					response.onNext(ByteBuffer.allocate(2 * Long.BYTES).putLong(Capabilities.SUPPORTED)
							.putLong(Capabilities.SUPPORTED).array());
				} else {
					response.onNext(new byte[] {Byte.MAX_VALUE});
				}
				response.onCompleted();
			};
			server = NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0))
					.addService(ServerServiceDefinition.builder(TermService.SERVICE_NAME)
							.addMethod(TermService.OPS, ServerCalls.asyncUnaryCall(ops)).build())
					.build()
					.start();
		}

		String peer(String id) {
			return id + "=127.0.0.1:" + server.getPort() + "/127.0.0.1:" + server.getPort();
		}

		@Override
		public void close() throws InterruptedException {
			server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	/**
	 * R29 review 1: an answer to the result question that cannot be read (here a state byte that names no state) is
	 * that member's failure, printed with the error, and the command fails with 1: asking again would not help. Before,
	 * the decoding error ended that member's question silently, and the command printed "no result within the time
	 * limit" and exited 4 ("run it again").
	 */
	@Test
	void anAnswerThatCannotBeReadIsThatMembersFailure() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("garbled")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.BACKUP)));
		try (var garbled = new GarbledMember(Duration.ofMillis(500))) {
			var args = new ArrayList<String>(List.of("admin", "--peer", garbled.peer("g1")));
			for (var id : List.of("n0", "n1", "n2")) {
				args.add("--peer");
				args.add(cluster.config(id).self().format());
			}
			args.addAll(List.of("--timeout-ms", "15000"));
			for (var command : List.of(List.of("backup", "b8"), List.of("checksum"))) {
				var result = run(with(args, command.toArray(String[]::new)));
				assertTrue(result.out().contains(" at log index "), result.toString());
				for (var id : List.of("n0", "n1", "n2")) {
					assertTrue(result.out().contains(id + ": DONE"), result.toString());
				}
				assertTrue(result.out().contains("g1: cannot read its answer (java.lang.ArrayIndexOutOfBoundsException"),
						result.toString());
				assertTrue(result.out().contains("FAILED: cannot read the answer of [g1]"), result.toString());
				assertEquals(1, result.code(), result.toString());
			}
		}
	}

	/**
	 * R28-L2: a member that answers the capabilities question and then hangs on the proposal (a leader on a hung disk).
	 * The healthy members are reached through links with 400 ms of latency each way, so the hanging member answers the
	 * question first and gets the proposal. {@code backup} (idempotent, by its request id) waits for it only
	 * {@link ReplicatedServerMain#proposeAnswerTimeout} (30 s, shortened here) and then proposes the same request through
	 * the next member: one backup, with the request id the hanging member was sent. {@code checksum} (not idempotent)
	 * waits for it as long as the time limit allows and ends incomplete within {@code --timeout-ms}, without proposing a
	 * second digest elsewhere.
	 */
	@Test
	void aMemberThatTookTheProposalAndHangsHoldsOnlyWhatItMay() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("hanging")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.BACKUP)));
		var previousWait = ReplicatedServerMain.proposeAnswerTimeout;
		ReplicatedServerMain.proposeAnswerTimeout = Duration.ofSeconds(2);
		var proxies = new ArrayList<LinkProxy>();
		try (var hanging = new HangingMember()) {
			var args = new ArrayList<String>(List.of("admin", "--peer", hanging.peer("h1")));
			for (var id : List.of("n0", "n1", "n2")) {
				var self = cluster.config(id).self();
				var proxy = new LinkProxy(new java.net.InetSocketAddress("127.0.0.1", 0),
						new java.net.InetSocketAddress(self.raftHost(), self.raftPort()));
				proxy.shape(LinkProxy.Shaping.latency(Duration.ofMillis(400), Duration.ZERO));
				proxies.add(proxy);
				args.add("--peer");
				args.add(id + "=127.0.0.1:" + proxy.listenAddress().getPort() + "/" + self.apiHost() + ":"
						+ self.apiPort());
			}

			var backupArgs = new ArrayList<>(args);
			backupArgs.addAll(List.of("--timeout-ms", "12000"));
			var backup = run(with(backupArgs, "backup", "b5"));
			assertEquals(1, hanging.count(ReplicaOps.OP_PROPOSE_BACKUP), backup.toString());
			assertTrue(backup.out().contains("backup b5 at log index"), backup.toString());
			for (var id : List.of("n0", "n1", "n2")) {
				assertTrue(backup.out().contains(id + ": DONE"), backup.toString());
			}
			assertTrue(backup.out().contains("h1: no result within the time limit"), backup.toString());
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, backup.code(), backup.toString());
			var sent = hanging.held.stream().filter(request -> request[0] == ReplicaOps.OP_PROPOSE_BACKUP).findFirst()
					.orElseThrow();
			long requestId = ByteBuffer.wrap(sent, 1, Long.BYTES).getLong();
			for (var id : List.of("n0", "n1", "n2")) {
				var properties = ReplicaOps.readBackupProperties(
						ReplicaOps.backupsDir(cluster.config(id).dataDir()).resolve("b5"));
				assertEquals(String.valueOf(requestId), properties.getProperty("request"), id);
			}

			var checksumArgs = new ArrayList<>(args);
			checksumArgs.addAll(List.of("--timeout-ms", "4000"));
			long start = System.nanoTime();
			var checksum = run(with(checksumArgs, "checksum"));
			long tookMs = (System.nanoTime() - start) / 1_000_000;
			assertEquals(1, hanging.count(ReplicaOps.OP_PROPOSE_CHECKSUM), checksum.toString());
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, checksum.code(), checksum.toString());
			assertTrue(checksum.out().contains("does not tell whether the operation was proposed"), checksum.toString());
			assertTrue(!checksum.out().contains("checksum at log index"), checksum.toString());
			assertTrue(tookMs >= 3_000 && tookMs < 6_000, "checksum took " + tookMs + " ms with --timeout-ms 4000");
		} finally {
			ReplicatedServerMain.proposeAnswerTimeout = previousWait;
			for (var proxy : proxies) {
				proxy.close();
			}
		}
	}

	/**
	 * R29-4: a word that no command expects is wrong usage (exit 2), for every command: before, {@code run},
	 * {@code repair-log}, {@code export-seed} and {@code restore-backup} ignored it, so a flag given a value
	 * ({@code --dry-run yes}, {@code --join false}) turned the flag on, and {@code admin status} took any word after it.
	 * A malformed argument of {@code add-peer} or {@code remove-peer} is wrong usage too, not a stack trace with 1.
	 */
	@Test
	void unexpectedWordsAndMalformedPeerArgumentsExitWithTwo() {
		var admin = List.of("admin", "--peer", "d1=127.0.0.1:1/127.0.0.1:1", "--timeout-ms", "1000");
		var missing = temp.resolve("missing").toString();
		var runs = List.of(
				run(with(admin, "status", "now")),
				run(with(admin, "add-peer", "n4=10.0.0.4:9001/10.0.0.4:53x3")),
				run(with(admin, "add-peer", "n4")),
				run(with(admin, "remove-peer", "n4=10.0.0.4:9001/10.0.0.4:9002")),
				run("repair-log", "--data-dir", missing, "--dry-run", "yes"),
				run("export-seed", "--data-dir", missing, "--target", temp.resolve("seed").toString(), "now"),
				run("restore-backup", "--backup", missing, "--data-dir", temp.resolve("target").toString(), "again"));
		for (var refused : runs) {
			assertEquals(2, refused.code(), refused.toString());
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Usage:"), refused.err());
		}
		assertTrue(runs.get(4).err().contains("--dry-run takes no value"), runs.get(4).err());
		assertTrue(Files.notExists(temp.resolve("seed")) && Files.notExists(temp.resolve("target")));
	}

	@Test
	void noCommandAndAnUnknownCommandExitWithTwo() {
		assertEquals(2, run().code());
		var unknown = run("serve");
		assertEquals(2, unknown.code());
		assertTrue(unknown.err().contains("unknown command serve"), unknown.err());
	}

	/**
	 * {@code export-seed} and {@code restore-backup} refuse a newer cluster's database or backup with exit code 5, like
	 * {@code run} refuses one copied in place, and leave no seed and no database behind (R18-1).
	 */
	@Test
	void exportSeedAndRestoreBackupOfANewerClusterExitWithFive() throws IOException {
		Path dataDir = temp.resolve("newer");
		NodeStoreTest.writeDatabaseOfVersion(dataDir.resolve("db"), NodeStoreTest.NEWER_VERSION);
		Path seed = temp.resolve("seed");
		var export = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(5, export.code(), export.toString());
		assertTrue(export.err().contains("0x10000000000"), export.err());
		assertTrue(Files.notExists(seed));

		Path backup = temp.resolve("backup");
		try (var store = NodeStore.open(dataDir.resolve("db"), null, "newer")) {
			store.checkpoint(backup.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backup.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Path target = temp.resolve("target");
		var restore = run("restore-backup", "--backup", backup.toString(), "--data-dir", target.toString());
		assertEquals(5, restore.code(), restore.toString());
		assertTrue(restore.out().startsWith("FAILED: ") && restore.out().contains("0x10000000000"), restore.out());
		assertTrue(Files.notExists(target.resolve("db")));
	}

	private static String[] with(List<String> base, String... more) {
		var all = new ArrayList<>(base);
		all.addAll(List.of(more));
		return all.toArray(String[]::new);
	}

	private static Keys key(long i) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array()));
	}

	private static Buf value(String text) {
		return Buf.wrap(text.getBytes(StandardCharsets.UTF_8));
	}
}
