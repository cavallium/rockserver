package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.IntConsumer;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
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
@Tag("slow")
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
		// R32-3: the puts commit on a majority, so the third member may not have applied the genesis yet; the test
		// checks the commands, not a lagging member's refusal (see aLaggingMemberThatAnswersFirstDoesNotStopTheCommand).
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.CHECKSUM | Capabilities.BACKUP)));
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
	 * R51-1: {@code add-peer} and {@code remove-peer} write the {@code --peer} address of every member that stays into the
	 * new configuration. One that differs from the member's address in the leader's configuration (a mistyped port) is
	 * refused with exit code 1 before anything is proposed, naming the member and both addresses; the configuration
	 * keeps the old address. With {@code --change-addresses} the same call goes through and writes the new address (here
	 * another spelling of the same host, which the running nodes keep reaching).
	 */
	@Test
	void adminRefusesAStayingMembersAddressThatDiffersFromTheConfiguration() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("addresses")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var lost = cluster.anyFollower().config().nodeId();
		var survivors = new ArrayList<>(List.of("n0", "n1", "n2"));
		survivors.remove(lost);
		cluster.stop(lost);
		var staying = survivors.getLast();
		var self = cluster.config(staying).self();
		var mistyped = new PeerConfig(staying, self.raftHost(), self.raftPort() + 1, self.apiHost(), self.apiPort());

		var refused = run(adminWith(List.of("n0", "n1", "n2"), staying, mistyped, "remove-peer", lost));
		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.out().contains("FAILED"), refused.out());
		assertTrue(refused.out().contains(staying + " (at " + self.raftAddress() + " in the configuration, "
				+ mistyped.raftAddress() + " in --peer)"), refused.out());
		assertTrue(refused.out().contains("--change-addresses"), refused.out());
		// R54-5: the advice matches the runbook: a move is under a new id, the flag never moves a member, and the
		// running nodes started with --join keep the old address until they are restarted one at a time.
		assertTrue(!refused.out().contains("which the nodes use once they restart"), refused.out());
		assertTrue(refused.out().contains("it never moves a member"), refused.out());
		assertTrue(refused.out().contains("every running node started with --join keeps the old address"),
				refused.out());
		assertVoters(List.of("n0", "n1", "n2"));
		assertEquals(self.raftAddress(), voterAddress(staying));

		var respelled = new PeerConfig(staying, "localhost", self.raftPort(), self.apiHost(), self.apiPort());
		var changed = run(with(List.of(adminWith(List.of("n0", "n1", "n2"), staying, respelled, "remove-peer", lost)),
				"--change-addresses"));
		assertEquals(0, changed.code(), changed.toString());
		assertTrue(changed.out().strip().endsWith("OK"), changed.out());
		assertTrue(changed.out().contains("--change-addresses: this change also moves " + staying + " ("), changed.out());
		assertTrue(changed.out().contains("this fixes the record only: once it prints OK, restart every running node"
				+ " started with --join, one at a time"), changed.out());
		assertVoters(survivors);
		assertEquals(respelled.raftAddress(), voterAddress(staying));

		var misplaced = run(with(List.of(admin(survivors, "status")), "--change-addresses"));
		assertEquals(2, misplaced.code(), misplaced.toString());
	}

	/**
	 * Review of R54, 3: Ratis answers a snapshot request with success and the latest snapshot's index, 0 when there is
	 * none, on a member that applied less than the creation gap past it: on a member that applied nothing past index 0
	 * that is no snapshot, as is -1 (R54-3); a positive index is one.
	 */
	@Test
	void aSnapshotReplyAtIndexZeroOrBelowIsNoSnapshot() {
		assertEquals(ReplicatedServerMain.NO_SNAPSHOT_REASONS, ReplicatedServerMain.noSnapshotReason(-1));
		assertEquals(ReplicatedServerMain.NOTHING_APPLIED_REASON, ReplicatedServerMain.noSnapshotReason(0));
		assertEquals(null, ReplicatedServerMain.noSnapshotReason(1));
		assertEquals(null, ReplicatedServerMain.noSnapshotReason(42));
	}

	/**
	 * R54-3: {@code admin snapshot} reports a member that took no snapshot (a reply with log index -1: here its state
	 * machine paused, as at the end of a snapshot install; also its retry backoff, or a database past what Ratis handed
	 * over after a start) as {@code FAILED}, exit code 1, instead of {@code OK}; once the member takes one, it prints its
	 * log index and {@code OK}. R54-4: {@code status} prints each member's applied index, which the restart of a node
	 * started with {@code --join} after {@code --change-addresses} waits for.
	 */
	@Test
	void adminSnapshotFailsWhenTheMemberTakesNoneAndStatusShowsTheAppliedIndex() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("snapshot")), 1,
				c -> c.withSnapshotEveryEntries(1_000_000)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), value("v1"), RequestType.none());
		var stateMachine = leader.stateMachine();
		InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= stateMachine.appliedIndex().get());
		var id = leader.config().nodeId();

		stateMachine.pause();
		try {
			var none = run(admin(List.of(id), "snapshot", id));
			assertEquals(1, none.code(), none.toString());
			assertTrue(none.out().contains("FAILED: " + id + " took no snapshot"), none.out());
			assertTrue(!none.out().contains("OK"), none.out());
		} finally {
			// As reinitialize does after an install.
			stateMachine.getLifeCycle().startAndTransition(() -> {
			});
		}
		var taken = run(admin(List.of(id), "snapshot", id));
		assertEquals(0, taken.code(), taken.toString());
		var latest = stateMachine.getLatestSnapshot();
		assertTrue(latest != null, "a snapshot was taken");
		assertTrue(taken.out().contains(id + " took a snapshot at log index " + latest.getIndex()), taken.out());
		assertTrue(taken.out().strip().endsWith("OK"), taken.out());

		var status = run(admin(List.of(id), "status"));
		assertEquals(0, status.code(), status.toString());
		assertTrue(status.out().contains(id + ": LEADER, applied index " + stateMachine.appliedIndex().get()),
				status.out());		// Not a terminal: the plain lines, for scripts. --pretty asks for the table.
		var table = run(with(List.of(admin(List.of(id), "status")), "--pretty"));
		assertEquals(0, table.code(), table.toString());
		assertTrue(table.out().startsWith("HEALTHY  leader " + id + ", 1 of 1 members answer"), table.out());
		assertFalse(table.out().contains("\u001b"), table.out());
	}

	/** R55-3: admin snapshot waits for the member's snapshot at most its own wait, and at most the time left. */
	@Test
	void theSnapshotWaitIsAtMostTheTimeLeft() {
		assertEquals(120_000, ReplicatedServerMain.snapshotWaitMillis(Duration.ofSeconds(120), Duration.ofMinutes(10).toNanos()));
		assertEquals(5_000, ReplicatedServerMain.snapshotWaitMillis(Duration.ofSeconds(120), Duration.ofSeconds(5).toNanos()));
		assertEquals(1, ReplicatedServerMain.snapshotWaitMillis(Duration.ofSeconds(120), -5));
	}

	/**
	 * Review of R55, 1: only a request that ran out of time or of retries (the member's own timeout, or the client's) gets the text
	 * of a wait that ran out; an exception the blocking client rethrows at once (a member of another group) says that
	 * the member refused, without a wait and without a failed snapshot.
	 */
	@Test
	void aSnapshotRequestRefusedAtOnceIsNotCalledATimeout() {
		var refused = ReplicatedServerMain.snapshotNotTaken("n1", 3000,
				new org.apache.ratis.protocol.exceptions.GroupMismatchException("n1: group-AAAA not found"));
		assertTrue(refused.startsWith("n1 took no snapshot: it refused the request"), refused);
		assertTrue(refused.contains("group-AAAA not found"), refused);
		assertTrue(!refused.contains("within"), refused);
		assertTrue(!refused.contains("snapshot failed"), refused);

		var timedOut = ReplicatedServerMain.snapshotNotTaken("n1", 3000,
				new org.apache.ratis.protocol.exceptions.TimeoutIOException("Failed to take a snapshot within timeout"));
		assertTrue(timedOut.startsWith("n1 took no snapshot within 3000 ms: its snapshot failed"), timedOut);
		var wrapped = ReplicatedServerMain.snapshotNotTaken("n1", 3000, new IOException("wrapped",
				new org.apache.ratis.protocol.exceptions.TimeoutIOException("Failed to take a snapshot within timeout")));
		assertTrue(wrapped.startsWith("n1 took no snapshot within 3000 ms"), wrapped);
	}

	/**
	 * R55-3: a snapshot that fails on the member while {@code admin snapshot} waits (a full disk) ends the command soon,
	 * with a snapshot-specific {@code FAILED} line and exit code 1. Ratis 3.3.1 answers a request whose take threw only
	 * at the request's own timeout ({@code TimeoutIOException}): the command waited for the whole {@code --timeout-ms}
	 * (10 minutes by default) and then printed the membership-change advice of {@code attempt()}.
	 */
	@Test
	void adminSnapshotWhoseTakeFailsEndsSoonAndSaysSo() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("snapshot-fails")), 1,
				c -> c.withSnapshotEveryEntries(1_000_000)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), value("v1"), RequestType.none());
		var stateMachine = leader.stateMachine();
		InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= stateMachine.appliedIndex().get());
		var id = leader.config().nodeId();

		var wait = ReplicatedServerMain.snapshotRequestWait;
		ReplicatedServerMain.snapshotRequestWait = Duration.ofSeconds(3);
		stateMachine.snapshotFailureForTests = new java.io.IOException("No space left on device (injected)");
		try {
			long start = System.nanoTime();
			var failed = run(admin(List.of(id), "snapshot", id));
			var elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertEquals(1, failed.code(), failed.toString());
			assertTrue(failed.out().contains("FAILED: " + id + " took no snapshot within 3000 ms"), failed.out());
			assertTrue(failed.out().contains("see its log"), failed.out());
			assertTrue(!failed.out().contains("A leader that started a change"), failed.out());
			assertTrue(!failed.out().contains("OK"), failed.out());
			assertTrue(elapsed.compareTo(Duration.ofSeconds(20)) < 0, () -> "took " + elapsed + " with --timeout-ms 30000: "
					+ failed.out());
		} finally {
			stateMachine.snapshotFailureForTests = null;
			ReplicatedServerMain.snapshotRequestWait = wait;
		}
	}

	/**
	 * R56-1: a snapshot asked by the byte trigger ({@code --snapshot-every-bytes}) whose take fails (a full disk) leaves
	 * its request pending in Ratis 3.3.1 until the request's own timeout, and an {@code admin snapshot} in the meantime
	 * joins it and runs no take. With the byte trigger's 10-minute timeout, the operator who freed the disk and ran
	 * {@code admin snapshot}, as "Raft log grows" says, got {@code FAILED ... took no snapshot within} each time for
	 * up to 10 minutes. The byte trigger's request now ends soon (its shortest timeout is set to 2 s here), and the
	 * command's next try asks again and gets a take.
	 */
	@Test
	void adminSnapshotRunsATakeAfterAFailedTakeOfTheByteTrigger() throws Exception {
		var minTimeout = RockserverStateMachine.snapshotByBytesMinRequestTimeout;
		var wait = ReplicatedServerMain.snapshotRequestWait;
		RockserverStateMachine.snapshotByBytesMinRequestTimeout = Duration.ofSeconds(2);
		ReplicatedServerMain.snapshotRequestWait = Duration.ofSeconds(20);
		try {
			cluster = new InProcessCluster(Files.createDirectories(temp.resolve("bytes-snapshot-fails")), 1,
					c -> c.withSnapshotEveryEntries(1_000_000).withSnapshotEveryBytes(1 << 20)).startAll();
			var leader = cluster.awaitLeader(TIMEOUT);
			var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumn("c", SCHEMA);
			var stateMachine = leader.stateMachine();
			var id = leader.config().nodeId();
			stateMachine.snapshotFailureForTests = new IOException("No space left on device (injected)");
			var large = Buf.wrap(new byte[256 * 1024]);
			for (long i = 0; i < 8; i++) {
				api.put(0, column, key(i), large, RequestType.none());
			}
			// 2 MiB past a 1 MiB threshold: the byte trigger asked, and its take failed.
			InProcessCluster.await(TIMEOUT, () -> stateMachine.snapshotAttempts.get() >= 1);
			assertTrue(stateMachine.getLatestSnapshot() == null, "the take failed");
			// The disk is freed; past the first backoff (1 s).
			stateMachine.snapshotFailureForTests = null;
			Thread.sleep(1500);

			long start = System.nanoTime();
			var taken = run(admin(List.of(id), "snapshot", id));
			var elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertEquals(0, taken.code(), taken.toString());
			var latest = stateMachine.getLatestSnapshot();
			assertTrue(latest != null, "a snapshot was taken");
			assertTrue(taken.out().contains(id + " took a snapshot at log index " + latest.getIndex()), taken.out());
			assertTrue(elapsed.compareTo(Duration.ofSeconds(15)) < 0, () -> "took " + elapsed + ": " + taken.out());
		} finally {
			RockserverStateMachine.snapshotByBytesMinRequestTimeout = minTimeout;
			ReplicatedServerMain.snapshotRequestWait = wait;
		}
	}

	/**
	 * R56-2: Ratis 3.3.1 never cancels the timer that {@code takingSnapshotAsync} schedules for a snapshot request, and
	 * the handler's {@code timeout()} fails whatever request is pending when it fires. A timer left by an earlier request
	 * that finished in time (here one with a 3 s timeout, answered at once) therefore fails an {@code admin snapshot}
	 * whose take is still running at that moment (here a take of 2.5 s started 1.5 s after the earlier request). The
	 * command must not print {@code FAILED} for it: it asks again and reports the take. Review of R56, 5: the test checks
	 * that the earlier request's timer fired while the command's take ran and that the client sent its request again,
	 * so a drift of the timing (a take that starts late or ends before the timer) fails instead of passing untested.
	 */
	@Test
	void aTimerLeftByAnEarlierSnapshotRequestDoesNotFailALaterAdminSnapshot() throws Exception {
		var wait = ReplicatedServerMain.snapshotRequestWait;
		ReplicatedServerMain.snapshotRequestWait = Duration.ofSeconds(20);
		try {
			cluster = new InProcessCluster(Files.createDirectories(temp.resolve("stale-timer")), 1,
					c -> c.withSnapshotEveryEntries(1_000_000)).startAll();
			var leader = cluster.awaitLeader(TIMEOUT);
			var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumn("c", SCHEMA);
			api.put(0, column, key(1), value("v1"), RequestType.none());
			var stateMachine = leader.stateMachine();
			InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= stateMachine.appliedIndex().get());
			var id = leader.config().nodeId();
			var division = java.util.Objects.requireNonNull(stateMachine.division());

			long earlier = System.nanoTime();
			var first = division.getRaftServer().snapshotManagementAsync(
					org.apache.ratis.protocol.SnapshotManagementRequest.newCreate(org.apache.ratis.protocol.ClientId.randomId(),
							division.getId(), division.getGroup().getGroupId(), 1, 3000)).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			// The earlier request's timer was armed between these two moments, so it fires between them plus 3 s.
			long answered = System.nanoTime();
			assertTrue(first.isSuccess() && first.getLogIndex() >= 0, first.toString());
			api.put(0, column, key(2), value("v2"), RequestType.none());
			InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= stateMachine.appliedIndex().get());
			stateMachine.snapshotDelayForTests = 2500;
			long sleep = TimeUnit.NANOSECONDS.toMillis(earlier + TimeUnit.MILLISECONDS.toNanos(1500) - System.nanoTime());
			assertTrue(sleep > 0, "the earlier request took " + (1500 - sleep) + " ms");
			Thread.sleep(sleep);

			long attempts = stateMachine.snapshotAttempts.get();
			long retries = ReplicatedServerMain.adminRetriesForTests.get();
			var taken = run(admin(List.of(id), "snapshot", id));
			assertEquals(0, taken.code(), taken.toString());
			assertTrue(!taken.out().contains("FAILED"), taken.out());
			var latest = stateMachine.getLatestSnapshot();
			assertTrue(latest != null && latest.getIndex() > first.getLogIndex(), "a later snapshot was taken: " + latest);
			assertTrue(taken.out().contains(id + " took a snapshot at log index "), taken.out());
			assertEquals(attempts + 1, stateMachine.snapshotAttempts.get(), "one take, for the command");
			long fireFrom = earlier + TimeUnit.SECONDS.toNanos(3);
			long fireTo = answered + TimeUnit.SECONDS.toNanos(3);
			long takeStarted = stateMachine.takeStartedForTests;
			long takeEnded = stateMachine.takeEndedForTests;
			assertTrue(takeStarted - fireFrom < 0 && takeEnded - fireTo > 0, () -> "the earlier timer fired between "
					+ TimeUnit.NANOSECONDS.toMillis(fireFrom - earlier) + " and " + TimeUnit.NANOSECONDS.toMillis(fireTo - earlier)
					+ " ms, the take ran from " + TimeUnit.NANOSECONDS.toMillis(takeStarted - earlier) + " to "
					+ TimeUnit.NANOSECONDS.toMillis(takeEnded - earlier) + " ms");
			assertTrue(ReplicatedServerMain.adminRetriesForTests.get() > retries,
					"the timer cleared the command's request and the client sent it again");
		} finally {
			ReplicatedServerMain.snapshotRequestWait = wait;
		}
	}

	/**
	 * R57-1: Ratis 3.3.1's gRPC client gives each snapshot request a deadline of the client's {@code Rpc.requestTimeout}
	 * ({@code GrpcClientProtocolClient.snapshotManagement}), not the request's own timeout; {@code admin} set it to the
	 * election timeout, 2 s. A take longer than that ran past the first attempt, the client sent the request again 500 ms
	 * later, and a take that ended in that gap answered no one: Ratis cleared its pending request, and the next attempt
	 * was a new request that ran a second take, past the entries applied meanwhile (the creation gap is 1). Here the
	 * take is held until the client's first attempt ran out (or, if none does, 3 s), so it ends in that gap, while
	 * writes wait to be applied after it. The command must get the answer of its one take.
	 */
	@Test
	void aTakeLongerThanTheElectionTimeoutAnswersAdminSnapshotWithOneTake() throws Exception {
		var wait = ReplicatedServerMain.snapshotRequestWait;
		ReplicatedServerMain.snapshotRequestWait = Duration.ofSeconds(20);
		var writer = java.util.concurrent.Executors.newSingleThreadExecutor();
		RockserverStateMachine held = null;
		try {
			cluster = new InProcessCluster(Files.createDirectories(temp.resolve("long-take")), 1,
					c -> c.withSnapshotEveryEntries(1_000_000)).startAll();
			var leader = cluster.awaitLeader(TIMEOUT);
			var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumn("c", SCHEMA);
			api.put(0, column, key(1), value("v1"), RequestType.none());
			var stateMachine = leader.stateMachine();
			InProcessCluster.await(TIMEOUT, () -> stateMachine.handedOverForTesting() >= stateMachine.appliedIndex().get());
			var id = leader.config().nodeId();

			var hold = new java.util.concurrent.CountDownLatch(1);
			stateMachine.snapshotHoldForTests = hold;
			held = stateMachine;
			long attempts = stateMachine.snapshotAttempts.get();
			long retries = ReplicatedServerMain.adminRetriesForTests.get();
			var released = new java.util.concurrent.atomic.AtomicLong();
			var releaser = writer.submit(() -> {
				InProcessCluster.await(TIMEOUT, () -> stateMachine.snapshotAttempts.get() > attempts);
				long entered = System.nanoTime();
				// Entries that are applied after the take, so that a second request would run a second take.
				var puts = java.util.concurrent.CompletableFuture.runAsync(() -> {
					for (long i = 2; i < 6; i++) {
						api.put(0, column, key(i), value("v" + i), RequestType.none());
					}
				});
				InProcessCluster.await(TIMEOUT, () -> ReplicatedServerMain.adminRetriesForTests.get() > retries
						|| System.nanoTime() - entered > TimeUnit.SECONDS.toNanos(3));
				released.set(System.nanoTime() - entered);
				hold.countDown();
				puts.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
				return null;
			});
			var taken = run(admin(List.of(id), "snapshot", id));
			releaser.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			assertEquals(0, taken.code(), taken.toString());
			assertTrue(!taken.out().contains("FAILED"), taken.out());
			assertEquals(attempts + 1, stateMachine.snapshotAttempts.get(), () -> "one take, for the command (the hold was"
					+ " released " + TimeUnit.NANOSECONDS.toMillis(released.get()) + " ms after the take started, after "
					+ (ReplicatedServerMain.adminRetriesForTests.get() - retries) + " retries): " + taken.out());
			var latest = stateMachine.getLatestSnapshot();
			assertTrue(latest != null, "a snapshot was taken");
			assertTrue(taken.out().contains(id + " took a snapshot at log index " + latest.getIndex()), taken.out());
			assertEquals(retries, ReplicatedServerMain.adminRetriesForTests.get(), "one call lasted the take");
		} finally {
			if (held != null) {
				held.snapshotHoldForTests = null;
			}
			writer.shutdownNow();
			ReplicatedServerMain.snapshotRequestWait = wait;
		}
	}

	/**
	 * Review of R57, 1: {@code requestSnapshot} decided to send a retryable attempt again before its 500 ms sleep and
	 * did not look at the clock after it, so a retryable failure in the last 500 ms of the wait sent one more attempt
	 * after the wait had ended (its {@code left} clamped to 1 ms), which on a real member can start a take after the
	 * command has given up. Here a fake member holds the first snapshot request until 200 ms before the end of a 3 s
	 * wait and answers it {@code UNAVAILABLE} (a retryable transport error), and every later one at once: the command
	 * must end with {@code took no snapshot within} without sending a second request.
	 */
	@Test
	void noSnapshotRequestIsSentAfterTheWaitEnded() throws Exception {
		var wait = ReplicatedServerMain.snapshotRequestWait;
		ReplicatedServerMain.snapshotRequestWait = Duration.ofSeconds(20);
		long timeoutMs = 3000;
		var calls = new CopyOnWriteArrayList<Long>();
		var start = new java.util.concurrent.atomic.AtomicLong();
		var bytes = new org.apache.ratis.thirdparty.io.grpc.MethodDescriptor.Marshaller<byte[]>() {
			@Override
			public java.io.InputStream stream(byte[] value) {
				return new java.io.ByteArrayInputStream(value);
			}

			@Override
			public byte[] parse(java.io.InputStream stream) {
				try {
					return stream.readAllBytes();
				} catch (IOException ex) {
					throw new java.io.UncheckedIOException(ex);
				}
			}
		};
		ServerCalls.UnaryMethod<byte[], byte[]> refuse = (request, response) -> {
			calls.add(System.nanoTime());
			if (calls.size() == 1) {
				long until = start.get() + TimeUnit.MILLISECONDS.toNanos(timeoutMs - 200);
				try {
					Thread.sleep(Math.max(0, TimeUnit.NANOSECONDS.toMillis(until - System.nanoTime())));
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
			response.onError(Status.UNAVAILABLE.withDescription("not up yet").asRuntimeException());
		};
		var registry = new org.apache.ratis.thirdparty.io.grpc.HandlerRegistry() {
			@Override
			public org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition<?, ?> lookupMethod(String method,
					@Nullable String authority) {
				if (!method.endsWith("/snapshotManagement")) {
					return null;
				}
				return org.apache.ratis.thirdparty.io.grpc.ServerMethodDefinition.create(
						org.apache.ratis.thirdparty.io.grpc.MethodDescriptor.<byte[], byte[]>newBuilder()
								.setType(org.apache.ratis.thirdparty.io.grpc.MethodDescriptor.MethodType.UNARY)
								.setFullMethodName(method).setRequestMarshaller(bytes).setResponseMarshaller(bytes).build(),
						ServerCalls.asyncUnaryCall(refuse));
			}
		};
		var member = NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0))
				.fallbackHandlerRegistry(registry).build().start();
		try {
			start.set(System.nanoTime());
			var result = run("admin", "--peer", "f1=127.0.0.1:" + member.getPort() + "/127.0.0.1:" + member.getPort(),
					"--timeout-ms", Long.toString(timeoutMs), "snapshot", "f1");
			long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start.get());
			assertTrue(!calls.isEmpty(), result.toString());
			long firstMs = TimeUnit.NANOSECONDS.toMillis(calls.getFirst() - start.get());
			assertTrue(firstMs < timeoutMs - 400, "the first request came " + firstMs + " ms after the start: " + result);
			assertEquals(1, result.code(), result.toString());
			assertTrue(result.out().contains("f1 took no snapshot within"), result.toString());
			assertEquals(1, calls.size(), () -> "requests at " + calls.stream()
					.map(at -> TimeUnit.NANOSECONDS.toMillis(at - start.get()) + " ms").toList() + ", the wait " + timeoutMs
					+ " ms, the command took " + tookMs + " ms: " + result);
			assertTrue(tookMs < timeoutMs + 1500, "the command took " + tookMs + " ms");
		} finally {
			member.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
			ReplicatedServerMain.snapshotRequestWait = wait;
		}
	}

	/**
	 * R52-3: Ratis 3.3.1 takes a configuration with the same voter ids and priorities for no change
	 * ({@code RaftConfigurationImpl.hasNoChange} never compares addresses) and answers success without writing it. A
	 * call with {@code --change-addresses} that adds and removes nothing (here a re-run of a remove-peer) therefore
	 * fails with exit code 1 and says that nothing was written, instead of printing the move and {@code OK}.
	 */
	@Test
	void adminRefusesAnAddressChangeThatChangesNoVoter() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("address-only")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var staying = cluster.anyFollower().config().nodeId();
		var self = cluster.config(staying).self();
		var respelled = new PeerConfig(staying, "localhost", self.raftPort(), self.apiHost(), self.apiPort());

		var refused = run(with(List.of(adminWith(List.of("n0", "n1", "n2"), staying, respelled, "remove-peer", "n9")),
				"--change-addresses"));
		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.out().contains("FAILED"), refused.out());
		assertTrue(refused.out().contains("Nothing was written"), refused.out());
		assertTrue(!refused.out().strip().endsWith("OK"), refused.out());
		assertVoters(List.of("n0", "n1", "n2"));
		assertEquals(self.raftAddress(), voterAddress(staying));

		// Review of R52, 1: an add-peer of a voter already (a re-run) adds and removes no voter either. With
		// --change-addresses it is refused the same way, and without it the address of a member that stays is checked
		// (R51-1) instead of printing OK.
		var voter = "n0".equals(staying) ? "n1" : "n0";
		var again = cluster.config(voter).self().format();
		var addAgain = run(with(List.of(adminWith(List.of("n0", "n1", "n2"), staying, respelled, "add-peer", again)),
				"--change-addresses"));
		assertEquals(1, addAgain.code(), addAgain.toString());
		assertTrue(addAgain.out().contains("Nothing was written"), addAgain.out());
		assertTrue(!addAgain.out().strip().endsWith("OK"), addAgain.out());
		var addAgainChecked = run(adminWith(List.of("n0", "n1", "n2"), staying, respelled, "add-peer", again));
		assertEquals(1, addAgainChecked.code(), addAgainChecked.toString());
		assertTrue(addAgainChecked.out().contains(staying + " (at " + self.raftAddress() + " in the configuration, "
				+ respelled.raftAddress() + " in --peer)"), addAgainChecked.out());
		assertTrue(!addAgainChecked.out().strip().endsWith("OK"), addAgainChecked.out());
		assertEquals(self.raftAddress(), voterAddress(staying));
	}

	/**
	 * R59-1: {@code remove-peer ID} of an id that is not a voter of the leader's configuration says so instead of a bare
	 * {@code OK}. An id that differs from a voter only by case ({@code N1} for {@code n1}; Ratis compares ids
	 * case-sensitively) is refused with exit code 1 as a likely typo, before anything is proposed: it cannot be the re-run
	 * of a removal that succeeded unless the cluster holds ids that differ only by case. Any other id that is not a voter
	 * prints {@code ID is not a voter: nothing to remove} with the voters, then {@code OK} with exit code 0, because the
	 * tool cannot tell a mistyped id from the re-run of a removal that already committed (documented idempotence: the
	 * re-run of a call that succeeded prints OK again), in both the all-members and the {@code --peer <the others>} form.
	 */
	@Test
	void adminRemovePeerOfAnIdThatIsNotAVoterSaysSo() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("not-a-voter")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var all = List.of("n0", "n1", "n2");
		var removed = cluster.anyFollower().config().nodeId();

		var typo = run(admin(all, "remove-peer", removed.toUpperCase(Locale.ROOT)));
		assertEquals(1, typo.code(), typo.toString());
		assertTrue(typo.out().contains("FAILED"), typo.out());
		assertTrue(typo.out().contains(removed.toUpperCase(Locale.ROOT) + " is not a voter"), typo.out());
		assertTrue(typo.out().contains("likely a typo of " + removed), typo.out());
		assertTrue(typo.out().contains("Nothing was proposed"), typo.out());
		assertTrue(!typo.out().strip().endsWith("OK"), typo.out());
		// Review of R59, 2: changeVoters, the only caller of setConfiguration for remove-peer, prints this line before it
		// proposes; assertVoters alone cannot tell a no-op proposal (Ratis writes nothing for the same voters) from none.
		assertTrue(!typo.out().contains("this change adds"), typo.out());
		assertVoters(all);

		var unknown = run(admin(all, "remove-peer", "n9"));
		assertEquals(0, unknown.code(), unknown.toString());
		assertTrue(unknown.out().contains("n9 is not a voter: nothing to remove (voters [n0, n1, n2])"), unknown.out());
		assertTrue(unknown.out().strip().endsWith("OK"), unknown.out());
		assertTrue(!unknown.out().contains("this change adds"), unknown.out());
		assertVoters(all);

		var remove = run(admin(all, "remove-peer", removed));
		assertEquals(0, remove.code(), remove.toString());
		assertTrue(!remove.out().contains("is not a voter"), remove.out());
		// Review of R59, 1: the removing call prints the voters read before its proposal, the removed id among them.
		assertTrue(remove.out().contains("voters " + all + "; this change adds [] and removes [" + removed + "]"),
				remove.out());
		assertTrue(remove.out().strip().endsWith("OK"), remove.out());
		var survivors = new ArrayList<>(all);
		survivors.remove(removed);
		assertVoters(survivors);

		// Re-runs of the removal that succeeded, as after a timeout: still OK, and the output says nothing was removed.
		for (var peerIds : List.of(all, survivors)) {
			var again = run(admin(peerIds, "remove-peer", removed));
			assertEquals(0, again.code(), again.toString());
			assertTrue(again.out().contains(removed + " is not a voter: nothing to remove (voters " + survivors + ")"),
					again.out());
			assertTrue(again.out().strip().endsWith("OK"), again.out());
			assertTrue(!again.out().contains("this change adds"), again.out());
			assertVoters(survivors);
		}
	}

	/**
	 * Review of R59, 2: a {@code --peer} that misses a voter, with {@code remove-peer} of an id that is not a voter,
	 * proposes nothing. Before R59-1 the target was {@code --peer} minus the id, so the call proposed removing the voter
	 * missing from {@code --peer} (one voter, a safe change) and printed OK.
	 */
	@Test
	void adminRemovePeerOfANonVoterWithAVoterMissingFromPeerProposesNothing() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("not-a-voter-short-peer")), 3, c -> c)
				.startAll();
		cluster.awaitLeader(TIMEOUT);
		var all = List.of("n0", "n1", "n2");
		var missing = cluster.anyFollower().config().nodeId();
		var given = new ArrayList<>(all);
		given.remove(missing);

		var result = run(admin(given, "remove-peer", "n9"));
		assertEquals(0, result.code(), result.toString());
		assertTrue(result.out().contains("n9 is not a voter: nothing to remove (voters " + all + ")"), result.out());
		assertTrue(!result.out().contains("this change adds"), result.out());
		assertTrue(result.out().strip().endsWith("OK"), result.out());
		assertVoters(all);
	}

	/** {@link #admin} with member {@code replaced} given as {@code peer} instead of its own address. */
	private String[] adminWith(List<String> peerIds, String replaced, PeerConfig peer, String... command) {
		var args = new ArrayList<>(List.of(admin(peerIds, command)));
		args.set(args.indexOf(cluster.config(replaced).self().format()), peer.format());
		return args.toArray(String[]::new);
	}

	/** The address of voter {@code id} in the leader's configuration. */
	private String voterAddress(String id) {
		return leader().stateMachine().division().getRaftConf().getCurrentPeers().stream()
				.filter(peer -> peer.getId().toString().equals(id)).findFirst().orElseThrow().getAddress();
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
	 * R58-2: {@code admin snapshot ID} of an id that is not one of {@code --peer} (a member outside the list, or a
	 * different case) is wrong usage: exit 2 with the usage and the members, before anything is sent. Ratis' client
	 * looked the id up in the group and threw an uncaught NullPointerException (exit 1, the code of a member whose
	 * snapshot failed).
	 */
	@Test
	void adminSnapshotOfAnIdOutsidePeerIsWrongUsage() {
		var peers = List.of("admin", "--peer", "d1=127.0.0.1:1/127.0.0.1:1", "--peer", "d2=127.0.0.1:2/127.0.0.1:2",
				"--timeout-ms", "3000");
		for (var id : List.of("n9", "D1")) {
			var refused = run(with(peers, "snapshot", id));
			assertEquals(2, refused.code(), refused.toString());
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Usage:"), refused.err());
			assertTrue(refused.err().contains(id + " is not one of --peer (d1, d2)"), refused.err());
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
		// R61-9: the problem line, not the option's name, which the usage text printed after it always holds.
		assertTrue(otherCommands.err().contains("Error: --allow-skipped is an option of checksum only"),
				otherCommands.err());
		// R28 review 5: add-peer's and remove-peer's flag is refused with the other commands too.
		var unsafeElsewhere = run(with(peer, "--unsafe-multi-voter-change", "status"));
		assertEquals(2, unsafeElsewhere.code(), unsafeElsewhere.toString());
		assertTrue(unsafeElsewhere.err().contains(
				"Error: --unsafe-multi-voter-change is an option of add-peer and remove-peer only"), unsafeElsewhere.err());
		var badTimeout = run(with(peer, "--timeout-ms", "soon", "status"));
		assertEquals(2, badTimeout.code(), badTimeout.toString());
		var badGroup = run(with(peer, "--group-id", "nope", "status"));
		assertEquals(2, badGroup.code(), badGroup.toString());
		var badPeer = run("admin", "--peer", "d1=nowhere", "status");
		assertEquals(2, badPeer.code(), badPeer.toString());
		var badIndex = run(with(peer, "--timeout-ms", "1000",
				"backup-status", "b1", "latest"));
		assertEquals(2, badIndex.code(), badIndex.toString());
		// R60-6: a repeated --peer id and a --timeout-ms beyond the nanoseconds of a long are usage errors too, as with
		// run; they ended with exit 1 and a stack trace (RaftGroup.valueOf's "Found duplicated element", toNanos).
		var repeatedPeer = run("admin", "--peer", "d1=127.0.0.1:1/127.0.0.1:1", "--peer", "d1=127.0.0.1:2/127.0.0.1:2",
				"status");
		assertEquals(2, repeatedPeer.code(), repeatedPeer.toString());
		assertTrue(repeatedPeer.err().contains("d1"), repeatedPeer.err());
		var hugeTimeout = run(with(peer, "--timeout-ms", "9300000000000", "status"));
		assertEquals(2, hugeTimeout.code(), hugeTimeout.toString());
		assertTrue(hugeTimeout.err().contains("Error: invalid --timeout-ms 9300000000000"), hugeTimeout.err());
		for (var refused : List.of(misspelled, otherCommands, unsafeElsewhere, badTimeout, badGroup, badPeer, badIndex,
				repeatedPeer, hugeTimeout)) {
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Usage:"), refused.err());
		}
	}

	/**
	 * R61-15: {@code add-peer} and {@code remove-peer} refuse, as wrong usage, a group in which two different members
	 * share a Raft or an API address, naming both ids. The member that {@code add-peer} replaces (same id) and the one
	 * that {@code remove-peer} removes are not part of the group the change leaves: a repair of a wrong recorded address
	 * is not refused (it goes on, here to the network, which fails with another code than 2).
	 */
	@Test
	void adminRefusesMembersSharingAnAddress() {
		var n1 = List.of("--peer", "n1=127.0.0.1:1/127.0.0.1:2");
		var shared = run("admin", "--peer", "n1=127.0.0.1:1/127.0.0.1:2", "--timeout-ms", "1000",
				"add-peer", "n2=127.0.0.1:1/127.0.0.1:4");
		assertEquals(2, shared.code(), shared.toString());
		assertTrue(shared.err().contains("n1") && shared.err().contains("n2") && shared.err().contains("share"), shared.err());
		var api = run("admin", "--peer", "n1=127.0.0.1:1/127.0.0.1:2", "--timeout-ms", "1000",
				"add-peer", "n2=127.0.0.1:3/127.0.0.1:2");
		assertEquals(2, api.code(), api.toString());
		var removeShared = run("admin", "--peer", "n1=127.0.0.1:1/127.0.0.1:2", "--peer", "n2=127.0.0.1:3/127.0.0.1:4",
				"--peer", "n3=127.0.0.1:3/127.0.0.1:6", "--timeout-ms", "1000", "remove-peer", "n1");
		assertEquals(2, removeShared.code(), removeShared.toString());
		assertTrue(removeShared.err().contains("n2") && removeShared.err().contains("n3"), removeShared.err());
		// repair: the removed member's recorded address is the wrong one (it is n2's)
		var repair = run("admin", "--peer", "n1=127.0.0.1:1/127.0.0.1:2", "--peer", "n2=127.0.0.1:3/127.0.0.1:4",
				"--peer", "n3=127.0.0.1:3/127.0.0.1:6", "--timeout-ms", "1000", "remove-peer", "n3");
		assertTrue(repair.code() != 2, repair.toString());
		// --change-addresses: the replaced member (same id) given a corrected address
		var changed = run("admin", "--peer", "n1=127.0.0.1:1/127.0.0.1:2", "--peer", "n2=127.0.0.1:1/127.0.0.1:4",
				"--timeout-ms", "1000", "--change-addresses", "add-peer", "n2=127.0.0.1:3/127.0.0.1:4");
		assertTrue(changed.code() != 2, changed.toString());
		for (var refused : List.of(shared, api, removeShared)) {
			assertTrue(refused.thrown() == null, refused.toString());
		}
	}

	/**
	 * R61-12, R61-3: a value below what the command can use is wrong usage too, exit 2 with the problem line, as an
	 * unparsable one: a zero or negative {@code admin --timeout-ms} (a deadline in the past, or an
	 * {@code ArithmeticException} below about -9.2e12), a port outside 1 to 65535 in {@code --peer} or in the argument
	 * of {@code add-peer} (the node failed later, at bind, with exit 1), and, in the node's configuration, a zero,
	 * negative or overflowing request timeout or election timeout ({@code run} maps what {@link NodeConfig} refuses to
	 * usage; {@code StartupChecksTest.runRefusesMalformedValuesWithTwo} runs them through {@code run}).
	 */
	@Test
	void nonPositiveAndOutOfRangeValuesAreUsageErrors() {
		var peer = List.of("admin", "--peer", "d1=127.0.0.1:1/127.0.0.1:1");
		for (var value : List.of("0", "-5", "-9300000000000")) {
			var refused = run(with(peer, "--timeout-ms", value, "status"));
			assertEquals(2, refused.code(), refused.toString());
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Error: invalid --timeout-ms " + value), refused.err());
		}
		for (var spec : List.of("d1=127.0.0.1:0/127.0.0.1:1", "d1=127.0.0.1:1/127.0.0.1:65536",
				"d1=127.0.0.1:-1/127.0.0.1:1", "d1=127.0.0.1:1/127.0.0.1:99999")) {
			var refused = run("admin", "--peer", spec, "status");
			assertEquals(2, refused.code(), refused.toString());
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains("Error: invalid --peer " + spec), refused.err());
			assertTrue(refused.err().contains("1 to 65535"), refused.err());
		}
		var badAdded = run(with(peer, "--timeout-ms", "1000", "add-peer", "d2=127.0.0.1:99999/127.0.0.1:1"));
		assertEquals(2, badAdded.code(), badAdded.toString());
		assertTrue(badAdded.err().contains("Error: invalid add-peer argument d2=127.0.0.1:99999/127.0.0.1:1"),
				badAdded.err());

		var defaults = NodeConfig.defaults("d1", List.of(PeerConfig.parse("d1=127.0.0.1:1/127.0.0.1:2")), Path.of("."));
		var tooLong = Duration.ofMillis(Long.MAX_VALUE);
		for (var timeout : List.of(Duration.ZERO, Duration.ofMillis(-5), tooLong)) {
			var refusal = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
					() -> defaults.withRequestTimeout(timeout), () -> "request timeout " + timeout);
			assertTrue(refusal.getMessage().contains("requestTimeout"), refusal.getMessage());
		}
		for (var min : List.of(Duration.ZERO, Duration.ofMillis(-5))) {
			var refusal = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
					() -> defaults.withElectionTimeouts(min, Duration.ofSeconds(2)), () -> "election timeout " + min);
			assertTrue(refusal.getMessage().contains("electionTimeoutMin"), refusal.getMessage());
		}
		var refusal = org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
				() -> defaults.withElectionTimeouts(Duration.ofSeconds(1), tooLong));
		assertTrue(refusal.getMessage().contains("electionTimeoutMax"), refusal.getMessage());
	}

	/**
	 * R61-11: {@code add-peer} and {@code remove-peer} read the leader's configuration from the leader that the members
	 * of {@code --peer} name when none of them leads, asking it at the address their configuration gives. "Replacing a
	 * node" in the runbooks prescribes such lists ({@code --peer n2 --peer n3} once n4, not yet in {@code PEERS}, was
	 * added and may lead). The commands asked only the {@code --peer} members: on a healthy cluster they waited the
	 * whole {@code --timeout-ms} and failed with "still waiting for a leader (.. is FOLLOWER, .. is FOLLOWER)".
	 */
	@Test
	void addAndRemovePeerFollowTheLeaderThatIsNotInPeer() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("leader-outside")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var all = List.of("n0", "n1", "n2");
		var leader = cluster.awaitLeader(TIMEOUT).config().nodeId();
		var given = new ArrayList<>(all);
		given.remove(leader);

		long start = System.nanoTime();
		var check = run(admin(given, "remove-peer", "n9"));
		var elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertEquals(0, check.code(), check.toString());
		assertTrue(check.out().contains("n9 is not a voter: nothing to remove (voters " + all + ")"), check.out());
		assertTrue(check.out().contains("the leader " + leader + " is not in --peer"), check.out());
		assertTrue(check.out().strip().endsWith("OK"), check.out());
		assertTrue(elapsed.compareTo(Duration.ofSeconds(15)) < 0, () -> "took " + elapsed + ": " + check.out());

		var again = run(admin(given, "add-peer", cluster.config(leader).self().format()));
		assertEquals(0, again.code(), again.toString());
		assertTrue(again.out().contains("leader " + leader + ": voters " + all), again.out());
		assertTrue(again.out().contains("is already a voter"), again.out());

		// A change is proposed to that leader too: its own removal, the one change such a --peer can ask for.
		var removal = run(admin(given, "remove-peer", leader));
		assertEquals(0, removal.code(), removal.toString());
		assertTrue(removal.out().contains("this change adds [] and removes [" + leader + "]"), removal.out());
		// Review of R61, 7: once per command, not once per read of the leader's configuration.
		assertEquals(1, removal.out().lines().filter(line -> line.startsWith("NOTE: the leader " + leader)).count(),
				removal.out());
		assertEquals(1, check.out().lines().filter(line -> line.startsWith("NOTE: the leader " + leader)).count(),
				check.out());
		assertEquals(1, again.out().lines().filter(line -> line.startsWith("NOTE: the leader " + leader)).count(),
				again.out());
		assertTrue(removal.out().strip().endsWith("OK"), removal.out());
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(node ->
				given.contains(node.config().nodeId()) && node.isLeader()
						&& node.stateMachine().division().getRaftConf().getCurrentPeers().stream()
								.map(peer -> peer.getId().toString()).sorted().toList().equals(given)));
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
	 * R32-4: a member that has not applied the cluster version that the leader committed (it lags: a member added with
	 * add-peer that is still catching up, a slow disk, right after a start) refuses {@code checksum} and {@code backup}
	 * from its own view, which is behind the cluster's, and here it answers first. Its refusal proposed nothing, so the
	 * command goes on with the next member that answered and succeeds; it says that the member refused. Before, the
	 * command ended with 1 on that first refusal ("Upgrade every member first") although every member supports both.
	 */
	@Test
	void aLaggingMemberThatAnswersFirstDoesNotStopTheCommand() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("lagging")), 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				Capabilities.has(node.stateMachine().ops().clusterVersion(), Capabilities.BACKUP)));
		var lagging = cluster.anyFollower();
		var laggingId = lagging.config().nodeId();
		// Its applied view lacks what checksum and backup need, as if the entry that raised them were still unapplied;
		// the rest (checksum-schemas among it) stays, so its digest is the others'.
		var ops = lagging.stateMachine().ops();
		ops.clusterVersion(ops.clusterVersion() & ~(Capabilities.CHECKSUM | Capabilities.BACKUP));
		var proxies = new ArrayList<LinkProxy>();
		try {
			var args = new ArrayList<String>(List.of("admin", "--peer", lagging.config().self().format()));
			for (var id : List.of("n0", "n1", "n2")) {
				if (id.equals(laggingId)) {
					continue;
				}
				// The others answer 800 ms later: the lagging member answers first and gets the proposal first.
				var self = cluster.config(id).self();
				var proxy = new LinkProxy(new java.net.InetSocketAddress("127.0.0.1", 0),
						new java.net.InetSocketAddress(self.raftHost(), self.raftPort()));
				proxy.shape(LinkProxy.Shaping.latency(Duration.ofMillis(400), Duration.ZERO));
				proxies.add(proxy);
				args.add("--peer");
				args.add(id + "=127.0.0.1:" + proxy.listenAddress().getPort() + "/" + self.apiHost() + ":"
						+ self.apiPort());
			}
			args.addAll(List.of("--timeout-ms", "20000"));
			for (var command : List.of(List.of("checksum"), List.of("backup", "b9"))) {
				var result = run(with(args, command.toArray(String[]::new)));
				assertEquals(0, result.code(), result.toString());
				assertTrue(result.out().contains(laggingId + " refused"), result.toString());
				assertTrue(result.out().contains(" at log index "), result.toString());
				for (var id : List.of("n0", "n1", "n2")) {
					assertTrue(result.out().contains(id + ": DONE"), result.toString());
				}
				assertTrue(result.out().strip().endsWith("OK"), result.toString());
			}
		} finally {
			for (var proxy : proxies) {
				proxy.close();
			}
		}
	}

	/**
	 * R32-4: when every member that answered refuses for its cluster version, the command fails with 1 and names each of
	 * them with the version it gave (here in the words of a member of an older version), having asked each once. A
	 * member that does not answer is left out, but only once the capabilities question timed out for it ({@code
	 * PEER_ANSWER_TIMEOUT}, 3 s): until then the command waits for its answer. The message tells apart the leader that
	 * has not raised the cluster version yet (every member refuses, the leader too, right after the last member of a
	 * rolling upgrade restarts: retry in about 10 s) from members that lag behind it (R32 review 1).
	 */
	@Test
	void everyMemberRefusingForItsVersionFailsNamingThem() throws Exception {
		var refusal = Status.FAILED_PRECONDITION.withDescription(
				"Not every member supports checksum yet: the cluster version is none. Upgrade every member first");
		try (var first = new RefusingMember(Duration.ZERO, refusal);
				var second = new RefusingMember(Duration.ofMillis(300), refusal);
				var silent = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress())) {
			var result = run("admin", "--peer", first.peer("v1"), "--peer", second.peer("v2"),
					"--peer", "d1=127.0.0.1:" + silent.getLocalPort() + "/127.0.0.1:" + silent.getLocalPort(),
					"--timeout-ms", "10000", "checksum");
			assertEquals(1, result.code(), result.toString());
			assertEquals(1, first.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
			assertEquals(1, second.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
			var failed = result.out().lines().filter(line -> line.startsWith("FAILED: ")).findFirst().orElseThrow();
			assertTrue(failed.contains("v1") && failed.contains("v2") && !failed.contains("d1"), failed);
			assertTrue(failed.contains("the cluster version is none"), failed);
			assertTrue(failed.contains("every member that answered"), failed);
			assertTrue(failed.contains("the leader has not raised the cluster version yet"), failed);
			assertTrue(failed.contains("in about 10 s"), failed);
		}
	}

	/**
	 * R32-4: a refusal that is not about the cluster version (here a made-up precondition) still ends the command with 1
	 * at once: the next member that answered is not asked.
	 */
	@Test
	void anotherRefusalStillEndsTheCommandAtOnce() throws Exception {
		try (var first = new RefusingMember(Duration.ZERO,
				Status.FAILED_PRECONDITION.withDescription("A backup named b1 is in the way"));
				var second = new RefusingMember(Duration.ofMillis(300), Status.FAILED_PRECONDITION.withDescription(
						"Not every member supports backup yet: the cluster version is none. Upgrade every member first"))) {
			var result = run("admin", "--peer", first.peer("r1"), "--peer", second.peer("r2"),
					"--timeout-ms", "10000", "backup", "b1");
			assertEquals(1, result.code(), result.toString());
			assertTrue(result.out().contains("r1 refused: A backup named b1 is in the way"), result.toString());
			assertEquals(1, first.count(ReplicaOps.OP_PROPOSE_BACKUP), result.toString());
			assertEquals(0, second.count(ReplicaOps.OP_PROPOSE_BACKUP), result.toString());
		}
	}

	/**
	 * A member that answers the capabilities question after {@code delay} and refuses every proposal with {@code
	 * refusal}, keeping what it was asked.
	 */
	private static final class RefusingMember implements AutoCloseable {
		final List<byte[]> asked = new CopyOnWriteArrayList<>();
		final Server server;

		RefusingMember(Duration delay, Status refusal) throws IOException {
			ServerCalls.UnaryMethod<byte[], byte[]> ops = (request, response) -> {
				var rest = java.util.Arrays.copyOfRange(request, 16, request.length);
				if (rest.length > 0 && rest[0] == ReplicaOps.OP_CAPABILITIES) {
					try {
						Thread.sleep(delay);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
					response.onNext(ByteBuffer.allocate(2 * Long.BYTES).putLong(Capabilities.SUPPORTED).putLong(0).array());
					response.onCompleted();
				} else {
					asked.add(rest);
					response.onError(refusal.asRuntimeException());
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
			return asked.stream().filter(request -> request[0] == op).count();
		}

		@Override
		public void close() throws InterruptedException {
			server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
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
	 * A member that answers the capabilities question, takes the first proposal and then breaks the call without
	 * answering it (R40-1, review 1). With {@code reset}, the member sits behind a TCP proxy that then resets every
	 * client connection (SO_LINGER 0) and refuses new ones, as a member that is killed or whose host drops the
	 * connection mid-call: the client sees the transport close under the open call, which grpc-java answers with
	 * UNAVAILABLE. Without it, the member's server shuts down with {@code shutdownNow}, which resets its open streams
	 * with RST_STREAM(CANCEL): the client sees CANCELLED.
	 */
	private static final class DroppingMember implements AutoCloseable {
		final java.util.concurrent.atomic.AtomicInteger taken = new java.util.concurrent.atomic.AtomicInteger();
		final Server server;
		final java.net.@Nullable ServerSocket proxy;
		final List<java.net.Socket> clients = new CopyOnWriteArrayList<>();

		DroppingMember(boolean reset) throws IOException {
			var self = new java.util.concurrent.atomic.AtomicReference<Server>();
			ServerCalls.UnaryMethod<byte[], byte[]> ops = (request, response) -> {
				var rest = java.util.Arrays.copyOfRange(request, 16, request.length);
				if (rest.length > 0 && rest[0] == ReplicaOps.OP_CAPABILITIES) {
					response.onNext(ByteBuffer.allocate(2 * Long.BYTES).putLong(Capabilities.SUPPORTED)
							.putLong(Capabilities.SUPPORTED).array());
					response.onCompleted();
				} else if (taken.getAndIncrement() == 0) {
					Thread.ofPlatform().start(reset ? this::resetClients : () -> self.get().shutdownNow());
				}
			};
			server = NettyServerBuilder.forAddress(new java.net.InetSocketAddress("127.0.0.1", 0))
					.addService(ServerServiceDefinition.builder(TermService.SERVICE_NAME)
							.addMethod(TermService.OPS, ServerCalls.asyncUnaryCall(ops)).build())
					.build()
					.start();
			self.set(server);
			if (reset) {
				var loopback = java.net.InetAddress.getByName("127.0.0.1");
				var listener = new java.net.ServerSocket(0, 50, loopback);
				proxy = listener;
				Thread.ofPlatform().daemon().start(() -> {
					while (true) {
						try {
							var client = listener.accept();
							var backend = new java.net.Socket(loopback, server.getPort());
							clients.add(client);
							pump(client, backend);
							pump(backend, client);
						} catch (IOException ex) {
							return;
						}
					}
				});
			} else {
				proxy = null;
			}
		}

		private static void pump(java.net.Socket from, java.net.Socket to) {
			Thread.ofPlatform().daemon().start(() -> {
				try (from; to) {
					from.getInputStream().transferTo(to.getOutputStream());
				} catch (IOException ignored) {
					// The other side closed or was reset.
				}
			});
		}

		private void resetClients() {
			try {
				proxy.close();
			} catch (IOException ignored) {
				// Closing it only refuses new connections.
			}
			for (var client : clients) {
				try {
					client.setSoLinger(true, 0);
					client.close();
				} catch (IOException ignored) {
					// Closed already.
				}
			}
		}

		String address() {
			return "127.0.0.1:" + (proxy != null ? proxy.getLocalPort() : server.getPort());
		}

		String peer(String id) {
			return id + "=" + address() + "/" + address();
		}

		@Override
		public void close() throws InterruptedException {
			if (proxy != null) {
				resetClients();
			}
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
	 * R39-1: a member that took a {@code backup} proposal and gave no answer within {@code --timeout-ms} (a leader whose
	 * disk stalls: the entry may be appended and commit later) leaves open whether the backup is written. The command
	 * still exits 4, but must not say "Nothing was proposed: run the command again": a rerun has a new request id and
	 * is refused with "A backup named ... exists already" once the first one is applied. It names the member, the
	 * request id and how to find the index for {@code backup-status}, or advises another name. When no member took the
	 * request (a closed port), "Nothing was proposed" stays.
	 */
	@Test
	void aBackupThatAMemberTookWithoutAnsweringMayHaveBeenProposed() throws Exception {
		try (var hanging = new HangingMember()) {
			var taken = run("admin", "--peer", hanging.peer("h1"), "--timeout-ms", "3000", "backup", "b9");
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, taken.code(), taken.toString());
			assertTrue(hanging.count(ReplicaOps.OP_PROPOSE_BACKUP) >= 1, taken.toString());
			var sent = hanging.held.stream().filter(request -> request[0] == ReplicaOps.OP_PROPOSE_BACKUP).findFirst()
					.orElseThrow();
			long requestId = ByteBuffer.wrap(sent, 1, Long.BYTES).getLong();
			assertTrue(taken.out().contains("FAILED: "), taken.toString());
			assertTrue(!taken.out().contains("Nothing was proposed"), taken.toString());
			assertTrue(taken.out().contains("h1 took the request"), taken.toString());
			assertTrue(taken.out().contains("may have been proposed"), taken.toString());
			assertTrue(taken.out().contains("request=" + requestId), taken.toString());
			assertTrue(taken.out().contains("backup-status b9 <index>"), taken.toString());
			assertTrue(taken.out().contains("another name"), taken.toString());
			// R39 review 3: an answer of an earlier round is kept, and a missing one is not printed as "null".
			assertTrue(!taken.out().contains("last answer null"), taken.toString());
		}
		int closedPort;
		try (var probe = new java.net.ServerSocket(0)) {
			closedPort = probe.getLocalPort();
		}
		var none = run("admin", "--peer", "d1=127.0.0.1:" + closedPort + "/127.0.0.1:" + closedPort,
				"--timeout-ms", "2000", "backup", "b9");
		assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, none.code(), none.toString());
		assertTrue(none.out().contains("Nothing was proposed: run the command again"), none.toString());
		assertTrue(!none.out().contains("may have been proposed"), none.toString());
		assertTrue(!none.out().contains("last answer null"), none.toString());
	}

	/**
	 * R40-1 to R40-3: grpc-java answers UNAVAILABLE also when the transport breaks after the member received the call
	 * (a kill, a restart, a reset, a GOAWAY), when its Proposer may have sent the entry already. Such a bare
	 * UNAVAILABLE on the proposal call does not tell whether the operation was proposed: {@code checksum}, which is not
	 * idempotent, ends with 4 ("does not tell") without proposing it again through the next member, which would start a
	 * second full-database digest on every replica. Before, every UNAVAILABLE counted as nothing proposed.
	 */
	@Test
	void aChecksumWhoseCallBrokeAfterTheMemberTookItIsNotProposedAgain() throws Exception {
		try (var dropped = new RefusingMember(Duration.ZERO,
				Status.UNAVAILABLE.withDescription("Network closed for unknown reason"));
				var next = new RefusingMember(Duration.ofMillis(500),
						Status.FAILED_PRECONDITION.withDescription("asked the next member"))) {
			var result = run("admin", "--peer", dropped.peer("m1"), "--peer", next.peer("m2"), "--timeout-ms", "10000",
					"checksum");
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, result.code(), result.toString());
			assertEquals(1, dropped.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
			assertEquals(0, next.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
			assertTrue(result.out().contains("the answer of m1"), result.toString());
			assertTrue(result.out().contains("does not tell whether the operation was proposed"), result.toString());
		}
	}

	/**
	 * R40-1 to R40-3: a {@code backup} that a member took before its call broke with a bare UNAVAILABLE (here answered
	 * so, and then by a member whose connection is reset under the open call) may have been proposed: at the deadline
	 * the command says so and names the member, not "Nothing was proposed: run the command again", whose rerun would be
	 * refused with "A backup named ... exists already" once the first one is written. A member whose server shuts down
	 * with {@code shutdownNow} mid-call is CANCELLED (RST_STREAM CANCEL), not UNAVAILABLE, and counts as took too
	 * (review 1 of R40: that half alone was green before R40; the reset half was red then, "Nothing was proposed").
	 */
	@Test
	void aBackupWhoseCallBrokeAfterTheMemberTookItMayHaveBeenProposed() throws Exception {
		try (var dropped = new RefusingMember(Duration.ZERO,
				Status.UNAVAILABLE.withDescription("Network closed for unknown reason"))) {
			var result = run("admin", "--peer", dropped.peer("m1"), "--timeout-ms", "3000", "backup", "b9");
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, result.code(), result.toString());
			assertTrue(dropped.count(ReplicaOps.OP_PROPOSE_BACKUP) >= 1, result.toString());
			assertTrue(!result.out().contains("Nothing was proposed"), result.toString());
			assertTrue(result.out().contains("m1 took the request"), result.toString());
			assertTrue(result.out().contains("may have been proposed"), result.toString());
		}
		// Review 1 of R40: each kind of break, first with the status that a direct call sees, so that the half tests
		// what it claims: a connection reset under the open call is the bare UNAVAILABLE of R40-1; a server
		// shutdownNow resets the stream and is CANCELLED, which never counted as nothing proposed.
		var groupId = org.apache.ratis.protocol.RaftGroupId.valueOf(java.util.UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));
		for (var reset : List.of(true, false)) {
			var expected = reset ? Status.Code.UNAVAILABLE : Status.Code.CANCELLED;
			try (var probe = new DroppingMember(reset)) {
				var ex = org.junit.jupiter.api.Assertions.assertThrows(java.util.concurrent.CompletionException.class,
						() -> ReplicaOps.call(probe.address(), groupId, ReplicaOps.OP_PROPOSE_BACKUP, new byte[0],
								Duration.ofSeconds(10)).join());
				assertEquals(expected, Status.fromThrowable(ex.getCause()).getCode(), "reset " + reset + ": " + ex);
			}
			try (var dropping = new DroppingMember(reset)) {
				var result = run("admin", "--peer", dropping.peer("m1"), "--timeout-ms", "3000", "backup", "b9");
				assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, result.code(), result.toString());
				assertEquals(1, dropping.taken.get(), result.toString());
				assertTrue(!result.out().contains("Nothing was proposed"), result.toString());
				assertTrue(result.out().contains("m1 took the request"), result.toString());
				assertTrue(result.out().contains("may have been proposed"), result.toString());
			}
		}
	}

	/**
	 * R40-1 to R40-3: a member's own refusal that proves nothing was proposed (its Proposer did not apply it, or the
	 * node is starting, also in the words of an older member) is UNAVAILABLE with a description that says so: a
	 * {@code checksum} still moves to the next member, and a {@code backup} that every member refused so still says
	 * "Nothing was proposed".
	 */
	@Test
	void aMarkedNotProposedRefusalStillMovesToTheNextMember() throws Exception {
		for (var refusal : List.of("Not proposed: no leader accepted the request in time", "The node is starting",
				"Not proposed: the node is starting")) {
			try (var first = new RefusingMember(Duration.ZERO, Status.UNAVAILABLE.withDescription(refusal));
					var next = new RefusingMember(Duration.ofMillis(500),
							Status.FAILED_PRECONDITION.withDescription("asked the next member"))) {
				var result = run("admin", "--peer", first.peer("m1"), "--peer", next.peer("m2"), "--timeout-ms", "10000",
						"checksum");
				assertEquals(1, result.code(), result.toString());
				assertEquals(1, first.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
				assertEquals(1, next.count(ReplicaOps.OP_PROPOSE_CHECKSUM), result.toString());
				assertTrue(result.out().contains("m2 refused: asked the next member"), result.toString());
			}
		}
		try (var refusing = new RefusingMember(Duration.ZERO,
				Status.UNAVAILABLE.withDescription("Not proposed: no leader accepted the request in time"))) {
			var result = run("admin", "--peer", refusing.peer("m1"), "--timeout-ms", "2000", "backup", "b9");
			assertEquals(ReplicatedServerMain.EXIT_INCOMPLETE, result.code(), result.toString());
			assertTrue(result.out().contains("Nothing was proposed: run the command again"), result.toString());
			assertTrue(!result.out().contains("may have been proposed"), result.toString());
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

	/**
	 * R46-3: {@code repair-log} took {@code --group-id} as given, while {@code run}, {@code admin} and
	 * {@code export-seed} parse it as a UUID and name the group's directory by its canonical (lower-case) form. A group
	 * id that {@code run} accepted in upper case was refused ("does not exist: wrong --data-dir or --group-id?", exit 1),
	 * and a value that is not a UUID was used as a path component ({@code raft/../..}) instead of being wrong usage.
	 */
	@Test
	void repairLogParsesTheGroupIdAsTheOtherCommandsDo() throws IOException {
		String group = "0f1e2d3c-4b5a-4968-8776-655443322110";
		Path dataDir = temp.resolve("node");
		Files.createDirectories(dataDir.resolve("raft").resolve(group).resolve("current"));
		var upper = run("repair-log", "--data-dir", dataDir.toString(), "--group-id", group.toUpperCase(Locale.ROOT),
				"--dry-run");
		assertEquals(0, upper.code(), upper.toString());
		assertTrue(upper.out().contains(dataDir.resolve("raft").resolve(group).resolve("current").toString()),
				upper.out());

		Files.createDirectories(temp.resolve("current"));
		for (var wrong : List.of("../..", "nope")) {
			var refused = run("repair-log", "--data-dir", dataDir.toString(), "--group-id", wrong, "--dry-run");
			assertEquals(2, refused.code(), refused.toString());
			assertTrue(refused.err().contains("invalid value for --group-id: " + wrong + " (not a UUID)"), refused.err());
			assertTrue(refused.err().contains("Usage:"), refused.err());
		}
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

		// R36-3: "nothing is written", also with the old cluster's results in the data directory: they stay in place.
		Files.writeString(target.resolve("replica-ops.log"), "old cluster\n");
		Files.createDirectories(target.resolve(ReplicaOps.BACKUPS_DIR).resolve("old"));
		var again = run("restore-backup", "--backup", backup.toString(), "--data-dir", target.toString());
		assertEquals(5, again.code(), again.toString());
		try (var entries = Files.list(target)) {
			assertEquals(List.of("backups", "replica-ops.log"),
					entries.map(path -> path.getFileName().toString()).sorted().toList());
		}
	}

	/**
	 * R36-1/R36-2: {@code restore-backup --backup DATA-DIR/backups/<name>}, the backup the node itself wrote, restores
	 * and prints OK. Since R35-2 the command renamed {@code backups/} before reading the backup and failed with
	 * {@code NoSuchFileException}; at 52ea5e3 it worked.
	 */
	@Test
	void restoreBackupFromTheDataDirectorysOwnBackupsDirectory() throws IOException {
		Path source = temp.resolve("source");
		NodeStoreTest.writeDatabaseOfVersion(source.resolve("db"), Capabilities.SUPPORTED);
		Path dataDir = Files.createDirectories(temp.resolve("data"));
		Path backup = dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("2026-11-06");
		try (var store = NodeStore.open(source.resolve("db"), null, "source")) {
			store.checkpoint(backup.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backup.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");

		var restore = run("restore-backup", "--backup", backup.toString(), "--data-dir", dataDir.toString());

		assertEquals(0, restore.code(), restore.toString());
		assertTrue(restore.out().startsWith("OK: "), restore.out());
		assertTrue(Files.isRegularFile(dataDir.resolve("db").resolve("CURRENT")));
		assertTrue(Files.notExists(dataDir.resolve(ReplicaOps.BACKUPS_DIR)));
	}

	/**
	 * R38-RESTORE-STALE-RESTORE-READY: {@code restore-backup} into a data directory that has no {@code db/} but the
	 * {@code db.restore-ready} and {@code db.restore-old} of an interrupted snapshot install (R33-1) exits with 1, names
	 * the leftover and touches nothing; once the operator deletes the leftovers, the same command restores. Before, it
	 * printed OK, and the first start swapped the old database over the restored one.
	 */
	@Test
	void restoreBackupRefusesTheLeftoversOfAnInterruptedSwapWithOne() throws IOException {
		Path source = temp.resolve("source");
		NodeStoreTest.writeDatabaseOfVersion(source.resolve("db"), Capabilities.SUPPORTED);
		Path backup = temp.resolve("backup");
		try (var store = NodeStore.open(source.resolve("db"), null, "source")) {
			store.checkpoint(backup.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backup.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Path dataDir = temp.resolve("node").toAbsolutePath().normalize();
		NodeStoreTest.writeRestoreInterruptedDuringTheSwap(dataDir, true);

		var refused = run("restore-backup", "--backup", backup.toString(), "--data-dir", dataDir.toString());

		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.out().startsWith("FAILED: "), refused.out());
		assertTrue(refused.out().contains(dataDir.resolve("db.restore-ready").toString()), refused.out());
		assertTrue(Files.notExists(dataDir.resolve("db")));
		assertTrue(Files.notExists(dataDir.resolve(ReplicaOps.RESTORE_STAGING_DIR)));
		assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-ready").resolve("CURRENT")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-old").resolve("CURRENT")));

		NodeStore.deleteRecursively(dataDir.resolve("db.restore-ready"));
		NodeStore.deleteRecursively(dataDir.resolve("db.restore-old"));
		var restore = run("restore-backup", "--backup", backup.toString(), "--data-dir", dataDir.toString());
		assertEquals(0, restore.code(), restore.toString());
		assertTrue(restore.out().startsWith("OK: "), restore.out());
	}

	/**
	 * R35-1: {@code restore-backup --data-dir /x/link/..}, with {@code link} pointing to {@code /real/a/b}, restores into
	 * {@code /x/db}, the directory a {@code run} with the same {@code --data-dir} opens, and says so. Every command
	 * normalizes {@code --data-dir} once, as export-seed does. Before, the backup went to {@code /real/a/db}, an empty
	 * {@code /x/db} was opened, and the command printed OK with the raw path.
	 */
	@Test
	void restoreBackupNormalizesTheDataDirectoryOnce() throws IOException {
		Path source = temp.resolve("source");
		NodeStoreTest.writeDatabaseOfVersion(source.resolve("db"), Capabilities.SUPPORTED);
		Path backup = temp.resolve("backup");
		try (var store = NodeStore.open(source.resolve("db"), null, "source")) {
			store.checkpoint(backup.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backup.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Path real = Files.createDirectories(temp.resolve("real").resolve("a"));
		Path x = Files.createDirectories(temp.resolve("x"));
		Files.createSymbolicLink(x.resolve("link"), Files.createDirectories(real.resolve("b")));

		var restore = run("restore-backup", "--backup", backup.toString(), "--data-dir",
				x.resolve("link").resolve("..").toString());

		assertEquals(0, restore.code(), restore.toString());
		Path normalized = x.toAbsolutePath().normalize();
		assertTrue(restore.out().startsWith("OK: " + normalized + " holds the data up to log index 7 "), restore.out());
		assertTrue(Files.isRegularFile(normalized.resolve("db").resolve("CURRENT")));
		assertTrue(Files.notExists(real.resolve("db")), "restored where the OS resolves the data directory");
	}

	/**
	 * R31-2: {@code export-seed} of a stopped node whose {@code db/} is behind the snapshot in its Raft directory (a
	 * crash during a snapshot installation) restores the snapshot first, as the node's start would, and prints the
	 * snapshot's index. A node of another group needs {@code --group-id}, as {@code run} and {@code repair-log} do:
	 * without it, raft/ holding only other groups is wrong usage rather than a silent export of the older {@code db/}.
	 */
	@Test
	void exportSeedRestoresASnapshotAheadOfTheDatabase() throws IOException {
		Path dataDir = temp.resolve("node");
		writeNodeBehindASnapshot(dataDir, NodeConfig.DEFAULT_GROUP_ID);
		Path seed = temp.resolve("seed");
		var export = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(0, export.code(), export.toString());
		assertTrue(export.out().contains("applied up to index 40 of term 4"), export.out());
		assertTrue(export.out().contains("snapshot at index 40"), export.out());

		String otherGroup = "0f1e2d3c-4b5a-4968-8776-655443322110";
		Path other = temp.resolve("other");
		writeNodeBehindASnapshot(other, otherGroup);
		var withoutGroup = run("export-seed", "--data-dir", other.toString(), "--target", temp.resolve("s2").toString());
		assertEquals(2, withoutGroup.code(), withoutGroup.toString());
		assertTrue(withoutGroup.err().contains(otherGroup) && withoutGroup.err().contains("--group-id"),
				withoutGroup.err());
		assertTrue(Files.notExists(temp.resolve("s2")));
		var withGroup = run("export-seed", "--data-dir", other.toString(), "--target", temp.resolve("s3").toString(),
				"--group-id", otherGroup);
		assertEquals(0, withGroup.code(), withGroup.toString());
		assertTrue(withGroup.out().contains("applied up to index 40 of term 4"), withGroup.out());
	}

	/**
	 * R32-2: an explicit {@code --group-id} without a directory in raft/, which holds other groups (a typo), is wrong
	 * usage: exporting {@code db/} would miss the node's snapshot without a word, as R31-2 refuses for the default group.
	 */
	@Test
	void exportSeedRefusesAGroupIdWithoutRaftState() throws IOException {
		String group = "0f1e2d3c-4b5a-4968-8776-655443322110";
		String typo = "0f1e2d3c-4b5a-4968-8776-655443322111";
		Path dataDir = temp.resolve("node");
		writeNodeBehindASnapshot(dataDir, group);
		Path seed = temp.resolve("seed");
		var export = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString(),
				"--group-id", typo);
		assertEquals(2, export.code(), export.toString());
		assertTrue(export.err().contains(typo) && export.err().contains(group), export.err());
		assertTrue(Files.notExists(seed));
	}

	/**
	 * R32-1: a damaged snapshot ahead of {@code db/} makes {@code export-seed} fail with 1 and write nothing, naming the
	 * snapshot's index; with {@code --without-damaged-snapshot} it exports {@code db/}, exit 0, and stdout says that
	 * the snapshot at that index was not used.
	 */
	@Test
	void exportSeedRefusesADamagedSnapshotUnlessToldTo() throws IOException {
		Path dataDir = temp.resolve("node");
		writeNodeBehindASnapshot(dataDir, NodeConfig.DEFAULT_GROUP_ID);
		// A digest makes it a received snapshot, and this one matches nothing: damaged by a crash.
		Path snapshot = dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("sm")
				.resolve("snapshots").resolve("snapshot-4-40");
		Files.writeString(snapshot.resolve("snapshot.properties.md5"),
				"00000000000000000000000000000000 *snapshot.properties\n");
		Path seed = temp.resolve("seed");
		var refused = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.err().contains("index 40") && refused.err().contains("--without-damaged-snapshot"),
				refused.err());
		assertTrue(Files.notExists(seed));

		var exported = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString(),
				"--without-damaged-snapshot");
		assertEquals(0, exported.code(), exported.toString());
		assertTrue(exported.out().contains("applied up to index 5 of term 1"), exported.out());
		assertTrue(exported.out().contains("damaged snapshot at index 40"), exported.out());
		assertTrue(Files.exists(seed.resolve("CURRENT")));
	}

	/**
	 * R32-5: {@code export-seed} of a node that runs (it holds raft/&lt;group&gt;/in_use.lock for its lifetime, also
	 * while it installs a snapshot with {@code db/} closed) is refused with 1 before anything of the node is touched:
	 * its {@code db.restore-tmp} that an installation is building, its snapshot ahead of {@code db/}. Before, the open
	 * of {@code db/} finished or deleted the restore directories first, and the export went on.
	 */
	@Test
	void exportSeedRefusesWhileTheNodeHoldsItsRaftLock() throws IOException {
		Path dataDir = temp.resolve("node");
		writeNodeBehindASnapshot(dataDir, NodeConfig.DEFAULT_GROUP_ID);
		Path building = Files.createDirectories(dataDir.resolve("db.restore-tmp"));
		Files.writeString(building.resolve("000042.sst"), "being copied");
		Path lockFile = dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("in_use.lock");
		Path seed = temp.resolve("seed");
		try (var channel = java.nio.channels.FileChannel.open(lockFile, java.nio.file.StandardOpenOption.CREATE,
				java.nio.file.StandardOpenOption.WRITE); var lock = channel.lock()) {
			var refused = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
			assertEquals(1, refused.code(), refused.toString());
			assertTrue(refused.thrown() == null, refused.toString());
			assertTrue(refused.err().contains(lockFile + " is locked: stop the node first"), refused.err());
			assertTrue(Files.notExists(seed));
			assertEquals("being copied", Files.readString(building.resolve("000042.sst")));
		}
		// Stopped: the export goes on, the snapshot ahead restored first.
		var exported = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(0, exported.code(), exported.toString());
		assertTrue(exported.out().contains("applied up to index 40 of term 4"), exported.out());
	}

	/**
	 * R33-1: {@code export-seed} of a stopped node whose snapshot installation a crash interrupted between the restore's
	 * two renames (no {@code db/}; {@code db.restore-ready} holds the complete newer database, {@code db.restore-old}
	 * the old one) exports the newer database with 0, as a start of the node would swap it in, and leaves it in
	 * {@code db/}. While the node holds its Raft lock, it is refused with the lock's message and nothing is touched
	 * (R32-5). Before, it failed at once with "No database in", and the operator would seed from another replica.
	 */
	@Test
	void exportSeedFinishesARestoreInterruptedBetweenItsRenames() throws IOException {
		Path dataDir = temp.resolve("node");
		NodeStoreTest.writeRestoreInterruptedDuringTheSwap(dataDir, true);
		Path groupDir = Files.createDirectories(dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID));
		Path lockFile = groupDir.resolve("in_use.lock");
		Path seed = temp.resolve("seed");
		try (var channel = java.nio.channels.FileChannel.open(lockFile, java.nio.file.StandardOpenOption.CREATE,
				java.nio.file.StandardOpenOption.WRITE); var lock = channel.lock()) {
			var refused = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
			assertEquals(1, refused.code(), refused.toString());
			assertTrue(refused.err().contains(lockFile + " is locked: stop the node first"), refused.err());
			assertTrue(Files.notExists(seed));
			assertTrue(Files.notExists(dataDir.resolve("db")));
			assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-ready").resolve("CURRENT")));
			assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-old").resolve("CURRENT")));
		}
		var exported = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(0, exported.code(), exported.toString());
		assertTrue(exported.out().contains("applied up to index 40 of term 4"), exported.out());
		assertTrue(Files.isRegularFile(seed.resolve("CURRENT")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db").resolve("CURRENT")));
		assertTrue(Files.notExists(dataDir.resolve("db.restore-ready")));
		assertTrue(Files.notExists(dataDir.resolve("db.restore-old")));
	}

	/**
	 * R34-2: {@code export-seed --target DATA-DIR/db} of a node whose restore a crash interrupted between its renames (no
	 * {@code db/}, {@code db.restore-ready} at 4:40, {@code db.restore-old}) is refused with 1 before anything is touched,
	 * and the same node then exports into a proper target. Before, every pre-check passed, the open swapped the ready
	 * database into {@code db/} and deleted {@code db.restore-old}, the checkpoint failed as its target existed, and the
	 * failure cleanup deleted {@code db/}: the node was left with no database at all.
	 */
	@Test
	void exportSeedRefusesTheNodesOwnMissingDbAsTarget() throws IOException {
		Path dataDir = temp.resolve("node");
		NodeStoreTest.writeRestoreInterruptedDuringTheSwap(dataDir, true);
		Files.createDirectories(dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID));
		var refused = run("export-seed", "--data-dir", dataDir.toString(), "--target",
				dataDir.resolve("db").toString());
		assertEquals(1, refused.code(), refused.toString());
		assertInstanceOf(IOException.class, refused.thrown());
		assertTrue(refused.thrown().getMessage().contains("a directory of the node being exported"),
				refused.thrown().getMessage());
		assertTrue(Files.notExists(dataDir.resolve("db")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-ready").resolve("CURRENT")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-old").resolve("CURRENT")));

		Path seed = temp.resolve("seed");
		var exported = run("export-seed", "--data-dir", dataDir.toString(), "--target", seed.toString());
		assertEquals(0, exported.code(), exported.toString());
		assertTrue(exported.out().contains("applied up to index 40 of term 4"), exported.out());
	}

	/**
	 * A stopped node in {@code dataDir}: {@code db/} at 1:5, and in raft/{@code group} a snapshot at 4:40 as the node
	 * takes one (a checkpoint and its position, fsynced before the rename: no digest to verify).
	 */
	private static void writeNodeBehindASnapshot(Path dataDir, String group) throws IOException {
		Path snapshot = dataDir.resolve("raft").resolve(group).resolve("sm").resolve("snapshots")
				.resolve("snapshot-4-40");
		try (var node = NodeStore.open(dataDir.resolve("db"), null, "node")) {
			long column = node.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			NodeStoreTest.put(node, column, 7, "old");
			NodeStoreTest.setApplied(node, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 5));
			Files.createDirectories(snapshot);
			NodeStoreTest.put(node, column, 7, "new");
			NodeStoreTest.setApplied(node, org.apache.ratis.server.protocol.TermIndex.valueOf(4, 40));
			node.checkpoint(snapshot.resolve("db"));
			NodeStoreTest.put(node, column, 7, "old");
			NodeStoreTest.setApplied(node, org.apache.ratis.server.protocol.TermIndex.valueOf(1, 5));
		}
		Files.writeString(snapshot.resolve("snapshot.properties"), "term=4\nindex=40\n");
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
