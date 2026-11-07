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
import java.util.function.IntConsumer;
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
 * (exit 1).
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
		var again = run(admin(survivors, "add-peer", newPeer));
		assertEquals(0, again.code(), again.toString());
		assertTrue(again.out().strip().endsWith("OK"), again.out());

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
		var leader = leader();
		leader.connection().getSyncApi(RequestContext.batch(TIMEOUT)).put(0, column, key(2), value("v2"),
				RequestType.none());
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

	@Test
	void noCommandAndAnUnknownCommandExitWithTwo() {
		assertEquals(2, run().code());
		var unknown = run("serve");
		assertEquals(2, unknown.code());
		assertTrue(unknown.err().contains("unknown command serve"), unknown.err());
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
