package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.NodeStoreTest.DATA_COLUMN;
import static it.cavallium.rockserver.distributed.NodeStoreTest.DATA_SCHEMA;
import static it.cavallium.rockserver.distributed.NodeStoreTest.contents;
import static it.cavallium.rockserver.distributed.NodeStoreTest.delete;
import static it.cavallium.rockserver.distributed.NodeStoreTest.put;
import static it.cavallium.rockserver.distributed.NodeStoreTest.setApplied;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileChunkReader;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.SizeInBytes;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CheckpointStorageTest {

	@TempDir
	Path tempDir;

	private NodeStore store;
	private long column;
	private final List<RaftStorage> raftStorages = new ArrayList<>();

	@BeforeEach
	void openStore() throws IOException {
		store = NodeStore.open(tempDir.resolve("db"), null, "node");
		column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
	}

	@AfterEach
	void closeAll() throws IOException {
		store.close();
		closeRaftStorages();
	}

	@Test
	void emptyStorageHasNoSnapshot() throws IOException {
		var storage = initStorage(tempDir.resolve("raft"), RaftStorage.StartupOption.FORMAT);
		assertNull(storage.getLatestSnapshot());
		assertTrue(Files.isDirectory(storage.getTmpDir().toPath()));
	}

	@Test
	void takeKeepsTheLatestAndThePreviousSnapshot() throws IOException {
		var storage = initStorage(tempDir.resolve("raft"), RaftStorage.StartupOption.FORMAT);

		put(store, column, 1, "first");
		setApplied(store, TermIndex.valueOf(1, 10));
		assertEquals(TermIndex.valueOf(1, 10), storage.take(store));
		assertEquals(TermIndex.valueOf(1, 10), storage.getLatestSnapshot().getTermIndex());

		put(store, column, 2, "second");
		setApplied(store, TermIndex.valueOf(2, 20));
		assertEquals(TermIndex.valueOf(2, 20), storage.take(store));

		put(store, column, 3, "third");
		setApplied(store, TermIndex.valueOf(2, 30));
		assertEquals(TermIndex.valueOf(2, 30), storage.take(store));

		FileListSnapshotInfo latest = storage.getLatestSnapshot();
		assertEquals(TermIndex.valueOf(2, 30), latest.getTermIndex());
		assertListsExactlyTheSnapshotFiles(storage, latest);
		assertEquals(Set.of("snapshot-2-20", "snapshot-2-30"), fileNames(storage.getSnapshotDir().toPath()));
		Path stateMachineDir = storage.getSnapshotDir().toPath().getParent();
		assertEquals(Set.of("snapshots", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of(), fileNames(storage.getTmpDir().toPath()));

		Path restored = tempDir.resolve("restored");
		NodeStore.restore(storage.snapshotDbDir(), restored);
		try (var restoredStore = NodeStore.open(restored, null, "restored")) {
			assertEquals(TermIndex.valueOf(2, 30), restoredStore.applied());
			assertEquals(Map.of(1, "first", 2, "second", 3, "third"),
					contents(restoredStore, restoredStore.db().getColumnId(DATA_COLUMN)));
		}
	}

	@Test
	void takingAgainWithoutNewEntriesKeepsTheSameSnapshotFiles() throws IOException {
		var storage = initStorage(tempDir.resolve("raft"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 1, "value");
		setApplied(store, TermIndex.valueOf(1, 10));
		storage.take(store);
		FileListSnapshotInfo first = storage.getLatestSnapshot();

		assertEquals(TermIndex.valueOf(1, 10), storage.take(store));

		assertEquals(paths(first), paths(storage.getLatestSnapshot()));
		assertListsExactlyTheSnapshotFiles(storage, first);
	}

	@Test
	void snapshotSurvivesReinitFromDisk() throws IOException {
		Path raftDir = tempDir.resolve("raft");
		var storage = initStorage(raftDir, RaftStorage.StartupOption.FORMAT);
		put(store, column, 1, "value");
		setApplied(store, TermIndex.valueOf(3, 30));
		storage.take(store);
		List<Path> files = paths(storage.getLatestSnapshot());
		closeRaftStorages();

		var reopened = initStorage(raftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(3, 30), reopened.getLatestSnapshot().getTermIndex());
		assertEquals(files, paths(reopened.getLatestSnapshot()));
	}

	@Test
	void installStreamedWhileANewerSnapshotIsTakenStaysConsistent() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		for (int i = 0; i < 10; i++) {
			put(store, column, i, "old" + i);
		}
		setApplied(store, TermIndex.valueOf(1, 10));
		leader.take(store);
		// A leader starts installing this snapshot on a follower...
		FileListSnapshotInfo streamed = leader.getLatestSnapshot();

		// ...and takes a newer one before the transfer ends, after a compaction replaced every table file.
		for (int i = 0; i < 10; i++) {
			delete(store, column, i);
		}
		put(store, column, 100, "new");
		store.db().flush();
		store.db().compact();
		setApplied(store, TermIndex.valueOf(2, 20));
		leader.take(store);

		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);
		install(leader, streamed, follower);
		follower.reload();

		assertEquals(TermIndex.valueOf(1, 10), follower.getLatestSnapshot().getTermIndex());
		Path restored = tempDir.resolve("restored");
		NodeStore.restore(follower.snapshotDbDir(), restored);
		try (var restoredStore = NodeStore.open(restored, null, "restored")) {
			assertEquals(TermIndex.valueOf(1, 10), restoredStore.applied());
			assertEquals(10, contents(restoredStore, restoredStore.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	@Test
	void installedSnapshotIsListedWithoutRatisDigestFiles() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);

		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);
		assertNull(follower.getLatestSnapshot());

		follower.reload();

		FileListSnapshotInfo installed = follower.getLatestSnapshot();
		assertNotNull(installed);
		assertEquals(TermIndex.valueOf(4, 40), installed.getTermIndex());
		assertEquals(leader.getLatestSnapshot().getFiles().size(), installed.getFiles().size());
		assertTrue(installed.getFiles().stream().noneMatch(file -> file.getPath().toString().endsWith(".md5")));

		Path restored = tempDir.resolve("restored");
		NodeStore.restore(follower.snapshotDbDir(), restored);
		try (var restoredStore = NodeStore.open(restored, null, "restored")) {
			assertEquals(TermIndex.valueOf(4, 40), restoredStore.applied());
			assertEquals(Map.of(7, "installed"), contents(restoredStore, restoredStore.db().getColumnId(DATA_COLUMN)));
		}

		closeRaftStorages();
		var restarted = initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(paths(installed), paths(restarted.getLatestSnapshot()));
	}

	@Test
	void installedSnapshotDamagedByACrashFailsLoudly() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);

		// Power loss before the state machine reloaded: Ratis never synced the installed files, and one lost its data.
		Path table = firstTable(follower.getSnapshotDir().toPath());
		Files.write(table, new byte[0]);
		closeRaftStorages();

		// The Raft log may already start after the snapshot: the replica must not start as if it had none.
		var failure = assertThrows(IOException.class,
				() -> initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER));
		assertTrue(failure.getMessage().contains("snapshot-4-40"), failure.getMessage());
		assertTrue(Files.exists(table), "the damaged snapshot is kept for inspection");
	}

	/**
	 * R31-2: a crash after Ratis renamed a received snapshot into place, before the state machine verified it and
	 * restored it into {@code db/}, leaves {@code db/} behind the snapshot. A start of the node would restore it
	 * ({@code openStore}); {@code export-seed} on that stopped node does the same (verifying the snapshot first), so it
	 * copies and reports the snapshot's index, not the older one of {@code db/}.
	 */
	@Test
	void exportSeedRestoresAnInstalledSnapshotAheadOfTheDatabase() throws IOException {
		Path dataDir = tempDir.resolve("node");
		Path groupDir = writeNodeBehindAnInstalledSnapshot(dataDir);
		Path snapshot = groupDir.resolve("sm").resolve("snapshots").resolve("snapshot-4-40");
		assertTrue(Files.notExists(snapshot.resolve("installed.verified")), "not verified yet");

		Path target = tempDir.resolve("seed");
		var exported = NodeStore.exportSeed(dataDir.resolve("db"), groupDir.resolve("sm"), null, target);
		assertEquals(TermIndex.valueOf(4, 40), exported.applied());
		assertEquals(TermIndex.valueOf(4, 40), exported.restoredSnapshot());
		assertNull(exported.damagedSnapshot());
		assertTrue(Files.exists(snapshot.resolve("installed.verified")), "verified as a start verifies it");
		try (var seed = NodeStore.open(target, null, "seed")) {
			assertEquals(Map.of(7, "installed"), contents(seed, seed.db().getColumnId(DATA_COLUMN)));
		}
		// The node's own database was brought up to the snapshot, as its next start would have.
		try (var node = NodeStore.open(dataDir.resolve("db"), null, "node")) {
			assertEquals(TermIndex.valueOf(4, 40), node.applied());
		}
	}

	/**
	 * R31-2, R32-1: a snapshot that a crash damaged before it was verified cannot be used. {@code export-seed} refuses
	 * by default, naming the snapshot's index and the database's, since the replica may have applied entries up to the
	 * snapshot that its database lacks; told to export without it, it copies what {@code db/} holds and says why.
	 */
	@Test
	void exportSeedWithADamagedInstalledSnapshotRefusesUnlessToldToExportTheDatabase() throws IOException {
		Path dataDir = tempDir.resolve("node");
		Path groupDir = writeNodeBehindAnInstalledSnapshot(dataDir);
		Files.write(firstTable(groupDir.resolve("sm").resolve("snapshots")), new byte[0]);

		Path target = tempDir.resolve("seed");
		var refused = assertThrows(NodeStore.DamagedSnapshotNotExportedException.class,
				() -> NodeStore.exportSeed(dataDir.resolve("db"), groupDir.resolve("sm"), null, target));
		assertEquals(40, refused.snapshotIndex());
		assertEquals(TermIndex.valueOf(1, 5), refused.databaseApplied());
		assertTrue(refused.getMessage().contains("index 40") && refused.getMessage().contains("index 5"),
				refused.getMessage());
		assertTrue(!refused.getMessage().contains("rebuild it from the other members"), refused.getMessage());
		assertTrue(Files.notExists(target), "nothing written");

		var exported = NodeStore.exportSeed(dataDir.resolve("db"), groupDir.resolve("sm"), null, target, true);
		assertEquals(TermIndex.valueOf(1, 5), exported.applied());
		assertNull(exported.restoredSnapshot());
		assertNotNull(exported.damagedSnapshot());
		assertTrue(exported.damagedSnapshot().contains("snapshot-4-40"), exported.damagedSnapshot());
		assertEquals(40, exported.damagedSnapshotIndex());
		try (var seed = NodeStore.open(target, null, "seed")) {
			assertEquals(Map.of(7, "old"), contents(seed, seed.db().getColumnId(DATA_COLUMN)));
		}
	}

	/** A stopped node in {@code dataDir} whose {@code db/} is at 1:5 and whose received snapshot at 4:40 is unverified. */
	private Path writeNodeBehindAnInstalledSnapshot(Path dataDir) throws IOException {
		try (var node = NodeStore.open(dataDir.resolve("db"), null, "node")) {
			long nodeColumn = node.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(node, nodeColumn, 7, "old");
			setApplied(node, TermIndex.valueOf(1, 5));
		}
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path groupDir = dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID);
		var follower = initStorage(groupDir, RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);
		closeRaftStorages();
		return groupDir;
	}

	@Test
	void installedSnapshotThatLostItsPositionFailsLoudly() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);

		// Power loss lost the directory entry of the last file Ratis wrote.
		Path installed = follower.getSnapshotDir().toPath().resolve("snapshot-4-40");
		Files.delete(installed.resolve("snapshot.properties"));
		Files.delete(installed.resolve("snapshot.properties.md5"));

		assertThrows(IOException.class, follower::reload);
	}

	/**
	 * R52-1: a crash while an old installed snapshot is deleted. {@code NodeStore.deleteRecursively} deletes in reverse
	 * path order, so {@code snapshot.properties.md5}, {@code snapshot.properties} and {@code installed.verified} go
	 * before anything under {@code db/}, where Ratis' {@code .md5} side files stay: the leftover looks like an installed
	 * snapshot that lost its position. It is older than the latest snapshot, which is whole, so the start goes on with
	 * the latest and deletes the leftover instead of refusing to start.
	 */
	@Test
	void anOldInstalledSnapshotPartlyDeletedByACrashIsDeletedAtTheStart() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);
		follower.reload();
		setApplied(store, TermIndex.valueOf(5, 100));
		follower.take(store);
		Path snapshotsDir = follower.getSnapshotDir().toPath();
		assertEquals(Set.of("snapshot-4-40", "snapshot-5-100"), fileNames(snapshotsDir));

		Path old = snapshotsDir.resolve("snapshot-4-40");
		Files.delete(old.resolve("snapshot.properties.md5"));
		Files.delete(old.resolve("snapshot.properties"));
		Files.delete(old.resolve("installed.verified"));
		try (Stream<Path> walk = Files.walk(old.resolve("db"))) {
			assertTrue(walk.anyMatch(file -> file.toString().endsWith(".md5")), "Ratis' side files are still there");
		}
		closeRaftStorages();

		var restarted = initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(5, 100), restarted.getLatestSnapshot().getTermIndex());
		assertListsExactlyTheSnapshotFiles(restarted, restarted.getLatestSnapshot());
		assertEquals(Set.of("snapshot-5-100"), fileNames(snapshotsDir));
	}

	/**
	 * R52-1: an installed snapshot that lost its position stays fatal when it is the newest one, even with an older
	 * whole snapshot next to it: the Raft log may start after the damaged one.
	 */
	@Test
	void aDamagedInstalledSnapshotNewerThanAWholeOneStillFailsLoudly() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(1, 5));
		follower.take(store);
		Path snapshotsDir = follower.getSnapshotDir().toPath();
		install(leader, leader.getLatestSnapshot(), follower, file -> {
		}, false);
		Path aside = snapshotsDir.resolveSibling(snapshotsDir.getFileName() + ".tmp_installing");
		Files.move(aside.resolve("snapshot-1-5"), snapshotsDir.resolve("snapshot-1-5"));
		NodeStore.deleteRecursively(aside);
		Path installed = snapshotsDir.resolve("snapshot-4-40");
		Files.delete(installed.resolve("snapshot.properties"));
		Files.delete(installed.resolve("snapshot.properties.md5"));
		closeRaftStorages();

		var failure = assertThrows(CheckpointStorage.DamagedSnapshotException.class,
				() -> initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER));
		assertTrue(failure.getMessage().contains("snapshot-4-40"), failure.getMessage());
		assertTrue(Files.isDirectory(installed), "the damaged snapshot is kept for inspection");
	}

	/**
	 * R52-1: an unused snapshot is moved out of {@code snapshots/} in one rename before its files are deleted, so a
	 * crash during the delete never leaves part of one there; the start deletes what is left in {@code tmp/}.
	 */
	@Test
	void anUnusedSnapshotLeavesTheSnapshotDirectoryWholeBeforeItsFilesAreDeleted() throws IOException {
		Path raftDir = tempDir.resolve("raft");
		var storage = initStorage(raftDir, RaftStorage.StartupOption.FORMAT);
		Path snapshotsDir = storage.getSnapshotDir().toPath();
		put(store, column, 1, "first");
		setApplied(store, TermIndex.valueOf(1, 10));
		storage.take(store);
		setApplied(store, TermIndex.valueOf(2, 20));
		storage.take(store);
		setApplied(store, TermIndex.valueOf(2, 30));
		var realHalt = CrashPoints.halt;
		CrashPoints.halt = code -> {
			throw new HaltedForTest();
		};
		try {
			CrashPoints.arm(CrashPoints.Point.SNAPSHOT_DELETING, 0);
			assertThrows(HaltedForTest.class, () -> storage.take(store));
		} finally {
			CrashPoints.disarm();
			CrashPoints.halt = realHalt;
		}
		assertEquals(Set.of("snapshot-2-20", "snapshot-2-30"), fileNames(snapshotsDir));
		closeRaftStorages();

		var restarted = initStorage(raftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(2, 30), restarted.getLatestSnapshot().getTermIndex());
		// A start keeps the latest snapshot only.
		assertEquals(Set.of("snapshot-2-30"), fileNames(snapshotsDir));
		assertEquals(Set.of(), fileNames(restarted.getTmpDir().toPath()));
	}

	/**
	 * R53-1: a follower's snapshot whose checkpoint was built before Ratis swapped in an installed snapshot directory
	 * ({@code SnapshotManager.finalizeSnapshot} after {@code pause()}, which does not wait for a running take) moves into
	 * the installed directory and must not delete the installed snapshot, which is newer than it: the reload after the
	 * install then finds the installed one. Before, {@code deleteUnusedSnapshots} deleted it (neither the latest nor the
	 * previous one), the reload found the follower's own older snapshot, and the replica kept its database while Ratis
	 * had purged the log up to the installed index: a silent stall.
	 */
	@Test
	void aSnapshotTakenWhileAnInstallSwapsTheDirectoryKeepsTheInstalledOne() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 1, "local");
		setApplied(store, TermIndex.valueOf(1, 10));
		follower.take(store);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		var installed = leader.getLatestSnapshot();
		setApplied(store, TermIndex.valueOf(2, 20));
		var realHalt = CrashPoints.halt;
		// The hook: once the follower's checkpoint is built, the install's last chunk swaps the snapshot directory.
		CrashPoints.halt = code -> {
			try {
				install(leader, installed, follower);
			} catch (IOException e) {
				throw new java.io.UncheckedIOException(e);
			}
		};
		try {
			CrashPoints.arm(CrashPoints.Point.SNAPSHOT_BUILT, 0);
			follower.take(store);
		} finally {
			CrashPoints.disarm();
			CrashPoints.halt = realHalt;
		}
		Path snapshotsDir = follower.getSnapshotDir().toPath();
		assertTrue(fileNames(snapshotsDir).contains("snapshot-4-40"), "the installed snapshot stays: "
				+ fileNames(snapshotsDir));

		follower.reload();
		assertEquals(TermIndex.valueOf(4, 40), follower.getLatestSnapshot().getTermIndex());
		assertListsExactlyTheSnapshotFiles(follower, follower.getLatestSnapshot());
		// The next snapshot after the install deletes what the racing take left.
		setApplied(store, TermIndex.valueOf(4, 50));
		follower.take(store);
		setApplied(store, TermIndex.valueOf(4, 60));
		follower.take(store);
		assertEquals(Set.of("snapshot-4-50", "snapshot-4-60"), fileNames(snapshotsDir));
	}

	/** What the replaced {@link CrashPoints#halt} throws, in place of halting the JVM. */
	private static final class HaltedForTest extends RuntimeException {
	}

	@Test
	void damagedInstallInterruptedBeforeTheOldSnapshotsWereDeletedFallsBackToThem() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(1, 5));
		follower.take(store);
		Path snapshotsDir = follower.getSnapshotDir().toPath();
		Path stateMachineDir = snapshotsDir.getParent();
		install(leader, leader.getLatestSnapshot(), follower, file -> {
		}, false);

		// Crash after Ratis renamed the installed snapshot into place, before it deleted the old snapshots.
		Files.write(firstTable(snapshotsDir), new byte[0]);
		closeRaftStorages();

		var restarted = initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(1, 5), restarted.getLatestSnapshot().getTermIndex());
		assertListsExactlyTheSnapshotFiles(restarted, restarted.getLatestSnapshot());
		assertEquals(Set.of("snapshots", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of("snapshot-1-5"), fileNames(snapshotsDir));
	}

	/**
	 * R51-2: a crash while Ratis' {@code deleteFully} walks the old snapshots moved aside leaves them partly deleted,
	 * {@code snapshot.properties} and a {@code db/} still there. With the received snapshot damaged, the aside is
	 * promoted only if its checkpoint is complete: here one table file is gone (CURRENT and the MANIFEST are not), so
	 * it is discarded, and the node has no snapshot rather than an incomplete one it could stream to a follower.
	 */
	@Test
	void anAsidePartlyDeletedByACrashIsNotPromoted() throws IOException {
		Path stateMachineDir = damagedInstallWithTheOldSnapshotsAside();
		Path aside = stateMachineDir.resolve("snapshots.tmp_installing");
		Files.delete(firstTable(aside));
		closeRaftStorages();

		var restarted = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.RECOVER);
		assertNull(restarted.getLatestSnapshot());
		assertEquals(Set.of("snapshots", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of(), fileNames(stateMachineDir.resolve("snapshots")));
	}

	/**
	 * R51-2: the same with {@code CURRENT} gone from the aside's checkpoint, and no file list in its
	 * {@code snapshot.properties} (a snapshot taken by an older version): not promoted either.
	 */
	@Test
	void anAsideWithoutCurrentIsNotPromoted() throws IOException {
		Path stateMachineDir = damagedInstallWithTheOldSnapshotsAside();
		Path aside = stateMachineDir.resolve("snapshots.tmp_installing");
		Path snapshot = aside.resolve("snapshot-1-5");
		Files.writeString(snapshot.resolve("snapshot.properties"), "term=1\nindex=5\n");
		Files.delete(snapshot.resolve("db").resolve("CURRENT"));
		closeRaftStorages();

		var restarted = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.RECOVER);
		assertNull(restarted.getLatestSnapshot());
		assertEquals(Set.of(), fileNames(stateMachineDir.resolve("snapshots")));
	}

	/**
	 * R51-2: an aside taken by an older version (no file list) whose checkpoint is whole is still promoted, as before.
	 */
	@Test
	void aWholeAsideWithoutAFileListIsPromoted() throws IOException {
		Path stateMachineDir = damagedInstallWithTheOldSnapshotsAside();
		Files.writeString(stateMachineDir.resolve("snapshots.tmp_installing").resolve("snapshot-1-5")
				.resolve("snapshot.properties"), "term=1\nindex=5\n");
		closeRaftStorages();

		var restarted = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(1, 5), restarted.getLatestSnapshot().getTermIndex());
		assertEquals(Set.of("snapshot-1-5"), fileNames(stateMachineDir.resolve("snapshots")));
	}

	/**
	 * A follower with snapshot 1-5 that received snapshot 4-40, crashed after the received directory took the place of
	 * {@code snapshots/} and before the old one, moved aside, was deleted; the received one lost data. Returns the
	 * follower's state machine directory (its raft directory is {@code follower}).
	 */
	private Path damagedInstallWithTheOldSnapshotsAside() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(1, 5));
		follower.take(store);
		Path snapshotsDir = follower.getSnapshotDir().toPath();
		install(leader, leader.getLatestSnapshot(), follower, file -> {
		}, false);
		Files.write(firstTable(snapshotsDir), new byte[0]);
		return snapshotsDir.getParent();
	}

	@Test
	void installReadingASnapshotKeepsItUntilItStopsReading() throws IOException {
		var clock = new AtomicLong();
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT, clock);
		for (int i = 0; i < 10; i++) {
			put(store, column, i, "old" + i);
		}
		setApplied(store, TermIndex.valueOf(1, 10));
		leader.take(store);
		FileListSnapshotInfo streamed = leader.getLatestSnapshot();
		assertTrue(streamed.getFiles().size() > 4);
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);

		// A long installation reads the snapshot file by file while the leader takes a newer snapshot before each of
		// the next three files, every time after a compaction replaced every table file.
		install(leader, streamed, follower, file -> {
			if (file >= 1 && file <= 3) {
				int round = file + 1;
				clock.addAndGet(Duration.ofMinutes(5).toNanos());
				for (int i = 0; i < 10; i++) {
					put(store, column, i, "round" + round);
				}
				store.db().flush();
				store.db().compact();
				setApplied(store, TermIndex.valueOf(round, round * 10L));
				leader.take(store);
			}
		}, true);
		follower.reload();

		assertEquals(TermIndex.valueOf(1, 10), follower.getLatestSnapshot().getTermIndex());
		Path restored = tempDir.resolve("restored");
		NodeStore.restore(follower.snapshotDbDir(), restored);
		try (var restoredStore = NodeStore.open(restored, null, "restored")) {
			assertEquals(TermIndex.valueOf(1, 10), restoredStore.applied());
			Map<Integer, String> restoredContents = contents(restoredStore, restoredStore.db().getColumnId(DATA_COLUMN));
			assertEquals(10, restoredContents.size());
			assertEquals("old3", restoredContents.get(3));
		}
		Path leaderSnapshots = leader.getSnapshotDir().toPath();
		assertEquals(Set.of("snapshot-1-10", "snapshot-3-30", "snapshot-4-40"), fileNames(leaderSnapshots));

		// Once nothing read it for longer than the lease, the next snapshot deletes it.
		clock.addAndGet(CheckpointStorage.DEFAULT_INSTALL_READ_LEASE.plusSeconds(1).toNanos());
		put(store, column, 200, "newest");
		setApplied(store, TermIndex.valueOf(5, 50));
		leader.take(store);
		assertEquals(Set.of("snapshot-4-40", "snapshot-5-50"), fileNames(leaderSnapshots));
	}

	@Test
	void readerOfThePreviousSnapshotCanReadItAfterANewTake() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 1, "first");
		setApplied(store, TermIndex.valueOf(1, 10));
		leader.take(store);
		// The installation got the snapshot but has not read its file list yet.
		FileListSnapshotInfo streamed = leader.getLatestSnapshot();

		put(store, column, 2, "second");
		store.db().flush();
		store.db().compact();
		setApplied(store, TermIndex.valueOf(1, 20));
		leader.take(store);

		for (FileInfo file : streamed.getFiles()) {
			assertEquals(file.getFileSize(), Files.readAllBytes(file.getPath()).length);
		}
	}

	@Test
	void newInstallDiscardsThePartialCopiesOfReplacedOnes() throws IOException {
		var clock = new AtomicLong();
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT, clock);
		Path tmp = follower.getTmpDir().toPath();
		Path failed = writePartialInstall(tmp, "snapshot-failed");
		follower.discardFailedInstalls();
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		follower.discardFailedInstalls();
		assertTrue(Files.exists(failed), "the newest installation is kept");

		// Ratis restarts the installation under a new request id and receives its first chunk.
		writePartialInstall(tmp, "snapshot-retried");
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		follower.discardFailedInstalls();
		awaitFileNames(tmp, Set.of("snapshot-retried"));

		// A stale request of the replaced installation writes one more chunk.
		writePartialInstall(tmp, "snapshot-failed");
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		follower.discardFailedInstalls();
		awaitFileNames(tmp, Set.of("snapshot-retried"));
	}

	/**
	 * R24-1: Ratis asks for the latest snapshot under its server monitor on every follower append that carries entries
	 * (RaftServerImpl.appendEntriesAsync, checkInconsistentAppendEntries, ServerState.getSnapshotIndex), and on the
	 * leader's appender threads. Answering must not touch the disk nor wait for the install lock: the cleanup of failed
	 * installations runs on the install cleanup thread only, and a hung disk there must not stop the Raft RPCs.
	 */
	@Test
	void latestSnapshotNeverScansTheInstallations() throws IOException {
		var clock = new AtomicLong();
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT, clock);
		Path tmp = follower.getTmpDir().toPath();
		Path replaced = writePartialInstall(tmp, "snapshot-replaced");
		follower.getLatestSnapshot();
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		writePartialInstall(tmp, "snapshot-newer");
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		follower.getLatestSnapshot();
		clock.addAndGet(CheckpointStorage.INSTALL_SCAN_INTERVAL.toNanos());
		follower.getLatestSnapshot();
		assertTrue(Files.exists(replaced), "getLatestSnapshot deleted the partial copy of a replaced installation");
	}

	/** R24-1: the install cleanup holding the install lock (in a walk of a hung disk) does not hold back Ratis. */
	@Test
	void latestSnapshotDoesNotWaitForTheInstallLock() throws Exception {
		var clock = new AtomicLong();
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT, clock);
		writePartialInstall(follower.getTmpDir().toPath(), "snapshot-installing");
		Object installsLock = follower.installsLock();
		var held = new java.util.concurrent.CountDownLatch(1);
		var release = new java.util.concurrent.CountDownLatch(1);
		Thread holder = Thread.ofPlatform().start(() -> {
			synchronized (installsLock) {
				held.countDown();
				try {
					release.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		});
		try {
			assertTrue(held.await(10, java.util.concurrent.TimeUnit.SECONDS));
			clock.addAndGet(CheckpointStorage.INSTALL_PROGRESS_INTERVAL.toNanos());
			var answer = java.util.concurrent.CompletableFuture.supplyAsync(follower::getLatestSnapshot,
					command -> Thread.ofPlatform().daemon().start(command));
			try {
				answer.get(5, java.util.concurrent.TimeUnit.SECONDS);
			} catch (java.util.concurrent.TimeoutException blocked) {
				throw new AssertionError("getLatestSnapshot waited for the install lock", blocked);
			}
		} finally {
			release.countDown();
			holder.join();
		}
	}

	/** DD-20: an installation that writes nothing more is deleted, without waiting for a newer one or a restart. */
	@Test
	void anInstallationThatStopsWritingIsDeleted() throws IOException {
		var clock = new AtomicLong();
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT, clock);
		Path tmp = follower.getTmpDir().toPath();
		Path install = writePartialInstall(tmp, "snapshot-stopped");
		long step = CheckpointStorage.INSTALL_PROGRESS_INTERVAL.toNanos();
		follower.discardFailedInstalls();
		// It goes on writing for a while: kept.
		for (int i = 0; i < 5; i++) {
			clock.addAndGet(step);
			Files.write(install.resolve("snapshot-1-10").resolve("db").resolve("0000" + (20 + i) + ".sst"), new byte[16]);
			follower.discardFailedInstalls();
		}
		assertTrue(Files.exists(install));
		// Then nothing more, for almost the limit: still kept.
		long idle = 0;
		while (idle + step < CheckpointStorage.INSTALL_ABANDONED_AFTER.toNanos()) {
			clock.addAndGet(step);
			idle += step;
			follower.discardFailedInstalls();
		}
		assertTrue(Files.exists(install), "not yet");
		clock.addAndGet(2 * step);
		follower.discardFailedInstalls();
		awaitFileNames(tmp, Set.of());
		// A new installation after that is a current one again.
		Path next = writePartialInstall(tmp, "snapshot-next");
		clock.addAndGet(step);
		follower.discardFailedInstalls();
		assertTrue(Files.exists(next));
	}

	@Test
	void initDeletesPartialInstalls() throws IOException {
		Path raftDir = tempDir.resolve("raft");
		var storage = initStorage(raftDir, RaftStorage.StartupOption.FORMAT);
		writePartialInstall(storage.getTmpDir().toPath(), "snapshot-" + UUID.randomUUID());
		closeRaftStorages();

		var reopened = initStorage(raftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(Set.of(), fileNames(reopened.getTmpDir().toPath()));
	}

	@Test
	void initRecoversTheSnapshotsOfAnInstallInterruptedByACrash() throws IOException {
		Path raftDir = tempDir.resolve("raft");
		var storage = initStorage(raftDir, RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(1, 5));
		storage.take(store);
		Path snapshotsDir = storage.getSnapshotDir().toPath();
		Path stateMachineDir = snapshotsDir.getParent();
		closeRaftStorages();

		// Crash after Ratis moved the snapshots aside, before the installed ones took their place.
		Files.move(snapshotsDir, stateMachineDir.resolve("snapshots.tmp_2026_01_01"));
		Path installing = stateMachineDir.resolve("tmp").resolve("snapshot-" + UUID.randomUUID());
		Files.createDirectories(installing.resolve("snapshot-2-9").resolve("db"));

		var reopened = initStorage(raftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(1, 5), reopened.getLatestSnapshot().getTermIndex());
		assertListsExactlyTheSnapshotFiles(reopened, reopened.getLatestSnapshot());
		assertEquals(Set.of("snapshots", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of(), fileNames(stateMachineDir.resolve("tmp")));
	}

	/**
	 * Transfers {@code snapshot} as Ratis 3.3.1 installs a snapshot: the leader reads each listed file with Ratis'
	 * {@link FileChunkReader} (path and size as listed, opened when reached), the follower writes it into a new
	 * directory at its path relative to the snapshot directory and saves its digest next to it, then that directory
	 * replaces the follower's snapshot directory.
	 */
	private static void install(CheckpointStorage leader, FileListSnapshotInfo snapshot, CheckpointStorage follower)
			throws IOException {
		install(leader, snapshot, follower, file -> {
		}, true);
	}

	/**
	 * Like {@link #install(CheckpointStorage, FileListSnapshotInfo, CheckpointStorage)}, running {@code beforeFile}
	 * with the index of every file before sending it. Without {@code completed}, it stops as a crash would right after the received
	 * directory took the place of the snapshot directory, which is left aside.
	 */
	private static void install(CheckpointStorage leader, FileListSnapshotInfo snapshot, CheckpointStorage follower,
			BeforeFile beforeFile, boolean completed) throws IOException {
		Path leaderDir = leader.getSnapshotDir().toPath();
		Path installing = follower.getTmpDir().toPath().resolve("snapshot-" + UUID.randomUUID());
		int fileCount = snapshot.getFiles().size();
		for (int i = 0; i < fileCount; i++) {
			beforeFile.run(i);
			FileInfo file = snapshot.getFiles().get(i);
			Path relative = leaderDir.relativize(file.getPath());
			Path target = installing.resolve(relative);
			Files.createDirectories(target.getParent());
			try (var reader = FileChunkReader.newInstance(file, relative);
					var out = Files.newOutputStream(target)) {
				FileChunkProto chunk;
				do {
					// Ratis reads the file list again for every chunk.
					snapshot.getFiles();
					chunk = reader.readFileChunk(16 * 1024);
					chunk.getData().writeTo(out);
				} while (!chunk.getDone());
				MD5FileUtil.saveMD5File(target.toFile(), MD5Hash.newInstance(chunk.getFileDigest().toByteArray()));
			}
		}
		Path followerDir = follower.getSnapshotDir().toPath();
		Path aside = followerDir.resolveSibling(followerDir.getFileName() + ".tmp_installing");
		if (Files.exists(followerDir)) {
			Files.move(followerDir, aside);
		}
		Files.move(installing, followerDir);
		if (completed) {
			NodeStore.deleteRecursively(aside);
		}
	}

	private interface BeforeFile {
		void run(int index) throws IOException;
	}

	private CheckpointStorage initStorage(Path dir, RaftStorage.StartupOption option) throws IOException {
		return initStorage(dir, option, null);
	}

	/** With a {@code clock}, the storage uses it as its monotonic clock instead of {@link System#nanoTime()}. */
	private CheckpointStorage initStorage(Path dir, RaftStorage.StartupOption option, @Nullable AtomicLong clock)
			throws IOException {
		RaftStorage raftStorage = RaftStorage.newBuilder()
				.setDirectory(dir.toFile())
				.setOption(option)
				.setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.getDefault())
				.setStorageFreeSpaceMin(SizeInBytes.ZERO)
				.build();
		raftStorage.initialize();
		raftStorages.add(raftStorage);
		var storage = clock == null ? new CheckpointStorage()
				: new CheckpointStorage(CheckpointStorage.DEFAULT_INSTALL_READ_LEASE, clock::get);
		storage.init(raftStorage);
		return storage;
	}

	private void closeRaftStorages() throws IOException {
		for (RaftStorage raftStorage : raftStorages) {
			raftStorage.close();
		}
		raftStorages.clear();
	}

	/** Every listed file exists in the snapshot's directory, and every file there except digests is listed. */
	private static void assertListsExactlyTheSnapshotFiles(CheckpointStorage storage, FileListSnapshotInfo snapshot)
			throws IOException {
		Path snapshotDir = storage.snapshotDbDir().getParent();
		Set<Path> onDisk;
		try (Stream<Path> walk = Files.walk(snapshotDir)) {
			onDisk = walk.filter(Files::isRegularFile).collect(Collectors.toSet());
		}
		Set<Path> listed = Set.copyOf(paths(snapshot));
		assertEquals(onDisk, listed);
		for (FileInfo file : snapshot.getFiles()) {
			assertTrue(file.getPath().isAbsolute(), file.getPath().toString());
			assertEquals(Files.size(file.getPath()), file.getFileSize());
		}
		assertTrue(listed.contains(snapshotDir.resolve("snapshot.properties")));
		assertTrue(listed.contains(snapshotDir.resolve("db").resolve("CURRENT")));
	}

	private static Path firstTable(Path snapshotsDir) throws IOException {
		try (Stream<Path> walk = Files.walk(snapshotsDir)) {
			return walk.filter(file -> file.toString().endsWith(".sst")).sorted().findFirst().orElseThrow();
		}
	}

	/** A directory like the one Ratis receives a snapshot into, after a few chunks. */
	private static Path writePartialInstall(Path tmp, String name) throws IOException {
		Path dir = tmp.resolve(name);
		Path table = dir.resolve("snapshot-1-10").resolve("db").resolve("000010.sst");
		Files.createDirectories(table.getParent());
		Files.write(table, new byte[4096]);
		return dir;
	}

	/** Partial copies are deleted in the background, after being renamed out of the way. */
	private static void awaitFileNames(Path dir, Set<String> expected) throws IOException {
		long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
		while (!fileNames(dir).equals(expected) && System.nanoTime() - deadline < 0) {
			Thread.onSpinWait();
		}
		assertEquals(expected, fileNames(dir));
	}

	private static List<Path> paths(FileListSnapshotInfo snapshot) {
		return snapshot.getFiles().stream().map(FileInfo::getPath).toList();
	}

	private static Set<String> fileNames(Path dir) throws IOException {
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(file -> file.getFileName().toString()).collect(Collectors.toSet());
		}
	}
}
