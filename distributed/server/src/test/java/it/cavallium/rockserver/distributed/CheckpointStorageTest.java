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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
	void installedSnapshotDamagedByACrashIsNotUsed() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);
		Path followerRaftDir = tempDir.resolve("follower");
		var follower = initStorage(followerRaftDir, RaftStorage.StartupOption.FORMAT);
		install(leader, leader.getLatestSnapshot(), follower);

		// Power loss before the state machine reloaded: Ratis never synced the installed files, and one lost its data.
		Path table;
		try (Stream<Path> walk = Files.walk(follower.getSnapshotDir().toPath())) {
			table = walk.filter(file -> file.toString().endsWith(".sst")).findFirst().orElseThrow();
		}
		Files.write(table, new byte[0]);
		closeRaftStorages();

		var restarted = initStorage(followerRaftDir, RaftStorage.StartupOption.RECOVER);
		assertNull(restarted.getLatestSnapshot());
		assertEquals(Set.of(), fileNames(restarted.getSnapshotDir().toPath()));
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
		Path leaderDir = leader.getSnapshotDir().toPath();
		Path installing = follower.getTmpDir().toPath().resolve("snapshot-" + UUID.randomUUID());
		for (FileInfo file : snapshot.getFiles()) {
			Path relative = leaderDir.relativize(file.getPath());
			Path target = installing.resolve(relative);
			Files.createDirectories(target.getParent());
			try (var reader = FileChunkReader.newInstance(file, relative);
					var out = Files.newOutputStream(target)) {
				FileChunkProto chunk;
				do {
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
		NodeStore.deleteRecursively(aside);
	}

	private CheckpointStorage initStorage(Path dir, RaftStorage.StartupOption option) throws IOException {
		RaftStorage raftStorage = RaftStorage.newBuilder()
				.setDirectory(dir.toFile())
				.setOption(option)
				.setLogCorruptionPolicy(RaftServerConfigKeys.Log.CorruptionPolicy.getDefault())
				.setStorageFreeSpaceMin(SizeInBytes.ZERO)
				.build();
		raftStorage.initialize();
		raftStorages.add(raftStorage);
		var storage = new CheckpointStorage();
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

	private static List<Path> paths(FileListSnapshotInfo snapshot) {
		return snapshot.getFiles().stream().map(FileInfo::getPath).toList();
	}

	private static Set<String> fileNames(Path dir) throws IOException {
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(file -> file.getFileName().toString()).collect(Collectors.toSet());
		}
	}
}
