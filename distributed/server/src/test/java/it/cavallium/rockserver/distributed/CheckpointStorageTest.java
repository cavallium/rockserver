package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.NodeStoreTest.DATA_COLUMN;
import static it.cavallium.rockserver.distributed.NodeStoreTest.DATA_SCHEMA;
import static it.cavallium.rockserver.distributed.NodeStoreTest.contents;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
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
		for (RaftStorage raftStorage : raftStorages) {
			raftStorage.close();
		}
	}

	@Test
	void emptyStorageHasNoSnapshot() throws IOException {
		var storage = initStorage(tempDir.resolve("raft"), RaftStorage.StartupOption.FORMAT);
		assertNull(storage.getLatestSnapshot());
		assertTrue(Files.isDirectory(storage.getTmpDir().toPath()));
	}

	@Test
	void takeKeepsOnlyTheLatestSnapshot() throws IOException {
		var storage = initStorage(tempDir.resolve("raft"), RaftStorage.StartupOption.FORMAT);

		put(store, column, 1, "first");
		setApplied(store, TermIndex.valueOf(1, 10));
		assertEquals(TermIndex.valueOf(1, 10), storage.take(store));
		assertEquals(TermIndex.valueOf(1, 10), storage.getLatestSnapshot().getTermIndex());

		put(store, column, 2, "second");
		setApplied(store, TermIndex.valueOf(2, 20));
		assertEquals(TermIndex.valueOf(2, 20), storage.take(store));

		FileListSnapshotInfo latest = storage.getLatestSnapshot();
		assertEquals(TermIndex.valueOf(2, 20), latest.getTermIndex());
		assertListsExactlyTheSnapshotFiles(storage, latest);
		Path stateMachineDir = storage.getSnapshotDir().toPath().getParent();
		assertEquals(Set.of("snapshot", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of(), fileNames(storage.getTmpDir().toPath()));

		Path restored = tempDir.resolve("restored");
		NodeStore.restore(storage.snapshotDbDir(), restored);
		try (var restoredStore = NodeStore.open(restored, null, "restored")) {
			assertEquals(TermIndex.valueOf(2, 20), restoredStore.applied());
			assertEquals(Map.of(1, "first", 2, "second"),
					contents(restoredStore, restoredStore.db().getColumnId(DATA_COLUMN)));
		}
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
	void initRecoversASnapshotSwapInterruptedByACrash() throws IOException {
		Path raftDir = tempDir.resolve("raft");
		var storage = initStorage(raftDir, RaftStorage.StartupOption.FORMAT);
		setApplied(store, TermIndex.valueOf(1, 5));
		storage.take(store);
		Path snapshotDir = storage.getSnapshotDir().toPath();
		Path stateMachineDir = snapshotDir.getParent();
		closeRaftStorages();

		// Crash after the old snapshot was moved aside, before the new one took its place.
		Files.move(snapshotDir, stateMachineDir.resolve("snapshot.old"));
		Files.createDirectories(stateMachineDir.resolve("tmp").resolve("snapshot-6-123").resolve("db"));

		var reopened = initStorage(raftDir, RaftStorage.StartupOption.RECOVER);
		assertEquals(TermIndex.valueOf(1, 5), reopened.getLatestSnapshot().getTermIndex());
		assertListsExactlyTheSnapshotFiles(reopened, reopened.getLatestSnapshot());
		assertEquals(Set.of("snapshot", "tmp"), fileNames(stateMachineDir));
		assertEquals(Set.of(), fileNames(stateMachineDir.resolve("tmp")));
	}

	@Test
	void installedSnapshotIsListedWithoutRatisDigestFiles() throws IOException {
		var leader = initStorage(tempDir.resolve("leader"), RaftStorage.StartupOption.FORMAT);
		put(store, column, 7, "installed");
		setApplied(store, TermIndex.valueOf(4, 40));
		leader.take(store);

		// What a Ratis install leaves behind: the leader's files at the same relative paths, plus <file>.md5.
		var follower = initStorage(tempDir.resolve("follower"), RaftStorage.StartupOption.FORMAT);
		Path leaderDir = leader.getSnapshotDir().toPath();
		Path followerDir = follower.getSnapshotDir().toPath();
		for (FileInfo file : leader.getLatestSnapshot().getFiles()) {
			Path target = followerDir.resolve(leaderDir.relativize(file.getPath()));
			Files.createDirectories(target.getParent());
			Files.copy(file.getPath(), target);
			Files.writeString(target.resolveSibling(target.getFileName() + ".md5"), "digest");
		}
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

	/** Every listed file exists under the snapshot directory, and every snapshot file except digests is listed. */
	private static void assertListsExactlyTheSnapshotFiles(CheckpointStorage storage, FileListSnapshotInfo snapshot)
			throws IOException {
		Path snapshotDir = storage.getSnapshotDir().toPath();
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
