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
		var lockField = CheckpointStorage.class.getDeclaredField("installsLock");
		lockField.setAccessible(true);
		Object installsLock = lockField.get(follower);
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
