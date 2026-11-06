package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NodeStoreTest {

	static final String DATA_COLUMN = "data";
	static final ColumnSchema DATA_SCHEMA = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);

	@TempDir
	Path tempDir;

	@Test
	void freshDatabaseHasNoMetadataAndNoUserColumns() throws IOException {
		try (var store = NodeStore.open(tempDir.resolve("db"), null, "fresh")) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied());
			assertNull(store.clusterId());
			assertFalse(store.hasUserColumns());

			store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			assertTrue(store.hasUserColumns());
		}
	}

	/**
	 * A leader answers a copy of a commit whose session is expired at the replicated clock as a gone session, even
	 * before the sweep deletes its row (F13).
	 */
	@Test
	void aSessionExpiredAtTheReplicatedClockIsNotLive() throws IOException {
		try (var store = NodeStore.open(tempDir.resolve("db"), null, "sessions")) {
			var db = store.db();
			long meta = store.metaColumnId();
			assertFalse(CommitOutcomes.sessionLive(store, 5), "no row");
			db.put(0L, meta, NodeStore.sessionKey(5), NodeStore.encodeSession(1_000, 10_000, 0), RequestType.none());
			assertTrue(CommitOutcomes.sessionLive(store, 5), "no clock yet");
			db.put(0L, meta, NodeStore.key(NodeStore.CLOCK),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(11_000).array()), RequestType.none());
			assertTrue(CommitOutcomes.sessionLive(store, 5), "exactly at its timeout");
			db.put(0L, meta, NodeStore.key(NodeStore.CLOCK),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(11_001).array()), RequestType.none());
			assertFalse(CommitOutcomes.sessionLive(store, 5), "expired at the clock, row still there");
		}
	}

	@Test
	void reopeningKeepsMetaColumnAndMetadata() throws IOException {
		Path dbDir = tempDir.resolve("db");
		long metaColumnId;
		try (var store = NodeStore.open(dbDir, null, "first")) {
			metaColumnId = store.metaColumnId();
			setApplied(store, TermIndex.valueOf(3, 42));
			setClusterId(store, "cluster-a");
		}
		try (var store = NodeStore.open(dbDir, null, "second")) {
			assertEquals(metaColumnId, store.metaColumnId());
			assertEquals(TermIndex.valueOf(3, 42), store.applied());
			assertEquals("cluster-a", store.clusterId());
			assertFalse(store.hasUserColumns());
		}
	}

	/** The leader's native transactions must not hold locks that the apply waits on, whatever the config says. */
	@Test
	void theDatabaseIsOptimisticEvenIfTheConfigSaysOtherwise() throws Exception {
		Path config = tempDir.resolve("pessimistic.conf");
		Files.writeString(config, "database.global.optimistic = false\n");
		try (var store = NodeStore.open(tempDir.resolve("db"), config, "pessimistic")) {
			assertTrue(store.db().getConfig().global().optimistic());
			assertTrue(store.db().getDb().get() instanceof org.rocksdb.OptimisticTransactionDB,
					"opened as " + store.db().getDb().get().getClass().getName());
		}
	}

	/**
	 * With atomic flush, a column reaching its own write-buffer-size flushes every column: every column's limit is
	 * raised to the shared write buffer budget, so that the budget decides when to flush.
	 */
	@Test
	void everyColumnBufferIsSizedToTheSharedWriteBufferBudget() throws Exception {
		Path config = tempDir.resolve("buffers.conf");
		Files.writeString(config, """
				database.global.write-buffer-manager = 32MiB
				database.global.fallback-column-options.write-buffer-size = 4MiB
				database.global.column-options = [{ name: "data", write-buffer-size: 2MiB }]
				""");
		try (var store = NodeStore.open(tempDir.resolve("db"), config, "buffers")) {
			var global = store.db().getConfig().global();
			assertEquals(32L << 20, global.writeBufferManager().longValue());
			assertEquals(32L << 20, global.fallbackColumnOptions().writeBufferSize().longValue());
			boolean listed = false;
			for (var column : global.columnOptions()) {
				if (column.name().equals("data")) {
					listed = true;
					assertEquals(32L << 20, column.writeBufferSize().longValue());
				}
			}
			assertTrue(listed, "the column the config lists");
			assertTrue(global.atomicFlush());
		}
	}

	@Test
	void theWriteBufferBudgetDefaultsLikeTheDatabase() {
		assertEquals(128L << 20, NodeStore.writeBufferBudget(com.typesafe.config.ConfigFactory.empty()));
		assertEquals(256L << 20, NodeStore.writeBufferBudget(com.typesafe.config.ConfigFactory.parseString("""
				database.global.write-buffer-manager = 0
				database.global.block-cache = 512MiB
				""")));
		var resized = NodeStore.withReplicaWriteBuffers(com.typesafe.config.ConfigFactory.empty());
		assertEquals("134217728B", resized.getString("database.global.fallback-column-options.write-buffer-size"));
		assertFalse(resized.hasPath("database.global.column-options"), "only the columns the config lists");
	}

	/** A crash can tear the tail of the un-synced WAL: the replica opens the prefix and Raft replays the rest. */
	@Test
	void openRecoversThePrefixOfAWalWithATornTail() throws Exception {
		Path dbDir = tempDir.resolve("db");
		var crashed = NodeStore.open(dbDir, null, "first");
		for (int index = 1; index <= 10; index++) {
			setApplied(crashed, TermIndex.valueOf(1, index));
		}
		// Stop like a crash after the WAL write: NodeStore.close() would flush the memtables, and with them the WAL.
		crashed.db().getDb().get().flushWal(true);
		crashed.connection().close();
		Path wal;
		try (Stream<Path> files = Files.walk(dbDir)) {
			wal = files.filter(file -> file.getFileName().toString().matches("\\d+\\.log"))
					.max(Comparator.comparing(file -> file.getFileName().toString()))
					.orElseThrow();
		}
		try (var channel = FileChannel.open(wal, StandardOpenOption.WRITE)) {
			channel.truncate(channel.size() - 1);
		}
		try (var store = NodeStore.open(dbDir, null, "second")) {
			assertEquals(TermIndex.valueOf(1, 9), store.applied());
		}
	}

	@Test
	void termIndexIsSixteenBigEndianBytes() {
		Buf encoded = NodeStore.encodeTermIndex(TermIndex.valueOf(7, 0x0102030405060708L));
		byte[] expected = ByteBuffer.allocate(16).putLong(7).putLong(0x0102030405060708L).array();
		assertArrayEquals(expected, encoded.toByteArray());
		assertEquals(TermIndex.valueOf(7, 0x0102030405060708L), NodeStore.decodeTermIndex(encoded));
		assertThrows(IllegalArgumentException.class, () -> NodeStore.decodeTermIndex(Buf.wrap(new byte[8])));
	}

	@Test
	void restoreReplacesDatabaseWithCheckpointContents() throws IOException {
		Path dbDir = tempDir.resolve("db");
		Path checkpointDir = tempDir.resolve("checkpoint");
		Expected checkpointed;
		try (var store = NodeStore.open(dbDir, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 100));
			setClusterId(store, "cluster-b");
			checkpointed = Expected.of(store, column);

			assertEquals(TermIndex.valueOf(1, 100), store.checkpoint(checkpointDir));

			put(store, column, 1000, "after");
			delete(store, column, 0);
			store.db().createColumn("later", DATA_SCHEMA);
			setApplied(store, TermIndex.valueOf(2, 150));
		}

		Path freshDir = tempDir.resolve("fresh");
		NodeStore.restore(checkpointDir, freshDir);
		assertMatchesCheckpoint(freshDir, checkpointed);

		// Restoring over an existing database replaces it completely.
		NodeStore.restore(checkpointDir, dbDir);
		assertMatchesCheckpoint(dbDir, checkpointed);
		assertSiblingsAreOnly(tempDir, "db", "fresh", "checkpoint");
	}

	@Test
	void checkpointHardLinksTableFilesInsteadOfCopyingThem() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		checkpointWithData(checkpointDir, 10, TermIndex.valueOf(1, 10));

		List<Path> tables = regularFiles(checkpointDir).stream()
				.filter(file -> file.toString().endsWith(".sst"))
				.toList();
		assertFalse(tables.isEmpty());
		for (Path table : tables) {
			Path original = tempDir.resolve("source").resolve(checkpointDir.relativize(table));
			assertTrue(Files.isSameFile(original, table), table + " is not a hard link of " + original);
		}
	}

	@Test
	void checkpointLeavesOutWalsMadeObsoleteByItsFlush() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		String largeValue = "x".repeat(4096);
		Expected checkpointed;
		try (var store = NodeStore.open(tempDir.resolve("db"), null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			// About 4 MiB that only the WAL and the memtable hold until the checkpoint flushes them.
			for (int i = 0; i < 1024; i++) {
				put(store, column, i, largeValue);
			}
			setApplied(store, TermIndex.valueOf(1, 1024));
			checkpointed = Expected.of(store, column);
			store.checkpoint(checkpointDir);
		}

		long walBytes = 0;
		for (Path file : regularFiles(checkpointDir)) {
			if (file.toString().endsWith(".log")) {
				walBytes += Files.size(file);
			}
		}
		assertTrue(walBytes < 64 * 1024, "the checkpoint holds " + walBytes + " bytes of WAL");
		Path restored = tempDir.resolve("restored");
		NodeStore.restore(checkpointDir, restored);
		assertMatchesCheckpoint(restored, checkpointed);
	}

	@Test
	void checkpointAndRestoreCopyFilesThatCannotBeHardLinked() throws IOException {
		Path otherFileSystem = otherFileSystemDir();
		try {
			Path checkpointDir = otherFileSystem.resolve("checkpoint");
			Expected checkpointed = checkpointWithData(checkpointDir, 10, TermIndex.valueOf(2, 10));
			assertTrue(regularFiles(checkpointDir).stream().anyMatch(file -> file.toString().endsWith(".sst")));

			Path dbDir = tempDir.resolve("db");
			NodeStore.restore(checkpointDir, dbDir);
			assertMatchesCheckpoint(dbDir, checkpointed);
		} finally {
			NodeStore.deleteRecursively(otherFileSystem);
		}
	}

	@Test
	void openRefusesADatabaseDirectoryOnAnotherFileSystem() throws IOException {
		Path otherFileSystem = otherFileSystemDir();
		try {
			// restore() swaps directories by renaming, which cannot move a volume mounted from another disk.
			Path dbDir = tempDir.resolve("db");
			Files.createDirectories(dbDir);
			Files.createSymbolicLink(dbDir.resolve("volume"), otherFileSystem);

			var failure = assertThrows(IOException.class, () -> NodeStore.open(dbDir, null, "mounted").close());
			assertTrue(failure.getMessage().contains("file system"), failure.getMessage());
		} finally {
			NodeStore.deleteRecursively(otherFileSystem);
		}
	}

	@Test
	void checkpointStaysUsableAfterRestoredDatabaseChanges() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		Expected checkpointed;
		try (var store = NodeStore.open(tempDir.resolve("db"), null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 50; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 50));
			checkpointed = Expected.of(store, column);
			store.checkpoint(checkpointDir);
		}

		Path first = tempDir.resolve("first");
		NodeStore.restore(checkpointDir, first);
		try (var store = NodeStore.open(first, null, "first")) {
			long column = store.db().getColumnId(DATA_COLUMN);
			for (int i = 0; i < 50; i++) {
				delete(store, column, i);
			}
			// Compaction rewrites and deletes the SST files the restore hard-linked.
			store.db().flush();
			store.db().compact();
			assertEquals(Map.of(), contents(store, column));
		}

		Path second = tempDir.resolve("second");
		NodeStore.restore(checkpointDir, second);
		assertMatchesCheckpoint(second, checkpointed);
	}

	@Test
	void restoreDiscardsLeftoversOfAnIncompleteRestore() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		Expected checkpointed = checkpointWithData(checkpointDir, 10, TermIndex.valueOf(4, 10));

		Path dbDir = tempDir.resolve("db");
		Files.createDirectories(tempDir.resolve("db.restore-tmp"));
		Files.writeString(tempDir.resolve("db.restore-tmp").resolve("000001.sst"), "partial");
		Files.createDirectories(tempDir.resolve("db.restore-old"));
		Files.writeString(tempDir.resolve("db.restore-old").resolve("CURRENT"), "stale");

		NodeStore.restore(checkpointDir, dbDir);

		assertMatchesCheckpoint(dbDir, checkpointed);
		assertSiblingsAreOnly(tempDir, "db", "checkpoint", "source");
	}

	@Test
	void openFinishesARestoreInterruptedDuringTheSwap() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		Expected checkpointed = checkpointWithData(checkpointDir, 10, TermIndex.valueOf(5, 10));

		// State after a crash between "move the old database aside" and "rename the new one into place".
		Path prepared = tempDir.resolve("prepared");
		NodeStore.restore(checkpointDir, prepared);
		Files.move(prepared, tempDir.resolve("db.restore-ready"));
		Path dbDir = tempDir.resolve("db");
		try (var store = NodeStore.open(tempDir.resolve("db.restore-old"), null, "old")) {
			store.db().createColumn("old-only", DATA_SCHEMA);
		}

		assertMatchesCheckpoint(dbDir, checkpointed);
		assertSiblingsAreOnly(tempDir, "db", "checkpoint", "source");
	}

	@Test
	void restoreSkipsRatisDigestFiles() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		Expected checkpointed = checkpointWithData(checkpointDir, 3, TermIndex.valueOf(1, 3));
		for (Path file : regularFiles(checkpointDir)) {
			Files.writeString(file.resolveSibling(file.getFileName() + ".md5"), "digest");
		}

		Path dbDir = tempDir.resolve("db");
		NodeStore.restore(checkpointDir, dbDir);

		assertTrue(regularFiles(dbDir).stream().noneMatch(file -> file.toString().endsWith(".md5")));
		assertMatchesCheckpoint(dbDir, checkpointed);
	}

	/** What a database held when it was checkpointed. */
	record Expected(Map<Integer, String> contents, TermIndex applied, @Nullable String clusterId) {

		static Expected of(NodeStore store, long column) {
			return new Expected(NodeStoreTest.contents(store, column), store.applied(), store.clusterId());
		}
	}

	private Expected checkpointWithData(Path checkpointDir, int count, TermIndex applied) throws IOException {
		try (var store = NodeStore.open(tempDir.resolve("source"), null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < count; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, applied);
			assertEquals(applied, store.checkpoint(checkpointDir));
			return Expected.of(store, column);
		}
	}

	/** A new directory on a different file system than {@link #tempDir}; skips the test if there is none. */
	private Path otherFileSystemDir() throws IOException {
		Path dir = Files.createTempDirectory(Path.of("target").toAbsolutePath(), "other-file-system");
		if (Files.getFileStore(dir).equals(Files.getFileStore(tempDir))) {
			NodeStore.deleteRecursively(dir);
			Assumptions.abort("The build directory and the temporary directory are on the same file system");
		}
		return dir;
	}

	private static void assertMatchesCheckpoint(Path dbDir, Expected expected) throws IOException {
		try (var store = NodeStore.open(dbDir, null, "restored")) {
			assertEquals(Set.of(DATA_COLUMN, NodeStore.META_COLUMN), store.db().getAllColumnDefinitions().keySet());
			assertEquals(expected, Expected.of(store, store.db().getColumnId(DATA_COLUMN)));
		}
	}

	private static List<Path> regularFiles(Path dir) throws IOException {
		try (Stream<Path> walk = Files.walk(dir)) {
			return walk.filter(Files::isRegularFile).toList();
		}
	}

	private static void assertSiblingsAreOnly(Path dir, String... names) throws IOException {
		try (Stream<Path> files = Files.list(dir)) {
			assertEquals(Set.of(names), files.map(file -> file.getFileName().toString()).collect(Collectors.toSet()));
		}
	}

	static void setApplied(NodeStore store, TermIndex termIndex) {
		store.db().put(0, store.metaColumnId(), NodeStore.key(NodeStore.APPLIED),
				NodeStore.encodeTermIndex(termIndex), RequestType.none());
	}

	static void setClusterId(NodeStore store, String clusterId) {
		store.db().put(0, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_ID),
				Buf.wrap(clusterId.getBytes(StandardCharsets.UTF_8)), RequestType.none());
	}

	static void put(NodeStore store, long column, int key, String value) {
		store.db().put(0, column, intKey(key), Buf.wrap(value.getBytes(StandardCharsets.UTF_8)), RequestType.none());
	}

	static void delete(NodeStore store, long column, int key) {
		store.db().delete(0, column, intKey(key), RequestType.none());
	}

	static Map<Integer, String> contents(NodeStore store, long column) {
		var result = new TreeMap<Integer, String>();
		try (Stream<KV> range = store.db().getRange(0, column, null, null, false, RequestType.<KV>allInRange())) {
			range.forEach(kv -> result.put(ByteBuffer.wrap(kv.keys().keys()[0].toByteArray()).getInt(),
					kv.value().toString(StandardCharsets.UTF_8)));
		}
		return result;
	}

	private static Keys intKey(int key) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(key).array()));
	}
}
