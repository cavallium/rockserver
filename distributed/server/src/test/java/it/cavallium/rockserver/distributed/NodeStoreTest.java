package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
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
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.OptimisticTransactionDB;

@Tag("slow")
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

	/**
	 * R24-2: the state machine asks for the cluster id under its monitor, which {@code preAppendTransaction} waits for
	 * under Ratis' append lock, and on every proposal: the id is kept in memory (read when the database opens, set by
	 * genesis), never read from the database. A value put in the database behind the store's back is not seen until
	 * the store opens again.
	 */
	@Test
	void clusterIdIsAnsweredFromMemory() throws IOException {
		Path dbDir = tempDir.resolve("db");
		try (var store = NodeStore.open(dbDir, null, "first")) {
			setClusterId(store, "cluster-a");
		}
		try (var store = NodeStore.open(dbDir, null, "second")) {
			assertEquals("cluster-a", store.clusterId());
			store.db().put(0, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_ID),
					Buf.wrap("cluster-b".getBytes(StandardCharsets.UTF_8)), RequestType.none());
			assertEquals("cluster-a", store.clusterId());
		}
		try (var store = NodeStore.open(dbDir, null, "third")) {
			assertEquals("cluster-b", store.clusterId());
		}
	}

	/**
	 * R24-2 review: a closed store still refuses to answer, as it did when the id was read from the database. A node
	 * installing a snapshot has closed its old store; {@code ReplicatedConnection.getCapabilities} counts on the
	 * refusal to advertise no client sessions then, and {@code initializeCluster} to wait instead of reading null.
	 */
	@Test
	void clusterIdRefusesAClosedStore() throws IOException {
		Path dbDir = tempDir.resolve("db");
		try (var store = NodeStore.open(dbDir, null, "first")) {
			setClusterId(store, "cluster-a");
		}
		var store = NodeStore.open(dbDir, null, "second");
		store.close();
		var refused = assertThrows(IllegalStateException.class, store::clusterId);
		assertEquals("Closed", refused.getMessage());
		var fresh = NodeStore.open(tempDir.resolve("fresh"), null, "fresh");
		fresh.close();
		assertThrows(IllegalStateException.class, fresh::clusterId);
	}

	/**
	 * RocksDB deletes files in its directories from its own threads as soon as it is open (the inputs of the compaction
	 * that a database restored with many level-0 files starts at once, obsolete WALs): the checks that {@link
	 * NodeStore#open} runs on the open database must not fail when a file disappears under them. The thread below stands
	 * in for those deletions, in the volume directory, with files RocksDB does not know.
	 */
	@Test
	void openToleratesFilesDeletedConcurrentlyInTheDatabaseDirectory() throws Exception {
		reopenWhileChurning(false);
	}

	/**
	 * The same for directories: core does not delete a directory of an open database, but the check walks first and asks
	 * each directory's file system after, so a directory gone in between is skipped as a file is.
	 */
	@Test
	void openToleratesDirectoriesDeletedConcurrentlyInTheDatabaseDirectory() throws Exception {
		reopenWhileChurning(true);
	}

	/** Reopens a database 30 times while a thread creates and deletes files, or directories, in its volume directory. */
	private void reopenWhileChurning(boolean directories) throws Exception {
		Path dbDir = tempDir.resolve("db");
		try (var store = NodeStore.open(dbDir, null, "first")) {
			setApplied(store, TermIndex.valueOf(1, 1));
		}
		Path volume = dbDir.resolve("volume");
		assertTrue(Files.isDirectory(volume), "default volume directory " + volume);
		var stop = new java.util.concurrent.atomic.AtomicBoolean();
		var churner = new Thread(() -> {
			int n = 0;
			while (!stop.get()) {
				Path entry = volume.resolve("churn-" + (n++ % 256) + (directories ? ".d" : ".tmp"));
				try {
					if (!Files.deleteIfExists(entry)) {
						if (directories) {
							Files.createDirectory(entry);
						} else {
							Files.createFile(entry);
						}
					}
				} catch (IOException ignored) {
					// Racing with nothing but this thread: retry on the next name.
				}
			}
		}, "churner");
		churner.start();
		try {
			for (int i = 0; i < 30; i++) {
				try (var store = NodeStore.open(dbDir, null, "reopen-" + i)) {
					assertEquals(TermIndex.valueOf(1, 1), store.applied());
				}
			}
		} finally {
			stop.set(true);
			churner.join();
		}
	}

	/** The leader's native transactions must not hold locks that the apply waits on, whatever the config says. */
	@Test
	void theDatabaseIsOptimisticEvenIfTheConfigSaysOtherwise() throws Exception {
		Path config = tempDir.resolve("pessimistic.conf");
		Files.writeString(config, "database.global.optimistic = false\n");
		try (var store = NodeStore.open(tempDir.resolve("db"), config, "pessimistic")) {
			assertTrue(store.db().getConfig().global().optimistic());
			assertTrue(store.db().getDb().get() instanceof OptimisticTransactionDB,
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

	/**
	 * R2-TS-1 and R2-TS-2: the options RocksDB really runs with, read back from the OPTIONS file it writes, not the parsed
	 * config. Every column's write buffer is the budget, and the cap on range tombstones per memtable, derived from the
	 * write buffer size (one per 64 KiB), grows with it until the database's clamp of 65,536.
	 */
	@Test
	void theOpenColumnsRunWithTheBudgetAsWriteBufferAndAScaledRangeDeletionCap() throws Exception {
		var small = columnOptions("32MiB", "small");
		assertEquals(String.valueOf(32L << 20), small.get("meta").get("write_buffer_size"));
		assertEquals(String.valueOf(32L << 20), small.get("data").get("write_buffer_size"));
		assertEquals(String.valueOf(32L << 20), small.get("user").get("write_buffer_size"));
		assertEquals("512", small.get("user").get("memtable_max_range_deletions"));
		assertEquals("512", small.get("data").get("memtable_max_range_deletions"));

		// 8 GiB / 64 KiB would be 131,072 tombstones: clamped.
		var large = columnOptions("8GiB", "large");
		assertEquals(String.valueOf(8L << 30), large.get("user").get("write_buffer_size"));
		assertEquals("65536", large.get("user").get("memtable_max_range_deletions"));
	}

	/** The options of each column of a database opened with {@code budget}, from RocksDB's latest OPTIONS file. */
	private Map<String, Map<String, String>> columnOptions(String budget, String name) throws Exception {
		Path config = tempDir.resolve(name + ".conf");
		Files.writeString(config, """
				database.global.write-buffer-manager = %s
				database.global.column-options = [{ name: "data", write-buffer-size: 2MiB }]
				""".formatted(budget));
		Path dbDir = tempDir.resolve(name);
		try (var store = NodeStore.open(dbDir, config, name)) {
			store.db().createColumn("data", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			store.db().createColumn("user", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		}
		Path latest;
		try (var files = Files.list(dbDir)) {
			latest = files.filter(file -> file.getFileName().toString().startsWith("OPTIONS-"))
					.max(Comparator.comparingLong(file -> Long.parseLong(file.getFileName().toString().substring(8))))
					.orElseThrow();
		}
		var columns = new HashMap<String, Map<String, String>>();
		Map<String, String> current = null;
		for (String line : Files.readAllLines(latest)) {
			var trimmed = line.strip();
			if (trimmed.startsWith("[CFOptions \"")) {
				var column = trimmed.substring("[CFOptions \"".length(), trimmed.length() - 2);
				current = new HashMap<>();
				columns.put(column.equals(NodeStore.META_COLUMN) ? "meta" : column, current);
			} else if (trimmed.startsWith("[")) {
				current = null;
			} else if (current != null && trimmed.contains("=")) {
				current.put(trimmed.substring(0, trimmed.indexOf('=')), trimmed.substring(trimmed.indexOf('=') + 1));
			}
		}
		return columns;
	}

	@Test
	void theWriteBufferBudgetDefaultsLikeTheDatabase() {
		assertEquals(128L << 20, NodeStore.writeBufferBudget(ConfigFactory.empty()));
		assertEquals(256L << 20, NodeStore.writeBufferBudget(ConfigFactory.parseString("""
				database.global.write-buffer-manager = 0
				database.global.block-cache = 512MiB
				""")));
		var resized = NodeStore.withReplicaWriteBuffers(ConfigFactory.empty());
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

	/**
	 * {@code restore-backup} refuses a backup of a newer cluster, whose cluster version has a capability this version
	 * lacks, before it drops the replication metadata that holds the version: the new cluster would otherwise start at
	 * {@link Capabilities#GENESIS} with data a newer version wrote, and no start would refuse it (R18-1). The data
	 * directory stays empty, so the same command runs again with the newer binary.
	 */
	@Test
	void restoreBackupRefusesABackupOfANewerCluster() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		Path source = tempDir.resolve("source");
		writeDatabaseOfVersion(source, NEWER_VERSION);
		try (var store = NodeStore.open(source, null, "source")) {
			store.checkpoint(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Path dataDir = tempDir.resolve("data");
		var refused = assertThrows(ReplicatedNode.NewerVersionException.class,
				() -> ReplicaOps.restore(backupDir, dataDir, null));
		assertTrue(refused.getMessage().contains("0x10000000000"), refused.getMessage());
		assertTrue(Files.notExists(dataDir.resolve("db")), "the data directory holds no database after the refusal");
	}

	/**
	 * A {@code restore-backup} that fails after the backup was copied (here: a {@code --rockserver-config} that does not
	 * parse) leaves the data directory as it was given, so that the same command can run again; before, the half-made
	 * {@code db/} stayed and the rerun refused the directory as holding a database (R19-3).
	 */
	@Test
	void aFailedRestoreBackupLeavesNoDatabaseAndCanRunAgain() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		Path source = tempDir.resolve("source");
		writeDatabaseOfVersion(source, Capabilities.SUPPORTED);
		try (var store = NodeStore.open(source, null, "source")) {
			store.checkpoint(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Path badConfig = tempDir.resolve("bad.conf");
		Files.writeString(badConfig, "database { this does not parse");
		Path dataDir = tempDir.resolve("data");
		assertThrows(Exception.class, () -> ReplicaOps.restore(backupDir, dataDir, badConfig));
		assertTrue(Files.notExists(dataDir.resolve("db")), "a failed restore left a database behind");

		assertEquals(TermIndex.valueOf(1, 7), ReplicaOps.restore(backupDir, dataDir, null), "the rerun was refused");
		assertTrue(Files.isDirectory(dataDir.resolve("db")));
	}

	/**
	 * Every node of a restored cluster runs {@code restore-backup} from its own copy of the same backup, then records its
	 * seed digest at its first start, and the followers compare it with the leader's at genesis: the restores must leave
	 * the same files, or the followers stop with exit 3 (R26-1).
	 */
	@Test
	void restoresOfTheSameBackupGiveTheSameSeedDigest() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		Path source = tempDir.resolve("source");
		try (var store = NodeStore.open(source, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 200; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 200));
			setClusterId(store, "old-cluster");
			store.checkpoint(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=200\ncluster=old-cluster\n");
		var digests = new TreeMap<String, String>();
		var files = new TreeMap<String, Map<String, String>>();
		for (String node : List.of("n0", "n1", "n2")) {
			// Each node from its own copy of the backup, as the runbook says.
			Path copy = tempDir.resolve("copy-" + node);
			copyTree(backupDir, copy);
			Path dataDir = tempDir.resolve(node);
			ReplicaOps.restore(copy, dataDir, null);
			Path db = dataDir.resolve("db");
			NodeStore.recordSeedDigest(db);
			digests.put(node, NodeStore.readSeedDigest(db));
			files.put(node, fileDigests(db));
		}
		assertEquals(1, Set.copyOf(digests.values()).size(), "seed digests " + digests + ", files " + files);
	}

	private static void copyTree(Path from, Path to) throws IOException {
		try (Stream<Path> walk = Files.walk(from)) {
			for (Path file : walk.toList()) {
				Path target = to.resolve(from.relativize(file).toString());
				if (Files.isDirectory(file)) {
					Files.createDirectories(target);
				} else {
					Files.copy(file, target);
				}
			}
		}
	}

	/** Each file of {@code dir} with its size and a digest of its content, to name the files that differ. */
	private static Map<String, String> fileDigests(Path dir) throws IOException {
		var result = new TreeMap<String, String>();
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path file : walk.filter(Files::isRegularFile).toList()) {
				byte[] bytes = Files.readAllBytes(file);
				result.put(dir.relativize(file).toString(), bytes.length + ":" + java.util.HexFormat.of()
						.formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes)).substring(0, 12));
			}
		} catch (java.security.NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
		return result;
	}

	/** {@code export-seed} refuses the database of a newer cluster the same way, and writes no target (R18-1). */
	@Test
	void exportSeedRefusesADatabaseOfANewerCluster() throws IOException {
		Path source = tempDir.resolve("source");
		writeDatabaseOfVersion(source, NEWER_VERSION);
		Path target = tempDir.resolve("seed");
		var refused = assertThrows(ReplicatedNode.NewerVersionException.class,
				() -> NodeStore.exportSeed(source, null, target));
		assertTrue(refused.getMessage().contains("0x10000000000"), refused.getMessage());
		assertTrue(Files.notExists(target), "no target after the refusal");
	}

	/** This version's capabilities plus one it lacks. */
	static final long NEWER_VERSION = Capabilities.SUPPORTED | 1L << 40;

	static void writeDatabaseOfVersion(Path dbDir, long version) throws IOException {
		try (var store = NodeStore.open(dbDir, null, "versioned")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 1, "v1");
			setApplied(store, TermIndex.valueOf(1, 7));
			setClusterId(store, "newer-cluster");
			store.db().put(0, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_VERSION),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(version).array()), RequestType.none());
		}
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

	/**
	 * R33-1: {@code export-seed} of a replica whose restore a crash interrupted during the swap ({@link NodeStore#restore}
	 * renames {@code db/} to {@code db.restore-old}, then {@code db.restore-ready} to {@code db/}, and fsyncs the parent
	 * after both) exports the complete newer database in {@code db.restore-ready}, as a start of the node would, and
	 * leaves it swapped into {@code db/}. Both layouts: no {@code db/} (a crash between the renames, or a power loss
	 * before the fsync), and {@code db/} next to {@code db.restore-ready} (a crash before the swap). Before, the first
	 * was refused with "No database in", before the open that finishes the swap.
	 */
	@Test
	void exportSeedFinishesARestoreInterruptedDuringTheSwap() throws IOException {
		for (boolean betweenTheRenames : List.of(true, false)) {
			Path dataDir = Files.createDirectories(tempDir.resolve("node-" + betweenTheRenames));
			writeRestoreInterruptedDuringTheSwap(dataDir, betweenTheRenames);
			Path dbDir = dataDir.resolve("db");
			Path target = tempDir.resolve("seed-" + betweenTheRenames);

			var exported = NodeStore.exportSeed(dbDir, null, target);

			assertEquals(TermIndex.valueOf(4, 40), exported.applied(), "between the renames: " + betweenTheRenames);
			try (var seed = NodeStore.open(target, null, "seed")) {
				assertEquals(Map.of(7, "new"), contents(seed, seed.db().getColumnId(DATA_COLUMN)));
			}
			assertFalse(Files.exists(dataDir.resolve("db.restore-ready")));
			assertFalse(Files.exists(dataDir.resolve("db.restore-old")));
			try (var store = NodeStore.open(dbDir, null, "node")) {
				assertEquals(TermIndex.valueOf(4, 40), store.applied());
				assertEquals(Map.of(7, "new"), contents(store, store.db().getColumnId(DATA_COLUMN)));
			}
		}
	}

	/**
	 * R33-1 guard: with neither {@code db/CURRENT} nor {@code db.restore-ready}, {@code export-seed} still refuses with
	 * "No database in", writes nothing and touches nothing: a {@code db.restore-old} or {@code db.restore-tmp} alone
	 * is left as it was.
	 */
	@Test
	void exportSeedOfAMissingDatabaseStillRefusesAndTouchesNothing() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		Path old = Files.createDirectories(dataDir.resolve("db.restore-old"));
		Files.writeString(old.resolve("CURRENT"), "kept");
		Path building = Files.createDirectories(dataDir.resolve("db.restore-tmp"));
		Files.writeString(building.resolve("000001.sst"), "kept");
		Path target = tempDir.resolve("seed");

		var refused = assertThrows(IOException.class,
				() -> NodeStore.exportSeed(dataDir.resolve("db"), null, target));

		assertTrue(refused.getMessage().startsWith("No database in "), refused.getMessage());
		assertFalse(Files.exists(target));
		assertFalse(Files.exists(dataDir.resolve("db")));
		assertEquals("kept", Files.readString(old.resolve("CURRENT")));
		assertEquals("kept", Files.readString(building.resolve("000001.sst")));
	}

	/**
	 * R33 review guard: a {@code db.restore-ready} without {@code CURRENT} (not a database: only storage that lost
	 * fsynced data leaves one, as {@link NodeStore#restore} fsyncs its tree before the rename to ready) next to a
	 * complete {@code db.restore-old} and no {@code db/} is refused with "No database in", with nothing touched. Before,
	 * the directory alone was accepted, and the open swapped it into {@code db/}, deleted {@code db.restore-old} (the
	 * only complete copy), created an empty database and exported it at index 0.
	 */
	@Test
	void exportSeedRefusesARestoreReadyWithoutCurrentAndTouchesNothing() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		writeDatabase(dataDir.resolve("db.restore-old"), "old", TermIndex.valueOf(1, 5));
		Path ready = Files.createDirectories(dataDir.resolve("db.restore-ready"));
		Files.writeString(ready.resolve("000001.sst"), "kept");
		Path target = tempDir.resolve("seed");

		var refused = assertThrows(IOException.class,
				() -> NodeStore.exportSeed(dataDir.resolve("db"), null, target));

		assertTrue(refused.getMessage().startsWith("No database in "), refused.getMessage());
		assertFalse(Files.exists(target));
		assertFalse(Files.exists(dataDir.resolve("db")));
		assertEquals("kept", Files.readString(ready.resolve("000001.sst")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-old").resolve("CURRENT")));
		try (var store = NodeStore.open(dataDir.resolve("db.restore-old"), null, "old")) {
			assertEquals(TermIndex.valueOf(1, 5), store.applied());
			assertEquals(Map.of(7, "old"), contents(store, store.db().getColumnId(DATA_COLUMN)));
		}
	}

	/**
	 * R34-2: {@code export-seed} refuses, before anything is touched, a target that is the node's own {@code db/} (missing
	 * in the R33-1 layout: a crash between the restore's renames), inside it, one of its restore directories, inside the
	 * node's Raft directory, or any of these reached through a symbolic link. Before, every pre-check passed for
	 * {@code --target DATA-DIR/db}: the open swapped {@code db.restore-ready} into {@code db/} and deleted
	 * {@code db.restore-old}, the checkpoint failed as its target existed, and the failure cleanup deleted {@code db/},
	 * the node's only complete database; the other targets were written into the node's own directories.
	 */
	@Test
	void exportSeedRefusesATargetAmongTheNodesOwnDirectoriesAndTouchesNothing() throws IOException {
		var targets = new java.util.LinkedHashMap<String, java.util.function.Function<Path, Path>>();
		targets.put("db", dataDir -> dataDir.resolve("db"));
		targets.put("inside db", dataDir -> dataDir.resolve("db").resolve("seed"));
		targets.put("db.restore-tmp", dataDir -> dataDir.resolve("db.restore-tmp"));
		targets.put("inside db.restore-old", dataDir -> dataDir.resolve("db.restore-old").resolve("seed"));
		targets.put("inside raft", dataDir -> dataDir.resolve("raft").resolve("seed"));
		targets.put("db through a link", dataDir -> dataDir.resolveSibling(dataDir.getFileName() + "-alias")
				.resolve("db"));
		// R34 review 1: a link to the missing db/ is dangling, and must be followed all the same.
		targets.put("inside db through a dangling link", dataDir -> dataDir.resolveSibling(dataDir.getFileName()
				+ "-dangling").resolve("seed"));
		int n = 0;
		for (var entry : targets.entrySet()) {
			Path dataDir = Files.createDirectories(tempDir.resolve("node" + n++));
			writeRestoreInterruptedDuringTheSwap(dataDir, true);
			Path stateMachineDir = Files.createDirectories(dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID))
					.resolve("sm");
			Files.createSymbolicLink(dataDir.resolveSibling(dataDir.getFileName() + "-alias"), dataDir);
			Files.createSymbolicLink(dataDir.resolveSibling(dataDir.getFileName() + "-dangling"), dataDir.resolve("db"));
			Path target = entry.getValue().apply(dataDir);

			var refused = assertThrows(IOException.class,
					() -> NodeStore.exportSeed(dataDir.resolve("db"), stateMachineDir, null, target, false),
					entry.getKey());

			assertTrue(refused.getMessage().contains("a directory of the node being exported"),
					entry.getKey() + ": " + refused.getMessage());
			assertFalse(Files.exists(dataDir.resolve("db")), entry.getKey());
			assertFalse(Files.exists(dataDir.resolve("db.restore-tmp")), entry.getKey());
			assertFalse(Files.exists(dataDir.resolve("raft").resolve("seed")), entry.getKey());
			assertTrue(Files.isRegularFile(dataDir.resolve("db.restore-old").resolve("CURRENT")), entry.getKey());
			assertFalse(Files.exists(dataDir.resolve("db.restore-old").resolve("seed")), entry.getKey());
			try (var ready = NodeStore.open(dataDir.resolve("db.restore-ready"), null, "ready")) {
				assertEquals(TermIndex.valueOf(4, 40), ready.applied(), entry.getKey());
				assertEquals(Map.of(7, "new"), contents(ready, ready.db().getColumnId(DATA_COLUMN)), entry.getKey());
			}
		}
	}

	/**
	 * R34 review 2: the data directory {@code /x/link/..}, with {@code link} pointing to {@code /real/a/b}, is resolved
	 * by the OS to {@code /real/a} but by {@link NodeStore#open} (which normalizes) to {@code /x}. The export uses one
	 * form, the normalized one the open uses, for every step: here {@code /x/db} holds no database, so it refuses with
	 * "No database in" and touches nothing. Before, the "No database in" check found {@code /real/a/db.restore-ready}
	 * through the OS, the refusal compared the target with {@code /x}'s directories, and the open created an empty
	 * database in {@code /x/db} and exported it into {@code /real/a/db/seed}, inside the node's (missing) {@code db/}.
	 */
	@Test
	void exportSeedUsesTheDirectoriesTheOpenUsesWhenTheDataDirHasDotDotAfterALink() throws IOException {
		Path realDataDir = Files.createDirectories(tempDir.resolve("real").resolve("a"));
		writeRestoreInterruptedDuringTheSwap(realDataDir, true);
		Files.createDirectories(realDataDir.resolve("b"));
		Files.createDirectories(realDataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID));
		Path x = Files.createDirectories(tempDir.resolve("x"));
		Files.createSymbolicLink(x.resolve("link"), realDataDir.resolve("b"));
		Path dataDir = x.resolve("link").resolve("..");
		Path target = realDataDir.resolve("db").resolve("seed");

		var refused = assertThrows(IOException.class, () -> NodeStore.exportSeed(dataDir.resolve("db"),
				dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("sm"), null, target, false));

		assertTrue(refused.getMessage().startsWith("No database in " + x.resolve("db")), refused.getMessage());
		assertFalse(Files.exists(x.resolve("db")));
		assertFalse(Files.exists(realDataDir.resolve("db")));
		assertTrue(Files.isRegularFile(realDataDir.resolve("db.restore-old").resolve("CURRENT")));
		try (var ready = NodeStore.open(realDataDir.resolve("db.restore-ready"), null, "ready")) {
			assertEquals(TermIndex.valueOf(4, 40), ready.applied());
		}
	}

	/**
	 * R53-6: an export killed before its last rename also leaves the copy's config next to the target
	 * ({@code rockserver-replica-for-<target>.conf}, the resolved config with its secrets). The refusal of the
	 * {@code <target>.tmp} it left names that file too, so that it goes with it, and leaves both as they are; a rerun to
	 * the same target after removing {@code <target>.tmp} leaves no config behind.
	 */
	@Test
	void theRefusalOfAKilledExportsTemporaryDirectoryNamesItsLeftConfig() throws IOException {
		Path source = tempDir.resolve("source");
		writeDatabase(source, "v", TermIndex.valueOf(1, 5));
		Path target = tempDir.resolve("seed");
		Path building = Files.createDirectories(tempDir.resolve("seed.tmp"));
		Path leftConfig = NodeStore.replicaConfigFile(target);
		Files.writeString(leftConfig, "secret = 1");

		var refused = assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, target));

		assertTrue(refused.getMessage().contains("seed.tmp already exists"), refused.getMessage());
		assertTrue(refused.getMessage().contains(leftConfig.toString()), refused.getMessage());
		assertTrue(Files.isDirectory(building));
		assertEquals("secret = 1", Files.readString(leftConfig));

		NodeStore.deleteRecursively(building);
		NodeStore.exportSeed(source, null, target);
		assertFalse(Files.exists(leftConfig), "the rerun replaced and deleted it");
		assertFalse(Files.exists(building));

		// Killed after the last rename: a complete target and the config.
		Files.writeString(leftConfig, "secret = 1");
		var targetRefused = assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, target));
		assertTrue(targetRefused.getMessage().contains("seed already exists"), targetRefused.getMessage());
		assertTrue(targetRefused.getMessage().contains(leftConfig.toString()), targetRefused.getMessage());
		assertEquals("secret = 1", Files.readString(leftConfig));
	}

	/**
	 * R34 review 3: an existing {@code <target>.tmp}, which the checkpoint would build in, is refused before anything is
	 * touched, as an existing target is, and stays as it was. Before, the checkpoint deleted it first, whoever had made
	 * it (an operator's {@code /backups/seed.tmp} for {@code --target /backups/seed}).
	 */
	@Test
	void exportSeedRefusesAnExistingTemporaryDirectoryAndLeavesIt() throws IOException {
		Path source = tempDir.resolve("source");
		writeDatabase(source, "v", TermIndex.valueOf(1, 5));
		Path target = tempDir.resolve("seed");
		Path foreign = Files.createDirectories(tempDir.resolve("seed.tmp"));
		Files.writeString(foreign.resolve("operator"), "not the export's");

		var refused = assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, target));

		assertTrue(refused.getMessage().contains("already exists"), refused.getMessage());
		assertEquals("not the export's", Files.readString(foreign.resolve("operator")));
		assertFalse(Files.exists(target));
	}

	/**
	 * R34 review 3: the checkpoint of an export creates its {@code <target>.tmp} itself, atomically, and never deletes
	 * one it did not create: one that appears between the export's checks and the checkpoint fails the export and
	 * stays. The snapshot and backup callers still replace a leftover (they build inside a directory of their own).
	 */
	@Test
	void anExportCheckpointNeverDeletesATemporaryDirectoryItDidNotCreate() throws IOException {
		Path source = tempDir.resolve("source");
		writeDatabase(source, "v", TermIndex.valueOf(1, 5));
		Path target = tempDir.resolve("seed");
		Path foreign = tempDir.resolve("seed.tmp");
		try (var store = NodeStore.open(source, null, "source")) {
			Files.createDirectories(foreign);
			Files.writeString(foreign.resolve("operator"), "not the export's");
			assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> store.checkpoint(target, false));
			assertEquals("not the export's", Files.readString(foreign.resolve("operator")));
			assertFalse(Files.exists(target));

			store.checkpoint(target, true);
			assertFalse(Files.exists(foreign));
			assertTrue(Files.isRegularFile(target.resolve("CURRENT")));
		}
	}

	/**
	 * R34-2: a failed export deletes only what it created. Here another process creates the target while the export
	 * runs (a test hook, once the checkpoint's live files are in place): the export fails, and the target it did not
	 * create stays as that process left it. Before, the cleanup deleted the target whatever had made it.
	 */
	@Test
	void aFailedExportDeletesOnlyWhatItCreated() throws IOException {
		Path source = tempDir.resolve("source");
		writeDatabase(source, "v", TermIndex.valueOf(1, 5));
		Path target = tempDir.resolve("seed");
		NodeStore.checkpointBuiltForTesting = building -> {
			Files.createDirectories(target);
			Files.writeString(target.resolve("foreign"), "not the export's");
		};
		try {
			assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, target));
		} finally {
			NodeStore.checkpointBuiltForTesting = null;
		}
		assertEquals("not the export's", Files.readString(target.resolve("foreign")));
		assertFalse(Files.exists(tempDir.resolve("seed.tmp")));
		try (var store = NodeStore.open(source, null, "source")) {
			assertEquals(TermIndex.valueOf(1, 5), store.applied());
		}
	}

	/**
	 * R34-1: {@code restore-backup} fsyncs every table and blob file it hard-links from the backup copy, whose files an
	 * operator copied with {@code cp}, {@code rsync} or {@code scp} and nobody fsynced. Before, {@code syncTree} skipped
	 * every file with a second link, on the assumption that its source was durable, and a power loss soon after the
	 * restore could damage both {@code db/} and the backup copy (the same inodes).
	 */
	@Test
	void restoreBackupFsyncsTheTableFilesItLinksFromTheBackupCopy() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 200, TermIndex.valueOf(1, 200));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=200\ncluster=c\n");
		Path copy = tempDir.resolve("copy");
		copyTree(backupDir, copy);
		Set<String> tables = tableFileNames(copy.resolve(ReplicaOps.BACKUP_DB_DIR));
		assertFalse(tables.isEmpty());
		Path dataDir = tempDir.resolve("data");
		Set<String> synced = syncedNamesUnder(dataDir, () -> ReplicaOps.restore(copy, dataDir, null));
		Assumptions.assumeTrue(maxLinkCount(dataDir.resolve("db"), tables) > 1, "the restore copied instead of linking");
		assertTrue(synced.containsAll(tables), "synced " + synced + ", table files " + tables);
	}

	/**
	 * R34-1 guard: the restore of a Raft snapshot (installation, a start behind its snapshot) still skips the linked
	 * files, whose sources were fsynced when the snapshot was made: fsyncing thousands of them would stall the apply.
	 */
	@Test
	void aSnapshotRestoreDoesNotFsyncTheTableFilesItLinks() throws IOException {
		Path checkpointDir = tempDir.resolve("checkpoint");
		checkpointWithData(checkpointDir, 200, TermIndex.valueOf(1, 200));
		Set<String> tables = tableFileNames(checkpointDir);
		assertFalse(tables.isEmpty());
		Path dbDir = tempDir.resolve("db");
		Set<String> synced = syncedNamesUnder(tempDir, () -> NodeStore.restore(checkpointDir, dbDir));
		Assumptions.assumeTrue(maxLinkCount(dbDir, tables) > 1, "the restore copied instead of linking");
		assertTrue(synced.stream().noneMatch(tables::contains), "synced " + synced);
		assertTrue(synced.contains("CURRENT"), "synced " + synced);
	}

	/**
	 * R34-1: the first start of a seeded node (no Raft group directory yet) fsyncs the seed's files, its directories and
	 * the data directory before it records the seed digest: the operator rsynced {@code db/} into place, nobody fsynced
	 * it, and the seed snapshot taken after genesis hard-links the same inodes. Here the seed's table files already have
	 * a second link (as after {@code cp -al}), which {@code syncTree} would skip. Before, nothing fsynced them.
	 */
	@Test
	void theFirstStartOfASeedFsyncsItsFiles() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		Path db = dataDir.resolve("db");
		checkpointWithData(db, 200, TermIndex.valueOf(1, 200));
		Set<String> tables = tableFileNames(db);
		assertFalse(tables.isEmpty());
		Path links = Files.createDirectories(tempDir.resolve("links"));
		try (Stream<Path> walk = Files.walk(db)) {
			for (Path file : walk.filter(Files::isRegularFile).toList()) {
				if (tables.contains(file.getFileName().toString())) {
					Files.createLink(links.resolve(file.getFileName()), file);
				}
			}
		}
		var synced = new java.util.concurrent.ConcurrentLinkedQueue<Path>();
		NodeStore.syncedForTesting = synced::add;
		try {
			NodeStore.recordSeedDigest(db);
		} finally {
			NodeStore.syncedForTesting = null;
		}
		Set<String> names = synced.stream().filter(path -> path.startsWith(db))
				.map(path -> path.getFileName().toString()).collect(Collectors.toSet());
		assertTrue(names.containsAll(tables), "synced " + synced + ", table files " + tables);
		assertTrue(names.contains("CURRENT"), "synced " + synced);
		assertTrue(synced.contains(db.toAbsolutePath().normalize()), "synced " + synced);
		assertTrue(synced.contains(dataDir.toAbsolutePath().normalize()), "synced " + synced);
	}

	/**
	 * R35-1: {@code restore-backup --data-dir /x/link/..}, with {@code link} pointing to {@code /real/a/b}, restores into
	 * the directory it then opens, the normalized {@code /x/db}, as the node's start and export-seed do. Before, the
	 * precondition, the build and the swap resolved the path through the OS and put the backup in {@code /real/a/db},
	 * while the open normalized it, created an empty {@code /x/db} and returned its (initial) position with OK.
	 */
	@Test
	void restoreBackupRestoresIntoTheDirectoryItOpensWhenTheDataDirHasDotDotAfterALink() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path realDataDir = Files.createDirectories(tempDir.resolve("real").resolve("a"));
		Files.createDirectories(realDataDir.resolve("b"));
		Path x = Files.createDirectories(tempDir.resolve("x"));
		Files.createSymbolicLink(x.resolve("link"), realDataDir.resolve("b"));
		Path dataDir = x.resolve("link").resolve("..");

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, null));

		assertFalse(Files.exists(realDataDir.resolve("db")), "restored where the OS resolves the data directory");
		assertNotNull(NodeStore.readSeedDigest(x.resolve("db")));
		try (var store = NodeStore.open(x.resolve("db"), null, "restored")) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied(), "the replication metadata is dropped");
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	/**
	 * R35-1: the node's start uses one form of {@code --data-dir} for the database and for Ratis' {@code raft/}: the
	 * normalized one, which {@link NodeStore#open} uses. Before, {@code dbDir()} and {@code raftDir()} were the raw
	 * path, which Ratis resolves through the OS ({@code /real/a/raft}) while the store's open normalizes
	 * ({@code /x/db}).
	 */
	@Test
	void theNodeStartUsesTheNormalizedDataDirectoryForTheDatabaseAndTheRaftLog() throws IOException {
		Path x = Files.createDirectories(tempDir.resolve("x"));
		Files.createSymbolicLink(x.resolve("link"), Files.createDirectories(tempDir.resolve("real").resolve("a").resolve("b")));
		var config = NodeConfig.defaults("a", List.of(new PeerConfig("a", "localhost", 1, "localhost", 2)),
				x.resolve("link").resolve(".."));
		assertEquals(x.toAbsolutePath().normalize(), config.dataDir());
		assertEquals(x.toAbsolutePath().normalize().resolve("db"), config.dbDir());
		assertEquals(x.toAbsolutePath().normalize().resolve("raft"), config.raftDir());
		assertEquals(x.toAbsolutePath().normalize(), config.withPeers(config.peers()).dataDir());
	}

	/**
	 * R35-2: {@code restore-backup} moves the old cluster's results file and backups aside, and fsyncs the data
	 * directory, before the restored database appears there: whenever {@code db/} is visible, even after a power loss,
	 * the old results cannot answer for the new cluster. Before, the moves came last and nothing fsynced them: every
	 * fsync of the data directory during the restore saw them still in place, next to the restored database.
	 */
	@Test
	void restoreBackupMovesTheOldResultsAsideDurablyBeforeTheDatabaseAppears() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");
		Files.createDirectories(dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("nightly-1"));
		Path absolute = dataDir.toAbsolutePath().normalize();
		var observed = dataDirStatesAtEachFsync(absolute, () -> ReplicaOps.restore(backupDir, dataDir, null));
		assertOldResultsDurablyAsideBeforeTheDatabase(observed);
		try (var files = Files.list(dataDir)) {
			var names = files.map(path -> path.getFileName().toString()).toList();
			assertTrue(names.stream().anyMatch(name -> name.startsWith("replica-ops.log.before-restore-")), "" + names);
			assertTrue(names.stream().anyMatch(name -> name.startsWith("backups.before-restore-")), "" + names);
		}
	}

	/**
	 * R35-2 and R36-3: a {@code restore-backup} that fails before the database is complete (here: a
	 * {@code --rockserver-config} that does not parse) leaves the data directory as it found it, the old cluster's
	 * results in place and no database, and its rerun moves them aside and restores. Since R36-1 every check and every
	 * read of the backup comes before the renames, so a refusal renames nothing; at R35 the renames came first and a
	 * failure left them done.
	 */
	@Test
	void aFailedRestoreBackupLeavesTheOldResultsInPlaceAndItsRerunMovesThem() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");
		Path badConfig = tempDir.resolve("bad.conf");
		Files.writeString(badConfig, "database { this does not parse");
		assertThrows(Exception.class, () -> ReplicaOps.restore(backupDir, dataDir, badConfig));
		assertEquals(List.of("replica-ops.log"), entryNames(dataDir), "what the failed restore left");

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, null));
		assertFalse(Files.exists(dataDir.resolve("replica-ops.log")));
		assertTrue(Files.isRegularFile(dataDir.resolve("db").resolve("CURRENT")));
	}

	/**
	 * R35 review 2: a rerun of a {@code restore-backup} killed after its renames and before their fsync finds nothing
	 * to rename, and still fsyncs the data directory before the database is restored: the renames it inherits are
	 * durable by its own fsync, not only by {@link NodeStore#restore}'s later one. Before, the fsync ran only if
	 * something moved, and the first fsync of the data directory came after {@code db.restore-ready} existed.
	 */
	@Test
	void restoreBackupFsyncsTheDataDirectoryBeforeTheDatabaseEvenWithNothingToMove() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		// What the killed run left: the renames done, possibly not yet durable.
		Files.writeString(dataDir.resolve("replica-ops.log.before-restore-1"), "old cluster\n");
		Path absolute = dataDir.toAbsolutePath().normalize();
		var observed = dataDirStatesAtEachFsync(absolute, () -> ReplicaOps.restore(backupDir, dataDir, null));
		assertOldResultsDurablyAsideBeforeTheDatabase(observed);
	}

	/**
	 * R36-1/R36-2: {@code restore-backup --backup DATA-DIR/backups/<name>}, the directory the node's own {@code backup}
	 * wrote, restores. Since R35-2 the command renamed {@code backups/} before it read the backup's database and failed
	 * with {@link java.nio.file.NoSuchFileException}; at 52ea5e3 it worked. Now everything is read from the backup, and
	 * the database built aside, before the renames; the backup ends up, intact, under
	 * {@code backups.before-restore-<time>}.
	 */
	@Test
	void restoreBackupRestoresABackupInsideTheDataDirectorysOwnBackupsDirectory() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Path backupDir = dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("2026-11-06");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, null));

		assertRestoredWithTheBackupMovedAside(dataDir, "2026-11-06");
	}

	/** R36-1/R36-2, through a symbolic link outside the data directory to {@code DATA-DIR/backups/<name>}. */
	@Test
	void restoreBackupRestoresABackupInsideTheOwnBackupsDirectoryThroughASymbolicLink() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Path backupDir = dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("nightly");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path link = Files.createSymbolicLink(tempDir.resolve("latest-backup"), backupDir);

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(link, dataDir, null));

		assertRestoredWithTheBackupMovedAside(dataDir, "nightly");
	}

	/**
	 * R36-1/R36-2: a rerun after a run that stopped between its renames and the database's swap names the place its
	 * {@code --backup DATA-DIR/backups/<name>} was moved to, rather than failing with a bare
	 * {@link java.nio.file.NoSuchFileException}; the rerun with that path restores.
	 */
	@Test
	void aRerunAfterTheRenamesSaysWhereTheBackupWasMoved() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Path moved = dataDir.resolve(ReplicaOps.BACKUPS_DIR + ".before-restore-1").resolve("b1");
		checkpointWithData(moved.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(moved.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");

		var refused = assertThrows(IOException.class,
				() -> ReplicaOps.restore(dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("b1"), dataDir, null));
		assertTrue(refused.getMessage().contains("--backup " + moved.toAbsolutePath().normalize()), refused.getMessage());
		assertFalse(Files.exists(dataDir.resolve("db")));

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(moved, dataDir, null));
	}

	/**
	 * R36-3: a newer cluster's backup is refused before anything is renamed: the old cluster's {@code replica-ops.log}
	 * and {@code backups/} stay in place, and no database, not even the one built aside to read the version, stays.
	 * The exit-code table says "nothing is written"; at R35 the renames had happened.
	 */
	@Test
	void aNewerClustersBackupIsRefusedBeforeAnythingIsRenamed() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Path backupDir = dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("newer");
		Path source = tempDir.resolve("newer-source");
		writeDatabaseOfVersion(source, NEWER_VERSION);
		try (var store = NodeStore.open(source, null, "source")) {
			store.checkpoint(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR));
		}
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=7\ncluster=c\n");
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");

		assertThrows(ReplicatedNode.NewerVersionException.class, () -> ReplicaOps.restore(backupDir, dataDir, null));

		assertEquals(List.of("backups", "replica-ops.log"), entryNames(dataDir), "what the refused restore left");
		assertTrue(Files.isRegularFile(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES)));
	}

	/**
	 * R36-1 guard: the database is built and opened aside, then renamed to {@code db/}; a Rockserver config whose
	 * volume is an absolute path inside {@code DATA-DIR/db} still restores, with the volume inside the restored
	 * database and nothing left aside.
	 */
	@Test
	void restoreBackupWithAnAbsoluteVolumePathInsideTheDatabase() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data")).toAbsolutePath().normalize();
		Path config = tempDir.resolve("absolute.conf");
		Files.writeString(config, """
				database.global.fallback-column-options.volumes = [
				  { volume-path: "%s", target-size: "10TiB" }
				]
				""".formatted(dataDir.resolve("db").resolve("volume")));

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, config));

		assertEquals(List.of("db", "db.seed-digest"), entryNames(dataDir));
		try (var store = NodeStore.open(dataDir.resolve("db"), config, "restored")) {
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	/**
	 * R36 review 1: a Rockserver config whose {@code wal-path} is an absolute path inside {@code DATA-DIR/db} (which the
	 * node's start accepts) restores: the build aside opens its WAL inside {@code db.restore-backup}, not in
	 * {@code DATA-DIR/db/wal}, which made {@code db/} appear before the renames and the wal-path check refuse the build.
	 */
	@Test
	void restoreBackupWithAnAbsoluteWalPathInsideTheDatabase() throws IOException {
		assertRestoresWithAnAbsolutePathInsideTheDatabase("wal-path", "wal");
	}

	/**
	 * R36 review 1: the same for {@code log-path}: the build's info log went to {@code DATA-DIR/db/logs}, so {@code db/}
	 * existed before the renames, and the rename of the build to {@code db/} failed on every run.
	 */
	@Test
	void restoreBackupWithAnAbsoluteLogPathInsideTheDatabase() throws IOException {
		assertRestoresWithAnAbsolutePathInsideTheDatabase("log-path", "logs");
	}

	private void assertRestoresWithAnAbsolutePathInsideTheDatabase(String key, String child) throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data")).toAbsolutePath().normalize();
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");
		Path config = tempDir.resolve("absolute.conf");
		Files.writeString(config, "database.global.%s = \"%s\"\n".formatted(key, dataDir.resolve("db").resolve(child)));

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, config));

		var names = entryNames(dataDir);
		assertEquals(3, names.size(), names.toString());
		assertEquals(List.of("db", "db.seed-digest"), names.subList(0, 2));
		assertTrue(names.get(2).startsWith("replica-ops.log.before-restore-"), names.toString());
		assertTrue(Files.isDirectory(dataDir.resolve("db").resolve(child)), "the " + key + " directory is inside db/");
		try (var store = NodeStore.open(dataDir.resolve("db"), config, "restored")) {
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	/**
	 * R36 review 2: after an earlier restore-backup that succeeded (its {@code db/} is there and {@code backups/} was
	 * renamed), a repeat with the old {@code --backup DATA-DIR/backups/<x>} is refused for the database it would
	 * overwrite, not told to run again with the moved path, which would be refused for the same database.
	 */
	@Test
	void aRepeatAfterASuccessfulRestoreIsRefusedForTheDatabaseNotSentToTheMovedBackup() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("data"));
		Path moved = dataDir.resolve(ReplicaOps.BACKUPS_DIR + ".before-restore-1").resolve("b1");
		checkpointWithData(moved.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(moved.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(moved, dataDir, null));

		var refused = assertThrows(IOException.class,
				() -> ReplicaOps.restore(dataDir.resolve(ReplicaOps.BACKUPS_DIR).resolve("b1"), dataDir, null));
		assertTrue(refused.getMessage().contains("holds a database"), refused.getMessage());
		assertFalse(refused.getMessage().contains("run again with"), refused.getMessage());
	}

	/**
	 * R38-RESTORE-STALE-RESTORE-READY: a data directory without {@code db/} but with a complete {@code db.restore-ready}
	 * of another cluster (and its {@code db.restore-old}: the layout a crash between the two renames of a snapshot
	 * install's swap leaves, R33-1) is refused before anything is read, built or renamed, with a message that names the
	 * leftover. Since R36-1 the database is built aside, so nothing rolled that directory in first: the restore printed
	 * OK, and the first start's open swapped the old cluster's database over the restored {@code db/} and deleted it.
	 */
	@Test
	void restoreBackupRefusesAnInterruptedSwapOfAnotherClusterAndTouchesNothing() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data")).toAbsolutePath().normalize();
		for (String name : List.of("db.restore-ready", "db.restore-old")) {
			try (var store = NodeStore.open(dataDir.resolve(name), null, "old")) {
				long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
				put(store, column, 7, name);
				setApplied(store, TermIndex.valueOf(4, 40));
				setClusterId(store, "old-cluster");
			}
		}
		Files.writeString(dataDir.resolve("replica-ops.log"), "old cluster\n");
		var before = tree(dataDir);
		var backupBefore = tree(backupDir);

		var refused = assertThrows(IOException.class, () -> ReplicaOps.restore(backupDir, dataDir, null));

		assertTrue(refused.getMessage().contains(dataDir.resolve("db.restore-ready").toString()), refused.getMessage());
		assertTrue(refused.getMessage().contains("delete"), refused.getMessage());
		assertEquals(before, tree(dataDir), "the refused restore changed the data directory");
		assertEquals(backupBefore, tree(backupDir), "the refused restore changed the backup");
	}

	/**
	 * R38-RESTORE-READY-LEFTOVER: each restore directory of {@code db/} alone ({@code db.restore-ready},
	 * {@code db.restore-old}, {@code db.restore-tmp}, which the first start's open would swap in or delete) is refused
	 * the same way, naming it, with nothing touched.
	 */
	@Test
	void restoreBackupRefusesEachLeftoverRestoreDirectoryOfTheDatabase() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		for (String name : List.of("db.restore-ready", "db.restore-old", "db.restore-tmp")) {
			Path dataDir = Files.createDirectories(tempDir.resolve("data-" + name)).toAbsolutePath().normalize();
			Path leftover = Files.createDirectories(dataDir.resolve(name));
			Files.writeString(leftover.resolve("CURRENT"), "kept");
			var before = tree(dataDir);

			var refused = assertThrows(IOException.class, () -> ReplicaOps.restore(backupDir, dataDir, null), name);

			assertTrue(refused.getMessage().contains(leftover.toString()), refused.getMessage());
			assertEquals(before, tree(dataDir), name);
		}
	}

	/**
	 * R38 review 2: a rerun of a {@code restore-backup} that stopped while it built the database aside still deletes
	 * what that run left in {@code db.restore-backup} and in that directory's own restore directories
	 * ({@code db.restore-backup.restore-tmp}, {@code -ready}, {@code -old}), after the refusal of {@code db/}'s restore
	 * directories: those are other names (none is a prefix of {@code db.restore-backup}), so the refusal leaves the
	 * staging leftovers to the cleanup that follows it, and the rerun restores.
	 */
	@Test
	void restoreBackupRerunDeletesWhatAStoppedRunLeftAside() throws IOException {
		Path backupDir = tempDir.resolve("backup");
		checkpointWithData(backupDir.resolve(ReplicaOps.BACKUP_DB_DIR), 10, TermIndex.valueOf(1, 10));
		Files.writeString(backupDir.resolve(ReplicaOps.BACKUP_PROPERTIES), "name=b\nindex=10\ncluster=c\n");
		Path dataDir = Files.createDirectories(tempDir.resolve("data")).toAbsolutePath().normalize();
		var leftovers = new ArrayList<Path>();
		Path staging = dataDir.resolve(ReplicaOps.RESTORE_STAGING_DIR);
		leftovers.add(staging);
		leftovers.addAll(NodeStore.restoreDirectories(staging));
		for (Path leftover : leftovers) {
			Files.createDirectories(leftover);
			Files.writeString(leftover.resolve("CURRENT"), "left by the stopped run");
		}

		assertEquals(TermIndex.valueOf(1, 10), ReplicaOps.restore(backupDir, dataDir, null));

		for (Path leftover : leftovers) {
			assertFalse(Files.exists(leftover, LinkOption.NOFOLLOW_LINKS), leftover + " is still there");
		}
		try (var store = NodeStore.open(dataDir.resolve("db"), null, "restored")) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied());
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	/**
	 * R38-EXPORT-WALPATH-UNTESTED (R36 review 1 on export-seed's side): a Rockserver config whose {@code wal-path} is an
	 * absolute path inside the source's {@code db/} exports a seed whose copy opens its WAL under the target, not in the
	 * source's WAL directory; the source and the seed both open with their data.
	 */
	@Test
	void exportSeedWithAnAbsoluteWalPathInsideTheSourceDatabase() throws IOException {
		assertExportsWithAnAbsolutePathInsideTheSourceDatabase("wal-path", "wal");
	}

	/** The same for {@code log-path}: the copy's info log goes under the target, not into the source's log directory. */
	@Test
	void exportSeedWithAnAbsoluteLogPathInsideTheSourceDatabase() throws IOException {
		assertExportsWithAnAbsolutePathInsideTheSourceDatabase("log-path", "logs");
	}

	private void assertExportsWithAnAbsolutePathInsideTheSourceDatabase(String key, String child) throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node")).toAbsolutePath().normalize();
		Path source = dataDir.resolve("db");
		Path config = tempDir.resolve("absolute.conf");
		Files.writeString(config, "database.global.%s = \"%s\"\n".formatted(key, source.resolve(child)));
		try (var store = NodeStore.open(source, config, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 10; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 10));
		}
		assertTrue(Files.isDirectory(source.resolve(child)), "the source's " + key + " is inside its db/");
		Path target = tempDir.resolve("exported-seed").toAbsolutePath().normalize();

		// What the checkpoint put in the target's directory for it (the WAL is copied there), before the copy opens.
		var checkpointed = new AtomicReference<List<String>>(List.of());
		var sourceAtCheckpoint = new AtomicReference<Map<String, String>>();
		NodeStore.checkpointBuiltForTesting = building -> {
			sourceAtCheckpoint.set(fileDigests(source.resolve(child)));
			if (Files.isDirectory(building.resolve(child))) {
				checkpointed.set(entryNames(building.resolve(child)));
			}
		};
		try {
			assertEquals(TermIndex.valueOf(1, 10), NodeStore.exportSeed(source, config, target).applied());
		} finally {
			NodeStore.checkpointBuiltForTesting = null;
		}

		assertTrue(Files.isDirectory(target.resolve(child)), "the copy's " + key + " directory is inside the target");
		// The copy's open writes a new WAL, or its info log, under the target: before, into the source's directory.
		var written = new ArrayList<>(entryNames(target.resolve(child)));
		written.removeAll(checkpointed.get());
		assertFalse(written.isEmpty(), "the copy wrote no file into " + target.resolve(child) + ", which holds only "
				+ checkpointed.get());
		// The source's directory as the source left it, against its state when the checkpoint was built (the source
		// open, before its close and the copy's open). RocksDB numbers WAL files NNNNNN.log: a copy that opened the
		// source's WAL directory would add a WAL there (or change one); a file the source deleted as obsolete is fine.
		// The source's close appends to its own info log, so only the WAL files are compared by content (R38 review 1).
		var after = fileDigests(source.resolve(child));
		var added = new java.util.TreeSet<>(after.keySet());
		added.removeAll(sourceAtCheckpoint.get().keySet());
		assertEquals(Set.of(), added, "the copy wrote into the source's " + key);
		if (key.equals("wal-path")) {
			for (var file : after.entrySet()) {
				assertEquals(sourceAtCheckpoint.get().get(file.getKey()), file.getValue(),
						"the copy changed the source's WAL file " + file.getKey());
			}
		}
		try (var seed = NodeStore.open(target, null, "seed")) {
			assertEquals(10, contents(seed, seed.db().getColumnId(DATA_COLUMN)).size());
		}
		try (var store = NodeStore.open(source, config, "source")) {
			assertEquals(TermIndex.valueOf(1, 10), store.applied());
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
	}

	/** Every entry under {@code dir}: a directory as "dir", a file by its size and content digest. */
	private static Map<String, String> tree(Path dir) throws IOException {
		var result = new TreeMap<String, String>(fileDigests(dir));
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path path : walk.filter(Files::isDirectory).toList()) {
				result.put(dir.relativize(path) + "/", "dir");
			}
		}
		return result;
	}

	private void assertRestoredWithTheBackupMovedAside(Path dataDir, String name) throws IOException {
		try (var store = NodeStore.open(dataDir.resolve("db"), null, "restored")) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied(), "the replication metadata is dropped");
			assertEquals(10, contents(store, store.db().getColumnId(DATA_COLUMN)).size());
		}
		assertNotNull(NodeStore.readSeedDigest(dataDir.resolve("db")));
		assertFalse(Files.exists(dataDir.resolve(ReplicaOps.BACKUPS_DIR)), "the old backups are still in place");
		Path aside;
		try (Stream<Path> list = Files.list(dataDir)) {
			aside = list.filter(path -> path.getFileName().toString().startsWith(ReplicaOps.BACKUPS_DIR + ".before-restore-"))
					.findFirst().orElseThrow();
		}
		assertTrue(Files.isRegularFile(aside.resolve(name).resolve(ReplicaOps.BACKUP_PROPERTIES)), "the backup moved");
		assertTrue(Files.isRegularFile(aside.resolve(name).resolve(ReplicaOps.BACKUP_DB_DIR).resolve("CURRENT")));
	}

	private static List<String> entryNames(Path dir) throws IOException {
		try (Stream<Path> list = Files.list(dir)) {
			return list.map(path -> path.getFileName().toString()).sorted().toList();
		}
	}

	/**
	 * The guarantee of R35-2, over {@link #dataDirStatesAtEachFsync}: no fsync of the data directory sees the old
	 * results next to {@code db/}, and the last fsync before {@code db/} first appears sees neither the old results nor
	 * {@code db/}, so the renames are durable before the database can be. Since R36-1 the database is built aside, and
	 * fsyncs of the data directory during that build may still see the old results.
	 */
	private static void assertOldResultsDurablyAsideBeforeTheDatabase(java.util.Queue<String> observed) {
		var states = List.copyOf(observed);
		java.util.function.Predicate<String> hasDb = state -> !state.endsWith("no db");
		java.util.function.Predicate<String> hasOld = state -> state.contains("log") || state.contains("backups");
		assertTrue(states.stream().noneMatch(state -> hasDb.test(state) && hasOld.test(state)),
				"fsyncs of the data directory: " + states);
		int firstDb = 0;
		while (firstDb < states.size() && !hasDb.test(states.get(firstDb))) {
			firstDb++;
		}
		assertTrue(firstDb > 0 && firstDb < states.size(), "fsyncs of the data directory: " + states);
		String before = states.get(firstDb - 1);
		assertFalse(hasDb.test(before) || hasOld.test(before), "fsyncs of the data directory: " + states);
	}

	/**
	 * What {@code dataDir} holds at each of its fsyncs during {@code action}: {@code log} and {@code backups} for the old
	 * results in place, {@code restore} for any {@code db.restore-*} entry, then {@code db} or {@code no db}.
	 */
	private static java.util.Queue<String> dataDirStatesAtEachFsync(Path dataDir, IoAction action) throws IOException {
		var observed = new java.util.concurrent.ConcurrentLinkedQueue<String>();
		NodeStore.syncedForTesting = path -> {
			if (path.equals(dataDir)) {
				boolean restoreEntry;
				try (Stream<Path> list = Files.list(dataDir)) {
					restoreEntry = list.anyMatch(entry -> entry.getFileName().toString().startsWith("db.restore-"));
				} catch (IOException e) {
					throw new java.io.UncheckedIOException(e);
				}
				observed.add((Files.exists(dataDir.resolve("replica-ops.log")) ? "log " : "")
						+ (Files.exists(dataDir.resolve(ReplicaOps.BACKUPS_DIR)) ? "backups " : "")
						+ (restoreEntry ? "restore " : "")
						+ (Files.exists(dataDir.resolve("db")) ? "db" : "no db"));
			}
		};
		try {
			action.run();
		} finally {
			NodeStore.syncedForTesting = null;
		}
		return observed;
	}

	@FunctionalInterface
	interface IoAction {
		void run() throws IOException;
	}

	/** The names of the files and directories under {@code root} that {@code action} fsynced. */
	private static Set<String> syncedNamesUnder(Path root, IoAction action) throws IOException {
		Path absolute = root.toAbsolutePath().normalize();
		var synced = new java.util.concurrent.ConcurrentLinkedQueue<Path>();
		NodeStore.syncedForTesting = synced::add;
		try {
			action.run();
		} finally {
			NodeStore.syncedForTesting = null;
		}
		return synced.stream().filter(path -> path.startsWith(absolute)).map(path -> path.getFileName().toString())
				.collect(Collectors.toSet());
	}

	private static Set<String> tableFileNames(Path dir) throws IOException {
		try (Stream<Path> walk = Files.walk(dir)) {
			return walk.filter(Files::isRegularFile).map(path -> path.getFileName().toString())
					.filter(name -> name.endsWith(".sst") || name.endsWith(".blob")).collect(Collectors.toSet());
		}
	}

	/** The highest link count among the files under {@code dir} named in {@code names} (0 if none is there). */
	private static int maxLinkCount(Path dir, Set<String> names) throws IOException {
		int max = 0;
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path file : walk.filter(Files::isRegularFile).toList()) {
				if (names.contains(file.getFileName().toString())) {
					max = Math.max(max, (Integer) Files.getAttribute(file, "unix:nlink"));
				}
			}
		}
		return max;
	}

	/**
	 * The layout a crash during {@link NodeStore#restore}'s swap leaves in {@code dataDir}: {@code db.restore-ready}
	 * holds the new database (key 7 = "new", applied 4:40), the old one (key 7 = "old", applied 1:5) is
	 * {@code db.restore-old} when {@code betweenTheRenames}, else still {@code db/}.
	 */
	static void writeRestoreInterruptedDuringTheSwap(Path dataDir, boolean betweenTheRenames) throws IOException {
		// The old database first: an open of db/ next to db.restore-ready would finish the swap.
		writeDatabase(dataDir.resolve("db"), "old", TermIndex.valueOf(1, 5));
		writeDatabase(dataDir.resolve("db.restore-ready"), "new", TermIndex.valueOf(4, 40));
		if (betweenTheRenames) {
			Files.move(dataDir.resolve("db"), dataDir.resolve("db.restore-old"));
		}
	}

	private static void writeDatabase(Path dbDir, String value, TermIndex applied) throws IOException {
		try (var store = NodeStore.open(dbDir, null, "written")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 7, value);
			setApplied(store, applied);
		}
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
		store.writeClusterId(clusterId);
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

	/**
	 * R29-1: an {@code export-seed} whose checkpoint fails leaves neither {@code <target>.tmp} (hard links to every live
	 * file of the source) nor the target, so it can be run again as it was.
	 */
	@Test
	void aFailedExportLeavesNoTemporaryCopy() throws IOException {
		Path source = tempDir.resolve("node/db");
		try (var store = NodeStore.open(source, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 1, "v1");
		}
		Path target = tempDir.resolve("seed");
		NodeStore.checkpointBuiltForTesting = building -> {
			throw new IOException("No space left on device (test)");
		};
		try {
			assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, target));
		} finally {
			NodeStore.checkpointBuiltForTesting = null;
		}
		assertFalse(Files.exists(target), "a half-made target");
		assertFalse(Files.exists(tempDir.resolve("seed.tmp")), "the checkpoint's building directory");
		try (var leftovers = Files.list(tempDir)) {
			assertTrue(leftovers.noneMatch(path -> path.getFileName().toString().startsWith("rockserver-replica-")));
		}
		NodeStore.exportSeed(source, null, target);
		assertTrue(Files.isRegularFile(target.resolve("CURRENT")));
	}

	/**
	 * R44-2: {@code export-seed} drops the old cluster's meta column (cluster id, applied position, sessions) before the
	 * copy gets the target's name. Before, it renamed the checkpoint to the target first and dropped the meta column in
	 * place: a kill (OOM, SIGKILL, power loss) in between left a complete-looking target that every node of the new
	 * cluster refused, and a rerun said only that the target exists. The test copies the directories as a kill at the
	 * copy's open would leave them: no target, only {@code <target>.tmp}, which a rerun refuses by name.
	 */
	@Test
	void anExportKilledWhileDroppingTheMetaColumnLeavesNoTarget() throws IOException {
		Path source = tempDir.resolve("source");
		try (var store = NodeStore.open(source, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 7, "v");
			setApplied(store, TermIndex.valueOf(1, 5));
			setClusterId(store, "old-cluster");
		}
		Path target = tempDir.resolve("seed");
		Path building = tempDir.resolve("seed.tmp");
		Path crash = tempDir.resolve("crash");
		NodeStore.exportCopyOpeningForTesting = copy -> {
			try {
				for (Path dir : List.of(target, building)) {
					if (Files.exists(dir)) {
						copyTree(dir, crash.resolve(dir.getFileName().toString()));
					}
				}
			} catch (IOException e) {
				throw new java.io.UncheckedIOException(e);
			}
		};
		try {
			NodeStore.exportSeed(source, null, target);
		} finally {
			NodeStore.exportCopyOpeningForTesting = null;
		}
		assertFalse(Files.exists(crash.resolve("seed")),
				"a kill while the copy still held the old cluster's meta column leaves a complete-looking target");
		assertTrue(Files.isRegularFile(crash.resolve("seed.tmp/CURRENT")));
		var refused = assertThrows(IOException.class, () -> NodeStore.exportSeed(source, null, crash.resolve("seed")));
		assertTrue(refused.getMessage().contains("seed.tmp already exists"), refused.getMessage());

		// The export that ran on: the target, without the meta column.
		try (var store = NodeStore.open(target, null, "seed")) {
			assertNull(store.clusterId());
			assertEquals(TermIndex.INITIAL_VALUE, store.applied());
		}
	}

	/**
	 * Review of R44, 1: export-seed opens its copy in {@code <target>.tmp} to drop the meta column while the source is
	 * still open. Two databases with the config's block cache (512 MiB by default) and every table reader opened
	 * ({@code maximum-open-files: -1}) would make an OOM exit during the export more likely. The copy's open only drops
	 * a column and flushes: it reads with a small block cache, no metadata reserve and a bounded table cache, also when
	 * the config asks for a large reserve and asynchronous file opening (which needs every file open).
	 */
	@Test
	void anExportOpensItsCopyWithASmallFootprint() throws IOException {
		Path rockserverConfig = Files.writeString(tempDir.resolve("rockserver.conf"), """
				database.global.block-cache = 1GiB
				database.global.block-cache-metadata-size = 256MiB
				database.global.open-files-async = true
				""");
		Path source = tempDir.resolve("source");
		try (var store = NodeStore.open(source, rockserverConfig, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 7, "v");
			setApplied(store, TermIndex.valueOf(1, 5));
		}
		Path target = tempDir.resolve("seed");
		var seen = new AtomicReference<com.typesafe.config.Config>();
		NodeStore.exportCopyOpeningForTesting = copy -> seen.set(
				ConfigFactory.parseFile(NodeStore.replicaConfigFile(target).toFile()).resolve());
		try {
			NodeStore.exportSeed(source, rockserverConfig, target);
		} finally {
			NodeStore.exportCopyOpeningForTesting = null;
		}
		var config = seen.get();
		assertNotNull(config, "the copy was not opened");
		assertEquals("8MiB", config.getString("database.global.block-cache"));
		assertEquals("0B", config.getString("database.global.block-cache-metadata-size"));
		assertEquals(NodeStore.EXPORT_COPY_MAX_OPEN_FILES, config.getInt("database.global.maximum-open-files"));
		assertFalse(config.getBoolean("database.global.open-files-async"));
		// What RocksDB really ran the copy's open with: its OPTIONS file, the newest one in the target.
		Path latest;
		try (var files = Files.list(target)) {
			latest = files.filter(file -> file.getFileName().toString().matches("OPTIONS-[0-9]+"))
					.max(Comparator.comparingLong(file -> Long.parseLong(file.getFileName().toString().substring(8))))
					.orElseThrow();
		}
		assertTrue(Files.readAllLines(latest).stream().map(String::strip)
				.anyMatch(line -> line.equals("max_open_files=" + NodeStore.EXPORT_COPY_MAX_OPEN_FILES)), latest.toString());
		try (var store = NodeStore.open(target, rockserverConfig, "seed")) {
			assertEquals(TermIndex.INITIAL_VALUE, store.applied());
		}
	}

	/**
	 * R29-2: the config file a replica opens its database with is deleted once the database opened, but a process killed
	 * during the open (WAL replay, a large MANIFEST) left it in DATA-DIR for good, one per killed start. The next open
	 * removes what earlier opens of that database left.
	 */
	@Test
	void aConfigLeftByAKilledOpenIsRemovedByTheNextOpen() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		// What NodeStore.open of older versions made, and a killed process left behind.
		var left = List.of(Files.createTempFile(dataDir, "rockserver-replica-", ".conf"),
				Files.createTempFile(dataDir, "rockserver-replica-", ".conf"));
		var unrelated = Files.writeString(dataDir.resolve("rockserver-replica-notes.txt"), "not a config of ours");
		try (var store = NodeStore.open(dataDir.resolve("db"), null, "n0")) {
			assertNull(store.clusterId());
		}
		for (var file : left) {
			assertFalse(Files.exists(file), file + " is still there");
		}
		assertTrue(Files.exists(unrelated));
		try (var files = Files.list(dataDir)) {
			assertEquals(List.of(), files.map(path -> path.getFileName().toString())
					.filter(name -> name.startsWith("rockserver-replica-") && name.endsWith(".conf")).toList());
		}
	}

	/**
	 * R30-1: an {@code export-seed} target next to the source whose name is all digits (a date-named seed kept in
	 * DATA-DIR so its files hard-link). Its config was named {@code rockserver-replica-<digits>.conf}, which the source's
	 * open took for a config that an older version's killed open left, and deleted before the copy opened with it: the
	 * export failed on every retry with that target.
	 */
	@Test
	void anExportIntoAnAllDigitSiblingOfTheSourceWorks() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		Path source = dataDir.resolve("db");
		try (var store = NodeStore.open(source, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 1, "v1");
		}
		Path target = dataDir.resolve("20261108");
		NodeStore.exportSeed(source, null, target);
		assertTrue(Files.isRegularFile(target.resolve("CURRENT")));
		try (var files = Files.list(dataDir)) {
			assertEquals(List.of(), files.map(path -> path.getFileName().toString())
					.filter(name -> name.startsWith("rockserver-replica-")).toList());
		}
	}

	/**
	 * R30-1: the config of a database whose directory name is all digits, written for an open in progress, is not one
	 * that an older version's killed open left: an open of another database in the same directory leaves it alone.
	 */
	@Test
	void theConfigOfAnAllDigitDatabaseIsNotTakenForALeftover() throws IOException {
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		Path inUse = NodeStore.replicaConfigFile(dataDir.resolve("20261108"));
		Files.writeString(inUse, "{}");
		Path legacy = Files.createTempFile(dataDir, "rockserver-replica-", ".conf");
		try (var store = NodeStore.open(dataDir.resolve("db"), null, "n0")) {
			assertNull(store.clusterId());
		}
		assertTrue(Files.exists(inUse), inUse + " was deleted");
		assertFalse(Files.exists(legacy), legacy + " is still there");
	}

	/**
	 * R31-1: the config a replica opens its database with holds the resolved Rockserver config, with the InfluxDB token
	 * and every {@code ${?ENV}} substitution in plain text: it is owner-only (rw-------) as R29-2's
	 * {@code Files.createTempFile} made it, whatever the umask, and also when it replaces the file a killed open left
	 * with a wider mode.
	 */
	@Test
	void theReplicaConfigIsOwnerOnly() throws IOException {
		Assumptions.assumeTrue(posix(tempDir), "a POSIX file system");
		Path dataDir = Files.createDirectories(tempDir.resolve("node"));
		Path dbDir = dataDir.resolve("db");
		Path config = NodeStore.replicaConfig(null, dbDir, null, "n0");
		assertEquals(NodeStore.replicaConfigFile(dbDir), config);
		assertEquals("rw-------", mode(config), "a new config");
		Files.delete(config);

		// What a killed open left, with the mode that round 29 to 30 gave it (the umask's, usually 0644).
		Files.writeString(config, "{}");
		Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-r--r--"));
		NodeStore.replicaConfig(null, dbDir, null, "n0");
		assertEquals("rw-------", mode(config), "a config that replaced a leftover");
		assertTrue(Files.readString(config).contains("n0"), Files.readString(config));
		Files.delete(config);
	}

	/**
	 * R31-1: the config of an {@code export-seed} target, which lives next to the target while the source is copied,
	 * is owner-only too, also when it replaces a leftover with a wider mode.
	 */
	@Test
	void theExportTargetConfigIsOwnerOnly() throws IOException {
		Assumptions.assumeTrue(posix(tempDir), "a POSIX file system");
		Path source = tempDir.resolve("node/db");
		try (var store = NodeStore.open(source, null, "source")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 1, "v1");
		}
		Path target = tempDir.resolve("seed");
		Path config = NodeStore.replicaConfigFile(target);
		Files.writeString(config, "{}");
		Files.setPosixFilePermissions(config, PosixFilePermissions.fromString("rw-rw-r--"));
		var seen = new AtomicReference<String>();
		NodeStore.checkpointBuiltForTesting = building -> seen.set(mode(config));
		try {
			NodeStore.exportSeed(source, null, target);
		} finally {
			NodeStore.checkpointBuiltForTesting = null;
		}
		assertEquals("rw-------", seen.get(), "the target's config while the source was copied");
		assertFalse(Files.exists(config));
	}

	static boolean posix(Path dir) {
		return dir.getFileSystem().supportedFileAttributeViews().contains("posix");
	}

	/**
	 * A WAL directory outside the database directory (an absolute {@code wal-path}): a checkpoint carries its WAL files
	 * in {@link NodeStore#DETACHED_WAL}, and the open of the database restored from it on another such node deletes the
	 * WAL files of the database that was there and moves the checkpoint's in.
	 */
	@Test
	void aCheckpointOfANodeWithAnOutsideWalRestoresOnAnotherSuchNode() throws IOException {
		Path walA = tempDir.resolve("wal-a").toAbsolutePath().normalize();
		Path walB = tempDir.resolve("wal-b").toAbsolutePath().normalize();
		Path configA = Files.writeString(tempDir.resolve("a.conf"), "database.global.wal-path = \"%s\"\n".formatted(walA));
		Path configB = Files.writeString(tempDir.resolve("b.conf"), "database.global.wal-path = \"%s\"\n".formatted(walB));
		Path checkpoint = tempDir.resolve("checkpoint");
		Path dbA = tempDir.resolve("a").resolve("db");
		Expected expected;
		try (var store = NodeStore.open(dbA, configA, "a")) {
			assertTrue(store.directories().contains(walA), store.directories().toString());
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 10; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 10));
			store.checkpoint(checkpoint);
			expected = Expected.of(store, column);
		}
		assertTrue(Files.isDirectory(checkpoint.resolve(NodeStore.DETACHED_WAL)), "no detached WAL in the checkpoint");
		assertFalse(Files.exists(dbA.resolve(NodeStore.DETACHED_WAL)));

		Path dbB = tempDir.resolve("b").resolve("db");
		try (var store = NodeStore.open(dbB, configB, "b")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			put(store, column, 99, "old");
		}
		Files.writeString(walB.resolve("999999.log"), "a WAL file of the database that is replaced");
		NodeStore.restore(checkpoint, dbB);
		try (var store = NodeStore.open(dbB, configB, "b")) {
			assertEquals(expected, Expected.of(store, store.db().getColumnId(DATA_COLUMN)));
			assertFalse(Files.exists(walB.resolve("999999.log")), "the old database's WAL file stayed");
			assertFalse(Files.exists(dbB.resolve(NodeStore.DETACHED_WAL)));
			assertFalse(Files.exists(dbB.resolve(NodeStore.WAL_RESET)));
		}
		// Again: nothing is left to move, and the WAL files in the outside directory are the database's own.
		try (var store = NodeStore.open(dbB, configB, "b")) {
			assertEquals(expected, Expected.of(store, store.db().getColumnId(DATA_COLUMN)));
		}
	}

	/** A checkpoint of a node with an outside WAL restores on a node whose WAL is inside its database, and back. */
	@Test
	void aCheckpointOfANodeWithAnOutsideWalRestoresOnANodeWithItsWalInside() throws IOException {
		Path walA = tempDir.resolve("wal-a").toAbsolutePath().normalize();
		Path configA = Files.writeString(tempDir.resolve("a.conf"), "database.global.wal-path = \"%s\"\n".formatted(walA));
		Path checkpoint = tempDir.resolve("checkpoint");
		Expected expected;
		try (var store = NodeStore.open(tempDir.resolve("a").resolve("db"), configA, "a")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			for (int i = 0; i < 10; i++) {
				put(store, column, i, "v" + i);
			}
			setApplied(store, TermIndex.valueOf(1, 10));
			store.checkpoint(checkpoint);
			expected = Expected.of(store, column);
		}
		Path dbB = tempDir.resolve("b").resolve("db");
		NodeStore.restore(checkpoint, dbB);
		assertMatchesCheckpoint(dbB, expected);
		assertFalse(Files.exists(dbB.resolve(NodeStore.DETACHED_WAL)));
	}

	/** WAL files in an outside WAL directory with no database: refused, they are another database's. */
	@Test
	void anOutsideWalDirectoryWithWalFilesAndNoDatabaseIsRefused() throws IOException {
		Path wal = Files.createDirectories(tempDir.resolve("wal")).toAbsolutePath().normalize();
		Path config = Files.writeString(tempDir.resolve("n.conf"), "database.global.wal-path = \"%s\"\n".formatted(wal));
		Files.writeString(wal.resolve("000012.log"), "left");
		var refused = assertThrows(IOException.class, () -> NodeStore.open(tempDir.resolve("n").resolve("db"), config, "n"));
		assertTrue(refused.getMessage().contains("belong to a database that was deleted"), refused.getMessage());
		assertTrue(Files.exists(wal.resolve("000012.log")));
	}

	/**
	 * Production, 2026-11-12: with {@code adaptive-compaction-io = true} (copied from a single node's config) core's
	 * compaction I/O limiter sat at its floor, 40 KiB/s, on a replica that had not flushed yet, and the flush inside the
	 * first snapshot took 206 to 229 s on each of three nodes while their applies waited. A replica runs without that
	 * limiter whatever the config says: its checkpoints flush on the apply path.
	 */
	@Test
	void aCheckpointIsNotThrottledByTheAdaptiveCompactionIoLimiter() throws Exception {
		Path config = Files.writeString(tempDir.resolve("adaptive.conf"), "database.global.adaptive-compaction-io = true\n");
		try (var store = NodeStore.open(tempDir.resolve("adaptive").resolve("db"), config, "adaptive")) {
			long column = store.db().createColumn(DATA_COLUMN, DATA_SCHEMA);
			// A first, tiny flush: the limiter takes the rate of its first active window as its budget (here a few
			// bytes a second, so its floor), and then grows it by a tenth per window.
			// After the limiter's first poll (it counts bytes between polls, one a second, in windows of five).
			Thread.sleep(2500);
			put(store, column, 100_000, "small");
			store.db().flush();
			Thread.sleep(8000);
			var random = new java.util.Random(7);
			var chars = new char[64 * 1024];
			for (int i = 0; i < 256; i++) {
				for (int c = 0; c < chars.length; c++) {
					chars[c] = (char) ('a' + random.nextInt(26));
				}
				put(store, column, i, new String(chars));
			}
			setApplied(store, TermIndex.valueOf(1, 256));
			// The limiter's controller polls in the background: give it time to settle where it would.
			Thread.sleep(3000);
			assertEquals("none", limiterState(store), "a replica must run without core's compaction I/O limiter");
			long start = System.nanoTime();
			store.checkpoint(tempDir.resolve("adaptive-checkpoint"));
			long millis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(millis < 10_000, "a checkpoint of 16 MiB of memtables took " + millis + " ms");
		}
	}

	/** The budget and state of core's compaction I/O limiter, "none" without one (read by reflection: core hides it). */
	private static String limiterState(NodeStore store) throws ReflectiveOperationException {
		var controllerField = store.db().getClass().getDeclaredField("compactionIoController");
		controllerField.setAccessible(true);
		Object controller = controllerField.get(store.db());
		if (controller == null) {
			return "none";
		}
		var budgetField = controller.getClass().getDeclaredField("budget");
		budgetField.setAccessible(true);
		var budget = (it.cavallium.rockserver.core.impl.CompactionIoBudget) budgetField.get(controller);
		return budget.budget() + " B/s, " + budget.state();
	}

	static String mode(Path file) throws IOException {
		Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(file);
		return PosixFilePermissions.toString(permissions);
	}
}
