package it.cavallium.rockserver.distributed;

import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.LogFile;
import org.rocksdb.RocksDB;
import org.rocksdb.WalFileType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The node's local Rockserver database plus the replication metadata stored inside it.
 *
 * <p>Metadata lives in the internal column {@value #META_COLUMN} (fixed 17-byte keys: a kind byte, then two
 * longs), so that it is written in the same RocksDB WAL as the data and travels inside checkpoints:</p>
 * <ul>
 *   <li>{@link #APPLIED}: the last applied {@link TermIndex}, encoded by {@link #encodeTermIndex}</li>
 *   <li>{@link #CLUSTER_ID}: UTF-8 cluster id, written when the {@link LogEntry.Genesis} entry is applied</li>
 *   <li>{@link #REQUEST} + proposer id + sequence: the encoded result of an applied {@link Request}, kept until its
 *   proposer reports it complete</li>
 * </ul>
 *
 * <p>The meta column is created locally when a database is first opened, before any log entry is applied, so it
 * gets the same column id on every replica. No other column may ever be created locally.</p>
 *
 * <p>Every database file must live under the database directory: column volumes and a WAL directory inside it
 * (such as the default {@code ./volume} and {@code ./wal}) are supported, directories elsewhere are not. The
 * database directory and everything under it must be on the same file system as its parent directory, because
 * {@link #restore} swaps it by renaming: mount the node's data directory, not the database directory or a volume
 * inside it. {@link #open} refuses other layouts.</p>
 *
 * <p>The database is opened with the node's Rockserver config plus {@link #REPLICA_OVERRIDES}, settings that every
 * replica needs whatever that config says.</p>
 */
final class NodeStore implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(NodeStore.class);

	static final String META_COLUMN = "__rockserver_replication";
	static final byte APPLIED = 1;
	static final byte CLUSTER_ID = 2;
	static final byte REQUEST = 3;

	private static final int META_KEY_BYTES = 1 + 2 * Long.BYTES;
	private static final ColumnSchema META_SCHEMA = ColumnSchema.of(IntList.of(META_KEY_BYTES), ObjectList.of(), true);
	private static final int TERM_INDEX_BYTES = 2 * Long.BYTES;

	/**
	 * The RocksDB WAL is not synced, so a crash can tear its tail: a power loss keeps an arbitrary prefix of what was
	 * written, and with a delayed WAL flush ({@code delay-wal-flush-duration}) even a killed process loses its
	 * buffer. A replica then needs the consistent prefix that RocksDB's point-in-time recovery opens, and replays the
	 * rest from the Raft log. Rockserver's {@code absolute-consistency} (on by default) would refuse to open such a
	 * database, so the replica could not restart without being repaired.
	 */
	private static final String REPLICA_OVERRIDES = "database.global.absolute-consistency = false\n";

	private final Path dbDir;
	/** RocksDB WAL directory: {@code dbDir} itself or the configured {@code wal-path} (default {@code ./wal}). */
	private final Path walDir;
	private final EmbeddedConnection connection;
	private final long metaColumnId;

	private NodeStore(Path dbDir, Path walDir, EmbeddedConnection connection, long metaColumnId) {
		this.dbDir = dbDir;
		this.walDir = walDir;
		this.connection = connection;
		this.metaColumnId = metaColumnId;
	}

	/**
	 * Opens (creating if needed) the database in {@code dbDir} with the given Rockserver config, and creates the
	 * meta column if missing. A {@link #restore} interrupted by a crash after its new contents were complete is
	 * finished first, so the database never opens empty in its place.
	 */
	static NodeStore open(Path dbDir, @Nullable Path rockserverConfig, String name) throws IOException {
		dbDir = dbDir.toAbsolutePath().normalize();
		RestoreDirs.of(dbDir).recover();
		Path config = replicaConfig(rockserverConfig);
		EmbeddedConnection connection;
		try {
			connection = new EmbeddedConnection(dbDir, name, config);
		} finally {
			// The database parses its config once, when it opens.
			Files.deleteIfExists(config);
		}
		try {
			checkSingleFileSystem(dbDir);
			Path walDir = walDir(dbDir, connection.getEmbeddedDB());
			// createColumn returns the existing id when the column already exists with the same schema.
			long metaColumnId = connection.getEmbeddedDB().createColumn(META_COLUMN, META_SCHEMA);
			return new NodeStore(dbDir, walDir, connection, metaColumnId);
		} catch (IOException | RuntimeException | Error failure) {
			try {
				connection.close();
			} catch (IOException | RuntimeException closeFailure) {
				failure.addSuppressed(closeFailure);
			}
			throw failure;
		}
	}

	/**
	 * Writes the config the replica opens its database with to a new temporary file: {@link #REPLICA_OVERRIDES} over
	 * the given Rockserver config (HOCON), or over the defaults when there is none.
	 */
	private static Path replicaConfig(@Nullable Path rockserverConfig) throws IOException {
		var config = ConfigFactory.parseString(REPLICA_OVERRIDES);
		try {
			if (rockserverConfig != null) {
				config = config.withFallback(ConfigFactory.parseFile(rockserverConfig.toFile(),
						ConfigParseOptions.defaults().setAllowMissing(false)));
			}
			config = config.resolve();
		} catch (ConfigException e) {
			throw new IOException("Failed to read the Rockserver config " + rockserverConfig, e);
		}
		Path file = Files.createTempFile("rockserver-replica-", ".conf");
		try {
			Files.writeString(file, config.root().render(ConfigRenderOptions.concise()), StandardCharsets.UTF_8);
		} catch (IOException | RuntimeException e) {
			Files.deleteIfExists(file);
			throw e;
		}
		return file;
	}

	private static Path walDir(Path dbDir, EmbeddedDB db) throws IOException {
		Path walPath;
		try {
			walPath = db.getConfig().global().walPath();
		} catch (GestaltException e) {
			throw new IOException("Failed to read wal-path from the Rockserver config", e);
		}
		Path walDir = walPath == null ? dbDir : dbDir.resolve(walPath).normalize();
		if (!walDir.startsWith(dbDir)) {
			throw new IOException("wal-path must be inside the database directory " + dbDir + ": " + walDir);
		}
		return walDir;
	}

	/** Fails unless {@code dbDir} and every directory under it are on the file system of its parent. */
	private static void checkSingleFileSystem(Path dbDir) throws IOException {
		if (!Files.exists(dbDir)) {
			return;
		}
		FileStore parentStore = Files.getFileStore(dbDir.getParent());
		List<Path> directories;
		try (Stream<Path> walk = Files.walk(dbDir)) {
			// Files.isDirectory and getFileStore follow symbolic links, so a linked volume is checked too.
			directories = walk.filter(Files::isDirectory).toList();
		}
		for (Path directory : directories) {
			if (!Files.getFileStore(directory).equals(parentStore)) {
				throw new IOException(directory + " is on a different file system than " + dbDir.getParent()
						+ ": the database directory and its volumes must be on the file system of its parent, so that"
						+ " a snapshot can be restored by renaming");
			}
		}
	}

	EmbeddedConnection connection() {
		return connection;
	}

	EmbeddedDB db() {
		return connection.getEmbeddedDB();
	}

	long metaColumnId() {
		return metaColumnId;
	}

	/** True if the database has any column besides the meta column. */
	boolean hasUserColumns() {
		return db().getAllColumnDefinitions().keySet().stream().anyMatch(name -> !name.equals(META_COLUMN));
	}

	/** The persisted applied position, or {@code TermIndex.INITIAL_VALUE} (term 0, index -1) if none. */
	TermIndex applied() {
		Buf value = readMeta(APPLIED);
		return value == null ? TermIndex.INITIAL_VALUE : decodeTermIndex(value);
	}

	/** The persisted cluster id, or null before genesis. */
	@Nullable String clusterId() {
		Buf value = readMeta(CLUSTER_ID);
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	private @Nullable Buf readMeta(byte metaKey) {
		return db().get(0, metaColumnId, key(metaKey), RequestType.<Buf>current());
	}

	/**
	 * Writes a consistent checkpoint of the live database to {@code target} (must not exist): memtables are
	 * flushed and SST files hard-linked. The caller guarantees that no entry is being applied concurrently. Returns
	 * the applied position contained in the checkpoint.
	 *
	 * <p>RocksDB's own {@code Checkpoint} refuses databases whose SST files live in column volumes
	 * ({@code cf_paths}, which the default Rockserver config uses), so the checkpoint is built from the public
	 * live-file APIs while file deletions are disabled: live SST and blob files are hard-linked at their path
	 * relative to the database directory (copied if {@code target} is on another file system), the MANIFEST is
	 * copied up to its live size, CURRENT is written for it, and the OPTIONS file and the WAL files that RocksDB
	 * still needs after the flush are copied. It is built in {@code <target>.tmp}, fsynced and renamed into
	 * place.</p>
	 *
	 * <p>The applied position is read before the live files are listed: without concurrent applies it equals the
	 * checkpoint's own marker, and if that guarantee were broken it could only be lower (Raft then re-sends
	 * entries that are skipped), never higher.</p>
	 */
	TermIndex checkpoint(Path target) throws IOException {
		target = target.toAbsolutePath();
		if (Files.exists(target)) {
			throw new IOException("Checkpoint target already exists: " + target);
		}
		Path building = target.resolveSibling(target.getFileName() + ".tmp");
		deleteRecursively(building);
		Files.createDirectories(building);
		TermIndex applied = applied();
		RocksDB rocksDB = db().getDb().get();
		try {
			rocksDB.disableFileDeletions();
			try {
				copyLiveFiles(rocksDB, building);
			} finally {
				rocksDB.enableFileDeletions();
			}
		} catch (org.rocksdb.RocksDBException e) {
			throw new IOException("Failed to checkpoint " + dbDir + " into " + target, e);
		}
		syncTree(building);
		Files.move(building, target);
		syncDirectory(target.getParent());
		return applied;
	}

	private void copyLiveFiles(RocksDB rocksDB, Path target) throws org.rocksdb.RocksDBException, IOException {
		// Flushes every column family, then lists the files of the current version and the MANIFEST size.
		RocksDB.LiveFiles live = rocksDB.getLiveFiles(true);
		rocksDB.flushWal(true);
		// The flush made the older WALs obsolete, but they stay in the WAL directory while deletions are disabled.
		long minWalNumber = rocksDB.getLongProperty("rocksdb.min-log-number-to-keep");
		List<LogFile> wals = rocksDB.getSortedWalFiles();

		// The live file list has no directories. Table files are found through their metadata; a file that a
		// compaction replaced after the listing is still on disk (deletions are disabled) in one of those directories.
		var tablePaths = new HashMap<String, Path>();
		var dataDirs = new LinkedHashSet<Path>();
		dataDirs.add(dbDir);
		for (LiveFileMetaData table : rocksDB.getLiveFilesMetaData()) {
			Path path = tablePath(table);
			tablePaths.put(path.getFileName().toString(), path);
			dataDirs.add(path.getParent());
		}

		String manifest = null;
		boolean linked = true;
		for (String liveFile : live.files) {
			String name = relativeName(liveFile);
			if (name.equals("CURRENT")) {
				continue;
			}
			if (name.startsWith("MANIFEST-")) {
				manifest = name;
				copyPrefix(dbDir.resolve(name), target.resolve(name), live.manifestFileSize);
			} else if (name.endsWith(".sst") || name.endsWith(".blob")) {
				Path source = locateDataFile(name, tablePaths, dataDirs);
				Path link = target.resolve(dbDir.relativize(source));
				Files.createDirectories(link.getParent());
				linked &= linkOrCopy(source, link);
			} else {
				Files.copy(dbDir.resolve(name), target.resolve(name));
			}
		}
		if (!linked) {
			LOG.warn("Checkpoint {} copies the table files of {}: hard links between them are not possible",
					target, dbDir);
		}
		if (manifest == null) {
			throw new IOException("RocksDB listed no MANIFEST among the live files of " + dbDir);
		}
		Files.writeString(target.resolve("CURRENT"), manifest + "\n", StandardCharsets.UTF_8);
		Path walTarget = target.resolve(dbDir.relativize(walDir));
		Files.createDirectories(walTarget);
		for (LogFile wal : wals) {
			if (wal.type() == WalFileType.kAliveLogFile && wal.logNumber() >= minWalNumber) {
				String name = relativeName(wal.pathName());
				copyPrefix(walDir.resolve(name), walTarget.resolve(name), wal.sizeFileBytes());
			}
		}
	}

	private Path locateDataFile(String name, Map<String, Path> tablePaths, Set<Path> dataDirs) throws IOException {
		Path path = tablePaths.get(name);
		if (path == null) {
			path = dataDirs.stream()
					.map(dir -> dir.resolve(name))
					.filter(Files::isRegularFile)
					.findFirst()
					.orElseThrow(() -> new IOException("Live file " + name + " not found in " + dataDirs));
		}
		if (!path.startsWith(dbDir)) {
			throw new IOException("Live file " + path + " is outside the database directory " + dbDir);
		}
		return path;
	}

	/** {@code path()} is the file's directory, though some RocksJava versions report the full file path. */
	private static Path tablePath(LiveFileMetaData table) {
		Path path = Path.of(table.path());
		if (!table.path().endsWith(".sst")) {
			path = path.resolve(relativeName(table.fileName()));
		}
		return path.toAbsolutePath().normalize();
	}

	/** RocksDB names live files relative to their directory, with a leading slash. */
	private static String relativeName(String rocksDbName) {
		return rocksDbName.startsWith("/") ? rocksDbName.substring(1) : rocksDbName;
	}

	/**
	 * Hard-links an immutable file, or copies it when that is not possible (such as across file systems); returns
	 * false if it copied. Copies are fsynced with the rest of the directory they are in.
	 */
	private static boolean linkOrCopy(Path source, Path target) throws IOException {
		try {
			Files.createLink(target, source);
			return true;
		} catch (FileAlreadyExistsException e) {
			throw e;
		} catch (FileSystemException | UnsupportedOperationException e) {
			Files.copy(source, target);
			return false;
		}
	}

	private static void copyPrefix(Path source, Path target, long size) throws IOException {
		try (var in = FileChannel.open(source, StandardOpenOption.READ);
				var out = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
			long position = 0;
			while (position < size) {
				long copied = in.transferTo(position, size - position, out);
				if (copied <= 0) {
					throw new IOException(source + " is shorter than " + size + " bytes");
				}
				position += copied;
			}
		}
	}

	@Override
	public void close() throws IOException {
		try {
			// With a delayed WAL flush (the default config), the last applied entries may still be in RocksDB's
			// in-process WAL buffer: write them out, so a cleanly stopped replica has its whole applied state on disk.
			db().getDb().get().flushWal(true);
		} catch (org.rocksdb.RocksDBException | RuntimeException e) {
			LOG.warn("Failed to flush the WAL of {} before closing; Raft replays the missing entries", dbDir, e);
		} finally {
			connection.close();
		}
	}

	/**
	 * Replaces the contents of {@code dbDir} (which must not be open) with the checkpoint in {@code checkpointDir}.
	 * Immutable files ({@code *.sst}, {@code *.blob}) are hard-linked when both are on the same file system, the
	 * rest copied, so the checkpoint itself stays intact. Crash-safe: the new contents are prepared next to
	 * {@code dbDir} and swapped in by rename.
	 *
	 * <p>Subdirectories (column volumes) are kept. Ratis' digest side files ({@code *.md5}, written next to every
	 * file of an installed snapshot) are skipped. Steps: build {@code <db>.restore-tmp}, fsync it, rename it to
	 * {@code <db>.restore-ready}; then move {@code dbDir} to {@code <db>.restore-old}, rename the ready directory to
	 * {@code dbDir} and delete the old one. {@link #open} and the next restore roll an interrupted swap forward once
	 * the ready directory exists.</p>
	 */
	static void restore(Path checkpointDir, Path dbDir) throws IOException {
		var dirs = RestoreDirs.of(dbDir);
		checkSingleFileSystem(dirs.dbDir);
		dirs.recover();
		List<Path> sources;
		try (Stream<Path> walk = Files.walk(checkpointDir)) {
			sources = walk.sorted().toList();
		}
		boolean linked = true;
		for (Path source : sources) {
			Path copy = dirs.building.resolve(checkpointDir.relativize(source).toString());
			String fileName = source.getFileName().toString();
			if (Files.isDirectory(source)) {
				Files.createDirectories(copy);
			} else if (!Files.isRegularFile(source)) {
				throw new IOException("Unexpected non-regular file in checkpoint: " + source);
			} else if (fileName.endsWith(".sst") || fileName.endsWith(".blob")) {
				linked &= linkOrCopy(source, copy);
			} else if (!fileName.endsWith(".md5")) {
				Files.copy(source, copy);
			}
		}
		if (!linked) {
			LOG.warn("Restoring {} copies the table files of {}: hard links between them are not possible", dbDir,
					checkpointDir);
		}
		syncTree(dirs.building);
		Files.move(dirs.building, dirs.ready);
		syncDirectory(dirs.parent);
		dirs.recover();
	}

	/** Sibling directories used by {@link #restore}. */
	private record RestoreDirs(Path dbDir, Path parent, Path building, Path ready, Path old) {

		static RestoreDirs of(Path dbDir) {
			Path absolute = dbDir.toAbsolutePath();
			String name = absolute.getFileName().toString();
			return new RestoreDirs(absolute,
					absolute.getParent(),
					absolute.resolveSibling(name + ".restore-tmp"),
					absolute.resolveSibling(name + ".restore-ready"),
					absolute.resolveSibling(name + ".restore-old"));
		}

		/** Discards an incomplete restore and finishes a complete one. */
		void recover() throws IOException {
			deleteRecursively(building);
			if (Files.exists(ready)) {
				if (Files.exists(dbDir)) {
					deleteRecursively(old);
					Files.move(dbDir, old);
				}
				Files.move(ready, dbDir);
				syncDirectory(parent);
			}
			deleteRecursively(old);
		}
	}

	static Keys key(byte metaKey) {
		return metaKey(metaKey, 0, 0);
	}

	/** Key of the recorded result of request {@code sequence} of proposer {@code proposerId}. */
	static Keys requestKey(long proposerId, long sequence) {
		return metaKey(REQUEST, proposerId, sequence);
	}

	private static Keys metaKey(byte kind, long first, long second) {
		var key = java.nio.ByteBuffer.allocate(META_KEY_BYTES).put(kind).putLong(first).putLong(second).array();
		return new Keys(Buf.wrap(key));
	}

	/** 16 bytes: term then index, big-endian. */
	static Buf encodeTermIndex(TermIndex termIndex) {
		var bytes = ByteBuffer.allocate(TERM_INDEX_BYTES)
				.putLong(termIndex.getTerm())
				.putLong(termIndex.getIndex())
				.array();
		return Buf.wrap(bytes);
	}

	static TermIndex decodeTermIndex(Buf value) {
		if (value.size() != TERM_INDEX_BYTES) {
			throw new IllegalArgumentException("Expected " + TERM_INDEX_BYTES + " bytes, got " + value.size());
		}
		var buffer = ByteBuffer.wrap(value.toByteArray());
		return TermIndex.valueOf(buffer.getLong(), buffer.getLong());
	}

	/** Fsyncs every file and directory under {@code root}, including {@code root} itself. */
	static void syncTree(Path root) throws IOException {
		List<Path> paths;
		try (Stream<Path> walk = Files.walk(root)) {
			// Deepest first, so each directory is synced after its entries.
			paths = walk.sorted(Comparator.reverseOrder()).toList();
		}
		for (Path path : paths) {
			syncFile(path);
		}
	}

	/** Fsyncs a file, or a directory's entries (Linux allows opening a directory for reading). */
	static void syncFile(Path file) throws IOException {
		try (var channel = FileChannel.open(file, StandardOpenOption.READ)) {
			channel.force(true);
		}
	}

	static void syncDirectory(Path directory) throws IOException {
		syncFile(directory);
	}

	/** Deletes {@code path} and everything under it; does nothing if it does not exist. */
	static void deleteRecursively(Path path) throws IOException {
		if (!Files.exists(path)) {
			return;
		}
		List<Path> paths;
		try (Stream<Path> walk = Files.walk(path)) {
			paths = walk.sorted(Comparator.reverseOrder()).toList();
		}
		for (Path entry : paths) {
			Files.delete(entry);
		}
	}
}
