package it.cavallium.rockserver.distributed;

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

/**
 * The node's local Rockserver database plus the replication metadata stored inside it.
 *
 * <p>Metadata lives in the internal column {@value #META_COLUMN} (one fixed 1-byte key, with values), so that
 * it is written in the same RocksDB WAL as the data and travels inside checkpoints:</p>
 * <ul>
 *   <li>{@link #APPLIED}: the last applied {@link TermIndex}, encoded by {@link #encodeTermIndex}</li>
 *   <li>{@link #CLUSTER_ID}: UTF-8 cluster id, written when the {@link LogEntry.Genesis} entry is applied</li>
 * </ul>
 *
 * <p>Every database file must live under the database directory: column volumes and a WAL directory inside it
 * (such as the default {@code ./volume} and {@code ./wal}) are supported, directories elsewhere are not.</p>
 */
final class NodeStore implements Closeable {

	static final String META_COLUMN = "__rockserver_replication";
	static final byte APPLIED = 1;
	static final byte CLUSTER_ID = 2;

	private static final ColumnSchema META_SCHEMA = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
	private static final int TERM_INDEX_BYTES = 2 * Long.BYTES;

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
		var connection = new EmbeddedConnection(dbDir, name, rockserverConfig);
		try {
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
	 * relative to the database directory, the MANIFEST is copied up to its live size, CURRENT is written for it,
	 * and the OPTIONS file and live WAL files are copied. It is built in {@code <target>.tmp}, fsynced and renamed
	 * into place.</p>
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
				Files.createLink(link, source);
			} else {
				Files.copy(dbDir.resolve(name), target.resolve(name));
			}
		}
		if (manifest == null) {
			throw new IOException("RocksDB listed no MANIFEST among the live files of " + dbDir);
		}
		Files.writeString(target.resolve("CURRENT"), manifest + "\n", StandardCharsets.UTF_8);
		Path walTarget = target.resolve(dbDir.relativize(walDir));
		Files.createDirectories(walTarget);
		for (LogFile wal : wals) {
			if (wal.type() == WalFileType.kAliveLogFile) {
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
		connection.close();
	}

	/**
	 * Replaces the contents of {@code dbDir} (which must not be open) with the checkpoint in {@code checkpointDir}.
	 * Immutable files ({@code *.sst}, {@code *.blob}) are hard-linked, the rest copied, so the checkpoint itself
	 * stays intact. Crash-safe: the new contents are prepared next to {@code dbDir} and swapped in by rename.
	 *
	 * <p>Subdirectories (column volumes) are kept. Ratis' digest side files ({@code *.md5}, written next to every
	 * file of an installed snapshot) are skipped. Steps: build {@code <db>.restore-tmp}, fsync it, rename it to
	 * {@code <db>.restore-ready}; then move {@code dbDir} to {@code <db>.restore-old}, rename the ready directory to
	 * {@code dbDir} and delete the old one. {@link #open} and the next restore roll an interrupted swap forward once
	 * the ready directory exists.</p>
	 */
	static void restore(Path checkpointDir, Path dbDir) throws IOException {
		var dirs = RestoreDirs.of(dbDir);
		dirs.recover();
		List<Path> sources;
		try (Stream<Path> walk = Files.walk(checkpointDir)) {
			sources = walk.sorted().toList();
		}
		for (Path source : sources) {
			Path copy = dirs.building.resolve(checkpointDir.relativize(source).toString());
			String fileName = source.getFileName().toString();
			if (Files.isDirectory(source)) {
				Files.createDirectories(copy);
			} else if (!Files.isRegularFile(source)) {
				throw new IOException("Unexpected non-regular file in checkpoint: " + source);
			} else if (fileName.endsWith(".sst") || fileName.endsWith(".blob")) {
				Files.createLink(copy, source);
			} else if (!fileName.endsWith(".md5")) {
				Files.copy(source, copy);
			}
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
		return new Keys(Buf.wrap(new byte[] {metaKey}));
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
