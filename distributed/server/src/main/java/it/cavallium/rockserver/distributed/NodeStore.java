package it.cavallium.rockserver.distributed;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.config.DataSize;
import it.cavallium.rockserver.core.resources.DefaultConfig;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileStore;
import java.nio.file.FileSystemException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.LogFile;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
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
 *   <li>{@link #SEEDED}: present if that entry said that the cluster was seeded from an existing database</li>
 *   <li>{@link #REQUEST} + proposer id + sequence: the encoded result of an applied {@link Request}, kept until its
 *   proposer reports it complete</li>
 *   <li>{@link #FLOOR} + proposer id: the highest {@link Request#completedBelow()} applied for that proposer</li>
 *   <li>{@link #DELETION}: log index and outcome of a column deletion whose record is not written yet</li>
 *   <li>{@link #SESSION} + session id: a client session, see {@link #encodeSession}</li>
 *   <li>{@link #SESSION_REQUEST} + session id + sequence: the encoded result of an applied write of that session</li>
 *   <li>{@link #CLOCK}: the latest leader time stamped on an applied entry, see {@link LogEntryCodec#stamp}</li>
 *   <li>{@link #CLUSTER_VERSION}: the 8-byte {@link Capabilities} of the cluster, see {@link #clusterVersion}</li>
 *   <li>{@link #CONFIGURATION} + index + term: the halves of a committed configuration entry applied here, see
 *   {@link CommittedConfigurations}</li>
 *   <li>{@link #CONFIGURATION_HISTORY}: the first index from which those are complete and the applied index written
 *   with it (two longs), see {@link #configurationHistoryFrom}</li>
 * </ul>
 *
 * <p>The meta column is created locally when a database is first opened, before any log entry is applied, so it
 * gets the same column id on every replica. No other column may ever be created locally.</p>
 *
 * <p>Every database file must live under the database directory: column volumes and a WAL directory inside it
 * (such as the default {@code ./volume} and {@code ./wal}) are supported, directories elsewhere are not. The
 * database directory and everything under it must be on the same file system as its parent directory, because
 * {@link #restore} swaps it by renaming: mount the node's data directory, not the database directory or a volume
 * inside it. {@link #open} refuses a WAL directory outside the database directory and anything under it on another
 * file system; a live file outside it makes {@link #checkpoint} fail.</p>
 *
 * <p>The database is opened with the node's Rockserver config plus {@link #REPLICA_OVERRIDES}, settings that every
 * replica needs whatever that config says, and the memtable sizing of {@link #withReplicaWriteBuffers}.</p>
 */
final class NodeStore implements Closeable {

	private static final Logger LOG = LoggerFactory.getLogger(NodeStore.class);

	static final String META_COLUMN = "__rockserver_replication";
	static final byte APPLIED = 1;
	static final byte CLUSTER_ID = 2;
	static final byte REQUEST = 3;
	static final byte FLOOR = 4;
	/** A column deletion in progress: its log index and whether the column existed before it ran. */
	static final byte DELETION = 5;
	static final byte SEEDED = 6;
	static final byte SESSION = 7;
	static final byte SESSION_REQUEST = 8;
	static final byte CLOCK = 9;
	/** Far from the kinds above, so that other new kinds can take the next ones. */
	static final byte CLUSTER_VERSION = 40;
	/** A committed configuration entry applied here: its index and term, see {@link CommittedConfigurations}. */
	static final byte CONFIGURATION = 41;
	/** The first index from which every applied configuration is recorded, see {@link #configurationHistoryFrom}. */
	static final byte CONFIGURATION_HISTORY = 42;

	private static final int META_KEY_BYTES = 1 + 2 * Long.BYTES;
	private static final ColumnSchema META_SCHEMA = ColumnSchema.of(IntList.of(META_KEY_BYTES), ObjectList.of(), true);
	private static final int TERM_INDEX_BYTES = 2 * Long.BYTES;

	/**
	 * Settings every replica needs:
	 * <ul>
	 *   <li>{@code absolute-consistency = false}: the RocksDB WAL is not synced, so a crash can tear its tail: a power
	 *   loss keeps an arbitrary prefix of what was written, and with a delayed WAL flush
	 *   ({@code delay-wal-flush-duration}) even a killed process loses its buffer. A replica then needs the consistent
	 *   prefix that RocksDB's point-in-time recovery opens, and replays the rest from the Raft log. Rockserver's
	 *   {@code absolute-consistency} (on by default) would refuse to open such a database, so the replica could not
	 *   restart without being repaired.</li>
	 *   <li>{@code atomic-flush = true}: entries are applied without the WAL (see {@link Applier}), so after a crash
	 *   only flushed data survives; flushing every column together keeps that a consistent point, where the applied
	 *   position matches the data.</li>
	 *   <li>{@code optimistic = true}: on the leader, native transactions and updates only stage writes and serve
	 *   reads; they are never committed, and the apply transaction writes the same keys when their commit is applied.
	 *   A pessimistic database would make them hold row locks that the apply waits on, until it times out and the
	 *   leader stops.</li>
	 * </ul>
	 */
	/**
	 * Tags every meter of the database with the node id, unless the config sets a name that is not blank: see
	 * {@code MetricsConfig}. The replication meters take the same tag, see {@link #metricsNodeName}.
	 */
	private static final String METRICS_NODE_NAME = "database.metrics.node-name";

	private static final String REPLICA_OVERRIDES = """
			database.global.absolute-consistency = false
			database.global.atomic-flush = true
			database.global.optimistic = true
			""";

	private final Path dbDir;
	/** RocksDB WAL directory: {@code dbDir} itself or the configured {@code wal-path} (default {@code ./wal}). */
	private final Path walDir;
	private final EmbeddedConnection connection;
	private final long metaColumnId;
	private final AtomicBoolean closed = new AtomicBoolean();
	/**
	 * {@link #CLUSTER_ID}, kept in memory: read when the database opens and set by {@link #writeClusterId}, the only
	 * writer while it is open. Snapshot installation and backup restore replace the database files while it is
	 * closed, and the store opened afterwards reads the id again.
	 */
	private volatile @Nullable String clusterId;

	private NodeStore(Path dbDir, Path walDir, EmbeddedConnection connection, long metaColumnId) {
		this.dbDir = dbDir;
		this.walDir = walDir;
		this.connection = connection;
		this.metaColumnId = metaColumnId;
		Buf value = readMeta(CLUSTER_ID);
		this.clusterId = value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	/**
	 * Opens (creating if needed) the database in {@code dbDir} with the given Rockserver config, and creates the
	 * meta column if missing. A {@link #restore} interrupted by a crash after its new contents were complete is
	 * finished first, so the database never opens empty in its place.
	 */
	static NodeStore open(Path dbDir, @Nullable Path rockserverConfig, String name) throws IOException {
		dbDir = dbDir.toAbsolutePath().normalize();
		RestoreDirs.of(dbDir).recover();
		deleteLeftConfigs(dbDir.getParent());
		Path config = replicaConfig(rockserverConfig, dbDir, name);
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
	 * Returns {@code opened}, or, when the latest snapshot of {@code storage} is ahead of it, the database reopened after
	 * the snapshot was restored into it ({@code opened} is closed): the replica's start ({@code openStore}) and
	 * {@code export-seed} both go through here (R31-2). A received snapshot is ahead after a crash between Ratis renaming
	 * it into place and its restore; {@link CheckpointStorage#init} verified it.
	 */
	static NodeStore restoreSnapshotIfAhead(NodeStore opened, CheckpointStorage storage, @Nullable Path rockserverConfig,
			String name) throws IOException {
		var snapshot = storage.getLatestSnapshot();
		if (snapshot == null || snapshot.getIndex() <= opened.applied().getIndex()) {
			return opened;
		}
		LOG.warn("{}: database at {} is behind the snapshot at {}, restoring the snapshot", name, opened.applied(),
				snapshot.getTermIndex());
		Path dbDir = opened.dbDir;
		opened.close();
		restore(storage.snapshotDbDir(), dbDir);
		return open(dbDir, rockserverConfig, name);
	}

	/**
	 * Writes the config the replica opens its database with to {@link #replicaConfigFile}: {@link #REPLICA_OVERRIDES} over
	 * the given Rockserver config (HOCON), or over the defaults when there is none. The file is owner-only
	 * ({@link #writeOwnerOnly}, R31-1).
	 */
	private static Path replicaConfig(@Nullable Path rockserverConfig, Path dbDir, String nodeId) throws IOException {
		return replicaConfig(rockserverConfig, dbDir, null, nodeId);
	}

	/**
	 * Like {@link #replicaConfig(Path, Path, String)}, for a copy in {@code dbDir} of the database in {@code sourceDbDir}
	 * (when not null): an absolute {@code volume-path} under {@code sourceDbDir} becomes the same path relative to the
	 * database directory, so that the copy opens its own volumes. The file is made next to {@code dbDir}, not in the
	 * JDK's temporary directory (see {@code ReplicatedServerMain}), owner-only since it holds the resolved config with
	 * its secrets ({@link #writeOwnerOnly}, R31-1). {@code nodeId} is the metrics node name when the config sets none.
	 * Package-private for tests.
	 */
	static Path replicaConfig(@Nullable Path rockserverConfig, Path dbDir, @Nullable Path sourceDbDir,
			String nodeId) throws IOException {
		var config = ConfigFactory.parseString(REPLICA_OVERRIDES);
		try {
			if (rockserverConfig != null) {
				config = config.withFallback(ConfigFactory.parseFile(rockserverConfig.toFile(),
						ConfigParseOptions.defaults().setAllowMissing(false)));
			}
			if (!config.hasPath(METRICS_NODE_NAME) || config.getString(METRICS_NODE_NAME).isBlank()) {
				// The nodes of a cluster share one config: their meters must still tell them apart. The default config
				// has node-name "", which means unset.
				config = config.withValue(METRICS_NODE_NAME, ConfigValueFactory.fromAnyRef(nodeId));
			}
			config = withReplicaWriteBuffers(config.resolve());
			if (sourceDbDir != null) {
				config = withRelativeVolumes(config, sourceDbDir);
			}
		} catch (ConfigException | IllegalArgumentException e) {
			throw new IOException("Failed to read the Rockserver config " + rockserverConfig, e);
		}
		checkVolumesInside(config, dbDir);
		Path file = replicaConfigFile(dbDir);
		Files.createDirectories(file.getParent());
		try {
			// Replaces the file that a process killed during an earlier open of this database left (R29-2).
			writeOwnerOnly(file, config.root().render(ConfigRenderOptions.concise()));
		} catch (IOException | RuntimeException e) {
			Files.deleteIfExists(file);
			throw e;
		}
		return file;
	}

	private static final Set<PosixFilePermission> OWNER_READ_WRITE = PosixFilePermissions.fromString("rw-------");

	/**
	 * Writes {@code content} to {@code file} readable and writable by its owner only (rw-------) on a file system with
	 * POSIX permissions, whatever the umask: the replica config is the resolved Rockserver config, with the InfluxDB
	 * token and every {@code ${?ENV}} substitution in plain text (R31-1; R29-2's {@code Files.createTempFile} made it so
	 * by chance, the fixed name had the umask's mode). An existing file (a killed open's) is deleted first, so that
	 * neither its mode nor a link in its place is kept, and the file is created with the mode before any byte is
	 * written. Elsewhere the file system's default applies.
	 */
	static void writeOwnerOnly(Path file, String content) throws IOException {
		Files.deleteIfExists(file);
		if (file.getFileSystem().supportedFileAttributeViews().contains("posix")) {
			Files.createFile(file, PosixFilePermissions.asFileAttribute(OWNER_READ_WRITE));
			// The umask can only remove bits from the mode given at creation: set it again, in case it removed the owner's.
			Files.setPosixFilePermissions(file, OWNER_READ_WRITE);
		} else {
			Files.createFile(file);
		}
		Files.writeString(file, content, StandardCharsets.UTF_8, StandardOpenOption.WRITE,
				StandardOpenOption.TRUNCATE_EXISTING);
	}

	/**
	 * The config file that the database in {@code dbDir} is opened with, next to it: one name per database, so that a
	 * process killed while the database opens (WAL replay, a large MANIFEST: the file is deleted only once it opened)
	 * leaves at most one, which the next open of that database replaces and deletes (R29-2). A random name left one per
	 * killed start, for good.
	 *
	 * <p>The name is {@code rockserver-replica-for-<db dir name>.conf}: the {@code for-} keeps it out of
	 * {@link #LEGACY_REPLICA_CONFIG} whatever the directory is called, so that {@link #deleteLeftConfigs} never takes
	 * the config of a database being opened or exported for a leftover, even when the directory's name is all digits
	 * (R30-1: {@code rockserver-replica-<db dir name>.conf} did, and deleted an export target's config).
	 */
	static Path replicaConfigFile(Path dbDir) {
		Path db = dbDir.toAbsolutePath().normalize();
		return db.resolveSibling(REPLICA_CONFIG_PREFIX + "for-" + db.getFileName() + ".conf");
	}

	private static final String REPLICA_CONFIG_PREFIX = "rockserver-replica-";
	/**
	 * The config files that released versions made with a random name ({@code Files.createTempFile} with prefix
	 * {@code rockserver-replica-} and suffix {@code .conf}: digits only in between). No {@link #replicaConfigFile}
	 * matches it.
	 */
	private static final java.util.regex.Pattern LEGACY_REPLICA_CONFIG =
			java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(REPLICA_CONFIG_PREFIX) + "[0-9]+\\.conf");

	/**
	 * Deletes from {@code dir} the randomly named config files that released versions' opens left there when the
	 * process was killed during the open (R29-2). Only that name pattern, which no {@link #replicaConfigFile} matches
	 * (R30-1): the config of another database in the same directory (an {@code export-seed} target next to the source,
	 * whatever its name) stays.
	 */
	private static void deleteLeftConfigs(@Nullable Path dir) throws IOException {
		if (dir == null || !Files.isDirectory(dir)) {
			return;
		}
		try (Stream<Path> files = Files.list(dir)) {
			for (Path file : files.filter(path -> LEGACY_REPLICA_CONFIG.matcher(path.getFileName().toString()).matches())
					.toList()) {
				LOG.info("Deleting {}, the database config of an open that was killed", file);
				Files.deleteIfExists(file);
			}
		}
	}

	/**
	 * Lets the shared write buffer budget, not a per-column limit, decide when memtables are flushed.
	 *
	 * <p>Replicas use atomic flush (see {@link #REPLICA_OVERRIDES}): whatever triggers a flush, every column family
	 * with unflushed data is flushed with it. With the default per-column {@code write-buffer-size} (64 MiB), one hot
	 * column filling its memtable would flush every cold column too, each into a tiny SST file, as often as the hot
	 * column fills up. So every column's {@code write-buffer-size} is raised to the database's write buffer budget,
	 * {@code database.global.write-buffer-manager} (default 128 MiB; when it is 0 or null, half of {@code block-cache},
	 * as the database computes it): no single column reaches its own limit before the {@code WriteBufferManager},
	 * which counts every column's memtables, asks for a flush. Flushes then come once per budget of writes to all
	 * columns together, and the prefix guarantee of atomic flush is unchanged.</p>
	 *
	 * <p>Applied to {@code fallback-column-options} and to every entry of {@code column-options} that the config
	 * itself lists; a per-column {@code write-buffer-size} in the config is replaced. To flush less often, raise
	 * {@code write-buffer-manager} (it is charged to the block cache).</p>
	 */
	static Config withReplicaWriteBuffers(Config config) {
		long budget = writeBufferBudget(config);
		if (budget <= 0) {
			return config;
		}
		ConfigValue size = ConfigValueFactory.fromAnyRef(budget + "B");
		Config result = config.withValue("database.global.fallback-column-options.write-buffer-size", size);
		String columnsPath = "database.global.column-options";
		if (config.hasPath(columnsPath)) {
			var columns = new ArrayList<ConfigValue>();
			for (ConfigObject column : config.getObjectList(columnsPath)) {
				columns.add(column.withValue("write-buffer-size", size));
			}
			result = result.withValue(columnsPath, ConfigValueFactory.fromIterable(columns));
		}
		return result;
	}

	/**
	 * Rewrites every absolute {@code volume-path} (of {@code fallback-column-options} and of the entries of
	 * {@code column-options}) that lies under {@code dbDir} into the same path relative to the database directory.
	 */
	static Config withRelativeVolumes(Config config, Path dbDir) {
		Path root = dbDir.toAbsolutePath().normalize();
		String fallbackPath = "database.global.fallback-column-options.volumes";
		Config result = config;
		if (config.hasPath(fallbackPath)) {
			result = result.withValue(fallbackPath, relativeVolumes(config.getObjectList(fallbackPath), root));
		}
		String columnsPath = "database.global.column-options";
		if (config.hasPath(columnsPath)) {
			var columns = new ArrayList<ConfigValue>();
			for (ConfigObject column : config.getObjectList(columnsPath)) {
				if (column.toConfig().hasPath("volumes")) {
					column = column.withValue("volumes", relativeVolumes(column.toConfig().getObjectList("volumes"), root));
				}
				columns.add(column);
			}
			result = result.withValue(columnsPath, ConfigValueFactory.fromIterable(columns));
		}
		return result;
	}

	private static ConfigValue relativeVolumes(List<? extends ConfigObject> volumes, Path root) {
		var rewritten = new ArrayList<ConfigValue>();
		for (ConfigObject volume : volumes) {
			var config = volume.toConfig();
			if (config.hasPath("volume-path")) {
				Path path = Path.of(config.getString("volume-path"));
				if (path.isAbsolute() && path.normalize().startsWith(root)) {
					String relative = root.relativize(path.normalize()).toString();
					volume = volume.withValue("volume-path",
							ConfigValueFactory.fromAnyRef(relative.isEmpty() ? "." : "./" + relative));
				}
			}
			rewritten.add(volume);
		}
		return ConfigValueFactory.fromIterable(rewritten);
	}

	/**
	 * Fails if a column volume ({@code volumes[].volume-path} of {@code fallback-column-options} or of an entry of
	 * {@code column-options}, which RocksDB gets as {@code cf_paths}) resolves outside {@code dbDir}: a snapshot links
	 * only files under the database directory, so every snapshot would fail, and the database opens such a volume (and
	 * creates its directory) before anything would notice. Relative paths resolve against {@code dbDir}, as the database
	 * resolves them.
	 */
	static void checkVolumesInside(Config config, Path dbDir) throws IOException {
		Config effective;
		try {
			effective = config.withFallback(defaultConfig()).resolve();
		} catch (ConfigException e) {
			throw new IOException("Failed to read the Rockserver config", e);
		}
		var columns = new ArrayList<Config>();
		if (effective.hasPath("database.global.fallback-column-options")) {
			columns.add(effective.getConfig("database.global.fallback-column-options"));
		}
		if (effective.hasPath("database.global.column-options")) {
			for (ConfigObject column : effective.getObjectList("database.global.column-options")) {
				columns.add(column.toConfig());
			}
		}
		Path root = dbDir.toAbsolutePath().normalize();
		for (Config column : columns) {
			if (!column.hasPath("volumes")) {
				continue;
			}
			for (ConfigObject volume : column.getObjectList("volumes")) {
				var path = volume.toConfig();
				if (!path.hasPath("volume-path")) {
					continue;
				}
				Path resolved = root.resolve(path.getString("volume-path")).normalize();
				if (!resolved.startsWith(root)) {
					throw new IOException("Column volume " + path.getString("volume-path") + " resolves to " + resolved
							+ ", outside the database directory " + root + ": every snapshot would fail. Put each volume-path"
							+ " under the database directory, such as the default ./volume");
				}
			}
		}
	}

	private static Config defaultConfig() {
		try (var reader = new InputStreamReader(DefaultConfig.getDefaultConfig(), StandardCharsets.UTF_8)) {
			return ConfigFactory.parseReader(reader);
		} catch (IOException e) {
			throw new IllegalArgumentException("Failed to read the default Rockserver config", e);
		}
	}

	/**
	 * A digest of the Rockserver config file as parsed, with substitutions resolved: comments, layout and key order do
	 * not change it. Members compare it through {@link ClusterCheck}, since every node must run the same config. A node
	 * without a config file has the digest of an empty config.
	 */
	static String configDigest(@Nullable Path rockserverConfig) throws IOException {
		Config config = ConfigFactory.empty();
		try {
			if (rockserverConfig != null) {
				config = ConfigFactory.parseFile(rockserverConfig.toFile(), ConfigParseOptions.defaults().setAllowMissing(false))
						.resolve();
			}
		} catch (ConfigException e) {
			throw new IOException("Failed to read the Rockserver config " + rockserverConfig, e);
		}
		return sha256(config.root().render(ConfigRenderOptions.concise()).getBytes(StandardCharsets.UTF_8));
	}

	/** The size of the database's {@code WriteBufferManager} for {@code config}, computed as the database does. */
	static long writeBufferBudget(Config config) {
		Config effective = config.withFallback(defaultConfig()).resolve();
		long manager = dataSize(effective, "database.global.write-buffer-manager");
		if (manager > 0) {
			return manager;
		}
		// A null block-cache means the database's 512 MiB default.
		long blockCache = effective.hasPath("database.global.block-cache")
				? dataSize(effective, "database.global.block-cache") : 512L << 20;
		return blockCache / 2;
	}

	private static long dataSize(Config config, String path) {
		if (!config.hasPath(path)) {
			return 0;
		}
		return new DataSize(config.getString(path)).longValue();
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
		List<Path> directories = new ArrayList<>();
		// The database may be open: RocksDB deletes files (compaction inputs, obsolete WALs) from its own threads while
		// this walks, so an entry, file or directory, that is gone before it is read is skipped, here and in the loop
		// below. Symbolic links are not followed, as by Files.walk; Files.isDirectory and getFileStore follow them, so a
		// linked volume is checked too.
		Files.walkFileTree(dbDir, new SimpleFileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				directories.add(dir);
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				if (attrs.isSymbolicLink() && Files.isDirectory(file)) {
					directories.add(file);
				}
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
				if (exc instanceof NoSuchFileException) {
					return FileVisitResult.CONTINUE;
				}
				throw exc;
			}
		});
		for (Path directory : directories) {
			FileStore store;
			try {
				store = Files.getFileStore(directory);
			} catch (NoSuchFileException gone) {
				continue;
			}
			if (!store.equals(parentStore)) {
				throw new IOException(directory + " is on a different file system than " + dbDir.getParent()
						+ ": the database directory and its volumes must be on the file system of its parent, so that"
						+ " a snapshot can be restored by renaming");
			}
		}
	}

	/**
	 * The absolute directories this node's files live in (the database, its WAL and the node directory holding the
	 * database), which differ between nodes: removed from the failure texts replicas record (see
	 * {@link it.cavallium.rockserver.core.common.Failures#sanitizeForReplica}).
	 */
	List<Path> directories() {
		var parent = dbDir.getParent();
		return parent == null ? List.of(dbDir, walDir) : List.of(dbDir, walDir, parent);
	}

	EmbeddedConnection connection() {
		return connection;
	}

	/**
	 * The {@code node} tag of the database's meters ({@code database.metrics.node-name}, the node id unless configured),
	 * or {@code fallback} if it has none: the replication meters must carry the same one.
	 */
	String metricsNodeName(String fallback) {
		try {
			var name = db().getConfig().metrics().nodeName();
			return name == null || name.isBlank() ? fallback : name;
		} catch (Exception unreadable) {
			return fallback;
		}
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

	/**
	 * The persisted cluster id, or null before genesis. From memory, without reading the database: the state machine
	 * asks under its monitor, which Ratis' append lock waits for, and for every proposal.
	 *
	 * @throws IllegalStateException "Closed" once the store is closing, as when the id was read from the database: a
	 *     node installing a snapshot has closed its old store, and callers take the refusal for "being replaced", never a
	 *     stale or null id ({@code ReplicatedConnection.getCapabilities} advertises no client sessions,
	 *     {@code ReplicatedNode.initializeCluster} retries). Only a flag is read, no lock and no disk.
	 */
	@Nullable String clusterId() {
		if (closed.get()) {
			throw new IllegalStateException("Closed");
		}
		return clusterId;
	}

	/** Persists the cluster id, for {@link LogEntry.Genesis}: visible to {@link #clusterId()} once written. */
	void writeClusterId(String id) {
		db().put(0L, metaColumnId, key(CLUSTER_ID), Buf.wrap(id.getBytes(StandardCharsets.UTF_8)), RequestType.none());
		clusterId = id;
	}

	/**
	 * The cluster version ({@link Capabilities}) applied here: written by {@link LogEntry.Genesis} and raised by
	 * {@link LogEntry.ClusterVersion}. 0 on a cluster created before the version existed, until a leader raises it.
	 */
	long clusterVersion() {
		Buf value = readMeta(CLUSTER_VERSION);
		return value == null || value.size() != Long.BYTES ? 0 : ByteBuffer.wrap(value.toByteArray()).getLong();
	}

	/**
	 * The first log index from which every configuration entry this database applied is recorded
	 * ({@link #CONFIGURATION}). {@link #CONFIGURATION_HISTORY} holds that index and the applied position it was written
	 * with, in the same write as {@link #APPLIED}: if the applied position is still that one, the history is complete
	 * from that index. Otherwise a version that does not keep the history applied entries after it (a rolling upgrade
	 * or downgrade, or a snapshot that such a version took), and configurations it applied may be missing: the history
	 * is complete only from the index after the applied position, as for a database without the key, since every later
	 * entry is applied by a version that records them (R11 review, finding 2).
	 */
	long configurationHistoryFrom() {
		long applied = applied().getIndex();
		Buf value = readMeta(CONFIGURATION_HISTORY);
		if (value != null && value.size() == 2 * Long.BYTES) {
			var bytes = ByteBuffer.wrap(value.toByteArray());
			long from = bytes.getLong();
			long through = bytes.getLong();
			if (through == applied) {
				return from;
			}
		}
		return applied + 1;
	}

	/** The value of {@link #CONFIGURATION_HISTORY}: complete from {@code from}, written with the position {@code through}. */
	static Buf encodeHistory(long from, long through) {
		return Buf.wrap(ByteBuffer.allocate(2 * Long.BYTES).putLong(from).putLong(through).array());
	}

	/** Whether {@link #CONFIGURATION_HISTORY} is written. */
	boolean configurationHistoryRecorded() {
		return readMeta(CONFIGURATION_HISTORY) != null;
	}

	/** The committed configurations recorded here ({@link #CONFIGURATION}), in log order. */
	List<RemovalRule.Conf> configurations() {
		var result = new java.util.ArrayList<RemovalRule.Conf>();
		try (var rows = db().getRange(0L, metaColumnId, key(CONFIGURATION), key((byte) (CONFIGURATION + 1)), false,
				RequestType.<KV>allInRange())) {
			rows.forEach(kv -> {
				var key = ByteBuffer.wrap(kv.keys().keys()[0].toByteArray());
				long index = key.getLong(1);
				long term = key.getLong(1 + Long.BYTES);
				result.add(CommittedConfigurations.decodeHalves(term, index, kv.value().toByteArray()));
			});
		}
		return result;
	}

	/** Key of the configuration entry at {@code index}, of term {@code term}. */
	static Keys configurationKey(long index, long term) {
		return metaKey(CONFIGURATION, index, term);
	}

	/** True if the {@link LogEntry.Genesis} entry applied here said that the cluster started from a seed database. */
	boolean seeded() {
		return readMeta(SEEDED) != null;
	}

	/**
	 * The digest of the seed this database started from, as {@link #recordSeedDigest} recorded it at the first start of
	 * the node; null if none was recorded (the node started empty, or with a version that did not record it).
	 */
	@Nullable String seedDigest() throws IOException {
		return readSeedDigest(dbDir);
	}

	static @Nullable String readSeedDigest(Path dbDir) throws IOException {
		var lines = readSeedDigestLines(dbDir);
		return lines.isEmpty() ? null : lines.getFirst();
	}

	/** The seed digest, then, once {@link #noteSeedOpened} ran, {@code opened <digest>}. Empty if there is no file. */
	private static List<String> readSeedDigestLines(Path dbDir) throws IOException {
		Path file = seedDigestFile(dbDir);
		if (!Files.exists(file)) {
			return List.of();
		}
		return Files.readString(file, StandardCharsets.UTF_8).strip().lines().map(String::strip).toList();
	}

	/** Next to the database directory, so that a restore, which swaps the database directory, keeps it. */
	private static Path seedDigestFile(Path dbDir) {
		Path absolute = dbDir.toAbsolutePath().normalize();
		return absolute.resolveSibling(absolute.getFileName() + ".seed-digest");
	}

	/**
	 * Records a digest of the database in {@code dbDir} as it is before it is ever opened as a replica, if it holds one
	 * and none was recorded yet. Called at the first start of a node, the only moment when every copy of a seed is still
	 * the same: opening a database writes a new MANIFEST and lets compactions run, at times that differ on each node.
	 *
	 * <p>The digest covers every file under the database directory (volumes and WAL directory included), by path: table
	 * and blob files ({@code *.sst}, {@code *.blob}, immutable and large) by size, every other file (MANIFEST, CURRENT,
	 * OPTIONS, WAL files) by content. It finds a copy that misses files or holds others, a copy of the database taken at
	 * another moment and a truncated copy; a table file damaged without a change of size goes unnoticed. {@code LOCK}
	 * and the info logs ({@code LOG*}) are skipped.</p>
	 *
	 * <p>A first start can fail after the digest is recorded: before the database opens (a partial copy that RocksDB
	 * refuses, a refused config), or after the start-up check opened and closed it ({@link #noteSeedOpened}) but before
	 * Raft made its group directory. A recorded digest is kept only while the files are those it was taken from, or
	 * those the start-up check left: otherwise the database was replaced since (the operator copied the seed again) and
	 * the digest is taken again. Once Raft has its group directory the node no longer calls this.</p>
	 *
	 * <p>The seed's files are fsynced first ({@link #syncSeed}, R34-1).</p>
	 *
	 * @return the recorded digest, null if {@code dbDir} holds no database
	 */
	static @Nullable String recordSeedDigest(Path dbDir) throws IOException {
		var lines = readSeedDigestLines(dbDir);
		String recorded = lines.isEmpty() ? null : lines.getFirst();
		if (!Files.isRegularFile(dbDir.resolve("CURRENT"))) {
			return recorded;
		}
		syncSeed(dbDir);
		String digest = databaseFilesDigest(dbDir);
		if (recorded != null) {
			String opened = lines.size() > 1 && lines.get(1).startsWith("opened ")
					? lines.get(1).substring("opened ".length()) : null;
			if (digest.equals(recorded) || digest.equals(opened)) {
				return recorded;
			}
			LOG.warn("The database in {} changed since its seed digest was recorded by an earlier first start that failed:"
					+ " recording the digest of the files it holds now", dbDir);
		}
		writeSeedDigestFile(dbDir, digest + "\n");
		return digest;
	}

	/**
	 * Fsyncs every file and directory under {@code dbDir} (its volumes and WAL directory included, hard-linked files
	 * too) and its parent, the data directory (R34-1). An operator copied the seed into place ({@code rsync},
	 * {@code cp}, {@code restore-backup} from a copy) and nothing fsynced it; once the seeded genesis commits, the Raft
	 * log assumes the seed is in every replica's database, and no entry holds it. The seed snapshot taken after genesis
	 * hard-links the same inodes, which {@link #syncTree(Path)} skips. Runs at every first start (no Raft group
	 * directory yet, see {@link #recordSeedDigest}), so once per node unless a first start fails: one fsync per file of
	 * the seed, which forces out what writeback has not flushed yet and is cheap for what it has. Volumes or a WAL
	 * directory configured outside {@code dbDir} are not reached: the runbooks' {@code sync} after the copy covers them.
	 */
	static void syncSeed(Path dbDir) throws IOException {
		Path absolute = dbDir.toAbsolutePath().normalize();
		syncTree(absolute, true);
		syncDirectory(absolute.getParent());
	}

	/**
	 * Notes, next to the recorded seed digest, the digest of the files as the start-up check left them after it opened and
	 * closed the database: a first start that fails after this point still keeps the seed digest (see
	 * {@link #recordSeedDigest}). Does nothing when no seed digest was recorded.
	 */
	static void noteSeedOpened(Path dbDir) throws IOException {
		String recorded = readSeedDigest(dbDir);
		if (recorded == null || !Files.isRegularFile(dbDir.resolve("CURRENT"))) {
			return;
		}
		writeSeedDigestFile(dbDir, recorded + "\nopened " + databaseFilesDigest(dbDir) + "\n");
	}

	/**
	 * Records, for the database just restored into {@code dbDir} by {@code restore-backup}, the digest of the backup it
	 * came from ({@code seedDigest}, taken from the backup's files before anything opened them), with the digest of the
	 * files the restore left as the {@code opened} line: the first start then keeps the backup's digest (see
	 * {@link #recordSeedDigest}). Every node restored from a copy of the same backup records the same digest, although
	 * the restore opened each database and left files of its own ({@code OPTIONS}, with the node's paths; R26-1).
	 */
	static void recordRestoredSeedDigest(Path dbDir, String seedDigest) throws IOException {
		writeSeedDigestFile(dbDir, seedDigest + "\nopened " + databaseFilesDigest(dbDir) + "\n");
	}

	/** Deletes the seed digest recorded next to {@code dbDir}, if any. */
	static void deleteSeedDigest(Path dbDir) throws IOException {
		if (Files.deleteIfExists(seedDigestFile(dbDir))) {
			syncDirectory(seedDigestFile(dbDir).getParent());
		}
	}

	private static void writeSeedDigestFile(Path dbDir, String content) throws IOException {
		Path file = seedDigestFile(dbDir);
		Path building = file.resolveSibling(file.getFileName() + ".tmp");
		Files.writeString(building, content, StandardCharsets.UTF_8);
		syncFile(building);
		Files.move(building, file, StandardCopyOption.REPLACE_EXISTING,
				StandardCopyOption.ATOMIC_MOVE);
		syncDirectory(file.getParent());
	}

	/** See {@link #recordSeedDigest}. */
	static String databaseFilesDigest(Path dbDir) throws IOException {
		Path root = dbDir.toAbsolutePath().normalize();
		List<Path> files;
		try (Stream<Path> walk = Files.walk(root)) {
			files = walk.filter(Files::isRegularFile)
					.map(root::relativize)
					.sorted(Comparator.comparing(Path::toString))
					.toList();
		}
		MessageDigest digest = sha256();
		for (Path relative : files) {
			String name = relative.getFileName().toString();
			if (isSkippedByDigest(name)) {
				continue;
			}
			Path file = root.resolve(relative);
			update(digest, relative.toString().replace('\\', '/'));
			if (name.endsWith(".sst") || name.endsWith(".blob")) {
				update(digest, "size " + Files.size(file));
			} else {
				update(digest, "content");
				try (var in = Files.newInputStream(file)) {
					var buffer = new byte[64 * 1024];
					int read;
					while ((read = in.read(buffer)) > 0) {
						digest.update(buffer, 0, read);
					}
				}
				update(digest, "end " + Files.size(file));
			}
		}
		return HexFormat.of().formatHex(digest.digest());
	}

	/**
	 * {@code LOCK} and the info logs: {@code LOG*} in the database directory, and in its {@code logs/} directory
	 * {@code <absolute path>_LOG} and its rotated {@code _LOG.old.<time>}, whose names hold the path of the database, so
	 * that copies made elsewhere would differ by them (R26-1).
	 */
	private static boolean isSkippedByDigest(String name) {
		return name.equals("LOCK") || name.startsWith("LOG") || name.endsWith("_LOG") || name.contains("_LOG.old.");
	}

	private static void update(MessageDigest digest, String value) {
		var bytes = value.getBytes(StandardCharsets.UTF_8);
		digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
		digest.update(bytes);
	}

	private static MessageDigest sha256() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	private static String sha256(byte[] value) {
		return HexFormat.of().formatHex(sha256().digest(value));
	}

	/**
	 * Writes to {@code target} (must not exist) a copy of the stopped replica's database in {@code dbDir} that can seed a
	 * new cluster, the last resort of "No quorum" in the design document: a checkpoint of what the replica holds (its
	 * flushed state plus what its RocksDB WAL replays), without the replication metadata ({@link #META_COLUMN}: the
	 * cluster id, the applied position, results and sessions), whose cluster id would make every node of a new cluster
	 * refuse it. The source is opened as a replica, which also finishes a restore that a crash interrupted, also one
	 * that left no {@code dbDir} but its complete {@code <db>.restore-ready} (a crash between the swap's two renames,
	 * R33-1); it must not run, which this form cannot check (the command uses {@link #exportSeed(Path, Path, Path, Path, boolean)}, which
	 * checks the node's Raft lock, R32-5). Table files are hard-linked when {@code target} is on the same file system. This form ignores the Raft
	 * snapshots: the command passes them, see {@link #exportSeed(Path, Path, Path, Path)} (R31-2).
	 *
	 * @return what the copy held: the applied position and cluster of the replica
	 * @throws ReplicatedNode.NewerVersionException if the replica's cluster version has capabilities this version lacks:
	 *                                              nothing is written
	 */
	static ExportedSeed exportSeed(Path dbDir, @Nullable Path rockserverConfig, Path target) throws IOException {
		return exportSeed(dbDir, null, rockserverConfig, target);
	}

	/**
	 * Like {@link #exportSeed(Path, Path, Path)}, for a replica whose Raft state machine directory
	 * ({@code raft/<group>/sm}) is {@code stateMachineDir} (when not null and present): when its latest snapshot is ahead
	 * of the database (a crash after a received snapshot was renamed into place, before it was restored into
	 * {@code db/}), the snapshot is verified and restored into {@code dbDir} first, exactly as the replica's start would
	 * ({@link CheckpointStorage#init(Path)}, then {@link #restoreSnapshotIfAhead}), and the copy holds it (R31-2). A
	 * snapshot damaged by such a crash is not used, and nothing is exported (R32-1): the replica may have applied
	 * entries up to it that its database lacks, see {@link #exportSeed(Path, Path, Path, Path, boolean)}.
	 *
	 * @throws DamagedSnapshotNotExportedException if the latest snapshot is damaged: nothing is written
	 */
	static ExportedSeed exportSeed(Path dbDir, @Nullable Path stateMachineDir, @Nullable Path rockserverConfig,
			Path target) throws IOException {
		return exportSeed(dbDir, stateMachineDir, rockserverConfig, target, false);
	}

	/**
	 * Like {@link #exportSeed(Path, Path, Path, Path)}; with {@code withoutDamagedSnapshot}, a damaged latest snapshot
	 * does not stop the export: the copy holds what {@code dbDir} holds, and {@link ExportedSeed#damagedSnapshot} and
	 * {@link ExportedSeed#damagedSnapshotIndex} say which snapshot was left out (R32-1).
	 *
	 * <p>When the group directory (the parent of {@code stateMachineDir}) exists, its {@link RaftLogRepair#LOCK_FILE}
	 * is locked first and held to the end (R32-5): a node holds it for as long as it runs, also while it installs a
	 * snapshot with {@code db/} closed and so without RocksDB's LOCK, and the open would otherwise finish or delete the
	 * restore directories it is building ({@link #open} recovers them before RocksDB locks the database).</p>
	 *
	 * <p>A missing {@code dbDir} is accepted when {@code <db>.restore-ready} holds a database (its {@code CURRENT},
	 * R33-1 and its review): a crash during a restore's swap left the complete new database there, and the open, under
	 * the lock, swaps it in as the node's start would. With neither {@code dbDir/CURRENT} nor
	 * {@code <db>.restore-ready/CURRENT}, it throws "No database in" before anything is touched.</p>
	 *
	 * @throws NodeRunningException if the node holds its group's lock: nothing of the node is touched, nothing written
	 */
	static ExportedSeed exportSeed(Path dbDir, @Nullable Path stateMachineDir, @Nullable Path rockserverConfig,
			Path target, boolean withoutDamagedSnapshot) throws IOException {
		target = target.toAbsolutePath().normalize();
		refuseTargetAmongNodeDirectories(dbDir, stateMachineDir, target);
		// NOFOLLOW_LINKS: a symbolic link to nothing is refused too, as the copy would be written where it points (R34-2).
		if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) {
			throw new IOException("The target " + target + " already exists");
		}
		// R33-1: a restore that a crash interrupted during its swap may have left no db/ (between its two renames, or
		// before the parent's fsync), with the complete newer database in <db>.restore-ready: the open below finishes
		// the swap, under the group's lock, as a start of the node would. Only read here: nothing is touched yet. The
		// ready directory must hold a database (its CURRENT, R33 review): one without it is not accepted, as the open
		// would swap it in, delete <db>.restore-old (then the only complete copy) and create an empty database.
		if (!Files.isRegularFile(dbDir.resolve("CURRENT"))
				&& !Files.isRegularFile(RestoreDirs.of(dbDir).ready.resolve("CURRENT"))) {
			throw new IOException("No database in " + dbDir);
		}
		Path groupDir = stateMachineDir == null ? null : stateMachineDir.toAbsolutePath().normalize().getParent();
		if (groupDir == null || !Files.isDirectory(groupDir)) {
			return exportSeedStopped(dbDir, stateMachineDir, rockserverConfig, target, withoutDamagedSnapshot);
		}
		// R32-5: a node holds its group's lock for as long as it runs, also while it installs a snapshot with db/ closed
		// (no RocksDB LOCK on it then), when the open below would finish or delete the restore directories it builds.
		// Taken first, as repair-log does; the file stays, as Ratis accepts (see RaftLogRepair.run).
		Path lockFile = groupDir.resolve(RaftLogRepair.LOCK_FILE);
		try (var channel = FileChannel.open(lockFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
			try (var lock = tryLock(channel)) {
				if (lock == null) {
					throw new NodeRunningException(RaftLogRepair.nodeRunning(lockFile));
				}
				return exportSeedStopped(dbDir, stateMachineDir, rockserverConfig, target, withoutDamagedSnapshot);
			}
		}
	}

	/**
	 * Refuses, before anything is touched, a {@code target} whose copy (or the checkpoint's {@code <target>.tmp}, which it
	 * deletes first) would be, contain or lie inside a directory of the node being exported: {@code dbDir}, its restore
	 * directories ({@code <db>.restore-tmp}, {@code -ready}, {@code -old}) and, when {@code stateMachineDir} is given,
	 * the node's Raft directory ({@code raft/}, the parent of its group directory). Paths are compared by their real
	 * paths, so a symbolic link to one of them is refused too; a path that does not exist is compared as its nearest
	 * existing ancestor's real path plus the rest (R34-2). Before, with no {@code db/} and a complete
	 * {@code <db>.restore-ready} (R33-1), {@code --target DATA-DIR/db} passed every check: the open swapped the ready
	 * database into {@code db/} and deleted {@code <db>.restore-old}, the checkpoint failed as its target existed, and
	 * the failure cleanup deleted {@code db/}, the node's only complete database.
	 */
	private static void refuseTargetAmongNodeDirectories(Path dbDir, @Nullable Path stateMachineDir, Path target)
			throws IOException {
		var dirs = RestoreDirs.of(dbDir.toAbsolutePath().normalize());
		var nodeDirs = new ArrayList<>(List.of(dirs.dbDir, dirs.building, dirs.ready, dirs.old));
		if (stateMachineDir != null) {
			Path groupDir = stateMachineDir.toAbsolutePath().normalize().getParent();
			if (groupDir != null) {
				nodeDirs.add(groupDir.getParent() != null ? groupDir.getParent() : groupDir);
			}
		}
		Path building = target.resolveSibling(target.getFileName() + ".tmp");
		for (Path written : List.of(target, building)) {
			Path real = realPathOf(written);
			for (Path nodeDir : nodeDirs) {
				Path realNodeDir = realPathOf(nodeDir);
				if (real.startsWith(realNodeDir) || realNodeDir.startsWith(real)) {
					throw new IOException("The target " + target + (written.equals(target) ? "" : " (its " + written + ")")
							+ " is, contains or is inside " + nodeDir + ", a directory of the node being exported: write"
							+ " the seed outside the node's database, restore and Raft directories");
				}
			}
		}
	}

	/**
	 * The real path of {@code path} when it exists; otherwise the real path of its nearest existing ancestor with the
	 * rest of {@code path} appended. A symbolic link to nothing counts as existing, and is taken as it is.
	 */
	private static Path realPathOf(Path path) throws IOException {
		Path absolute = path.toAbsolutePath().normalize();
		Path existing = absolute;
		while (existing != null && !Files.exists(existing, LinkOption.NOFOLLOW_LINKS)) {
			existing = existing.getParent();
		}
		if (existing == null) {
			return absolute;
		}
		Path real;
		if (Files.exists(existing)) {
			real = existing.toRealPath();
		} else {
			Path parent = existing.getParent();
			real = parent == null ? existing : realPathOf(parent).resolve(existing.getFileName().toString());
		}
		String rest = existing.relativize(absolute).toString();
		return rest.isEmpty() ? real : real.resolve(rest);
	}

	/** The lock on {@code channel}'s file, or null when another process or a channel of this one holds it. */
	private static java.nio.channels.@Nullable FileLock tryLock(FileChannel channel) throws IOException {
		try {
			return channel.tryLock();
		} catch (java.nio.channels.OverlappingFileLockException e) {
			return null;
		}
	}

	/** {@link #exportSeed(Path, Path, Path, Path, boolean)} once the node is known to be stopped, as far as it can be. */
	private static ExportedSeed exportSeedStopped(Path dbDir, @Nullable Path stateMachineDir,
			@Nullable Path rockserverConfig, Path target, boolean withoutDamagedSnapshot) throws IOException {
		// The copy's config first: when it is refused, nothing has been written yet.
		Path config = replicaConfig(rockserverConfig, target, dbDir, "export-target");
		var created = new AtomicBoolean();
		try {
			return exportSeedInto(dbDir, stateMachineDir, rockserverConfig, target, config, withoutDamagedSnapshot,
					created);
		} catch (IOException | RuntimeException | Error failure) {
			// Nothing half-made stays (R29-1), and only what this run made goes (R34-2): the target once the checkpoint
			// moved it into place. A checkpoint that failed removed its own temporary copy (and the target, if it failed
			// after the move); a target that something else made meanwhile stays.
			if (created.get()) {
				try {
					deleteRecursively(target);
				} catch (IOException | RuntimeException cleanupFailure) {
					failure.addSuppressed(cleanupFailure);
				}
			}
			throw failure;
		} finally {
			Files.deleteIfExists(config);
		}
	}

	private static ExportedSeed exportSeedInto(Path dbDir, @Nullable Path stateMachineDir,
			@Nullable Path rockserverConfig, Path target, Path config, boolean withoutDamagedSnapshot,
			AtomicBoolean created) throws IOException {
		TermIndex applied;
		String clusterId;
		TermIndex restoredSnapshot = null;
		String damagedSnapshot = null;
		long damagedSnapshotIndex = -1;
		var store = open(dbDir, rockserverConfig, "export-source");
		try {
			// The node does not run: its group's lock is held (R32-5), and the open took the database's LOCK.
			if (stateMachineDir != null && Files.isDirectory(stateMachineDir)) {
				var storage = new CheckpointStorage();
				try {
					storage.init(stateMachineDir);
					var snapshot = storage.getLatestSnapshot();
					var before = store.applied();
					store = restoreSnapshotIfAhead(store, storage, rockserverConfig, "export-source");
					if (snapshot != null && snapshot.getIndex() > before.getIndex()) {
						restoredSnapshot = snapshot.getTermIndex();
					}
				} catch (CheckpointStorage.DamagedSnapshotException damaged) {
					// A start refuses this replica. The database may lack entries up to the snapshot's index that only
					// the snapshot held: export it only when told to, never with an exit status that looks complete.
					var databaseApplied = store.applied();
					long index = damaged.snapshotIndex();
					String at = index >= 0 ? "at index " + index : "of unknown index";
					if (!withoutDamagedSnapshot) {
						throw new DamagedSnapshotNotExportedException("The replica's latest snapshot "
								+ damaged.dir() + " (" + at + ") is damaged (" + damaged.reason() + "), probably by a"
								+ " crash while it was installed, and cannot be used; its database is at index "
								+ databaseApplied.getIndex() + " and may lack entries up to the snapshot's index,"
								+ " which no tool reads out of the damaged files. Export the other replicas and"
								+ " compare their indexes; to export this database as it is, pass"
								+ " --without-damaged-snapshot", index, databaseApplied, damaged);
					}
					LOG.warn("Exporting {} at index {} without the damaged snapshot {} in {}", dbDir,
							databaseApplied.getIndex(), at, stateMachineDir, damaged);
					damagedSnapshot = damaged.dir() + " (" + damaged.reason() + ")";
					damagedSnapshotIndex = index;
				}
			}
			// The copy leaves out the cluster version with the rest of the metadata: refuse a newer cluster's database
			// here, or the new cluster would start at GENESIS with data that a newer version wrote (R18-1).
			long version = store.clusterVersion();
			long unknown = version & ~Capabilities.SUPPORTED;
			if (unknown != 0) {
				throw ReplicatedNode.NewerVersionException.of(version, unknown);
			}
			clusterId = store.clusterId();
			applied = store.checkpoint(target);
			created.set(true);
		} finally {
			// A no-op when restoreSnapshotIfAhead failed after closing it.
			store.close();
		}
		try (var copy = new EmbeddedConnection(target, "export-target", config)) {
			var db = copy.getEmbeddedDB();
			if (db.getAllColumnDefinitions().containsKey(META_COLUMN)) {
				db.deleteColumn(db.getColumnId(META_COLUMN));
			}
			db.flush();
		}
		syncTree(target);
		return new ExportedSeed(applied, clusterId, restoredSnapshot, damagedSnapshot, damagedSnapshotIndex);
	}

	/**
	 * What {@link #exportSeed} copied: the applied position and the cluster (null if none) of the replica; the position
	 * of the snapshot restored into its database first (null if none was ahead); the damaged snapshot left out and why
	 * (null if none was damaged) and its index (-1 if none, or if its name does not tell), see
	 * {@link #exportSeed(Path, Path, Path, Path, boolean)}.
	 */
	record ExportedSeed(TermIndex applied, @Nullable String clusterId, @Nullable TermIndex restoredSnapshot,
			@Nullable String damagedSnapshot, long damagedSnapshotIndex) {}

	/**
	 * {@code export-seed} found the node running: it holds its group's {@link RaftLogRepair#LOCK_FILE}. Nothing of the
	 * node was touched and nothing was written (R32-5).
	 */
	static final class NodeRunningException extends IOException {
		NodeRunningException(String message) {
			super(message);
		}
	}

	/**
	 * {@code export-seed} found the replica's latest snapshot damaged and was not told to export without it: nothing
	 * was written (R32-1). The message is meant for that command: it names the snapshot's index and the database's.
	 */
	static final class DamagedSnapshotNotExportedException extends IOException {
		private final long snapshotIndex;
		private final TermIndex databaseApplied;

		DamagedSnapshotNotExportedException(String message, long snapshotIndex, TermIndex databaseApplied,
				Throwable cause) {
			super(message, cause);
			this.snapshotIndex = snapshotIndex;
			this.databaseApplied = databaseApplied;
		}

		/** The damaged snapshot's index, -1 if its name does not tell. */
		long snapshotIndex() {
			return snapshotIndex;
		}

		/** The position the replica's database was applied up to. */
		TermIndex databaseApplied() {
			return databaseApplied;
		}
	}

	private @Nullable Buf readMeta(byte metaKey) {
		return db().get(0, metaColumnId, key(metaKey), RequestType.<Buf>current());
	}

	/** Tests only: called with a checkpoint's building directory once the live files are in it; may throw. */
	@FunctionalInterface
	interface CheckpointHook {
		void built(Path building) throws IOException;
	}

	/** Tests only: see {@link CheckpointHook}; null in production. */
	static volatile @Nullable CheckpointHook checkpointBuiltForTesting;

	/**
	 * Writes a consistent checkpoint of the live database to {@code target} (must not exist): memtables are
	 * flushed and SST files hard-linked. Returns the applied position contained in the checkpoint.
	 *
	 * <p>RocksDB's own {@code Checkpoint} refuses databases whose SST files live in column volumes
	 * ({@code cf_paths}, which the default Rockserver config uses), so the checkpoint is built from the public
	 * live-file APIs while file deletions are disabled: live SST and blob files are hard-linked at their path
	 * relative to the database directory (copied if {@code target} is on another file system), the MANIFEST is
	 * copied up to its live size, CURRENT is written for it, and the OPTIONS file and the WAL files that RocksDB
	 * still needs after the flush are copied. It is built in {@code <target>.tmp}, fsynced and renamed into
	 * place.</p>
	 *
	 * <p>The caller guarantees that no entry is applied while this runs: {@code takeSnapshot} calls it on Ratis'
	 * updater thread, which is the thread that queues entries for the apply, after draining the apply queue. The
	 * applied position is read before the live files are listed, so it equals the checkpoint's own marker. An entry
	 * applied meanwhile would break this: the copy could hold a later point than the returned position, and a write
	 * that goes through the WAL could make the file list and the WAL copy disagree.</p>
	 */
	TermIndex checkpoint(Path target) throws IOException {
		target = target.toAbsolutePath();
		if (Files.exists(target)) {
			throw new IOException("Checkpoint target already exists: " + target);
		}
		Path building = target.resolveSibling(target.getFileName() + ".tmp");
		deleteRecursively(building);
		TermIndex applied = applied();
		try {
			Files.createDirectories(building);
			RocksDB rocksDB = db().getDb().get();
			try {
				rocksDB.disableFileDeletions();
				try {
					copyLiveFiles(rocksDB, building);
				} finally {
					rocksDB.enableFileDeletions();
				}
			} catch (RocksDBException e) {
				throw new IOException("Failed to checkpoint " + dbDir + " into " + target, e);
			}
			var hook = checkpointBuiltForTesting;
			if (hook != null) {
				hook.built(building);
			}
			syncTree(building);
			Files.move(building, target);
			try {
				syncDirectory(target.getParent());
			} catch (IOException | RuntimeException | Error failure) {
				// Moved into place by this call: nothing half-made stays (R34-2: the export's cleanup deletes only a target
				// that a checkpoint returned).
				try {
					deleteRecursively(target);
				} catch (IOException | RuntimeException cleanupFailure) {
					failure.addSuppressed(cleanupFailure);
				}
				throw failure;
			}
		} catch (IOException | RuntimeException | Error failure) {
			// What was built hard-links every live table and blob file of the moment: left behind, it would keep them on
			// disk after compactions replace them, on a node that may be short of space already (R29-1).
			try {
				deleteRecursively(building);
			} catch (IOException | RuntimeException cleanupFailure) {
				failure.addSuppressed(cleanupFailure);
			}
			throw failure;
		}
		return applied;
	}

	private void copyLiveFiles(RocksDB rocksDB, Path target) throws RocksDBException, IOException {
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
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		try {
			// Entries are applied without the WAL (see Applier): flush them, so a cleanly stopped replica has its
			// whole applied state on disk and restarts without replaying them. This also writes out the WAL buffer.
			db().flush();
		} catch (RuntimeException e) {
			LOG.warn("Failed to flush {} before closing; Raft replays the missing entries", dbDir, e);
		} finally {
			connection.close();
		}
	}

	/**
	 * {@link #restore(Path, Path, boolean)} of a checkpoint whose files are durable: a Raft snapshot, which was fsynced
	 * when it was made or verified when it was installed. The table and blob files it hard-links are not fsynced again
	 * (thousands of them would stall the apply thread).
	 */
	static void restore(Path checkpointDir, Path dbDir) throws IOException {
		restore(checkpointDir, dbDir, true);
	}

	/**
	 * Replaces the contents of {@code dbDir} (which must not be open) with the checkpoint in {@code checkpointDir}.
	 * Immutable files ({@code *.sst}, {@code *.blob}) are hard-linked when both are on the same file system, the
	 * rest copied, so the checkpoint itself stays intact. Crash-safe in the sense that the swap is atomic: the new
	 * contents are prepared next to {@code dbDir}, fsynced, and swapped in by rename, so a crash leaves either the old
	 * database or the complete new one. The new one survives a power loss only if the files it hard-links are durable:
	 * with {@code sourceDurable} false (a backup that an operator copied with {@code cp}, {@code rsync} or {@code scp},
	 * which do not fsync), every linked file is fsynced too, which costs one fsync per table file, most of them cheap
	 * once writeback has flushed the copy; with true (a Raft snapshot), linked files are not fsynced again (R34-1).
	 *
	 * <p>Subdirectories (column volumes) are kept. Ratis' digest side files ({@code *.md5}, written next to every
	 * file of an installed snapshot) are skipped. Steps: build {@code <db>.restore-tmp}, fsync it, rename it to
	 * {@code <db>.restore-ready}; then move {@code dbDir} to {@code <db>.restore-old}, rename the ready directory to
	 * {@code dbDir} and delete the old one. {@link #open} and the next restore roll an interrupted swap forward once
	 * the ready directory exists.</p>
	 */
	static void restore(Path checkpointDir, Path dbDir, boolean sourceDurable) throws IOException {
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
		syncTree(dirs.building, !sourceDurable);
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

	/** Key of the completion floor of proposer {@code proposerId}. */
	static Keys floorKey(long proposerId) {
		return metaKey(FLOOR, proposerId, 0);
	}

	/** The proposer id in a key made by {@link #floorKey}. */
	static long floorProposerId(Keys key) {
		return ByteBuffer.wrap(key.keys()[0].toByteArray()).getLong(1);
	}

	/** Key of the recorded result of request {@code sequence} of proposer {@code proposerId}. */
	static Keys requestKey(long proposerId, long sequence) {
		return metaKey(REQUEST, proposerId, sequence);
	}

	/** The sequence in a key made by {@link #requestKey}. */
	static long requestSequence(Keys key) {
		return ByteBuffer.wrap(key.keys()[0].toByteArray()).getLong(1 + Long.BYTES);
	}

	/** Key of client session {@code sessionId}. */
	static Keys sessionKey(long sessionId) {
		return metaKey(SESSION, sessionId, 0);
	}

	/** The session id in a key made by {@link #sessionKey} or {@link #sessionRequestKey}. */
	static long sessionId(Keys key) {
		return ByteBuffer.wrap(key.keys()[0].toByteArray()).getLong(1);
	}

	/** Key of the recorded result of write {@code sequence} of client session {@code sessionId}. */
	static Keys sessionRequestKey(long sessionId, long sequence) {
		return metaKey(SESSION_REQUEST, sessionId, sequence);
	}

	/** A session's row: its timeout, the leader time of its last applied write (or registration), and its floor. */
	static Buf encodeSession(long timeoutMillis, long lastActiveMillis, long floor) {
		return Buf.wrap(ByteBuffer.allocate(3 * Long.BYTES).putLong(timeoutMillis).putLong(lastActiveMillis)
				.putLong(floor).array());
	}

	/** {@link #encodeSession}'s fields, in order. */
	static long[] decodeSession(Buf value) {
		var buffer = ByteBuffer.wrap(value.toByteArray());
		return new long[] {buffer.getLong(), buffer.getLong(), buffer.getLong()};
	}

	/**
	 * True if a session whose row says {@code timeoutMillis} and {@code lastActiveMillis} is expired at the replicated
	 * clock {@code clockMillis}: the one rule the applier and the leader's answers use.
	 */
	static boolean sessionExpired(long timeoutMillis, long lastActiveMillis, long clockMillis) {
		return clockMillis - lastActiveMillis > timeoutMillis;
	}

	/** The replicated clock stored by {@link #CLOCK}: 0 if none was written yet. */
	static long decodeClock(@Nullable Buf value) {
		return value == null ? 0 : ByteBuffer.wrap(value.toByteArray()).getLong();
	}

	private static Keys metaKey(byte kind, long first, long second) {
		var key = ByteBuffer.allocate(META_KEY_BYTES).put(kind).putLong(first).putLong(second).array();
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

	/**
	 * Fsyncs every file and directory under {@code root}, including {@code root} itself, except files with more than
	 * one hard link: see {@link #syncTree(Path, boolean)}.
	 */
	static void syncTree(Path root) throws IOException {
		syncTree(root, false);
	}

	/**
	 * Fsyncs every file and directory under {@code root}, including {@code root} itself; files with more than one hard
	 * link only when {@code linkedToo}.
	 */
	static void syncTree(Path root, boolean linkedToo) throws IOException {
		List<Path> paths;
		try (Stream<Path> walk = Files.walk(root)) {
			// Deepest first, so each directory is synced after its entries.
			paths = walk.sorted(Comparator.reverseOrder()).toList();
		}
		for (Path path : paths) {
			// Without linkedToo, a file with another hard link is skipped (its directory entry is synced below): the
			// callers that pass false link only from files that are already durable, an SST that RocksDB synced, a
			// checkpoint or snapshot file synced when it was made or verified when it was installed. A large database has
			// thousands of such files, and fsyncing each one would stall the apply thread. The link count says nothing
			// about the source's durability: a file an operator copied (a backup copy, a seed rsynced into place) and
			// then linked is not durable, and those paths pass true (restore-backup, the first start of a seed, R34-1).
			if (!linkedToo && !Files.isDirectory(path) && hardLinks(path) > 1) {
				continue;
			}
			syncFile(path);
		}
	}

	private static int hardLinks(Path file) {
		try {
			return (Integer) Files.getAttribute(file, "unix:nlink");
		} catch (IOException | UnsupportedOperationException | IllegalArgumentException e) {
			return 1;
		}
	}

	/** Tests only: called with the absolute, normalized path of every file and directory {@link #syncFile} fsyncs. */
	static volatile java.util.function.@Nullable Consumer<Path> syncedForTesting;

	/** Fsyncs a file, or a directory's entries (Linux allows opening a directory for reading). */
	static void syncFile(Path file) throws IOException {
		var hook = syncedForTesting;
		if (hook != null) {
			hook.accept(file.toAbsolutePath().normalize());
		}
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
