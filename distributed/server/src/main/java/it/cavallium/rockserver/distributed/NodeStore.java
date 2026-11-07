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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
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
	 * Writes the config the replica opens its database with to a new temporary file: {@link #REPLICA_OVERRIDES} over
	 * the given Rockserver config (HOCON), or over the defaults when there is none.
	 */
	private static Path replicaConfig(@Nullable Path rockserverConfig, Path dbDir, String nodeId) throws IOException {
		return replicaConfig(rockserverConfig, dbDir, null, nodeId);
	}

	/**
	 * Like {@link #replicaConfig(Path, Path, String)}, for a copy in {@code dbDir} of the database in {@code sourceDbDir}
	 * (when not null): an absolute {@code volume-path} under {@code sourceDbDir} becomes the same path relative to the
	 * database directory, so that the copy opens its own volumes. The file is made next to {@code dbDir}, not in the
	 * JDK's temporary directory (see {@code ReplicatedServerMain}). {@code nodeId} is the metrics node name when the
	 * config sets none.
	 */
	private static Path replicaConfig(@Nullable Path rockserverConfig, Path dbDir, @Nullable Path sourceDbDir,
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
		Path parent = dbDir.toAbsolutePath().normalize().getParent();
		Files.createDirectories(parent);
		Path file = Files.createTempFile(parent, "rockserver-replica-", ".conf");
		try {
			Files.writeString(file, config.root().render(ConfigRenderOptions.concise()), StandardCharsets.UTF_8);
		} catch (IOException | RuntimeException e) {
			Files.deleteIfExists(file);
			throw e;
		}
		return file;
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

	/** The persisted cluster id, or null before genesis. */
	@Nullable String clusterId() {
		Buf value = readMeta(CLUSTER_ID);
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
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
	 * @return the recorded digest, null if {@code dbDir} holds no database
	 */
	static @Nullable String recordSeedDigest(Path dbDir) throws IOException {
		var lines = readSeedDigestLines(dbDir);
		String recorded = lines.isEmpty() ? null : lines.getFirst();
		if (!Files.isRegularFile(dbDir.resolve("CURRENT"))) {
			return recorded;
		}
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
			if (name.equals("LOCK") || name.startsWith("LOG")) {
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
	 * refuse it. The source is opened as a replica, which also finishes a restore that a crash interrupted; it must not
	 * run. Table files are hard-linked when {@code target} is on the same file system.
	 *
	 * @return what the copy held: the applied position and cluster of the replica
	 */
	static ExportedSeed exportSeed(Path dbDir, @Nullable Path rockserverConfig, Path target) throws IOException {
		target = target.toAbsolutePath().normalize();
		if (Files.exists(target)) {
			throw new IOException("The target " + target + " already exists");
		}
		if (!Files.isRegularFile(dbDir.resolve("CURRENT"))) {
			throw new IOException("No database in " + dbDir);
		}
		// The copy's config first: when it is refused, nothing has been written yet.
		Path config = replicaConfig(rockserverConfig, target, dbDir, "export-target");
		TermIndex applied;
		String clusterId;
		try (var store = open(dbDir, rockserverConfig, "export-source")) {
			clusterId = store.clusterId();
			applied = store.checkpoint(target);
		} catch (IOException | RuntimeException e) {
			Files.deleteIfExists(config);
			throw e;
		}
		try (var copy = new EmbeddedConnection(target, "export-target", config)) {
			var db = copy.getEmbeddedDB();
			if (db.getAllColumnDefinitions().containsKey(META_COLUMN)) {
				db.deleteColumn(db.getColumnId(META_COLUMN));
			}
			db.flush();
		} catch (IOException | RuntimeException e) {
			try {
				deleteRecursively(target);
			} catch (IOException deleteFailure) {
				e.addSuppressed(deleteFailure);
			}
			throw e;
		} finally {
			Files.deleteIfExists(config);
		}
		syncTree(target);
		return new ExportedSeed(applied, clusterId);
	}

	/** What {@link #exportSeed} copied: the applied position and the cluster (null if none) of the replica. */
	record ExportedSeed(TermIndex applied, @Nullable String clusterId) {}

	private @Nullable Buf readMeta(byte metaKey) {
		return db().get(0, metaColumnId, key(metaKey), RequestType.<Buf>current());
	}

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
		} catch (RocksDBException e) {
			throw new IOException("Failed to checkpoint " + dbDir + " into " + target, e);
		}
		syncTree(building);
		Files.move(building, target);
		syncDirectory(target.getParent());
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

	/** Fsyncs every file and directory under {@code root}, including {@code root} itself. */
	static void syncTree(Path root) throws IOException {
		List<Path> paths;
		try (Stream<Path> walk = Files.walk(root)) {
			// Deepest first, so each directory is synced after its entries.
			paths = walk.sorted(Comparator.reverseOrder()).toList();
		}
		for (Path path : paths) {
			// A file with another hard link was linked from a file that is already durable (an SST that RocksDB
			// synced, a checkpoint or snapshot file synced when it was made): its directory entry is synced below. A
			// large database has thousands of such files, and fsyncing each one would stall the apply thread.
			if (!Files.isDirectory(path) && hardLinks(path) > 1) {
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
