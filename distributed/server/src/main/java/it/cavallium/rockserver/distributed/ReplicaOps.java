package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The cluster-wide operations of a replica that go through the log but act outside the database: the cluster version
 * ({@link Capabilities}), cross-replica checksums ({@link LogEntry.Checksum}) and cluster-consistent backups
 * ({@link LogEntry.Backup}). It also answers the {@link TermService#OPS} calls about them, on the Raft port.
 *
 * <p><b>Checksums.</b> Every replica applies a checksum entry at the same log position. There it opens a database
 * transaction with a snapshot, which takes no time, and lists the user columns; a thread of its own then reads every
 * key and value of those columns at that snapshot, in key order, and digests them (SHA-256 of each column's name and
 * entries, columns in name order; with {@link Capabilities#CHECKSUM_SCHEMAS}, each column's schema too, merge operator
 * included). Equal histories give equal digests on every replica, whatever their files and compactions. The internal meta column is left out: it holds things that are local by design (results purged at a
 * start). Each replica then asks the others for their digest of the same position, until each one has answered, or
 * skipped the position (a snapshot install jumped over it), or for {@link #COMPARE_FOR}; a different digest is logged
 * as an error on every replica that sees it and counted in {@code checksum.mismatches}. A replica whose database is
 * replaced or closed while it reads gives up on that check. The read holds a snapshot of the database, which keeps the
 * files that compactions replace meanwhile: a check of a large database costs a full read of it and that space while it
 * runs. Checks run when an operator asks ({@code admin checksum}) or, with the system property
 * {@value #CHECKSUM_EVERY_HOURS_PROPERTY}, every that many hours from the leader. That interval counts from the last
 * check this replica applied, whoever proposed it (its time is kept in the results file), so a leader change does not
 * start it over: a leader proposes the next one once the interval passed since the last one, or, if it knows of none,
 * since its first start with periodic checks on ({@value #SCHEDULE_FILE} in the data directory), so that restarts do
 * not start it over either. A proposal that fails is tried again a minute later.</p>
 *
 * <p><b>Backups.</b> Every replica applies a backup entry at the same log position, by writing a checkpoint of its
 * database ({@link NodeStore#checkpoint}: a memtable flush and hard links, 100–300 ms during which the apply waits) to
 * {@code <data-dir>/backups/<name>/db}, with {@code backup.properties} beside it. All of them hold the same data, that of
 * every entry before the backup entry: a copy of the whole cluster at one moment, on every node. A backup that a replica
 * could not write (disk full, a name used before) is logged, recorded in {@code replica-ops.log} and reported by
 * {@code admin backup} and {@code admin backup-status}, and does not stop the replica: the entry's result is its log
 * index on every replica. Results are kept by log index, so a name used again later is never answered with an older
 * result. The same request proposed twice ({@link LogEntry.Backup#requestId}, after a lost answer) finds its own backup
 * and reports it done. Restoring one starts a new cluster seeded from it, see {@link #restore}. The runbook is
 * {@code docs/distributed/runbooks.md}.</p>
 */
final class ReplicaOps implements Applier.Maintenance, AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicaOps.class);
	static final String CHECKSUM_EVERY_HOURS_PROPERTY = DistributedProperties.PREFIX + "checksum-every-hours";
	static final String BACKUPS_DIR = "backups";
	static final String BACKUP_DB_DIR = "db";
	static final String BACKUP_PROPERTIES = "backup.properties";
	private static final String RESULTS_FILE = "replica-ops.log";
	/** When the periodic schedule of this node started, while it knows of no check: see {@link #scheduleStartMillis}. */
	static final String SCHEDULE_FILE = "checksum-schedule";
	/** After a periodic checksum that could not be proposed, how soon the leader tries again (at most the interval). */
	static final Duration PROPOSAL_RETRY = Duration.ofMinutes(1);
	private static final Pattern BACKUP_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
	/** How long a replica keeps asking the others for their digest of one check. */
	static final Duration COMPARE_FOR = Duration.ofHours(24);
	private static final Duration COMPARE_EVERY = Duration.ofSeconds(10);
	private static final Duration PEER_TIMEOUT = Duration.ofSeconds(5);
	private static final Duration VERSION_CHECK_EVERY = Duration.ofSeconds(10);
	/** Results kept in memory and answered. */
	private static final int KEPT_RESULTS = 256;
	private static final long CHECK_TRANSACTION_MILLIS = Duration.ofDays(7).toMillis();

	/** Operations of {@link TermService#OPS}. */
	static final byte OP_CAPABILITIES = 1;
	static final byte OP_CHECKSUM_RESULT = 2;
	static final byte OP_BACKUP_RESULT = 3;
	static final byte OP_PROPOSE_CHECKSUM = 4;
	static final byte OP_PROPOSE_BACKUP = 5;

	/** State of one replica's result of a check or backup. */
	enum State {
		/** Not applied here yet, or still running. */
		PENDING,
		DONE,
		/** Applied past it without a result: a snapshot install jumped over it, or the node restarted while it ran. */
		NONE,
		FAILED
	}

	/** A replica's result. {@code detail}: the hex digest of a checksum, the directory of a backup, or why it failed. */
	record Result(State state, long index, long keys, String detail) {}

	/** A replica's result of the backup {@code name}. */
	private record BackupResult(String name, Result result) {}

	private final NodeConfig config;
	private final RockserverStateMachine stateMachine;
	private final NodeMetrics.Series mismatches;
	private final NodeMetrics.Series matches;
	private final ScheduledExecutorService worker;
	private final Path resultsFile;
	private final Map<Long, Result> checksums = new ConcurrentHashMap<>();
	/** By log index of the backup entry. */
	private final Map<Long, BackupResult> backups = new ConcurrentHashMap<>();
	private volatile long clusterVersion;
	private volatile @Nullable Proposer proposer;
	/**
	 * The channels to the other members: the node's shared cache once {@link #channels(PeerChannels)} set it, until then
	 * one of this instance's own, closed with it.
	 */
	private volatile PeerChannels channels = new PeerChannels();
	private volatile boolean ownChannels = true;
	/** Leader only, initialization thread only. */
	private long versionCheckTerm = -1;
	private long nextVersionCheckNanos;
	/** Set on the initialization thread, cleared by the thread that ends the check. */
	private volatile boolean versionCheckRunning;
	/** How often the leader proposes a checksum ({@link #CHECKSUM_EVERY_HOURS_PROPERTY}, read at start); zero: never. */
	private volatile Duration checksumEvery;
	/** Wall-clock time this replica last applied a checksum entry (from the results file at start); 0: none known. */
	private volatile long lastChecksumMillis;
	/**
	 * Per checksum index in the results file, when this replica first applied it: a replay of that entry (a lost WAL
	 * tail, entries after the snapshot) keeps that time instead of moving the schedule to the replay.
	 */
	private final Map<Long, Long> recordedChecksumMillis = new ConcurrentHashMap<>();
	/**
	 * What the interval counts from while this node knows of no check: the first start of this node with periodic checks
	 * on, kept in {@link #SCHEDULE_FILE} so that restarts more frequent than the interval do not start it over. 0 until
	 * read.
	 */
	private volatile long scheduleStartMillis;
	private final Path scheduleFile;
	/**
	 * Leader only: when this node last proposed a periodic check itself, or the time that delays the next try as long as
	 * {@link #PROPOSAL_RETRY} after a proposal that failed. Written on the initialization thread, and by the failure.
	 */
	private volatile long proposedChecksumMillis;
	private volatile long lastMismatchIndex = -1;
	private volatile long lastMatchIndex = -1;

	ReplicaOps(NodeConfig config, RockserverStateMachine stateMachine, NodeMetrics metrics) {
		this.config = config;
		this.stateMachine = stateMachine;
		this.mismatches = metrics.amount("checksum.mismatches");
		this.matches = metrics.amount("checksum.matches");
		this.resultsFile = config.dataDir().resolve(RESULTS_FILE);
		this.worker = Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon()
				.name("rockserver-" + config.nodeId() + "-replica-ops").factory());
		metrics.exportedGauge("cluster.version", () -> clusterVersion);
		metrics.exportedGauge("checksum.last_mismatch_index", () -> lastMismatchIndex);
		metrics.exportedGauge("checksum.last_match_index", () -> lastMatchIndex);
		this.checksumEvery = Duration.ofHours(Math.max(0, DistributedProperties.getLong(0, CHECKSUM_EVERY_HOURS_PROPERTY)));
		this.scheduleFile = config.dataDir().resolve(SCHEDULE_FILE);
		if (!checksumEvery.isZero()) {
			// On every member, not only the leader: any of them may lead when the first check is due.
			scheduleStartMillis = loadScheduleStart(System.currentTimeMillis());
		}
		loadResults();
	}

	/** Tests only: the interval of the periodic checksums, below the hour of the property. */
	void checksumEveryForTesting(Duration every) {
		this.checksumEvery = every;
	}

	/** Tests only: see {@link #lastChecksumMillis}. */
	long lastChecksumMillisForTesting() {
		return lastChecksumMillis;
	}

	/** Tests only: whether a cluster version check is running (asking the members, or proposing its raise). */
	boolean versionCheckRunningForTesting() {
		return versionCheckRunning;
	}

	/** Tests only: see {@link #scheduleStartMillis}. */
	long scheduleStartMillisForTesting() {
		return scheduleStartMillis;
	}

	/**
	 * The start of the periodic schedule from {@link #SCHEDULE_FILE}, or {@code nowMillis}, written there, if the file is
	 * missing or unreadable. If it cannot be written, the schedule counts from this start, as before the file existed.
	 */
	private long loadScheduleStart(long nowMillis) {
		try {
			if (Files.isRegularFile(scheduleFile)) {
				long recorded = Long.parseLong(Files.readString(scheduleFile, StandardCharsets.UTF_8).strip());
				if (recorded > 0) {
					return Math.min(recorded, nowMillis);
				}
			}
		} catch (IOException | NumberFormatException ex) {
			LOG.warn("{}: failed to read {}, the periodic checksums count from now: {}", config.nodeId(), scheduleFile,
					ex.toString());
		}
		try {
			Files.createDirectories(scheduleFile.getParent());
			var temporary = scheduleFile.resolveSibling(SCHEDULE_FILE + ".tmp");
			Files.writeString(temporary, nowMillis + "\n", StandardCharsets.UTF_8);
			NodeStore.syncFile(temporary);
			Files.move(temporary, scheduleFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
			NodeStore.syncDirectory(scheduleFile.getParent());
		} catch (IOException ex) {
			LOG.warn("{}: failed to write {}: {}", config.nodeId(), scheduleFile, ex.toString());
		}
		return nowMillis;
	}

	static boolean validBackupName(@Nullable String name) {
		return name != null && BACKUP_NAME.matcher(name).matches();
	}

	/** The proposer of the node, once it exists: for the operations that propose entries. */
	void proposer(Proposer proposer) {
		this.proposer = proposer;
	}

	/**
	 * Tests only: what the node with this id says it supports, instead of {@link Capabilities#SUPPORTED}: a node of an
	 * older version, in a cluster run in one JVM.
	 */
	static final Map<String, Long> SUPPORTED_FOR_TESTING = new ConcurrentHashMap<>();

	/** What this node supports: {@link Capabilities#SUPPORTED}, unless a test says otherwise. */
	@Override
	public long supported() {
		return SUPPORTED_FOR_TESTING.getOrDefault(config.nodeId(), Capabilities.SUPPORTED);
	}

	/** The node's shared channels to the other members, used from now on; the caller closes them. */
	void channels(PeerChannels shared) {
		var own = ownChannels ? channels : null;
		this.channels = shared;
		this.ownChannels = false;
		if (own != null) {
			own.close();
		}
	}

	/** The cluster version of the database just opened. */
	void opened(NodeStore store) {
		clusterVersion = store.clusterVersion();
	}

	long clusterVersion() {
		return clusterVersion;
	}

	/** Whether the cluster may use {@code capability}: every member supports it, see {@link Capabilities}. */
	boolean clusterHas(long capability) {
		return Capabilities.has(clusterVersion, capability);
	}

	@Override
	public void clusterVersion(long version) {
		long previous = clusterVersion;
		clusterVersion = version;
		if (version != previous) {
			LOG.info("{}: cluster version is now {}", config.nodeId(), Capabilities.describe(version));
		}
	}

	// Checksums

	@Override
	public void checksum(TermIndex position, NodeStore store) {
		long index = position.getIndex();
		// When this replica first applied it, if it did: a replay must not restart the interval (CR2-2).
		var recorded = recordedChecksumMillis.get(index);
		lastChecksumMillis = Math.max(lastChecksumMillis, recorded != null ? recorded : System.currentTimeMillis());
		var db = store.db();
		var columns = new TreeMap<String, Long>();
		// The cluster version at this position, the same on every replica: digests with schemas only once all have it.
		var schemas = clusterHas(Capabilities.CHECKSUM_SCHEMAS) ? new TreeMap<String, String>() : null;
		long transaction;
		try {
			for (var definition : db.getAllColumnDefinitions().entrySet()) {
				String name = definition.getKey();
				if (!name.equals(NodeStore.META_COLUMN)) {
					columns.put(name, db.getColumnId(name));
					if (schemas != null) {
						schemas.put(name, describeSchema(definition.getValue()));
					}
				}
			}
			// A snapshot of the database as of this position: what the apply writes after it is not seen.
			transaction = db.openTransaction(CHECK_TRANSACTION_MILLIS, WorkloadProfile.BATCH);
		} catch (RuntimeException ex) {
			LOG.error("{}: checksum at {} could not start", config.nodeId(), index, ex);
			record(new Result(State.FAILED, index, 0, String.valueOf(ex)));
			return;
		}
		record(new Result(State.PENDING, index, 0, ""));
		worker.execute(() -> computeChecksum(index, db, columns, schemas, transaction));
	}

	/**
	 * A column's schema as the checksum digests it: its key layout, whether it has values, and its merge operator (name,
	 * version and class). Two replicas whose columns were created or altered differently differ here even with equal
	 * data.
	 */
	static String describeSchema(ColumnSchema schema) {
		return "keys=" + schema.keys() + ";tail=" + schema.variableTailKeys() + ";value=" + schema.hasValue()
				+ ";merge=" + schema.mergeOperatorName() + "/" + schema.mergeOperatorVersion() + "/"
				+ schema.mergeOperatorClass();
	}

	private void computeChecksum(long index, EmbeddedDB db, TreeMap<String, Long> columns,
			@Nullable TreeMap<String, String> schemas, long transaction) {
		long start = System.nanoTime();
		Result result;
		try {
			var digest = sha256();
			long keys = 0;
			for (var column : columns.entrySet()) {
				update(digest, column.getKey().getBytes(StandardCharsets.UTF_8));
				if (schemas != null) {
					update(digest, schemas.get(column.getKey()).getBytes(StandardCharsets.UTF_8));
				}
				Keys resumeAfter = null;
				while (true) {
					var page = db.getRangePage(transaction, column.getValue(), null, null, false, resumeAfter,
							RequestType.<KV>allInRangeNoCache(), RangeBudget.DEFAULT);
					for (KV kv : page.items()) {
						digestEntry(digest, kv);
						keys++;
					}
					if (!page.hasMore()) {
						break;
					}
					resumeAfter = page.resumeAfter();
				}
				// Ends the column: no entry of one column can pass for an entry of the next.
				digest.update((byte) 0xFF);
			}
			result = new Result(State.DONE, index, keys, HexFormat.of().formatHex(digest.digest()));
			LOG.info("{}: checksum at {}: {} keys in {} columns, {} ({} ms)", config.nodeId(), index, keys, columns.size(),
					result.detail(), (System.nanoTime() - start) / 1_000_000);
		} catch (RuntimeException ex) {
			// Its database was replaced (snapshot install) or closed, or a column was deleted while it was read.
			LOG.warn("{}: checksum at {} given up: {}", config.nodeId(), index, ex.toString());
			result = new Result(State.FAILED, index, 0, String.valueOf(ex));
		} finally {
			try {
				db.closeTransaction(transaction, false);
			} catch (RuntimeException closed) {
				// The database closed meanwhile, with the transaction.
			}
		}
		record(result);
		if (result.state() == State.DONE) {
			compare(result, System.nanoTime() + COMPARE_FOR.toNanos(), new LinkedHashMap<>());
		}
	}

	static void digestEntry(MessageDigest digest, KV kv) {
		var keys = kv.keys().keys();
		digest.update((byte) 1);
		digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(keys.length).array());
		for (Buf key : keys) {
			update(digest, key.toByteArray());
		}
		var value = kv.value();
		if (value == null) {
			digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(-1).array());
		} else {
			update(digest, value.toByteArray());
		}
	}

	private static void update(MessageDigest digest, byte[] bytes) {
		digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
		digest.update(bytes);
	}

	private static MessageDigest sha256() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException ex) {
			throw new IllegalStateException("Every Java platform has SHA-256", ex);
		}
	}

	/**
	 * Asks every other member for its digest of {@code own}'s position, until each one gave a final answer or the
	 * deadline passed. {@code answered} holds the final answers so far, by member.
	 */
	private void compare(Result own, long deadlineNanos, Map<String, Result> answered) {
		var division = stateMachine.division();
		if (division == null) {
			return;
		}
		var asked = new ArrayList<CompletableFuture<Void>>();
		for (RaftPeer peer : division.getRaftConf().getAllPeers()) {
			var id = peer.getId().toString();
			if (id.equals(config.nodeId()) || answered.containsKey(id)) {
				continue;
			}
			asked.add(ask(peer, OP_CHECKSUM_RESULT, ByteBuffer.allocate(Long.BYTES).putLong(own.index()).array())
					.thenAccept(reply -> {
						var result = decodeResult(reply);
						if (result.state() != State.PENDING) {
							synchronized (answered) {
								answered.put(id, result);
							}
						}
					})
					.exceptionally(_ -> null));
		}
		CompletableFuture.allOf(asked.toArray(CompletableFuture[]::new)).whenComplete((_, _) -> {
			boolean pending;
			synchronized (answered) {
				pending = division.getRaftConf().getAllPeers().stream()
						.map(peer -> peer.getId().toString())
						.anyMatch(id -> !id.equals(config.nodeId()) && !answered.containsKey(id));
			}
			if (pending && System.nanoTime() - deadlineNanos < 0) {
				worker.schedule(() -> compare(own, deadlineNanos, answered), COMPARE_EVERY.toMillis(), TimeUnit.MILLISECONDS);
				return;
			}
			report(own, answered, pending);
		});
	}

	private void report(Result own, Map<String, Result> answered, boolean incomplete) {
		var differing = new TreeMap<String, Result>();
		var same = new ArrayList<String>();
		synchronized (answered) {
			answered.forEach((id, result) -> {
				if (result.state() == State.DONE) {
					if (result.detail().equals(own.detail()) && result.keys() == own.keys()) {
						same.add(id);
					} else {
						differing.put(id, result);
					}
				}
			});
		}
		if (!differing.isEmpty()) {
			lastMismatchIndex = own.index();
			mismatches.record(1);
			LOG.error("{}: REPLICAS DIVERGED: checksum at log index {} is {} ({} keys) here, but {}. The replicas applied the"
							+ " same entries and hold different data: a merge operator that is not deterministic, a bug, or a damaged"
							+ " disk. Stop writing through the cluster, keep every data directory, and see \"Checksums\" in"
							+ " docs/distributed/runbooks.md", config.nodeId(), own.index(), own.detail(), own.keys(),
					differing.entrySet().stream().map(e -> e.getKey() + " has " + e.getValue().detail() + " ("
							+ e.getValue().keys() + " keys)").toList());
			return;
		}
		if (!same.isEmpty()) {
			lastMatchIndex = Math.max(lastMatchIndex, own.index());
			matches.record(1);
		}
		LOG.info("{}: checksum at {} matches on {}{}", config.nodeId(), own.index(), same.isEmpty() ? "no other member" : same,
				incomplete ? " (not every member answered: they compare by themselves once they have a result)" : "");
	}

	// Backups

	@Override
	public void backup(String name, long requestId, TermIndex position, NodeStore store) {
		long index = position.getIndex();
		Path dir = config.dataDir().resolve(BACKUPS_DIR).resolve(name);
		Result result;
		// Set once this attempt owns the directory: an existing backup of that name was refused before.
		boolean writing = false;
		try {
			Properties existing = Files.exists(dir.resolve(BACKUP_PROPERTIES)) ? readBackupProperties(dir) : null;
			if (existing != null && existing.getProperty("index") == null) {
				// No index: the file a power loss left empty after the rename, before it was synced (R26-2). The attempt
				// was interrupted: it is taken again below, as one without the file.
				LOG.warn("{}: backup {} at log index {}: {} holds no index, the trace of an interrupted attempt: taking it"
						+ " again", config.nodeId(), name, index, dir.resolve(BACKUP_PROPERTIES));
				existing = null;
			}
			if (existing != null) {
				if (Long.parseLong(existing.getProperty("index", "-1")) == index) {
					// The same entry applied again after a restart: the backup is there already.
					result = new Result(State.DONE, index, 0, dir.toString());
					record(name, result);
					return;
				}
				if (requestId != 0 && String.valueOf(requestId).equals(existing.getProperty("request"))) {
					// The same request proposed again, after its first answer was lost: that backup is this one.
					LOG.info("{}: backup {} at log index {} is the one written at index {} for the same request",
							config.nodeId(), name, index, existing.getProperty("index"));
					record(name, new Result(State.DONE, index, 0, dir.toString()));
					return;
				}
				throw new IOException("A backup named " + name + " exists already, at index " + existing.getProperty("index"));
			}
			long start = System.nanoTime();
			writing = true;
			// An interrupted attempt leaves a database without properties: start over.
			NodeStore.deleteRecursively(dir);
			Files.createDirectories(dir);
			var contained = store.checkpoint(dir.resolve(BACKUP_DB_DIR));
			var properties = new Properties();
			properties.setProperty("name", name);
			properties.setProperty("index", String.valueOf(index));
			properties.setProperty("request", String.valueOf(requestId));
			properties.setProperty("term", String.valueOf(position.getTerm()));
			properties.setProperty("applied", contained.getTerm() + ":" + contained.getIndex());
			properties.setProperty("cluster", String.valueOf(store.clusterId()));
			properties.setProperty("node", config.nodeId());
			properties.setProperty("taken", Instant.now().toString());
			var temporary = dir.resolve(BACKUP_PROPERTIES + ".tmp");
			try (Writer out = Files.newBufferedWriter(temporary, StandardCharsets.UTF_8)) {
				properties.store(out, "Rockserver cluster backup: every node took it at the same log index");
			}
			// Synced before the rename, which does not replace a file: otherwise a power loss can leave it empty (R26-2).
			NodeStore.syncFile(temporary);
			Files.move(temporary, dir.resolve(BACKUP_PROPERTIES));
			NodeStore.syncDirectory(dir);
			result = new Result(State.DONE, index, 0, dir.toString());
			LOG.info("{}: backup {} at log index {} written to {} in {} ms", config.nodeId(), name, index, dir,
					(System.nanoTime() - start) / 1_000_000);
		} catch (IOException | RuntimeException ex) {
			LOG.error("{}: backup {} at log index {} failed on this node: {}", config.nodeId(), name, index, ex.toString(), ex);
			result = new Result(State.FAILED, index, 0, String.valueOf(ex));
			if (writing) {
				// What this attempt wrote hard-links the live table and blob files of the moment: left behind (a complete
				// database without backup.properties, say), it would keep them on disk after compactions replace them,
				// until a backup of the same name runs again, which a date-named one never does (R29-1).
				try {
					NodeStore.deleteRecursively(dir);
				} catch (IOException | RuntimeException cleanupFailure) {
					LOG.error("{}: cannot delete {}, what the failed backup {} wrote: delete it by hand", config.nodeId(), dir,
							name, cleanupFailure);
				}
			}
		}
		record(name, result);
	}

	static Properties readBackupProperties(Path backupDir) throws IOException {
		var properties = new Properties();
		try (var in = Files.newBufferedReader(backupDir.resolve(BACKUP_PROPERTIES), StandardCharsets.UTF_8)) {
			properties.load(in);
		}
		return properties;
	}

	/**
	 * Prepares {@code dataDir}, which must hold no database and no Raft state, to seed a new cluster from the backup in
	 * {@code backupDir}: restores its database ({@link NodeStore#restore}) and deletes its replication metadata, so that
	 * the database belongs to no cluster. Every node of the new cluster is prepared from a copy of the same backup and
	 * started without {@code --join}: the cluster starts seeded, see "Migrating from a single node" in the design
	 * document. Client sessions and the results kept for them are gone with the metadata: clients open new sessions.
	 * The results file and the backups of the old cluster in {@code dataDir}, if any, are renamed with the suffix
	 * {@code .before-restore-<time>}: their results, by log index and by name, would otherwise answer for the new cluster,
	 * whose log starts again from 1.
	 *
	 * @throws ReplicatedNode.NewerVersionException if the backup's cluster version has capabilities this version lacks
	 *                                              (a newer cluster's backup). On this or any other failure,
	 *                                              {@code dataDir} is left without a database, so the command can run
	 *                                              again
	 */
	static TermIndex restore(Path backupDir, Path dataDir, @Nullable Path rockserverConfig) throws IOException {
		var properties = readBackupProperties(backupDir);
		Path db = dataDir.resolve("db");
		Path raft = dataDir.resolve("raft");
		if (Files.exists(db) || (Files.isDirectory(raft) && !isEmpty(raft))) {
			throw new IOException(dataDir + " holds a database or a Raft log: restore into an empty data directory");
		}
		Files.createDirectories(dataDir);
		TermIndex applied;
		try {
			// The seed digest of the backup's own files, before anything opens them: each restore opens its database and
			// leaves files of its own (OPTIONS, with this node's paths), so a digest of the restored db/ would differ on
			// every node restored from a copy of the same backup, and the followers would stop at genesis (R26-1).
			String seedDigest = NodeStore.databaseFilesDigest(backupDir.resolve(BACKUP_DB_DIR));
			applied = restoreDatabase(backupDir, db, rockserverConfig);
			NodeStore.recordRestoredSeedDigest(db, seedDigest);
			String suffix = ".before-restore-" + System.currentTimeMillis();
			for (Path old : List.of(dataDir.resolve(RESULTS_FILE), dataDir.resolve(BACKUPS_DIR))) {
				if (Files.exists(old)) {
					Path aside = old.resolveSibling(old.getFileName() + suffix);
					Files.move(old, aside);
					LOG.info("Moved {} of the old cluster to {}", old, aside);
				}
			}
		} catch (Throwable failure) {
			// Leaves the data directory without a database, so that the same command can run again (with the newer
			// binary after a version refusal, R18-1; after any other failure too, R19-3: the half-made db/ used to stay,
			// and the rerun refused the directory as holding a database). A replica-ops.log already moved aside to
			// .before-restore-<ts> when the move of backups/ fails stays moved: the rerun needs neither, and moves
			// backups/ aside itself.
			try {
				NodeStore.deleteRecursively(db);
				NodeStore.deleteSeedDigest(db);
			} catch (IOException | RuntimeException cleanup) {
				failure.addSuppressed(cleanup);
			}
			throw failure;
		}
		LOG.info("Restored backup {} (log index {} of cluster {}) into {}, holding the entries up to {}",
				properties.getProperty("name"), properties.getProperty("index"), properties.getProperty("cluster"), db, applied);
		return applied;
	}

	/** Copies the backup's database to {@code db} and drops its replication metadata; returns its applied entry. */
	private static TermIndex restoreDatabase(Path backupDir, Path db, @Nullable Path rockserverConfig)
			throws IOException {
		NodeStore.restore(backupDir.resolve(BACKUP_DB_DIR), db);
		TermIndex applied;
		long version;
		try (var store = NodeStore.open(db, rockserverConfig, "restore")) {
			applied = store.applied();
			// Read before the metadata, which holds it, is dropped: the new cluster starts at GENESIS and no start would
			// refuse data that a newer version wrote (R18-1).
			version = store.clusterVersion();
			if ((version & ~Capabilities.SUPPORTED) == 0) {
				store.db().deleteColumn(store.metaColumnId());
			}
		}
		long unknown = version & ~Capabilities.SUPPORTED;
		if (unknown != 0) {
			throw ReplicatedNode.NewerVersionException.of(version, unknown);
		}
		return applied;
	}

	private static boolean isEmpty(Path dir) throws IOException {
		try (var files = Files.list(dir)) {
			return files.findAny().isEmpty();
		}
	}

	// Results

	private void record(Result result) {
		checksums.put(result.index(), result);
		trim(checksums);
		if (result.state() != State.PENDING) {
			persist("checksum " + result.index() + " " + result.state() + " " + result.keys() + " " + oneLine(result.detail()));
		}
	}

	private void record(String name, Result result) {
		backups.put(result.index(), new BackupResult(name, result));
		trim(backups);
		persist("backup " + result.index() + " " + result.state() + " " + name + " " + oneLine(result.detail()));
	}

	private static String oneLine(String text) {
		return text.isEmpty() ? "-" : text.replace('\n', ' ').replace('\r', ' ');
	}

	/** Forgets the oldest result, by log index, beyond {@link #KEPT_RESULTS}. */
	private static void trim(Map<Long, ?> results) {
		if (results.size() > KEPT_RESULTS) {
			results.keySet().stream().min(Long::compare).ifPresent(results::remove);
		}
	}

	/** Appends one line to the results file, so that a restarted node still answers about its past results. */
	private synchronized void persist(String line) {
		try {
			Files.createDirectories(resultsFile.getParent());
			Files.writeString(resultsFile, Instant.now() + " " + line + "\n", StandardCharsets.UTF_8,
					StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		} catch (IOException ex) {
			LOG.warn("{}: failed to write {}: {}", config.nodeId(), resultsFile, ex.toString());
		}
	}

	private void loadResults() {
		if (!Files.isRegularFile(resultsFile)) {
			return;
		}
		try {
			for (String line : Files.readAllLines(resultsFile, StandardCharsets.UTF_8)) {
				var parts = line.split(" ", 6);
				if (parts.length < 6) {
					continue;
				}
				try {
					long index = Long.parseLong(parts[2]);
					var state = State.valueOf(parts[3]);
					switch (parts[1]) {
						case "checksum" -> {
							checksums.put(index, new Result(state, index, Long.parseLong(parts[4]), parts[5]));
							trim(checksums);
							try {
								// The earliest line of an index is its first apply: later ones come from replays.
								long millis = Instant.parse(parts[0]).toEpochMilli();
								long first = recordedChecksumMillis.merge(index, millis, Math::min);
								lastChecksumMillis = Math.max(lastChecksumMillis, first);
							} catch (DateTimeParseException malformed) {
								// The interval then counts from the start.
							}
						}
						case "backup" -> {
							backups.put(index, new BackupResult(parts[4], new Result(state, index, 0, parts[5])));
							trim(backups);
						}
						default -> {}
					}
				} catch (IllegalArgumentException malformed) {
					// A line cut by a crash.
				}
			}
		} catch (IOException ex) {
			LOG.warn("{}: failed to read {}: {}", config.nodeId(), resultsFile, ex.toString());
		}
	}

	/** This replica's result of the check at {@code index}. */
	Result checksumResult(long index) {
		var result = checksums.get(index);
		if (result != null) {
			return result;
		}
		return new Result(stateMachine.appliedIndex().get() >= index ? State.NONE : State.PENDING, index, 0, "");
	}

	/**
	 * This replica's result of the backup {@code name} proposed at log index {@code index}: never the result of an
	 * earlier backup of the same name.
	 */
	Result backupResult(String name, long index) {
		var result = backups.get(index);
		if (result == null) {
			return new Result(stateMachine.appliedIndex().get() >= index ? State.NONE : State.PENDING, index, 0, "");
		}
		if (!result.name().equals(name)) {
			return new Result(State.FAILED, index, 0, "The entry at log index " + index + " is the backup " + result.name()
					+ ", not " + name);
		}
		return result.result();
	}

	// The cluster version, on the leader

	/**
	 * On the leader: asks every member what it supports, every {@link #VERSION_CHECK_EVERY} and at each new term, and
	 * proposes a higher cluster version when all of them answered with more. Also proposes the periodic checksums.
	 * Initialization thread only.
	 */
	void onLeader(RaftServer.Division division, RaftGroupId groupId) {
		var info = division.getInfo();
		if (!info.isLeader() || !info.isLeaderReady()) {
			return;
		}
		long now = System.nanoTime();
		long term = info.getCurrentTerm();
		proposePeriodicChecksum(System.currentTimeMillis());
		long supported = supported();
		if ((supported & ~clusterVersion) == 0 || versionCheckRunning
				|| (term == versionCheckTerm && now - nextVersionCheckNanos < 0)) {
			return;
		}
		versionCheckTerm = term;
		nextVersionCheckNanos = now + VERSION_CHECK_EVERY.toNanos();
		versionCheckRunning = true;
		var answers = new ArrayList<CompletableFuture<Long>>();
		for (RaftPeer peer : pollPeers(division).values()) {
			if (peer.getId().toString().equals(config.nodeId())) {
				answers.add(CompletableFuture.completedFuture(supported));
			} else {
				answers.add(ask(peer, OP_CAPABILITIES, new byte[0]).thenApply(reply -> ByteBuffer.wrap(reply).getLong()));
			}
		}
		// Ends on whichever thread answers last, without blocking it: versionCheckRunning is volatile for that.
		CompletableFuture.allOf(answers.toArray(CompletableFuture[]::new)).thenCompose(_ -> {
			long common = supported;
			for (var answer : answers) {
				common &= answer.join();
			}
			long current = clusterVersion;
			var proposer = this.proposer;
			if ((common & ~current) == 0 || proposer == null) {
				return CompletableFuture.<Object>completedFuture(null);
			}
			LOG.info("{}: every member supports {}: raising the cluster version", config.nodeId(),
					Capabilities.describe(common | current));
			return proposer.propose(new LogEntry.ClusterVersion(common | current))
					.orTimeout(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
		}).whenComplete((_, error) -> {
			if (error != null) {
				LOG.debug("{}: the cluster version stays {} for now: {}", config.nodeId(),
						Capabilities.describe(clusterVersion), error.toString());
			}
			versionCheckRunning = false;
		});
	}

	/**
	 * The peers the leader asks for their capabilities: the members of its configuration, old and new while it changes,
	 * and every peer it sends the log to, which includes a peer that {@code add-peer} is bringing in and that is still
	 * catching up (Ratis' staging peers, not yet in the configuration). Such a peer receives every entry of the log, a
	 * {@link LogEntry.ClusterVersion} among them, so it must support what that entry turns on.
	 */
	static Map<String, RaftPeer> pollPeers(RaftServer.Division division) {
		var peers = new TreeMap<String, RaftPeer>();
		for (RaftPeer peer : division.getRaftConf().getAllPeers()) {
			peers.put(peer.getId().toString(), peer);
		}
		for (var follower : division.getInfo().getRoleInfoProto().getLeaderInfo().getFollowerInfoList()) {
			var id = follower.getId();
			var name = RaftPeerId.valueOf(id.getId()).toString();
			if (!peers.containsKey(name) && !id.getAddress().isEmpty()) {
				peers.put(name, RaftPeer.newBuilder().setId(name).setAddress(id.getAddress()).build());
			}
		}
		return peers;
	}

	/**
	 * Proposes a checksum once {@link #checksumEvery} passed since the last one this replica applied (the replicated
	 * history, so a new leader goes on with the schedule of the old one), or since it proposed one itself (applied
	 * later), or since the start of its schedule ({@link #scheduleStartMillis}) if it knows of neither. Wall clock: it
	 * outlives a restart through the results file and the schedule file. A proposal that fails is tried again after
	 * {@link #PROPOSAL_RETRY} if its error says it was not applied, a full interval later otherwise.
	 */
	private void proposePeriodicChecksum(long nowMillis) {
		if (!periodicChecksumDue(nowMillis)) {
			return;
		}
		var proposer = this.proposer;
		if (proposer != null) {
			proposer.propose(new LogEntry.Checksum()).whenComplete((index, error) -> {
				if (error != null && periodicChecksumFailed(nowMillis, error)) {
					LOG.warn("{}: periodic checksum not proposed, trying again in {} s", config.nodeId(),
							Math.min(checksumEvery.toMillis(), PROPOSAL_RETRY.toMillis()) / 1000, error);
				} else if (error != null) {
					LOG.warn("{}: periodic checksum proposal ended with an unknown outcome, next try in one interval",
							config.nodeId(), error);
				} else {
					LOG.info("{}: periodic checksum at log index {}", config.nodeId(), index);
				}
			});
		}
	}

	/** Whether a periodic checksum is due at {@code nowMillis}; if it is, records it as proposed then. */
	boolean periodicChecksumDue(long nowMillis) {
		var every = checksumEvery;
		if (every.isZero() || every.isNegative() || !clusterHas(Capabilities.CHECKSUM)) {
			return false;
		}
		if (scheduleStartMillis == 0) {
			scheduleStartMillis = loadScheduleStart(nowMillis);
		}
		long last = Math.max(lastChecksumMillis, proposedChecksumMillis);
		if (nowMillis - (last != 0 ? last : scheduleStartMillis) < every.toMillis()) {
			return false;
		}
		proposedChecksumMillis = nowMillis;
		return true;
	}

	/**
	 * The periodic checksum proposed at {@code proposedAtMillis} failed with {@code error}. If the error says that the
	 * entry was not applied ({@link RocksDBException#isNotApplied()}: no leader accepted it in time, a full queue), the
	 * next try comes {@link #PROPOSAL_RETRY} later, unless a newer proposal happened meanwhile, and this returns true.
	 * Any other error (a timeout after it was sent, a lost leadership after it was appended: {@code OUTCOME_UNKNOWN})
	 * leaves the next try a full interval after the proposal and returns false: the entry may still commit and apply
	 * later than the retry would come, and a second full-database digest so soon is the load the schedule avoids.
	 */
	boolean periodicChecksumFailed(long proposedAtMillis, Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		if (!(cause instanceof RocksDBException rocks) || !rocks.isNotApplied()) {
			return false;
		}
		if (proposedChecksumMillis == proposedAtMillis) {
			long every = checksumEvery.toMillis();
			proposedChecksumMillis = Math.max(1, proposedAtMillis - every + Math.min(every, PROPOSAL_RETRY.toMillis()));
		}
		return true;
	}

	// The Raft port

	private CompletableFuture<byte[]> ask(RaftPeer peer, byte op, byte[] payload) {
		var groupId = stateMachine.groupId();
		return TermService.ops(channels.get(peer.getAddress()), groupId, request(op, payload), PEER_TIMEOUT);
	}

	static byte[] request(byte op, byte[] payload) {
		return ByteBuffer.allocate(1 + payload.length).put(op).put(payload).array();
	}

	/** The payload of {@link #OP_PROPOSE_BACKUP} (a request id) or {@link #OP_BACKUP_RESULT} (a log index). */
	static byte[] backupPayload(long number, String name) {
		var bytes = name.getBytes(StandardCharsets.UTF_8);
		return ByteBuffer.allocate(Long.BYTES + bytes.length).putLong(number).put(bytes).array();
	}

	/** Answers a {@link TermService#OPS} call: {@code request} is the operation byte, then its payload. */
	CompletableFuture<byte[]> answer(byte[] request) {
		if (request.length == 0) {
			return failed(Status.INVALID_ARGUMENT, "Empty request");
		}
		var payload = ByteBuffer.wrap(request, 1, request.length - 1).slice();
		if ((request[0] == OP_CHECKSUM_RESULT || request[0] == OP_BACKUP_RESULT || request[0] == OP_PROPOSE_BACKUP)
				&& payload.remaining() < Long.BYTES) {
			return failed(Status.INVALID_ARGUMENT, "Operation " + request[0] + " needs a number first");
		}
		switch (request[0]) {
			case OP_CAPABILITIES -> {
				return CompletableFuture.completedFuture(ByteBuffer.allocate(2 * Long.BYTES)
						.putLong(supported()).putLong(clusterVersion).array());
			}
			case OP_CHECKSUM_RESULT -> {
				return CompletableFuture.completedFuture(encodeResult(checksumResult(payload.getLong())));
			}
			case OP_BACKUP_RESULT -> {
				long index = payload.getLong();
				var name = StandardCharsets.UTF_8.decode(payload).toString();
				return CompletableFuture.completedFuture(encodeResult(backupResult(name, index)));
			}
			case OP_PROPOSE_CHECKSUM -> {
				return propose(Capabilities.CHECKSUM, new LogEntry.Checksum());
			}
			case OP_PROPOSE_BACKUP -> {
				long requestId = payload.getLong();
				var name = StandardCharsets.UTF_8.decode(payload).toString();
				if (!validBackupName(name)) {
					return failed(Status.INVALID_ARGUMENT, "Invalid backup name " + name + ": 1 to 64 letters, digits, '.', '_'"
							+ " or '-', starting with a letter or a digit");
				}
				return propose(Capabilities.BACKUP, new LogEntry.Backup(name, requestId));
			}
			default -> {
				return failed(Status.UNIMPLEMENTED, "Unknown operation " + request[0]);
			}
		}
	}

	/**
	 * How a refusal of {@link #OP_PROPOSE_CHECKSUM} or {@link #OP_PROPOSE_BACKUP} for the cluster version begins, in
	 * this version and in the older ones ("... yet: the cluster version is X. Upgrade every member first"): see
	 * {@link #versionRefusal}.
	 */
	static final String VERSION_REFUSAL = "Not every member supports ";

	/**
	 * Whether {@code status} is a member's refusal of a proposal because the cluster version it has applied lacks the
	 * operation (R32-4). That is the member's own view: a member that lags behind the leader has not applied the
	 * latest cluster version yet. Nothing was proposed, so {@code admin} asks the next member.
	 */
	static boolean versionRefusal(Status status) {
		return status.getCode() == Status.Code.FAILED_PRECONDITION && status.getDescription() != null
				&& status.getDescription().startsWith(VERSION_REFUSAL);
	}

	private CompletableFuture<byte[]> propose(long capability, LogEntry entry) {
		var proposer = this.proposer;
		if (proposer == null) {
			return failed(Status.UNAVAILABLE, "The node is starting");
		}
		if (!clusterHas(capability)) {
			// Before anything is proposed: see versionRefusal.
			return failed(Status.FAILED_PRECONDITION, VERSION_REFUSAL + Capabilities.describe(capability)
					+ " yet as far as " + config.nodeId() + " knows: it has applied cluster version "
					+ Capabilities.describe(clusterVersion) + ", and a member that lags behind the leader has not applied"
					+ " the latest one yet");
		}
		return proposer.propose(entry).thenApply(index -> ByteBuffer.allocate(Long.BYTES).putLong((Long) index).array());
	}

	private static CompletableFuture<byte[]> failed(Status status, String message) {
		return CompletableFuture.failedFuture(status.withDescription(message).asRuntimeException());
	}

	static byte[] encodeResult(Result result) {
		var detail = result.detail().getBytes(StandardCharsets.UTF_8);
		return ByteBuffer.allocate(1 + 2 * Long.BYTES + detail.length)
				.put((byte) result.state().ordinal()).putLong(result.index()).putLong(result.keys()).put(detail).array();
	}

	static Result decodeResult(byte[] value) {
		var buffer = ByteBuffer.wrap(value);
		var state = State.values()[buffer.get()];
		long index = buffer.getLong();
		long keys = buffer.getLong();
		return new Result(state, index, keys, StandardCharsets.UTF_8.decode(buffer).toString());
	}

	/**
	 * Asks the node behind {@code address} (its Raft address) to run {@code op}: for {@code admin}, a process that makes
	 * one call per node, so on a channel of its own.
	 */
	static CompletableFuture<byte[]> call(String address, RaftGroupId groupId, byte op, byte[] payload, Duration timeout) {
		var channel = PeerChannels.open(address);
		return TermService.ops(channel, groupId, request(op, payload), timeout).whenComplete((_, _) -> channel.shutdown());
	}

	@Override
	public void close() {
		worker.shutdownNow();
		if (ownChannels) {
			channels.close();
		}
	}

	/** The log index of the last check that found a divergence, or -1: the gauge {@code checksum.last_mismatch_index}. */
	long lastMismatchIndex() {
		return lastMismatchIndex;
	}

	/** Tests only. */
	List<Result> checksumResults() {
		return checksums.values().stream().sorted((a, b) -> Long.compare(a.index(), b.index())).toList();
	}

	/** The backups directory of a data directory. */
	static Path backupsDir(Path dataDir) {
		return dataDir.resolve(BACKUPS_DIR);
	}
}
