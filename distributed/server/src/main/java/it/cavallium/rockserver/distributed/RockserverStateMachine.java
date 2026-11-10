package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.ClientRequestId;
import java.io.IOException;
import it.unimi.dsi.fastutil.ints.IntList;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteOutput;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.apache.ratis.util.LifeCycle;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis state machine over the node's {@link NodeStore}.
 *
 * <p>Leader-side admission happens in {@link #startTransaction} (decode, reject stale transactions) and
 * {@link #preAppendTransaction} (conflict validation and tracking, in log order under Ratis' append lock).
 * Every replica then applies entries in log order on an apply thread of its own, in groups (see
 * {@link #applyTransaction}).</p>
 */
final class RockserverStateMachine extends BaseStateMachine {

	private static final Logger LOG = LoggerFactory.getLogger(RockserverStateMachine.class);

	/**
	 * Transaction commits of a log entry were rejected because one of their keys changed after their snapshot. The
	 * message lists their positions in the entry ({@link ConflictPositions}); the entry was not appended. Ratis rebuilds
	 * it on the proposer from its class name and this constructor.
	 */
	public static final class ConflictException extends IOException {

		public ConflictException(String message) {
			super(message);
		}

		static ConflictException of(IntList positions) {
			return new ConflictException(ConflictPositions.format(positions));
		}
	}

	/** The transaction was opened by another leadership: the client must start it over. */
	public static final class RetryableException extends IOException {
		public RetryableException(String message) {
			super(message);
		}
	}

	/**
	 * The entry was refused before being appended because this node cannot admit it yet (cluster not
	 * initialized, leadership changing): resending the same entry is safe.
	 */
	public static final class NotReadyException extends IOException {
		public NotReadyException(String message) {
			super(message);
		}
	}

	/** State attached to a log entry on the leader between admission and apply. */
	private static final class Proposal {
		final List<Request> requests;
		/** The leader's clock stamped on the entry ({@link LogEntryCodec#stamp}), -1 if it has none. */
		long leaderTimeMillis = -1;
		final long startedNanos = System.nanoTime();
		long appendedNanos;
		@Nullable LeaderSession session;
		ConflictTracker.@Nullable Token token;
		/** Commits of this entry that handed their key over, see {@link LeaderSession#admitted}. */
		@Nullable List<LeaderSession.ChainLink> links;

		Proposal(List<Request> requests) {
			this.requests = requests;
		}

		List<LogEntry> entries() {
			return requests.stream().map(Request::entry).toList();
		}
	}

	/**
	 * A batch that this node's proposer encoded and submitted to its own Raft server, with its requests as
	 * {@link LogEntryCodec#encodeBatch} returned them: {@link #startTransaction} takes them instead of decoding the
	 * batch. Keyed by the Ratis invocation, which is unique to one batch of one proposer; the proposer withdraws it
	 * once Ratis answers, so that one Ratis answers without starting a transaction for (a refusal, a retry-cache hit)
	 * does not stay behind.
	 */
	final class LocalProposal {
		private final LocalKey key;
		private final ByteString content;
		private final List<Request> requests;

		private LocalProposal(LocalKey key, ByteString content, List<Request> requests) {
			this.key = key;
			this.content = content;
			this.requests = requests;
		}

		void withdraw() {
			localProposals.remove(key, this);
		}
	}

	private record LocalKey(ClientId clientId, long callId) {}

	private final ConcurrentHashMap<LocalKey, LocalProposal> localProposals =
			new ConcurrentHashMap<>();
	/** Transactions started from a {@link LocalProposal}, without decoding; for tests. */
	private final LongAdder localProposalsTaken = new LongAdder();

	private final NodeConfig config;
	private final CheckpointStorage storage = new CheckpointStorage();
	private final Consumer<Throwable> onReplicaFailure;
	private final AppliedIndex applied = new AppliedIndex(-1);
	private volatile RaftServer server;
	private volatile RaftGroupId groupId;
	private volatile NodeStore store;
	/**
	 * The highest term of a log entry whose effects may be visible in the local database: raised before the apply thread
	 * commits an entry's writes, and before a restored database becomes {@link #store}, while the applied index follows
	 * them. A follower read checks it once it has read ({@link Router}).
	 */
	private volatile long visibleTerm = -1;
	/**
	 * Raised before a snapshot install closes {@link #store}, and again once the restored one replaced it: odd while
	 * the database is being replaced. Written by the state machine updater thread only.
	 */
	private volatile long storeGeneration;
	private volatile Applier applier;
	/** This node's streams by column, which a column deletion ends: one for the node, through every database it opens. */
	private final ColumnStreams columnStreams = new ColumnStreams();
	/** The clock this node stamps on session entries while it leads, see {@link LeaderClock}. */
	private final LeaderClock leaderClock = new LeaderClock();
	private volatile @Nullable LeaderSession leaderSession;
	private volatile boolean failed;

	/** Entries waiting for the apply thread, in log order; see {@link #applyTransaction}. */
	private final LinkedBlockingQueue<Applying> applyQueue = new LinkedBlockingQueue<>();
	/**
	 * KiB of queued entries: Ratis' thread waits in {@link #applyTransaction} while the apply thread is this far
	 * behind, so that a lagging replica keeps its backlog in the Raft log on disk, not decoded on the heap.
	 */
	private final Semaphore queuedKiB = new Semaphore(MAX_QUEUED_KIB);
	private static final int MAX_QUEUED_KIB = 256 << 10;

	/**
	 * The log index up to which this node must have applied before it serves as the leader of {@code term}: the
	 * entries of earlier leaders. See {@link #notifyLeaderReady}.
	 */
	private record Readiness(long term, long index) {}

	private volatile Readiness readiness = new Readiness(-1, -1);
	/** The highest log index handed to the apply thread (data entries and positions alike). */
	private volatile long handedOver = -1;
	private final Thread applyThread;
	/**
	 * Runs what follows the commit of each group, in log order: replies (Ratis replies to clients and advances its
	 * applied index inline when a future completes), leader bookkeeping and the applied index, so that the apply
	 * thread can go on with the next group meanwhile.
	 */
	private final ExecutorService completer;
	/** At most this many entries, and about this many bytes of entries, are applied in one local transaction. */
	private static final int MAX_GROUP_ENTRIES = 256;
	private static final long MAX_GROUP_BYTES = 16L << 20;

	/** Work for the apply thread. */
	private sealed interface Applying {}

	/** A data entry to apply; {@code reply} completes once it is committed. It holds {@code kib} of {@link #queuedKiB}. */
	private record ApplyEntry(TermIndex position, List<Request> requests, long leaderTimeMillis,
			@Nullable Proposal proposal, long bytes, int kib, CompletableFuture<Message> reply) implements Applying {}

	/** A position without data (configuration entries, leader no-ops) to record. */
	/** A position without data; {@code configuration}: the committed configuration entry at it, if it is one. */
	private record MarkPosition(TermIndex position, RemovalRule.@Nullable Conf configuration) implements Applying {}

	/** Completes {@code done} once everything queued before it is applied. */
	private record Drain(CompletableFuture<Void> done) implements Applying {}

	/** Stops the apply thread. */
	private record Stop() implements Applying {}

	RockserverStateMachine(NodeConfig config, Consumer<Throwable> onReplicaFailure, NodeMetrics metrics) {
		this.config = config;
		this.metrics = metrics;
		this.onReplicaFailure = onReplicaFailure;
		this.applyTime = metrics.duration("apply");
		this.applyRequests = metrics.amount("apply.requests");
		this.admitTime = metrics.duration("raft.admit");
		this.commitWait = metrics.duration("raft.appendToApply");
		this.snapshotTime = metrics.duration("snapshot");
		metrics.gauge("leader.trackedKeys", () -> {
			var session = leaderSession;
			return session == null ? -1 : session.tracker().size();
		});
		metrics.gauge("leader.openTransactions", () -> {
			var session = leaderSession;
			return session == null ? -1 : session.openTransactions();
		});
		metrics.gauge("leader.lockedKeys", () -> {
			var session = leaderSession;
			return session == null ? -1 : session.lockedKeys();
		});
		metrics.gauge("leader.chainLinks", () -> {
			var session = leaderSession;
			return session == null ? -1 : session.chainLinks();
		});
		metrics.gauge("heapUsedMiB", () -> (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) >> 20);
		this.applyGroupEntries = metrics.amount("apply.groupEntries");
		this.completer = Executors.newSingleThreadExecutor(
				Thread.ofPlatform().name(config.nodeId() + "-applied").daemon().factory());
		this.applyLanes = config.parallelApplyThreads() >= 2
				? new ApplyLanes(config.nodeId(), config.parallelApplyThreads()) : null;
		this.applyThread = Thread.ofPlatform().name(config.nodeId() + "-apply").daemon().unstarted(this::applyLoop);
		this.ops = new ReplicaOps(config, this, metrics);
		applyThread.start();
	}

	/** Cluster version, checksums and backups: see {@link ReplicaOps}. */
	private final ReplicaOps ops;

	/** This node's streams by column: see {@link ColumnStreams}. */
	ColumnStreams columnStreams() {
		return columnStreams;
	}

	ReplicaOps ops() {
		return ops;
	}

	RaftGroupId groupId() {
		return groupId;
	}

	/**
	 * Deletes the partial copies of failed snapshot installations, see {@link CheckpointStorage#discardFailedInstalls}:
	 * disk I/O under the install lock, so the install cleanup thread of {@link ReplicatedNode} only.
	 */
	void discardFailedInstalls() {
		storage.discardFailedInstalls();
	}

	/** When the latest snapshot was taken or installed, from its files; 0 if unknown. */
	long latestSnapshotTimeMillis() {
		var dir = storage.latestSnapshotDir();
		if (dir == null) {
			return 0;
		}
		try {
			return Files.getLastModifiedTime(dir).toMillis();
		} catch (IOException | RuntimeException ex) {
			return 0;
		}
	}

	/** Threads of the partitioned apply ({@link NodeConfig#parallelApplyThreads()}), or null for the serial apply. */
	private final @Nullable ApplyLanes applyLanes;

	private final NodeMetrics.Series applyGroupEntries;

	private final NodeMetrics.Series snapshotTime;
	private final NodeMetrics metrics;

	private final NodeMetrics.Series admitTime;
	private final NodeMetrics.Series commitWait;

	private final NodeMetrics.Series applyTime;
	private final NodeMetrics.Series applyRequests;

	@Override
	public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
		super.initialize(server, groupId, raftStorage);
		this.server = server;
		this.groupId = groupId;
		storage.init(raftStorage);
		getLifeCycle().startAndTransition(() -> {
			openStore();
			requireKnownClusterVersion();
		});
		seedBytesSinceSnapshot(raftStorage.getStorageDir().getCurrentDir().toPath());
	}

	/**
	 * Starts the count of {@link #snapshotByBytes} from the Raft log that this database applied past the latest
	 * snapshot, instead of 0: otherwise restarts more frequent than {@code snapshotEveryBytes} of writes (a few deploys
	 * a day) would leave only Ratis' entry-count trigger. Entries past the database's applied position are counted
	 * again when Ratis applies them after the start.
	 */
	private void seedBytesSinceSnapshot(Path raftLogDir) {
		var snapshot = storage.getLatestSnapshot();
		long from = snapshot != null ? snapshot.getIndex() : -1;
		long through = store.applied().getIndex();
		long seed = logBytesBetween(raftLogDir, from, through);
		seededThrough = through;
		bytesSinceSnapshot.set(seed);
		if (seed > 0) {
			LOG.info("{}: {} bytes of Raft log applied past the snapshot at {}", config.nodeId(), seed, from);
		}
	}

	private static final Pattern CLOSED_SEGMENT = Pattern.compile("log_(\\d+)-(\\d+)");
	private static final Pattern OPEN_SEGMENT = Pattern.compile("log_inprogress_(\\d+)");

	/**
	 * The bytes of the Raft log segments in {@code dir} that hold the entries after {@code fromExclusive} up to
	 * {@code toInclusive}. A closed segment that holds both entries in and out of the range counts in proportion to
	 * its entries in it; the open segment, whose end is unknown here, counts from its start to {@code toInclusive} as
	 * if that were its end. Files that vanish meanwhile count 0.
	 */
	static long logBytesBetween(Path dir, long fromExclusive, long toInclusive) {
		if (toInclusive <= fromExclusive) {
			return 0;
		}
		long total = 0;
		try (var files = Files.list(dir)) {
			for (var file : (Iterable<Path>) files::iterator) {
				var name = file.getFileName().toString();
				long start;
				long end;
				var closed = CLOSED_SEGMENT.matcher(name);
				var open = OPEN_SEGMENT.matcher(name);
				if (closed.matches()) {
					start = Long.parseLong(closed.group(1));
					end = Long.parseLong(closed.group(2));
				} else if (open.matches()) {
					start = Long.parseLong(open.group(1));
					end = Math.max(start, toInclusive);
				} else {
					continue;
				}
				long first = Math.max(start, fromExclusive + 1);
				long last = Math.min(end, toInclusive);
				if (last < first) {
					continue;
				}
				long size;
				try {
					size = Files.size(file);
				} catch (IOException vanished) {
					continue;
				}
				total += (long) ((double) size * (last - first + 1) / (end - start + 1));
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("Could not measure the Raft log in {}", dir, ex);
		}
		return total;
	}

	/** Tests only: see {@link #bytesSinceSnapshot}. */
	long bytesSinceSnapshotForTesting() {
		return bytesSinceSnapshot.get();
	}

	/**
	 * Refuses a database whose cluster version has a capability this version of the node lacks: a downgrade below the
	 * cluster version, or a backup of a newer cluster restored here. The log may hold entries of that capability, which
	 * this node cannot read, and a capability check would never ask it again (the version never goes down). At the
	 * start, where the refused database is closed again; the same check runs on an installed snapshot (a member rolled
	 * back after it answered the leader's poll, then installing the leader's database) and on the apply of a raise
	 * ({@link Applier}).
	 */
	private void requireKnownClusterVersion() throws IOException {
		var opened = store;
		var newer = newerVersion(opened);
		if (newer != null) {
			store = null;
			opened.close();
			throw newer;
		}
	}

	private ReplicatedNode.@Nullable NewerVersionException newerVersion(NodeStore opened) {
		long version = opened.clusterVersion();
		long unknown = version & ~ops.supported();
		if (unknown == 0) {
			return null;
		}
		return new ReplicatedNode.NewerVersionException("Node " + config.nodeId() + " has a database of cluster version "
				+ Capabilities.describe(version) + ", with capabilities this version lacks (" + Capabilities.describe(unknown)
				+ "): it was written by a newer version. Run the newer version, see \"Upgrades\" in docs/distributed/runbooks.md");
	}

	/**
	 * Called by Ratis once a snapshot from the leader is fully received, before {@link #reinitialize}: Ratis reloads
	 * the state machine only if it is {@code PAUSED}, and otherwise stops this replica's Raft server for good. The
	 * state machine updater thread then runs {@link #reinitialize} instead of applying entries, which makes it
	 * {@code RUNNING} again. Until then {@link #takeSnapshot} takes none (R53-1); a take already running is not waited
	 * for (see there).
	 */
	@Override
	public synchronized void pause() {
		var lifeCycle = getLifeCycle();
		if (lifeCycle.getCurrentState() == LifeCycle.State.RUNNING) {
			lifeCycle.transition(LifeCycle.State.PAUSING);
			lifeCycle.transition(LifeCycle.State.PAUSED);
		}
	}

	/**
	 * Opens the local database, first restoring the snapshot if it is ahead of the database
	 * ({@link NodeStore#restoreSnapshotIfAhead}, which {@code export-seed} runs too).
	 */
	private void openStore() throws IOException {
		var opened = NodeStore.restoreSnapshotIfAhead(
				NodeStore.open(config.dbDir(), config.rockserverConfig(), config.nodeId()), storage,
				config.rockserverConfig(), config.nodeId());
		var position = opened.applied();
		raiseVisibleTerm(position.getTerm());
		this.configurations = new CommittedConfigurations(opened.configurations(), opened.configurationHistoryFrom());
		// The version first: a database with a cluster id next to the version of the one it replaces (a node that had
		// not applied genesis, installing a snapshot) would advertise no client sessions (R16b-1).
		ops.opened(opened);
		this.store = opened;
		this.applier = new Applier(opened, metrics, applyLanes);
		applier.maintenance(ops);
		applier.columnStreams(columnStreams);
		metrics.bind(opened.db().getMetricsRegistry(), opened.metricsNodeName(config.nodeId()));
		setLastAppliedTermIndex(position);
		applied.advance(Math.max(applied.get(), position.getIndex()));
		LOG.info("{}: database open, applied position {}", config.nodeId(), position);
	}

	@Override
	public StateMachineStorage getStateMachineStorage() {
		return storage;
	}

	NodeStore store() {
		return store;
	}

	AppliedIndex appliedIndex() {
		return applied;
	}

	/** See {@link #visibleTerm}. Any thread. */
	long visibleTerm() {
		return visibleTerm;
	}

	/** Tests only: the applier of the database open now. */
	Applier applierForTesting() {
		return applier;
	}

	/** See {@link #storeGeneration}. Any thread. */
	long storeGeneration() {
		return storeGeneration;
	}

	private void raiseVisibleTerm(long term) {
		if (term > visibleTerm) {
			visibleTerm = term;
		}
	}

	/** Tests only: as if an entry of {@code term} had been applied. */
	void raiseVisibleTermForTesting(long term) {
		raiseVisibleTerm(term);
	}

	/** The result this replica recorded for request {@code sequence} of {@code proposerId}, or null. Any thread. */
	@Nullable ApplyResult recordedResult(long proposerId, long sequence) {
		return Applier.recordedResult(store, proposerId, sequence);
	}

	/** The result this replica recorded for write {@code id} of a client session, or null. Any thread. */
	@Nullable ApplyResult recordedResult(ClientRequestId id) {
		return Applier.recordedResult(store, id);
	}

	boolean isFailed() {
		return failed;
	}

	/**
	 * The leader session of the current term, if this node leads it; created on first use. The session of an earlier
	 * term is retired outside the monitor, as in {@link #notifyLeaderChanged}: {@link #preAppendTransaction} waits for
	 * this monitor under Ratis' append lock, and the rollbacks of its native transactions run on a thread of their own.
	 */
	@Nullable LeaderSession leaderSession(long term) {
		LeaderSession replaced;
		LeaderSession created;
		synchronized (this) {
			var current = leaderSession;
			if (current != null && current.term() == term) {
				return current;
			}
			var division = division();
			if (division == null || !division.getInfo().isLeader() || division.getInfo().getCurrentTerm() != term) {
				return null;
			}
			replaced = current;
			// Only once every member can read the entries of a pipelined leader (see Capabilities). A cluster version
			// raised during the term takes effect at the next term. Before genesis this leader creates the cluster, whose
			// genesis gives it Capabilities.GENESIS. Both the version and the cluster id are in memory: no disk read under
			// this monitor.
			boolean pipelined = config.pipelinedUpdates() && (ops.clusterHas(Capabilities.PIPELINED_UPDATES)
					|| (store.clusterId() == null && Capabilities.has(Capabilities.GENESIS, Capabilities.PIPELINED_UPDATES)));
			if (config.pipelinedUpdates() && !pipelined) {
				LOG.warn("{}: --pipelined-updates is on, but not every member supports it yet (cluster version {}): term {}"
						+ " runs without it", config.nodeId(), Capabilities.describe(ops.clusterVersion()), term);
			}
			created = new LeaderSession(term, applied, new ConflictTracker(), pipelined, metrics);
			// The session tracks writes from this term on: a transaction whose snapshot misses an earlier leader's
			// entries would not see them conflict. Reads wait for those entries (see leaderReadyIndex); this makes
			// sure no commit validates against such a snapshot anyway.
			created.tracker().prune(leaderReadyIndex(term));
			leaderSession = created;
		}
		if (replaced != null) {
			retire(replaced);
		}
		LOG.info("{}: leading term {}", config.nodeId(), term);
		return created;
	}

	@Nullable RaftServer.Division division() {
		var server = this.server;
		if (server == null) {
			return null;
		}
		try {
			return server.getDivision(groupId);
		} catch (IOException e) {
			return null;
		}
	}

	/**
	 * Offers the requests of a batch this node is about to submit to its own Raft server as invocation
	 * {@code callId} of {@code clientId}, with {@code content} as its message: {@link #startTransaction} takes them
	 * instead of decoding the batch, if the request it gets carries that very content. {@code requests} must be what
	 * decoding {@code content} in place gives (see {@link LogEntryCodec#encodeBatch}). The caller withdraws the offer
	 * once Ratis answers.
	 */
	LocalProposal offerLocal(ClientId clientId, long callId, ByteString content, List<Request> requests) {
		var offered = new LocalProposal(new LocalKey(clientId, callId), content, requests);
		localProposals.put(offered.key, offered);
		return offered;
	}

	/** The requests offered for {@code request} ({@link #offerLocal}), taken; null if none, or not for its content. */
	private @Nullable List<Request> takeLocal(RaftClientRequest request, ByteString content) {
		if (localProposals.isEmpty()) {
			return null;
		}
		var offered = localProposals.remove(new LocalKey(request.getClientId(), request.getCallId()));
		// The very bytes the requests were encoded into: another request with the same invocation, if one could
		// ever come, is decoded.
		if (offered == null || offered.content != content) {
			return null;
		}
		localProposalsTaken.increment();
		return offered.requests;
	}

	/** Offers not taken nor withdrawn yet, for tests. */
	int localProposalsOffered() {
		return localProposals.size();
	}

	/** Transactions started from an offer, without decoding, for tests. */
	long localProposalsTaken() {
		return localProposalsTaken.sum();
	}

	@Override
	public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
		var builder = TransactionContext.newBuilder()
				.setStateMachine(this)
				.setClientRequest(request);
		try {
			var content = request.getMessage().getContent();
			if (!content.isEmpty() && content.byteAt(0) == LogEntryCodec.STAMPED) {
				// Only the leader stamps, below: a stamped proposal would make an entry that followers cannot decode.
				throw new IllegalArgumentException("A proposal must not carry a leader stamp");
			}
			// The proposer of this node offers the requests it encoded (see offerLocal); any other batch is decoded:
			// from other nodes, resends after the offer was taken, offers withdrawn.
			var offered = takeLocal(request, content);
			var decoded = offered != null ? offered : decodeBatch(content);
			if (decoded.size() > 1 && !decoded.stream().allMatch(r -> Applier.isBatchable(r.entry()))) {
				throw new IllegalArgumentException("Only data writes can share a log entry");
			}
			if (!(decoded.getFirst().entry() instanceof LogEntry.Genesis) && store.clusterId() == null) {
				throw new NotReadyException("The cluster is not initialized yet");
			}
			long term = -2;
			for (var decodedRequest : decoded) {
				if (decodedRequest.entry() instanceof LogEntry.Guarded guarded) {
					if (term == -2) {
						var division = division();
						term = division != null ? division.getInfo().getCurrentTerm() : -1;
					}
					if (guarded.leaderEpoch() != term) {
						throw new RetryableException("The transaction was opened by another leader");
					}
				}
			}
			var proposal = new Proposal(decoded);
			var logData = request.getMessage().getContent();
			if (LogEntryCodec.needsStamp(decoded)) {
				// Session expiry reads the leader's clock, the same on every replica. Prepended: the batch is not copied.
				var division = division();
				var applier = this.applier;
				proposal.leaderTimeMillis = leaderClock.stamp(division != null ? division.getInfo().getCurrentTerm() : -1,
						applier != null ? applier.clock() : 0, leaderClockMillis(), System.nanoTime());
				logData = UnsafeByteOperations.unsafeWrap(LogEntryCodec.stamp(proposal.leaderTimeMillis)).concat(logData);
			}
			return builder.setLogData(logData)
					.setStateMachineContext(proposal)
					.build();
		} catch (IllegalArgumentException ex) {
			return builder.build().setException(new IOException("Malformed log entry: " + ex.getMessage(), ex));
		} catch (IOException ex) {
			return builder.build().setException(ex);
		}
	}

	/** Runs on the leader under Ratis' log append lock, in log order. */
	@Override
	public TransactionContext preAppendTransaction(TransactionContext trx) throws IOException {
		if (!(trx.getStateMachineContext() instanceof Proposal proposal)) {
			return trx;
		}
		var division = division();
		if (division == null) {
			return trx;
		}
		var session = leaderSession(division.getInfo().getCurrentTerm());
		if (session == null) {
			throw new StateMachineException("Not the leader", new NotReadyException("Not the leader"), false);
		}
		var entries = proposal.entries();
		boolean guarded = false;
		for (var entry : entries) {
			if (entry instanceof LogEntry.Guarded commit) {
				guarded = true;
				if (commit.leaderEpoch() != session.term()) {
					throw new StateMachineException("Stale transaction",
							new RetryableException("The transaction was opened by another leader"), false);
				}
			}
		}
		if (guarded) {
			var conflicting = session.tracker().conflicts(entries, session::chainCheck);
			if (!conflicting.isEmpty()) {
				var conflict = ConflictException.of(conflicting);
				throw new StateMachineException(conflict.getMessage(), conflict, false);
			}
		}
		CrashPoints.reach(CrashPoints.Point.BEFORE_APPEND);
		proposal.session = session;
		proposal.token = session.tracker().register(entries);
		proposal.appendedNanos = System.nanoTime();
		admitTime.record(proposal.appendedNanos - proposal.startedNanos);
		// Last: a refusal must hand nothing over.
		proposal.links = session.admitted(proposal.requests, proposal.token);
		return trx;
	}

	/**
	 * Hands the entry to the apply thread and returns at once: Ratis goes on with the next committed entries, and
	 * the apply thread applies everything that queued up meanwhile in one local transaction (see
	 * {@link Applier#beginGroup}). Ratis waits for the returned futures before it takes a snapshot, and replies to a
	 * client, or counts the entry applied, only once its future completes, after the commit. Decoding happens here,
	 * on Ratis' thread, off the apply thread.
	 */
	@Override
	public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
		var logEntry = trx.getLogEntry();
		var position = TermIndex.valueOf(logEntry);
		if (failed) {
			return CompletableFuture.failedFuture(new IllegalStateException("Replica failed"));
		}
		var proposal = trx.getStateMachineContext() instanceof Proposal p ? p : null;
		var data = logEntry.getStateMachineLogEntry().getLogData();
		List<Request> requests;
		long leaderTime;
		try {
			if (proposal != null) {
				requests = proposal.requests;
				leaderTime = proposal.leaderTimeMillis;
			} else {
				leaderTime = LogEntryCodec.leaderTime(data.asReadOnlyByteBuffer());
				requests = decodeBatch(data);
			}
		} catch (RuntimeException ex) {
			return CompletableFuture.failedFuture(fail("decode entry " + position, ex));
		}
		int kib = (int) Math.min(MAX_QUEUED_KIB, Math.max(1, data.size() >> 10));
		try {
			queuedKiB.acquire(kib);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return CompletableFuture.failedFuture(ex);
		}
		var reply = new CompletableFuture<Message>();
		applyQueue.add(new ApplyEntry(position, requests, leaderTime, proposal, data.size(), kib, reply));
		handedOver = position.getIndex();
		return reply;
	}

	/**
	 * Decodes the batch of a log entry. When its bytes are one heap array, as Ratis reads them from the log or from
	 * a request (protobuf copies bytes fields unless it aliases its input, which Ratis does not do), the keys and
	 * values of the writes are slices of that array instead of copies: a {@code ByteString} never changes.
	 */
	static List<Request> decodeBatch(ByteString data) {
		var array = new WholeArray();
		try {
			UnsafeByteOperations.unsafeWriteTo(data, array);
		} catch (IOException ex) {
			array.whole = false;
		}
		return array.whole && array.value != null
				? LogEntryCodec.decodeBatchInPlace(array.value, array.offset, array.length)
				: LogEntryCodec.decodeBatch(data.asReadOnlyByteBuffer());
	}

	/** Catches the backing array of a {@code ByteString} made of exactly one array, see {@link #decodeBatch}. */
	private static final class WholeArray extends ByteOutput {
		byte @Nullable [] value;
		int offset;
		int length;
		boolean whole = true;

		@Override
		public void writeLazy(byte[] value, int offset, int length) {
			if (this.value != null) {
				whole = false;
				return;
			}
			this.value = value;
			this.offset = offset;
			this.length = length;
		}

		@Override
		public void write(byte value) {
			whole = false;
		}

		@Override
		public void write(byte[] value, int offset, int length) {
			whole = false;
		}

		@Override
		public void write(ByteBuffer value) {
			whole = false;
		}

		@Override
		public void writeLazy(ByteBuffer value) {
			// A buffer may be pooled and reused once the message is processed: copy.
			whole = false;
		}
	}

	/**
	 * Called by Ratis on its apply thread when it hands over the first entry of this node's term as leader, which
	 * makes it a ready leader: every entry before it was handed over, but may still wait for the apply thread.
	 * Reads and the snapshots of transactions must wait for them (see {@link #leaderReadyIndex}): before the apply
	 * moved to a thread of its own, being the ready leader meant they were applied.
	 */
	@Override
	public void notifyLeaderReady() {
		var division = division();
		if (division != null) {
			// Not info.getLastAppliedIndex(): that is the state machine's own applied index, which lags behind.
			readiness = new Readiness(division.getInfo().getCurrentTerm(), handedOver);
		}
		leaderListener.run();
	}

	/** Runs on every leader change and when this node becomes a ready leader, on a Ratis thread: it must not block. */
	private volatile Runnable leaderListener = () -> {};

	/** Sets what runs on every leader change and when this node becomes a ready leader; it must not block. */
	void onLeaderChange(Runnable listener) {
		this.leaderListener = listener;
	}

	/**
	 * The log index this node must have applied before it serves reads, or opens transactions, as the leader of
	 * {@code term}; the commit index if Ratis did not report this term ready yet, which covers it too.
	 */
	long leaderReadyIndex(long term) {
		var current = readiness;
		if (current.term() == term) {
			return current.index();
		}
		var division = division();
		return division == null ? Long.MAX_VALUE : division.getRaftLog().getLastCommittedIndex();
	}

	/** Configuration entries and leader no-ops: record the position so restarts resume after them. */
	@Override
	public void notifyTermIndexUpdated(long term, long index) {
		var pending = pendingConfiguration;
		pendingConfiguration = null;
		RemovalRule.Conf configuration = null;
		// The apply path passes a configuration entry on just before its position, on the same thread
		// (RaftServerImpl.applyLogToStateMachine); a snapshot install reports one without a position or a term.
		if (pending != null && pending.thread() == Thread.currentThread() && pending.conf().term() == term
				&& pending.conf().index() == index) {
			configuration = pending.conf();
		}
		if (!failed) {
			applyQueue.add(new MarkPosition(TermIndex.valueOf(term, index), configuration));
			handedOver = index;
		}
	}

	/** Waits until the apply thread applied everything queued so far. */
	private void drainApplyQueue() throws IOException {
		var done = new CompletableFuture<Void>();
		applyQueue.add(new Drain(done));
		try {
			done.get();
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			throw new InterruptedIOException("Interrupted while waiting for the apply thread");
		} catch (ExecutionException ex) {
			throw new IOException("The apply thread failed", ex.getCause());
		}
	}

	private void applyLoop() {
		var group = new ArrayList<Applying>();
		while (true) {
			Applying first;
			try {
				first = applyQueue.take();
			} catch (InterruptedException ex) {
				return;
			}
			switch (first) {
				case Stop _ -> {
					failQueued(new IllegalStateException("The state machine is closed"));
					return;
				}
				case Drain drain -> {
					// After the completions of everything before it.
					completer.execute(() -> drain.done().complete(null));
					continue;
				}
				default -> {
				}
			}
			group.clear();
			group.add(first);
			long bytes = first instanceof ApplyEntry entry ? entry.bytes() : 0;
			while (group.size() < MAX_GROUP_ENTRIES && bytes < MAX_GROUP_BYTES) {
				var next = applyQueue.peek();
				if (!(next instanceof ApplyEntry) && !(next instanceof MarkPosition)) {
					break;
				}
				applyQueue.poll();
				group.add(next);
				bytes += next instanceof ApplyEntry entry ? entry.bytes() : 0;
			}
			try {
				applyGroup(group);
			} catch (Throwable error) {
				// An Error (out of memory...): stop the replica, and keep answering whatever is queued, failed. The
				// futures first: reporting the failure may itself fail.
				failed = true;
				failAll(group, error);
				failQueued(error);
				try {
					applier.abortGroup();
					fail("apply entries up to " + positionOf(group.getLast()), error);
				} catch (Throwable secondary) {
					error.addSuppressed(secondary);
				}
			} finally {
				releaseQueued(group);
			}
		}
	}

	private void releaseQueued(List<Applying> items) {
		int kib = 0;
		for (var item : items) {
			if (item instanceof ApplyEntry entry) {
				kib += entry.kib();
			}
		}
		if (kib > 0) {
			queuedKiB.release(kib);
		}
	}

	private final java.util.concurrent.CountDownLatch closed = new java.util.concurrent.CountDownLatch(1);

	/** Hooks for tests, see {@link #testHooks()}. */
	final class TestHooks {
		/** While set, the apply thread waits for it before each group. */
		volatile java.util.concurrent.@Nullable CountDownLatch applyGate;
		/** While set, applying a group throws it, as a failing apply would. */
		volatile @Nullable RuntimeException applyFailure;
		/** Added to this node's wall clock where it stamps entries as leader, like {@link #CLOCK_SKEW_PROPERTY}. */
		volatile long clockSkewMillis;

		/** Lowers the limit of {@link #queuedKiB} to {@code kib}; it waits until that much is free. */
		void limitApplyQueue(int kib) {
			queuedKiB.acquireUninterruptibly(MAX_QUEUED_KIB - kib);
		}
	}

	/**
	 * Tests only: system property adding a fixed offset, in milliseconds (negative = behind), to the wall clock this
	 * node stamps entries with as leader ({@link LogEntryCodec#stamp}). That stamp is the only wall-clock time the
	 * replicated state reads (session expiry); leader leases, read barriers, transaction leases and timeouts all use
	 * {@link System#nanoTime()}, which a wall-clock skew does not move. Lets a process test run one node with a
	 * skewed clock without faking the operating system's.
	 */
	static final String CLOCK_SKEW_PROPERTY = "rockserver.test.clockSkewMillis";
	private static final long CLOCK_SKEW_MILLIS = Long.getLong(CLOCK_SKEW_PROPERTY, 0);

	/** This node's wall clock as a leader stamps it: the system clock plus any test skew. */
	private long leaderClockMillis() {
		var hooks = testHooks;
		return System.currentTimeMillis() + CLOCK_SKEW_MILLIS + (hooks != null ? hooks.clockSkewMillis : 0);
	}

	/** Null unless a test asked for them: the apply thread then reads one field per group, and nothing else. */
	private volatile @Nullable TestHooks testHooks;

	/** Tests only: the test hooks of this state machine, created on first use. */
	synchronized TestHooks testHooks() {
		var hooks = testHooks;
		if (hooks == null) {
			hooks = new TestHooks();
			testHooks = hooks;
		}
		return hooks;
	}

	/** Tests only: the KiB of {@link #queuedKiB} that no queued entry holds. */
	int freeQueuedKiBForTesting() {
		return queuedKiB.availablePermits();
	}

	/** Tests only: the highest log index handed to the apply thread. */
	long handedOverForTesting() {
		return handedOver;
	}

	/**
	 * Applies {@code group} in one local transaction, then has {@link #completer} make each entry count as applied
	 * and complete its reply, in log order. A failure stops the replica: no entry of the group, nor any queued after
	 * it, is answered.
	 */
	private void applyGroup(List<Applying> group) {
		var hooks = testHooks;
		var gate = hooks != null ? hooks.applyGate : null;
		if (gate != null) {
			try {
				gate.await();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
		if (failed) {
			failAll(group, new IllegalStateException("Replica failed"));
			return;
		}
		CrashPoints.reach(CrashPoints.Point.BEFORE_APPLY);
		var completions = new ArrayList<Runnable>(group.size());
		long start = System.nanoTime();
		var applier = this.applier;
		// Before any write of the group can be visible: terms do not decrease along the log.
		var last = positionOf(group.getLast());
		if (last != null) {
			raiseVisibleTerm(last.getTerm());
		}
		applier.beginGroup();
		try {
			var injected = hooks != null ? hooks.applyFailure : null;
			if (injected != null) {
				throw injected;
			}
			for (var item : group) {
				switch (item) {
					case ApplyEntry entry -> completions.add(applyEntry(applier, entry));
					case MarkPosition mark -> {
						var position = mark.position();
						if (position.getIndex() > applied.get()) {
							var configuration = mark.configuration();
							if (configuration != null) {
								applier.recordConfiguration(configuration);
							}
							applier.markApplied(position);
							var history = configurations;
							completions.add(() -> {
								if (configuration != null) {
									history.add(configuration);
								}
								updateLastAppliedTermIndex(position);
								applied.advance(position.getIndex());
							});
						}
					}
					default -> throw new IllegalStateException("Not an entry: " + item);
				}
			}
			applier.endGroup();
		} catch (RuntimeException ex) {
			failed = true;
			failAll(group, ex);
			failQueued(ex);
			applier.abortGroup();
			fail("apply entries up to " + positionOf(group.getLast()), ex);
			return;
		}
		CrashPoints.reach(CrashPoints.Point.AFTER_APPLY_BEFORE_REPLY);
		applyTime.since(start);
		applyGroupEntries.record(group.size());
		prepareSnapshot(positionOf(group.getLast()));
		long groupBytes = 0;
		for (var item : group) {
			// Entries that Ratis replays after a start, up to the database's applied position, are in the seed already.
			if (item instanceof ApplyEntry entry && entry.position().getIndex() > seededThrough) {
				groupBytes += entry.bytes();
			}
		}
		long appliedBytes = groupBytes;
		completer.execute(() -> {
			try {
				for (var completion : completions) {
					completion.run();
				}
				// After the applied index moved: a snapshot covers only what was applied.
				snapshotByBytes(appliedBytes);
			} catch (Throwable error) {
				// The replies first: reporting the failure may itself fail.
				failed = true;
				failAll(group, error);
				try {
					fail("complete entries up to " + positionOf(group.getLast()), error);
				} catch (Throwable secondary) {
					error.addSuppressed(secondary);
				}
			}
		});
	}

	/** Bytes of log entries applied since the latest snapshot, or since the start: see {@link #snapshotByBytes}. */
	private final AtomicLong bytesSinceSnapshot = new AtomicLong();
	/** The last log index counted by the seed of {@link #bytesSinceSnapshot} at start: replayed entries up to it count 0. */
	private volatile long seededThrough = -1;
	/** True while a snapshot asked by {@link #snapshotByBytes} is pending. */
	private final AtomicBoolean snapshotByBytesPending =
			new AtomicBoolean();
	private final AtomicLong snapshotCalls = new AtomicLong();
	private static final long SNAPSHOT_REQUEST_TIMEOUT_MS = 10L * 60 * 1000;

	/**
	 * Asks Ratis for a snapshot once {@code snapshotEveryBytes} of log entries were applied since the latest one.
	 * Ratis' own trigger counts entries ({@code snapshotEveryEntries}), and an entry holds up to 4096 requests or
	 * 8 MiB, so the log that a snapshot lets Ratis purge could otherwise grow to hundreds of GiB. It goes through
	 * Ratis' snapshot management API, as an operator's {@code admin snapshot} does, so Ratis takes the snapshot on its
	 * own thread, as usual. Runs on {@link #completer}.
	 */
	private void snapshotByBytes(long appliedBytes) {
		if (bytesSinceSnapshot.addAndGet(appliedBytes) < config.snapshotEveryBytes()
				|| snapshotBackoff.waiting(System.nanoTime())
				// A replay after a start whose seed reached the threshold: takeSnapshot would answer "none" (R48-1). The
				// next group asks again, at the latest the leader's first entry of its term once the replay is done.
				|| behindTheDatabase()
				|| !snapshotByBytesPending.compareAndSet(false, true)) {
			return;
		}
		var server = this.server;
		var groupId = this.groupId;
		if (server == null || groupId == null) {
			snapshotByBytesPending.set(false);
			return;
		}
		LOG.info("{}: {} bytes of log entries applied since the latest snapshot, taking one", config.nodeId(),
				bytesSinceSnapshot.get());
		try {
			var request = SnapshotManagementRequest.newCreate(
					ClientId.randomId(), server.getId(), groupId, snapshotCalls.incrementAndGet(),
					SNAPSHOT_REQUEST_TIMEOUT_MS);
			server.snapshotManagementAsync(request).whenComplete((reply, error) -> {
				snapshotByBytesPending.set(false);
				if (error != null || !reply.isSuccess()) {
					LOG.warn("{}: the snapshot asked after {} bytes of log failed; asking again after the next entries",
							config.nodeId(), config.snapshotEveryBytes(), error != null ? error : reply.getException());
				}
			});
		} catch (RuntimeException ex) {
			snapshotByBytesPending.set(false);
			LOG.warn("{}: could not ask for a snapshot", config.nodeId(), ex);
		}
	}

	/** The next snapshot's due index whose memtables were already flushed ahead, see {@link #prepareSnapshot}. */
	private long preflushedFor = -1;

	/**
	 * Starts a flush of the memtables shortly before Ratis asks for the next snapshot. Ratis takes snapshots on its
	 * own thread and hands over no entry meanwhile, and most of a snapshot's time is the flush of every memtable
	 * that the checkpoint starts with (100-300 ms): flushed ahead, without waiting, the checkpoint finds only the
	 * last moments of writes to flush. Ratis asks once {@code snapshotEveryEntries} entries were applied since the
	 * latest snapshot; this starts the flush a hundredth of that interval earlier.
	 */
	private void prepareSnapshot(@Nullable TermIndex applied) {
		if (applied == null) {
			return;
		}
		var latest = storage.getLatestSnapshot();
		long due = (latest == null ? 0 : latest.getIndex()) + config.snapshotEveryEntries();
		long margin = Math.max(256, config.snapshotEveryEntries() / 100);
		if (applied.getIndex() < due - margin || preflushedFor == due) {
			return;
		}
		preflushedFor = due;
		try {
			store.db().requestFlush();
		} catch (RuntimeException ex) {
			// Only an optimization: the checkpoint flushes anyway.
			LOG.warn("{}: could not start the flush ahead of the snapshot", config.nodeId(), ex);
		}
	}

	/** Applies one entry into the group; returns what to do once the group is committed. */
	private Runnable applyEntry(Applier applier, ApplyEntry entry) {
		var position = entry.position();
		var requests = entry.requests();
		var proposal = entry.proposal();
		if (position.getIndex() <= applied.get()) {
			// Already applied before a restart: the database holds its effects and, unless its proposer already
			// reported it complete, its result.
			var recordedResults = new ArrayList<ApplyResult>(requests.size());
			for (var request : requests) {
				var recorded = applier.recordedResult(request);
				recordedResults.add(recorded != null ? recorded
						: new ApplyResult.Failed(RocksDBErrorType.INTERNAL_ERROR, "Result no longer available"));
			}
			return () -> entry.reply().complete(encodeReply(recordedResults));
		}
		if (proposal != null && proposal.appendedNanos != 0) {
			commitWait.record(System.nanoTime() - proposal.appendedNanos);
		}
		// With the partitioned apply the results are known only once the group is committed: read them only below.
		var results = applier.applyBatch(requests, position, entry.leaderTimeMillis());
		applyRequests.record(requests.size());
		return () -> {
			if (proposal != null && proposal.token != null && proposal.session != null) {
				proposal.session.tracker().resolve(proposal.token, position.getIndex());
				proposal.session.afterApply();
			}
			updateLastAppliedTermIndex(position);
			applied.advance(position.getIndex());
			if (proposal != null && proposal.links != null) {
				// After the applied index: the next update of the key may be a head, which reads the database at that
				// index or later.
				proposal.session.settle(proposal.links, results);
			}
			// The entry is applied: building its reply must not stop the replica, and encodeAll never fails
			// (results that do not fit become failures, the same on every replica).
			entry.reply().complete(encodeReply(results));
		};
	}

	private static Message encodeReply(List<ApplyResult> results) {
		return Message.valueOf(UnsafeByteOperations.unsafeWrap(ResultCodec.encodeAll(results)));
	}

	private static TermIndex positionOf(Applying item) {
		return switch (item) {
			case ApplyEntry entry -> entry.position();
			case MarkPosition mark -> mark.position();
			default -> null;
		};
	}

	private static void failAll(List<Applying> items, Throwable failure) {
		for (var item : items) {
			switch (item) {
				case ApplyEntry entry -> entry.reply().completeExceptionally(failure);
				case Drain drain -> drain.done().completeExceptionally(failure);
				default -> {
				}
			}
		}
	}

	private void failQueued(Throwable failure) {
		var rest = new ArrayList<Applying>();
		applyQueue.drainTo(rest);
		failAll(rest, failure);
		releaseQueued(rest);
		// close() waits for the apply thread to see its Stop.
		for (var item : rest) {
			if (item instanceof Stop) {
				applyQueue.add(item);
			}
		}
	}

	/** After failed snapshots, when to try again; see {@link #takeSnapshot}. */
	private final SnapshotBackoff snapshotBackoff = new SnapshotBackoff();
	/** Tests only: a failure that the next snapshots throw before their checkpoint. */
	volatile @Nullable IOException snapshotFailureForTests;
	/** Snapshots that this replica tried (the checkpoint started), for tests. */
	final AtomicLong snapshotAttempts = new AtomicLong();

	/**
	 * Takes a snapshot, unless one failed recently. Ratis asks again after every applied entry while a snapshot is due,
	 * and each try drains the apply and flushes every memtable (100-300 ms) before it can fail: after a failure this
	 * answers "no snapshot taken" ({@code -1}, which Ratis passes over without an error or a purge) until
	 * {@link SnapshotBackoff} allows the next try, 1 s after the first failure, doubling up to a minute.
	 *
	 * <p>It answers the same, without a checkpoint and keeping the byte count, while Ratis has not handed over the log up
	 * to the database's applied index ({@link #behindTheDatabase}): a snapshot is at that index, and Ratis takes one past
	 * its own applied index for a bug of the state machine (an ERROR, no purge) (R48-1).</p>
	 *
	 * <p>And it answers the same unless the state machine is {@code RUNNING} (R53-1): from {@link #pause} until
	 * {@link #reinitialize} made it running again, Ratis is swapping in an installed snapshot directory and the database
	 * is about to be replaced. A take that passed this check before the pause can still run beside the swap, which
	 * {@link CheckpointStorage} copes with (it keeps a snapshot past its latest one). {@link #pause} does not wait for
	 * it: Ratis calls it holding the division's {@code RaftServerImpl} monitor, which its {@code appendEntries} and
	 * {@code requestVote} take too, and a take waits for the apply queue and the disk (a hung disk would silence the
	 * division for good).</p>
	 */
	@Override
	public long takeSnapshot() throws IOException {
		if (failed) {
			throw new IOException("Replica failed, not taking a snapshot");
		}
		if (getLifeCycleState() != LifeCycle.State.RUNNING) {
			LOG.info("{}: not taking a snapshot while {} (an install)", config.nodeId(), getLifeCycleState());
			return RaftLog.INVALID_LOG_INDEX;
		}
		if (snapshotBackoff.waiting(System.nanoTime())) {
			return RaftLog.INVALID_LOG_INDEX;
		}
		if (behindTheDatabase()) {
			// Ratis asks again after each entry it hands over while a snapshot is due: not worth a line each time.
			LOG.debug("{}: not taking a snapshot: the database applied {}, Ratis handed over up to {}", config.nodeId(),
					applied.get(), handedOver);
			return RaftLog.INVALID_LOG_INDEX;
		}
		snapshotAttempts.incrementAndGet();
		try {
			// Ratis waited for the entries it handed over; positions without data may still be queued.
			drainApplyQueue();
			long start = System.nanoTime();
			var injected = snapshotFailureForTests;
			if (injected != null) {
				throw injected;
			}
			// Before the checkpoint: entries applied while it runs count towards the next one.
			long bytesBefore = bytesSinceSnapshot.get();
			var position = storage.take(store);
			CrashPoints.reach(CrashPoints.Point.SNAPSHOT_TAKEN);
			bytesSinceSnapshot.addAndGet(-bytesBefore);
			snapshotTime.since(start);
			snapshotBackoff.succeeded();
			LOG.info("{}: snapshot taken at {} in {} ms", config.nodeId(), position,
					(System.nanoTime() - start) / 1_000_000);
			return position.getIndex();
		} catch (IOException | RuntimeException ex) {
			long delay = snapshotBackoff.failed(System.nanoTime());
			LOG.warn("{}: snapshot failed ({} in a row), not trying again for {} ms", config.nodeId(),
					snapshotBackoff.failures(), delay / 1_000_000, ex);
			throw ex;
		}
	}

	/**
	 * True while Ratis has not handed over every log entry up to the database's applied index: after a start, Ratis'
	 * StateMachineUpdater begins at the log's snapshot index and hands the log over again (the entries this database
	 * already applied are skipped) only as commits arrive, while the database is already further. A snapshot is at the
	 * database's index, and Ratis' updater refuses one past its own applied index: it logs "Bug in StateMachine" and
	 * does not purge the log (R48-1). Ratis' applied index is at least {@link #handedOver} (it counts an entry applied
	 * when it hands it over) and at least the latest snapshot's index (where it starts, and where an install puts it).
	 * One volatile read and the snapshot's index: no work per applied entry.
	 */
	private boolean behindTheDatabase() {
		long ratisApplied = handedOver;
		var latest = storage.getLatestSnapshot();
		if (latest != null) {
			ratisApplied = Math.max(ratisApplied, latest.getIndex());
		}
		return applied.get() > ratisApplied;
	}

	/**
	 * When to try a snapshot again after failures: 1 s after the first, doubling with each failure in a row up to a
	 * minute, and at once after a success.
	 */
	static final class SnapshotBackoff {
		static final long FIRST_NANOS = TimeUnit.SECONDS.toNanos(1);
		static final long MAX_NANOS = TimeUnit.MINUTES.toNanos(1);
		private int failures;
		private long retryAtNanos;

		synchronized boolean waiting(long nowNanos) {
			return failures > 0 && nowNanos - retryAtNanos < 0;
		}

		/** Records a failure at {@code nowNanos}; returns the delay before the next try. */
		synchronized long failed(long nowNanos) {
			failures++;
			long delay = FIRST_NANOS << Math.min(failures - 1, 16);
			delay = Math.min(delay, MAX_NANOS);
			retryAtNanos = nowNanos + delay;
			return delay;
		}

		synchronized void succeeded() {
			failures = 0;
		}

		synchronized int failures() {
			return failures;
		}
	}

	/** Called after Ratis installed a snapshot received from the leader, once {@link #pause} returned. */
	@Override
	public void reinitialize() throws IOException {
		try {
			// The database is replaced: nothing may still be applying to it.
			drainApplyQueue();
			CrashPoints.reach(CrashPoints.Point.INSTALL_RECEIVED);
			getLifeCycle().startAndTransition(this::installSnapshot);
		} catch (IOException | RuntimeException ex) {
			// Ratis would only close this division and leave the process running: stop the node for good instead.
			fail("install a snapshot", ex instanceof RuntimeException runtime ? runtime
					: new UncheckedIOException((IOException) ex));
			throw ex;
		}
	}

	private void installSnapshot() throws IOException {
		LOG.info("{}: installing snapshot", config.nodeId());
		storage.reload();
		var snapshot = storage.getLatestSnapshot();
		if (snapshot == null) {
			throw new IOException("No snapshot to install");
		}
		var old = store;
		if (snapshot.getIndex() <= old.applied().getIndex()) {
			// The local database already contains everything in the snapshot: rewinding it would make the
			// entries in between look applied while their effects are gone.
			LOG.info("{}: keeping the local database at {}, ahead of the installed snapshot at {}",
					config.nodeId(), old.applied(), snapshot.getTermIndex());
			return;
		}
		// Odd from here until the restored database replaced the old one: reads that fail meanwhile are retried elsewhere.
		storeGeneration++;
		awaitSessionCloses();
		old.close();
		CrashPoints.reach(CrashPoints.Point.INSTALL_RESTORING);
		NodeStore.restore(storage.snapshotDbDir(), config.dbDir());
		openStore();
		// The leader's database may carry a raise this node cannot read (it was rolled back after answering the poll).
		// The store stays open, the generation odd: the failure stops the replica, whose close closes it.
		var newer = newerVersion(store);
		if (newer != null) {
			throw newer;
		}
		var gate = installGateForTesting;
		if (gate != null) {
			var reached = installReachedForTesting;
			if (reached != null) {
				reached.countDown();
			}
			try {
				gate.await();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		}
		storeGeneration++;
	}

	/**
	 * Tests only: while set, an install holds after the new database opened (and the applied index moved to the
	 * snapshot), before the second {@link #storeGeneration} bump, counting {@link #installReachedForTesting} down first.
	 * Static, so that it is set before the node that installs starts.
	 */
	static volatile java.util.concurrent.@Nullable CountDownLatch installGateForTesting;
	static volatile java.util.concurrent.@Nullable CountDownLatch installReachedForTesting;

	@Override
	public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
		if (groupMemberId.getPeerId().equals(newLeaderId)) {
			CrashPoints.reach(CrashPoints.Point.ELECTED);
		}
		if (!groupMemberId.getPeerId().equals(newLeaderId)) {
			LeaderSession session;
			synchronized (this) {
				session = leaderSession;
				leaderSession = null;
			}
			if (session != null) {
				LOG.info("{}: no longer leading term {}", config.nodeId(), session.term());
				retire(session);
			}
		}
		leaderListener.run();
	}

	/** A configuration entry that the apply path passed on, waiting for its position: see {@link #notifyTermIndexUpdated}. */
	private record PendingConfiguration(Thread thread, RemovalRule.Conf conf) {}

	/** Set and read on Ratis' state machine updater thread. */
	private volatile @Nullable PendingConfiguration pendingConfiguration;

	/**
	 * Ratis calls this when it applies a committed configuration entry, just before {@link #notifyTermIndexUpdated} of
	 * the same position on the same thread; the pair goes to the apply thread, which records the configuration with
	 * the position ({@link CommittedConfigurations}). Ratis also calls it on every chunk of a snapshot install, with
	 * the leader's latest configuration, which may not be committed, and with no term (0): that call has no position
	 * after it, is not recorded, and leaves a pending configuration of the updater thread alone.
	 */
	@Override
	public void notifyConfigurationChanged(long term, long index,
			RaftProtos.RaftConfigurationProto newRaftConfiguration) {
		if (newRaftConfiguration.getOldPeersCount() > 0) {
			CrashPoints.reach(CrashPoints.Point.JOINT_CONFIGURATION_APPLIED);
		}
		if (term <= RemovalRule.UNKNOWN_TERM) {
			// The install path, on another thread: it must not drop a configuration the updater thread passed on and
			// is about to record with its position (R11 review, finding 2).
			return;
		}
		pendingConfiguration = new PendingConfiguration(Thread.currentThread(), new RemovalRule.Conf(term, index,
				ids(newRaftConfiguration.getPeersList()), ids(newRaftConfiguration.getOldPeersList())));
	}

	private static List<String> ids(List<RaftProtos.RaftPeerProto> peers) {
		return peers.stream().map(peer -> peer.getId().toStringUtf8()).toList();
	}

	/** The committed configurations applied here, see {@link CommittedConfigurations}. Any thread. */
	CommittedConfigurations configurations() {
		return configurations;
	}

	/** Replaced when a database opens. */
	private volatile CommittedConfigurations configurations = CommittedConfigurations.none();

	@Override
	public void notifyServerShutdown(RaftProtos.RoleInfoProto roleInfo, boolean allServer) {
		LeaderSession session;
		synchronized (this) {
			session = leaderSession;
			leaderSession = null;
		}
		if (session != null) {
			retire(session);
		}
	}

	/**
	 * Ends the leader session of a term that ended. Ratis calls {@link #notifyLeaderChanged} holding its server's
	 * monitor (from {@code changeToFollower} and inside {@code appendEntriesAsync} and {@code requestVote}), and
	 * {@link #leaderSession} under its append lock; the session's rollbacks take each native transaction's monitor,
	 * which a read or a write in that transaction holds for a whole native call, for as long as a stalled disk makes it
	 * last, and are thousands of native calls on a busy term. So only {@link LeaderSession#seal()} runs here, after
	 * which nothing registers in the session or queues for one of its keys; the waiters, the links and the rollbacks
	 * ({@link LeaderSession#drain}) run on a platform thread of this session: one per session, so that a stuck rollback
	 * of term t does not delay the waiters of a later term. The database is the one open now: a database installed
	 * later never knew these transactions. {@link #close()} and {@link #installSnapshot()} wait for these threads before
	 * closing the database. Nothing is thrown into Ratis if that thread cannot be created (an {@link OutOfMemoryError}
	 * "unable to create native thread"): the waiters and links are answered here, which takes no native call, and the
	 * drain runs on a virtual thread; failing that too, the transactions stay open until they expire or the database
	 * closes, and the session counts as drained so that nothing waits for it forever.
	 */
	private void retire(LeaderSession session) {
		session.seal();
		var store = this.store;
		var db = store != null ? store.db() : null;
		var done = new CompletableFuture<Void>();
		sessionCloses.put(done, session.term());
		Runnable drain = () -> {
			try {
				if (db != null) {
					session.drain(db);
				} else {
					session.drain(_ -> {}, _ -> {});
				}
			} catch (Throwable ex) {
				LOG.warn("{}: failed to close the leader session of term {}", config.nodeId(), session.term(), ex);
			} finally {
				sessionCloses.remove(done);
				done.complete(null);
			}
		};
		String name = config.nodeId() + "-session-close-" + session.term();
		try {
			sessionCloseStarter.accept(name, drain);
			return;
		} catch (Throwable ex) {
			// Typically OutOfMemoryError "unable to create native thread". Nothing may be thrown into Ratis from here.
			LOG.error("{}: could not start a thread to close the leader session of term {}", config.nodeId(),
					session.term(), ex);
		}
		try {
			// No native work, so fine on Ratis' thread; the waiters must not wait for the rollbacks to get a thread.
			session.answerWaiters();
		} catch (Throwable ex) {
			LOG.warn("{}: failed to answer the waiters of the leader session of term {}", config.nodeId(),
					session.term(), ex);
		}
		try {
			// A virtual thread needs no new native thread, only a carrier of the shared pool. A rollback stuck in a native
			// call pins its carrier: acceptable once a platform thread could not even be created.
			Thread.ofVirtual().name(name).start(drain);
		} catch (Throwable ex) {
			LOG.error("{}: could not start a thread to roll back the transactions of term {}; they stay open until they"
					+ " expire or the database closes", config.nodeId(), session.term(), ex);
			sessionCloses.remove(done);
			done.complete(null);
		}
	}

	/**
	 * Starts the thread named by the first argument that runs the second: what {@link #retire} drains a session on.
	 * Tests replace it to make the thread fail to start.
	 */
	volatile BiConsumer<String, Runnable> sessionCloseStarter =
			(name, task) -> Thread.ofPlatform().daemon().name(name).start(task);

	/** Sessions {@link #retire}d whose rollbacks still run, with their terms. */
	private final ConcurrentHashMap<CompletableFuture<Void>, Long> sessionCloses = new ConcurrentHashMap<>();

	/**
	 * Waits for the rollbacks of every {@link #retire}d session, however long: the database must not close under one.
	 * (Its own close would wait anyway, for the same monitors.)
	 */
	private void awaitSessionCloses() {
		for (var entry : List.copyOf(sessionCloses.entrySet())) {
			while (true) {
				try {
					entry.getKey().get(10, TimeUnit.SECONDS);
					break;
				} catch (java.util.concurrent.TimeoutException ex) {
					LOG.warn("{}: still rolling back the transactions of term {} before closing the database",
							config.nodeId(), entry.getValue());
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					return;
				} catch (ExecutionException ex) {
					break;
				}
			}
		}
	}

	private RuntimeException fail(String what, Throwable error) {
		failed = true;
		var ex = error instanceof RuntimeException runtime ? runtime : new IllegalStateException(error);
		LOG.error("{}: replica failed to {}; it stops serving and must be repaired", config.nodeId(), what, error);
		onReplicaFailure.accept(ex);
		return ex;
	}

	@Override
	public void close() throws IOException {
		ops.close();
		// Offers whose answers never come, if any: nothing will take them now.
		localProposals.clear();
		// Ratis waited for every reply it handed over; stop the apply thread before closing the database.
		applyQueue.add(new Stop());
		try {
			applyThread.join();
			if (applyLanes != null) {
				applyLanes.close();
			}
			completer.shutdown();
			completer.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		var session = leaderSession;
		leaderSession = null;
		var store = this.store;
		try {
			// A session retired off Ratis' threads may still be rolling back: not under a closing database.
			awaitSessionCloses();
			if (store != null) {
				if (session != null) {
					session.close(store.db());
				}
				store.close();
			}
			super.close();
		} finally {
			closed.countDown();
		}
	}

	/** Waits until {@link #close()} ended, on whichever thread Ratis runs it; returns false if it did not in time. */
	boolean awaitClosed(Duration timeout) throws InterruptedException {
		return closed.await(timeout.toNanos(), TimeUnit.NANOSECONDS);
	}
}
