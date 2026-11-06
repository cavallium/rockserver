package it.cavallium.rockserver.distributed;

import java.io.IOException;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
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

	private final NodeConfig config;
	private final CheckpointStorage storage = new CheckpointStorage();
	private final Consumer<Throwable> onReplicaFailure;
	private final AppliedIndex applied = new AppliedIndex(-1);
	private volatile RaftServer server;
	private volatile RaftGroupId groupId;
	private volatile NodeStore store;
	private volatile Applier applier;
	private volatile @Nullable LeaderSession leaderSession;
	private volatile boolean failed;

	/** Entries waiting for the apply thread, in log order; see {@link #applyTransaction}. */
	private final LinkedBlockingQueue<Applying> applyQueue = new LinkedBlockingQueue<>();
	/**
	 * KiB of queued entries: Ratis' thread waits in {@link #applyTransaction} while the apply thread is this far
	 * behind, so that a lagging replica keeps its backlog in the Raft log on disk, not decoded on the heap.
	 */
	private final java.util.concurrent.Semaphore queuedKiB = new java.util.concurrent.Semaphore(MAX_QUEUED_KIB);
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
	private final java.util.concurrent.ExecutorService completer;
	/** At most this many entries, and about this many bytes of entries, are applied in one local transaction. */
	private static final int MAX_GROUP_ENTRIES = 256;
	private static final long MAX_GROUP_BYTES = 16L << 20;

	/** Work for the apply thread. */
	private sealed interface Applying {}

	/** A data entry to apply; {@code reply} completes once it is committed. It holds {@code kib} of {@link #queuedKiB}. */
	private record ApplyEntry(TermIndex position, List<Request> requests, long leaderTimeMillis,
			@Nullable Proposal proposal, long bytes, int kib, CompletableFuture<Message> reply) implements Applying {}

	/** A position without data (configuration entries, leader no-ops) to record. */
	private record MarkPosition(TermIndex position) implements Applying {}

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
		this.completer = java.util.concurrent.Executors.newSingleThreadExecutor(
				Thread.ofPlatform().name(config.nodeId() + "-applied").daemon().factory());
		this.applyThread = Thread.ofPlatform().name(config.nodeId() + "-apply").daemon().unstarted(this::applyLoop);
		applyThread.start();
	}

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
		getLifeCycle().startAndTransition(this::openStore);
	}

	/**
	 * Called by Ratis once a snapshot from the leader is fully received, before {@link #reinitialize}: Ratis reloads
	 * the state machine only if it is {@code PAUSED}, and otherwise stops this replica's Raft server for good. The
	 * state machine updater thread then runs {@link #reinitialize} instead of applying entries, which makes it
	 * {@code RUNNING} again.
	 */
	@Override
	public synchronized void pause() {
		var lifeCycle = getLifeCycle();
		if (lifeCycle.getCurrentState() == LifeCycle.State.RUNNING) {
			lifeCycle.transition(LifeCycle.State.PAUSING);
			lifeCycle.transition(LifeCycle.State.PAUSED);
		}
	}

	/** Opens the local database, first restoring the snapshot if it is ahead of the database. */
	private void openStore() throws IOException {
		var opened = NodeStore.open(config.dbDir(), config.rockserverConfig(), config.nodeId());
		var snapshot = storage.getLatestSnapshot();
		if (snapshot != null && snapshot.getIndex() > opened.applied().getIndex()) {
			LOG.warn("{}: database at {} is behind the snapshot at {}, restoring the snapshot",
					config.nodeId(), opened.applied(), snapshot.getTermIndex());
			opened.close();
			NodeStore.restore(storage.snapshotDbDir(), config.dbDir());
			opened = NodeStore.open(config.dbDir(), config.rockserverConfig(), config.nodeId());
		}
		var position = opened.applied();
		this.store = opened;
		this.applier = new Applier(opened, metrics);
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

	/** The result this replica recorded for request {@code sequence} of {@code proposerId}, or null. Any thread. */
	@Nullable ApplyResult recordedResult(long proposerId, long sequence) {
		return Applier.recordedResult(store, proposerId, sequence);
	}

	/** The result this replica recorded for write {@code id} of a client session, or null. Any thread. */
	@Nullable ApplyResult recordedResult(it.cavallium.rockserver.core.common.ClientRequestId id) {
		return Applier.recordedResult(store, id);
	}

	boolean isFailed() {
		return failed;
	}

	/**
	 * The leader session of the current term, if this node leads it; created on first use. The session of an earlier
	 * term is closed outside the monitor, as in {@link #notifyLeaderChanged}: closing rolls back its native
	 * transactions and updates, and {@link #preAppendTransaction} waits for this monitor under Ratis' append lock.
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
			created = new LeaderSession(term, applied, new ConflictTracker(), config.pipelinedUpdates(), metrics);
			// The session tracks writes from this term on: a transaction whose snapshot misses an earlier leader's
			// entries would not see them conflict. Reads wait for those entries (see leaderReadyIndex); this makes
			// sure no commit validates against such a snapshot anyway.
			created.tracker().prune(leaderReadyIndex(term));
			leaderSession = created;
		}
		if (replaced != null) {
			replaced.close(store.db());
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

	@Override
	public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
		var builder = TransactionContext.newBuilder()
				.setStateMachine(this)
				.setClientRequest(request);
		try {
			var decoded = decodeBatch(request.getMessage().getContent());
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
				proposal.leaderTimeMillis = System.currentTimeMillis();
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
		public void write(java.nio.ByteBuffer value) {
			whole = false;
		}

		@Override
		public void writeLazy(java.nio.ByteBuffer value) {
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
		if (!failed) {
			applyQueue.add(new MarkPosition(TermIndex.valueOf(term, index)));
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
			throw new java.io.InterruptedIOException("Interrupted while waiting for the apply thread");
		} catch (java.util.concurrent.ExecutionException ex) {
			throw new IOException("The apply thread failed", ex.getCause());
		}
	}

	private void applyLoop() {
		var group = new java.util.ArrayList<Applying>();
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

		/** Lowers the limit of {@link #queuedKiB} to {@code kib}; it waits until that much is free. */
		void limitApplyQueue(int kib) {
			queuedKiB.acquireUninterruptibly(MAX_QUEUED_KIB - kib);
		}
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
		var completions = new java.util.ArrayList<Runnable>(group.size());
		long start = System.nanoTime();
		var applier = this.applier;
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
							applier.markApplied(position);
							completions.add(() -> {
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
		applyTime.since(start);
		applyGroupEntries.record(group.size());
		prepareSnapshot(positionOf(group.getLast()));
		long groupBytes = 0;
		for (var item : group) {
			if (item instanceof ApplyEntry entry) {
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
	private final java.util.concurrent.atomic.AtomicLong bytesSinceSnapshot = new java.util.concurrent.atomic.AtomicLong();
	/** True while a snapshot asked by {@link #snapshotByBytes} is pending. */
	private final java.util.concurrent.atomic.AtomicBoolean snapshotByBytesPending =
			new java.util.concurrent.atomic.AtomicBoolean();
	private final java.util.concurrent.atomic.AtomicLong snapshotCalls = new java.util.concurrent.atomic.AtomicLong();
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
			var request = org.apache.ratis.protocol.SnapshotManagementRequest.newCreate(
					org.apache.ratis.protocol.ClientId.randomId(), server.getId(), groupId, snapshotCalls.incrementAndGet(),
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
			var recordedResults = new java.util.ArrayList<ApplyResult>(requests.size());
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
		var rest = new java.util.ArrayList<Applying>();
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

	@Override
	public long takeSnapshot() throws IOException {
		if (failed) {
			throw new IOException("Replica failed, not taking a snapshot");
		}
		// Ratis waited for the entries it handed over; positions without data may still be queued.
		drainApplyQueue();
		long start = System.nanoTime();
		// Before the checkpoint: entries applied while it runs count towards the next one.
		long bytesBefore = bytesSinceSnapshot.get();
		var position = storage.take(store);
		bytesSinceSnapshot.addAndGet(-bytesBefore);
		snapshotTime.since(start);
		LOG.info("{}: snapshot taken at {} in {} ms", config.nodeId(), position, (System.nanoTime() - start) / 1_000_000);
		return position.getIndex();
	}

	/** Called after Ratis installed a snapshot received from the leader, once {@link #pause} returned. */
	@Override
	public void reinitialize() throws IOException {
		try {
			// The database is replaced: nothing may still be applying to it.
			drainApplyQueue();
			getLifeCycle().startAndTransition(this::installSnapshot);
		} catch (IOException | RuntimeException ex) {
			// Ratis would only close this division and leave the process running: stop the node for good instead.
			fail("install a snapshot", ex instanceof RuntimeException runtime ? runtime
					: new java.io.UncheckedIOException((IOException) ex));
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
		old.close();
		NodeStore.restore(storage.snapshotDbDir(), config.dbDir());
		openStore();
	}

	@Override
	public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
		if (!groupMemberId.getPeerId().equals(newLeaderId)) {
			LeaderSession session;
			synchronized (this) {
				session = leaderSession;
				leaderSession = null;
			}
			if (session != null) {
				LOG.info("{}: no longer leading term {}", config.nodeId(), session.term());
				session.close(store.db());
			}
		}
	}

	@Override
	public void notifyServerShutdown(org.apache.ratis.proto.RaftProtos.RoleInfoProto roleInfo, boolean allServer) {
		LeaderSession session;
		synchronized (this) {
			session = leaderSession;
			leaderSession = null;
		}
		if (session != null) {
			session.close(store.db());
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
		// Ratis waited for every reply it handed over; stop the apply thread before closing the database.
		applyQueue.add(new Stop());
		try {
			applyThread.join();
			completer.shutdown();
			completer.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		var session = leaderSession;
		leaderSession = null;
		var store = this.store;
		try {
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
	boolean awaitClosed(java.time.Duration timeout) throws InterruptedException {
		return closed.await(timeout.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
	}
}
