package it.cavallium.rockserver.distributed;

import java.io.IOException;
import it.unimi.dsi.fastutil.ints.IntList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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
 * Every replica then applies entries synchronously, in order, on the Ratis apply thread.</p>
 */
final class RockserverStateMachine extends BaseStateMachine {

	private static final Logger LOG = LoggerFactory.getLogger(RockserverStateMachine.class);

	/**
	 * Transaction commits of a log entry were rejected because one of their keys changed after their snapshot. The
	 * message lists their positions in the entry, after {@link #POSITIONS}; the entry was not appended.
	 */
	public static final class ConflictException extends IOException {
		static final String POSITIONS = "conflicting positions: ";

		public ConflictException(String message) {
			super(message);
		}

		static ConflictException of(IntList positions) {
			var text = new StringBuilder("Transaction conflict, ").append(POSITIONS);
			for (int i = 0; i < positions.size(); i++) {
				text.append(i == 0 ? "" : ",").append(positions.getInt(i));
			}
			return new ConflictException(text.toString());
		}

		/** The positions listed in {@code message}, or null if it lists none. */
		static int @Nullable [] positions(@Nullable String message) {
			int at = message == null ? -1 : message.indexOf(POSITIONS);
			if (at < 0) {
				return null;
			}
			try {
				return java.util.Arrays.stream(message.substring(at + POSITIONS.length()).strip().split(","))
						.mapToInt(Integer::parseInt).toArray();
			} catch (NumberFormatException ex) {
				return null;
			}
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
		final long startedNanos = System.nanoTime();
		long appendedNanos;
		@Nullable LeaderSession session;
		@Nullable ConflictTracker.Token token;

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

	RockserverStateMachine(NodeConfig config, Consumer<Throwable> onReplicaFailure, NodeMetrics metrics) {
		this.config = config;
		this.onReplicaFailure = onReplicaFailure;
		this.applyTime = metrics.duration("apply");
		this.applyRequests = metrics.amount("apply.requests");
		this.admitTime = metrics.duration("raft.admit");
		this.commitWait = metrics.duration("raft.appendToApply");
		this.snapshotTime = metrics.duration("snapshot");
	}

	private final NodeMetrics.Series snapshotTime;

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
		this.applier = new Applier(opened);
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

	boolean isFailed() {
		return failed;
	}

	/** The leader session of the current term, if this node leads it; created on first use. */
	@Nullable LeaderSession leaderSession(long term) {
		synchronized (this) {
			var current = leaderSession;
			if (current != null && current.term() == term) {
				return current;
			}
			var division = division();
			if (division == null || !division.getInfo().isLeader() || division.getInfo().getCurrentTerm() != term) {
				return null;
			}
			if (current != null) {
				current.close(store.db());
			}
			var created = new LeaderSession(term, applied);
			leaderSession = created;
			LOG.info("{}: leading term {}", config.nodeId(), term);
			return created;
		}
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
			var decoded = LogEntryCodec.decodeBatch(request.getMessage().getContent().toByteArray());
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
			return builder.setLogData(request.getMessage().getContent())
					.setStateMachineContext(new Proposal(decoded))
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
			var conflicting = session.tracker().conflicts(entries);
			if (!conflicting.isEmpty()) {
				var conflict = ConflictException.of(conflicting);
				throw new StateMachineException(conflict.getMessage(), conflict, false);
			}
		}
		proposal.session = session;
		proposal.token = session.tracker().register(entries);
		proposal.appendedNanos = System.nanoTime();
		admitTime.record(proposal.appendedNanos - proposal.startedNanos);
		return trx;
	}

	@Override
	public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
		var logEntry = trx.getLogEntry();
		var position = TermIndex.valueOf(logEntry);
		if (failed) {
			return CompletableFuture.failedFuture(new IllegalStateException("Replica failed"));
		}
		try {
			var proposal = trx.getStateMachineContext() instanceof Proposal p ? p : null;
			var requests = proposal != null ? proposal.requests
					: LogEntryCodec.decodeBatch(logEntry.getStateMachineLogEntry().getLogData().toByteArray());
			if (position.getIndex() <= applied.get()) {
				// Already applied before a restart: the database holds its effects and, unless its proposer already
				// reported it complete, its result.
				var results = new java.util.ArrayList<ApplyResult>(requests.size());
				for (var request : requests) {
					var recorded = applier.recordedResult(request);
					results.add(recorded != null ? recorded
							: new ApplyResult.Failed(RocksDBErrorType.INTERNAL_ERROR, "Result no longer available"));
				}
				return CompletableFuture.completedFuture(Message.valueOf(UnsafeByteOperations.unsafeWrap(
						ResultCodec.encodeAll(results))));
			}
			long applyStart = System.nanoTime();
			if (proposal != null && proposal.appendedNanos != 0) {
				commitWait.record(applyStart - proposal.appendedNanos);
			}
			var results = applier.applyBatch(requests, position);
			applyTime.since(applyStart);
			applyRequests.record(requests.size());
			if (proposal != null && proposal.token != null && proposal.session != null) {
				proposal.session.tracker().resolve(proposal.token, position.getIndex());
				proposal.session.afterApply();
			}
			updateLastAppliedTermIndex(position);
			applied.advance(position.getIndex());
			return CompletableFuture.completedFuture(Message.valueOf(UnsafeByteOperations.unsafeWrap(
					ResultCodec.encodeAll(results))));
		} catch (RuntimeException ex) {
			return CompletableFuture.failedFuture(fail("apply entry " + position, ex));
		}
	}

	/** Configuration entries and leader no-ops: record the position so restarts resume after them. */
	@Override
	public void notifyTermIndexUpdated(long term, long index) {
		if (failed || index <= applied.get()) {
			return;
		}
		var position = TermIndex.valueOf(term, index);
		try {
			applier.markApplied(position);
			updateLastAppliedTermIndex(position);
			applied.advance(index);
		} catch (RuntimeException ex) {
			fail("record position " + position, ex);
		}
	}

	@Override
	public long takeSnapshot() throws IOException {
		if (failed) {
			throw new IOException("Replica failed, not taking a snapshot");
		}
		long start = System.nanoTime();
		var position = storage.take(store);
		snapshotTime.since(start);
		LOG.info("{}: snapshot taken at {} in {} ms", config.nodeId(), position, (System.nanoTime() - start) / 1_000_000);
		return position.getIndex();
	}

	/** Called after Ratis installed a snapshot received from the leader, once {@link #pause} returned. */
	@Override
	public void reinitialize() throws IOException {
		try {
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

	private RuntimeException fail(String what, RuntimeException ex) {
		failed = true;
		LOG.error("{}: replica failed to {}; it stops serving and must be repaired", config.nodeId(), what, ex);
		onReplicaFailure.accept(ex);
		return ex;
	}

	@Override
	public void close() throws IOException {
		var session = leaderSession;
		leaderSession = null;
		var store = this.store;
		if (store != null) {
			if (session != null) {
				session.close(store.db());
			}
			store.close();
		}
		super.close();
	}
}
