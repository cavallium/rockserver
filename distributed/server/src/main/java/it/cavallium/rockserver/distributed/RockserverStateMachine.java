package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
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
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
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

	/** A transaction commit was rejected because one of its keys changed after its snapshot. */
	public static final class ConflictException extends IOException {
		public ConflictException(String message) {
			super(message);
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

	/** State attached to a request on the leader between admission and apply. */
	private static final class Proposal {
		final LogEntry entry;
		@Nullable LeaderSession session;
		@Nullable ConflictTracker.Token token;

		Proposal(LogEntry entry) {
			this.entry = entry;
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

	RockserverStateMachine(NodeConfig config, Consumer<Throwable> onReplicaFailure) {
		this.config = config;
		this.onReplicaFailure = onReplicaFailure;
	}

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
			var created = new LeaderSession(term);
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
			var entry = LogEntryCodec.decode(request.getMessage().getContent().toByteArray());
			if (!(entry instanceof LogEntry.Genesis) && store.clusterId() == null) {
				throw new NotReadyException("The cluster is not initialized yet");
			}
			if (entry instanceof LogEntry.Guarded guarded) {
				var division = division();
				long term = division != null ? division.getInfo().getCurrentTerm() : -1;
				if (guarded.leaderEpoch() != term) {
					throw new RetryableException("The transaction was opened by another leader");
				}
			}
			return builder.setLogData(request.getMessage().getContent())
					.setStateMachineContext(new Proposal(entry))
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
		if (proposal.entry instanceof LogEntry.Guarded guarded) {
			if (guarded.leaderEpoch() != session.term()) {
				throw new StateMachineException("Stale transaction",
						new RetryableException("The transaction was opened by another leader"), false);
			}
			if (session.tracker().conflicts(guarded)) {
				throw new StateMachineException("Transaction conflict", new ConflictException("Transaction conflict"), false);
			}
		}
		proposal.session = session;
		proposal.token = session.tracker().register(proposal.entry);
		return trx;
	}

	@Override
	public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
		var logEntry = trx.getLogEntry();
		var position = TermIndex.valueOf(logEntry);
		if (failed) {
			return CompletableFuture.failedFuture(new IllegalStateException("Replica failed"));
		}
		if (position.getIndex() <= applied.get()) {
			// Already applied before a restart: the database contains its effects.
			return CompletableFuture.completedFuture(Message.EMPTY);
		}
		try {
			var proposal = trx.getStateMachineContext() instanceof Proposal p ? p : null;
			var entry = proposal != null ? proposal.entry
					: LogEntryCodec.decode(logEntry.getStateMachineLogEntry().getLogData().toByteArray());
			var result = applier.apply(entry, position);
			if (proposal != null && proposal.token != null && proposal.session != null) {
				proposal.session.tracker().resolve(proposal.token, position.getIndex());
			}
			updateLastAppliedTermIndex(position);
			applied.advance(position.getIndex());
			return CompletableFuture.completedFuture(Message.valueOf(ByteString.copyFrom(ResultCodec.encode(result))));
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
		var position = storage.take(store);
		LOG.info("{}: snapshot taken at {}", config.nodeId(), position);
		return position.getIndex();
	}

	/**
	 * Called by Ratis before it installs a snapshot received from the leader. Ratis refuses to install it unless
	 * the life cycle is then PAUSED; no entry is applied until {@link #reinitialize}.
	 */
	@Override
	public void pause() {
		getLifeCycle().transition(LifeCycle.State.PAUSING);
		getLifeCycle().transition(LifeCycle.State.PAUSED);
	}

	/** Called after Ratis installed a snapshot received from the leader, once {@link #pause} returned. */
	@Override
	public void reinitialize() throws IOException {
		getLifeCycle().startAndTransition(() -> {
			LOG.info("{}: installing snapshot", config.nodeId());
			storage.reload();
			var snapshot = storage.getLatestSnapshot();
			if (snapshot == null) {
				throw new IOException("No snapshot to install");
			}
			var old = store;
			old.close();
			NodeStore.restore(storage.snapshotDbDir(), config.dbDir());
			openStore();
		});
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
