package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.Failures;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.EmbeddedDB.ApplyWriter;
import it.cavallium.rockserver.core.impl.FFMAbstractMergeOperator;
import java.io.IOException;
import java.io.UncheckedIOException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongRBTreeSet;
import it.unimi.dsi.fastutil.longs.LongSortedSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Applies log entries to the local database. Called only by the state machine's apply thread, apart from the static
 * {@link #recordedResult(NodeStore, long, long)}.
 *
 * <p>Applying a {@link Request} makes three things durable together: the entry's effects, its result (recorded under
 * the request's identity, see {@link Request}) and the new applied position. The data writes of a group of entries
 * run inside one local transaction without the WAL that writes all three (see {@link #beginGroup}), so a crash keeps
 * all or none of them. A {@code deleteRange} runs alone without the WAL, followed by its record and position (see
 * {@link #applyAlone}). Schema changes cannot run in a transaction: the memtables are flushed first, then their record
 * and position are written in one logged write and the WAL is synced.</p>
 *
 * <p>A failure that follows from the entry (a logical rejection, a failing merge operator, an exception of the apply
 * code) happens the same way on every replica: that becomes an {@link ApplyResult.Failed} and the log moves on. A
 * failure of the local storage or of the JVM is specific to this replica (see {@link #isLocalFailure}): it is thrown
 * as {@link ReplicaFailedException}, and the replica stops instead of skipping the entry.</p>
 */
final class Applier {

	private static final Logger LOG = LoggerFactory.getLogger(Applier.class);

	/** This replica cannot apply the log any more and must be repaired. */
	static final class ReplicaFailedException extends RuntimeException {
		ReplicaFailedException(String message, Throwable cause) {
			super(message, cause);
		}

		ReplicaFailedException(String message) {
			super(message);
		}
	}

	/** How far a proposer's floor moves between two deletions of the records below it, see {@link #purge}. */
	static final long PURGE_EVERY_SEQUENCES = 1 << 10;
	/** Bytes of results a proposer records between two deletions of its records below the floor, see {@link #purge}. */
	static final long PURGE_EVERY_BYTES = 4 << 20;

	private final NodeStore store;
	private final long purgeEverySequences;
	private final long purgeEveryBytes;
	/** True once the records below every proposer's floor were deleted, see {@link #purgeAllProposers}. */
	private boolean purgedAllProposers;

	Applier(NodeStore store) {
		this(store, null);
	}

	Applier(NodeStore store, @Nullable NodeMetrics metrics) {
		this(store, metrics, PURGE_EVERY_SEQUENCES, PURGE_EVERY_BYTES);
	}

	Applier(NodeStore store, @Nullable NodeMetrics metrics, long purgeEverySequences, long purgeEveryBytes) {
		this(store, metrics, purgeEverySequences, purgeEveryBytes, null);
	}

	/** With {@code lanes}, data entries of a group are applied in parallel: see "Partitioned apply" below. */
	Applier(NodeStore store, @Nullable NodeMetrics metrics, @Nullable ApplyLanes lanes) {
		this(store, metrics, PURGE_EVERY_SEQUENCES, PURGE_EVERY_BYTES, lanes);
	}

	Applier(NodeStore store, @Nullable NodeMetrics metrics, long purgeEverySequences, long purgeEveryBytes,
			@Nullable ApplyLanes lanes) {
		this.store = store;
		this.lanes = lanes;
		this.parallelCuts = metrics == null ? null : metrics.amount("apply.parallelCuts");
		this.purgeEverySequences = purgeEverySequences;
		this.purgeEveryBytes = purgeEveryBytes;
		this.openTime = metrics == null ? null : metrics.duration("apply.open");
		this.executeTime = metrics == null ? null : metrics.duration("apply.execute");
		this.commitTime = metrics == null ? null : metrics.duration("apply.commit");
		this.guardRejected = metrics == null ? null : metrics.amount("update.guardRejected");
	}

	private final NodeMetrics.@Nullable Series openTime;
	private final NodeMetrics.@Nullable Series executeTime;
	private final NodeMetrics.@Nullable Series commitTime;
	private final NodeMetrics.@Nullable Series guardRejected;
	private final NodeMetrics.@Nullable Series parallelCuts;

	/**
	 * Tests only: runs on the apply thread right after each database write of the apply, with what it committed
	 * ("entry", "group", "prefix" before a segment, "segment"). A crash test flushes there, to keep exactly that point.
	 */
	@Nullable java.util.function.Consumer<String> afterCommit;

	private void committed(String what) {
		var hook = afterCommit;
		if (hook != null) {
			hook.accept(what);
		}
	}

	private static void record(NodeMetrics.@Nullable Series series, long startNanos) {
		if (series != null) {
			series.since(startNanos);
		}
	}

	/**
	 * What the meta column says about one proposer's requests: its floor and which requests at or above it have a
	 * record. Kept in memory, loaded from the database the first time the proposer is seen, so that checking a
	 * request for an earlier copy costs no database read. Changed only together with the database: after a commit, or
	 * as an entry is written to the open transaction of a group (see {@link #beginGroup}).
	 */
	private static final class ProposerRecords {
		/** A client session's records ({@link Request#client()}), not a proposer's. */
		final boolean session;
		/** The proposer id or the session id. */
		final long id;
		long floor;
		/** Records below this were deleted from the database by this applier, see {@link #purge}. */
		long purgedBelow;
		/** Bytes of the results recorded since the last purge, see {@link #purge}. */
		long recordedBytes;
		final LongSortedSet recorded = new LongRBTreeSet();
		/** Sessions only: the session's timeout, from its {@link LogEntry.RegisterClient}. */
		long timeoutMillis;
		/** Sessions only: the leader time of its last applied write, or of its registration. */
		long lastActiveMillis;
		/** Sessions only: {@link #recorded} was loaded from the database. */
		boolean loaded;

		ProposerRecords(boolean session, long id) {
			this.session = session;
			this.id = id;
		}

		/** Key of the record of request {@code sequence} of this proposer or session. */
		Keys recordKey(long sequence) {
			return session ? NodeStore.sessionRequestKey(id, sequence) : NodeStore.requestKey(id, sequence);
		}
	}

	private final Long2ObjectOpenHashMap<ProposerRecords> proposers = new Long2ObjectOpenHashMap<>();
	/**
	 * Every client session in the database, loaded before the first entry is applied ({@link #loadSessions}). A session
	 * expired at the current {@link #clock} stays here until {@link #sweepSessions} deletes it, and is treated as gone.
	 */
	private final Long2ObjectOpenHashMap<ProposerRecords> sessions = new Long2ObjectOpenHashMap<>();
	/**
	 * The latest leader time stamped on an applied entry ({@link LogEntryCodec#stamp}), the same on every replica at
	 * the same log position: the only clock that session expiry reads. It never goes back, even if a new leader's clock
	 * is behind. Written ({@link NodeStore#CLOCK}) with the applied position of the entry that moved it.
	 */
	private long clock;
	/** {@link #clock} moved since it was last written. */
	private boolean clockMoved;
	/** When {@link #sweepSessions} looks for expired sessions next: only this replica's pace, not replicated state. */
	private long nextSweepMillis;
	/** How often, in leader time, {@link #sweepSessions} looks for expired sessions. */
	private static final long SWEEP_EVERY_MILLIS = 1000;

	/** True between {@link #beginGroup} and {@link #endGroup}. */
	private boolean grouping;
	/** The open local transaction of the current group, or null if none is open. */
	private @Nullable ApplyWriter groupTx;
	/** The position of the last entry written to {@link #groupTx}. */
	private @Nullable TermIndex groupPosition;
	/** Proposers and sessions whose floor moved in the current group: their records are purged once it commits. */
	private final List<ProposerRecords> groupPurges = new ArrayList<>();

	/** The result recorded for {@code request} by an earlier application, or null if it was never applied. */
	@Nullable ApplyResult recordedResult(Request request) {
		return recordedResult(store, recordKey(request));
	}

	/**
	 * The result recorded in {@code store} for write {@code id} of a client session, or null if there is none. Safe
	 * from any thread.
	 */
	static @Nullable ApplyResult recordedResult(NodeStore store, ClientRequestId id) {
		return recordedResult(store, NodeStore.sessionRequestKey(id.sessionId(), id.sequence()));
	}

	private static @Nullable ApplyResult recordedResult(NodeStore store, Keys key) {
		Buf recorded = store.db().get(0L, store.metaColumnId(), key, RequestType.<Buf>current());
		return recorded == null ? null : ResultCodec.decode(recorded.toByteArray());
	}

	/** Key of the record of {@code request}: under its client session if it has one, else under its proposer. */
	static Keys recordKey(Request request) {
		var client = request.client();
		return client != null ? NodeStore.sessionRequestKey(client.sessionId(), client.sequence())
				: NodeStore.requestKey(request.proposerId(), request.sequence());
	}

	/** The number that identifies {@code request} within its session, or else within its proposer. */
	private static long sequence(Request request) {
		return request.client() != null ? request.client().sequence() : request.sequence();
	}

	/**
	 * The result recorded in {@code store} for request {@code sequence} of {@code proposerId}, or null if there is
	 * none. Safe from any thread.
	 */
	static @Nullable ApplyResult recordedResult(NodeStore store, long proposerId, long sequence) {
		return recordedResult(store, NodeStore.requestKey(proposerId, sequence));
	}

	/** Applies one request alone, see {@link #applyBatch}. */
	ApplyResult apply(Request request, TermIndex position) {
		return applyBatch(List.of(request), position).getFirst();
	}

	/** Applies one request alone, stamped by the leader with {@code leaderTimeMillis}, see {@link #applyBatch}. */
	ApplyResult apply(Request request, TermIndex position, long leaderTimeMillis) {
		return applyBatch(List.of(request), position, leaderTimeMillis).getFirst();
	}

	/** {@link #applyBatch(List, TermIndex, long)} of an entry without a leader time. */
	List<ApplyResult> applyBatch(List<Request> requests, TermIndex position) {
		return applyBatch(requests, position, -1);
	}

	/**
	 * Applies the requests of one log entry at {@code position}, in order, and returns their results.
	 *
	 * <p>Data writes (and transaction commits) of a batch run in one local transaction that also writes their
	 * records, the proposer's floor and the applied position: one database write per log entry. If one of them is
	 * rejected for a logical reason, its partial writes must not stay, so the batch is applied again one request at a
	 * time, each in its own transaction. Any other entry (schema changes, {@code deleteRange}, genesis) travels
	 * alone, see {@link #isBatchable}.</p>
	 *
	 * <p>A request numbered below its proposer's floor is a late copy of a request the proposer already finished
	 * with (answered, or given up as unknown): it is never executed, so cleaning up records cannot make an old
	 * request run again. A request with a record is a copy of one that already took effect: it gets the recorded
	 * result.</p>
	 *
	 * <p>A request of a client session is identified by the session's numbering, with the session's own floor and
	 * records. A session that is unknown, or expired at this entry's leader time ({@code leaderTimeMillis}, -1 if the
	 * entry has none), rejects its requests with {@code SESSION_EXPIRED}, which is not recorded.</p>
	 */
	List<ApplyResult> applyBatch(List<Request> requests, TermIndex position, long leaderTimeMillis) {
		try {
			purgeAllProposers();
			return applyRouted(requests, position, leaderTimeMillis, null, false);
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to apply entry " + position, ex);
		}
	}

	/**
	 * {@link #applyBatch} after the first-entry setup: into the open segment of a partitioned apply if the entry
	 * can run in lanes (and {@code serial} is false), else serially as before. {@code pending}, if not null, is the
	 * list an earlier attempt returned for this entry (see {@link #finishSegment}): it receives the results.
	 */
	private List<ApplyResult> applyRouted(List<Request> requests, TermIndex position, long leaderTimeMillis,
			@Nullable PendingResults pending, boolean serial) {
		int[] requestLanes = null;
		if (!serial && grouping && lanes != null && !(requests.size() == 1 && !isBatchable(requests.getFirst().entry()))) {
			requestLanes = lanesOf(requests);
		}
		if (requestLanes == null) {
			// Everything before this entry is committed or in the group's transaction first: it may read any of it.
			finishSegment();
		}
		long clockBefore = clock;
		boolean clockMovedBefore = clockMoved;
		if (leaderTimeMillis > clock) {
			clock = leaderTimeMillis;
			clockMoved = true;
		}
		List<ApplyResult> results;
		if (requestLanes != null) {
			results = applySegmented(requests, position, leaderTimeMillis, clockBefore, clockMovedBefore, requestLanes,
					pending);
		} else if (requests.size() == 1 && !isBatchable(requests.getFirst().entry())) {
			commitGroup();
			results = List.of(applyAlone(requests.getFirst(), position));
		} else {
			results = grouping ? applyGrouped(requests, position) : applyData(requests, position, true);
		}
		if (groupTx == null && segment == null) {
			sweepSessions();
		}
		if (pending != null && results != pending) {
			pending.set(results);
			return pending;
		}
		return results;
	}

	/**
	 * Deletes a column so that running the deletion again at the same position gives the result of the first run. A
	 * crash between the deletion (written to the MANIFEST at once) and its record (written to the WAL after it) makes
	 * the entry run again, and the column is then gone already: whether it existed is decided before the first run
	 * and kept, synced ({@link NodeStore#DELETION}). The marker is never deleted: it names its log index, and the
	 * next deletion overwrites it; one left behind is never read again, since no later entry has its index.
	 */
	private ApplyResult deleteColumn(LogEntry entry, TermIndex position) {
		var db = store.db();
		var markerKey = NodeStore.key(NodeStore.DELETION);
		Buf marker = db.get(0L, store.metaColumnId(), markerKey, RequestType.<Buf>current());
		boolean existed;
		if (marker != null && java.nio.ByteBuffer.wrap(marker.toByteArray()).getLong() == position.getIndex()) {
			existed = marker.toByteArray()[Long.BYTES] == 1;
		} else {
			existed = switch (entry) {
				// The internal column is refused whether it exists or not.
				case LogEntry.DeleteColumn delete -> delete.columnId() != store.metaColumnId()
						&& columnExists(delete.columnId());
				case LogEntry.DeleteColumnIfExists delete -> !NodeStore.META_COLUMN.equals(delete.name())
						&& db.getAllColumnDefinitions().containsKey(delete.name());
				default -> throw new IllegalArgumentException("Not a column deletion: " + entry);
			};
			db.put(0L, store.metaColumnId(), markerKey, Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES + 1)
					.putLong(position.getIndex()).put((byte) (existed ? 1 : 0)).array()), RequestType.none());
			syncWal();
		}
		var outcome = applyLogical(() -> execute(null, entry));
		if (!existed) {
			// Not found, now as on the first run.
			return outcome;
		}
		return entry instanceof LogEntry.DeleteColumnIfExists ? new ApplyResult.Ok(true) : ApplyResult.OK_NULL;
	}

	/** The id of the column a {@code CreateColumn} names, if it exists with the same schema; null otherwise. */
	private @Nullable Long existingColumn(LogEntry entry) {
		if (!(entry instanceof LogEntry.CreateColumn create) || NodeStore.META_COLUMN.equals(create.name())) {
			return null;
		}
		var schema = store.db().getAllColumnDefinitions().get(create.name());
		if (schema == null || !schema.equals(create.schema())) {
			return null;
		}
		try {
			return store.db().getColumnId(create.name());
		} catch (RocksDBException ex) {
			return null;
		}
	}

	private boolean columnExists(long columnId) {
		var db = store.db();
		for (String name : db.getAllColumnDefinitions().keySet()) {
			try {
				if (db.getColumnId(name) == columnId) {
					return true;
				}
			} catch (RocksDBException ex) {
				// Deleted meanwhile.
			}
		}
		return false;
	}

	/** True for entries that may share a log entry, and a local transaction, with others. */
	static boolean isBatchable(LogEntry entry) {
		return (entry instanceof LogEntry.Write && !(entry instanceof LogEntry.DeleteRange))
				|| entry instanceof LogEntry.Guarded || entry instanceof LogEntry.VerifyColumn;
	}

	private ApplyResult applyAlone(Request request, TermIndex position) {
		var records = owner(request);
		if (records == null) {
			inApplyTransaction(position, tx -> markApplied(tx, position));
			return SESSION_GONE;
		}
		long floor = records.floor;
		if (sequence(request) < floor) {
			inApplyTransaction(position, tx -> markApplied(tx, position));
			return STALE_COPY;
		}
		if (records.recorded.contains(sequence(request))) {
			inApplyTransaction(position, tx -> markApplied(tx, position));
			return requireRecorded(request);
		}
		var entry = request.entry();
		var existingId = existingColumn(entry);
		if (existingId != null || entry instanceof LogEntry.RegisterClient) {
			// Creating a column that exists with the same schema changes nothing: it needs no flush nor fsync. Neither
			// does registering a client session, which only writes the meta column. The session id is the entry's index.
			ApplyResult outcome = new ApplyResult.Ok(existingId != null ? existingId : position.getIndex());
			finishUnlogged(records, request, outcome, position);
			return outcome;
		}
		if (entry instanceof LogEntry.DeleteRange) {
			// A plain write, which changes no MANIFEST: neither a flush nor the WAL. The range deletion skips the WAL
			// like the apply transactions, after the group before it committed (see applyBatch), and its record and
			// position follow it. With atomic flush a crash keeps a prefix of these writes: the deletion without its
			// record makes the entry run again, and deleting the same range again removes nothing that the later
			// writes (lost with the record) could have added, with the same result as the first run.
			var outcome = ResultCodec.bounded(applyLogical(() -> execute(null, entry)));
			finishUnlogged(records, request, outcome, position);
			return outcome;
		}
		// Schema changes write the RocksDB MANIFEST synchronously, while their schema rows, record and position go to
		// the WAL. Make everything before them durable first (the data writes applied without the WAL included) and
		// everything after them durable at once, so a crash cannot keep one without the other.
		makeDurable();
		ApplyResult executed;
		if (entry instanceof LogEntry.DeleteColumn || entry instanceof LogEntry.DeleteColumnIfExists) {
			executed = deleteColumn(entry, position);
		} else {
			executed = applyLogical(() -> execute(null, entry));
		}
		var outcome = ResultCodec.bounded(executed);
		// Record, floor and position in one logged write: all or none of them survive a crash.
		inLoggedTransaction(position, tx -> finish(tx, records, request, outcome, List.of(request), position, true));
		syncWal();
		committed(List.of(request), List.of(request));
		purge(List.of(request));
		return outcome;
	}

	/**
	 * Writes the record, floor and position of an entry applied alone without the WAL: into the open group, if any,
	 * else in a transaction of its own.
	 */
	private void finishUnlogged(ProposerRecords records, Request request, ApplyResult outcome, TermIndex position) {
		var entry = request.entry();
		TransactionWrites writes = tx -> {
			if (entry instanceof LogEntry.RegisterClient register) {
				tx.put(store.metaColumnId(), NodeStore.sessionKey(position.getIndex()),
						NodeStore.encodeSession(register.timeoutMillis(), clock, 0), RequestType.none());
			}
			finish(tx, records, request, outcome, List.of(request), position, true);
		};
		if (grouping) {
			writes.write(openGroupTx(position));
		} else {
			inApplyTransaction(position, writes);
		}
		if (entry instanceof LogEntry.RegisterClient register) {
			var session = new ProposerRecords(true, position.getIndex());
			session.timeoutMillis = register.timeoutMillis();
			session.lastActiveMillis = clock;
			session.loaded = true;
			sessions.put(session.id, session);
		}
		committed(List.of(request), List.of(request));
		if (grouping) {
			groupPurges.addAll(owners(List.of(request)));
		} else {
			purge(List.of(request));
		}
	}

	/**
	 * Applies data requests in one local transaction. {@code markPosition} false leaves the applied position alone,
	 * for all but the last request of an entry applied one request at a time: the entry is then marked applied only
	 * with its last request, and a crash in between makes it run again, the requests already recorded getting their
	 * recorded results.
	 */
	private List<ApplyResult> applyData(List<Request> requests, TermIndex position, boolean markPosition) {
		var batch = classify(requests);
		var toRun = batch.toRun();
		var outcomes = new ApplyResult[toRun.size()];
		long openStart = System.nanoTime();
		var tx = beginApply();
		record(openTime, openStart);
		boolean open = true;
		try {
			long executeStart = System.nanoTime();
			for (int j = 0; j < toRun.size(); j++) {
				var request = toRun.get(j);
				outcomes[j] = applyLogical(() -> execute(tx, request.entry()));
				if (outcomes[j] instanceof ApplyResult.Failed failed && toRun.size() > 1) {
					// Its partial writes are in the transaction with the others' writes: start over one at a time.
					tx.close();
					open = false;
					return applyOneByOne(requests, position, markPosition);
				}
			}
			if (toRun.size() == 1 && outcomes[0] instanceof ApplyResult.Failed) {
				// A rejected entry has no effects: roll back its partial writes, then record the rejection.
				tx.close();
				open = false;
				var rejected = toRun.getFirst();
				outcomes[0] = ResultCodec.bounded(outcomes[0]);
				inApplyTransaction(position, recordTx -> finish(recordTx, owner(rejected), rejected, outcomes[0],
						requests, position, markPosition));
			} else {
				for (int j = 0; j < toRun.size(); j++) {
					outcomes[j] = record(tx, owner(toRun.get(j)), toRun.get(j), outcomes[j]);
				}
				finishBatch(tx, requests, position, markPosition);
				record(executeTime, executeStart);
				long commitStart = System.nanoTime();
				// Closing it afterwards, in the finally block, does nothing once it committed.
				boolean committed = tx.commit();
				record(commitTime, commitStart);
				if (!committed) {
					// Only the apply thread writes the database, so an optimistic conflict means local corruption.
					throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
				}
				committed("entry");
			}
		} finally {
			if (open) {
				tx.close();
			}
		}
		committed(requests, toRun);
		purge(requests);
		return batch.complete(outcomes);
	}

	/** Identity of a request at apply: its session's numbering if it has one, else its proposer's. */
	private record Identity(boolean session, long id, long sequence) {
		static Identity of(Request request) {
			var client = request.client();
			return client != null ? new Identity(true, client.sessionId(), client.sequence())
					: new Identity(false, request.proposerId(), request.sequence());
		}
	}

	/**
	 * The requests of one entry, sorted out before anything runs: those already answered (a late copy below the
	 * floor, a recorded one, or one of a session that is gone) have their {@link #results}; {@link #toRun} are to
	 * apply, at {@link #runIndexes} in the entry; {@link #duplicates} are second copies of one of them in the same
	 * entry, which get its result.
	 */
	private record Classified(ApplyResult[] results, List<Request> toRun, IntArrayList runIndexes,
			IntArrayList duplicates, List<Request> requests) {

		/** The results of the entry, given those of {@link #toRun}, in its order. */
		List<ApplyResult> complete(ApplyResult[] outcomes) {
			for (int j = 0; j < toRun.size(); j++) {
				results[runIndexes.getInt(j)] = outcomes[j];
			}
			for (int d = 0; d < duplicates.size(); d++) {
				int i = duplicates.getInt(d);
				var identity = Identity.of(requests.get(i));
				for (int j = 0; j < toRun.size(); j++) {
					if (Identity.of(toRun.get(j)).equals(identity)) {
						results[i] = outcomes[j];
						break;
					}
				}
			}
			return List.of(results);
		}
	}

	/**
	 * Sorts out the requests of one entry, see {@link Classified}. A request is identified by its client session's
	 * numbering if it has one ({@link #owner}), else by its proposer's.
	 */
	private Classified classify(List<Request> requests) {
		long proposerId = requests.getFirst().proposerId();
		long completedBelow = requests.getFirst().completedBelow();
		var results = new ApplyResult[requests.size()];
		var toRun = new ArrayList<Request>(requests.size());
		var runIndexes = new IntArrayList(requests.size());
		var duplicates = new IntArrayList(0);
		var seen = new java.util.HashSet<Identity>();
		for (int i = 0; i < requests.size(); i++) {
			var request = requests.get(i);
			if (request.proposerId() != proposerId || request.completedBelow() != completedBelow) {
				throw new IllegalArgumentException("A batch holds requests of one proposer, with one completedBelow");
			}
			var owner = owner(request);
			if (owner == null) {
				results[i] = SESSION_GONE;
			} else if (sequence(request) < owner.floor) {
				results[i] = STALE_COPY;
			} else if (owner.recorded.contains(sequence(request))) {
				results[i] = requireRecorded(request);
			} else if (!seen.add(Identity.of(request))) {
				duplicates.add(i);
			} else {
				if (!isBatchable(request.entry())) {
					throw new IllegalArgumentException("A batch cannot contain " + request.entry().getClass().getSimpleName());
				}
				toRun.add(request);
				runIndexes.add(i);
			}
		}
		return new Classified(results, toRun, runIndexes, duplicates, requests);
	}

	/**
	 * Starts a group: the data entries applied until {@link #endGroup} share one local transaction, committed once,
	 * so that its fixed costs (the commit, the floor and position writes) are paid once per group instead of once per
	 * entry. The group holds a consistent prefix of the log at any moment: each entry's writes, record, floor and
	 * position are in it, and a crash keeps all of the group or none. The in-memory records are updated as each entry
	 * is applied, so that the next entries of the group see them; a group that fails to commit stops the replica.
	 * Anything that cannot join the group (an entry applied alone, a rejected request) first commits it.
	 */
	void beginGroup() {
		grouping = true;
	}

	/** Commits the current group, see {@link #beginGroup}. */
	void endGroup() {
		try {
			commitGroup();
		} finally {
			grouping = false;
		}
	}

	/** Drops the current group without committing it: the replica stops after this. */
	void abortGroup() {
		grouping = false;
		groupPurges.clear();
		var open = segment;
		segment = null;
		if (open != null) {
			discardSegment(open);
		}
		var tx = groupTx;
		groupTx = null;
		if (tx != null) {
			try {
				tx.close();
			} catch (RuntimeException ex) {
				// The replica stops anyway.
			}
		}
	}

	private ApplyWriter openGroupTx(TermIndex position) {
		if (groupTx == null) {
			long openStart = System.nanoTime();
			groupTx = beginApply();
			record(openTime, openStart);
		}
		groupPosition = position;
		return groupTx;
	}

	/** Commits the writes of the group so far, if any, then deletes the records their floors passed. */
	private void commitGroup() {
		finishSegment();
		commitGroupTx("group");
		for (var records : groupPurges) {
			purge(records);
		}
		groupPurges.clear();
		sweepSessions();
	}

	/**
	 * Commits {@link #groupTx}, if open, without the deletions that follow a group's commit: for the partitioned
	 * apply, which commits a prefix of the group when it changes between lanes and the serial path.
	 */
	private void commitGroupTx(String what) {
		var tx = groupTx;
		if (tx != null) {
			groupTx = null;
			long commitStart = System.nanoTime();
			boolean committed;
			try {
				committed = tx.commit();
				if (!committed) {
					tx.close();
				}
			} catch (RuntimeException ex) {
				throw new ReplicaFailedException("Failed to commit the entries applied up to " + groupPosition, ex);
			}
			record(commitTime, commitStart);
			if (!committed) {
				// Only the apply thread writes the database, so an optimistic conflict means local corruption.
				throw new ReplicaFailedException("Local apply transaction conflicted at " + groupPosition);
			}
			committed(what);
		}
	}

	/**
	 * {@link #applyData} inside the group's transaction. A rejected request rolls the entry's writes back to the
	 * savepoint taken before it, commits the group, and applies the entry again on its own, as outside a group.
	 */
	private List<ApplyResult> applyGrouped(List<Request> requests, TermIndex position) {
		var batch = classify(requests);
		var toRun = batch.toRun();
		var tx = openGroupTx(position);
		long executeStart = System.nanoTime();
		tx.setSavePoint();
		var outcomes = new ApplyResult[toRun.size()];
		for (int j = 0; j < toRun.size(); j++) {
			var request = toRun.get(j);
			outcomes[j] = applyLogical(() -> execute(tx, request.entry()));
			if (outcomes[j] instanceof ApplyResult.Failed) {
				tx.rollbackToSavePoint();
				commitGroup();
				return applyData(requests, position, true);
			}
		}
		for (int j = 0; j < toRun.size(); j++) {
			outcomes[j] = record(tx, owner(toRun.get(j)), toRun.get(j), outcomes[j]);
		}
		finishBatch(tx, requests, position, true);
		record(executeTime, executeStart);
		committed(requests, toRun);
		groupPurges.addAll(owners(requests));
		return batch.complete(outcomes);
	}

	/**
	 * The slow path of {@link #applyData}: each request in its own local transaction, all at {@code position}, which
	 * only the last one records as applied.
	 */
	private List<ApplyResult> applyOneByOne(List<Request> requests, TermIndex position, boolean markPosition) {
		var results = new ArrayList<ApplyResult>(requests.size());
		for (int i = 0; i < requests.size(); i++) {
			boolean last = i == requests.size() - 1;
			results.add(applyData(List.of(requests.get(i)), position, markPosition && last).getFirst());
		}
		return results;
	}

	/**
	 * The in-memory view of the meta column for {@code proposerId}, loaded on first use. Records below the floor are
	 * left out: they may still be in the database (see {@link #purge}), but are never read.
	 */
	private ProposerRecords records(long proposerId) {
		var records = proposers.get(proposerId);
		if (records == null) {
			records = new ProposerRecords(false, proposerId);
			records.floor = floor(proposerId);
			// Only this applier moved the floor since purgeAllProposers deleted everything below it.
			records.purgedBelow = records.floor;
			loadRecorded(records);
			proposers.put(proposerId, records);
		}
		return records;
	}

	private void loadRecorded(ProposerRecords records) {
		try (var keys = store.db().getRange(0L, store.metaColumnId(), records.recordKey(records.floor),
				records.recordKey(Long.MAX_VALUE), false, RequestType.<KV>allInRange())) {
			var recorded = records.recorded;
			keys.forEach(kv -> recorded.add(NodeStore.requestSequence(kv.keys())));
		}
		records.loaded = true;
	}

	/**
	 * The records that identify {@code request}: its client session's if it has one, else its proposer's. Null if its
	 * session is unknown, or expired at the current {@link #clock}: then it must not run.
	 */
	private @Nullable ProposerRecords owner(Request request) {
		var client = request.client();
		if (client == null) {
			return records(request.proposerId());
		}
		var session = sessions.get(client.sessionId());
		if (session == null || expired(session)) {
			return null;
		}
		if (!session.loaded) {
			loadRecorded(session);
		}
		return session;
	}

	private boolean expired(ProposerRecords session) {
		return clock - session.lastActiveMillis > session.timeoutMillis;
	}

	/** The proposer of {@code requests} and the live sessions of their client requests, each once. */
	private List<ProposerRecords> owners(List<Request> requests) {
		var owners = new ArrayList<ProposerRecords>(1);
		owners.add(records(requests.getFirst().proposerId()));
		for (var request : requests) {
			if (request.client() != null) {
				var session = owner(request);
				if (session != null && !owners.contains(session)) {
					owners.add(session);
				}
			}
		}
		return owners;
	}

	/**
	 * Updates the in-memory records after the database committed the requests {@code applied} of the entry
	 * {@code requests}, the new floors, and the sessions' activity.
	 */
	private void committed(List<Request> requests, List<Request> applied) {
		var undo = undoLog;
		for (var request : applied) {
			var owner = owner(request);
			if (owner != null) {
				long sequence = sequence(request);
				if (owner.recorded.add(sequence) && undo != null) {
					undo.add(() -> owner.recorded.remove(sequence));
				}
			}
		}
		raiseFloor(records(requests.getFirst().proposerId()), requests.getFirst().completedBelow());
		for (var request : requests) {
			var client = request.client();
			if (client != null) {
				var session = owner(request);
				if (session != null) {
					if (undo != null) {
						long lastActive = session.lastActiveMillis;
						undo.add(() -> session.lastActiveMillis = lastActive);
					}
					session.lastActiveMillis = clock;
					raiseFloor(session, client.completedBelow());
				}
			}
		}
	}

	private void raiseFloor(ProposerRecords records, long completedBelow) {
		if (completedBelow > records.floor) {
			var undo = undoLog;
			if (undo != null) {
				long floor = records.floor;
				var dropped = new it.unimi.dsi.fastutil.longs.LongArrayList(records.recorded.headSet(completedBelow));
				undo.add(() -> {
					records.floor = floor;
					records.recorded.addAll(dropped);
				});
			}
			records.floor = completedBelow;
			records.recorded.headSet(completedBelow).clear();
		}
	}

	/** {@link #purge(ProposerRecords)} of the proposer and sessions of {@code requests}. */
	private void purge(List<Request> requests) {
		for (var owner : owners(requests)) {
			purge(owner);
		}
	}

	/**
	 * Deletes the records below the floor of a proposer or session from the database, with one range deletion, once
	 * the floor moved {@link #purgeEverySequences} past the last one or {@link #purgeEveryBytes} of results were
	 * recorded since then. Records below the floor are never read: a copy below the floor is refused before any
	 * lookup, and {@link #records} does not load them. So they need not be deleted in the apply transaction, which
	 * would cost a write per request.
	 *
	 * <p>The deletion skips the WAL, like the apply transactions (see {@link #beginApply}), and runs after the
	 * transaction that wrote the floor committed. With atomic flush, a crash keeps a prefix of the writes made
	 * without the WAL: if the deletion survives, so does the floor that covers it, together with every entry applied
	 * before it, so no entry that could look up one of those records is replayed. If it is lost, the records come
	 * back below a floor that covers them, and stay unread until a later deletion. It must not go through the WAL:
	 * it could then survive without the floor, and a replayed copy of a request whose record it deleted would run
	 * again.</p>
	 */
	private void purge(ProposerRecords records) {
		if (records.floor <= records.purgedBelow || (records.floor - records.purgedBelow < purgeEverySequences
				&& records.recordedBytes < purgeEveryBytes)) {
			return;
		}
		store.db().deleteRangeUnlogged(store.metaColumnId(), records.recordKey(records.purgedBelow),
				records.recordKey(records.floor));
		records.purgedBelow = records.floor;
		records.recordedBytes = 0;
	}

	/**
	 * Loads every client session, and the clock, before this applier applies its first entry, and deletes the records
	 * below each session's floor, as {@link #purgeAllProposers} does for proposers.
	 */
	private void loadSessions() {
		var db = store.db();
		long metaColumn = store.metaColumnId();
		Buf clockValue = db.get(0L, metaColumn, NodeStore.key(NodeStore.CLOCK), RequestType.<Buf>current());
		clock = clockValue == null ? 0 : java.nio.ByteBuffer.wrap(clockValue.toByteArray()).getLong();
		try (var rows = db.getRange(0L, metaColumn, NodeStore.key(NodeStore.SESSION),
				NodeStore.key((byte) (NodeStore.SESSION + 1)), false, RequestType.<KV>allInRange())) {
			rows.forEach(kv -> {
				var fields = NodeStore.decodeSession(kv.value());
				var session = new ProposerRecords(true, NodeStore.sessionId(kv.keys()));
				session.timeoutMillis = fields[0];
				session.lastActiveMillis = fields[1];
				session.floor = fields[2];
				session.purgedBelow = session.floor;
				sessions.put(session.id, session);
			});
		}
		for (var session : sessions.values()) {
			var start = session.recordKey(0);
			var end = session.recordKey(session.floor);
			if (session.floor > 0 && db.reduceRange(0L, metaColumn, start, end, false,
					RequestType.<KV>firstAndLast()).first() != null) {
				db.deleteRangeUnlogged(metaColumn, start, end);
			}
		}
	}

	/**
	 * Deletes the sessions expired at the current {@link #clock}, with their records. Only after the transaction that
	 * moved the clock past their expiry committed, and without the WAL: as for {@link #purge}, a crash keeps a
	 * deletion only together with that clock, and a session expired at a clock stays expired, since the clock never
	 * goes back. A deletion that a crash loses leaves a session that is expired anyway, until a later sweep. So when
	 * it runs is up to each replica: what a request sees depends only on the clock and the session's row.
	 */
	private void sweepSessions() {
		if (clock < nextSweepMillis || sessions.isEmpty()) {
			return;
		}
		nextSweepMillis = clock + SWEEP_EVERY_MILLIS;
		var db = store.db();
		long metaColumn = store.metaColumnId();
		for (var it = sessions.values().iterator(); it.hasNext(); ) {
			var session = it.next();
			if (expired(session)) {
				// The row first: a reader that finds no record of a write must not find its session alive (it would take
				// the write for never applied).
				db.deleteRangeUnlogged(metaColumn, NodeStore.sessionKey(session.id), NodeStore.sessionKey(session.id + 1));
				db.deleteRangeUnlogged(metaColumn, NodeStore.sessionRequestKey(session.id, 0),
						NodeStore.sessionRequestKey(session.id + 1, 0));
				it.remove();
			}
		}
	}

	/**
	 * Deletes the records below the floor of every proposer in the database, once, before this applier applies its
	 * first entry. {@link #purge} only runs for a proposer whose own entries move its floor, and a proposer id lives
	 * as long as its node process: without this, the records a stopped process left below its floor would stay.
	 * Each deletion is like the one of {@link #purge}, after the floor it covers was written, so a crash keeps it
	 * only together with that floor.
	 */
	private void purgeAllProposers() {
		if (purgedAllProposers) {
			return;
		}
		loadSessions();
		var db = store.db();
		long metaColumn = store.metaColumnId();
		var floors = new Long2LongOpenHashMap();
		try (var keys = db.getRange(0L, metaColumn, NodeStore.key(NodeStore.FLOOR),
				NodeStore.key((byte) (NodeStore.FLOOR + 1)), false, RequestType.<KV>allInRange())) {
			keys.forEach(kv -> floors.put(NodeStore.floorProposerId(kv.keys()),
					java.nio.ByteBuffer.wrap(kv.value().toByteArray()).getLong()));
		}
		for (var floor : floors.long2LongEntrySet()) {
			var start = NodeStore.requestKey(floor.getLongKey(), 0);
			var end = NodeStore.requestKey(floor.getLongKey(), floor.getLongValue());
			// Skips proposers with nothing left below their floor, so that restarts do not pile up range deletions.
			if (floor.getLongValue() > 0 && db.reduceRange(0L, metaColumn, start, end, false,
					RequestType.<KV>firstAndLast()).first() != null) {
				db.deleteRangeUnlogged(metaColumn, start, end);
			}
		}
		purgedAllProposers = true;
	}

	private ApplyResult requireRecorded(Request request) {
		var open = segment;
		if (open != null && open.pending.containsKey(Identity.of(request))) {
			// Applied by an earlier entry of the open segment, whose result is known only once its lane ran it.
			return PENDING_COPY;
		}
		// Through the open group transaction, if any: the record may have been written by an earlier entry of it.
		var key = recordKey(request);
		var tx = groupTx;
		Buf value = tx != null ? tx.get(store.metaColumnId(), key, RequestType.<Buf>current())
				: store.db().get(0L, store.metaColumnId(), key, RequestType.<Buf>current());
		var recorded = value == null ? null : ResultCodec.decode(value.toByteArray());
		if (recorded == null) {
			throw new ReplicaFailedException("Request " + Identity.of(request) + " is marked applied, but its record is"
					+ " missing");
		}
		return recorded;
	}

	/** Records a position that carries no data entry (configuration changes, leader no-ops). */
	void markApplied(TermIndex position) {
		try {
			if (grouping && lanes != null) {
				addMark(position);
				return;
			}
			if (grouping) {
				markApplied(openGroupTx(position), position);
				return;
			}
			inApplyTransaction(position, tx -> markApplied(tx, position));
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to record applied position " + position, ex);
		}
	}

	/**
	 * A local transaction for the writes of the apply thread. It skips the RocksDB WAL: the Raft log already holds
	 * every entry, so writing them again to the WAL only doubles the disk traffic. A crash loses what was not flushed
	 * yet, and the replica replays it from the Raft log: the database is opened with atomic flush (see
	 * {@link NodeStore}), so what survives is a consistent point of every column, the applied position included, and
	 * the log is only ever purged up to a snapshot, which flushes. Any write that does go through the WAL must first
	 * call {@link #makeDurable}, or recovering that WAL could keep it without the writes before it.
	 */
	private ApplyWriter beginApply() {
		return store.db().openApplyWriter(true);
	}

	private interface TransactionWrites {
		void write(ApplyWriter tx);
	}

	/** Runs {@code writes} in an apply transaction ({@link #beginApply}) and commits it. */
	private void inApplyTransaction(TermIndex position, TransactionWrites writes) {
		inTransaction(position, writes, true);
	}

	/** Like {@link #inApplyTransaction}, through the WAL: only after {@link #makeDurable}. */
	private void inLoggedTransaction(TermIndex position, TransactionWrites writes) {
		inTransaction(position, writes, false);
	}

	private void inTransaction(TermIndex position, TransactionWrites writes, boolean unlogged) {
		var db = store.db();
		try (var tx = unlogged ? beginApply() : db.openApplyWriter(false)) {
			writes.write(tx);
			if (!tx.commit()) {
				throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
			}
		}
		committed("entry");
	}

	/** Flushes the memtables, so that the writes applied without the WAL become durable before a logged one. */
	private void makeDurable() {
		try {
			store.db().flush();
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to flush the database", ex);
		}
	}

	private static final ApplyResult STALE_COPY = new ApplyResult.Failed(RocksDBErrorType.INVALID_REQUEST,
			"Late copy of a request that its proposer already finished");
	/** Not recorded: there is no session to record it under. The same on every replica, whatever was swept. */
	static final ApplyResult SESSION_GONE = new ApplyResult.Failed(RocksDBErrorType.SESSION_EXPIRED,
			"The client session is unknown or expired; this copy of the write was not applied");

	private long floor(long proposerId) {
		Buf value = store.db().get(0L, store.metaColumnId(), NodeStore.floorKey(proposerId), RequestType.<Buf>current());
		return value == null ? 0 : java.nio.ByteBuffer.wrap(value.toByteArray()).getLong();
	}

	private void syncWal() {
		try {
			store.db().getDb().get().flushWal(true);
		} catch (org.rocksdb.RocksDBException e) {
			throw new ReplicaFailedException("Failed to sync the WAL", e);
		}
	}

	private Object execute(@Nullable ApplyWriter tx, LogEntry entry) {
		var db = store.db();
		if (entry instanceof LogEntry.Write write) {
			requireUserColumn(write.columnId());
		}
		return switch (entry) {
			case LogEntry.Put put ->
					requireTransaction(tx, entry).put(put.columnId(), put.keys(), put.value(), putType(put.requestType()));
			case LogEntry.PutMulti put -> requireTransaction(tx, entry)
					.putMulti(put.columnId(), put.keys(), put.values(), putType(put.requestType()));
			case LogEntry.Delete delete ->
					requireTransaction(tx, entry).delete(delete.columnId(), delete.keys(), deleteType(delete.requestType()));
			case LogEntry.DeleteMulti delete -> requireTransaction(tx, entry)
					.deleteMulti(delete.columnId(), delete.keys(), deleteType(delete.requestType()));
			case LogEntry.Merge merge -> requireTransaction(tx, entry)
					.merge(merge.columnId(), merge.keys(), merge.value(), mergeType(merge.requestType()));
			case LogEntry.MergeMulti merge -> requireTransaction(tx, entry)
					.mergeMulti(merge.columnId(), merge.keys(), merge.values(), mergeType(merge.requestType()));
			case LogEntry.DeleteRange range -> {
				requireNoTransaction(tx, entry);
				// Without the WAL, like every data write of the apply: see applyAlone.
				db.deleteRangeUnlogged(range.columnId(), range.start(), range.end());
				yield null;
			}
			case LogEntry.CreateColumn create -> {
				requireNoTransaction(tx, entry);
				if (NodeStore.META_COLUMN.equals(create.name())) {
					throw new IllegalArgumentException("Reserved column name: " + create.name());
				}
				yield db.createColumn(create.name(), create.schema());
			}
			case LogEntry.DeleteColumn delete -> {
				requireNoTransaction(tx, entry);
				requireUserColumn(delete.columnId());
				db.deleteColumn(delete.columnId());
				yield null;
			}
			case LogEntry.DeleteColumnIfExists delete -> {
				requireNoTransaction(tx, entry);
				if (NodeStore.META_COLUMN.equals(delete.name())) {
					throw new IllegalArgumentException("Reserved column name: " + delete.name());
				}
				yield db.deleteColumnIfExists(delete.name());
			}
			case LogEntry.UploadMergeOperator upload -> {
				requireNoTransaction(tx, entry);
				try {
					yield db.uploadMergeOperator(upload.name(), upload.className(), upload.jarData());
				} catch (LinkageError badJar) {
					// Malformed or incompatible classes fail the same way on every replica: a rejection, not a local
					// failure that would stop them all. Unless the JVM ended a class initialization (out of memory).
					if (Failures.isJvmFailure(badJar)) {
						throw badJar;
					}
					throw new IllegalArgumentException("Merge operator " + upload.name() + " cannot be loaded: "
							+ Failures.describe(badJar), null);
				} catch (RocksDBException failed) {
					// An operator that cannot be instantiated (missing class, wrong type, a constructor that throws, an
					// Error included) is a PUT_INVALID_REQUEST whose failure is only suppressed; only a storage error or a
					// failure of the JVM below it is a local failure.
					if (isLocalFailure(failed)) {
						throw failed;
					}
					throw new IllegalArgumentException("Merge operator " + upload.name() + " rejected: "
							+ failed.getMessage(), null);
				}
			}
			case LogEntry.Guarded guarded -> {
				if (guarded.guard() != null) {
					var guard = guarded.guard();
					requireUserColumn(guard.columnId());
					// Through the transaction, so that an earlier entry of the same group is seen. Every replica sees the
					// same value at this log position, so all reach the same verdict.
					Buf current = requireTransaction(tx, entry).get(guard.columnId(), guard.keys(),
							RequestType.<Buf>current());
					if (!guard.matches(current)) {
						throw new RocksDBRetryException();
					}
				}
				Object last = null;
				for (LogEntry.Write write : guarded.writes()) {
					if (write instanceof LogEntry.DeleteRange) {
						throw new IllegalArgumentException("A transaction cannot contain deleteRange");
					}
					last = execute(tx, write);
				}
				yield guarded.singleResult() ? last : Boolean.TRUE;
			}
			case LogEntry.Genesis genesis -> {
				requireNoTransaction(tx, entry);
				applyGenesis(genesis);
				yield null;
			}
			case LogEntry.RegisterClient _ ->
					// Applied by applyAlone, which knows the entry's position, the session id.
					throw new IllegalStateException("A client registration is applied alone");
			case LogEntry.VerifyColumn verify -> {
				verifyColumn(verify);
				yield null;
			}
		};
	}

	private void requireUserColumn(long columnId) {
		if (columnId == store.metaColumnId()) {
			throw new IllegalArgumentException("Column " + columnId + " is internal to replication");
		}
	}

	/**
	 * The first of these entries binds the database to its cluster. Later ones change nothing: a batch of a former leader
	 * can reach the next one after that one proposed its own, with another id. Every replica applies them in the same
	 * order, so they all keep the same id. What finds a node of another cluster is {@code ClusterCheck}.
	 */
	private void applyGenesis(LogEntry.Genesis genesis) {
		if (store.clusterId() != null) {
			return;
		}
		boolean hasData = store.hasUserColumns();
		if (genesis.seeded() && !hasData) {
			throw new ReplicaFailedException("The cluster was seeded with an existing database, but this replica's"
					+ " database is empty, and it got the log from a leader without a snapshot. It cannot catch up this way."
					+ " Take this node out with remove-peer (restart it if the cluster has lost its majority), then wipe its"
					+ " data directory and add it again. A wiped node that is still a member gets no vote, so do not wipe it"
					+ " first");
		}
		if (!genesis.seeded() && hasData) {
			throw new ReplicaFailedException("The cluster started empty, but this replica's database has data");
		}
		store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_ID),
				Buf.wrap(genesis.clusterId().getBytes(StandardCharsets.UTF_8)), RequestType.none());
		if (genesis.seeded()) {
			store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.SEEDED), Buf.wrap(new byte[] {1}),
					RequestType.none());
		}
	}

	/**
	 * Stops this replica if the id the leader assigned to {@code name} belongs here to another column: writes meant
	 * for {@code name} would land in it. The column may have been deleted (and its name reused) since it was created,
	 * so a missing id or a different id for the name is not evidence of divergence.
	 */
	private void verifyColumn(LogEntry.VerifyColumn verify) {
		for (String name : store.db().getAllColumnDefinitions().keySet()) {
			long id;
			try {
				id = store.db().getColumnId(name);
			} catch (RocksDBException ex) {
				continue;
			}
			if (id == verify.columnId() && !name.equals(verify.name())) {
				throw new ReplicaFailedException("Column id " + id + " is " + verify.name() + " on the leader but " + name
						+ " on this replica: the replicas diverged");
			}
		}
	}

	/**
	 * Writes the result record of {@code request}, then the floors and sessions of its entry {@code requests} and the
	 * applied position, in transaction {@code tx}.
	 */
	private void finish(ApplyWriter tx, ProposerRecords records, Request request, ApplyResult result,
			List<Request> requests,
			TermIndex position, boolean markPosition) {
		record(tx, records, request, result);
		finishBatch(tx, requests, position, markPosition);
	}

	/**
	 * Writes the record of {@code request} and returns the result it holds, which the reply must carry too:
	 * {@code result} bounded by {@link ResultCodec#bounded}. A result that cannot be encoded, or too large for a
	 * reply by itself, would otherwise stop every replica here, at every restart. Bounded, any recorded result fits
	 * alone in a reply, so a copy of the request sent alone always gets it.
	 */
	private ApplyResult record(ApplyWriter tx, ProposerRecords records, Request request, ApplyResult result) {
		if (guardRejected != null && result instanceof ApplyResult.Failed failed
				&& failed.type() == RocksDBErrorType.UPDATE_RETRY) {
			// Counted here, not where the guard is checked: a rejected entry is applied up to three times.
			guardRejected.record(1);
		}
		var bounded = ResultCodec.bounded(result);
		byte[] encoded = ResultCodec.encode(bounded);
		tx.put(store.metaColumnId(), records.recordKey(sequence(request)), Buf.wrap(encoded), RequestType.none());
		records.recordedBytes += encoded.length;
		return bounded;
	}

	/**
	 * Writes the new floor of the proposer of {@code requests}, if it moved, the row of each live session of their
	 * client requests (its activity at the current {@link #clock}, and its floor), and the applied position. The
	 * records a floor passed are never read again (copies below the floor are refused before any lookup); they are
	 * deleted later, in bulk, by {@link #purge}.
	 */
	private void finishBatch(ApplyWriter tx, List<Request> requests, TermIndex position, boolean markPosition) {
		finishBatch(metaSink(tx), requests, position, markPosition);
	}

	/** Where the meta-column writes of {@link #finishBatch} and {@link #markApplied(MetaSink, TermIndex)} go. */
	private interface MetaSink {
		void put(Keys key, Buf value);
	}

	private MetaSink metaSink(ApplyWriter tx) {
		long metaColumn = store.metaColumnId();
		return (key, value) -> tx.put(metaColumn, key, value, RequestType.none());
	}

	private void finishBatch(MetaSink tx, List<Request> requests, TermIndex position, boolean markPosition) {
		var first = requests.getFirst();
		if (first.completedBelow() > records(first.proposerId()).floor) {
			tx.put(NodeStore.floorKey(first.proposerId()),
					Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(first.completedBelow()).array()));
		}
		Long2LongOpenHashMap sessionFloors = null;
		for (var request : requests) {
			var client = request.client();
			if (client != null && owner(request) != null) {
				if (sessionFloors == null) {
					sessionFloors = new Long2LongOpenHashMap();
				}
				sessionFloors.put(client.sessionId(),
						Math.max(sessionFloors.getOrDefault(client.sessionId(), 0L), client.completedBelow()));
			}
		}
		if (sessionFloors != null) {
			for (var touched : sessionFloors.long2LongEntrySet()) {
				var session = sessions.get(touched.getLongKey());
				tx.put(NodeStore.sessionKey(session.id),
						NodeStore.encodeSession(session.timeoutMillis, clock, Math.max(session.floor, touched.getLongValue())));
			}
		}
		if (markPosition) {
			markApplied(tx, position);
		}
	}

	/** Writes the applied position, and the {@link #clock} if it moved. */
	private void markApplied(ApplyWriter tx, TermIndex position) {
		markApplied(metaSink(tx), position);
	}

	private void markApplied(MetaSink tx, TermIndex position) {
		tx.put(NodeStore.key(NodeStore.APPLIED), NodeStore.encodeTermIndex(position));
		if (clockMoved) {
			tx.put(NodeStore.key(NodeStore.CLOCK),
					Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(clock).array()));
			clockMoved = false;
		}
	}

	// ---------------------------------------------------------------------------------------------------------------
	// Partitioned apply
	// ---------------------------------------------------------------------------------------------------------------

	/*
	 * With lanes (NodeConfig.parallelApplyThreads), the data entries of a group run on several threads, and the result
	 * is the one of the serial apply, byte for byte, whatever the timing:
	 *
	 * - Every request is given a lane from the RocksDB keys it can touch: the first key part of a column whose schema
	 *   has fixed key parts (every RocksDB key of such a column starts with it, buckets included), or else the whole
	 *   column. A request that touches two lanes, a VerifyColumn, and every entry applied alone are not run in lanes:
	 *   they take the serial path, after what came before them was committed.
	 * - The apply thread sorts the entry out as the serial path does (floors, records, sessions, clock), writes down its
	 *   meta-column writes (floor, sessions, position) and hands its requests to their lanes, then goes on with the
	 *   next entry: none of that depends on the results, which only the records hold.
	 * - Each lane runs its requests in log order in a writer of its own (an ApplyWriter: same code as the serial
	 *   path), so a request reads every earlier write to its keys: they are all in its lane.
	 * - At the end of a segment (the group's end, or anything that needs the serial path) the apply thread waits for
	 *   the lanes, writes the records and the meta writes in log order, and commits the lanes' writers and its own in
	 *   one atomic write (ApplyWriter.commitWith): a crash keeps all of it or none, as for a serial group.
	 * - A rejected request rolls back its whole entry in the serial path, and applies it again one request at a time.
	 *   Here its lane marks the entry as the segment's cut and the lanes skip what follows; at the end the lanes roll
	 *   back to their save point before the cut, the entries before it commit, the in-memory changes of the entries
	 *   from the cut on are undone (undoLog), and these entries are applied again: the cut one on the serial path,
	 *   which rejects it exactly as the serial apply does, the next ones in a new segment.
	 */

	/** Lanes of the partitioned apply, or null for the serial apply. */
	private final @Nullable ApplyLanes lanes;
	/** The open segment of the partitioned apply, or null. */
	private @Nullable Segment segment;
	/** Entries sorted out into a segment, and segments cut by a rejection: statistics, for tests. */
	long segmentedEntries;
	long segmentCuts;
	/** While the apply thread sorts out an entry of a segment: what undoes its in-memory changes. */
	private @Nullable List<Runnable> undoLog;

	/** A recorded result that is not known yet: that of an earlier entry of the open segment, see {@link #segment}. */
	private static final ApplyResult PENDING_COPY = new ApplyResult.Failed(RocksDBErrorType.INTERNAL_ERROR,
			"Pending result of the partitioned apply");

	/**
	 * The results of an entry of a segment, known once the segment ends (at the latest when the group ends). Reading
	 * them earlier throws.
	 */
	static final class PendingResults extends java.util.AbstractList<ApplyResult> {
		private @Nullable List<ApplyResult> value;

		void set(List<ApplyResult> results) {
			value = results;
		}

		private List<ApplyResult> require() {
			var results = value;
			if (results == null) {
				throw new IllegalStateException("The results of this entry are known once its group is committed");
			}
			return results;
		}

		@Override
		public ApplyResult get(int index) {
			return require().get(index);
		}

		@Override
		public int size() {
			return require().size();
		}
	}

	/** A request of a segment, run by its lane. */
	private static final class Task {
		final Request request;
		final ProposerRecords owner;
		/** Set by the lane. */
		@Nullable ApplyResult outcome;
		/** The result recorded for it, once the segment ended. */
		@Nullable ApplyResult recorded;

		Task(Request request, ProposerRecords owner) {
			this.request = request;
			this.owner = owner;
		}
	}

	/** An entry of a segment: a data entry, or a position without data ({@code requests} null). */
	private static final class SegmentEntry {
		final int ordinal;
		final @Nullable List<Request> requests;
		final TermIndex position;
		final long leaderTimeMillis;
		final long clockBefore;
		final boolean clockMovedBefore;
		final int purgesBefore;
		final int undoBefore;
		final List<Keys> metaKeys = new ArrayList<>(4);
		final List<Buf> metaValues = new ArrayList<>(4);
		@Nullable Classified batch;
		Task @Nullable [] tasks;
		@Nullable PendingResults results;

		SegmentEntry(int ordinal, @Nullable List<Request> requests, TermIndex position, long leaderTimeMillis,
				long clockBefore, boolean clockMovedBefore, int purgesBefore, int undoBefore) {
			this.ordinal = ordinal;
			this.requests = requests;
			this.position = position;
			this.leaderTimeMillis = leaderTimeMillis;
			this.clockBefore = clockBefore;
			this.clockMovedBefore = clockMovedBefore;
			this.purgesBefore = purgesBefore;
			this.undoBefore = undoBefore;
		}

		MetaSink meta() {
			return (key, value) -> {
				metaKeys.add(key);
				metaValues.add(value);
			};
		}
	}

	/** Entries handed to the lanes and not committed yet, see "Partitioned apply". */
	private static final class Segment {
		final List<SegmentEntry> entries = new ArrayList<>();
		final ApplyWriter[] writers;
		/** Per lane: the ordinals of the entries before which it set a save point, in order. */
		final IntArrayList[] savePoints;
		/** The first entry with a rejected request: it and the entries after it are applied again. */
		final java.util.concurrent.atomic.AtomicInteger cut =
				new java.util.concurrent.atomic.AtomicInteger(Integer.MAX_VALUE);
		/** A failure of a lane that is not a result (a local failure, an Error): the replica stops. */
		final java.util.concurrent.atomic.AtomicReference<Throwable> failure =
				new java.util.concurrent.atomic.AtomicReference<>();
		/** The requests run in this segment, by identity, for later copies of them, see {@link #requireRecorded}. */
		final java.util.HashMap<Identity, Task> pending = new java.util.HashMap<>();
		final List<Runnable> undo = new ArrayList<>();

		Segment(int lanes) {
			writers = new ApplyWriter[lanes];
			savePoints = new IntArrayList[lanes];
			for (int i = 0; i < lanes; i++) {
				savePoints[i] = new IntArrayList();
			}
		}
	}

	/** The lane of each request of an entry, or null if one of them cannot run in a lane. */
	private int @Nullable [] lanesOf(List<Request> requests) {
		var laneCount = lanes.count();
		var result = new int[requests.size()];
		for (int i = 0; i < requests.size(); i++) {
			int lane = laneOf(requests.get(i).entry(), laneCount);
			if (lane < 0) {
				return null;
			}
			result[i] = lane;
		}
		return result;
	}

	/** The lane of {@code entry}, or -1 if it can touch more than one lane, or cannot run in a lane. */
	private int laneOf(LogEntry entry, int laneCount) {
		return switch (entry) {
			case LogEntry.Put put -> keyLane(put.columnId(), put.keys(), laneCount);
			case LogEntry.Delete delete -> keyLane(delete.columnId(), delete.keys(), laneCount);
			case LogEntry.Merge merge -> keyLane(merge.columnId(), merge.keys(), laneCount);
			case LogEntry.PutMulti put -> keysLane(put.columnId(), put.keys(), laneCount);
			case LogEntry.DeleteMulti delete -> keysLane(delete.columnId(), delete.keys(), laneCount);
			case LogEntry.MergeMulti merge -> keysLane(merge.columnId(), merge.keys(), laneCount);
			case LogEntry.Guarded guarded -> {
				int lane = guarded.guard() == null ? -2
						: keyLane(guarded.guard().columnId(), guarded.guard().keys(), laneCount);
				for (var write : guarded.writes()) {
					int writeLane = laneOf(write, laneCount);
					if (writeLane < 0 || (lane != -2 && lane != writeLane)) {
						yield -1;
					}
					lane = writeLane;
				}
				yield lane == -2 ? 0 : lane;
			}
			// Deletions of ranges, schema changes, column checks: the serial path.
			default -> -1;
		};
	}

	private int keysLane(long columnId, List<Keys> keys, int laneCount) {
		int lane = 0;
		for (int i = 0; i < keys.size(); i++) {
			int keyLane = keyLane(columnId, keys.get(i), laneCount);
			if (i > 0 && keyLane != lane) {
				return -1;
			}
			lane = keyLane;
		}
		return lane;
	}

	/**
	 * The lane of one key of a column: by its first key part if the column's schema has fixed key parts, else by the
	 * column. Two requests that can touch the same RocksDB key get the same lane: every RocksDB key of a column with
	 * fixed key parts (a bucket too) starts with the first one. Which lane does not matter for the result, only that.
	 */
	private int keyLane(long columnId, Keys keys, int laneCount) {
		var schema = store.db().columnSchemaOrNull(columnId);
		int hash;
		var parts = keys.keys();
		if (schema == null || schema.fixedLengthKeysCount() == 0 || parts.length == 0) {
			hash = Long.hashCode(columnId * 0x9E3779B97F4A7C15L);
		} else {
			var first = parts[0];
			byte[] array = first.getBackingByteArray();
			int from = first.getBackingByteArrayOffset();
			int to = from + first.getBackingByteArrayLength();
			hash = 1;
			for (int i = from; i < to; i++) {
				hash = 31 * hash + array[i];
			}
			hash ^= hash >>> 16;
			hash *= 0x85EBCA6B;
			hash ^= hash >>> 13;
		}
		return Math.floorMod(hash, laneCount);
	}

	/** The open segment, opened if there is none: what the group's transaction holds is committed first. */
	private Segment openSegment() {
		var open = segment;
		if (open == null) {
			// The lanes read the database: the writes of serial entries before them must be there.
			commitGroupTx("prefix");
			open = new Segment(lanes.count());
			segment = open;
		}
		return open;
	}

	/** {@link #applyGrouped} for an entry whose requests run in lanes, see "Partitioned apply". */
	private List<ApplyResult> applySegmented(List<Request> requests, TermIndex position, long leaderTimeMillis,
			long clockBefore, boolean clockMovedBefore, int[] requestLanes, @Nullable PendingResults pending) {
		var open = openSegment();
		var entry = new SegmentEntry(open.entries.size(), requests, position, leaderTimeMillis, clockBefore,
				clockMovedBefore, groupPurges.size(), open.undo.size());
		open.entries.add(entry);
		segmentedEntries++;
		undoLog = open.undo;
		try {
			var batch = classify(requests);
			entry.batch = batch;
			var toRun = batch.toRun();
			var tasks = new Task[toRun.size()];
			for (int j = 0; j < toRun.size(); j++) {
				var request = toRun.get(j);
				tasks[j] = new Task(request, owner(request));
				open.pending.put(Identity.of(request), tasks[j]);
			}
			entry.tasks = tasks;
			finishBatch(entry.meta(), requests, position, true);
			committed(requests, toRun);
			groupPurges.addAll(owners(requests));
			// One piece of work per lane, with the entry's requests of that lane in their order.
			var byLane = new java.util.ArrayList[lanes.count()];
			for (int j = 0; j < toRun.size(); j++) {
				int lane = requestLanes[batch.runIndexes().getInt(j)];
				if (byLane[lane] == null) {
					byLane[lane] = new ArrayList<Task>();
				}
				byLane[lane].add(tasks[j]);
			}
			for (int lane = 0; lane < byLane.length; lane++) {
				if (byLane[lane] == null) {
					continue;
				}
				if (open.writers[lane] == null) {
					open.writers[lane] = beginApply();
				}
				@SuppressWarnings("unchecked")
				List<Task> laneTasks = byLane[lane];
				int laneIndex = lane;
				lanes.submit(lane, () -> runLane(open, laneIndex, entry.ordinal, laneTasks));
			}
		} finally {
			undoLog = null;
		}
		var results = pending != null ? pending : new PendingResults();
		entry.results = results;
		return results;
	}

	/** A position without data, recorded in the open segment. */
	private void addMark(TermIndex position) {
		var open = openSegment();
		var entry = new SegmentEntry(open.entries.size(), null, position, -1, clock, clockMoved, groupPurges.size(),
				open.undo.size());
		open.entries.add(entry);
		markApplied(entry.meta(), position);
	}

	/** Runs, on lane {@code lane}, the requests of entry {@code ordinal} of {@code open} that belong to it. */
	private void runLane(Segment open, int lane, int ordinal, List<Task> tasks) {
		if (open.failure.get() != null || ordinal >= open.cut.get()) {
			return;
		}
		try {
			var writer = open.writers[lane];
			writer.setSavePoint();
			open.savePoints[lane].add(ordinal);
			for (var task : tasks) {
				var outcome = applyLogical(() -> execute(writer, task.request.entry()));
				task.outcome = outcome;
				if (outcome instanceof ApplyResult.Failed) {
					open.cut.accumulateAndGet(ordinal, Math::min);
					return;
				}
			}
		} catch (Throwable error) {
			open.failure.compareAndSet(null, error);
		}
	}

	/**
	 * Ends the open segment, if any: commits it up to its cut, and applies the entries from the cut on again (which
	 * may open and end further segments). Afterwards no segment is open.
	 */
	private void finishSegment() {
		while (segment != null) {
			var open = segment;
			segment = null;
			var again = completeSegment(open);
			for (int i = 0; i < again.size(); i++) {
				var entry = again.get(i);
				if (entry.requests == null) {
					markApplied(entry.position);
				} else {
					// The cut entry on the serial path, which rejects it as the serial apply does.
					applyRouted(entry.requests, entry.position, entry.leaderTimeMillis, entry.results, i == 0);
				}
			}
		}
	}

	/**
	 * Waits for the lanes of {@code open}, records the results of its entries before the cut, commits them in one
	 * write, and returns the entries from the cut on, whose in-memory changes are undone.
	 */
	private List<SegmentEntry> completeSegment(Segment open) {
		boolean done = false;
		try {
			awaitLanes(open);
			var failure = open.failure.get();
			if (failure != null) {
				switch (failure) {
					case Error error -> throw error;
					case ReplicaFailedException replicaFailed -> throw replicaFailed;
					default -> throw new ReplicaFailedException("Failed to apply in a lane", failure);
				}
			}
			int cut = Math.min(open.cut.get(), open.entries.size());
			for (int lane = 0; lane < open.writers.length; lane++) {
				var savePoints = open.savePoints[lane];
				while (!savePoints.isEmpty() && savePoints.getInt(savePoints.size() - 1) >= cut) {
					open.writers[lane].rollbackToSavePoint();
					savePoints.removeInt(savePoints.size() - 1);
				}
			}
			long executeStart = System.nanoTime();
			ApplyWriter tx = cut > 0 ? openGroupTx(open.entries.get(cut - 1).position) : null;
			for (int i = 0; i < cut; i++) {
				recordEntry(tx, open, open.entries.get(i));
			}
			record(executeTime, executeStart);
			var writers = new ArrayList<ApplyWriter>(open.writers.length);
			for (var writer : open.writers) {
				if (writer != null) {
					writers.add(writer);
				}
			}
			if (tx != null) {
				groupTx = null;
				long commitStart = System.nanoTime();
				try {
					tx.commitWith(writers);
				} catch (RuntimeException ex) {
					throw new ReplicaFailedException("Failed to commit the entries applied up to " + groupPosition, ex);
				}
				record(commitTime, commitStart);
				committed("segment");
			} else {
				for (var writer : writers) {
					writer.close();
				}
			}
			java.util.Arrays.fill(open.writers, null);
			done = true;
			if (cut == open.entries.size()) {
				return List.of();
			}
			segmentCuts++;
			if (parallelCuts != null) {
				parallelCuts.record(1);
			}
			var first = open.entries.get(cut);
			for (int i = open.undo.size() - 1; i >= first.undoBefore; i--) {
				open.undo.get(i).run();
			}
			clock = first.clockBefore;
			clockMoved = first.clockMovedBefore;
			while (groupPurges.size() > first.purgesBefore) {
				groupPurges.removeLast();
			}
			return List.copyOf(open.entries.subList(cut, open.entries.size()));
		} finally {
			if (!done) {
				discardSegment(open);
			}
		}
	}

	/** Writes the records and meta writes of an entry of a segment into {@code tx}, and completes its results. */
	private void recordEntry(ApplyWriter tx, Segment open, SegmentEntry entry) {
		var batch = entry.batch;
		if (batch != null) {
			var tasks = entry.tasks;
			var outcomes = new ApplyResult[tasks.length];
			for (int j = 0; j < tasks.length; j++) {
				var task = tasks[j];
				if (task.outcome == null) {
					throw new ReplicaFailedException("A lane did not run a request of entry " + entry.position);
				}
				task.recorded = record(tx, task.owner, task.request, task.outcome);
				outcomes[j] = task.recorded;
			}
			var results = batch.complete(outcomes);
			if (results.contains(PENDING_COPY)) {
				var resolved = new ArrayList<>(results);
				for (int i = 0; i < resolved.size(); i++) {
					if (resolved.get(i) == PENDING_COPY) {
						var original = segmentTask(open, Identity.of(entry.requests.get(i)));
						// As read back from its record by the serial path.
						resolved.set(i, ResultCodec.decode(ResultCodec.encode(original.recorded)));
					}
				}
				results = List.copyOf(resolved);
			}
			entry.results.set(results);
		}
		long metaColumn = store.metaColumnId();
		for (int i = 0; i < entry.metaKeys.size(); i++) {
			tx.put(metaColumn, entry.metaKeys.get(i), entry.metaValues.get(i), RequestType.none());
		}
	}

	/** The task of {@code open} that ran the request {@code identity}, recorded already. */
	private static Task segmentTask(Segment open, Identity identity) {
		var task = open.pending.get(identity);
		if (task == null || task.recorded == null) {
			throw new ReplicaFailedException("Request " + identity + " is marked applied, but its record is missing");
		}
		return task;
	}

	/** Waits until the lanes ran everything given to them for {@code open}. */
	private void awaitLanes(Segment open) {
		int used = 0;
		for (var writer : open.writers) {
			if (writer != null) {
				used++;
			}
		}
		var done = new java.util.concurrent.CountDownLatch(used);
		for (int lane = 0; lane < open.writers.length; lane++) {
			if (open.writers[lane] != null) {
				lanes.submit(lane, done::countDown);
			}
		}
		boolean interrupted = false;
		while (true) {
			try {
				done.await();
				break;
			} catch (InterruptedException ex) {
				// The lanes use the writers until they are done: never close them earlier.
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	/** Drops a segment without committing it: the replica stops after this. */
	private void discardSegment(Segment open) {
		open.cut.set(-1);
		try {
			awaitLanes(open);
		} catch (RuntimeException ex) {
			// Lanes already closed: nothing runs any more.
		}
		for (int lane = 0; lane < open.writers.length; lane++) {
			var writer = open.writers[lane];
			open.writers[lane] = null;
			if (writer != null) {
				try {
					writer.close();
				} catch (RuntimeException ex) {
					// The replica stops anyway.
				}
			}
		}
	}

	private interface Operation {
		Object run();
	}

	/**
	 * Runs a logical operation and turns every failure that follows from the entry into its result, the same on every
	 * replica: rejections by the database, failures of a merge operator, and exceptions of the code that applies it
	 * (a bug there fails the same way everywhere, since every replica runs the same binary). Only a local failure
	 * ({@link #isLocalFailure}) is thrown, as {@link ReplicaFailedException}: it stops this replica, which must not
	 * record a result that the others may not reach. An {@link Error} is not caught: it stops the replica too.
	 */
	private static ApplyResult applyLogical(Operation operation) {
		// Forget an operator failure of an earlier operation on this thread: only one of this operation counts.
		FFMAbstractMergeOperator.takeFailureOnThisThread();
		try {
			return new ApplyResult.Ok(operation.run());
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			var failure = mergeOperatorFailure(ex);
			if (isLocalFailure(failure)) {
				throw new ReplicaFailedException("Storage failure while applying", failure);
			}
			// The recorded text must be the same on every replica: without addresses, identity hashes or thread names
			// (see Failures). The full failure is only logged here.
			var result = switch (failure) {
				case RocksDBException rocksDBException -> new ApplyResult.Failed(rocksDBException.getErrorUniqueId(),
						Failures.sanitize(String.valueOf(failure.getMessage())));
				case IllegalArgumentException _, UnsupportedOperationException _ -> new ApplyResult.Failed(
						RocksDBErrorType.INVALID_REQUEST, Failures.sanitize(String.valueOf(failure.getMessage())));
				default -> new ApplyResult.Failed(RocksDBErrorType.INTERNAL_ERROR, Failures.describe(failure));
			};
			if (result.type() == RocksDBErrorType.INTERNAL_ERROR) {
				// A bug of the apply code: worth its stack trace.
				LOG.warn("Entry failed the same way on every replica, recorded as {}", result.type(), failure);
			} else if (result.type() == RocksDBErrorType.MERGE_OPERATOR_FAILED) {
				LOG.warn("Merge operator failed, recorded as {}: {}", result.type(),
						String.valueOf(failure.getCause() != null ? failure.getCause() : failure));
			}
			return result;
		}
	}

	/**
	 * {@code failure}, or a {@link RocksDBErrorType#MERGE_OPERATOR_FAILED} exception if a merge operator failed on this
	 * thread while the operation ran. A read of a merged key runs the operator on the reading thread, and RocksDB
	 * reports its failure as a corruption, which would otherwise look like a local storage failure on every replica.
	 */
	private static RuntimeException mergeOperatorFailure(RuntimeException failure) {
		var operatorFailure = FFMAbstractMergeOperator.takeFailureOnThisThread();
		if (operatorFailure == null || (failure instanceof RocksDBException rocksDBException
				&& rocksDBException.getErrorUniqueId() == RocksDBErrorType.MERGE_OPERATOR_FAILED)) {
			return failure;
		}
		var named = EmbeddedDB.mergeOperatorFailed(operatorFailure);
		named.addSuppressed(failure);
		return named;
	}

	/**
	 * A failure caused by this machine rather than by the entry: it might not happen on other replicas, so it must
	 * not become a result. Decided by origin, along the causes: an I/O error, an {@link Error} (out of memory...), a
	 * native status that reports the storage ({@link #isLocalStatus}), or an error type that reports a lost local
	 * transaction or a broken database. A merge operator failure is never local, whatever native status reported it.
	 * Everything else (a rejection, an exception of the apply code) is deterministic.
	 */
	static boolean isLocalFailure(Throwable error) {
		// First the whole chain, past a merge operator failure too: an operator may wrap a failure of this JVM (out of
		// memory, stack overflow), which other replicas did not have.
		if (Failures.isJvmFailure(error)) {
			return true;
		}
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof RocksDBException rocksDBException) {
				if (rocksDBException.getErrorUniqueId() == RocksDBErrorType.MERGE_OPERATOR_FAILED) {
					return false;
				}
				if (isLocalErrorType(rocksDBException.getErrorUniqueId())) {
					return true;
				}
			}
			if (t instanceof org.rocksdb.RocksDBException nativeFailure && isLocalStatus(nativeFailure.getStatus())) {
				return true;
			}
			if (t instanceof IOException || t instanceof UncheckedIOException || t instanceof Error) {
				return true;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		return false;
	}

	/**
	 * True for a native status that reports the storage or the process rather than the request: I/O errors,
	 * corruption, no space, busy, timeouts, shutdown, and anything unknown, including a missing status. Only statuses
	 * that follow from the request and the data (invalid argument, not found, not supported) are deterministic.
	 */
	static boolean isLocalStatus(org.rocksdb.@Nullable Status status) {
		if (status == null) {
			return true;
		}
		var subCode = status.getSubCode();
		if (subCode == org.rocksdb.Status.SubCode.NoSpace || subCode == org.rocksdb.Status.SubCode.MemoryLimit) {
			return true;
		}
		return switch (status.getCode()) {
			case InvalidArgument, NotFound, NotSupported -> false;
			default -> true;
		};
	}

	/**
	 * Error types that report a failure of the local database rather than a property of the request: a lost local
	 * transaction, a database that cannot load or write files. {@code PUT_UNKNOWN_ERROR} and {@code INTERNAL_ERROR}
	 * are not among them: they wrap anything, so their causes decide.
	 */
	private static boolean isLocalErrorType(RocksDBErrorType type) {
		return switch (type) {
			case TX_NOT_FOUND, TRANSACTION_NOT_FOUND, COMMIT_FAILED, COMMIT_FAILED_TRY_AGAIN, ROCKSDB_LOAD_ERROR,
					ROCKSDB_CONFIG_ERROR, WRITE_BATCH_1, DIRECTORY_DELETE, SST_WRITE_1, SST_WRITE_2, SST_WRITE_3,
					SST_WRITE_4, SST_GET_SIZE_FAILED, GET_PROPERTY_ERROR -> true;
			default -> false;
		};
	}

	private static ApplyWriter requireTransaction(@Nullable ApplyWriter tx, LogEntry entry) {
		if (tx == null) {
			throw new IllegalArgumentException(entry.getClass().getSimpleName() + " needs a transaction");
		}
		return tx;
	}

	private static void requireNoTransaction(@Nullable ApplyWriter tx, LogEntry entry) {
		if (tx != null) {
			throw new IllegalArgumentException(entry.getClass().getSimpleName() + " cannot run in a transaction");
		}
	}

	@SuppressWarnings("unchecked")
	static RequestPut<? super Buf, Object> putType(RequestTypeId id) {
		if (!(id.getRequestType() instanceof RequestPut<?, ?> type)) {
			throw new IllegalArgumentException("Not a put request type: " + id);
		}
		return (RequestPut<? super Buf, Object>) type;
	}

	@SuppressWarnings("unchecked")
	static RequestDelete<? super Buf, Object> deleteType(RequestTypeId id) {
		if (!(id.getRequestType() instanceof RequestDelete<?, ?> type)) {
			throw new IllegalArgumentException("Not a delete request type: " + id);
		}
		return (RequestDelete<? super Buf, Object>) type;
	}

	@SuppressWarnings("unchecked")
	static RequestMerge<? super Buf, Object> mergeType(RequestTypeId id) {
		if (!(id.getRequestType() instanceof RequestMerge<?, ?> type)) {
			throw new IllegalArgumentException("Not a merge request type: " + id);
		}
		return (RequestMerge<? super Buf, Object>) type;
	}
}
