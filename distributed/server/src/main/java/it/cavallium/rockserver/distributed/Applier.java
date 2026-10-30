package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.io.IOException;
import java.io.UncheckedIOException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.ratis.server.protocol.TermIndex;
import org.jetbrains.annotations.Nullable;

/**
 * Applies log entries to the local database. Called only by the Raft apply thread.
 *
 * <p>Applying a {@link Request} makes three things durable together: the entry's effects, its result (recorded under
 * the request's identity, see {@link Request}) and the new applied position. Data writes run inside one short local
 * transaction that writes all three, so a crash keeps all or none of them. Schema changes and {@code deleteRange}
 * cannot run in a transaction; they are idempotent at their log position, so their record and position are written
 * right after them: RocksDB recovers a prefix of its WAL, and a crash between the two writes only makes that one
 * entry run again on the same state.</p>
 *
 * <p>An entry that the database rejects for a logical reason (missing column, malformed keys...) is rejected the
 * same way on every replica: that becomes an {@link ApplyResult.Failed} and the log moves on. A storage or native
 * failure is specific to this replica: it is thrown as {@link ReplicaFailedException}, and the replica stops
 * instead of skipping the entry.</p>
 */
final class Applier {

	/** This replica cannot apply the log any more and must be repaired. */
	static final class ReplicaFailedException extends RuntimeException {
		ReplicaFailedException(String message, Throwable cause) {
			super(message, cause);
		}

		ReplicaFailedException(String message) {
			super(message);
		}
	}

	/**
	 * Lease of the local apply transaction. It must never expire while the entry is applied: an expired transaction
	 * would look like a rejected entry on this replica only.
	 */
	private static final long APPLY_TRANSACTION_TIMEOUT_MS = 24L * 60 * 60 * 1000;

	private final NodeStore store;

	Applier(NodeStore store) {
		this(store, null);
	}

	Applier(NodeStore store, @Nullable NodeMetrics metrics) {
		this.store = store;
		this.openTime = metrics == null ? null : metrics.duration("apply.open");
		this.executeTime = metrics == null ? null : metrics.duration("apply.execute");
		this.commitTime = metrics == null ? null : metrics.duration("apply.commit");
	}

	private final NodeMetrics.@Nullable Series openTime;
	private final NodeMetrics.@Nullable Series executeTime;
	private final NodeMetrics.@Nullable Series commitTime;

	private static void record(NodeMetrics.@Nullable Series series, long startNanos) {
		if (series != null) {
			series.since(startNanos);
		}
	}

	/**
	 * What the meta column says about one proposer's requests: its floor and which requests at or above it have a
	 * record. Kept in memory, loaded from the database the first time the proposer is seen, so that checking a
	 * request for an earlier copy costs no database read. Changed only together with the database, after a commit.
	 */
	private static final class ProposerRecords {
		long floor;
		final LongOpenHashSet recorded = new LongOpenHashSet();
	}

	private final Long2ObjectOpenHashMap<ProposerRecords> proposers = new Long2ObjectOpenHashMap<>();

	/** The result recorded for {@code request} by an earlier application, or null if it was never applied. */
	@Nullable ApplyResult recordedResult(Request request) {
		Buf recorded = store.db().get(0L, store.metaColumnId(),
				NodeStore.requestKey(request.proposerId(), request.sequence()), RequestType.<Buf>current());
		return recorded == null ? null : ResultCodec.decode(recorded.toByteArray());
	}

	/** Applies one request alone, see {@link #applyBatch}. */
	ApplyResult apply(Request request, TermIndex position) {
		return applyBatch(List.of(request), position).getFirst();
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
	 */
	List<ApplyResult> applyBatch(List<Request> requests, TermIndex position) {
		try {
			if (requests.size() == 1 && !isBatchable(requests.getFirst().entry())) {
				return List.of(applyAlone(requests.getFirst(), position));
			}
			return applyData(requests, position);
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to apply entry " + position, ex);
		}
	}

	/** True for entries that may share a log entry, and a local transaction, with others. */
	static boolean isBatchable(LogEntry entry) {
		return (entry instanceof LogEntry.Write && !(entry instanceof LogEntry.DeleteRange))
				|| entry instanceof LogEntry.Guarded;
	}

	private ApplyResult applyAlone(Request request, TermIndex position) {
		var records = records(request.proposerId());
		long floor = records.floor;
		if (request.sequence() < floor) {
			inApplyTransaction(position, tx -> markApplied(tx, position));
			return STALE_COPY;
		}
		if (records.recorded.contains(request.sequence())) {
			inApplyTransaction(position, tx -> markApplied(tx, position));
			return requireRecorded(request);
		}
		// Schema changes write the RocksDB MANIFEST synchronously, while their schema rows, record and position go to
		// the WAL. Make everything before them durable first (the data writes applied without the WAL included) and
		// everything after them durable at once, so a crash cannot keep one without the other.
		makeDurable();
		var outcome = applyLogical(() -> execute(0L, request.entry()));
		finish(0L, request, floor, outcome, position);
		syncWal();
		committed(request.proposerId(), records, List.of(request), request.completedBelow());
		return outcome;
	}

	private List<ApplyResult> applyData(List<Request> requests, TermIndex position) {
		long proposerId = requests.getFirst().proposerId();
		long completedBelow = requests.getFirst().completedBelow();
		var records = records(proposerId);
		long floor = records.floor;
		var results = new ApplyResult[requests.size()];
		var toRun = new ArrayList<Request>(requests.size());
		var runIndexes = new IntArrayList(requests.size());
		var seen = new LongOpenHashSet();
		for (int i = 0; i < requests.size(); i++) {
			var request = requests.get(i);
			if (request.proposerId() != proposerId || request.completedBelow() != completedBelow) {
				throw new IllegalArgumentException("A batch holds requests of one proposer, with one completedBelow");
			}
			if (request.sequence() < floor) {
				results[i] = STALE_COPY;
			} else if (records.recorded.contains(request.sequence())) {
				results[i] = requireRecorded(request);
			} else if (!seen.add(request.sequence())) {
				// A second copy in the same entry: it gets the first copy's result below.
				results[i] = null;
			} else {
				toRun.add(request);
				runIndexes.add(i);
			}
		}
		var db = store.db();
		long openStart = System.nanoTime();
		long tx = beginApply();
		record(openTime, openStart);
		boolean open = true;
		try {
			var outcomes = new ApplyResult[toRun.size()];
			long executeStart = System.nanoTime();
			for (int j = 0; j < toRun.size(); j++) {
				var request = toRun.get(j);
				if (!isBatchable(request.entry())) {
					throw new IllegalArgumentException("A batch cannot contain " + request.entry().getClass().getSimpleName());
				}
				outcomes[j] = applyLogical(() -> execute(tx, request.entry()));
				if (outcomes[j] instanceof ApplyResult.Failed failed && toRun.size() > 1) {
					// Its partial writes are in the transaction with the others' writes: start over one at a time.
					db.closeTransaction(tx, false);
					open = false;
					return applyOneByOne(requests, position);
				}
			}
			if (toRun.size() == 1 && outcomes[0] instanceof ApplyResult.Failed) {
				// A rejected entry has no effects: roll back its partial writes, then record the rejection.
				db.closeTransaction(tx, false);
				open = false;
				var rejected = toRun.getFirst();
				inApplyTransaction(position, recordTx -> finish(recordTx, rejected, floor, outcomes[0], position));
			} else {
				for (int j = 0; j < toRun.size(); j++) {
					record(tx, toRun.get(j), outcomes[j]);
				}
				finishBatch(tx, proposerId, completedBelow, floor, position);
				record(executeTime, executeStart);
				open = false;
				long commitStart = System.nanoTime();
				boolean committed = db.closeTransaction(tx, true);
				record(commitTime, commitStart);
				if (!committed) {
					// Only the apply thread writes the database, so an optimistic conflict means local corruption.
					throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
				}
			}
			for (int j = 0; j < toRun.size(); j++) {
				results[runIndexes.getInt(j)] = outcomes[j];
			}
		} finally {
			if (open) {
				db.closeTransaction(tx, false);
			}
		}
		committed(proposerId, records, toRun, completedBelow);
		for (int i = 0; i < results.length; i++) {
			if (results[i] == null) {
				results[i] = requireRecorded(requests.get(i));
			}
		}
		return List.of(results);
	}

	/** The slow path of {@link #applyData}: each request in its own local transaction, all at {@code position}. */
	private List<ApplyResult> applyOneByOne(List<Request> requests, TermIndex position) {
		var results = new ArrayList<ApplyResult>(requests.size());
		for (var request : requests) {
			results.add(applyBatch(List.of(request), position).getFirst());
		}
		return results;
	}

	/** The in-memory view of the meta column for {@code proposerId}, loaded on first use. */
	private ProposerRecords records(long proposerId) {
		var records = proposers.get(proposerId);
		if (records == null) {
			records = new ProposerRecords();
			records.floor = floor(proposerId);
			try (var keys = store.db().getRange(0L, store.metaColumnId(), NodeStore.requestKey(proposerId, records.floor),
					NodeStore.requestKey(proposerId, Long.MAX_VALUE), false, RequestType.<KV>allInRange())) {
				var recorded = records.recorded;
				keys.forEach(kv -> recorded.add(NodeStore.requestSequence(kv.keys())));
			}
			proposers.put(proposerId, records);
		}
		return records;
	}

	/** Updates the in-memory records after the database committed {@code applied} and the new floor. */
	private void committed(long proposerId, ProposerRecords records, List<Request> applied, long completedBelow) {
		for (var request : applied) {
			records.recorded.add(request.sequence());
		}
		if (completedBelow > records.floor) {
			records.floor = completedBelow;
			records.recorded.removeIf(sequence -> sequence < completedBelow);
		}
	}

	private ApplyResult requireRecorded(Request request) {
		var recorded = recordedResult(request);
		if (recorded == null) {
			throw new ReplicaFailedException("Request " + request.sequence() + " of proposer " + request.proposerId()
					+ " is marked applied, but its record is missing");
		}
		return recorded;
	}

	/** Records a position that carries no data entry (configuration changes, leader no-ops). */
	void markApplied(TermIndex position) {
		try {
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
	private long beginApply() {
		return store.db().openUnloggedTransaction(APPLY_TRANSACTION_TIMEOUT_MS, WorkloadProfile.BATCH, true);
	}

	private interface TransactionWrites {
		void write(long tx);
	}

	/** Runs {@code writes} in an apply transaction ({@link #beginApply}) and commits it. */
	private void inApplyTransaction(TermIndex position, TransactionWrites writes) {
		var db = store.db();
		long tx = beginApply();
		boolean open = true;
		try {
			writes.write(tx);
			open = false;
			if (!db.closeTransaction(tx, true)) {
				throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
			}
		} finally {
			if (open) {
				db.closeTransaction(tx, false);
			}
		}
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

	private Object execute(long tx, LogEntry entry) {
		var db = store.db();
		if (entry instanceof LogEntry.Write write) {
			requireUserColumn(write.columnId());
		}
		return switch (entry) {
			case LogEntry.Put put -> db.put(tx, put.columnId(), put.keys(), put.value(), putType(put.requestType()));
			case LogEntry.PutMulti put ->
					db.putMulti(tx, put.columnId(), put.keys(), put.values(), putType(put.requestType()));
			case LogEntry.Delete delete -> db.delete(tx, delete.columnId(), delete.keys(), deleteType(delete.requestType()));
			case LogEntry.DeleteMulti delete ->
					db.deleteMulti(tx, delete.columnId(), delete.keys(), deleteType(delete.requestType()));
			case LogEntry.Merge merge ->
					db.merge(tx, merge.columnId(), merge.keys(), merge.value(), mergeType(merge.requestType()));
			case LogEntry.MergeMulti merge ->
					db.mergeMulti(tx, merge.columnId(), merge.keys(), merge.values(), mergeType(merge.requestType()));
			case LogEntry.DeleteRange range -> {
				requireNoTransaction(tx, entry);
				db.deleteRange(range.columnId(), range.start(), range.end());
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
					// failure that would stop them all.
					throw new IllegalArgumentException("Merge operator " + upload.name() + " cannot be loaded: " + badJar,
							null);
				} catch (RocksDBException failed) {
					// An operator that cannot be instantiated (missing class, wrong type, failing constructor) is
					// reported as PUT_UNKNOWN_ERROR; only a native storage error below it is a local failure.
					if (isNativeFailure(failed)) {
						throw failed;
					}
					throw new IllegalArgumentException("Merge operator " + upload.name() + " rejected: "
							+ failed.getMessage(), null);
				}
			}
			case LogEntry.Guarded guarded -> {
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

	private void applyGenesis(LogEntry.Genesis genesis) {
		var existing = store.clusterId();
		if (existing != null) {
			if (!existing.equals(genesis.clusterId())) {
				throw new ReplicaFailedException("Database belongs to cluster " + existing
						+ ", but the log belongs to cluster " + genesis.clusterId());
			}
			return;
		}
		boolean hasData = store.hasUserColumns();
		if (genesis.seeded() && !hasData) {
			throw new ReplicaFailedException("The cluster was seeded with an existing database, but this replica's"
					+ " database is empty: give it a copy of the seed database, or remove and re-add it so that it"
					+ " receives a snapshot");
		}
		if (!genesis.seeded() && hasData) {
			throw new ReplicaFailedException("The cluster started empty, but this replica's database has data");
		}
		store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_ID),
				Buf.wrap(genesis.clusterId().getBytes(StandardCharsets.UTF_8)), RequestType.none());
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

	/** Writes the result record, the proposer's floor and the applied position, in transaction {@code tx} if not 0. */
	private void finish(long tx, Request request, long floor, ApplyResult result, TermIndex position) {
		record(tx, request, result);
		finishBatch(tx, request.proposerId(), request.completedBelow(), floor, position);
	}

	private void record(long tx, Request request, ApplyResult result) {
		store.db().put(tx, store.metaColumnId(), NodeStore.requestKey(request.proposerId(), request.sequence()),
				Buf.wrap(ResultCodec.encode(result)), RequestType.none());
	}

	/**
	 * Writes the proposer's new floor, if it moved, deleting the records it passed, and the applied position. Those
	 * records are never read again (copies below the floor are refused before any lookup); deleting them in the same
	 * write keeps every apply write in one atomic, unlogged transaction (see {@link #beginApply}).
	 */
	private void finishBatch(long tx, long proposerId, long completedBelow, long floor, TermIndex position) {
		if (completedBelow > floor) {
			store.db().put(tx, store.metaColumnId(), NodeStore.floorKey(proposerId),
					Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(completedBelow).array()),
					RequestType.none());
			var records = proposers.get(proposerId);
			if (records != null) {
				var iterator = records.recorded.iterator();
				while (iterator.hasNext()) {
					long sequence = iterator.nextLong();
					if (sequence < completedBelow) {
						store.db().delete(tx, store.metaColumnId(), NodeStore.requestKey(proposerId, sequence),
								RequestType.none());
					}
				}
			}
		}
		markApplied(tx, position);
	}

	private void markApplied(long tx, TermIndex position) {
		store.db().put(tx, store.metaColumnId(), NodeStore.key(NodeStore.APPLIED),
				NodeStore.encodeTermIndex(position), RequestType.none());
	}

	private interface Operation {
		Object run();
	}

	/** Runs a logical operation, turning deterministic rejections into results. */
	private static ApplyResult applyLogical(Operation operation) {
		try {
			return new ApplyResult.Ok(operation.run());
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			if (isLocalFailure(ex)) {
				throw new ReplicaFailedException("Storage failure while applying", ex);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				return new ApplyResult.Failed(rocksDBException.getErrorUniqueId(), String.valueOf(ex.getMessage()));
			}
			if (ex instanceof IllegalArgumentException || ex instanceof UnsupportedOperationException) {
				return new ApplyResult.Failed(RocksDBErrorType.INVALID_REQUEST, String.valueOf(ex.getMessage()));
			}
			throw ex;
		}
	}

	/**
	 * A failure caused by this machine (disk, native storage, a lost local transaction) rather than by the entry: it
	 * might not happen on other replicas, so it must not become a result.
	 */
	static boolean isLocalFailure(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof org.rocksdb.RocksDBException || t instanceof IOException
					|| t instanceof UncheckedIOException || t instanceof Error) {
				return true;
			}
			if (t instanceof RocksDBException rocksDBException && isLocalErrorType(rocksDBException.getErrorUniqueId())) {
				return true;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		return false;
	}

	private static boolean isNativeFailure(Throwable error) {
		for (Throwable t = error; t != null && t.getCause() != t; t = t.getCause()) {
			if (t instanceof org.rocksdb.RocksDBException || t instanceof IOException || t instanceof UncheckedIOException) {
				return true;
			}
		}
		return false;
	}

	/** Error types that report a failure of the local database rather than a property of the request. */
	private static boolean isLocalErrorType(RocksDBErrorType type) {
		return switch (type) {
			case TX_NOT_FOUND, TRANSACTION_NOT_FOUND, COMMIT_FAILED, COMMIT_FAILED_TRY_AGAIN, INTERNAL_ERROR,
					ROCKSDB_LOAD_ERROR, ROCKSDB_CONFIG_ERROR, WRITE_BATCH_1, PUT_UNKNOWN_ERROR, DIRECTORY_DELETE,
					SST_WRITE_1, SST_WRITE_2, SST_WRITE_3, SST_WRITE_4, SST_GET_SIZE_FAILED, GET_PROPERTY_ERROR -> true;
			default -> false;
		};
	}

	private static void requireNoTransaction(long tx, LogEntry entry) {
		if (tx != 0L) {
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
