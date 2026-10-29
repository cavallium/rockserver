package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
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
import java.nio.charset.StandardCharsets;
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
	/** Records below a proposer's floor are deleted each time the floor crosses a multiple of this. */
	private static final long RECORD_CLEANUP_EVERY = 256;

	private final NodeStore store;

	Applier(NodeStore store) {
		this.store = store;
	}

	/** The result recorded for {@code request} by an earlier application, or null if it was never applied. */
	@Nullable ApplyResult recordedResult(Request request) {
		Buf recorded = store.db().get(0L, store.metaColumnId(),
				NodeStore.requestKey(request.proposerId(), request.sequence()), RequestType.<Buf>current());
		return recorded == null ? null : ResultCodec.decode(recorded.toByteArray());
	}

	/**
	 * Applies {@code request} at {@code position} and records its result and the position.
	 *
	 * <p>A request numbered below its proposer's floor is a late copy of a request the proposer already finished
	 * with (answered, or given up as unknown): it is never executed, so cleaning up records cannot make an old
	 * request run again. A request with a record is a copy of one that already took effect: it gets the recorded
	 * result.</p>
	 */
	ApplyResult apply(Request request, TermIndex position) {
		try {
			long floor = floor(request.proposerId());
			if (request.sequence() < floor) {
				markApplied(0L, position);
				return STALE_COPY;
			}
			var recorded = recordedResult(request);
			if (recorded != null) {
				markApplied(0L, position);
				return recorded;
			}
			var entry = request.entry();
			ApplyResult result = switch (entry) {
				case LogEntry.Write write when !(write instanceof LogEntry.DeleteRange) ->
						applyInTransaction(request, floor, position);
				case LogEntry.Guarded _ -> applyInTransaction(request, floor, position);
				default -> {
					// Schema changes write the RocksDB MANIFEST synchronously, while their schema rows, record and
					// position go to the WAL. Make everything before them durable first and everything after them
					// durable at once, so a crash cannot keep one without the other.
					syncWal();
					var outcome = applyLogical(() -> execute(0L, entry));
					finish(0L, request, floor, outcome, position);
					syncWal();
					yield outcome;
				}
			};
			cleanUpRecords(request, floor);
			return result;
		} catch (ReplicaFailedException ex) {
			throw ex;
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to apply entry " + position, ex);
		}
	}

	/** Records a position that carries no data entry (configuration changes, leader no-ops). */
	void markApplied(TermIndex position) {
		try {
			markApplied(0L, position);
		} catch (RuntimeException ex) {
			throw new ReplicaFailedException("Failed to record applied position " + position, ex);
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

	private ApplyResult applyInTransaction(Request request, long floor, TermIndex position) {
		var db = store.db();
		long tx = db.openTransaction(APPLY_TRANSACTION_TIMEOUT_MS, WorkloadProfile.BATCH);
		boolean open = true;
		try {
			var result = applyLogical(() -> execute(tx, request.entry()));
			if (result instanceof ApplyResult.Failed) {
				// A rejected entry has no effects: roll back partial writes, then record the rejection.
				db.closeTransaction(tx, false);
				open = false;
				finish(0L, request, floor, result, position);
				return result;
			}
			finish(tx, request, floor, result, position);
			open = false;
			if (!db.closeTransaction(tx, true)) {
				// Only the apply thread writes the database, so an optimistic conflict means local corruption.
				throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
			}
			return result;
		} finally {
			if (open) {
				db.closeTransaction(tx, false);
			}
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
		store.db().put(tx, store.metaColumnId(), NodeStore.requestKey(request.proposerId(), request.sequence()),
				Buf.wrap(ResultCodec.encode(result)), RequestType.none());
		if (request.completedBelow() > floor) {
			store.db().put(tx, store.metaColumnId(), NodeStore.floorKey(request.proposerId()),
					Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(request.completedBelow()).array()),
					RequestType.none());
		}
		markApplied(tx, position);
	}

	private void markApplied(long tx, TermIndex position) {
		store.db().put(tx, store.metaColumnId(), NodeStore.key(NodeStore.APPLIED),
				NodeStore.encodeTermIndex(position), RequestType.none());
	}

	/**
	 * Deletes the records below a proposer's new floor. They are never read again (copies below the floor are
	 * refused before any lookup), so whether a crash interrupts this does not change any result.
	 */
	private void cleanUpRecords(Request request, long oldFloor) {
		long newFloor = request.completedBelow();
		if (newFloor / RECORD_CLEANUP_EVERY <= oldFloor / RECORD_CLEANUP_EVERY) {
			return;
		}
		store.db().deleteRange(store.metaColumnId(), NodeStore.requestKey(request.proposerId(), 0),
				NodeStore.requestKey(request.proposerId(), newFloor));
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
