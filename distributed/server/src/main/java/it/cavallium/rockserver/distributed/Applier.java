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
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * Applies log entries to the local database. Called only by the Raft apply thread.
 *
 * <p>Each entry's effects and the new applied position become durable together, so that replaying the log after
 * a crash skips exactly the entries already applied. Entries whose replay would not be idempotent (merges, and
 * transaction commits, which must also be atomic) run inside one short local transaction that also writes the
 * applied position. Other entries are idempotent at their log position, so their applied position is written
 * right after them: RocksDB recovers a prefix of its WAL, so a crash between the two writes only causes that one
 * entry to be applied again, which yields the same state.</p>
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

	private static final long APPLY_TRANSACTION_TIMEOUT_MS = 60_000;

	private final NodeStore store;

	Applier(NodeStore store) {
		this.store = store;
	}

	/** Applies {@code entry} at {@code position} and records the position. */
	ApplyResult apply(LogEntry entry, TermIndex position) {
		try {
			return switch (entry) {
				case LogEntry.Merge _, LogEntry.MergeMulti _, LogEntry.Guarded _ -> applyInTransaction(entry, position);
				case LogEntry.Genesis genesis -> {
					applyGenesis(genesis);
					markApplied(0L, position);
					yield ApplyResult.OK_NULL;
				}
				default -> {
					var result = applyLogical(() -> execute(0L, entry));
					markApplied(0L, position);
					yield result;
				}
			};
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

	private ApplyResult applyInTransaction(LogEntry entry, TermIndex position) {
		var db = store.db();
		long tx = db.openTransaction(APPLY_TRANSACTION_TIMEOUT_MS, WorkloadProfile.BATCH);
		boolean closed = false;
		try {
			var result = applyLogical(() -> execute(tx, entry));
			if (result instanceof ApplyResult.Failed) {
				// Roll back partial effects of a rejected entry, then record the position alone.
				db.closeTransaction(tx, false);
				closed = true;
				markApplied(0L, position);
				return result;
			}
			markApplied(tx, position);
			closed = true;
			if (!db.closeTransaction(tx, true)) {
				// Only the apply thread writes the database, so an optimistic conflict means local corruption.
				throw new ReplicaFailedException("Local apply transaction conflicted at " + position);
			}
			return result;
		} finally {
			if (!closed) {
				db.closeTransaction(tx, false);
			}
		}
	}

	private Object execute(long tx, LogEntry entry) {
		var db = store.db();
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
				yield db.createColumn(create.name(), create.schema());
			}
			case LogEntry.DeleteColumn delete -> {
				requireNoTransaction(tx, entry);
				db.deleteColumn(delete.columnId());
				yield null;
			}
			case LogEntry.DeleteColumnIfExists delete -> {
				requireNoTransaction(tx, entry);
				yield db.deleteColumnIfExists(delete.name());
			}
			case LogEntry.UploadMergeOperator upload -> {
				requireNoTransaction(tx, entry);
				yield db.uploadMergeOperator(upload.name(), upload.className(), upload.jarData());
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
			case LogEntry.Genesis _ -> throw new IllegalStateException("Genesis is applied separately");
		};
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
	 * A failure caused by this machine (disk, native storage) rather than by the entry: it might not happen on
	 * other replicas, so it must not become a result.
	 */
	static boolean isLocalFailure(Throwable error) {
		for (Throwable t = error; t != null; t = t.getCause()) {
			if (t instanceof org.rocksdb.RocksDBException || t instanceof IOException
					|| t instanceof UncheckedIOException || t instanceof Error) {
				return true;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		return false;
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
