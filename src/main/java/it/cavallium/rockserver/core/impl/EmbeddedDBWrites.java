package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.dummyRocksDBEmptyValue;
import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Slice;
import org.rocksdb.OptimisticTransactionDB;
import org.rocksdb.Status.Code;
import reactor.util.retry.Retry;
import org.rocksdb.WriteBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDBWriteElision.*;

/**
 * Point and multi-key updates of {@link EmbeddedDB}: put, delete, merge (single and multi), deleteRange and the
 * shared transactional write paths.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBWrites {

	private final EmbeddedDB edb;

	EmbeddedDBWrites(EmbeddedDB edb) {
		this.edb = edb;
	}

	byte[] pointGetForUpdate(Tx tx, ReadOptions options,
			ColumnFamilyHandle column, byte[] key) throws org.rocksdb.RocksDBException {
		long started = System.nanoTime();
		try {
			// An untracked transaction has no concurrent writer to conflict with: a plain read of its batch and the DB.
			return tx.untracked() ? tx.val().get(options, column, key) : tx.val().getForUpdate(options, column, key, true);
		}
		finally { edb.storageMetrics.recordCompactionForegroundRead(started, 0); }
	}

	private static void txPut(Tx tx, ColumnFamilyHandle column, byte[] key, byte[] value)
			throws org.rocksdb.RocksDBException {
		if (tx.untracked()) {
			tx.val().putUntracked(column, key, value);
		} else {
			tx.val().put(column, key, value);
		}
	}

	private static void txDelete(Tx tx, ColumnFamilyHandle column, byte[] key) throws org.rocksdb.RocksDBException {
		if (tx.untracked()) {
			tx.val().deleteUntracked(column, key);
		} else {
			tx.val().delete(column, key);
		}
	}

	private static void txMerge(Tx tx, ColumnFamilyHandle column, byte[] key, Buf value)
			throws org.rocksdb.RocksDBException {
		if (tx.untracked()) {
			byte[] array = value.asArrayStrict();
			if (array != null) {
				tx.val().mergeUntracked(column, key, array);
			} else {
				// A slice of a larger array (such as a value decoded in place from a replicated log entry): passed with
				// its offset, without a copy. Heap buffers both, as RocksDB's binding requires.
				tx.val().mergeUntracked(column, java.nio.ByteBuffer.wrap(key), value.asHeapByteBuffer());
			}
		} else {
			tx.val().merge(column, key, EmbeddedDBWriteElision.jniArray(value));
		}
	}
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("Put", start, columnId, keys, value, transactionOrUpdateId, null, null, requestType);
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			// Column id
			col = edb.beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return put(tx, col, updateId, keys, value, requestType);
				}
			}
			return put(tx, col, updateId, keys, value, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public <T> T delete(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull RequestType.RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("Delete", start, columnId, keys, null, transactionOrUpdateId, null, null, requestType);
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			// Column id
			col = edb.beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return delete(tx, col, updateId, keys, requestType);
				}
			}
			return delete(tx, col, updateId, keys, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putTimer.record(end - start, TimeUnit.NANOSECONDS); // Re-use put timer? or new timer? using putTimer for now
		}
	}
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("Merge", start, columnId, keys, value, transactionOrUpdateId, null, null, requestType);
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;
			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return merge(tx, col, updateId, keys, value, requestType);
				}
			}
			return merge(tx, col, updateId, keys, value, requestType);
		} catch (RocksDBRetryException ex) {
			throw ex;
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public <T> List<T> deleteMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			edb.actionLogger.logAction("deleteMulti (begin)",
					start,
					columnId,
					keysList.size(),
					0,
					transactionOrUpdateId,
					null,
					null,
					requestType
			);

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				synchronized (tx) {
					edb.requireLiveTransaction(updateId, tx);
					return deleteMultiWithUpdateId(tx, updateId, col, keysList, requestType);
				}
			}

			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return deleteMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, requestType);
				}
			}
			return deleteMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, requestType);
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putMultiTimer.record(end - start, TimeUnit.NANOSECONDS); // Re-use putMultiTimer
		}
	}

	private <T> List<T> deleteMultiWithoutUpdateId(long start,
			long columnId,
			long transactionOrUpdateId,
			@Nullable Tx tx,
			ColumnInstance col,
			List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		List<T> responses =
				requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
		for (int i = 0; i < keysList.size(); i++) {
			var keys = keysList.get(i);
			edb.actionLogger.logAction("deleteMulti (next)",
					start,
					columnId,
					keys,
					null,
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			T result = delete(tx, col, 0L, keys, requestType);
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
	}

	private <T> List<T> deleteMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			RequestDelete<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					// Handle case where setSavePoint might not be supported or fails
					edb.logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						T result = delete(tx, col, 0L, keys, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = edb.closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in deleteMultiWithUpdateId", e);
							}
						}
						edb.closeTransaction(updateId, false);
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during retry in deleteMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during error in deleteMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			edb.closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}
	public void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		deleteRange(columnId, startKeysInclusive, endKeysExclusive, false);
	}
	public void deleteRangeUnlogged(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive) throws RocksDBException {
		deleteRange(columnId, startKeysInclusive, endKeysExclusive, true);
	}

	private void deleteRange(long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean disableWal) throws RocksDBException {
		var start = System.nanoTime();
		edb.actionLogger.logAction("DeleteRange",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				null,
				null,
				null,
				null
		);
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			var beginKey = calculateDeleteRangeBeginKey(col, startKeysInclusive);
			var endKey = calculateDeleteRangeEndKey(col, endKeysExclusive);
			if (Arrays.compareUnsigned(beginKey, endKey) >= 0) {
				return;
			}
			try {
				deleteRangeNative(col, beginKey, endKey, disableWal);
			} catch (org.rocksdb.RocksDBException e) {
				if (!isDeleteRangeUnsupported(e)) {
					throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
				}
				if (edb.nativeDeleteRangeFallbackLogged.compareAndSet(false, true)) {
					edb.logger.warn("Native RocksDB range delete is not supported by this database handle; falling back to batched point deletes. Further deleteRange fallback messages are suppressed");
				}
				deleteRangeByIterating(col, beginKey, endKey, disableWal);
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.deleteRangeTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private static boolean isDeleteRangeUnsupported(org.rocksdb.RocksDBException e) {
		if (e.getStatus() != null && e.getStatus().getCode() == Code.NotSupported) {
			return true;
		}
		String message = e.getMessage();
		return message != null
				&& message.contains("DeleteRange")
				&& (message.contains("not implemented") || message.contains("NotSupported"));
	}

	private void deleteRangeNative(ColumnInstance col, byte[] beginKey, byte[] endKey, boolean disableWal)
			throws org.rocksdb.RocksDBException {
		var rocks = edb.db.get();
		if (rocks instanceof OptimisticTransactionDB optimisticTransactionDB) {
			rocks = optimisticTransactionDB.getBaseDB();
		}
		try (var w = new LeakSafeWriteOptions(null); var wb = new LeakSafeWriteBatch()) {
			w.setDisableWAL(disableWal);
			wb.deleteRange(col.cfh(), beginKey, endKey);
			rocks.write(w, wb);
		}
	}

	private void deleteRangeByIterating(ColumnInstance col, byte[] beginKey, byte[] endKey, boolean disableWal)
			throws RocksDBException {
		var snapshot = edb.db.get().getSnapshot();
		try (var ro = edb.newReadOptions("delete-range-fallback-read-options");
				var startKeySlice = new Slice(beginKey);
				var endKeySlice = new Slice(endKey)) {
			ro.setSnapshot(snapshot);
			ro.setIterateLowerBound(startKeySlice);
			ro.setIterateUpperBound(endKeySlice);
			try (var it = edb.db.get().newIterator(col.cfh(), ro); var wb = new LeakSafeWriteBatch()) {
				int pendingDeletes = 0;
				it.seekToFirst();
				while (it.isValid()) {
					wb.delete(col.cfh(), it.key());
					pendingDeletes++;
					if (pendingDeletes >= 10_000) {
						writeDeleteRangeFallbackBatch(wb, disableWal);
						wb.clear();
						pendingDeletes = 0;
					}
					it.next();
				}
				if (pendingDeletes > 0) {
					writeDeleteRangeFallbackBatch(wb, disableWal);
				}
			} catch (org.rocksdb.RocksDBException e) {
				throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
			}
		} finally {
			edb.db.get().releaseSnapshot(snapshot);
		}
	}

	private void writeDeleteRangeFallbackBatch(LeakSafeWriteBatch wb, boolean disableWal)
			throws org.rocksdb.RocksDBException {
		try (var w = new LeakSafeWriteOptions(null)) {
			w.setDisableWAL(disableWal);
			edb.db.get().write(w, wb);
		}
	}

	private byte[] calculateDeleteRangeBeginKey(ColumnInstance col, @Nullable Keys startKeysInclusive) {
		if (startKeysInclusive != null && startKeysInclusive.keys().length > 0) {
			return col.calculateKeyBytes(startKeysInclusive.keys());
		}
		return new byte[0];
	}

	private byte[] calculateDeleteRangeEndKey(ColumnInstance col, @Nullable Keys endKeysExclusive) {
		if (endKeysExclusive != null && endKeysExclusive.keys().length > 0) {
			return col.calculateKeyBytes(endKeysExclusive.keys());
		}
		return encodedKeyUpperBound(col.finalKeySizeBytes());
	}

	private static byte[] encodedKeyUpperBound(int keySizeBytes) {
		var endKey = new byte[keySizeBytes + 1];
		Arrays.fill(endKey, 0, keySizeBytes, (byte) 0xff);
		return endKey;
	}
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			edb.actionLogger.logAction("putMulti (begin)",
					start,
					columnId,
					keysList.size(),
					valueList.size(),
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			if (keysList.size() != valueList.size()) {
				throw new IllegalArgumentException(
						"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
			}

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				synchronized (tx) {
					edb.requireLiveTransaction(updateId, tx);
					return putMultiWithUpdateId(tx, updateId, col, keysList, valueList, requestType);
				}
			}
			if (tx == null && WriteElisionRequest.from(requestType) != null) {
				return putMultiWithWriteElision(start, columnId, col, keysList, valueList, requestType);
			}

			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return putMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, valueList, requestType);
				}
			}
			return putMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, valueList, requestType);
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private <T> List<T> putMultiWithWriteElision(long start,
			long columnId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestPut<? super Buf, T> requestType) {
		var writeElisionRequest = Objects.requireNonNull(WriteElisionRequest.from(requestType));
		var probe = edb.writeElision.probeCachedLogicalValues(col, keysList, valueList);
		var dirtyPhysicalKeys = new HashSet<PhysicalKey>();
		List<T> responses = requestType instanceof RequestType.RequestEnsure<?>
				? null
				: new ArrayList<>(keysList.size());
		for (int i = 0; i < keysList.size(); i++) {
			var keys = keysList.get(i);
			var value = valueList.get(i);
			edb.actionLogger.logAction("putMulti (next)",
					start,
					columnId,
					keys,
					value,
					0L,
					null,
					null,
					requestType
			);
			var physicalKey = probe.physicalKeys()[i];
			var decision = probe.decisions()[i];
			if (decision != WriteElisionDecision.BYPASS_OVERSIZED
					&& dirtyPhysicalKeys.contains(physicalKey)) {
				decision = edb.writeElision.probeCachedLogicalValues(col, List.of(keys), List.of(value)).decisions()[0];
			}
			edb.writeElision.recordWriteElisionDecision(writeElisionRequest, decision);
			T result;
			if (decision == WriteElisionDecision.ELIDED) {
				result = RequestType.safeCast(requestType instanceof RequestType.RequestPreviousPresence<?>
						? Boolean.TRUE
						: null);
			} else {
				result = put(null, col, 0L, keys, value, requestType, false);
				dirtyPhysicalKeys.add(physicalKey);
			}
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
	}

	private <T> List<T> putMultiWithoutUpdateId(long start,
			long columnId,
			long transactionOrUpdateId,
			@Nullable Tx tx,
			ColumnInstance col,
			List<Keys> keysList,
			List<@NotNull Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		List<T> responses =
				requestType instanceof RequestType.RequestNothing<?>
						|| requestType instanceof RequestType.RequestEnsure<?>
						? null
						: new ArrayList<>(keysList.size());
		for (int i = 0; i < keysList.size(); i++) {
			var keys = keysList.get(i);
			var value = valueList.get(i);
			edb.actionLogger.logAction("putMulti (next)",
					start,
					columnId,
					keys,
					value,
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			T result = put(tx, col, 0L, keys, value, requestType);
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
	}

	private <T> List<T> putMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					edb.logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?>
						|| requestType instanceof RequestType.RequestEnsure<?>
						? null
						: new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						var value = valueList.get(i);
						T result = put(tx, col, 0L, keys, value, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = edb.closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in putMultiWithUpdateId", e);
							}
						}
						edb.closeTransaction(updateId, false);
						// We don't know which keys were locked internally by put() because we passed updateId=0 to it.
						// However, if we are here, it means we are in an updateId transaction.
						// Conflict happened during commit.
						// We must throw retry exception so the caller can retry.
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during retry in putMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during error in putMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			edb.closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keysList,
			@NotNull List<@NotNull Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			col = edb.beginColumnUse(columnId);
			edb.actionLogger.logAction("mergeMulti (begin)",
					start,
					columnId,
					keysList.size(),
					valueList.size(),
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			if (keysList.size() != valueList.size()) {
				throw new IllegalArgumentException(
						"keys length is different than values length: " + keysList.size() + " != " + valueList.size());
			}

			Tx tx;
			if (transactionOrUpdateId != 0) {
				tx = edb.getTransaction(transactionOrUpdateId, true);
			} else {
				tx = null;
			}
			long updateId = tx != null && tx.isFromGetForUpdate() ? transactionOrUpdateId : 0L;

			if (updateId != 0) {
				synchronized (tx) {
					edb.requireLiveTransaction(updateId, tx);
					return mergeMultiWithUpdateId(tx, updateId, col, keysList, valueList, requestType);
				}
			}

			if (tx != null) {
				synchronized (tx) {
					edb.requireLiveTransaction(transactionOrUpdateId, tx);
					return mergeMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, valueList, requestType);
				}
			}
			return mergeMultiWithoutUpdateId(start, columnId, transactionOrUpdateId, tx, col, keysList, valueList, requestType);
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.putMultiTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private <T> List<T> mergeMultiWithoutUpdateId(long start,
			long columnId,
			long transactionOrUpdateId,
			@Nullable Tx tx,
			ColumnInstance col,
			List<Keys> keysList,
			List<@NotNull Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		List<T> responses =
				requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
		for (int i = 0; i < keysList.size(); i++) {
			var keys = keysList.get(i);
			var value = valueList.get(i);
			edb.actionLogger.logAction("mergeMulti (next)",
					start,
					columnId,
					keys,
					value,
					transactionOrUpdateId,
					null,
					null,
					requestType
			);
			T result = merge(tx, col, 0L, keys, value, requestType);
			if (responses != null) {
				responses.add(result);
			}
		}
		return responses != null ? responses : List.of();
	}

	private <T> List<T> mergeMultiWithUpdateId(Tx tx,
			long updateId,
			ColumnInstance col,
			List<Keys> keysList,
			List<Buf> valueList,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		try {
			boolean committedOwnedTx;
			List<T> responses;
			do {
				boolean savePointSet = false;
				try {
					tx.val().setSavePoint();
					savePointSet = true;
				} catch (org.rocksdb.RocksDBException e) {
					edb.logger.debug("Failed to set savepoint", e);
				}
				responses = requestType instanceof RequestType.RequestNothing<?> ? null : new ArrayList<>(keysList.size());
				try {
					for (int i = 0; i < keysList.size(); i++) {
						var keys = keysList.get(i);
						var value = valueList.get(i);
						T result = merge(tx, col, 0L, keys, value, requestType);
						if (responses != null) {
							responses.add(result);
						}
					}

					boolean committed = edb.closeTransaction(updateId, true);
					if (!committed) {
						if (savePointSet) {
							try {
								tx.val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in mergeMultiWithUpdateId", e);
							}
						}
						edb.closeTransaction(updateId, false);
						throw new RocksDBRetryException();
					}
					committedOwnedTx = true;
				} catch (RocksDBRetryException e) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during retry in mergeMultiWithUpdateId", ex);
						}
					}
					throw e;
				} catch (Throwable t) {
					if (savePointSet && tx.val().isOwningHandle()) {
						try {
							tx.val().rollbackToSavePoint();
						} catch (org.rocksdb.RocksDBException | AssertionError ex) {
							edb.logger.debug("Failed to rollback to savepoint during error in mergeMultiWithUpdateId", ex);
						}
					}
					throw t;
				}
			} while (!committedOwnedTx);
			return responses != null ? responses : List.of();
		} catch (RocksDBRetryException e) {
			throw e;
		} catch (Throwable t) {
			edb.closeTransaction(updateId, false);
			if (t instanceof RocksDBException r) throw r;
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, t);
		}
	}

	<U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback) throws RocksDBException {
		return put(optionalDbWriter, col, updateId, keys, value, callback, true);
	}

	<U> U put(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, U> callback,
			boolean allowWriteElisionProbe) throws RocksDBException {
		// Check for null value
		col.checkNullableValue(value);
		try {
			var writeElisionRequest = WriteElisionRequest.from(callback);
			if (writeElisionRequest != null && allowWriteElisionProbe) {
				if (optionalDbWriter == null && updateId == 0L) {
					var physicalKey = new PhysicalKey(col.calculateKeyBytes(keys.keys()));
					var decision = edb.writeElision.probeCachedLogicalValue(col, physicalKey, keys, value);
					edb.writeElision.recordWriteElisionDecision(writeElisionRequest, decision);
					if (decision == WriteElisionDecision.ELIDED) {
						return RequestType.safeCast(callback instanceof RequestType.RequestPreviousPresence<?>
								? Boolean.TRUE
								: null);
					}
				} else {
					edb.writeElision.recordWriteElisionDecision(writeElisionRequest, WriteElisionDecision.BYPASS_WRITER);
				}
			}
			boolean requirePreviousValue = RequestType.requiresGettingPreviousValue(callback);
			boolean requirePreviousPresence = RequestType.requiresGettingPreviousPresence(callback);
			boolean needsTx = col.hasBuckets() || requirePreviousValue || requirePreviousPresence;
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && (requirePreviousValue
					|| requirePreviousPresence)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}
			if (col.hasBuckets() && (optionalDbWriter != null && !(optionalDbWriter instanceof Tx))) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Column with buckets don't support write batches"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? edb.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					Buf previousValue;
					byte[] calculatedKeyArray = col.calculateKeyBytes(keys.keys());
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = edb.newPointReadOptions(null)) {
							var previousRawBucketByteArray
									= pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							previousValue = transformResultValue(col, bucket.addElement(bucketElementKeys, value));
							var v = EmbeddedDBWriteElision.jniArray(bucket.toSegment());
							txPut((Tx) newTx, col.cfh(), calculatedKeyArray, v);
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						if (RequestType.requiresGettingPreviousValue(callback)) {
							assert newTx instanceof Tx;
							try (var readOptions = edb.newPointReadOptions(null)) {
								byte[] previousValueByteArray
										= pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
								didGetForUpdateInternally = true;
								previousValue = transformResultValue(col, toBuf(previousValueByteArray));
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else if (RequestType.requiresGettingPreviousPresence(callback)) {
							// todo: in the future this should be replaced with just keyExists
							assert newTx instanceof Tx;
							try (var readOptions = edb.newPointReadOptions(null)) {
								byte[] previousValueByteArray = pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
								didGetForUpdateInternally = true;
								previousValue = previousValueByteArray != null ? emptyBuf() : null;
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else {
							previousValue = null;
						}
						switch (newTx) {
							case WB wb -> wb.wb().put(col.cfh(), calculatedKeyArray, EmbeddedDBWriteElision.jniArray(value));
							case SSTWriter sstWriter -> {
								var valueBB = EmbeddedDBWriteElision.jniArray(col.schema().hasValue() ? value : dummyRocksDBEmptyValue());
								sstWriter.put(calculatedKeyArray, valueBB);
							}
							case Tx t -> txPut(t, col.cfh(), calculatedKeyArray, EmbeddedDBWriteElision.jniArray(value));
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									var valueBB = EmbeddedDBWriteElision.jniArray(col.schema().hasValue() ? value : dummyRocksDBEmptyValue());
									edb.db.get().put(col.cfh(), w, calculatedKeyArray, valueBB);
								}
							}
						}
					}
					result = RequestType.safeCast(switch (callback) {
						case RequestType.RequestNothing<?> ignored -> null;
						case RequestType.RequestEnsure<?> ignored -> null;
						case RequestType.RequestPrevious<?> ignored -> previousValue;
						case RequestType.RequestPreviousPresence<?> ignored -> previousValue != null;
						case RequestType.RequestChanged<?> ignored -> !Utils.valueEquals(previousValue, value);
						case RequestType.RequestDelta<?> ignored -> new Delta<>(previousValue, value);
					});

					if (updateId != 0L) {
						boolean committed = edb.closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in put", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = edb.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							// FIX: We MUST close the failed transaction and open a fresh one.
							// If we reuse it, the C++ WriteBatch grows indefinitely on every retry!
							edb.closeTransactionInternal((Tx) newTx, false);
							newTx = edb.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
				} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					edb.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				edb.closeTransaction(updateId, false);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	<U> U delete(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			RequestType.RequestDelete<? super Buf, U> callback) throws RocksDBException {
		try {
			boolean requirePreviousValue = RequestType.requiresGettingPreviousValue(callback);
			boolean requirePreviousPresence = RequestType.requiresGettingPreviousPresence(callback);
			boolean needsTx = col.hasBuckets() || requirePreviousValue || requirePreviousPresence;
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && (requirePreviousValue
					|| requirePreviousPresence)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the previous value or delta, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}
			if (col.hasBuckets() && (optionalDbWriter != null && !(optionalDbWriter instanceof Tx))) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Column with buckets don't support write batches"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? edb.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					Buf previousValue;
					byte[] calculatedKeyArray = col.calculateKeyBytes(keys.keys());
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = edb.newPointReadOptions(null)) {
							var previousRawBucketByteArray = pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							previousValue = transformResultValue(col, bucket.removeElement(bucketElementKeys));
							var v = EmbeddedDBWriteElision.jniArray(bucket.toSegment());
							txPut((Tx) newTx, col.cfh(), calculatedKeyArray, v);
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						if (RequestType.requiresGettingPreviousValue(callback)) {
							assert newTx instanceof Tx;
							try (var readOptions = edb.newPointReadOptions(null)) {
								byte[] previousValueByteArray;
								previousValueByteArray = pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
								didGetForUpdateInternally = true;
								previousValue = transformResultValue(col, toBuf(previousValueByteArray));
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else if (RequestType.requiresGettingPreviousPresence(callback)) {
							// todo: in the future this should be replaced with just keyExists
							assert newTx instanceof Tx;
							try (var readOptions = edb.newPointReadOptions(null)) {
								byte[] previousValueByteArray;
								previousValueByteArray = pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
								didGetForUpdateInternally = true;
								previousValue = previousValueByteArray != null ? emptyBuf() : null;
							} catch (org.rocksdb.RocksDBException e) {
								throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
							}
						} else {
							previousValue = null;
						}
						switch (newTx) {
							case WB wb -> wb.wb().delete(col.cfh(), calculatedKeyArray);
							case SSTWriter sstWriter -> {
								// SSTWriter doesn't support delete in standard way if using ingest, but here we can't delete from SST.
								// Actually SST ingestion is usually for loading new data.
								// RocksDB SstFileWriter doesn't seem to support delete explicitly?
								// Actually it does if we put empty value? No.
								// Delete is not supported in SST ingestion usually for update-in-place.
								// But we can check if we should support it.
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Delete not supported in SST writer mode");
							}
							case Tx t -> txDelete(t, col.cfh(), calculatedKeyArray);
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									edb.db.get().delete(col.cfh(), w, calculatedKeyArray);
								}
							}
						}
					}
					result = RequestType.safeCast(switch (callback) {
						case RequestType.RequestNothing<?> ignored -> null;
						case RequestType.RequestPrevious<?> ignored -> previousValue;
						case RequestType.RequestPreviousPresence<?> ignored -> previousValue != null;
						default -> throw new IllegalStateException("Unexpected value: " + callback);
					});

					if (updateId != 0L) {
						boolean committed = edb.closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in delete", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = edb.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							// FIX: We MUST close the failed transaction and open a fresh one.
							// If we reuse it, the C++ WriteBatch grows indefinitely on every retry!
							edb.closeTransactionInternal((Tx) newTx, false);
							newTx = edb.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
 			} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					edb.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				edb.closeTransaction(updateId, false);
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	<U> U merge(@Nullable DBWriter optionalDbWriter,
			ColumnInstance col,
			long updateId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, U> callback) throws RocksDBException {
		// Forget a failure of an earlier merge on this thread: only one of this merge may name the operator.
		FFMAbstractMergeOperator.takeFailureOnThisThread();
		try {
			boolean needsTx = col.hasBuckets() || !(callback instanceof RequestType.RequestNothing<?>);
			if (optionalDbWriter instanceof Tx tx && tx.isFromGetForUpdate() && callback instanceof RequestType.RequestMerged<?>) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"You can't get the merged value, when you are already updating that value"
				);
			}
			if (updateId != 0L && !(optionalDbWriter instanceof Tx)) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Update id must be accompanied with a valid transaction"
				);
			}

			U result;
			DBWriter newTx;
			boolean owningNewTx = needsTx && optionalDbWriter == null;
			// Retry using a transaction: transactions are required to handle this kind of data
			newTx = owningNewTx ? edb.openTransactionInternal(120_000, false) : optionalDbWriter;
			try {
				boolean didGetForUpdateInternally = false;
				boolean committedOwnedTx;
				do {
					byte[] calculatedKeyArray = col.calculateKeyBytes(keys.keys());
					Buf calculatedKey = Buf.wrap(calculatedKeyArray);
					if (updateId != 0L) {
						assert !owningNewTx;
						((Tx) newTx).val().setSavePoint();
					}
					if (col.hasBuckets()) {
						assert newTx instanceof Tx;
						var bucketElementKeys = col.getBucketElementKeys(keys.keys());
						try (var readOptions = edb.newPointReadOptions(null)) {
							var previousRawBucketByteArray = pointGetForUpdate((Tx) newTx, readOptions, col.cfh(), calculatedKeyArray);
							didGetForUpdateInternally = true;
							Buf previousRawBucket = toBuf(previousRawBucketByteArray);
							var bucket = previousRawBucket != null ? new Bucket(col, previousRawBucket) : new Bucket(col);
							var existing = bucket.getElement(bucketElementKeys);
							Buf mergedValue;
							if (existing != null) {
								var mergedRes = mergeInJava(col, calculatedKey, existing, value);
								mergedValue = mergedRes != null ? mergedRes : existing;
							} else {
								mergedValue = value;
							}
							bucket.addElement(bucketElementKeys, mergedValue);
							var v = EmbeddedDBWriteElision.jniArray(bucket.toSegment());
							txPut((Tx) newTx, col.cfh(), calculatedKeyArray, v);
							if (callback instanceof RequestType.RequestMerged<?>) {
								var merged = bucket.getElement(col.getBucketElementKeys(keys.keys()));
								result = RequestType.safeCast(transformResultValue(col, merged));
							} else {
								result = null;
							}
						} catch (org.rocksdb.RocksDBException e) {
							throw RocksDBException.of(RocksDBErrorType.PUT_1, e);
						}
					} else {
						switch (newTx) {
							case WB wb -> wb.wb().merge(col.cfh(), calculatedKeyArray, EmbeddedDBWriteElision.jniArray(value));
							case SSTWriter ignored ->
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Merge not supported with SST writer");
							case Tx t -> txMerge(t, col.cfh(), calculatedKeyArray, value);
							case null -> {
								try (var w = new LeakSafeWriteOptions(null)) {
									edb.db.get().merge(col.cfh(), w, calculatedKeyArray, EmbeddedDBWriteElision.jniArray(value));
								}
							}
						}
						if (callback instanceof RequestType.RequestMerged<?>) {
							Buf merged;
							try (var readOptions = edb.newPointReadOptions(null)) {
								merged = edb.dbGet(newTx instanceof Tx ? (Tx) newTx : null, col, readOptions, calculatedKey);
							}
							result = RequestType.safeCast(merged);
						} else {
							result = null;
						}
					}

					if (updateId != 0L) {
						boolean committed = edb.closeTransaction(updateId, true);
						if (!committed) {
							try {
								((Tx) newTx).val().rollbackToSavePoint();
							} catch (org.rocksdb.RocksDBException | AssertionError e) {
								edb.logger.debug("Failed to rollback to savepoint during commit failure in merge", e);
							}
							int undosCount = 0;
							if (((Tx) newTx).isFromGetForUpdate()) {
								undosCount++;
							}
							if (didGetForUpdateInternally) {
								undosCount++;
							}
							for (int i = 0; i < undosCount; i++) {
								((Tx) newTx).val().undoGetForUpdate(col.cfh(), calculatedKeyArray);
							}
							throw new RocksDBRetryException();
						}
					}

					if (owningNewTx) {
						committedOwnedTx = edb.closeTransactionInternal((Tx) newTx, true);
						if (!committedOwnedTx) {
							edb.closeTransactionInternal((Tx) newTx, false);
							newTx = edb.openTransactionInternal(120_000, false);
							didGetForUpdateInternally = false;
							Thread.yield();
						}
					} else {
						committedOwnedTx = true;
					}
				} while (!committedOwnedTx);
			} finally {
				if (owningNewTx) {
					edb.closeTransactionInternal((Tx) newTx, false);
				}
			}
			return result;
		} catch (Exception ex) {
			if (updateId != 0L && !(ex instanceof RocksDBRetryException)) {
				edb.closeTransaction(updateId, false);
			}
			// A read of the merged value runs the operator on this thread, and RocksDB reports its failure as a
			// corruption: name the operator instead, so that the failure is not taken for a storage one.
			var operatorFailure = FFMAbstractMergeOperator.takeFailureOnThisThread();
			if (operatorFailure != null && !(ex instanceof RocksDBException rocksDBException
					&& rocksDBException.getErrorUniqueId() == RocksDBErrorType.MERGE_OPERATOR_FAILED)) {
				var failure = EmbeddedDB.mergeOperatorFailed(operatorFailure);
				failure.addSuppressed(ex);
				throw failure;
			}
			if (ex instanceof RocksDBException rocksDBException) {
				throw rocksDBException;
			} else {
				throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
			}
		}
	}

	/**
	 * Merges {@code value} into {@code existing} with the column's operator, called from Java (bucketed columns keep
	 * their elements inside one RocksDB value, so RocksDB cannot merge them). A failure of the operator is reported
	 * as {@link RocksDBErrorType#MERGE_OPERATOR_FAILED}; a failure of the JVM itself is not the operator's.
	 */
	private static @Nullable Buf mergeInJava(ColumnInstance col, Buf key, Buf existing, Buf value) {
		var operator = col.mergeOperator();
		if (operator == null) {
			throw RocksDBException.of(RocksDBErrorType.MERGE_OPERATOR_FAILED, "The column has no merge operator");
		}
		try {
			return operator.merge(key, existing, List.of(value));
		} catch (Throwable operatorFailure) {
			if (Failures.isJvmFailure(operatorFailure)) {
				// Anywhere in the chain: an operator may wrap it.
				throw Failures.rethrow(operatorFailure);
			}
			throw EmbeddedDB.mergeOperatorFailed(operatorFailure);
		}
	}

	private Buf transformResultValue(ColumnInstance col, Buf realPreviousValue) {
		return col.schema().hasValue() ? realPreviousValue : (realPreviousValue != null ? emptyBuf() : null);
	}
}
