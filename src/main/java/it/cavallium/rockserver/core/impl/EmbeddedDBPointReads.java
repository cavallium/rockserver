package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.PinnedMultiGet;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import it.cavallium.rockserver.core.impl.EmbeddedDB.*;

/**
 * Point reads of {@link EmbeddedDB}: get, the native fast get, getMulti (paged, pinned and async).
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBPointReads {

	private final EmbeddedDB edb;

	EmbeddedDBPointReads(EmbeddedDB edb) {
		this.edb = edb;
	}

	public @Nullable FastGetResult tryGetFast(long columnId, Keys keys, FastGetOutput output) {
		Objects.requireNonNull(keys, "keys");
		Objects.requireNonNull(output, "output");
		if (!edb.fastGet) {
			return null;
		}

		long start = System.nanoTime();
		ColumnInstance col = null;
		boolean operationStarted = false;
		boolean ownershipTransferred = false;
		try {
			col = edb.beginColumnUse(columnId);
			if (col.hasBuckets() || !col.schema().hasValue()) {
				return null;
			}
			edb.ops.beginOp();
			operationStarted = true;

			Buf calculatedKey = col.calculateKey(keys.keys());
			var reader = Objects.requireNonNull(edb.fastGetReader);
			byte[] keyArray = calculatedKey.getBackingByteArray();
			int keyOffset = calculatedKey.getBackingByteArrayOffset();
			int keyLength = calculatedKey.getBackingByteArrayLength();

			if (output == FastGetOutput.EXACT_HEAP) {
				byte[] value = reader.getHeap(col.cfh(), keyArray, keyOffset, keyLength);
				return value == null
						? new FastGetResult(false, null, false, null)
						: new FastGetResult(true, Buf.wrap(value), false, null);
			}

			NativeRocksDBGet.PinnedGetLease lease = reader.getPinned(
					col.cfh(), keyArray, keyOffset, keyLength);
			if (lease == null) {
				return new FastGetResult(false, null, false, null);
			}
			boolean leaseTransferred = false;
			try {
					if (output != FastGetOutput.PINNED
							&& !prefersPinnedFastGet(lease.value().size())) {
						byte[] value = lease.copyAndClose();
						return new FastGetResult(true, Buf.wrap(value), false, null);
					}

					var pinnedValue = lease.value();
					ColumnInstance leasedColumn = col;
				var result = new FastGetResult(true,
						pinnedValue,
						true,
						() -> closePinnedFastGet(lease, leasedColumn, start));
				ownershipTransferred = true;
				leaseTransferred = true;
				return result;
			} finally {
				if (!leaseTransferred) {
					lease.close();
				}
			}
		} catch (org.rocksdb.RocksDBException exception) {
			throw RocksDBException.of(RocksDBErrorType.GET_1, exception);
		} finally {
			if (operationStarted) edb.storageMetrics.recordCompactionForegroundRead(start, 0);
			if (!ownershipTransferred) {
				if (operationStarted) {
					edb.ops.endOp();
				}
				if (col != null) {
					col.endUse();
				}
				edb.getTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
			}
		}
	}

	private static boolean prefersPinnedFastGet(int valueBytes) {
		if (EmbeddedDB.PINNED_GET_MIN_BYTES_OVERRIDE >= 0) {
			return valueBytes >= EmbeddedDB.PINNED_GET_MIN_BYTES_OVERRIDE;
		}
		return valueBytes >= 32 * 1024;
	}

	private void closePinnedFastGet(NativeRocksDBGet.PinnedGetLease lease,
			ColumnInstance col,
			long start) {
		Throwable failure = null;
		try {
			lease.close();
		} catch (Throwable exception) {
			failure = exception;
		}
		try {
			edb.ops.endOp();
		} catch (Throwable exception) {
			if (failure == null) {
				failure = exception;
			} else {
				failure.addSuppressed(exception);
			}
		}
		try {
			col.endUse();
		} catch (Throwable exception) {
			if (failure == null) {
				failure = exception;
			} else {
				failure.addSuppressed(exception);
			}
		} finally {
			edb.getTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
		if (failure instanceof RuntimeException runtimeException) {
			throw runtimeException;
		}
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure != null) {
			throw new IllegalStateException("Failed to close pinned gRPC Get", failure);
		}
	}
	public <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType)
			throws RocksDBException {
		return get(transactionOrUpdateId,
				columnId,
				keys,
				requestType,
				WorkloadProfile.BATCH);
	}
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType,
			WorkloadProfile workloadProfile) throws RocksDBException {
		var start = System.nanoTime();
		ColumnInstance col = null;
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction("Get", start, columnId, keys, null, transactionOrUpdateId, null, null, requestType);
			// Column id
			col = edb.beginColumnUse(columnId);
			Tx prevTx = transactionOrUpdateId != 0
					? edb.getTransaction(transactionOrUpdateId, true, workloadProfile)
					: null;
			Tx tx;
			long updateId;
			if (requestType instanceof RequestType.RequestForUpdate<?>) {
				if (prevTx == null) {
					tx = edb.openTransactionInternal(EmbeddedDB.MAX_TRANSACTION_DURATION_MS,
							true,
							workloadProfile,
							false);
					updateId = edb.allocateTransactionInternal(tx);
				} else {
					tx = prevTx;
					updateId = transactionOrUpdateId;
				}
			} else {
				tx = prevTx;
				updateId = 0;
			}

			try {
				T result;
				if (tx != null) {
					synchronized (tx) {
						edb.requireLiveTransaction(transactionOrUpdateId != 0L ? transactionOrUpdateId : updateId, tx);
						result = get(tx, updateId, col, keys, requestType);
					}
				} else {
					result = get(tx, updateId, col, keys, requestType);
				}
				edb.actionLogger.logAction("Get (result)",
						start,
						columnId,
						keys,
						result,
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				return result;
			} catch (Throwable ex) {
				edb.actionLogger.logAction("Get (result)",
						start,
						columnId,
						keys,
						"failure (exception)",
						transactionOrUpdateId,
						null,
						null,
						requestType
				);
				if (tx != prevTx) {
					edb.closeTransaction(updateId, false);
				}
				throw ex;
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.getTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public MultiGetPage getMulti(long columnId, @NotNull List<Keys> keys, @NotNull RangeBudget budget) {
		return getMulti(columnId, keys, budget, WorkloadProfile.BATCH, Long.MAX_VALUE);
	}
	public MultiGetPage getMulti(long columnId, @NotNull List<Keys> keys, @NotNull RangeBudget budget,
			WorkloadProfile workloadProfile, long contextMonotonicDeadlineNanos) {
		checkMultiGetDeadline(contextMonotonicDeadlineNanos);
		var requestKeys = MultiGetPage.copyRequestKeys(keys, budget);
		return readMultiGetPage(columnId, requestKeys, budget, workloadProfile, contextMonotonicDeadlineNanos, () -> false);
	}
	public CompletableFuture<MultiGetPage> getMultiAsyncInternal(long columnId, @NotNull List<Keys> keys,
			@NotNull RangeBudget budget, WorkloadProfile workloadProfile, Executor executor,
			long contextMonotonicDeadlineNanos) {
		try {
			checkMultiGetDeadline(contextMonotonicDeadlineNanos);
			var requestKeys = MultiGetPage.copyRequestKeys(keys, budget);
			Objects.requireNonNull(workloadProfile, "workloadProfile");
			Objects.requireNonNull(executor, "executor");
			var request = new AsyncGetMultiRequest(columnId, requestKeys, budget, workloadProfile,
					executor, contextMonotonicDeadlineNanos);
			try {
				executor.execute(request);
			} catch (Throwable failure) {
				request.rejectBeforeRun(failure);
			}
			return request;
		} catch (Throwable failure) {
			return CompletableFuture.failedFuture(failure);
		}
	}

	private MultiGetPage readMultiGetPage(long columnId, List<Keys> keys, RangeBudget budget,
			WorkloadProfile workloadProfile, long monotonicDeadlineNanos, BooleanSupplier cancelled) {
		try (var lease = readMultiGetPinned(columnId, keys, budget, workloadProfile,
				monotonicDeadlineNanos, cancelled)) {
			var page = lease.page();
			var values = new ArrayList<Buf>(page.values().size());
			for (var value : page.values()) {
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				values.add(value == null ? null : Buf.wrap(value.toByteArray()));
			}
			checkMultiGetActive(monotonicDeadlineNanos, cancelled);
			return new MultiGetPage(values, page.hasMore());
		}
	}
	public PinnedMultiGetResult getMultiPinned(long columnId, @NotNull List<Keys> keys,
			@NotNull RangeBudget budget, WorkloadProfile workloadProfile,
			long monotonicDeadlineNanos, BooleanSupplier cancelled) {
		Objects.requireNonNull(cancelled, "cancelled");
		checkMultiGetActive(monotonicDeadlineNanos, cancelled);
		var requestKeys = MultiGetPage.copyRequestKeys(keys, budget);
		return readMultiGetPinned(columnId, requestKeys, budget, workloadProfile,
				monotonicDeadlineNanos, cancelled);
	}

	private PinnedMultiGetResult readMultiGetPinned(long columnId, List<Keys> keys, RangeBudget budget,
			WorkloadProfile workloadProfile, long monotonicDeadlineNanos, BooleanSupplier cancelled) {
		Objects.requireNonNull(workloadProfile, "workloadProfile");
		checkMultiGetActive(monotonicDeadlineNanos, cancelled);
		edb.ops.beginOp();
		ColumnInstance.ColumnUse columnUse = null;
		PinnedMultiGet pinned = null;
		boolean transferred = false;
		try {
			columnUse = edb.acquireColumnUse(columnId);
			var col = columnUse.column();
			if (!col.schema().hasValue()) {
				throw RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"MultiGet requires a column with values");
			}
			if (keys.isEmpty()) {
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				var result = edb.new PinnedMultiGetResult(MultiGetPage.empty(), null, columnUse);
				transferred = true;
				return result;
			}
			checkMultiGetActive(monotonicDeadlineNanos, cancelled);
			// Even missing values consume a response slot; do not read keys that cannot fit.
			long maximumSlots = Math.max(0L,
					(budget.maxBytes() - MultiGetPage.RESPONSE_BASE_BYTES) / MultiGetPage.RESPONSE_ITEM_BYTES);
			int count = (int) Math.min(Math.min(keys.size(), budget.maxItems()), maximumSlots);
			if (count == 0) {
				throw RocksDBException.of(RocksDBErrorType.GET_MULTI_ITEM_TOO_LARGE,
						"MultiGet byte budget cannot hold one response slot");
			}
			var nativeKeys = new byte[count][];
			long physicalKeyBytes = 0;
			for (int i = 0; i < count; i++) {
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				var calculatedKey = col.calculateKeyBytes(keys.get(i).keys());
				physicalKeyBytes += calculatedKey.length;
				if (physicalKeyBytes > MultiGetPage.MAX_KEY_BYTES) {
					throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
							"MultiGet physical keys exceed server byte limit");
				}
				nativeKeys[i] = calculatedKey;
			}
			// Retain one snapshot for the uncommon first-key retry after native key sorting.
			var nativeDb = edb.db.get();
			var snapshot = nativeDb.getSnapshot();
			MultiGetPage page;
			try (var readOptions = edb.newReadOptions("get-multi-read-options")) {
				readOptions.setSnapshot(snapshot);
				readOptions.setDeadline(edb.ranges.nativeReadDeadlineMicros(monotonicDeadlineNanos));
				readOptions.setFillCache(true);
				// This is a soft value limit, not a hard native allocation bound: a
				// native chunk can retain the value which crosses it before aborting.
				readOptions.setValueSizeSoftLimit(Math.max(1L, budget.maxBytes()
						- MultiGetPage.RESPONSE_BASE_BYTES - count * MultiGetPage.RESPONSE_ITEM_BYTES));
				pinned = new PinnedMultiGet();
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				runPinnedValueMultiGet(col, readOptions, nativeKeys, pinned);
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				if (pinned.isAborted(0)) {
					// RocksDB may sort keys internally. An aborted first input must
					// still make progress, rather than publish an empty continuation.
					pinned.close();
					pinned = null;
					checkMultiGetActive(monotonicDeadlineNanos, cancelled);
					pinned = new PinnedMultiGet();
					// Resolve this one value so an oversized first item receives
					// the page-budget error below instead of another soft-limit abort.
					readOptions.setValueSizeSoftLimit(Long.MAX_VALUE);
					runPinnedValueMultiGet(col, readOptions, new byte[][] { nativeKeys[0] }, pinned);
					checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				}
				int accepted = 0;
				long responseBytes = MultiGetPage.RESPONSE_BASE_BYTES;
				for (int i = 0; i < pinned.size(); i++) {
					checkMultiGetActive(monotonicDeadlineNanos, cancelled);
					if (pinned.isAborted(i)) {
						if (i == 0) {
							throw new IllegalStateException("Single-key pinned MultiGet aborted without progress");
						}
						break;
					}
					long physicalValueBytes = pinned.isPresent(i) ? pinned.valueSize(i) : 0L;
					long itemBytes = EmbeddedDB.saturatingAdd(MultiGetPage.RESPONSE_ITEM_BYTES, physicalValueBytes);
					if (itemBytes > budget.maxBytes() - responseBytes) {
						if (accepted == 0) {
							throw RocksDBException.of(RocksDBErrorType.GET_MULTI_ITEM_TOO_LARGE,
									"First MultiGet physical value exceeds the page byte budget");
						}
						break;
					}
					responseBytes += itemBytes;
					accepted++;
				}
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				// Drop rejected pins before creating views: truncation invalidates
				// existing views, and only the accepted prefix must survive serialization.
				pinned.retainPrefix(accepted);
				var values = new ArrayList<Buf>(accepted);
				for (int i = 0; i < accepted; i++) {
					checkMultiGetActive(monotonicDeadlineNanos, cancelled);
					Buf value = pinned.isPresent(i) ? new MemorySegmentBuf(pinned.value(i)) : null;
					if (value != null && col.hasBuckets()) {
						value = new Bucket(col, value).getElement(col.getBucketElementKeys(keys.get(i).keys()));
					}
					values.add(value);
				}
				checkMultiGetActive(monotonicDeadlineNanos, cancelled);
				page = new MultiGetPage(values, accepted < keys.size());
			} finally {
				nativeDb.releaseSnapshot(snapshot);
			}
			checkMultiGetActive(monotonicDeadlineNanos, cancelled);
			var result = edb.new PinnedMultiGetResult(page, pinned, columnUse);
			transferred = true;
			return result;
		} catch (org.rocksdb.RocksDBException failure) {
			throw EmbeddedDB.mapIteratorStatusException(failure);
		} finally {
			if (!transferred) {
				try {
					if (pinned != null) {
						pinned.close();
					}
				} finally {
					try {
						if (columnUse != null) {
							columnUse.close();
						}
					} finally {
						edb.ops.endOp();
					}
				}
			}
		}
	}

	private void checkMultiGetActive(long deadline, BooleanSupplier cancelled) {
		checkMultiGetDeadline(deadline);
		if (cancelled.getAsBoolean()) {
			throw new CancellationException();
		}
	}

	private void checkMultiGetDeadline(long monotonicDeadlineNanos) {
		if (edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
			throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
		}
	}

	private void runPinnedValueMultiGet(ColumnInstance col, ReadOptions readOptions,
			byte[][] keys, PinnedMultiGet pinned) throws org.rocksdb.RocksDBException {
		long start = System.nanoTime();
		edb.activeNativeMultiGets.incrementAndGet();
		try {
			pinned.read(edb.db.get(), col.cfh(), readOptions, keys);
		} finally {
			edb.activeNativeMultiGets.decrementAndGet();
			edb.storageMetrics.recordCompactionForegroundRead(start, keys.length);
		}
	}

	private final class AsyncGetMultiRequest extends CompletableFuture<MultiGetPage>
			implements Runnable, RWScheduler.RejectionAwareTask, RWScheduler.EstimatedWork {
		private final long columnId;
		private final List<Keys> keys;
		private final RangeBudget budget;
		private final WorkloadProfile workloadProfile;
		private final Executor executor;
		private final long deadline;
		private final long estimatedBytes;
		private boolean running;
		private boolean finished;
		private boolean cancelRequested;

		@Override
		public long estimatedBytes() {
			return estimatedBytes;
		}

		private AsyncGetMultiRequest(long columnId, List<Keys> keys, RangeBudget budget,
				WorkloadProfile workloadProfile, Executor executor, long deadline) {
			this.columnId = columnId;
			this.keys = keys;
			this.budget = budget;
			this.workloadProfile = workloadProfile;
			this.executor = executor;
			this.deadline = deadline;
			this.estimatedBytes = new RocksDBAPICommand.RocksDBAPICommandSingle.GetMulti(
					columnId, keys, budget).estimatedBytes();
		}

		@Override
		public void run() {
			synchronized (this) {
				if (finished || running) {
					return;
				}
				running = true;
			}
			MultiGetPage result = null;
			Throwable failure = null;
			try {
				result = readMultiGetPage(columnId, keys, budget, workloadProfile, deadline, this::isCancellationRequested);
			} catch (Throwable error) {
				failure = error;
			}
			synchronized (this) {
				finished = true;
				if (cancelRequested && failure == null) {
					failure = new CancellationException();
				}
			}
			// readMultiGetPage has copied its values and released native ownership before publication.
			if (failure != null) {
				completeExceptionally(failure);
			} else {
				complete(Objects.requireNonNull(result));
			}
		}

		private synchronized boolean isCancellationRequested() {
			return cancelRequested;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			synchronized (this) {
				if (finished) {
					return false;
				}
				if (running) {
					cancelRequested = true;
					return false;
				}
				finished = true;
			}
			edb.scheduler.removeQueuedTask(executor, this);
			return super.cancel(false);
		}

		private void rejectBeforeRun(Throwable failure) {
			synchronized (this) {
				if (finished || running) {
					return;
				}
				finished = true;
			}
			completeExceptionally(failure);
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectBeforeRun(failure);
		}
	}

	<T> T get(Tx tx, long updateId, ColumnInstance col, Keys keys, RequestGet<? super Buf, T> callback)
			throws RocksDBException {
		try {
			if (!col.schema().hasValue() && RequestType.requiresGettingCurrentValue(callback)) {
				throw RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"The specified callback requires a return value, but this column does not have values!"
				);
			}
			Buf foundValue;
			boolean existsValue;

			Buf calculatedKey = col.calculateKey(keys.keys());
			if (col.hasBuckets()) {
				var bucketElementKeys = col.getBucketElementKeys(keys.keys());
				try {
					Buf previousRawBucket = edb.dbGetWithDefaultOptions(tx, col, calculatedKey, callback instanceof RequestType.RequestForUpdate<?>);
					if (previousRawBucket != null) {
						var bucket = new Bucket(col, previousRawBucket);
						foundValue = bucket.getElement(bucketElementKeys);
					} else {
						foundValue = null;
					}
					existsValue = foundValue != null;
				} catch (org.rocksdb.RocksDBException e) {
					throw RocksDBException.of(RocksDBErrorType.GET_1, e);
				}
			} else {
				boolean shouldGetCurrent = RequestType.requiresGettingCurrentValue(callback) || (tx != null
						&& callback instanceof RequestType.RequestExists<?>);
				if (shouldGetCurrent) {
					try {
						foundValue = edb.dbGetWithDefaultOptions(tx, col, calculatedKey, callback instanceof RequestType.RequestForUpdate<?>);
						existsValue = foundValue != null;
					} catch (org.rocksdb.RocksDBException e) {
						throw RocksDBException.of(RocksDBErrorType.PUT_2, e);
					}
				} else if (callback instanceof RequestType.RequestExists<?>) {
					// tx is always null here
					//noinspection ConstantValue
					assert tx == null;
					foundValue = null;
					existsValue = edb.db
							.get()
							.keyExists(col.cfh(),
									calculatedKey.getBackingByteArray(),
									calculatedKey.getBackingByteArrayOffset(),
									calculatedKey.getBackingByteArrayLength()
							);
				} else {
					foundValue = null;
					existsValue = false;
				}
			}
			return RequestType.safeCast(switch (callback) {
				case RequestType.RequestNothing<?> ignored -> null;
				case RequestType.RequestCurrent<?> ignored -> foundValue;
				case RequestType.RequestForUpdate<?> ignored -> {
					assert updateId != 0;
					yield new UpdateContext<>(foundValue, updateId);
				}
				case RequestType.RequestExists<?> ignored -> existsValue;
			});
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, ex);
		}
	}
}
