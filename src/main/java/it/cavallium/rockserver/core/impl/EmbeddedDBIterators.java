package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksIterator;
import it.cavallium.rockserver.core.impl.EmbeddedDB.*;

/**
 * Iterators of {@link EmbeddedDB}: open/close, seek, subsequent and quantum advance/read.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBIterators {

	private final EmbeddedDB edb;

	EmbeddedDBIterators(EmbeddedDB edb) {
		this.edb = edb;
	}

	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			Duration iteratorLeaseTtl) throws RocksDBException {
		return openIterator(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				LeaseTtl.toMillisCeil(iteratorLeaseTtl, "iteratorLeaseTtl"),
				WorkloadProfile.BATCH);
	}
	public long openIterator(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs,
			WorkloadProfile workloadProfile) throws RocksDBException {
		var start = System.nanoTime();
		// Protect iterator construction as active work; the installed iterator's
		// longer lifetime is accounted separately from shutdown admission.
		edb.ops.beginOp();
		boolean resourceLeaseRetained = false;
		try {
			edb.actionLogger.logAction("OpenIterator",
					start,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					transactionId,
					reverse,
					timeoutMs,
					null
			);
			var expirationTimestamp = iteratorExpirationTimestamp(timeoutMs);
			var columnUse = edb.acquireColumnUse(columnId);
			var col = columnUse.column();
			var state = new IteratorState(columnUse, reverse);
			RocksIterator it = null;
			REntry<RocksIterator> itEntry = null;
			try {
				var calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0
						? col.calculateKey(startKeysInclusive.keys())
						: null;
				var calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0
						? col.calculateKey(endKeysExclusive.keys())
						: null;
				var startKeySlice = calculatedStartKey != null ? edb.toSlice(calculatedStartKey) : null;
				if (startKeySlice != null) {
					state.add(startKeySlice);
				}
				var endKeySlice = calculatedEndKey != null ? edb.toSlice(calculatedEndKey) : null;
				if (endKeySlice != null) {
					state.add(endKeySlice);
				}

				var ro = edb.newReadOptions("open-iterator-read-options");
				state.add(ro);
				ro.setAsyncIo(false);
				ro.setDeadline(iteratorReadDeadlineMicros(timeoutMs));
				if (startKeySlice != null) {
					ro.setIterateLowerBound(startKeySlice);
				}
				if (endKeySlice != null) {
					ro.setIterateUpperBound(endKeySlice);
				}
				if (transactionId != 0L) {
					//noinspection resource
					it = edb.getTransaction(transactionId, false, workloadProfile).val().getIterator(ro, col.cfh());
				} else {
					it = edb.db.get().newIterator(col.cfh(), ro);
				}
				state.iterator = it;
				if (reverse) {
					it.seekToLast();
				} else {
					it.seekToFirst();
				}
				checkIteratorStatusIfInvalid(it);

				itEntry = new REntry<>(it, expirationTimestamp, state, workloadProfile);
				edb.retainResourceLease();
				resourceLeaseRetained = true;
				long iteratorId = FastRandomUtils.allocateNewValue(edb.its, itEntry, 1, Long.MAX_VALUE);
				return iteratorId;
			} catch (Throwable ex) {
				try {
					if (itEntry != null) {
						itEntry.close();
					} else {
						if (it != null) {
							it.close();
						}
						state.close();
					}
				} catch (Throwable closeError) {
					ex.addSuppressed(closeError);
				} finally {
					if (resourceLeaseRetained) {
						edb.releaseResourceLease();
					}
				}
				throw ex;
			}
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.openIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private static long iteratorExpirationTimestamp(long timeoutMs) {
		if (timeoutMs < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator timeout must be non-negative");
		}
		long now = System.currentTimeMillis();
		return timeoutMs >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + timeoutMs;
	}

	private static long iteratorReadDeadlineMicros(long timeoutMs) {
		if (timeoutMs < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Read timeout must be non-negative");
		}
		long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
		long timeoutMicros = TimeUnit.MILLISECONDS.toMicros(timeoutMs);
		return timeoutMicros >= Long.MAX_VALUE - nowMicros ? Long.MAX_VALUE : nowMicros + timeoutMicros;
	}

	final class IteratorState extends RocksDBObjects {

		private final ColumnInstance column;
		final RWScheduler workloadScheduler;
		private final boolean reverse;
		private RocksIterator iterator;
		private ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private boolean closed;

		private IteratorState(ColumnInstance.ColumnUse columnUse, boolean reverse) {
			super(columnUse);
			this.column = columnUse.column();
			this.workloadScheduler = edb.getSchedulerForColumn(column.cfh().getID());
			this.reverse = reverse;
		}
	}

	private record LogicalIteratorStep(boolean present, @Nullable Buf value) {

		private static final LogicalIteratorStep END = new LogicalIteratorStep(false, null);
	}
	public void closeIterator(long iteratorId) throws RocksDBException {
		var start = System.nanoTime();
		try {
			edb.actionLogger.logAction("CloseIterator", start, null, null, null, iteratorId, null, null, null);
			closeIteratorInternal(iteratorId);
		} finally {
			var end = System.nanoTime();
			edb.closeIteratorTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	/**
	 * Atomically claims an iterator before closing it. Removing the map entry is the ownership
	 * transfer that guarantees both the native handle and its resource lease are completed
	 * exactly once when explicit close, leak cleanup, and forced shutdown race.
	 */
	boolean closeIteratorInternal(long iteratorId) {
		var entry = edb.its.remove(iteratorId);
		if (entry == null) {
			return false;
		}
		try {
			if (entry.objs() instanceof IteratorState state) {
				synchronized (state) {
					state.closed = true;
					state.bucketIterator = null;
					entry.close();
				}
			} else {
				// Compatibility with synthetic entries used by close-race tests.
				entry.close();
			}
		} finally {
			edb.releaseResourceLease();
		}
		return true;
	}
	/**
	 * Closes every iterator open on the column named {@code name}, if it exists, and returns how many it closed. A
	 * column deletion waits for the iterators of the column to end ({@code stopNewUsesAndWait}): a replicated server
	 * calls this before it applies one, so that its apply does not stop until a client closes them or their lease ends.
	 * Their owners then get "no iterator" on their next call, as after an expiry.
	 */
	public int closeIteratorsOfColumn(@NotNull String name) {
		Long columnId = edb.columnOps.getColumnIdOrNull(name);
		return columnId == null ? 0 : closeIteratorsOfColumn(columnId);
	}

	/** {@link #closeIteratorsOfColumn(String)} by column id; 0 if the column does not exist. */
	public int closeIteratorsOfColumn(long columnId) {
		ColumnInstance column;
		try {
			column = edb.getColumn(columnId);
		} catch (RocksDBException missing) {
			return 0;
		}
		var ids = new LongArrayList();
		edb.its.forEach((id, entry) -> {
			if (entry.objs() instanceof IteratorState state && state.column.cfh() == column.cfh()) {
				ids.add((long) id);
			}
		});
		int closed = 0;
		for (int i = 0; i < ids.size(); i++) {
			if (closeIteratorInternal(ids.getLong(i))) {
				closed++;
			}
		}
		return closed;
	}
	public void seekTo(long iterationId, @NotNull Keys keys) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction("SeekTo", start, null, keys, null, iterationId, null, null, null);
			if (keys == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Iterator seek keys cannot be null");
			}
			withIterator(iterationId, state -> {
				state.bucketIterator = null;
				if (keys.keys().length == 0) {
					if (state.reverse) {
						state.iterator.seekToLast();
					} else {
						state.iterator.seekToFirst();
					}
					checkIteratorStatusIfInvalid(state.iterator);
					positionBucketCursor(state, null, null);
					return null;
				}

				var target = state.column.calculateKeyBytes(keys.keys());
				if (state.reverse) {
					state.iterator.seekForPrev(target);
				} else {
					state.iterator.seek(target);
				}
				checkIteratorStatusIfInvalid(state.iterator);
				positionBucketCursor(state, keys, target);
				return null;
			});
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.seekToTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction("Subsequent", start, null, skipCount, takeCount, iterationId, null, null, requestType);
			if (requestType == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Iterator request type cannot be null");
			}
			if (skipCount < 0 || takeCount < 0) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator skip and take counts must be non-negative");
			}

			return withIterator(iterationId, state -> {
				for (long i = 0; i < skipCount; i++) {
					if (!advanceIterator(state, false).present()) {
						break;
					}
				}

				Object result = switch (requestType) {
					case RequestType.RequestNothing<?> _ -> {
						for (long i = 0; i < takeCount; i++) {
							if (!advanceIterator(state, false).present()) {
								break;
							}
						}
						yield null;
					}
					case RequestType.RequestExists<?> _ -> {
						boolean found = false;
						for (long i = 0; i < takeCount; i++) {
							if (!advanceIterator(state, false).present()) {
								break;
							}
							found = true;
						}
						yield found;
					}
					case RequestType.RequestMulti<?> _ -> {
						var values = new ArrayList<Buf>((int) Math.min(takeCount, 1_024));
						for (long i = 0; i < takeCount; i++) {
							var step = advanceIterator(state, true);
							if (!step.present()) {
								break;
							}
							values.add(Objects.requireNonNull(step.value()));
						}
						yield values;
					}
				};
				return RequestType.safeCast(result);
			});
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.subsequentTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
	public long advanceIteratorInternal(long iterationId, long maxCount) {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			if (maxCount < 0) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator advance count must be non-negative");
			}
			return withIterator(iterationId, state -> {
				long advanced = 0L;
				while (advanced < maxCount && advanceIterator(state, false).present()) {
					advanced++;
				}
				var observer = edb.iteratorAdvanceStepCompletedObserver;
				if (observer != null) {
					observer.run();
				}
				return advanced;
			});
		} finally {
			edb.ops.endOp();
			edb.subsequentTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}
	public IteratorAdvanceQuantum advanceIteratorQuantumInternal(long iterationId,
			long maxCount,
			long maximumDurationNanos,
			RWScheduler.CooperativeContext context) {
		Objects.requireNonNull(context, "context");
		if (maxCount < 0L) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator quantum count must be non-negative");
		}
		if (maximumDurationNanos <= 0L) {
			throw new IllegalArgumentException("Iterator quantum duration must be positive");
		}
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			return withIterator(iterationId, state -> {
				long advanced = 0L;
				boolean exhausted = false;
				boolean checkpoint = false;
				// Cancellation keeps the established item-quantum granularity. The continuation
				// checks again after this call; polling it for every item would add a second
				// volatile signal read to the peer-preemption hot path without improving the
				// non-preemptible native-step bound.
				if (context.terminationRequested()) {
					return new IteratorAdvanceQuantum(0L, false, false);
				}
				while (advanced < maxCount) {
					if (!advanceIterator(state, false).present()) {
						exhausted = true;
						break;
					}
					advanced++;
					if (context.preemptionRequested()
							&& System.nanoTime() - start >= maximumDurationNanos) {
						checkpoint = true;
						break;
					}
				}
				notifyIteratorQuantumCompleted();
				return new IteratorAdvanceQuantum(advanced, exhausted, checkpoint);
			});
		} finally {
			edb.ops.endOp();
			edb.subsequentTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}
	public IteratorMultiQuantum readIteratorQuantumInternal(long iterationId,
			long maxCount,
			long maximumBytes,
			long maximumDurationNanos,
			RWScheduler.CooperativeContext context) {
		Objects.requireNonNull(context, "context");
		if (maxCount < 0L) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator quantum count must be non-negative");
		}
		if (maximumBytes <= 0L || maximumDurationNanos <= 0L) {
			throw new IllegalArgumentException("Iterator quantum byte and duration limits must be positive");
		}
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			return withIterator(iterationId, state -> {
				var values = new ArrayList<Buf>((int) Math.min(maxCount, 1_024L));
				long decodedBytes = 0L;
				boolean exhausted = false;
				boolean checkpoint = false;
				// See advanceIteratorQuantumInternal: cancellation is rechecked by the logical
				// continuation at this item-quantum boundary; peer preemption remains per item.
				if (context.terminationRequested()) {
					return new IteratorMultiQuantum(values, 0L, false, false);
				}
				while (values.size() < maxCount) {
					var step = advanceIterator(state, true);
					if (!step.present()) {
						exhausted = true;
						break;
					}
					var value = Objects.requireNonNull(step.value());
					values.add(value);
					decodedBytes = EmbeddedDB.saturatingAdd(decodedBytes, value.size());
					if (context.preemptionRequested()
							&& (decodedBytes >= maximumBytes
							|| System.nanoTime() - start >= maximumDurationNanos)) {
						checkpoint = true;
						break;
					}
				}
				notifyIteratorQuantumCompleted();
				return new IteratorMultiQuantum(values, decodedBytes, exhausted, checkpoint);
			});
		} finally {
			edb.ops.endOp();
			edb.subsequentTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}

	private void notifyIteratorQuantumCompleted() {
		var observer = edb.iteratorAdvanceStepCompletedObserver;
		if (observer != null) {
			observer.run();
		}
	}

	private <T> T withIterator(long iteratorId, Function<IteratorState, T> action) {
		var entry = edb.its.get(iteratorId);
		if (entry == null || !(entry.objs() instanceof IteratorState state)) {
			throw iteratorNotFound(iteratorId);
		}
		// The embedded API historically serializes operations on the same iterator.
		// Remote adapters may reject overlapping requests before queueing them, but the
		// native ownership boundary itself must remain race-safe and blocking.
		synchronized (state) {
			if (state.closed || edb.its.get(iteratorId) != entry) {
				throw iteratorNotFound(iteratorId);
			}
			Long expirationTimestamp = entry.expirationTimestamp();
			if (expirationTimestamp != null && System.currentTimeMillis() >= expirationTimestamp) {
				var removed = edb.its.remove(iteratorId);
				if (removed == entry) {
					state.closed = true;
					state.bucketIterator = null;
					try {
						entry.close();
					} finally {
						edb.releaseResourceLease();
					}
				}
				throw iteratorNotFound(iteratorId);
			}
			return action.apply(state);
		}
	}

	private static RocksDBException iteratorNotFound(long iteratorId) {
		return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "No iterator with id " + iteratorId);
	}

	private LogicalIteratorStep advanceIterator(IteratorState state, boolean materializeValue) {
		while (true) {
			if (state.bucketIterator != null) {
				boolean hasNext = state.reverse ? state.bucketIterator.hasPrevious() : state.bucketIterator.hasNext();
				if (hasNext) {
					var entry = state.reverse ? state.bucketIterator.previous() : state.bucketIterator.next();
					boolean hasMoreInBucket = state.reverse
							? state.bucketIterator.hasPrevious()
							: state.bucketIterator.hasNext();
					if (!hasMoreInBucket) {
						state.bucketIterator = null;
						advanceNativeIterator(state);
						checkIteratorStatusIfInvalid(state.iterator);
					}
					return new LogicalIteratorStep(true, materializeValue ? entry.getValue() : null);
				}
				state.bucketIterator = null;
				advanceNativeIterator(state);
				continue;
			}

			if (!state.iterator.isValid()) {
				checkIteratorStatusIfInvalid(state.iterator);
				return LogicalIteratorStep.END;
			}

			if (state.column.hasBuckets()) {
				var bucket = new Bucket(state.column, toBuf(state.iterator.value()));
				var elements = bucket.getElements();
				state.bucketIterator = elements.listIterator(state.reverse ? elements.size() : 0);
				continue;
			}

			Buf value = null;
			if (materializeValue) {
				value = state.column.schema().hasValue() ? toBuf(state.iterator.value()) : emptyBuf();
			}
			advanceNativeIterator(state);
			checkIteratorStatusIfInvalid(state.iterator);
			return new LogicalIteratorStep(true, value);
		}
	}

	private static void advanceNativeIterator(IteratorState state) {
		if (state.reverse) {
			state.iterator.prev();
		} else {
			state.iterator.next();
		}
	}

	private void positionBucketCursor(IteratorState state, @Nullable Keys exactKeys, @Nullable byte[] target) {
		if (!state.iterator.isValid() || !state.column.hasBuckets()) {
			return;
		}
		var bucket = new Bucket(state.column, toBuf(state.iterator.value()));
		var elements = bucket.getElements();
		int cursor = state.reverse ? elements.size() : 0;
		if (exactKeys != null && target != null && Arrays.equals(state.iterator.key(), target)) {
			var variableKeys = state.column.getBucketElementKeys(exactKeys.keys());
			for (int i = 0; i < elements.size(); i++) {
				var candidate = elements.get(i).getKey();
				if (bucketKeysEqual(candidate, variableKeys)) {
					cursor = state.reverse ? i + 1 : i;
					break;
				}
			}
		}
		state.bucketIterator = elements.listIterator(cursor);
	}

	private static boolean bucketKeysEqual(Buf[] left, Buf[] right) {
		if (left.length != right.length) {
			return false;
		}
		for (int i = 0; i < left.length; i++) {
			if (!Utils.valueEquals(left[i], right[i])) {
				return false;
			}
		}
		return true;
	}

	static void checkIteratorStatusIfInvalid(RocksIterator iterator) {
		if (iterator.isValid()) {
			return;
		}
		try {
			iterator.status();
		} catch (org.rocksdb.RocksDBException exception) {
			throw EmbeddedDB.mapIteratorStatusException(exception);
		}
	}
}
