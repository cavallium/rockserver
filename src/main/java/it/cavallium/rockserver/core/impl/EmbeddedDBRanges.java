package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import io.micrometer.core.instrument.Timer;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestGetRange;
import it.cavallium.rockserver.core.common.RequestType.RequestReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream.GetRange;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import org.rocksdb.AbstractSlice;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Range operations of {@link EmbeddedDB}: reduceRange, getRange (paged, streamed, async), countRange, range
 * cursors, retained cooperative range tasks and the retained query limiter.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBRanges {

	private final EmbeddedDB edb;

	EmbeddedDBRanges(EmbeddedDB edb) {
		this.edb = edb;
	}

	long nativeReadDeadlineMicros(long contextMonotonicDeadlineNanos) {
		if (contextMonotonicDeadlineNanos == Long.MAX_VALUE) {
			return Long.MAX_VALUE;
		}
		long nowMicros = TimeUnit.MILLISECONDS.toMicros(System.currentTimeMillis());
		long remainingNanos = edb.scheduler.remainingMonotonicDeadlineNanos(contextMonotonicDeadlineNanos);
		long remainingMicros = TimeUnit.NANOSECONDS.toMicros(remainingNanos);
		long contextDeadlineMicros = remainingMicros >= Long.MAX_VALUE - nowMicros
				? Long.MAX_VALUE
				: nowMicros + remainingMicros;
		return contextDeadlineMicros;
	}

	/** Compute the one immutable deadline shared by permit waiting, admission and native reads. */
	private ReadDeadline retainedReadDeadline(RequestContext context) {
		long localBudgetNanos = TimeUnit.MILLISECONDS.toNanos(edb.maxRetainedSnapshotAgeMs);
		long localDeadlineNanos = edb.scheduler.bindTimeoutNanos(localBudgetNanos);
		long contextDeadlineNanos = edb.scheduler.resolveMonotonicDeadline(context);
		long monotonicDeadlineNanos = Math.min(localDeadlineNanos, contextDeadlineNanos);
		long deadlineMicros = nativeReadDeadlineMicros(monotonicDeadlineNanos);
		return new ReadDeadline(deadlineMicros, monotonicDeadlineNanos);
	}
	@SuppressWarnings("unchecked")
	public <T> T reduceRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestReduceRange<? super KV, T> requestType) throws RocksDBException {
		return reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive,
				reverse, requestType, Long.MAX_VALUE);
	}
	public <T> T reduceRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestReduceRange<? super KV, T> requestType,
			long monotonicDeadlineNanos) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		ColumnInstance col = null;
		try {
			edb.actionLogger.logAction("ReduceRange",
					start,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					transactionId,
					reverse,
					null,
					requestType
			);
			col = edb.beginColumnUse(columnId);

			// A bounded endpoint seek still fans out to every relevant RocksDB merge child.
			// With many SSTs, async_io lets RocksDB submit those child reads together before
			// polling them; disabling it serializes cold index/data-block misses.
			try (var ro = edb.newReadOptions(null)) {
				var asyncIoObserver = edb.reduceRangeAsyncIoObserver;
				if (asyncIoObserver != null) {
					asyncIoObserver.accept(ro.asyncIo());
				}
				long deadlineMicros = nativeReadDeadlineMicros(monotonicDeadlineNanos);
				if (deadlineMicros != Long.MAX_VALUE) ro.setDeadline(deadlineMicros);
				Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0 ? col.calculateKey(
						startKeysInclusive.keys()) : null;
				Buf calculatedEndKey =
						endKeysExclusive != null && endKeysExclusive.keys().length > 0 ? col.calculateKey(endKeysExclusive.keys())
								: null;
				try (var startKeySlice = calculatedStartKey != null ? edb.toSlice(calculatedStartKey) : null; var endKeySlice =
						calculatedEndKey != null ? edb.toSlice(calculatedEndKey) : null) {
					boolean endpointSeek = requestType instanceof RequestType.RequestGetFirstAndLast<?>;
					if (!endpointSeek) {
						if (startKeySlice != null) {
							ro.setIterateLowerBound(startKeySlice);
						}
						if (endKeySlice != null) {
							ro.setIterateUpperBound(endKeySlice);
						}
					}

					RocksIterator it;
					if (transactionId != 0L) {
						//noinspection resource
						it = edb.getTransaction(transactionId, false).val().getIterator(ro, col.cfh());
					} else {
						it = edb.db.get().newIterator(col.cfh(), ro);
					}
					try (it) {
						edb.notifyRangeIteratorOpened();
						return (T) switch (requestType) {
							case RequestEntriesCount<?> _ -> {
								long count = 0;
								it.seekToFirst();
								while (it.isValid()) {
									if (col.hasBuckets()) {
										count += Bucket.readElementCount(toBuf(it.value()));
									} else {
										count++;
									}
									it.next();
								}
								EmbeddedDBIterators.checkIteratorStatusIfInvalid(it);
								yield count;
							}
							case RequestType.RequestGetFirstAndLast<?> _ -> {
								var first = seekLogicalEndpoint(it,
										col,
										reverse,
										calculatedStartKey,
										calculatedEndKey);
								if (first == null) {
									yield new FirstAndLast<>(null, null);
								}
								var last = Objects.requireNonNull(seekLogicalEndpoint(it,
										col,
										!reverse,
										calculatedStartKey,
										calculatedEndKey));
								yield new FirstAndLast<>(first, last);
							}
						};
					}
				}
			}
		} finally {
			if (col != null) {
				col.endUse();
			}
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.reduceRangeTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

	private @Nullable KV seekLogicalEndpoint(RocksIterator iterator,
			ColumnInstance column,
			boolean fromEnd,
			@Nullable Buf startKeyInclusive,
			@Nullable Buf endKeyExclusive) {
		byte[] inclusiveStart = startKeyInclusive != null ? startKeyInclusive.toByteArray() : null;
		byte[] exclusiveEnd = endKeyExclusive != null ? endKeyExclusive.toByteArray() : null;
		if (fromEnd) {
			if (exclusiveEnd == null) {
				iterator.seekToLast();
			} else {
				iterator.seek(exclusiveEnd);
				if (iterator.isValid()) {
					iterator.prev();
				} else {
					iterator.seekToLast();
				}
			}
		} else {
			if (inclusiveStart == null) {
				iterator.seekToFirst();
			} else {
				iterator.seek(inclusiveStart);
			}
		}
		while (iterator.isValid()) {
			byte[] rawKey = iterator.key();
			if (inclusiveStart != null && Arrays.compareUnsigned(rawKey, inclusiveStart) < 0) {
				if (fromEnd) {
					return null;
				}
				iterator.next();
				continue;
			}
			if (exclusiveEnd != null && Arrays.compareUnsigned(rawKey, exclusiveEnd) >= 0) {
				if (!fromEnd) {
					return null;
				}
				iterator.prev();
				continue;
			}
			var calculatedKey = toBuf(rawKey);
			var calculatedValue = (column.schema().hasValue() || column.hasBuckets())
					? toBuf(iterator.value())
					: emptyBuf();
			if (!column.hasBuckets()) {
				return edb.decodeKV(column, calculatedKey, calculatedValue);
			}
			var elements = new Bucket(column, calculatedValue).getElements();
			if (!elements.isEmpty()) {
				var entry = fromEnd ? elements.getLast() : elements.getFirst();
				return edb.decodeBucketEntry(column, calculatedKey, entry);
			}
			if (fromEnd) {
				iterator.prev();
			} else {
				iterator.next();
			}
		}
		EmbeddedDBIterators.checkIteratorStatusIfInvalid(iterator);
		return null;
	}
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestGetRange<? super KV, T> requestType,
			@NotNull RangeBudget budget) throws RocksDBException {
		return getRangePage(transactionId, columnId, startKeysInclusive, endKeysExclusive,
				reverse, resumeAfter, requestType, budget, Long.MAX_VALUE);
	}
	public <T> RangePage<T> getRangePage(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@Nullable Keys resumeAfter,
			@NotNull RequestGetRange<? super KV, T> requestType,
			@NotNull RangeBudget budget,
			long monotonicDeadlineNanos) throws RocksDBException {
		Objects.requireNonNull(requestType, "requestType");
		Objects.requireNonNull(budget, "budget");
		if (budget.maxItems() > RangeBudget.DEFAULT_MAX_ITEMS) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Range maxItems exceeds the server maximum of " + RangeBudget.DEFAULT_MAX_ITEMS);
		}
		if (budget.maxBytes() > RangeBudget.DEFAULT_MAX_BYTES) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Range maxBytes exceeds the server maximum of " + RangeBudget.DEFAULT_MAX_BYTES);
		}

		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		long deadlineMicros = nativeReadDeadlineMicros(monotonicDeadlineNanos);
		boolean fillCache = !(requestType instanceof RequestType.RequestGetAllInRangeNoCache<?>);
		edb.actionLogger.logAction("GetRangePage",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				budget.maxItems(),
				null,
				requestType);

		RangeCursor cursor = openRangeCursor(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				deadlineMicros,
				monotonicDeadlineNanos,
				fillCache,
				false,
				totalTime,
				resumeAfter,
				true,
				null,
				true);
		try {
			var page = cursor.readPage(totalTime, budget);
			@SuppressWarnings("unchecked")
			var typedItems = (List<T>) (List<?>) page.items();
			return new RangePage<>(typedItems, page.resumeAfter(), page.hasMore());
		} finally {
			var closeStart = System.nanoTime();
			try {
				cursor.close();
			} finally {
				totalTime.add(System.nanoTime() - closeStart);
				edb.getRangeTimer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
			}
		}
	}
	public <T> Stream<T> getRange(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			@NotNull RequestType.RequestGetRange<? super KV, T> requestType) throws RocksDBException {
		return Flux
				.from(this.getRangeAsyncInternal(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						requestType
				))
				.toStream();
	}
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType) throws RocksDBException {
		return getRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				requestType,
				RequestContext.batch(),
				WorkloadProfile.BATCH);
	}
	public <T> Publisher<T> getRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestGetRange<? super KV, T> requestType,
			RequestContext context,
			@NotNull WorkloadProfile workloadProfile) throws RocksDBException {
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		var deadline = retainedReadDeadline(context);
		var workloadExecutor = edb.getSchedulerForColumn(columnId).executor(workloadProfile,
				OperationFamily.RANGE_PAGE,
				deadline.monotonicNanos());
		boolean fillCache = !(requestType instanceof RequestType.RequestGetAllInRangeNoCache<?>);
		edb.actionLogger.logAction("GetRange (begin)",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				reverse,
				null,
				requestType
		);
		if (!EmbeddedDBExistsMulti.isCooperativeRetainedProfile(workloadProfile)) {
			return Flux.usingWhen(
					openOrdinaryRetainedRangeCursorAsync(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline,
							fillCache,
							false,
							totalTime,
							workloadExecutor),
					cursor -> Flux.defer(() -> scheduleOrdinaryRangeStep(workloadExecutor,
								() -> cursor.readChunk(totalTime)))
							.publishOn(Schedulers.parallel(), 1)
							.doOnNext(chunk -> {
								var chunkObserver = edb.rangeReadChunkSizeObserver;
								if (chunkObserver != null) {
									chunkObserver.accept(chunk.items().size());
								}
								if (!chunk.exhausted()) {
									notifyRangeContinuation();
								}
							})
							.repeat()
							.takeUntil(RangeReadChunk::exhausted)
							.concatMap(chunk -> Flux.fromIterable(EmbeddedDBRanges.<T>castRangeItems(chunk.items())), 0)
							.takeUntilOther(retainedRangeDeadlineSignal(deadline)),
					cursor -> closeRangeCursor(cursor, totalTime, edb.getRangeTimer),
					(cursor, _) -> closeRangeCursor(cursor, totalTime, edb.getRangeTimer),
					cursor -> closeRangeCursor(cursor, totalTime, edb.getRangeTimer));
		}

		return edb.retainedQueryLimiter.acquire(deadline.monotonicNanos())
				.flatMapMany(permit -> Flux.<RangeReadChunk>create(sink -> {
					var state = new RetainedRangeReadTask(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline,
							fillCache,
							totalTime,
							workloadExecutor,
							permit,
							sink);
					sink.onRequest(state);
					sink.onCancel(state::cancelDelivery);
					sink.onDispose(state::cancelDelivery);
					state.start();
				}, FluxSink.OverflowStrategy.ERROR)
						.publishOn(Schedulers.parallel(), 1)
						.doOnNext(chunk -> {
							var chunkObserver = edb.rangeReadChunkSizeObserver;
							if (chunkObserver != null) {
								chunkObserver.accept(chunk.items().size());
							}
							if (!chunk.exhausted()) {
								notifyRangeContinuation();
							}
						})
						// A single decoded chunk is in flight. Its items drain before publishOn
						// requests another chunk and resumes the same parked scheduler node.
						.concatMap(chunk -> Flux.fromIterable(EmbeddedDBRanges.<T>castRangeItems(chunk.items())), 0)
						.takeUntilOther(retainedRangeDeadlineSignal(deadline)));
	}

	@SuppressWarnings("unchecked")
	private static <T> List<T> castRangeItems(List<KV> items) {
		return (List<T>) (List<?>) items;
	}
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse) {
		return countRangeAsyncInternal(transactionId,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				reverse,
				RequestContext.batch(),
				WorkloadProfile.BATCH);
	}
	public Mono<Long> countRangeAsyncInternal(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			RequestContext context,
			@NotNull WorkloadProfile workloadProfile) {
		LongAdder totalTime = new LongAdder();
		long start = System.nanoTime();
		var deadline = retainedReadDeadline(context);
		var workloadExecutor = edb.getSchedulerForColumn(columnId).executor(workloadProfile,
				OperationFamily.FULL_SCAN_AGGREGATE,
				deadline.monotonicNanos());
		edb.actionLogger.logAction("ReduceRange (begin)",
				start,
				columnId,
				startKeysInclusive,
				endKeysExclusive,
				transactionId,
				reverse,
				null,
				RequestType.entriesCount()
		);
		if (!EmbeddedDBExistsMulti.isCooperativeRetainedProfile(workloadProfile)) {
			return Flux.usingWhen(
					openOrdinaryRetainedRangeCursorAsync(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline,
							false,
							true,
							totalTime,
							workloadExecutor),
					cursor -> Flux.defer(() -> scheduleOrdinaryRangeStep(workloadExecutor, () -> {
						var chunk = cursor.countChunk(totalTime);
						var chunkObserver = edb.rangeCountChunkObserver;
						if (chunkObserver != null) {
							chunkObserver.run();
						}
						return chunk;
					}))
							.publishOn(Schedulers.parallel(), 1)
							.doOnNext(chunk -> {
								var sizeObserver = edb.rangeCountQuantumItemsObserver;
								if (sizeObserver != null) {
									sizeObserver.accept(chunk.count());
								}
								if (!chunk.exhausted()) {
									notifyRangeContinuation();
								}
							})
							.repeat()
							.takeUntil(RangeCountChunk::exhausted)
							.takeUntilOther(retainedRangeDeadlineSignal(deadline)),
					cursor -> closeRangeCursor(cursor, totalTime, edb.reduceRangeTimer),
					(cursor, _) -> closeRangeCursor(cursor, totalTime, edb.reduceRangeTimer),
					cursor -> closeRangeCursor(cursor, totalTime, edb.reduceRangeTimer))
					.reduce(0L, (total, chunk) -> total + chunk.count());
		}

		return edb.retainedQueryLimiter.acquire(deadline.monotonicNanos())
				.flatMap(permit -> Mono.create(sink -> {
					var state = new RetainedRangeCountTask(transactionId,
							columnId,
							startKeysInclusive,
							endKeysExclusive,
							reverse,
							deadline,
							totalTime,
							workloadExecutor,
							permit,
							sink);
					sink.onCancel(state::cancelDelivery);
					sink.onDispose(state::cancelDelivery);
					state.start();
				}));
	}

	/** Keep the immutable logical-read deadline active while decoded items await demand. */
	private Mono<Void> retainedRangeDeadlineSignal(ReadDeadline deadline) {
		return Mono.defer(() -> {
			long delayNanos = edb.scheduler.remainingMonotonicDeadlineNanos(deadline.monotonicNanos());
			return Mono.delay(Duration.ofNanos(delayNanos))
					.then(Mono.error(retainedRangeDeadlineExceeded()));
		});
	}

	private void notifyRangeContinuation() {
		var observer = edb.rangeContinuationObserver;
		if (observer != null) {
			observer.run();
		}
	}

	/** Preserve bounded ordinary submissions for profiles that cannot execute cooperatively. */
	private Mono<RangeCursor> openOrdinaryRetainedRangeCursorAsync(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			ReadDeadline deadline,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime,
			RWScheduler.WorkloadExecutor workloadExecutor) {
		return edb.retainedQueryLimiter.acquire(deadline.monotonicNanos())
				.flatMap(permit -> scheduleOrdinaryRangeStep(workloadExecutor,
						() -> permit.open(() -> openRangeCursor(transactionId,
								columnId,
								startKeysInclusive,
								endKeysExclusive,
								reverse,
								deadline.micros(),
								deadline.monotonicNanos(),
								fillCache,
								retainSnapshot,
								totalTime,
								null,
								false,
								permit,
								true)),
						RangeCursor::close)
						.doOnError(_ -> permit.close())
						.doOnCancel(permit::close));
	}

	private Mono<Void> closeRangeCursor(RangeCursor cursor, LongAdder totalTime, Timer timer) {
		return Mono.fromRunnable(() -> {
			var closeStart = System.nanoTime();
			try {
				cursor.close();
			} finally {
				totalTime.add(System.nanoTime() - closeStart);
				timer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
			}
		});
	}

	private <T> Mono<T> scheduleOrdinaryRangeStep(RWScheduler.WorkloadExecutor target,
			Callable<T> callable) {
		return scheduleOrdinaryRangeStep(target, callable, null);
	}

	private <T> Mono<T> scheduleOrdinaryRangeStep(RWScheduler.WorkloadExecutor target,
			Callable<T> callable,
			@Nullable Consumer<? super T> lateSuccessCleanup) {
		return Mono.create(sink -> {
			var task = new OrdinaryRetainedRangeTask<>(target, callable, lateSuccessCleanup, sink);
			sink.onCancel(task::cancelDelivery);
			sink.onDispose(task::deliveryDisposed);
			try {
				target.execute(task);
			} catch (Throwable error) {
				task.reject(error instanceof RuntimeException runtime
						? runtime
						: new RejectedExecutionException("Range quantum submission failed", error));
			}
		});
	}

	private final class OrdinaryRetainedRangeTask<T> extends CompletableFuture<T>
			implements Runnable, RWScheduler.RejectionAwareTask {

		private static final VarHandle STATE =
				EmbeddedDB.varHandle(OrdinaryRetainedRangeTask.class, "state", int.class);
		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CANCELLED = 3;

		private final RWScheduler.WorkloadExecutor target;
		private final Callable<T> callable;
		private final @Nullable Consumer<? super T> lateSuccessCleanup;
		private final MonoSink<T> sink;
		private volatile int state = QUEUED;
		private @Nullable Object pendingResult;
		private boolean deliveryCancelled;

		private OrdinaryRetainedRangeTask(RWScheduler.WorkloadExecutor target,
				Callable<T> callable,
				@Nullable Consumer<? super T> lateSuccessCleanup,
				MonoSink<T> sink) {
			this.target = target;
			this.callable = callable;
			this.lateSuccessCleanup = lateSuccessCleanup;
			this.sink = sink;
		}

		private void cancelDelivery() {
			Object cancelledResult;
			synchronized (this) {
				deliveryCancelled = true;
				cancelledResult = pendingResult;
				pendingResult = null;
			}
			cleanupCancelledResult(cancelledResult);
			if (STATE.compareAndSet(this, QUEUED, CANCELLED)) {
				super.cancel(false);
				edb.scheduler.removeQueuedTask(target, this);
			}
		}

		private void deliveryDisposed() {
			Object cancelledResult = null;
			synchronized (this) {
				if (deliveryCancelled) {
					cancelledResult = pendingResult;
				}
				pendingResult = null;
			}
			cleanupCancelledResult(cancelledResult);
		}

		@Override
		public void run() {
			if (!STATE.compareAndSet(this, QUEUED, RUNNING)) {
				return;
			}
			try {
				var result = callable.call();
				complete(result);
				publishResult(result);
			} catch (Throwable error) {
				completeExceptionally(error);
				if (!isDeliveryCancelled()) {
					sink.error(error);
				} else {
					edb.logger.debug("Range quantum failed after subscriber cancellation", error);
				}
			} finally {
				state = FINISHED;
			}
		}

		private boolean isDeliveryCancelled() {
			synchronized (this) {
				return deliveryCancelled;
			}
		}

		private void publishResult(T result) {
			Object cancelledResult = null;
			boolean emit;
			synchronized (this) {
				pendingResult = ReactorResultOwnership.encode(result);
				emit = !deliveryCancelled;
				if (!emit) {
					cancelledResult = pendingResult;
					pendingResult = null;
				}
			}
			if (emit) {
				sink.success(result);
			} else {
				cleanupCancelledResult(cancelledResult);
			}
		}

		private void cleanupCancelledResult(@Nullable Object encodedResult) {
			if (encodedResult == null || lateSuccessCleanup == null) return;
			try {
				lateSuccessCleanup.accept(ReactorResultOwnership.decode(encodedResult));
			} catch (Throwable cleanupError) {
				edb.logger.warn("Failed to clean a retained range result after cancellation", cleanupError);
			}
		}

		@Override
		public void reject(RuntimeException failure) {
			if (!STATE.compareAndSet(this, QUEUED, FINISHED)) {
				return;
			}
			completeExceptionally(failure);
			if (!isDeliveryCancelled()) {
				sink.error(failure);
			}
		}
	}

	private RangeCursor openRangeCursor(long transactionId,
			long columnId,
			@Nullable Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long deadlineMicros,
			long monotonicDeadlineNanos,
			boolean fillCache,
			boolean retainSnapshot,
			LongAdder totalTime,
			@Nullable Keys resumeAfter,
			boolean deterministicBucketOrder,
			@Nullable RetainedQueryPermit retainedPermit,
			boolean registerActiveResource) {
		edb.ops.beginOp();
		var initializationStart = System.nanoTime();
		ColumnInstance.ColumnUse columnUse = null;
		try {
			columnUse = edb.acquireColumnUse(columnId);
			var col = columnUse.column();
			Buf calculatedStartKey = startKeysInclusive != null && startKeysInclusive.keys().length > 0
					? col.calculateKey(startKeysInclusive.keys())
					: null;
			Buf calculatedEndKey = endKeysExclusive != null && endKeysExclusive.keys().length > 0
					? col.calculateKey(endKeysExclusive.keys())
					: null;
			Buf calculatedResumeKey = resumeAfter != null
					? col.calculateKey(resumeAfter.keys())
					: null;
			validateResumeBounds(calculatedStartKey, calculatedEndKey, calculatedResumeKey);
			var cursor = new RangeCursor(transactionId,
					columnUse,
					calculatedStartKey,
					calculatedEndKey,
					reverse,
					deadlineMicros,
					monotonicDeadlineNanos,
					fillCache,
					retainSnapshot,
					calculatedResumeKey,
					resumeAfter,
					deterministicBucketOrder,
					retainedPermit);
			try {
				if (registerActiveResource) {
					edb.activeRangeResources.add(cursor);
				}
				return cursor;
			} catch (Throwable error) {
				cursor.close();
				throw error;
			}
		} catch (Throwable error) {
			if (columnUse != null) {
				columnUse.close();
			}
			throw error;
		} finally {
			totalTime.add(System.nanoTime() - initializationStart);
			edb.ops.endOp();
		}
	}

	private static void validateResumeBounds(@Nullable Buf startKey,
			@Nullable Buf endKey,
			@Nullable Buf resumeKey) {
		if (resumeKey == null) {
			return;
		}
		var resumeBytes = resumeKey.toByteArray();
		if (startKey != null && Arrays.compareUnsigned(resumeBytes, startKey.toByteArray()) < 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"resumeAfter is before the original inclusive range bound");
		}
		if (endKey != null && Arrays.compareUnsigned(resumeBytes, endKey.toByteArray()) >= 0) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"resumeAfter is at or after the original exclusive range bound");
		}
	}

	abstract class RetainedRangeCooperativeTask
			implements RWScheduler.CooperativeCompletionTask,
			ActiveRangeResource,
			LongConsumer,
			Disposable {

		private static final VarHandle TERMINATED =
				EmbeddedDB.varHandle(RetainedRangeCooperativeTask.class, "terminated", boolean.class);
		private final long transactionId;
		private final long columnId;
		private final @Nullable Keys startKeysInclusive;
		private final @Nullable Keys endKeysExclusive;
		private final boolean reverse;
		private final ReadDeadline deadline;
		private final boolean fillCache;
		private final boolean retainSnapshot;
		protected final LongAdder totalTime;
		private final RWScheduler.WorkloadExecutor workloadExecutor;
		private final RetainedQueryPermit retainedPermit;
		private final Timer timer;
		private final Object admissionLock = new Object();
		private final Object resourceLock = new Object();
		private volatile boolean terminated;
		private final CountDownLatch cleanupFinished = new CountDownLatch(1);
		private volatile boolean deliveryCancelled;
		protected volatile @Nullable RWScheduler.CooperativeHandle handle;
		private @Nullable RangeCursor cursor;
		private boolean completionPrepared;
		private boolean cleanupStarted;
		private @Nullable Throwable cleanupFailure;

		private RetainedRangeCooperativeTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				ReadDeadline deadline,
				boolean fillCache,
				boolean retainSnapshot,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				Timer timer) {
			this.transactionId = transactionId;
			this.columnId = columnId;
			this.startKeysInclusive = startKeysInclusive;
			this.endKeysExclusive = endKeysExclusive;
			this.reverse = reverse;
			this.deadline = deadline;
			this.fillCache = fillCache;
			this.retainSnapshot = retainSnapshot;
			this.totalTime = totalTime;
			this.workloadExecutor = workloadExecutor;
			this.retainedPermit = retainedPermit;
			this.timer = timer;
		}

		protected final void start() {
			if (terminated) {
				return;
			}
			edb.activeRangeResources.add(this);
			synchronized (admissionLock) {
				if (terminated) {
					edb.activeRangeResources.remove(this);
					return;
				}
				try {
					handle = workloadExecutor.executeCooperatively(this,
							edb.workloadSettings.rangeQuantumMaxBytes());
				} catch (RuntimeException admissionFailure) {
					reject(admissionFailure);
				}
			}
		}

		protected final @Nullable RangeCursor cursor(RWScheduler.CooperativeContext context) {
			// The worker can dispatch immediately after admission. Waiting for this
			// short monitor handoff guarantees that the stable handle is installed first.
			synchronized (admissionLock) {
				if (terminated) {
					return null;
				}
			}
			if (context.terminationRequested()) {
				return null;
			}
			synchronized (resourceLock) {
				if (cursor != null) {
					return cursor;
				}
				RangeCursor created = retainedPermit.open(() -> openRangeCursor(transactionId,
						columnId,
						startKeysInclusive,
						endKeysExclusive,
						reverse,
						deadline.micros(),
						deadline.monotonicNanos(),
						fillCache,
						retainSnapshot,
						totalTime,
						null,
						false,
						null,
						false));
				if (terminated) {
					created.close();
					return null;
				}
				cursor = created;
				return created;
			}
		}

		protected final boolean schedulerTerminationRequested(RWScheduler.CooperativeContext context) {
			return context.terminationRequested();
		}

		protected final void cancelDelivery() {
			if (terminated) {
				return;
			}
			deliveryCancelled = true;
			if (edb.scheduler.isMonotonicDeadlineExpired(deadline.monotonicNanos())) {
				var failure = retainedRangeDeadlineExceeded();
				cancelAndFinish(failure, false, RWScheduler.TerminalOutcome.DEADLINE);
			} else {
				cancelAndFinish(null, false, null);
			}
		}

		@Override
		public final void dispose() {
			cancelDelivery();
		}

		@Override
		public final boolean isDisposed() {
			return terminated;
		}

		@Override
		public final void close() {
			var failure = new RejectedExecutionException("Database is shutting down");
			cancelAndFinish(failure, !deliveryCancelled, RWScheduler.TerminalOutcome.SHUTDOWN);
			awaitCleanupFinished();
		}

		@Override
		public final void expireIfDeadlinePassed() {
			if (!edb.scheduler.isMonotonicDeadlineExpired(deadline.monotonicNanos()) || terminated) {
				return;
			}
			var failure = retainedRangeDeadlineExceeded();
			cancelAndFinish(failure, !deliveryCancelled, RWScheduler.TerminalOutcome.DEADLINE);
		}

		@Override
		public final long cleanupDeadlineNanos() {
			return deadline.monotonicNanos();
		}

		private void cancelAndFinish(@Nullable RuntimeException failure,
				boolean signalTerminal,
				@Nullable RWScheduler.TerminalOutcome outcome) {
			// Serialize cancellation with admission. Otherwise cancellation can observe no
			// handle, release the permit, and then race a late cooperative submission.
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				var currentHandle = handle;
				if (currentHandle != null) {
					if (outcome != null && currentHandle instanceof CooperativeTerminationHandle classifiedHandle) {
						classifiedHandle.terminate(outcome, Objects.requireNonNull(failure));
					} else {
						currentHandle.dispose();
					}
					// A queued or parked task is rejected synchronously by the scheduler. An active
					// task is rejected only after its current quantum returns, which keeps cursor
					// cleanup from racing an in-flight JNI call.
					return;
				}
				finish(failure, signalTerminal);
			}
		}

		@Override
		public final void reject(RuntimeException failure) {
			// The scheduler owns first-cause arbitration for admitted tasks. A later
			// deadline, shutdown, or cancellation request must not change the public
			// failure after the scheduler has already selected a terminal outcome.
			finish(failure, !deliveryCancelled);
		}

		protected final void prepareCompletion(RWScheduler.CooperativeContext context) {
			var failure = cleanupResources(null);
			if (failure != null) {
				if (failure instanceof VirtualMachineError fatal) {
					throw fatal;
				}
				context.fail(EmbeddedDBExistsMulti.cooperativeFailure(failure,
						"Cooperative retained range cleanup failed"));
				return;
			}
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				if (completionPrepared) {
					throw new IllegalStateException("Retained range completion was prepared twice");
				}
				completionPrepared = true;
			}
		}

		@Override
		public final void completeCooperatively() {
			synchronized (admissionLock) {
				if (terminated) {
					return;
				}
				if (!completionPrepared) {
					throw new IllegalStateException("Scheduler selected RUN without a retained range result");
				}
			}
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			signalTerminal(null);
		}

		protected final void finish(@Nullable Throwable originalFailure, boolean signalTerminal) {
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			var failure = cleanupResources(originalFailure);
			if (signalTerminal) {
				signalTerminal(failure);
			}
		}

		private @Nullable Throwable cleanupResources(@Nullable Throwable originalFailure) {
			if (cleanupStarted) {
				awaitCleanupFinished();
				var existingCleanupFailure = cleanupFailure;
				return existingCleanupFailure == null
						? originalFailure
						: EmbeddedDBExistsMulti.appendFailure(originalFailure, existingCleanupFailure);
			}
			cleanupStarted = true;
			Throwable currentCleanupFailure = null;
			try {
				var cleanupObserver = edb.retainedRangeCleanupObserver;
				if (cleanupObserver != null) {
					try {
						cleanupObserver.run();
					} catch (Throwable observerFailure) {
						currentCleanupFailure = EmbeddedDBExistsMulti.appendFailure(currentCleanupFailure, observerFailure);
					}
				}
				RangeCursor localCursor;
				synchronized (resourceLock) {
					localCursor = cursor;
					cursor = null;
				}
				var closeStart = System.nanoTime();
				try {
					if (localCursor != null) {
						localCursor.close();
					}
				} catch (Throwable closeError) {
					currentCleanupFailure = EmbeddedDBExistsMulti.appendFailure(currentCleanupFailure, closeError);
				} finally {
					totalTime.add(System.nanoTime() - closeStart);
					try {
						retainedPermit.close();
					} catch (Throwable closeError) {
						edb.rangeCleanupTelemetry.closeFailed();
						currentCleanupFailure = EmbeddedDBExistsMulti.appendFailure(currentCleanupFailure, closeError);
					}
					edb.activeRangeResources.remove(this);
					try {
						timer.record(totalTime.sum(), TimeUnit.NANOSECONDS);
					} catch (Throwable timerFailure) {
						currentCleanupFailure = EmbeddedDBExistsMulti.appendFailure(currentCleanupFailure, timerFailure);
					}
				}
			} finally {
				cleanupFailure = currentCleanupFailure;
				cleanupFinished.countDown();
			}
			return currentCleanupFailure == null
					? originalFailure
					: EmbeddedDBExistsMulti.appendFailure(originalFailure, currentCleanupFailure);
		}

		private void awaitCleanupFinished() {
			boolean interrupted = false;
			while (true) {
				try {
					cleanupFinished.await();
					break;
				} catch (InterruptedException ignored) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		protected abstract void signalTerminal(@Nullable Throwable failure);

		protected final boolean isTerminated() {
			return terminated;
		}

		boolean hasOpenCursor() {
			synchronized (resourceLock) {
				return cursor != null;
			}
		}

		@Override
		public abstract void accept(long requested);
	}

	private final class RetainedRangeReadTask extends RetainedRangeCooperativeTask {

		private static final VarHandle DEMAND =
				EmbeddedDB.varHandle(RetainedRangeReadTask.class, "demand", long.class);
		private final FluxSink<RangeReadChunk> sink;
		private volatile long demand;

		private RetainedRangeReadTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				ReadDeadline deadline,
				boolean fillCache,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				FluxSink<RangeReadChunk> sink) {
			super(transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					deadline,
					fillCache,
					false,
					totalTime,
					workloadExecutor,
					retainedPermit,
					edb.getRangeTimer);
			this.sink = sink;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (isTerminated()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (schedulerTerminationRequested(context)) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (demand == 0L) {
				return RWScheduler.CooperativeResult.PARK;
			}
			try {
				var activeCursor = cursor(context);
				if (activeCursor == null) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				var chunk = activeCursor.readChunk(totalTime);
				if (isTerminated() || schedulerTerminationRequested(context)) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				producedOne();
				sink.next(chunk);
				if (chunk.exhausted()) {
					prepareCompletion(context);
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				if (schedulerTerminationRequested(context)) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				// Backpressure is stronger than scheduler competition here. If delivery has
				// not requested another decoded chunk, queueing this node for a competitive
				// redispatch only makes it run once more to discover zero demand and park.
				// A concurrent request records resume while this quantum is active, so the
				// scheduler still requeues the node atomically when PARK is committed.
				return demand == 0L
						? RWScheduler.CooperativeResult.PARK
						: RWScheduler.CooperativeResult.YIELD;
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(EmbeddedDBExistsMulti.cooperativeFailure(failure,
						"Cooperative retained range read failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private void producedOne() {
			while (true) {
				long current = demand;
				if (current == Long.MAX_VALUE) {
					return;
				}
				if (current <= 0L) {
					throw new IllegalStateException("Range chunk produced without downstream demand");
				}
				if (DEMAND.compareAndSet(this, current, current - 1L)) {
					return;
				}
			}
		}

		@Override
		public void accept(long requested) {
			if (requested <= 0L || isTerminated()) {
				return;
			}
			while (true) {
				long current = demand;
				long updated = current + requested;
				if (updated < 0L) {
					updated = Long.MAX_VALUE;
				}
				if (DEMAND.compareAndSet(this, current, updated)) {
					break;
				}
			}
			var currentHandle = handle;
			if (currentHandle != null) {
				currentHandle.resume();
			}
		}

		@Override
		protected void signalTerminal(@Nullable Throwable failure) {
			if (failure == null) {
				sink.complete();
			} else {
				sink.error(failure);
			}
		}
	}

	private final class RetainedRangeCountTask extends RetainedRangeCooperativeTask {

		private final MonoSink<Long> sink;
		private long count;

		private RetainedRangeCountTask(long transactionId,
				long columnId,
				@Nullable Keys startKeysInclusive,
				@Nullable Keys endKeysExclusive,
				boolean reverse,
				ReadDeadline deadline,
				LongAdder totalTime,
				RWScheduler.WorkloadExecutor workloadExecutor,
				RetainedQueryPermit retainedPermit,
				MonoSink<Long> sink) {
			super(transactionId,
					columnId,
					startKeysInclusive,
					endKeysExclusive,
					reverse,
					deadline,
					false,
					true,
					totalTime,
					workloadExecutor,
					retainedPermit,
					edb.reduceRangeTimer);
			this.sink = sink;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (isTerminated()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (schedulerTerminationRequested(context)) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			try {
				var activeCursor = cursor(context);
				if (activeCursor == null) {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
				while (true) {
					var chunk = activeCursor.countChunk(totalTime);
					count += chunk.count();
					var chunkObserver = edb.rangeCountChunkObserver;
					if (chunkObserver != null) {
						chunkObserver.run();
					}
					var sizeObserver = edb.rangeCountQuantumItemsObserver;
					if (sizeObserver != null) {
						sizeObserver.accept(chunk.count());
					}
					if (isTerminated() || schedulerTerminationRequested(context)) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (chunk.exhausted()) {
						prepareCompletion(context);
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (notifyRangeContinuationAndPark()) {
						return RWScheduler.CooperativeResult.PARK;
					}
					if (context.preemptionRequested()) {
						return RWScheduler.CooperativeResult.YIELD;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable failure) {
				context.fail(EmbeddedDBExistsMulti.cooperativeFailure(failure,
						"Cooperative retained range count failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private boolean notifyRangeContinuationAndPark() {
			var observer = edb.rangeContinuationObserver;
			if (observer == null) {
				return false;
			}
			try {
				Schedulers.parallel().schedule(() -> {
					try {
						observer.run();
					} finally {
						var currentHandle = handle;
						if (currentHandle != null) {
							currentHandle.resume();
						}
					}
				});
				return true;
			} catch (RuntimeException schedulingFailure) {
				throw new RejectedExecutionException("Failed to notify a retained count continuation",
						schedulingFailure);
			}
		}

		@Override
		public void accept(long requested) {
			// Mono demand is terminal-result demand, not a native-count quantum budget.
		}

		@Override
		protected void signalTerminal(@Nullable Throwable failure) {
			if (failure == null) {
				sink.success(count);
			} else {
				sink.error(failure);
			}
		}
	}

	/** FIFO admission for logical reads that retain a native point-in-time view between quanta. */
	final class RetainedQueryLimiter implements AutoCloseable {

		private static final int WAITING = 0;
		private static final int GRANTED = 1;
		private static final int TERMINAL = 2;

		private final int capacity;
		private final ScheduledExecutorService deadlineScheduler;
		private final ArrayDeque<RetainedQueryWaiter> waiters = new ArrayDeque<>();
		private final Set<RetainedQueryPermit> activePermits = new HashSet<>();
		private long nextTicket;
		private boolean closed;

		RetainedQueryLimiter(int capacity, ScheduledExecutorService deadlineScheduler) {
			this.capacity = capacity;
			this.deadlineScheduler = deadlineScheduler;
		}

		private Mono<RetainedQueryPermit> acquire(long monotonicDeadlineNanos) {
			return Mono.create(sink -> {
				final RetainedQueryWaiter waiter;
				final Runnable action;
				final boolean queued;
				synchronized (this) {
					waiter = new RetainedQueryWaiter(nextTicket++, monotonicDeadlineNanos, sink);
					sink.onCancel(waiter::cancel);
					sink.onDispose(waiter::deliveryDisposed);
					if (closed) {
						waiter.state = TERMINAL;
						action = () -> sink.error(new RejectedExecutionException("Database is shutting down"));
						queued = false;
					} else if (edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
						waiter.state = TERMINAL;
						action = () -> sink.error(retainedRangeDeadlineExceeded());
						queued = false;
					} else if (activePermits.size() < capacity && waiters.isEmpty()) {
						action = grantUnsafe(waiter);
						queued = false;
					} else {
						waiters.addLast(waiter);
						action = null;
						queued = true;
					}
				}
				if (queued) {
					waiter.scheduleExpiration();
				} else if (action != null) {
					action.run();
				}
			});
		}

		private Runnable grantUnsafe(RetainedQueryWaiter waiter) {
			var permit = new RetainedQueryPermit(this, waiter.ticket);
			waiter.state = GRANTED;
			waiter.permit = permit;
			activePermits.add(permit);
			return waiter::deliverGrant;
		}

		private void cancel(RetainedQueryWaiter waiter) {
			RetainedQueryPermit permit = null;
			synchronized (this) {
				if (waiter.state == WAITING) {
					waiters.remove(waiter);
					waiter.state = TERMINAL;
				} else if (waiter.state == GRANTED) {
					waiter.state = TERMINAL;
					permit = waiter.permit;
				}
			}
			waiter.cancelExpiration();
			if (permit != null) {
				permit.close();
			}
		}

		private void expire(RetainedQueryWaiter waiter) {
			boolean expired = false;
			synchronized (this) {
				if (waiter.state == WAITING && waiters.remove(waiter)) {
					waiter.state = TERMINAL;
					expired = true;
				}
			}
			if (expired) {
				waiter.sink.error(retainedRangeDeadlineExceeded());
			}
		}

		private void release(RetainedQueryPermit permit) {
			var actions = new ArrayList<Runnable>();
			synchronized (this) {
				if (!activePermits.remove(permit) || closed) {
					return;
				}
				while (activePermits.size() < capacity && !waiters.isEmpty()) {
					var waiter = waiters.removeFirst();
					if (waiter.state != WAITING) {
						continue;
					}
					if (edb.scheduler.isMonotonicDeadlineExpired(waiter.monotonicDeadlineNanos)) {
						waiter.state = TERMINAL;
						actions.add(() -> waiter.sink.error(retainedRangeDeadlineExceeded()));
					} else {
						actions.add(grantUnsafe(waiter));
					}
				}
			}
			if (!actions.isEmpty()) {
				try {
					deadlineScheduler.execute(() -> actions.forEach(Runnable::run));
				} catch (RejectedExecutionException shutdown) {
					actions.forEach(Runnable::run);
				}
			}
		}

		synchronized int activeCount() {
			return activePermits.size();
		}

		synchronized int waitingCount() {
			return waiters.size();
		}

		@Override
		public void close() {
			final List<RetainedQueryWaiter> pending;
			final List<RetainedQueryPermit> permits;
			synchronized (this) {
				if (closed) {
					return;
				}
				closed = true;
				pending = new ArrayList<>(waiters);
				waiters.clear();
				for (var waiter : pending) {
					waiter.state = TERMINAL;
				}
				permits = new ArrayList<>(activePermits);
			}
			for (var waiter : pending) {
				waiter.cancelExpiration();
				waiter.sink.error(new RejectedExecutionException("Database is shutting down"));
			}
			permits.forEach(RetainedQueryPermit::close);
		}

		private final class RetainedQueryWaiter {

			private final long ticket;
			private final long monotonicDeadlineNanos;
			private final MonoSink<RetainedQueryPermit> sink;
			private int state = WAITING;
			private @Nullable RetainedQueryPermit permit;
			private @Nullable ScheduledFuture<?> expiration;
			private @Nullable Object pendingResult;
			private boolean deliveryCancelled;

			private RetainedQueryWaiter(long ticket,
					long monotonicDeadlineNanos,
					MonoSink<RetainedQueryPermit> sink) {
				this.ticket = ticket;
				this.monotonicDeadlineNanos = monotonicDeadlineNanos;
				this.sink = sink;
			}

			private void scheduleExpiration() {
				long delayNanos = edb.scheduler.remainingMonotonicDeadlineNanos(monotonicDeadlineNanos);
				final ScheduledFuture<?> scheduled;
				try {
					scheduled = deadlineScheduler.schedule(this::expire, delayNanos, TimeUnit.NANOSECONDS);
				} catch (RejectedExecutionException shutdown) {
					expire();
					return;
				}
				synchronized (RetainedQueryLimiter.this) {
					if (state == WAITING) {
						expiration = scheduled;
					} else {
						scheduled.cancel(false);
					}
				}
			}

			private void deliverGrant() {
				cancelExpiration();
				var granted = Objects.requireNonNull(permit);
				try {
					var observer = edb.retainedQueryPermitGrantedObserver;
					if (observer != null) {
						observer.accept(ticket);
					}
				} catch (Throwable error) {
					granted.close();
					sink.error(error);
					return;
				}
				publishResult(granted);
			}

			private void cancel() {
				Object cancelledResult;
				synchronized (this) {
					deliveryCancelled = true;
					cancelledResult = pendingResult;
					pendingResult = null;
				}
				cleanupCancelledResult(cancelledResult);
				RetainedQueryLimiter.this.cancel(this);
			}

			private void deliveryDisposed() {
				Object cancelledResult = null;
				synchronized (this) {
					if (deliveryCancelled) {
						cancelledResult = pendingResult;
					}
					pendingResult = null;
				}
				cleanupCancelledResult(cancelledResult);
			}

			private void publishResult(RetainedQueryPermit result) {
				Object cancelledResult = null;
				boolean emit;
				synchronized (this) {
					pendingResult = ReactorResultOwnership.encode(result);
					emit = !deliveryCancelled;
					if (!emit) {
						cancelledResult = pendingResult;
						pendingResult = null;
					}
				}
				if (emit) {
					sink.success(result);
				} else {
					cleanupCancelledResult(cancelledResult);
				}
			}

			private void cleanupCancelledResult(@Nullable Object encodedResult) {
				if (encodedResult == null) return;
				try {
					ReactorResultOwnership.<RetainedQueryPermit>decode(encodedResult).close();
				} catch (Throwable cleanupFailure) {
					edb.logger.warn("Failed to release a retained range permit after cancellation", cleanupFailure);
				}
			}

			private void expire() {
				RetainedQueryLimiter.this.expire(this);
			}

			private void cancelExpiration() {
				var scheduled = expiration;
				if (scheduled != null) {
					scheduled.cancel(false);
				}
			}
		}
	}

	private static final class RetainedQueryPermit implements AutoCloseable {

		private final RetainedQueryLimiter owner;
		private final long ticket;
		private boolean closed;

		private RetainedQueryPermit(RetainedQueryLimiter owner, long ticket) {
			this.owner = owner;
			this.ticket = ticket;
		}

		private synchronized <T> T open(Supplier<T> resourceFactory) {
			if (closed) {
				throw new CancellationException("Retained range permit " + ticket + " was cancelled");
			}
			return resourceFactory.get();
		}

		@Override
		public void close() {
			boolean release;
			synchronized (this) {
				release = !closed;
				closed = true;
			}
			if (release) {
				owner.release(this);
			}
		}
	}

	private static RocksDBException retainedRangeDeadlineExceeded() {
		return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
	}

	interface ActiveRangeResource {

		void close();

		void expireIfDeadlinePassed();

		long cleanupDeadlineNanos();
	}

	private record RangeCountChunk(long count, boolean exhausted) {
	}

	private record RangeReadChunk(List<KV> items, boolean exhausted) {
	}

	private record ReadDeadline(long micros, long monotonicNanos) {
	}

	final class RangeCursor implements ActiveRangeResource {

		private final long transactionId;
		private final ColumnInstance.ColumnUse columnUse;
		private final ColumnInstance col;
		private final boolean reverse;
		private final long monotonicDeadlineNanos;
		private final boolean deterministicBucketOrder;
		private final AbstractSlice<?> startKeySlice;
		private final AbstractSlice<?> endKeySlice;
		private final @Nullable Snapshot snapshot;
		private final @Nullable RetainedQueryPermit retainedPermit;
		private final ReadOptions readOptions;
		private final RocksIterator iterator;
		private ListIterator<Entry<Buf[], Buf>> bucketIterator;
		private @Nullable KV pendingRangeItem;
		private long pendingCountItems;
		private long pendingCountBytes;
		private boolean exhausted;
		private boolean expired;
		private boolean closed;

		private RangeCursor(long transactionId,
				ColumnInstance.ColumnUse columnUse,
				@Nullable Buf startKey,
				@Nullable Buf endKey,
				boolean reverse,
				long deadlineMicros,
				long monotonicDeadlineNanos,
				boolean fillCache,
				boolean retainSnapshot,
				@Nullable Buf resumeKey,
				@Nullable Keys resumeAfter,
				boolean deterministicBucketOrder,
				@Nullable RetainedQueryPermit retainedPermit) {
			this.transactionId = transactionId;
			this.columnUse = columnUse;
			this.col = columnUse.column();
			this.reverse = reverse;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			this.deterministicBucketOrder = deterministicBucketOrder;
			this.retainedPermit = retainedPermit;

			AbstractSlice<?> createdStartKeySlice = null;
			AbstractSlice<?> createdEndKeySlice = null;
			ReadOptions createdReadOptions = null;
			Snapshot createdSnapshot = null;
			RocksIterator createdIterator = null;
			try {
				createdStartKeySlice = startKey != null ? edb.toSlice(startKey) : null;
				createdEndKeySlice = endKey != null ? edb.toSlice(endKey) : null;
				createdReadOptions = edb.newRangeReadOptions(deadlineMicros, fillCache, createdStartKeySlice,
						createdEndKeySlice);
				if (transactionId != 0L) {
					var transactionSnapshot = edb.getTransaction(transactionId, false).val().getSnapshot();
					if (transactionSnapshot != null) {
						createdReadOptions.setSnapshot(transactionSnapshot);
					}
				}
				if (retainSnapshot && transactionId == 0L) {
					createdSnapshot = edb.db.get().getSnapshot();
					edb.retainedRangeSnapshots.incrementAndGet();
					createdReadOptions.setSnapshot(createdSnapshot);
				}
				createdIterator = createIterator(createdReadOptions);
				positionIterator(createdIterator, resumeKey, resumeAfter);
			} catch (Throwable error) {
				if (createdIterator != null) {
					createdIterator.close();
				}
				if (createdReadOptions != null) {
					createdReadOptions.close();
				}
				if (createdSnapshot != null) {
					try {
						edb.db.get().releaseSnapshot(createdSnapshot);
					} finally {
						edb.retainedRangeSnapshots.decrementAndGet();
					}
				}
				if (createdEndKeySlice != null) {
					createdEndKeySlice.close();
				}
				if (createdStartKeySlice != null) {
					createdStartKeySlice.close();
				}
				throw error;
			}
			this.startKeySlice = createdStartKeySlice;
			this.endKeySlice = createdEndKeySlice;
			this.snapshot = createdSnapshot;
			this.readOptions = createdReadOptions;
			this.iterator = createdIterator;
		}

		private void positionIterator(RocksIterator createdIterator,
				@Nullable Buf resumeKey,
				@Nullable Keys resumeAfter) {
			if (resumeKey == null) {
				if (reverse) {
					createdIterator.seekToLast();
				} else {
					createdIterator.seekToFirst();
				}
				return;
			}

			byte[] resumeBytes = resumeKey.toByteArray();
			if (reverse) {
				createdIterator.seekForPrev(resumeBytes);
			} else {
				createdIterator.seek(resumeBytes);
			}
			if (!createdIterator.isValid()
					|| Arrays.compareUnsigned(createdIterator.key(), resumeBytes) != 0) {
				return;
			}
			if (!col.hasBuckets()) {
				advanceIterator(createdIterator);
				return;
			}

			var elements = orderedBucketElements(createdIterator);
			var resumeVariableKeys = col.getBucketElementKeys(Objects.requireNonNull(resumeAfter).keys());
			int insertionPoint = 0;
			while (insertionPoint < elements.size()
					&& compareKeys(elements.get(insertionPoint).getKey(), resumeVariableKeys) < 0) {
				insertionPoint++;
			}
			while (insertionPoint < elements.size()
					&& compareKeys(elements.get(insertionPoint).getKey(), resumeVariableKeys) == 0) {
				insertionPoint++;
			}
			if (reverse) {
				int beforeResume = insertionPoint;
				while (beforeResume > 0
						&& compareKeys(elements.get(beforeResume - 1).getKey(), resumeVariableKeys) >= 0) {
					beforeResume--;
				}
				if (beforeResume == 0) {
					advanceIterator(createdIterator);
				} else {
					bucketIterator = elements.listIterator(beforeResume);
				}
			} else if (insertionPoint == elements.size()) {
				advanceIterator(createdIterator);
			} else {
				bucketIterator = elements.listIterator(insertionPoint);
			}
		}

		private void advanceIterator(RocksIterator target) {
			if (reverse) {
				target.prev();
			} else {
				target.next();
			}
		}

		private ArrayList<Entry<Buf[], Buf>> orderedBucketElements(RocksIterator target) {
			var elements = new ArrayList<>(new Bucket(col, toBuf(target.value())).getElements());
			if (deterministicBucketOrder) {
				elements.sort((left, right) -> compareKeys(left.getKey(), right.getKey()));
			}
			return elements;
		}

		private int compareKeys(Buf[] left, Buf[] right) {
			for (int i = 0; i < left.length; i++) {
				int compared = Arrays.compareUnsigned(left[i].toByteArray(), right[i].toByteArray());
				if (compared != 0) {
					return compared;
				}
			}
			return Integer.compare(left.length, right.length);
		}

		private RocksIterator createIterator(ReadOptions options) {
			var created = transactionId != 0L
					? edb.getTransaction(transactionId, false).val().getIterator(options, col.cfh())
					: edb.db.get().newIterator(col.cfh(), options);
			try {
				edb.notifyRangeIteratorOpened();
				return created;
			} catch (Throwable error) {
				created.close();
				throw error;
			}
		}

		private RangePage<KV> readPage(LongAdder totalTime, RangeBudget budget) {
			edb.ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return RangePage.empty();
					}

					var results = new ArrayList<KV>(budget.maxItems());
					long encodedBytes = 0L;
					boolean hasMore = false;
					while (results.size() < budget.maxItems()) {
						var next = nextPageItem();
						if (next == null) {
							break;
						}
						long nextBytes = EmbeddedDB.decodedKVBytes(next);
						if (nextBytes > budget.maxBytes() - encodedBytes) {
							if (results.isEmpty()) {
								throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
										"Range item requires " + nextBytes
												+ " encoded key/value bytes, exceeding page budget "
												+ budget.maxBytes());
							}
							hasMore = true;
							break;
						}
						results.add(next);
						encodedBytes += nextBytes;
					}
					if (!hasMore && results.size() == budget.maxItems()) {
						hasMore = nextPageItem() != null;
					}
					if (results.isEmpty()) {
						return RangePage.empty();
					}
					return new RangePage<>(results, results.getLast().keys(), hasMore);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				edb.ops.endOp();
			}
		}

		private @Nullable KV nextPageItem() {
			while (!exhausted) {
				if (edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
					throw rangeDeadlineExceeded();
				}
				if (bucketIterator != null) {
					if (reverse ? bucketIterator.hasPrevious() : bucketIterator.hasNext()) {
						var entry = reverse ? bucketIterator.previous() : bucketIterator.next();
						var result = edb.decodeBucketEntry(col, toBuf(iterator.key()), entry);
						boolean bucketHasMore = reverse
								? bucketIterator.hasPrevious()
								: bucketIterator.hasNext();
						if (!bucketHasMore) {
							bucketIterator = null;
							advanceIterator();
						}
						return Objects.requireNonNull(result);
					}
					bucketIterator = null;
					advanceIterator();
					continue;
				}

				if (!iterator.isValid()) {
					EmbeddedDBIterators.checkIteratorStatusIfInvalid(iterator);
					exhausted = true;
					return null;
				}
				if (col.hasBuckets()) {
					var elements = orderedBucketElements(iterator);
					bucketIterator = elements.listIterator(reverse ? elements.size() : 0);
					if (elements.isEmpty()) {
						bucketIterator = null;
						advanceIterator();
					}
					continue;
				}

				var calculatedKey = toBuf(iterator.key());
				var calculatedValue = col.schema().hasValue() ? toBuf(iterator.value()) : emptyBuf();
				var result = Objects.requireNonNull(edb.decodeKV(col, calculatedKey, calculatedValue));
				advanceIterator();
				return result;
			}
			return null;
		}

		private RangeReadChunk readChunk(LongAdder totalTime) {
			edb.ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return new RangeReadChunk(List.of(), true);
					}
					int maximumItems = edb.workloadSettings.rangeQuantumMaxItems();
					long maximumBytes = edb.workloadSettings.rangeQuantumMaxBytes();
					long maximumDurationNanos = edb.workloadSettings.rangeQuantumMaxDuration().toNanos();
					var results = new ArrayList<KV>(Math.min(maximumItems, 1_024));
					long decodedBytes = 0L;
					while (results.size() < maximumItems && !exhausted) {
						if (!results.isEmpty() && System.nanoTime() - sliceStart >= maximumDurationNanos) {
							break;
						}
						var next = pendingRangeItem;
						if (next != null) {
							pendingRangeItem = null;
						} else {
							next = nextPageItem();
							if (next == null) {
								break;
							}
						}
						long nextBytes = EmbeddedDB.decodedKVBytes(next);
						if (nextBytes > maximumBytes) {
							throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
									"Range item requires " + nextBytes
											+ " encoded key/value bytes, exceeding quantum maximum "
											+ maximumBytes);
						}
						if (!results.isEmpty() && nextBytes > maximumBytes - decodedBytes) {
							pendingRangeItem = next;
							break;
						}
						results.add(next);
						decodedBytes += nextBytes;
					}
					return new RangeReadChunk(results, exhausted && pendingRangeItem == null);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				edb.ops.endOp();
			}
		}

		private RangeCountChunk countChunk(LongAdder totalTime) {
			edb.ops.beginOp();
			var sliceStart = System.nanoTime();
			try {
				synchronized (this) {
					if (expired) {
						throw rangeDeadlineExceeded();
					}
					if (closed || exhausted) {
						return new RangeCountChunk(0L, true);
					}
					long maximumItems = edb.workloadSettings.rangeQuantumMaxItems();
					long maximumBytes = edb.workloadSettings.rangeQuantumMaxBytes();
					long maximumDurationNanos = edb.workloadSettings.rangeQuantumMaxDuration().toNanos();
					long count = 0L;
					long scannedBytes = 0L;
					while (count < maximumItems) {
						if (edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
							throw rangeDeadlineExceeded();
						}
						if (count != 0L && System.nanoTime() - sliceStart >= maximumDurationNanos) {
							break;
						}
						if (pendingCountItems == 0L) {
							if (!iterator.isValid()) {
								EmbeddedDBIterators.checkIteratorStatusIfInvalid(iterator);
								exhausted = true;
								break;
							}
							pendingCountBytes = EmbeddedDB.saturatingAdd(iterator.key().length, iterator.value().length);
							pendingCountItems = col.hasBuckets()
									? Bucket.readElementCount(toBuf(iterator.value()))
									: 1L;
							advanceIterator();
							if (pendingCountItems == 0L) {
								pendingCountBytes = 0L;
								continue;
							}
						}
						if (pendingCountBytes > maximumBytes) {
							throw RocksDBException.of(RocksDBErrorType.RANGE_ITEM_TOO_LARGE,
									"Count input requires " + pendingCountBytes
											+ " encoded bytes, exceeding quantum maximum " + maximumBytes);
						}
						if (count != 0L && pendingCountBytes > maximumBytes - scannedBytes) {
							break;
						}
						scannedBytes += pendingCountBytes;
						pendingCountBytes = 0L;
						long taken = Math.min(maximumItems - count, pendingCountItems);
						count += taken;
						pendingCountItems -= taken;
					}
					if (pendingCountItems == 0L && !iterator.isValid()) {
						EmbeddedDBIterators.checkIteratorStatusIfInvalid(iterator);
						exhausted = true;
					}
					return new RangeCountChunk(count, exhausted && pendingCountItems == 0L);
				}
			} finally {
				totalTime.add(System.nanoTime() - sliceStart);
				edb.ops.endOp();
			}
		}

		private void advanceIterator() {
			if (reverse) {
				iterator.prev();
			} else {
				iterator.next();
			}
		}

		@Override
		public void expireIfDeadlinePassed() {
			synchronized (this) {
				if (closed || !edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
					return;
				}
				expired = true;
			}
			close();
		}

		@Override
		public long cleanupDeadlineNanos() {
			return monotonicDeadlineNanos;
		}

		private RocksDBException rangeDeadlineExceeded() {
			return RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
		}

		@Override
		public void close() {
			try {
				closeOwnedResources();
			} catch (Throwable closeError) {
				edb.rangeCleanupTelemetry.closeFailed();
				throw closeError;
			}
		}

		private void closeOwnedResources() {
			try {
				synchronized (this) {
					if (closed) {
						return;
					}
					closed = true;
					exhausted = true;
					bucketIterator = null;
					try {
						iterator.close();
					} finally {
						try {
							readOptions.close();
						} finally {
							try {
								if (snapshot != null) {
									try {
										edb.db.get().releaseSnapshot(snapshot);
									} finally {
										edb.retainedRangeSnapshots.decrementAndGet();
									}
								}
							} finally {
								try {
									if (endKeySlice != null) {
										endKeySlice.close();
									}
								} finally {
									try {
										if (startKeySlice != null) {
											startKeySlice.close();
										}
									} finally {
										columnUse.close();
									}
								}
							}
						}
					}
				}
			} finally {
				try {
					edb.activeRangeResources.remove(this);
				} finally {
					if (retainedPermit != null) {
						retainedPermit.close();
					}
				}
			}
		}
	}

	static void ensureCapacity(Buf buf, int offset, int len) {
		int required = offset + len;
		if (buf.size() < required) {
			int newSize = buf.size() + (buf.size() >> 1);
			if (newSize < required) {
				newSize = required;
			}
			buf.size(newSize);
		}
	}
}
