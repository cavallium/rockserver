package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.VarHandle;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ReadOptions;
import org.rocksdb.Snapshot;
import reactor.core.scheduler.Scheduler;

/**
 * existsMulti of {@link EmbeddedDB}: the synchronous path and the cooperative retained asynchronous request.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBExistsMulti {

	private final EmbeddedDB edb;

	EmbeddedDBExistsMulti(EmbeddedDB edb) {
		this.edb = edb;
	}

	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys) throws RocksDBException {
		return existsMulti(transactionId,
				columnId,
				keys,
				WorkloadProfile.BATCH,
				Long.MAX_VALUE);
	}
	public List<Boolean> existsMulti(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			WorkloadProfile workloadProfile,
			long contextMonotonicDeadlineNanos) throws RocksDBException {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			if (keys == null) {
				throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys");
			}
			edb.actionLogger.logAction("ExistsMulti",
					start,
					columnId,
					keys.size(),
					null,
					transactionId,
					null,
					null,
					null
			);

			var deadlineMicros = edb.ranges.nativeReadDeadlineMicros(contextMonotonicDeadlineNanos);
			var columnUse = edb.acquireColumnUse(columnId);
			try (columnUse) {
				Tx tx = transactionId != 0
						? edb.getTransaction(transactionId, false, workloadProfile)
						: null;
				try (var cursor = new ExistsMultiCursor(columnUse, tx, keys, deadlineMicros, workloadProfile == WorkloadProfile.LATENCY)) {
				while (!cursor.readChunk()) {
					// The synchronous API intentionally keeps processing on its caller thread.
				}
				return cursor.result();
				}
			}
		} catch (RocksDBException exception) {
			throw exception;
		} catch (Exception exception) {
			throw RocksDBException.of(RocksDBErrorType.GET_1, exception);
		} finally {
			edb.ops.endOp();
			edb.existsMultiTimer.record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
		}
	}
	public CompletableFuture<List<Boolean>> existsMultiAsyncInternal(long transactionId,
			long columnId,
			@NotNull List<@NotNull Keys> keys,
			WorkloadProfile workloadProfile,
			Executor executor,
			long contextMonotonicDeadlineNanos) {
		long requestStartNanos = System.nanoTime();
		if (keys == null) {
			return CompletableFuture.failedFuture(
					RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys"));
		}
		final long deadlineMicros;
		try {
			// Establish the request deadline before validation/copying so admission work
			// for a very large logical request cannot grant the native reads fresh time.
			deadlineMicros = edb.ranges.nativeReadDeadlineMicros(contextMonotonicDeadlineNanos);
		} catch (Throwable error) {
			return CompletableFuture.failedFuture(error);
		}
		// Yielding between native chunks lengthens the interval in which a caller could
		// mutate its list. Retain a stable logical request just as we retain its snapshot.
		final List<Keys> requestKeys;
		try {
			requestKeys = List.copyOf(keys);
		} catch (NullPointerException nullKey) {
			return CompletableFuture.failedFuture(
					RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys contains null"));
		} catch (Throwable error) {
			return CompletableFuture.failedFuture(error);
		}
		var request = new AsyncExistsMultiRequest(
				transactionId,
				columnId,
				requestKeys,
				0L,
				requestStartNanos,
				deadlineMicros,
				contextMonotonicDeadlineNanos,
				workloadProfile,
				executor,
				isCooperativeRetainedProfile(workloadProfile)
						&& !isExistsMultiProvablySingleChunk(requestKeys)
						&& executor instanceof RWScheduler.WorkloadExecutor workloadExecutor
						? workloadExecutor
						: null);
		request.scheduleInitial();
		return request;
	}

	/**
	 * A column-independent upper bound is enough to preserve the ordinary fast path:
	 * fixed key parts retain their input length and variable-key hashes have a known
	 * bounded output. A request below both native limits therefore cannot split.
	 */
	private static boolean isExistsMultiProvablySingleChunk(List<Keys> keys) {
		if (keys.size() > EmbeddedDB.EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL) {
			return false;
		}
		long maximumCalculatedBytes = 0L;
		for (var logicalKeys : keys) {
			for (var key : logicalKeys.keys()) {
				if (key == null) {
					return false;
				}
				maximumCalculatedBytes = EmbeddedDB.saturatingAdd(maximumCalculatedBytes,
						Math.max(EmbeddedDB.EXISTS_MULTI_MAX_VARIABLE_HASH_BYTES, key.size()));
				if (maximumCalculatedBytes > EmbeddedDB.EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL) {
					return false;
				}
			}
		}
		return true;
	}

	static boolean isCooperativeRetainedProfile(WorkloadProfile profile) {
		return switch (profile) {
			case ANALYTICAL, INGEST, BATCH -> true;
			case LATENCY, CONTROL, PHYSICAL_MAINTENANCE -> false;
		};
	}

	final class AsyncExistsMultiRequest extends CompletableFuture<List<Boolean>>
			implements Runnable, RWScheduler.CooperativeCompletionTask {

		private static final VarHandle RESOURCES_CLEANED =
				EmbeddedDB.varHandle(AsyncExistsMultiRequest.class, "resourcesCleaned", boolean.class);
		private static final int INITIAL_QUEUED = 0;
		private static final int CHUNK_RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CHUNK_QUEUED = 3;

		private final long transactionId;
		private final long columnId;
		private final List<Keys> keys;
		private final long timeoutMs;
		private final long requestStartNanos;
		private final long deadlineMicros;
		private final long monotonicDeadlineNanos;
		private final WorkloadProfile workloadProfile;
		private final Executor executor;
		private final @Nullable RWScheduler.WorkloadExecutor cooperativeExecutor;
		private final Object lifecycleLock = new Object();
		private volatile boolean cancelRequested;
		private volatile boolean resourcesCleaned;
		private int state = INITIAL_QUEUED;
		private @Nullable ExistsMultiCursor cursor;
		private volatile @Nullable RWScheduler.CooperativeHandle cooperativeHandle;
		private boolean operationStarted;
		private boolean cooperativeCompletionPrepared;
		private @Nullable List<Boolean> cooperativeResult;
		private @Nullable Throwable resourceCleanupFailure;

		private AsyncExistsMultiRequest(long transactionId,
				long columnId,
				List<Keys> keys,
				long timeoutMs,
				long requestStartNanos,
				long deadlineMicros,
				long monotonicDeadlineNanos,
				WorkloadProfile workloadProfile,
				Executor executor,
				@Nullable RWScheduler.WorkloadExecutor cooperativeExecutor) {
			this.transactionId = transactionId;
			this.columnId = columnId;
			this.keys = keys;
			this.timeoutMs = timeoutMs;
			this.requestStartNanos = requestStartNanos;
			this.deadlineMicros = deadlineMicros;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			this.workloadProfile = Objects.requireNonNull(workloadProfile, "workloadProfile");
			this.executor = Objects.requireNonNull(executor, "executor");
			this.cooperativeExecutor = cooperativeExecutor;
		}

		private void scheduleInitial() {
			Throwable schedulingFailure = null;
			synchronized (edb.asyncExistsMultiAdmissionLock) {
				synchronized (lifecycleLock) {
					if (state != INITIAL_QUEUED) {
						return;
					}
					try {
						// Count queue residence as part of the operation. Besides making the
						// request deadline honest, this makes admission linearizable with close().
						edb.ops.beginOp();
						operationStarted = true;
						edb.activeExistsMultiRequests.add(this);
						if (cooperativeExecutor == null) {
							executor.execute(this);
						} else {
							cooperativeHandle = cooperativeExecutor.executeCooperatively(this,
									EmbeddedDB.EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL);
						}
					} catch (Throwable error) {
						state = FINISHED;
						edb.activeExistsMultiRequests.remove(this);
						schedulingFailure = error;
					}
				}
			}
			if (schedulingFailure != null) {
				completeTerminal(null, schedulingFailure);
			}
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (cooperativeExecutor != null) {
				synchronized (lifecycleLock) {
					if (state == FINISHED) {
						return false;
					}
					boolean beforeFirstRun = state == INITIAL_QUEUED;
					var handle = Objects.requireNonNull(cooperativeHandle, "cooperativeHandle");
					// Let the scheduler's terminal CAS choose between cancellation,
					// deadline, shutdown, and a concurrently completed RUN before the
					// local flag is published for idempotency.
					boolean cancellationWon = handle.cancel();
					if (cancellationWon) {
						cancelRequested = true;
					}
					return beforeFirstRun && cancellationWon;
				}
			}
			boolean cleanupQueuedRequest = false;
			boolean initialQueuedCancellation = false;
			synchronized (lifecycleLock) {
				switch (state) {
					case INITIAL_QUEUED -> {
						state = FINISHED;
						removeQueuedTask();
						cleanupQueuedRequest = true;
						initialQueuedCancellation = true;
					}
					case CHUNK_RUNNING -> {
						cancelRequested = true;
						return false;
					}
					case CHUNK_QUEUED -> {
						cancelRequested = true;
						state = FINISHED;
						removeQueuedTask();
						cleanupQueuedRequest = true;
					}
					default -> {
						return false;
					}
				}
			}
			if (cleanupQueuedRequest) {
				completeTerminal(null, new java.util.concurrent.CancellationException());
			}
			// Once a native chunk has run, keep the original future observable until
			// cleanup finishes rather than reporting that cancellation won immediately.
			return initialQueuedCancellation;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			final boolean initialize;
			synchronized (lifecycleLock) {
				if (state == INITIAL_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = true;
				} else if (state == CHUNK_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = false;
				} else {
					return RWScheduler.CooperativeResult.COMPLETE;
				}
			}
			if (context.terminationRequested()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (initialize) {
				try {
					initialize();
				} catch (VirtualMachineError fatal) {
					throw fatal;
				} catch (Throwable error) {
					context.fail(cooperativeFailure(mapExistsMultiFailure(error),
							"Cooperative existsMulti initialization failed"));
					return RWScheduler.CooperativeResult.COMPLETE;
				}
			}

			try {
				var activeCursor = Objects.requireNonNull(cursor);
				while (true) {
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					boolean exhausted = activeCursor.readChunk();
					if (context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (exhausted) {
						prepareCooperativeCompletion(context, activeCursor.result());
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (context.preemptionRequested()) {
						synchronized (lifecycleLock) {
							if (state == CHUNK_RUNNING) {
								state = CHUNK_QUEUED;
							}
						}
						return RWScheduler.CooperativeResult.YIELD;
					}
				}
			} catch (VirtualMachineError fatal) {
				throw fatal;
			} catch (Throwable error) {
				context.fail(cooperativeFailure(mapExistsMultiFailure(error),
						"Cooperative existsMulti native read failed"));
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private void prepareCooperativeCompletion(RWScheduler.CooperativeContext context,
				List<Boolean> result) {
			var cleanupFailure = cleanupTerminalResources(null);
			if (cleanupFailure != null) {
				if (cleanupFailure instanceof VirtualMachineError fatal) {
					throw fatal;
				}
				context.fail(cooperativeFailure(cleanupFailure,
						"Cooperative existsMulti cleanup failed"));
				return;
			}
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				if (cooperativeCompletionPrepared) {
					throw new IllegalStateException("existsMulti completion was prepared twice");
				}
				cooperativeResult = result;
				cooperativeCompletionPrepared = true;
			}
		}

		@Override
		public void completeCooperatively() {
			final List<Boolean> result;
			synchronized (lifecycleLock) {
				if (state == FINISHED) {
					return;
				}
				if (state != CHUNK_RUNNING || !cooperativeCompletionPrepared) {
					throw new IllegalStateException("Scheduler selected RUN without an existsMulti result");
				}
				state = FINISHED;
				result = Objects.requireNonNull(cooperativeResult,
						"Prepared existsMulti result");
			}
			complete(result);
		}

		@Override
		public void run() {
			boolean initialize;
			synchronized (lifecycleLock) {
				if (state == INITIAL_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = true;
				} else if (state == CHUNK_QUEUED) {
					state = CHUNK_RUNNING;
					initialize = false;
				} else {
					return;
				}
			}
			if (initialize && cancelRequested) {
				finishRunning(null, new java.util.concurrent.CancellationException());
				return;
			}
			if (initialize) {
				try {
					initialize();
				} catch (Throwable error) {
					finishRunning(null, mapExistsMultiFailure(error));
					return;
				}
			}
			if (cancelRequested) {
				finishRunning(null, new java.util.concurrent.CancellationException());
				return;
			}

			try {
				var activeCursor = Objects.requireNonNull(cursor);
				boolean exhausted = activeCursor.readChunk();
				if (cancelRequested) {
					finishRunning(null, new java.util.concurrent.CancellationException());
				} else if (exhausted) {
					finishRunning(activeCursor.result(), null);
				} else {
					scheduleNext();
				}
			} catch (Throwable error) {
				finishRunning(null, mapExistsMultiFailure(error));
			}
		}

		private void initialize() {
			edb.actionLogger.logAction("ExistsMulti",
					requestStartNanos,
					columnId,
					keys.size(),
					null,
					transactionId,
					null,
					timeoutMs,
					null);
			if (monotonicDeadlineNanos != Long.MAX_VALUE
					&& edb.scheduler.isMonotonicDeadlineExpired(monotonicDeadlineNanos)) {
				throw RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Deadline exceeded");
			}
			var columnUse = edb.acquireColumnUse(columnId);
			try {
				Tx tx = transactionId != 0
						? edb.getTransaction(transactionId, false, workloadProfile)
						: null;
				cursor = new ExistsMultiCursor(columnUse, tx, keys, deadlineMicros, workloadProfile == WorkloadProfile.LATENCY);
			} catch (Throwable error) {
				columnUse.close();
				throw error;
			}
		}

		private void scheduleNext() {
			Throwable schedulingFailure = null;
			boolean cancelled = false;
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				if (cancelRequested) {
					state = FINISHED;
					cancelled = true;
				} else {
					state = CHUNK_QUEUED;
					try {
						executor.execute(this);
					} catch (Throwable error) {
						state = FINISHED;
						schedulingFailure = error;
					}
				}
			}
			if (cancelled) {
				completeTerminal(null, new java.util.concurrent.CancellationException());
			} else if (schedulingFailure != null) {
				completeTerminal(null, schedulingFailure);
			}
		}

		private void finishRunning(@Nullable List<Boolean> result, @Nullable Throwable failure) {
			synchronized (lifecycleLock) {
				if (state != CHUNK_RUNNING) {
					return;
				}
				state = FINISHED;
			}
			completeTerminal(result, failure);
		}

		private void completeTerminal(@Nullable List<Boolean> result, @Nullable Throwable failure) {
			var terminalFailure = cleanupTerminalResources(failure);
			if (terminalFailure != null) {
				completeExceptionally(terminalFailure);
			} else {
				complete(Objects.requireNonNull(result));
			}
		}

		private @Nullable Throwable cleanupTerminalResources(@Nullable Throwable originalFailure) {
			if (!RESOURCES_CLEANED.compareAndSet(this, false, true)) {
				var existingCleanupFailure = resourceCleanupFailure;
				return existingCleanupFailure == null
						? originalFailure
						: appendFailure(originalFailure, existingCleanupFailure);
			}
			Throwable cleanupFailure = null;
			if (cursor != null) {
				try {
					cursor.close();
				} catch (Throwable closeFailure) {
					cleanupFailure = appendFailure(cleanupFailure, closeFailure);
				}
			}
			if (operationStarted) {
				try {
					edb.existsMultiTimer.record(System.nanoTime() - requestStartNanos, TimeUnit.NANOSECONDS);
				} catch (Throwable timerFailure) {
					cleanupFailure = appendFailure(cleanupFailure, timerFailure);
				}
				edb.activeExistsMultiRequests.remove(this);
				try {
					// End last: close() may release meters and native handles as soon as
					// this request is the final SafeShutdown operation. The request must
					// no longer be visible as active before publishing that final release.
					edb.ops.endOp();
				} catch (Throwable endFailure) {
					cleanupFailure = appendFailure(cleanupFailure, endFailure);
				}
			} else {
				edb.activeExistsMultiRequests.remove(this);
			}
			resourceCleanupFailure = cleanupFailure;
			return cleanupFailure == null
					? originalFailure
					: appendFailure(originalFailure, cleanupFailure);
		}

		private void removeQueuedTask() {
			edb.scheduler.removeQueuedTask(executor, this);
		}

		@Override
		public void reject(RuntimeException failure) {
			synchronized (lifecycleLock) {
				if (state == FINISHED) {
					return;
				}
				state = FINISHED;
			}
			completeTerminal(null, failure);
		}
	}

	private final class ExistsMultiCursor implements AutoCloseable {

		private final ColumnInstance.ColumnUse columnUse;
		private final ColumnInstance col;
		private final @Nullable Tx tx;
		private final List<Keys> keys;
		private final ArrayList<Boolean> result;
		private final @Nullable Snapshot snapshot;
		private final @Nullable ReadOptions readOptions;
		private @Nullable ExistsMultiChunk chunk;
		private boolean exhausted;
		private boolean closed;
		private final boolean sampleLatency;

		private ExistsMultiCursor(ColumnInstance.ColumnUse columnUse,
				@Nullable Tx tx,
				List<Keys> keys,
				long deadlineMicros, boolean sampleLatency) {
			this.sampleLatency = sampleLatency;
			this.columnUse = columnUse;
			this.col = columnUse.column();
			this.tx = tx;
			this.keys = keys;
			this.result = new ArrayList<>(keys.size());
			for (var logicalKeys : keys) {
				if (logicalKeys == null) {
					throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "keys contains null");
				}
			}
			if (keys.isEmpty()) {
				this.snapshot = null;
				this.readOptions = null;
				this.exhausted = true;
				return;
			}

			this.chunk = calculateExistsMultiChunk(col, keys, 0);
			Snapshot createdSnapshot = null;
			ReadOptions createdReadOptions = null;
			try {
				if (chunk.nextOffset() < keys.size()) {
					createdSnapshot = edb.db.get().getSnapshot();
					var snapshotObserver = edb.existsMultiSnapshotObserver;
					if (snapshotObserver != null) {
						snapshotObserver.run();
					}
				}
				createdReadOptions = edb.newReadOptions("exists-multi-read-options");
				createdReadOptions.setDeadline(deadlineMicros);
				// Bounded point lookups should warm SST metadata; data admission also uses the existing bounded cache.
				createdReadOptions.setFillCache(true);
				if (createdSnapshot != null) {
					createdReadOptions.setSnapshot(createdSnapshot);
				}
			} catch (Throwable error) {
				if (createdReadOptions != null) {
					try {
						createdReadOptions.close();
					} catch (Throwable closeFailure) {
						error.addSuppressed(closeFailure);
					}
				}
				if (createdSnapshot != null) {
					try {
						edb.db.get().releaseSnapshot(createdSnapshot);
					} catch (Throwable closeFailure) {
						error.addSuppressed(closeFailure);
					}
				}
				throw error;
			}
			this.snapshot = createdSnapshot;
			this.readOptions = createdReadOptions;
		}

		private boolean readChunk() {
			if (exhausted) {
				return true;
			}
			var currentChunk = Objects.requireNonNull(chunk);
			try {
				if (tx == null && !col.hasBuckets()) {
					result.addAll(existsMultiStatusOnly(col,
							Objects.requireNonNull(readOptions),
							currentChunk.calculatedKeys(), sampleLatency));
				} else {
					result.addAll(existsMultiWithValues(tx,
							col,
							Objects.requireNonNull(readOptions),
							currentChunk.logicalKeys(),
							currentChunk.calculatedKeys()));
				}
			} catch (org.rocksdb.RocksDBException exception) {
				throw EmbeddedDB.mapIteratorStatusException(exception);
			}
			var chunkObserver = edb.existsMultiChunkObserver;
			if (chunkObserver != null) {
				chunkObserver.run();
			}
			if (currentChunk.nextOffset() >= keys.size()) {
				exhausted = true;
				return true;
			}
			chunk = calculateExistsMultiChunk(col, keys, currentChunk.nextOffset());
			return false;
		}

		private List<Boolean> result() {
			if (!exhausted) {
				throw new IllegalStateException("ExistsMulti result requested before exhaustion");
			}
			return result;
		}

		@Override
		public void close() {
			if (closed) {
				return;
			}
			closed = true;
			Throwable failure = null;
			if (readOptions != null) {
				try {
					readOptions.close();
				} catch (Throwable error) {
					failure = error;
				}
			}
			if (snapshot != null) {
				try {
					edb.db.get().releaseSnapshot(snapshot);
				} catch (Throwable error) {
					failure = appendFailure(failure, error);
				}
			}
			try {
				columnUse.close();
			} catch (Throwable error) {
				failure = appendFailure(failure, error);
			}
			if (failure instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (failure instanceof Error error) {
				throw error;
			}
		}
	}

	private static Throwable mapExistsMultiFailure(Throwable error) {
		if (error instanceof RocksDBException) {
			return error;
		}
		return error instanceof Exception
				? RocksDBException.of(RocksDBErrorType.GET_1, error)
				: error;
	}

	static RuntimeException cooperativeFailure(Throwable failure, String message) {
		if (failure instanceof RuntimeException runtimeException) {
			return runtimeException;
		}
		if (failure instanceof org.rocksdb.RocksDBException rocksDBException) {
			return EmbeddedDB.mapIteratorStatusException(rocksDBException);
		}
		return new RejectedExecutionException(message, failure);
	}

	static Throwable appendFailure(@Nullable Throwable current, Throwable next) {
		if (current == null) {
			return next;
		}
		if (current != next) {
			current.addSuppressed(next);
		}
		return current;
	}

	private record ExistsMultiChunk(List<Keys> logicalKeys, List<Buf> calculatedKeys, int nextOffset) {
	}

	private ExistsMultiChunk calculateExistsMultiChunk(ColumnInstance col, List<Keys> keys, int offset) {
		int chunkCapacity = Math.min(EmbeddedDB.EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL, keys.size() - offset);
		var logicalChunk = new ArrayList<Keys>(chunkCapacity);
		var calculatedChunk = new ArrayList<Buf>(chunkCapacity);
		long calculatedKeyBytes = 0L;
		while (offset < keys.size() && calculatedChunk.size() < EmbeddedDB.EXISTS_MULTI_MAX_KEYS_PER_NATIVE_CALL) {
			var logicalKey = keys.get(offset++);
			var calculatedKey = col.calculateKey(logicalKey.keys());
			logicalChunk.add(logicalKey);
			calculatedChunk.add(calculatedKey);
			calculatedKeyBytes = EmbeddedDB.saturatingAdd(calculatedKeyBytes, calculatedKey.size());
			if (calculatedKeyBytes >= EmbeddedDB.EXISTS_MULTI_MAX_KEY_BYTES_PER_NATIVE_CALL) {
				break;
			}
		}
		return new ExistsMultiChunk(logicalChunk, calculatedChunk, offset);
	}

	private List<Boolean> existsMultiStatusOnly(ColumnInstance col,
			ReadOptions readOptions,
			List<Buf> calculatedKeys, boolean sampleLatency) throws org.rocksdb.RocksDBException {
		var arena = Arena.ofConfined();
		var arenaObserver = edb.existsMultiArenaObserver;
		try {
			if (arenaObserver != null) {
				arenaObserver.accept(true);
			}
			// One native block for all keys, sliced per key
			var nativeKeys = NativeStaging.stageKeys(arena, calculatedKeys);
			var emptyValues = new MemorySegment[calculatedKeys.size()];
			// The MemorySegment overload creates a distinct ByteBuffer per entry; zero capacity prevents writes.
			Arrays.fill(emptyValues, arena.allocate(0));

			var nativeDb = edb.db.get();
			var columnHandles = List.of(col.cfh());
			var sample = sampleLatency ? edb.existsMultiPerfSampler.begin(nativeDb) : null;
			List<org.rocksdb.ByteBufferGetStatus> statuses;
			long readStarted = System.nanoTime();
			edb.activeNativeMultiGets.incrementAndGet();
			try {
				statuses = nativeDb.multiGetByteBuffers(readOptions, columnHandles, nativeKeys, emptyValues);
			} finally {
				edb.activeNativeMultiGets.decrementAndGet();
				edb.storageMetrics.recordCompactionForegroundRead(readStarted, nativeKeys.length);
				if (sample != null) edb.existsMultiPerfSampler.finish(sample);
			}
			var result = new ArrayList<Boolean>(statuses.size());
			for (var status : statuses) {
				result.add(switch (status.status.getCode()) {
					case Ok -> true;
					case NotFound -> false;
					default -> throw EmbeddedDB.mapIteratorStatusException(new org.rocksdb.RocksDBException(status.status));
				});
			}
			return result;
		} finally {
			try {
				arena.close();
			} finally {
				if (arenaObserver != null) {
					arenaObserver.accept(false);
				}
			}
		}
	}

	private List<Boolean> existsMultiWithValues(@Nullable Tx tx,
			ColumnInstance col,
			ReadOptions readOptions,
			List<Keys> logicalKeys,
			List<Buf> calculatedKeys) throws org.rocksdb.RocksDBException {
		var nativeKeys = calculatedKeys.stream().map(EmbeddedDBWriteElision::jniArray).toList();
		List<byte[]> values;
		long readStarted = System.nanoTime();
		edb.activeNativeMultiGets.incrementAndGet();
		try {
			if (tx == null) {
				values = edb.db.get().multiGetAsList(readOptions,
						Collections.nCopies(nativeKeys.size(), col.cfh()),
						nativeKeys);
			} else {
				values = tx.val().multiGetAsList(readOptions, col.cfh(), nativeKeys);
			}
		} finally {
			edb.activeNativeMultiGets.decrementAndGet();
			edb.storageMetrics.recordCompactionForegroundRead(readStarted, nativeKeys.size());
		}

		var result = new ArrayList<Boolean>(values.size());
		for (int i = 0; i < values.size(); i++) {
			var value = values.get(i);
			if (value == null) {
				result.add(false);
			} else if (col.hasBuckets()) {
				var bucket = new Bucket(col, Buf.wrap(value));
				result.add(bucket.getElement(col.getBucketElementKeys(logicalKeys.get(i).keys())) != null);
			} else {
				result.add(true);
			}
		}
		return result;
	}
}
