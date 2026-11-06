package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.buffer.Buf;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;

/** One operation at a time per iterator, and the cooperative MULTI continuation stream. */
import static it.cavallium.rockserver.core.server.GrpcRequestErrors.lateErrorHandler;

final class GrpcIteratorOperations {

	private final GrpcServer server;
	private final GrpcRequestContexts contexts;

	static final Object ITERATOR_OPERATION_LEASE_CONTEXT_KEY = new Object();

	private static final long DEFAULT_ITERATOR_QUANTUM_BYTES = 8L * 1_024L * 1_024L;

	private static final long DEFAULT_ITERATOR_QUANTUM_NANOS = TimeUnit.MILLISECONDS.toNanos(8L);

	final ConcurrentMap<Long, Object> iteratorOperations = new ConcurrentHashMap<>();

	GrpcIteratorOperations(GrpcServer server, GrpcRequestContexts contexts) {
		this.server = server;
		this.contexts = contexts;
	}

	IteratorQuantumLimits iteratorQuantumLimits() {
		var database = server.embeddedDatabase;
		if (database == null) {
			return new IteratorQuantumLimits(
					GrpcServerImpl.ITERATOR_ADVANCE_STEP_SIZE,
					DEFAULT_ITERATOR_QUANTUM_BYTES,
					DEFAULT_ITERATOR_QUANTUM_NANOS);
		}
		var settings = database.getWorkloadSettings();
		return new IteratorQuantumLimits(
				settings.rangeQuantumMaxItems(),
				settings.rangeQuantumMaxBytes(),
				settings.rangeQuantumMaxDuration().toNanos());
	}

	/**
	 * Preserve one logical scheduler node for a long MULTI continuation while keeping
	 * gRPC delivery incremental. Native skip calls remain bounded to 4,096 entries and
	 * value calls to 64 entries; only heap-backed values survive a cooperative park.
	 */
	Flux<KV> cooperativeIteratorMulti(SubsequentRequest request) {
		return Flux.deferContextual(contextView -> {
			var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
					ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
			if (iteratorLease == null) {
				return Flux.error(new IllegalStateException(
						"Cooperative iterator stream was started without an operation lease"));
			}

			final ResolvedRequestContext requestContext;
			final RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<List<Buf>> command;
			final WorkloadProfile profile;
			final RocksDBSyncAPI contextualApi;
			final RWScheduler.WorkloadExecutor workloadExecutor;
			final IteratorQuantumLimits quantumLimits;
			try {
				requestContext = contexts.mapRequestContext(request.getContext());
				command = new RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<>(
						request.getIterationId(),
						request.getSkipCount(),
						request.getTakeCount(),
						RequestType.multi());
				profile = contexts.preAdmit(requestContext, OperationFamily.RANGE_PAGE, command);
				contextualApi = contexts.syncApi(requestContext);
				workloadExecutor = server.commandWorkloadScheduler(command).executor(
						profile,
						command.operationFamily(),
						requestContext.localMonotonicDeadlineNanos());
				quantumLimits = iteratorQuantumLimits();
			} catch (Throwable failure) {
				iteratorLease.operationTerminated();
				return Flux.error(failure);
			}

			return Flux.<KV>create(sink -> {
				if (!iteratorLease.registerTask()) {
					iteratorLease.operationTerminated();
					sink.error(new java.util.concurrent.CancellationException(
							"Iterator operation terminated before its stream could start"));
					return;
				}
				final CooperativeIteratorMultiStream state;
				try {
					state = new CooperativeIteratorMultiStream(
							request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							contextualApi,
							quantumLimits,
							iteratorLease,
							sink,
							lateErrorHandler(contextView));
					sink.onRequest(state::request);
					sink.onCancel(state::cancel);
				} catch (Throwable initializationFailure) {
					iteratorLease.taskAndOperationTerminated();
					sink.error(initializationFailure);
					return;
				}
				try {
					state.attach(workloadExecutor.executeCooperatively(
							state,
							state.estimatedBytes()));
					state.start();
				} catch (Throwable admissionFailure) {
					state.admissionFailed(admissionFailure);
				}
			}, FluxSink.OverflowStrategy.ERROR);
		});
	}

	<T> Mono<T> withIteratorLease(long iteratorId, Supplier<Mono<T>> operation) {
		return Mono.defer(() -> {
			var lease = new IteratorOperationLease(iteratorId);
			if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
				return Mono.error(concurrentIteratorOperation(iteratorId));
			}
			try {
				return operation.get()
						.doOnTerminate(lease::operationTerminated)
						.doFinally(_ -> lease.operationTerminated())
						.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
			} catch (Throwable error) {
				lease.operationTerminated();
				return Mono.error(error);
			}
		});
	}

	<T> Flux<T> withIteratorFluxLease(long iteratorId, Supplier<Flux<T>> operation) {
		return Flux.defer(() -> {
			var lease = new IteratorOperationLease(iteratorId);
			if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
				return Flux.error(concurrentIteratorOperation(iteratorId));
			}
			try {
				return operation.get()
						.doOnTerminate(lease::operationTerminated)
						.doFinally(_ -> lease.operationTerminated())
						.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
			} catch (Throwable error) {
				lease.operationTerminated();
				return Flux.error(error);
			}
		});
	}

	/**
	 * A subscriber owns the iterator lease until it terminates and every native task that
	 * it started has actually returned. Cancelling an RPC can interrupt a RocksDB call,
	 * but JNI is allowed to keep running until its own deadline; releasing the lease from
	 * {@code doFinally(CANCEL)} alone would then admit a concurrent operation on the same
	 * native iterator.
	 */
	final class IteratorOperationLease {

		private static final int OPERATION_TERMINATED = 1 << 31;
		private static final int ACTIVE_TASKS_MASK = Integer.MAX_VALUE;
		private static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup().findVarHandle(IteratorOperationLease.class, "state", int.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final long iteratorId;
		private volatile int state;

		private IteratorOperationLease(long iteratorId) {
			this.iteratorId = iteratorId;
		}

		boolean registerTask() {
			while (true) {
				int current = state;
				if ((current & OPERATION_TERMINATED) != 0) {
					return false;
				}
				if ((current & ACTIVE_TASKS_MASK) == ACTIVE_TASKS_MASK) {
					throw new IllegalStateException("Too many active iterator tasks");
				}
				if (STATE.compareAndSet(this, current, current + 1)) {
					return true;
				}
			}
		}

		void taskTerminated() {
			while (true) {
				int current = state;
				int activeTasks = current & ACTIVE_TASKS_MASK;
				if (activeTasks == 0) {
					throw new IllegalStateException("Iterator task accounting underflow");
				}
				int updated = (current & OPERATION_TERMINATED) | (activeTasks - 1);
				if (STATE.compareAndSet(this, current, updated)) {
					if (updated == OPERATION_TERMINATED) {
						iteratorOperations.remove(iteratorId, this);
					}
					return;
				}
			}
		}

		void operationTerminated() {
			while (true) {
				int current = state;
				if ((current & OPERATION_TERMINATED) != 0) {
					return;
				}
				int updated = current | OPERATION_TERMINATED;
				if (STATE.compareAndSet(this, current, updated)) {
					if (updated == OPERATION_TERMINATED) {
						iteratorOperations.remove(iteratorId, this);
					}
					return;
				}
			}
		}

		void taskAndOperationTerminated() {
			// Both dimensions must be released before a cooperative stream publishes
			// completion or failure; the outer doFinally remains an idempotent cancel guard.
			try {
				taskTerminated();
			} finally {
				operationTerminated();
			}
		}
	}

	/**
	 * Server-side streaming counterpart to the embedded iterator continuation. It
	 * retains only logical counters and one 64-value heap page across redispatches;
	 * every native call returns before this task can yield or park.
	 */
	private final class CooperativeIteratorMultiStream
			implements RWScheduler.CooperativeCompletionTask, Disposable {

		private static final VarHandle DEMAND;
		private static final VarHandle TERMINATED;

		static {
			try {
				var lookup = MethodHandles.lookup();
				DEMAND = lookup.findVarHandle(CooperativeIteratorMultiStream.class, "demand", long.class);
				TERMINATED = lookup.findVarHandle(
						CooperativeIteratorMultiStream.class, "terminated", boolean.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final long iteratorId;
		private final RocksDBSyncAPI contextualApi;
		private final IteratorQuantumLimits quantumLimits;
		private final IteratorOperationLease iteratorLease;
		private final FluxSink<KV> sink;
		private final Consumer<Throwable> lateErrors;
		private volatile long demand;
		private volatile boolean terminated;
		private volatile boolean ready;
		private volatile boolean cancellationRequested;
		private volatile @Nullable RWScheduler.CooperativeHandle handle;
		private long remainingSkip;
		private long remainingTake;
		private @Nullable List<Buf> page;
		private int pageIndex;
		private boolean sourceExhausted;
		private boolean checkpointAfterPage;
		private boolean completionPrepared;

		private CooperativeIteratorMultiStream(long iteratorId,
				long skipCount,
				long takeCount,
				RocksDBSyncAPI contextualApi,
				IteratorQuantumLimits quantumLimits,
				IteratorOperationLease iteratorLease,
				FluxSink<KV> sink,
				Consumer<Throwable> lateErrors) {
			this.iteratorId = iteratorId;
			this.remainingSkip = skipCount;
			this.remainingTake = takeCount;
			this.contextualApi = contextualApi;
			this.quantumLimits = quantumLimits;
			this.iteratorLease = iteratorLease;
			this.sink = sink;
			this.lateErrors = lateErrors;
		}

		private void attach(RWScheduler.CooperativeHandle handle) {
			boolean cancel;
			boolean resume;
			synchronized (this) {
				this.handle = Objects.requireNonNull(handle, "handle");
				cancel = cancellationRequested || terminated;
				resume = ready && (remainingTake == 0L || demand > 0L);
			}
			if (cancel) {
				handle.cancel();
			} else if (resume) {
				handle.resume();
			}
		}

		private void start() {
			RWScheduler.CooperativeHandle currentHandle;
			boolean cancel;
			boolean resume;
			synchronized (this) {
				if (terminated) {
					return;
				}
				ready = true;
				currentHandle = Objects.requireNonNull(handle, "handle");
				cancel = cancellationRequested;
				resume = remainingTake == 0L || demand > 0L;
			}
			if (cancel) {
				currentHandle.cancel();
			} else if (resume) {
				currentHandle.resume();
			}
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(
				RWScheduler.CooperativeContext context) {
			if (terminated) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}
			if (!ready) {
				return RWScheduler.CooperativeResult.PARK;
			}
			if (cancellationRequested || context.terminationRequested()) {
				return RWScheduler.CooperativeResult.COMPLETE;
			}

			try {
				while (true) {
					if (cancellationRequested || context.terminationRequested()) {
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (page != null) {
						if (!emitPage()) {
							return RWScheduler.CooperativeResult.PARK;
						}
						boolean checkpointReached = checkpointAfterPage;
						checkpointAfterPage = false;
						if (sourceExhausted || remainingTake == 0L) {
							prepareCompletion();
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						if (demand == 0L) {
							return RWScheduler.CooperativeResult.PARK;
						}
						if (checkpointReached || context.preemptionRequested()) {
							return RWScheduler.CooperativeResult.YIELD;
						}
						continue;
					}

					if (remainingSkip == 0L && remainingTake == 0L) {
						prepareCompletion();
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					if (remainingTake > 0L && demand == 0L) {
						return RWScheduler.CooperativeResult.PARK;
					}

					if (remainingSkip > 0L) {
						long step = Math.min(remainingSkip, quantumLimits.maximumItems());
						boolean exhausted = false;
						boolean checkpointReached = false;
						if (server.embeddedDatabase != null) {
							var quantum = server.embeddedDatabase.advanceIteratorQuantumInternal(
									iteratorId,
									step,
									quantumLimits.maximumDurationNanos(),
									context);
							remainingSkip -= quantum.advanced();
							exhausted = quantum.exhausted();
							checkpointReached = quantum.checkpointRequested();
						} else {
							boolean advanced = contextualApi.subsequent(
									iteratorId, 0L, step, RequestType.exists());
							remainingSkip -= step;
							exhausted = !advanced;
						}
						if (exhausted) {
							sourceExhausted = true;
							remainingSkip = 0L;
							remainingTake = 0L;
							prepareCompletion();
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						if (cancellationRequested || context.terminationRequested()) {
							return RWScheduler.CooperativeResult.COMPLETE;
						}
						if (checkpointReached || context.preemptionRequested()) {
							return RWScheduler.CooperativeResult.YIELD;
						}
						continue;
					}

					long step = Math.min(remainingTake,
							Math.min(GrpcServerImpl.ITERATOR_VALUE_PAGE_SIZE, quantumLimits.maximumItems()));
					final List<Buf> values;
					if (server.embeddedDatabase != null) {
						var quantum = server.embeddedDatabase.readIteratorQuantumInternal(
								iteratorId,
								step,
								quantumLimits.maximumBytes(),
								quantumLimits.maximumDurationNanos(),
								context);
						values = quantum.values();
						sourceExhausted = quantum.exhausted();
						checkpointAfterPage = quantum.checkpointRequested();
					} else {
						values = Objects.requireNonNull(contextualApi.subsequent(
								iteratorId, 0L, step, RequestType.<Buf>multi()),
								"Iterator MULTI page");
						sourceExhausted = values.size() < step;
						checkpointAfterPage = context.preemptionRequested();
					}
					if (values.size() > step) {
						throw new IllegalStateException("Iterator MULTI page exceeded its requested size");
					}
					remainingTake -= values.size();
					if (values.isEmpty()) {
						prepareCompletion();
						return RWScheduler.CooperativeResult.COMPLETE;
					}
					page = values;
					pageIndex = 0;
				}
			} catch (RuntimeException failure) {
				// Keep terminal attribution and first-cause arbitration scheduler-authoritative.
				context.fail(failure);
				return RWScheduler.CooperativeResult.COMPLETE;
			}
		}

		private long estimatedBytes() {
			return quantumLimits.maximumBytes();
		}

		private boolean emitPage() {
			var currentPage = Objects.requireNonNull(page, "page");
			while (pageIndex < currentPage.size()) {
				if (cancellationRequested || demand == 0L) {
					return false;
				}
				var value = currentPage.get(pageIndex++);
				producedOne();
				sink.next(KV.newBuilder()
						.setValue(Utils.toByteString(value))
						.build());
			}
			page = null;
			pageIndex = 0;
			return true;
		}

		private void producedOne() {
			while (true) {
				long current = demand;
				if (current == Long.MAX_VALUE) {
					return;
				}
				if (current <= 0L) {
					throw new IllegalStateException("Iterator value produced without downstream demand");
				}
				if (DEMAND.compareAndSet(this, current, current - 1L)) {
					return;
				}
			}
		}

		private void request(long requested) {
			if (requested <= 0L || terminated) {
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

		private void cancel() {
			if (terminated) {
				return;
			}
			cancellationRequested = true;
			var currentHandle = handle;
			if (currentHandle != null) {
				currentHandle.cancel();
			}
		}

		private void admissionFailed(Throwable failure) {
			finish(failure);
		}

		private void prepareCompletion() {
			synchronized (this) {
				if (terminated) {
					return;
				}
				if (completionPrepared) {
					throw new IllegalStateException("Iterator stream completion was prepared twice");
				}
				completionPrepared = true;
			}
		}

		@Override
		public void completeCooperatively() {
			synchronized (this) {
				if (terminated) {
					return;
				}
				if (!completionPrepared) {
					throw new IllegalStateException(
							"Scheduler selected RUN without an iterator stream result");
				}
			}
			finish(null);
		}

		@Override
		public void reject(RuntimeException failure) {
			finish(failure);
		}

		private void finish(@Nullable Throwable failure) {
			if (!TERMINATED.compareAndSet(this, false, true)) {
				return;
			}
			page = null;
			iteratorLease.taskAndOperationTerminated();
			if (cancellationRequested || sink.isCancelled()) {
				if (failure != null
						&& !(failure instanceof java.util.concurrent.CancellationException)) {
					lateErrors.accept(failure);
				}
			} else if (failure == null) {
				sink.complete();
			} else {
				sink.error(failure);
			}
		}

		@Override
		public void dispose() {
			cancel();
		}

		@Override
		public boolean isDisposed() {
			return terminated;
		}
	}

	record IteratorQuantumLimits(long maximumItems,
			long maximumBytes,
			long maximumDurationNanos) {
	}

	RocksDBException concurrentIteratorOperation(long iteratorId) {
		return RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
				"Concurrent operation on iterator " + iteratorId + " is not supported");
	}
}
