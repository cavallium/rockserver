package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.ReactorResultOwnership;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

/** Runs single-shot calls on a scheduler with cancellation, rejection and must-complete semantics. */
import static it.cavallium.rockserver.core.server.GrpcIteratorOperations.ITERATOR_OPERATION_LEASE_CONTEXT_KEY;
import static it.cavallium.rockserver.core.server.GrpcRequestErrors.lateErrorHandler;
import it.cavallium.rockserver.core.server.GrpcIteratorOperations.IteratorOperationLease;

final class GrpcScheduledCalls {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final MustCompleteOperationTracker mustCompleteOperations;

	GrpcScheduledCalls(MustCompleteOperationTracker mustCompleteOperations) {
		this.mustCompleteOperations = mustCompleteOperations;
	}

	private enum ScheduledCancellationPolicy {
		CANCEL_WHILE_QUEUED,
		MUST_COMPLETE
	}

	/** Tracks whether cancellation won before a scheduled callable began running. */
	private final class ScheduledTaskLifecycle {

		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int TERMINATED = 2;
		private static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup().findVarHandle(ScheduledTaskLifecycle.class, "state", int.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final IteratorOperationLease iteratorLease;
		private volatile int state = QUEUED;

		private ScheduledTaskLifecycle(IteratorOperationLease iteratorLease) {
			this.iteratorLease = iteratorLease;
		}

		private boolean start() {
			return STATE.compareAndSet(this, QUEUED, RUNNING);
		}

		private void cancelBeforeStart() {
			if (STATE.compareAndSet(this, QUEUED, TERMINATED)) {
				taskTerminated();
			}
		}

		private void runningTaskTerminated() {
			if (!STATE.compareAndSet(this, RUNNING, TERMINATED)) {
				throw new IllegalStateException("Scheduled task did not terminate from running state");
			}
			taskTerminated();
		}

		private void taskTerminated() {
			iteratorLease.taskTerminated();
		}
	}

	/**
	 * Bridge an externally-owned future without delivering its terminal signal after
	 * the RPC has been cancelled. Native calls may finish after cancellation; their
	 * late failures still go through the request-scoped diagnostic handler instead of
	 * Reactor's global onErrorDropped hook.
	 */
	<T> Mono<T> fromCancellableFuture(CompletableFuture<T> future) {
		return bridgeCancellableFuture(future, null);
	}

	/**
	 * Start one logical iterator continuation only after its server-side lease has
	 * registered an active task. RPC cancellation releases the subscriber side of the
	 * lease immediately, but the task side remains registered until the underlying
	 * future terminates after any in-flight JNI step.
	 */
	<T> Mono<T> fromCancellableIteratorFuture(
			Supplier<CompletableFuture<T>> futureSupplier) {
		return Mono.deferContextual(contextView -> {
			var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
					ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
			if (iteratorLease == null) {
				return Mono.error(new IllegalStateException(
						"Iterator future was started without an operation lease"));
			}
			if (!iteratorLease.registerTask()) {
				return Mono.error(new java.util.concurrent.CancellationException(
						"Iterator operation terminated before its continuation could start"));
			}
			final CompletableFuture<T> future;
			try {
				future = Objects.requireNonNull(futureSupplier.get(), "futureSupplier result");
			} catch (Throwable failure) {
				iteratorLease.taskTerminated();
				return Mono.error(failure);
			}
			return bridgeCancellableFuture(future, iteratorLease);
		});
	}

	<T> Mono<T> bridgeCancellableFuture(CompletableFuture<T> future,
			@Nullable IteratorOperationLease terminalLease) {
		return Mono.create(sink -> {
			var bridge = new CancellableFutureBridge<>(future, sink, terminalLease);
			sink.onCancel(bridge);
			future.whenComplete(bridge);
		});
	}

	private final class CancellableFutureBridge<T> implements Disposable, BiConsumer<T, Throwable> {

		private final CompletableFuture<T> future;
		private final reactor.core.publisher.MonoSink<T> sink;
		private final @Nullable IteratorOperationLease terminalLease;
		private boolean cancelled;

		private CancellableFutureBridge(CompletableFuture<T> future,
				reactor.core.publisher.MonoSink<T> sink,
				@Nullable IteratorOperationLease terminalLease) {
			this.future = future;
			this.sink = sink;
			this.terminalLease = terminalLease;
		}

		@Override
		public void dispose() {
			synchronized (this) {
				cancelled = true;
			}
			future.cancel(true);
		}

		@Override
		public void accept(T value, Throwable failure) {
			Throwable error = failure instanceof CompletionException completionError
					&& completionError.getCause() != null
					? completionError.getCause()
					: failure;
			if (terminalLease != null) {
				terminalLease.taskTerminated();
			}
			boolean late;
			synchronized (this) {
				late = cancelled;
				if (!late) {
					if (error != null) {
						sink.error(error);
					} else {
						sink.success(value);
					}
				}
			}
			if (late && error != null
					&& !(error instanceof java.util.concurrent.CancellationException)) {
				lateErrorHandler(sink.contextView()).accept(error);
			}
		}
	}

	private final class ScheduledCall<T> implements Disposable,
			Runnable,
			RWScheduler.EstimatedWork,
			RWScheduler.RejectionAwareTask {

		private final reactor.core.publisher.MonoSink<T> sink;
		private final Callable<T> callable;
		private final reactor.core.scheduler.Scheduler executionScheduler;
		private final @Nullable Consumer<T> lateSuccessCleanup;
		private final @Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler;
		private final @Nullable ScheduledTaskLifecycle taskLifecycle;
		private final boolean mustComplete;
		private final @Nullable String protectedOperation;
		private final long estimatedBytes;
		private @Nullable Disposable task;
		private @Nullable Object pendingResult;
		private boolean cancelled;
		private boolean running;
		private boolean terminated;

		private ScheduledCall(reactor.core.publisher.MonoSink<T> sink,
				Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				@Nullable Consumer<T> lateSuccessCleanup,
				@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
				@Nullable ScheduledTaskLifecycle taskLifecycle,
				boolean mustComplete,
				@Nullable String protectedOperation,
				long estimatedBytes) {
			this.sink = sink;
			this.callable = callable;
			this.executionScheduler = executionScheduler;
			this.lateSuccessCleanup = lateSuccessCleanup;
			this.lateSuccessCleanupScheduler = lateSuccessCleanupScheduler;
			this.taskLifecycle = taskLifecycle;
			this.mustComplete = mustComplete;
			this.protectedOperation = protectedOperation;
			this.estimatedBytes = estimatedBytes;
		}

		@Override
		public long estimatedBytes() {
			return estimatedBytes;
		}

		private void schedule() {
			final Disposable submitted;
			try {
				submitted = executionScheduler.schedule(this);
			} catch (Throwable schedulingError) {
				schedulingFailed(schedulingError);
				return;
			}
			boolean disposeSubmitted;
			synchronized (this) {
				task = submitted;
				disposeSubmitted = cancelled && !mustComplete;
			}
			if (disposeSubmitted) {
				submitted.dispose();
			}
		}

		@Override
		public void dispose() {
			final Disposable submitted;
			final @Nullable Object cancelledResult;
			synchronized (this) {
				if (cancelled) {
					return;
				}
				cancelled = true;
				cancelledResult = pendingResult;
				pendingResult = null;
				submitted = mustComplete ? null : task;
			}
			cleanupCancelledResult(cancelledResult);
			if (mustComplete) {
				mustCompleteOperations.cancelledMustCompleteOperations.incrementAndGet();
			} else {
				if (taskLifecycle != null) {
					taskLifecycle.cancelBeforeStart();
				}
				if (submitted != null) {
					submitted.dispose();
				}
			}
		}

		@Override
		public void run() {
			if (!claimRun()) {
				return;
			}
			if (taskLifecycle != null && !taskLifecycle.start()) {
				markAbortedRunTerminated();
				return;
			}
			final T result;
			try {
				result = callable.call();
			} catch (Throwable error) {
				runningTaskTerminated();
				emitOrRecordLateError(error);
				return;
			}
			runningTaskTerminated();
			publishResult(result);
		}

		private void runningTaskTerminated() {
			synchronized (this) {
				if (!running || terminated) {
					throw new IllegalStateException("Scheduled call did not terminate exactly once from running state");
				}
				running = false;
				terminated = true;
			}
			if (taskLifecycle != null) {
				taskLifecycle.runningTaskTerminated();
			}
			if (mustComplete) {
				mustCompleteOperations.operationTerminated();
			}
		}

		private void runLateSuccessCleanup(T result) {
			Runnable cleanup = () -> {
				try {
					lateSuccessCleanup.accept(result);
				} catch (Throwable cleanupError) {
					lateErrorHandler(sink.contextView()).accept(cleanupError);
				}
			};
			try {
				if (lateSuccessCleanupScheduler != null) {
					lateSuccessCleanupScheduler.schedule(cleanup);
				} else {
					cleanup.run();
				}
			} catch (Throwable schedulingError) {
				LOG.debug("Late-success cleanup scheduler rejected the task; "
						+ "running iterator cleanup inline", schedulingError);
				cleanup.run();
			}
		}

		private void publishResult(T result) {
			Object cancelledResult = null;
			boolean emit;
			synchronized (this) {
				if (pendingResult != null) {
					throw new IllegalStateException("Scheduled call already owns a pending result");
				}
				pendingResult = ReactorResultOwnership.encode(result);
				emit = !cancelled;
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

		private void resultDisposed() {
			Object cancelledResult = null;
			synchronized (this) {
				if (cancelled) {
					cancelledResult = pendingResult;
				}
				pendingResult = null;
			}
			cleanupCancelledResult(cancelledResult);
		}

		private void cleanupCancelledResult(@Nullable Object encodedResult) {
			if (encodedResult == null || lateSuccessCleanup == null) {
				return;
			}
			runLateSuccessCleanup(ReactorResultOwnership.decode(encodedResult));
		}

		private void schedulingFailed(Throwable schedulingError) {
			if (schedulingError instanceof RuntimeException runtimeFailure) {
				reject(runtimeFailure);
			} else {
				reject(new RejectedExecutionException("gRPC scheduling failed", schedulingError));
			}
		}

		@Override
		public void reject(RuntimeException failure) {
			if (!claimRejection()) {
				return;
			}
			if (taskLifecycle != null) {
				taskLifecycle.cancelBeforeStart();
			}
			if (mustComplete) {
				mustCompleteOperations.operationTerminated();
			}
			emitOrRecordLateError(failure);
		}

		private boolean claimRun() {
			synchronized (this) {
				if (terminated || running) {
					return false;
				}
				if (cancelled && !mustComplete) {
					terminated = true;
					return false;
				}
				running = true;
				return true;
			}
		}

		private boolean claimRejection() {
			synchronized (this) {
				if (terminated || running) {
					return false;
				}
				terminated = true;
				return true;
			}
		}

		private void markAbortedRunTerminated() {
			synchronized (this) {
				running = false;
				terminated = true;
			}
		}

		private void emitOrRecordLateError(Throwable error) {
			boolean lateError;
			synchronized (this) {
				lateError = cancelled;
				if (!lateError) {
					sink.error(error);
				}
			}
			if (lateError) {
				if (mustComplete) {
					mustCompleteOperations.recordLateProtectedOperationFailure(
							Objects.requireNonNull(protectedOperation),
							error,
							sink.contextView());
				} else {
					lateErrorHandler(sink.contextView()).accept(error);
				}
			}
		}
	}

	<T> Mono<T> executeScheduled(Callable<T> callable, reactor.core.scheduler.Scheduler executionScheduler) {
		return executeScheduled(callable, executionScheduler, 0L);
	}

	<T> Mono<T> executeScheduled(Callable<T> callable,
			reactor.core.scheduler.Scheduler executionScheduler,
			long estimatedBytes) {
		return executeScheduled(callable,
				executionScheduler,
				null,
				null,
				ScheduledCancellationPolicy.CANCEL_WHILE_QUEUED,
				null,
				estimatedBytes);
	}

	<T> Mono<T> executeMustComplete(String operation,
			Callable<T> callable,
			reactor.core.scheduler.Scheduler executionScheduler) {
		return executeScheduled(callable,
				executionScheduler,
				null,
				null,
				ScheduledCancellationPolicy.MUST_COMPLETE,
				operation,
				0L);
	}

	/**
	 * {@link #executeMustComplete} of an operation of a non-embedded connection, which is only started here, on the
	 * call's own thread, and takes no worker while it waits. Its caller's cancellation does not stop it, and shutdown
	 * waits for it, like any other must-complete operation.
	 */
	<T> Mono<T> executeMustCompleteAsync(String operation, Supplier<CompletableFuture<T>> start) {
		return Mono.create(sink -> {
			if (!mustCompleteOperations.register()) {
				sink.error(new RejectedExecutionException("gRPC server is shutting down"));
				return;
			}
			var cancelled = new AtomicBoolean();
			sink.onCancel(() -> {
				cancelled.set(true);
				mustCompleteOperations.cancelledMustCompleteOperations.incrementAndGet();
			});
			CompletableFuture<T> future;
			try {
				future = start.get();
			} catch (Throwable error) {
				future = CompletableFuture.failedFuture(error);
			}
			future.whenComplete((result, error) -> {
				mustCompleteOperations.operationTerminated();
				if (error == null) {
					sink.success(result);
				} else if (!cancelled.get()) {
					sink.error(error);
				} else if (!GrpcServerImpl.isRetryOfGoneCaller(error)) {
					mustCompleteOperations.recordLateProtectedOperationFailure(operation, error, sink.contextView());
				}
			});
		});
	}

	<T> Mono<T> executeScheduled(Callable<T> callable,
			reactor.core.scheduler.Scheduler executionScheduler,
			@Nullable Consumer<T> lateSuccessCleanup,
			@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler) {
		return executeScheduled(callable,
				executionScheduler,
				lateSuccessCleanup,
				lateSuccessCleanupScheduler,
				0L);
	}

	<T> Mono<T> executeScheduled(Callable<T> callable,
			reactor.core.scheduler.Scheduler executionScheduler,
			@Nullable Consumer<T> lateSuccessCleanup,
			@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
			long estimatedBytes) {
		return executeScheduled(callable,
				executionScheduler,
				lateSuccessCleanup,
				lateSuccessCleanupScheduler,
				ScheduledCancellationPolicy.CANCEL_WHILE_QUEUED,
				null,
				estimatedBytes);
	}

	<T> Mono<T> executeScheduled(Callable<T> callable,
			reactor.core.scheduler.Scheduler executionScheduler,
			@Nullable Consumer<T> lateSuccessCleanup,
			@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler,
			ScheduledCancellationPolicy cancellationPolicy,
			@Nullable String protectedOperation,
			long estimatedBytes) {
		return Mono.deferContextual(contextView -> {
			var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
					ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
			ScheduledTaskLifecycle taskLifecycle;
			if (iteratorLease != null) {
				if (!iteratorLease.registerTask()) {
					return Mono.error(new java.util.concurrent.CancellationException(
							"Iterator operation terminated before its task could be scheduled"));
				}
				taskLifecycle = new ScheduledTaskLifecycle(iteratorLease);
			} else {
				taskLifecycle = null;
			}
			return Mono.<T>create(sink -> {
				boolean mustComplete = cancellationPolicy == ScheduledCancellationPolicy.MUST_COMPLETE;
				if (mustComplete && !mustCompleteOperations.register()) {
					if (taskLifecycle != null) {
						taskLifecycle.cancelBeforeStart();
					}
					sink.error(new RejectedExecutionException("gRPC server is shutting down"));
					return;
				}
				// RocksDB JNI calls may keep running after interruption until their native deadline. The
				// lifecycle object serializes cancellation with terminal delivery and retains the submitted
				// handle so cancellation before handle publication still removes queued work.
				var scheduledCall = new ScheduledCall<>(sink,
						callable,
						executionScheduler,
						lateSuccessCleanup,
						lateSuccessCleanupScheduler,
							taskLifecycle,
							mustComplete,
							protectedOperation,
							estimatedBytes);
				sink.onCancel(scheduledCall);
				sink.onDispose(scheduledCall::resultDisposed);
				scheduledCall.schedule();
			});
		});
	}
}
