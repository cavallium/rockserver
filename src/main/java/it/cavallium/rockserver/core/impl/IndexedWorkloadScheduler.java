package it.cavallium.rockserver.core.impl;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.Exceptions;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Reactor adapter that preserves indexed removal when a queued workload task is disposed.
 *
 * <p>{@link Schedulers#fromExecutor(java.util.concurrent.Executor)} can suppress a disposed
 * task when it eventually runs, but it cannot remove that wrapper from an arbitrary executor.
 * Keeping the exact submitted wrapper here lets cancellation use the scheduler's identity index
 * immediately instead of waiting for a blocked workload lane to dispatch it.</p>
 */
final class IndexedWorkloadScheduler implements Scheduler, RWScheduler.WorkloadExecutor {

	private final ProfiledWorkloadExecutor executor;
	private final it.cavallium.rockserver.core.common.WorkloadProfile profile;
	private final it.cavallium.rockserver.core.common.OperationFamily family;
	private final long deadlineEpochMillis;
	private final Set<IndexedWorker> workers = new HashSet<>();
	private volatile boolean disposed;

	IndexedWorkloadScheduler(ProfiledWorkloadExecutor executor,
			it.cavallium.rockserver.core.common.WorkloadProfile profile,
			it.cavallium.rockserver.core.common.OperationFamily family,
			long deadlineEpochMillis) {
		this.executor = Objects.requireNonNull(executor, "executor");
		this.profile = Objects.requireNonNull(profile, "profile");
		this.family = Objects.requireNonNull(family, "family");
		this.deadlineEpochMillis = deadlineEpochMillis;
	}

	RWScheduler.WorkloadExecutor workloadExecutor() {
		return this;
	}

	Disposable executeWhenCapacity(Runnable command) {
		if (disposed) {
			throw Exceptions.failWithRejected();
		}
		long estimatedBytes = command instanceof RWScheduler.EstimatedWork estimatedWork
				? estimatedWork.estimatedBytes()
				: 0L;
		return executor.executeWhenCapacity(profile,
				family,
				deadlineEpochMillis,
				estimatedBytes,
				command);
	}

	@Override
	public void execute(Runnable command) {
		long estimatedBytes = command instanceof RWScheduler.EstimatedWork estimatedWork
				? estimatedWork.estimatedBytes()
				: 0L;
		execute(command, estimatedBytes);
	}

	@Override
	public void execute(Runnable command, long estimatedBytes) {
		executor.execute(profile, family, deadlineEpochMillis, estimatedBytes, command);
	}

	@Override
	public RWScheduler.CooperativeHandle executeCooperatively(RWScheduler.CooperativeTask command,
			long estimatedBytes) {
		return executor.executeCooperatively(profile, family, deadlineEpochMillis, estimatedBytes, command);
	}

	private boolean removeQueuedTask(Runnable command) {
		return executor.remove(profile, family, command);
	}

	@Override
	public Disposable schedule(Runnable task) {
		if (disposed) {
			throw Exceptions.failWithRejected();
		}
		Runnable original = Objects.requireNonNull(task, "task");
		var scheduledTask = scheduledTask(this,
				Schedulers.onSchedule(original),
				null,
				metadata(original));
		scheduledTask.submit();
		return scheduledTask;
	}

	private static @Nullable Runnable metadata(Runnable task) {
		return task instanceof RWScheduler.EstimatedWork
				|| task instanceof RWScheduler.RejectionAwareTask
				? task
				: null;
	}

	private static IndexedScheduledTask scheduledTask(IndexedWorkloadScheduler scheduler,
	                                                  Runnable task,
	                                                  @Nullable IndexedWorker parent,
	                                                  @Nullable Runnable originalMetadata) {
		return originalMetadata instanceof RWScheduler.EstimatedWork
				? new EstimatedIndexedScheduledTask(scheduler, task, parent, originalMetadata)
				: new IndexedScheduledTask(scheduler, task, parent, originalMetadata);
	}

	@Override
	public synchronized Worker createWorker() {
		if (disposed) {
			throw Exceptions.failWithRejected();
		}
		var worker = new IndexedWorker(this);
		workers.add(worker);
		return worker;
	}

	@Override
	public void dispose() {
		IndexedWorker[] registered;
		synchronized (this) {
			if (disposed) return;
			disposed = true;
			registered = workers.toArray(IndexedWorker[]::new);
			workers.clear();
		}
		for (var worker : registered) worker.dispose();
	}

	@Override
	public boolean isDisposed() {
		return disposed;
	}

	private synchronized void delete(IndexedWorker worker) {
		workers.remove(worker);
	}

	private static final class IndexedWorker implements Worker {

		private final IndexedWorkloadScheduler scheduler;
		private final Disposable.Composite tasks = Disposables.composite();

		private IndexedWorker(IndexedWorkloadScheduler scheduler) {
			this.scheduler = scheduler;
		}

		@Override
		public Disposable schedule(Runnable task) {
			if (scheduler.isDisposed()) {
				throw Exceptions.failWithRejected();
			}
			Runnable original = Objects.requireNonNull(task, "task");
			var scheduledTask = scheduledTask(scheduler,
					Schedulers.onSchedule(original),
					this,
					metadata(original));
			if (!tasks.add(scheduledTask)) {
				throw Exceptions.failWithRejected();
			}
			try {
				scheduledTask.submit();
			} catch (RuntimeException | Error failure) {
				tasks.remove(scheduledTask);
				throw failure;
			}
			return scheduledTask;
		}

		@Override
		public void dispose() {
			tasks.dispose();
			scheduler.delete(this);
		}

		@Override
		public boolean isDisposed() {
			return tasks.isDisposed();
		}

		private void delete(IndexedScheduledTask task) {
			tasks.remove(task);
		}
	}

	private static class IndexedScheduledTask implements Runnable,
			Disposable,
			ProfiledWorkloadExecutor.CancellationTrackedTask,
			RWScheduler.RejectionAwareTask {

		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CANCELLED = 3;
		private static final int DISPATCH_PENDING = 0;
		private static final int DISPATCH_CLAIMED = 1;
		private static final int DISPATCH_CANCELLED = 2;
		private static final int TERMINAL_FAILURE_PUBLISHED_FLAG = 1 << 2;
		private static final VarHandle STATE;
		private static final VarHandle DISPATCH_STATE;

		static {
			try {
				STATE = MethodHandles.lookup().findVarHandle(IndexedScheduledTask.class, "state", int.class);
				DISPATCH_STATE = MethodHandles.lookup()
						.findVarHandle(IndexedScheduledTask.class, "dispatchState", int.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final IndexedWorkloadScheduler scheduler;
		private final Runnable task;
		private final @Nullable IndexedWorker parent;
		private final @Nullable Runnable originalMetadata;
		private volatile int state = QUEUED;
		private volatile int dispatchState = DISPATCH_PENDING;
		private boolean submitted;

		private IndexedScheduledTask(IndexedWorkloadScheduler scheduler,
				Runnable task,
				@Nullable IndexedWorker parent,
				@Nullable Runnable originalMetadata) {
			this.scheduler = scheduler;
			this.task = task;
			this.parent = parent;
			this.originalMetadata = originalMetadata;
		}

		@Override
		public final void reject(RuntimeException failure) {
			Objects.requireNonNull(failure, "failure");
			int priorDispatchState = (int) DISPATCH_STATE.getAndBitwiseOr(
					this, TERMINAL_FAILURE_PUBLISHED_FLAG);
			if ((priorDispatchState & TERMINAL_FAILURE_PUBLISHED_FLAG) != 0) {
				return;
			}
			finishRejectedState();
			if (originalMetadata instanceof RWScheduler.RejectionAwareTask rejectionAwareTask) {
				rejectionAwareTask.reject(failure);
			}
		}

		private void finishRejectedState() {
			while (true) {
				int current = state;
				if (current == FINISHED || STATE.compareAndSet(this, current, FINISHED)) {
					deleteFromParent();
					return;
				}
			}
		}

		private synchronized void submit() {
			if (state != QUEUED) {
				return;
			}
			try {
				scheduler.execute(this);
				submitted = true;
			} catch (Throwable failure) {
				state = FINISHED;
				deleteFromParent();
				Exceptions.throwIfFatal(failure);
				throw Exceptions.failWithRejected(failure);
			}
		}

		@Override
		public final void run() {
			int currentState = state;
			if (currentState == QUEUED) {
				if (!STATE.compareAndSet(this, QUEUED, RUNNING)) {
					currentState = state;
				} else {
					currentState = RUNNING;
				}
			}
			if (currentState != RUNNING) {
				deleteFromParent();
				return;
			}
			try {
				task.run();
			} finally {
				state = FINISHED;
				deleteFromParent();
			}
		}

		@Override
		public final void dispose() {
			boolean cancelledBeforeDispatch = DISPATCH_STATE.compareAndSet(
					this, DISPATCH_PENDING, DISPATCH_CANCELLED);
			boolean removeQueued = false;
			boolean cancellationWon = false;
			synchronized (this) {
				if (cancelledBeforeDispatch && STATE.compareAndSet(this, QUEUED, CANCELLED)) {
					removeQueued = submitted;
					cancellationWon = true;
				}
			}
			if (removeQueued) {
				scheduler.removeQueuedTask(this);
			}
			if (cancellationWon) {
				deleteFromParent();
			}
		}

		@Override
		public final boolean isDisposed() {
			return state >= FINISHED;
		}

		@Override
		public final boolean workloadCancellationRequested() {
			return dispatchState == DISPATCH_CANCELLED;
		}

		@Override
		public final boolean claimWorkloadDispatch() {
			return DISPATCH_STATE.compareAndSet(this, DISPATCH_PENDING, DISPATCH_CLAIMED)
					|| dispatchState == DISPATCH_CLAIMED;
		}

		private void deleteFromParent() {
			if (parent != null) {
				parent.delete(this);
			}
		}

		final @Nullable Runnable originalMetadata() {
			return originalMetadata;
		}
	}

	private static final class EstimatedIndexedScheduledTask extends IndexedScheduledTask
			implements RWScheduler.EstimatedWork {

		private EstimatedIndexedScheduledTask(IndexedWorkloadScheduler scheduler,
				Runnable task,
				@Nullable IndexedWorker parent,
				Runnable originalMetadata) {
			super(scheduler, task, parent, originalMetadata);
		}

		@Override
		public long estimatedBytes() {
			return ((RWScheduler.EstimatedWork) Objects.requireNonNull(originalMetadata())).estimatedBytes();
		}
	}
}
