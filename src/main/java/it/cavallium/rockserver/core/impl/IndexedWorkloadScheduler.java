package it.cavallium.rockserver.core.impl;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
final class IndexedWorkloadScheduler implements Scheduler {

	private final RWScheduler scheduler;
	private final RWScheduler.WorkloadExecutor executor;
	private final AtomicBoolean disposed = new AtomicBoolean();

	IndexedWorkloadScheduler(RWScheduler scheduler, RWScheduler.WorkloadExecutor executor) {
		this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
		this.executor = Objects.requireNonNull(executor, "executor");
	}

	RWScheduler.WorkloadExecutor workloadExecutor() {
		return executor;
	}

	@Override
	public Disposable schedule(Runnable task) {
		if (disposed.get()) {
			throw Exceptions.failWithRejected();
		}
		var scheduledTask = new IndexedScheduledTask(scheduler,
				executor,
				Schedulers.onSchedule(Objects.requireNonNull(task, "task")),
				null);
		scheduledTask.submit();
		return scheduledTask;
	}

	@Override
	public Worker createWorker() {
		if (disposed.get()) {
			throw Exceptions.failWithRejected();
		}
		return new IndexedWorker(scheduler, executor);
	}

	@Override
	public void dispose() {
		disposed.set(true);
	}

	@Override
	public boolean isDisposed() {
		return disposed.get();
	}

	private static final class IndexedWorker implements Worker {

		private final RWScheduler scheduler;
		private final RWScheduler.WorkloadExecutor executor;
		private final Disposable.Composite tasks = Disposables.composite();

		private IndexedWorker(RWScheduler scheduler, RWScheduler.WorkloadExecutor executor) {
			this.scheduler = scheduler;
			this.executor = executor;
		}

		@Override
		public Disposable schedule(Runnable task) {
			var scheduledTask = new IndexedScheduledTask(scheduler,
					executor,
					Schedulers.onSchedule(Objects.requireNonNull(task, "task")),
					this);
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
		}

		@Override
		public boolean isDisposed() {
			return tasks.isDisposed();
		}

		private void delete(IndexedScheduledTask task) {
			tasks.remove(task);
		}
	}

	private static final class IndexedScheduledTask implements Runnable,
			Disposable,
			ProfiledWorkloadExecutor.CancellationTrackedTask {

		private static final int QUEUED = 0;
		private static final int RUNNING = 1;
		private static final int FINISHED = 2;
		private static final int CANCELLED = 3;
		private static final int CANCELLED_WHILE_RUNNING = 4;

		private final RWScheduler scheduler;
		private final RWScheduler.WorkloadExecutor executor;
		private final Runnable task;
		private final @Nullable IndexedWorker parent;
		private final AtomicInteger state = new AtomicInteger(QUEUED);
		private final ProfiledWorkloadExecutor.CancellationState cancellationState =
				new ProfiledWorkloadExecutor.CancellationState();
		private boolean submitted;

		private IndexedScheduledTask(RWScheduler scheduler,
				RWScheduler.WorkloadExecutor executor,
				Runnable task,
				@Nullable IndexedWorker parent) {
			this.scheduler = scheduler;
			this.executor = executor;
			this.task = task;
			this.parent = parent;
		}

		private synchronized void submit() {
			if (state.get() != QUEUED) {
				return;
			}
			try {
				executor.execute(this);
				submitted = true;
			} catch (Throwable failure) {
				state.set(FINISHED);
				deleteFromParent();
				Exceptions.throwIfFatal(failure);
				throw Exceptions.failWithRejected(failure);
			}
		}

		@Override
		public void run() {
			int currentState = state.get();
			if (currentState == QUEUED) {
				if (!state.compareAndSet(QUEUED, RUNNING)) {
					currentState = state.get();
				} else {
					currentState = RUNNING;
				}
			}
			if (currentState != RUNNING && currentState != CANCELLED_WHILE_RUNNING) {
				deleteFromParent();
				return;
			}
			try {
				task.run();
			} finally {
				state.set(FINISHED);
				deleteFromParent();
			}
		}

		@Override
		public void dispose() {
			boolean cancelledBeforeDispatch = cancellationState.cancel();
			boolean removeQueued = false;
			synchronized (this) {
				if (cancelledBeforeDispatch && state.compareAndSet(QUEUED, CANCELLED)) {
					removeQueued = submitted;
				} else if (!cancelledBeforeDispatch) {
					if (!state.compareAndSet(RUNNING, CANCELLED_WHILE_RUNNING)) {
						state.compareAndSet(QUEUED, CANCELLED_WHILE_RUNNING);
					}
				}
			}
			if (removeQueued) {
				scheduler.removeQueuedTask(executor, this);
			}
			deleteFromParent();
		}

		@Override
		public boolean isDisposed() {
			return state.get() >= FINISHED;
		}

		@Override
		public ProfiledWorkloadExecutor.CancellationState workloadCancellationState() {
			return cancellationState;
		}

		private void deleteFromParent() {
			if (parent != null) {
				parent.delete(this);
			}
		}
	}
}
