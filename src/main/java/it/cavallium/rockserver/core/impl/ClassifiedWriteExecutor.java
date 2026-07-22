package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.WriteClass;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;

/**
 * Fixed-size, two-class write executor with independent bounded queues.
 *
 * <p>Foreground work may consume every worker. Maintenance work shares those workers but is
 * limited to {@code maintenanceLimit} active/reserved slots. Pending foreground work normally
 * overtakes pending maintenance work; after a bounded number of foreground dequeues, one
 * maintenance task is selected whenever a maintenance slot is available.</p>
 */
final class ClassifiedWriteExecutor {

	static final int MAX_CONSECUTIVE_FOREGROUND_DEQUEUES = 32;
	static final String QUEUED_METRIC = "rockserver.write.admission.queued";
	static final String ACTIVE_METRIC = "rockserver.write.admission.active";
	static final String QUEUE_WAIT_METRIC = "rockserver.write.admission.queue.wait";
	static final String EXECUTION_METRIC = "rockserver.write.admission.execution";
	static final String COMPLETED_METRIC = "rockserver.write.admission.completed";
	static final String CANCELLED_METRIC = "rockserver.write.admission.cancelled";
	static final String REJECTED_METRIC = "rockserver.write.admission.rejected";
	static final String WORKER_LIMIT_METRIC = "rockserver.write.admission.worker.limit";
	static final String QUEUE_LIMIT_METRIC = "rockserver.write.admission.queue.limit";

	private final int workerLimit;
	private final int maintenanceLimit;
	private final int foregroundQueueCapacity;
	private final int maintenanceQueueCapacity;
	private final String databaseName;
	private final ThreadFactory threadFactory;
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition taskAvailable = lock.newCondition();
	private final Condition termination = lock.newCondition();
	private final ArrayDeque<ClassifiedTask> foregroundQueue = new ArrayDeque<>();
	private final ArrayDeque<ClassifiedTask> maintenanceQueue = new ArrayDeque<>();
	private final ArrayDeque<ClassifiedTask> assignedTasks = new ArrayDeque<>();
	private final Set<Thread> workerThreads = new HashSet<>();
	private final ThreadLocal<Boolean> executingTask = new ThreadLocal<>();
	private final LaneMetrics foregroundMetrics;
	private final LaneMetrics maintenanceMetrics;
	private final ClassifiedExecutorService foregroundView;
	private final ClassifiedExecutorService maintenanceView;

	private boolean shutdown;
	private boolean stopNow;
	private boolean terminated;
	private int workers;
	private int startingWorkers;
	private int idleWorkers;
	private int reservedForeground;
	private int reservedMaintenance;
	private int consecutiveForegroundDequeues;

	ClassifiedWriteExecutor(int workerLimit,
			int maintenanceLimit,
			int foregroundQueueCapacity,
			int maintenanceQueueCapacity,
			String databaseName,
			ThreadFactory threadFactory,
			@Nullable MeterRegistry registry) {
		if (workerLimit < 1) {
			throw new IllegalArgumentException("Write worker limit must be positive");
		}
		if (maintenanceLimit < 1 || maintenanceLimit > workerLimit) {
			throw new IllegalArgumentException("Maintenance write limit must be between 1 and the write worker limit");
		}
		if (foregroundQueueCapacity < 1 || maintenanceQueueCapacity < 1) {
			throw new IllegalArgumentException("Write queue capacities must be positive");
		}
		this.workerLimit = workerLimit;
		this.maintenanceLimit = maintenanceLimit;
		this.foregroundQueueCapacity = foregroundQueueCapacity;
		this.maintenanceQueueCapacity = maintenanceQueueCapacity;
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.threadFactory = Objects.requireNonNull(threadFactory, "threadFactory");
		this.foregroundMetrics = new LaneMetrics(
				WriteClass.FOREGROUND, workerLimit, foregroundQueueCapacity, databaseName, registry);
		this.maintenanceMetrics = new LaneMetrics(
				WriteClass.MAINTENANCE, maintenanceLimit, maintenanceQueueCapacity, databaseName, registry);
		this.foregroundView = new ClassifiedExecutorService(this, WriteClass.FOREGROUND);
		this.maintenanceView = new ClassifiedExecutorService(this, WriteClass.MAINTENANCE);
	}

	ExecutorService executor(WriteClass writeClass) {
		return switch (Objects.requireNonNull(writeClass, "writeClass")) {
			case FOREGROUND -> foregroundView;
			case MAINTENANCE -> maintenanceView;
		};
	}

	private void execute(WriteClass writeClass, Runnable command) {
		Objects.requireNonNull(command, "command");
		var metrics = metrics(writeClass);
		lock.lock();
		try {
			if (shutdown) {
				metrics.rejected();
				throw new RejectedExecutionException("Rockserver write executor is shut down");
			}

			// Fill any currently free worker slots from older work before applying the queue
			// capacity to this submission. This preserves FIFO and avoids rejecting a task merely
			// because a worker has just become available but has not yet resumed from its condition.
			dispatchAvailableUnsafe();
			var queue = queue(writeClass);
			int capacity = queueCapacity(writeClass);
			if (queue.size() >= capacity) {
				metrics.rejected();
				throw RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED,
						"Write admission queue is full: database=" + databaseName
								+ ", lane=" + laneName(writeClass)
								+ ", capacity=" + capacity);
			}

			var task = new ClassifiedTask(writeClass, command, System.nanoTime());
			queue.addLast(task);
			metrics.enqueued();
			try {
				dispatchAvailableUnsafe();
			} catch (RuntimeException | Error workerStartFailure) {
				cancelTaskUnsafe(task);
				throw workerStartFailure;
			}
		} finally {
			lock.unlock();
		}
	}

	private void dispatchAvailableUnsafe() {
		while (true) {
			var next = peekNextEligibleUnsafe();
			if (next == null) {
				return;
			}

			int availableReceivers = idleWorkers + startingWorkers - assignedTasks.size();
			if (availableReceivers <= 0) {
				if (workers >= workerLimit) {
					return;
				}
				startWorkerUnsafe();
			}

			next = removeNextEligibleUnsafe();
			if (next == null) {
				return;
			}
			next.state = TaskState.ASSIGNED;
			reserveUnsafe(next.writeClass);
			assignedTasks.addLast(next);
			taskAvailable.signal();
		}
	}

	private @Nullable ClassifiedTask peekNextEligibleUnsafe() {
		if (!foregroundQueue.isEmpty()
				&& (maintenanceQueue.isEmpty()
				|| reservedMaintenance >= maintenanceLimit
				|| consecutiveForegroundDequeues < MAX_CONSECUTIVE_FOREGROUND_DEQUEUES)) {
			return foregroundQueue.peekFirst();
		}
		if (!maintenanceQueue.isEmpty() && reservedMaintenance < maintenanceLimit) {
			return maintenanceQueue.peekFirst();
		}
		return foregroundQueue.peekFirst();
	}

	private @Nullable ClassifiedTask removeNextEligibleUnsafe() {
		var selected = peekNextEligibleUnsafe();
		if (selected == null) {
			return null;
		}
		if (selected.writeClass == WriteClass.FOREGROUND) {
			foregroundQueue.removeFirst();
			consecutiveForegroundDequeues = maintenanceQueue.isEmpty()
					? 0
					: Math.min(MAX_CONSECUTIVE_FOREGROUND_DEQUEUES, consecutiveForegroundDequeues + 1);
		} else {
			maintenanceQueue.removeFirst();
			consecutiveForegroundDequeues = 0;
		}
		return selected;
	}

	private void startWorkerUnsafe() {
		Thread worker = threadFactory.newThread(this::runWorker);
		if (worker == null) {
			throw new RejectedExecutionException("Write thread factory returned null");
		}
		workers++;
		startingWorkers++;
		workerThreads.add(worker);
		try {
			worker.start();
		} catch (RuntimeException | Error failure) {
			workerThreads.remove(worker);
			startingWorkers--;
			workers--;
			throw failure;
		}
	}

	private void runWorker() {
		lock.lock();
		try {
			startingWorkers--;
		} finally {
			lock.unlock();
		}

		try {
			while (true) {
				ClassifiedTask task;
				lock.lock();
				try {
					task = awaitAssignedTaskUnsafe();
					if (task == null) {
						return;
					}
					if (isCancelled(task.delegate)) {
						task.state = TaskState.CANCELLED;
						metrics(task.writeClass).cancelledBeforeStart();
						releaseReservationUnsafe(task.writeClass);
						taskAvailable.signalAll();
						continue;
					}
					task.state = TaskState.RUNNING;
					metrics(task.writeClass).started(task.enqueuedNanos);
				} finally {
					lock.unlock();
				}

				Throwable taskFailure = null;
				long startedNanos = System.nanoTime();
				try {
					executingTask.set(Boolean.TRUE);
					task.delegate.run();
				} catch (Throwable failure) {
					taskFailure = failure;
				} finally {
					executingTask.remove();
					lock.lock();
					try {
						task.state = TaskState.FINISHED;
						metrics(task.writeClass).finished(startedNanos);
						releaseReservationUnsafe(task.writeClass);
						taskAvailable.signalAll();
					} finally {
						lock.unlock();
					}
				}
				if (taskFailure != null) {
					reportTaskFailure(taskFailure);
				}
			}
		} finally {
			workerExited();
		}
	}

	private @Nullable ClassifiedTask awaitAssignedTaskUnsafe() {
		idleWorkers++;
		try {
			dispatchAvailableUnsafe();
			while (assignedTasks.isEmpty()) {
				if (stopNow || (shutdown && foregroundQueue.isEmpty() && maintenanceQueue.isEmpty())) {
					return null;
				}
				try {
					taskAvailable.await();
				} catch (InterruptedException interrupted) {
					if (stopNow) {
						return null;
					}
				}
				dispatchAvailableUnsafe();
			}
			return assignedTasks.removeFirst();
		} finally {
			idleWorkers--;
		}
	}

	private void workerExited() {
		lock.lock();
		try {
			workerThreads.remove(Thread.currentThread());
			workers--;
			if (!shutdown && (!foregroundQueue.isEmpty() || !maintenanceQueue.isEmpty())) {
				dispatchAvailableUnsafe();
			}
			tryTerminateUnsafe();
		} finally {
			lock.unlock();
		}
	}

	private static void reportTaskFailure(Throwable failure) {
		Thread thread = Thread.currentThread();
		var handler = thread.getUncaughtExceptionHandler();
		if (handler != null) {
			handler.uncaughtException(thread, failure);
		}
	}

	boolean remove(WriteClass writeClass, Runnable command) {
		Objects.requireNonNull(command, "command");
		lock.lock();
		try {
			var task = removeMatchingUnsafe(queue(writeClass), command);
			if (task == null) {
				task = removeMatchingUnsafe(assignedTasks, writeClass, command);
				if (task != null) {
					releaseReservationUnsafe(writeClass);
				}
			}
			if (task == null) {
				return false;
			}
			task.state = TaskState.CANCELLED;
			metrics(writeClass).cancelledBeforeStart();
			dispatchAvailableUnsafe();
			tryTerminateUnsafe();
			return true;
		} finally {
			lock.unlock();
		}
	}

	private static @Nullable ClassifiedTask removeMatchingUnsafe(
			Collection<ClassifiedTask> tasks, Runnable command) {
		for (var iterator = tasks.iterator(); iterator.hasNext();) {
			var task = iterator.next();
			if (matches(task, command)) {
				iterator.remove();
				return task;
			}
		}
		return null;
	}

	private static @Nullable ClassifiedTask removeMatchingUnsafe(
			Collection<ClassifiedTask> tasks, WriteClass writeClass, Runnable command) {
		for (var iterator = tasks.iterator(); iterator.hasNext();) {
			var task = iterator.next();
			if (task.writeClass == writeClass && matches(task, command)) {
				iterator.remove();
				return task;
			}
		}
		return null;
	}

	private static boolean matches(ClassifiedTask task, Runnable command) {
		return task.delegate == command || task.delegate.equals(command);
	}

	private void cancelTaskUnsafe(ClassifiedTask task) {
		boolean removed = queue(task.writeClass).remove(task);
		if (!removed && assignedTasks.remove(task)) {
			releaseReservationUnsafe(task.writeClass);
			removed = true;
		}
		if (removed) {
			task.state = TaskState.CANCELLED;
			metrics(task.writeClass).cancelledBeforeStart();
		}
	}

	private void reserveUnsafe(WriteClass writeClass) {
		if (writeClass == WriteClass.FOREGROUND) {
			reservedForeground++;
		} else {
			reservedMaintenance++;
		}
	}

	private void releaseReservationUnsafe(WriteClass writeClass) {
		if (writeClass == WriteClass.FOREGROUND) {
			reservedForeground--;
		} else {
			reservedMaintenance--;
		}
	}

	private void shutdown() {
		lock.lock();
		try {
			if (shutdown) {
				return;
			}
			shutdown = true;
			dispatchAvailableUnsafe();
			taskAvailable.signalAll();
			tryTerminateUnsafe();
		} finally {
			lock.unlock();
		}
	}

	private List<Runnable> shutdownNow() {
		List<Thread> threads;
		List<Runnable> pending = new ArrayList<>();
		lock.lock();
		try {
			shutdown = true;
			stopNow = true;
			drainCancelledUnsafe(foregroundQueue, pending, false);
			drainCancelledUnsafe(maintenanceQueue, pending, false);
			drainCancelledUnsafe(assignedTasks, pending, true);
			consecutiveForegroundDequeues = 0;
			threads = List.copyOf(workerThreads);
			taskAvailable.signalAll();
			tryTerminateUnsafe();
		} finally {
			lock.unlock();
		}
		for (var thread : threads) {
			thread.interrupt();
		}
		for (var task : pending) {
			if (task instanceof Future<?> future) {
				future.cancel(false);
			} else if (task instanceof Disposable disposable) {
				disposable.dispose();
			}
		}
		return pending;
	}

	private void drainCancelledUnsafe(ArrayDeque<ClassifiedTask> source,
			List<Runnable> pending,
			boolean releaseReservations) {
		ClassifiedTask task;
		while ((task = source.pollFirst()) != null) {
			if (releaseReservations) {
				releaseReservationUnsafe(task.writeClass);
			}
			task.state = TaskState.CANCELLED;
			metrics(task.writeClass).cancelledBeforeStart();
			pending.add(task.delegate);
		}
	}

	private boolean isShutdown() {
		lock.lock();
		try {
			return shutdown;
		} finally {
			lock.unlock();
		}
	}

	private boolean isTerminated() {
		lock.lock();
		try {
			return terminated;
		} finally {
			lock.unlock();
		}
	}

	private boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		long remaining = unit.toNanos(timeout);
		lock.lockInterruptibly();
		try {
			while (!terminated) {
				if (remaining <= 0L) {
					return false;
				}
				remaining = termination.awaitNanos(remaining);
			}
			return true;
		} finally {
			lock.unlock();
		}
	}

	private void tryTerminateUnsafe() {
		if (shutdown && workers == 0 && !terminated) {
			terminated = true;
			termination.signalAll();
		}
	}

	int queuedTasks(WriteClass writeClass) {
		return metrics(writeClass).queued.get();
	}

	int activeTasks(WriteClass writeClass) {
		return metrics(writeClass).active.get();
	}

	int workerCount() {
		lock.lock();
		try {
			return workers;
		} finally {
			lock.unlock();
		}
	}

	int workerLimit(WriteClass writeClass) {
		return writeClass == WriteClass.FOREGROUND ? workerLimit : maintenanceLimit;
	}

	int queueCapacity(WriteClass writeClass) {
		return writeClass == WriteClass.FOREGROUND ? foregroundQueueCapacity : maintenanceQueueCapacity;
	}

	boolean isExecutingTask() {
		return Boolean.TRUE.equals(executingTask.get());
	}

	private ArrayDeque<ClassifiedTask> queue(WriteClass writeClass) {
		return writeClass == WriteClass.FOREGROUND ? foregroundQueue : maintenanceQueue;
	}

	private LaneMetrics metrics(WriteClass writeClass) {
		return writeClass == WriteClass.FOREGROUND ? foregroundMetrics : maintenanceMetrics;
	}

	private static boolean isCancelled(Runnable task) {
		return task instanceof Future<?> future && future.isCancelled()
				|| task instanceof Disposable disposable && disposable.isDisposed();
	}

	private static String laneName(WriteClass writeClass) {
		return writeClass.name().toLowerCase(java.util.Locale.ROOT);
	}

	private enum TaskState {
		QUEUED,
		ASSIGNED,
		RUNNING,
		FINISHED,
		CANCELLED
	}

	private static final class ClassifiedTask {

		private final WriteClass writeClass;
		private final Runnable delegate;
		private final long enqueuedNanos;
		private TaskState state = TaskState.QUEUED;

		private ClassifiedTask(WriteClass writeClass, Runnable delegate, long enqueuedNanos) {
			this.writeClass = writeClass;
			this.delegate = delegate;
			this.enqueuedNanos = enqueuedNanos;
		}
	}

	private static final class LaneMetrics {

		private final AtomicInteger queued = new AtomicInteger();
		private final AtomicInteger active = new AtomicInteger();
		private final @Nullable Counter completed;
		private final @Nullable Counter cancelled;
		private final @Nullable Counter rejected;
		private final @Nullable Timer queueWait;
		private final @Nullable Timer execution;

		private LaneMetrics(WriteClass writeClass,
				int workerLimit,
				int queueLimit,
				String databaseName,
				@Nullable MeterRegistry registry) {
			if (registry == null) {
				this.completed = null;
				this.cancelled = null;
				this.rejected = null;
				this.queueWait = null;
				this.execution = null;
				return;
			}
			String lane = laneName(writeClass);
			Gauge.builder(QUEUED_METRIC, queued, AtomicInteger::get)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			Gauge.builder(ACTIVE_METRIC, active, AtomicInteger::get)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			Gauge.builder(WORKER_LIMIT_METRIC, () -> workerLimit)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			Gauge.builder(QUEUE_LIMIT_METRIC, () -> queueLimit)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			this.completed = Counter.builder(COMPLETED_METRIC)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			this.cancelled = Counter.builder(CANCELLED_METRIC)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			this.rejected = Counter.builder(REJECTED_METRIC)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			this.queueWait = Timer.builder(QUEUE_WAIT_METRIC)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
			this.execution = Timer.builder(EXECUTION_METRIC)
					.tag("database", databaseName)
					.tag("lane", lane)
					.register(registry);
		}

		private void enqueued() {
			queued.incrementAndGet();
		}

		private void started(long enqueuedNanos) {
			queued.decrementAndGet();
			active.incrementAndGet();
			if (queueWait != null) {
				queueWait.record(Math.max(0L, System.nanoTime() - enqueuedNanos), TimeUnit.NANOSECONDS);
			}
		}

		private void finished(long startedNanos) {
			active.decrementAndGet();
			if (execution != null) {
				execution.record(Math.max(0L, System.nanoTime() - startedNanos), TimeUnit.NANOSECONDS);
			}
			if (completed != null) {
				completed.increment();
			}
		}

		private void cancelledBeforeStart() {
			queued.decrementAndGet();
			if (cancelled != null) {
				cancelled.increment();
			}
		}

		private void rejected() {
			if (rejected != null) {
				rejected.increment();
			}
		}
	}

	private static final class ClassifiedExecutorService extends AbstractExecutorService {

		private final ClassifiedWriteExecutor owner;
		private final WriteClass writeClass;

		private ClassifiedExecutorService(ClassifiedWriteExecutor owner, WriteClass writeClass) {
			this.owner = owner;
			this.writeClass = writeClass;
		}

		@Override
		public void execute(@NotNull Runnable command) {
			owner.execute(writeClass, command);
		}

		private boolean remove(Runnable command) {
			return owner.remove(writeClass, command);
		}

		@Override
		protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
			return new RemovableFutureTask<>(callable, this);
		}

		@Override
		protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
			return new RemovableFutureTask<>(runnable, value, this);
		}

		@Override
		public void shutdown() {
			owner.shutdown();
		}

		@Override
		public @NotNull List<Runnable> shutdownNow() {
			return owner.shutdownNow();
		}

		@Override
		public boolean isShutdown() {
			return owner.isShutdown();
		}

		@Override
		public boolean isTerminated() {
			return owner.isTerminated();
		}

		@Override
		public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
			return owner.awaitTermination(timeout, unit);
		}

		@Override
		public String toString() {
			return "ClassifiedWriteExecutor[database=" + owner.databaseName
					+ ", lane=" + laneName(writeClass) + "]";
		}
	}

	private static final class RemovableFutureTask<T> extends FutureTask<T> {

		private final ClassifiedExecutorService executor;

		private RemovableFutureTask(Callable<T> callable, ClassifiedExecutorService executor) {
			super(callable);
			this.executor = executor;
		}

		private RemovableFutureTask(Runnable runnable, T value, ClassifiedExecutorService executor) {
			super(runnable, value);
			this.executor = executor;
		}

		@Override
		protected void done() {
			if (isCancelled()) {
				executor.remove(this);
			}
		}
	}
}
