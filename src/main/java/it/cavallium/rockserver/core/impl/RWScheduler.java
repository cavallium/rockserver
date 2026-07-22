package it.cavallium.rockserver.core.impl;

import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.rockserver.core.common.WriteClass;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Shared read/write schedulers for a database.
 *
 * <p>The read side has two scheduling views backed by the same fixed-size executor:
 * {@link #read()} for composite/background steps and {@link #interactiveRead()} for
 * latency-sensitive bounded calls. Interactive work may overtake queued composite work,
 * but a weighted handoff guarantees composite progress under a continuous interactive load.
 * The configured read cap is therefore still the hard limit on running read tasks.</p>
 *
 * <p>The write side exposes foreground and maintenance views over one bounded executor.
 * Foreground work can use the full write limit, maintenance work has a smaller sub-limit, and
 * both lanes retain FIFO order with bounded foreground preference.</p>
 *
 * <p>Long flush/compact work is serialized on {@link #maintenance()}. Iterator cleanup uses
 * {@link #control()}, while CDC validation/WAL publication has the independent {@link #cdc()}
 * lane. Blocking iterator closes therefore cannot stall WAL visibility or consume a
 * read/write worker.</p>
 */
public final class RWScheduler {

	private static final int MAX_CONSECUTIVE_INTERACTIVE_READS = 8;
	public static final int DEFAULT_MAINTENANCE_WRITE_PARALLELISM = 1;
	public static final int DEFAULT_FOREGROUND_WRITE_QUEUE_CAPACITY = 4_096;
	public static final int DEFAULT_MAINTENANCE_WRITE_QUEUE_CAPACITY = 512;
	/**
	 * Cleanup can wait for an in-flight iterator operation. Two lazy workers let an
	 * unrelated cleanup still progress; CDC has its own independent lane below.
	 */
	private static final int CONTROL_THREAD_CAPACITY = 2;

	private final Scheduler read;
	private final Scheduler interactiveRead;
	private final Scheduler write;
	private final Scheduler maintenanceWrite;
	private final Scheduler maintenance;
	private final Scheduler control;
	private final Scheduler cdc;
	private final Executor readExecutor;
	private final Executor interactiveReadExecutor;
	private final Executor writeExecutor;
	private final Executor maintenanceWriteExecutor;
	private final Executor maintenanceExecutor;
	private final Executor controlExecutor;
	private final Executor cdcExecutor;
	private final @Nullable ClassifiedWriteExecutor classifiedWriteExecutor;

	public RWScheduler(Scheduler read, Scheduler write) {
		this(read,
				read,
				write,
				write,
				write,
				write,
				write,
				read::schedule,
				read::schedule,
				write::schedule,
				write::schedule,
				write::schedule,
				write::schedule,
				write::schedule,
				null);
	}

	/** Preserves the original public constructor used by custom/in-process schedulers. */
	public RWScheduler(Scheduler read, Scheduler write, Executor readExecutor, Executor writeExecutor) {
		this(read,
				read,
				write,
				write,
				write,
				write,
				write,
				readExecutor,
				readExecutor,
				writeExecutor,
				writeExecutor,
				writeExecutor,
				writeExecutor,
				writeExecutor,
				null);
	}

	public RWScheduler(int readCap, int writeCap, String name) {
		this(readCap,
				writeCap,
				DEFAULT_MAINTENANCE_WRITE_PARALLELISM,
				DEFAULT_FOREGROUND_WRITE_QUEUE_CAPACITY,
				DEFAULT_MAINTENANCE_WRITE_QUEUE_CAPACITY,
				name);
	}

	public RWScheduler(int readCap,
			int writeCap,
			int maintenanceWriteCap,
			int foregroundWriteQueueCapacity,
			int maintenanceWriteQueueCapacity,
			String name) {
		this(readCap,
				writeCap,
				maintenanceWriteCap,
				foregroundWriteQueueCapacity,
				maintenanceWriteQueueCapacity,
				name,
				null,
				name);
	}

	public RWScheduler(int readCap,
			int writeCap,
			int maintenanceWriteCap,
			int foregroundWriteQueueCapacity,
			int maintenanceWriteQueueCapacity,
			String name,
			@Nullable MeterRegistry meterRegistry,
			String databaseName) {
		this(createResources(readCap,
				writeCap,
				maintenanceWriteCap,
				foregroundWriteQueueCapacity,
				maintenanceWriteQueueCapacity,
				name,
				meterRegistry,
				databaseName));
	}

	private RWScheduler(Resources resources) {
		this(resources.read(),
				resources.interactiveRead(),
				resources.write(),
				resources.maintenanceWrite(),
				resources.maintenance(),
				resources.control(),
				resources.cdc(),
				resources.readExecutor(),
				resources.interactiveReadExecutor(),
				resources.writeExecutor(),
				resources.maintenanceWriteExecutor(),
				resources.maintenanceExecutor(),
				resources.controlExecutor(),
				resources.cdcExecutor(),
				resources.classifiedWriteExecutor());
	}

	private RWScheduler(Scheduler read,
			Scheduler interactiveRead,
			Scheduler write,
			Scheduler maintenanceWrite,
			Scheduler maintenance,
			Scheduler control,
			Scheduler cdc,
			Executor readExecutor,
			Executor interactiveReadExecutor,
			Executor writeExecutor,
			Executor maintenanceWriteExecutor,
			Executor maintenanceExecutor,
			Executor controlExecutor,
			Executor cdcExecutor,
			@Nullable ClassifiedWriteExecutor classifiedWriteExecutor) {
		this.read = Objects.requireNonNull(read, "read");
		this.interactiveRead = Objects.requireNonNull(interactiveRead, "interactiveRead");
		this.write = Objects.requireNonNull(write, "write");
		this.maintenanceWrite = Objects.requireNonNull(maintenanceWrite, "maintenanceWrite");
		this.maintenance = Objects.requireNonNull(maintenance, "maintenance");
		this.control = Objects.requireNonNull(control, "control");
		this.cdc = Objects.requireNonNull(cdc, "cdc");
		this.readExecutor = Objects.requireNonNull(readExecutor, "readExecutor");
		this.interactiveReadExecutor = Objects.requireNonNull(interactiveReadExecutor, "interactiveReadExecutor");
		this.writeExecutor = Objects.requireNonNull(writeExecutor, "writeExecutor");
		this.maintenanceWriteExecutor = Objects.requireNonNull(maintenanceWriteExecutor, "maintenanceWriteExecutor");
		this.maintenanceExecutor = Objects.requireNonNull(maintenanceExecutor, "maintenanceExecutor");
		this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
		this.cdcExecutor = Objects.requireNonNull(cdcExecutor, "cdcExecutor");
		this.classifiedWriteExecutor = classifiedWriteExecutor;
	}

	private static Resources createResources(int readCap,
			int writeCap,
			int maintenanceWriteCap,
			int foregroundWriteQueueCapacity,
			int maintenanceWriteQueueCapacity,
			String name,
			@Nullable MeterRegistry meterRegistry,
			String databaseName) {
		if (readCap < 1) {
			throw new IllegalArgumentException("Read scheduler capacity must be positive");
		}
		var readExecutor = createReadExecutor(readCap, name + "-read");
		var interactiveReadExecutor = new InteractiveExecutor(readExecutor);
		var classifiedWriteExecutor = new ClassifiedWriteExecutor(
				writeCap,
				maintenanceWriteCap,
				foregroundWriteQueueCapacity,
				maintenanceWriteQueueCapacity,
				databaseName,
				threadFactory(name + "-write"),
				meterRegistry);
		var writeExecutor = classifiedWriteExecutor.executor(WriteClass.FOREGROUND);
		var maintenanceWriteExecutor = classifiedWriteExecutor.executor(WriteClass.MAINTENANCE);
		var maintenanceExecutor = createExecutor(1, name + "-maintenance");
		var controlExecutor = createExecutor(CONTROL_THREAD_CAPACITY, name + "-control");
		var cdcExecutor = createExecutor(1, name + "-cdc");
		return new Resources(
				Schedulers.fromExecutorService(readExecutor, name + "-read"),
				Schedulers.fromExecutor(interactiveReadExecutor),
				Schedulers.fromExecutorService(writeExecutor, name + "-write-foreground"),
				Schedulers.fromExecutorService(maintenanceWriteExecutor, name + "-write-maintenance"),
				Schedulers.fromExecutorService(maintenanceExecutor, name + "-maintenance"),
				Schedulers.fromExecutorService(controlExecutor, name + "-control"),
				Schedulers.fromExecutorService(cdcExecutor, name + "-cdc"),
				readExecutor,
				interactiveReadExecutor,
				writeExecutor,
				maintenanceWriteExecutor,
				maintenanceExecutor,
				controlExecutor,
				cdcExecutor,
				classifiedWriteExecutor);
	}

	private static ExecutorService createReadExecutor(int cap, String name) {
		var executor = new ThreadPoolExecutor(cap,
				cap,
				0L,
				TimeUnit.MILLISECONDS,
				new WeightedReadQueue(DEFAULT_BOUNDED_ELASTIC_QUEUESIZE,
						MAX_CONSECUTIVE_INTERACTIVE_READS),
				threadFactory(name),
				new ThreadPoolExecutor.AbortPolicy());
		executor.allowCoreThreadTimeOut(false);
		return executor;
	}

	private static ExecutorService createExecutor(int cap, String name) {
		var executor = new ThreadPoolExecutor(cap,
				cap,
				0L,
				TimeUnit.MILLISECONDS,
				new java.util.concurrent.LinkedBlockingQueue<>(DEFAULT_BOUNDED_ELASTIC_QUEUESIZE),
				threadFactory(name),
				new ThreadPoolExecutor.AbortPolicy());
		executor.allowCoreThreadTimeOut(false);
		return executor;
	}

	private static java.util.concurrent.ThreadFactory threadFactory(String name) {
		return new ThreadFactoryBuilder()
				.setDaemon(false)
				.setNameFormat(name + "-%d")
				.build();
	}

	public Scheduler read() {
		return read;
	}

	public Scheduler interactiveRead() {
		return interactiveRead;
	}

	public Scheduler write() {
		return write;
	}

	/** Return the scheduling view for the requested write class. */
	public Scheduler write(WriteClass writeClass) {
		return switch (Objects.requireNonNull(writeClass, "writeClass")) {
			case FOREGROUND -> write;
			case MAINTENANCE -> maintenanceWrite;
		};
	}

	public Scheduler maintenance() {
		return maintenance;
	}

	public Scheduler control() {
		return control;
	}

	public Scheduler cdc() {
		return cdc;
	}

	public Executor readExecutor() {
		return readExecutor;
	}

	public Executor interactiveReadExecutor() {
		return interactiveReadExecutor;
	}

	public Executor writeExecutor() {
		return writeExecutor;
	}

	/** Return the executor view for the requested write class. */
	public Executor writeExecutor(WriteClass writeClass) {
		return switch (Objects.requireNonNull(writeClass, "writeClass")) {
			case FOREGROUND -> writeExecutor;
			case MAINTENANCE -> maintenanceWriteExecutor;
		};
	}

	public int queuedWriteTasks(WriteClass writeClass) {
		Objects.requireNonNull(writeClass, "writeClass");
		return classifiedWriteExecutor != null ? classifiedWriteExecutor.queuedTasks(writeClass) : 0;
	}

	public int activeWriteTasks(WriteClass writeClass) {
		Objects.requireNonNull(writeClass, "writeClass");
		return classifiedWriteExecutor != null ? classifiedWriteExecutor.activeTasks(writeClass) : 0;
	}

	public int writeWorkerCount() {
		return classifiedWriteExecutor != null ? classifiedWriteExecutor.workerCount() : 0;
	}

	public int writeWorkerLimit(WriteClass writeClass) {
		Objects.requireNonNull(writeClass, "writeClass");
		return classifiedWriteExecutor != null ? classifiedWriteExecutor.workerLimit(writeClass) : 0;
	}

	public int writeQueueCapacity(WriteClass writeClass) {
		Objects.requireNonNull(writeClass, "writeClass");
		return classifiedWriteExecutor != null ? classifiedWriteExecutor.queueCapacity(writeClass) : 0;
	}

	/** Whether the calling thread is already running inside this scheduler's write admission. */
	public boolean isExecutingWriteTask() {
		return classifiedWriteExecutor != null && classifiedWriteExecutor.isExecutingTask();
	}

	public Executor maintenanceExecutor() {
		return maintenanceExecutor;
	}

	public Executor controlExecutor() {
		return controlExecutor;
	}

	public Executor cdcExecutor() {
		return cdcExecutor;
	}

	/**
	 * Remove work that has not started from either scheduling view. Interactive reads are
	 * wrapped only to classify them in the shared weighted queue, so cancellation must remove
	 * that wrapper rather than the original runnable.
	 */
	public boolean removeQueuedTask(Executor schedulingView, Runnable task) {
		Objects.requireNonNull(schedulingView, "schedulingView");
		Objects.requireNonNull(task, "task");
		if (schedulingView instanceof InteractiveExecutor interactiveExecutor) {
			return interactiveExecutor.remove(task);
		}
		if (classifiedWriteExecutor != null
				&& (schedulingView == writeExecutor || schedulingView == maintenanceWriteExecutor)) {
			return classifiedWriteExecutor.remove(
					schedulingView == writeExecutor ? WriteClass.FOREGROUND : WriteClass.MAINTENANCE,
					task);
		}
		return schedulingView instanceof ThreadPoolExecutor threadPool && threadPool.remove(task);
	}

	/** Preserve the value semantics of the original four-component public record. */
	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (!(object instanceof RWScheduler other)) {
			return false;
		}
		return read.equals(other.read)
				&& write.equals(other.write)
				&& readExecutor.equals(other.readExecutor)
				&& writeExecutor.equals(other.writeExecutor);
	}

	@Override
	public int hashCode() {
		return Objects.hash(read, write, readExecutor, writeExecutor);
	}

	@Override
	public String toString() {
		return "RWScheduler[read=" + read
				+ ", write=" + write
				+ ", readExecutor=" + readExecutor
				+ ", writeExecutor=" + writeExecutor + "]";
	}

	public Mono<Void> disposeGracefully() {
		return Mono.whenDelayError(distinctSchedulers().stream()
				.map(Scheduler::disposeGracefully)
				.toList());
	}

	public void dispose() {
		for (var scheduler : distinctSchedulers()) {
			scheduler.dispose();
		}
		for (var executor : distinctExecutors()) {
			shutdownExecutor(executor);
		}
	}

	private List<Scheduler> distinctSchedulers() {
		Set<Scheduler> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		var result = new ArrayList<Scheduler>(7);
		for (var scheduler : List.of(interactiveRead, read, write, maintenanceWrite, maintenance, control, cdc)) {
			if (seen.add(scheduler)) {
				result.add(scheduler);
			}
		}
		return result;
	}

	private List<Executor> distinctExecutors() {
		Set<Executor> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		var result = new ArrayList<Executor>(7);
		for (var executor : List.of(
				interactiveReadExecutor,
				readExecutor,
				writeExecutor,
				maintenanceWriteExecutor,
				maintenanceExecutor,
				controlExecutor,
				cdcExecutor)) {
			if (seen.add(executor)) {
				result.add(executor);
			}
		}
		return result;
	}

	private static void shutdownExecutor(Executor executor) {
		if (executor instanceof ExecutorService es) {
			es.shutdown();
			try {
				if (!es.awaitTermination(10, TimeUnit.SECONDS)) {
					es.shutdownNow();
				}
			} catch (InterruptedException e) {
				es.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}

	private record Resources(Scheduler read,
			Scheduler interactiveRead,
			Scheduler write,
			Scheduler maintenanceWrite,
			Scheduler maintenance,
			Scheduler control,
			Scheduler cdc,
			Executor readExecutor,
			Executor interactiveReadExecutor,
			Executor writeExecutor,
			Executor maintenanceWriteExecutor,
			Executor maintenanceExecutor,
			Executor controlExecutor,
			Executor cdcExecutor,
			ClassifiedWriteExecutor classifiedWriteExecutor) {
	}

	private record InteractiveTask(Runnable delegate) implements Runnable {

		private InteractiveTask {
			Objects.requireNonNull(delegate, "delegate");
		}

		@Override
		public void run() {
			delegate.run();
		}
	}

	private record InteractiveExecutor(Executor delegate) implements Executor {

		private InteractiveExecutor {
			Objects.requireNonNull(delegate, "delegate");
		}

		@Override
		public void execute(@NotNull Runnable command) {
			delegate.execute(command instanceof InteractiveTask ? command : new InteractiveTask(command));
		}

		private boolean remove(Runnable command) {
			if (!(delegate instanceof ThreadPoolExecutor threadPool)) {
				return false;
			}
			return threadPool.remove(command instanceof InteractiveTask
					? command
					: new InteractiveTask(command));
		}
	}

	/** A bounded two-class queue with FIFO order inside each class. */
	private static final class WeightedReadQueue extends AbstractQueue<Runnable>
			implements BlockingQueue<Runnable> {

		private final int capacity;
		private final int maxConsecutiveInteractive;
		private final ArrayDeque<Runnable> interactive = new ArrayDeque<>();
		private final ArrayDeque<Runnable> composite = new ArrayDeque<>();
		private final ReentrantLock lock = new ReentrantLock();
		private final Condition notEmpty = lock.newCondition();
		private final Condition notFull = lock.newCondition();
		private int consecutiveInteractive;

		private WeightedReadQueue(int capacity, int maxConsecutiveInteractive) {
			if (capacity < 1 || maxConsecutiveInteractive < 1) {
				throw new IllegalArgumentException("Queue capacity and interactive weight must be positive");
			}
			this.capacity = capacity;
			this.maxConsecutiveInteractive = maxConsecutiveInteractive;
		}

		@Override
		public boolean add(@NotNull Runnable task) {
			return super.add(task);
		}

		@Override
		public boolean offer(@NotNull Runnable task) {
			Objects.requireNonNull(task, "task");
			lock.lock();
			try {
				if (sizeUnsafe() >= capacity) {
					return false;
				}
				addUnsafe(task);
				return true;
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void put(@NotNull Runnable task) throws InterruptedException {
			Objects.requireNonNull(task, "task");
			lock.lockInterruptibly();
			try {
				while (sizeUnsafe() >= capacity) {
					notFull.await();
				}
				addUnsafe(task);
			} finally {
				lock.unlock();
			}
		}

		@Override
		public boolean offer(@NotNull Runnable task, long timeout, @NotNull TimeUnit unit)
				throws InterruptedException {
			Objects.requireNonNull(task, "task");
			long remaining = unit.toNanos(timeout);
			lock.lockInterruptibly();
			try {
				while (sizeUnsafe() >= capacity) {
					if (remaining <= 0L) {
						return false;
					}
					remaining = notFull.awaitNanos(remaining);
				}
				addUnsafe(task);
				return true;
			} finally {
				lock.unlock();
			}
		}

		private void addUnsafe(Runnable task) {
			(task instanceof InteractiveTask ? interactive : composite).addLast(task);
			notEmpty.signal();
		}

		@Override
		public @NotNull Runnable take() throws InterruptedException {
			lock.lockInterruptibly();
			try {
				while (sizeUnsafe() == 0) {
					notEmpty.await();
				}
				return removeNextUnsafe();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public Runnable poll(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
			long remaining = unit.toNanos(timeout);
			lock.lockInterruptibly();
			try {
				while (sizeUnsafe() == 0) {
					if (remaining <= 0L) {
						return null;
					}
					remaining = notEmpty.awaitNanos(remaining);
				}
				return removeNextUnsafe();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public Runnable poll() {
			lock.lock();
			try {
				return sizeUnsafe() == 0 ? null : removeNextUnsafe();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public Runnable peek() {
			lock.lock();
			try {
				return nextQueueUnsafe().peekFirst();
			} finally {
				lock.unlock();
			}
		}

		private Runnable removeNextUnsafe() {
			var nextQueue = nextQueueUnsafe();
			var task = nextQueue.removeFirst();
			if (nextQueue == interactive) {
				// Priority debt only matters while composite work is actually waiting. Reset
				// it during purely interactive traffic and saturate it so a long-running
				// server can never wrap the counter and postpone the next composite task.
				consecutiveInteractive = composite.isEmpty()
						? 0
						: Math.min(maxConsecutiveInteractive, consecutiveInteractive + 1);
			} else {
				consecutiveInteractive = 0;
			}
			notFull.signal();
			return task;
		}

		private ArrayDeque<Runnable> nextQueueUnsafe() {
			if (!interactive.isEmpty()
					&& (composite.isEmpty() || consecutiveInteractive < maxConsecutiveInteractive)) {
				return interactive;
			}
			return composite;
		}

		@Override
		public int remainingCapacity() {
			lock.lock();
			try {
				return capacity - sizeUnsafe();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public int drainTo(@NotNull Collection<? super Runnable> target) {
			return drainTo(target, Integer.MAX_VALUE);
		}

		@Override
		public int drainTo(@NotNull Collection<? super Runnable> target, int maxElements) {
			Objects.requireNonNull(target, "target");
			if (target == this) {
				throw new IllegalArgumentException("Cannot drain a queue into itself");
			}
			if (maxElements <= 0) {
				return 0;
			}
			lock.lock();
			try {
				int count = 0;
				while (count < maxElements && sizeUnsafe() > 0) {
					target.add(removeNextUnsafe());
					count++;
				}
				return count;
			} finally {
				lock.unlock();
			}
		}

		@Override
		public boolean remove(Object object) {
			lock.lock();
			try {
				boolean removed = interactive.remove(object) || composite.remove(object);
				if (removed) {
					notFull.signal();
				}
				return removed;
			} finally {
				lock.unlock();
			}
		}

		@Override
		public boolean contains(Object object) {
			lock.lock();
			try {
				return interactive.contains(object) || composite.contains(object);
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void clear() {
			lock.lock();
			try {
				if (sizeUnsafe() != 0) {
					interactive.clear();
					composite.clear();
					consecutiveInteractive = 0;
					notFull.signalAll();
				}
			} finally {
				lock.unlock();
			}
		}

		@Override
		public int size() {
			lock.lock();
			try {
				return sizeUnsafe();
			} finally {
				lock.unlock();
			}
		}

		private int sizeUnsafe() {
			return interactive.size() + composite.size();
		}

		@Override
		public @NotNull Iterator<Runnable> iterator() {
			lock.lock();
			try {
				List<Runnable> snapshot = new ArrayList<>(sizeUnsafe());
				snapshot.addAll(interactive);
				snapshot.addAll(composite);
				return List.copyOf(snapshot).iterator();
			} finally {
				lock.unlock();
			}
		}
	}
}
