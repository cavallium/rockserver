package it.cavallium.rockserver.core.impl;

import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
 * <p>Long flush/compact work is serialized on {@link #maintenance()}. Iterator cleanup uses
 * {@link #control()}, while CDC validation/WAL publication has the independent {@link #cdc()}
 * lane. Blocking iterator closes therefore cannot stall WAL visibility or consume a
 * read/write worker.</p>
 */
public final class RWScheduler {

	private static final int MAX_CONSECUTIVE_INTERACTIVE_READS = 8;
	/**
	 * Cleanup can wait for an in-flight iterator operation. Two lazy workers let an
	 * unrelated cleanup still progress; CDC has its own independent lane below.
	 */
	private static final int CONTROL_THREAD_CAPACITY = 2;

	private final Scheduler read;
	private final Scheduler interactiveRead;
	private final Scheduler write;
	private final Scheduler maintenance;
	private final Scheduler control;
	private final Scheduler cdc;
	private final Executor readExecutor;
	private final Executor interactiveReadExecutor;
	private final Executor writeExecutor;
	private final Executor maintenanceExecutor;
	private final Executor controlExecutor;
	private final Executor cdcExecutor;

	public RWScheduler(Scheduler read, Scheduler write) {
		this(read,
				read,
				write,
				write,
				write,
				write,
				read::schedule,
				read::schedule,
				write::schedule,
				write::schedule,
				write::schedule,
				write::schedule);
	}

	/** Preserves the original public constructor used by custom/in-process schedulers. */
	public RWScheduler(Scheduler read, Scheduler write, Executor readExecutor, Executor writeExecutor) {
		this(read,
				read,
				write,
				write,
				write,
				write,
				readExecutor,
				readExecutor,
				writeExecutor,
				writeExecutor,
				writeExecutor,
				writeExecutor);
	}

	public RWScheduler(int readCap, int writeCap, String name) {
		this(createResources(readCap, writeCap, name));
	}

	private RWScheduler(Resources resources) {
		this(resources.read(),
				resources.interactiveRead(),
				resources.write(),
				resources.maintenance(),
				resources.control(),
				resources.cdc(),
				resources.readExecutor(),
				resources.interactiveReadExecutor(),
				resources.writeExecutor(),
				resources.maintenanceExecutor(),
				resources.controlExecutor(),
				resources.cdcExecutor());
	}

	private RWScheduler(Scheduler read,
			Scheduler interactiveRead,
			Scheduler write,
			Scheduler maintenance,
			Scheduler control,
			Scheduler cdc,
			Executor readExecutor,
			Executor interactiveReadExecutor,
			Executor writeExecutor,
			Executor maintenanceExecutor,
			Executor controlExecutor,
			Executor cdcExecutor) {
		this.read = Objects.requireNonNull(read, "read");
		this.interactiveRead = Objects.requireNonNull(interactiveRead, "interactiveRead");
		this.write = Objects.requireNonNull(write, "write");
		this.maintenance = Objects.requireNonNull(maintenance, "maintenance");
		this.control = Objects.requireNonNull(control, "control");
		this.cdc = Objects.requireNonNull(cdc, "cdc");
		this.readExecutor = Objects.requireNonNull(readExecutor, "readExecutor");
		this.interactiveReadExecutor = Objects.requireNonNull(interactiveReadExecutor, "interactiveReadExecutor");
		this.writeExecutor = Objects.requireNonNull(writeExecutor, "writeExecutor");
		this.maintenanceExecutor = Objects.requireNonNull(maintenanceExecutor, "maintenanceExecutor");
		this.controlExecutor = Objects.requireNonNull(controlExecutor, "controlExecutor");
		this.cdcExecutor = Objects.requireNonNull(cdcExecutor, "cdcExecutor");
	}

	private static Resources createResources(int readCap, int writeCap, String name) {
		if (readCap < 1 || writeCap < 1) {
			throw new IllegalArgumentException("Read and write scheduler capacities must be positive");
		}
		var readExecutor = createReadExecutor(readCap, name + "-read");
		var interactiveReadExecutor = new InteractiveExecutor(readExecutor);
		var writeExecutor = createExecutor(writeCap, name + "-write");
		var maintenanceExecutor = createExecutor(1, name + "-maintenance");
		var controlExecutor = createExecutor(CONTROL_THREAD_CAPACITY, name + "-control");
		var cdcExecutor = createExecutor(1, name + "-cdc");
		return new Resources(
				Schedulers.fromExecutorService(readExecutor, name + "-read"),
				Schedulers.fromExecutor(interactiveReadExecutor),
				Schedulers.fromExecutorService(writeExecutor, name + "-write"),
				Schedulers.fromExecutorService(maintenanceExecutor, name + "-maintenance"),
				Schedulers.fromExecutorService(controlExecutor, name + "-control"),
				Schedulers.fromExecutorService(cdcExecutor, name + "-cdc"),
				readExecutor,
				interactiveReadExecutor,
				writeExecutor,
				maintenanceExecutor,
				controlExecutor,
				cdcExecutor);
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
		var result = new ArrayList<Scheduler>(6);
		for (var scheduler : List.of(interactiveRead, read, write, maintenance, control, cdc)) {
			if (seen.add(scheduler)) {
				result.add(scheduler);
			}
		}
		return result;
	}

	private List<Executor> distinctExecutors() {
		Set<Executor> seen = Collections.newSetFromMap(new IdentityHashMap<>());
		var result = new ArrayList<Executor>(6);
		for (var executor : List.of(
				interactiveReadExecutor, readExecutor, writeExecutor, maintenanceExecutor, controlExecutor, cdcExecutor)) {
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
			Scheduler maintenance,
			Scheduler control,
			Scheduler cdc,
			Executor readExecutor,
			Executor interactiveReadExecutor,
			Executor writeExecutor,
			Executor maintenanceExecutor,
			Executor controlExecutor,
			Executor cdcExecutor) {
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
