package it.cavallium.rockserver.core.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.Nullable;
import reactor.core.Disposable;

/**
 * One bounded worker pool with independent workload queues. LATENCY uses EDF;
 * guaranteed-progress profiles use deficit round-robin without a global priority chain.
 */
final class ProfiledWorkloadExecutor extends AbstractExecutorService {

	private static final int MAX_LATENCY_BURST = 8;
	private static final ThreadLocal<Boolean> EXECUTING = ThreadLocal.withInitial(() -> false);
	private static final long PRESSURED_BATCH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
	private static final List<WorkloadProfile> GUARANTEED = List.of(
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH);
	private static final Map<WorkloadProfile, Integer> QUANTA = Map.of(
			WorkloadProfile.INGEST, 4,
			WorkloadProfile.CDC, 4,
			WorkloadProfile.ANALYTICAL, 2,
			WorkloadProfile.BATCH, 1);

	private final String poolName;
	private final int sharedWorkerLimit;
	private final int workerCount;
	private final int analyticalLimit;
	private final EnumMap<WorkloadProfile, Integer> capacities;
	private final EnumMap<WorkloadProfile, PriorityQueue<WorkloadTask>> latencyQueues;
	private final EnumMap<WorkloadProfile, ArrayDeque<WorkloadTask>> queues;
	private final EnumMap<WorkloadProfile, Integer> active = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<WorkloadProfile, Integer> deficit = new EnumMap<>(WorkloadProfile.class);
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition workAvailable = lock.newCondition();
	private final List<Thread> workers = new ArrayList<>();
	private final AtomicLong sequence = new AtomicLong();
	private final AtomicBoolean storagePressure;
	private final @Nullable MeterRegistry registry;
	private final String databaseName;
	private final String resourceKind;
	private volatile boolean shutdown;
	private volatile boolean terminated;
	private int startedWorkers;
	private int latencyBurst;
	private int guaranteedCursor;
	private long lastPressuredBatchAdmissionNanos;

	ProfiledWorkloadExecutor(int workerCount,
			int analyticalLimit,
			Map<WorkloadProfile, Integer> capacities,
			String poolName,
			String resourceKind,
			AtomicBoolean storagePressure,
			@Nullable MeterRegistry registry,
			String databaseName) {
		if (workerCount < 1) {
			throw new IllegalArgumentException("workerCount must be positive");
		}
		this.poolName = Objects.requireNonNull(poolName, "poolName");
		this.sharedWorkerLimit = workerCount;
		// Data pools retain one slot exclusively for CDC. The configured read/write
		// cap still bounds all non-CDC work; the extra worker cannot take another
		// profile while that cap is occupied.
		this.workerCount = capacities.getOrDefault(WorkloadProfile.CDC, 0) > 0
				? Math.addExact(workerCount, 1)
				: workerCount;
		this.analyticalLimit = Math.max(1, Math.min(workerCount, analyticalLimit));
		this.capacities = new EnumMap<>(WorkloadProfile.class);
		this.latencyQueues = new EnumMap<>(WorkloadProfile.class);
		this.queues = new EnumMap<>(WorkloadProfile.class);
		this.storagePressure = Objects.requireNonNull(storagePressure, "storagePressure");
		this.registry = registry;
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.resourceKind = Objects.requireNonNull(resourceKind, "resourceKind");
		for (var profile : WorkloadProfile.values()) {
			int capacity = capacities.getOrDefault(profile, 0);
			if (capacity < 0) {
				throw new IllegalArgumentException("Negative queue capacity for " + profile);
			}
			this.capacities.put(profile, capacity);
			this.active.put(profile, 0);
			this.deficit.put(profile, 0);
			if (profile == WorkloadProfile.LATENCY) {
				this.latencyQueues.put(profile, new PriorityQueue<>(Comparator
						.comparingLong(WorkloadTask::deadlineEpochMillis)
						.thenComparingLong(WorkloadTask::sequence)));
			} else {
				this.queues.put(profile, new ArrayDeque<>());
			}
		}
		registerGauges();
	}

	Executor view(WorkloadProfile profile, OperationFamily family, long deadlineEpochMillis) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		return new WorkloadExecutorView(this, profile, family, deadlineEpochMillis);
	}

	void execute(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis,
			Runnable command) {
		Objects.requireNonNull(command, "command");
		long nowMillis = System.currentTimeMillis();
		if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowMillis >= deadlineEpochMillis) {
			rejection(profile, family, "deadline");
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					"Workload deadline expired before admission");
		}
		lock.lock();
		try {
			if (shutdown) {
				rejection(profile, family, "shutdown");
				throw new RejectedExecutionException(poolName + " is shutting down");
			}
			if (!admitUnderPressure(profile, family)) {
				rejection(profile, family, "storage_pressure");
				throw RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Storage pressure paused " + profile + " " + family);
			}
			purgeDisposedUnsafe(profile);
			if (capacities.get(profile) == 0 || queuedUnsafe(profile) >= capacities.get(profile)) {
				rejection(profile, family, "queue_full");
				throw RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
			}
			var task = new WorkloadTask(profile,
					family,
					deadlineEpochMillis,
					sequence.getAndIncrement(),
					System.nanoTime(),
					command);
			queueUnsafe(profile).add(task);
			ensureWorkersStartedUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
	}

	private boolean admitUnderPressure(WorkloadProfile profile, OperationFamily family) {
		if (!storagePressure.get()) {
			return true;
		}
		if (profile == WorkloadProfile.PHYSICAL_MAINTENANCE) {
			return false;
		}
		if (profile != WorkloadProfile.BATCH) {
			return true;
		}
		long now = System.nanoTime();
		if (lastPressuredBatchAdmissionNanos == 0L
				|| now - lastPressuredBatchAdmissionNanos >= PRESSURED_BATCH_INTERVAL_NANOS) {
			lastPressuredBatchAdmissionNanos = now;
			return true;
		}
		return false;
	}

	private void ensureWorkersStartedUnsafe() {
		while (startedWorkers < workerCount) {
			var worker = new ThreadFactoryBuilder()
					.setDaemon(false)
					.setNameFormat(poolName + "-%d")
					.build()
					.newThread(this::workerLoop);
			startedWorkers++;
			workers.add(worker);
			worker.start();
		}
	}

	private void workerLoop() {
		while (true) {
			WorkloadTask task;
			lock.lock();
			try {
				while ((task = selectUnsafe()) == null) {
					if (shutdown && queuedTotalUnsafe() == 0) {
						workerTerminatedUnsafe();
						return;
					}
					workAvailable.awaitUninterruptibly();
				}
				active.put(task.profile(), active.get(task.profile()) + 1);
			} finally {
				lock.unlock();
			}
			var metrics = metrics(task.profile(), task.family());
			if (metrics != null) {
				metrics.queueWait().record(System.nanoTime() - task.enqueuedNanos(), TimeUnit.NANOSECONDS);
				metrics.quantum().increment();
			}
			long executionStart = System.nanoTime();
			try {
				EXECUTING.set(true);
				task.command().run();
			} catch (Throwable error) {
				if (error instanceof VirtualMachineError virtualMachineError) {
					throw virtualMachineError;
				}
			} finally {
				EXECUTING.remove();
				if (metrics != null) {
					metrics.execution().record(System.nanoTime() - executionStart, TimeUnit.NANOSECONDS);
				}
				lock.lock();
				try {
					active.put(task.profile(), active.get(task.profile()) - 1);
					workAvailable.signalAll();
				} finally {
					lock.unlock();
				}
			}
		}
	}

	static boolean isExecutingTask() {
		return EXECUTING.get();
	}

	private WorkloadTask selectUnsafe() {
		purgeDisposedUnsafe();
		var latency = latencyQueues.get(WorkloadProfile.LATENCY);
		boolean latencyEligible = latency != null && !latency.isEmpty() && belowActiveLimit(WorkloadProfile.LATENCY);
		boolean guaranteedEligible = hasGuaranteedEligibleUnsafe();
		if (latencyEligible && (latencyBurst < MAX_LATENCY_BURST || !guaranteedEligible)) {
			latencyBurst++;
			return latency.poll();
		}
		var guaranteed = selectGuaranteedUnsafe();
		if (guaranteed != null) {
			latencyBurst = 0;
			return guaranteed;
		}
		if (latencyEligible) {
			latencyBurst++;
			return latency.poll();
		}
		for (var profile : List.of(WorkloadProfile.CONTROL, WorkloadProfile.PHYSICAL_MAINTENANCE)) {
			var queue = queues.get(profile);
			if (queue != null && !queue.isEmpty() && belowActiveLimit(profile)) {
				return queue.poll();
			}
		}
		return null;
	}

	private boolean hasGuaranteedEligibleUnsafe() {
		for (var profile : GUARANTEED) {
			var queue = queues.get(profile);
			if (queue != null && !queue.isEmpty() && belowActiveLimit(profile)) {
				return true;
			}
		}
		return false;
	}

	private WorkloadTask selectGuaranteedUnsafe() {
		for (int attempts = 0; attempts < GUARANTEED.size() * 2; attempts++) {
			var profile = GUARANTEED.get(guaranteedCursor);
			var queue = queues.get(profile);
			if (queue == null || queue.isEmpty() || !belowActiveLimit(profile)) {
				deficit.put(profile, 0);
				advanceGuaranteedCursor();
				continue;
			}
			int available = deficit.get(profile);
			if (available == 0) {
				available = QUANTA.get(profile);
			}
			var task = queue.poll();
			available--;
			deficit.put(profile, available);
			if (available == 0 || queue.isEmpty()) {
				advanceGuaranteedCursor();
			}
			return task;
		}
		return null;
	}

	private void advanceGuaranteedCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.size();
	}

	private boolean belowActiveLimit(WorkloadProfile profile) {
		if (profile == WorkloadProfile.ANALYTICAL && active.get(profile) >= analyticalLimit) {
			return false;
		}
		if (profile == WorkloadProfile.CDC || capacities.get(WorkloadProfile.CDC) == 0) {
			return true;
		}
		int activeNonCdc = 0;
		for (var entry : active.entrySet()) {
			if (entry.getKey() != WorkloadProfile.CDC) {
				activeNonCdc += entry.getValue();
			}
		}
		return activeNonCdc < sharedWorkerLimit;
	}

	boolean remove(Executor view, Runnable command) {
		if (!(view instanceof WorkloadExecutorView workloadView) || workloadView.owner() != this) {
			return false;
		}
		return remove(workloadView.profile(), workloadView.family(), command);
	}

	boolean remove(WorkloadProfile profile, OperationFamily family, Runnable command) {
		lock.lock();
		try {
			var iterator = queueUnsafe(profile).iterator();
			while (iterator.hasNext()) {
				var task = iterator.next();
				if (task.family() == family && task.command().equals(command)) {
					iterator.remove();
					var metrics = metrics(profile, family);
					if (metrics != null) {
						metrics.cancellation().increment();
					}
					return true;
				}
			}
			return false;
		} finally {
			lock.unlock();
		}
	}

	int queued(WorkloadProfile profile) {
		lock.lock();
		try {
			purgeDisposedUnsafe(profile);
			return queuedUnsafe(profile);
		} finally {
			lock.unlock();
		}
	}

	int active(WorkloadProfile profile) {
		lock.lock();
		try {
			return active.get(profile);
		} finally {
			lock.unlock();
		}
	}

	int capacity(WorkloadProfile profile) {
		return capacities.get(profile);
	}

	int workerCount() {
		return workerCount;
	}

	private int queuedUnsafe(WorkloadProfile profile) {
		return queueUnsafe(profile).size();
	}

	private void purgeDisposedUnsafe() {
		for (var profile : WorkloadProfile.values()) {
			purgeDisposedUnsafe(profile);
		}
	}

	private void purgeDisposedUnsafe(WorkloadProfile profile) {
		var iterator = queueUnsafe(profile).iterator();
		while (iterator.hasNext()) {
			var task = iterator.next();
			if (task.command() instanceof Disposable disposable && disposable.isDisposed()) {
				iterator.remove();
				var taskMetrics = metrics(task.profile(), task.family());
				if (taskMetrics != null) {
					taskMetrics.cancellation().increment();
				}
			}
		}
	}

	private int queuedTotalUnsafe() {
		int total = 0;
		for (var profile : WorkloadProfile.values()) {
			total += queuedUnsafe(profile);
		}
		return total;
	}

	private java.util.Queue<WorkloadTask> queueUnsafe(WorkloadProfile profile) {
		return profile == WorkloadProfile.LATENCY ? latencyQueues.get(profile) : queues.get(profile);
	}

	private void rejection(WorkloadProfile profile, OperationFamily family, String reason) {
		if (registry != null) {
			registry.counter("rockserver.workload.rejections",
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(profile),
					"operation", metricName(family),
					"reason", reason).increment();
		}
	}

	private @Nullable TaskMetrics metrics(WorkloadProfile profile, OperationFamily family) {
		if (registry == null) {
			return null;
		}
		return new TaskMetrics(
				registry.timer("rockserver.workload.queue.wait",
						"database", databaseName,
						"resource", resourceKind,
						"profile", metricName(profile),
						"operation", metricName(family)),
				registry.timer("rockserver.workload.execution",
						"database", databaseName,
						"resource", resourceKind,
						"profile", metricName(profile),
						"operation", metricName(family)),
				registry.counter("rockserver.workload.cancellations",
						"database", databaseName,
						"resource", resourceKind,
						"profile", metricName(profile),
						"operation", metricName(family)),
				registry.counter("rockserver.workload.quantums",
						"database", databaseName,
						"resource", resourceKind,
						"profile", metricName(profile),
						"operation", metricName(family)));
	}

	private void registerGauges() {
		if (registry == null) {
			return;
		}
		for (var profile : WorkloadProfile.values()) {
			Gauge.builder("rockserver.workload.queued", this, pool -> pool.queued(profile))
					.tag("database", databaseName)
					.tag("resource", resourceKind)
					.tag("profile", metricName(profile))
					.register(registry);
			Gauge.builder("rockserver.workload.active", this, pool -> pool.active(profile))
					.tag("database", databaseName)
					.tag("resource", resourceKind)
					.tag("profile", metricName(profile))
					.register(registry);
		}
	}

	private static String metricName(Enum<?> value) {
		return value.name().toLowerCase(java.util.Locale.ROOT);
	}

	@Override
	public void execute(Runnable command) {
		throw new UnsupportedOperationException("Use a workload-profile view");
	}

	@Override
	public void shutdown() {
		lock.lock();
		try {
			shutdown = true;
			if (startedWorkers == 0) {
				terminated = true;
			}
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
	}

	@Override
	public List<Runnable> shutdownNow() {
		lock.lock();
		try {
			shutdown = true;
			var remaining = new ArrayList<Runnable>();
			for (var profile : WorkloadProfile.values()) {
				var queue = queueUnsafe(profile);
				while (!queue.isEmpty()) {
					remaining.add(queue.remove().command());
				}
			}
			for (var worker : workers) {
				worker.interrupt();
			}
			workAvailable.signalAll();
			return remaining;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public boolean isShutdown() {
		return shutdown;
	}

	@Override
	public boolean isTerminated() {
		return terminated;
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		long deadline = System.nanoTime() + unit.toNanos(timeout);
		for (var worker : List.copyOf(workers)) {
			long remaining = deadline - System.nanoTime();
			if (remaining <= 0L) {
				return false;
			}
			worker.join(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
		}
		return terminated;
	}

	private void workerTerminatedUnsafe() {
		startedWorkers--;
		if (startedWorkers == 0) {
			terminated = true;
			workAvailable.signalAll();
		}
	}

	record WorkloadExecutorView(ProfiledWorkloadExecutor owner,
			WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis) implements Executor {

		WorkloadExecutorView {
			Objects.requireNonNull(owner, "owner");
			Objects.requireNonNull(profile, "profile");
			Objects.requireNonNull(family, "family");
		}

		@Override
		public void execute(Runnable command) {
			owner.execute(profile, family, deadlineEpochMillis, command);
		}
	}

	private record WorkloadTask(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis,
			long sequence,
			long enqueuedNanos,
			Runnable command) {
	}

	private record TaskMetrics(Timer queueWait,
			Timer execution,
			Counter cancellation,
			Counter quantum) {
	}
}
