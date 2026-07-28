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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

/**
 * One hard-bounded worker pool with independent workload queues. LATENCY uses EDF;
 * guaranteed-progress profiles use byte-cost deficit round-robin.
 */
final class ProfiledWorkloadExecutor extends AbstractExecutorService {

	static final int MAX_LATENCY_BURST = 8;
	static final long COST_BYTES = 2L * 1024L * 1024L;
	static final int MAX_TASK_COST = 16;
	private static final int MAX_DEFICIT = MAX_TASK_COST;
	private static final Logger LOG = LoggerFactory.getLogger(ProfiledWorkloadExecutor.class);
	private static final ThreadLocal<ProfiledWorkloadExecutor> EXECUTING_POOL = new ThreadLocal<>();
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
	private final int workerCount;
	private final int analyticalLimit;
	private final EnumMap<WorkloadProfile, Integer> capacities;
	private final EnumMap<WorkloadProfile, Integer> reservations;
	private final EnumMap<WorkloadProfile, PriorityQueue<WorkloadTask>> latencyQueues;
	private final EnumMap<WorkloadProfile, ArrayDeque<WorkloadTask>> queues;
	private final EnumMap<WorkloadProfile, Integer> active = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<WorkloadProfile, Integer> deficit = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<RWScheduler.TerminalOutcome, Long> outcomes =
			new EnumMap<>(RWScheduler.TerminalOutcome.class);
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition workAvailable = lock.newCondition();
	private final List<Thread> workers = new ArrayList<>();
	private final AtomicLong sequence = new AtomicLong();
	private final WorkloadPressureController pressureController;
	private final @Nullable MeterRegistry registry;
	private final String databaseName;
	private final String resourceKind;
	private final ThreadFactory threadFactory;
	private volatile boolean shutdown;
	private volatile boolean terminated;
	private int startedWorkers;
	private int latencyBurst;
	private int guaranteedCursor;
	private boolean guaranteedNeedsQuantum = true;
	private long acceptedTasks;
	private long startedTasks;
	private long completedTasks;
	private long failedTasks;

	ProfiledWorkloadExecutor(int workerCount,
			int analyticalLimit,
			Map<WorkloadProfile, Integer> capacities,
			Map<WorkloadProfile, Integer> reservations,
			String poolName,
			String resourceKind,
			WorkloadPressureController pressureController,
			@Nullable MeterRegistry registry,
			String databaseName) {
		if (workerCount < 1) {
			throw new IllegalArgumentException("workerCount must be positive");
		}
		if (analyticalLimit < 1 || analyticalLimit > workerCount) {
			throw new IllegalArgumentException("analyticalLimit must be between one and workerCount");
		}
		this.poolName = Objects.requireNonNull(poolName, "poolName");
		this.workerCount = workerCount;
		this.analyticalLimit = analyticalLimit;
		this.capacities = new EnumMap<>(WorkloadProfile.class);
		this.reservations = new EnumMap<>(WorkloadProfile.class);
		this.latencyQueues = new EnumMap<>(WorkloadProfile.class);
		this.queues = new EnumMap<>(WorkloadProfile.class);
		this.pressureController = Objects.requireNonNull(pressureController, "pressureController");
		this.registry = registry;
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.resourceKind = Objects.requireNonNull(resourceKind, "resourceKind");
		int reservationTotal = 0;
		for (var profile : WorkloadProfile.values()) {
			int capacity = capacities.getOrDefault(profile, 0);
			int reservation = reservations.getOrDefault(profile, 0);
			if (capacity < 0) {
				throw new IllegalArgumentException("Negative queue capacity for " + profile);
			}
			if (reservation < 0 || reservation > workerCount || (reservation > 0 && capacity == 0)) {
				throw new IllegalArgumentException("Invalid reservation for " + profile);
			}
			reservationTotal = Math.addExact(reservationTotal, reservation);
			this.capacities.put(profile, capacity);
			this.reservations.put(profile, reservation);
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
		if (reservationTotal > workerCount) {
			throw new IllegalArgumentException("Reservation sum exceeds workerCount");
		}
		for (var outcome : RWScheduler.TerminalOutcome.values()) {
			outcomes.put(outcome, 0L);
		}
		this.threadFactory = new ThreadFactoryBuilder()
				.setDaemon(false)
				.setNameFormat(poolName + "-%d")
				.setUncaughtExceptionHandler(this::uncaughtWorkerFailure)
				.build();
		registerGauges();
	}

	RWScheduler.WorkloadExecutor view(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		return new WorkloadExecutorView(this, profile, family, deadlineEpochMillis);
	}

	void execute(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis,
			long estimatedBytes,
			Runnable command) {
		Objects.requireNonNull(command, "command");
		int cost = taskCost(estimatedBytes);
		var terminalActions = new ArrayList<TerminalAction>();
		RuntimeException admissionFailure = null;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			purgeTerminalUnsafe(profile, nowMillis, terminalActions);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowMillis >= deadlineEpochMillis) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.DEADLINE,
						"deadline",
						command,
						admissionFailure,
						terminalActions);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.SHUTDOWN,
						"shutdown",
						command,
						admissionFailure,
						terminalActions);
			} else if (capacities.get(profile) == 0 || queuedUnsafe(profile) >= capacities.get(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.OVERLOAD,
						"queue_full",
						command,
						admissionFailure,
						terminalActions);
			} else {
				var task = new WorkloadTask(profile,
						family,
						deadlineEpochMillis,
						sequence.getAndIncrement(),
						System.nanoTime(),
						cost,
						command);
				queueUnsafe(profile).add(task);
				acceptedTasks++;
				ensureWorkersStartedUnsafe();
				workAvailable.signalAll();
			}
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		if (admissionFailure != null) {
			throw admissionFailure;
		}
	}

	static int taskCost(long estimatedBytes) {
		if (estimatedBytes < 0L) {
			throw new IllegalArgumentException("estimatedBytes must not be negative");
		}
		if (estimatedBytes == 0L) {
			return 1;
		}
		long cost = 1L + (estimatedBytes - 1L) / COST_BYTES;
		return (int) Math.max(1L, Math.min(MAX_TASK_COST, cost));
	}

	private void recordUnacceptedUnsafe(WorkloadProfile profile,
			OperationFamily family,
			RWScheduler.TerminalOutcome outcome,
			String reason,
			Runnable command,
			RuntimeException failure,
			List<TerminalAction> terminalActions) {
		recordOutcomeUnsafe(outcome);
		terminalActions.add(new TerminalAction(command, profile, family, failure, outcome, reason));
	}

	private void ensureWorkersStartedUnsafe() {
		while (startedWorkers < workerCount) {
			var worker = threadFactory.newThread(this::workerLoop);
			startedWorkers++;
			workers.add(worker);
			worker.start();
		}
	}

	private void workerLoop() {
		try {
			while (true) {
				WorkloadTask task = null;
				var terminalActions = new ArrayList<TerminalAction>();
				lock.lock();
				try {
					purgeTerminalUnsafe(System.currentTimeMillis(), terminalActions);
					if (terminalActions.isEmpty()) {
						task = selectUnsafe();
						if (task != null) {
							active.put(task.profile(), active.get(task.profile()) + 1);
						} else if (shutdown && queuedTotalUnsafe() == 0) {
							return;
						} else {
							awaitWorkUnsafe();
						}
					}
				} finally {
					lock.unlock();
				}
				completeTerminalActions(terminalActions);
				if (task != null) {
					runSelected(task);
				}
			}
		} finally {
			lock.lock();
			try {
				workerTerminatedUnsafe();
			} finally {
				lock.unlock();
			}
		}
	}

	private void awaitWorkUnsafe() {
		long waitNanos = Math.min(pressureWaitNanosUnsafe(), deadlineWaitNanosUnsafe());
		try {
			if (waitNanos == Long.MAX_VALUE) {
				workAvailable.await();
			} else if (waitNanos > 0L) {
				workAvailable.awaitNanos(waitNanos);
			}
		} catch (InterruptedException interrupted) {
			// shutdownNow interrupts workers to break waits. A stray interrupt is
			// consumed so it cannot turn the worker into a busy loop.
		}
	}

	private long pressureWaitNanosUnsafe() {
		if (shutdown || queueUnsafe(WorkloadProfile.BATCH).isEmpty()) {
			return Long.MAX_VALUE;
		}
		return pressureController.nanosUntilBatchEligible(System.nanoTime());
	}

	private long deadlineWaitNanosUnsafe() {
		long earliestDeadlineMillis = RequestContext.NO_DEADLINE;
		for (var profile : WorkloadProfile.values()) {
			for (var task : queueUnsafe(profile)) {
				earliestDeadlineMillis = Math.min(earliestDeadlineMillis, task.deadlineEpochMillis());
			}
		}
		if (earliestDeadlineMillis == RequestContext.NO_DEADLINE) {
			return Long.MAX_VALUE;
		}
		long remainingMillis = earliestDeadlineMillis - System.currentTimeMillis();
		return remainingMillis <= 0L ? 0L : TimeUnit.MILLISECONDS.toNanos(remainingMillis);
	}

	private void runSelected(WorkloadTask task) {
		var metrics = metrics(task.profile(), task.family());
		var terminalActions = new ArrayList<TerminalAction>(1);
		boolean run;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			if (isCancelled(task.command())) {
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						"cancellation",
						new CancellationException("Workload submission cancelled before execution"),
						terminalActions);
				run = false;
			} else if (task.deadlineEpochMillis() != RequestContext.NO_DEADLINE
					&& nowMillis >= task.deadlineEpochMillis()) {
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.DEADLINE,
						"deadline",
						deadlineFailure("Workload deadline expired immediately before execution"),
						terminalActions);
				run = false;
			} else {
				run = terminateUnsafe(task, RWScheduler.TerminalOutcome.RUN, "run", null, terminalActions);
				if (run) {
					startedTasks++;
				}
			}
		} finally {
			lock.unlock();
		}
		if (!run) {
			completeTerminalActions(terminalActions);
			finishActive(task, false);
			return;
		}

		long executionStart = System.nanoTime();
		var previousPool = EXECUTING_POOL.get();
		try {
			EXECUTING_POOL.set(this);
			task.command().run();
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable error) {
			recordTaskFailure(task, error);
		} finally {
			if (previousPool == null) {
				EXECUTING_POOL.remove();
			} else {
				EXECUTING_POOL.set(previousPool);
			}
			try {
				recordOutcomeMetric(task.profile(), task.family(), RWScheduler.TerminalOutcome.RUN, "run");
				if (metrics != null) {
					recordMetric("queue-wait timer", () -> metrics.queueWait()
							.record(executionStart - task.enqueuedNanos(), TimeUnit.NANOSECONDS));
					recordMetric("quantum counter", () -> metrics.quantum().increment());
					recordMetric("execution timer", () -> metrics.execution()
							.record(System.nanoTime() - executionStart, TimeUnit.NANOSECONDS));
				}
			} finally {
				finishActive(task, true);
			}
		}
	}

	private void finishActive(WorkloadTask task, boolean completed) {
		lock.lock();
		try {
			active.put(task.profile(), active.get(task.profile()) - 1);
			if (completed) {
				completedTasks++;
			}
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
		if (task.batchPermit() != null) {
			pressureController.finishBatch(Objects.requireNonNull(task.batchPermit()), completed);
		}
	}

	boolean isExecutingTask() {
		return EXECUTING_POOL.get() == this;
	}

	private WorkloadTask selectUnsafe() {
		if (activeTotalUnsafe() >= workerCount) {
			return null;
		}
		boolean reservedLatency = hasReservationDeficitUnsafe(WorkloadProfile.LATENCY);
		boolean reservedGuaranteed = hasGuaranteedReservationDeficitUnsafe();
		if (reservedLatency || reservedGuaranteed) {
			if (reservedLatency && (latencyBurst < MAX_LATENCY_BURST || !reservedGuaranteed)) {
				return selectLatencyUnsafe();
			}
			var guaranteed = selectGuaranteedUnsafe(true);
			if (guaranteed != null) {
				latencyBurst = 0;
				return guaranteed;
			}
			if (reservedLatency) {
				return selectLatencyUnsafe();
			}
		}

		boolean latencyEligible = isEligibleUnsafe(WorkloadProfile.LATENCY);
		boolean guaranteedEligible = hasGuaranteedEligibleUnsafe(false);
		if (latencyEligible && (latencyBurst < MAX_LATENCY_BURST || !guaranteedEligible)) {
			return selectLatencyUnsafe();
		}
		var guaranteed = selectGuaranteedUnsafe(false);
		if (guaranteed != null) {
			latencyBurst = 0;
			return guaranteed;
		}
		if (latencyEligible) {
			return selectLatencyUnsafe();
		}
		for (var profile : List.of(WorkloadProfile.CONTROL, WorkloadProfile.PHYSICAL_MAINTENANCE)) {
			if (isEligibleUnsafe(profile)) {
				return pollUnsafe(profile);
			}
		}
		return null;
	}

	private WorkloadTask selectLatencyUnsafe() {
		latencyBurst++;
		return pollUnsafe(WorkloadProfile.LATENCY);
	}

	private boolean hasGuaranteedReservationDeficitUnsafe() {
		for (var profile : GUARANTEED) {
			if (hasReservationDeficitUnsafe(profile) && isEligibleUnsafe(profile)) {
				return true;
			}
		}
		return false;
	}

	private boolean hasGuaranteedEligibleUnsafe(boolean reservationOnly) {
		for (var profile : GUARANTEED) {
			if ((!reservationOnly || hasReservationDeficitUnsafe(profile)) && isEligibleUnsafe(profile)) {
				return true;
			}
		}
		return false;
	}

	private WorkloadTask selectGuaranteedUnsafe(boolean reservationOnly) {
		int maxAttempts = GUARANTEED.size() * (MAX_TASK_COST + 1);
		for (int attempts = 0; attempts < maxAttempts; attempts++) {
			var profile = GUARANTEED.get(guaranteedCursor);
			var queue = queues.get(profile);
			if (queue == null || queue.isEmpty()) {
				deficit.put(profile, 0);
				advanceGuaranteedCursor();
				continue;
			}
			if ((reservationOnly && !hasReservationDeficitUnsafe(profile)) || !isEligibleUnsafe(profile)) {
				advanceGuaranteedCursor();
				continue;
			}
			if (guaranteedNeedsQuantum) {
				deficit.put(profile, Math.min(MAX_DEFICIT, deficit.get(profile) + QUANTA.get(profile)));
				guaranteedNeedsQuantum = false;
			}
			var head = queue.peek();
			if (deficit.get(profile) < head.cost()) {
				advanceGuaranteedCursor();
				continue;
			}
			if (profile == WorkloadProfile.BATCH) {
				var permit = pressureController.tryStartBatch(shutdown, System.nanoTime());
				if (permit == null) {
					advanceGuaranteedCursor();
					continue;
				}
				head.batchPermit(permit);
			}
			var task = queue.remove();
			deficit.put(profile, deficit.get(profile) - task.cost());
			if (queue.isEmpty()) {
				deficit.put(profile, 0);
				advanceGuaranteedCursor();
			} else if (deficit.get(profile) < queue.peek().cost()) {
				advanceGuaranteedCursor();
			}
			return task;
		}
		return null;
	}

	private void advanceGuaranteedCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.size();
		guaranteedNeedsQuantum = true;
	}

	private WorkloadTask pollUnsafe(WorkloadProfile profile) {
		return queueUnsafe(profile).remove();
	}

	private boolean hasReservationDeficitUnsafe(WorkloadProfile profile) {
		return reservations.get(profile) > active.get(profile) && !queueUnsafe(profile).isEmpty();
	}

	private boolean isEligibleUnsafe(WorkloadProfile profile) {
		if (queueUnsafe(profile).isEmpty() || activeTotalUnsafe() >= workerCount) {
			return false;
		}
		if (profile == WorkloadProfile.ANALYTICAL && active.get(profile) >= analyticalLimit) {
			return false;
		}
		if (!shutdown && profile == WorkloadProfile.PHYSICAL_MAINTENANCE && pressureController.isPressured()) {
			return false;
		}
		return profile != WorkloadProfile.BATCH
				|| pressureController.canStartBatch(shutdown, System.nanoTime());
	}

	boolean remove(Executor view, Runnable command) {
		if (!(view instanceof WorkloadExecutorView workloadView) || workloadView.owner() != this) {
			return false;
		}
		return remove(workloadView.profile(), workloadView.family(), command);
	}

	private boolean remove(WorkloadProfile profile, OperationFamily family, Runnable command) {
		var terminalActions = new ArrayList<TerminalAction>(1);
		boolean removed = false;
		lock.lock();
		try {
			var iterator = queueUnsafe(profile).iterator();
			while (iterator.hasNext()) {
				var task = iterator.next();
				if (task.family() == family && task.command() == command) {
					iterator.remove();
					removed = terminateUnsafe(task,
							RWScheduler.TerminalOutcome.CANCELLATION,
							"cancellation",
							new CancellationException("Workload submission cancelled while queued"),
							terminalActions);
					workAvailable.signalAll();
					break;
				}
			}
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		return removed;
	}

	int queued(WorkloadProfile profile) {
		var terminalActions = new ArrayList<TerminalAction>();
		int queued;
		lock.lock();
		try {
			purgeTerminalUnsafe(profile, System.currentTimeMillis(), terminalActions);
			queued = queuedUnsafe(profile);
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		return queued;
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

	ExecutorSnapshot snapshot() {
		var terminalActions = new ArrayList<TerminalAction>();
		ExecutorSnapshot snapshot;
		lock.lock();
		try {
			purgeTerminalUnsafe(System.currentTimeMillis(), terminalActions);
			var queuedByProfile = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
			for (var profile : WorkloadProfile.values()) {
				queuedByProfile.put(profile, queuedUnsafe(profile));
			}
			snapshot = new ExecutorSnapshot(
					workerCount,
					queuedTotalUnsafe(),
					activeTotalUnsafe(),
					acceptedTasks,
					startedTasks,
					completedTasks,
					failedTasks,
					Map.copyOf(queuedByProfile),
					Map.copyOf(active),
					Map.copyOf(outcomes),
					workers.stream().map(Thread::getName).toList(),
					shutdown,
					terminated);
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		return snapshot;
	}

	void signalAvailability() {
		lock.lock();
		try {
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
	}

	private int queuedUnsafe(WorkloadProfile profile) {
		return queueUnsafe(profile).size();
	}

	private void purgeTerminalUnsafe(long nowMillis, List<TerminalAction> terminalActions) {
		for (var profile : WorkloadProfile.values()) {
			purgeTerminalUnsafe(profile, nowMillis, terminalActions);
		}
	}

	private void purgeTerminalUnsafe(WorkloadProfile profile,
			long nowMillis,
			List<TerminalAction> terminalActions) {
		var iterator = queueUnsafe(profile).iterator();
		while (iterator.hasNext()) {
			var task = iterator.next();
			if (isCancelled(task.command())) {
				iterator.remove();
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						"cancellation",
						new CancellationException("Workload submission cancelled while queued"),
						terminalActions);
			} else if (task.deadlineEpochMillis() != RequestContext.NO_DEADLINE
					&& nowMillis >= task.deadlineEpochMillis()) {
				iterator.remove();
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.DEADLINE,
						"deadline",
						deadlineFailure("Workload deadline expired while queued"),
						terminalActions);
			}
		}
	}

	private boolean terminateUnsafe(WorkloadTask task,
			RWScheduler.TerminalOutcome outcome,
			String reason,
			@Nullable RuntimeException failure,
			List<TerminalAction> terminalActions) {
		if (task.outcome() != null) {
			return false;
		}
		task.outcome(outcome);
		recordOutcomeUnsafe(outcome);
		if (outcome != RWScheduler.TerminalOutcome.RUN) {
			terminalActions.add(new TerminalAction(task.command(),
					task.profile(),
					task.family(),
					Objects.requireNonNull(failure),
					outcome,
					reason));
		}
		return true;
	}

	private void recordOutcomeUnsafe(RWScheduler.TerminalOutcome outcome) {
		outcomes.put(outcome, outcomes.get(outcome) + 1L);
	}

	private void recordOutcomeMetric(WorkloadProfile profile,
			OperationFamily family,
			RWScheduler.TerminalOutcome outcome,
			String reason) {
		if (registry == null) {
			return;
		}
		recordMetric("terminal outcome counter", () -> registry.counter("rockserver.workload.outcomes",
				"database", databaseName,
				"resource", resourceKind,
				"profile", metricName(profile),
				"operation", metricName(family),
				"outcome", metricName(outcome)).increment());
		if (outcome == RWScheduler.TerminalOutcome.CANCELLATION) {
			recordMetric("cancellation counter", () -> registry.counter("rockserver.workload.cancellations",
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(profile),
					"operation", metricName(family)).increment());
		} else if (outcome == RWScheduler.TerminalOutcome.DEADLINE
				|| outcome == RWScheduler.TerminalOutcome.OVERLOAD
				|| outcome == RWScheduler.TerminalOutcome.SHUTDOWN) {
			recordMetric("rejection counter", () -> registry.counter("rockserver.workload.rejections",
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(profile),
					"operation", metricName(family),
					"reason", reason).increment());
		}
	}

	private static boolean isCancelled(Runnable command) {
		return command instanceof Future<?> future && future.isCancelled()
				|| command instanceof Disposable disposable && disposable.isDisposed();
	}

	private static RocksDBException deadlineFailure(String message) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, message);
	}

	private void completeTerminalActions(List<TerminalAction> terminalActions) {
		for (var action : terminalActions) {
			try {
				if (action.command() instanceof RWScheduler.RejectionAwareTask rejectionAwareTask) {
					rejectionAwareTask.reject(action.failure());
				} else if (action.command() instanceof CompletableFuture<?> future) {
					future.completeExceptionally(action.failure());
				} else if (action.command() instanceof Future<?> future) {
					future.cancel(false);
				}
			} catch (Throwable terminalFailure) {
				recordInfrastructureFailure("Failed to complete " + action.outcome()
						+ " workload submission in " + poolName, terminalFailure);
			}
			try {
				if (action.command() instanceof Disposable disposable && !disposable.isDisposed()) {
					disposable.dispose();
				}
			} catch (Throwable disposalFailure) {
				recordInfrastructureFailure("Failed to dispose " + action.outcome()
						+ " workload submission in " + poolName, disposalFailure);
			}
			recordOutcomeMetric(action.profile(), action.family(), action.outcome(), action.reason());
		}
	}

	private int queuedTotalUnsafe() {
		int total = 0;
		for (var profile : WorkloadProfile.values()) {
			total += queuedUnsafe(profile);
		}
		return total;
	}

	private int activeTotalUnsafe() {
		int total = 0;
		for (var count : active.values()) {
			total += count;
		}
		return total;
	}

	private java.util.Queue<WorkloadTask> queueUnsafe(WorkloadProfile profile) {
		return profile == WorkloadProfile.LATENCY ? latencyQueues.get(profile) : queues.get(profile);
	}

	private @Nullable TaskMetrics metrics(WorkloadProfile profile, OperationFamily family) {
		if (registry == null) {
			return null;
		}
		try {
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
					registry.counter("rockserver.workload.quantums",
							"database", databaseName,
							"resource", resourceKind,
							"profile", metricName(profile),
							"operation", metricName(family)));
		} catch (RuntimeException metricFailure) {
			recordMetricFailure("task meters", metricFailure);
			return null;
		}
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

	private void recordTaskFailure(WorkloadTask task, Throwable failure) {
		lock.lock();
		try {
			failedTasks++;
		} finally {
			lock.unlock();
		}
		if (registry != null) {
			recordMetric("task failure counter", () -> registry.counter("rockserver.workload.failures",
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(task.profile()),
					"operation", metricName(task.family())).increment());
		}
		LOG.error("Workload task failed: pool={}, profile={}, operation={}",
				poolName,
				task.profile(),
				task.family(),
				failure);
	}

	private void uncaughtWorkerFailure(Thread worker, Throwable failure) {
		recordInfrastructureFailure("Uncaught workload worker failure: pool=" + poolName
				+ ", thread=" + worker.getName(), failure);
	}

	private void recordInfrastructureFailure(String message, Throwable failure) {
		lock.lock();
		try {
			failedTasks++;
		} finally {
			lock.unlock();
		}
		if (registry != null) {
			recordMetric("worker failure counter", () -> registry.counter("rockserver.workload.worker.failures",
					"database", databaseName,
					"resource", resourceKind).increment());
		}
		LOG.error(message, failure);
	}

	private void recordMetric(String meterDescription, Runnable recorder) {
		try {
			recorder.run();
		} catch (RuntimeException metricFailure) {
			recordMetricFailure(meterDescription, metricFailure);
		}
	}

	private void recordMetricFailure(String meterDescription, Throwable failure) {
		LOG.error("Failed to record workload metric: pool={}, meter={}", poolName, meterDescription, failure);
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
		var terminalActions = new ArrayList<TerminalAction>();
		var remaining = new ArrayList<Runnable>();
		lock.lock();
		try {
			shutdown = true;
			for (var profile : WorkloadProfile.values()) {
				var queue = queueUnsafe(profile);
				while (!queue.isEmpty()) {
					var task = queue.remove();
					remaining.add(task.command());
					terminateUnsafe(task,
							RWScheduler.TerminalOutcome.SHUTDOWN,
							"shutdown",
							new RejectedExecutionException(poolName + " was forced to shut down"),
							terminalActions);
				}
			}
			for (var worker : workers) {
				worker.interrupt();
			}
			if (startedWorkers == 0) {
				terminated = true;
			}
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		return remaining;
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
		if (startedWorkers == 0 && shutdown) {
			terminated = true;
			workAvailable.signalAll();
		}
	}

	private record WorkloadExecutorView(ProfiledWorkloadExecutor owner,
			WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis) implements RWScheduler.WorkloadExecutor {

		private WorkloadExecutorView {
			Objects.requireNonNull(owner, "owner");
			Objects.requireNonNull(profile, "profile");
			Objects.requireNonNull(family, "family");
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
			owner.execute(profile, family, deadlineEpochMillis, estimatedBytes, command);
		}
	}

	private static final class WorkloadTask {

		private final WorkloadProfile profile;
		private final OperationFamily family;
		private final long deadlineEpochMillis;
		private final long sequence;
		private final long enqueuedNanos;
		private final int cost;
		private final Runnable command;
		private @Nullable WorkloadPressureController.BatchPermit batchPermit;
		private @Nullable RWScheduler.TerminalOutcome outcome;

		private WorkloadTask(WorkloadProfile profile,
				OperationFamily family,
				long deadlineEpochMillis,
				long sequence,
				long enqueuedNanos,
				int cost,
				Runnable command) {
			this.profile = profile;
			this.family = family;
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.sequence = sequence;
			this.enqueuedNanos = enqueuedNanos;
			this.cost = cost;
			this.command = command;
		}

		private WorkloadProfile profile() {
			return profile;
		}

		private OperationFamily family() {
			return family;
		}

		private long deadlineEpochMillis() {
			return deadlineEpochMillis;
		}

		private long sequence() {
			return sequence;
		}

		private long enqueuedNanos() {
			return enqueuedNanos;
		}

		private int cost() {
			return cost;
		}

		private Runnable command() {
			return command;
		}

		private @Nullable WorkloadPressureController.BatchPermit batchPermit() {
			return batchPermit;
		}

		private void batchPermit(WorkloadPressureController.BatchPermit batchPermit) {
			this.batchPermit = batchPermit;
		}

		private @Nullable RWScheduler.TerminalOutcome outcome() {
			return outcome;
		}

		private void outcome(RWScheduler.TerminalOutcome outcome) {
			this.outcome = outcome;
		}
	}

	private record TerminalAction(Runnable command,
			WorkloadProfile profile,
			OperationFamily family,
			RuntimeException failure,
			RWScheduler.TerminalOutcome outcome,
			String reason) {
	}

	private record TaskMetrics(Timer queueWait, Timer execution, Counter quantum) {
	}
}

final class WorkloadPressureController {

	private static final long BATCH_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1);
	private boolean pressured;
	private int activeBatches;
	private boolean completedBatchAwaitingIdle;
	private long nextBatchNanos = Long.MIN_VALUE;
	private volatile Runnable notifier = () -> {};

	synchronized boolean isPressured() {
		return pressured;
	}

	void setNotifier(Runnable notifier) {
		this.notifier = Objects.requireNonNull(notifier, "notifier");
	}

	void setPressured(boolean pressured) {
		synchronized (this) {
			this.pressured = pressured;
			if (!pressured) {
				nextBatchNanos = Long.MIN_VALUE;
				completedBatchAwaitingIdle = false;
			}
		}
		notifier.run();
	}

	synchronized boolean canStartBatch(boolean ignorePressure, long nowNanos) {
		return ignorePressure || !pressured || activeBatches == 0 && nowNanos >= nextBatchNanos;
	}

	synchronized @Nullable BatchPermit tryStartBatch(boolean ignorePressure, long nowNanos) {
		if (!canStartBatch(ignorePressure, nowNanos)) {
			return null;
		}
		boolean startedUnderPressure = pressured && !ignorePressure;
		activeBatches++;
		return new BatchPermit(startedUnderPressure);
	}

	void finishBatch(BatchPermit permit, boolean ran) {
		synchronized (this) {
			if (activeBatches <= 0) {
				throw new IllegalStateException("No active BATCH quantum to finish");
			}
			if (ran && permit.startedUnderPressure() && pressured) {
				completedBatchAwaitingIdle = true;
			}
			activeBatches--;
			if (activeBatches == 0) {
				if (pressured && completedBatchAwaitingIdle) {
					nextBatchNanos = System.nanoTime() + BATCH_INTERVAL_NANOS;
				}
				completedBatchAwaitingIdle = false;
			}
		}
		notifier.run();
	}

	synchronized long nanosUntilBatchEligible(long nowNanos) {
		if (!pressured) {
			return 0L;
		}
		if (activeBatches > 0) {
			return Long.MAX_VALUE;
		}
		return Math.max(0L, nextBatchNanos - nowNanos);
	}

	record BatchPermit(boolean startedUnderPressure) {
	}
}

record ExecutorSnapshot(int workerCount,
		int queuedTasks,
		int activeTasks,
		long acceptedTasks,
		long startedTasks,
		long completedTasks,
		long failedTasks,
		Map<WorkloadProfile, Integer> queuedByProfile,
		Map<WorkloadProfile, Integer> activeByProfile,
		Map<RWScheduler.TerminalOutcome, Long> outcomes,
		List<String> workerThreadNames,
		boolean shutdown,
		boolean terminated) {
}
