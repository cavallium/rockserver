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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
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
import java.util.function.ToDoubleFunction;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

/**
 * One hard-bounded worker pool with independent workload queues. LATENCY uses EDF;
 * guaranteed-progress profiles use byte-cost deficit round-robin.
 */
final class ProfiledWorkloadExecutor extends AbstractExecutorService {

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
	private static final Comparator<WorkloadTask> DEADLINE_ORDER = Comparator
			.comparingLong(WorkloadTask::deadlineEpochMillis)
			.thenComparingLong(WorkloadTask::sequence);
	private static final CounterHandle INERT_COUNTER = () -> {};
	private static final TimerHandle INERT_TIMER = _ -> {};
	private static final TaskMetrics INERT_TASK_METRICS = TaskMetrics.inert();
	private final String poolName;
	private final int workerCount;
	private final int analyticalLimit;
	private final int maxLatencyBurst;
	private final EnumMap<WorkloadProfile, Integer> quanta;
	private final EnumMap<WorkloadProfile, Integer> capacities;
	private final EnumMap<WorkloadProfile, Integer> reservations;
	private final NavigableSet<WorkloadTask> latencyQueue = new TreeSet<>(DEADLINE_ORDER);
	private final EnumMap<WorkloadProfile, LinkedHashSet<WorkloadTask>> queues;
	private final NavigableSet<WorkloadTask> deadlineQueue = new TreeSet<>(DEADLINE_ORDER);
	private final Map<CancellationKey, CancellationChain> cancellationIndex = new HashMap<>();
	private final EnumMap<WorkloadProfile, Integer> queued = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<WorkloadProfile, Integer> active = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<WorkloadProfile, Integer> deficit = new EnumMap<>(WorkloadProfile.class);
	private final EnumMap<RWScheduler.TerminalOutcome, Long> outcomes =
			new EnumMap<>(RWScheduler.TerminalOutcome.class);
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition workAvailable = lock.newCondition();
	private final List<Thread> workers = new ArrayList<>();
	private final AtomicLong sequence = new AtomicLong();
	private final WorkloadPressureController pressureController;
	private final String databaseName;
	private final String resourceKind;
	private final ThreadFactory threadFactory;
	private final EnumMap<WorkloadProfile, EnumMap<OperationFamily, TaskMetrics>> taskMetrics;
	private final CounterHandle workerFailureMetric;
	private volatile boolean shutdown;
	private volatile boolean terminated;
	private @Nullable Thread timedWaitLeader;
	private int startedWorkers;
	private int queuedTotal;
	private int activeTotal;
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
	                         int maxLatencyBurst,
	                         Map<WorkloadProfile, Integer> quanta,
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
		if (maxLatencyBurst < 1) {
			throw new IllegalArgumentException("maxLatencyBurst must be positive");
		}
		this.poolName = Objects.requireNonNull(poolName, "poolName");
		this.workerCount = workerCount;
		this.analyticalLimit = analyticalLimit;
		this.maxLatencyBurst = maxLatencyBurst;
		this.quanta = new EnumMap<>(WorkloadProfile.class);
		for (var profile : GUARANTEED) {
			int quantum = Objects.requireNonNull(quanta.get(profile), "Missing DRR weight for " + profile);
			if (quantum < 1) {
				throw new IllegalArgumentException("DRR weight must be positive for " + profile);
			}
			this.quanta.put(profile, quantum);
		}
		this.capacities = new EnumMap<>(WorkloadProfile.class);
		this.reservations = new EnumMap<>(WorkloadProfile.class);
		this.queues = new EnumMap<>(WorkloadProfile.class);
		this.pressureController = Objects.requireNonNull(pressureController, "pressureController");
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
			this.queued.put(profile, 0);
			this.active.put(profile, 0);
			this.deficit.put(profile, 0);
			if (profile != WorkloadProfile.LATENCY) {
				this.queues.put(profile, new LinkedHashSet<>());
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
		this.taskMetrics = registerTaskMetrics(registry);
		this.workerFailureMetric = registerCounter(registry,
				"rockserver.workload.worker.failures",
				"database", databaseName,
				"resource", resourceKind);
		registerGauges(registry);
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
		AdmissionResult admissionResult;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			expireDueUnsafe(nowMillis, terminalActions);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowMillis >= deadlineEpochMillis) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				admissionResult = AdmissionResult.DEADLINE;
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.DEADLINE,
						command,
						admissionFailure,
						terminalActions);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionResult = AdmissionResult.SHUTDOWN;
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.SHUTDOWN,
						command,
						admissionFailure,
						terminalActions);
			} else if (capacities.get(profile) == 0 || queuedUnsafe(profile) >= capacities.get(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				recordUnacceptedUnsafe(profile,
						family,
						RWScheduler.TerminalOutcome.OVERLOAD,
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
				boolean becomesDeadlineHead = task.hasDeadline()
						&& (deadlineQueue.isEmpty() || DEADLINE_ORDER.compare(task, deadlineQueue.first()) < 0);
				enqueueUnsafe(task);
				acceptedTasks++;
				admissionResult = AdmissionResult.ACCEPTED;
				ensureWorkersStartedUnsafe();
				if (becomesDeadlineHead || profile == WorkloadProfile.BATCH) {
					timedWaitLeader = null;
				}
				workAvailable.signal();
			}
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		recordAdmissionMetric(profile, family, admissionResult);
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
	                                    Runnable command,
	                                    RuntimeException failure,
	                                    List<TerminalAction> terminalActions) {
		recordOutcomeUnsafe(outcome);
		terminalActions.add(new TerminalAction(command, profile, family, failure, outcome));
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
				WorkloadTask task;
				var terminalActions = new ArrayList<TerminalAction>();
				boolean stop = false;
				lock.lock();
				try {
					long nowMillis = System.currentTimeMillis();
					expireDueUnsafe(nowMillis, terminalActions);
					task = dispatchUnsafe(nowMillis, terminalActions);
					if (task == null && shutdown && queuedTotal == 0) {
						stop = true;
					} else if (task == null && terminalActions.isEmpty()) {
						awaitWorkUnsafe();
					}
				} finally {
					lock.unlock();
				}
				completeTerminalActions(terminalActions);
				if (stop) {
					return;
				}
				if (task != null) {
					runDispatched(task);
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
			if (waitNanos <= 0L) {
				return;
			}
			if (waitNanos == Long.MAX_VALUE || timedWaitLeader != null) {
				workAvailable.await();
			} else {
				var current = Thread.currentThread();
				timedWaitLeader = current;
				try {
					workAvailable.awaitNanos(waitNanos);
				} finally {
					if (timedWaitLeader == current) {
						timedWaitLeader = null;
					}
				}
			}
		} catch (InterruptedException interrupted) {
			// shutdownNow interrupts workers to break waits. A stray interrupt is
			// consumed so it cannot turn the worker into a busy loop.
		} finally {
			if (timedWaitLeader == null && queuedTotal > 0) {
				workAvailable.signal();
			}
		}
	}

	private long pressureWaitNanosUnsafe() {
		if (shutdown || queuedUnsafe(WorkloadProfile.BATCH) == 0) {
			return Long.MAX_VALUE;
		}
		return pressureController.nanosUntilBatchEligible(System.nanoTime());
	}

	private long deadlineWaitNanosUnsafe() {
		if (deadlineQueue.isEmpty()) {
			return Long.MAX_VALUE;
		}
		long earliestDeadlineMillis = deadlineQueue.first().deadlineEpochMillis();
		long remainingMillis = earliestDeadlineMillis - System.currentTimeMillis();
		return remainingMillis <= 0L ? 0L : TimeUnit.MILLISECONDS.toNanos(remainingMillis);
	}

	private @Nullable WorkloadTask dispatchUnsafe(long nowMillis,
	                                              List<TerminalAction> terminalActions) {
		while (queuedTotal > 0) {
			var task = selectCandidateUnsafe();
			if (task == null) {
				return null;
			}
			if (cancelSelectedUnsafe(task, terminalActions)) {
				continue;
			}
			if (task.hasDeadline() && nowMillis >= task.deadlineEpochMillis()) {
				unlinkUnsafe(task);
				discardSelectionUnsafe(task);
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.DEADLINE,
						deadlineFailure("Workload deadline expired immediately before execution"),
						terminalActions);
				continue;
			}
			// Re-read at the last pre-permit boundary so a cancellation racing
			// candidate validation cannot consume pressured BATCH pacing.
			if (cancelSelectedUnsafe(task, terminalActions)) {
				continue;
			}
			WorkloadPressureController.BatchPermit batchPermit = null;
			if (task.profile() == WorkloadProfile.BATCH) {
				batchPermit = pressureController.tryStartBatch(shutdown, System.nanoTime());
				if (batchPermit == null) {
					advanceGuaranteedCursor();
					return null;
				}
			}
			unlinkUnsafe(task);
			if (batchPermit != null) {
				task.batchPermit(batchPermit);
			}
			commitSelectionUnsafe(task);
			active.put(task.profile(), active.get(task.profile()) + 1);
			activeTotal++;
			if (!terminateUnsafe(task, RWScheduler.TerminalOutcome.RUN, null, terminalActions)) {
				throw new IllegalStateException("Queued workload task already has a terminal outcome");
			}
			startedTasks++;
			return task;
		}
		return null;
	}

	private boolean cancelSelectedUnsafe(WorkloadTask task,
	                                     List<TerminalAction> terminalActions) {
		if (!isCancelled(task.command())) {
			return false;
		}
		unlinkUnsafe(task);
		discardSelectionUnsafe(task);
		terminateUnsafe(task,
				RWScheduler.TerminalOutcome.CANCELLATION,
				new CancellationException("Workload submission cancelled before execution"),
				terminalActions);
		return true;
	}

	private void runDispatched(WorkloadTask task) {
		var metrics = metrics(task.profile(), task.family());
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
				recordOutcomeMetric(task.profile(), task.family(), RWScheduler.TerminalOutcome.RUN);
				metrics.queueWait().record(executionStart - task.enqueuedNanos());
				metrics.quantum().increment();
				metrics.execution().record(System.nanoTime() - executionStart);
			} finally {
				finishActive(task);
			}
		}
	}

	private void finishActive(WorkloadTask task) {
		lock.lock();
		try {
			active.put(task.profile(), active.get(task.profile()) - 1);
			activeTotal--;
			completedTasks++;
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		if (task.batchPermit() != null) {
			pressureController.finishBatch(Objects.requireNonNull(task.batchPermit()));
		}
	}

	boolean isExecutingTask() {
		return EXECUTING_POOL.get() == this;
	}

	private @Nullable WorkloadTask selectCandidateUnsafe() {
		if (activeTotal >= workerCount) {
			return null;
		}
		boolean reservedLatency = hasReservationDeficitUnsafe(WorkloadProfile.LATENCY);
		boolean reservedGuaranteed = hasGuaranteedReservationDeficitUnsafe();
		if (reservedLatency || reservedGuaranteed) {
			if (reservedLatency && (latencyBurst < maxLatencyBurst || !reservedGuaranteed)) {
				return peekUnsafe(WorkloadProfile.LATENCY);
			}
			var guaranteed = selectGuaranteedCandidateUnsafe(true);
			if (guaranteed != null) {
				return guaranteed;
			}
			if (reservedLatency) {
				return peekUnsafe(WorkloadProfile.LATENCY);
			}
		}

		boolean latencyEligible = isEligibleUnsafe(WorkloadProfile.LATENCY);
		boolean guaranteedEligible = hasGuaranteedEligibleUnsafe();
		if (latencyEligible && (latencyBurst < maxLatencyBurst || !guaranteedEligible)) {
			return peekUnsafe(WorkloadProfile.LATENCY);
		}
		var guaranteed = selectGuaranteedCandidateUnsafe(false);
		if (guaranteed != null) {
			return guaranteed;
		}
		if (latencyEligible) {
			return peekUnsafe(WorkloadProfile.LATENCY);
		}
		for (var profile : List.of(WorkloadProfile.CONTROL, WorkloadProfile.PHYSICAL_MAINTENANCE)) {
			if (isEligibleUnsafe(profile)) {
				return peekUnsafe(profile);
			}
		}
		return null;
	}

	private boolean hasGuaranteedReservationDeficitUnsafe() {
		for (var profile : GUARANTEED) {
			if (hasReservationDeficitUnsafe(profile) && isEligibleUnsafe(profile)) {
				return true;
			}
		}
		return false;
	}

	private boolean hasGuaranteedEligibleUnsafe() {
		for (var profile : GUARANTEED) {
			if (isEligibleUnsafe(profile)) {
				return true;
			}
		}
		return false;
	}

	private @Nullable WorkloadTask selectGuaranteedCandidateUnsafe(boolean reservationOnly) {
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
				deficit.put(profile, Math.min(MAX_DEFICIT, deficit.get(profile) + quanta.get(profile)));
				guaranteedNeedsQuantum = false;
			}
			var head = queue.getFirst();
			if (deficit.get(profile) < head.cost()) {
				advanceGuaranteedCursor();
				continue;
			}
			return head;
		}
		return null;
	}

	private void commitSelectionUnsafe(WorkloadTask task) {
		if (task.profile() == WorkloadProfile.LATENCY) {
			latencyBurst++;
			return;
		}
		if (!GUARANTEED.contains(task.profile())) {
			return;
		}
		latencyBurst = 0;
		deficit.put(task.profile(), deficit.get(task.profile()) - task.cost());
		var queue = queues.get(task.profile());
		if (queue.isEmpty()) {
			deficit.put(task.profile(), 0);
			advanceGuaranteedCursor();
		} else if (deficit.get(task.profile()) < queue.getFirst().cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void discardSelectionUnsafe(WorkloadTask task) {
		if (!GUARANTEED.contains(task.profile())) {
			return;
		}
		var queue = queues.get(task.profile());
		if (queue.isEmpty()) {
			deficit.put(task.profile(), 0);
			advanceGuaranteedCursor();
		} else if (deficit.get(task.profile()) < queue.getFirst().cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void advanceGuaranteedCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.size();
		guaranteedNeedsQuantum = true;
	}

	private boolean hasReservationDeficitUnsafe(WorkloadProfile profile) {
		return reservations.get(profile) > active.get(profile) && queuedUnsafe(profile) > 0;
	}

	private boolean isEligibleUnsafe(WorkloadProfile profile) {
		if (queuedUnsafe(profile) == 0 || activeTotal >= workerCount) {
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
			var chain = cancellationIndex.get(new CancellationKey(command, profile, family));
			if (chain != null) {
				var task = chain.first();
				boolean wasDeadlineHead = task.hasDeadline() && deadlineQueue.first() == task;
				unlinkUnsafe(task);
				removed = terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						new CancellationException("Workload submission cancelled while queued"),
						terminalActions);
				if (wasDeadlineHead || task.profile() == WorkloadProfile.BATCH) {
					timedWaitLeader = null;
				}
				workAvailable.signal();
			}
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
		return removed;
	}

	int queued(WorkloadProfile profile) {
		lock.lock();
		try {
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

	ExecutorSnapshot snapshot() {
		lock.lock();
		try {
			return new ExecutorSnapshot(
					workerCount,
					queuedTotal,
					activeTotal,
					acceptedTasks,
					startedTasks,
					completedTasks,
					failedTasks,
					Map.copyOf(queued),
					Map.copyOf(active),
					Map.copyOf(outcomes),
					workers.stream().map(Thread::getName).toList(),
					shutdown,
					terminated);
		} finally {
			lock.unlock();
		}
	}

	void signalAvailability() {
		lock.lock();
		try {
			timedWaitLeader = null;
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
	}

	private int queuedUnsafe(WorkloadProfile profile) {
		return queued.get(profile);
	}

	private void expireDueUnsafe(long nowMillis, List<TerminalAction> terminalActions) {
		while (!deadlineQueue.isEmpty()) {
			var task = deadlineQueue.first();
			if (nowMillis < task.deadlineEpochMillis()) {
				return;
			}
			unlinkUnsafe(task);
			terminateUnsafe(task,
					RWScheduler.TerminalOutcome.DEADLINE,
					deadlineFailure("Workload deadline expired while queued"),
					terminalActions);
		}
	}

	private void enqueueUnsafe(WorkloadTask task) {
		if (!queueUnsafe(task.profile()).add(task)) {
			throw new IllegalStateException("Duplicate workload task sequence " + task.sequence());
		}
		if (task.hasDeadline() && !deadlineQueue.add(task)) {
			throw new IllegalStateException("Duplicate workload deadline sequence " + task.sequence());
		}
		cancellationIndex.computeIfAbsent(task.cancellationKey(), ignored -> new CancellationChain())
				.addLast(task);
		queued.put(task.profile(), queued.get(task.profile()) + 1);
		queuedTotal++;
	}

	private void unlinkUnsafe(WorkloadTask task) {
		if (!queueUnsafe(task.profile()).remove(task)) {
			throw new IllegalStateException("Workload task is not queued: " + task.sequence());
		}
		if (task.hasDeadline() && !deadlineQueue.remove(task)) {
			throw new IllegalStateException("Finite-deadline workload task is not deadline-indexed: "
					+ task.sequence());
		}
		var chain = cancellationIndex.get(task.cancellationKey());
		if (chain == null) {
			throw new IllegalStateException("Workload task is not cancellation-indexed: " + task.sequence());
		}
		chain.unlink(task);
		if (chain.isEmpty()) {
			cancellationIndex.remove(task.cancellationKey());
		}
		queued.put(task.profile(), queued.get(task.profile()) - 1);
		queuedTotal--;
	}

	private boolean terminateUnsafe(WorkloadTask task,
	                                RWScheduler.TerminalOutcome outcome,
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
					outcome));
		}
		return true;
	}

	private void recordOutcomeUnsafe(RWScheduler.TerminalOutcome outcome) {
		outcomes.put(outcome, outcomes.get(outcome) + 1L);
	}

	private void recordOutcomeMetric(WorkloadProfile profile,
	                                 OperationFamily family,
	                                 RWScheduler.TerminalOutcome outcome) {
		metrics(profile, family).recordOutcome(outcome);
	}

	private void recordAdmissionMetric(WorkloadProfile profile,
	                                   OperationFamily family,
	                                   AdmissionResult result) {
		metrics(profile, family).recordAdmission(result);
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
			recordOutcomeMetric(action.profile(), action.family(), action.outcome());
		}
	}

	private Collection<WorkloadTask> queueUnsafe(WorkloadProfile profile) {
		return profile == WorkloadProfile.LATENCY ? latencyQueue : queues.get(profile);
	}

	private WorkloadTask peekUnsafe(WorkloadProfile profile) {
		if (profile == WorkloadProfile.LATENCY) {
			return latencyQueue.first();
		}
		return queues.get(profile).getFirst();
	}

	private TaskMetrics metrics(WorkloadProfile profile, OperationFamily family) {
		var byFamily = taskMetrics.get(profile);
		if (byFamily == null) {
			return INERT_TASK_METRICS;
		}
		return byFamily.getOrDefault(family, INERT_TASK_METRICS);
	}

	private EnumMap<WorkloadProfile, EnumMap<OperationFamily, TaskMetrics>> registerTaskMetrics(
			@Nullable MeterRegistry registry) {
		var result = new EnumMap<WorkloadProfile, EnumMap<OperationFamily, TaskMetrics>>(WorkloadProfile.class);
		for (var profile : WorkloadProfile.values()) {
			if (capacities.get(profile) == 0) {
				continue;
			}
			var byFamily = new EnumMap<OperationFamily, TaskMetrics>(OperationFamily.class);
			for (var family : OperationFamily.values()) {
				if (WorkloadAdmission.isAllowed(profile, family)) {
					byFamily.put(family, registerTaskMetrics(registry, profile, family));
				}
			}
			result.put(profile, byFamily);
		}
		return result;
	}

	private TaskMetrics registerTaskMetrics(@Nullable MeterRegistry registry,
	                                        WorkloadProfile profile,
	                                        OperationFamily family) {
		String[] tags = {
				"database", databaseName,
				"resource", resourceKind,
				"profile", metricName(profile),
				"operation", metricName(family)
		};
		var outcomeMetrics = new EnumMap<RWScheduler.TerminalOutcome, CounterHandle>(
				RWScheduler.TerminalOutcome.class);
		for (var outcome : RWScheduler.TerminalOutcome.values()) {
			outcomeMetrics.put(outcome, registerCounter(registry,
					"rockserver.workload.outcomes",
					tagsWith(tags, "outcome", metricName(outcome))));
		}
		var rejectionMetrics = new EnumMap<RWScheduler.TerminalOutcome, CounterHandle>(
				RWScheduler.TerminalOutcome.class);
		for (var outcome : List.of(RWScheduler.TerminalOutcome.DEADLINE,
				RWScheduler.TerminalOutcome.OVERLOAD,
				RWScheduler.TerminalOutcome.SHUTDOWN)) {
			rejectionMetrics.put(outcome, registerCounter(registry,
					"rockserver.workload.rejections",
					tagsWith(tags, "reason", rejectionReason(outcome))));
		}
		var admissionMetrics = new EnumMap<AdmissionResult, CounterHandle>(AdmissionResult.class);
		for (var result : AdmissionResult.values()) {
			admissionMetrics.put(result, registerCounter(registry,
					"rockserver.workload.admission",
					tagsWith(tags, "result", metricName(result))));
		}
		return new TaskMetrics(
				registerTimer(registry, "rockserver.workload.queue.wait", tags),
				registerTimer(registry, "rockserver.workload.execution", tags),
				registerCounter(registry, "rockserver.workload.quantums", tags),
				registerCounter(registry, "rockserver.workload.failures", tags),
				outcomeMetrics,
				registerCounter(registry, "rockserver.workload.cancellations", tags),
				rejectionMetrics,
				admissionMetrics);
	}

	private void registerGauges(@Nullable MeterRegistry registry) {
		if (registry == null) {
			return;
		}
		registerGauge(registry,
				"rockserver.workload.worker.limit",
				ProfiledWorkloadExecutor::workerCount,
				"database", databaseName,
				"resource", resourceKind);
		for (var profile : WorkloadProfile.values()) {
			String[] tags = {
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(profile)
			};
			registerGauge(registry, "rockserver.workload.queued", pool -> pool.queued(profile), tags);
			registerGauge(registry, "rockserver.workload.active", pool -> pool.active(profile), tags);
			registerGauge(registry,
					"rockserver.workload.queue.capacity",
					pool -> pool.capacity(profile),
					tags);
		}
	}

	private CounterHandle registerCounter(@Nullable MeterRegistry registry, String name, String... tags) {
		if (registry == null) {
			return INERT_COUNTER;
		}
		try {
			Counter counter = registry.counter(name, tags);
			return () -> recordMetric(name, counter::increment);
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable registrationFailure) {
			recordMetricFailure(name + " registration", registrationFailure);
			return INERT_COUNTER;
		}
	}

	private TimerHandle registerTimer(@Nullable MeterRegistry registry, String name, String... tags) {
		if (registry == null) {
			return INERT_TIMER;
		}
		try {
			Timer timer = registry.timer(name, tags);
			return nanos -> recordMetric(name, () -> timer.record(nanos, TimeUnit.NANOSECONDS));
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable registrationFailure) {
			recordMetricFailure(name + " registration", registrationFailure);
			return INERT_TIMER;
		}
	}

	private void registerGauge(MeterRegistry registry,
	                           String name,
	                           ToDoubleFunction<ProfiledWorkloadExecutor> value,
	                           String... tags) {
		try {
			Gauge.builder(name, this, value).tags(tags).register(registry);
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable registrationFailure) {
			recordMetricFailure(name + " registration", registrationFailure);
		}
	}

	private static String[] tagsWith(String[] tags, String key, String value) {
		var result = java.util.Arrays.copyOf(tags, tags.length + 2);
		result[tags.length] = key;
		result[tags.length + 1] = value;
		return result;
	}

	private static String rejectionReason(RWScheduler.TerminalOutcome outcome) {
		return switch (outcome) {
			case DEADLINE -> "deadline";
			case OVERLOAD -> "queue_full";
			case SHUTDOWN -> "shutdown";
			case RUN, CANCELLATION -> throw new IllegalArgumentException("Not a rejection outcome: " + outcome);
		};
	}

	private void recordTaskFailure(WorkloadTask task, Throwable failure) {
		lock.lock();
		try {
			failedTasks++;
		} finally {
			lock.unlock();
		}
		metrics(task.profile(), task.family()).failure().increment();
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
		workerFailureMetric.increment();
		LOG.error(message, failure);
	}

	private void recordMetric(String meterDescription, Runnable recorder) {
		try {
			recorder.run();
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable metricFailure) {
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
			timedWaitLeader = null;
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
			timedWaitLeader = null;
			for (var profile : WorkloadProfile.values()) {
				while (queuedUnsafe(profile) > 0) {
					var task = peekUnsafe(profile);
					unlinkUnsafe(task);
					remaining.add(task.command());
					terminateUnsafe(task,
							RWScheduler.TerminalOutcome.SHUTDOWN,
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
		private final CancellationKey cancellationKey;
		private @Nullable WorkloadTask previousCancellation;
		private @Nullable WorkloadTask nextCancellation;
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
			this.cancellationKey = new CancellationKey(command, profile, family);
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

		private boolean hasDeadline() {
			return deadlineEpochMillis != RequestContext.NO_DEADLINE;
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

		private CancellationKey cancellationKey() {
			return cancellationKey;
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

	private static final class CancellationKey {

		private final Runnable command;
		private final WorkloadProfile profile;
		private final OperationFamily family;
		private final int hashCode;

		private CancellationKey(Runnable command, WorkloadProfile profile, OperationFamily family) {
			this.command = Objects.requireNonNull(command, "command");
			this.profile = Objects.requireNonNull(profile, "profile");
			this.family = Objects.requireNonNull(family, "family");
			this.hashCode = 31 * (31 * System.identityHashCode(command) + profile.hashCode()) + family.hashCode();
		}

		@Override
		public boolean equals(Object other) {
			return other instanceof CancellationKey key
					&& command == key.command
					&& profile == key.profile
					&& family == key.family;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private static final class CancellationChain {

		private @Nullable WorkloadTask first;
		private @Nullable WorkloadTask last;

		private void addLast(WorkloadTask task) {
			if (task.previousCancellation != null || task.nextCancellation != null) {
				throw new IllegalStateException("Workload task is already cancellation-indexed");
			}
			if (last == null) {
				first = task;
			} else {
				last.nextCancellation = task;
				task.previousCancellation = last;
			}
			last = task;
		}

		private WorkloadTask first() {
			return Objects.requireNonNull(first, "Empty cancellation chain");
		}

		private void unlink(WorkloadTask task) {
			var previous = task.previousCancellation;
			var next = task.nextCancellation;
			if (previous == null) {
				if (first != task) {
					throw new IllegalStateException("Workload task is not in this cancellation chain");
				}
				first = next;
			} else {
				previous.nextCancellation = next;
			}
			if (next == null) {
				if (last != task) {
					throw new IllegalStateException("Workload task is not in this cancellation chain");
				}
				last = previous;
			} else {
				next.previousCancellation = previous;
			}
			task.previousCancellation = null;
			task.nextCancellation = null;
		}

		private boolean isEmpty() {
			return first == null;
		}
	}

	private record TerminalAction(Runnable command,
	                              WorkloadProfile profile,
	                              OperationFamily family,
	                              RuntimeException failure,
	                              RWScheduler.TerminalOutcome outcome) {
	}

	private enum AdmissionResult {
		ACCEPTED,
		DEADLINE,
		OVERLOAD,
		SHUTDOWN
	}

	@FunctionalInterface
	private interface CounterHandle {

		void increment();
	}

	@FunctionalInterface
	private interface TimerHandle {

		void record(long nanos);
	}

	private record TaskMetrics(TimerHandle queueWait,
	                           TimerHandle execution,
	                           CounterHandle quantum,
	                           CounterHandle failure,
	                           EnumMap<RWScheduler.TerminalOutcome, CounterHandle> outcomes,
	                           CounterHandle cancellation,
	                           EnumMap<RWScheduler.TerminalOutcome, CounterHandle> rejections,
	                           EnumMap<AdmissionResult, CounterHandle> admissions) {

		private static TaskMetrics inert() {
			var outcomes = new EnumMap<RWScheduler.TerminalOutcome, CounterHandle>(
					RWScheduler.TerminalOutcome.class);
			for (var outcome : RWScheduler.TerminalOutcome.values()) {
				outcomes.put(outcome, INERT_COUNTER);
			}
			var rejections = new EnumMap<RWScheduler.TerminalOutcome, CounterHandle>(
					RWScheduler.TerminalOutcome.class);
			for (var outcome : RWScheduler.TerminalOutcome.values()) {
				rejections.put(outcome, INERT_COUNTER);
			}
			var admissions = new EnumMap<AdmissionResult, CounterHandle>(AdmissionResult.class);
			for (var result : AdmissionResult.values()) {
				admissions.put(result, INERT_COUNTER);
			}
			return new TaskMetrics(INERT_TIMER,
					INERT_TIMER,
					INERT_COUNTER,
					INERT_COUNTER,
					outcomes,
					INERT_COUNTER,
					rejections,
					admissions);
		}

		private void recordOutcome(RWScheduler.TerminalOutcome outcome) {
			outcomes.get(outcome).increment();
			if (outcome == RWScheduler.TerminalOutcome.CANCELLATION) {
				cancellation.increment();
			}
			var rejection = rejections.get(outcome);
			if (rejection != null) {
				rejection.increment();
			}
		}

		private void recordAdmission(AdmissionResult result) {
			admissions.get(result).increment();
		}
	}
}

final class WorkloadPressureController {

	private final int maximumActiveBatches;
	private final long batchIntervalNanos;
	private boolean pressured;
	private int activeBatches;
	private long nextBatchNanos = Long.MIN_VALUE;
	private volatile Runnable notifier = () -> {
	};

	WorkloadPressureController(int maximumActiveBatches, java.time.Duration batchInterval) {
		if (maximumActiveBatches < 1) {
			throw new IllegalArgumentException("maximumActiveBatches must be positive");
		}
		this.maximumActiveBatches = maximumActiveBatches;
		this.batchIntervalNanos = Objects.requireNonNull(batchInterval, "batchInterval").toNanos();
		if (batchIntervalNanos < 1L) {
			throw new IllegalArgumentException("batchInterval must be positive");
		}
	}

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
			}
		}
		notifier.run();
	}

	synchronized boolean canStartBatch(boolean ignorePressure, long nowNanos) {
		return ignorePressure
				|| !pressured
				|| activeBatches < maximumActiveBatches && nowNanos >= nextBatchNanos;
	}

	synchronized @Nullable BatchPermit tryStartBatch(boolean ignorePressure, long nowNanos) {
		if (!canStartBatch(ignorePressure, nowNanos)) {
			return null;
		}
		boolean startedUnderPressure = pressured && !ignorePressure;
		activeBatches++;
		return new BatchPermit(startedUnderPressure);
	}

	void finishBatch(BatchPermit permit) {
		boolean notifySharedPressureWaiters;
		synchronized (this) {
			if (activeBatches <= 0) {
				throw new IllegalStateException("No active BATCH quantum to finish");
			}
			activeBatches--;
			notifySharedPressureWaiters = pressured;
			if (permit.startedUnderPressure() && pressured) {
				long nowNanos = System.nanoTime();
				try {
					nextBatchNanos = Math.addExact(nowNanos, batchIntervalNanos);
				} catch (ArithmeticException overflow) {
					nextBatchNanos = Long.MAX_VALUE;
				}
			}
		}
		if (notifySharedPressureWaiters) {
			notifier.run();
		}
	}

	synchronized long nanosUntilBatchEligible(long nowNanos) {
		if (!pressured) {
			return 0L;
		}
		if (activeBatches >= maximumActiveBatches) {
			return Long.MAX_VALUE;
		}
		if (nowNanos >= nextBatchNanos) {
			return 0L;
		}
		long remaining = nextBatchNanos - nowNanos;
		return remaining > 0L ? remaining : Long.MAX_VALUE;
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
