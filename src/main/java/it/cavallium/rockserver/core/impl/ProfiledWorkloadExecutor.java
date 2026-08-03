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
import java.util.concurrent.atomic.AtomicReference;
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
	private static final WorkloadProfile[] GUARANTEED = {
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH
	};
	private static final WorkloadProfile[] ISOLATED = {
			WorkloadProfile.CONTROL,
			WorkloadProfile.PHYSICAL_MAINTENANCE
	};
	private static final Comparator<WorkloadTask> DEADLINE_ORDER = Comparator
			.comparingLong(WorkloadTask::deadlineEpochMillis)
			.thenComparingLong(WorkloadTask::deadlineSequence);
	private static final CounterHandle INERT_COUNTER = _ -> {};
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
	private final EnumMap<WorkloadProfile, CooperativeQueue> cooperativeQueues;
	private final NavigableSet<WorkloadTask> deadlineQueue = new TreeSet<>(DEADLINE_ORDER);
	private final Map<CancellationKey, CancellationChain> cancellationIndex = new HashMap<>();
	private final LinkedHashSet<WorkloadTask> cooperativeTasks = new LinkedHashSet<>();
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
	private final RWScheduler.Pool resourcePool;
	private final ThreadFactory threadFactory;
	private final EnumMap<WorkloadProfile, EnumMap<OperationFamily, TaskMetrics>> taskMetrics;
	private final CounterHandle workerFailureMetric;
	private volatile boolean shutdown;
	private volatile boolean terminated;
	private @Nullable Thread timedWaitLeader;
	private int startedWorkers;
	private int waitingWorkers;
	private int queuedTotal;
	private int activeTotal;
	private int competingTasks;
	private boolean publishedPreemption;
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
	                         RWScheduler.Pool resourcePool,
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
		this.cooperativeQueues = new EnumMap<>(WorkloadProfile.class);
		this.pressureController = Objects.requireNonNull(pressureController, "pressureController");
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.resourceKind = Objects.requireNonNull(resourceKind, "resourceKind");
		this.resourcePool = Objects.requireNonNull(resourcePool, "resourcePool");
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
				this.cooperativeQueues.put(profile, new CooperativeQueue());
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

	RWScheduler.CooperativeHandle executeCooperatively(WorkloadProfile profile,
	                                                   OperationFamily family,
	                                                   long deadlineEpochMillis,
	                                                   long estimatedBytes,
	                                                   RWScheduler.CooperativeTask command) {
		Objects.requireNonNull(command, "command");
		if (profile != WorkloadProfile.BATCH) {
			throw new IllegalArgumentException("Cooperative execution is reserved for BATCH work");
		}
		int cost = taskCost(estimatedBytes);
		var taskMetrics = metrics(profile, family);
		var task = WorkloadTask.cooperative(this,
				profile,
				family,
				deadlineEpochMillis,
				sequence.getAndIncrement(),
				System.nanoTime(),
				cost,
				command,
				taskMetrics);
		RuntimeException admissionFailure = null;
		AdmissionResult admissionResult;
		List<TerminalAction> terminalActions = null;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			terminalActions = expireDueUnsafe(nowMillis, terminalActions);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowMillis >= deadlineEpochMillis) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				admissionResult = AdmissionResult.DEADLINE;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.DEADLINE,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionResult = AdmissionResult.SHUTDOWN;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.SHUTDOWN,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (capacities.get(profile) == 0 || queuedUnsafe(profile) >= capacities.get(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.OVERLOAD,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else {
				enqueueCooperativeUnsafe(task, true);
				acceptedTasks++;
				admissionResult = AdmissionResult.ACCEPTED;
				ensureWorkersStartedUnsafe();
				timedWaitLeader = null;
				refreshPreemptionUnsafe();
				workAvailable.signal();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		completeTerminalActions(terminalActions);
		taskMetrics.recordAdmission(admissionResult);
		if (admissionFailure != null) {
			throw admissionFailure;
		}
		return task;
	}

	void execute(WorkloadProfile profile,
	             OperationFamily family,
	             long deadlineEpochMillis,
	             long estimatedBytes,
	             Runnable command) {
		Objects.requireNonNull(command, "command");
		int cost = taskCost(estimatedBytes);
		var taskMetrics = metrics(profile, family);
		var cancellationState = command instanceof CancellationTrackedTask trackedTask
				? trackedTask.workloadCancellationState()
				: null;
		List<TerminalAction> terminalActions = null;
		RuntimeException admissionFailure = null;
		AdmissionResult admissionResult;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			terminalActions = expireDueUnsafe(nowMillis, terminalActions);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowMillis >= deadlineEpochMillis) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				admissionResult = AdmissionResult.DEADLINE;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.DEADLINE,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionResult = AdmissionResult.SHUTDOWN;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.SHUTDOWN,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (capacities.get(profile) == 0 || queuedUnsafe(profile) >= capacities.get(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.OVERLOAD,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else {
				var task = WorkloadTask.normal(profile,
						family,
						deadlineEpochMillis,
						sequence.getAndIncrement(),
						System.nanoTime(),
						cost,
						command,
						cancellationState,
						taskMetrics);
				boolean becomesDeadlineHead = task.hasDeadline()
						&& (deadlineQueue.isEmpty() || DEADLINE_ORDER.compare(task, deadlineQueue.first()) < 0);
				enqueueUnsafe(task);
				admitCompetitionUnsafe(task);
				acceptedTasks++;
				admissionResult = AdmissionResult.ACCEPTED;
				ensureWorkersStartedUnsafe();
				if (becomesDeadlineHead || profile == WorkloadProfile.BATCH) {
					timedWaitLeader = null;
				}
				refreshPreemptionUnsafe();
				workAvailable.signal();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		completeTerminalActions(terminalActions);
		taskMetrics.recordAdmission(admissionResult);
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

	private List<TerminalAction> recordUnacceptedUnsafe(RWScheduler.TerminalOutcome outcome,
	                                                    Runnable command,
	                                                    TaskMetrics taskMetrics,
	                                                    RuntimeException failure,
	                                                    @Nullable List<TerminalAction> terminalActions) {
		recordOutcomeUnsafe(outcome);
		var actions = terminalActions != null ? terminalActions : new ArrayList<TerminalAction>(1);
		actions.add(new TerminalAction(command, null, taskMetrics, failure, outcome));
		return actions;
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
		var terminalActions = new ArrayList<TerminalAction>(1);
		try {
			while (true) {
				WorkloadTask task;
				terminalActions.clear();
				boolean stop = false;
				lock.lock();
				try {
					long nowMillis = System.currentTimeMillis();
					expireDueUnsafe(nowMillis, terminalActions);
					task = dispatchUnsafe(nowMillis, terminalActions);
					if (task == null && shutdown && queuedTotal == 0 && cooperativeTasks.isEmpty()) {
						stop = true;
					} else if (task == null && terminalActions.isEmpty()) {
						awaitWorkUnsafe();
					}
				} finally {
					lock.unlock();
				}
				pressureController.signalPendingAvailability();
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
			waitingWorkers++;
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
			if (waitNanos > 0L) {
				waitingWorkers--;
			}
			if (timedWaitLeader == null && queuedTotal > 0) {
				workAvailable.signal();
			}
		}
	}

	private long pressureWaitNanosUnsafe() {
		if (shutdown || queuedUnsafe(WorkloadProfile.BATCH) == 0) {
			return Long.MAX_VALUE;
		}
		return pressureController.nanosUntilBatchEligible(resourcePool, System.nanoTime());
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
			// Atomically claim the internal cancellation state at the last
			// pre-permit boundary. Once claimed, disposal is a running-task race.
			if (!task.claimForDispatch()) {
				cancelSelectedUnsafe(task, terminalActions);
				continue;
			}
			WorkloadPressureController.BatchPermit batchPermit = null;
			if (task.profile() == WorkloadProfile.BATCH) {
				batchPermit = pressureController.tryStartBatch(shutdown, resourcePool, System.nanoTime());
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
			task.markActive();
			if (task.isCooperative()) {
				if (task.markStarted()) {
					startedTasks++;
				}
			} else {
				if (!terminateUnsafe(task, RWScheduler.TerminalOutcome.RUN, null, terminalActions)) {
					throw new IllegalStateException("Queued workload task already has a terminal outcome");
				}
				startedTasks++;
			}
			refreshPreemptionUnsafe();
			return task;
		}
		refreshPreemptionUnsafe();
		return null;
	}

	private boolean cancelSelectedUnsafe(WorkloadTask task,
	                                     List<TerminalAction> terminalActions) {
		if (!task.cancellationRequested()) {
			return false;
		}
		unlinkUnsafe(task);
		discardSelectionUnsafe(task);
		terminateUnsafe(task,
				RWScheduler.TerminalOutcome.CANCELLATION,
				new CancellationException("Workload submission cancelled before execution"),
				terminalActions);
		refreshPreemptionUnsafe();
		return true;
	}

	private void runDispatched(WorkloadTask task) {
		if (task.isCooperative()) {
			runCooperativeDispatched(task);
			return;
		}
		var metrics = task.metrics();
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
				EXECUTING_POOL.set(null);
			} else {
				EXECUTING_POOL.set(previousPool);
			}
			try {
				metrics.recordOutcome(RWScheduler.TerminalOutcome.RUN);
				metrics.queueWait().record(executionStart - task.enqueuedNanos());
				metrics.quantum().increment();
				metrics.execution().record(System.nanoTime() - executionStart);
			} finally {
				finishActive(task);
			}
		}
	}

	private void runCooperativeDispatched(WorkloadTask task) {
		var metrics = task.metrics();
		long executionStart = System.nanoTime();
		var previousPool = EXECUTING_POOL.get();
		RWScheduler.CooperativeResult result = RWScheduler.CooperativeResult.COMPLETE;
		RuntimeException executionFailure = null;
		try {
			EXECUTING_POOL.set(this);
			result = Objects.requireNonNull(task.cooperativeCommand().runCooperatively(task),
					"Cooperative task returned no result");
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable error) {
			recordTaskFailure(task, error);
			executionFailure = error instanceof RuntimeException runtimeException
					? runtimeException
					: new RejectedExecutionException("Cooperative workload quantum failed", error);
		} finally {
			if (previousPool == null) {
				EXECUTING_POOL.set(null);
			} else {
				EXECUTING_POOL.set(previousPool);
			}
			task.recordCooperativeQuantum(executionStart - task.enqueuedNanos(),
					System.nanoTime() - executionStart);
			var terminalAction = finishCooperative(task, result, executionFailure);
			if (task.state() == TaskState.TERMINAL) {
				task.flushCooperativeMetrics();
			}
			if (terminalAction != null) {
				completeTerminalAction(terminalAction);
			} else if (task.outcome() == RWScheduler.TerminalOutcome.RUN) {
				metrics.recordOutcome(RWScheduler.TerminalOutcome.RUN);
			}
		}
	}

	private void finishActive(WorkloadTask task) {
		var batchPermit = task.takeBatchPermit();
		if (batchPermit != null) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		lock.lock();
		try {
			active.put(task.profile(), active.get(task.profile()) - 1);
			activeTotal--;
			completeCompetitionUnsafe(task);
			completedTasks++;
			task.markTerminal();
			refreshPreemptionUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
	}

	private @Nullable TerminalAction finishCooperative(WorkloadTask task,
	                                                   RWScheduler.CooperativeResult result,
	                                                   @Nullable RuntimeException executionFailure) {
		var batchPermit = task.takeBatchPermit();
		if (batchPermit != null) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		TerminalAction terminalAction = null;
		lock.lock();
		try {
			if (task.state() != TaskState.ACTIVE) {
				throw new IllegalStateException("Cooperative task is not active");
			}
			active.put(task.profile(), active.get(task.profile()) - 1);
			activeTotal--;
			if (executionFailure != null) {
				terminalAction = terminateCooperativeUnsafe(task,
						RWScheduler.TerminalOutcome.RUN,
						executionFailure);
			} else if (task.requestedOutcome() != null) {
				terminalAction = terminateCooperativeUnsafe(task,
						Objects.requireNonNull(task.requestedOutcome()),
						Objects.requireNonNull(task.terminationFailure()));
			} else {
				switch (result) {
					case COMPLETE -> {
						terminateCooperativeUnsafe(task, RWScheduler.TerminalOutcome.RUN, null);
					}
					case YIELD -> {
						task.clearResumeRequested();
						refreshCooperativeSequenceUnsafe(task);
						enqueueCooperativeUnsafe(task, false);
					}
					case PARK -> {
						if (task.consumeResumeRequested()) {
							refreshCooperativeSequenceUnsafe(task);
							enqueueCooperativeUnsafe(task, false);
						} else {
							task.markParked();
						}
					}
				}
			}
			refreshPreemptionUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		return terminalAction;
	}

	private void refreshCooperativeSequenceUnsafe(WorkloadTask task) {
		task.refreshSequence(sequence.getAndIncrement(), System.nanoTime());
	}

	private void resumeCooperative(WorkloadTask task) {
		lock.lock();
		try {
			switch (task.state()) {
				case ACTIVE -> task.requestResume();
				case PARKED -> {
					if (task.requestedOutcome() == null) {
						refreshCooperativeSequenceUnsafe(task);
						enqueueCooperativeUnsafe(task, false);
						refreshPreemptionUnsafe();
						workAvailable.signal();
					}
				}
				case QUEUED, TERMINAL -> {
				}
			}
		} finally {
			lock.unlock();
		}
	}

	private void cancelCooperative(WorkloadTask task) {
		TerminalAction terminalAction = null;
		lock.lock();
		try {
			if (task.state() == TaskState.TERMINAL || task.requestedOutcome() != null) {
				return;
			}
			var failure = new CancellationException("Cooperative workload submission cancelled");
			switch (task.state()) {
				case QUEUED -> {
					unlinkUnsafe(task);
					terminalAction = terminateCooperativeUnsafe(task,
							RWScheduler.TerminalOutcome.CANCELLATION,
							failure);
				}
				case PARKED -> terminalAction = terminateCooperativeUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						failure);
				case ACTIVE -> task.requestTermination(RWScheduler.TerminalOutcome.CANCELLATION, failure);
				case TERMINAL -> {
				}
			}
			refreshPreemptionUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		if (terminalAction != null) {
			completeTerminalAction(terminalAction);
		}
	}

	private void refreshPreemptionUnsafe() {
		boolean requested = queuedTotal > 0
				|| activeTotal > active.get(WorkloadProfile.BATCH);
		if (requested != publishedPreemption) {
			publishedPreemption = requested;
			pressureController.setPoolPreemption(resourcePool, requested);
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
		for (var profile : ISOLATED) {
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
		int maxAttempts = GUARANTEED.length * (MAX_TASK_COST + 1);
		for (int attempts = 0; attempts < maxAttempts; attempts++) {
			var profile = GUARANTEED[guaranteedCursor];
			if (queuedUnsafe(profile) == 0) {
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
			var head = peekUnsafe(profile);
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
		if (!isGuaranteedProfile(task.profile())) {
			return;
		}
		latencyBurst = 0;
		deficit.put(task.profile(), deficit.get(task.profile()) - task.cost());
		if (queuedUnsafe(task.profile()) == 0) {
			deficit.put(task.profile(), 0);
			advanceGuaranteedCursor();
		} else if (deficit.get(task.profile()) < peekUnsafe(task.profile()).cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void discardSelectionUnsafe(WorkloadTask task) {
		if (!isGuaranteedProfile(task.profile())) {
			return;
		}
		if (queuedUnsafe(task.profile()) == 0) {
			deficit.put(task.profile(), 0);
			advanceGuaranteedCursor();
		} else if (deficit.get(task.profile()) < peekUnsafe(task.profile()).cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void advanceGuaranteedCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.length;
		guaranteedNeedsQuantum = true;
	}

	private static boolean isGuaranteedProfile(WorkloadProfile profile) {
		return switch (profile) {
			case INGEST, CDC, ANALYTICAL, BATCH -> true;
			default -> false;
		};
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
				|| pressureController.canStartBatch(shutdown, resourcePool, System.nanoTime());
	}

	boolean remove(Executor view, Runnable command) {
		if (!(view instanceof WorkloadExecutorView workloadView) || workloadView.owner() != this) {
			return false;
		}
		return remove(workloadView.profile(), workloadView.family(), command);
	}

	private boolean remove(WorkloadProfile profile, OperationFamily family, Runnable command) {
		List<TerminalAction> terminalActions = null;
		boolean removed = false;
		lock.lock();
		try {
			var chain = cancellationIndex.get(new CancellationKey(command, profile, family));
			if (chain != null && !chain.isEmpty()) {
				var task = chain.first();
				terminalActions = new ArrayList<>(1);
				boolean wasDeadlineHead = task.hasDeadline() && deadlineQueue.first() == task;
				unlinkUnsafe(task);
				removed = terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						new CancellationException("Workload submission cancelled while queued"),
						terminalActions);
				if (wasDeadlineHead || task.profile() == WorkloadProfile.BATCH) {
					timedWaitLeader = null;
				}
				refreshPreemptionUnsafe();
				workAvailable.signal();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
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
			int batchQueued = queuedUnsafe(WorkloadProfile.BATCH);
			int batchStartAllowance = pressureController.batchStartAllowance(
					shutdown, resourcePool, System.nanoTime());
			return new ExecutorSnapshot(
					workerCount,
					waitingWorkers,
					queuedTotal,
					activeTotal,
					acceptedTasks,
					startedTasks,
					completedTasks,
					failedTasks,
					Map.copyOf(queued),
					Map.copyOf(active),
					Map.copyOf(outcomes),
					batchQueued > batchStartAllowance,
					batchStartAllowance,
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

	void signalOneAvailability() {
		lock.lock();
		try {
			timedWaitLeader = null;
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
	}

	private int queuedUnsafe(WorkloadProfile profile) {
		return queued.get(profile);
	}

	private @Nullable List<TerminalAction> expireDueUnsafe(long nowMillis,
	                                                       @Nullable List<TerminalAction> terminalActions) {
		while (!deadlineQueue.isEmpty()) {
			var task = deadlineQueue.first();
			if (nowMillis < task.deadlineEpochMillis()) {
				refreshPreemptionUnsafe();
				return terminalActions;
			}
			if (task.isCooperative()) {
				var failure = deadlineFailure("Cooperative workload deadline expired");
				if (!deadlineQueue.remove(task)) {
					throw new IllegalStateException("Cooperative deadline task is not indexed");
				}
				task.markDeadlineUnindexed();
				if (task.state() == TaskState.ACTIVE) {
					task.requestTermination(RWScheduler.TerminalOutcome.DEADLINE, failure);
					continue;
				}
				if (task.state() == TaskState.QUEUED) {
					unlinkUnsafe(task);
				}
				var action = terminateCooperativeUnsafe(task,
						RWScheduler.TerminalOutcome.DEADLINE,
						failure);
				if (action != null) {
					if (terminalActions == null) {
						terminalActions = new ArrayList<>(1);
					}
					terminalActions.add(action);
				}
				continue;
			}
			if (terminalActions == null) {
				terminalActions = new ArrayList<>(1);
			}
			unlinkUnsafe(task);
			terminateUnsafe(task,
					RWScheduler.TerminalOutcome.DEADLINE,
					deadlineFailure("Workload deadline expired while queued"),
					terminalActions);
		}
		refreshPreemptionUnsafe();
		return terminalActions;
	}

	private void enqueueUnsafe(WorkloadTask task) {
		if (!normalQueueUnsafe(task.profile()).add(task)) {
			throw new IllegalStateException("Duplicate workload task sequence " + task.sequence());
		}
		if (task.hasDeadline() && !deadlineQueue.add(task)) {
			throw new IllegalStateException("Duplicate workload deadline sequence " + task.sequence());
		}
		var cancellationChain = cancellationIndex.computeIfAbsent(task.cancellationKey(),
				ignored -> new CancellationChain());
		cancellationChain.addLast(task);
		task.indexCancellation(cancellationChain);
		int previousQueued = queued.get(task.profile());
		queued.put(task.profile(), previousQueued + 1);
		if (task.profile() == WorkloadProfile.BATCH && previousQueued == 0) {
			pressureController.setBatchQueued(resourcePool, true);
		}
		queuedTotal++;
		task.markQueued();
	}

	private void enqueueCooperativeUnsafe(WorkloadTask task, boolean initialAdmission) {
		var queue = cooperativeQueues.get(task.profile());
		if (queue == null) {
			throw new IllegalStateException("No cooperative queue for " + task.profile());
		}
		queue.addLast(task);
		if (initialAdmission) {
			if (!cooperativeTasks.add(task)) {
				throw new IllegalStateException("Cooperative workload task is already admitted");
			}
			if (task.hasDeadline()) {
				if (!deadlineQueue.add(task)) {
					throw new IllegalStateException("Duplicate cooperative workload deadline sequence "
							+ task.sequence());
				}
				task.markDeadlineIndexed();
			}
			var cancellationChain = cancellationIndex.computeIfAbsent(task.cancellationKey(),
					ignored -> new CancellationChain());
			task.mapCancellation(cancellationChain);
		}
		var cancellationChain = Objects.requireNonNull(task.cancellationChain());
		cancellationChain.addLast(task);
		task.markCancellationLinked();
		int previousQueued = queued.get(task.profile());
		queued.put(task.profile(), previousQueued + 1);
		if (task.profile() == WorkloadProfile.BATCH && previousQueued == 0) {
			pressureController.setBatchQueued(resourcePool, true);
		}
		queuedTotal++;
		task.markQueued();
	}

	private void unlinkUnsafe(WorkloadTask task) {
		if (task.isCooperative()) {
			unlinkCooperativeQueuedUnsafe(task);
			return;
		}
		if (!normalQueueUnsafe(task.profile()).remove(task)) {
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
		task.clearCancellationIndex();
		int remainingQueued = queued.get(task.profile()) - 1;
		queued.put(task.profile(), remainingQueued);
		if (task.profile() == WorkloadProfile.BATCH && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		queuedTotal--;
	}

	private void unlinkCooperativeQueuedUnsafe(WorkloadTask task) {
		var queue = cooperativeQueues.get(task.profile());
		if (queue == null || !queue.remove(task)) {
			throw new IllegalStateException("Cooperative workload task is not queued: " + task.sequence());
		}
		var chain = Objects.requireNonNull(task.cancellationChain());
		chain.unlink(task);
		task.markCancellationUnlinked();
		int remainingQueued = queued.get(task.profile()) - 1;
		queued.put(task.profile(), remainingQueued);
		if (task.profile() == WorkloadProfile.BATCH && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		queuedTotal--;
	}

	private boolean terminateUnsafe(WorkloadTask task,
	                                RWScheduler.TerminalOutcome outcome,
	                                @Nullable RuntimeException failure,
	                                List<TerminalAction> terminalActions) {
		if (task.isCooperative()) {
			var action = terminateCooperativeUnsafe(task, outcome, failure);
			if (action != null) {
				terminalActions.add(action);
			}
			return action != null || outcome == RWScheduler.TerminalOutcome.RUN;
		}
		if (task.outcome() != null) {
			return false;
		}
		task.outcome(outcome);
		if (outcome != RWScheduler.TerminalOutcome.RUN) {
			completeCompetitionUnsafe(task);
			task.markTerminal();
		}
		recordOutcomeUnsafe(outcome);
		if (outcome != RWScheduler.TerminalOutcome.RUN) {
			terminalActions.add(new TerminalAction(task.command(),
					null,
					task.metrics(),
					Objects.requireNonNull(failure),
					outcome));
		}
		return true;
	}

	private @Nullable TerminalAction terminateCooperativeUnsafe(WorkloadTask task,
	                                                            RWScheduler.TerminalOutcome outcome,
	                                                            @Nullable RuntimeException failure) {
		if (task.outcome() != null) {
			return null;
		}
		if (task.deadlineIndexed()) {
			if (!deadlineQueue.remove(task)) {
				throw new IllegalStateException("Cooperative workload deadline task is not indexed");
			}
			task.markDeadlineUnindexed();
		}
		if (task.cancellationLinked()) {
			Objects.requireNonNull(task.cancellationChain()).unlink(task);
			task.markCancellationUnlinked();
		}
		var cancellationChain = task.cancellationChain();
		if (cancellationChain != null && cancellationChain.isEmpty()) {
			cancellationIndex.remove(task.cancellationKey());
		}
		task.clearCancellationIndex();
		if (!cooperativeTasks.remove(task)) {
			throw new IllegalStateException("Cooperative workload task is not admitted");
		}
		task.outcome(outcome);
		task.markTerminal();
		if (task.hasStarted()) {
			completedTasks++;
		}
		recordOutcomeUnsafe(outcome);
		if (outcome == RWScheduler.TerminalOutcome.RUN && failure == null) {
			return null;
		}
		return new TerminalAction(task.command(),
				task,
				task.metrics(),
				Objects.requireNonNull(failure),
				outcome);
	}

	private void recordOutcomeUnsafe(RWScheduler.TerminalOutcome outcome) {
		outcomes.put(outcome, outcomes.get(outcome) + 1L);
	}

	private void admitCompetitionUnsafe(WorkloadTask task) {
		if (task.profile() == WorkloadProfile.BATCH) {
			return;
		}
		if (competingTasks++ == 0) {
			pressureController.setPoolCompetition(resourcePool, true);
		}
	}

	private void completeCompetitionUnsafe(WorkloadTask task) {
		if (task.profile() == WorkloadProfile.BATCH) {
			return;
		}
		if (competingTasks <= 0) {
			throw new IllegalStateException("Competing workload count underflow in " + poolName);
		}
		if (--competingTasks == 0) {
			pressureController.setPoolCompetition(resourcePool, false);
		}
	}

	private static RocksDBException deadlineFailure(String message) {
		return RocksDBException.of(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, message);
	}

	private void completeTerminalActions(@Nullable List<TerminalAction> terminalActions) {
		if (terminalActions == null) {
			return;
		}
		for (int i = 0, size = terminalActions.size(); i < size; i++) {
			completeTerminalAction(terminalActions.get(i));
		}
	}

	private void completeTerminalAction(TerminalAction action) {
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
		if (action.cooperativeTask() != null) {
			action.cooperativeTask().flushCooperativeMetrics();
		}
		action.metrics().recordOutcome(action.outcome());
	}

	private Collection<WorkloadTask> normalQueueUnsafe(WorkloadProfile profile) {
		return profile == WorkloadProfile.LATENCY ? latencyQueue : queues.get(profile);
	}

	private WorkloadTask peekUnsafe(WorkloadProfile profile) {
		if (profile == WorkloadProfile.LATENCY) {
			return latencyQueue.first();
		}
		var normalQueue = queues.get(profile);
		var cooperativeQueue = cooperativeQueues.get(profile);
		var normalHead = normalQueue.isEmpty() ? null : normalQueue.getFirst();
		var cooperativeHead = cooperativeQueue == null ? null : cooperativeQueue.peekFirst();
		if (normalHead == null) {
			return Objects.requireNonNull(cooperativeHead);
		}
		if (cooperativeHead == null) {
			return normalHead;
		}
		return normalHead.sequence() <= cooperativeHead.sequence() ? normalHead : cooperativeHead;
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
			return amount -> {
				try {
					counter.increment(amount);
				} catch (VirtualMachineError fatal) {
					throw fatal;
				} catch (Throwable metricFailure) {
					recordMetricFailure(name, metricFailure);
				}
			};
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
			return nanos -> {
				try {
					timer.record(nanos, TimeUnit.NANOSECONDS);
				} catch (VirtualMachineError fatal) {
					throw fatal;
				} catch (Throwable metricFailure) {
					recordMetricFailure(name, metricFailure);
				}
			};
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
		task.metrics().failure().increment();
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
			for (var task : new ArrayList<>(cooperativeTasks)) {
				if (task.state() == TaskState.PARKED) {
					remaining.add(task.command());
					terminateUnsafe(task,
							RWScheduler.TerminalOutcome.SHUTDOWN,
							new RejectedExecutionException(poolName + " was forced to shut down"),
							terminalActions);
				} else if (task.state() == TaskState.ACTIVE && task.requestedOutcome() == null) {
					task.requestTermination(RWScheduler.TerminalOutcome.SHUTDOWN,
							new RejectedExecutionException(poolName + " was forced to shut down"));
				}
			}
			for (var worker : workers) {
				worker.interrupt();
			}
			if (startedWorkers == 0) {
				terminated = true;
			}
			refreshPreemptionUnsafe();
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
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

		@Override
		public RWScheduler.CooperativeHandle executeCooperatively(RWScheduler.CooperativeTask command,
		                                                          long estimatedBytes) {
			return owner.executeCooperatively(profile, family, deadlineEpochMillis, estimatedBytes, command);
		}
	}

	private static final class WorkloadTask implements RWScheduler.CooperativeHandle,
			RWScheduler.CooperativeContext {

		private final @Nullable ProfiledWorkloadExecutor owner;
		private final WorkloadProfile profile;
		private final OperationFamily family;
		private final long deadlineEpochMillis;
		private final long deadlineSequence;
		private long sequence;
		private long enqueuedNanos;
		private final int cost;
		private final Runnable command;
		private final CancellationKey cancellationKey;
		private final @Nullable CancellationState cancellationState;
		private final TaskMetrics metrics;
		private final boolean cooperative;
		private @Nullable WorkloadTask previousCancellation;
		private @Nullable WorkloadTask nextCancellation;
		private @Nullable WorkloadTask previousCooperative;
		private @Nullable WorkloadTask nextCooperative;
		private @Nullable CancellationChain cancellationChain;
		private @Nullable WorkloadPressureController.BatchPermit batchPermit;
		private volatile @Nullable RWScheduler.TerminalOutcome outcome;
		private final @Nullable AtomicReference<RequestedTermination> requestedTermination;
		private volatile TaskState state = TaskState.QUEUED;
		private boolean cancellationLinked;
		private boolean deadlineIndexed;
		private boolean cooperativeQueued;
		private boolean resumeRequested;
		private boolean started;
		private long cooperativeQueueWaitNanos;
		private long cooperativeExecutionNanos;
		private long cooperativeQuantumCount;
		private boolean cooperativeMetricsFlushed;

		private WorkloadTask(@Nullable ProfiledWorkloadExecutor owner,
		                     WorkloadProfile profile,
		                     OperationFamily family,
		                     long deadlineEpochMillis,
		                     long sequence,
		                     long enqueuedNanos,
		                     int cost,
		                     Runnable command,
		                     @Nullable CancellationState cancellationState,
		                     TaskMetrics metrics,
		                     boolean cooperative) {
			this.owner = owner;
			this.profile = profile;
			this.family = family;
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.deadlineSequence = sequence;
			this.sequence = sequence;
			this.enqueuedNanos = enqueuedNanos;
			this.cost = cost;
			this.command = command;
			this.cancellationKey = new CancellationKey(command, profile, family);
			this.cancellationState = cancellationState;
			this.metrics = metrics;
			this.cooperative = cooperative;
			this.requestedTermination = cooperative ? new AtomicReference<>() : null;
		}

		private static WorkloadTask normal(WorkloadProfile profile,
		                                   OperationFamily family,
		                                   long deadlineEpochMillis,
		                                   long sequence,
		                                   long enqueuedNanos,
		                                   int cost,
		                                   Runnable command,
		                                   @Nullable CancellationState cancellationState,
		                                   TaskMetrics metrics) {
			return new WorkloadTask(null,
					profile,
					family,
					deadlineEpochMillis,
					sequence,
					enqueuedNanos,
					cost,
					command,
					cancellationState,
					metrics,
					false);
		}

		private static WorkloadTask cooperative(ProfiledWorkloadExecutor owner,
		                                        WorkloadProfile profile,
		                                        OperationFamily family,
		                                        long deadlineEpochMillis,
		                                        long sequence,
		                                        long enqueuedNanos,
		                                        int cost,
		                                        RWScheduler.CooperativeTask command,
		                                        TaskMetrics metrics) {
			return new WorkloadTask(owner,
					profile,
					family,
					deadlineEpochMillis,
					sequence,
					enqueuedNanos,
					cost,
					command,
					null,
					metrics,
					true);
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

		private long deadlineSequence() {
			return deadlineSequence;
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

		private RWScheduler.CooperativeTask cooperativeCommand() {
			return (RWScheduler.CooperativeTask) command;
		}

		private TaskMetrics metrics() {
			return metrics;
		}

		private boolean isCooperative() {
			return cooperative;
		}

		private CancellationKey cancellationKey() {
			return cancellationKey;
		}

		private void batchPermit(WorkloadPressureController.BatchPermit batchPermit) {
			this.batchPermit = batchPermit;
		}

		private @Nullable WorkloadPressureController.BatchPermit takeBatchPermit() {
			var result = batchPermit;
			batchPermit = null;
			return result;
		}

		private @Nullable RWScheduler.TerminalOutcome outcome() {
			return outcome;
		}

		private void outcome(RWScheduler.TerminalOutcome outcome) {
			this.outcome = outcome;
		}

		private boolean cancellationRequested() {
			return cancellationState != null && cancellationState.isCancellationRequested();
		}

		private boolean claimForDispatch() {
			return cancellationState == null || cancellationState.claimForDispatch();
		}

		private void indexCancellation(CancellationChain cancellationChain) {
			this.cancellationChain = cancellationChain;
			this.cancellationLinked = true;
		}

		private void mapCancellation(CancellationChain cancellationChain) {
			this.cancellationChain = cancellationChain;
		}

		private @Nullable CancellationChain cancellationChain() {
			return cancellationChain;
		}

		private boolean cancellationLinked() {
			return cancellationLinked;
		}

		private void markCancellationLinked() {
			cancellationLinked = true;
		}

		private void markCancellationUnlinked() {
			cancellationLinked = false;
		}

		private void clearCancellationIndex() {
			cancellationLinked = false;
			cancellationChain = null;
		}

		private boolean deadlineIndexed() {
			return deadlineIndexed;
		}

		private void markDeadlineIndexed() {
			deadlineIndexed = true;
		}

		private void markDeadlineUnindexed() {
			deadlineIndexed = false;
		}

		private TaskState state() {
			return state;
		}

		private void markQueued() {
			state = TaskState.QUEUED;
		}

		private void markActive() {
			state = TaskState.ACTIVE;
		}

		private void markParked() {
			state = TaskState.PARKED;
		}

		private void markTerminal() {
			state = TaskState.TERMINAL;
		}

		private boolean markStarted() {
			if (started) {
				return false;
			}
			started = true;
			return true;
		}

		private boolean hasStarted() {
			return started;
		}

		private void refreshSequence(long sequence, long enqueuedNanos) {
			this.sequence = sequence;
			this.enqueuedNanos = enqueuedNanos;
		}

		private void recordCooperativeQuantum(long queueWaitNanos, long executionNanos) {
			if (!cooperative) {
				throw new IllegalStateException("Normal workload task cannot record a cooperative quantum");
			}
			// queue.wait is admission-to-first-dispatch latency for one logical submission. A parked
			// downstream consumer or a cooperative yield is part of the same task and must not be
			// accumulated into a misleading multi-quantum queue latency. The quantum counter retains
			// the exact number of redispatches; execution remains total logical active time.
			if (cooperativeQuantumCount == 0L) {
				cooperativeQueueWaitNanos = queueWaitNanos;
			}
			cooperativeExecutionNanos = saturatingAdd(cooperativeExecutionNanos, executionNanos);
			cooperativeQuantumCount++;
		}

		private void flushCooperativeMetrics() {
			if (!cooperative || cooperativeMetricsFlushed) {
				return;
			}
			cooperativeMetricsFlushed = true;
			if (cooperativeQuantumCount == 0L) {
				return;
			}
			// Micrometer's contention-adaptive adders can allocate cells while growing. Keep
			// registry calls off repeated yields and publish one logical-task sample at terminal.
			metrics.queueWait().record(cooperativeQueueWaitNanos);
			metrics.quantum().increment(cooperativeQuantumCount);
			metrics.execution().record(cooperativeExecutionNanos);
		}

		private static long saturatingAdd(long current, long increment) {
			return increment > Long.MAX_VALUE - current ? Long.MAX_VALUE : current + increment;
		}

		private void requestResume() {
			resumeRequested = true;
		}

		private void clearResumeRequested() {
			resumeRequested = false;
		}

		private boolean consumeResumeRequested() {
			boolean result = resumeRequested;
			resumeRequested = false;
			return result;
		}

		private void requestTermination(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
			Objects.requireNonNull(requestedTermination, "Non-cooperative workload task")
					.compareAndSet(null, new RequestedTermination(outcome, failure));
		}

		private @Nullable RWScheduler.TerminalOutcome requestedOutcome() {
			var requested = Objects.requireNonNull(requestedTermination, "Non-cooperative workload task").get();
			return requested == null ? null : requested.outcome();
		}

		@Override
		public boolean preemptionRequested() {
			return Objects.requireNonNull(owner).pressureController.preemptionRequested();
		}

		@Override
		public boolean terminationRequested() {
			var termination = Objects.requireNonNull(requestedTermination, "Non-cooperative workload task");
			if (termination.get() != null) {
				return true;
			}
			if (hasDeadline() && System.currentTimeMillis() >= deadlineEpochMillis) {
				termination.compareAndSet(null, new RequestedTermination(
						RWScheduler.TerminalOutcome.DEADLINE,
						deadlineFailure("Cooperative workload deadline expired while active")));
				return true;
			}
			return false;
		}

		@Override
		public @Nullable RuntimeException terminationFailure() {
			var requested = Objects.requireNonNull(requestedTermination, "Non-cooperative workload task").get();
			return requested == null ? null : requested.failure();
		}

		@Override
		public void resume() {
			Objects.requireNonNull(owner).resumeCooperative(this);
		}

		@Override
		public void dispose() {
			Objects.requireNonNull(owner).cancelCooperative(this);
		}

		@Override
		public boolean isDisposed() {
			return state == TaskState.TERMINAL
					|| Objects.requireNonNull(requestedTermination, "Non-cooperative workload task").get() != null;
		}
	}

	private record RequestedTermination(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
	}

	interface CancellationTrackedTask {

		CancellationState workloadCancellationState();
	}

	static final class CancellationState {

		private static final int PENDING = 0;
		private static final int CLAIMED = 1;
		private static final int CANCELLED = 2;
		private final java.util.concurrent.atomic.AtomicInteger state =
				new java.util.concurrent.atomic.AtomicInteger(PENDING);

		boolean cancel() {
			return state.compareAndSet(PENDING, CANCELLED);
		}

		boolean isCancellationRequested() {
			return state.get() == CANCELLED;
		}

		boolean claimForDispatch() {
			return state.compareAndSet(PENDING, CLAIMED) || state.get() == CLAIMED;
		}
	}

	private enum TaskState {
		QUEUED,
		ACTIVE,
		PARKED,
		TERMINAL
	}

	private static final class CooperativeQueue {

		private @Nullable WorkloadTask first;
		private @Nullable WorkloadTask last;

		private void addLast(WorkloadTask task) {
			if (task.cooperativeQueued || task.previousCooperative != null || task.nextCooperative != null) {
				throw new IllegalStateException("Cooperative workload task is already queued");
			}
			if (last == null) {
				first = task;
			} else {
				last.nextCooperative = task;
				task.previousCooperative = last;
			}
			last = task;
			task.cooperativeQueued = true;
		}

		private boolean remove(WorkloadTask task) {
			if (!task.cooperativeQueued) {
				return false;
			}
			var previous = task.previousCooperative;
			var next = task.nextCooperative;
			if (previous == null) {
				if (first != task) {
					throw new IllegalStateException("Cooperative workload task is not in this queue");
				}
				first = next;
			} else {
				previous.nextCooperative = next;
			}
			if (next == null) {
				if (last != task) {
					throw new IllegalStateException("Cooperative workload task is not in this queue");
				}
				last = previous;
			} else {
				next.previousCooperative = previous;
			}
			task.previousCooperative = null;
			task.nextCooperative = null;
			task.cooperativeQueued = false;
			return true;
		}

		private @Nullable WorkloadTask peekFirst() {
			return first;
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
			if (task.cancellationLinked
					|| task.previousCancellation != null
					|| task.nextCancellation != null) {
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
	                              @Nullable WorkloadTask cooperativeTask,
	                              TaskMetrics metrics,
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

		void increment(double amount);

		default void increment() {
			increment(1.0);
		}
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

	private static final BatchPermit UNRESTRICTED_PERMIT = new BatchPermit(false, false);
	private static final BatchPermit PRESSURED_PERMIT = new BatchPermit(true, false);
	private static final BatchPermit COMPETING_PERMIT = new BatchPermit(false, true);
	private static final BatchPermit PRESSURED_AND_COMPETING_PERMIT = new BatchPermit(true, true);
	private final int competingMaximumActiveReadBatches;
	private final int competingMaximumActiveWriteBatches;
	private final long competingWriteIntervalNanos;
	private final int pressuredMaximumActiveBatches;
	private final long competitionHoldNanos;
	private final long batchIntervalNanos;
	private boolean pressured;
	private int activeBatches;
	private int activeReadBatches;
	private int activeWriteBatches;
	private long nextCompetingWriteNanos = Long.MIN_VALUE;
	private long nextBatchNanos = Long.MIN_VALUE;
	private int preemptionPoolMask;
	private int competitionPoolMask;
	private int queuedBatchPoolMask;
	private long competitionUntilNanos = Long.MIN_VALUE;
	private volatile boolean preemptionRequested;
	private volatile boolean notificationPending;
	private volatile Runnable notifier = () -> {
	};
	private volatile java.util.function.IntConsumer batchNotifier = _ -> {
	};

	WorkloadPressureController(int competingMaximumActiveReadBatches,
	                           int competingMaximumActiveWriteBatches,
	                           java.time.Duration competingWriteInterval,
	                           int pressuredMaximumActiveBatches,
	                           java.time.Duration competitionHold,
	                           java.time.Duration batchInterval) {
		if (competingMaximumActiveReadBatches < 1 || competingMaximumActiveWriteBatches < 1) {
			throw new IllegalArgumentException("competing BATCH maxima must be positive");
		}
		if (pressuredMaximumActiveBatches < 1) {
			throw new IllegalArgumentException("pressuredMaximumActiveBatches must be positive");
		}
		this.competingMaximumActiveReadBatches = competingMaximumActiveReadBatches;
		this.competingMaximumActiveWriteBatches = competingMaximumActiveWriteBatches;
		this.competingWriteIntervalNanos = Objects.requireNonNull(
				competingWriteInterval, "competingWriteInterval").toNanos();
		this.pressuredMaximumActiveBatches = pressuredMaximumActiveBatches;
		this.competitionHoldNanos = Objects.requireNonNull(competitionHold, "competitionHold").toNanos();
		this.batchIntervalNanos = Objects.requireNonNull(batchInterval, "batchInterval").toNanos();
		if (competingWriteIntervalNanos < 1L || competitionHoldNanos < 1L || batchIntervalNanos < 1L) {
			throw new IllegalArgumentException(
					"competingWriteInterval, competitionHold, and batchInterval must be positive");
		}
	}

	synchronized boolean isPressured() {
		return pressured;
	}

	void setNotifier(Runnable notifier) {
		this.notifier = Objects.requireNonNull(notifier, "notifier");
	}

	void setBatchNotifier(java.util.function.IntConsumer batchNotifier) {
		this.batchNotifier = Objects.requireNonNull(batchNotifier, "batchNotifier");
	}

	boolean preemptionRequested() {
		return preemptionRequested;
	}

	synchronized void setPoolPreemption(RWScheduler.Pool pool, boolean requested) {
		int bit = poolBit(pool);
		preemptionPoolMask = requested ? preemptionPoolMask | bit : preemptionPoolMask & ~bit;
		preemptionRequested = preemptionPoolMask != 0;
	}

	synchronized void setPoolCompetition(RWScheduler.Pool pool, boolean competing) {
		int bit = poolBit(pool);
		boolean wasCompeting = competitionPoolMask != 0;
		competitionPoolMask = competing ? competitionPoolMask | bit : competitionPoolMask & ~bit;
		if (competitionPoolMask != 0) {
			competitionUntilNanos = Long.MAX_VALUE;
		} else if (wasCompeting) {
			competitionUntilNanos = saturatingDeadline(System.nanoTime(), competitionHoldNanos);
			notificationPending = true;
		}
	}

	synchronized void setBatchQueued(RWScheduler.Pool pool, boolean queued) {
		int bit = poolBit(pool);
		queuedBatchPoolMask = queued ? queuedBatchPoolMask | bit : queuedBatchPoolMask & ~bit;
	}

	private boolean competitionActiveUnsafe(long nowNanos) {
		if (competitionPoolMask != 0 || nowNanos < competitionUntilNanos) {
			return true;
		}
		competitionUntilNanos = Long.MIN_VALUE;
		return false;
	}

	private static long saturatingDeadline(long nowNanos, long delayNanos) {
		try {
			return Math.addExact(nowNanos, delayNanos);
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	void signalPendingAvailability() {
		if (!notificationPending) {
			return;
		}
		boolean notify;
		synchronized (this) {
			notify = notificationPending;
			notificationPending = false;
		}
		if (notify) {
			notifier.run();
		}
	}

	private static int poolBit(RWScheduler.Pool pool) {
		return 1 << Objects.requireNonNull(pool, "pool").ordinal();
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

	synchronized boolean canStartBatch(boolean ignoreLimits,
	                                  RWScheduler.Pool pool,
	                                  long nowNanos) {
		return batchStartAllowance(ignoreLimits, pool, nowNanos) > 0;
	}

	synchronized int batchStartAllowance(boolean ignoreLimits,
	                                    RWScheduler.Pool pool,
	                                    long nowNanos) {
		Objects.requireNonNull(pool, "pool");
		if (pool != RWScheduler.Pool.READ && pool != RWScheduler.Pool.WRITE) {
			return 0;
		}
		if (ignoreLimits) {
			return Integer.MAX_VALUE;
		}
		var dataPool = resourcePool(pool);
		int allowance = Integer.MAX_VALUE;
		boolean competing = competitionActiveUnsafe(nowNanos);
		if (competing) {
			allowance = Math.max(0, competingMaximumActiveBatches(dataPool) - activeBatches(dataPool));
			if (dataPool == RWScheduler.Pool.WRITE && nowNanos < nextCompetingWriteNanos) {
				return 0;
			}
		}
		if (pressured) {
			if (nowNanos < nextBatchNanos) {
				return 0;
			}
			allowance = Math.min(allowance,
					Math.max(0, pressuredMaximumActiveBatches - activeBatches));
		}
		return allowance;
	}

	synchronized @Nullable BatchPermit tryStartBatch(boolean ignoreLimits,
	                                                RWScheduler.Pool pool,
	                                                long nowNanos) {
		if (!canStartBatch(ignoreLimits, pool, nowNanos)) {
			return null;
		}
		boolean startedUnderPressure = pressured && !ignoreLimits;
		boolean startedUnderCompetition = competitionActiveUnsafe(nowNanos) && !ignoreLimits;
		activeBatches++;
		incrementActiveBatches(resourcePool(pool));
		if (startedUnderPressure) {
			return startedUnderCompetition ? PRESSURED_AND_COMPETING_PERMIT : PRESSURED_PERMIT;
		}
		return startedUnderCompetition ? COMPETING_PERMIT : UNRESTRICTED_PERMIT;
	}

	void finishBatch(BatchPermit permit, RWScheduler.Pool completedPool) {
		int otherQueuedBatchPools;
		synchronized (this) {
			if (activeBatches <= 0) {
				throw new IllegalStateException("No active BATCH quantum to finish");
			}
			activeBatches--;
			decrementActiveBatches(resourcePool(completedPool));
			otherQueuedBatchPools = queuedBatchPoolMask & ~poolBit(completedPool);
			if (completedPool == RWScheduler.Pool.WRITE && permit.startedUnderCompetition()) {
				nextCompetingWriteNanos = saturatingDeadline(System.nanoTime(), competingWriteIntervalNanos);
			}
			if (permit.startedUnderPressure() && pressured) {
				long nowNanos = System.nanoTime();
				try {
					nextBatchNanos = Math.addExact(nowNanos, batchIntervalNanos);
				} catch (ArithmeticException overflow) {
					nextBatchNanos = Long.MAX_VALUE;
				}
			}
		}
		if (otherQueuedBatchPools != 0) {
			batchNotifier.accept(otherQueuedBatchPools);
		}
	}

	synchronized long nanosUntilBatchEligible(RWScheduler.Pool pool, long nowNanos) {
		Objects.requireNonNull(pool, "pool");
		var dataPool = resourcePool(pool);
		boolean competing = competitionActiveUnsafe(nowNanos);
		if (competing
				&& activeBatches(dataPool) >= competingMaximumActiveBatches(dataPool)) {
			if (competitionPoolMask != 0 || competitionUntilNanos == Long.MAX_VALUE) {
				return Long.MAX_VALUE;
			}
			long competitionRemaining = competitionUntilNanos - nowNanos;
			return competitionRemaining > 0L ? competitionRemaining : 0L;
		}
		long waitNanos = 0L;
		if (competing && dataPool == RWScheduler.Pool.WRITE && nowNanos < nextCompetingWriteNanos) {
			waitNanos = nextCompetingWriteNanos - nowNanos;
		}
		if (!pressured) {
			return waitNanos;
		}
		if (activeBatches >= pressuredMaximumActiveBatches) {
			return Long.MAX_VALUE;
		}
		if (nowNanos < nextBatchNanos) {
			waitNanos = Math.max(waitNanos, nextBatchNanos - nowNanos);
		}
		return waitNanos;
	}

	record BatchPermit(boolean startedUnderPressure, boolean startedUnderCompetition) {
	}

	private static RWScheduler.Pool resourcePool(RWScheduler.Pool pool) {
		return switch (Objects.requireNonNull(pool, "pool")) {
			case READ, WRITE -> pool;
			case CONTROL, PHYSICAL -> throw new IllegalArgumentException("BATCH work requires a data pool");
		};
	}

	private int competingMaximumActiveBatches(RWScheduler.Pool pool) {
		return pool == RWScheduler.Pool.READ
				? competingMaximumActiveReadBatches
				: competingMaximumActiveWriteBatches;
	}

	private int activeBatches(RWScheduler.Pool pool) {
		return pool == RWScheduler.Pool.READ ? activeReadBatches : activeWriteBatches;
	}

	private void incrementActiveBatches(RWScheduler.Pool pool) {
		if (pool == RWScheduler.Pool.READ) {
			activeReadBatches++;
		} else {
			activeWriteBatches++;
		}
	}

	private void decrementActiveBatches(RWScheduler.Pool pool) {
		if (pool == RWScheduler.Pool.READ) {
			if (activeReadBatches <= 0) {
				throw new IllegalStateException("No active read BATCH quantum to finish");
			}
			activeReadBatches--;
		} else {
			if (activeWriteBatches <= 0) {
				throw new IllegalStateException("No active write BATCH quantum to finish");
			}
			activeWriteBatches--;
		}
	}
}

record ExecutorSnapshot(int workerCount,
						int waitingWorkers,
                        int queuedTasks,
                        int activeTasks,
                        long acceptedTasks,
                        long startedTasks,
                        long completedTasks,
                        long failedTasks,
                        Map<WorkloadProfile, Integer> queuedByProfile,
                        Map<WorkloadProfile, Integer> activeByProfile,
                        Map<RWScheduler.TerminalOutcome, Long> outcomes,
						boolean batchDispatchLimited,
						int batchStartAllowance,
                        List<String> workerThreadNames,
                        boolean shutdown,
                        boolean terminated) {
}
