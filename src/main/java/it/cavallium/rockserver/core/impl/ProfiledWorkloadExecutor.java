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
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
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
	private static final WorkloadProfile[] PROFILES = WorkloadProfile.values();
	private static final RWScheduler.TerminalOutcome[] TERMINAL_OUTCOMES =
			RWScheduler.TerminalOutcome.values();
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
	private final int[] quanta = new int[PROFILES.length];
	private final int[] capacities = new int[PROFILES.length];
	private final int[] reservations = new int[PROFILES.length];
	private final NavigableSet<WorkloadTask> latencyQueue = new TreeSet<>(DEADLINE_ORDER);
	private final EnumMap<WorkloadProfile, LinkedHashSet<WorkloadTask>> queues;
	private final EnumMap<WorkloadProfile, CooperativeQueue> cooperativeQueues;
	private final NavigableSet<WorkloadTask> deadlineQueue = new TreeSet<>(DEADLINE_ORDER);
	private final CancellationIndex cancellationIndex = new CancellationIndex();
	private final int[] queued = new int[PROFILES.length];
	private final int[] active = new int[PROFILES.length];
	private final int[] parked = new int[PROFILES.length];
	private final int[] outstanding = new int[PROFILES.length];
	private final long[] submissionAttemptsByProfile = new long[PROFILES.length];
	private final int[] deficit = new int[PROFILES.length];
	private final long[] outcomes = new long[TERMINAL_OUTCOMES.length];
	private final ReentrantLock lock = new ReentrantLock();
	private final Condition workAvailable = lock.newCondition();
	private final List<Thread> workers = new ArrayList<>();
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
	private @Nullable CooperativeWorkloadTask firstCooperativeTask;
	private @Nullable CooperativeWorkloadTask lastCooperativeTask;
	private int startedWorkers;
	private int waitingWorkers;
	private int cooperativeTaskCount;
	private int queuedTotal;
	private int activeTotal;
	private int parkedTotal;
	private int outstandingTotal;
	private int competingTasks;
	// Updated under the scheduler lock on empty/nonempty queue transitions and read by running quantums.
	private volatile boolean localQueuedCompetition;
	private boolean publishedPreemption;
	private int latencyBurst;
	private int guaranteedCursor;
	private boolean guaranteedNeedsQuantum = true;
	private long acceptedTasks;
	private long submissionAttempts;
	private long startedTasks;
	private long completedTasks;
	private long failedTasks;
	private long sequence;

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
		for (var profile : GUARANTEED) {
			int quantum = Objects.requireNonNull(quanta.get(profile), "Missing DRR weight for " + profile);
			if (quantum < 1) {
				throw new IllegalArgumentException("DRR weight must be positive for " + profile);
			}
			this.quanta[profile.ordinal()] = quantum;
		}
		this.queues = new EnumMap<>(WorkloadProfile.class);
		this.cooperativeQueues = new EnumMap<>(WorkloadProfile.class);
		this.pressureController = Objects.requireNonNull(pressureController, "pressureController");
		this.databaseName = Objects.requireNonNull(databaseName, "databaseName");
		this.resourceKind = Objects.requireNonNull(resourceKind, "resourceKind");
		this.resourcePool = Objects.requireNonNull(resourcePool, "resourcePool");
		int reservationTotal = 0;
		for (var profile : PROFILES) {
			int capacity = capacities.getOrDefault(profile, 0);
			int reservation = reservations.getOrDefault(profile, 0);
			if (capacity < 0) {
				throw new IllegalArgumentException("Negative queue capacity for " + profile);
			}
			if (reservation < 0 || reservation > workerCount || (reservation > 0 && capacity == 0)) {
				throw new IllegalArgumentException("Invalid reservation for " + profile);
			}
			reservationTotal = Math.addExact(reservationTotal, reservation);
			this.capacities[profile.ordinal()] = capacity;
			this.reservations[profile.ordinal()] = reservation;
			if (profile != WorkloadProfile.LATENCY) {
				this.queues.put(profile, new LinkedHashSet<>());
				this.cooperativeQueues.put(profile, new CooperativeQueue());
			}
		}
		if (reservationTotal > workerCount) {
			throw new IllegalArgumentException("Reservation sum exceeds workerCount");
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
		if (profile != WorkloadProfile.ANALYTICAL
				&& profile != WorkloadProfile.INGEST
				&& profile != WorkloadProfile.BATCH) {
			throw new IllegalArgumentException(
					"Cooperative execution requires ANALYTICAL, INGEST, or BATCH work");
		}
		int cost = taskCost(estimatedBytes);
		var taskMetrics = metrics(profile, family);
		WorkloadTask task = null;
		RuntimeException admissionFailure = null;
		AdmissionResult admissionResult;
		List<TerminalAction> terminalActions = null;
		lock.lock();
		try {
			long nowMillis = System.currentTimeMillis();
			terminalActions = expireDueUnsafe(nowMillis, terminalActions);
			recordSubmissionAttemptUnsafe(profile);
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
			} else if (capacityUnsafe(profile) == 0 || queuedUnsafe(profile) >= capacityUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.OVERLOAD,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (outstandingAtLimitUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload outstanding limit is reached for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.OVERLOAD,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else {
				task = WorkloadTask.cooperative(this,
						profile,
						family,
						deadlineEpochMillis,
						sequence++,
						System.nanoTime(),
						cost,
						command,
						taskMetrics);
				enqueueCooperativeUnsafe(task, true);
				incrementOutstandingUnsafe(profile);
				admitCompetitionUnsafe(task);
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
		return Objects.requireNonNull(task, "Accepted cooperative task");
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
			recordSubmissionAttemptUnsafe(profile);
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
			} else if (capacityUnsafe(profile) == 0 || queuedUnsafe(profile) >= capacityUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				terminalActions = recordUnacceptedUnsafe(
						RWScheduler.TerminalOutcome.OVERLOAD,
						command,
						taskMetrics,
						admissionFailure,
						terminalActions);
			} else if (outstandingAtLimitUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload outstanding limit is reached for " + profile + " " + family);
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
						sequence++,
						System.nanoTime(),
						cost,
						command,
						cancellationState,
						taskMetrics);
				boolean becomesDeadlineHead = task.hasDeadline()
						&& (deadlineQueue.isEmpty() || DEADLINE_ORDER.compare(task, deadlineQueue.first()) < 0);
				enqueueUnsafe(task);
				incrementOutstandingUnsafe(profile);
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
					if (task == null && shutdown && queuedTotal == 0 && cooperativeTaskCount == 0) {
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
			WorkloadPressureController.BatchPermit batchPermit = null;
			if (task.profile() == WorkloadProfile.BATCH) {
				batchPermit = pressureController.tryStartBatch(shutdown, resourcePool, System.nanoTime());
				if (batchPermit == null) {
					advanceGuaranteedCursor();
					return null;
				}
			}
			// A BATCH permit must exist before the one-shot dispatch claim. Cancellation
			// can therefore either remove queued ownership or lose to execution ownership;
			// it can never consume a pacing interval without running a quantum.
			if (!task.claimForDispatch()) {
				if (batchPermit != null) {
					pressureController.abortBatch(batchPermit, resourcePool);
				}
				if (!cancelSelectedUnsafe(task, terminalActions)) {
					throw new IllegalStateException("Dispatch claim failed without cancellation");
				}
				continue;
			}
			unlinkUnsafe(task);
			if (batchPermit != null) {
				task.batchPermit(batchPermit);
			}
			commitSelectionUnsafe(task);
			active[task.profile().ordinal()]++;
			activeTotal++;
			task.markActive();
			if (task.isCooperative()) {
				if (task.markStarted()) {
					startedTasks++;
				}
			} else {
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
		var outcome = RWScheduler.TerminalOutcome.FAILURE;
		try {
			EXECUTING_POOL.set(this);
			task.command().run();
			outcome = RWScheduler.TerminalOutcome.RUN;
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
				if (outcome == RWScheduler.TerminalOutcome.RUN) {
					metrics.recordRunOutcome();
				} else {
					metrics.recordOutcome(outcome);
				}
				metrics.queueWait().record(executionStart - task.enqueuedNanos());
				metrics.quantum().increment();
				metrics.execution().record(System.nanoTime() - executionStart);
			} finally {
				finishActive(task, outcome);
			}
		}
	}

	private void runCooperativeDispatched(WorkloadTask task) {
		var metrics = task.metrics();
		long executionStart = System.nanoTime();
		var previousPool = EXECUTING_POOL.get();
		RWScheduler.CooperativeResult result = RWScheduler.CooperativeResult.COMPLETE;
		try {
			EXECUTING_POOL.set(this);
			result = Objects.requireNonNull(task.cooperativeCommand().runCooperatively(task),
					"Cooperative task returned no result");
		} catch (VirtualMachineError fatal) {
			task.fail(new RejectedExecutionException("Cooperative workload quantum failed", fatal));
			throw fatal;
		} catch (Throwable error) {
			recordTaskFailure(task, error);
			var failure = error instanceof RuntimeException runtimeException
					? runtimeException
					: new RejectedExecutionException("Cooperative workload quantum failed", error);
			task.fail(failure);
		} finally {
			if (previousPool == null) {
				EXECUTING_POOL.set(null);
			} else {
				EXECUTING_POOL.set(previousPool);
			}
			task.recordCooperativeQuantum(executionStart - task.enqueuedNanos(),
					System.nanoTime() - executionStart);
			var terminalAction = finishCooperative(task, result);
			if (task.state() == TaskState.TERMINAL) {
				task.flushCooperativeMetrics();
			}
			if (terminalAction != null) {
				completeTerminalAction(terminalAction);
			} else if (task.outcome() == RWScheduler.TerminalOutcome.RUN) {
				if (task.command() instanceof RWScheduler.CooperativeCompletionTask) {
					completeCooperativeSuccess(task);
				} else {
					metrics.recordRunOutcome();
				}
			}
		}
	}

	private void finishActive(WorkloadTask task, RWScheduler.TerminalOutcome outcome) {
		var batchPermit = task.takeBatchPermit();
		if (batchPermit != null) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		lock.lock();
		try {
			if (task.outcome() != null) {
				throw new IllegalStateException("Active workload task already has a terminal outcome");
			}
			active[task.profile().ordinal()]--;
			activeTotal--;
			completeCompetitionUnsafe(task);
			task.outcome(outcome);
			decrementOutstandingUnsafe(task.profile());
			completedTasks++;
			task.markTerminal();
			recordOutcomeUnsafe(outcome);
			refreshPreemptionUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
	}

	private @Nullable TerminalAction finishCooperative(WorkloadTask task,
	                                                   RWScheduler.CooperativeResult result) {
		var batchPermit = task.takeBatchPermit();
		if (batchPermit != null) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		TerminalAction terminalAction = null;
		boolean notifyCompetitionEnded = false;
		lock.lock();
		try {
			if (task.state() != TaskState.ACTIVE) {
				throw new IllegalStateException("Cooperative task is not active");
			}
			active[task.profile().ordinal()]--;
			activeTotal--;
			if (task.requestedOutcome() != null) {
				terminalAction = terminateCooperativeUnsafe(task,
						Objects.requireNonNull(task.requestedOutcome()),
						Objects.requireNonNull(task.terminationFailure()));
			} else {
				switch (result) {
					case COMPLETE -> {
						terminalAction = terminateCooperativeUnsafe(task,
								RWScheduler.TerminalOutcome.RUN,
								null);
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
							markParkedUnsafe(task);
						}
					}
				}
			}
			refreshPreemptionUnsafe();
			workAvailable.signal();
			notifyCompetitionEnded = task.profile() != WorkloadProfile.BATCH
					&& task.state() == TaskState.TERMINAL;
		} finally {
			lock.unlock();
		}
		if (notifyCompetitionEnded) {
			pressureController.signalPendingAvailability();
		}
		return terminalAction;
	}

	private void refreshCooperativeSequenceUnsafe(WorkloadTask task) {
		task.refreshSequence(sequence++, System.nanoTime());
	}

	private void resumeCooperative(WorkloadTask task) {
		lock.lock();
		try {
			switch (task.state()) {
				case ACTIVE -> task.requestResume();
				case PARKED -> {
					if (task.requestedOutcome() == null) {
						unmarkParkedUnsafe(task);
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

	private boolean cancelCooperative(WorkloadTask task) {
		return requestCooperativeTermination(task,
				RWScheduler.TerminalOutcome.CANCELLATION,
				new CancellationException("Cooperative workload submission cancelled"));
	}

	private boolean requestCooperativeTermination(WorkloadTask task,
	                                              RWScheduler.TerminalOutcome outcome,
	                                              RuntimeException failure) {
		if (outcome == RWScheduler.TerminalOutcome.RUN
				|| outcome == RWScheduler.TerminalOutcome.OVERLOAD) {
			throw new IllegalArgumentException("Invalid admitted cooperative terminal outcome: " + outcome);
		}
		TerminalAction terminalAction = null;
		boolean selected = false;
		lock.lock();
		try {
			if (task.state() == TaskState.TERMINAL || task.requestedOutcome() != null) {
				return false;
			}
			switch (task.state()) {
				case QUEUED -> {
					unlinkUnsafe(task);
					terminalAction = terminateCooperativeUnsafe(task,
							outcome,
							failure);
					selected = terminalAction != null;
				}
				case PARKED -> {
					terminalAction = terminateCooperativeUnsafe(task, outcome, failure);
					selected = terminalAction != null;
				}
				case ACTIVE -> selected = task.requestTermination(outcome, failure);
				case TERMINAL -> {
				}
			}
			refreshPreemptionUnsafe();
			workAvailable.signal();
		} finally {
			lock.unlock();
		}
		if (terminalAction != null && task.profile() != WorkloadProfile.BATCH) {
			pressureController.signalPendingAvailability();
		}
		if (terminalAction != null) {
			completeTerminalAction(terminalAction);
		}
		return selected;
	}

	private void refreshPreemptionUnsafe() {
		boolean requested = localQueuedCompetition
				|| activeTotal > activeUnsafe(WorkloadProfile.BATCH);
		if (requested != publishedPreemption) {
			publishedPreemption = requested;
			pressureController.setPoolPreemption(resourcePool, requested);
		}
	}

	private boolean cooperativePreemptionRequested(WorkloadProfile profile) {
		// The controller's aggregate BATCH signal already includes this pool's published
		// queued-contention bit, so the raw-scan hot path needs only one volatile read.
		return profile == WorkloadProfile.BATCH
				? pressureController.preemptionRequested()
				: localQueuedCompetition;
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
				deficit[profile.ordinal()] = 0;
				advanceGuaranteedCursor();
				continue;
			}
			if ((reservationOnly && !hasReservationDeficitUnsafe(profile)) || !isEligibleUnsafe(profile)) {
				advanceGuaranteedCursor();
				continue;
			}
			if (guaranteedNeedsQuantum) {
				int profileIndex = profile.ordinal();
				deficit[profileIndex] = Math.min(MAX_DEFICIT,
						deficit[profileIndex] + quanta[profileIndex]);
				guaranteedNeedsQuantum = false;
			}
			var head = peekUnsafe(profile);
			if (deficit[profile.ordinal()] < head.cost()) {
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
		int profileIndex = task.profile().ordinal();
		deficit[profileIndex] -= task.cost();
		if (queuedUnsafe(task.profile()) == 0) {
			deficit[profileIndex] = 0;
			advanceGuaranteedCursor();
		} else if (deficit[profileIndex] < peekUnsafe(task.profile()).cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void discardSelectionUnsafe(WorkloadTask task) {
		if (!isGuaranteedProfile(task.profile())) {
			return;
		}
		int profileIndex = task.profile().ordinal();
		if (queuedUnsafe(task.profile()) == 0) {
			deficit[profileIndex] = 0;
			advanceGuaranteedCursor();
		} else if (deficit[profileIndex] < peekUnsafe(task.profile()).cost()) {
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
		int profileIndex = profile.ordinal();
		return reservations[profileIndex] > active[profileIndex] && queued[profileIndex] > 0;
	}

	private boolean isEligibleUnsafe(WorkloadProfile profile) {
		if (queuedUnsafe(profile) == 0 || activeTotal >= workerCount) {
			return false;
		}
		if (profile == WorkloadProfile.ANALYTICAL && activeUnsafe(profile) >= analyticalLimit) {
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
			var task = cancellationIndex.first(command, profile, family);
			if (task != null) {
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
			return activeUnsafe(profile);
		} finally {
			lock.unlock();
		}
	}

	int parked(WorkloadProfile profile) {
		lock.lock();
		try {
			return parked[profile.ordinal()];
		} finally {
			lock.unlock();
		}
	}

	int outstanding(WorkloadProfile profile) {
		lock.lock();
		try {
			return outstanding[profile.ordinal()];
		} finally {
			lock.unlock();
		}
	}

	long submissionAttempts(WorkloadProfile profile) {
		lock.lock();
		try {
			return submissionAttemptsByProfile[profile.ordinal()];
		} finally {
			lock.unlock();
		}
	}

	int capacity(WorkloadProfile profile) {
		return capacityUnsafe(profile);
	}

	long outstandingLimit(WorkloadProfile profile) {
		return (long) capacityUnsafe(profile) + workerCount;
	}

	int workerCount() {
		return workerCount;
	}

	ExecutorSnapshot snapshot() {
		lock.lock();
		try {
			validateAccountingUnsafe();
			int batchQueued = queuedUnsafe(WorkloadProfile.BATCH);
			int batchStartAllowance = pressureController.batchStartAllowance(
					shutdown, resourcePool, System.nanoTime());
			return new ExecutorSnapshot(
					workerCount,
					waitingWorkers,
					queuedTotal,
					activeTotal,
					parkedTotal,
					outstandingTotal,
					submissionAttempts,
					acceptedTasks,
					startedTasks,
					completedTasks,
					failedTasks,
					snapshot(submissionAttemptsByProfile),
					snapshot(queued),
					snapshot(active),
					snapshot(parked),
					snapshot(outstanding),
					snapshotOutcomes(outcomes),
					batchQueued > batchStartAllowance,
					batchStartAllowance,
					workers.stream().map(Thread::getName).toList(),
					shutdown,
					terminated);
		} finally {
			lock.unlock();
		}
	}

	private static final class CancellationEntry {

		private @Nullable Runnable command;
		private byte profile;
		private byte family;
		private int hash;
		private int mappedCount;
		private @Nullable WorkloadTask firstQueued;
		private @Nullable WorkloadTask lastQueued;
		private @Nullable CancellationEntry bucketNext;
		private @Nullable CancellationEntry freeNext;
	}

	/**
	 * Copy the high-frequency scheduler counters used by benchmark and diagnostic samplers into a
	 * caller-owned primitive buffer. The full immutable {@link ExecutorSnapshot} remains the
	 * authoritative drain/conservation view; this path avoids materializing its profile maps when a
	 * sampler only needs primitive utilization counters.
	 */
	void copyPoolTelemetry(long[] target) {
		Objects.requireNonNull(target, "target");
		int profileCount = PROFILES.length;
		int requiredLength = RWScheduler.POOL_TELEMETRY_SCALARS + profileCount * 2;
		if (target.length < requiredLength) {
			throw new IllegalArgumentException("Pool telemetry buffer requires at least "
					+ requiredLength + " entries");
		}
		lock.lock();
		try {
			long terminalOutcomes = validateAccountingUnsafe();
			int batchQueued = queuedUnsafe(WorkloadProfile.BATCH);
			int batchStartAllowance = pressureController.batchStartAllowance(
					shutdown, resourcePool, System.nanoTime());
			target[RWScheduler.POOL_TELEMETRY_WORKER_COUNT] = workerCount;
			target[RWScheduler.POOL_TELEMETRY_WAITING_WORKERS] = waitingWorkers;
			target[RWScheduler.POOL_TELEMETRY_QUEUED_TASKS] = queuedTotal;
			target[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS] = activeTotal;
			target[RWScheduler.POOL_TELEMETRY_PARKED_TASKS] = parkedTotal;
			target[RWScheduler.POOL_TELEMETRY_OUTSTANDING_TASKS] = outstandingTotal;
			target[RWScheduler.POOL_TELEMETRY_SUBMISSION_ATTEMPTS] = submissionAttempts;
			target[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS] = acceptedTasks;
			target[RWScheduler.POOL_TELEMETRY_STARTED_TASKS] = startedTasks;
			target[RWScheduler.POOL_TELEMETRY_COMPLETED_TASKS] = completedTasks;
			target[RWScheduler.POOL_TELEMETRY_FAILED_TASKS] = failedTasks;
			target[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES] = terminalOutcomes;
			target[RWScheduler.POOL_TELEMETRY_BATCH_LIMITED] =
					batchQueued > batchStartAllowance ? 1L : 0L;
			target[RWScheduler.POOL_TELEMETRY_BATCH_ALLOWANCE] = batchStartAllowance;
			for (var profile : PROFILES) {
				int profileIndex = profile.ordinal();
				target[RWScheduler.POOL_TELEMETRY_SCALARS + profileIndex] = queued[profileIndex];
				target[RWScheduler.POOL_TELEMETRY_SCALARS + profileCount + profileIndex] =
						active[profileIndex];
			}
		} finally {
			lock.unlock();
		}
	}

	private long validateAccountingUnsafe() {
		if ((long) queuedTotal + activeTotal + parkedTotal != outstandingTotal) {
			throw new IllegalStateException("Pool outstanding accounting mismatch in " + poolName);
		}
		long terminalOutcomes = 0L;
		for (long outcome : outcomes) {
			terminalOutcomes += outcome;
		}
		if (terminalOutcomes + outstandingTotal != submissionAttempts) {
			throw new IllegalStateException("Pool submission conservation mismatch in " + poolName);
		}
		for (var profile : PROFILES) {
			int profileIndex = profile.ordinal();
			int profileOutstanding = outstanding[profileIndex];
			if ((long) queued[profileIndex] + active[profileIndex] + parked[profileIndex] != profileOutstanding) {
				throw new IllegalStateException("Profile outstanding accounting mismatch for " + profile);
			}
			if (profileOutstanding > (long) capacities[profileIndex] + workerCount) {
				throw new IllegalStateException("Profile outstanding limit exceeded for " + profile);
			}
		}
		return terminalOutcomes;
	}

	private static Map<WorkloadProfile, Integer> snapshot(int[] values) {
		var result = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : PROFILES) {
			result.put(profile, values[profile.ordinal()]);
		}
		return Map.copyOf(result);
	}

	private static Map<WorkloadProfile, Long> snapshot(long[] values) {
		var result = new EnumMap<WorkloadProfile, Long>(WorkloadProfile.class);
		for (var profile : PROFILES) {
			result.put(profile, values[profile.ordinal()]);
		}
		return Map.copyOf(result);
	}

	private static Map<RWScheduler.TerminalOutcome, Long> snapshotOutcomes(long[] values) {
		var result = new EnumMap<RWScheduler.TerminalOutcome, Long>(RWScheduler.TerminalOutcome.class);
		for (var outcome : TERMINAL_OUTCOMES) {
			result.put(outcome, values[outcome.ordinal()]);
		}
		return Map.copyOf(result);
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
		return queued[profile.ordinal()];
	}

	private int activeUnsafe(WorkloadProfile profile) {
		return active[profile.ordinal()];
	}

	private int capacityUnsafe(WorkloadProfile profile) {
		return capacities[profile.ordinal()];
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
		cancellationIndex.map(task);
		cancellationIndex.link(task);
		int profileIndex = task.profile().ordinal();
		int previousQueued = queued[profileIndex]++;
		if (task.profile() == WorkloadProfile.BATCH && previousQueued == 0) {
			pressureController.setBatchQueued(resourcePool, true);
		}
		incrementQueuedTotalUnsafe();
		task.markQueued();
	}

	private void enqueueCooperativeUnsafe(WorkloadTask task, boolean initialAdmission) {
		var queue = cooperativeQueues.get(task.profile());
		if (queue == null) {
			throw new IllegalStateException("No cooperative queue for " + task.profile());
		}
		queue.addLast(task);
		if (initialAdmission) {
			admitCooperativeTaskUnsafe(task);
			if (task.hasDeadline()) {
				if (!deadlineQueue.add(task)) {
					throw new IllegalStateException("Duplicate cooperative workload deadline sequence "
							+ task.sequence());
				}
				task.markDeadlineIndexed();
			}
			cancellationIndex.map(task);
		}
		cancellationIndex.link(task);
		int profileIndex = task.profile().ordinal();
		int previousQueued = queued[profileIndex]++;
		if (task.profile() == WorkloadProfile.BATCH && previousQueued == 0) {
			pressureController.setBatchQueued(resourcePool, true);
		}
		incrementQueuedTotalUnsafe();
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
		cancellationIndex.unlink(task);
		cancellationIndex.unmap(task);
		int profileIndex = task.profile().ordinal();
		int remainingQueued = --queued[profileIndex];
		if (task.profile() == WorkloadProfile.BATCH && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		decrementQueuedTotalUnsafe();
	}

	private void unlinkCooperativeQueuedUnsafe(WorkloadTask task) {
		var queue = cooperativeQueues.get(task.profile());
		if (queue == null || !queue.remove(task)) {
			throw new IllegalStateException("Cooperative workload task is not queued: " + task.sequence());
		}
		cancellationIndex.unlink(task);
		int profileIndex = task.profile().ordinal();
		int remainingQueued = --queued[profileIndex];
		if (task.profile() == WorkloadProfile.BATCH && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		decrementQueuedTotalUnsafe();
	}

	private void admitCooperativeTaskUnsafe(WorkloadTask task) {
		var cooperativeTask = (CooperativeWorkloadTask) task;
		if (cooperativeTask.cooperativeAdmitted
				|| cooperativeTask.previousLifetime != null
				|| cooperativeTask.nextLifetime != null) {
			throw new IllegalStateException("Cooperative workload task is already admitted");
		}
		if (lastCooperativeTask == null) {
			firstCooperativeTask = cooperativeTask;
		} else {
			lastCooperativeTask.nextLifetime = cooperativeTask;
			cooperativeTask.previousLifetime = lastCooperativeTask;
		}
		lastCooperativeTask = cooperativeTask;
		cooperativeTask.cooperativeAdmitted = true;
		cooperativeTaskCount++;
	}

	private void removeCooperativeTaskUnsafe(WorkloadTask task) {
		var cooperativeTask = (CooperativeWorkloadTask) task;
		if (!cooperativeTask.cooperativeAdmitted) {
			throw new IllegalStateException("Cooperative workload task is not admitted");
		}
		var previous = cooperativeTask.previousLifetime;
		var next = cooperativeTask.nextLifetime;
		if (previous == null) {
			if (firstCooperativeTask != cooperativeTask) {
				throw new IllegalStateException("Cooperative workload task is not lifetime-indexed");
			}
			firstCooperativeTask = next;
		} else {
			previous.nextLifetime = next;
		}
		if (next == null) {
			if (lastCooperativeTask != cooperativeTask) {
				throw new IllegalStateException("Cooperative workload task is not lifetime-indexed");
			}
			lastCooperativeTask = previous;
		} else {
			next.previousLifetime = previous;
		}
		cooperativeTask.previousLifetime = null;
		cooperativeTask.nextLifetime = null;
		cooperativeTask.cooperativeAdmitted = false;
		if (--cooperativeTaskCount < 0) {
			throw new IllegalStateException("Cooperative workload task count underflow");
		}
	}

	private void incrementQueuedTotalUnsafe() {
		if (queuedTotal++ == 0) {
			localQueuedCompetition = true;
		}
	}

	private void decrementQueuedTotalUnsafe() {
		if (--queuedTotal == 0) {
			localQueuedCompetition = false;
		}
	}

	private void recordSubmissionAttemptUnsafe(WorkloadProfile profile) {
		submissionAttempts++;
		submissionAttemptsByProfile[profile.ordinal()]++;
	}

	private boolean outstandingAtLimitUnsafe(WorkloadProfile profile) {
		int profileIndex = profile.ordinal();
		return outstanding[profileIndex] >= (long) capacities[profileIndex] + workerCount;
	}

	private void incrementOutstandingUnsafe(WorkloadProfile profile) {
		int index = profile.ordinal();
		int next = outstanding[index] + 1;
		if (next > (long) capacities[index] + workerCount) {
			throw new IllegalStateException("Outstanding workload limit exceeded for " + profile);
		}
		outstanding[index] = next;
		outstandingTotal++;
	}

	private void decrementOutstandingUnsafe(WorkloadProfile profile) {
		int index = profile.ordinal();
		int current = outstanding[index];
		if (current <= 0 || outstandingTotal <= 0) {
			throw new IllegalStateException("Outstanding workload count underflow for " + profile);
		}
		outstanding[index] = current - 1;
		outstandingTotal--;
	}

	private void markParkedUnsafe(WorkloadTask task) {
		if (task.state() != TaskState.ACTIVE) {
			throw new IllegalStateException("Only an active cooperative task can park");
		}
		parked[task.profile().ordinal()]++;
		parkedTotal++;
		task.markParked();
	}

	private void unmarkParkedUnsafe(WorkloadTask task) {
		if (task.state() != TaskState.PARKED) {
			throw new IllegalStateException("Cooperative task is not parked");
		}
		int index = task.profile().ordinal();
		int current = parked[index];
		if (current <= 0 || parkedTotal <= 0) {
			throw new IllegalStateException("Parked workload count underflow for " + task.profile());
		}
		parked[index] = current - 1;
		parkedTotal--;
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
			decrementOutstandingUnsafe(task.profile());
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
			cancellationIndex.unlink(task);
		}
		cancellationIndex.unmap(task);
		if (task.state() == TaskState.PARKED) {
			unmarkParkedUnsafe(task);
		}
		removeCooperativeTaskUnsafe(task);
		completeCompetitionUnsafe(task);
		task.outcome(outcome);
		decrementOutstandingUnsafe(task.profile());
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
				failure,
				outcome);
	}

	private void recordOutcomeUnsafe(RWScheduler.TerminalOutcome outcome) {
		outcomes[outcome.ordinal()]++;
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
			if (action.outcome() == RWScheduler.TerminalOutcome.RUN && action.failure() == null) {
				((RWScheduler.CooperativeCompletionTask) action.command()).completeCooperatively();
			} else {
				var failure = Objects.requireNonNull(action.failure(), "Non-RUN terminal failure");
				if (action.command() instanceof RWScheduler.RejectionAwareTask rejectionAwareTask) {
					rejectionAwareTask.reject(failure);
				} else if (action.command() instanceof CompletableFuture<?> future) {
					future.completeExceptionally(failure);
				} else if (action.command() instanceof Future<?> future) {
					future.cancel(false);
				}
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

	private void completeCooperativeSuccess(WorkloadTask task) {
		try {
			((RWScheduler.CooperativeCompletionTask) task.command()).completeCooperatively();
		} catch (Throwable terminalFailure) {
			recordInfrastructureFailure("Failed to complete RUN workload submission in " + poolName,
					terminalFailure);
		}
		try {
			if (task.command() instanceof Disposable disposable && !disposable.isDisposed()) {
				disposable.dispose();
			}
		} catch (Throwable disposalFailure) {
			recordInfrastructureFailure("Failed to dispose RUN workload submission in " + poolName,
					disposalFailure);
		}
		task.metrics().recordRunOutcome();
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
		for (var profile : PROFILES) {
			if (capacityUnsafe(profile) == 0) {
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
		var outcomeMetrics = new CounterHandle[TERMINAL_OUTCOMES.length];
		for (var outcome : RWScheduler.TerminalOutcome.values()) {
			outcomeMetrics[outcome.ordinal()] = registerCounter(registry,
					"rockserver.workload.outcomes",
					tagsWith(tags, "outcome", metricName(outcome)));
		}
		var rejectionMetrics = new CounterHandle[TERMINAL_OUTCOMES.length];
		for (var outcome : List.of(RWScheduler.TerminalOutcome.DEADLINE,
				RWScheduler.TerminalOutcome.OVERLOAD,
				RWScheduler.TerminalOutcome.SHUTDOWN)) {
			rejectionMetrics[outcome.ordinal()] = registerCounter(registry,
					"rockserver.workload.rejections",
					tagsWith(tags, "reason", rejectionReason(outcome)));
		}
		var admissionMetrics = new CounterHandle[AdmissionResult.values().length];
		for (var result : AdmissionResult.values()) {
			admissionMetrics[result.ordinal()] = registerCounter(registry,
					"rockserver.workload.admission",
					tagsWith(tags, "result", metricName(result)));
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
		for (var profile : PROFILES) {
			if (capacityUnsafe(profile) == 0) {
				continue;
			}
			String[] tags = {
					"database", databaseName,
					"resource", resourceKind,
					"profile", metricName(profile)
			};
			registerGauge(registry,
					"rockserver.workload.submission.attempts",
					pool -> pool.submissionAttempts(profile),
					tags);
			registerGauge(registry, "rockserver.workload.queued", pool -> pool.queued(profile), tags);
			registerGauge(registry, "rockserver.workload.active", pool -> pool.active(profile), tags);
			registerGauge(registry, "rockserver.workload.parked", pool -> pool.parked(profile), tags);
			registerGauge(registry, "rockserver.workload.outstanding", pool -> pool.outstanding(profile), tags);
			registerGauge(registry,
					"rockserver.workload.queue.capacity",
					pool -> pool.capacity(profile),
					tags);
			registerGauge(registry,
					"rockserver.workload.outstanding.limit",
					pool -> pool.outstandingLimit(profile),
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
			case RUN, FAILURE, CANCELLATION ->
					throw new IllegalArgumentException("Not a rejection outcome: " + outcome);
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
			for (var profile : PROFILES) {
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
			WorkloadTask task = firstCooperativeTask;
			while (task != null) {
				var nextTask = ((CooperativeWorkloadTask) task).nextLifetime;
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
				task = nextTask;
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

	private static class WorkloadTask implements CooperativeTerminationHandle,
			RWScheduler.CooperativeContext {

		private static final VarHandle REQUESTED_TERMINATION;

		static {
			try {
				REQUESTED_TERMINATION = MethodHandles.lookup().findVarHandle(
						WorkloadTask.class, "requestedTermination", RequestedTermination.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final @Nullable ProfiledWorkloadExecutor owner;
		private final WorkloadProfile profile;
		private final OperationFamily family;
		private final long deadlineEpochMillis;
		private final long deadlineSequence;
		private long sequence;
		private long enqueuedNanos;
		private final int cost;
		private final Runnable command;
		private final @Nullable CancellationState cancellationState;
		private final TaskMetrics metrics;
		private final boolean cooperative;
		private @Nullable WorkloadTask previousCancellation;
		private @Nullable WorkloadTask nextCancellation;
		private @Nullable WorkloadTask previousCooperative;
		private @Nullable WorkloadTask nextCooperative;
		private @Nullable WorkloadPressureController.BatchPermit batchPermit;
		private volatile @Nullable RWScheduler.TerminalOutcome outcome;
		private volatile @Nullable RequestedTermination requestedTermination;
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
		private @Nullable CancellationEntry cancellationEntry;

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
			this.cancellationState = cancellationState;
			this.metrics = metrics;
			this.cooperative = cooperative;
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
			return new CooperativeWorkloadTask(owner,
					profile,
					family,
					deadlineEpochMillis,
					sequence,
					enqueuedNanos,
					cost,
					command,
					metrics);
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

		private @Nullable CancellationEntry cancellationEntry() {
			return cancellationEntry;
		}

		private void mapCancellation(CancellationEntry cancellationEntry) {
			this.cancellationEntry = cancellationEntry;
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
			cancellationEntry = null;
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

		private boolean requestTermination(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
			if (!cooperative) {
				throw new IllegalStateException("Non-cooperative workload task");
			}
			return REQUESTED_TERMINATION.compareAndSet(
					this, null, new RequestedTermination(outcome, failure));
		}

		private @Nullable RWScheduler.TerminalOutcome requestedOutcome() {
			if (!cooperative) {
				throw new IllegalStateException("Non-cooperative workload task");
			}
			var requested = requestedTermination;
			return requested == null ? null : requested.outcome();
		}

		@Override
		public boolean preemptionRequested() {
			return Objects.requireNonNull(owner).cooperativePreemptionRequested(profile);
		}

		@Override
		public boolean terminationRequested() {
			if (!cooperative) {
				throw new IllegalStateException("Non-cooperative workload task");
			}
			if (requestedTermination != null) {
				return true;
			}
			if (hasDeadline() && System.currentTimeMillis() >= deadlineEpochMillis) {
				REQUESTED_TERMINATION.compareAndSet(this, null, new RequestedTermination(
						RWScheduler.TerminalOutcome.DEADLINE,
						deadlineFailure("Cooperative workload deadline expired while active")));
				return true;
			}
			return false;
		}

		@Override
		public @Nullable RuntimeException terminationFailure() {
			if (!cooperative) {
				throw new IllegalStateException("Non-cooperative workload task");
			}
			var requested = requestedTermination;
			return requested == null ? null : requested.failure();
		}

		@Override
		public boolean fail(RuntimeException failure) {
			return requestTermination(RWScheduler.TerminalOutcome.FAILURE,
					Objects.requireNonNull(failure, "failure"));
		}

		@Override
		public void resume() {
			Objects.requireNonNull(owner).resumeCooperative(this);
		}

		@Override
		public boolean cancel() {
			return Objects.requireNonNull(owner).cancelCooperative(this);
		}

		@Override
		public void dispose() {
			cancel();
		}

		@Override
		public void terminate(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
			Objects.requireNonNull(owner).requestCooperativeTermination(
					this, Objects.requireNonNull(outcome), Objects.requireNonNull(failure));
		}

		@Override
		public boolean isDisposed() {
			return state == TaskState.TERMINAL
					|| requestedTermination != null;
		}
	}

	private static final class CooperativeWorkloadTask extends WorkloadTask {

		private @Nullable CooperativeWorkloadTask previousLifetime;
		private @Nullable CooperativeWorkloadTask nextLifetime;
		private boolean cooperativeAdmitted;

		private CooperativeWorkloadTask(ProfiledWorkloadExecutor owner,
		                                WorkloadProfile profile,
		                                OperationFamily family,
		                                long deadlineEpochMillis,
		                                long sequence,
		                                long enqueuedNanos,
		                                int cost,
		                                Runnable command,
		                                TaskMetrics metrics) {
			super(owner,
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
		private static final VarHandle STATE;

		static {
			try {
				STATE = MethodHandles.lookup().findVarHandle(CancellationState.class, "state", int.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private volatile int state = PENDING;

		boolean cancel() {
			return STATE.compareAndSet(this, PENDING, CANCELLED);
		}

		boolean isCancellationRequested() {
			return state == CANCELLED;
		}

		boolean claimForDispatch() {
			return STATE.compareAndSet(this, PENDING, CLAIMED) || state == CLAIMED;
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

	/**
	 * Pooled identity index for queued cancellation. Entries stay stable for each task lifetime and
	 * are reused after drain, avoiding per-submission index allocation once the concurrent high-water
	 * mark is warm. Duplicate submissions use intrusive task links. Cooperative tasks remain mapped
	 * while active or parked, but only linked while queued.
	 */
	private static final class CancellationIndex {

		private static final int INITIAL_CAPACITY = 16;
		private static final int LOAD_NUMERATOR = 3;
		private static final int LOAD_DENOMINATOR = 4;

		private CancellationEntry[] buckets = new CancellationEntry[INITIAL_CAPACITY];
		private int resizeThreshold = resizeThreshold(INITIAL_CAPACITY);
		private int size;
		private @Nullable CancellationEntry freeHead;

		private @Nullable WorkloadTask first(Runnable command,
		                                             WorkloadProfile profile,
		                                             OperationFamily family) {
			var entry = findEntry(command, profile.ordinal(), family.ordinal());
			return entry == null ? null : entry.firstQueued;
		}

		private void map(WorkloadTask task) {
			if (task.cancellationEntry != null) {
				throw new IllegalStateException("Workload task is already cancellation-indexed: "
						+ task.sequence());
			}
			int profile = task.profile().ordinal();
			int family = task.family().ordinal();
			int hash = hash(task.command(), profile, family);
			var entry = findEntry(task.command(), profile, family, hash);
			if (entry == null) {
				ensureInsertCapacity();
				entry = allocateEntry();
				entry.command = task.command();
				entry.profile = checkedOrdinal(profile);
				entry.family = checkedOrdinal(family);
				entry.hash = hash;
				int bucket = hash & (buckets.length - 1);
				entry.bucketNext = buckets[bucket];
				buckets[bucket] = entry;
				size++;
			}
			int mapped = entry.mappedCount;
			if (mapped == Integer.MAX_VALUE) {
				throw new IllegalStateException("Cancellation mapping count overflow");
			}
			entry.mappedCount = mapped + 1;
			task.mapCancellation(entry);
		}

		private void link(WorkloadTask task) {
			var entry = task.cancellationEntry;
			if (task.cancellationLinked
					|| task.previousCancellation != null
					|| task.nextCancellation != null) {
				throw new IllegalStateException("Workload task is already cancellation-linked: "
						+ task.sequence());
			}
			var previous = entry.lastQueued;
			if (previous == null) {
				entry.firstQueued = task;
			} else {
				previous.nextCancellation = task;
				task.previousCancellation = previous;
			}
			entry.lastQueued = task;
			task.markCancellationLinked();
		}

		private void unlink(WorkloadTask task) {
			var entry = task.cancellationEntry;
			var previous = task.previousCancellation;
			var next = task.nextCancellation;
			if (previous == null) {
				if (entry.firstQueued != task) {
					throw new IllegalStateException("Workload task is not the cancellation head: "
							+ task.sequence());
				}
				entry.firstQueued = next;
			} else {
				previous.nextCancellation = next;
			}
			if (next == null) {
				if (entry.lastQueued != task) {
					throw new IllegalStateException("Workload task is not the cancellation tail: "
							+ task.sequence());
				}
				entry.lastQueued = previous;
			} else {
				next.previousCancellation = previous;
			}
			task.previousCancellation = null;
			task.nextCancellation = null;
			task.markCancellationUnlinked();
		}

		private void unmap(WorkloadTask task) {
			var entry = Objects.requireNonNull(task.cancellationEntry(),
					"Workload task is not cancellation-indexed");
			if (task.cancellationLinked) {
				throw new IllegalStateException("Linked workload task cannot be unmapped: " + task.sequence());
			}
			int remaining = entry.mappedCount - 1;
			if (remaining < 0) {
				throw new IllegalStateException("Cancellation mapping count underflow");
			}
			entry.mappedCount = remaining;
			task.clearCancellationIndex();
			if (remaining == 0) {
				if (entry.firstQueued != null || entry.lastQueued != null) {
					throw new IllegalStateException("Empty cancellation mapping still has queued tasks");
				}
				removeEntry(entry);
			}
		}

		private void removeEntry(CancellationEntry entry) {
			int bucket = entry.hash & (buckets.length - 1);
			var cursor = buckets[bucket];
			CancellationEntry previous = null;
			while (cursor != null && cursor != entry) {
				previous = cursor;
				cursor = cursor.bucketNext;
			}
			if (cursor == null) {
				throw new IllegalStateException("Cancellation entry is not hash-indexed");
			}
			if (previous == null) {
				buckets[bucket] = entry.bucketNext;
			} else {
				previous.bucketNext = entry.bucketNext;
			}
			entry.command = null;
			entry.profile = 0;
			entry.family = 0;
			entry.hash = 0;
			entry.mappedCount = 0;
			entry.firstQueued = null;
			entry.lastQueued = null;
			entry.bucketNext = null;
			entry.freeNext = freeHead;
			freeHead = entry;
			size--;
		}

		private CancellationEntry allocateEntry() {
			var entry = freeHead;
			if (entry == null) {
				return new CancellationEntry();
			}
			freeHead = entry.freeNext;
			entry.freeNext = null;
			return entry;
		}

		private void ensureInsertCapacity() {
			if (size + 1 <= resizeThreshold) {
				return;
			}
			rehash(Math.multiplyExact(buckets.length, 2));
		}

		private void rehash(int newCapacity) {
			var replacement = new CancellationEntry[newCapacity];
			for (var head : buckets) {
				var entry = head;
				while (entry != null) {
					var next = entry.bucketNext;
					int bucket = entry.hash & (newCapacity - 1);
					entry.bucketNext = replacement[bucket];
					replacement[bucket] = entry;
					entry = next;
				}
			}
			buckets = replacement;
			resizeThreshold = resizeThreshold(newCapacity);
		}

		private @Nullable CancellationEntry findEntry(Runnable command, int profile, int family) {
			return findEntry(command, profile, family, hash(command, profile, family));
		}

		private @Nullable CancellationEntry findEntry(Runnable command,
		                                                       int profile,
		                                                       int family,
		                                                       int hash) {
			var entry = buckets[hash & (buckets.length - 1)];
			while (entry != null) {
				if (entry.command == command
						&& Byte.toUnsignedInt(entry.profile) == profile
						&& Byte.toUnsignedInt(entry.family) == family) {
					return entry;
				}
				entry = entry.bucketNext;
			}
			return null;
		}

		private static int hash(Runnable command, int profile, int family) {
			int hash = 31 * (31 * System.identityHashCode(command) + profile) + family;
			return hash ^ (hash >>> 16);
		}

		private static byte checkedOrdinal(int ordinal) {
			if (ordinal < 0 || ordinal > Byte.MAX_VALUE) {
				throw new IllegalArgumentException("Enum ordinal does not fit in one byte: " + ordinal);
			}
			return (byte) ordinal;
		}

		private static int resizeThreshold(int capacity) {
			return capacity / LOAD_DENOMINATOR * LOAD_NUMERATOR;
		}
	}

	private record TerminalAction(Runnable command,
	                              @Nullable WorkloadTask cooperativeTask,
	                              TaskMetrics metrics,
	                              @Nullable RuntimeException failure,
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
	                           CounterHandle[] outcomes,
	                           CounterHandle cancellation,
	                           CounterHandle[] rejections,
	                           CounterHandle[] admissions) {

		private static TaskMetrics inert() {
			var outcomes = new CounterHandle[TERMINAL_OUTCOMES.length];
			java.util.Arrays.fill(outcomes, INERT_COUNTER);
			var rejections = new CounterHandle[TERMINAL_OUTCOMES.length];
			java.util.Arrays.fill(rejections, INERT_COUNTER);
			var admissions = new CounterHandle[AdmissionResult.values().length];
			java.util.Arrays.fill(admissions, INERT_COUNTER);
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
			outcomes[outcome.ordinal()].increment();
			if (outcome == RWScheduler.TerminalOutcome.CANCELLATION) {
				cancellation.increment();
			}
			var rejection = rejections[outcome.ordinal()];
			if (rejection != null) {
				rejection.increment();
			}
		}

		private void recordRunOutcome() {
			outcomes[RWScheduler.TerminalOutcome.RUN.ordinal()].increment();
		}

		private void recordAdmission(AdmissionResult result) {
			admissions[result.ordinal()].increment();
		}
	}
}

/** Internal classified termination path for retained cooperative resources. */
interface CooperativeTerminationHandle extends RWScheduler.CooperativeHandle {

	void terminate(RWScheduler.TerminalOutcome outcome, RuntimeException failure);
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
			releaseActiveBatchUnsafe(completedPool);
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

	void abortBatch(BatchPermit permit, RWScheduler.Pool abortedPool) {
		Objects.requireNonNull(permit, "permit");
		synchronized (this) {
			releaseActiveBatchUnsafe(abortedPool);
			// Aborting a pre-dispatch permit deliberately leaves both pacing clocks
			// unchanged: no BATCH quantum consumed storage or worker time. The caller
			// still owns its executor lock here, so defer every external wakeup until
			// workerLoop calls signalPendingAvailability() after unlocking.
			if ((queuedBatchPoolMask & ~poolBit(abortedPool)) != 0) {
				notificationPending = true;
			}
		}
	}

	private void releaseActiveBatchUnsafe(RWScheduler.Pool pool) {
		var dataPool = resourcePool(pool);
		if (activeBatches <= 0) {
			throw new IllegalStateException("No active BATCH permit to release");
		}
		activeBatches--;
		decrementActiveBatches(dataPool);
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
						int parkedTasks,
						int outstandingTasks,
						long submissionAttempts,
                        long acceptedTasks,
                        long startedTasks,
                        long completedTasks,
                        long failedTasks,
						Map<WorkloadProfile, Long> submissionAttemptsByProfile,
                        Map<WorkloadProfile, Integer> queuedByProfile,
                        Map<WorkloadProfile, Integer> activeByProfile,
						Map<WorkloadProfile, Integer> parkedByProfile,
						Map<WorkloadProfile, Integer> outstandingByProfile,
                        Map<RWScheduler.TerminalOutcome, Long> outcomes,
						boolean batchDispatchLimited,
						int batchStartAllowance,
                        List<String> workerThreadNames,
                        boolean shutdown,
                        boolean terminated) {
}
