package it.cavallium.rockserver.core.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.IdentityHashMap;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
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

	private static final int MAX_DEFICIT = WorkloadCost.MAX_UNITS;
	private static final Logger LOG = LoggerFactory.getLogger(ProfiledWorkloadExecutor.class);
	private static final ThreadLocal<ProfiledWorkloadExecutor> EXECUTING_POOL = new ThreadLocal<>();
	private static final WorkloadProfile[] PROFILES = WorkloadProfile.values();
	private static final OperationFamily[] FAMILIES = OperationFamily.values();
	private static final RWScheduler.TerminalOutcome[] TERMINAL_OUTCOMES =
			RWScheduler.TerminalOutcome.values();
	private static final TaskState[] TASK_STATES = TaskState.values();
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
	private static final Comparator<WorkloadTask> EDF_ORDER = Comparator
			.comparingLong(WorkloadTask::deadlineEpochMillis)
			.thenComparingLong(WorkloadTask::deadlineSequence);
	private static final Comparator<WorkloadTask> EXPIRY_ORDER = Comparator
			.comparingLong(WorkloadTask::monotonicDeadlineNanos)
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
	private final TaskHeap latencyQueue = new TaskHeap(false);
	private final TaskQueue[] queues = new TaskQueue[PROFILES.length];
	private final TaskQueue[] cooperativeQueues = new TaskQueue[PROFILES.length];
	private final TaskHeap deadlineQueue = new TaskHeap(true);
	private final ArrayDeque<DeferredAdmission> deferredAdmissions = new ArrayDeque<>();
	private final PriorityQueue<DeferredAdmission> deferredDeadlines = new PriorityQueue<>(Comparator
			.comparingLong((DeferredAdmission deferred) -> deferred.monotonicDeadlineNanos)
			.thenComparingLong(deferred -> deferred.sequence));
	private final CancellationIndex cancellationIndex = new CancellationIndex();
	private int indexedDeadlineCount;
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
	private final SchedulerDeadlineClock deadlineClock;
	private final String databaseName;
	private final String resourceKind;
	private final RWScheduler.Pool resourcePool;
	private final ThreadFactory threadFactory;
	private final @Nullable TaskMetrics[][] taskMetrics;
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
	private boolean batchDispatchabilityTracking;
	private volatile boolean publishedBatchDispatchable;
	private int latencyBurst;
	private int guaranteedCursor;
	private boolean guaranteedNeedsQuantum = true;
	private long acceptedTasks;
	private long submissionAttempts;
	private long startedTasks;
	private long completedTasks;
	private long failedTasks;
	private long sequence;
	private @Nullable Runnable beforeBatchPermitAcquisitionObserver;

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
	                         SchedulerDeadlineClock deadlineClock,
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
		this.pressureController = Objects.requireNonNull(pressureController, "pressureController");
		this.deadlineClock = Objects.requireNonNull(deadlineClock, "deadlineClock");
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
				this.queues[profile.ordinal()] = new TaskQueue();
				this.cooperativeQueues[profile.ordinal()] = new TaskQueue();
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
		this.taskMetrics = registry == null ? null : registerTaskMetrics(registry);
		this.workerFailureMetric = registerCounter(registry,
				"rockserver.workload.worker.failures",
				"database", databaseName,
				"resource", resourceKind);
		registerGauges(registry);
	}

	/**
	 * Bounded FIFO admission for a one-shot scheduler task that is allowed to wait for a real
	 * profile queue slot. Unlike retry/backoff, ownership remains in this executor and promotion is
	 * atomic with capacity release, so later submissions cannot overtake a waiter. The pre-admission
	 * lane is BATCH-only and capped at the configured BATCH queue capacity; waiters do not enter
	 * workload attempt/outstanding/outcome accounting until promotion into the real queue.
	 */
	Disposable executeWhenCapacity(WorkloadProfile profile,
	                               OperationFamily family,
	                               long deadlineEpochMillis,
	                               long estimatedBytes,
	                               Runnable command) {
		return executeWhenCapacity(profile,
				family,
				deadlineEpochMillis,
				RWScheduler.UNBOUND_MONOTONIC_DEADLINE,
				estimatedBytes,
				command);
	}

	Disposable executeWhenCapacity(WorkloadProfile profile,
	                               OperationFamily family,
	                               long deadlineEpochMillis,
	                               long localMonotonicDeadlineNanos,
	                               long estimatedBytes,
	                               Runnable command) {
		Objects.requireNonNull(command, "command");
		if (profile != WorkloadProfile.BATCH) {
			throw new IllegalArgumentException("Deferred admission is reserved for restartable BATCH work");
		}
		if (!(command instanceof RWScheduler.RejectionAwareTask)) {
			throw new IllegalArgumentException("Deferred workload tasks must handle asynchronous rejection");
		}
		int cost = taskCost(estimatedBytes);
		long monotonicDeadlineNanos = deadlineEpochMillis == RequestContext.NO_DEADLINE
				? Long.MAX_VALUE
				: boundOrComputeDeadline(deadlineEpochMillis, localMonotonicDeadlineNanos);
		var taskMetrics = metrics(profile, family);
		DeferredAdmission deferred = null;
		RuntimeException admissionFailure = null;
		RWScheduler.TerminalOutcome admissionOutcome = null;
		lock.lock();
		try {
			long nowNanos = deadlineEpochMillis == RequestContext.NO_DEADLINE
					? 0L
					: deadlineClock.monotonicNanos();
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowNanos >= monotonicDeadlineNanos) {
				admissionFailure = deadlineFailure("Workload deadline expired before deferred admission");
				admissionOutcome = RWScheduler.TerminalOutcome.DEADLINE;
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionOutcome = RWScheduler.TerminalOutcome.SHUTDOWN;
			} else {
				int waiterCapacity = capacityUnsafe(profile);
				if (waiterCapacity == 0 || deferredAdmissions.size() >= waiterCapacity) {
					admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
							"Deferred workload admission is full for " + profile + " " + family);
					admissionOutcome = RWScheduler.TerminalOutcome.OVERLOAD;
				} else {
					deferred = new DeferredAdmission(this,
							profile,
							family,
							deadlineEpochMillis,
							monotonicDeadlineNanos,
								nextSequenceUnsafe(),
							metricsTimestamp(taskMetrics),
							cost,
							command,
							taskMetrics);
					deferredAdmissions.addLast(deferred);
					if (deferred.hasDeadline()) {
						deferredDeadlines.add(deferred);
						timedWaitLeader = null;
					}
					promoteDeferredUnsafe(profile);
					ensureWorkersStartedUnsafe();
					signalWorkerUnsafe();
				}
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		if (admissionFailure != null) {
			completeTerminalAction(command,
					null,
					INERT_TASK_METRICS,
					admissionFailure,
					Objects.requireNonNull(admissionOutcome));
			throw admissionFailure;
		}
		return Objects.requireNonNull(deferred, "Accepted deferred workload task");
	}

	private long boundOrComputeDeadline(long deadlineEpochMillis, long localMonotonicDeadlineNanos) {
		return localMonotonicDeadlineNanos == RWScheduler.UNBOUND_MONOTONIC_DEADLINE
				? deadlineClock.monotonicDeadlineNanos(deadlineEpochMillis)
				: localMonotonicDeadlineNanos;
	}

	RWScheduler.WorkloadExecutor view(WorkloadProfile profile,
	                                  OperationFamily family,
	                                  long deadlineEpochMillis) {
		return view(profile, family, deadlineEpochMillis, RWScheduler.UNBOUND_MONOTONIC_DEADLINE);
	}

	RWScheduler.WorkloadExecutor view(WorkloadProfile profile,
	                                  OperationFamily family,
	                                  long deadlineEpochMillis,
	                                  long localMonotonicDeadlineNanos) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		return new WorkloadExecutorView(this,
				profile,
				family,
				deadlineEpochMillis,
				localMonotonicDeadlineNanos);
	}

	RWScheduler.CooperativeHandle executeCooperatively(WorkloadProfile profile,
	                                                   OperationFamily family,
	                                                   long deadlineEpochMillis,
	                                                   long estimatedBytes,
	                                                   RWScheduler.CooperativeTask command) {
		return executeCooperatively(profile,
				family,
				deadlineEpochMillis,
				RWScheduler.UNBOUND_MONOTONIC_DEADLINE,
				estimatedBytes,
				command);
	}

	RWScheduler.CooperativeHandle executeCooperatively(WorkloadProfile profile,
	                                                   OperationFamily family,
	                                                   long deadlineEpochMillis,
	                                                   long localMonotonicDeadlineNanos,
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
		long monotonicDeadlineNanos = deadlineEpochMillis == RequestContext.NO_DEADLINE
				? Long.MAX_VALUE
				: boundOrComputeDeadline(deadlineEpochMillis, localMonotonicDeadlineNanos);
		var taskMetrics = metrics(profile, family);
		WorkloadTask task = null;
		RuntimeException admissionFailure = null;
		RWScheduler.TerminalOutcome admissionOutcome = null;
		AdmissionResult admissionResult;
		List<TerminalAction> terminalActions = null;
		lock.lock();
		try {
			boolean hasQueuedDeadlines = indexedDeadlineCount != 0;
			long nowNanos = deadlineEpochMillis != RequestContext.NO_DEADLINE || hasQueuedDeadlines
					? deadlineClock.monotonicNanos()
					: 0L;
			if (hasQueuedDeadlines) {
				terminalActions = expireDueUnsafe(nowNanos, terminalActions);
			}
			recordSubmissionAttemptUnsafe(profile);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowNanos >= monotonicDeadlineNanos) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				admissionResult = AdmissionResult.DEADLINE;
				admissionOutcome = RWScheduler.TerminalOutcome.DEADLINE;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionResult = AdmissionResult.SHUTDOWN;
				admissionOutcome = RWScheduler.TerminalOutcome.SHUTDOWN;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (capacityUnsafe(profile) == 0 || queuedUnsafe(profile) >= capacityUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				admissionOutcome = RWScheduler.TerminalOutcome.OVERLOAD;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (outstandingAtLimitUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload outstanding limit is reached for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				admissionOutcome = RWScheduler.TerminalOutcome.OVERLOAD;
				recordOutcomeUnsafe(admissionOutcome);
			} else {
				task = WorkloadTask.cooperative(this,
						profile,
						family,
						deadlineEpochMillis,
						monotonicDeadlineNanos,
							nextSequenceUnsafe(),
						metricsTimestamp(taskMetrics),
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
				signalWorkerUnsafe();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		completeTerminalActions(terminalActions);
		if (admissionFailure != null) {
			completeTerminalAction(command,
					null,
					taskMetrics,
					admissionFailure,
					Objects.requireNonNull(admissionOutcome));
		}
		if (taskMetrics != INERT_TASK_METRICS) {
			taskMetrics.recordAdmission(admissionResult);
		}
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
		execute(profile,
				family,
				deadlineEpochMillis,
				RWScheduler.UNBOUND_MONOTONIC_DEADLINE,
				estimatedBytes,
				command);
	}

	void execute(WorkloadProfile profile,
	             OperationFamily family,
	             long deadlineEpochMillis,
	             long localMonotonicDeadlineNanos,
	             long estimatedBytes,
	             Runnable command) {
		Objects.requireNonNull(command, "command");
		int cost = taskCost(estimatedBytes);
		long monotonicDeadlineNanos = deadlineEpochMillis == RequestContext.NO_DEADLINE
				? Long.MAX_VALUE
				: boundOrComputeDeadline(deadlineEpochMillis, localMonotonicDeadlineNanos);
		var taskMetrics = metrics(profile, family);
		var cancellationTask = command instanceof CancellationTrackedTask trackedTask
				? trackedTask
				: null;
		List<TerminalAction> terminalActions = null;
		RuntimeException admissionFailure = null;
		RWScheduler.TerminalOutcome admissionOutcome = null;
		AdmissionResult admissionResult;
		lock.lock();
		try {
			boolean hasQueuedDeadlines = indexedDeadlineCount != 0;
			long nowNanos = deadlineEpochMillis != RequestContext.NO_DEADLINE || hasQueuedDeadlines
					? deadlineClock.monotonicNanos()
					: 0L;
			if (hasQueuedDeadlines) {
				terminalActions = expireDueUnsafe(nowNanos, terminalActions);
			}
			recordSubmissionAttemptUnsafe(profile);
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE && nowNanos >= monotonicDeadlineNanos) {
				admissionFailure = deadlineFailure("Workload deadline expired before admission");
				admissionResult = AdmissionResult.DEADLINE;
				admissionOutcome = RWScheduler.TerminalOutcome.DEADLINE;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (shutdown) {
				admissionFailure = new RejectedExecutionException(poolName + " is shutting down");
				admissionResult = AdmissionResult.SHUTDOWN;
				admissionOutcome = RWScheduler.TerminalOutcome.SHUTDOWN;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (capacityUnsafe(profile) == 0 || queuedUnsafe(profile) >= capacityUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload queue is full for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				admissionOutcome = RWScheduler.TerminalOutcome.OVERLOAD;
				recordOutcomeUnsafe(admissionOutcome);
			} else if (outstandingAtLimitUnsafe(profile)) {
				admissionFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						"Workload outstanding limit is reached for " + profile + " " + family);
				admissionResult = AdmissionResult.OVERLOAD;
				admissionOutcome = RWScheduler.TerminalOutcome.OVERLOAD;
				recordOutcomeUnsafe(admissionOutcome);
			} else {
				var task = WorkloadTask.normal(profile,
						family,
						deadlineEpochMillis,
						monotonicDeadlineNanos,
							nextSequenceUnsafe(),
						metricsTimestamp(taskMetrics),
						cost,
						command,
						cancellationTask,
						taskMetrics);
				boolean becomesDeadlineHead = task.hasDeadline()
						&& (indexedDeadlineCount == 0
						|| EXPIRY_ORDER.compare(task, earliestDeadlineUnsafe()) < 0);
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
				signalWorkerUnsafe();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		completeTerminalActions(terminalActions);
		if (admissionFailure != null) {
			completeTerminalAction(command,
					null,
					taskMetrics,
					admissionFailure,
					Objects.requireNonNull(admissionOutcome));
		}
		if (taskMetrics != INERT_TASK_METRICS) {
			taskMetrics.recordAdmission(admissionResult);
		}
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
		long cost = 1L + (estimatedBytes - 1L) / WorkloadCost.QUANTUM_BYTES;
		return (int) Math.max(1L, Math.min(WorkloadCost.MAX_UNITS, cost));
	}

	/**
	 * Issue a lifetime ordering ticket without ever exposing signed wraparound to EDF or FIFO
	 * comparisons. When the practically unreachable boundary is reached, every live object whose
	 * ticket can still participate in ordering is renumbered under this executor's lock. The live
	 * inventory is int-bounded by queue/outstanding limits, so its new maximum is strictly below
	 * {@link Long#MAX_VALUE}. The increasing transform preserves every heap and queue order.
	 *
	 * <p>Serial-number arithmetic is deliberately not used here: bounded live cardinality does not
	 * bound ticket distance when a far-future deadline or parked continuation survives arbitrarily
	 * many later submissions.</p>
	 */
	private long nextSequenceUnsafe() {
		long next = sequence;
		if (next != Long.MAX_VALUE) {
			sequence = next + 1L;
			return next;
		}
		return rebaseAndNextSequenceUnsafe();
	}

	private long rebaseAndNextSequenceUnsafe() {
		var live = new IdentityHashMap<Object, Boolean>();
		latencyQueue.collectSequenceOwners(live);
		deadlineQueue.collectSequenceOwners(live);
		for (var queue : queues) {
			if (queue != null) queue.collectSequenceOwners(live);
		}
		for (var queue : cooperativeQueues) {
			if (queue != null) queue.collectSequenceOwners(live);
		}
		for (var task = firstCooperativeTask; task != null; task = task.nextLifetime) {
			live.put(task, Boolean.TRUE);
		}
		for (var deferred : deferredAdmissions) live.put(deferred, Boolean.TRUE);
		for (var deferred : deferredDeadlines) live.put(deferred, Boolean.TRUE);
		cancellationIndex.collectSequenceOwners(live);

		var ordered = new ArrayList<SequenceSlot>(live.size());
		for (var owner : live.keySet()) {
			switch (owner) {
				case CooperativeWorkloadTask task -> {
					var workloadTask = (WorkloadTask) task;
					ordered.add(new SequenceSlot(task, false, workloadTask.sequence()));
					if (workloadTask.hasDeadline()) {
						ordered.add(new SequenceSlot(task, true, workloadTask.deadlineSequence()));
					}
				}
				case WorkloadTask task -> ordered.add(new SequenceSlot(task, false, task.sequence()));
				case DeferredAdmission deferred ->
						ordered.add(new SequenceSlot(deferred, false, deferred.sequence));
				default -> throw new IllegalStateException("Unrecognized workload ordering owner: "
						+ owner.getClass().getName());
			}
		}
		ordered.sort(Comparator.comparingLong(SequenceSlot::current));
		long replacement = 0L;
		for (int start = 0; start < ordered.size();) {
			var first = ordered.get(start);
			long current = first.current();
			if (current < 0L) {
				throw new IllegalStateException("Wrapped live workload ordering sequence: " + current);
			}
			int end = start + 1;
			while (end < ordered.size() && ordered.get(end).current() == current) end++;
			validateSequenceAliases(ordered, start, end);
			for (int index = start; index < end; index++) {
				assignSequence(ordered.get(index), replacement);
			}
			replacement++;
			start = end;
		}
		sequence = replacement;
		if (sequence < 0L || sequence == Long.MAX_VALUE) {
			throw new IllegalStateException("Live workload ordering inventory exceeds the ticket range");
		}
		return sequence++;
	}

	private static void validateSequenceAliases(List<SequenceSlot> ordered, int start, int end) {
		if (end - start == 1) {
			return;
		}
		if (end - start != 2) {
			throw new IllegalStateException("Workload ordering sequence has more than two live owners: "
					+ ordered.get(start).current());
		}
		var first = ordered.get(start);
		var second = ordered.get(start + 1);
		if (first.owner() != second.owner()
				|| !(first.owner() instanceof CooperativeWorkloadTask)
				|| first.deadline() == second.deadline()) {
			throw new IllegalStateException("Duplicate workload ordering sequence: " + first.current());
		}
	}

	private static void assignSequence(SequenceSlot slot, long replacement) {
		switch (slot.owner()) {
			case WorkloadTask task -> {
				if (slot.deadline()) {
					task.deadlineSequence(replacement);
				} else {
					task.sequence(replacement);
				}
			}
			case DeferredAdmission deferred -> deferred.sequence = replacement;
			default -> throw new IllegalStateException("Unrecognized workload ordering owner: "
					+ slot.owner().getClass().getName());
		}
	}

	private record SequenceSlot(Object owner, boolean deadline, long current) {
	}

	private void ensureWorkersStartedUnsafe() {
		while (startedWorkers < workerCount) {
			var worker = threadFactory.newThread(this::workerLoop);
			startedWorkers++;
			workers.add(worker);
			worker.start();
		}
	}

	private void signalWorkerUnsafe() {
		if (waitingWorkers != 0) {
			workAvailable.signal();
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
					long nowNanos = 0L;
					if (indexedDeadlineCount != 0 || !deferredDeadlines.isEmpty()) {
						nowNanos = deadlineClock.monotonicNanos();
					}
					if (indexedDeadlineCount != 0) {
						expireDueUnsafe(nowNanos, terminalActions);
					}
					if (!deferredDeadlines.isEmpty()) {
						expireDeferredDueUnsafe(nowNanos, terminalActions);
					}
					task = dispatchUnsafe(nowNanos, terminalActions);
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
				signalWorkerUnsafe();
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
		if (indexedDeadlineCount == 0 && deferredDeadlines.isEmpty()) {
			return Long.MAX_VALUE;
		}
		long earliestDeadlineNanos = indexedDeadlineCount == 0
				? Long.MAX_VALUE
				: earliestDeadlineUnsafe().monotonicDeadlineNanos();
		if (!deferredDeadlines.isEmpty()) {
			earliestDeadlineNanos = Math.min(earliestDeadlineNanos,
					Objects.requireNonNull(deferredDeadlines.peek()).monotonicDeadlineNanos);
		}
		return deadlineClock.remainingNanos(earliestDeadlineNanos);
	}

	private WorkloadTask earliestDeadlineUnsafe() {
		if (indexedDeadlineCount == 0) {
			throw new IllegalStateException("No indexed workload deadline");
		}
		return deadlineQueue.first();
	}

	private void incrementIndexedDeadlineCountUnsafe() {
		if (indexedDeadlineCount == Integer.MAX_VALUE) {
			throw new IllegalStateException("Indexed workload deadline count overflow");
		}
		indexedDeadlineCount++;
	}

	private void decrementIndexedDeadlineCountUnsafe() {
		if (indexedDeadlineCount == 0) {
			throw new IllegalStateException("Indexed workload deadline count underflow");
		}
		indexedDeadlineCount--;
	}

	private @Nullable WorkloadTask dispatchUnsafe(long nowNanos,
	                                              List<TerminalAction> terminalActions) {
		boolean batchEligible = true;
		while (queuedTotal > 0) {
			var task = batchEligible ? selectCandidateUnsafe() : selectCandidateUnsafe(false);
			if (task == null) {
				return null;
			}
			var dispatchCancellation = task.dispatchCancellation();
			if (dispatchCancellation != null
					&& cancelSelectedUnsafe(task, dispatchCancellation, terminalActions)) {
				continue;
			}
			if (task.hasDeadline() && nowNanos >= task.monotonicDeadlineNanos()) {
				unlinkUnsafe(task);
				discardSelectionUnsafe(task);
				terminateUnsafe(task,
						RWScheduler.TerminalOutcome.DEADLINE,
						deadlineFailure("Workload deadline expired immediately before execution"),
						terminalActions);
				continue;
			}
			long batchPermit = WorkloadPressureController.NO_BATCH_PERMIT;
			if (task.hasProfile(WorkloadProfile.BATCH)) {
				var observer = beforeBatchPermitAcquisitionObserver;
				if (observer != null) {
					observer.run();
				}
				batchPermit = pressureController.tryStartBatch(shutdown, resourcePool, System.nanoTime());
				if (batchPermit == WorkloadPressureController.NO_BATCH_PERMIT) {
					// Eligibility is only a snapshot: the other data pool can claim the
					// final shared permit before this pool reaches the atomic start. Keep
					// dispatch work-conserving by re-selecting queued foreground work, but
					// do not retry this stale BATCH candidate while holding the pool lock.
					batchEligible = false;
					advanceGuaranteedCursor();
					continue;
				}
			}
			// A BATCH permit must exist before the one-shot dispatch claim. Cancellation
			// can therefore either remove queued ownership or lose to execution ownership;
			// it can never consume a pacing interval without running a quantum.
			if (dispatchCancellation != null && !dispatchCancellation.claimWorkloadDispatch()) {
				if (batchPermit != WorkloadPressureController.NO_BATCH_PERMIT) {
					pressureController.abortBatch(batchPermit, resourcePool);
				}
				if (!cancelSelectedUnsafe(task, dispatchCancellation, terminalActions)) {
					throw new IllegalStateException("Dispatch claim failed without cancellation");
				}
				continue;
			}
			unlinkUnsafe(task);
			if (batchPermit != WorkloadPressureController.NO_BATCH_PERMIT) {
				task.batchPermit(batchPermit);
			}
			commitSelectionUnsafe(task);
			active[task.profileIndex()]++;
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

	void setBeforeBatchPermitAcquisitionObserverForTesting(@Nullable Runnable observer) {
		lock.lock();
		try {
			beforeBatchPermitAcquisitionObserver = observer;
		} finally {
			lock.unlock();
		}
	}

	private boolean cancelSelectedUnsafe(WorkloadTask task,
	                                     CancellationTrackedTask cancellationTask,
	                                     List<TerminalAction> terminalActions) {
		if (!cancellationTask.workloadCancellationRequested()) {
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
		if (task.metrics() == INERT_TASK_METRICS) {
			runInertDispatched(task);
			return;
		}
		runInstrumentedDispatched(task);
	}

	private void runInertDispatched(WorkloadTask task) {
		var outcome = RWScheduler.TerminalOutcome.FAILURE;
		try {
			assert EXECUTING_POOL.get() == null : "Scheduler worker is already executing a task";
			EXECUTING_POOL.set(this);
			task.command().run();
			outcome = RWScheduler.TerminalOutcome.RUN;
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable error) {
			logTaskExecutionFailure(task, error);
		} finally {
			EXECUTING_POOL.set(null);
			finishActive(task, outcome);
		}
	}

	private void runInstrumentedDispatched(WorkloadTask task) {
		var metrics = task.metrics();
		long executionStart = System.nanoTime();
		var outcome = RWScheduler.TerminalOutcome.FAILURE;
		try {
			assert EXECUTING_POOL.get() == null : "Scheduler worker is already executing a task";
			EXECUTING_POOL.set(this);
			task.command().run();
			outcome = RWScheduler.TerminalOutcome.RUN;
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable error) {
			logTaskExecutionFailure(task, error);
		} finally {
			EXECUTING_POOL.set(null);
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
		if (task.metrics() == INERT_TASK_METRICS) {
			runInertCooperativeDispatched(task);
			return;
		}
		runInstrumentedCooperativeDispatched(task);
	}

	private void runInertCooperativeDispatched(WorkloadTask task) {
		RWScheduler.CooperativeResult result = RWScheduler.CooperativeResult.COMPLETE;
		try {
			assert EXECUTING_POOL.get() == null : "Scheduler worker is already executing a task";
			EXECUTING_POOL.set(this);
			result = Objects.requireNonNull(task.cooperativeCommand().runCooperatively(task),
					"Cooperative task returned no result");
		} catch (VirtualMachineError fatal) {
			task.fail(new RejectedExecutionException("Cooperative workload quantum failed", fatal));
			throw fatal;
		} catch (Throwable error) {
			logTaskExecutionFailure(task, error);
			var failure = error instanceof RuntimeException runtimeException
					? runtimeException
					: new RejectedExecutionException("Cooperative workload quantum failed", error);
			task.fail(failure);
		} finally {
			EXECUTING_POOL.set(null);
			var terminalAction = finishCooperative(task, result);
			if (terminalAction != null) {
				completeTerminalAction(terminalAction);
			} else if (task.outcome() == RWScheduler.TerminalOutcome.RUN
					&& task.command() instanceof RWScheduler.CooperativeCompletionTask) {
				completeCooperativeSuccess(task);
			}
		}
	}

	private void runInstrumentedCooperativeDispatched(WorkloadTask task) {
		var metrics = task.metrics();
		long executionStart = System.nanoTime();
		RWScheduler.CooperativeResult result = RWScheduler.CooperativeResult.COMPLETE;
		try {
			assert EXECUTING_POOL.get() == null : "Scheduler worker is already executing a task";
			EXECUTING_POOL.set(this);
			result = Objects.requireNonNull(task.cooperativeCommand().runCooperatively(task),
					"Cooperative task returned no result");
		} catch (VirtualMachineError fatal) {
			task.fail(new RejectedExecutionException("Cooperative workload quantum failed", fatal));
			throw fatal;
		} catch (Throwable error) {
			logTaskExecutionFailure(task, error);
			var failure = error instanceof RuntimeException runtimeException
					? runtimeException
					: new RejectedExecutionException("Cooperative workload quantum failed", error);
			task.fail(failure);
		} finally {
			EXECUTING_POOL.set(null);
			task.recordCooperativeQuantum(executionStart,
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
		if (batchPermit != WorkloadPressureController.NO_BATCH_PERMIT) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		lock.lock();
		try {
			if (task.outcome() != null) {
				throw new IllegalStateException("Active workload task already has a terminal outcome");
			}
			active[task.profileIndex()]--;
			activeTotal--;
			completeCompetitionUnsafe(task);
			task.outcome(outcome);
			decrementOutstandingUnsafe(task.profileIndex());
			completedTasks++;
			task.markTerminal();
			recordOutcomeUnsafe(outcome);
			refreshPreemptionUnsafe();
			signalWorkerUnsafe();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
	}

	private @Nullable TerminalAction finishCooperative(WorkloadTask task,
	                                                   RWScheduler.CooperativeResult result) {
		var batchPermit = task.takeBatchPermit();
		if (batchPermit != WorkloadPressureController.NO_BATCH_PERMIT) {
			pressureController.finishBatch(batchPermit, resourcePool);
		}
		TerminalAction terminalAction = null;
		lock.lock();
		try {
			if (task.state() != TaskState.ACTIVE) {
				throw new IllegalStateException("Cooperative task is not active");
			}
			active[task.profileIndex()]--;
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
			signalWorkerUnsafe();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		return terminalAction;
	}

	private void refreshCooperativeSequenceUnsafe(WorkloadTask task) {
		task.refreshSequence(nextSequenceUnsafe());
	}

	private void resumeCooperative(WorkloadTask task) {
		lock.lock();
		try {
			switch (task.state()) {
				case ACTIVE -> task.requestResume();
				case PARKED -> {
					if (task.requestedOutcome() == null) {
						unmarkParkedUnsafe(task);
						// Publish competition before exposing the continuation to a worker. Both
						// transitions happen under this executor lock, so local dispatch cannot race it.
						admitCompetitionUnsafe(task);
						refreshCooperativeSequenceUnsafe(task);
						enqueueCooperativeUnsafe(task, false);
						refreshPreemptionUnsafe();
						signalWorkerUnsafe();
					}
				}
				case QUEUED, TERMINAL -> {
				}
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
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
			signalWorkerUnsafe();
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		if (terminalAction != null) {
			completeTerminalAction(terminalAction);
		}
		return selected;
	}

	private void refreshPreemptionUnsafe() {
		if (batchDispatchabilityTracking) {
			boolean batchDispatchable = !shutdown
					&& queuedUnsafe(WorkloadProfile.BATCH) > 0
					&& activeTotal < workerCount;
			if (batchDispatchable != publishedBatchDispatchable) {
				publishedBatchDispatchable = batchDispatchable;
				if (!batchDispatchable && pressureController.pressureActive()) {
					pressureController.batchDispatchabilityLost(resourcePool);
				}
			}
		}
		boolean requested = localQueuedCompetition
				|| activeTotal > activeUnsafe(WorkloadProfile.BATCH);
		if (requested != publishedPreemption) {
			publishedPreemption = requested;
			pressureController.setPoolPreemption(resourcePool, requested);
		}
	}

	boolean batchDispatchable() {
		return publishedBatchDispatchable;
	}

	void setBatchDispatchabilityTracking(boolean tracking) {
		lock.lock();
		try {
			batchDispatchabilityTracking = tracking;
			if (tracking) {
				refreshPreemptionUnsafe();
			}
		} finally {
			lock.unlock();
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
		return selectCandidateUnsafe(true);
	}

	private @Nullable WorkloadTask selectCandidateUnsafe(boolean batchEligible) {
		if (activeTotal >= workerCount) {
			return null;
		}
		boolean reservedLatency = hasReservationDeficitUnsafe(WorkloadProfile.LATENCY);
		boolean reservedGuaranteed = hasGuaranteedReservationDeficitUnsafe(batchEligible);
		if (reservedLatency || reservedGuaranteed) {
			if (reservedLatency && (latencyBurst < maxLatencyBurst || !reservedGuaranteed)) {
				return peekUnsafe(WorkloadProfile.LATENCY);
			}
			var guaranteed = selectGuaranteedCandidateUnsafe(true, batchEligible);
			if (guaranteed != null) {
				return guaranteed;
			}
			if (reservedLatency) {
				return peekUnsafe(WorkloadProfile.LATENCY);
			}
		}

		boolean latencyEligible = isEligibleUnsafe(WorkloadProfile.LATENCY, batchEligible);
		boolean guaranteedEligible = hasGuaranteedEligibleUnsafe(batchEligible);
		if (latencyEligible && (latencyBurst < maxLatencyBurst || !guaranteedEligible)) {
			return peekUnsafe(WorkloadProfile.LATENCY);
		}
		var guaranteed = selectGuaranteedCandidateUnsafe(false, batchEligible);
		if (guaranteed != null) {
			return guaranteed;
		}
		if (latencyEligible) {
			return peekUnsafe(WorkloadProfile.LATENCY);
		}
		for (var profile : ISOLATED) {
			if (isEligibleUnsafe(profile, batchEligible)) {
				return peekUnsafe(profile);
			}
		}
		return null;
	}

	private boolean hasGuaranteedReservationDeficitUnsafe(boolean batchEligible) {
		for (var profile : GUARANTEED) {
			if (hasReservationDeficitUnsafe(profile) && isEligibleUnsafe(profile, batchEligible)) {
				return true;
			}
		}
		return false;
	}

	private boolean hasGuaranteedEligibleUnsafe(boolean batchEligible) {
		for (var profile : GUARANTEED) {
			if (isEligibleUnsafe(profile, batchEligible)) {
				return true;
			}
		}
		return false;
	}

	private @Nullable WorkloadTask selectGuaranteedCandidateUnsafe(boolean reservationOnly,
	                                                               boolean batchEligible) {
		int maxAttempts = GUARANTEED.length * (WorkloadCost.MAX_UNITS + 1);
		for (int attempts = 0; attempts < maxAttempts; attempts++) {
			var profile = GUARANTEED[guaranteedCursor];
			if (queuedUnsafe(profile) == 0) {
				deficit[profile.ordinal()] = 0;
				advanceGuaranteedCursor();
				continue;
			}
			if ((reservationOnly && !hasReservationDeficitUnsafe(profile))
					|| !isEligibleUnsafe(profile, batchEligible)) {
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
		int profileIndex = task.profileIndex();
		if (profileIndex == WorkloadProfile.LATENCY.ordinal()) {
			latencyBurst = latencyBurst < 0 || latencyBurst >= maxLatencyBurst
					? maxLatencyBurst
					: latencyBurst + 1;
			return;
		}
		if (!isGuaranteedProfile(profileIndex)) {
			return;
		}
		latencyBurst = 0;
		deficit[profileIndex] -= task.cost();
		if (queued[profileIndex] == 0) {
			deficit[profileIndex] = 0;
			advanceGuaranteedCursor();
		} else if (deficit[profileIndex] < peekUnsafe(profileIndex).cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void discardSelectionUnsafe(WorkloadTask task) {
		int profileIndex = task.profileIndex();
		if (!isGuaranteedProfile(profileIndex)) {
			return;
		}
		if (queued[profileIndex] == 0) {
			deficit[profileIndex] = 0;
			advanceGuaranteedCursor();
		} else if (deficit[profileIndex] < peekUnsafe(profileIndex).cost()) {
			advanceGuaranteedCursor();
		}
	}

	private void advanceGuaranteedCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.length;
		guaranteedNeedsQuantum = true;
	}

	private static boolean isGuaranteedProfile(int profileIndex) {
		return profileIndex == WorkloadProfile.INGEST.ordinal()
				|| profileIndex == WorkloadProfile.CDC.ordinal()
				|| profileIndex == WorkloadProfile.ANALYTICAL.ordinal()
				|| profileIndex == WorkloadProfile.BATCH.ordinal();
	}

	private boolean hasReservationDeficitUnsafe(WorkloadProfile profile) {
		int profileIndex = profile.ordinal();
		return reservations[profileIndex] > active[profileIndex] && queued[profileIndex] > 0;
	}

	private boolean isEligibleUnsafe(WorkloadProfile profile, boolean batchEligible) {
		if (queuedUnsafe(profile) == 0 || activeTotal >= workerCount) {
			return false;
		}
		if (profile == WorkloadProfile.BATCH && !batchEligible) {
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

	boolean remove(WorkloadProfile profile, OperationFamily family, Runnable command) {
		List<TerminalAction> terminalActions = null;
		boolean removed = false;
		lock.lock();
		try {
			var task = cancellationIndex.first(command, profile, family);
			if (task != null) {
				terminalActions = new ArrayList<>(1);
				boolean wasDeadlineHead = task.hasDeadline() && earliestDeadlineUnsafe() == task;
				unlinkUnsafe(task);
				removed = terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						new CancellationException("Workload submission cancelled while queued"),
						terminalActions);
				if (wasDeadlineHead || task.hasProfile(WorkloadProfile.BATCH)) {
					timedWaitLeader = null;
				}
				refreshPreemptionUnsafe();
				signalWorkerUnsafe();
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
		private @Nullable CancellationEntry bucketPrevious;
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
		if (deferredAdmissions.size() > capacityUnsafe(WorkloadProfile.BATCH)
				|| deferredDeadlines.size() > deferredAdmissions.size()) {
			throw new IllegalStateException("Deferred BATCH admission index mismatch in " + poolName);
		}
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
			signalWorkerUnsafe();
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

	private @Nullable List<TerminalAction> expireDueUnsafe(long nowNanos,
	                                                       @Nullable List<TerminalAction> terminalActions) {
		boolean expired = false;
		while (indexedDeadlineCount != 0) {
			var task = earliestDeadlineUnsafe();
			if (nowNanos < task.monotonicDeadlineNanos()) {
				if (expired) {
					refreshPreemptionUnsafe();
				}
				return terminalActions;
			}
			expired = true;
			if (task.isCooperative()) {
				var failure = deadlineFailure("Cooperative workload deadline expired");
				if (!deadlineQueue.remove(task)) {
					throw new IllegalStateException("Cooperative deadline task is not indexed");
				}
				decrementIndexedDeadlineCountUnsafe();
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
		if (expired) {
			refreshPreemptionUnsafe();
		}
		return terminalActions;
	}

	private void expireDeferredDueUnsafe(long nowNanos, List<TerminalAction> terminalActions) {
		while (!deferredDeadlines.isEmpty()) {
			var deferred = Objects.requireNonNull(deferredDeadlines.peek());
			if (nowNanos < deferred.monotonicDeadlineNanos) {
				return;
			}
			deferredDeadlines.remove();
			if (!deferred.isWaiting()) {
				throw new IllegalStateException("Deferred deadline task is not waiting");
			}
			if (!deferredAdmissions.remove(deferred)) {
				throw new IllegalStateException("Deferred deadline task is not admission-indexed");
			}
			deferred.markTerminal();
			terminalActions.add(new TerminalAction(deferred,
					null,
					INERT_TASK_METRICS,
					deadlineFailure("Workload deadline expired while awaiting admission"),
					RWScheduler.TerminalOutcome.DEADLINE));
		}
	}

	private void promoteDeferredUnsafe(WorkloadProfile profile) {
		if (profile != WorkloadProfile.BATCH) {
			return;
		}
		while (!deferredAdmissions.isEmpty()
				&& !shutdown
				&& queuedUnsafe(profile) < capacityUnsafe(profile)
				&& !outstandingAtLimitUnsafe(profile)) {
			var deferred = Objects.requireNonNull(deferredAdmissions.removeFirst());
			if (!deferred.isWaiting()) {
				continue;
			}
			deferred.unlinkDeadlineUnsafe();
			deferred.markQueued();
			recordSubmissionAttemptUnsafe(profile);
			var task = WorkloadTask.normal(profile,
					deferred.family,
					deferred.deadlineEpochMillis,
					deferred.monotonicDeadlineNanos,
					deferred.sequence,
					deferred.enqueuedNanos,
					deferred.cost,
					deferred,
					deferred,
					deferred.metrics);
			enqueueUnsafe(task);
			incrementOutstandingUnsafe(profile);
			admitCompetitionUnsafe(task);
			acceptedTasks++;
			deferred.metrics.recordAdmission(AdmissionResult.ACCEPTED);
			refreshPreemptionUnsafe();
		}
	}

	private void enqueueUnsafe(WorkloadTask task) {
		int profileIndex = task.profileIndex();
		if (task.hasProfile(WorkloadProfile.LATENCY)) {
			if (!latencyQueue.add(task)) {
				throw new IllegalStateException("Duplicate workload task sequence " + task.sequence());
			}
		} else {
			Objects.requireNonNull(queues[profileIndex], "Missing workload queue").addLast(task);
		}
		if (task.hasDeadline()) {
			if (!deadlineQueue.add(task)) {
				throw new IllegalStateException("Duplicate workload deadline sequence " + task.sequence());
			}
			incrementIndexedDeadlineCountUnsafe();
		}
		cancellationIndex.map(task);
		cancellationIndex.link(task);
		int previousQueued = queued[profileIndex]++;
		if (task.hasProfile(WorkloadProfile.BATCH) && previousQueued == 0) {
			pressureController.setBatchQueued(resourcePool, true);
		}
		incrementQueuedTotalUnsafe();
		task.markQueued();
	}

	private void enqueueCooperativeUnsafe(WorkloadTask task, boolean initialAdmission) {
		int profileIndex = task.profileIndex();
		var queue = cooperativeQueues[profileIndex];
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
				incrementIndexedDeadlineCountUnsafe();
				task.markDeadlineIndexed();
			}
			cancellationIndex.map(task);
		}
		cancellationIndex.link(task);
		int previousQueued = queued[profileIndex]++;
		if (task.hasProfile(WorkloadProfile.BATCH) && previousQueued == 0) {
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
		int profileIndex = task.profileIndex();
		boolean removed = task.hasProfile(WorkloadProfile.LATENCY)
				? latencyQueue.remove(task)
				: Objects.requireNonNull(queues[profileIndex], "Missing workload queue").remove(task);
		if (!removed) {
			throw new IllegalStateException("Workload task is not queued: " + task.sequence());
		}
		if (task.hasDeadline()) {
			if (!deadlineQueue.remove(task)) {
				throw new IllegalStateException("Finite-deadline workload task is not deadline-indexed: "
						+ task.sequence());
			}
			decrementIndexedDeadlineCountUnsafe();
		}
		cancellationIndex.unlink(task);
		cancellationIndex.unmap(task);
		int remainingQueued = --queued[profileIndex];
		if (task.hasProfile(WorkloadProfile.BATCH) && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		decrementQueuedTotalUnsafe();
		if (task.hasProfile(WorkloadProfile.BATCH) && !deferredAdmissions.isEmpty()) {
			promoteDeferredUnsafe(WorkloadProfile.BATCH);
		}
	}

	private void unlinkCooperativeQueuedUnsafe(WorkloadTask task) {
		int profileIndex = task.profileIndex();
		var queue = cooperativeQueues[profileIndex];
		if (queue == null || !queue.remove(task)) {
			throw new IllegalStateException("Cooperative workload task is not queued: " + task.sequence());
		}
		cancellationIndex.unlink(task);
		int remainingQueued = --queued[profileIndex];
		if (task.hasProfile(WorkloadProfile.BATCH) && remainingQueued == 0) {
			pressureController.setBatchQueued(resourcePool, false);
		}
		decrementQueuedTotalUnsafe();
		if (task.hasProfile(WorkloadProfile.BATCH) && !deferredAdmissions.isEmpty()) {
			promoteDeferredUnsafe(WorkloadProfile.BATCH);
		}
	}

	private void admitCooperativeTaskUnsafe(WorkloadTask task) {
		var cooperativeTask = (CooperativeWorkloadTask) task;
		if (cooperativeTask.flag(CooperativeWorkloadTask.ADMITTED)
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
		cooperativeTask.setFlag(CooperativeWorkloadTask.ADMITTED);
		cooperativeTaskCount++;
	}

	private void removeCooperativeTaskUnsafe(WorkloadTask task) {
		var cooperativeTask = (CooperativeWorkloadTask) task;
		if (!cooperativeTask.flag(CooperativeWorkloadTask.ADMITTED)) {
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
		cooperativeTask.clearFlag(CooperativeWorkloadTask.ADMITTED);
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

	private void decrementOutstandingUnsafe(int index) {
		int current = outstanding[index];
		if (current <= 0 || outstandingTotal <= 0) {
			throw new IllegalStateException("Outstanding workload count underflow for " + PROFILES[index]);
		}
		outstanding[index] = current - 1;
		outstandingTotal--;
		if (index == WorkloadProfile.BATCH.ordinal() && !deferredAdmissions.isEmpty()) {
			promoteDeferredUnsafe(WorkloadProfile.BATCH);
		}
	}

	private void markParkedUnsafe(WorkloadTask task) {
		if (task.state() != TaskState.ACTIVE) {
			throw new IllegalStateException("Only an active cooperative task can park");
		}
		// A genuinely parked continuation has no runnable quantum and must not reserve global
		// BATCH capacity. The pressure controller retains its configured transition hold.
		completeCompetitionUnsafe(task);
		parked[task.profileIndex()]++;
		parkedTotal++;
		task.markParked();
	}

	private void unmarkParkedUnsafe(WorkloadTask task) {
		if (task.state() != TaskState.PARKED) {
			throw new IllegalStateException("Cooperative task is not parked");
		}
		int index = task.profileIndex();
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
			decrementOutstandingUnsafe(task.profileIndex());
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
			decrementIndexedDeadlineCountUnsafe();
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
		decrementOutstandingUnsafe(task.profileIndex());
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
		if (outcome == RWScheduler.TerminalOutcome.FAILURE) {
			failedTasks++;
		}
	}

	private void admitCompetitionUnsafe(WorkloadTask task) {
		if (task.hasProfile(WorkloadProfile.BATCH)) {
			return;
		}
		if (task.isCooperative()) {
			if (task.competitionActive()) {
				throw new IllegalStateException("Workload task already publishes competition in " + poolName);
			}
			task.markCompetitionActive();
		}
		if (competingTasks++ == 0) {
			pressureController.setPoolCompetition(resourcePool, true);
		}
	}

	private void completeCompetitionUnsafe(WorkloadTask task) {
		if (task.hasProfile(WorkloadProfile.BATCH)) {
			return;
		}
		if (task.isCooperative()) {
			if (!task.competitionActive()) {
				if (task.state() == TaskState.PARKED) {
					return;
				}
				throw new IllegalStateException("Workload task does not publish competition in " + poolName);
			}
			task.markCompetitionInactive();
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
		completeTerminalAction(action.command(),
				action.cooperativeTask(),
				action.metrics(),
				action.failure(),
				action.outcome());
	}

	private void completeTerminalAction(Runnable command,
	                                    @Nullable WorkloadTask cooperativeTask,
	                                    TaskMetrics metrics,
	                                    @Nullable RuntimeException failure,
	                                    RWScheduler.TerminalOutcome outcome) {
		try {
			if (outcome == RWScheduler.TerminalOutcome.RUN && failure == null) {
				((RWScheduler.CooperativeCompletionTask) command).completeCooperatively();
			} else {
				var terminalFailure = Objects.requireNonNull(failure, "Non-RUN terminal failure");
				if (command instanceof RWScheduler.RejectionAwareTask rejectionAwareTask) {
					rejectionAwareTask.reject(terminalFailure);
				} else if (command instanceof CompletableFuture<?> future) {
					future.completeExceptionally(terminalFailure);
				} else if (command instanceof Future<?> future) {
					future.cancel(false);
				}
			}
		} catch (Throwable terminalFailure) {
			recordInfrastructureFailure("Failed to complete " + outcome
					+ " workload submission in " + poolName, terminalFailure);
		}
		try {
			if (command instanceof Disposable disposable && !disposable.isDisposed()) {
				disposable.dispose();
			}
		} catch (Throwable disposalFailure) {
			recordInfrastructureFailure("Failed to dispose " + outcome
					+ " workload submission in " + poolName, disposalFailure);
		}
		if (cooperativeTask != null) {
			cooperativeTask.flushCooperativeMetrics();
		}
		if (metrics != INERT_TASK_METRICS) {
			metrics.recordOutcome(outcome);
		}
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
		var metrics = task.metrics();
		if (metrics != INERT_TASK_METRICS) {
			metrics.recordRunOutcome();
		}
	}

	private WorkloadTask peekUnsafe(WorkloadProfile profile) {
		return peekUnsafe(profile.ordinal());
	}

	private WorkloadTask peekUnsafe(int profileIndex) {
		if (profileIndex == WorkloadProfile.LATENCY.ordinal()) {
			return latencyQueue.first();
		}
		var normalQueue = queues[profileIndex];
		var cooperativeQueue = cooperativeQueues[profileIndex];
		var normalHead = normalQueue.peekFirst();
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
		var registeredMetrics = taskMetrics;
		if (registeredMetrics == null) {
			return INERT_TASK_METRICS;
		}
		var result = registeredMetrics[profile.ordinal()][family.ordinal()];
		return result == null ? INERT_TASK_METRICS : result;
	}

	private static long metricsTimestamp(TaskMetrics metrics) {
		return metrics == INERT_TASK_METRICS ? 0L : System.nanoTime();
	}

	private TaskMetrics[][] registerTaskMetrics(
			MeterRegistry registry) {
		var result = new TaskMetrics[PROFILES.length][FAMILIES.length];
		for (var profile : PROFILES) {
			if (capacityUnsafe(profile) == 0) {
				continue;
			}
			for (var family : FAMILIES) {
				if (WorkloadAdmission.isAllowed(profile, family)
						&& RWScheduler.resourcePool(profile, family) == resourcePool) {
					result[profile.ordinal()][family.ordinal()] = registerTaskMetrics(registry, profile, family);
				}
			}
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

	private void logTaskExecutionFailure(WorkloadTask task, Throwable failure) {
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
		var terminalActions = new ArrayList<TerminalAction>();
		lock.lock();
		try {
			shutdown = true;
			refreshPreemptionUnsafe();
			rejectDeferredUnsafe(new RejectedExecutionException(poolName + " is shutting down"),
					RWScheduler.TerminalOutcome.SHUTDOWN,
					terminalActions);
			timedWaitLeader = null;
			if (startedWorkers == 0) {
				terminated = true;
			}
			workAvailable.signalAll();
		} finally {
			lock.unlock();
		}
		completeTerminalActions(terminalActions);
	}

	@Override
	public List<Runnable> shutdownNow() {
		var terminalActions = new ArrayList<TerminalAction>();
		var remaining = new ArrayList<Runnable>();
		lock.lock();
		try {
			shutdown = true;
			rejectDeferredUnsafe(new RejectedExecutionException(poolName + " was forced to shut down"),
					RWScheduler.TerminalOutcome.SHUTDOWN,
					terminalActions);
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

	private void rejectDeferredUnsafe(RuntimeException failure,
	                                  RWScheduler.TerminalOutcome outcome,
	                                  List<TerminalAction> terminalActions) {
		while (!deferredAdmissions.isEmpty()) {
			var deferred = Objects.requireNonNull(deferredAdmissions.removeFirst());
			if (!deferred.isWaiting()) {
				continue;
			}
			deferred.unlinkDeadlineUnsafe();
			deferred.markTerminal();
			terminalActions.add(new TerminalAction(deferred,
					null,
					INERT_TASK_METRICS,
					failure,
					outcome));
		}
		if (!deferredDeadlines.isEmpty()) {
			throw new IllegalStateException("Shutdown left deferred workload deadlines indexed");
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
		Objects.requireNonNull(unit, "unit");
		if (terminated) return true;
		long timeoutNanos = unit.toNanos(timeout);
		if (timeoutNanos <= 0L) return false;
		long startedNanos = System.nanoTime();
		for (var worker : List.copyOf(workers)) {
			if (!worker.isAlive()) continue;
			long elapsedNanos = System.nanoTime() - startedNanos;
			if (elapsedNanos < 0L) return terminated;
			long remaining = timeoutNanos - elapsedNanos;
			if (remaining <= 0L) {
				return terminated;
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
	                                    long deadlineEpochMillis,
	                                    long localMonotonicDeadlineNanos) implements RWScheduler.WorkloadExecutor {

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
			owner.execute(profile,
					family,
					deadlineEpochMillis,
					localMonotonicDeadlineNanos,
					estimatedBytes,
					command);
		}

		@Override
		public RWScheduler.CooperativeHandle executeCooperatively(RWScheduler.CooperativeTask command,
		                                                          long estimatedBytes) {
			return owner.executeCooperatively(profile,
					family,
					deadlineEpochMillis,
					localMonotonicDeadlineNanos,
					estimatedBytes,
					command);
		}
	}

	private void cancelDeferred(DeferredAdmission deferred) {
		List<TerminalAction> terminalActions = null;
		lock.lock();
		try {
			if (deferred.isWaiting()) {
				if (!deferredAdmissions.remove(deferred)) {
					throw new IllegalStateException("Deferred workload task is not waiting");
				}
				deferred.unlinkDeadlineUnsafe();
				deferred.markCancelled();
			} else if (deferred.isQueued()) {
				var task = cancellationIndex.first(deferred, deferred.profile, deferred.family);
				if (task == null) {
					throw new IllegalStateException("Deferred workload task is not queued");
				}
				deferred.markCancelled();
				terminalActions = new ArrayList<>(1);
				unlinkUnsafe(task);
				if (!terminateUnsafe(task,
						RWScheduler.TerminalOutcome.CANCELLATION,
						new CancellationException("Deferred workload admission cancelled while queued"),
						terminalActions)) {
					throw new IllegalStateException("Deferred workload cancellation did not terminate the task");
				}
				refreshPreemptionUnsafe();
				signalWorkerUnsafe();
			}
		} finally {
			lock.unlock();
		}
		pressureController.signalPendingAvailability();
		completeTerminalActions(terminalActions);
	}

	private static final class DeferredAdmission implements Runnable,
			Disposable,
			CancellationTrackedTask,
			RWScheduler.RejectionAwareTask {

		private static final int WAITING = 0;
		private static final int QUEUED = 1;
		private static final int RUNNING = 2;
		private static final int TERMINAL = 3;
		private static final int CANCELLED = 4;

		private final ProfiledWorkloadExecutor owner;
		private final WorkloadProfile profile;
		private final OperationFamily family;
		private final long deadlineEpochMillis;
		private final long monotonicDeadlineNanos;
		private long sequence;
		private final long enqueuedNanos;
		private final int cost;
		private final Runnable command;
		private final TaskMetrics metrics;
		private volatile int state = WAITING;

		private DeferredAdmission(ProfiledWorkloadExecutor owner,
				WorkloadProfile profile,
				OperationFamily family,
				long deadlineEpochMillis,
				long monotonicDeadlineNanos,
				long sequence,
				long enqueuedNanos,
				int cost,
				Runnable command,
				TaskMetrics metrics) {
			this.owner = owner;
			this.profile = profile;
			this.family = family;
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			this.sequence = sequence;
			this.enqueuedNanos = enqueuedNanos;
			this.cost = cost;
			this.command = command;
			this.metrics = metrics;
		}

		@Override
		public void run() {
			try {
				command.run();
			} finally {
				markTerminal();
			}
		}

		@Override
		public void reject(RuntimeException failure) {
			try {
				((RWScheduler.RejectionAwareTask) command).reject(failure);
			} finally {
				markTerminal();
			}
		}

		@Override
		public void dispose() {
			owner.cancelDeferred(this);
		}

		@Override
		public boolean isDisposed() {
			return state >= TERMINAL;
		}

		@Override
		public boolean workloadCancellationRequested() {
			return state == CANCELLED;
		}

		@Override
		public boolean claimWorkloadDispatch() {
			if (state == QUEUED) {
				state = RUNNING;
			}
			return state == RUNNING;
		}

		private boolean isWaiting() {
			return state == WAITING;
		}

		private boolean isQueued() {
			return state == QUEUED;
		}

		private boolean hasDeadline() {
			return deadlineEpochMillis != RequestContext.NO_DEADLINE;
		}

		private void unlinkDeadlineUnsafe() {
			if (hasDeadline() && !owner.deferredDeadlines.remove(this)) {
				throw new IllegalStateException("Deferred workload deadline is not indexed");
			}
		}

		private void markQueued() {
			if (state != WAITING) {
				throw new IllegalStateException("Deferred workload task is not waiting");
			}
			state = QUEUED;
		}

		private void markCancelled() {
			state = CANCELLED;
		}

		private void markTerminal() {
			if (state != CANCELLED) {
				state = TERMINAL;
			}
		}
	}

	private static class WorkloadTask implements CooperativeTerminationHandle,
			RWScheduler.CooperativeContext {

		private static final int STATE_MASK = 0b11;
		private static final int DISPATCH_CANCELLATION = 0b100;
		private static final int HAS_DEADLINE = 0b1000;
		private static final int COMPETITION_ACTIVE = 0b1_0000;
		private long deadlineSequence;
		private final long enqueuedNanos;
		private final int cost;
		private final byte profile;
		private final byte family;
		private final Runnable command;
		private final TaskMetrics metrics;
		private @Nullable WorkloadTask previousCancellation;
		private @Nullable WorkloadTask nextCancellation;
		private @Nullable WorkloadTask previousQueue;
		private @Nullable WorkloadTask nextQueue;
		private long batchPermit;
		private byte outcome = -1;
		private byte state;
		private boolean cancellationLinked;
		private boolean deadlineIndexed;
		private boolean profileQueued;
		private @Nullable CancellationEntry cancellationEntry;

		private WorkloadTask(WorkloadProfile profile,
		                     OperationFamily family,
		                     long sequence,
		                     long enqueuedNanos,
		                     int cost,
		                     Runnable command,
		                     @Nullable CancellationTrackedTask cancellationTask,
		                     TaskMetrics metrics) {
			this.profile = (byte) profile.ordinal();
			this.family = (byte) family.ordinal();
			this.deadlineSequence = sequence;
			this.enqueuedNanos = enqueuedNanos;
			this.cost = cost;
			this.command = command;
			this.metrics = metrics;
			this.state = (byte) (TaskState.QUEUED.ordinal()
					| (cancellationTask != null ? DISPATCH_CANCELLATION : 0));
		}

		private static WorkloadTask normal(WorkloadProfile profile,
		                                   OperationFamily family,
		                                   long deadlineEpochMillis,
		                                   long monotonicDeadlineNanos,
		                                   long sequence,
		                                   long enqueuedNanos,
		                                   int cost,
		                                   Runnable command,
		                                   @Nullable CancellationTrackedTask cancellationTask,
		                                   TaskMetrics metrics) {
			if (profile == WorkloadProfile.LATENCY) {
				return new LatencyWorkloadTask(profile,
						family,
						deadlineEpochMillis,
						monotonicDeadlineNanos,
						sequence,
						enqueuedNanos,
						cost,
						command,
						cancellationTask,
						metrics);
			}
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE) {
				return new DeadlineWorkloadTask(profile,
						family,
						deadlineEpochMillis,
						monotonicDeadlineNanos,
						sequence,
						enqueuedNanos,
						cost,
						command,
						cancellationTask,
						metrics);
			}
			return new WorkloadTask(profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					cancellationTask,
					metrics);
		}

		private static CooperativeWorkloadTask cooperative(ProfiledWorkloadExecutor owner,
		                                                   WorkloadProfile profile,
		                                                   OperationFamily family,
		                                                   long deadlineEpochMillis,
		                                                   long monotonicDeadlineNanos,
		                                                   long sequence,
		                                                   long enqueuedNanos,
		                                                   int cost,
		                                                   RWScheduler.CooperativeTask command,
		                                                   TaskMetrics metrics) {
			if (profile == WorkloadProfile.LATENCY || deadlineEpochMillis != RequestContext.NO_DEADLINE) {
				return new OrderedCooperativeWorkloadTask(owner,
						profile,
						family,
						deadlineEpochMillis,
						monotonicDeadlineNanos,
						sequence,
						enqueuedNanos,
						cost,
						command,
						metrics);
			}
			return new CooperativeWorkloadTask(owner,
					profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					metrics);
		}

		private WorkloadProfile profile() {
			return PROFILES[profileIndex()];
		}

		private int profileIndex() {
			return Byte.toUnsignedInt(profile);
		}

		private boolean hasProfile(WorkloadProfile profile) {
			return this.profile == (byte) profile.ordinal();
		}

		private OperationFamily family() {
			return FAMILIES[familyIndex()];
		}

		private int familyIndex() {
			return Byte.toUnsignedInt(family);
		}

		long deadlineEpochMillis() {
			return RequestContext.NO_DEADLINE;
		}

		long monotonicDeadlineNanos() {
			return Long.MAX_VALUE;
		}

		private boolean hasDeadline() {
			return (state & HAS_DEADLINE) != 0;
		}

		final void markHasDeadline() {
			state |= HAS_DEADLINE;
		}

		private long deadlineSequence() {
			return deadlineSequence;
		}

		private long sequence() {
			return this instanceof CooperativeWorkloadTask cooperativeTask
					? cooperativeTask.sequence
					: deadlineSequence;
		}

		private void sequence(long replacement) {
			if (replacement < 0L) {
				throw new IllegalArgumentException("Workload ordering sequence must not be negative");
			}
			if (this instanceof CooperativeWorkloadTask cooperativeTask) {
				cooperativeTask.sequence = replacement;
			} else {
				deadlineSequence = replacement;
			}
		}

		private void deadlineSequence(long replacement) {
			if (replacement < 0L) {
				throw new IllegalArgumentException("Workload deadline sequence must not be negative");
			}
			deadlineSequence = replacement;
		}

		private long enqueuedNanos() {
			return enqueuedNanos;
		}

		int latencyHeapIndex() {
			return -1;
		}

		void latencyHeapIndex(int index) {
			throw new IllegalStateException("Task has no latency-heap index");
		}

		int deadlineHeapIndex() {
			return -1;
		}

		void deadlineHeapIndex(int index) {
			throw new IllegalStateException("Task has no deadline-heap index");
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
			return this instanceof CooperativeWorkloadTask;
		}

		private CooperativeWorkloadTask cooperativeTask() {
			if (this instanceof CooperativeWorkloadTask cooperativeTask) {
				return cooperativeTask;
			}
			throw new IllegalStateException("Normal workload task has no cooperative state");
		}

		private void batchPermit(long batchPermit) {
			if (batchPermit == WorkloadPressureController.NO_BATCH_PERMIT) {
				throw new IllegalArgumentException("Missing BATCH permit");
			}
			this.batchPermit = batchPermit;
		}

		private long takeBatchPermit() {
			long permit = batchPermit;
			batchPermit = WorkloadPressureController.NO_BATCH_PERMIT;
			return permit;
		}

		private @Nullable RWScheduler.TerminalOutcome outcome() {
			return outcome < 0 ? null : TERMINAL_OUTCOMES[outcome];
		}

		private void outcome(RWScheduler.TerminalOutcome outcome) {
			this.outcome = (byte) outcome.ordinal();
		}

		private @Nullable CancellationTrackedTask dispatchCancellation() {
			return (state & DISPATCH_CANCELLATION) == 0
					? null
					: (CancellationTrackedTask) command;
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
			return TASK_STATES[state & STATE_MASK];
		}

		private void state(TaskState state) {
			this.state = (byte) ((this.state & ~STATE_MASK) | state.ordinal());
		}

		private void markQueued() {
			state(TaskState.QUEUED);
		}

		private void markActive() {
			state(TaskState.ACTIVE);
		}

		private void markParked() {
			state(TaskState.PARKED);
		}

		private void markTerminal() {
			state(TaskState.TERMINAL);
			if (this instanceof CooperativeWorkloadTask cooperativeTask) {
				cooperativeTask.terminalPublished = true;
			}
		}

		private boolean competitionActive() {
			return (state & COMPETITION_ACTIVE) != 0;
		}

		private void markCompetitionActive() {
			state |= COMPETITION_ACTIVE;
		}

		private void markCompetitionInactive() {
			state &= (byte) ~COMPETITION_ACTIVE;
		}

		private boolean markStarted() {
			var cooperativeTask = cooperativeTask();
			if (cooperativeTask.flag(CooperativeWorkloadTask.STARTED)) {
				return false;
			}
			cooperativeTask.setFlag(CooperativeWorkloadTask.STARTED);
			return true;
		}

		private boolean hasStarted() {
			return cooperativeTask().flag(CooperativeWorkloadTask.STARTED);
		}

		private void refreshSequence(long sequence) {
			cooperativeTask().sequence = sequence;
		}

		private void recordCooperativeQuantum(long executionStartNanos, long executionNanos) {
			var cooperativeTask = cooperativeTask();
			// queue.wait is admission-to-first-dispatch latency for one logical submission. A parked
			// downstream consumer or a cooperative yield is part of the same task and must not be
			// accumulated into a misleading multi-quantum queue latency. The quantum counter retains
			// the exact number of redispatches; execution remains total logical active time.
			if (cooperativeTask.cooperativeQuantumCount == 0L) {
				cooperativeTask.cooperativeQueueWaitNanos = executionStartNanos - enqueuedNanos;
			}
			cooperativeTask.cooperativeExecutionNanos = saturatingAdd(
					cooperativeTask.cooperativeExecutionNanos, executionNanos);
			cooperativeTask.cooperativeQuantumCount++;
		}

		private void flushCooperativeMetrics() {
			if (!(this instanceof CooperativeWorkloadTask cooperativeTask)
					|| cooperativeTask.flag(CooperativeWorkloadTask.METRICS_FLUSHED)) {
				return;
			}
			cooperativeTask.setFlag(CooperativeWorkloadTask.METRICS_FLUSHED);
			if (metrics == INERT_TASK_METRICS || cooperativeTask.cooperativeQuantumCount == 0L) {
				return;
			}
			// Micrometer's contention-adaptive adders can allocate cells while growing. Keep
			// registry calls off repeated yields and publish one logical-task sample at terminal.
			metrics.queueWait().record(cooperativeTask.cooperativeQueueWaitNanos);
			metrics.quantum().increment(cooperativeTask.cooperativeQuantumCount);
			metrics.execution().record(cooperativeTask.cooperativeExecutionNanos);
		}

		private static long saturatingAdd(long current, long increment) {
			return increment > Long.MAX_VALUE - current ? Long.MAX_VALUE : current + increment;
		}

		private void requestResume() {
			cooperativeTask().setFlag(CooperativeWorkloadTask.RESUME_REQUESTED);
		}

		private void clearResumeRequested() {
			cooperativeTask().clearFlag(CooperativeWorkloadTask.RESUME_REQUESTED);
		}

		private boolean consumeResumeRequested() {
			var cooperativeTask = cooperativeTask();
			boolean result = cooperativeTask.flag(CooperativeWorkloadTask.RESUME_REQUESTED);
			cooperativeTask.clearFlag(CooperativeWorkloadTask.RESUME_REQUESTED);
			return result;
		}

		private boolean requestTermination(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
			var cooperativeTask = cooperativeTask();
			return CooperativeWorkloadTask.REQUESTED_TERMINATION.compareAndSet(
					cooperativeTask, null, new RequestedTermination(outcome, failure));
		}

		private @Nullable RWScheduler.TerminalOutcome requestedOutcome() {
			var requested = cooperativeTask().requestedTermination;
			return requested == null ? null : requested.outcome();
		}

		@Override
		public boolean preemptionRequested() {
			return cooperativeTask().owner.cooperativePreemptionRequested(profile());
		}

		@Override
		public boolean terminationRequested() {
			var cooperativeTask = cooperativeTask();
			if (cooperativeTask.requestedTermination != null) {
				return true;
			}
			if (hasDeadline()
					&& cooperativeTask().owner.deadlineClock.monotonicNanos() >= monotonicDeadlineNanos()) {
				CooperativeWorkloadTask.REQUESTED_TERMINATION.compareAndSet(
						cooperativeTask, null, new RequestedTermination(
						RWScheduler.TerminalOutcome.DEADLINE,
						deadlineFailure("Cooperative workload deadline expired while active")));
				return true;
			}
			return false;
		}

		@Override
		public @Nullable RuntimeException terminationFailure() {
			var requested = cooperativeTask().requestedTermination;
			return requested == null ? null : requested.failure();
		}

		@Override
		public boolean fail(RuntimeException failure) {
			return requestTermination(RWScheduler.TerminalOutcome.FAILURE,
					Objects.requireNonNull(failure, "failure"));
		}

		@Override
		public void resume() {
			cooperativeTask().owner.resumeCooperative(this);
		}

		@Override
		public boolean cancel() {
			return cooperativeTask().owner.cancelCooperative(this);
		}

		@Override
		public void dispose() {
			cancel();
		}

		@Override
		public void terminate(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
			cooperativeTask().owner.requestCooperativeTermination(
					this, Objects.requireNonNull(outcome), Objects.requireNonNull(failure));
		}

		@Override
		public boolean isDisposed() {
			var cooperativeTask = cooperativeTask();
			return cooperativeTask.terminalPublished
					|| cooperativeTask.requestedTermination != null;
		}
	}

	private static final class LatencyWorkloadTask extends WorkloadTask {

		private final long deadlineEpochMillis;
		private final long monotonicDeadlineNanos;
		private int latencyHeapIndex = -1;
		private int deadlineHeapIndex = -1;

		private LatencyWorkloadTask(WorkloadProfile profile,
		                                OperationFamily family,
		                                long deadlineEpochMillis,
		                                long monotonicDeadlineNanos,
		                                long sequence,
		                                long enqueuedNanos,
		                                int cost,
		                                Runnable command,
		                                @Nullable CancellationTrackedTask cancellationTask,
		                                TaskMetrics metrics) {
			super(profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					cancellationTask,
					metrics);
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE) {
				markHasDeadline();
			}
		}

		@Override
		long deadlineEpochMillis() {
			return deadlineEpochMillis;
		}

		@Override
		long monotonicDeadlineNanos() {
			return monotonicDeadlineNanos;
		}

		@Override
		int latencyHeapIndex() {
			return latencyHeapIndex;
		}

		@Override
		void latencyHeapIndex(int index) {
			latencyHeapIndex = index;
		}

		@Override
		int deadlineHeapIndex() {
			return deadlineHeapIndex;
		}

		@Override
		void deadlineHeapIndex(int index) {
			deadlineHeapIndex = index;
		}

	}

	private static final class DeadlineWorkloadTask extends WorkloadTask {

		private final long deadlineEpochMillis;
		private final long monotonicDeadlineNanos;
		private int deadlineHeapIndex = -1;

		private DeadlineWorkloadTask(WorkloadProfile profile,
		                                 OperationFamily family,
		                                 long deadlineEpochMillis,
		                                 long monotonicDeadlineNanos,
		                                 long sequence,
		                                 long enqueuedNanos,
		                                 int cost,
		                                 Runnable command,
		                                 @Nullable CancellationTrackedTask cancellationTask,
		                                 TaskMetrics metrics) {
			super(profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					cancellationTask,
					metrics);
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			markHasDeadline();
		}

		@Override
		long deadlineEpochMillis() {
			return deadlineEpochMillis;
		}

		@Override
		long monotonicDeadlineNanos() {
			return monotonicDeadlineNanos;
		}

		@Override
		int deadlineHeapIndex() {
			return deadlineHeapIndex;
		}

		@Override
		void deadlineHeapIndex(int index) {
			deadlineHeapIndex = index;
		}
	}

	private static class CooperativeWorkloadTask extends WorkloadTask {

		private static final int ADMITTED = 1;
		private static final int RESUME_REQUESTED = 1 << 1;
		private static final int STARTED = 1 << 2;
		private static final int METRICS_FLUSHED = 1 << 3;
		private static final VarHandle REQUESTED_TERMINATION;

		static {
			try {
				REQUESTED_TERMINATION = MethodHandles.lookup().findVarHandle(
						CooperativeWorkloadTask.class, "requestedTermination", RequestedTermination.class);
			} catch (NoSuchFieldException | IllegalAccessException failure) {
				throw new ExceptionInInitializerError(failure);
			}
		}

		private final ProfiledWorkloadExecutor owner;
		private @Nullable CooperativeWorkloadTask previousLifetime;
		private @Nullable CooperativeWorkloadTask nextLifetime;
		private volatile @Nullable RequestedTermination requestedTermination;
		private volatile boolean terminalPublished;
		private byte flags;
		private long sequence;
		private long cooperativeQueueWaitNanos;
		private long cooperativeExecutionNanos;
		private long cooperativeQuantumCount;

		private CooperativeWorkloadTask(ProfiledWorkloadExecutor owner,
		                                WorkloadProfile profile,
		                                OperationFamily family,
		                                long sequence,
		                                long enqueuedNanos,
		                                int cost,
		                                Runnable command,
		                                TaskMetrics metrics) {
			super(profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					null,
					metrics);
			this.owner = owner;
			this.sequence = sequence;
		}

		private boolean flag(int flag) {
			return (flags & flag) != 0;
		}

		private void setFlag(int flag) {
			flags |= (byte) flag;
		}

		private void clearFlag(int flag) {
			flags &= (byte) ~flag;
		}
	}

	private static final class OrderedCooperativeWorkloadTask extends CooperativeWorkloadTask {

		private final long deadlineEpochMillis;
		private final long monotonicDeadlineNanos;
		private int deadlineHeapIndex = -1;

		private OrderedCooperativeWorkloadTask(ProfiledWorkloadExecutor owner,
		                                           WorkloadProfile profile,
		                                       OperationFamily family,
		                                       long deadlineEpochMillis,
		                                       long monotonicDeadlineNanos,
		                                           long sequence,
		                                           long enqueuedNanos,
		                                           int cost,
		                                           Runnable command,
		                                           TaskMetrics metrics) {
			super(owner,
					profile,
					family,
					sequence,
					enqueuedNanos,
					cost,
					command,
					metrics);
			this.deadlineEpochMillis = deadlineEpochMillis;
			this.monotonicDeadlineNanos = monotonicDeadlineNanos;
			if (deadlineEpochMillis != RequestContext.NO_DEADLINE) {
				markHasDeadline();
			}
		}

		@Override
		long deadlineEpochMillis() {
			return deadlineEpochMillis;
		}

		@Override
		long monotonicDeadlineNanos() {
			return monotonicDeadlineNanos;
		}

		@Override
		int deadlineHeapIndex() {
			return deadlineHeapIndex;
		}

		@Override
		void deadlineHeapIndex(int index) {
			deadlineHeapIndex = index;
		}
	}

	private record RequestedTermination(RWScheduler.TerminalOutcome outcome, RuntimeException failure) {
	}

	interface CancellationTrackedTask {

		boolean workloadCancellationRequested();

		boolean claimWorkloadDispatch();
	}

	private enum TaskState {
		QUEUED,
		ACTIVE,
		PARKED,
		TERMINAL
	}

	/**
	 * Intrusive binary heap for dispatch or expiry indexes. The backing reference array is contiguous and grows
	 * only at a new queue high-water mark; task insertion/removal allocates no per-entry tree node.
	 */
	private static final class TaskHeap {

		private static final int INITIAL_CAPACITY = 16;

		private final boolean deadlineIndex;
		private WorkloadTask[] elements = new WorkloadTask[INITIAL_CAPACITY];
		private int size;

		private TaskHeap(boolean deadlineIndex) {
			this.deadlineIndex = deadlineIndex;
		}

		private boolean isEmpty() {
			return size == 0;
		}

		private WorkloadTask first() {
			if (size == 0) {
				throw new IllegalStateException("EDF task heap is empty");
			}
			return Objects.requireNonNull(elements[0]);
		}

		private boolean add(WorkloadTask task) {
			if (indexOf(task) >= 0) {
				return false;
			}
			ensureCapacity(size + 1);
			siftUp(size, task);
			size++;
			return true;
		}

		private boolean remove(WorkloadTask task) {
			int index = indexOf(task);
			if (index < 0) {
				return false;
			}
			if (index >= size || elements[index] != task) {
				throw new IllegalStateException("Corrupt EDF task heap index");
			}
			int lastIndex = --size;
			var moved = elements[lastIndex];
			elements[lastIndex] = null;
			setIndex(task, -1);
			if (index == lastIndex) {
				return true;
			}
			var movedTask = Objects.requireNonNull(moved);
			int parentIndex = (index - 1) >>> 1;
			if (index > 0 && compare(movedTask, elements[parentIndex]) < 0) {
				siftUp(index, movedTask);
			} else {
				siftDown(index, movedTask);
			}
			return true;
		}

		private void siftUp(int index, WorkloadTask task) {
			while (index > 0) {
				int parentIndex = (index - 1) >>> 1;
				var parent = Objects.requireNonNull(elements[parentIndex]);
				if (compare(task, parent) >= 0) {
					break;
				}
				elements[index] = parent;
				setIndex(parent, index);
				index = parentIndex;
			}
			elements[index] = task;
			setIndex(task, index);
		}

		private void siftDown(int index, WorkloadTask task) {
			int half = size >>> 1;
			while (index < half) {
				int childIndex = (index << 1) + 1;
				var child = Objects.requireNonNull(elements[childIndex]);
				int rightIndex = childIndex + 1;
				if (rightIndex < size) {
					var right = Objects.requireNonNull(elements[rightIndex]);
					if (compare(right, child) < 0) {
						childIndex = rightIndex;
						child = right;
					}
				}
				if (compare(task, child) <= 0) {
					break;
				}
				elements[index] = child;
				setIndex(child, index);
				index = childIndex;
			}
			elements[index] = task;
			setIndex(task, index);
		}

		private void ensureCapacity(int required) {
			if (required <= elements.length) {
				return;
			}
			int grown = elements.length + (elements.length >>> 1);
			elements = Arrays.copyOf(elements, Math.max(required, grown));
		}

		private int compare(WorkloadTask left, WorkloadTask right) {
			return (deadlineIndex ? EXPIRY_ORDER : EDF_ORDER).compare(left, right);
		}

		private void collectSequenceOwners(IdentityHashMap<Object, Boolean> target) {
			for (int index = 0; index < size; index++) {
				target.put(Objects.requireNonNull(elements[index]), Boolean.TRUE);
			}
		}

		private int indexOf(WorkloadTask task) {
			return deadlineIndex ? task.deadlineHeapIndex() : task.latencyHeapIndex();
		}

		private void setIndex(WorkloadTask task, int index) {
			if (deadlineIndex) {
				task.deadlineHeapIndex(index);
			} else {
				task.latencyHeapIndex(index);
			}
		}
	}

	private static final class TaskQueue {

		private @Nullable WorkloadTask first;
		private @Nullable WorkloadTask last;

		private void addLast(WorkloadTask task) {
			if (task.profileQueued || task.previousQueue != null || task.nextQueue != null) {
				throw new IllegalStateException("Workload task is already queued");
			}
			if (last == null) {
				first = task;
			} else {
				last.nextQueue = task;
				task.previousQueue = last;
			}
			last = task;
			task.profileQueued = true;
		}

		private boolean remove(WorkloadTask task) {
			if (!task.profileQueued) {
				return false;
			}
			var previous = task.previousQueue;
			var next = task.nextQueue;
			if (previous == null) {
				if (first != task) {
					throw new IllegalStateException("Workload task is not in this queue");
				}
				first = next;
			} else {
				previous.nextQueue = next;
			}
			if (next == null) {
				if (last != task) {
					throw new IllegalStateException("Workload task is not in this queue");
				}
				last = previous;
			} else {
				next.previousQueue = previous;
			}
			task.previousQueue = null;
			task.nextQueue = null;
			task.profileQueued = false;
			return true;
		}

		private @Nullable WorkloadTask peekFirst() {
			return first;
		}

		private void collectSequenceOwners(IdentityHashMap<Object, Boolean> target) {
			for (var task = first; task != null; task = task.nextQueue) {
				target.put(task, Boolean.TRUE);
			}
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

		private void collectSequenceOwners(IdentityHashMap<Object, Boolean> target) {
			for (var bucket : buckets) {
				for (var entry = bucket; entry != null; entry = entry.bucketNext) {
					for (var task = entry.firstQueued; task != null; task = task.nextCancellation) {
						target.put(task, Boolean.TRUE);
					}
				}
			}
		}

		private void map(WorkloadTask task) {
			if (task.cancellationEntry != null) {
				throw new IllegalStateException("Workload task is already cancellation-indexed: "
						+ task.sequence());
			}
			int profile = task.profileIndex();
			int family = task.familyIndex();
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
				var previousHead = buckets[bucket];
				entry.bucketNext = previousHead;
				if (previousHead != null) {
					previousHead.bucketPrevious = entry;
				}
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
			var previous = entry.bucketPrevious;
			var next = entry.bucketNext;
			if (previous == null) {
				if (buckets[bucket] != entry) {
					throw new IllegalStateException("Cancellation entry is not hash-indexed");
				}
				buckets[bucket] = next;
			} else {
				previous.bucketNext = next;
			}
			if (next != null) {
				next.bucketPrevious = previous;
			}
			entry.command = null;
			entry.bucketPrevious = null;
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
					var previousHead = replacement[bucket];
					entry.bucketPrevious = null;
					entry.bucketNext = previousHead;
					if (previousHead != null) {
						previousHead.bucketPrevious = entry;
					}
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
			if (outcome == RWScheduler.TerminalOutcome.FAILURE) {
				failure.increment();
			}
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

	static final long NO_BATCH_PERMIT = 0L;
	// Allocation-free permit token: flags, then two independent 30-bit episode generations.
	private static final long ACQUIRED_FLAG = 1L;
	private static final long PRESSURED_FLAG = 1L << 1;
	private static final long COMPETING_FLAG = 1L << 2;
	private static final int PRESSURE_GENERATION_SHIFT = 3;
	private static final int COMPETITION_GENERATION_SHIFT = 33;
	private static final int GENERATION_MASK = (1 << 30) - 1;
	private final int competingMaximumActiveReadBatches;
	private final int competingMaximumActiveWriteBatches;
	private final long competingWriteIntervalNanos;
	private final int pressuredMaximumActiveBatches;
	private final long competitionHoldNanos;
	private final long batchIntervalNanos;
	private volatile boolean pressured;
	private int activeBatches;
	private int activeReadBatches;
	private int activeWriteBatches;
	private long nextCompetingWriteNanos = Long.MIN_VALUE;
	private long nextBatchNanos = Long.MIN_VALUE;
	private int preemptionPoolMask;
	private int competitionPoolMask;
	private int queuedBatchPoolMask;
	private volatile BooleanSupplier readBatchDispatchable = () -> false;
	private volatile BooleanSupplier writeBatchDispatchable = () -> false;
	private int lastCompletedPressuredBatchPoolBit;
	private int pressureGeneration;
	private int competitionGeneration;
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

	void setBatchDispatchabilitySources(BooleanSupplier readBatchDispatchable,
	                                   BooleanSupplier writeBatchDispatchable) {
		this.readBatchDispatchable = Objects.requireNonNull(readBatchDispatchable,
				"readBatchDispatchable");
		this.writeBatchDispatchable = Objects.requireNonNull(writeBatchDispatchable,
				"writeBatchDispatchable");
	}

	boolean pressureActive() {
		return pressured;
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
		long nowNanos = System.nanoTime();
		competitionActiveUnsafe(nowNanos);
		int bit = poolBit(pool);
		int previousMask = competitionPoolMask;
		boolean wasCompeting = competitionPoolMask != 0;
		competitionPoolMask = competing ? competitionPoolMask | bit : competitionPoolMask & ~bit;
		if (competitionPoolMask != 0) {
			competitionUntilNanos = Long.MAX_VALUE;
		} else if (wasCompeting) {
			competitionUntilNanos = saturatingDeadline(nowNanos, competitionHoldNanos);
			notificationPending = true;
		}
		if (competitionPoolMask != previousMask
				&& pressured
				&& lastCompletedPressuredBatchPoolBit != 0
				&& batchDispatchable(lastCompletedPressuredBatchPoolBit)) {
			// Entering or leaving competition can change whether the peer can consume a
			// reserved pressure slot. The caller owns an executor lock, so publish the
			// cross-pool wakeup only after it unlocks.
			notificationPending = true;
		}
	}

	synchronized void setBatchQueued(RWScheduler.Pool pool, boolean queued) {
		int bit = poolBit(pool);
		boolean releasedFairnessWait = !queued
				&& pressured
				&& lastCompletedPressuredBatchPoolBit != 0
				&& bit != lastCompletedPressuredBatchPoolBit
				&& (queuedBatchPoolMask & bit) != 0
				&& (queuedBatchPoolMask & lastCompletedPressuredBatchPoolBit) != 0;
		queuedBatchPoolMask = queued ? queuedBatchPoolMask | bit : queuedBatchPoolMask & ~bit;
		if (releasedFairnessWait) {
			// Queue transitions happen under an executor lock. Defer the cross-pool
			// wakeup until that executor calls signalPendingAvailability after unlock.
			notificationPending = true;
		}
	}

	void batchDispatchabilityLost(RWScheduler.Pool pool) {
		if (!pressured) return;
		synchronized (this) {
			int bit = poolBit(resourcePool(pool));
			if (pressured
					&& lastCompletedPressuredBatchPoolBit != 0
					&& bit != lastCompletedPressuredBatchPoolBit
					&& batchDispatchable(lastCompletedPressuredBatchPoolBit)) {
				// The last pool may be asleep indefinitely waiting for this peer's turn.
				// The caller owns an executor lock, so defer cross-pool signaling until unlock.
				notificationPending = true;
			}
		}
	}

	synchronized boolean isBatchDispatchable(RWScheduler.Pool pool) {
		return batchDispatchable(resourcePool(pool));
	}

	synchronized boolean hasFairPressureTurn(RWScheduler.Pool pool) {
		long nowNanos = System.nanoTime();
		return hasFairPressureTurnUnsafe(resourcePool(pool),
				nowNanos,
				competitionActiveUnsafe(nowNanos));
	}

	private boolean competitionActiveUnsafe(long nowNanos) {
		if (competitionPoolMask != 0 || nowNanos < competitionUntilNanos) {
			return true;
		}
		if (competitionUntilNanos != Long.MIN_VALUE) {
			competitionUntilNanos = Long.MIN_VALUE;
			nextCompetingWriteNanos = Long.MIN_VALUE;
			competitionGeneration = nextGeneration(competitionGeneration);
		}
		return false;
	}

	private static int nextGeneration(int generation) {
		return generation == GENERATION_MASK ? 0 : generation + 1;
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
			if (this.pressured != pressured) {
				pressureGeneration = nextGeneration(pressureGeneration);
			}
			this.pressured = pressured;
			if (!pressured) {
				nextBatchNanos = Long.MIN_VALUE;
				lastCompletedPressuredBatchPoolBit = 0;
			}
		}
		notifier.run();
	}

	synchronized boolean canStartBatch(boolean ignoreLimits,
	                                  RWScheduler.Pool pool,
	                                  long nowNanos) {
		return batchStartAllowanceUnsafe(ignoreLimits, pool, nowNanos) > 0;
	}

	synchronized int batchStartAllowance(boolean ignoreLimits,
	                                    RWScheduler.Pool pool,
	                                    long nowNanos) {
		return batchStartAllowanceUnsafe(ignoreLimits, pool, nowNanos);
	}

	private int batchStartAllowanceUnsafe(boolean ignoreLimits,
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
			if (!hasFairPressureTurnUnsafe(dataPool, nowNanos, competing)) {
				return 0;
			}
			allowance = Math.min(allowance,
					Math.max(0, pressuredMaximumActiveBatches - activeBatches));
		}
		return allowance;
	}

	synchronized long tryStartBatch(boolean ignoreLimits,
	                                RWScheduler.Pool pool,
	                                long nowNanos) {
		Objects.requireNonNull(pool, "pool");
		if (pool != RWScheduler.Pool.READ && pool != RWScheduler.Pool.WRITE) {
			return NO_BATCH_PERMIT;
		}
		var dataPool = resourcePool(pool);
		boolean startedUnderCompetition = false;
		boolean startedUnderPressure = false;
		if (!ignoreLimits) {
			startedUnderCompetition = competitionActiveUnsafe(nowNanos);
			if (startedUnderCompetition) {
				if (activeBatches(dataPool) >= competingMaximumActiveBatches(dataPool)
						|| dataPool == RWScheduler.Pool.WRITE && nowNanos < nextCompetingWriteNanos) {
					return NO_BATCH_PERMIT;
				}
			}
			startedUnderPressure = pressured;
			if (startedUnderPressure
					&& (nowNanos < nextBatchNanos
					|| !hasFairPressureTurnUnsafe(dataPool, nowNanos, startedUnderCompetition)
					|| activeBatches >= pressuredMaximumActiveBatches)) {
				return NO_BATCH_PERMIT;
			}
		}
		activeBatches++;
		incrementActiveBatches(dataPool);
		return batchPermit(startedUnderPressure,
				startedUnderCompetition,
				pressureGeneration,
				competitionGeneration);
	}

	private static long batchPermit(boolean startedUnderPressure,
	                                boolean startedUnderCompetition,
	                                int pressureGeneration,
	                                int competitionGeneration) {
		long permit = ACQUIRED_FLAG;
		if (startedUnderPressure) {
			permit |= PRESSURED_FLAG | (long) pressureGeneration << PRESSURE_GENERATION_SHIFT;
		}
		if (startedUnderCompetition) {
			permit |= COMPETING_FLAG | (long) competitionGeneration << COMPETITION_GENERATION_SHIFT;
		}
		return permit;
	}

	void finishBatch(long permit, RWScheduler.Pool completedPool) {
		if (permit == NO_BATCH_PERMIT) {
			throw new IllegalArgumentException("Missing BATCH permit");
		}
		int otherQueuedBatchPools;
		synchronized (this) {
			releaseActiveBatchUnsafe(completedPool);
			otherQueuedBatchPools = queuedBatchPoolMask & ~poolBit(completedPool);
			boolean competitionCompletion = completedPool == RWScheduler.Pool.WRITE
					&& startedUnderCompetition(permit);
			boolean pressureCompletion = startedUnderPressure(permit) && pressured;
			if (competitionCompletion || pressureCompletion) {
				long nowNanos = System.nanoTime();
				if (competitionCompletion
						&& competitionActiveUnsafe(nowNanos)
						&& competitionGeneration(permit) == competitionGeneration) {
					nextCompetingWriteNanos = saturatingDeadline(nowNanos, competingWriteIntervalNanos);
				}
				if (pressureCompletion && pressureGeneration(permit) == pressureGeneration) {
				lastCompletedPressuredBatchPoolBit = poolBit(completedPool);
					nextBatchNanos = saturatingDeadline(nowNanos, batchIntervalNanos);
				}
			}
		}
		if (otherQueuedBatchPools != 0) {
			batchNotifier.accept(otherQueuedBatchPools);
		}
	}

	void abortBatch(long permit, RWScheduler.Pool abortedPool) {
		if (permit == NO_BATCH_PERMIT) {
			throw new IllegalArgumentException("Missing BATCH permit");
		}
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
		if (!hasFairPressureTurnUnsafe(dataPool, nowNanos, competing)) {
			return Long.MAX_VALUE;
		}
		if (nowNanos < nextBatchNanos) {
			waitNanos = Math.max(waitNanos, nextBatchNanos - nowNanos);
		}
		return waitNanos;
	}

	private boolean hasFairPressureTurnUnsafe(RWScheduler.Pool pool,
	                                          long nowNanos,
	                                          boolean competing) {
		int currentPoolBit = poolBit(pool);
		if (lastCompletedPressuredBatchPoolBit == 0
				|| lastCompletedPressuredBatchPoolBit != currentPoolBit) {
			return true;
		}
		// With more than one free slot, admitting this pool still preserves a slot for
		// the peer. Serialize only the final slot; this keeps the cap work-conserving.
		int availableSlots = pressuredMaximumActiveBatches - activeBatches;
		if (availableSlots > 1) {
			return true;
		}
		var otherPool = pool == RWScheduler.Pool.READ
				? RWScheduler.Pool.WRITE
				: RWScheduler.Pool.READ;
		if (!batchDispatchable(otherPool)) {
			return true;
		}
		return availableSlots == 1
				&& !competitionAllowsStartUnsafe(otherPool, nowNanos, competing);
	}

	private boolean competitionAllowsStartUnsafe(RWScheduler.Pool pool,
	                                             long nowNanos,
	                                             boolean competing) {
		return !competing
				|| activeBatches(pool) < competingMaximumActiveBatches(pool)
				&& (pool != RWScheduler.Pool.WRITE || nowNanos >= nextCompetingWriteNanos);
	}

	private boolean batchDispatchable(RWScheduler.Pool pool) {
		return pool == RWScheduler.Pool.READ
				? readBatchDispatchable.getAsBoolean()
				: writeBatchDispatchable.getAsBoolean();
	}

	private boolean batchDispatchable(int poolBit) {
		return poolBit == poolBit(RWScheduler.Pool.READ)
				? readBatchDispatchable.getAsBoolean()
				: writeBatchDispatchable.getAsBoolean();
	}

	private static boolean startedUnderPressure(long permit) {
		return (permit & PRESSURED_FLAG) != 0L;
	}

	private static boolean startedUnderCompetition(long permit) {
		return (permit & COMPETING_FLAG) != 0L;
	}

	private static int pressureGeneration(long permit) {
		return (int) (permit >>> PRESSURE_GENERATION_SHIFT) & GENERATION_MASK;
	}

	private static int competitionGeneration(long permit) {
		return (int) (permit >>> COMPETITION_GENERATION_SHIFT) & GENERATION_MASK;
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
