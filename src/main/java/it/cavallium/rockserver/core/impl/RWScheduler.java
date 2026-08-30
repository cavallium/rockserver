package it.cavallium.rockserver.core.impl;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

/**
 * Workload-aware shared read/write admission for one database.
 *
 * <p>LATENCY requests are ordered by an immutable local monotonic deadline. INGEST, CDC, ANALYTICAL,
 * and BATCH use byte-cost deficit round-robin. Read and write pools each reserve
 * one borrowable slot for LATENCY, INGEST, and CDC by default while retaining a
 * hard worker cap. CONTROL and physical maintenance have isolated pools.</p>
 */
public final class RWScheduler {
	private static final long UNBOUND_MONOTONIC_DEADLINE = Long.MIN_VALUE;

	public static final int POOL_TELEMETRY_WORKER_COUNT = 0;
	public static final int POOL_TELEMETRY_WAITING_WORKERS = 1;
	public static final int POOL_TELEMETRY_QUEUED_TASKS = 2;
	public static final int POOL_TELEMETRY_ACTIVE_TASKS = 3;
	public static final int POOL_TELEMETRY_PARKED_TASKS = 4;
	public static final int POOL_TELEMETRY_OUTSTANDING_TASKS = 5;
	public static final int POOL_TELEMETRY_SUBMISSION_ATTEMPTS = 6;
	public static final int POOL_TELEMETRY_ACCEPTED_TASKS = 7;
	public static final int POOL_TELEMETRY_STARTED_TASKS = 8;
	public static final int POOL_TELEMETRY_COMPLETED_TASKS = 9;
	public static final int POOL_TELEMETRY_FAILED_TASKS = 10;
	public static final int POOL_TELEMETRY_TERMINAL_OUTCOMES = 11;
	public static final int POOL_TELEMETRY_BATCH_LIMITED = 12;
	public static final int POOL_TELEMETRY_BATCH_ALLOWANCE = 13;
	public static final int POOL_TELEMETRY_SCALARS = 14;
	public static final int POOL_TELEMETRY_QUEUED_BY_PROFILE = POOL_TELEMETRY_SCALARS;
	public static final int POOL_TELEMETRY_ACTIVE_BY_PROFILE =
			POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.values().length;
	public static final int POOL_TELEMETRY_LENGTH =
			POOL_TELEMETRY_ACTIVE_BY_PROFILE + WorkloadProfile.values().length;

	private static final long SHUTDOWN_WAIT_SECONDS = 10L;
	private static final Logger LOG = LoggerFactory.getLogger(RWScheduler.class);

	private final ProfiledWorkloadExecutor readPool;
	private final ProfiledWorkloadExecutor writePool;
	private final ProfiledWorkloadExecutor controlPool;
	private final ProfiledWorkloadExecutor physicalPool;
	private final List<ProfiledWorkloadExecutor> pools;
	private final WorkloadPressureController pressureController;
	private final SchedulerDeadlineClock deadlineClock;
	private final ThreadLocal<DeadlineBindingSlot> localDeadlineBinding = new ThreadLocal<>();
	private final WorkloadExecutor[][] noDeadlineExecutors;

	public RWScheduler(int readCap, int writeCap, String name) {
		this(WorkloadSettings.defaults(readCap, writeCap), name, null, name, true,
				SchedulerDeadlineClock.system());
	}

	public RWScheduler(WorkloadSettings settings,
			String name,
			@Nullable MeterRegistry registry,
			String databaseName) {
		this(settings, name, registry, databaseName, true, SchedulerDeadlineClock.system());
	}

	private RWScheduler(WorkloadSettings settings,
			String name,
			@Nullable MeterRegistry registry,
			String databaseName,
			boolean productionCapacities,
			SchedulerDeadlineClock deadlineClock) {
		Objects.requireNonNull(settings, "settings");
		Objects.requireNonNull(name, "name");
		Objects.requireNonNull(databaseName, "databaseName");
		Objects.requireNonNull(deadlineClock, "deadlineClock");
		this.deadlineClock = deadlineClock;
		if (productionCapacities
				&& (settings.readParallelism() < WorkloadSettings.MIN_PRODUCTION_DATA_THREADS
				|| settings.writeParallelism() < WorkloadSettings.MIN_PRODUCTION_DATA_THREADS)) {
			settings.validateProductionCapacities();
		}
		this.pressureController = new WorkloadPressureController(
				settings.competingBatchReadMaximumActive(),
				settings.competingBatchWriteMaximumActive(),
				settings.competingBatchWriteInterval(),
				settings.pressuredBatchMaximumActive(),
				settings.rangeQuantumMaxDuration(),
				settings.pressuredBatchInterval());
		var readCapacities = dataCapacities(settings, Pool.READ);
		var writeCapacities = dataCapacities(settings, Pool.WRITE);
		var drrWeights = settings.drrWeights();
		this.readPool = new ProfiledWorkloadExecutor(settings.readParallelism(),
				settings.analyticalActiveLimit(),
				readCapacities,
				settings.readReservations(),
				settings.latencyBurst(),
				drrWeights,
				name + "-read",
				"read",
				Pool.READ,
				pressureController,
				deadlineClock,
				registry,
				databaseName);
		this.writePool = new ProfiledWorkloadExecutor(settings.writeParallelism(),
				1,
				writeCapacities,
				settings.writeReservations(),
				settings.latencyBurst(),
				drrWeights,
				name + "-write",
				"write",
				Pool.WRITE,
				pressureController,
				deadlineClock,
				registry,
				databaseName);
		this.controlPool = new ProfiledWorkloadExecutor(settings.controlThreads(),
				settings.controlThreads(),
				Map.of(WorkloadProfile.CONTROL, settings.controlQueueCapacity()),
				Map.of(),
				settings.latencyBurst(),
				drrWeights,
				name + "-control",
				"control",
				Pool.CONTROL,
				pressureController,
				deadlineClock,
				registry,
				databaseName);
		this.physicalPool = new ProfiledWorkloadExecutor(settings.physicalConcurrency(),
				settings.physicalConcurrency(),
				Map.of(WorkloadProfile.PHYSICAL_MAINTENANCE, settings.physicalMaintenanceQueueCapacity()),
				Map.of(),
				settings.latencyBurst(),
				drrWeights,
				name + "-physical",
				"physical",
				Pool.PHYSICAL,
				pressureController,
				deadlineClock,
				registry,
				databaseName);
		this.pools = List.of(readPool, writePool, controlPool, physicalPool);
		this.noDeadlineExecutors = createNoDeadlineExecutors();
		pressureController.setBatchDispatchabilitySources(
				readPool::batchDispatchable,
				writePool::batchDispatchable);
		pressureController.setNotifier(this::signalAllPools);
		pressureController.setBatchNotifier(this::signalBatchPools);
		registerStoragePressureGauge(registry, databaseName);
	}

	private WorkloadExecutor[][] createNoDeadlineExecutors() {
		var result = new WorkloadExecutor[WorkloadProfile.values().length][OperationFamily.values().length];
		for (var profile : WorkloadProfile.values()) {
			for (var family : OperationFamily.values()) {
				if (WorkloadAdmission.isAllowed(profile, family)) {
					result[profile.ordinal()][family.ordinal()] = pool(profile, family)
							.view(profile, family, Long.MAX_VALUE);
				}
			}
		}
		return result;
	}

	private static Map<WorkloadProfile, Integer> dataCapacities(WorkloadSettings settings, Pool pool) {
		var capacities = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var entry : settings.queueCapacities().entrySet()) {
			if (profileUsesPool(entry.getKey(), pool)) {
				capacities.put(entry.getKey(), entry.getValue());
			}
		}
		return Map.copyOf(capacities);
	}

	private static boolean profileUsesPool(WorkloadProfile profile, Pool pool) {
		for (var family : OperationFamily.values()) {
			if (WorkloadAdmission.isAllowed(profile, family) && resourceKind(profile, family) == pool) {
				return true;
			}
		}
		return false;
	}

	private void registerStoragePressureGauge(@Nullable MeterRegistry registry, String databaseName) {
		if (registry == null) {
			return;
		}
		try {
			Gauge.builder("rockserver.workload.storage.pressure",
					pressureController,
					value -> value.isPressured() ? 1 : 0)
					.tag("database", databaseName)
					.register(registry);
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable registrationFailure) {
			LOG.error("Failed to register workload storage-pressure gauge: database={}",
					databaseName,
					registrationFailure);
		}
	}

	/** Explicit low-capacity construction for deterministic scheduler tests only. */
	public static RWScheduler forTesting(int readCap,
			int writeCap,
			int analyticalCap,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			String name) {
		return new RWScheduler(WorkloadSettings.testingDefaults(readCap,
				writeCap,
				analyticalCap,
				foregroundQueueCapacity,
				batchQueueCapacity),
				name,
				null,
				name,
				false,
				SchedulerDeadlineClock.system());
	}

	/** Explicit low-capacity construction with metrics for deterministic scheduler tests only. */
	public static RWScheduler forTesting(int readCap,
			int writeCap,
			int analyticalCap,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			String name,
			@Nullable MeterRegistry registry,
			String databaseName) {
		return new RWScheduler(WorkloadSettings.testingDefaults(readCap,
				writeCap,
				analyticalCap,
				foregroundQueueCapacity,
				batchQueueCapacity),
				name,
				registry,
				databaseName,
				false,
				SchedulerDeadlineClock.system());
	}

	@VisibleForTesting
	static RWScheduler forTesting(int readCap,
			int writeCap,
			int analyticalCap,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			String name,
			LongSupplier nanoTimeSource) {
		return new RWScheduler(WorkloadSettings.testingDefaults(readCap,
				writeCap,
				analyticalCap,
				foregroundQueueCapacity,
				batchQueueCapacity),
				name,
				null,
				name,
				false,
				SchedulerDeadlineClock.testing(nanoTimeSource));
	}

	/** Resolve and validate the caller context, then return its resource-specific view. */
	public Scheduler scheduler(RequestContext context, OperationFamily family) {
		var profile = WorkloadAdmission.resolve(context, family);
		return schedulerResolved(profile,
				family,
				resolveMonotonicDeadline(context));
	}

	public WorkloadExecutor executor(RequestContext context, OperationFamily family) {
		var profile = WorkloadAdmission.resolve(context, family);
		return executorResolved(profile,
				family,
				resolveMonotonicDeadline(context));
	}

	/** Internal adapter for a pre-resolved command while retaining a server-bound deadline. */
	public Scheduler scheduler(WorkloadProfile profile,
			OperationFamily family,
			RequestContext context) {
		Objects.requireNonNull(context, "context");
		WorkloadAdmission.validate(profile, family);
		return schedulerResolved(profile,
				family,
				resolveMonotonicDeadline(context));
	}

	/** Internal adapter for a pre-resolved command while retaining a server-bound deadline. */
	public WorkloadExecutor executor(WorkloadProfile profile,
			OperationFamily family,
			RequestContext context) {
		Objects.requireNonNull(context, "context");
		WorkloadAdmission.validate(profile, family);
		return executorResolved(profile,
				family,
				resolveMonotonicDeadline(context));
	}

	/** Internal/server adapter retaining an explicitly bound monotonic deadline. */
	public Scheduler scheduler(WorkloadProfile profile,
			OperationFamily family,
			long localMonotonicDeadlineNanos) {
		WorkloadAdmission.validate(profile, family);
		return schedulerResolved(profile, family, localMonotonicDeadlineNanos);
	}

	/** Internal/server adapter retaining an explicitly bound monotonic deadline. */
	public WorkloadExecutor executor(WorkloadProfile profile,
			OperationFamily family,
			long localMonotonicDeadlineNanos) {
		WorkloadAdmission.validate(profile, family);
		return executorResolved(profile, family, localMonotonicDeadlineNanos);
	}

	private Scheduler schedulerResolved(WorkloadProfile profile,
			OperationFamily family,
			long localMonotonicDeadlineNanos) {
		return new IndexedWorkloadScheduler(pool(profile, family),
				profile,
				family,
				localMonotonicDeadlineNanos);
	}

	private WorkloadExecutor executorResolved(WorkloadProfile profile,
			OperationFamily family,
			long localMonotonicDeadlineNanos) {
		if (localMonotonicDeadlineNanos == Long.MAX_VALUE) {
			return Objects.requireNonNull(noDeadlineExecutors[profile.ordinal()][family.ordinal()]);
		}
		return pool(profile, family).view(profile,
				family,
				localMonotonicDeadlineNanos);
	}

	/** Bind a positive relative budget to this scheduler's monotonic time domain. */
	public long bindTimeoutNanos(long timeoutNanos) {
		if (timeoutNanos == RequestContext.NO_TIMEOUT) {
			return Long.MAX_VALUE;
		}
		if (timeoutNanos <= 0L) {
			return deadlineClock.monotonicNanos();
		}
		return deadlineClock.monotonicDeadlineAfterNanos(timeoutNanos);
	}

	/** Resolve a server sidecar, or bind a direct embedded request exactly once here. */
	public long resolveMonotonicDeadline(RequestContext context) {
		Objects.requireNonNull(context, "context");
		long bound = localMonotonicDeadline(context);
		return bound == UNBOUND_MONOTONIC_DEADLINE
				? bindTimeoutNanos(context.timeoutNanos())
				: bound;
	}

	/** True once an immutable local deadline has elapsed. */
	public boolean isMonotonicDeadlineExpired(long localMonotonicDeadlineNanos) {
		return deadlineClock.monotonicNanos() >= localMonotonicDeadlineNanos;
	}

	/** Remaining time for an immutable local deadline, saturated at zero. */
	public long remainingMonotonicDeadlineNanos(long localMonotonicDeadlineNanos) {
		return deadlineClock.remainingNanos(localMonotonicDeadlineNanos);
	}

	/**
	 * Run a server-bound API dispatch with one immutable local deadline sidecar. Scheduler views
	 * created by the dispatch retain the sidecar after this lexical scope exits.
	 */
	public <T> T withDeadlineBinding(RequestContext context,
			long localMonotonicDeadlineNanos,
			Supplier<T> dispatch) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(dispatch, "dispatch");
		if (localMonotonicDeadlineNanos == UNBOUND_MONOTONIC_DEADLINE) {
			throw new IllegalArgumentException("A deadline binding must be resolved before dispatch");
		}
		var slot = localDeadlineBinding.get();
		if (slot == null) {
			slot = new DeadlineBindingSlot();
			localDeadlineBinding.set(slot);
		}
		RequestContext previousContext = slot.context;
		long previousDeadline = slot.localMonotonicDeadlineNanos;
		slot.context = context;
		slot.localMonotonicDeadlineNanos = localMonotonicDeadlineNanos;
		try {
			return dispatch.get();
		} finally {
			slot.context = previousContext;
			slot.localMonotonicDeadlineNanos = previousDeadline;
		}
	}

	private long localMonotonicDeadline(RequestContext context) {
		var binding = localDeadlineBinding.get();
		return binding != null && binding.context == context
				? binding.localMonotonicDeadlineNanos
				: UNBOUND_MONOTONIC_DEADLINE;
	}

	private static final class DeadlineBindingSlot {

		private @Nullable RequestContext context;
		private long localMonotonicDeadlineNanos = UNBOUND_MONOTONIC_DEADLINE;
	}

	private ProfiledWorkloadExecutor pool(WorkloadProfile profile, OperationFamily family) {
		return switch (resourceKind(profile, family)) {
			case READ -> readPool;
			case WRITE -> writePool;
			case CONTROL -> controlPool;
			case PHYSICAL -> physicalPool;
		};
	}

	/** Authoritative profile/family to physical scheduler-pool routing. */
	public static Pool resourcePool(WorkloadProfile profile, OperationFamily family) {
		WorkloadAdmission.validate(profile, family);
		return resourceKind(profile, family);
	}

	private static Pool resourceKind(WorkloadProfile profile, OperationFamily family) {
		if (profile == WorkloadProfile.CONTROL) {
			return Pool.CONTROL;
		}
		if (profile == WorkloadProfile.PHYSICAL_MAINTENANCE) {
			return Pool.PHYSICAL;
		}
		return switch (family) {
			case MUTATION, FLUSH -> Pool.WRITE;
			case CONTROL -> Pool.CONTROL;
			case COMPACTION -> Pool.PHYSICAL;
			case METADATA, POINT_LOOKUP, BOUNDARY_SEEK, BOUNDED_FAN_OUT,
					RANGE_PAGE, FULL_SCAN_AGGREGATE, WAL_PAGE -> Pool.READ;
		};
	}

	public int queuedTasks(WorkloadProfile profile) {
		return addExact(readPool.queued(profile), writePool.queued(profile),
				controlPool.queued(profile), physicalPool.queued(profile));
	}

	public int activeTasks(WorkloadProfile profile) {
		return addExact(readPool.active(profile), writePool.active(profile),
				controlPool.active(profile), physicalPool.active(profile));
	}

	public int queueCapacity(WorkloadProfile profile) {
		return addExact(readPool.capacity(profile), writePool.capacity(profile),
				controlPool.capacity(profile), physicalPool.capacity(profile));
	}

	private static int addExact(int... values) {
		int result = 0;
		for (int value : values) result = Math.addExact(result, value);
		return result;
	}

	public ProfileAdmissionSnapshot admissionSnapshot() {
		var queued = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		var active = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : WorkloadProfile.values()) {
			queued.put(profile, queuedTasks(profile));
			active.put(profile, activeTasks(profile));
		}
		return new ProfileAdmissionSnapshot(Map.copyOf(queued), Map.copyOf(active), isStoragePressure());
	}

	/** Immutable scheduler-neutral instrumentation for tests and diagnostics. */
	public PoolSnapshot poolSnapshot(Pool pool) {
		var snapshot = switch (Objects.requireNonNull(pool, "pool")) {
			case READ -> readPool.snapshot();
			case WRITE -> writePool.snapshot();
			case CONTROL -> controlPool.snapshot();
			case PHYSICAL -> physicalPool.snapshot();
		};
		return new PoolSnapshot(snapshot.workerCount(),
				snapshot.waitingWorkers(),
				snapshot.queuedTasks(),
				snapshot.activeTasks(),
				snapshot.parkedTasks(),
				snapshot.outstandingTasks(),
				snapshot.submissionAttempts(),
				snapshot.acceptedTasks(),
				snapshot.startedTasks(),
				snapshot.completedTasks(),
				snapshot.failedTasks(),
				snapshot.submissionAttemptsByProfile(),
				snapshot.queuedByProfile(),
				snapshot.activeByProfile(),
				snapshot.parkedByProfile(),
				snapshot.outstandingByProfile(),
				snapshot.outcomes(),
				snapshot.batchDispatchLimited(),
				snapshot.batchStartAllowance(),
				snapshot.workerThreadNames(),
				snapshot.shutdown(),
				snapshot.terminated());
	}

	/**
	 * Copy allocation-free high-frequency pool telemetry into a caller-owned buffer. Full immutable
	 * snapshots remain available through {@link #poolSnapshot(Pool)} for drain and conservation
	 * checks.
	 */
	public void copyPoolTelemetry(Pool pool, long[] target) {
		var executor = switch (Objects.requireNonNull(pool, "pool")) {
			case READ -> readPool;
			case WRITE -> writePool;
			case CONTROL -> controlPool;
			case PHYSICAL -> physicalPool;
		};
		executor.copyPoolTelemetry(target);
	}

	public SchedulerSnapshot instrumentationSnapshot() {
		var pools = new EnumMap<Pool, PoolSnapshot>(Pool.class);
		for (var pool : Pool.values()) {
			pools.put(pool, poolSnapshot(pool));
		}
		return new SchedulerSnapshot(Map.copyOf(pools), isStoragePressure());
	}

	public boolean removeQueuedTask(Executor schedulingView, Runnable task) {
		Objects.requireNonNull(schedulingView, "schedulingView");
		Objects.requireNonNull(task, "task");
		for (var pool : pools()) {
			if (pool.remove(schedulingView, task)) {
				return true;
			}
		}
		return false;
	}

	/** True only while this scheduler, rather than another database scheduler, owns the calling thread. */
	public boolean isExecutingWorkloadTask() {
		for (var pool : pools()) {
			if (pool.isExecutingTask()) {
				return true;
			}
		}
		return false;
	}

	public synchronized void setStoragePressure(boolean pressured) {
		if (pressured) {
			// Enable publication and take each pool's lock-protected snapshot before pressure
			// becomes observable. Subsequent pool transitions publish until pressure is cleared.
			readPool.setBatchDispatchabilityTracking(true);
			writePool.setBatchDispatchabilityTracking(true);
			pressureController.setPressured(true);
		} else {
			pressureController.setPressured(false);
			readPool.setBatchDispatchabilityTracking(false);
			writePool.setBatchDispatchabilityTracking(false);
		}
	}

	public boolean isStoragePressure() {
		return pressureController.isPressured();
	}

	@VisibleForTesting
	public void setBeforeBatchPermitAcquisitionObserverForTesting(Pool pool, @Nullable Runnable observer) {
		var executor = switch (Objects.requireNonNull(pool, "pool")) {
			case READ -> readPool;
			case WRITE -> writePool;
			case CONTROL, PHYSICAL -> throw new IllegalArgumentException(
					"BATCH permit observers require a data pool");
		};
		executor.setBeforeBatchPermitAcquisitionObserverForTesting(observer);
	}

	@VisibleForTesting
	public boolean isBatchDispatchableForTesting(Pool pool) {
		return pressureController.isBatchDispatchable(pool);
	}

	@VisibleForTesting
	public boolean hasFairPressureTurnForTesting(Pool pool) {
		return pressureController.hasFairPressureTurn(pool);
	}

	private void signalAllPools() {
		for (var pool : pools()) {
			pool.signalAvailability();
		}
	}

	private void signalBatchPools(int poolMask) {
		if ((poolMask & 1 << Pool.READ.ordinal()) != 0) {
			readPool.signalOneAvailability();
		}
		if ((poolMask & 1 << Pool.WRITE.ordinal()) != 0) {
			writePool.signalOneAvailability();
		}
	}

	private List<ProfiledWorkloadExecutor> pools() {
		return pools;
	}

	public Mono<Void> disposeGracefully() {
		return Mono.fromRunnable(this::dispose);
	}

	public void dispose() {
		var pools = pools();
		for (var pool : pools) {
			pool.shutdown();
		}
		boolean interrupted = false;
		long gracefulDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_WAIT_SECONDS);
		for (var pool : pools) {
			try {
				awaitUntil(pool, gracefulDeadline);
			} catch (InterruptedException interruption) {
				interrupted = true;
				break;
			}
		}
		var forced = pools.stream().filter(pool -> !pool.isTerminated()).toList();
		for (var pool : forced) {
			pool.shutdownNow();
		}
		long forcedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_WAIT_SECONDS);
		for (var pool : forced) {
			try {
				if (!awaitUntil(pool, forcedDeadline)) {
					LOG.warn("Workload pool did not terminate after forced shutdown: {}", pool);
				}
			} catch (InterruptedException interruption) {
				interrupted = true;
				break;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	/** Force queued work to its SHUTDOWN outcome, interrupt running work, and wait once for termination. */
	public void disposeNow() {
		var pools = pools();
		for (var pool : pools) {
			pool.shutdownNow();
		}
		boolean interrupted = false;
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_WAIT_SECONDS);
		for (var pool : pools) {
			try {
				if (!awaitUntil(pool, deadline)) {
					LOG.warn("Workload pool did not terminate after forced shutdown: {}", pool);
				}
			} catch (InterruptedException interruption) {
				interrupted = true;
				break;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	boolean isFullyTerminated() {
		return pools().stream().allMatch(ProfiledWorkloadExecutor::isTerminated);
	}

	private static boolean awaitUntil(ProfiledWorkloadExecutor pool,
			long deadlineNanos) throws InterruptedException {
		long remaining = deadlineNanos - System.nanoTime();
		return remaining > 0L && pool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
	}

	public static int taskCost(long estimatedBytes) {
		return ProfiledWorkloadExecutor.taskCost(estimatedBytes);
	}

	@FunctionalInterface
	public interface EstimatedWork {

		long estimatedBytes();
	}

	public interface WorkloadExecutor extends Executor {

		void execute(Runnable command, long estimatedBytes);

		default CooperativeHandle executeCooperatively(CooperativeTask command, long estimatedBytes) {
			throw new UnsupportedOperationException("Cooperative execution is not supported by this view");
		}
	}

	/**
	 * A reusable command whose scheduler node survives cooperative yields and backpressure parks.
	 */
	public interface CooperativeTask extends Runnable, RejectionAwareTask {

		CooperativeResult runCooperatively(CooperativeContext context);

		@Override
		default void run() {
			throw new IllegalStateException("Cooperative tasks require a cooperative scheduler context");
		}
	}

	/**
	 * Cooperative task whose successful logical completion is published only after the
	 * scheduler has atomically selected {@link TerminalOutcome#RUN}. Failure outcomes
	 * continue to arrive through {@link RejectionAwareTask#reject(RuntimeException)}.
	 *
	 * <p>This keeps the scheduler as the single terminal authority when a completed
	 * quantum races cancellation, deadline expiry, or shutdown.</p>
	 */
	public interface CooperativeCompletionTask extends CooperativeTask {

		void completeCooperatively();
	}

	/**
	 * Lock-free state exposed to the currently running cooperative quantum.
	 */
	public interface CooperativeContext {

		boolean preemptionRequested();

		boolean terminationRequested();

		@Nullable RuntimeException terminationFailure();

		/**
		 * Atomically publish a command failure as this submission's terminal cause.
		 *
		 * @return {@code true} when this failure won first-cause arbitration against
		 * cancellation, deadline expiry, or shutdown
		 */
		boolean fail(RuntimeException failure);
	}

	/**
	 * Stable handle for resuming parked work or cancelling its logical submission.
	 */
	public interface CooperativeHandle extends reactor.core.Disposable {

		void resume();

		/** Atomically request scheduler cancellation. */
		boolean cancel();
	}

	public enum CooperativeResult {
		COMPLETE,
		YIELD,
		PARK
	}

	/**
	 * Optional submission hook for work that must release logical resources when it is
	 * rejected after admission. The scheduler invokes it exactly once for DEADLINE,
	 * CANCELLATION, OVERLOAD, or SHUTDOWN before disposing the task when applicable.
	 */
	@FunctionalInterface
	public interface RejectionAwareTask {

		void reject(RuntimeException failure);
	}

	public enum TerminalOutcome {
		RUN,
		FAILURE,
		DEADLINE,
		CANCELLATION,
		OVERLOAD,
		SHUTDOWN
	}

	public enum Pool {
		READ,
		WRITE,
		CONTROL,
		PHYSICAL
	}

	public record ProfileAdmissionSnapshot(Map<WorkloadProfile, Integer> queued,
			Map<WorkloadProfile, Integer> active,
			boolean storagePressure) {

		public int totalActive() {
			return active.values().stream().reduce(0, Math::addExact);
		}

		public int totalQueued() {
			return queued.values().stream().reduce(0, Math::addExact);
		}
	}

	public record PoolSnapshot(int workerCount,
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
			Map<TerminalOutcome, Long> outcomes,
			boolean batchDispatchLimited,
			int batchStartAllowance,
			List<String> workerThreadNames,
			boolean shutdown,
			boolean terminated) {

		public long terminalOutcomes() {
			return outcomes.values().stream().reduce(0L, Math::addExact);
		}

		public boolean drainedAndConserved() {
			return queuedTasks == 0
					&& activeTasks == 0
					&& parkedTasks == 0
					&& outstandingTasks == 0
					&& terminalOutcomes() == submissionAttempts;
		}
	}

	public record SchedulerSnapshot(Map<Pool, PoolSnapshot> pools, boolean storagePressure) {
	}
}
