package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.time.Duration;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * Workload-aware shared read/write admission for one database.
 *
 * <p>LATENCY requests are ordered by absolute deadline. INGEST, CDC, ANALYTICAL,
 * and BATCH use weighted deficit round-robin, so no profile depends on a strict
 * global-priority chain. Read and write resources are shared but have independent
 * bounded queues. CONTROL owns a small isolated executor; physical maintenance is
 * serialized.</p>
 */
public final class RWScheduler {

	public static final int DEFAULT_ANALYTICAL_QUEUE_CAPACITY = 512;
	public static final int DEFAULT_BATCH_QUEUE_CAPACITY = 512;
	public static final int DEFAULT_LATENCY_QUEUE_CAPACITY = 4_096;
	public static final int DEFAULT_INGEST_QUEUE_CAPACITY = 4_096;
	public static final int DEFAULT_CDC_QUEUE_CAPACITY = 1_024;
	// Configuration compatibility: these keys predate workload profiles but now
	// size the analytical cap and the shared foreground/batch queues.
	public static final int DEFAULT_MAINTENANCE_WRITE_PARALLELISM = 1;
	public static final int DEFAULT_FOREGROUND_WRITE_QUEUE_CAPACITY = DEFAULT_INGEST_QUEUE_CAPACITY;
	public static final int DEFAULT_MAINTENANCE_WRITE_QUEUE_CAPACITY = DEFAULT_BATCH_QUEUE_CAPACITY;
	private static final int CONTROL_THREADS = 2;
	private static final int CONTROL_QUEUE_CAPACITY = 256;
	private static final int PHYSICAL_QUEUE_CAPACITY = 16;

	private final @Nullable ProfiledWorkloadExecutor readPool;
	private final @Nullable ProfiledWorkloadExecutor writePool;
	private final @Nullable ProfiledWorkloadExecutor controlPool;
	private final @Nullable ProfiledWorkloadExecutor physicalPool;
	private final @Nullable Scheduler externalRead;
	private final @Nullable Scheduler externalWrite;
	private final @Nullable Executor externalReadExecutor;
	private final @Nullable Executor externalWriteExecutor;
	private final AtomicBoolean storagePressure = new AtomicBoolean();

	/** Preserve custom in-process scheduling while requiring contexts at API boundaries. */
	public RWScheduler(Scheduler read, Scheduler write) {
		this(read, write, read::schedule, write::schedule);
	}

	/** Preserve the original custom executor constructor. */
	public RWScheduler(Scheduler read, Scheduler write, Executor readExecutor, Executor writeExecutor) {
		this.readPool = null;
		this.writePool = null;
		this.controlPool = null;
		this.physicalPool = null;
		this.externalRead = Objects.requireNonNull(read, "read");
		this.externalWrite = Objects.requireNonNull(write, "write");
		this.externalReadExecutor = Objects.requireNonNull(readExecutor, "readExecutor");
		this.externalWriteExecutor = Objects.requireNonNull(writeExecutor, "writeExecutor");
	}

	public RWScheduler(int readCap, int writeCap, String name) {
		this(readCap,
				writeCap,
				1,
				DEFAULT_INGEST_QUEUE_CAPACITY,
				DEFAULT_BATCH_QUEUE_CAPACITY,
				name);
	}

	public RWScheduler(int readCap,
			int writeCap,
			int analyticalCap,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			String name) {
		this(readCap,
				writeCap,
				analyticalCap,
				foregroundQueueCapacity,
				batchQueueCapacity,
				name,
				null,
				name);
	}

	public RWScheduler(int readCap,
			int writeCap,
			int analyticalCap,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			String name,
			@Nullable MeterRegistry registry,
			String databaseName) {
		if (readCap < 1 || writeCap < 1 || analyticalCap < 1) {
			throw new IllegalArgumentException("Scheduler capacities must be positive");
		}
		if (foregroundQueueCapacity < 1 || batchQueueCapacity < 1) {
			throw new IllegalArgumentException("Queue capacities must be positive");
		}
		this.externalRead = null;
		this.externalWrite = null;
		this.externalReadExecutor = null;
		this.externalWriteExecutor = null;
		var dataCapacities = dataCapacities(foregroundQueueCapacity, batchQueueCapacity);
		this.readPool = new ProfiledWorkloadExecutor(readCap,
				analyticalCap,
				dataCapacities,
				name + "-read",
				"read",
				storagePressure,
				registry,
				databaseName);
		this.writePool = new ProfiledWorkloadExecutor(writeCap,
				analyticalCap,
				dataCapacities,
				name + "-write",
				"write",
				storagePressure,
				registry,
				databaseName);
		this.controlPool = new ProfiledWorkloadExecutor(CONTROL_THREADS,
				CONTROL_THREADS,
				Map.of(WorkloadProfile.CONTROL, CONTROL_QUEUE_CAPACITY),
				name + "-control",
				"control",
				storagePressure,
				registry,
				databaseName);
		this.physicalPool = new ProfiledWorkloadExecutor(1,
				1,
				Map.of(WorkloadProfile.PHYSICAL_MAINTENANCE, PHYSICAL_QUEUE_CAPACITY),
				name + "-physical",
				"physical",
				storagePressure,
				registry,
				databaseName);
		if (registry != null) {
			Gauge.builder("rockserver.workload.storage.pressure", storagePressure, value -> value.get() ? 1 : 0)
					.tag("database", databaseName)
					.register(registry);
		}
	}

	private static Map<WorkloadProfile, Integer> dataCapacities(int foreground, int batch) {
		var capacities = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		capacities.put(WorkloadProfile.LATENCY, foreground);
		capacities.put(WorkloadProfile.INGEST, foreground);
		capacities.put(WorkloadProfile.CDC, Math.max(64, Math.min(foreground, DEFAULT_CDC_QUEUE_CAPACITY)));
		capacities.put(WorkloadProfile.ANALYTICAL, Math.max(1, Math.min(batch, DEFAULT_ANALYTICAL_QUEUE_CAPACITY)));
		capacities.put(WorkloadProfile.BATCH, batch);
		return capacities;
	}

	/** Resolve and validate the caller context, then return its resource-specific view. */
	public Scheduler scheduler(RequestContext context, OperationFamily family) {
		var profile = WorkloadAdmission.resolve(context, family);
		return scheduler(profile, family, context.deadlineEpochMillis());
	}

	public Executor executor(RequestContext context, OperationFamily family) {
		var profile = WorkloadAdmission.resolve(context, family);
		return executor(profile, family, context.deadlineEpochMillis());
	}

	/** Server-only scheduling entry for protected CDC/control/physical work. */
	public Scheduler scheduler(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis) {
		return Schedulers.fromExecutor(executor(profile, family, deadlineEpochMillis));
	}

	/** Server-only executor entry for protected CDC/control/physical work. */
	public Executor executor(WorkloadProfile profile,
			OperationFamily family,
			long deadlineEpochMillis) {
		WorkloadAdmission.validate(profile, family);
		if (readPool == null) {
			return resourceKind(profile, family) == ResourceKind.READ
					? Objects.requireNonNull(externalReadExecutor)
					: Objects.requireNonNull(externalWriteExecutor);
		}
		return pool(profile, family).view(profile, family, deadlineEpochMillis);
	}

	private ProfiledWorkloadExecutor pool(WorkloadProfile profile, OperationFamily family) {
		return switch (resourceKind(profile, family)) {
			case READ -> Objects.requireNonNull(readPool);
			case WRITE -> Objects.requireNonNull(writePool);
			case CONTROL -> Objects.requireNonNull(controlPool);
			case PHYSICAL -> Objects.requireNonNull(physicalPool);
		};
	}

	private static ResourceKind resourceKind(WorkloadProfile profile, OperationFamily family) {
		if (profile == WorkloadProfile.CONTROL) {
			return ResourceKind.CONTROL;
		}
		if (profile == WorkloadProfile.PHYSICAL_MAINTENANCE) {
			return ResourceKind.PHYSICAL;
		}
		return switch (family) {
			case MUTATION, FLUSH -> ResourceKind.WRITE;
			case CONTROL -> ResourceKind.CONTROL;
			case COMPACTION -> ResourceKind.PHYSICAL;
			case METADATA, POINT_LOOKUP, BOUNDARY_SEEK, BOUNDED_FAN_OUT,
					RANGE_PAGE, FULL_SCAN_AGGREGATE, WAL_PAGE -> ResourceKind.READ;
		};
	}

	/** Legacy internal aliases. Generic public requests must use {@link #scheduler(RequestContext, OperationFamily)}. */
	public Scheduler read() {
		return scheduler(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
	}

	public Scheduler interactiveRead() {
		return scheduler(WorkloadProfile.LATENCY,
				OperationFamily.POINT_LOOKUP,
				System.currentTimeMillis() + Duration.ofMinutes(1).toMillis());
	}

	public Scheduler write() {
		return scheduler(WorkloadProfile.INGEST, OperationFamily.MUTATION, RequestContext.NO_DEADLINE);
	}

	public Scheduler maintenance() {
		return scheduler(WorkloadProfile.PHYSICAL_MAINTENANCE,
				OperationFamily.COMPACTION,
				RequestContext.NO_DEADLINE);
	}

	public Scheduler control() {
		return scheduler(WorkloadProfile.CONTROL, OperationFamily.CONTROL, RequestContext.NO_DEADLINE);
	}

	public Scheduler cdc() {
		return scheduler(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE);
	}

	public Executor readExecutor() {
		return executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
	}

	public Executor interactiveReadExecutor() {
		return executor(WorkloadProfile.LATENCY,
				OperationFamily.POINT_LOOKUP,
				System.currentTimeMillis() + Duration.ofMinutes(1).toMillis());
	}

	public Executor writeExecutor() {
		return executor(WorkloadProfile.INGEST, OperationFamily.MUTATION, RequestContext.NO_DEADLINE);
	}

	public Executor maintenanceExecutor() {
		return executor(WorkloadProfile.PHYSICAL_MAINTENANCE,
				OperationFamily.COMPACTION,
				RequestContext.NO_DEADLINE);
	}

	public Executor controlExecutor() {
		return executor(WorkloadProfile.CONTROL, OperationFamily.CONTROL, RequestContext.NO_DEADLINE);
	}

	public Executor cdcExecutor() {
		return executor(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE);
	}

	public int queuedTasks(WorkloadProfile profile) {
		return readPool == null ? 0 : readPool.queued(profile) + writePool.queued(profile)
				+ controlPool.queued(profile) + physicalPool.queued(profile);
	}

	public int activeTasks(WorkloadProfile profile) {
		return readPool == null ? 0 : readPool.active(profile) + writePool.active(profile)
				+ controlPool.active(profile) + physicalPool.active(profile);
	}

	public int queueCapacity(WorkloadProfile profile) {
		return readPool == null ? 0 : readPool.capacity(profile) + writePool.capacity(profile)
				+ controlPool.capacity(profile) + physicalPool.capacity(profile);
	}

	public ProfileAdmissionSnapshot admissionSnapshot() {
		var queued = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		var active = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : WorkloadProfile.values()) {
			queued.put(profile, queuedTasks(profile));
			active.put(profile, activeTasks(profile));
		}
		return new ProfileAdmissionSnapshot(Map.copyOf(queued), Map.copyOf(active), storagePressure.get());
	}

	public boolean removeQueuedTask(Executor schedulingView, Runnable task) {
		Objects.requireNonNull(schedulingView, "schedulingView");
		Objects.requireNonNull(task, "task");
		if (readPool != null) {
			for (var pool : List.of(readPool, writePool, controlPool, physicalPool)) {
				if (pool.remove(schedulingView, task)) {
					return true;
				}
			}
		}
		return schedulingView instanceof java.util.concurrent.ThreadPoolExecutor threadPool
				&& threadPool.remove(task);
	}

	/** True while the current thread owns any database workload worker. */
	public boolean isExecutingWorkloadTask() {
		return ProfiledWorkloadExecutor.isExecutingTask();
	}

	public void setStoragePressure(boolean pressured) {
		storagePressure.set(pressured);
	}

	public boolean isStoragePressure() {
		return storagePressure.get();
	}

	public Mono<Void> disposeGracefully() {
		return Mono.fromRunnable(this::dispose);
	}

	public void dispose() {
		if (readPool == null) {
			for (var scheduler : distinctExternalSchedulers()) {
				scheduler.dispose();
			}
			for (var executor : distinctExternalExecutors()) {
				shutdownExecutor(executor);
			}
			return;
		}
		for (var pool : List.of(readPool, writePool, controlPool, physicalPool)) {
			shutdownExecutor(pool);
		}
	}

	private List<Scheduler> distinctExternalSchedulers() {
		Set<Scheduler> seen = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
		var result = new java.util.ArrayList<Scheduler>(2);
		for (var scheduler : List.of(Objects.requireNonNull(externalRead), Objects.requireNonNull(externalWrite))) {
			if (seen.add(scheduler)) {
				result.add(scheduler);
			}
		}
		return result;
	}

	private List<Executor> distinctExternalExecutors() {
		Set<Executor> seen = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
		var result = new java.util.ArrayList<Executor>(2);
		for (var executor : List.of(Objects.requireNonNull(externalReadExecutor), Objects.requireNonNull(externalWriteExecutor))) {
			if (seen.add(executor)) {
				result.add(executor);
			}
		}
		return result;
	}

	private static void shutdownExecutor(Executor executor) {
		if (executor instanceof ExecutorService service) {
			service.shutdown();
			try {
				if (!service.awaitTermination(10, TimeUnit.SECONDS)) {
					service.shutdownNow();
				}
			} catch (InterruptedException interrupted) {
				service.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}

	public record ProfileAdmissionSnapshot(Map<WorkloadProfile, Integer> queued,
			Map<WorkloadProfile, Integer> active,
			boolean storagePressure) {

		public int totalActive() {
			return active.values().stream().mapToInt(Integer::intValue).sum();
		}

		public int totalQueued() {
			return queued.values().stream().mapToInt(Integer::intValue).sum();
		}
	}

	private enum ResourceKind {
		READ,
		WRITE,
		CONTROL,
		PHYSICAL
	}
}
