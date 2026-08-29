package it.cavallium.rockserver.core.config;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.RangeBudget;
import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import org.github.gestalt.config.exceptions.GestaltException;

/**
 * Immutable, startup-validated workload configuration used by runtime components.
 * Configuration interfaces remain nullable-friendly for Gestalt; this type does not.
 */
public record WorkloadSettings(
		int readParallelism,
		int writeParallelism,
		int latencyQueueCapacity,
		int ingestQueueCapacity,
		int cdcQueueCapacity,
		int analyticalQueueCapacity,
		int batchQueueCapacity,
		int controlQueueCapacity,
		int physicalMaintenanceQueueCapacity,
		int readLatencyReservation,
		int readIngestReservation,
		int readCdcReservation,
		int writeLatencyReservation,
		int writeIngestReservation,
		int writeCdcReservation,
		int controlThreads,
		int physicalConcurrency,
		int analyticalActiveLimit,
		int retainedAnalyticalSnapshots,
		Duration retainedSnapshotMaximumAge,
		int latencyBurst,
		int ingestDrrWeight,
		int cdcDrrWeight,
		int analyticalDrrWeight,
		int batchDrrWeight,
		int competingBatchReadMaximumActive,
		int competingBatchWriteMaximumActive,
		Duration competingBatchWriteInterval,
		int pressuredBatchMaximumActive,
		Duration pressuredBatchInterval,
		int rangeQuantumMaxItems,
		long rangeQuantumMaxBytes,
		Duration rangeQuantumMaxDuration,
		int rawScanFileConcurrency,
		long rawScanReadaheadBytes,
		int cdcQuantumMaxMutations,
		long cdcQuantumMaxBytes,
		Duration cdcQuantumMaxDuration,
		int latencyRangeMaxItems,
		long latencyRangeMaxBytes,
		int latencyFanOutMaxItems,
		long latencyFanOutMaxBytes) {

	public static final int MIN_PRODUCTION_DATA_THREADS = 3;
	public static final int DEFAULT_RAW_SCAN_FILE_CONCURRENCY = 4;
	public static final long DEFAULT_RAW_SCAN_READAHEAD_BYTES = 8L * 1024L * 1024L;
	public static final int MAX_RAW_SCAN_FILE_CONCURRENCY = 64;
	private static final int MAX_DRR_WEIGHT = 16;
	private static final int LATENCY_RANGE_HARD_MAX_ITEMS = RangeBudget.DEFAULT_MAX_ITEMS;
	private static final long LATENCY_RANGE_HARD_MAX_BYTES = RangeBudget.DEFAULT_MAX_BYTES;
	private static final int LATENCY_FAN_OUT_HARD_MAX_ITEMS = 256;
	private static final long LATENCY_FAN_OUT_HARD_MAX_BYTES = 2L * 1024L * 1024L;

	public WorkloadSettings {
		positive("database.parallelism.read", readParallelism);
		positive("database.parallelism.write", writeParallelism);
		positive("latency-queue-capacity", latencyQueueCapacity);
		positive("ingest-queue-capacity", ingestQueueCapacity);
		positive("cdc-queue-capacity", cdcQueueCapacity);
		positive("analytical-queue-capacity", analyticalQueueCapacity);
		positive("batch-queue-capacity", batchQueueCapacity);
		positive("control-queue-capacity", controlQueueCapacity);
		positive("physical-maintenance-queue-capacity", physicalMaintenanceQueueCapacity);
		positive("control-threads", controlThreads);
		positive("physical-concurrency", physicalConcurrency);
		positive("analytical-active-limit", analyticalActiveLimit);
		positive("retained-analytical-snapshots", retainedAnalyticalSnapshots);
		positive("latency-burst", latencyBurst);
		drrWeight("ingest-drr-weight", ingestDrrWeight);
		drrWeight("cdc-drr-weight", cdcDrrWeight);
		drrWeight("analytical-drr-weight", analyticalDrrWeight);
		drrWeight("batch-drr-weight", batchDrrWeight);
		positive("competing-batch-read-maximum-active", competingBatchReadMaximumActive);
		positive("competing-batch-write-maximum-active", competingBatchWriteMaximumActive);
		positiveDuration("competing-batch-write-interval", competingBatchWriteInterval, false);
		positive("pressured-batch-maximum-active", pressuredBatchMaximumActive);
		positive("range-quantum-max-items", rangeQuantumMaxItems);
		positive("range-quantum-max-bytes", rangeQuantumMaxBytes);
		positive("raw-scan-file-concurrency", rawScanFileConcurrency);
		positive("raw-scan-readahead-bytes", rawScanReadaheadBytes);
		positive("cdc-quantum-max-mutations", cdcQuantumMaxMutations);
		positive("cdc-quantum-max-bytes", cdcQuantumMaxBytes);
		positive("latency-range-max-items", latencyRangeMaxItems);
		positive("latency-range-max-bytes", latencyRangeMaxBytes);
		positive("latency-fan-out-max-items", latencyFanOutMaxItems);
		positive("latency-fan-out-max-bytes", latencyFanOutMaxBytes);
		positiveDuration("retained-snapshot-maximum-age", retainedSnapshotMaximumAge, true);
		positiveDuration("pressured-batch-interval", pressuredBatchInterval, false);
		positiveDuration("range-quantum-max-duration", rangeQuantumMaxDuration, false);
		positiveDuration("cdc-quantum-max-duration", cdcQuantumMaxDuration, false);
		if (analyticalActiveLimit > readParallelism) {
			throw invalid("analytical-active-limit must not exceed read-pool capacity");
		}
		if (competingBatchReadMaximumActive > readParallelism) {
			throw invalid("competing-batch-read-maximum-active must not exceed read-pool capacity");
		}
		if (competingBatchWriteMaximumActive > writeParallelism) {
			throw invalid("competing-batch-write-maximum-active must not exceed write-pool capacity");
		}
		if (rawScanFileConcurrency > MAX_RAW_SCAN_FILE_CONCURRENCY) {
			throw invalid("raw-scan-file-concurrency must not exceed " + MAX_RAW_SCAN_FILE_CONCURRENCY);
		}
		long combinedDataCapacity = (long) readParallelism + writeParallelism;
		if (pressuredBatchMaximumActive > combinedDataCapacity) {
			throw invalid("pressured-batch-maximum-active must not exceed combined data-pool capacity");
		}
		if (latencyRangeMaxItems > LATENCY_RANGE_HARD_MAX_ITEMS
				|| latencyRangeMaxBytes > LATENCY_RANGE_HARD_MAX_BYTES) {
			throw invalid("LATENCY range maximum exceeds the 4096-item/8-MiB contract ceiling");
		}
		if (latencyFanOutMaxItems > LATENCY_FAN_OUT_HARD_MAX_ITEMS
				|| latencyFanOutMaxBytes > LATENCY_FAN_OUT_HARD_MAX_BYTES) {
			throw invalid("LATENCY fan-out maximum exceeds the 256-item/2-MiB contract ceiling");
		}
		validateReservations("read", readParallelism,
				readLatencyReservation, readIngestReservation, readCdcReservation);
		validateReservations("write", writeParallelism,
				writeLatencyReservation, writeIngestReservation, writeCdcReservation);
		validateCounterBounds(readParallelism,
				writeParallelism,
				latencyQueueCapacity,
				ingestQueueCapacity,
				cdcQueueCapacity,
				analyticalQueueCapacity,
				batchQueueCapacity,
				controlQueueCapacity,
				physicalMaintenanceQueueCapacity,
				controlThreads,
				physicalConcurrency);
	}

	private static void validateCounterBounds(int readParallelism,
			int writeParallelism,
			int latencyQueueCapacity,
			int ingestQueueCapacity,
			int cdcQueueCapacity,
			int analyticalQueueCapacity,
			int batchQueueCapacity,
			int controlQueueCapacity,
			int physicalMaintenanceQueueCapacity,
			int controlThreads,
			int physicalConcurrency) {
		long readQueueCapacity = sum(latencyQueueCapacity,
				ingestQueueCapacity,
				cdcQueueCapacity,
				analyticalQueueCapacity,
				batchQueueCapacity);
		long writeQueueCapacity = sum(latencyQueueCapacity,
				ingestQueueCapacity,
				cdcQueueCapacity,
				batchQueueCapacity);
		maximumInt("read-pool aggregate queue capacity", readQueueCapacity);
		maximumInt("write-pool aggregate queue capacity", writeQueueCapacity);
		maximumInt("read-pool aggregate outstanding limit",
				readQueueCapacity + (long) readParallelism * 5L);
		maximumInt("write-pool aggregate outstanding limit",
				writeQueueCapacity + (long) writeParallelism * 4L);
		maximumInt("CONTROL outstanding limit", (long) controlQueueCapacity + controlThreads);
		maximumInt("PHYSICAL_MAINTENANCE outstanding limit",
				(long) physicalMaintenanceQueueCapacity + physicalConcurrency);
		maximumInt("scheduler aggregate queue capacity",
				readQueueCapacity + writeQueueCapacity
						+ controlQueueCapacity + (long) physicalMaintenanceQueueCapacity);
		maximumInt("scheduler aggregate worker capacity",
				(long) readParallelism + writeParallelism + controlThreads + physicalConcurrency);
		for (var entry : Map.of(
				"LATENCY", latencyQueueCapacity,
				"INGEST", ingestQueueCapacity,
				"CDC", cdcQueueCapacity,
				"BATCH", batchQueueCapacity).entrySet()) {
			maximumInt(entry.getKey() + " aggregate queue capacity", (long) entry.getValue() * 2L);
		}
	}

	public static WorkloadSettings resolve(DatabaseConfig config) throws GestaltException {
		Objects.requireNonNull(config, "config");
		var parallelism = Objects.requireNonNull(config.parallelism(), "database.parallelism");
		var workload = Objects.requireNonNull(parallelism.workload(), "database.parallelism.workload");
		int processors = Runtime.getRuntime().availableProcessors();
		var settings = from(workload,
				Objects.requireNonNullElse(parallelism.read(), processors),
				Objects.requireNonNullElse(parallelism.write(), processors));
		settings.validateProductionCapacities();
		return settings;
	}

	public void validateProductionCapacities() {
		if (readParallelism < MIN_PRODUCTION_DATA_THREADS || writeParallelism < MIN_PRODUCTION_DATA_THREADS) {
			throw invalid("read and write capacities must each be at least "
					+ MIN_PRODUCTION_DATA_THREADS + " for LATENCY, INGEST, and CDC reservations");
		}
	}

	public static WorkloadSettings defaults(int readParallelism, int writeParallelism) {
		return defaults(readParallelism, writeParallelism, 1, 4_096, 512, true);
	}

	public static WorkloadSettings testingDefaults(int readParallelism,
			int writeParallelism,
			int analyticalActiveLimit,
			int foregroundQueueCapacity,
			int batchQueueCapacity) {
		return defaults(readParallelism,
				writeParallelism,
				analyticalActiveLimit,
				foregroundQueueCapacity,
				batchQueueCapacity,
				false);
	}

	private static WorkloadSettings from(WorkloadConfig o,
			int readParallelism,
			int writeParallelism) throws GestaltException {
		return new WorkloadSettings(
				readParallelism,
				writeParallelism,
				require(o.latencyQueueCapacity(), "latency-queue-capacity"),
				require(o.ingestQueueCapacity(), "ingest-queue-capacity"),
				require(o.cdcQueueCapacity(), "cdc-queue-capacity"),
				require(o.analyticalQueueCapacity(), "analytical-queue-capacity"),
				require(o.batchQueueCapacity(), "batch-queue-capacity"),
				require(o.controlQueueCapacity(), "control-queue-capacity"),
				require(o.physicalMaintenanceQueueCapacity(), "physical-maintenance-queue-capacity"),
				require(o.readLatencyReservation(), "read-latency-reservation"),
				require(o.readIngestReservation(), "read-ingest-reservation"),
				require(o.readCdcReservation(), "read-cdc-reservation"),
				require(o.writeLatencyReservation(), "write-latency-reservation"),
				require(o.writeIngestReservation(), "write-ingest-reservation"),
				require(o.writeCdcReservation(), "write-cdc-reservation"),
				require(o.controlThreads(), "control-threads"),
				require(o.physicalConcurrency(), "physical-concurrency"),
				require(o.analyticalActiveLimit(), "analytical-active-limit"),
				require(o.retainedAnalyticalSnapshots(), "retained-analytical-snapshots"),
				require(o.retainedSnapshotMaximumAge(), "retained-snapshot-maximum-age"),
				require(o.latencyBurst(), "latency-burst"),
				require(o.ingestDrrWeight(), "ingest-drr-weight"),
				require(o.cdcDrrWeight(), "cdc-drr-weight"),
				require(o.analyticalDrrWeight(), "analytical-drr-weight"),
				require(o.batchDrrWeight(), "batch-drr-weight"),
				require(o.competingBatchReadMaximumActive(), "competing-batch-read-maximum-active"),
				require(o.competingBatchWriteMaximumActive(), "competing-batch-write-maximum-active"),
				require(o.competingBatchWriteInterval(), "competing-batch-write-interval"),
				require(o.pressuredBatchMaximumActive(), "pressured-batch-maximum-active"),
				require(o.pressuredBatchInterval(), "pressured-batch-interval"),
				require(o.rangeQuantumMaxItems(), "range-quantum-max-items"),
				require(o.rangeQuantumMaxBytes(), "range-quantum-max-bytes").longValue(),
				require(o.rangeQuantumMaxDuration(), "range-quantum-max-duration"),
				require(o.rawScanFileConcurrency(), "raw-scan-file-concurrency"),
				require(o.rawScanReadaheadBytes(), "raw-scan-readahead-bytes").longValue(),
				require(o.cdcQuantumMaxMutations(), "cdc-quantum-max-mutations"),
				require(o.cdcQuantumMaxBytes(), "cdc-quantum-max-bytes").longValue(),
				require(o.cdcQuantumMaxDuration(), "cdc-quantum-max-duration"),
				require(o.latencyRangeMaxItems(), "latency-range-max-items"),
				require(o.latencyRangeMaxBytes(), "latency-range-max-bytes").longValue(),
				require(o.latencyFanOutMaxItems(), "latency-fan-out-max-items"),
				require(o.latencyFanOutMaxBytes(), "latency-fan-out-max-bytes").longValue());
	}

	private static WorkloadSettings defaults(int readParallelism,
			int writeParallelism,
			int analyticalActiveLimit,
			int foregroundQueueCapacity,
			int batchQueueCapacity,
			boolean productionDefaults) {
		int readReservation = productionDefaults || readParallelism >= MIN_PRODUCTION_DATA_THREADS ? 1 : 0;
		int writeReservation = productionDefaults || writeParallelism >= MIN_PRODUCTION_DATA_THREADS ? 1 : 0;
		return new WorkloadSettings(readParallelism,
				writeParallelism,
				foregroundQueueCapacity,
				foregroundQueueCapacity,
				Math.max(64, Math.min(foregroundQueueCapacity, 1_024)),
				Math.max(1, Math.min(batchQueueCapacity, 512)),
				batchQueueCapacity,
				256,
				16,
				readReservation,
				readReservation,
				readReservation,
				writeReservation,
				writeReservation,
				writeReservation,
				2,
				1,
				analyticalActiveLimit,
				1,
				Duration.ofSeconds(60),
				8,
				4,
				4,
				2,
				1,
				Math.min(4, readParallelism),
				1,
				Duration.ofMillis(1),
				1,
				Duration.ofSeconds(1),
				4_096,
				8L * 1024L * 1024L,
				Duration.ofMillis(8),
				DEFAULT_RAW_SCAN_FILE_CONCURRENCY,
				DEFAULT_RAW_SCAN_READAHEAD_BYTES,
				4_096,
				8L * 1024L * 1024L,
				Duration.ofMillis(8),
				4_096,
				8L * 1024L * 1024L,
				256,
				2L * 1024L * 1024L);
	}

	public Map<WorkloadProfile, Integer> queueCapacities() {
		var result = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		result.put(WorkloadProfile.LATENCY, latencyQueueCapacity);
		result.put(WorkloadProfile.INGEST, ingestQueueCapacity);
		result.put(WorkloadProfile.CDC, cdcQueueCapacity);
		result.put(WorkloadProfile.ANALYTICAL, analyticalQueueCapacity);
		result.put(WorkloadProfile.BATCH, batchQueueCapacity);
		return Map.copyOf(result);
	}

	public Map<WorkloadProfile, Integer> readReservations() {
		return reservations(readLatencyReservation, readIngestReservation, readCdcReservation);
	}

	public Map<WorkloadProfile, Integer> writeReservations() {
		return reservations(writeLatencyReservation, writeIngestReservation, writeCdcReservation);
	}

	public Map<WorkloadProfile, Integer> drrWeights() {
		return Map.of(
				WorkloadProfile.INGEST, ingestDrrWeight,
				WorkloadProfile.CDC, cdcDrrWeight,
				WorkloadProfile.ANALYTICAL, analyticalDrrWeight,
				WorkloadProfile.BATCH, batchDrrWeight);
	}

	private static Map<WorkloadProfile, Integer> reservations(int latency, int ingest, int cdc) {
		return Map.of(
				WorkloadProfile.LATENCY, latency,
				WorkloadProfile.INGEST, ingest,
				WorkloadProfile.CDC, cdc);
	}

	private static void validateReservations(String pool,
			int capacity,
			int latency,
			int ingest,
			int cdc) {
		if (latency < 0 || ingest < 0 || cdc < 0) {
			throw invalid(pool + " reservations must not be negative");
		}
		long sum = (long) latency + ingest + cdc;
		if (sum > capacity) {
			throw invalid(pool + " reservation sum " + sum + " exceeds capacity " + capacity);
		}
	}

	private static long sum(int... values) {
		long result = 0L;
		for (int value : values) result += value;
		return result;
	}

	private static void maximumInt(String key, long value) {
		if (value > Integer.MAX_VALUE) {
			throw invalid(key + " exceeds the signed scheduler counter range");
		}
	}

	private static void positive(String key, long value) {
		if (value <= 0L) {
			throw invalid(key + " must be positive");
		}
	}

	private static void drrWeight(String key, int value) {
		positive(key, value);
		if (value > MAX_DRR_WEIGHT) {
			throw invalid(key + " must not exceed " + MAX_DRR_WEIGHT);
		}
	}

	private static void positiveDuration(String key, Duration value, boolean millisecondResolution) {
		Objects.requireNonNull(value, "database.parallelism.workload." + key);
		try {
			if (value.isZero() || value.isNegative() || value.toNanos() <= 0L
					|| millisecondResolution && value.toMillis() <= 0L) {
				throw invalid(key + " must be a positive duration");
			}
		} catch (ArithmeticException e) {
			throw invalid(key + " is too large");
		}
	}

	private static <T> T require(T value, String key) {
		return Objects.requireNonNull(value, "database.parallelism.workload." + key);
	}

	private static IllegalArgumentException invalid(String detail) {
		return new IllegalArgumentException("Invalid database.parallelism.workload configuration: " + detail);
	}
}
