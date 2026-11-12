package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.HistogramType;
import org.rocksdb.MutableDBOptions;
import org.rocksdb.RateLimiter;
import org.rocksdb.RocksDB;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.Status.Code;
import it.cavallium.rockserver.core.impl.EmbeddedDB.*;
import org.rocksdb.TickerType;

/**
 * Metrics, storage-pressure and compaction-I/O sampling of {@link EmbeddedDB}: timers, storage-pressure columns
 * and refresh, RocksDB property aggregation and compaction read accounting.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBStorageMetrics {

	private final EmbeddedDB edb;

	EmbeddedDBStorageMetrics(EmbeddedDB edb) {
		this.edb = edb;
	}

	Timer createActionTimer(Class<? extends RocksDBAPICommand> className) {
		return createTimer(Tags.of("action", className.getSimpleName()));
	}

	Timer createTimer(Tags tags) {
		var t = Timer
				.builder("rocksdb.operation.timer")
				.publishPercentiles(0.3, 0.5, 0.95)
				.publishPercentileHistogram()
				.tag("database", edb.name)
				.tags(tags)
				.register(edb.metrics.getRegistry());
		edb.meters.add(t);
		return t;
	}

	void registerStoragePressureSignalMetrics(MeterRegistry registry) {
		registerStoragePressureReasonGauge(registry,
				"write_stopped",
				StoragePressureSignal.REASON_WRITE_STOPPED);
		registerStoragePressureReasonGauge(registry,
				"delayed_write",
				StoragePressureSignal.REASON_DELAYED_WRITE);
		registerStoragePressureReasonGauge(registry,
				"pending_compaction",
				StoragePressureSignal.REASON_PENDING_COMPACTION);
		registerStoragePressureReasonGauge(registry,
				"signal_failure",
				StoragePressureSignal.REASON_SIGNAL_FAILURE);
		edb.meters.add(Gauge.builder("rockserver.workload.storage.pressure.actual.delayed.write.rate",
					edb.storagePressureSignal,
					signal -> unsignedLongToDouble(signal.actualDelayedWriteRate()))
				.description("RocksDB's current database-wide delayed-write rate")
				.baseUnit("bytes")
				.tag("database", edb.name)
				.register(registry));
		edb.meters.add(Gauge.builder("rockserver.workload.storage.pressure.pending.compaction.observed.max",
					edb.storagePressureSignal,
					signal -> unsignedLongToDouble(signal.maximumPendingCompactionBytes()))
				.description("Largest pending-compaction byte count observed before pressure short-circuited the poll")
				.baseUnit("bytes")
				.tag("database", edb.name)
				.register(registry));
		edb.meters.add(Gauge.builder("rockserver.workload.storage.pressure.pending.compaction.trigger.limit",
					edb.storagePressureSignal,
					signal -> unsignedLongToDouble(signal.triggeringPendingCompactionLimit()))
				.description("Effective per-column pending-compaction limit that triggered scheduler pressure")
				.baseUnit("bytes")
				.tag("database", edb.name)
				.register(registry));
	}

	private void registerStoragePressureReasonGauge(MeterRegistry registry, String reason, int reasonBit) {
		edb.meters.add(Gauge.builder("rockserver.workload.storage.pressure.reason",
					edb.storagePressureSignal,
					signal -> signal.hasReason(reasonBit) ? 1.0d : 0.0d)
				.description("Active RocksDB storage-pressure reasons")
				.tag("database", edb.name)
				.tag("reason", reason)
				.register(registry));
	}

	private static double unsignedLongToDouble(long value) {
		return value >= 0L ? value : (double) (value & Long.MAX_VALUE) + 0x1.0p63;
	}

	void registerSystemStoragePressureColumns() {
		ColumnFamilyHandle defaultColumn = edb.db.getStartupColumns()
				.entrySet()
				.stream()
				.filter(entry -> Arrays.equals(entry.getKey().getName(), RocksDB.DEFAULT_COLUMN_FAMILY))
				.map(Entry::getValue)
				.findFirst()
				.orElseThrow(() -> new IllegalStateException("RocksDB default column family was not opened"));
		upsertStoragePressureColumn(storagePressureColumn("default", defaultColumn));
		upsertStoragePressureColumn(storagePressureColumn(
				new String(EmbeddedDB.COLUMN_SCHEMAS_COLUMN, StandardCharsets.UTF_8),
				edb.columnSchemasColumnDescriptorHandle));
		upsertStoragePressureColumn(storagePressureColumn(
				new String(EmbeddedDB.MERGE_OPERATORS_COLUMN, StandardCharsets.UTF_8),
				edb.mergeOperatorsColumnDescriptorHandle));
	}

	StoragePressureColumn storagePressureColumn(String name, ColumnFamilyHandle handle) {
		ColumnFamilyOptions options = Objects.requireNonNull(edb.columnsConifg.get(name),
				() -> "Column config not found while registering storage-pressure signal: " + name);
		return new StoragePressureColumn(handle.getID(),
				handle,
				null,
				options.softPendingCompactionBytesLimit(), options.level0SlowdownWritesTrigger(),
				options.maxWriteBufferNumber(), options.minWriteBufferNumberToMerge());
	}

	void upsertStoragePressureColumn(StoragePressureColumn replacement) {
		synchronized (edb.columnEditLock) {
			StoragePressureColumn[] current = edb.storagePressureColumns;
			for (int i = 0; i < current.length; i++) {
				if (current[i].id() == replacement.id()) {
					StoragePressureColumn[] updated = current.clone();
					updated[i] = replacement;
					edb.storagePressureColumns = updated;
					return;
				}
			}
			StoragePressureColumn[] updated = Arrays.copyOf(current, current.length + 1);
			updated[current.length] = replacement;
			edb.storagePressureColumns = updated;
		}
	}

	void removeStoragePressureColumn(long columnId) {
		StoragePressureColumn[] current = edb.storagePressureColumns;
		for (int i = 0; i < current.length; i++) {
			if (current[i].id() != columnId) {
				continue;
			}
			if (current.length == 1) {
				edb.storagePressureColumns = EmbeddedDB.EMPTY_STORAGE_PRESSURE_COLUMNS;
			} else {
				StoragePressureColumn[] updated = new StoragePressureColumn[current.length - 1];
				System.arraycopy(current, 0, updated, 0, i);
				System.arraycopy(current, i + 1, updated, i, current.length - i - 1);
				edb.storagePressureColumns = updated;
			}
			return;
		}
	}

	/**
	 * Return long property based on the aggregation mode:
	 * - PER_CF: sum values across all column families
	 * - DB_WIDE: query once without a CF handle
	 * - SINGLE_CF: query once with any single CF handle (for shared-resource properties like block cache)
	 */
	BigInteger getLongProperty(String name, RocksDBLongProperty.AggregationMode aggregationMode) {
		edb.ops.beginOp();
		try {
			return switch (aggregationMode) {
				case PER_CF -> {
					var val = BigInteger.ZERO;
					for (Entry<Long, ColumnInstance> entry : edb.columns.entrySet()) {
						ColumnInstance ci = entry.getValue();
						if (!EmbeddedDB.tryBeginColumnUse(ci)) {
							continue;
						}
						try {
							long value = name.startsWith("rocksdb.num-files-at-level")
									? Long.parseUnsignedLong(edb.db.get().getProperty(ci.cfh(), name))
									: edb.db.get().getLongProperty(ci.cfh(), name);
							val = val.add(new BigInteger(Long.toUnsignedString(value)));
						} catch (org.rocksdb.RocksDBException e) {
							if (e.getStatus().getCode() == Code.NotFound) {
								// skip
							} else {
								throw new RuntimeException(e);
							}
						} finally {
							ci.endUse();
						}
					}
					yield val;
				}
				case DB_WIDE -> {
					try {
						yield new BigInteger(Long.toUnsignedString(edb.db.get().getLongProperty(name)));
					} catch (org.rocksdb.RocksDBException e) {
						if (e.getStatus().getCode() == Code.NotFound) {
							yield BigInteger.ZERO;
						} else {
							throw new RuntimeException(e);
						}
					}
				}
				case SINGLE_CF -> {
					// Shared-resource properties (e.g. block cache) return the same value
					// for every CF handle, so query with any single one.
					BigInteger value = BigInteger.ZERO;
					for (var entry : edb.columns.entrySet()) {
						var ci = entry.getValue();
						if (!EmbeddedDB.tryBeginColumnUse(ci)) {
							continue;
						}
						try {
							value = new BigInteger(Long.toUnsignedString(edb.db.get().getLongProperty(ci.cfh(), name)));
							break;
						} catch (org.rocksdb.RocksDBException e) {
							if (e.getStatus().getCode() != Code.NotFound) {
								throw new RuntimeException(e);
							}
						} finally {
							ci.endUse();
						}
					}
					yield value;
				}
			};
		} finally {
			edb.ops.endOp();
		}
	}

	long applyCompactionReadahead(long bytes) {
		edb.ops.beginOp();
		try {
			var nativeDb = edb.db.get();
			nativeDb.setDBOptions(MutableDBOptions.builder().setCompactionReadaheadSize(bytes).build());
			// Successful JNI acknowledgement; the public options getter cannot parse absolute path values.
			return bytes;
		} catch (org.rocksdb.RocksDBException failure) {
			throw new RuntimeException("Failed to apply compaction readahead", failure);
		} finally { edb.ops.endOp(); }
	}

	void recordCompactionForegroundRead(long started, int bulkKeys) {
		if (edb.compactionIoController == null) return;
		long elapsed = Math.max(0, System.nanoTime() - started);
		if (bulkKeys <= 0) {
			edb.pointReadCompletions.record(elapsed);
		}
		else {
			edb.bulkReadTailNanos.accumulateAndGet(elapsed, Math::max);
			edb.nativeBulkReadCompletions.incrementAndGet();
			edb.nativeBulkReadKeys.addAndGet(bulkKeys);
		}
	}

	// Each stripe publishes and drains a completed POINT cohort. The aggregate is a
	// vector cut: count, duration and tail agree, although stripe cut times differ.
	// Native calls and elapsed-time measurement stay outside the critical sections.
	static final class PointReadCompletionCounters {
		private final Stripe[] stripes = new Stripe[64];

		PointReadCompletionCounters() {
			for (int i = 0; i < stripes.length; i++) stripes[i] = new Stripe();
		}

		void record(long elapsed) {
			var stripe = stripes[(int) Thread.currentThread().threadId() & (stripes.length - 1)];
			synchronized (stripe) {
				stripe.nanos += elapsed;
				stripe.count++;
				stripe.tailNanos = Math.max(stripe.tailNanos, elapsed);
			}
		}

		synchronized Snapshot sample() {
			long count = 0, nanos = 0, tailNanos = 0;
			for (var stripe : stripes) {
				synchronized (stripe) {
					count += stripe.count;
					nanos += stripe.nanos;
					tailNanos = Math.max(tailNanos, stripe.tailNanos);
					stripe.tailNanos = 0;
				}
			}
			return new Snapshot(count, nanos, tailNanos);
		}

		private static final class Stripe {
			private long count, nanos, tailNanos;
		}

		record Snapshot(long count, long nanos, long tailNanos) {}
	}

	private record CompactionReadCompletion(long bytes, long micros, long count) {
		private static final CompactionReadCompletion UNAVAILABLE = new CompactionReadCompletion(-1, -1, -1);
	}

	private static CompactionReadCompletion sampleCompactionReadCompletion(Statistics statistics) {
		try {
			var completion = statistics.getCompactionReadCompletion();
			if (completion == null || completion.length != 3) return CompactionReadCompletion.UNAVAILABLE;
			long count = completion[0], bytes = completion[1], micros = completion[2];
			if (count < 0 || bytes < 0 || micros < 0 || count == 0 && (bytes != 0 || micros != 0))
				return CompactionReadCompletion.UNAVAILABLE;
			return new CompactionReadCompletion(bytes, micros, count);
		} catch (RuntimeException unavailable) {
			// Optional read-service telemetry must not fail pressure/foreground sampling.
		}
		return CompactionReadCompletion.UNAVAILABLE;
	}

	CompactionIoBudget.Sample sampleCompactionIo(RateLimiter limiter) {
		return sampleCompactionIo(limiter, new long[RWScheduler.POOL_TELEMETRY_LENGTH],
				new long[RWScheduler.POOL_TELEMETRY_LENGTH]);
	}

	CompactionIoBudget.Sample sampleCompactionIo(RateLimiter limiter, long[] readPoolTelemetry,
			long[] groupReadPoolTelemetry) {
		edb.ops.beginOp();
		boolean stopped = false;
		boolean pressure = false;
		boolean urgentPressure = false;
		// The write controller delays writes: with stopped, the two states in which the limiter must not limit.
		boolean writeDelayed = false;
		// Worst column's distance to the write slowdown triggers, 0 to 1: the limiter's floor rises with it.
		double compactionDebt = 0;
		try {
			var observer = edb.compactionIoSampleObserver;
			if (observer != null) observer.run();
			var nativeDb = edb.db.get();
			stopped = nativeDb.getLongProperty(EmbeddedDB.ROCKSDB_IS_WRITE_STOPPED_PROPERTY) != 0;
			writeDelayed = nativeDb.getLongProperty(EmbeddedDB.ROCKSDB_ACTUAL_DELAYED_WRITE_RATE_PROPERTY) != 0;
			urgentPressure = stopped || writeDelayed;
			pressure = urgentPressure;
			boolean recoveryClear = !pressure;
			// Unlike admission polling, inspect every column even after a pressure trigger.
			boolean complete = true;
			for (var column : edb.storagePressureColumns) {
				var registered = column.registeredColumn();
				if (registered != null && !EmbeddedDB.tryBeginColumnUse(registered)) { complete = false; continue; }
				try {
					long debt = nativeDb.getLongProperty(column.handle(), EmbeddedDB.ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES_PROPERTY);
					long limit = column.effectiveSoftPendingCompactionBytesLimit();
					pressure |= limit > 0 && limit < Long.MAX_VALUE && Long.compareUnsigned(debt, limit) >= 0;
					if (limit > 0 && limit < Long.MAX_VALUE) {
						compactionDebt = Math.max(compactionDebt, debt < 0 ? 1 : (double) debt / limit);
					}
					recoveryClear &= !(limit > 0 && limit < Long.MAX_VALUE && Long.compareUnsigned(debt, limit - limit / 5) >= 0);
					long levelZeroFiles = Long.parseUnsignedLong(nativeDb.getProperty(column.handle(), "rocksdb.num-files-at-level0"));
					int slowdownTrigger = column.effectiveLevel0SlowdownWritesTrigger();
					urgentPressure |= slowdownTrigger >= 0 && Long.compareUnsigned(levelZeroFiles, slowdownTrigger) >= 0;
					if (slowdownTrigger > 0) {
						compactionDebt = Math.max(compactionDebt, levelZeroFiles < 0 ? 1 : (double) levelZeroFiles / slowdownTrigger);
					}
					int maxWriteBuffers = column.effectiveMaxWriteBufferNumber();
					// Native delay cannot precede stop for small maxima or large merge batches.
					// NumNotFlushed includes in-flight flushes even when flush-pending is false.
					if (maxWriteBuffers <= 3 || column.effectiveMinWriteBufferNumberToMerge() > maxWriteBuffers - 2) {
						long immutableTables = nativeDb.getLongProperty(column.handle(),
								RocksDBLongProperty.NUM_IMMUTABLE_MEM_TABLE.getName());
						boolean flushHeadroomPressure = Long.compareUnsigned(immutableTables,
								Math.max(1L, (long) maxWriteBuffers - 1)) >= 0;
						urgentPressure |= flushHeadroomPressure;
						recoveryClear &= !flushHeadroomPressure;
					}
					pressure |= urgentPressure;
					recoveryClear &= slowdownTrigger < 0
							|| Long.compareUnsigned(levelZeroFiles, slowdownTrigger - slowdownTrigger / 5) < 0;
				} finally { if (registered != null) registered.endUse(); }
			}
			try (var statistics = edb.dbOptions.statistics()) {
				var histogram = statistics.getHistogramData(HistogramType.READ_BLOCK_GET_MICROS);
				var compactionReads = sampleCompactionReadCompletion(statistics);
				var prefetch = statistics.getHistogramData(HistogramType.COMPACTION_PREFETCH_BYTES);
				var bulk = statistics.getHistogramData(HistogramType.DB_MULTIGET);
				int readTopologyWorkers = edb.scheduler.copyCompactionReadTelemetryAndTopology(readPoolTelemetry, groupReadPoolTelemetry);
				boolean background = nativeDb.getLongProperty("rocksdb.num-running-compactions") > 0
						|| nativeDb.getLongProperty("rocksdb.num-running-flushes") > 0;
				boolean foregroundPending = edb.ops.getPendingOpsCount() > 1;
				for (var profile : WorkloadProfile.values()) {
					foregroundPending |= edb.scheduler.activeTasks(profile) + edb.scheduler.queuedTasks(profile) > 0;
				}
				var point = edb.pointReadCompletions.sample();
				return new CompactionIoBudget.Sample(System.nanoTime(), histogram.getCount(), histogram.getSum(),
						limiter.getTotalBytesThrough(), background, pressure, stopped, complete,
						foregroundPending,
						statistics.getTickerCount(TickerType.NUMBER_KEYS_READ)
								+ statistics.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN), recoveryClear, urgentPressure,
						compactionReads.bytes(), compactionReads.micros(), compactionReads.count(), prefetch.getCount(), prefetch.getSum(),
						bulk.getCount(), bulk.getSum(), statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_READ),
						(int) readPoolTelemetry[RWScheduler.POOL_TELEMETRY_WORKER_COUNT],
						(int) readPoolTelemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS],
						(int) readPoolTelemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_BY_PROFILE + WorkloadProfile.LATENCY.ordinal()],
						(int) readPoolTelemetry[RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.LATENCY.ordinal()],
						readPoolTelemetry[RWScheduler.POOL_TELEMETRY_COMPLETED_TASKS],
						edb.activeNativeMultiGets.get() > 0 || !edb.activeExistsMultiRequests.isEmpty(),
						(long) Math.ceil(point.tailNanos() / 1000d),
						(long) Math.ceil(edb.bulkReadTailNanos.getAndSet(0) / 1000d),
						point.count(), edb.nativeBulkReadCompletions.get(), edb.nativeBulkReadKeys.get(), point.nanos(),
						statistics.getTickerCount(TickerType.NUMBER_MULTIGET_KEYS_FOUND),
						statistics.getHistogramData(HistogramType.BYTES_PER_MULTIGET).getSum(),
						edb.workloadGroups.isEmpty() ? readPoolTelemetry[RWScheduler.POOL_TELEMETRY_FAILED_TASKS] : -1,
						readPoolTelemetry[RWScheduler.POOL_TELEMETRY_ACCEPTED_TASKS],
						readPoolTelemetry[RWScheduler.POOL_TELEMETRY_TERMINAL_OUTCOMES],
						readPoolTelemetry[RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES], readTopologyWorkers,
						Math.min(1, compactionDebt), writeDelayed);
			}
		} catch (org.rocksdb.RocksDBException failure) {
			if (pressure || urgentPressure) {
				return new CompactionIoBudget.Sample(System.nanoTime(), 0, 0, 0,
						false, pressure, stopped, false, false, 0, false, urgentPressure).withWriteDelayed(writeDelayed);
			}
			throw new RuntimeException(failure);
		} finally { edb.ops.endOp(); }
	}

	void refreshStoragePressure() {
		if (!edb.ops.isOpen()) {
			return;
		}
		boolean operationStarted = false;
		try {
			edb.ops.beginOp();
			operationStarted = true;
			StoragePressureColumn[] pressureColumns = edb.storagePressureColumns;
			var pollAdmittedObserver = edb.storagePressurePollAdmittedObserver;
			if (pollAdmittedObserver != null) {
				pollAdmittedObserver.run();
			}
			edb.storagePressureSignal.reset(0L, 0L);
			var rocksDb = edb.db.get();
			long writeStopped = rocksDb.getLongProperty(EmbeddedDB.ROCKSDB_IS_WRITE_STOPPED_PROPERTY);
			long actualDelayedWriteRate = rocksDb.getLongProperty(EmbeddedDB.ROCKSDB_ACTUAL_DELAYED_WRITE_RATE_PROPERTY);
			edb.storagePressureSignal.reset(writeStopped, actualDelayedWriteRate);
			for (var signal : edb.workloadGroupPressure.values()) signal.reset(writeStopped, actualDelayedWriteRate);
			boolean databasePressure = edb.storagePressureSignal.pressured();

			// The DB-wide signals already prove pressure. Avoid one JNI call per
			// column until RocksDB reports that writes are flowing normally again.
			if (!databasePressure) {
				for (StoragePressureColumn pressureColumn : pressureColumns) {
					ColumnInstance registeredColumn = pressureColumn.registeredColumn();
					if (registeredColumn != null && !EmbeddedDB.tryBeginColumnUse(registeredColumn)) {
						continue;
					}
					try {
						long pendingCompactionBytes = rocksDb.getLongProperty(
								pressureColumn.handle(),
								EmbeddedDB.ROCKSDB_ESTIMATE_PENDING_COMPACTION_BYTES_PROPERTY);
						var groupSignal = edb.workloadGroupPressure.get(edb.columnWorkloadNames.getOrDefault(pressureColumn.id(), "default"));
						if (groupSignal != null) groupSignal.observeColumn(pressureColumn.id(), pendingCompactionBytes,
								pressureColumn.effectiveSoftPendingCompactionBytesLimit());
						edb.storagePressureSignal.observeColumn(pressureColumn.id(),
								pendingCompactionBytes,
								pressureColumn.effectiveSoftPendingCompactionBytesLimit());
					} finally {
						if (registeredColumn != null) {
							registeredColumn.endUse();
						}
					}
					if (edb.workloadGroups.isEmpty() && edb.storagePressureSignal.pressured()) {
						break;
					}
				}
			}
			edb.scheduler.setStoragePressure(edb.workloadGroups.isEmpty() ? edb.storagePressureSignal.pressured()
					: edb.workloadGroupPressure.get("default").pressured());
			for (var group : edb.workloadGroups.entrySet()) {
				group.getValue().setStoragePressure(edb.workloadGroupPressure.get(group.getKey()).pressured());
			}
		} catch (VirtualMachineError fatal) {
			throw fatal;
		} catch (Throwable error) {
			if (!operationStarted && !edb.ops.isOpen()) {
				return;
			}
			handleStoragePressureRefreshFailure(error);
		} finally {
			if (operationStarted) {
				edb.ops.endOp();
			}
		}
	}

	static StoragePressureSignal newWorkloadGroupPressureSignal() {
		return EmbeddedDB.STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_OVERRIDE == 0L
				? new StoragePressureSignal() : new StoragePressureSignal(EmbeddedDB.STORAGE_PRESSURE_PENDING_COMPACTION_BYTES_OVERRIDE);
	}

	void handleStoragePressureRefreshFailure(Throwable error) {
		edb.storagePressureSignal.markSignalFailure();
		edb.scheduler.setStoragePressure(true);
		for (var group : edb.workloadGroups.values()) group.setStoragePressure(true);
		edb.storagePressureRefreshFailures.increment();
		edb.logger.debug("Unable to refresh workload storage-pressure state", error);
	}

	/**
	 * Return per-column-family long property values as a map from column name to value.
	 * Only meaningful for PER_CF properties.
	 */
	Map<String, Long> getPerCfLongProperty(String name) {
		edb.ops.beginOp();
		try {
			var result = new LinkedHashMap<String, Long>();
			for (Entry<Long, ColumnInstance> entry : edb.columns.entrySet()) {
				ColumnInstance ci = entry.getValue();
				if (!EmbeddedDB.tryBeginColumnUse(ci)) {
					continue;
				}
				try {
					String colName = new String(ci.cfh().getName());
					long value = name.startsWith("rocksdb.num-files-at-level")
							? Long.parseUnsignedLong(edb.db.get().getProperty(ci.cfh(), name))
							: edb.db.get().getLongProperty(ci.cfh(), name);
					result.merge(colName, value, Long::sum);
				} catch (org.rocksdb.RocksDBException e) {
					if (e.getStatus().getCode() != Code.NotFound) {
						throw new RuntimeException(e);
					}
				} finally {
					ci.endUse();
				}
			}
			return result;
		} finally {
			edb.ops.endOp();
		}
	}
}
