package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.SstMaintenance;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Compact;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.Flush;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.config.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.github.gestalt.config.exceptions.GestaltException;
import org.rocksdb.TableProperties;
import org.rocksdb.AbstractImmutableNativeReference;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactRangeOptions;
import org.rocksdb.CompactRangeOptions.BottommostLevelCompaction;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.Status.Code;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Flush, compaction and SST maintenance of {@link EmbeddedDB}.
 *
 * <p>Moved out of {@link EmbeddedDB} unchanged; {@code edb} is the owning database, whose state
 * (fields, locks, executors) stays in {@link EmbeddedDB}.
 */
final class EmbeddedDBMaintenance {

	final EmbeddedDB edb;

	EmbeddedDBMaintenance(EmbeddedDB edb) {
		this.edb = edb;
	}

	public void flush() {
		flush(false);
	}
	public void flushWal() {
		flush(true);
	}
	public void requestFlush() {
		edb.ops.beginOp();
		try {
			synchronized (edb.columnEditLock) {
				try (var fo = new FlushOptions().setWaitForFlush(false).setAllowWriteStall(true)) {
					edb.db.get().flush(fo, edb.columns.values().stream()
							.map(ColumnInstance::cfh)
							.filter(AbstractImmutableNativeReference::isOwningHandle)
							.toList());
				}
			}
		} catch (org.rocksdb.RocksDBException ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			edb.ops.endOp();
		}
	}

	void flush(boolean walOnly) {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction(walOnly ? "FlushWal" : "Flush", start, null, null, null, null, null, null, null);
			if (walOnly) {
				edb.db.get().flushWal(true);
				return;
			}
			synchronized (edb.columnEditLock) {
				var observer = edb.columnMaintenanceObserver;
				if (observer != null) {
					observer.run();
				}
				edb.db.get().flushWal(true);
				try (var fo = new FlushOptions().setWaitForFlush(true).setAllowWriteStall(true)) {
					edb.db
							.get()
							.flush(fo,
									edb.columns
											.values()
											.stream()
											.map(ColumnInstance::cfh)
											.filter(AbstractImmutableNativeReference::isOwningHandle)
											.toList()
							);
				}
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.flushTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}

    private static ColumnTableProperties aggregateTableProperties(Collection<TableProperties> tables) {
        long tableCount = 0;
        long dataSize = 0;
        long indexSize = 0;
        long indexPartitions = 0;
        long topLevelIndexSize = 0;
        long filterSize = 0;
        long rawKeySize = 0;
        long rawValueSize = 0;
        long numDataBlocks = 0;
        long numEntries = 0;
        long numDeletions = 0;
        long numMergeOperands = 0;
        long numRangeDeletions = 0;
        long slowCompressionEstimatedDataSize = 0;
        long fastCompressionEstimatedDataSize = 0;
        long oldestCreationTime = 0;
        long newestCreationTime = 0;
        long oldestKeyTime = 0;
        Map<Long, Long> formatVersions = new HashMap<>();
        Map<Long, Long> fixedKeyLengths = new HashMap<>();
        Map<Long, Long> indexKeysAreUserKeys = new HashMap<>();
        Map<Long, Long> indexValuesAreDeltaEncoded = new HashMap<>();
        Map<Long, Long> columnFamilyIds = new HashMap<>();
        Map<String, Long> filterPolicies = new HashMap<>();
        Map<String, Long> comparators = new HashMap<>();
        Map<String, Long> mergeOperators = new HashMap<>();
        Map<String, Long> prefixExtractors = new HashMap<>();
        Map<String, Long> propertyCollectors = new HashMap<>();
        Map<String, Long> compressions = new HashMap<>();
        for (var p : tables) {
            tableCount = Math.addExact(tableCount, 1);
            dataSize = Math.addExact(dataSize, checkedTableProperty(p.getDataSize()));
            indexSize = Math.addExact(indexSize, checkedTableProperty(p.getIndexSize()));
            indexPartitions = Math.addExact(indexPartitions, checkedTableProperty(p.getIndexPartitions()));
            topLevelIndexSize = Math.addExact(topLevelIndexSize, checkedTableProperty(p.getTopLevelIndexSize()));
            filterSize = Math.addExact(filterSize, checkedTableProperty(p.getFilterSize()));
            rawKeySize = Math.addExact(rawKeySize, checkedTableProperty(p.getRawKeySize()));
            rawValueSize = Math.addExact(rawValueSize, checkedTableProperty(p.getRawValueSize()));
            numDataBlocks = Math.addExact(numDataBlocks, checkedTableProperty(p.getNumDataBlocks()));
            numEntries = Math.addExact(numEntries, checkedTableProperty(p.getNumEntries()));
            numDeletions = Math.addExact(numDeletions, checkedTableProperty(p.getNumDeletions()));
            numMergeOperands = Math.addExact(numMergeOperands, checkedTableProperty(p.getNumMergeOperands()));
            numRangeDeletions = Math.addExact(numRangeDeletions, checkedTableProperty(p.getNumRangeDeletions()));
            slowCompressionEstimatedDataSize = Math.addExact(slowCompressionEstimatedDataSize, checkedTableProperty(p.getSlowCompressionEstimatedDataSize()));
            fastCompressionEstimatedDataSize = Math.addExact(fastCompressionEstimatedDataSize, checkedTableProperty(p.getFastCompressionEstimatedDataSize()));
            oldestCreationTime = minimumKnownTableTime(oldestCreationTime, checkedTableProperty(p.getCreationTime()));
            newestCreationTime = Math.max(newestCreationTime, checkedTableProperty(p.getCreationTime()));
            oldestKeyTime = minimumKnownTableTime(oldestKeyTime, checkedTableProperty(p.getOldestKeyTime()));
            formatVersions.merge(checkedTableProperty(p.getFormatVersion()), 1L, Math::addExact);
            fixedKeyLengths.merge(checkedTableProperty(p.getFixedKeyLen()), 1L, Math::addExact);
            indexKeysAreUserKeys.merge(checkedTableProperty(p.getIndexKeyIsUserKey()), 1L, Math::addExact);
            indexValuesAreDeltaEncoded.merge(checkedTableProperty(p.getIndexValueIsDeltaEncoded()), 1L, Math::addExact);
            columnFamilyIds.merge(checkedTableProperty(p.getColumnFamilyId()), 1L, Math::addExact);
            filterPolicies.merge(p.getFilterPolicyName() == null ? "" : p.getFilterPolicyName(), 1L, Math::addExact);
            comparators.merge(p.getComparatorName() == null ? "" : p.getComparatorName(), 1L, Math::addExact);
            mergeOperators.merge(p.getMergeOperatorName() == null ? "" : p.getMergeOperatorName(), 1L, Math::addExact);
            prefixExtractors.merge(p.getPrefixExtractorName() == null ? "" : p.getPrefixExtractorName(), 1L, Math::addExact);
            propertyCollectors.merge(p.getPropertyCollectorsNames() == null ? "" : p.getPropertyCollectorsNames(), 1L, Math::addExact);
            compressions.merge(p.getCompressionName() == null ? "" : p.getCompressionName(), 1L, Math::addExact);
        }
        return new ColumnTableProperties(
                tableCount, dataSize, indexSize, indexPartitions,
                topLevelIndexSize, filterSize, rawKeySize, rawValueSize,
                numDataBlocks, numEntries, numDeletions, numMergeOperands,
                numRangeDeletions, slowCompressionEstimatedDataSize, fastCompressionEstimatedDataSize, oldestCreationTime,
                newestCreationTime, oldestKeyTime, formatVersions, fixedKeyLengths,
                indexKeysAreUserKeys, indexValuesAreDeltaEncoded, columnFamilyIds, filterPolicies,
                comparators, mergeOperators, prefixExtractors, propertyCollectors,
                compressions);
    }

    private static long checkedTableProperty(long value) {
        if (value < 0) throw new ArithmeticException("Unsigned table property exceeds signed long range");
        return value;
    }

    private static long minimumKnownTableTime(long a, long b) {
        return a == 0 ? b : b == 0 ? a : Math.min(a, b);
    }

    Mono<Map<String, ColumnTableProperties>> collectTablePropertiesMetrics() {
        // Schedule each column separately, so batch admission/pressure is rechecked between columns.
        return Flux.fromIterable(List.copyOf(edb.columns.entrySet()))
                .concatMap(entry -> edb.scheduleTracked(edb.getSchedulerForColumn(entry.getKey()).scheduler(WorkloadProfile.BATCH,
                        OperationFamily.FULL_SCAN_AGGREGATE, Long.MAX_VALUE), () -> {
                    var column = entry.getValue();
                    if (!EmbeddedDB.tryBeginColumnUse(column)) return null; // An empty Mono skips retired columns.
                    try {
                        var properties = aggregateTableProperties(
                                edb.db.get().getPropertiesOfAllTables(column.cfh()).values());
                        return Map.entry(new String(column.cfh().getName(), StandardCharsets.UTF_8), properties);
                    } finally { column.endUse(); }
                }), 1)
                .collectMap(Map.Entry::getKey, Map.Entry::getValue);
    }
    public ColumnTableProperties getTableProperties(long columnId) {
        edb.ops.beginOp();
        try (var use = edb.acquireColumnUse(columnId)) {
            // RocksDB owns a version reference during this call; the column lease prevents drop/close.
            return aggregateTableProperties(edb.db.get().getPropertiesOfAllTables(use.column().cfh()).values());
        } catch (org.rocksdb.RocksDBException | ArithmeticException e) {
            throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
        } finally {
            edb.ops.endOp();
        }
    }
    public SstMaintenance.Metadata getSstMetadata(long columnId, int level) {
        edb.ops.beginOp();
        try (var use = edb.acquireColumnUse(columnId)) {
            return selectiveMetadata(use.column(), columnId, level);
        } catch (org.rocksdb.RocksDBException e) {
            throw RocksDBException.of(RocksDBErrorType.GET_PROPERTY_ERROR, e);
        } finally { edb.ops.endOp(); }
    }

    private SstMaintenance.Metadata selectiveMetadata(ColumnInstance column, long columnId, int level)
            throws org.rocksdb.RocksDBException {
        var name = new String(column.cfh().getName(), StandardCharsets.UTF_8);
        // Resolve the same immutable volume configuration used when this CF was opened.
        // Do not take columnEditLock after acquiring a lease: deletion holds it while draining.
        try {
            var global = edb.config.global();
            FallbackColumnConfig columnConfig = global.fallbackColumnOptions();
            for (var named : global.columnOptions()) {
                if (named.name().equals(name)) { columnConfig = named; break; }
            }
            var paths = edb.path == null ? List.<String>of()
                    : RocksDBLoader.getVolumeConfigs(edb.definitiveDbPath, columnConfig).stream()
                            .map(p -> p.path().toAbsolutePath().normalize().toString()).toList();
            return SelectiveCompaction.metadata(edb.db.get(), column.cfh(), paths,
                    edb.sstMaintenanceSession, columnId, level);
        } catch (GestaltException e) {
            throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, e);
        }
    }
    public SstMaintenance.Result compactFiles(SstMaintenance.Request request) {
        Objects.requireNonNull(request, "request");
        long start = System.nanoTime();
        edb.ops.beginOp();
        try (var use = edb.acquireColumnUse(request.columnId())) {
            var observer = edb.columnMaintenanceObserver;
            if (observer != null) observer.run();
            return SelectiveCompaction.compact(edb.db.get(), use.column().cfh(),
                    selectiveMetadata(use.column(), request.columnId(), request.level()), request);
        } catch (org.rocksdb.RocksDBException e) {
            var status = e.getStatus();
            var type = status != null && (status.getCode() == org.rocksdb.Status.Code.Aborted
                    || status.getCode() == org.rocksdb.Status.Code.InvalidArgument)
                    ? RocksDBErrorType.COMPACTION_CONFLICT : RocksDBErrorType.COMPACTION_FAILED;
            throw RocksDBException.of(type, e);
        } finally {
            edb.ops.endOp();
            edb.compactTimer.record(System.nanoTime()-start, TimeUnit.NANOSECONDS);
        }
    }
	public void compact() {
		var start = System.nanoTime();
		edb.ops.beginOp();
		try {
			edb.actionLogger.logAction("Compact", start, null, null, null, null, null, null, null);
			synchronized (edb.columnEditLock) {
				var observer = edb.columnMaintenanceObserver;
				if (observer != null) {
					observer.run();
				}
				var db = edb.db.get();
				for (ColumnInstance value : edb.columns.values()) {
					if (value.cfh().isOwningHandle()) {
						String columnName = new String(value.cfh().getName(), StandardCharsets.UTF_8);
						ColumnFamilyOptions columnOptions = edb.columnsConifg.get(columnName);
						try (var cro = new CompactRangeOptions()
								.setAllowWriteStall(false)
								.setExclusiveManualCompaction(true)
								.setChangeLevel(false)
								.setTargetPathId(EmbeddedDB.bottommostCompactionTargetPathId(columnOptions))
								.setMaxSubcompactions(16)
								.setBottommostLevelCompaction(BottommostLevelCompaction.kForceOptimized)) {
							db.compactRange(value.cfh(), null, null, cro);
						}
					}
				}
			}
		} catch (RocksDBException ex) {
			throw ex;
		} catch (Exception ex) {
			throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex);
		} finally {
			edb.ops.endOp();
			var end = System.nanoTime();
			edb.compactTimer.record(end - start, TimeUnit.NANOSECONDS);
		}
	}
}
