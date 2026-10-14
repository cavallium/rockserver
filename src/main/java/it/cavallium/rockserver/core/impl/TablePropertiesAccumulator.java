package it.cavallium.rockserver.core.impl;

import it.cavallium.rockserver.core.common.ColumnTableProperties;
import java.util.HashMap;
import java.util.Map;
import org.rocksdb.TableProperties;

/**
 * Aggregates one SST at a time, including rows supplied by a bounded native decoder.
 * Not thread-safe. Discard the accumulator if adding a row fails.
 */
public final class TablePropertiesAccumulator {
    private long tableCount = 0;
    private long dataSize = 0;
    private long indexSize = 0;
    private long indexPartitions = 0;
    private long topLevelIndexSize = 0;
    private long filterSize = 0;
    private long rawKeySize = 0;
    private long rawValueSize = 0;
    private long numDataBlocks = 0;
    private long numEntries = 0;
    private long numDeletions = 0;
    private long numMergeOperands = 0;
    private long numRangeDeletions = 0;
    private long slowCompressionEstimatedDataSize = 0;
    private long fastCompressionEstimatedDataSize = 0;
    private long oldestCreationTime = 0;
    private long newestCreationTime = 0;
    private long oldestKeyTime = 0;
    private final Map<Long, Long> formatVersions = new HashMap<>();
    private final Map<Long, Long> fixedKeyLengths = new HashMap<>();
    private final Map<Long, Long> indexKeysAreUserKeys = new HashMap<>();
    private final Map<Long, Long> indexValuesAreDeltaEncoded = new HashMap<>();
    private final Map<Long, Long> columnFamilyIds = new HashMap<>();
    private final Map<String, Long> filterPolicies = new HashMap<>();
    private final Map<String, Long> comparators = new HashMap<>();
    private final Map<String, Long> mergeOperators = new HashMap<>();
    private final Map<String, Long> prefixExtractors = new HashMap<>();
    private final Map<String, Long> propertyCollectors = new HashMap<>();
    private final Map<String, Long> compressions = new HashMap<>();

    public void add(TableProperties p) {
        add(
            p.getDataSize(), p.getIndexSize(), p.getIndexPartitions(),
            p.getTopLevelIndexSize(), p.getFilterSize(), p.getRawKeySize(),
            p.getRawValueSize(), p.getNumDataBlocks(), p.getNumEntries(),
            p.getNumDeletions(), p.getNumMergeOperands(), p.getNumRangeDeletions(),
            p.getSlowCompressionEstimatedDataSize(), p.getFastCompressionEstimatedDataSize(), p.getCreationTime(),
            p.getOldestKeyTime(), p.getFormatVersion(), p.getFixedKeyLen(),
            p.getIndexKeyIsUserKey(), p.getIndexValueIsDeltaEncoded(), p.getColumnFamilyId(),
            p.getFilterPolicyName(), p.getComparatorName(), p.getMergeOperatorName(),
            p.getPrefixExtractorName(), p.getPropertyCollectorsNames(), p.getCompressionName());
    }

    /** Adds the unmodified native values; unsigned values outside signed long range fail. */
    public void add(
            long valueDataSize, long valueIndexSize, long valueIndexPartitions,
            long valueTopLevelIndexSize, long valueFilterSize, long valueRawKeySize,
            long valueRawValueSize, long valueNumDataBlocks, long valueNumEntries,
            long valueNumDeletions, long valueNumMergeOperands, long valueNumRangeDeletions,
            long valueSlowCompressionEstimatedDataSize, long valueFastCompressionEstimatedDataSize, long valueCreationTime,
            long valueOldestKeyTime, long valueFormatVersion, long valueFixedKeyLen,
            long valueIndexKeyIsUserKey, long valueIndexValueIsDeltaEncoded, long valueColumnFamilyId,
            String valueFilterPolicyName, String valueComparatorName, String valueMergeOperatorName,
            String valuePrefixExtractorName, String valuePropertyCollectorsNames, String valueCompressionName) {
        tableCount = Math.addExact(tableCount, 1);
        dataSize = Math.addExact(dataSize, checkedTableProperty(valueDataSize));
        indexSize = Math.addExact(indexSize, checkedTableProperty(valueIndexSize));
        indexPartitions = Math.addExact(indexPartitions, checkedTableProperty(valueIndexPartitions));
        topLevelIndexSize = Math.addExact(topLevelIndexSize, checkedTableProperty(valueTopLevelIndexSize));
        filterSize = Math.addExact(filterSize, checkedTableProperty(valueFilterSize));
        rawKeySize = Math.addExact(rawKeySize, checkedTableProperty(valueRawKeySize));
        rawValueSize = Math.addExact(rawValueSize, checkedTableProperty(valueRawValueSize));
        numDataBlocks = Math.addExact(numDataBlocks, checkedTableProperty(valueNumDataBlocks));
        numEntries = Math.addExact(numEntries, checkedTableProperty(valueNumEntries));
        numDeletions = Math.addExact(numDeletions, checkedTableProperty(valueNumDeletions));
        numMergeOperands = Math.addExact(numMergeOperands, checkedTableProperty(valueNumMergeOperands));
        numRangeDeletions = Math.addExact(numRangeDeletions, checkedTableProperty(valueNumRangeDeletions));
        slowCompressionEstimatedDataSize = Math.addExact(slowCompressionEstimatedDataSize, checkedTableProperty(valueSlowCompressionEstimatedDataSize));
        fastCompressionEstimatedDataSize = Math.addExact(fastCompressionEstimatedDataSize, checkedTableProperty(valueFastCompressionEstimatedDataSize));
        oldestCreationTime = minimumKnownTableTime(oldestCreationTime, checkedTableProperty(valueCreationTime));
        newestCreationTime = Math.max(newestCreationTime, checkedTableProperty(valueCreationTime));
        oldestKeyTime = minimumKnownTableTime(oldestKeyTime, checkedTableProperty(valueOldestKeyTime));
        formatVersions.merge(checkedTableProperty(valueFormatVersion), 1L, Math::addExact);
        fixedKeyLengths.merge(checkedTableProperty(valueFixedKeyLen), 1L, Math::addExact);
        indexKeysAreUserKeys.merge(checkedTableProperty(valueIndexKeyIsUserKey), 1L, Math::addExact);
        indexValuesAreDeltaEncoded.merge(checkedTableProperty(valueIndexValueIsDeltaEncoded), 1L, Math::addExact);
        columnFamilyIds.merge(checkedTableProperty(valueColumnFamilyId), 1L, Math::addExact);
        filterPolicies.merge(valueFilterPolicyName == null ? "" : valueFilterPolicyName, 1L, Math::addExact);
        comparators.merge(valueComparatorName == null ? "" : valueComparatorName, 1L, Math::addExact);
        mergeOperators.merge(valueMergeOperatorName == null ? "" : valueMergeOperatorName, 1L, Math::addExact);
        prefixExtractors.merge(valuePrefixExtractorName == null ? "" : valuePrefixExtractorName, 1L, Math::addExact);
        propertyCollectors.merge(valuePropertyCollectorsNames == null ? "" : valuePropertyCollectorsNames, 1L, Math::addExact);
        compressions.merge(valueCompressionName == null ? "" : valueCompressionName, 1L, Math::addExact);
    }

    public ColumnTableProperties result() {
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
}
