package it.cavallium.rockserver.core.common;

import java.util.Map;

/**
 * Aggregate of the current SST table properties of one column family, without flushing.
 * Memtables, WALs and blob files are excluded. Entries include obsolete versions and tombstones;
 * they are physical RocksDB entries, not live logical rows (nor bucket contents).
 * Byte sizes retain RocksDB semantics: raw sizes are uncompressed; index/filter sizes are
 * table properties, not cache usage. Top-level index size is not an additional data-size total.
 * Times are Unix seconds; zero means unknown. Minima ignore unknown zero timestamps.
 * Creation-time bounds retain RocksDB's creation_time (oldest ancestor time), not filesystem
 * creation times. Compression estimates sum available samples only; zero means unknown and
 * positive totals may cover only a subset of the files.
 * Distribution maps count SST files with each value, rather than adding unlike values.
 * Empty columns have zero totals and empty distributions. All maps are immutable.
 * Numeric values use signed longs; unsigned native values or sums that overflow fail explicitly.
 * This is an observation of one native file-set version, not a pinned or transactional snapshot.
 * Custom collector byte strings are intentionally excluded from this portable numeric contract.
 */
public record ColumnTableProperties(
        long tableCount,
        long dataSize,
        long indexSize,
        long indexPartitions,
        long topLevelIndexSize,
        long filterSize,
        long rawKeySize,
        long rawValueSize,
        long numDataBlocks,
        long numEntries,
        long numDeletions,
        long numMergeOperands,
        long numRangeDeletions,
        long slowCompressionEstimatedDataSize,
        long fastCompressionEstimatedDataSize,
        long oldestCreationTime,
        long newestCreationTime,
        long oldestKeyTime,
        Map<Long, Long> formatVersions,
        Map<Long, Long> fixedKeyLengths,
        Map<Long, Long> indexKeysAreUserKeys,
        Map<Long, Long> indexValuesAreDeltaEncoded,
        Map<Long, Long> columnFamilyIds,
        Map<String, Long> filterPolicies,
        Map<String, Long> comparators,
        Map<String, Long> mergeOperators,
        Map<String, Long> prefixExtractors,
        Map<String, Long> propertyCollectors,
        Map<String, Long> compressions) {
    public ColumnTableProperties {
        formatVersions = Map.copyOf(formatVersions);
        fixedKeyLengths = Map.copyOf(fixedKeyLengths);
        indexKeysAreUserKeys = Map.copyOf(indexKeysAreUserKeys);
        indexValuesAreDeltaEncoded = Map.copyOf(indexValuesAreDeltaEncoded);
        columnFamilyIds = Map.copyOf(columnFamilyIds);
        filterPolicies = Map.copyOf(filterPolicies);
        comparators = Map.copyOf(comparators);
        mergeOperators = Map.copyOf(mergeOperators);
        prefixExtractors = Map.copyOf(prefixExtractors);
        propertyCollectors = Map.copyOf(propertyCollectors);
        compressions = Map.copyOf(compressions);
    }
}
