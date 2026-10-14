package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.ColumnTableProperties;
import it.cavallium.rockserver.core.common.api.ColumnTablePropertiesData;

/** Shared portable table properties; each route imposes its own authority and budgets. */
public final class ThriftTablePropertiesWire {
    private ThriftTablePropertiesWire() {}
    public static ColumnTablePropertiesData properties(ColumnTableProperties p) {
        return new ColumnTablePropertiesData()
                        .setTableCount(p.tableCount())
                        .setDataSize(p.dataSize())
                        .setIndexSize(p.indexSize())
                        .setIndexPartitions(p.indexPartitions())
                        .setTopLevelIndexSize(p.topLevelIndexSize())
                        .setFilterSize(p.filterSize())
                        .setRawKeySize(p.rawKeySize())
                        .setRawValueSize(p.rawValueSize())
                        .setNumDataBlocks(p.numDataBlocks())
                        .setNumEntries(p.numEntries())
                        .setNumDeletions(p.numDeletions())
                        .setNumMergeOperands(p.numMergeOperands())
                        .setNumRangeDeletions(p.numRangeDeletions())
                        .setSlowCompressionEstimatedDataSize(p.slowCompressionEstimatedDataSize())
                        .setFastCompressionEstimatedDataSize(p.fastCompressionEstimatedDataSize())
                        .setOldestCreationTime(p.oldestCreationTime())
                        .setNewestCreationTime(p.newestCreationTime())
                        .setOldestKeyTime(p.oldestKeyTime())
                        .setFormatVersions(p.formatVersions())
                        .setFixedKeyLengths(p.fixedKeyLengths())
                        .setIndexKeysAreUserKeys(p.indexKeysAreUserKeys())
                        .setIndexValuesAreDeltaEncoded(p.indexValuesAreDeltaEncoded())
                        .setColumnFamilyIds(p.columnFamilyIds())
                        .setFilterPolicies(p.filterPolicies())
                        .setComparators(p.comparators())
                        .setMergeOperators(p.mergeOperators())
                        .setPrefixExtractors(p.prefixExtractors())
                        .setPropertyCollectors(p.propertyCollectors())
                        .setCompressions(p.compressions());
    }
    public static ColumnTableProperties properties(ColumnTablePropertiesData p) {
        return new ColumnTableProperties(
                    p.getTableCount(),
                    p.getDataSize(),
                    p.getIndexSize(),
                    p.getIndexPartitions(),
                    p.getTopLevelIndexSize(),
                    p.getFilterSize(),
                    p.getRawKeySize(),
                    p.getRawValueSize(),
                    p.getNumDataBlocks(),
                    p.getNumEntries(),
                    p.getNumDeletions(),
                    p.getNumMergeOperands(),
                    p.getNumRangeDeletions(),
                    p.getSlowCompressionEstimatedDataSize(),
                    p.getFastCompressionEstimatedDataSize(),
                    p.getOldestCreationTime(),
                    p.getNewestCreationTime(),
                    p.getOldestKeyTime(),
                    p.getFormatVersions(),
                    p.getFixedKeyLengths(),
                    p.getIndexKeysAreUserKeys(),
                    p.getIndexValuesAreDeltaEncoded(),
                    p.getColumnFamilyIds(),
                    p.getFilterPolicies(),
                    p.getComparators(),
                    p.getMergeOperators(),
                    p.getPrefixExtractors(),
                    p.getPropertyCollectors(),
                    p.getCompressions());
    }
}
