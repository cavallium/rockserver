package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.ColumnTableProperties;
import it.cavallium.rockserver.core.common.SstMaintenance;
import it.cavallium.rockserver.core.common.api.proto.*;

/** Shared portable property and SST representation; callers impose their route's budgets and authority. */
public final class GrpcPortableWire {
    private GrpcPortableWire() {}
    public static ColumnTablePropertiesResponse properties(ColumnTableProperties p) {
        return ColumnTablePropertiesResponse.newBuilder()
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
                .putAllFormatVersions(p.formatVersions())
                .putAllFixedKeyLengths(p.fixedKeyLengths())
                .putAllIndexKeysAreUserKeys(p.indexKeysAreUserKeys())
                .putAllIndexValuesAreDeltaEncoded(p.indexValuesAreDeltaEncoded())
                .putAllColumnFamilyIds(p.columnFamilyIds())
                .putAllFilterPolicies(p.filterPolicies())
                .putAllComparators(p.comparators())
                .putAllMergeOperators(p.mergeOperators())
                .putAllPrefixExtractors(p.prefixExtractors())
                .putAllPropertyCollectors(p.propertyCollectors())
                .putAllCompressions(p.compressions())
                .build();
    }
    public static ColumnTableProperties properties(ColumnTablePropertiesResponse p) {
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
                p.getFormatVersionsMap(),
                p.getFixedKeyLengthsMap(),
                p.getIndexKeysAreUserKeysMap(),
                p.getIndexValuesAreDeltaEncodedMap(),
                p.getColumnFamilyIdsMap(),
                p.getFilterPoliciesMap(),
                p.getComparatorsMap(),
                p.getMergeOperatorsMap(),
                p.getPrefixExtractorsMap(),
                p.getPropertyCollectorsMap(),
                p.getCompressionsMap());
    }
    public static GetSstMetadataResponse metadata(SstMaintenance.Metadata m) {
        var b=GetSstMetadataResponse.newBuilder().setSession(m.session()).setColumnId(m.columnId()).setColumnName(m.columnName()).setNumLevels(m.numLevels()).setBaseLevel(m.baseLevel()).addAllPaths(m.paths());
        for(var f:m.files())b.addFiles(SstFileMetadata.newBuilder().setName(f.name()).setLevel(f.level()).setPathId(f.pathId()).setSizeBytes(f.sizeBytes()).setSmallestKeyHex(f.smallestKeyHex()).setLargestKeyHex(f.largestKeyHex()).setBeingCompacted(f.beingCompacted()));return b.build();
    }
    public static SstMaintenance.Metadata metadata(GetSstMetadataResponse m) { return new SstMaintenance.Metadata(m.getSession(),m.getColumnId(),m.getColumnName(),m.getNumLevels(),m.getBaseLevel(),m.getPathsList(),m.getFilesList().stream().map(f->new SstMaintenance.File(f.getName(),f.getLevel(),f.getPathId(),f.getSizeBytes(),f.getSmallestKeyHex(),f.getLargestKeyHex(),f.getBeingCompacted())).toList()); }
    public static CompactFilesRequest request(SstMaintenance.Request r) {return CompactFilesRequest.newBuilder().setColumnId(r.columnId()).setSession(r.session()).addAllFiles(r.files()).setLevel(r.level()).setOutputPathId(r.outputPathId()).setOutputFileSizeLimit(r.outputFileSizeLimit()).setMaxInputBytes(r.maxInputBytes()).setMaxSubcompactions(r.maxSubcompactions()).setExecute(r.execute()).build();}
    public static SstMaintenance.Request request(CompactFilesRequest r) {return new SstMaintenance.Request(r.getColumnId(),r.getSession(),r.getFilesList(),r.getLevel(),r.getOutputPathId(),r.getOutputFileSizeLimit(),r.getMaxInputBytes(),r.getMaxSubcompactions(),r.getExecute());}
    public static CompactFilesResponse result(SstMaintenance.Result r) {return CompactFilesResponse.newBuilder().setExecuted(r.executed()).addAllInputFiles(r.inputFiles()).addAllOutputFiles(r.outputFiles()).setValidatedInputBytes(r.validatedInputBytes()).setOutputBytes(r.outputBytes()).setElapsedNanos(r.elapsedNanos()).build();}
    public static SstMaintenance.Result result(CompactFilesResponse r) {return new SstMaintenance.Result(r.getExecuted(),r.getInputFilesList(),r.getOutputFilesList(),r.getValidatedInputBytes(),r.getOutputBytes(),r.getElapsedNanos());}
}
