package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.api.proto.*;

/** Shared wire mapping; keeps embedded and remote maintenance contracts identical. */
public final class SstMaintenanceProto {
    private SstMaintenanceProto() {}
    public static GetSstMetadataResponse encode(SstMaintenance.Metadata m) {
        var b = GetSstMetadataResponse.newBuilder().setSession(m.session()).setColumnId(m.columnId())
                .setColumnName(m.columnName()).setNumLevels(m.numLevels()).setBaseLevel(m.baseLevel()).addAllPaths(m.paths());
        for(var f : m.files()) b.addFiles(SstFileMetadata.newBuilder().setName(f.name()).setLevel(f.level())
                .setPathId(f.pathId()).setSizeBytes(f.sizeBytes()).setSmallestKeyHex(f.smallestKeyHex())
                .setLargestKeyHex(f.largestKeyHex()).setBeingCompacted(f.beingCompacted()));
        var result = b.build();
        if(result.getSerializedSize() > SstMaintenance.MAX_METADATA_RESPONSE_BYTES)
            throw SstMaintenance.invalid("SST metadata exceeds 64 MiB; query one level at a time");
        return result;
    }
    public static SstMaintenance.Metadata decode(GetSstMetadataResponse m) {
        return new SstMaintenance.Metadata(m.getSession(),m.getColumnId(),m.getColumnName(),m.getNumLevels(),
                m.getBaseLevel(),m.getPathsList(),m.getFilesList().stream().map(f -> new SstMaintenance.File(
                    f.getName(),f.getLevel(),f.getPathId(),f.getSizeBytes(),f.getSmallestKeyHex(),f.getLargestKeyHex(),f.getBeingCompacted())).toList());
    }
    public static CompactFilesRequest encode(SstMaintenance.Request r) {
        return CompactFilesRequest.newBuilder().setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
                .setColumnId(r.columnId()).setSession(r.session()).addAllFiles(r.files()).setLevel(r.level())
                .setOutputPathId(r.outputPathId()).setOutputFileSizeLimit(r.outputFileSizeLimit()).setMaxInputBytes(r.maxInputBytes())
                .setMaxSubcompactions(r.maxSubcompactions()).setExecute(r.execute()).build();
    }
    public static SstMaintenance.Request decode(CompactFilesRequest r) {
        return new SstMaintenance.Request(r.getColumnId(),r.getSession(),r.getFilesList(),r.getLevel(),r.getOutputPathId(),
                r.getOutputFileSizeLimit(),r.getMaxInputBytes(),r.getMaxSubcompactions(),r.getExecute());
    }
    public static CompactFilesResponse encode(SstMaintenance.Result r) {
        return CompactFilesResponse.newBuilder().setExecuted(r.executed()).addAllInputFiles(r.inputFiles()).addAllOutputFiles(r.outputFiles())
                .setValidatedInputBytes(r.validatedInputBytes()).setOutputBytes(r.outputBytes()).setElapsedNanos(r.elapsedNanos()).build();
    }
    public static SstMaintenance.Result decode(CompactFilesResponse r) {
        return new SstMaintenance.Result(r.getExecuted(),r.getInputFilesList(),r.getOutputFilesList(),
                r.getValidatedInputBytes(),r.getOutputBytes(),r.getElapsedNanos());
    }
}
