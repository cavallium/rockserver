package it.cavallium.rockserver.core.common.export;

import it.cavallium.rockserver.core.common.api.*;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

/** Bounded, path-free Thrift export DTO conversion. */
public final class ThriftExportWire {
    private ThriftExportWire() {}
    public static ByteBuffer uuid(UUID v) { return ByteBuffer.allocate(16).putLong(v.getMostSignificantBits()).putLong(v.getLeastSignificantBits()).flip(); }
    public static UUID uuid(ByteBuffer v) { if (v == null || v.remaining()!=16) throw new IllegalArgumentException("Invalid UUID"); var b=v.duplicate(); return new UUID(b.getLong(),b.getLong()); }
    private static byte[] bytes(ByteBuffer v, int maximum) { if(v.remaining()>maximum) throw new IllegalArgumentException("Oversized binary"); var b=new byte[v.remaining()]; v.duplicate().get(b); return b; }
    public static ExportCreate create(ExportJobs.Create v) {
        var l=v.limits();
        return new ExportCreate(uuid(v.requestId()),v.columns(),new ExportLimits(l.maximumChunks(),l.maximumJournalBytes(),l.maximumDiskBytes(),l.maximumRows(),l.maximumPayloadBytes(),l.maximumFileBytes(),l.maximumChunkDuration().toNanos(),l.snapshotTtl().toMillis()));
    }
    public static ExportJobs.Create create(ExportCreate v) {
        if(v.columns==null || v.columns.size()>ExportJobs.MAX_COLUMNS) throw new IllegalArgumentException("Oversized columns");
        var l=v.limits;
        return new ExportJobs.Create(uuid(v.requestId),v.columns,new ExportJobs.Limits(l.maximumChunks,l.maximumJournalBytes,l.maximumDiskBytes,l.maximumRows,l.maximumPayloadBytes,l.maximumFileBytes,Duration.ofNanos(l.maximumChunkDurationNanos),Duration.ofMillis(l.snapshotTtlMillis)));
    }
    public static ExportChunkId id(ExportJobs.ChunkId v) { return new ExportChunkId(uuid(v.jobId()),uuid(v.groupId()),uuid(v.rangeLineage()),v.rangeEpoch(),v.snapshotTerm(),v.snapshotIndex(),v.ordinal(),ByteBuffer.wrap(v.contentHash())); }
    public static ExportJobs.ChunkId id(ExportChunkId v) { return new ExportJobs.ChunkId(uuid(v.jobId),uuid(v.groupId),uuid(v.rangeLineage),v.rangeEpoch,v.snapshotTerm,v.snapshotIndex,v.ordinal,bytes(v.contentHash,32)); }
    public static ExportStatus status(ExportJobs.Status v) {
        return new ExportStatus(uuid(v.jobId()),ExportPhase.valueOf(v.phase().name()),v.columns(),v.participantCount(),v.chunkCount(),
                v.acknowledgedCount(),v.fileBytes(),v.cleanupPending(),ByteBuffer.wrap(v.definitionDigest()),v.definitionBytes(),
                v.rosterGeneration(),v.domainCount(),ByteBuffer.wrap(v.captureDigest()),v.captureBytes());
    }
    public static ExportJobs.Status status(ExportStatus v) {
        if(v.columns==null || v.columns.isEmpty() || v.columns.size()>ExportJobs.MAX_COLUMNS) throw new IllegalArgumentException("Invalid columns");
        if(v.phase==null || !v.isSetDefinitionBytes() || !v.isSetRosterGeneration() || !v.isSetDomainCount() || !v.isSetCaptureBytes())
            throw new IllegalArgumentException("Missing export witness metadata");
        hash(v.definitionDigest);
        if(v.captureDigest==null || v.captureDigest.remaining()!=0 && v.captureDigest.remaining()!=32)
            throw new IllegalArgumentException("Invalid capture digest");
        if(v.definitionBytes<1 || v.definitionBytes>ExportJobs.MAX_WITNESS_BYTES || v.captureBytes<0 || v.captureBytes>ExportJobs.MAX_WITNESS_BYTES
                || v.rosterGeneration<0 || v.domainCount<1 || v.domainCount>it.cavallium.rockserver.core.common.distributed.DatabaseDomainRoster.MAX_DOMAINS
                || (v.captureDigest.remaining()==0)!=(v.captureBytes==0)) throw new IllegalArgumentException("Invalid witness metadata bounds");
        return new ExportJobs.Status(uuid(v.jobId),ExportJobs.Phase.valueOf(v.phase.name()),v.columns,v.participantCount,v.chunkCount,
                v.acknowledgedCount,v.fileBytes,v.cleanupPending,bytes(v.definitionDigest,32),v.definitionBytes,v.rosterGeneration,
                v.domainCount,bytes(v.captureDigest,32),v.captureBytes);
    }
    private static void hash(ByteBuffer digest) {
        if(digest==null || digest.remaining()!=32) throw new IllegalArgumentException("Digest must contain 32 bytes");
    }
    public static ExportWitnessRequest witnessRequest(ExportJobs.WitnessRequest v) {
        return new ExportWitnessRequest(uuid(v.jobId()),ExportWitnessKind.valueOf(v.kind().name()),ByteBuffer.wrap(v.expectedDigest()),v.offset(),v.maximumBytes());
    }
    public static ExportJobs.WitnessRequest witnessRequest(ExportWitnessRequest v) {
        if(v.kind==null || !v.isSetOffset() || !v.isSetMaximumBytes() || v.offset<0 || v.offset>ExportJobs.MAX_WITNESS_BYTES
                || v.maximumBytes<ExportJobs.MIN_PAGE_BYTES || v.maximumBytes>ExportJobs.MAX_WITNESS_PAGE_BYTES)
            throw new IllegalArgumentException("Invalid witness request bounds");
        hash(v.expectedDigest);
        return new ExportJobs.WitnessRequest(uuid(v.jobId),ExportJobs.WitnessKind.valueOf(v.kind.name()),bytes(v.expectedDigest,32),v.offset,v.maximumBytes);
    }
    private static void validateWitness(UUID job,ExportJobs.WitnessKind kind,ByteBuffer digest,long total,long offset,int length,boolean more,ExportJobs.WitnessRequest request) {
        hash(digest);
        if(!job.equals(request.jobId()) || kind!=request.kind() || !digest.equals(ByteBuffer.wrap(request.expectedDigest()))
                || total<1 || total>ExportJobs.MAX_WITNESS_BYTES || offset!=request.offset() || offset<0 || offset>total
                || length<0 || length>request.maximumBytes() || length>total-offset
                || more!=(offset+length<total) || more && length==0)
            throw new IllegalArgumentException("Invalid witness identity, digest or progress bounds");
    }
    public static ExportWitnessPage witness(ExportJobs.WitnessPage v,ExportJobs.WitnessRequest request) {
        var data=v.bytes();
        validateWitness(v.jobId(),v.kind(),ByteBuffer.wrap(v.digest()),v.totalBytes(),v.offset(),data.length,v.hasMore(),request);
        return new ExportWitnessPage(uuid(v.jobId()),ExportWitnessKind.valueOf(v.kind().name()),ByteBuffer.wrap(v.digest()),v.totalBytes(),v.offset(),ByteBuffer.wrap(data),v.hasMore());
    }
    public static ExportJobs.WitnessPage witness(ExportWitnessPage v,ExportJobs.WitnessRequest request) {
        if(v.data==null || v.kind==null || !v.isSetTotalBytes() || !v.isSetOffset() || !v.isSetHasMore()) throw new IllegalArgumentException("Missing witness fields");
        var job=uuid(v.jobId); var kind=ExportJobs.WitnessKind.valueOf(v.kind.name());
        validateWitness(job,kind,v.digest,v.totalBytes,v.offset,v.data.remaining(),v.hasMore,request);
        return new ExportJobs.WitnessPage(job,kind,bytes(v.digest,32),v.totalBytes,v.offset,bytes(v.data,request.maximumBytes()),v.hasMore);
    }
    public static ExportJobs.Status status(ExportStatus v,UUID expected) { var r=status(v); if(!r.jobId().equals(expected)) throw new IllegalArgumentException("Wrong status identity"); return r; }
    public static ExportStatus status(ExportJobs.Status v,UUID expected) { if(!v.jobId().equals(expected)) throw new IllegalArgumentException("Wrong status identity"); return status(v); }
    public static ExportManifestRequest manifestRequest(ExportJobs.ManifestRequest v) { return new ExportManifestRequest(uuid(v.jobId()),v.offset(),v.maximumItems(),v.maximumBytes()); }
    public static ExportJobs.ManifestRequest manifestRequest(ExportManifestRequest v) { return new ExportJobs.ManifestRequest(uuid(v.jobId),v.offset,v.maximumItems,v.maximumBytes); }
    public static ExportReadRequest readRequest(ExportJobs.ReadRequest v) { return new ExportReadRequest(id(v.id()),v.offset(),v.maximumBytes()); }
    public static ExportJobs.ReadRequest readRequest(ExportReadRequest v) { return new ExportJobs.ReadRequest(id(v.id),v.offset,v.maximumBytes); }
    private static ExportChunk chunk(ExportJobs.Chunk v) {
        var r=new ExportChunk().setId(id(v.id())).setColumnId(v.columnId()).setRows(v.rows()).setPayloadBytes(v.payloadBytes()).setFileBytes(v.fileBytes()).setHasMore(v.hasMore());
        if(v.lowerInclusive()!=null) r.setLowerInclusive(v.lowerInclusive()); if(v.upperExclusive()!=null) r.setUpperExclusive(v.upperExclusive()); return r;
    }
    private static ExportJobs.Chunk chunk(ExportChunk v) {
        long bounds=(v.lowerInclusive==null?0:v.lowerInclusive.remaining())+(long)(v.upperExclusive==null?0:v.upperExclusive.remaining());
        if(bounds>ExportJobs.MAX_BOUND_BYTES) throw new IllegalArgumentException("Oversized bounds");
        return new ExportJobs.Chunk(id(v.id),v.columnId,v.lowerInclusive==null?null:bytes(v.lowerInclusive,ExportJobs.MAX_BOUND_BYTES),v.upperExclusive==null?null:bytes(v.upperExclusive,ExportJobs.MAX_BOUND_BYTES),v.rows,v.payloadBytes,v.fileBytes,v.hasMore);
    }
    public static ExportManifestPage manifest(ExportJobs.ManifestPage v,ExportJobs.ManifestRequest request) {
        ExportWire.manifest(v,request); // shared identity, duplicate, continuation and byte-budget validation
        return new ExportManifestPage(v.chunks().stream().map(ThriftExportWire::chunk).toList(),v.next(),v.hasMore());
    }
    public static ExportJobs.ManifestPage manifest(ExportManifestPage v,ExportJobs.ManifestRequest request) {
        if(v.chunks==null || v.chunks.size()>request.maximumItems()) throw new IllegalArgumentException("Oversized manifest");
        long size=9;
        for(var c:v.chunks) size+=ExportJobs.CHUNK_METADATA_BYTES+(c.lowerInclusive==null?0:c.lowerInclusive.remaining())+(long)(c.upperExclusive==null?0:c.upperExclusive.remaining());
        if(size>request.maximumBytes()) throw new IllegalArgumentException("Oversized manifest");
        var result=new ExportJobs.ManifestPage(v.chunks.stream().map(ThriftExportWire::chunk).toList(),v.next,v.hasMore);
        ExportWire.manifest(result,request); return result;
    }
    public static ExportFilePage file(ExportJobs.FilePage v,ExportJobs.ReadRequest request) {
        ExportWire.file(v,request); return new ExportFilePage(id(v.id()),v.offset(),ByteBuffer.wrap(v.bytes()),v.hasMore());
    }
    public static ExportJobs.FilePage file(ExportFilePage v,ExportJobs.ReadRequest request) {
        var identity=id(v.id);
        if(!identity.equals(request.id()) || v.offset!=request.offset()) throw new IllegalArgumentException("Wrong file identity or offset");
        return new ExportJobs.FilePage(identity,v.offset,bytes(v.data,request.maximumBytes()),v.hasMore);
    }
}
