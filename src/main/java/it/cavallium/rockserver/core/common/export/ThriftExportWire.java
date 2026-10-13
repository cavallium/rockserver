package it.cavallium.rockserver.core.common.export;

import it.cavallium.rockserver.core.common.api.*;
import it.cavallium.rockserver.core.common.distributed.DistributedSchemaCodec;
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
    public static ExportStatus status(ExportJobs.Status v) { return new ExportStatus(uuid(v.jobId()),ExportPhase.valueOf(v.phase().name()),v.columns(),v.participantCount(),v.chunkCount(),v.acknowledgedCount(),v.fileBytes(),v.cleanupPending(),ByteBuffer.wrap(v.schemaBytes())); }
    public static ExportJobs.Status status(ExportStatus v) {
        if(v.columns==null || v.columns.size()>ExportJobs.MAX_COLUMNS) throw new IllegalArgumentException("Oversized columns");
        if(v.schema==null || !v.schema.hasRemaining() || v.schema.remaining()>DistributedSchemaCodec.MAX_BYTES)
            throw new IllegalArgumentException("Missing or oversized frozen export schema");
        return new ExportJobs.Status(uuid(v.jobId),ExportJobs.Phase.valueOf(v.phase.name()),v.columns,v.participantCount,v.chunkCount,v.acknowledgedCount,v.fileBytes,v.cleanupPending,
                bytes(v.schema,DistributedSchemaCodec.MAX_BYTES));
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
