package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Downloads exact node-owned artifacts through the original persisted snapshot owner. */
public final class RatisRemoteExportSource implements RatisExportClient.ChunkSource {
    private final RatisJournalAccess journalAccess;
    private final RatisSnapshotClient snapshots;
    private final RatisRoutedClient routed;
    private final Map<UUID,RatisExportJournal.Snapshot> bindings=new HashMap<>();
    private final Map<UUID,Set<UUID>> cleanup=new HashMap<>();
    public RatisRemoteExportSource(RatisSnapshotClient snapshots,RatisRoutedClient routed,RatisJournalAccess journalAccess) {
        this.journalAccess=Objects.requireNonNull(journalAccess);
        this.snapshots=Objects.requireNonNull(snapshots); this.routed=Objects.requireNonNull(routed);
    }
    @Override public synchronized void bind(RatisExportJournal.Snapshot saved) throws IOException {
        if (!Arrays.equals(snapshots.identityDigest(),saved.definition().sourceIdentity())) throw new IOException("Original export source identity changed");
        var previous=bindings.get(saved.definition().jobId());
        if (previous!=null && (!previous.owners().equals(saved.owners()) || !previous.captures().equals(saved.captures()) || !sameDefinition(previous.definition(),saved.definition()))) throw new IOException("Original export owner binding changed");
        if (previous==null && bindings.size()>=RatisExportClient.MAX_JOBS) throw new IOException("Remote export source binding capacity exhausted");
        bindings.put(saved.definition().jobId(),saved); cleanup.computeIfAbsent(saved.definition().jobId(),ignored -> new HashSet<>(saved.sourceCleanup()));
    }
    private static boolean sameDefinition(RatisExportJournal.Definition a,RatisExportJournal.Definition b) {
        return a.jobId().equals(b.jobId()) && a.fence().equals(b.fence()) && a.columns().equals(b.columns()) && a.sourceJournal().equals(b.sourceJournal()) && Arrays.equals(a.sourceIdentity(),b.sourceIdentity()) && a.limits().equals(b.limits());
    }
    private synchronized RatisRoutedClient.SnapshotTarget target(RatisExportSourceRpc.JobDefinition definition) throws IOException {
        var saved=bindings.get(definition.jobId());
        if (saved==null || !Arrays.equals(RatisExportJournal.sourceDefinition(saved,definition.group()).encode(),definition.encode())) throw new IOException("Unbound remote export source");
        var owner=saved.owners().get(definition.group()); if (owner==null) throw new IOException("Original remote export owner missing");
        return snapshots.restoreExportTarget(definition.fence(),owner,definition.capture());
    }
    @Override public CompletableFuture<NativeRangeSstExport.Chunk> write(RatisExportSourceRpc.BuildRequest request,Path artifact,RequestContext context) {
        try {
            long deadline=deadline(context); check(deadline); var target=target(request.definition());
            var descriptor=routed.exportBuild(target,request);
            check(deadline);
            if (descriptor.rows()>0) {
                if (Files.exists(artifact,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Remote artifact already published; recover exact local descriptor first");
                var download=RatisExportJournal.download(artifact);
                // An incomplete exclusive download has no publication authority; retry the same BUILD first.
                if (Files.exists(download,LinkOption.NOFOLLOW_LINKS)) {
                    if (!Files.isRegularFile(download,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid export download file");
                    Files.delete(download);
                }
                var digest=MessageDigest.getInstance("SHA-256");
                try (var output=FileChannel.open(download,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS)) {
                    long offset=0;
                    while (offset<descriptor.fileBytes()) {
                        check(deadline); int length=(int)Math.min(RatisExportSourceRpc.MAX_READ_BYTES,descriptor.fileBytes()-offset);
                        var read=new RatisExportSourceRpc.ReadRequest(request.definition(),descriptor.ordinal(),descriptor.digest(),offset,length);
                        byte[] bytes=routed.exportRead(target,read);
                        if (bytes.length!=length) throw new IOException("Truncated remote export download");
                        digest.update(bytes); var buffer=ByteBuffer.wrap(bytes); while (buffer.hasRemaining()) output.write(buffer); offset+=bytes.length;
                    }
                    if (!Arrays.equals(digest.digest(),descriptor.contentHash())) throw new IOException("Remote export content checksum differs");
                    output.force(true);
                }
                SessionJournalFiles.force(download.getParent());
            }
            RatisExportJournal.publishDownload(request,descriptor,artifact,journalAccess);
            return CompletableFuture.completedFuture(descriptor.nativeChunk(artifact));
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    private static long deadline(RequestContext context) { long timeout=context.timeoutNanos(); if (timeout==RequestContext.NO_TIMEOUT) return Long.MAX_VALUE; return System.nanoTime()+timeout; }
    private static void check(long deadline) throws IOException { if (deadline!=Long.MAX_VALUE && System.nanoTime()-deadline>=0) throw new IOException("Remote export request deadline expired"); }
    @Override public CompletableFuture<Void> release(RatisExportSourceRpc.JobDefinition definition,RequestContext context) {
        try { check(deadline(context)); routed.releaseExport(target(definition),new RatisExportSourceRpc.ReleaseRequest(definition)); synchronized (this) { var remaining=cleanup.get(definition.jobId()); remaining.remove(definition.group()); if (remaining.isEmpty()) { cleanup.remove(definition.jobId()); bindings.remove(definition.jobId()); } } return CompletableFuture.completedFuture(null); }
        catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
}
