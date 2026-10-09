package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded asynchronous control and delivery of immutable exports from one frozen source capture. */
public final class RatisExportClient implements AutoCloseable {
    public static final int MAX_JOBS=8,MAX_WAITERS=16,MIN_READ_BYTES=64*1024,MAX_READ_BYTES=1024*1024,MAX_MANIFEST_ITEMS=128,MAX_MANIFEST_BYTES=4*1024*1024;
    public static final long MAX_BYTES=2L*1024*1024*1024,MAX_QUEUED_BYTES=256L*1024*1024;
    public record Limits(int jobs,long metadataBytes,long queuedBytes) {
        public Limits { if (jobs<1 || jobs>MAX_JOBS || metadataBytes<1 || metadataBytes>MAX_BYTES || queuedBytes<1 || queuedBytes>MAX_QUEUED_BYTES) throw new IllegalArgumentException("Invalid export client limits"); }
        public static Limits defaults() { return new Limits(MAX_JOBS,1024L*1024*1024,128L*1024*1024); }
    }
    public record Stats(int jobs,int active,long metadataBytes,long queuedBytes) {}
    public record ManifestPage(List<RatisExportJournal.Chunk> chunks,int next,boolean hasMore) { public ManifestPage { chunks=List.copyOf(chunks); } }
    public record FilePage(RatisExportJournal.Chunk chunk,long offset,byte[] bytes,boolean hasMore) {
        public FilePage { Objects.requireNonNull(chunk); bytes=bytes.clone(); }
        @Override public byte[] bytes() { return bytes.clone(); }
    }
    public interface ChunkSource {
        void bind(RatisExportJournal.Snapshot snapshot) throws IOException;
        CompletableFuture<NativeRangeSstExport.Chunk> write(RatisExportSourceRpc.BuildRequest request,Path artifact,RequestContext context);
        CompletableFuture<Void> release(RatisExportSourceRpc.JobDefinition definition,RequestContext context);
    }
    private final RatisSnapshotClient source;
    private final RWScheduler scheduler;
    private final ChunkSource writer;
    private final Limits limits;
    private final Map<Path,Job> jobs=new LinkedHashMap<>();
    private final Map<Path,Job> sourcePaths=new HashMap<>();
    private long metadataBytes,queuedBytes;
    private boolean closing;
    /** A null source/writer is sufficient to reopen and deliver READY artifacts without source access. */
    public RatisExportClient(RatisSnapshotClient source,RWScheduler scheduler,ChunkSource writer,Limits limits) {
        this.source=source; this.scheduler=Objects.requireNonNull(scheduler); this.writer=writer; this.limits=Objects.requireNonNull(limits);
    }
    public Job create(Path path,RatisExportJournal.Definition definition,RequestContext context) throws IOException {
        Objects.requireNonNull(definition); var job=admit(path,definition); job.initializing=job.start("initialize",context,65536,false,task -> { job.initialize(); return CompletableFuture.completedFuture(job.journal.snapshot()); }); return job;
    }
    public Job reopen(Path path,RequestContext context) throws IOException {
        var job=admit(path,null); job.initializing=job.start("initialize",context,65536,false,task -> { job.initialize(); return CompletableFuture.completedFuture(job.journal.snapshot()); }); return job;
    }
    private synchronized Job admit(Path file,RatisExportJournal.Definition expected) throws IOException {
        if (closing) throw new IOException("Export client closing"); Path path=file.toAbsolutePath().normalize();
        long charge=3L*(expected==null ? 64L*1024*1024 : expected.limits().maximumJournalBytes())+64L*1024*1024+(MAX_WAITERS+1L)*MAX_READ_BYTES+65536;
        if (expected!=null && source!=null && source.lease(expected.sourceJournal())!=null) throw new IOException("Source snapshot lease already owned by another caller");
        if (sourcePaths.containsKey(path)) throw new IOException("Export job overlaps an owned source journal");
        if (expected!=null && (path.equals(expected.sourceJournal()) || jobs.containsKey(expected.sourceJournal()) || sourcePaths.containsKey(expected.sourceJournal()))) throw new IOException("Export source journal already owned or overlaps job");
        if (jobs.size()>=limits.jobs() || charge>limits.metadataBytes()-metadataBytes || jobs.containsKey(path)) throw new IOException("Export job metadata/ownership admission exhausted");
        var job=new Job(path,expected,charge); jobs.put(path,job); if (expected!=null) sourcePaths.put(expected.sourceJournal(),job); metadataBytes+=charge; return job;
    }
    public synchronized Stats stats() { return new Stats(jobs.size(),(int)jobs.values().stream().filter(job -> job.running!=null).count(),metadataBytes,queuedBytes); }
    private synchronized void reserve(long bytes) throws IOException { if (bytes<0 || bytes>limits.queuedBytes()-queuedBytes) throw new IOException("Export queued-work byte admission exhausted"); queuedBytes+=bytes; }
    private synchronized void refund(long bytes) { queuedBytes-=bytes; }
    private synchronized void remove(Job job) { if (jobs.remove(job.path,job)) { metadataBytes-=job.charge; if (job.definition!=null) sourcePaths.remove(job.definition.sourceJournal(),job); } }
    private synchronized void reserveSource(Job job,Path sourcePath) throws IOException { var previous=sourcePaths.get(sourcePath); if (sourcePath.equals(job.path) || jobs.containsKey(sourcePath) || previous!=null && previous!=job) throw new IOException("Export source journal already owned or overlaps job"); sourcePaths.put(sourcePath,job); }
    @FunctionalInterface private interface Work<T> { CompletableFuture<T> run(Task<T> task) throws Exception; }
    public final class Job implements AutoCloseable {
        private final Path path; private final RatisExportJournal.Definition expected; private final long charge;
        private volatile RatisExportJournal journal;
        private RatisSnapshotClient.Lease capture;
        private boolean ownCapture,cancelRequested,closed,creationAttempted,cleanupConfirmed,localClosing,foreignRejected;
        private RatisExportJournal.Definition definition;
        private final Map<Path,FileGuard> artifacts=new HashMap<>();
        private NativeRangeSstExport.Chunk knownNative;
        private RatisExportJournal.ChunkIntent knownIntent;
        private long knownCharge;
        private Task<?> knownOwner;
        private volatile Task<?> running;
        private Task<Void> queuedCancel;
        private Task<RatisExportJournal.Snapshot> initializing;
        private Task<Void> cancelling;
        private Job(Path path,RatisExportJournal.Definition expected,long charge) { this.path=path; this.expected=expected; this.definition=expected; this.charge=charge; }
        public Path path() { return path; }
        public synchronized CompletableFuture<RatisExportJournal.Snapshot> completion() { return initializing.view(); }
        public synchronized RatisExportJournal.Snapshot snapshot() throws IOException { requireJob(); if (journal==null) throw new IOException("Export initialization unfinished"); return journal.snapshot(); }
        private void requireJob() throws IOException { if (closed) throw new IOException("Export job closed"); }
        private void initialize() throws IOException {
            if (journal==null) {
                if (expected==null) journal=RatisExportJournal.reopen(path);
                else {
                    if (!creationAttempted && Files.exists(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Export journal exists; reopen it");
                    boolean retry=creationAttempted; creationAttempted=true;
                    journal=retry && Files.exists(path,LinkOption.NOFOLLOW_LINKS) ? RatisExportJournal.reopen(path) : RatisExportJournal.create(path,expected);
                    if (!sameDefinition(expected,journal.snapshot().definition())) { journal.close(); journal=null; throw new IOException("Uncertain export definition changed"); }
                }
                try {
                    definition=journal.snapshot().definition(); reserveSource(this,definition.sourceJournal());
                    if (writer!=null && !journal.snapshot().sourceCleanup().isEmpty()) writer.bind(journal.snapshot());
                    if (journal.snapshot().phase()==RatisExportJournal.Phase.READY) cacheArtifacts();
                } catch (IOException | RuntimeException failure) {
                    var failed=journal; journal=null;
                    try { failed.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
                    throw failure;
                }
            }
        }
        private synchronized <T> Task<T> start(Object key,RequestContext context,long bytes,boolean control,Work<T> work) throws IOException {
            requireJob(); if (!key.equals("initialize") && journal==null) throw new IOException("Await successful export initialization first"); if (localClosing) throw new IOException("Export local close active"); if (running!=null) {
                if (!running.key.equals(key)) throw new IOException("Export operation already active");
                @SuppressWarnings("unchecked") var same=(Task<T>)running; return same;
            }
            if ((cancelRequested || closing) && !control && !key.equals("reclaim")) throw new IOException("Export cancellation/close requested");
            var task=new Task<T>(this,key,context,bytes,control,work);
            if (key.equals("step") && knownNative!=null) {
                // Transfer the existing outcome reservation; a retry must fit the original admission.
                task.chargedBytes=knownCharge; task.retainedBytes=knownCharge; knownOwner=task;
            } else reserve(bytes);
            running=task; task.dispatch(); return task;
        }
        public synchronized CompletableFuture<RatisExportJournal.Snapshot> step(RequestContext context) {
            try {
                if (running!=null) { if (running.key.equals("step")) { @SuppressWarnings("unchecked") var current=(Task<RatisExportJournal.Snapshot>)running; return current.view(); } throw new IOException("Export operation already active"); }
                initializeIfReady(); var definition=journal.snapshot().definition();
                long bytes=Math.addExact(definition.limits().chunk().maximumPayloadBytes(),Math.addExact(definition.limits().chunk().maximumFileBytes(),8L*1024*1024));
                return start("step",context,bytes,false,this::advance).view();
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private void initializeIfReady() throws IOException { requireJob(); if (journal==null) throw new IOException("Await export initialization first"); }
        private CompletableFuture<RatisExportJournal.Snapshot> advance(Task<RatisExportJournal.Snapshot> task) {
            CompletableFuture<RatisExportJournal.Snapshot> actual;
            try { actual=advanceOriginal(task); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            return actual.handle((value,failure) -> new Advanced(value,failure)).thenCompose(done -> {
                if (done.failure()==null) return CompletableFuture.completedFuture(done.value());
                var failure=unwrap(done.failure());
                return task.io(false,() -> {
                    var phase=journal.snapshot().phase();
                    if (lost(failure) && (phase==RatisExportJournal.Phase.CAPTURING || phase==RatisExportJournal.Phase.EXPORTING)) journal.failSnapshotLost();
                    if (failure instanceof Exception problem) throw problem; if (failure instanceof Error fatal) throw fatal; throw new IOException(failure);
                });
            });
        }
        private CompletableFuture<RatisExportJournal.Snapshot> advanceOriginal(Task<RatisExportJournal.Snapshot> task) throws Exception {
            task.check(); var saved=journal.snapshot();
            switch (saved.phase()) {
                case READY, RELEASED, CANCELLED, SNAPSHOT_LOST -> { return CompletableFuture.completedFuture(saved); }
                case CAPTURING -> {
                    return capture(task,saved.definition()).thenCompose(lease -> task.io(false,() -> { journal.bindSource(lease.snapshot()); if (writer!=null) writer.bind(journal.snapshot()); return journal.snapshot(); }));
                }
                case MATERIALIZED -> {
                    return captureForCleanup(task,saved.definition()).thenCompose(lease -> task.io(true,() -> lease.release(task.remaining())).thenCompose(value -> value))
                            .thenCompose(ignored -> cleanupRemote(task)).thenCompose(ignored -> task.io(false,() -> { journal.markReady(capture.snapshot()); cleanupConfirmed=true; cacheArtifacts(); closeCapture(); return journal.snapshot(); }));
                }
                case EXPORTING -> {
                    if (knownNative!=null) return CompletableFuture.completedFuture(forceKnownNative());
                    var recovered=journal.recoverChunk(); if (recovered!=null) return CompletableFuture.completedFuture(journal.snapshot());
                    var intent=saved.pending();
                    if (intent==null) {
                        UUID group=saved.definition().fence().participants().stream().map(p -> p.groupId()).filter(id -> saved.chunks().stream().noneMatch(chunk -> chunk.group().equals(id) && !chunk.hasMore())).findFirst().orElseThrow();
                        intent=journal.freezeChunk(group);
                    }
                    var frozen=intent; var request=new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(saved,frozen.group()),frozen);
                    if (writer==null) throw new IOException("Original export chunk source unavailable");
                    if (source==null || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !Arrays.equals(source.identityDigest(),saved.definition().sourceIdentity())) throw new IOException("Original export source identity changed");
                    if (capture==null && source.lease(saved.definition().sourceJournal())!=null) { foreignRejected=true; throw new IOException("Source snapshot lease already owned by another caller"); }
                    if (!Files.exists(saved.definition().sourceJournal(),LinkOption.NOFOLLOW_LINKS)) throw new RatisSnapshotLostException("Original bound source journal is missing");
                    {
                        CompletableFuture<NativeRangeSstExport.Chunk> actual;
                        try { actual=Objects.requireNonNull(writer.write(request,journal.artifactPath(frozen),task.remaining())); }
                        catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
                        return actual.handle((chunk,failure) -> {
                            if (failure==null) synchronized (this) {
                                knownNative=Objects.requireNonNull(chunk); knownIntent=frozen; knownCharge=task.bytes; knownOwner=task; task.retainedBytes=task.bytes;
                            }
                            return new Built(chunk,failure);
                        }).thenCompose(result -> task.io(false,() -> {
                            if (result.failure()!=null) {
                                var failure=unwrap(result.failure());
                                if (lost(failure)) journal.failSnapshotLost();
                                if (failure instanceof Exception problem) throw problem; if (failure instanceof Error fatal) throw fatal; throw new IOException(failure);
                            }
                            // The native artifact is now known: force its exact descriptor even if the observer deadline elapsed.
                            return forceKnownNative();
                        }));
                    }
                }
            }
            throw new IOException("Unknown export phase");
        }
        private RatisExportJournal.Snapshot forceKnownNative() throws IOException {
            journal.publish(knownIntent,knownNative);
            clearKnownNative(); return journal.snapshot();
        }
        private synchronized void clearKnownNative() {
            knownNative=null; knownIntent=null; long charge=knownCharge; knownCharge=0;
            var owner=knownOwner; knownOwner=null;
            if (owner!=null && !owner.finished.get()) owner.retainedBytes=0;
            else if (charge!=0) refund(charge);
        }
        private CompletableFuture<RatisSnapshotClient.Lease> capture(Task<?> task,RatisExportJournal.Definition definition) {
            try {
                if (source==null || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !Arrays.equals(source.identityDigest(),definition.sourceIdentity())) throw new IOException("Original export source identity changed");
                if (capture==null) {
                    if (!journal.snapshot().captures().isEmpty() && !Files.exists(definition.sourceJournal(),LinkOption.NOFOLLOW_LINKS)) throw new RatisSnapshotLostException("Original bound source journal is missing; capture cannot be recreated");
                    var current=source.lease(definition.sourceJournal());
                    if (current!=null) { foreignRejected=true; throw new IOException("Source snapshot lease already owned by another caller"); }
                    else {
                        capture=Files.exists(definition.sourceJournal(),LinkOption.NOFOLLOW_LINKS) ? source.reopenFrozen(definition.sourceJournal(),definition.fence(),task.remaining()) : source.openFrozen(definition.sourceJournal(),definition.fence(),Duration.ofSeconds(60),task.remaining()); ownCapture=true;
                    }
                }
                CompletableFuture<Void> acquired=capture.completion();
                if (acquired.isCompletedExceptionally()) acquired=capture.resume(task.remaining());
                var original=capture;
                return acquired.thenApply(ignored -> { try { if (!original.snapshot().initial().fence().equals(definition.fence())) throw new IOException("Original export source cut changed"); } catch (IOException failure) { throw new CompletionException(failure); } return original; });
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private CompletableFuture<RatisSnapshotClient.Lease> captureForCleanup(Task<?> task,RatisExportJournal.Definition definition) {
            if (capture!=null) return CompletableFuture.completedFuture(capture);
            try {
                if (source==null || source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !Arrays.equals(source.identityDigest(),definition.sourceIdentity())) throw new IOException("Original cleanup source identity changed");
                var current=source.lease(definition.sourceJournal());
                if (current!=null) { foreignRejected=true; throw new IOException("Source cleanup lease already owned by another caller"); }
                else { capture=source.reopenForReleaseFrozen(definition.sourceJournal(),definition.fence(),task.remaining()); ownCapture=true; }
                return CompletableFuture.completedFuture(capture);
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<ManifestPage> manifest(int offset,int maximumItems,int maximumBytes,RequestContext context) {
            if (offset<0 || maximumItems<1 || maximumItems>MAX_MANIFEST_ITEMS || maximumBytes<MIN_READ_BYTES || maximumBytes>MAX_MANIFEST_BYTES) return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid export manifest page limits"));
            try { return this.<ManifestPage>start(List.of("manifest",offset,maximumItems,maximumBytes),context,maximumBytes,false,task -> {
                task.check(); var saved=ready(); if (offset>saved.chunks().size()) throw new IllegalArgumentException("Manifest offset beyond retained coverage");
                var chunks=new ArrayList<RatisExportJournal.Chunk>(); int bytes=0,index=offset;
                while (index<saved.chunks().size() && chunks.size()<maximumItems) { var chunk=saved.chunks().get(index); long size=256L+length(chunk.lowerInclusive())+length(chunk.upperExclusive()); if (size>maximumBytes-bytes) { if (chunks.isEmpty()) throw new IOException("Export descriptor exceeds manifest page budget"); break; } bytes+=size; chunks.add(chunk); index++; }
                return CompletableFuture.completedFuture(new ManifestPage(chunks,index,index<saved.chunks().size()));
            }).view(); } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<FilePage> read(RatisExportJournal.Chunk chunk,long offset,int maximumBytes,RequestContext context) {
            if (offset<0 || maximumBytes<MIN_READ_BYTES || maximumBytes>MAX_READ_BYTES) return CompletableFuture.failedFuture(new IllegalArgumentException("Invalid export file read limits"));
            try { return this.<FilePage>start(List.of("read",chunk,offset,maximumBytes),context,maximumBytes+65536L,false,task -> {
                task.check(); requireChunk(chunk); if (offset>chunk.fileBytes()) throw new IllegalArgumentException("Offset beyond original export file");
                byte[] data=new byte[(int)Math.min(maximumBytes,chunk.fileBytes()-offset)];
                if (chunk.file()!=null) {
                    verify(chunk); try (var channel=FileChannel.open(chunk.file(),StandardOpenOption.READ)) { var target=ByteBuffer.wrap(data); long cursor=offset; while (target.hasRemaining()) { task.check(); int count=channel.read(target,cursor); if (count<0) throw new EOFException("Original export artifact truncated"); cursor+=count; } } verify(chunk);
                }
                return CompletableFuture.completedFuture(new FilePage(chunk,offset,data,offset+data.length<chunk.fileBytes()));
            }).view(); } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<Void> acknowledge(RatisExportJournal.Chunk chunk,RequestContext context) {
            try { return this.<Void>start(List.of("ack",chunk),context,65536,false,task -> { task.check(); requireChunk(chunk); journal.acknowledge(chunk); return CompletableFuture.<Void>completedFuture(null); }).view(); }
            catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<Void> release(RequestContext context) {
            try { return this.<Void>start("release",context,65536,true,task -> {
                task.check(); var phase=journal.snapshot().phase();
                if (phase==RatisExportJournal.Phase.READY || phase==RatisExportJournal.Phase.RELEASED) { return cleanupRemote(task).thenCompose(ignored -> task.io(true,() -> { journal.release(); return null; })); }
                return cleanupSource(task).thenCompose(ignored -> task.io(true,() -> { journal.release(); return null; }));
            }).view(); }
            catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<Void> reclaim(RequestContext context) {
            try { return this.<Void>start("reclaim",context,65536,false,task -> { task.check(); journal.reclaimArtifacts(); return CompletableFuture.<Void>completedFuture(null); }).view(); }
            catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public synchronized CompletableFuture<Void> cancel(RequestContext context) {
            if (closed) return CompletableFuture.failedFuture(new IOException("Export job closed"));
            cancelRequested=true;
            if (cancelling!=null && !cancelling.result.isDone()) return cancelling.view();
            try {
                var task=new Task<Void>(this,"cancel",context,65536,true,work -> work.io(false,() -> { initialize(); return null; })
                        .thenCompose(ignored -> work.io(true,() -> { journal.cancel(); clearKnownNative(); return null; }))
                        .thenCompose(ignored -> cleanupSource(work)));
                // One bounded cancellation task uses the per-job 64KiB metadata headroom,
                // so saturation of retained data ownership cannot prevent protected cleanup.
                task.chargedBytes=0; cancelling=task;
                if (running!=null) {
                    var original=running; queuedCancel=task;
                    original.drained.whenComplete((ignored,failure) -> task.dispatch());
                } else { running=task; task.dispatch(); }
                return task.view();
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private CompletableFuture<Void> cleanupSource(Task<?> task) {
            try {
                if (cleanupConfirmed || foreignRejected && capture==null) { cleanupConfirmed=true; return cleanupRemote(task); }
                var saved=journal.snapshot();
                if (!Files.exists(saved.definition().sourceJournal()) && capture==null) { cleanupConfirmed=true; return cleanupRemote(task); }
                return captureForCleanup(task,saved.definition()).thenCompose(lease -> task.io(true,() -> lease.release(task.remaining())).thenCompose(value -> value)).thenCompose(ignored -> task.io(true,() -> { cleanupConfirmed=true; closeCapture(); return null; })).thenCompose(ignored -> cleanupRemote(task));
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private CompletableFuture<Void> cleanupRemote(Task<?> task) {
            try {
                var saved=journal.snapshot(); if (saved.sourceCleanup().isEmpty()) return CompletableFuture.completedFuture(null);
                if (writer==null) throw new IOException("Original export artifact cleanup source unavailable");
                writer.bind(saved);
                CompletableFuture<Void> result=CompletableFuture.completedFuture(null);
                for (var group:saved.sourceCleanup()) result=result.thenCompose(ignored -> task.io(true,() -> writer.release(RatisExportJournal.sourceDefinition(saved,group),task.remaining())).thenCompose(value -> value)).thenCompose(ignored -> task.io(true,() -> { journal.sourceReleased(group); return null; }));
                return result;
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private RatisExportJournal.Snapshot ready() throws IOException { initializeIfReady(); var saved=journal.snapshot(); if (saved.phase()!=RatisExportJournal.Phase.READY) throw new IOException("Export delivery requires READY immutable artifacts"); return saved; }
        private void requireChunk(RatisExportJournal.Chunk chunk) throws IOException {
            var saved=ready(); if (saved.chunks().stream().noneMatch(original -> same(original,chunk))) throw new IllegalArgumentException("Delivery descriptor differs from original published chunk");
        }
        private void cacheArtifacts() throws IOException {
            for (var chunk:journal.snapshot().chunks()) if (chunk.file()!=null) {
                var guard=fileGuard(chunk.file()); if (!guard.regular() || guard.size()!=chunk.fileBytes()) throw new IOException("Verified export artifact identity changed"); artifacts.put(chunk.file(),guard);
            }
        }
        private void verify(RatisExportJournal.Chunk chunk) throws IOException {
            var expected=artifacts.get(chunk.file());
            if (expected==null || !expected.equals(fileGuard(chunk.file()))) throw new IOException("Immutable export artifact identity/size/time changed");
        }
        private void closeCapture() throws IOException { if (capture!=null && ownCapture) capture.close(); capture=null; ownCapture=false; }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; if (running!=null || localClosing || knownNative!=null) throw new IOException("Export actual work/close active; ownership retained"); localClosing=true; }
            try {
                if (foreignRejected && capture==null) cleanupConfirmed=true;
                if (capture!=null) {
                    if (capture.uninitializedFailure()) { closeCapture(); cleanupConfirmed=true; }
                    else { var state=capture.snapshot(); if (!settled(state)) throw new IOException("Original source captures remain owned or unresolved; retry cleanup"); closeCapture(); cleanupConfirmed=true; }
                }
                if (journal!=null) { var phase=journal.snapshot().phase(); if (!journal.snapshot().sourceCleanup().isEmpty() && phase!=RatisExportJournal.Phase.CAPTURING) throw new IOException("Remote artifact cleanup remains pending"); if (!cleanupConfirmed && (phase==RatisExportJournal.Phase.SNAPSHOT_LOST || phase==RatisExportJournal.Phase.CANCELLED || phase==RatisExportJournal.Phase.MATERIALIZED || phase==RatisExportJournal.Phase.EXPORTING) && Files.exists(journal.snapshot().definition().sourceJournal())) throw new IOException("Recovered source cleanup must settle before ownership release"); journal.close(); }
                synchronized (this) { closed=true; } remove(this);
            } finally { synchronized (this) { localClosing=false; } }
        }
    }
    private record Advanced(RatisExportJournal.Snapshot value,Throwable failure) {}
    private record Built(NativeRangeSstExport.Chunk chunk,Throwable failure) {}
    @FunctionalInterface private interface Io<T> { T run() throws Exception; }
    private final class Task<T> implements Runnable,RWScheduler.RejectionAwareTask {
        final Job job; final Object key; final long bytes,deadline; final RequestContext context; final boolean control; final Work<T> work;
        final CompletableFuture<T> result=new CompletableFuture<>(); final CompletableFuture<Void> drained=new CompletableFuture<>(); final AtomicInteger phase=new AtomicInteger(); final java.util.concurrent.atomic.AtomicBoolean finished=new java.util.concurrent.atomic.AtomicBoolean(); int views; volatile long chargedBytes,retainedBytes; volatile RuntimeException rejection;
        Task(Job job,Object key,RequestContext context,long bytes,boolean control,Work<T> work) { this.job=job; this.key=key; this.context=new RequestContext(WorkloadProfile.BATCH,context.timeoutNanos()); this.bytes=bytes; this.chargedBytes=bytes; this.control=control; this.work=work; deadline=scheduler.resolveMonotonicDeadline(context); }
        synchronized CompletableFuture<T> view() { if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Export observer capacity exhausted")); if (!result.isDone()) views++; return result.copy(); }
        RequestContext remaining() throws IOException { if (deadline==Long.MAX_VALUE) return context; long remaining=deadline-System.nanoTime(); if (remaining<=0) throw new IOException("Export request deadline expired",new TimeoutException()); return new RequestContext(context.profile(),remaining); }
        void check() throws IOException { if (rejection!=null) throw new IOException("Export task rejected; original ownership retained",rejection); if (deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Export request deadline expired",new TimeoutException()); }
        void dispatch() { try { executor(control).execute(this,Math.min(bytes,WorkloadCost.MAX_ESTIMATED_BYTES)); } catch (RuntimeException failure) { reject(failure); } }
        RWScheduler.WorkloadExecutor executor(boolean protectedLane) { return protectedLane ? scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT) : scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION); }
        <V> CompletableFuture<V> io(boolean protectedLane,Io<V> body) {
            var value=new CompletableFuture<V>();
            final class Segment implements Runnable,RWScheduler.RejectionAwareTask {
                final AtomicInteger phase=new AtomicInteger(); volatile RuntimeException rejection;
                public void run() {
                    if (!phase.compareAndSet(0,1)) return; V result=null; Throwable failure=null;
                    try { result=body.run(); } catch (Throwable problem) { failure=problem; }
                    phase.set(2); if (failure==null) failure=rejection;
                    if (failure==null) value.complete(result); else value.completeExceptionally(failure);
                    if (failure instanceof Error fatal) throw fatal;
                }
                public void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) value.completeExceptionally(failure); else if (phase.get()==1) rejection=failure; }
            }
            try { executor(protectedLane).execute(new Segment(),Math.min(bytes,WorkloadCost.MAX_ESTIMATED_BYTES)); } catch (RuntimeException failure) { value.completeExceptionally(failure); }
            return value;
        }
        public void run() {
            if (!phase.compareAndSet(0,1)) return; CompletableFuture<T> actual;
            try { check(); actual=Objects.requireNonNull(work.run(this)); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
            actual.whenComplete((value,failure) -> { if (failure==null) try { check(); } catch (Throwable late) { failure=late; } finish(value,failure); });
        }
        public void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) finish(null,failure); else rejection=failure; }
        void finish(T value,Throwable failure) {
            if (!finished.compareAndSet(false,true)) return; phase.set(2);
            synchronized (job) { if (job.running==this) { job.running=job.queuedCancel; job.queuedCancel=null; } }
            refund(chargedBytes-retainedBytes); drained.complete(null); if (failure==null) result.complete(value); else result.completeExceptionally(unwrap(failure));
        }
    }
    private static boolean sameDefinition(RatisExportJournal.Definition a,RatisExportJournal.Definition b) { return a.jobId().equals(b.jobId()) && a.fence().equals(b.fence()) && a.column().equals(b.column()) && a.sourceJournal().equals(b.sourceJournal()) && Arrays.equals(a.sourceIdentity(),b.sourceIdentity()) && a.limits().equals(b.limits()); }
    private static boolean settled(RatisDurableSnapshot.Snapshot source) {
        if (!source.releaseRequested() || source.observedHome()==null || !source.observedHome().terminal()) return false;
        for (var participant:source.participants()) {
            if (participant.retirementProof()==null && (participant.releaseReceipt()==null || participant.nativeGate()==null || participant.nativeGate().released()==null)) return false;
            if ((participant.capture()!=null || participant.captureToken()!=null) && !participant.captureReleased() && !participant.captureReleaseLost()) return false;
        }
        return true;
    }
    private static int length(byte[] bytes) { return bytes==null ? 0 : bytes.length; }
    private static boolean same(RatisExportJournal.Chunk a,RatisExportJournal.Chunk b) { return a.jobId().equals(b.jobId()) && a.group().equals(b.group()) && a.rangeEpoch()==b.rangeEpoch() && a.cut().equals(b.cut()) && a.ordinal()==b.ordinal() && a.column().equals(b.column()) && Arrays.equals(a.lowerInclusive(),b.lowerInclusive()) && Arrays.equals(a.upperExclusive(),b.upperExclusive()) && Arrays.equals(a.sha256(),b.sha256()) && a.rows()==b.rows() && a.payloadBytes()==b.payloadBytes() && a.fileBytes()==b.fileBytes() && Objects.equals(a.file(),b.file()) && a.hasMore()==b.hasMore(); }
    private static boolean lost(Throwable failure) { for (int n=0;failure!=null && n<32;n++,failure=failure.getCause()) if (failure instanceof RatisSnapshotLostException || failure instanceof it.cavallium.rockserver.core.common.distributed.SnapshotRetentionExpiredException) return true; return false; }
    private static Throwable unwrap(Throwable failure) { while ((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null) failure=failure.getCause(); return failure; }
    private record FileGuard(Object key,long size,java.nio.file.attribute.FileTime modified,java.nio.file.attribute.FileTime created,boolean regular) {}
    private static FileGuard fileGuard(Path file) throws IOException {
        var attributes=Files.readAttributes(file,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
        return new FileGuard(attributes.fileKey(),attributes.size(),attributes.lastModifiedTime(),attributes.creationTime(),attributes.isRegularFile());
    }
    @Override public void close() throws IOException {
        List<Job> owners; synchronized (this) { closing=true; owners=List.copyOf(jobs.values()); }
        IOException failure=null; for (var job:owners) try { job.close(); } catch (IOException active) { if (failure==null) failure=active; else failure.addSuppressed(active); } if (failure!=null) throw failure;
    }
}
