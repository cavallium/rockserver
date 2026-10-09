package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Public export facade. Supplied clients, scheduler and allocation source remain caller-owned. */
public final class RatisExportBackend implements ExportAsyncAPI,AutoCloseable {
    public record Limits(int requests,int requestBytes,int activeJobs,int queuedWork) {
        public Limits { if (requests<1 || requests>1024 || requestBytes<65536 || requestBytes>64*1024*1024 || activeJobs<1 || activeJobs>RatisExportClient.MAX_JOBS || queuedWork<1 || queuedWork>32) throw new IllegalArgumentException("Invalid export backend limits"); }
        public static Limits defaults() { return new Limits(128,16*1024*1024,8,16); }
    }
    private final Path root;
    private final DistributedSchema schema;
    private final RatisSnapshotClient source;
    private final RWScheduler scheduler;
    private final RatisExportClient exports;
    private final RatisRocksDBAdapter.AllocationSource allocations;
    private final Limits limits;
    private final FileChannel ownership;
    private final FileLock lock;
    private final RatisExportRequests requests;
    private final Map<UUID,RatisExportClient.Job> jobs=new HashMap<>();
    private final Map<UUID,Opening> opening=new HashMap<>();
    private final Set<UUID> verified=new HashSet<>();
    private final Map<UUID,Control<?>> controls=new HashMap<>();
    private static final class Control<T> {
        final Object key; final CompletableFuture<T> result=new CompletableFuture<>(); int observers;
        Control(Object key) { this.key=key; }
        synchronized CompletableFuture<T> view() {
            if (!result.isDone() && observers>=RatisExportClient.MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Export control observer capacity exhausted"));
            if (!result.isDone()) observers++; return result.copy();
        }
    }
    private int active;
    private boolean closed,closing;
    private record Opening(ExportJobs.Create request,CompletableFuture<RatisExportClient.Job> result) {}

    /** Root preparation and exclusive ownership happen here, never on a public async API call. */
    public RatisExportBackend(Path root,DistributedSchema schema,RatisSnapshotClient source,RWScheduler scheduler,RatisExportClient exports,RatisRocksDBAdapter.AllocationSource allocations,Limits limits) throws IOException {
        this.root=root.toAbsolutePath().normalize(); this.schema=Objects.requireNonNull(schema); this.source=source; this.scheduler=Objects.requireNonNull(scheduler); this.exports=Objects.requireNonNull(exports); this.allocations=allocations; this.limits=Objects.requireNonNull(limits);
        if (source!=null && source.captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY) throw new IllegalArgumentException("Export requires a read-only snapshot source");
        Files.createDirectories(this.root); if (!Files.isDirectory(this.root,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Export root is not a directory"); SessionJournalFiles.force(this.root.getParent());
        boolean existingOwner=Files.exists(this.root.resolve("root.lock"),LinkOption.NOFOLLOW_LINKS);
        ownership=FileChannel.open(this.root.resolve("root.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
        FileLock acquired=null;
        try {
            try { acquired=ownership.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Export root already owned",owned); }
            if (acquired==null) throw new IOException("Export root already owned"); lock=acquired;
            if (existingOwner && !Files.exists(this.root.resolve("requests"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Original export nonce catalog is missing");
            requests=new RatisExportRequests(this.root,limits.requests(),limits.requestBytes());
            for (var entry:requests.entries()) validate(entry);
        } catch (IOException | RuntimeException | Error failure) { if (acquired!=null) acquired.release(); ownership.close(); throw failure; }
    }
    private List<UUID> columns(ExportJobs.Create request) { return request.columns().stream().map(id -> schema.column(id).columnId()).sorted((a,b) -> { int first=Long.compareUnsigned(a.getMostSignificantBits(),b.getMostSignificantBits()); return first!=0 ? first : Long.compareUnsigned(a.getLeastSignificantBits(),b.getLeastSignificantBits()); }).toList(); }
    private void validate(RatisExportRequests.Entry entry) throws IOException {
        if (!Arrays.equals(entry.schema(),schema.digest()) || source!=null && !Arrays.equals(entry.source(),source.identityDigest())) throw new IOException("Original export schema/source identity changed");
        var ids=columns(entry.request());
        if (entry.fence()!=null) {
            if (entry.snapshot().startsWith(root)) throw new IOException("Export source allocation must be disjoint from backend control root");
            if (!entry.fence().clusterId().equals(schema.clusterId()) || !entry.fence().databaseId().equals(schema.databaseId()) || !entry.fence().domainId().equals(schema.domainId()) || !Arrays.equals(entry.fence().schemaDigest(),schema.digest())) throw new IOException("Saved export fence differs from qualified schema");
            var definition=entry.definition(ids); var scope=RatisSnapshotPlan.scope(definition.fence());
            if (scope.spans().size()!=ids.size() || scope.spans().stream().anyMatch(span -> span.isPoint() || span.lowerInclusive()!=null || span.upperExclusive()!=null || !ids.contains(span.columnId()))) throw new IOException("Saved export is not the selected full-column scope");
        }
    }
    private final class Deadline {
        final long bound;
        Deadline(RequestContext context) { bound=scheduler.resolveMonotonicDeadline(Objects.requireNonNull(context)); }
        RequestContext remaining() {
            if (bound==Long.MAX_VALUE) return RequestContext.batch();
            long nanos=scheduler.remainingMonotonicDeadlineNanos(bound);
            if (nanos<=0) throw new CompletionException(new TimeoutException("Export request deadline expired"));
            return new RequestContext(WorkloadProfile.BATCH,nanos);
        }
    }
    @FunctionalInterface private interface DeadlineWork<T> { CompletableFuture<T> run(Deadline deadline) throws Exception; }
    @FunctionalInterface private interface Work<T> { CompletableFuture<T> run() throws Exception; }
    /** Retain admission through the actual SDK future, independent of cancellation of any observer. */
    private <T> CompletableFuture<T> submit(RequestContext context,DeadlineWork<T> work) {
        var result=new CompletableFuture<T>(); final Deadline deadline;
        try { deadline=new Deadline(context); synchronized (this) { if (closed || closing) throw new IOException("Export backend closed"); if (active>=limits.queuedWork()) throw new RejectedExecutionException("Export backend queued work exhausted"); active++; } }
        catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        var phase=new AtomicInteger();
        class Task implements Runnable,RWScheduler.RejectionAwareTask {
            public void run() {
                if (!phase.compareAndSet(0,1)) return;
                CompletableFuture<T> actual;
                try { deadline.remaining(); actual=Objects.requireNonNull(work.run(deadline)); } catch (Throwable failure) { actual=CompletableFuture.failedFuture(failure); }
                actual.whenComplete((value,failure) -> { synchronized (RatisExportBackend.this) { active--; } phase.set(2); if (failure==null) try { deadline.remaining(); } catch (Throwable expired) { failure=expired; } if (failure==null) result.complete(value); else result.completeExceptionally(failure); });
            }
            public void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) { synchronized (RatisExportBackend.this) { active--; } result.completeExceptionally(failure); } }
        }
        var task=new Task(); try { scheduler.executor(WorkloadProfile.BATCH,OperationFamily.MUTATION,deadline.bound).execute(task,65536); } catch (RuntimeException failure) { task.reject(failure); }
        return result.copy();
    }
    private <T> CompletableFuture<T> io(Work<T> work) { return io(false,work); }
    private <T> CompletableFuture<T> io(boolean control,Work<T> work) {
        var result=new CompletableFuture<T>();
        class Segment implements Runnable,RWScheduler.RejectionAwareTask {
            final AtomicInteger phase=new AtomicInteger();
            public void run() { if (!phase.compareAndSet(0,1)) return; try { work.run().whenComplete((value,failure) -> { phase.set(2); if (failure==null) result.complete(value); else result.completeExceptionally(failure); }); } catch (Throwable failure) { phase.set(2); result.completeExceptionally(failure); } }
            public void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) result.completeExceptionally(failure); }
        }
        var task=new Segment(); try { (control ? scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,RequestContext.NO_TIMEOUT) : scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION)).execute(task,65536); } catch (RuntimeException failure) { task.reject(failure); } return result;
    }
    private synchronized CompletableFuture<RatisExportClient.Job> open(RatisExportRequests.Entry saved,Deadline deadline) throws IOException {
        validate(saved); var current=jobs.get(saved.job()); if (current!=null) return current.completion().thenCompose(snapshot -> io(() -> {
            if (!same(saved.definition(columns(saved.request())),snapshot.definition())) throw new IOException("Original export definition changed");
            synchronized (RatisExportBackend.this) { requests.initialized(saved); verified.add(saved.job()); }
            return CompletableFuture.completedFuture(current);
        }));
        var pending=opening.get(saved.job()); if (pending!=null) return pending.result();
        if (jobs.size()+opening.size()>=limits.activeJobs()) throw new IOException("Export backend job capacity exhausted");
        var actual=new CompletableFuture<RatisExportClient.Job>(); opening.put(saved.job(),new Opening(saved.request(),actual));
        CompletableFuture<RatisExportRequests.Entry> bound;
        try {
            if (saved.fence()!=null) bound=CompletableFuture.completedFuture(saved);
            else {
                if (source==null || allocations==null) throw new IOException("Export allocation source unavailable");
                var ids=columns(saved.request()); var scope=new TransactionScope(ids.stream().map(id -> TransactionScope.Span.range(id,null,null)).toList());
                bound=allocations.allocate(deadline.remaining()).thenCompose(allocation -> io(() -> {
                    var plan=source.plan(allocation.identity().snapshot(),scope);
                    var definition=saved.bind(plan.fence(),allocation.snapshot()); validate(definition); definition.definition(ids);
                    synchronized (RatisExportBackend.this) { var entry=requests.bind(saved,plan.fence(),allocation.snapshot()); return CompletableFuture.completedFuture(entry); }
                }));
            }
        } catch (Throwable failure) { bound=CompletableFuture.failedFuture(failure); }
        bound.thenCompose(entry -> io(() -> {
            validate(entry); deadline.remaining(); Path directory=root.resolve(entry.job().toString()); Files.createDirectories(directory); if (!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Invalid export job directory");
            SessionJournalFiles.force(root);
            Path journal=directory.resolve("export.log"); var definition=entry.definition(columns(entry.request()));
            if (entry.initialized() && !Files.exists(journal,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Original initialized export journal is missing");
            var job=Files.exists(journal,LinkOption.NOFOLLOW_LINKS) ? exports.reopen(journal,deadline.remaining()) : exports.create(journal,definition,deadline.remaining());
            synchronized (RatisExportBackend.this) { jobs.put(entry.job(),job); }
            return job.completion().thenCompose(snapshot -> io(() -> {
                if (!same(definition,snapshot.definition())) throw new IOException("Original export definition changed");
                synchronized (RatisExportBackend.this) { requests.initialized(entry); verified.add(entry.job()); }
                return CompletableFuture.completedFuture(job);
            }));
        })).whenComplete((job,failure) -> { synchronized (RatisExportBackend.this) { opening.remove(saved.job()); } if (failure==null) actual.complete(job); else actual.completeExceptionally(failure); });
        return actual;
    }
    private static boolean same(RatisExportJournal.Definition a,RatisExportJournal.Definition b) { return a.jobId().equals(b.jobId()) && a.fence().equals(b.fence()) && a.columns().equals(b.columns()) && a.sourceJournal().equals(b.sourceJournal()) && Arrays.equals(a.sourceIdentity(),b.sourceIdentity()) && a.limits().equals(b.limits()) && a.snapshotTtl().equals(b.snapshotTtl()); }
    private synchronized CompletableFuture<RatisExportClient.Job> job(UUID id,Deadline deadline) throws IOException { return open(requests.job(Objects.requireNonNull(id)),deadline); }
    private synchronized ExportJobs.Status status(RatisExportClient.Job job) throws IOException {
        var s=job.summary(); var entry=requests.job(s.definition().jobId());
        return new ExportJobs.Status(entry.job(),ExportJobs.Phase.valueOf(s.phase().name()),entry.request().columns(),s.definition().groups().size(),s.chunks(),s.acknowledged(),s.fileBytes(),s.cleanupPending());
    }
    @Override public CompletableFuture<ExportJobs.Status> createExport(ExportJobs.Create request,RequestContext context) {
        return submit(context,deadline -> {
            Objects.requireNonNull(request); columns(request); RatisExportRequests.Entry entry;
            synchronized (this) { entry=requests.nonce(request.requestId()); if (entry!=null && !entry.request().equals(request)) throw new IllegalArgumentException("Export nonce payload changed"); if (entry==null) { if (source==null || allocations==null) throw new IOException("Export source unavailable"); entry=requests.pending(request,schema.digest(),source.identityDigest()); } }
            return open(entry,deadline).thenCompose(job -> io(() -> CompletableFuture.completedFuture(status(job))));
        });
    }
    @Override public CompletableFuture<ExportJobs.Status> recoverExport(UUID id,RequestContext context) { return exportStatus(id,context); }
    @Override public CompletableFuture<ExportJobs.Status> exportStatus(UUID id,RequestContext context) { return submit(context,deadline -> job(id,deadline).thenCompose(job -> io(() -> CompletableFuture.completedFuture(status(job))))); }
    @Override public CompletableFuture<ExportJobs.Status> stepExport(UUID id,RequestContext context) { return submit(context,deadline -> job(id,deadline).thenCompose(job -> job.step(deadline.remaining()).thenCompose(ignored -> io(() -> CompletableFuture.completedFuture(status(job)))))); }
    private ExportJobs.ChunkId id(RatisExportJournal.Definition definition,RatisExportJournal.Chunk chunk) {
        var participant=definition.fence().participant(chunk.group());
        return new ExportJobs.ChunkId(chunk.jobId(),chunk.group(),participant.rangeId(),chunk.rangeEpoch(),chunk.cut().term(),chunk.cut().index(),chunk.ordinal(),chunk.sha256());
    }
    private ExportJobs.Chunk exposed(RatisExportJournal.Definition definition,RatisExportJournal.Chunk chunk) { return new ExportJobs.Chunk(id(definition,chunk),schema.column(chunk.column()).apiId(),chunk.lowerInclusive(),chunk.upperExclusive(),chunk.rows(),chunk.payloadBytes(),chunk.fileBytes(),chunk.hasMore()); }
    private RatisExportJournal.Chunk qualified(RatisExportClient.Job job,ExportJobs.ChunkId wanted) throws IOException {
        var chunk=job.chunk(wanted.groupId(),wanted.ordinal());
        if (!id(job.summary().definition(),chunk).equals(wanted)) throw new IllegalArgumentException("Unknown original export chunk identity");
        return chunk;
    }
    @FunctionalInterface private interface JobWork<T> { CompletableFuture<T> run(RatisExportClient.Job job,Deadline deadline) throws Exception; }
    /** One reserved 64KiB control slot per already-qualified owned job, independent of bulk admission. */
    private <T> CompletableFuture<T> protectedJob(UUID id,Object key,RequestContext context,JobWork<T> work) {
        final RatisExportClient.Job known; final Control<T> task; final Deadline deadline;
        try {
            Objects.requireNonNull(context); Objects.requireNonNull(id);
            synchronized (this) {
                if (closed || closing) throw new IOException("Export backend closed");
                known=verified.contains(id) ? jobs.get(id) : null;
                if (known==null) return submit(context,coldDeadline -> job(id,coldDeadline).thenCompose(job -> { try { return work.run(job,coldDeadline); } catch (Exception failure) { return CompletableFuture.failedFuture(failure); } }));
                deadline=new Deadline(context);
                var previous=controls.get(id);
                if (previous!=null) {
                    if (!previous.key.equals(key)) throw new IOException("Export protected operation already active");
                    @SuppressWarnings("unchecked") var same=(Control<T>)previous; return same.view();
                }
                if (controls.size()>=limits.activeJobs()) throw new RejectedExecutionException("Export protected capacity exhausted");
                task=new Control<>(key); controls.put(id,task);
            }
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        io(true,() -> { deadline.remaining(); return work.run(known,deadline); }).whenComplete((value,failure) -> {
            synchronized (this) { controls.remove(id,task); }
            if (failure==null) try { deadline.remaining(); } catch (Throwable expired) { failure=expired; }
            if (failure==null) task.result.complete(value); else task.result.completeExceptionally(failure);
        });
        return task.view();
    }
    @Override public CompletableFuture<ExportJobs.ManifestPage> exportManifest(ExportJobs.ManifestRequest request,RequestContext context) {
        return submit(context,deadline -> job(request.jobId(),deadline).thenCompose(job -> job.manifest(request.offset(),request.maximumItems(),request.maximumBytes(),deadline.remaining()).thenCompose(page -> io(() -> { var definition=job.summary().definition(); return CompletableFuture.completedFuture(new ExportJobs.ManifestPage(page.chunks().stream().map(chunk -> exposed(definition,chunk)).toList(),page.next(),page.hasMore())); }))));
    }
    @Override public CompletableFuture<ExportJobs.FilePage> readExport(ExportJobs.ReadRequest request,RequestContext context) { return submit(context,deadline -> job(request.id().jobId(),deadline).thenCompose(job -> io(() -> job.read(qualified(job,request.id()),request.offset(),request.maximumBytes(),deadline.remaining())).thenApply(page -> new ExportJobs.FilePage(request.id(),page.offset(),page.bytes(),page.hasMore())))); }
    @Override public CompletableFuture<Void> acknowledgeExport(ExportJobs.ChunkId id,RequestContext context) { return protectedJob(id.jobId(),id,context,(job,deadline) -> job.acknowledge(qualified(job,id),deadline.remaining())); }
    private CompletableFuture<Void> retire(UUID id,RatisExportClient.Job job) { return io(true,() -> { synchronized (this) { job.close(); jobs.remove(id,job); verified.remove(id); } return CompletableFuture.completedFuture(null); }); }
    @Override public CompletableFuture<Void> releaseExport(UUID id,RequestContext context) { return protectedJob(id,"release",context,(job,deadline) -> job.release(deadline.remaining()).thenCompose(ignored -> job.reclaim(deadline.remaining())).thenCompose(ignored -> retire(id,job))); }
    @Override public CompletableFuture<Void> cancelExport(UUID id,RequestContext context) { return protectedJob(id,"cancel",context,(job,deadline) -> job.cancel(deadline.remaining()).thenCompose(ignored -> retire(id,job))); }
    @Override public synchronized void close() throws IOException {
        if (closed) return; if (active!=0 || !opening.isEmpty() || !controls.isEmpty()) throw new IOException("Export actual work active; ownership retained"); closing=true;
        try { for (var iterator=jobs.values().iterator();iterator.hasNext();) { iterator.next().close(); iterator.remove(); } lock.release(); ownership.close(); closed=true; }
        finally { closing=false; }
    }
}
