package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import org.reactivestreams.*;

/** One exclusive bulk session and bounded copied input. Group chunks are independently atomic, never a global prefix. */
public final class RatisRocksDBBatches implements AutoCloseable {
    public static final int MAX_INPUT_ITEMS=4096,MAX_INPUT_BYTES=8*1024*1024,MAX_CHUNK_ITEMS=256,MAX_CHUNK_BYTES=2*1024*1024;
    public enum SessionOwnership { OWNED,BORROWED }
    public enum RecoveryState { NONE,ACTIVE,PENDING,COMPLETE }
    public record Progress(UUID requestId,long copiedBatches,long acknowledgedChunks,long acknowledgedMutations,boolean sourceComplete,int retainedChunks,CommandResult.Status failure) {}
    private final DistributedSchema schema; private final RangeDirectory directory; private final RatisRoutedClient routed; private final RatisDurableSessionClient session; private final RWScheduler scheduler; private final SessionOwnership ownership;
    private final RatisBatchRequestJournal journal; private final ReentrantLock persistence=new ReentrantLock();
    private final Map<CompletableFuture<?>,CompletableFuture<Void>> completions=new IdentityHashMap<>();
    private Task active; private boolean closed,published;
    private ScheduledThreadPoolExecutor deadlineTimer;
    private volatile Progress retainedProgress;
    private volatile Runnable beforeAcknowledgementMarker;
    public RatisRocksDBBatches(DistributedSchema schema,RatisRoutedClient routed,RatisDurableSessionClient dedicatedSession,RWScheduler scheduler,Path path,SessionOwnership ownership) throws IOException {
        this.schema=Objects.requireNonNull(schema); this.routed=Objects.requireNonNull(routed); session=Objects.requireNonNull(dedicatedSession); this.scheduler=Objects.requireNonNull(scheduler); this.ownership=Objects.requireNonNull(ownership); routed.requireKnownSchema(schema); directory=routed.directory(schema);
        byte[] binding; var grant=session.grant();
        try { var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(schema.digest()); digest.update(routed.sessionIdentityDigest()); digest.update(ByteBuffer.allocate(40).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.epoch()).putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).array()); binding=digest.digest(); }
        catch(java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        journal=new RatisBatchRequestJournal(path,binding);
        try { if(journal.request()==null && !session.pristine() || session.pending().stream().filter(p -> !p.consumed()).count()>1) throw new IOException("Bulk session must be exclusively bound to its own retained input"); }
        catch(IOException|RuntimeException|Error failure) { try { journal.close(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
        refreshProgress();
    }
    boolean binds(DistributedSchema schema,RatisRoutedClient routed) { return this.routed==routed && Arrays.equals(this.schema.digest(),schema.digest()); }
    boolean binds(DistributedSchema schema,RangeDirectory directory) { return Arrays.equals(this.schema.digest(),schema.digest()) && Arrays.equals(this.directory.payloadDigest(),directory.payloadDigest()); }
    void beforeAcknowledgementMarker(Runnable hook) { beforeAcknowledgementMarker=hook; }
    RatisDurableSessionClient ownedSession() { return session; }
    public CompletableFuture<Void> putBatch(long column,Publisher<KVBatch> source,PutBatchMode mode,RequestContext context) {
        if(mode!=PutBatchMode.WRITE_BATCH) return CompletableFuture.failedFuture(new UnsupportedOperationException("Unsupported distributed PUT batch mode: "+mode));
        try { it.cavallium.rockserver.core.impl.WorkloadAdmission.validateClient(context,new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(column,source,mode)); return voidObserver(start(column,source,false,context)); }
        catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    public CompletableFuture<Void> mergeBatch(long column,Publisher<KVBatch> source,MergeBatchMode mode,RequestContext context) {
        if(mode!=MergeBatchMode.MERGE_WRITE_BATCH) return CompletableFuture.failedFuture(new UnsupportedOperationException("Unsupported distributed MERGE batch mode: "+mode));
        try { it.cavallium.rockserver.core.impl.WorkloadAdmission.validateClient(context,new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(column,source,mode)); return voidObserver(start(column,source,true,context)); }
        catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    private CompletableFuture<Void> voidObserver(CompletableFuture<Progress> pending) {
        var observer=new CompletableFuture<Void>(); var actual=actualCompletion(pending);
        synchronized(this) { completions.put(observer,actual); }
        CompletableFuture.allOf(actual,observer.handle((value,error) -> null)).whenComplete((ignored,error) -> { synchronized(this) { completions.remove(observer); } });
        pending.whenComplete((value,error) -> { if(error==null) observer.complete(null); else observer.completeExceptionally(error); });
        observer.whenComplete((value,error) -> { if(observer.isCancelled()) pending.cancel(false); }); return observer;
    }
    private CompletableFuture<Progress> start(long column,Publisher<KVBatch> source,boolean merge,RequestContext context) {
        try {
            Objects.requireNonNull(source); schema.column(column); var task=reserve(context); task.schedule(() -> {
                task.check(); persistence.lock(); try { if(journal.request()!=null && !published) throw new IOException("Resolve retained batch progress before subscribing another source"); journal.begin(merge,directory); refreshProgress(); published=false; } finally { persistence.unlock(); }
                task.column=column; task.merge=merge; source.subscribe(task);
            }); return task.view;
        } catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    public CompletableFuture<Progress> resumeRetained(RequestContext context) {
        try { it.cavallium.rockserver.core.impl.WorkloadAdmission.validateClient(context,new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(0,ignored -> {},PutBatchMode.WRITE_BATCH)); var task=reserve(context); task.schedule(() -> { Progress value; persistence.lock(); try { if(journal.request()==null || published) throw new IOException("No retained bulk input"); drain(task); value=snapshot(); } finally { persistence.unlock(); } task.settle(value,null); }); return task.view; }
        catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    public Progress progress() throws IOException { return retainedProgress; }
    private void refreshProgress() throws IOException { retainedProgress=snapshot(); }
    private Progress snapshot() throws IOException { var r=journal.request(); return r==null ? null : new Progress(r.id,r.copiedBatches,r.acknowledgedChunks,r.acknowledgedMutations,r.complete(),r.chunks.size()-r.cursor,r.failure==null ? null : r.failure.status()); }
    public RecoveryState recoveryState() throws IOException {
        synchronized(this) { if(active!=null) return RecoveryState.ACTIVE; }
        persistence.lock(); try { var r=journal.request(); return r==null || published ? RecoveryState.NONE : r.complete() || r.failure!=null ? RecoveryState.COMPLETE : RecoveryState.PENDING; } finally { persistence.unlock(); }
    }
    public synchronized CompletableFuture<Void> actualCompletion(CompletableFuture<?> observer) { var actual=completions.get(observer); if(actual!=null) return actual.copy(); if(observer.isDone()) return CompletableFuture.completedFuture(null); throw new IllegalArgumentException("Unknown bulk observer"); }
    private Task reserve(RequestContext context) {
        long deadline=scheduler.resolveMonotonicDeadline(context);
        synchronized(this) { if(closed || active!=null) throw new IllegalStateException("Bulk actor already owns work or is closed"); var task=new Task(context,deadline); active=task; completions.put(task.view,task.drained); if(deadline!=Long.MAX_VALUE) {
            if(deadlineTimer==null) { deadlineTimer=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon(true).name("ratis-bulk-deadlines").factory()); deadlineTimer.setRemoveOnCancelPolicy(true); }
            task.armDeadline();
        } return task; }
    }
    private List<DistributedCommand> copy(KVBatch batch,long column,boolean merge) {
        var keys=Objects.requireNonNull(batch.keys()); var values=Objects.requireNonNull(batch.values());
        if(keys.size()!=values.size() || keys.size()>MAX_INPUT_ITEMS) throw new IllegalArgumentException("Invalid copied KVBatch counts");
        var grouped=new LinkedHashMap<UUID,List<Mutation>>(); long copied=0; var grant=session.grant();
        for(int i=0;i<keys.size();i++) {
            var change=merge ? RatisLogicalClient.Change.merge(column,keys.get(i),values.get(i),Mutation.ReturnMode.NONE) : RatisLogicalClient.Change.put(column,keys.get(i),values.get(i),Mutation.ReturnMode.NONE);
            var plan=RatisLogicalClient.plan(schema,List.of(change),List.of()); var mutation=plan.mutations().getFirst(); var route=directory.route(mutation.columnId(),mutation.key());
            var single=new DistributedCommand(route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new OperationId(grant.sessionId(),grant.epoch(),0),List.of(mutation));
            int bytes=DistributedCommandCodec.encodedSize(single); if(bytes>MAX_CHUNK_BYTES) throw new IllegalArgumentException("One batch mutation exceeds native chunk bound"); copied+=bytes-DistributedCommandCodec.HEADER_BYTES; if(copied>MAX_INPUT_BYTES) throw new IllegalArgumentException("Copied KVBatch exceeds input bound");
            grouped.computeIfAbsent(route.groupId(),ignored -> new ArrayList<>()).add(mutation);
        }
        var chunks=new ArrayList<DistributedCommand>(); long encoded=0;
        for(var mutations:grouped.values()) {
            var pending=new ArrayList<Mutation>(); int bytes=DistributedCommandCodec.HEADER_BYTES;
            for(var mutation:mutations) {
                var route=directory.route(mutation.columnId(),mutation.key()); int row=DistributedCommandCodec.encodedSize(new DistributedCommand(route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new OperationId(grant.sessionId(),grant.epoch(),0),List.of(mutation)))-DistributedCommandCodec.HEADER_BYTES;
                if(!pending.isEmpty() && (pending.size()==MAX_CHUNK_ITEMS || bytes+row>MAX_CHUNK_BYTES)) { chunks.add(template(pending)); encoded+=bytes; pending=new ArrayList<>(); bytes=DistributedCommandCodec.HEADER_BYTES; }
                pending.add(mutation); bytes+=row;
            }
            if(!pending.isEmpty()) { chunks.add(template(pending)); encoded+=bytes; }
        }
        if(encoded>MAX_INPUT_BYTES) throw new IllegalArgumentException("Complete encoded copied KVBatch exceeds input bound"); return List.copyOf(chunks);
    }
    private DistributedCommand template(List<Mutation> mutations) { var first=mutations.getFirst(); var route=directory.route(first.columnId(),first.key()); var grant=session.grant(); return new DistributedCommand(route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new OperationId(grant.sessionId(),grant.epoch(),0),mutations); }
    private RatisDurableSessionClient.Pending find(OperationId id) throws IOException { if(id==null) return null; for(var pending:session.pending()) if(pending.command().operationId().equals(id)) return pending; return null; }
    private static void require(DistributedCommand expected,RatisDurableSessionClient.Pending pending) throws IOException {
        var actual=pending.command(); if(actual.kind()!=DistributedCommand.Kind.DATA || pending.awaitingAcceptance() || !expected.groupId().equals(actual.groupId()) || expected.rangeEpoch()!=actual.rangeEpoch() || !Arrays.equals(expected.schemaDigest(),actual.schemaDigest()) || !expected.mutations().equals(actual.mutations()) || !actual.predicates().isEmpty()) throw new IOException("Original pending batch command differs");
    }
    private void drain(Task task) throws Exception {
        var r=journal.request();
        while(!r.chunks.isEmpty() && r.failure==null) {
            var expected=r.chunks.get(r.cursor); var pending=find(r.pending);
            if(r.result!=null) {
                if(pending!=null) { require(expected,pending); if(pending.savedResult()==null || !Arrays.equals(pending.savedResult().encode(),r.result.encode())) throw new IOException("Bulk saved receipt disagrees with session"); session.acknowledge(pending,task.context,task::check); }
                else if(session.acknowledgedThrough()<r.pending.sequence()) throw new IOException("Bulk result lacks original durable ACK");
                session.recoverReroutes(task.context,task::check,false);journal.acknowledged(); refreshProgress(); continue;
            }
            task.check();
            if(pending==null) {
                if(r.pending!=null) throw new IOException("Original bulk pending identity disappeared");
                var unresolved=session.pending().stream().filter(p -> !p.consumed()).toList(); if(unresolved.size()>1) throw new IOException("Bulk session lost exclusive ownership");
                pending=unresolved.isEmpty() ? session.prepareAt(r.directory,expected.mutations(),List.of()) : unresolved.getFirst(); require(expected,pending); journal.bind(pending.command().operationId());
            }
            require(expected,pending); var result=session.execute(pending,task.context,task::check); journal.result(result); session.acknowledge(pending,task.context,task::check); var hook=beforeAcknowledgementMarker; if(hook!=null) hook.run(); journal.acknowledged(); refreshProgress(); task.check();
        }
    }
    @FunctionalInterface private interface Work { void run() throws Exception; }
    private final class Task implements Subscriber<KVBatch> {
        final RequestContext context; final long deadline; final CompletableFuture<Progress> result=new CompletableFuture<>(),view=result.copy(); final CompletableFuture<Void> drained=new CompletableFuture<>();
        final ArrayDeque<Work> queue=new ArrayDeque<>(); Subscription subscription; ScheduledFuture<?> deadlineHook; boolean requested,inFlight,running,stop,eof,settled; Throwable sourceFailure,outcomeFailure; Progress outcome; long column; boolean merge;
        Task(RequestContext context,long deadline) { this.context=context; this.deadline=deadline; view.whenComplete((value,error) -> { if(view.isCancelled()) cancel(); }); }
        void armDeadline() {
            var hook=deadlineTimer.schedule(this::expired,Math.max(0,scheduler.remainingMonotonicDeadlineNanos(deadline)),TimeUnit.NANOSECONDS);
            synchronized(this) { if(settled) hook.cancel(false); else deadlineHook=hook; }
        }
        void expired() {
            synchronized(this) { if(settled) return; }
            if(!scheduler.isMonotonicDeadlineExpired(deadline)) { armDeadline(); return; }
            var timeout=new IOException("Bulk deadline expired; copied input retained",new TimeoutException()); Subscription source; boolean idle;
            synchronized(this) { if(settled) return; stop=true; source=subscription; idle=!running && !inFlight; }
            if(source!=null) try { source.cancel(); } catch(RuntimeException failure) { timeout.addSuppressed(failure); }
            view.completeExceptionally(timeout);
            synchronized(this) { idle=!running && !inFlight; }
            if(idle) { settle(null,timeout); synchronized(this) { idle=!running && !inFlight; } if(idle) publish(); }
        }
        void check() throws IOException { synchronized(this) { if(stop) throw new IOException("Bulk observer stopped; copied input retained"); if(sourceFailure!=null) throw new IOException("Publisher failed; copied input retained",sourceFailure); } if(deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Bulk deadline expired; copied input retained"); }
        void cancel() { Subscription source; boolean idle; synchronized(this) { stop=true; source=subscription; idle=!inFlight && !running; } if(source!=null) source.cancel(); if(idle) schedule(() -> { throw new IOException("Bulk observer canceled"); }); }
        void schedule(Work work) { synchronized(this) { if(settled) return; if(running) { if(queue.size()>=1) throw new IllegalStateException("Bulk callback queue overflow"); queue.add(work); return; } running=true; } dispatch(work); }
        void dispatch(Work work) {
            class Job implements Runnable,RWScheduler.RejectionAwareTask {
                boolean used;
                public void run() { synchronized(this) { if(used) return; used=true; } try { work.run(); } catch(Throwable error) { settle(null,error); } finally { next(); } }
                public void reject(RuntimeException error) { synchronized(this) { if(used) return; used=true; } settle(null,error); next(); }
            }
            var job=new Job();
            try { scheduler.executor(context.profile(),OperationFamily.MUTATION,deadline).execute(job,Math.min(48L*1024*1024,WorkloadCost.MAX_ESTIMATED_BYTES)); } catch(RuntimeException error) { job.reject(error); }
        }
        void next() { Work next; boolean finish; synchronized(this) { next=settled ? null : queue.poll(); if(next==null) running=false; finish=settled && !running; } if(next!=null) dispatch(next); else if(finish) publish(); }
        void settle(Progress value,Throwable error) { Subscription source; ScheduledFuture<?> hook; synchronized(this) { if(settled) return; settled=true; outcome=value; outcomeFailure=error; queue.clear(); source=subscription; hook=deadlineHook; deadlineHook=null; } if(hook!=null) hook.cancel(false); if(source!=null) try { source.cancel(); } catch(RuntimeException cleanup) { synchronized(this) { if(outcomeFailure==null) outcomeFailure=cleanup; else outcomeFailure.addSuppressed(cleanup); } } }
        void publish() {
            synchronized(RatisRocksDBBatches.this) { if(active!=this) return; if(outcomeFailure==null && !view.isCancelled()) published=true; active=null; }
            drained.complete(null); if(outcomeFailure==null) result.complete(outcome); else result.completeExceptionally(outcomeFailure);
            synchronized(RatisRocksDBBatches.this) { completions.remove(view); }
        }
        void requestNext() { Subscription source; synchronized(this) { if(stop || settled || eof || sourceFailure!=null) return; requested=true; source=subscription; } source.request(1); }
        @Override public void onSubscribe(Subscription source) { boolean reject; synchronized(this) { reject=subscription!=null || stop || settled; if(!reject) subscription=source; } if(reject) source.cancel(); else requestNext(); }
        @Override public void onNext(KVBatch batch) {
            boolean invalid; synchronized(this) { invalid=!requested || inFlight || eof || stop || settled; if(!invalid) { requested=false; inFlight=true; } }
            if(invalid) { onError(new IllegalStateException("Publisher exceeded demand one")); return; }
            try { var chunks=copy(Objects.requireNonNull(batch),column,merge); schedule(() -> {
                persistence.lock(); try { boolean end; synchronized(this) { end=eof; } journal.input(chunks,end); refreshProgress(); drain(this); synchronized(this) { inFlight=false; } } finally { persistence.unlock(); }
                finishSource();
            }); } catch(Throwable error) { onError(error); synchronized(this) { inFlight=false; } schedule(() -> { throw new IOException("Invalid copied batch",error); }); }
        }
        void finishSource() throws Exception {
            boolean end; Throwable error; synchronized(this) { end=eof; error=sourceFailure; }
            if(error!=null) throw new IOException("Publisher failed; acknowledged group chunks retained",error);
            check(); Progress complete=null;
            persistence.lock(); try {
                if(journal.request().failure!=null) throw new IOException("Native bulk chunk retained terminal outcome: "+journal.request().failure.status());
                if(end) { journal.eof(); refreshProgress(); complete=snapshot(); }
            } finally { persistence.unlock(); }
            if(end) settle(complete,null); else requestNext();
        }
        @Override public void onError(Throwable error) { boolean idle; synchronized(this) { if(settled) return; sourceFailure=Objects.requireNonNull(error); idle=!inFlight; } if(idle) schedule(() -> { throw new IOException("Publisher failed",error); }); }
        @Override public void onComplete() { boolean idle; synchronized(this) { if(settled || sourceFailure!=null) return; eof=true; idle=!inFlight; } if(idle) schedule(this::finishSource); }
    }
    @Override public void close() throws IOException { synchronized(this) { if(active!=null) throw new IOException("Bulk work active; native/session ownership retained"); closed=true; } journal.close(); if(ownership==SessionOwnership.OWNED) session.close(); if(deadlineTimer!=null) deadlineTimer.shutdown(); }
}
