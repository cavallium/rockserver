package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

/** One exclusive bulk session and bounded copied input. Group chunks are independently atomic, never a global prefix. */
public final class RatisRocksDBBatches implements AutoCloseable {
    public static final int MAX_INPUT_ITEMS=4096,MAX_INPUT_BYTES=8*1024*1024,MAX_CHUNK_ITEMS=256,MAX_CHUNK_BYTES=2*1024*1024;
    public enum SessionOwnership { OWNED,BORROWED }
    public enum RecoveryState { NONE,ACTIVE,PENDING,COMPLETE }
    public record Progress(UUID requestId,long copiedBatches,long acknowledgedChunks,long acknowledgedMutations,boolean sourceComplete,int retainedChunks,CommandResult.Status failure) {}
    private final DistributedSchema schema; private final RangeDirectory directory; private final RatisRoutedClient routed; private final RatisDurableSessionClient session; private final RWScheduler scheduler; private final SessionOwnership ownership;
    private final RatisRequestBudget budget;private final RatisRequestBudget.Owner ownerCredit;private RatisRequestBudget.Lease requestCredit;
    private final RatisRemoteMutationReceipts receipts; private final RatisBatchRequestJournal journal; private final ReentrantLock persistence=new ReentrantLock();
    private final Map<CompletableFuture<?>,CompletableFuture<Void>> completions=new IdentityHashMap<>();
    private Task active; private boolean closed;
    private ScheduledThreadPoolExecutor deadlineTimer;
    private volatile Progress retainedProgress;
    private volatile Runnable beforeAcknowledgementMarker;
    public RatisRocksDBBatches(DistributedSchema schema,RangeDirectory frozenDirectory,RatisRoutedClient routed,RatisDurableSessionClient dedicatedSession,RWScheduler scheduler,Path path,SessionOwnership ownership,RatisRequestBudget budget,RatisRequestBudget.Owner ownerCredit) throws IOException {
        this.budget=Objects.requireNonNull(budget);this.ownerCredit=Objects.requireNonNull(ownerCredit);if(ownerCredit.kind!=RemoteMutationToken.Kind.BULK)throw new IllegalArgumentException("BULK owner credit required");
        this.schema=Objects.requireNonNull(schema); this.routed=Objects.requireNonNull(routed); session=Objects.requireNonNull(dedicatedSession); this.scheduler=Objects.requireNonNull(scheduler); this.ownership=Objects.requireNonNull(ownership); routed.requireKnownSchema(schema); directory=Objects.requireNonNull(frozenDirectory);schema.requireMatches(directory);
        byte[] binding; var grant=session.grant();
        try { var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(schema.digest()); digest.update(routed.sessionIdentityDigest()); digest.update(ByteBuffer.allocate(40).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.epoch()).putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).array()); binding=digest.digest(); }
        catch(java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        ownerCredit.requireOpening(RatisRequestBudget.files(path,path.resolveSibling(path.getFileName()+".pending")));var disk=budget.disk(path,RatisBatchRequestJournal.MAX_BYTES);
        boolean usedOwner=Files.exists(path.resolveSibling(path.getFileName()+".lock"));try{journal=new RatisBatchRequestJournal(path,binding,budget,disk);}catch(IOException|RuntimeException|Error failure){disk.close();throw failure;}
        try { receipts=new RatisRemoteMutationReceipts(path.resolveSibling(path.getFileName()+".released"),authority(0),usedOwner || journal.request()!=null || !session.pristine(),budget);receipts.validateJournal(journal.request()==null?null:journal.request().token);if(journal.request()!=null)journal.request().resultAcknowledged=receipts.retired(journal.request().token); if(journal.request()==null && (!session.pristine() && !receipts.established() || session.pending().stream().anyMatch(p -> !p.consumed())) || session.pending().stream().filter(p -> !p.consumed()).count()>1) throw new IOException("Bulk session must be exclusively bound to its own retained input");
            if(journal.request()!=null && journal.request().resultAcknowledged){
                long cleanup=Math.addExact(RatisRequestBudget.bulkRetained(journal.request()),6*journal.encodedSize());
                try(var opening=ownerCredit.openedWorkspace(cleanup);var scratch=journal.workspace(opening)){journal.retire();}
            }else requestCredit=ownerCredit.opened(journal.request()==null?0:RatisRequestBudget.bulkRetained(journal.request()),journal.request()!=null);
        }
        catch(IOException|RuntimeException|Error failure) { try { journal.close(); } catch(IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
        refreshProgress();
    }
    boolean binds(DistributedSchema schema,RatisRoutedClient routed) { return this.routed==routed && Arrays.equals(this.schema.digest(),schema.digest()); }
    boolean binds(DistributedSchema schema,RangeDirectory directory) { return Arrays.equals(this.schema.digest(),schema.digest()) && Arrays.equals(this.directory.payloadDigest(),directory.payloadDigest()); }
    void beforeAcknowledgementMarker(Runnable hook) { beforeAcknowledgementMarker=hook; }
    RatisDurableSessionClient ownedSession() { return session; }
    public CompletableFuture<Progress> resumeRetained(RemoteMutationToken token,RequestContext context) {
        try { it.cavallium.rockserver.core.impl.WorkloadAdmission.validateClient(context,new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(0,ignored -> {},PutBatchMode.WRITE_BATCH)); var task=reserve(context,false); task.schedule(() -> { Progress value; persistence.lock(); try { if(exact(token).resultAcknowledged) throw new IOException("No retained bulk input"); drain(task); value=snapshot(); } finally { persistence.unlock(); } task.settle(value,null); }); return task.view; }
        catch(Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    public RemoteMutationOwner owner() throws IOException {return authority(receipts.nextSequence());}
    private RemoteMutationOwner authority(long nextSequence) {var grant=session.grant();return new RemoteMutationOwner(schema.clusterId(),schema.databaseId(),grant.sessionId(),grant.epoch(),nextSequence,RemoteMutationToken.Kind.BULK,List.of(DomainBinding.of(directory)));}
    public RemoteMutationToken prepare(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,boolean merge) throws IOException {
        if(!owner().sameAuthority(expectedOwner))throw new IOException("Bulk owner differs from installed durable session"); schema.column(column); var token=new RemoteMutationToken(schema.clusterId(),schema.databaseId(),expectedOwner.ownerId(),requestId,expectedOwner.ownerEpoch(),requestSequence,RemoteMutationToken.Kind.BULK,List.of(DomainBinding.of(directory)),RemoteMutationInputs.bulkHeader(column,merge));
        synchronized(this) { if(active!=null || closed)throw new IOException("Bulk actor busy or closed"); persistence.lock(); try { receipts.requireNew(token); var retained=journal.request(); if(retained!=null && !retained.resultAcknowledged) { if(!token.equals(retained.token))throw new IOException("Original bulk owner or input differs"); return token; } if(requestCredit==null)requestCredit=budget.request(RemoteMutationToken.Kind.BULK,RatisRequestBudget.bulkBase(directory));journal.begin(token,column,merge,directory); receipts.bind(token); refreshProgress();return token; } finally { persistence.unlock(); } }
    }
    private RatisBatchRequestJournal.Request exact(RemoteMutationToken token) throws IOException { var r=journal.request(); if(receipts.retired(token))throw new IOException("Request sequence expired or acknowledged"); if(r==null || !token.equals(r.token))throw new IOException("Original bulk token unavailable or differs"); return r; }
    private static void validateMutationProfile(RequestContext context) {it.cavallium.rockserver.core.impl.WorkloadAdmission.validateClient(context,new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(0,ignored -> {},PutBatchMode.WRITE_BATCH));}
    public CompletableFuture<Progress> append(RemoteMutationToken token,long ordinal,KVBatch batch,byte[] inputDigest,RequestContext context) {
        final Task task; try { validateMutationProfile(context); task=reserve(context,true); } catch(Exception failure) {return CompletableFuture.failedFuture(failure);}
        final KVBatch frozen;final byte[] actual;final boolean retained;
        try{
            if(inputDigest==null || inputDigest.length!=32)throw new IOException("Invalid input digest");
            if(!persistence.tryLock())throw new IOException("Bulk copied input busy");
            try{
                task.check();var r=exact(token);if(r.resultAcknowledged)throw new IOException("Bulk result already acknowledged");if(ordinal<0)throw new IOException("Negative input ordinal");if(ordinal<=r.retiredInputThrough)throw new RatisBulkInputExpiredException(ordinal,r.retiredInputThrough);
                retained=r.inputDigests.containsKey(ordinal);if(retained && !Arrays.equals(r.inputDigests.get(ordinal),inputDigest))throw new IOException("Copied ordinal input changed");
                if(!retained && (ordinal!=r.copiedBatches || journal.unresolvedInput() || r.eof))throw new IOException("Resolve previous copied batch before append");
                long encoded=journal.encodedSize();task.workCredit.growTo(Math.max(task.workCredit.amount,128L*1024+6*encoded));var captured=capture(batch,r.column,task);
                task.workCredit.growTo(Math.max(task.workCredit.amount,RatisRequestBudget.bulkInput(schema,r.column,captured.keySizes(),captured.valueSizes(),encoded)));
                frozen=freeze(captured);actual=RemoteMutationInputs.batch(r.column,r.merge,frozen);if(!Arrays.equals(actual,inputDigest))throw new IOException("Copied batch digest differs");task.check();
            }finally{persistence.unlock();}
        }catch(Throwable failure){task.intakeFailed(failure);return task.view;}
        task.intakeReady(()->{persistence.lock();try{
            task.check();var r=exact(token);
            if(retained){journal.retainedInput(ordinal,actual);if(ordinal==r.copiedBatches-1 && journal.unresolvedInput())drain(task);}
            else{var chunks=copy(frozen,r.column,r.merge);long graph=3*chunks.stream().mapToLong(DistributedCommandCodec::encodedSize).sum()+128L*chunks.stream().mapToInt(c->c.mutations().size()).sum();task.workCredit.transferTo(requestCredit,graph);journal.remoteInput(ordinal,actual,chunks);refreshProgress();drain(task);}
            task.settle(snapshot(),null);
        }finally{persistence.unlock();}});return task.view;
    }
    public CompletableFuture<Progress> finishInput(RemoteMutationToken token,RequestContext context) { try { validateMutationProfile(context); var task=reserve(context,false);task.schedule(() -> { persistence.lock();try { var r=exact(token);if(r.resultAcknowledged || !r.chunks.isEmpty() || r.pending!=null)throw new IOException("Bulk copied input remains unresolved");journal.eof();refreshProgress();task.settle(snapshot(),null); } finally { persistence.unlock(); } });return task.view; } catch(Exception failure) {return CompletableFuture.failedFuture(failure);} }
    public RemoteMutationStatus status(RemoteMutationToken token) throws IOException { if(!persistence.tryLock())return new RemoteMutationStatus(token,RemoteMutationStatus.State.PENDING,-1,-1,0,null,null);try {var released=receipts.find(token);if(released!=null)return released;var retained=journal.request();if(retained==null || retained.token==null || !retained.token.requestId().equals(token.requestId()))return new RemoteMutationStatus(token,RemoteMutationStatus.State.UNAVAILABLE,-1,-1,0,null,null);var r=exact(token); var state=r.resultAcknowledged?RemoteMutationStatus.State.ACKNOWLEDGED:r.complete() || r.failure!=null?RemoteMutationStatus.State.TERMINAL:r.copiedBatches==0?RemoteMutationStatus.State.PREPARED:RemoteMutationStatus.State.PENDING;return new RemoteMutationStatus(token,state,r.acknowledgedMutations,r.copiedBatches,0,r.complete() || r.failure!=null?RatisBatchRequestJournal.terminalOutcome(r):null,state==RemoteMutationStatus.State.TERMINAL || state==RemoteMutationStatus.State.ACKNOWLEDGED?RatisBatchRequestJournal.resultDigest(r):null); } finally {persistence.unlock();} }
    public void acknowledgeResult(RemoteMutationToken token,byte[] digest)throws IOException {
        persistence.lock();try{
            synchronized(this){if(active!=null)throw new IOException("Bulk actual work remains active");}
            var released=receipts.find(token);if(released!=null){if(!Arrays.equals(released.resultDigest(),digest))throw new IOException("Released bulk ACK changed");var retained=journal.request();if(retained!=null && token.equals(retained.token))retireRequest();return;}
            var r=exact(token);if(!(r.complete() || r.failure!=null) || !Arrays.equals(RatisBatchRequestJournal.resultDigest(r),digest))throw new IOException("Bulk ACK differs from terminal outcome");
            receipts.acknowledge(new RemoteMutationStatus(token,RemoteMutationStatus.State.ACKNOWLEDGED,r.acknowledgedMutations,r.copiedBatches,0,RatisBatchRequestJournal.terminalOutcome(r),digest));r.resultAcknowledged=true;retireRequest();
        }finally{persistence.unlock();}
    }
    private void retireRequest()throws IOException {journal.retire();if(requestCredit!=null){requestCredit.close();requestCredit=null;}}
    public Progress progress() throws IOException { return retainedProgress; }
    private void refreshProgress() throws IOException { retainedProgress=snapshot(); }
    private Progress snapshot() throws IOException { var r=journal.request(); return r==null ? null : new Progress(r.id,r.copiedBatches,r.acknowledgedChunks,r.acknowledgedMutations,r.complete(),r.chunks.size()-r.cursor,r.failure==null ? null : r.failure.status()); }
    public RecoveryState recoveryState() throws IOException {
        synchronized(this) { if(active!=null) return RecoveryState.ACTIVE; }
        persistence.lock(); try { var r=journal.request(); return r==null || r.resultAcknowledged ? RecoveryState.NONE : r.complete() || r.failure!=null ? RecoveryState.COMPLETE : RecoveryState.PENDING; } finally { persistence.unlock(); }
    }
    public synchronized CompletableFuture<Void> actualCompletion(CompletableFuture<?> observer) { var actual=completions.get(observer); if(actual!=null) return actual.copy(); if(observer.isDone()) return CompletableFuture.completedFuture(null); throw new IllegalArgumentException("Unknown bulk observer"); }
    private void releaseRetained(Task task)throws IOException {if(requestCredit==null)return;long retained=RatisRequestBudget.bulkRetained(journal.request());if(requestCredit.amount>retained)requestCredit.transferTo(task.workCredit,requestCredit.amount-retained);}
    private Task reserve(RequestContext context,boolean intake) {
        long deadline=scheduler.resolveMonotonicDeadline(context);
        synchronized(this) { if(closed || active!=null) throw new IllegalStateException("Bulk actor already owns work or is closed"); var task=new Task(context,deadline,budget.work(64L<<10),intake); active=task; completions.put(task.view,task.drained); if(deadline!=Long.MAX_VALUE) {
            if(deadlineTimer==null) { deadlineTimer=new ScheduledThreadPoolExecutor(1,Thread.ofPlatform().daemon(true).name("ratis-bulk-deadlines").factory()); deadlineTimer.setRemoveOnCancelPolicy(true); }
            task.armDeadline();
        } return task; }
    }
    private record Captured(KVBatch references,int[][] keySizes,int[] valueSizes) {}
    /** Prepaid metadata snapshots exactly which buffers and lengths the payload credit will cover. */
    private Captured capture(KVBatch input,long column,Task task){
        var keys=Objects.requireNonNull(input.keys());var values=Objects.requireNonNull(input.values());int count=keys.size(),components=schema.column(column).schema().keys().size();if(count!=values.size() || count>MAX_INPUT_ITEMS)throw new IllegalArgumentException("Invalid copied input count");
        long metadata=Math.addExact(task.workCredit.amount,128L*count+16L*count*components);task.workCredit.growTo(metadata);
        var selectedKeys=new ArrayList<Keys>(count);var selectedValues=new ArrayList<it.cavallium.buffer.Buf>(count);var sizes=new int[count][];var valueSizes=new int[count];
        for(int n=0;n<count;n++){var source=Objects.requireNonNull(keys.get(n)).keys();if(source.length!=components)throw new IllegalArgumentException("Logical key shape changed");var refs=new it.cavallium.buffer.Buf[components];sizes[n]=new int[components];for(int c=0;c<components;c++){refs[c]=Objects.requireNonNull(source[c]);sizes[n][c]=refs[c].size();}selectedKeys.add(new Keys(refs));var value=values.get(n);selectedValues.add(value);valueSizes[n]=value==null?-1:value.size();}
        return new Captured(new KVBatch.KVBatchRef(List.copyOf(selectedKeys),Collections.unmodifiableList(selectedValues)),sizes,valueSizes);
    }
    private KVBatch freeze(Captured input){
        var keys=input.references().keys();var values=input.references().values();var copiedKeys=new ArrayList<Keys>(keys.size());var copiedValues=new ArrayList<it.cavallium.buffer.Buf>(keys.size());
        for(int n=0;n<keys.size();n++){var source=keys.get(n).keys();var copied=new it.cavallium.buffer.Buf[source.length];for(int c=0;c<source.length;c++)copied[c]=freezeBytes(source[c],input.keySizes()[n][c]);copiedKeys.add(new Keys(copied));var value=values.get(n);copiedValues.add(value==null?null:freezeBytes(value,input.valueSizes()[n]));}
        return new KVBatch.KVBatchRef(List.copyOf(copiedKeys),Collections.unmodifiableList(copiedValues));
    }
    private static it.cavallium.buffer.Buf freezeBytes(it.cavallium.buffer.Buf source,int size){if(source.size()!=size)throw new IllegalArgumentException("Logical component length changed before copy");byte[] bytes=new byte[size];for(int n=0;n<size;n++)bytes[n]=source.getByte(n);if(source.size()!=size)throw new IllegalArgumentException("Logical component length changed during copy");return it.cavallium.buffer.Buf.wrap(bytes);}
    private List<DistributedCommand> copy(KVBatch batch,long column,boolean merge) {
        var keys=Objects.requireNonNull(batch.keys()); var values=Objects.requireNonNull(batch.values());
        if(keys.size()!=values.size() || keys.size()>MAX_INPUT_ITEMS) throw new IllegalArgumentException("Invalid copied KVBatch counts");
        var grouped=new LinkedHashMap<UUID,List<Mutation>>(); long copied=0; var grant=session.grant();
        for(int i=0;i<keys.size();i++) {
            var change=merge ? RatisLogicalClient.Change.merge(column,keys.get(i),values.get(i),Mutation.ReturnMode.NONE) : RatisLogicalClient.Change.put(column,keys.get(i),values.get(i),Mutation.ReturnMode.NONE);
            var plan=RatisLogicalClient.plan(scopeColumnId -> schema,List.of(change),List.of()); var mutation=plan.mutations().getFirst(); var route=directory.route(mutation.columnId(),mutation.key());
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
        var r=journal.request();receipts.requireNew(r.token);
        while(!r.chunks.isEmpty() && r.failure==null) {
            var expected=r.chunks.get(r.cursor); var pending=find(r.pending);
            if(r.result!=null) {
                if(pending!=null) { require(expected,pending); if(pending.savedResult()==null || !Arrays.equals(pending.savedResult().encode(),r.result.encode())) throw new IOException("Bulk saved receipt disagrees with session"); session.acknowledge(pending,task.context,task::check); }
                else if(session.acknowledgedThrough()<r.pending.sequence()) throw new IOException("Bulk result lacks original durable ACK");
                session.recoverReroutes(task.context,task::check,false);journal.acknowledged();releaseRetained(task); refreshProgress(); continue;
            }
            task.check();
            if(pending==null) {
                if(r.pending!=null) throw new IOException("Original bulk pending identity disappeared");
                var unresolved=session.pending().stream().filter(p -> !p.consumed()).toList(); if(unresolved.size()>1) throw new IOException("Bulk session lost exclusive ownership");
                pending=unresolved.isEmpty() ? session.prepareAt(r.directory,expected.mutations(),List.of()) : unresolved.getFirst(); require(expected,pending); journal.bind(pending.command().operationId());
            }
            require(expected,pending); var result=session.execute(pending,task.context,task::check); journal.result(result); session.acknowledge(pending,task.context,task::check);session.recoverReroutes(task.context,task::check,false); var hook=beforeAcknowledgementMarker; if(hook!=null) hook.run(); journal.acknowledged();releaseRetained(task); refreshProgress(); task.check();
        }
    }
    @FunctionalInterface private interface Work { void run() throws Exception; }
    private final class Task {
        final RequestContext context; final long deadline;final RatisRequestBudget.Lease workCredit; final CompletableFuture<Progress> result=new CompletableFuture<>(),view=result.copy(); final CompletableFuture<Void> drained=new CompletableFuture<>();
        final ArrayDeque<Work> queue=new ArrayDeque<>(); ScheduledFuture<?> deadlineHook; boolean running,intaking,stop,settled; Throwable outcomeFailure; Progress outcome;
        Task(RequestContext context,long deadline,RatisRequestBudget.Lease workCredit,boolean intake) {this.workCredit=workCredit;running=intaking=intake; this.context=context; this.deadline=deadline; view.whenComplete((value,error) -> { if(view.isCancelled()) cancel(); }); }
        void intakeReady(Work work){boolean finish;synchronized(this){if(!intaking)throw new IllegalStateException("Input ownership already transferred");intaking=false;finish=settled;if(finish)running=false;}if(finish)publish();else dispatch(work);}
        void intakeFailed(Throwable failure){synchronized(this){intaking=false;running=false;}settle(null,failure);publish();}
        void armDeadline() {
            var hook=deadlineTimer.schedule(this::expired,Math.max(0,scheduler.remainingMonotonicDeadlineNanos(deadline)),TimeUnit.NANOSECONDS);
            synchronized(this) { if(settled) hook.cancel(false); else deadlineHook=hook; }
        }
        void expired() {
            synchronized(this) {if(settled)return;}
            if(!scheduler.isMonotonicDeadlineExpired(deadline)){armDeadline();return;}
            var timeout=new IOException("Bulk deadline expired; copied input retained",new TimeoutException());boolean idle;
            synchronized(this){if(settled)return;stop=true;idle=!running;}
            view.completeExceptionally(timeout);if(idle){settle(null,timeout);synchronized(this){idle=!running;}if(idle)publish();}
        }
        void check() throws IOException {synchronized(this){if(stop)throw new IOException("Bulk observer stopped; copied input retained");}if(deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline))throw new IOException("Bulk deadline expired; copied input retained");}
        void cancel() {boolean idle;synchronized(this){stop=true;idle=!running;}if(idle)schedule(() -> {throw new IOException("Bulk observer canceled");});}
        void schedule(Work work) { synchronized(this) { if(settled) return; if(running) { if(queue.size()>=1) throw new IllegalStateException("Bulk callback queue overflow"); queue.add(work); return; } running=true; } dispatch(work); }
        void dispatch(Work work) {
            class Job implements Runnable,RWScheduler.RejectionAwareTask {
                boolean used;
                public void run() { synchronized(this) { if(used) return; used=true; } try {persistence.lock();try{var retained=journal.request();if(retained!=null)workCredit.growTo(Math.max(workCredit.amount,128L*1024+6*journal.encodedSize()));}finally{persistence.unlock();}try(var scratch=journal.workspace(workCredit)){work.run();}} catch(Throwable error) { settle(null,error); } finally { next(); } }
                public void reject(RuntimeException error) { synchronized(this) { if(used) return; used=true; } settle(null,error); next(); }
            }
            var job=new Job();
            try { scheduler.executor(context.profile(),OperationFamily.MUTATION,deadline).execute(job,Math.min(48L*1024*1024,WorkloadCost.MAX_ESTIMATED_BYTES)); } catch(RuntimeException error) { job.reject(error); }
        }
        void next() { Work next; boolean finish; synchronized(this) { next=settled ? null : queue.poll(); if(next==null) running=false; finish=settled && !running; } if(next!=null) dispatch(next); else if(finish) publish(); }
        void settle(Progress value,Throwable error) {ScheduledFuture<?> hook;synchronized(this){if(settled)return;settled=true;outcome=value;outcomeFailure=error;queue.clear();hook=deadlineHook;deadlineHook=null;}if(hook!=null)hook.cancel(false);}
        void publish() {
            synchronized(RatisRocksDBBatches.this) { if(active!=this) return;workCredit.close();active=null; }
            drained.complete(null); if(outcomeFailure==null) result.complete(outcome); else result.completeExceptionally(outcomeFailure);
            synchronized(RatisRocksDBBatches.this) { completions.remove(view); }
        }
    }
    @Override public void close() throws IOException { synchronized(this) { if(active!=null) throw new IOException("Bulk work active; native/session ownership retained"); closed=true; } journal.close();if(requestCredit!=null){requestCredit.close();requestCredit=null;}if(ownership==SessionOwnership.OWNED) session.close();ownerCredit.close(); if(deadlineTimer!=null) deadlineTimer.shutdown(); }
}
