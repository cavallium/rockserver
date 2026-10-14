package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.*;

/**
 * Bounded direct reads and one exclusively owned durable ordered write request. Infrastructure is borrowed.
 * Caller result acknowledgement is durable and independent of local observer publication.
 */
public final class RatisRocksDBDirect implements AutoCloseable {
    public static final int MAX_TASKS=32;
    public enum WriteKind { PUT,DELETE,MERGE }
    public enum RecoveryState { NONE,ACTIVE,PENDING,COMPLETE }
    private static final long READ_BYTES=8L*1024*1024,WRITE_BYTES=88L*1024*1024,MAX_BYTES=128L*1024*1024;
    private final DistributedSchema schema; private final RangeDirectory directory; private final RatisRoutedClient routed; private final RatisDurableSessionClient session; private final RWScheduler scheduler;
    private final RatisRemoteMutationReceipts receipts; private final RatisDirectRequestJournal journal; private final ReentrantLock persistence=new ReentrantLock();
    private final Map<CompletableFuture<?>,CompletableFuture<Void>> actualCompletions=new IdentityHashMap<>();
    synchronized CompletableFuture<Void> actualCompletion(CompletableFuture<?> observer) { var actual=actualCompletions.get(observer); if (actual!=null) return actual.copy(); if (observer.isDone()) return CompletableFuture.completedFuture(null); throw new IllegalArgumentException("Unknown direct observer"); }
    private boolean writing,closed; private volatile Runnable acknowledgementHook; private int active; private long bytes;
    public RatisRocksDBDirect(DistributedSchema schema,RatisRoutedClient routed,RatisDurableSessionClient session,RWScheduler scheduler,Path path) throws IOException {
        this.schema=Objects.requireNonNull(schema); this.routed=Objects.requireNonNull(routed); this.session=Objects.requireNonNull(session); this.scheduler=Objects.requireNonNull(scheduler); routed.requireKnownSchema(schema); directory=routed.directory(schema);
        var grant=session.grant(); byte[] binding;
        try { var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(schema.digest()); digest.update(routed.sessionIdentityDigest()); digest.update(ByteBuffer.allocate(40).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.epoch()).putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).array()); binding=digest.digest(); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        boolean usedOwner=Files.exists(path.resolveSibling(path.getFileName()+".lock")); journal=new RatisDirectRequestJournal(Objects.requireNonNull(path),binding);
        try { receipts=new RatisRemoteMutationReceipts(path.resolveSibling(path.getFileName()+".released"),authority(0),usedOwner || journal.request()!=null || !session.pristine()); receipts.validateJournal(journal.request()==null?null:journal.request().token); if(journal.request()!=null)journal.request().resultAcknowledged=receipts.retired(journal.request().token); if (journal.request()==null && !session.pristine() && !receipts.established()) throw new IOException("Missing direct request journal for a used session window"); var outstanding=session.pending().stream().filter(p -> !p.consumed()).toList(); if (outstanding.size()>1 || journal.request()==null && !outstanding.isEmpty()) throw new IOException("Direct session has no exclusively bound full request"); }
        catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public RemoteMutationOwner owner() throws IOException {return authority(receipts.nextSequence());}
    private RemoteMutationOwner authority(long nextSequence) {var grant=session.grant();return new RemoteMutationOwner(schema.clusterId(),schema.databaseId(),schema.domainId(),grant.sessionId(),grant.epoch(),nextSequence,RemoteMutationToken.Kind.POINT,schema.digest(),directory.payloadDigest());}
    public CompletableFuture<Object> get(long column,Keys keys,RequestType.RequestTypeId type,RequestContext context) {
        try {
            if (type!=RequestType.RequestTypeId.CURRENT && type!=RequestType.RequestTypeId.EXISTS && type!=RequestType.RequestTypeId.NOTHING) throw unsupported();
            var binding=RatisLogicalClient.supported(schema,column); var encoded=schema.encodeLogicalKey(binding.columnId(),keys);
            if (!binding.schema().hasValue() && type==RequestType.RequestTypeId.CURRENT) throw RocksDBException.of(VALUE_MUST_BE_NULL,"This callback requires a value-bearing column");
            byte[] key=encoded.physicalKey(); var members=encoded.variableKeys();
            return submit(context,OperationFamily.POINT_LOOKUP,false,task -> {
                if (type==RequestType.RequestTypeId.NOTHING) return null;
                byte[] value=routed.read(binding.columnId(),key); task.check(); if (binding.schema().variableLengthKeysCount()!=0) value=schema.decodeBucket(binding.columnId(),key,value).get(members);
                if (!binding.schema().hasValue() && value!=null && value.length!=0) throw new IOException("Nonempty key-only value"); return type==RequestType.RequestTypeId.EXISTS ? value!=null : value==null ? null : Buf.wrap(value);
            });
        } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    public CompletableFuture<Object> write(long column,Keys keys,Buf value,WriteKind kind,RequestType.RequestTypeId type,RequestContext context) {
        return writeSequence(column,List.of(keys),kind==WriteKind.DELETE ? List.of() : Collections.singletonList(value),kind,type,false,context);
    }
    public CompletableFuture<Object> writeSequence(long column,List<Keys> keys,List<Buf> values,WriteKind kind,RequestType.RequestTypeId type,boolean multi,RequestContext context) { return CompletableFuture.failedFuture(new IOException("Distributed mutation requires prepare and exact caller token")); }
    public CompletableFuture<RemoteMutationToken> prepareSequence(RemoteMutationOwner expectedOwner,UUID requestId,long requestSequence,long column,List<Keys> keys,List<Buf> values,WriteKind kind,RequestType.RequestTypeId type,boolean multi,RequestContext context) {
        final Task task;
        try { task=reserve(context,OperationFamily.MUTATION,true); } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        try {
            if(!owner().sameAuthority(expectedOwner))throw new IOException("Point owner differs from installed durable session"); Objects.requireNonNull(kind);
            if (kind==WriteKind.MERGE ? type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.MERGED
                    : !(type==RequestType.RequestTypeId.NOTHING || type==RequestType.RequestTypeId.PREVIOUS || type==RequestType.RequestTypeId.PREVIOUS_PRESENCE || kind==WriteKind.PUT && (type==RequestType.RequestTypeId.ENSURE || type==RequestType.RequestTypeId.DELTA || type==RequestType.RequestTypeId.CHANGED))) throw unsupported();
            if (keys.isEmpty() || keys.size()>256 || !multi && keys.size()!=1 || kind!=WriteKind.DELETE && keys.size()!=values.size()) throw new IllegalArgumentException("Invalid direct sequence count");
            RatisLogicalClient.supported(schema,column); boolean returns=returns(type); var items=new ArrayList<DistributedCommand>(); long input=0;
            var grant=session.grant();
            for (int n=0;n<keys.size();n++) {
                var mode=!returns ? Mutation.ReturnMode.NONE : kind==WriteKind.MERGE ? Mutation.ReturnMode.CURRENT : Mutation.ReturnMode.PREVIOUS;
                var change=switch (kind) {
                    case PUT -> RatisLogicalClient.Change.put(column,keys.get(n),values.get(n),mode);
                    case DELETE -> RatisLogicalClient.Change.delete(column,keys.get(n),mode);
                    case MERGE -> RatisLogicalClient.Change.merge(column,keys.get(n),values.get(n),mode);
                };
                var plan=RatisLogicalClient.plan(schema,List.of(change),List.of()); if (plan.mutations().size()!=1 || !plan.predicates().isEmpty()) throw new IllegalArgumentException("Direct sequence requires point effects");
                var mutation=plan.mutations().getFirst(); var route=directory.route(mutation.columnId(),mutation.key()); var item=new DistributedCommand(route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new OperationId(grant.sessionId(),grant.epoch(),0),List.of(mutation));
                input+=DistributedCommandCodec.encodedSize(item); if (input>RatisDirectRequestJournal.MAX_INPUT) throw new IllegalArgumentException("Direct request exceeds aggregate input bound"); items.add(item);
            }
            var request=new RatisDirectRequestJournal.Request(kind,type,multi,items); request.token=new RemoteMutationToken(schema.clusterId(),schema.databaseId(),schema.domainId(),expectedOwner.ownerId(),requestId,expectedOwner.ownerEpoch(),requestSequence,RemoteMutationToken.Kind.POINT,schema.digest(),directory.payloadDigest(),RemoteMutationInputs.point(column,kind.ordinal(),type,multi,keys,values)); task.work=owner -> { persistence.lock(); try { receipts.requireNew(request.token); var retained=journal.request(); if(retained!=null && !retained.resultAcknowledged) { if(!retained.same(request))throw new IOException("Resolve original direct owner/input before prepare"); return retained.token; } journal.begin(request); receipts.bind(request.token); return request.token; } finally { persistence.unlock(); } }; dispatch(task,context,OperationFamily.MUTATION);
        } catch (Throwable failure) { task.fail(failure); if (failure instanceof Error fatal) throw fatal; }
        var prepared=task.view.thenApply(value -> (RemoteMutationToken)value); synchronized(this) { actualCompletions.put(prepared,task.drained); } CompletableFuture.allOf(task.drained,prepared.handle((value,failure) -> null)).whenComplete((ignored,failure) -> { synchronized(this) { actualCompletions.remove(prepared); } }); return prepared;
    }
    public CompletableFuture<Object> execute(RemoteMutationToken token,RequestContext context) { return submit(context,OperationFamily.MUTATION,true,task -> { persistence.lock(); try { var request=exact(token); if(request.resultAcknowledged)throw new IOException("Direct result already acknowledged"); return execute(request,task); } finally { persistence.unlock(); } }); }
    private RatisDirectRequestJournal.Request exact(RemoteMutationToken token) throws IOException { var request=journal.request(); if(receipts.retired(token))throw new IOException("Request sequence expired or acknowledged"); if(request==null || !token.equals(request.token))throw new IOException("Original direct token unavailable or differs"); return request; }
    public RemoteMutationStatus status(RemoteMutationToken token) throws IOException { if(!persistence.tryLock())return new RemoteMutationStatus(token,RemoteMutationStatus.State.PENDING,-1,-1,0,null); try { var released=receipts.find(token); if(released!=null)return released; var retained=journal.request(); if(retained==null || !retained.token.requestId().equals(token.requestId()))return new RemoteMutationStatus(token,RemoteMutationStatus.State.UNAVAILABLE,-1,-1,0,null); var r=exact(token); var state=r.resultAcknowledged?RemoteMutationStatus.State.ACKNOWLEDGED:r.terminal()?(r.results.stream().anyMatch(result -> result.status()==CommandResult.Status.SESSION_EXPIRED)?RemoteMutationStatus.State.EXPIRED:RemoteMutationStatus.State.TERMINAL):r.cursor()==0 && r.pending==null?RemoteMutationStatus.State.PREPARED:RemoteMutationStatus.State.PENDING; return new RemoteMutationStatus(token,state,r.results.stream().filter(result -> result.status()==CommandResult.Status.SUCCESS).count(),0,r.results.size(),r.terminal()?RatisDirectRequestJournal.resultDigest(r):null); } finally { persistence.unlock(); } }
    public List<byte[]> resultPage(RemoteMutationToken token,int offset,int maximumItems,int maximumBytes) throws IOException { if(offset<0 || maximumItems<1 || maximumItems>256 || maximumBytes<1 || maximumBytes>CommandResult.MAX_BYTES)throw new IllegalArgumentException("Invalid result page budget"); if(!persistence.tryLock())throw new IOException("Direct result retained while actual work remains active"); try { var r=exact(token); if(offset>r.results.size())throw new IllegalArgumentException("Result offset exceeds retained prefix"); var page=new ArrayList<byte[]>(); int bytes=0; for(int n=offset;n<r.results.size() && page.size()<maximumItems;n++) { byte[] value=r.results.get(n).encode(); if(value.length>maximumBytes-bytes)break; page.add(value); bytes+=value.length; } return List.copyOf(page); } finally { persistence.unlock(); } }
    public void acknowledgeResult(RemoteMutationToken token,byte[] digest) throws IOException { persistence.lock(); try { var released=receipts.find(token); if(released!=null) { if(!Arrays.equals(released.resultDigest(),digest))throw new IOException("Released result ACK changed"); return; } var r=exact(token); synchronized(this){if(writing)throw new IOException("Direct actual work remains active");} if(!r.terminal() || r.pending!=null || !Arrays.equals(RatisDirectRequestJournal.resultDigest(r),digest))throw new IOException("Result ACK differs from terminal outcome"); receipts.acknowledge(new RemoteMutationStatus(token,RemoteMutationStatus.State.ACKNOWLEDGED,r.results.stream().filter(result -> result.status()==CommandResult.Status.SUCCESS).count(),0,0,digest)); r.resultAcknowledged=true; } finally { persistence.unlock(); } }
    public RecoveryState recoveryState() throws IOException {
        if (!persistence.tryLock()) return RecoveryState.ACTIVE;
        try { synchronized (this) { if (writing) return RecoveryState.ACTIVE; } var request=journal.request(); return request==null || request.resultAcknowledged ? RecoveryState.NONE : request.terminal() ? RecoveryState.COMPLETE : RecoveryState.PENDING; }
        finally { persistence.unlock(); }
    }
    /** Resume the saved physical input, including a request forced before its first session identity. */
    public CompletableFuture<Object> resumeRetained(RequestContext context) {
        return submit(context,OperationFamily.MUTATION,true,task -> {
            persistence.lock();
            try { var request=journal.request(); if (request==null || request.resultAcknowledged) throw new IOException("No unresolved retained direct request"); return execute(request,task); }
            finally { persistence.unlock(); }
        });
    }
    /** Publish only a known complete outcome; unknown or incomplete work cannot be discarded. */
    public CompletableFuture<Object> consumeRetained(RequestContext context) {
        return submit(context,OperationFamily.MUTATION,true,task -> { persistence.lock(); try { var request=journal.request(); if (request==null || !request.terminal()) throw new IOException("Direct request is not definitively complete"); receipts.requireNew(request.token); reconcileAcknowledgement(request,task); task.check(); return output(request,task); } finally { persistence.unlock(); } });
    }
    private Object execute(RatisDirectRequestJournal.Request candidate,Task task) throws Exception {
        persistence.lock();
        try {
            receipts.requireNew(candidate.token); var request=journal.request();
            if (request==null) { journal.begin(candidate); request=candidate; }
            else if (!request.same(candidate)) throw new IOException("Resolve the exact retained callback and full direct request before issuing another");
            while (true) {
                reconcileAcknowledgement(request,task); if (request.terminal()) { task.check(); return output(request,task); }
                var pending=findPending(request.pending);
                if (pending==null) {
                    var unresolved=session.pending().stream().filter(p -> !p.consumed()).toList(); var item=request.items.get(request.cursor());
                    if (request.pending!=null) throw new IOException("Original unresolved direct session identity disappeared");
                    if (unresolved.size()>1) throw new IOException("Direct session is not exclusively owned");
                    if (!unresolved.isEmpty()) { pending=unresolved.getFirst(); requireItem(item,pending); }
                    else {
                        task.check();
                        if (!Arrays.equals(item.schemaDigest(),routed.directory().schemaDigest())) { journal.rejectUnsubmitted(CommandResult.Status.STALE_SCHEMA); continue; }
                        var route=directory.route(item.mutations().getFirst().columnId(),item.mutations().getFirst().key());
                        if (!route.groupId().equals(item.groupId()) || route.rangeEpoch()!=item.rangeEpoch()) throw new IOException("Frozen direct item route changed incarnation");
                        pending=session.prepareAt(directory,item.mutations(),List.of()); requireItem(item,pending);
                    }
                    journal.bind(pending.command().operationId());
                }
                requireItem(request.items.get(request.pendingIndex),pending); task.check(); var result=session.execute(pending,task.context,task::check);
                journal.result(result); // Never ACK until raw result and cursor are forced in the full envelope.
                session.acknowledge(pending,task.context,task::check); journal.acknowledged(); var hook=acknowledgementHook; if (hook!=null) hook.run(); task.check();
            }
        } finally { persistence.unlock(); }
    }
    private void reconcileAcknowledgement(RatisDirectRequestJournal.Request request,Task task) throws IOException {
        if (request.pending==null || request.pendingIndex>=request.cursor()) return;
        var pending=findPending(request.pending);
        if (pending!=null) {
            requireItem(request.items.get(request.pendingIndex),pending);
            var saved=pending.savedResult(); if (saved==null || !Arrays.equals(saved.encode(),request.results.get(request.pendingIndex).encode())) throw new IOException("Direct/session saved outcome differs"); session.acknowledge(pending,task.context,task::check);
        } else if (session.acknowledgedThrough()<request.pending.sequence()) throw new IOException("Direct saved result lacks original session ACK/identity");
        session.recoverReroutes(task.context,task::check,false);journal.acknowledged();
    }
    private RatisDurableSessionClient.Pending findPending(OperationId operation) throws IOException { if (operation==null) return null; for (var pending:session.pending()) if (pending.command().operationId().equals(operation)) return pending; return null; }
    private static void requireItem(DistributedCommand item,RatisDurableSessionClient.Pending pending) throws IOException {
        var command=pending.command(); if (command.kind()!=DistributedCommand.Kind.DATA || pending.awaitingAcceptance() || !command.groupId().equals(item.groupId()) || command.rangeEpoch()!=item.rangeEpoch() || !Arrays.equals(command.schemaDigest(),item.schemaDigest()) || !command.mutations().equals(item.mutations()) || !command.predicates().isEmpty()) throw new IOException("Session pending item differs from frozen direct request");
    }
    private Object output(RatisDirectRequestJournal.Request request,Task task) {
        if (request.unsubmittedStatus!=null) { var failure=RocksDBException.of(COMMIT_FAILED,"Direct request retained "+request.cursor()+" applied prefix items; remaining items were never submitted: "+request.unsubmittedStatus); task.definiteFailure=failure; throw failure; }
        if (request.overflow) { var failure=RocksDBException.of(SERVER_OVERLOADED,"Direct sequence retained its applied prefix but exceeds aggregate result budget"); task.definiteFailure=failure; throw failure; }
        if (!request.results.isEmpty() && request.results.getLast().status()!=CommandResult.Status.SUCCESS) { var failure=RocksDBException.of(COMMIT_FAILED_TRY_AGAIN,"Direct sequence has retained terminal outcome "+request.results.getLast().status()); task.definiteFailure=failure; throw failure; }
        if (!returns(request.type)) return request.multi ? List.of() : null;
        var values=new ArrayList<Object>();
        for (int n=0;n<request.results.size();n++) {
            var result=request.results.get(n); byte[] raw=returns(request.type) ? result.value(0) : null; Buf previous=raw==null ? null : Buf.wrap(raw); byte[] currentBytes=request.type==RequestType.RequestTypeId.DELTA || request.type==RequestType.RequestTypeId.CHANGED ? request.items.get(n).mutations().getFirst().value() : null; Buf current=currentBytes==null ? null : Buf.wrap(currentBytes);
            Object value=switch (request.type) { case PREVIOUS,MERGED -> previous; case PREVIOUS_PRESENCE -> raw!=null; case DELTA -> new Delta<>(previous,current); case CHANGED -> !Utils.valueEquals(previous,current); default -> null; };
            if (returns(request.type)) values.add(value);
        }
        return request.multi ? Collections.unmodifiableList(values) : values.isEmpty() ? null : values.getFirst();
    }
    private static boolean returns(RequestType.RequestTypeId type) { return type!=RequestType.RequestTypeId.NOTHING && type!=RequestType.RequestTypeId.ENSURE; }
    @FunctionalInterface private interface Work { Object run(Task task) throws Exception; }
    private Task reserve(RequestContext context,OperationFamily family,boolean write) {
        long deadline=scheduler.resolveMonotonicDeadline(context); long charge=write ? WRITE_BYTES : READ_BYTES;
        synchronized (this) { if (closed || active>=MAX_TASKS || charge>MAX_BYTES-RatisDirectRequestJournal.MAX_BYTES-bytes || write && writing) throw RocksDBException.of(SERVER_OVERLOADED,"Direct request capacity exhausted"); var task=new Task(context,deadline,write,charge); active++; bytes+=charge; actualCompletions.put(task.view,task.drained); if (write) writing=true; return task; }
    }
    private CompletableFuture<Object> submit(RequestContext context,OperationFamily family,boolean write,Work work) {
        try { var task=reserve(context,family,write); task.work=work; dispatch(task,context,family); return task.view; } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
    }
    private void dispatch(Task task,RequestContext context,OperationFamily family) { try { scheduler.executor(context.profile(),family,task.deadline).execute(task,Math.min(task.charge,WorkloadCost.MAX_ESTIMATED_BYTES)); } catch (RuntimeException rejected) { task.reject(rejected); } }
    private static Object copyResult(Object value) {
        if (value instanceof Buf buf) return Buf.wrap(buf.toByteArray()); if (value instanceof Delta<?> delta) return new Delta<>(copyResult(delta.previous()),copyResult(delta.current()));
        if (value instanceof List<?> list) { var copy=new ArrayList<Object>(list.size()); list.forEach(item -> copy.add(copyResult(item))); return Collections.unmodifiableList(copy); } return value;
    }
    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        final RequestContext context;final long deadline,charge; final boolean write; Work work; Throwable definiteFailure; final AtomicInteger phase=new AtomicInteger(); final CompletableFuture<Object> result=new CompletableFuture<>(),view=result.copy(); final CompletableFuture<Void> drained=new CompletableFuture<>();
        Task(RequestContext context,long deadline,boolean write,long charge) { this.context=context;this.deadline=deadline; this.write=write; this.charge=charge; }
        void check() throws IOException { if (deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Direct deadline expired; full durable request retained",new TimeoutException()); }
        public void run() { if (!phase.compareAndSet(0,1)) return; Object value=null; Throwable failure=null; try { check(); value=copyResult(work.run(this)); check(); } catch (Throwable problem) { failure=problem; } work=null; phase.set(2); finish(value,failure); if (failure instanceof Error fatal) throw fatal; }
        public void reject(RuntimeException failure) { fail(failure); }
        void fail(Throwable failure) { if (phase.compareAndSet(0,2)) { work=null; finish(null,failure); } }
        void finish(Object value,Throwable failure) {

            synchronized (RatisRocksDBDirect.this) {  active--; bytes-=charge; if (write) writing=false; }
            drained.complete(null);
            if (failure==null) result.complete(value); else result.completeExceptionally(failure);
            synchronized (RatisRocksDBDirect.this) { actualCompletions.remove(view); }
        }
    }
    /** Controlled ownership/deadline seam; runs after both durable ACK checkpoints outside admission monitor. */
    void afterRetirementForce(Runnable hook){receipts.afterForce(hook);}
    void afterAcknowledgement(Runnable hook) { acknowledgementHook=hook; }
    public synchronized int activeRequests() { return active; }
    @Override public void close() throws IOException { synchronized (this) { if (active!=0) throw new IOException("Direct work active; ownership retained"); closed=true; } journal.close(); }
    private static RocksDBException unsupported() { return RocksDBException.of(NOT_IMPLEMENTED,"Unsupported direct request callback"); }
}
