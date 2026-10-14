package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.common.OperationFamily;
import java.util.concurrent.locks.ReentrantLock;

/** Durable exclusive user window. Journal transitions serialize; disjoint synchronous RPC owners may overlap. */
public final class RatisDurableSessionClient implements AutoCloseable {
    public static final class Pending {
        private final RatisDurableSessionClient owner; private final RatisSessionClient.Pending pending;
        private CommandResult result; private DistributedCommand command; private boolean consumed,executing,awaitingAcceptance,abandoned;
        private Pending(RatisDurableSessionClient owner,RatisSessionClient.Pending pending,CommandResult result,boolean consumed) {
            this.owner=owner; this.pending=pending; this.result=result; this.consumed=consumed;
            this.command=pending.command(); this.awaitingAcceptance=pending.awaitingAcceptance(); this.abandoned=pending.abandoned();
        }
        public DistributedCommand command() { owner.lane.lock(); try { return command; } finally { owner.lane.unlock(); } }
        public boolean awaitingAcceptance() { owner.lane.lock(); try { return awaitingAcceptance; } finally { owner.lane.unlock(); } }
        public boolean abandoned() { owner.lane.lock(); try { return abandoned; } finally { owner.lane.unlock(); } }
        public RangeDescriptor descriptor() { return pending.descriptor(); }
        public boolean consumed() { owner.lane.lock(); try { return consumed; } finally { owner.lane.unlock(); } }
        /** Only durably saved results are exposed. Consumed holes remain marked until their contiguous prefix drains. */
        public CommandResult savedResult() { owner.lane.lock(); try { return result; } finally { owner.lane.unlock(); } }
    }
    private final RatisSessionClient client; private final RatisSessionClient.Limits limits; private final SessionWindowJournal journal;
    private final ReentrantLock lane=new ReentrantLock(); private final TreeMap<Long,Pending> retained=new TreeMap<>();
    private int active; private boolean closed,fenced,journalClosed;
    private RatisOperationRerouteCoordinator reroutes;private RWScheduler rerouteScheduler;private boolean recoveryPending;
    private RatisDurableSessionClient(RatisSessionClient client,RatisSessionClient.Limits limits,SessionWindowJournal journal) {
        this.client=client; this.limits=limits; this.journal=journal;
        for (var entry:client.retained()) retained.put(entry.command().operationId().sequence(),new Pending(this,entry,client.resolved(entry),client.consumed(entry)));
    }
    /** Only for one newly issued, exclusively owned grant. Never recreate a missing used-grant journal. */
    public static RatisDurableSessionClient create(Path path,RatisRoutedClient routed,SessionEpochLedger issuance,
            SessionEpochLedger.Grant grant,RatisSessionClient.Limits limits) throws IOException {
        var client=new RatisSessionClient(routed,issuance,grant,limits); var journal=SessionWindowJournal.open(path,routed,true);
        try { journal.write(grant,limits,client.snapshot()); return new RatisDurableSessionClient(client,limits,journal); }
        catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    /** Reopen exact pending identities even after grant expiry; native authority decides retry outcomes. */
    public static RatisDurableSessionClient reopen(Path path,RatisRoutedClient routed) throws IOException {
        var journal=SessionWindowJournal.open(path,routed,false);
        try { var saved=journal.read(); return new RatisDurableSessionClient(RatisSessionClient.restore(routed,saved.grant(),saved.limits(),saved.state()),saved.limits(),journal); }
        catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    void installReroutes(Path root,DistributedSchema schema,RatisSessionEpochs epochs,RatisRoutedClient routed,RatisTransactionStatuses homes,RWScheduler scheduler,RatisCatalogue directories,RatisRoutedClient.SourceAckDirectoryReader reader)throws IOException {
        lane.lock();try{requireOpen();if(active!=0 || reroutes!=null)throw new IOException("Durable reroute owner already configured or active");reroutes=new RatisOperationRerouteCoordinator(root,schema,epochs,routed,homes,scheduler,grant(),limits,directories,reader);rerouteScheduler=scheduler;recoveryPending=true;}finally{lane.unlock();}
    }
    public SessionEpochLedger.Grant grant() { return client.grant(); }
    RatisSessionClient.Limits limits() { return limits; }
    boolean quiescent()throws IOException {
        lane.lock();try{requireOpen();return active==0 && retained.isEmpty() && client.snapshot().entries().isEmpty() && (reroutes==null || reroutes.quiescent());}finally{lane.unlock();}
    }
    boolean renewalRequired(SessionEpochLedger authority)throws IOException {
        lane.lock();try{requireOpen();var grant=grant();return client.snapshot().exhausted() || authority.logicalTimeMillis()>=grant.expiresAtMillis() || grant.epoch()<authority.minimumEpoch() || !authority.grants().contains(grant);}finally{lane.unlock();}
    }
    boolean pristine() throws IOException {
        lane.lock(); try { requireOpen(); var state=client.snapshot();
            return state.nextSequence()==0 && state.acknowledgedThrough()==-1 && !state.exhausted() && state.entries().isEmpty();
        } finally { lane.unlock(); }
    }
    public List<Pending> pending() throws IOException { lane.lock(); try { requireOpen(); return List.copyOf(retained.values()); } finally { lane.unlock(); } }
    public long acknowledgedThrough() throws IOException { lane.lock(); try { requireOpen(); return client.acknowledgedThrough(); } finally { lane.unlock(); } }
    public Pending prepare(List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepare(mutations,predicates); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    public Pending prepare(List<Mutation> mutations) throws IOException { return prepare(mutations,List.of()); }
    public Pending prepareAt(RangeDirectory original,List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepareAt(original,mutations,predicates); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    Pending prepareAcknowledgementAt(RangeDirectory source,RangeDescriptor parent)throws IOException {
        lane.lock();try{requireOpen();var pending=client.prepareAcknowledgementAt(source,parent);checkpoint();return retain(pending);}finally{lane.unlock();}
    }
    public Pending prepareTransactionIntent(List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepareTransactionIntent(mutations,predicates); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    public Pending prepareTransactionIntent(List<Mutation> mutations) throws IOException { return prepareTransactionIntent(mutations,List.of()); }
    /** Accepted bytes are forced before the durable phase permits any execution. */
    public void acceptTransaction(Pending pending,DistributedCommand accepted) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending);
            if (!pending.awaitingAcceptance || pending.executing || pending.result!=null || pending.consumed) throw new IllegalStateException("Operation is not an unresolved durable transaction intent");
            client.acceptTransaction(pending.pending,accepted); checkpoint();
            pending.command=pending.pending.command(); pending.awaitingAcceptance=false;
        } finally { lane.unlock(); }
    }
    /** Persist a definitive local CONFLICT before exposure; application consumption and ACK remain explicit. */
    public CommandResult abandonTransactionIntent(Pending pending) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending);
            if (!pending.awaitingAcceptance || pending.executing || pending.result!=null || pending.consumed) throw new IllegalStateException("Operation is not an unresolved durable transaction intent");
            var result=client.abandonTransactionIntent(pending.pending); checkpoint();
            pending.result=result; pending.abandoned=true; pending.awaitingAcceptance=false; return result;
        } finally { lane.unlock(); }
    }
    public Pending prepareAcknowledgement(UUID column,byte[] routingKey) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepareAcknowledgement(column,routingKey); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    private Pending retain(RatisSessionClient.Pending entry) {
        var pending=new Pending(this,entry,null,false); retained.put(entry.command().operationId().sequence(),pending); return pending;
    }
    public CommandResult execute(Pending pending) throws IOException {
        return reroutes==null ? execute(pending,(RatisSessionClient.Execution)null) : admitted(()->execute(pending,RequestContext.batch(),()->{}));
    }
    CommandResult execute(Pending pending,RequestContext context,RatisAtomicClient.InlineCheck check)throws IOException {
        recoverReroutes(context,check,true);return execute(pending,reroutes==null ? null : original->reroutes.execute(original,context,check));
    }
    void recoverReroutes(RequestContext context,RatisAtomicClient.InlineCheck check,boolean onlyPending)throws IOException {
        lane.lock();try{requireOpen();if(reroutes==null || onlyPending && !recoveryPending)return;active++;}finally{lane.unlock();}
        try{reroutes.recoverConsumed(this,context,check);lane.lock();try{recoveryPending=false;}finally{lane.unlock();}}finally{lane.lock();try{active--;}finally{lane.unlock();}}
    }
    @FunctionalInterface private interface Admitted<T>{T run()throws IOException;}
    private <T> T admitted(Admitted<T> work)throws IOException {
        lane.lock();try{requireOpen();active++;}finally{lane.unlock();}var result=new CompletableFuture<T>();
        final class Task implements Runnable,RWScheduler.RejectionAwareTask {
            final java.util.concurrent.atomic.AtomicInteger phase=new java.util.concurrent.atomic.AtomicInteger();
            public void run(){if(!phase.compareAndSet(0,1))return;T value=null;Throwable failure=null;try{value=work.run();}catch(Throwable problem){failure=problem;}settle(value,failure);}
            public void reject(RuntimeException failure){if(phase.compareAndSet(0,1))settle(null,failure);}
            void settle(T value,Throwable failure){lane.lock();try{active--;}finally{lane.unlock();}phase.set(2);if(failure==null)result.complete(value);else result.completeExceptionally(failure);}
        }
        var task=new Task();try{rerouteScheduler.executor(it.cavallium.rockserver.core.common.WorkloadProfile.BATCH,OperationFamily.MUTATION,Long.MAX_VALUE).execute(task,Math.min(SessionWindowJournal.MAX_WINDOW_BYTES,it.cavallium.rockserver.core.common.WorkloadCost.MAX_ESTIMATED_BYTES));}catch(RuntimeException rejected){task.reject(rejected);}
        try{return result.get();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException("Durable work remains owned until completion",interrupted);}catch(ExecutionException failure){if(failure.getCause() instanceof IOException io)throw io;if(failure.getCause() instanceof RuntimeException runtime)throw runtime;if(failure.getCause() instanceof Error error)throw error;throw new IOException("Durable session execution failed",failure.getCause());}
    }
    /** Keeps the original pending identity active until the resolved outcome has been forced. */
    CommandResult execute(Pending pending,RatisSessionClient.Execution resolution) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending);
            if (pending.awaitingAcceptance) throw new IllegalStateException("Durable transaction intent has not been accepted");
            if (pending.result!=null) return pending.result;
            if (pending.executing) throw new IOException("This durable operation is already executing"); pending.executing=true; active++;
        } finally { lane.unlock(); }
        try {
            var result=resolution==null ? client.execute(pending.pending) : client.execute(pending.pending,resolution); lane.lock();
            try { requireOpen(); checkpoint(); pending.result=result; return result; } finally { lane.unlock(); }
        } finally { lane.lock(); try { pending.executing=false; active--; } finally { lane.unlock(); } }
    }
    CompletableFuture<CommandResult> executeAsync(Pending pending,RequestContext context,RatisAtomicClient.InlineCheck check,Executor checkpoints,RatisRequestBudget budget){
        lane.lock();try{requireOpen();owned(pending);if(pending.awaitingAcceptance)throw new IllegalStateException("Durable intent has not been accepted");if(pending.result!=null)return CompletableFuture.completedFuture(pending.result);if(pending.executing)throw new IOException("This durable operation is already executing");pending.executing=true;active++;}catch(Exception failure){return CompletableFuture.failedFuture(failure);}finally{lane.unlock();}
        var result=new CompletableFuture<CommandResult>();CompletableFuture<Void> recovery;
        if(reroutes==null || !recoveryPending)recovery=CompletableFuture.completedFuture(null);else recovery=budget.recover(() -> {recoverReroutesOwned(context,check,true,budget);return null;});
        recovery.thenCompose(ignored -> client.executeAsync(pending.pending)).handle((value,failure) -> {
            Throwable cause=unwrap(failure);if(cause instanceof RatisRoutedClient.OriginalOperationAbsent && reroutes!=null)return budget.recover(() -> client.execute(pending.pending,original -> reroutes.executeOwned(original,context,check,budget)));
            return cause==null?CompletableFuture.completedFuture(value):CompletableFuture.<CommandResult>failedFuture(cause);
        }).thenCompose(stage -> stage).whenComplete((value,failure) -> {
            final Throwable problem=unwrap(failure);
            Runnable checkpoint=() -> {Throwable failed=problem;lane.lock();try{if(failed==null){requireOpen();checkpoint();pending.result=value;}}catch(Throwable error){failed=error;}finally{pending.executing=false;active--;lane.unlock();}if(failed==null)result.complete(value);else result.completeExceptionally(failed);};
            try{checkpoints.execute(checkpoint);}catch(RuntimeException rejected){lane.lock();try{fenced=true;pending.executing=false;active--;}finally{lane.unlock();}result.completeExceptionally(rejected);}
        });return result.copy();
    }
    CompletableFuture<Void> recoverReroutesAsync(RequestContext context,RatisAtomicClient.InlineCheck check,RatisRequestBudget budget){if(reroutes==null)return CompletableFuture.completedFuture(null);return budget.recover(() -> {recoverReroutesOwned(context,check,false,budget);return null;});}
    private void recoverReroutesOwned(RequestContext context,RatisAtomicClient.InlineCheck check,boolean onlyPending,RatisRequestBudget budget)throws IOException {
        lane.lock();try{requireOpen();if(reroutes==null || onlyPending && !recoveryPending)return;active++;}finally{lane.unlock();}
        try{reroutes.recoverConsumedOwned(this,context,check,budget);lane.lock();try{recoveryPending=false;}finally{lane.unlock();}}finally{lane.lock();try{active--;}finally{lane.unlock();}}
    }
    private static Throwable unwrap(Throwable failure){while((failure instanceof CompletionException || failure instanceof ExecutionException) && failure.getCause()!=null)failure=failure.getCause();return failure;}
    /** Application must durably consume the returned outcome first. Persist consumption before any later piggyback ACK. */
    public void acknowledge(Pending pending) throws IOException {
        if(reroutes==null){acknowledgeLocally(pending);return;}admitted(()->{acknowledge(pending,RequestContext.batch(),()->{});return null;});
    }
    void acknowledge(Pending pending,RequestContext context,RatisAtomicClient.InlineCheck check)throws IOException {acknowledgeLocally(pending);recoverReroutes(context,check,false);}
    void acknowledgeLocally(Pending pending) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending);
            if (pending.awaitingAcceptance) throw new IllegalStateException("Durable transaction intent has not been accepted");
            if (pending.result==null) throw new IllegalStateException("No durably resolved outcome to consume");
            if (pending.consumed) return; client.acknowledge(pending.pending); checkpoint(); pending.consumed=true;
            retained.headMap(client.acknowledgedThrough(),true).clear();
        } finally { lane.unlock(); }
    }
    /** Separate durable maintenance owner still controls first-use binding identities. */
    public RatisRoutedClient.PreparedCommand prepareBinding(RatisSessionEpochs epochs,OperationId issuer,Pending pending,long acknowledgedThrough) throws IOException {
        lane.lock(); try { requireOpen(); owned(pending); active++; } finally { lane.unlock(); }
        try { return client.prepareBinding(epochs,issuer,pending.pending,acknowledgedThrough); }
        finally { lane.lock(); try { active--; } finally { lane.unlock(); } }
    }
    private void checkpoint() throws IOException {
        try { journal.write(client.grant(),limits,client.snapshot()); } catch (IOException | RuntimeException failure) { fenced=true; throw failure; }
    }
    private void owned(Pending pending) { if (pending==null || pending.owner!=this) throw new IllegalArgumentException("Operation belongs to another durable session"); }
    private void requireOpen() throws IOException { if (closed || fenced) throw new IOException("Durable session is closed/fenced; reopen its journal"); }
    @Override public void close() throws IOException {
        lane.lock(); try { if (journalClosed) return; if (active!=0) throw new IOException("Durable session RPC work is active"); if(reroutes!=null)reroutes.close();closed=true; journal.close(); journalClosed=true; } finally { lane.unlock(); }
    }
}
