package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** Bounded ephemeral handle ownership. No native work or close runs under the registry monitor. */
final class RatisScopedTransactions implements AutoCloseable {
    @FunctionalInterface interface PlanFactory { RatisRangeStateMachine.ScopedCommit prepare(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command) throws Exception; }
    record Limits(int handles,long scopeBytes,long planBytes,Duration maximumTtl) {
        Limits {
            if (handles<1 || handles>4096 || scopeBytes<1 || scopeBytes>256L*1024*1024 || planBytes<DistributedCommandCodec.MAX_ENCODED_BYTES
                    || planBytes>256L*1024*1024 || maximumTtl.isNegative() || maximumTtl.isZero() || maximumTtl.compareTo(Duration.ofMinutes(5))>0) throw new IllegalArgumentException("Invalid transaction handle limits");
        }
        static Limits defaults() { return new Limits(128,16L*1024*1024,16L*1024*1024,Duration.ofSeconds(60)); }
    }
    private final RatisRangeStateMachine machine;
    private final UUID owner;
    private final Limits limits;
    private final LongSupplier nanos;
    private final PlanFactory planner;
    private final Map<UUID,Entry> entries=new HashMap<>();
    private final Map<UUID,Entry> handles=new HashMap<>();
    private final Set<Entry> retired=new HashSet<>();
    private int chargedHandles;
    private long scopeBytes,planBytes;
    private boolean closed;
    private static final class Entry {
        final UUID id,leaseId=UUID.randomUUID(); final long generation,opened,ttl,scopeBytes; final TransactionScope scope;
        ReplicatedRangeStore.ScopedHistoryPin pin;
        ScopedTransactionRpc.Handle handle;
        RatisRangeStateMachine.ScopedCommit plan;
        byte[] baseDigest;
        int planBytes;
        boolean busy,planning,closed,cleaning;
        Entry(UUID id,long generation,long opened,long ttl,long scopeBytes,TransactionScope scope) {
            this.id=id; this.generation=generation; this.opened=opened; this.ttl=ttl; this.scopeBytes=scopeBytes; this.scope=scope;
        }
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos) {
        this(machine,limits,nanos,machine::prepareScopedCommit);
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos,PlanFactory planner) {
        this.machine=Objects.requireNonNull(machine); this.limits=Objects.requireNonNull(limits); this.nanos=Objects.requireNonNull(nanos);
        this.planner=Objects.requireNonNull(planner);
        owner=machine.transactionOwnerIncarnation();
    }
    ScopedTransactionRpc.Handle open(UUID id,TransactionScope scope,Duration ttl) throws Exception {
        Objects.requireNonNull(id); Objects.requireNonNull(scope); Objects.requireNonNull(ttl); expire();
        long duration=ttl.toNanos(); if (duration<=0 || duration>limits.maximumTtl().toNanos()) throw new IllegalArgumentException("Transaction TTL exceeds advertised limit");
        machine.checkTransactionOwner(); long generation=machine.transactionGeneration();
        final Entry entry;
        synchronized (this) {
            if (closed) throw new IOException("Transaction registry is closed");
            var existing=entries.get(id);
            if (existing!=null) {
                if (existing.busy || existing.handle==null) throw new IOException("Transaction open is still running");
                if (existing.ttl!=duration || !Arrays.equals(ScopedTransactionRpc.scopeBytes(existing.scope),ScopedTransactionRpc.scopeBytes(scope))) throw new IllegalArgumentException("Transaction open identity reused with different declaration");
                return existing.handle;
            }
            long bytes=RangeMutationHistory.accountedScopeBytes(scope);
            if (chargedHandles>=limits.handles() || bytes>limits.scopeBytes()-scopeBytes) throw new IOException("Transaction handle/scope admission exhausted");
            entry=new Entry(id,generation,nanos.getAsLong(),duration,bytes,scope); entry.busy=true;
            entries.put(id,entry); chargedHandles++; scopeBytes+=bytes;
        }
        boolean success=false;
        try {
            entry.pin=machine.captureTransaction(scope);
            var handle=new ScopedTransactionRpc.Handle(owner,entry.leaseId,generation,entry.pin.position());
            synchronized (this) { ensureLive(entry); entry.handle=handle; handles.put(entry.leaseId,entry); }
            success=true; return handle;
        } finally { finish(entry,!success); }
    }
    byte[] get(ScopedTransactionRpc.Handle handle,UUID column,byte[] key) throws Exception {
        var entry=begin(handle,false); try { var value=entry.pin.get(column,key); if (value!=null && value.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Transaction value exceeds reply budget"); checked(entry); return value; }
        finally { finish(entry,false); }
    }
    PhysicalRangePage page(ScopedTransactionRpc.Handle handle,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws Exception {
        var entry=begin(handle,false); try { var page=entry.pin.page(column,lower,upper,reverse,resume,items,bytes); checked(entry); return page; }
        finally { finish(entry,false); }
    }
    DistributedCommand plan(ScopedTransactionRpc.Handle handle,DistributedCommand command) throws Exception {
        var entry=begin(handle,true); boolean success=false;
        try {
            if (entry.plan!=null) {
                success=true; // A conflicting retry must not destroy the already frozen canonical plan.
                if (!Arrays.equals(entry.baseDigest,command.payloadDigest())) throw new IllegalArgumentException("Transaction plan retry differs from original operands");
                checked(entry); return entry.plan.command();
            }
            synchronized (this) {
                if (DistributedCommandCodec.MAX_ENCODED_BYTES>limits.planBytes()-planBytes) throw new IOException("Transaction retained-plan admission exhausted");
                entry.planBytes=DistributedCommandCodec.MAX_ENCODED_BYTES; planBytes+=entry.planBytes;
                entry.planning=true;
            }
            entry.plan=planner.prepare(entry.pin,command); entry.baseDigest=command.payloadDigest();
            int actual=DistributedCommandCodec.encodedSize(entry.plan.command());
            synchronized (this) { planBytes+=actual-entry.planBytes; entry.planBytes=actual; ensureLive(entry); }
            success=true; return entry.plan.command();
        } finally { finish(entry,!success); }
    }
    void release(ScopedTransactionRpc.Handle handle) throws Exception {
        Entry entry;
        synchronized (this) {
            if (!owner.equals(handle.owner())) throw new IOException("Transaction owner incarnation changed");
            entry=handles.get(handle.id()); if (entry==null) return;
            requireHandle(entry,handle); retire(entry);
        }
        cleanup();
    }
    void expire() throws Exception {
        synchronized (this) {
            for (var entry:List.copyOf(entries.values())) if (!live(entry)) retire(entry);
        }
        cleanup();
    }
    private Entry begin(ScopedTransactionRpc.Handle handle,boolean permitPlan) throws Exception {
        machine.checkTransactionOwner(); expire();
        synchronized (this) {
            var entry=handles.get(handle.id()); if (entry==null || closed) throw new IOException("Transaction expired or leader changed");
            requireHandle(entry,handle); ensureLive(entry);
            if (entry.busy) throw new IOException("Transaction operation is already running");
            if (!permitPlan && entry.plan!=null) throw new IOException("Transaction plan is already frozen");
            entry.busy=true; return entry;
        }
    }
    private void checked(Entry entry) throws Exception { machine.checkTransactionOwner(); synchronized (this) { ensureLive(entry); } }
    private boolean live(Entry entry) {
        return !closed && !entry.closed && entry.generation==machine.transactionGeneration() && nanos.getAsLong()-entry.opened<entry.ttl
                && (entry.pin==null || entry.pin.position().term()==machine.transactionTerm())
                && (entry.plan!=null || entry.planning || entry.pin==null || entry.pin.status()==RangeMutationHistory.Status.ACTIVE);
    }
    private void ensureLive(Entry entry) throws IOException { if (!live(entry)) throw new IOException("Transaction expired or leader changed"); }
    private void requireHandle(Entry entry,ScopedTransactionRpc.Handle handle) throws IOException {
        if (!owner.equals(handle.owner()) || !Objects.equals(entry.handle,handle)) throw new IOException("Transaction handle ownership changed");
    }
    private void retire(Entry entry) { entry.closed=true; entries.remove(entry.id,entry); handles.remove(entry.leaseId,entry); retired.add(entry); }
    private void finish(Entry entry,boolean retire) throws Exception {
        synchronized (this) { entry.busy=false; entry.planning=false; if (retire || !live(entry)) retire(entry); }
        cleanup();
    }
    private void cleanup() throws Exception {
        final List<Entry> release;
        synchronized (this) { release=retired.stream().filter(entry -> !entry.busy && !entry.cleaning).toList(); release.forEach(entry -> entry.cleaning=true); }
        Throwable failure=null;
        for (var entry:release) {
            boolean success=false;
            try { if (entry.plan!=null) entry.plan.close(); if (entry.pin!=null) entry.pin.close(); success=true; }
            catch (Throwable problem) { if (failure==null) failure=problem; else if (failure!=problem) failure.addSuppressed(problem); }
            finally {
                synchronized (this) {
                    entry.cleaning=false;
                    if (success && retired.remove(entry)) { chargedHandles--; scopeBytes-=entry.scopeBytes; planBytes-=entry.planBytes; }
                }
            }
        }
        if (failure instanceof Exception exception) throw exception; if (failure instanceof Error error) throw error;
    }
    synchronized int chargedHandles() { return chargedHandles; }
    synchronized long retainedPlanBytes() { return planBytes; }
    @Override public void close() throws Exception {
        synchronized (this) { closed=true; for (var entry:List.copyOf(entries.values())) retire(entry); }
        cleanup();
    }
}
