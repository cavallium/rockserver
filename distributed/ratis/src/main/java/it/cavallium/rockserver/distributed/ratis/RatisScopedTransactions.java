package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;

/** Bounded ephemeral handle ownership. No native work or close runs under the registry monitor. */
final class RatisScopedTransactions implements AutoCloseable {
    static final long STAGING_BUILD_BYTES=64L*1024*1024, STAGING_RETAINED_BYTES=32L*1024*1024;
    @FunctionalInterface interface StageFactory { ReplicatedRangeStore.ScopedStaging stage(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command) throws Exception; }
    @FunctionalInterface interface PlanFactory { RatisRangeStateMachine.ScopedCommit prepare(ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand command,RatisRangeStateMachine.ProposalDeadline deadline) throws Exception; }
    @FunctionalInterface interface PinCloser { void close(ReplicatedRangeStore.ScopedHistoryPin pin) throws Exception; }
    record Limits(int handles,long scopeBytes,long planBytes,long stagingBytes,Duration maximumTtl) {
        Limits {
            if (handles<1 || handles>4096 || scopeBytes<1 || scopeBytes>256L*1024*1024 || planBytes<DistributedCommandCodec.MAX_ENCODED_BYTES
                    || planBytes>256L*1024*1024 || stagingBytes<STAGING_BUILD_BYTES || stagingBytes>256L*1024*1024 || maximumTtl.isNegative() || maximumTtl.isZero() || maximumTtl.compareTo(Duration.ofMinutes(5))>0) throw new IllegalArgumentException("Invalid transaction handle limits");
        }
        static Limits defaults() { return new Limits(128,16L*1024*1024,16L*1024*1024,128L*1024*1024,Duration.ofSeconds(60)); }
    }
    private final RatisRangeStateMachine machine;
    private final UUID owner;
    private final Limits limits;
    private final LongSupplier nanos;
    private final PlanFactory planner;
    private final StageFactory stager;
    private final PinCloser pinCloser;
    private final Map<UUID,Entry> entries=new HashMap<>();
    private final Map<UUID,Entry> handles=new HashMap<>();
    private final Set<Entry> retired=new HashSet<>();
    private int chargedHandles;
    private long scopeBytes,planBytes,stagingBytes;
    private boolean closed;
    private static final class Entry {
        final UUID id,leaseId=UUID.randomUUID(); final long generation,opened,ttl,scopeBytes; final TransactionScope scope; final SnapshotFence snapshotFence;
        ReplicatedRangeStore.ScopedHistoryPin pin;
        ScopedTransactionRpc.Handle handle;
        RatisRangeStateMachine.ScopedCommit plan;
        RatisRangeStateMachine.ParticipantProposal participant;
        byte[] baseDigest;
        ReplicatedRangeStore.ScopedStaging staging;
        DistributedCommand stagedCommand;
        long stagingBytes;
        int planBytes;
        boolean busy,planning,closed,cleaning;
        Entry(UUID id,long generation,long opened,long ttl,long scopeBytes,TransactionScope scope,SnapshotFence snapshotFence) {
            this.id=id; this.generation=generation; this.opened=opened; this.ttl=ttl; this.scopeBytes=scopeBytes; this.scope=scope; this.snapshotFence=snapshotFence;
        }
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos) {
        this(machine,limits,nanos,machine::prepareScopedCommit);
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos,PlanFactory planner) {
        this(machine,limits,nanos,planner,ReplicatedRangeStore.ScopedHistoryPin::stage);
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos,PlanFactory planner,StageFactory stager) {
        this(machine,limits,nanos,planner,stager,ReplicatedRangeStore.ScopedHistoryPin::close);
    }
    RatisScopedTransactions(RatisRangeStateMachine machine,Limits limits,LongSupplier nanos,PlanFactory planner,StageFactory stager,PinCloser pinCloser) {
        this.pinCloser=Objects.requireNonNull(pinCloser); this.stager=Objects.requireNonNull(stager);
        this.machine=Objects.requireNonNull(machine); this.limits=Objects.requireNonNull(limits); this.nanos=Objects.requireNonNull(nanos);
        this.planner=Objects.requireNonNull(planner);
        owner=machine.transactionOwnerIncarnation();
    }
    ScopedTransactionRpc.Handle open(UUID id,TransactionScope scope,Duration ttl) throws Exception {
        return open(id,scope,ttl,null,0,() -> machine.captureTransaction(scope),null);
    }
    /** Adopt exactly one tracked native cut under its original snapshot capture identity and deadline. */
    ScopedTransactionRpc.Handle captureSnapshot(UUID id,RatisSnapshotFences homes,SnapshotFence fence,long opened,Duration ttl) throws Exception {
        return captureSnapshot(id,homes,fence,opened,ttl,null);
    }
    /** Publish ownership before any later expiration or cleanup failure can hide a captured pin. */
    ScopedTransactionRpc.Handle captureSnapshot(UUID id,RatisSnapshotFences homes,SnapshotFence fence,long opened,Duration ttl,java.util.function.Consumer<ScopedTransactionRpc.Handle> ownership) throws Exception {
        Objects.requireNonNull(homes); Objects.requireNonNull(fence);
        var scope=fence.participant(machine.getGroupId().getUuid()).scope();
        return open(id,scope,ttl,fence,opened,() -> machine.captureTrackedSnapshotGate(homes,fence),ownership);
    }
    private ScopedTransactionRpc.Handle open(UUID id,TransactionScope scope,Duration ttl,SnapshotFence fence,long opened,java.util.concurrent.Callable<ReplicatedRangeStore.ScopedHistoryPin> capture,java.util.function.Consumer<ScopedTransactionRpc.Handle> ownership) throws Exception {
        Objects.requireNonNull(id); Objects.requireNonNull(scope); Objects.requireNonNull(ttl);
        long duration=ttl.toNanos(); if (duration<=0 || duration>limits.maximumTtl().toNanos()) throw new IllegalArgumentException("Transaction TTL exceeds advertised limit");
        expire();
        if (fence!=null && nanos.getAsLong()-opened>=duration) throw new IOException("Snapshot transaction deadline expired");
        machine.checkTransactionOwner(); long generation=machine.transactionGeneration();
        final Entry entry; final ScopedTransactionRpc.Handle replay;
        synchronized (this) {
            if (closed) throw new IOException("Transaction registry is closed");
            var existing=entries.get(id);
            if (existing!=null) {
                if (!Objects.equals(existing.snapshotFence,fence) || fence!=null && existing.opened!=opened || existing.ttl!=duration
                        || !Arrays.equals(ScopedTransactionRpc.scopeBytes(existing.scope),ScopedTransactionRpc.scopeBytes(scope))) throw new IllegalArgumentException("Transaction open identity reused with different origin/declaration");
                if (existing.busy || existing.handle==null) throw new IOException("Transaction open is still running");
                entry=existing; replay=existing.handle;
            } else {
                replay=null;
                var retiring=retired.stream().filter(old -> old.id.equals(id) && (old.snapshotFence!=null || fence!=null)).findFirst().orElse(null);
                if (retiring!=null) {
                    if (!Objects.equals(retiring.snapshotFence,fence) || retiring.ttl!=duration || fence!=null && retiring.opened!=opened
                            || !Arrays.equals(ScopedTransactionRpc.scopeBytes(retiring.scope),ScopedTransactionRpc.scopeBytes(scope))) throw new IllegalArgumentException("Transaction open identity reused with different origin/declaration");
                    throw new IOException("Snapshot transaction cleanup is still running");
                }
                long bytes=RangeMutationHistory.accountedScopeBytes(scope)+(fence==null ? 0 : fence.encode().length);
                if (chargedHandles>=limits.handles() || bytes>limits.scopeBytes()-scopeBytes) throw new IOException("Transaction handle/scope admission exhausted");
                entry=new Entry(id,generation,fence==null ? nanos.getAsLong() : opened,duration,bytes,scope,fence); entry.busy=true;
                entries.put(id,entry); chargedHandles++; scopeBytes+=bytes;
            }
        }
        if (replay!=null) { if (ownership!=null) ownership.accept(replay); return replay; }
        boolean success=false;
        try {
            entry.pin=capture.call();
            var handle=new ScopedTransactionRpc.Handle(owner,entry.leaseId,generation,entry.pin.position());
            synchronized (this) {
                if (ownership==null) ensureLive(entry);
                entry.handle=handle; handles.put(entry.leaseId,entry);
            }
            if (ownership!=null) ownership.accept(handle);
            synchronized (this) { ensureLive(entry); }
            success=true; return handle;
        } finally { finish(entry,!success); }
    }
    /** Memory-only lease check after a fresh owner check; it never runs unrelated cleanup. */
    boolean snapshotLive(ScopedTransactionRpc.Handle handle) {
        Objects.requireNonNull(handle);
        try { machine.checkTransactionOwner(); } catch (IOException | RuntimeException unavailable) { return false; }
        synchronized (this) {
            var entry=handles.get(handle.id());
            return entry!=null && owner.equals(handle.owner()) && Objects.equals(entry.handle,handle) && live(entry);
        }
    }
    byte[] get(ScopedTransactionRpc.Handle handle,UUID column,byte[] key) throws Exception {
        var entry=begin(handle,false); try { var value=entry.staging==null ? entry.pin.get(column,key) : entry.staging.get(column,key); if (value!=null && value.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IOException("Transaction value exceeds reply budget"); checked(entry); return value; }
        finally { finish(entry,false); }
    }
    PhysicalRangePage page(ScopedTransactionRpc.Handle handle,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws Exception {
        var entry=begin(handle,false); try { var page=entry.staging==null ? entry.pin.page(column,lower,upper,reverse,resume,items,bytes) : entry.staging.page(column,lower,upper,reverse,resume,items,bytes); checked(entry); return page; }
        finally { finish(entry,false); }
    }
    CommandResult stage(ScopedTransactionRpc.Handle handle,DistributedCommand command) throws Exception {
        var entry=begin(handle,false); long reserved=0; ReplicatedRangeStore.ScopedStaging replacement=null;
        try {
            if (command.kind()!=DistributedCommand.Kind.DATA || command.acknowledgedThrough()!=-1
                    || !command.operationId().equals(new OperationId(handle.id(),0,0))) throw new IllegalArgumentException("Staging has no durable operation identity or ACK");
            if (entry.stagedCommand!=null) {
                if (!extendsPrefix(entry.stagedCommand,command)) throw new IllegalArgumentException("Staging retry changed or regressed the original prefix");
                if (Arrays.equals(entry.stagedCommand.payloadDigest(),command.payloadDigest())) { checked(entry); return entry.staging.result(); }
            }
            synchronized (this) {
                if (STAGING_BUILD_BYTES>limits.stagingBytes()-stagingBytes) throw new IOException("Transaction staging admission exhausted");
                stagingBytes+=STAGING_BUILD_BYTES; entry.stagingBytes+=STAGING_BUILD_BYTES; reserved=STAGING_BUILD_BYTES;
            }
            replacement=stager.stage(entry.pin,command); checked(entry);
            long retained=replacement.accountedBytes();
            if (retained>STAGING_RETAINED_BYTES) throw new IOException("Staging accounting exceeds retained bound");
            // Entry.busy excludes readers/planners; close the previous JNI owner before donating its charge.
            if (entry.staging!=null) entry.staging.close();
            synchronized (this) {
                ensureLive(entry);
                stagingBytes+=retained-entry.stagingBytes;
                entry.stagingBytes=retained;
                entry.staging=replacement; entry.stagedCommand=command; replacement=null; reserved=0;
            }
            return entry.staging.result();
        } finally {
            try { if (replacement!=null) replacement.close(); }
            finally {
                synchronized (this) { stagingBytes-=reserved; entry.stagingBytes-=reserved; }
                finish(entry,false);
            }
        }
    }
    private static boolean extendsPrefix(DistributedCommand previous,DistributedCommand next) {
        return previous.groupId().equals(next.groupId()) && previous.rangeEpoch()==next.rangeEpoch()
                && Arrays.equals(previous.schemaDigest(),next.schemaDigest())
                && prefix(previous.mutations(),next.mutations()) && prefix(previous.predicates(),next.predicates());
    }
    private static boolean prefix(List<?> previous,List<?> next) { return next.size()>=previous.size() && next.subList(0,previous.size()).equals(previous); }
    private static boolean samePlan(DistributedCommand staged,DistributedCommand intent) {
        return extendsPrefix(staged,intent) && staged.mutations().equals(intent.mutations()) && staged.predicates().equals(intent.predicates());
    }
    private void releaseStaging(Entry entry) {
        if (entry.staging==null) return;
        entry.staging.close();
        synchronized (this) { entry.staging=null; entry.stagedCommand=null; stagingBytes-=entry.stagingBytes; entry.stagingBytes=0; }
    }
    DistributedCommand plan(ScopedTransactionRpc.Handle handle,DistributedCommand command) throws Exception {
        var entry=begin(handle,true); boolean success=false;
        try {
            if (entry.participant!=null) throw new IllegalArgumentException("Participant already owns this handle");
            if (entry.plan!=null) {
                success=true; // A conflicting retry must not destroy the already frozen canonical plan.
                if (!Arrays.equals(entry.baseDigest,command.payloadDigest())) throw new IllegalArgumentException("Transaction plan retry differs from original operands");
                checked(entry); return entry.plan.command();
            }
            if (entry.stagedCommand!=null && !samePlan(entry.stagedCommand,command)) {
                success=true; throw new IllegalArgumentException("Commit differs from the staged mutation/predicate prefix");
            }
            releaseStaging(entry);
            synchronized (this) {
                if (DistributedCommandCodec.MAX_ENCODED_BYTES>limits.planBytes()-planBytes) throw new IOException("Transaction retained-plan admission exhausted");
                entry.planBytes=DistributedCommandCodec.MAX_ENCODED_BYTES; planBytes+=entry.planBytes;
                entry.planning=true;
            }
            entry.plan=planner.prepare(entry.pin,command,new RatisRangeStateMachine.ProposalDeadline(nanos,entry.opened,entry.ttl)); entry.baseDigest=command.payloadDigest();
            int actual=DistributedCommandCodec.encodedSize(entry.plan.command());
            synchronized (this) { planBytes+=actual-entry.planBytes; entry.planBytes=actual; ensureLive(entry); }
            success=true; return entry.plan.command();
        } finally { finish(entry,!success); }
    }
    byte[] authorizeParticipant(ScopedTransactionRpc.Handle handle,DistributedCommand accepted,OperationId control,UUID transaction) throws Exception {
        var entry=beginParticipant(handle); boolean preserve=true;
        try {
            if (entry.plan!=null) throw new IllegalArgumentException("Ordinary plan already owns this handle");
            var audit=accepted.acceptance();
            // In 2PC the original ACK is an eligibility header, not a child receipt-deletion
            // request. Native PREPARE accounts the frozen family; COMMIT leaves child floors alone.
            if (audit==null || !audit.snapshot().equals(handle.position()) || audit.leaderGeneration()!=handle.generation() || !audit.ownerIncarnation().equals(handle.owner()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(audit.scope()),ScopedTransactionRpc.scopeBytes(entry.scope))) throw new IllegalArgumentException("Participant accepted cut differs from original handle");
            if (entry.participant!=null) {
                var original=entry.participant.preparation();
                if (!original.transactionId().equals(transaction) || !original.operation().equals(control) || !Arrays.equals(DistributedCommandCodec.encode(original.business()),DistributedCommandCodec.encode(accepted))) throw new IllegalArgumentException("Participant authorization retry changed original image");
                try (var current=machine.reattachParticipant(transaction,control,accepted)) { checked(entry); return current.bytes(); }
            }
            if (accepted.readCertification()) {
                if (entry.stagedCommand!=null) throw new IllegalArgumentException("Read certification cannot discard staged effects");
            } else if (entry.stagedCommand==null || !samePlan(entry.stagedCommand,accepted) || !Arrays.equals(entry.staging.result().encode(),audit.result())) throw new IllegalArgumentException("Participant differs from exact staged prefix/result");
            int reserve=PreparedParticipant.Preparation.MAX_BYTES+8;
            synchronized (this) { if (reserve>limits.planBytes()-planBytes) throw new IOException("Participant retained-plan admission exhausted"); entry.planBytes=reserve; planBytes+=reserve; entry.planning=true; }
            preserve=false;
            entry.participant=machine.prepareHandleParticipant(entry.pin,accepted,control,transaction,new RatisRangeStateMachine.ProposalDeadline(nanos,entry.opened,entry.ttl));
            synchronized (this) { int bytes=entry.participant.bytes().length; planBytes+=bytes-entry.planBytes; entry.planBytes=bytes; ensureLive(entry); }
            releaseStaging(entry); preserve=true; checked(entry); return entry.participant.bytes();
        } catch (IllegalStateException failure) {
            if (entry.participant==null && entry.pin.status()==RangeMutationHistory.Status.CONFLICT) throw new RatisPreparationUnavailableException("Original participant pin failed serializable validation",failure);
            throw failure;
        } finally {
            if (!preserve && entry.participant==null) {
                synchronized (this) { planBytes-=entry.planBytes; entry.planBytes=0; }
                preserve=entry.pin.status()==RangeMutationHistory.Status.ACTIVE;
            }
            finish(entry,!preserve);
        }
    }
    void release(ScopedTransactionRpc.Handle handle) throws Exception { releaseAndDrain(handle); }
    /** Logical release is immediate; true requires actual native cleanup and accounting refund. */
    boolean releaseAndDrain(ScopedTransactionRpc.Handle handle) throws Exception {
        Objects.requireNonNull(handle); final Entry entry;
        synchronized (this) {
            if (!owner.equals(handle.owner())) return true; // The old incarnation alone owns its outstanding native work.
            var live=handles.get(handle.id());
            entry=live!=null ? live : retired.stream().filter(old -> old.leaseId.equals(handle.id())).findFirst().orElse(null);
            if (entry==null) return true;
            requireHandle(entry,handle); retire(entry);
        }
        cleanup();
        synchronized (this) { return !retired.contains(entry); }
    }
    void expire() throws Exception {
        synchronized (this) {
            for (var entry:List.copyOf(entries.values())) if (!live(entry)) retire(entry);
        }
        cleanup();
    }
    private Entry beginParticipant(ScopedTransactionRpc.Handle handle) throws Exception {
        machine.checkTransactionOwner(); expire();
        synchronized (this) {
            var entry=handles.get(handle.id());
            if (entry==null || closed || !owner.equals(handle.owner()) || !Objects.equals(entry.handle,handle) || !live(entry)) throw new RatisPreparationUnavailableException("Original participant handle expired or changed owner");
            if (entry.busy) throw new IOException("Transaction operation is already running");
            entry.busy=true; return entry;
        }
    }
    private Entry begin(ScopedTransactionRpc.Handle handle,boolean permitPlan) throws Exception {
        machine.checkTransactionOwner(); expire();
        synchronized (this) {
            var entry=handles.get(handle.id()); if (entry==null || closed) throw new IOException("Transaction expired or leader changed");
            requireHandle(entry,handle); ensureLive(entry);
            if (entry.busy) throw new IOException("Transaction operation is already running");
            if (!permitPlan && (entry.plan!=null || entry.participant!=null)) throw new IOException("Transaction plan is already frozen");
            entry.busy=true; return entry;
        }
    }
    private void checked(Entry entry) throws Exception { machine.checkTransactionOwner(); synchronized (this) { ensureLive(entry); } }
    private boolean live(Entry entry) {
        return !closed && !entry.closed && entry.generation==machine.transactionGeneration() && nanos.getAsLong()-entry.opened<entry.ttl
                && (entry.pin==null || entry.pin.position().term()==machine.transactionTerm())
                && (entry.plan!=null || entry.participant!=null || entry.planning || entry.pin==null || entry.pin.status()==RangeMutationHistory.Status.ACTIVE);
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
            try { releaseStaging(entry); if (entry.plan!=null) entry.plan.close(); if (entry.participant!=null) entry.participant.close(); if (entry.pin!=null) pinCloser.close(entry.pin); success=true; }
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
    synchronized long retainedStagingBytes() { return stagingBytes; }
    synchronized long retainedPlanBytes() { return planBytes; }
    @Override public void close() throws Exception {
        synchronized (this) { closed=true; for (var entry:List.copyOf(entries.values())) retire(entry); }
        cleanup();
    }
}
