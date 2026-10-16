package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Asynchronous atomic logical mutations over frozen local cuts, not a multi-group snapshot-read API. */
public final class RatisAtomicClient implements AutoCloseable {
    public static final int MAX_OPERATIONS=16,MAX_WAITERS=16,CONTROL_SLOTS=256;
    public static final long MAX_BYTES=256L*1024*1024;
    public record Limits(int operations,long bytes) {
        public Limits { if (operations<1 || operations>MAX_OPERATIONS || bytes<1 || bytes>MAX_BYTES) throw new IllegalArgumentException("Invalid atomic client limits"); }
        public static Limits defaults() { return new Limits(MAX_OPERATIONS,128L*1024*1024); }
    }
    /** The caller exclusively reserves 256 controls in an issued/bound grant; business may use a separate grant. */
    public record Identity(UUID transaction,OperationId business,OperationId firstControl) {
        public Identity {
            Objects.requireNonNull(transaction); Objects.requireNonNull(business); Objects.requireNonNull(firstControl);
            boolean sameGrant=business.sessionId().equals(firstControl.sessionId()) && business.epoch()==firstControl.epoch();
            if (business.epoch()<1 || firstControl.epoch()<1 || sameGrant && firstControl.sequence()<=business.sequence()
                    || firstControl.sequence()>Long.MAX_VALUE-CONTROL_SLOTS) throw new IllegalArgumentException("Invalid reserved atomic identities");
        }
        public OperationId lastControl() { return control(CONTROL_SLOTS-1); }
        private OperationId control(long offset) { return new OperationId(firstControl.sessionId(),firstControl.epoch(),Math.addExact(firstControl.sequence(),offset)); }
        private void requireHome(OperationId operation) throws IOException {
            if (!operation.sessionId().equals(firstControl.sessionId()) || operation.epoch()!=firstControl.epoch() || operation.sequence()<firstControl.sequence()+128 || operation.sequence()>lastControl().sequence()) throw new IOException("Atomic HOME control is outside its reserved identity block");
        }
        private OperationId next(OperationId last) throws IOException {
            requireHome(last); if (last.sequence()==lastControl().sequence()) throw new IOException("Atomic HOME control block exhausted");
            return new OperationId(last.sessionId(),last.epoch(),last.sequence()+1);
        }
    }
    public record Stats(int operations,long bytes) {}
    private final RatisTransactionStatuses homes;
    private final RatisTransactionBindings data;
    private final RWScheduler scheduler;
    private final Limits limits;
    private RatisJournalAccess journalAccess=RatisJournalAccess.standalone();
    private RatisRequestBudget budget;
    private java.util.function.Supplier<RatisRequestBudget.Lease> retainedCredit;
    @FunctionalInterface interface PublicationObserver { void published(Path path)throws IOException; }
    private final PublicationObserver publicationObserver;
    private final byte[] binding;
    private final Map<Path,Operation> operations=new LinkedHashMap<>();
    private long chargedBytes;
    private boolean closing,closed;

    /** Clients, scheduler, and grant maintenance are borrowed. Install adequate DATA/HOME grant windows before use. */
    public RatisAtomicClient(RatisTransactionBindings data,RatisTransactionStatuses homes,RWScheduler scheduler,Limits limits) {this(data,homes,scheduler,limits,path->{});}
    RatisAtomicClient(RatisTransactionBindings data,RatisTransactionStatuses homes,RWScheduler scheduler,Limits limits,PublicationObserver publicationObserver) {
        this.publicationObserver=Objects.requireNonNull(publicationObserver);this.data=Objects.requireNonNull(data);this.homes=Objects.requireNonNull(homes);this.scheduler=Objects.requireNonNull(scheduler);this.limits=Objects.requireNonNull(limits);homes.requireDatabaseBinding(data.clusterId(),data.databaseId());
        try{binding=java.security.MessageDigest.getInstance("SHA-256").digest(java.nio.ByteBuffer.allocate(64).put(homes.identityDigest()).put(data.sessionIdentityDigest()).array());}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
    }
    RatisAtomicClient(RatisTransactionBindings data,RatisTransactionStatuses homes,RWScheduler scheduler,Limits limits,RatisRequestBudget budget,java.util.function.Supplier<RatisRequestBudget.Lease> retainedCredit){this(data,homes,scheduler,limits);this.budget=Objects.requireNonNull(budget);this.retainedCredit=Objects.requireNonNull(retainedCredit);}
    static RatisAtomicClient journalOwned(RatisTransactionBindings data,RatisTransactionStatuses homes,RWScheduler scheduler,Limits limits,RatisJournalAccess access){var client=new RatisAtomicClient(data,homes,scheduler,limits);client.journalAccess=Objects.requireNonNull(access);return client;}
    private byte[] singletonDirectoryDigest(){if(data.domains().size()!=1)throw new IllegalArgumentException("Origin-bearing operation requires one domain");return data.domains().getFirst().directory().payloadDigest();}
    private static byte[] directoryDigest(List<RangeDirectory> directories){try{var digest=java.security.MessageDigest.getInstance("SHA-256");for(var domain:DomainBinding.canonical(directories.stream().map(DomainBinding::of).toList())){digest.update(domain.directoryDigest());digest.update(domain.schemaDigest());}return digest.digest();}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
    public Operation executeAtomic(Path journal,Identity identity,List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions,Duration ttl,RequestContext context) throws IOException {
        if(data.domains().size()!=1)throw new IllegalArgumentException("Cross-domain commits require the original globally captured projection");
        long deadline=scheduler.resolveMonotonicDeadline(context);
        var plan=RatisTransactionPlan.logical(data.schemas(),data.directories(),identity.business(),changes,conditions);
        return executePlan(journal,identity,plan,ttl,context,deadline);
    }
    /** Canonical normalized physical target plan, before any native acceptance or capture. */
    public static byte[] rawPlanDigest(RatisTransactionPlan plan) {
        try {
            var bytes=new java.io.ByteArrayOutputStream();var out=new java.io.DataOutputStream(bytes);
            out.write(directoryDigest(plan.directories()));out.writeLong(plan.operation().sessionId().getMostSignificantBits());out.writeLong(plan.operation().sessionId().getLeastSignificantBits());out.writeLong(plan.operation().epoch());out.writeLong(plan.operation().sequence());
            out.writeInt(plan.partitions().size());
            for(var part:plan.partitions().stream().sorted(Comparator.comparing(p->p.route().groupId(),RangeDirectoryCodec.UUID_ORDER)).toList()) {
                var command=part.command();if(command.acceptance()!=null)throw new IllegalArgumentException("Reserved plan must precede native acceptance");
                byte[] encoded=DistributedCommandCodec.encode(command);out.writeInt(encoded.length);out.write(encoded);out.writeInt(part.resultPositions().size());for(int index:part.resultPositions())out.writeInt(index);
            }
            out.flush();return java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray());
        }catch(java.io.IOException|java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}
    }
    @FunctionalInterface interface InlineCheck { void check()throws IOException; }
    /** Verify an exact existing HOME reservation before opening any frozen target captures. */
    public Operation executeReservedPlan(Path journal,Identity identity,TransactionStatus reservation,RatisTransactionPlan plan,RatisOperationRerouteAdmissionRpc.Evidence evidence,Duration ttl,RequestContext context)throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context);var operation=reservedOperation(journal,identity,reservation,plan,evidence,ttl,context);operation.beginExecution(context,deadline);return operation;
    }
    /** Caller already owns admitted MUTATION work; this performs the same driver without another queue. */
    Operation executeReservedPlanInline(Path journal,Identity identity,TransactionStatus reservation,RatisTransactionPlan plan,RatisOperationRerouteAdmissionRpc.Evidence evidence,Duration ttl,RequestContext context,InlineCheck check)throws IOException {
        Objects.requireNonNull(check);var operation=reservedOperation(journal,identity,reservation,plan,evidence,ttl,context);operation.beginInline(context,check);return operation;
    }
    private Operation reservedOperation(Path journal,Identity identity,TransactionStatus reservation,RatisTransactionPlan plan,RatisOperationRerouteAdmissionRpc.Evidence evidence,Duration ttl,RequestContext context)throws IOException {
        Objects.requireNonNull(reservation);Objects.requireNonNull(identity);Objects.requireNonNull(plan);Objects.requireNonNull(context);
        if(context.profile()==WorkloadProfile.CONTROL)throw new IOException("Reserved input preparation requires a data workload profile");
        byte[] retained=RatisDurableTransaction.validateReservationEvidence(reservation,evidence);
        var origin=reservation.origin();
        if(!reservation.reserved() || origin==null || !reservation.transactionId().equals(identity.transaction()) || !reservation.operation().equals(identity.business()) || !origin.firstControl().equals(identity.firstControl())
                || !reservation.clusterId().equals(data.clusterId()) || !reservation.databaseId().equals(data.databaseId())
                || !reservation.domains().equals(data.authority()) || !Arrays.equals(origin.targetDirectoryDigest(),singletonDirectoryDigest())
                || !Arrays.equals(origin.planDigest(),rawPlanDigest(plan)) || origin.targetParticipantCount()!=plan.partitions().size())throw new IllegalArgumentException("Reserved atomic origin differs from frozen target plan/identities");
        if(!identity.business().equals(plan.operation()) || !Arrays.equals(directoryDigest(data.directories()),directoryDigest(plan.directories())) || ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis())))throw new IllegalArgumentException("Reserved atomic plan/TTL changed");
        long bytes=estimate(plan)+3L*retained.length;if(bytes>RatisDurableTransaction.MAX_BYTES)throw new IOException("Reserved evidence future persistence exceeds journal budget");
        var operation=admit(journal,identity,plan,ttl,bytes);operation.reservation=reservation;operation.reservationEvidence=RatisOperationRerouteAdmissionRpc.Evidence.decode(retained);return operation;
    }
    /** Submit a fully frozen captured projection without staging again or opening replacement cuts. */
    public Operation executeAccepted(Path path,Identity identity,RatisTransactionPlan.Accepted accepted,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context); Objects.requireNonNull(identity); Objects.requireNonNull(accepted);
        var initial=accepted.initial(); initial.requireInitial();
        if (!initial.transactionId().equals(identity.transaction()) || !initial.operation().equals(identity.business())
                || !initial.clusterId().equals(data.clusterId()) || !initial.databaseId().equals(data.databaseId()) || !initial.domains().equals(data.authority())
                || !Arrays.equals(initial.planDigest(),RatisDurableTransaction.planDigest(accepted.intents())) || accepted.intents().size()!=initial.participants().size()) throw new IllegalArgumentException("Accepted projection differs from reserved atomic identity/schema");
        var ordered=accepted.intents().stream().sorted(Comparator.comparing(i -> i.route().groupId(),RangeDirectoryCodec.UUID_ORDER)).toList();
        var groups=new HashSet<UUID>(); int n=0;
        for (var intent:ordered) {
            if (!groups.add(intent.route().groupId()) || intent.captureId()==null || !intent.acceptedBusiness().operationId().equals(identity.business())
                    || !intent.prepareControl().equals(identity.control(n)) || !intent.finalizeControl().equals(identity.control(64+n++))) throw new IllegalArgumentException("Accepted projection changed its original handles or control block");
            var participant=initial.participants().stream().filter(p -> p.groupId().equals(intent.route().groupId())).findFirst().orElseThrow();
            if (!participant.rangeId().equals(intent.route().rangeId()) || participant.rangeEpoch()!=intent.route().rangeEpoch() || !Arrays.equals(participant.planDigest(),intent.acceptedBusiness().payloadDigest())) throw new IllegalArgumentException("Accepted participant differs from HOME definition");
            data.restoreTransaction(intent); // Pure binding validation; this never issues OPEN or reads latest data.
        }
        RatisDurableTransaction.validateResultPlan(accepted.intents());
        var expected=initial.origin()==null ? null : TransactionStatus.reserve(initial.origin(),initial.domains().getFirst());
        if(expected!=null && (!initial.origin().firstControl().equals(identity.firstControl()) || !Arrays.equals(initial.origin().targetDirectoryDigest(),singletonDirectoryDigest())))throw new IllegalArgumentException("Accepted reserved projection changed target/control binding");
        var registration=homes.prepare(identity.control(128),expected,initial);
        long bytes=RatisDurableTransaction.futureBytes(initial,accepted.intents(),registration.descriptor());
        var operation=admit(path,identity,null,null,bytes,accepted,registration); operation.beginExecution(context,deadline); return operation;
    }
    Operation executePlan(Path journal,Identity identity,RatisTransactionPlan plan,Duration ttl,RequestContext context,long deadline) throws IOException {
        Objects.requireNonNull(identity); Objects.requireNonNull(plan); Objects.requireNonNull(ttl); Objects.requireNonNull(context);
        if (!identity.business().equals(plan.operation()) || !Arrays.equals(directoryDigest(data.directories()),directoryDigest(plan.directories())) || ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Atomic plan/TTL differs from frozen request");
        var operation=admit(journal,identity,plan,ttl,estimate(plan)); operation.beginExecution(context,deadline); return operation;
    }
    /** Reopening reads the existing journal on a workload thread and never opens replacement server captures. */
    public Operation reopen(Path journal,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context);
        var operation=admit(journal,null,null,null,RatisDurableTransaction.MAX_BYTES); operation.beginExecution(context,deadline); return operation;
    }
    /** Reopen on the caller's already admitted MUTATION worker, preserving the original journal. */
    Operation reopenInline(Path journal,RequestContext context,InlineCheck check)throws IOException {
        Objects.requireNonNull(check);if(context.profile()==WorkloadProfile.CONTROL)throw new IOException("Atomic recovery requires a data workload profile");
        var operation=admit(journal,null,null,null,RatisDurableTransaction.MAX_BYTES);operation.beginInline(context,check);return operation;
    }
    /** Only the session owner may use this after forcing its application ACK. No result or capture is recreated. */
    Operation reopenForConsumptionInline(Path path,RequestContext context,InlineCheck check)throws IOException {
        Objects.requireNonNull(check);if(context.profile()==WorkloadProfile.CONTROL)throw new IOException("Atomic journal consumption requires a data workload profile");
        var operation=admit(path,null,null,null,RatisDurableTransaction.MAX_BYTES);operation.consumptionOnly=true;
        try{check.check();operation.openConsumptionJournal();return operation;}catch(IOException|RuntimeException failure){try{operation.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    public synchronized Operation operation(Path journal) { return operations.get(journal.toAbsolutePath().normalize()); }
    public synchronized Stats stats() { return new Stats(operations.size(),chargedBytes); }
    private synchronized void requireOpen() throws IOException { if (closing || closed) throw new IOException("Atomic client closing/closed"); }
    private synchronized Operation admit(Path journal,Identity identity,RatisTransactionPlan plan,Duration ttl,long bytes) throws IOException {
        return admit(journal,identity,plan,ttl,bytes,null,null);
    }
    private synchronized Operation admit(Path journal,Identity identity,RatisTransactionPlan plan,Duration ttl,long bytes,RatisTransactionPlan.Accepted accepted,RatisRoutedClient.PreparedCommand registration) throws IOException {
        requireOpen(); Path path=Objects.requireNonNull(journal).toAbsolutePath().normalize();
        if (operations.containsKey(path)) throw new IOException("Atomic journal already owned; use its existing operation handle");
        if (operations.size()>=limits.operations() || budget==null && bytes>limits.bytes()-chargedBytes) throw new IOException("Atomic operation/byte capacity exhausted");
        var credit=budget==null ? null : Objects.requireNonNull(retainedCredit.get());
        if(credit!=null) {
            if(identity==null)bytes=Math.max(1,RatisRequestBudget.files(path));
            credit.growTo(Math.max(credit.amount,retainedGeometry(bytes)));
        }
        var operation=new Operation(path,identity,plan,ttl,bytes,credit); operation.initialization=accepted; operation.registration=registration;
        operations.put(path,operation); chargedBytes+=bytes; return operation;
    }
    private synchronized void release(Operation operation) { if (operations.remove(operation.path,operation)) chargedBytes-=operation.bytes; }
    static long retainedGeometry(long bytes) { return Math.addExact(256L*1024,Math.multiplyExact(4,bytes)); }
    private static long estimate(RatisTransactionPlan plan) throws IOException {
        long bytes=64*1024L+2*1024*1024L;
        for (var part:plan.partitions()) bytes+=2L*DistributedCommandCodec.encodedSize(part.command())+2L*SessionWindowJournal.routeSize(part.route())+64*1024L;
        if (bytes>RatisDurableTransaction.MAX_BYTES) throw new IOException("Atomic retained request exceeds journal capacity"); return bytes;
    }
    private static boolean sameIntent(RatisDurableTransaction.Intent a,RatisDurableTransaction.Intent b) {
        return a.route().equals(b.route()) && a.prepareControl().equals(b.prepareControl()) && a.finalizeControl().equals(b.finalizeControl())
                && Objects.equals(a.captureId(),b.captureId()) && a.resultPositions().equals(b.resultPositions())
                && Arrays.equals(DistributedCommandCodec.encode(a.acceptedBusiness()),DistributedCommandCodec.encode(b.acceptedBusiness()));
    }
    private static byte[] requestDigest(RatisTransactionPlan plan) {
        try {
            var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(directoryDigest(plan.directories()));
            for (var part:plan.partitions()) {
                var bytes=DistributedCommandCodec.encode(part.command()); digest.update(java.nio.ByteBuffer.allocate(4).putInt(bytes.length).array()); digest.update(bytes);
                digest.update(java.nio.ByteBuffer.allocate(4).putInt(part.resultPositions().size()).array());
                for (int slot:part.resultPositions()) digest.update(java.nio.ByteBuffer.allocate(4).putInt(slot).array());
            }
            return digest.digest();
        } catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    private enum Kind { EXECUTE, CONSUME }
    @FunctionalInterface private interface Work<T> { T run(Task<T> task) throws Exception; }
    public final class Operation implements AutoCloseable {
        private final Path path; private final long bytes;
        private final RatisRequestBudget.Lease credit;
        private Identity identity; private RatisTransactionPlan plan; private final Duration ttl;
        private volatile RatisDurableTransaction journal;
        private volatile RatisAtomicRejection rejected;
        private byte[] rejectedRequest;
        private RatisTransactionPlan.Accepted initialization;
        private RatisRoutedClient.PreparedCommand registration;
        private boolean freshChecked;
        private final Map<UUID,RatisRoutedClient.ScopedTransaction> captures=new LinkedHashMap<>();
        private final Set<UUID> checkedConditions=new HashSet<>();
        private Task<?> active;
        private Task<CommandResult> execution;
        private boolean closeRequested,closingOperation,closed;
        private CommandResult terminalResult;
        private Operation(Path path,Identity identity,RatisTransactionPlan plan,Duration ttl,long bytes,RatisRequestBudget.Lease credit) { this.credit=credit;this.path=path; this.identity=identity; this.plan=plan; this.ttl=ttl; this.bytes=bytes; }
        public Path path() { return path; }
        synchronized boolean busy() { return active!=null || closingOperation; }
        synchronized boolean terminalReady() { return terminalResult!=null; }
        public boolean rejectedBeforeRegistration() { return rejected!=null; }
        public synchronized CompletableFuture<CommandResult> completion() { return execution==null ? CompletableFuture.failedFuture(new IOException("Atomic execution not admitted")) : execution.view(); }
        public CompletableFuture<CommandResult> resume(RequestContext context) { return execute(context,scheduler.resolveMonotonicDeadline(context)); }
        CommandResult resumeInline(RequestContext context,InlineCheck check)throws IOException {
            var task=beginInline(context,check);
            try{return task.result.getNow(null);}catch(CompletionException failure){if(failure.getCause() instanceof IOException io)throw io;throw new IOException("Inline atomic execution failed",failure.getCause());}
        }
        private Task<CommandResult> beginInline(RequestContext context,InlineCheck check)throws IOException {
            Objects.requireNonNull(check);if(context.profile()==WorkloadProfile.CONTROL)throw new IOException("Inline atomic work requires a data workload profile");
            final Task<CommandResult> task;
            synchronized(this) {
                requireOpen();if(closed || closeRequested || closingOperation)throw new IOException("Atomic operation closing/closed");
                if(active!=null)throw new IOException("Atomic operation already active");
                task=new Task<>(this,Kind.EXECUTE,scheduler.resolveMonotonicDeadline(context),this::drive);task.inlineCheck=check;execution=task;active=task;
            }
            task.run();return task;
        }
        private synchronized void beginExecution(RequestContext context,long deadline) {
            if (active!=null) return;
            var task=new Task<CommandResult>(this,Kind.EXECUTE,deadline,this::drive); execution=task; submit(task,context);
        }
        private synchronized CompletableFuture<CommandResult> execute(RequestContext context,long deadline) {
            if (active!=null) return active.kind==Kind.EXECUTE ? execution.view() : CompletableFuture.failedFuture(new IOException("Atomic operation already active"));
            beginExecution(context,deadline); return execution.view();
        }
        public synchronized RatisDurableTransaction.Snapshot snapshot() throws IOException { if (closed || closeRequested || journal==null) throw new IOException("Atomic journal unavailable"); return journal.snapshot(); }
        /** Explicit consumption follows durable result observation; ordinary completion and close do not consume. */
        public synchronized CompletableFuture<Void> consume(RequestContext context) {
            if (active!=null) return CompletableFuture.failedFuture(new IOException("Atomic operation already active"));
            if (journal==null && rejected==null) return CompletableFuture.failedFuture(new IOException("Atomic journal not initialized"));
            var task=new Task<Void>(this,Kind.CONSUME,scheduler.resolveMonotonicDeadline(context),this::consumeWork); submit(task,context); return task.view();
        }
        private Void consumeWork(Task<Void> work)throws Exception {
                work.check();
                if (rejected!=null) { rejected.consume(); return null; }
                try{journal.refreshHome();}catch(RatisHomeRetiredException retired){if(journal.initialRegistrationNoEffect())return null;if(!consumptionOnly || !retired.proof().equals(journal.snapshot().homeRetirementProof()))throw retired;return null;}
                journal.resolveHomeFromObserved();if(journal.initialRegistrationNoEffect())return null; var saved=journal.snapshot();
                if (saved.observedHome()!=null && saved.observedHome().consumed()) return null;
                if(consumptionOnly){if(saved.observedHome()==null || !saved.observedHome().fullyFinalized())throw new IOException("Application-ACK cleanup lacks a fully finalized HOME");}else journal.result();
                var consumed=saved.observedHome().consume();
                if (saved.pendingHome()!=null && saved.homeResult()==null && !saved.homeSuperseded() && (saved.pendingRecovery()==null || !saved.pendingRecovery().recovery().next().equals(consumed))) throw new IOException("Resume the exact pending HOME request before consumption");
                journal.queueRecovery(consumed);
                work.check();try { if (journal.executeHome().status()!=CommandResult.Status.SUCCESS) throw new IOException("Consumption CAS conflicted; refresh and retry this operation"); } catch(RatisHomeRetiredException retired) { if(!consumptionOnly || !retired.proof().equals(journal.snapshot().homeRetirementProof()))throw retired; } return null;
        }
        void consumeInline(RequestContext context,InlineCheck check)throws IOException {
            Objects.requireNonNull(check);if(context.profile()==WorkloadProfile.CONTROL)throw new IOException("Inline journal consumption requires a data workload profile");final Task<Void> task;
            synchronized(this){requireOpen();if(closed || closeRequested || active!=null || closingOperation)throw new IOException("Atomic consumption owner unavailable");if(journal==null && rejected==null)throw new IOException("Atomic journal not initialized");task=new Task<>(this,Kind.CONSUME,scheduler.resolveMonotonicDeadline(context),this::consumeWork);task.inlineCheck=check;active=task;}
            task.run();try{task.result.getNow(null);}catch(CompletionException failure){if(failure.getCause() instanceof IOException io)throw io;throw new IOException("Inline atomic consumption failed",failure.getCause());}
        }
        private <T> void submit(Task<T> task,RequestContext context) {
            try { requireOpen(); if (closed || closeRequested) throw new IOException("Atomic operation closing/closed"); active=task;
                scheduler.executor(context.profile(),OperationFamily.MUTATION,task.deadline).execute(task,Math.min(bytes,WorkloadCost.MAX_ESTIMATED_BYTES));
            } catch (Exception unavailable) { task.fail(unavailable); }
        }
        private boolean consumptionOnly;
        private void openConsumptionJournal()throws IOException {
            if(RatisAtomicRejection.isRejection(path)){rejected=RatisAtomicRejection.reopen(path,binding);identity=rejected.identity();return;}
            var candidate=openJournal();
            try {
                var saved=candidate.snapshot();var first=saved.participants().stream().map(p->p.intent().prepareControl()).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();var restored=new Identity(saved.initial().transactionId(),saved.initial().operation(),first);int n=0;
                for(var p:saved.participants())if(p.intent().captureId()==null || !p.intent().prepareControl().equals(restored.control(n)) || !p.intent().finalizeControl().equals(restored.control(64+n++)))throw new IOException("Consumption journal changed original control allocation");
                restored.requireHome(saved.lastHome());if(saved.initial().origin()!=null && (!saved.initial().origin().firstControl().equals(first) || !Arrays.equals(saved.initial().origin().targetDirectoryDigest(),singletonDirectoryDigest())))throw new IOException("Consumption journal changed reserved CONTROL grant");identity=restored;journal=candidate;
            }catch(IOException|RuntimeException failure){try{candidate.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
        }
        private TransactionStatus reservation;
        private RatisOperationRerouteAdmissionRpc.Evidence reservationEvidence;
        private boolean journalPublicationAttempted;
        private CommandResult drive(Task<CommandResult> task) throws Exception {
            task.check();
            if (rejected!=null) { rejected.consumed(); closeCaptures(); return completed(new CommandResult(CommandResult.Status.CONFLICT,List.of())); }
            if (journal==null) {
                if (initialization!=null) publish();
                else if (plan==null) {
                    if (RatisAtomicRejection.isRejection(path)) { rejected=RatisAtomicRejection.reopen(path,binding); identity=rejected.identity(); return completed(new CommandResult(CommandResult.Status.CONFLICT,List.of())); }
                    var candidate=openJournal();
                    try {
                        var saved=candidate.snapshot(); var first=saved.participants().stream().map(p -> p.intent().prepareControl()).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
                        var restored=new Identity(saved.initial().transactionId(),saved.initial().operation(),first);
                        int n=0; for (var p:saved.participants()) if (p.intent().captureId()==null || !p.intent().prepareControl().equals(restored.control(n)) || !p.intent().finalizeControl().equals(restored.control(64+n++))) throw new IOException("Journal differs from remote atomic identity block");
                        restored.requireHome(saved.lastHome());
                        if(saved.initial().origin()!=null && (!saved.initial().origin().firstControl().equals(first) || !Arrays.equals(saved.initial().origin().targetDirectoryDigest(),singletonDirectoryDigest())))throw new IOException("Reserved atomic journal changed control allocation"); if (saved.pendingRecovery()==null && saved.pendingHome()!=null) restored.requireHome(saved.pendingHome().command().operationId());
                        identity=restored; journal=candidate;
                    } catch (Exception invalid) { try { candidate.close(); } catch (IOException cleanup) { invalid.addSuppressed(cleanup); } throw invalid; }
                } else initialize(task);
            }
            if (rejected!=null) { rejected.consumed(); closeCaptures(); return completed(new CommandResult(CommandResult.Status.CONFLICT,List.of())); }
            try (var coordinator=new RatisTransactionCoordinator(journal,homes,data,identity::next,(transaction,participant) -> {
                task.check(); var intent=participant.intent(); var capture=captures.get(intent.route().groupId());
                if (capture==null) { capture=data.restoreTransaction(intent); captures.put(intent.route().groupId(),capture); }
                var authorized=capture.authorizeParticipant(intent.acceptedBusiness(),intent.prepareControl(),transaction);
                task.check();
                return new RatisTransactionCoordinator.Authorization(authorized.preparation(),authorized.negative(),() -> {});
            })) {
                for (int n=0;n<1024;n++) { task.check(); if (coordinator.step().complete()) { var result=journal.result(); closeCaptures(); return completed(result); } }
            }
            throw new IOException("Atomic work quantum exhausted; resume the same journal");
        }
        private synchronized CommandResult completed(CommandResult result) { terminalResult=result; return result; }
        private void initialize(Task<?> task) throws Exception {
            if (!freshChecked) { if (java.nio.file.Files.exists(path)) throw new IOException("Atomic journal already exists; reopen it"); freshChecked=true; }
            if (rejectedRequest!=null) { publishRejection(); return; }
            if (initialization!=null) { publish(); return; }
            if(reservation!=null) {
                var read=homes.readRegistration(reservation.transactionId());
                if(plan.partitions().stream().anyMatch(part->part.route().groupId().equals(read.descriptor().groupId())))throw new IOException("Reserved HOME must be distinct from every DATA participant group");
                var current=read.status();
                if(!reservation.equals(current))throw new IOException("Original HOME reservation changed before target captures");
            }
            var accepted=new HashMap<UUID,DistributedCommand>(); var prepares=new HashMap<UUID,OperationId>(); var finalizes=new HashMap<UUID,OperationId>(); var leases=new HashMap<UUID,UUID>(); int n=0;
            for (var part:plan.partitions()) {
                task.check(); var group=part.route().groupId(); var capture=captures.get(group);
                if (capture==null) { capture=data.client(part.route().groupId()).openTransaction(part.scope(),ttl); if (!capture.descriptor().equals(part.route())) { capture.close(); throw new IOException("Atomic directory changed while opening its capture"); } captures.put(group,capture); }
                task.check();
                if (!checkedConditions.contains(group)) {
                    for (var predicate:part.command().predicates()) {
                        byte[] value=capture.get(predicate.columnId(),predicate.key()); task.check();
                        if (predicate.bucket()) value=data.schema(predicate.columnId()).decodeBucket(predicate.columnId(),predicate.key(),value).get(predicate.memberKeys());
                        if (!Arrays.equals(value,predicate.expectedValue())) { rejectedRequest=requestDigest(plan); publishRejection(); return; }
                    }
                    checkedConditions.add(group);
                }
                var staged=capture.stage(part.command().mutations(),part.command().predicates());
                task.check();
                accepted.put(group,capture.participantIntent(part.command(),staged)); leases.put(group,capture.captureId()); prepares.put(group,identity.control(n)); finalizes.put(group,identity.control(64+n++));
            }
            task.check(); initialization=plan.accept(identity.transaction(),accepted,prepares,finalizes,leases);
            if(reservation==null)registration=homes.prepare(identity.control(128),null,initialization.initial());
            else {
                var promoted=reservation.promote(initialization.initial());initialization=new RatisTransactionPlan.Accepted(promoted,initialization.intents());
                registration=homes.prepare(identity.control(128),reservation,promoted);
            }
            publish();
        }
        private RatisDurableTransaction openJournal() throws IOException {
            return budget==null ? RatisDurableTransaction.reopenJournalOwned(path,homes,data,journalAccess)
                    : RatisDurableTransaction.reopenAccounted(path,homes,data,budget,credit);
        }
        private RatisDurableTransaction createJournal(RatisOperationRerouteAdmissionRpc.Evidence evidence) throws IOException {
            if(budget!=null)return RatisDurableTransaction.createAccounted(path,homes,data,initialization.initial(),registration,initialization.intents(),evidence,budget,credit);
            return RatisDurableTransaction.createJournalOwned(path,homes,data,initialization.initial(),registration,initialization.intents(),evidence,journalAccess);
        }
        private void publish() throws IOException {
            if (java.nio.file.Files.exists(path)) {
                var candidate=openJournal();
                var saved=candidate.snapshot();
                boolean same=saved.initial().equals(initialization.initial()) && saved.participants().size()==initialization.intents().size();
                for (int n=0;same && n<saved.participants().size();n++) same=sameIntent(saved.participants().get(n).intent(),initialization.intents().get(n));
                if (!same) { candidate.close(); throw new IOException("Uncertain atomic publication differs from frozen request"); }
                journal=candidate;
            } else {
                journalPublicationAttempted=true;
                var created=reservationEvidence==null ? createJournal(null)
                        : createJournal(reservationEvidence);
                try{publicationObserver.published(path);journal=created;}
                catch(IOException|RuntimeException|Error failure){try{created.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}throw failure;}
            }
            plan=null; initialization=null; registration=null;reservationEvidence=null;
        }
        private void publishRejection() throws IOException {
            if (java.nio.file.Files.exists(path)) {
                var candidate=RatisAtomicRejection.reopen(path,binding);
                if (!candidate.identity().equals(identity) || !Arrays.equals(candidate.requestDigest(),rejectedRequest)) { candidate.close(); throw new IOException("Uncertain atomic rejection differs from original request"); }
                rejected=candidate;
            } else rejected=RatisAtomicRejection.create(path,binding,identity,rejectedRequest);
            plan=null; rejectedRequest=null;
        }
        private void closeCaptures() throws IOException {
            IOException failure=null;
            for (var entry:List.copyOf(captures.entrySet())) try { entry.getValue().close(); captures.remove(entry.getKey()); }
            catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
            if (failure!=null) throw failure;
        }
        private boolean journalMayOwnCaptures() {
            if(journal!=null)return true;
            if(!journalPublicationAttempted)return false;
            for(var candidate:List.of(path,path.resolveSibling(path.getFileName()+".pending")))try {
                java.nio.file.Files.readAttributes(candidate,java.nio.file.attribute.BasicFileAttributes.class,java.nio.file.LinkOption.NOFOLLOW_LINKS);
                return true;
            }catch(java.nio.file.NoSuchFileException absent) {
                // Both owned publication paths must be proven absent before releasing captures.
            }catch(IOException|RuntimeException unknown){return true;}
            return false;
        }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; if (active!=null || closingOperation) throw new IOException("Atomic work active; ownership retained"); closeRequested=true; closingOperation=true; }
            try {
                // A forced unresolved journal owns the original remote handles. Releasing them here
                // would turn response-loss recovery into ABORT before participant authorization.
                // The server retains their original bounded TTL; reopening never opens replacement cuts.
                if(terminalResult==null && journalMayOwnCaptures())captures.clear();else closeCaptures();
                if (journal!=null) journal.close(); if (rejected!=null) rejected.close();
                journal=null;rejected=null;plan=null;initialization=null;registration=null;reservation=null;reservationEvidence=null;rejectedRequest=null;checkedConditions.clear();execution=null;
                synchronized (this) { closed=true; } release(this);
            }
            finally { synchronized (this) { closingOperation=false; } }
        }
    }
    private final class Task<T> implements Runnable,RWScheduler.RejectionAwareTask {
        final Operation operation; final Kind kind; final long deadline; final Work<T> work;
        final CompletableFuture<T> result=new CompletableFuture<>(); final AtomicInteger phase=new AtomicInteger();
        int views; volatile Throwable rejection; InlineCheck inlineCheck;
        Task(Operation operation,Kind kind,long deadline,Work<T> work) { this.operation=operation; this.kind=kind; this.deadline=deadline; this.work=work; }
        synchronized CompletableFuture<T> view() {
            if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Atomic waiter capacity exhausted"));
            if (!result.isDone()) views++; return result.copy();
        }
        void check() throws IOException {
            if(inlineCheck!=null)inlineCheck.check();
            if (rejection!=null) throw new IOException("Atomic task rejected; journal retained",rejection);
            if (deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Atomic deadline expired; journal retained",new TimeoutException());
        }
        public void run() {
            if (!phase.compareAndSet(0,1)) return; T value=null; Throwable failure=null;
            try { check(); value=work.run(this); check(); } catch (Throwable unavailable) { failure=unavailable; }
            phase.set(2); finish(value,failure);
            if (failure instanceof Error fatal) throw fatal;
        }
        void fail(Throwable failure) { if (phase.compareAndSet(0,2)) finish(null,failure); else if (phase.get()==1) rejection=failure; }
        public void reject(RuntimeException failure) { fail(failure); }
        void finish(T value,Throwable failure) {
            synchronized (operation) { if (operation.active==this) operation.active=null; }
            if (failure==null) result.complete(value); else result.completeExceptionally(failure);
        }
    }
    @Override public void close() throws IOException {
        List<Operation> owners; synchronized (this) { if (closed) return; closing=true; owners=List.copyOf(operations.values()); }
        IOException failure=null; for (var owner:owners) try { owner.close(); } catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
        if (failure!=null) throw failure; synchronized (this) { closed=true; }
    }
}
