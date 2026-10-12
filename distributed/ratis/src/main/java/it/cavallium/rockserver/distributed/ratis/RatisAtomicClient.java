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
    private final DistributedSchema schema;
    private final RangeDirectory directory;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final RWScheduler scheduler;
    private final Limits limits;
    private final byte[] binding;
    private final Map<Path,Operation> operations=new LinkedHashMap<>();
    private long chargedBytes;
    private boolean closing,closed;

    /** Clients, scheduler, and grant maintenance are borrowed. Install adequate DATA/HOME grant windows before use. */
    public RatisAtomicClient(DistributedSchema schema,RatisTransactionStatuses homes,RatisRoutedClient data,RWScheduler scheduler,Limits limits) {
        this.schema=Objects.requireNonNull(schema); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data); this.scheduler=Objects.requireNonNull(scheduler); this.limits=Objects.requireNonNull(limits);
        data.requireKnownSchema(schema); directory=data.directory(schema); homes.requireDatabaseBinding(schema.clusterId(),schema.databaseId());
        try { binding=java.security.MessageDigest.getInstance("SHA-256").digest(java.nio.ByteBuffer.allocate(96).put(schema.digest()).put(homes.identityDigest()).put(data.sessionIdentityDigest()).array()); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public Operation executeAtomic(Path journal,Identity identity,List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions,Duration ttl,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context);
        var plan=RatisTransactionPlan.logical(schema,directory,identity.business(),changes,conditions);
        return executePlan(journal,identity,plan,ttl,context,deadline);
    }
    /** Submit a fully frozen captured projection without staging again or opening replacement cuts. */
    public Operation executeAccepted(Path path,Identity identity,RatisTransactionPlan.Accepted accepted,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context); Objects.requireNonNull(identity); Objects.requireNonNull(accepted);
        var initial=accepted.initial(); initial.requireInitial();
        if (!initial.transactionId().equals(identity.transaction()) || !initial.operation().equals(identity.business())
                || !initial.clusterId().equals(schema.clusterId()) || !initial.databaseId().equals(schema.databaseId()) || !initial.domainId().equals(schema.domainId())
                || initial.schemaEpoch()!=schema.schemaEpoch() || !Arrays.equals(initial.schemaDigest(),schema.digest())
                || !Arrays.equals(initial.planDigest(),RatisDurableTransaction.planDigest(accepted.intents())) || accepted.intents().size()!=initial.participants().size()) throw new IllegalArgumentException("Accepted projection differs from reserved atomic identity/schema");
        var ordered=accepted.intents().stream().sorted(Comparator.comparing(i -> i.route().groupId())).toList();
        var groups=new HashSet<UUID>(); long bytes=64*1024L+2*1024*1024L; int n=0;
        for (var intent:ordered) {
            if (!groups.add(intent.route().groupId()) || intent.captureId()==null || !intent.acceptedBusiness().operationId().equals(identity.business())
                    || !intent.prepareControl().equals(identity.control(n)) || !intent.finalizeControl().equals(identity.control(64+n++))) throw new IllegalArgumentException("Accepted projection changed its original handles or control block");
            var participant=initial.participants().stream().filter(p -> p.groupId().equals(intent.route().groupId())).findFirst().orElseThrow();
            if (!participant.rangeId().equals(intent.route().rangeId()) || participant.rangeEpoch()!=intent.route().rangeEpoch() || !Arrays.equals(participant.planDigest(),intent.acceptedBusiness().payloadDigest())) throw new IllegalArgumentException("Accepted participant differs from HOME definition");
            data.restoreTransaction(intent); // Pure binding validation; this never issues OPEN or reads latest data.
            bytes+=2L*DistributedCommandCodec.encodedSize(intent.acceptedBusiness())+2L*SessionWindowJournal.routeSize(intent.route())+64*1024L;
        }
        if (bytes>RatisDurableTransaction.MAX_BYTES) throw new IOException("Accepted projection exceeds retained journal capacity");
        RatisDurableTransaction.validateResultPlan(accepted.intents());
        var registration=homes.prepare(identity.control(128),null,initial);
        var operation=admit(path,identity,null,null,bytes,accepted,registration); operation.beginExecution(context,deadline); return operation;
    }
    Operation executePlan(Path journal,Identity identity,RatisTransactionPlan plan,Duration ttl,RequestContext context,long deadline) throws IOException {
        Objects.requireNonNull(identity); Objects.requireNonNull(plan); Objects.requireNonNull(ttl); Objects.requireNonNull(context);
        if (!identity.business().equals(plan.operation()) || !Arrays.equals(directory.payloadDigest(),plan.directory().payloadDigest()) || ttl.isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Atomic plan/TTL differs from frozen request");
        var operation=admit(journal,identity,plan,ttl,estimate(plan)); operation.beginExecution(context,deadline); return operation;
    }
    /** Reopening reads the existing journal on a workload thread and never opens replacement server captures. */
    public Operation reopen(Path journal,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context);
        var operation=admit(journal,null,null,null,RatisDurableTransaction.MAX_BYTES); operation.beginExecution(context,deadline); return operation;
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
        if (operations.size()>=limits.operations() || bytes>limits.bytes()-chargedBytes) throw new IOException("Atomic operation/byte capacity exhausted");
        var operation=new Operation(path,identity,plan,ttl,bytes); operation.initialization=accepted; operation.registration=registration;
        operations.put(path,operation); chargedBytes+=bytes; return operation;
    }
    private synchronized void release(Operation operation) { if (operations.remove(operation.path,operation)) chargedBytes-=operation.bytes; }
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
            var digest=java.security.MessageDigest.getInstance("SHA-256"); digest.update(plan.directory().payloadDigest());
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
        private Operation(Path path,Identity identity,RatisTransactionPlan plan,Duration ttl,long bytes) { this.path=path; this.identity=identity; this.plan=plan; this.ttl=ttl; this.bytes=bytes; }
        public Path path() { return path; }
        synchronized boolean busy() { return active!=null || closingOperation; }
        synchronized boolean terminalReady() { return terminalResult!=null; }
        public boolean rejectedBeforeRegistration() { return rejected!=null; }
        public synchronized CompletableFuture<CommandResult> completion() { return execution==null ? CompletableFuture.failedFuture(new IOException("Atomic execution not admitted")) : execution.view(); }
        public CompletableFuture<CommandResult> resume(RequestContext context) { return execute(context,scheduler.resolveMonotonicDeadline(context)); }
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
            var task=new Task<Void>(this,Kind.CONSUME,scheduler.resolveMonotonicDeadline(context),work -> {
                work.check();
                if (rejected!=null) { rejected.consume(); return null; }
                journal.refreshHome(); journal.resolveHomeFromObserved(); var saved=journal.snapshot();
                if (saved.observedHome()!=null && saved.observedHome().consumed()) return null;
                journal.result();
                if (saved.pendingHome()!=null && saved.homeResult()==null && !saved.homeSuperseded()) throw new IOException("Resume the exact pending HOME request before consumption");
                journal.queueHome(homes.prepare(identity.next(saved.lastHome()),saved.observedHome(),saved.observedHome().consume()));
                work.check(); if (journal.executeHome().status()!=CommandResult.Status.SUCCESS) throw new IOException("Consumption CAS conflicted; refresh and retry this operation"); return null;
            }); submit(task,context); return task.view();
        }
        private <T> void submit(Task<T> task,RequestContext context) {
            try { requireOpen(); if (closed || closeRequested) throw new IOException("Atomic operation closing/closed"); active=task;
                scheduler.executor(context.profile(),OperationFamily.MUTATION,task.deadline).execute(task,Math.min(bytes,WorkloadCost.MAX_ESTIMATED_BYTES));
            } catch (Exception unavailable) { task.fail(unavailable); }
        }
        private CommandResult drive(Task<CommandResult> task) throws Exception {
            task.check();
            if (rejected!=null) { rejected.consumed(); closeCaptures(); return completed(new CommandResult(CommandResult.Status.CONFLICT,List.of())); }
            if (journal==null) {
                if (initialization!=null) publish();
                else if (plan==null) {
                    if (RatisAtomicRejection.isRejection(path)) { rejected=RatisAtomicRejection.reopen(path,binding); identity=rejected.identity(); return completed(new CommandResult(CommandResult.Status.CONFLICT,List.of())); }
                    var candidate=RatisDurableTransaction.reopen(path,homes,data);
                    try {
                        var saved=candidate.snapshot(); var first=saved.participants().stream().map(p -> p.intent().prepareControl()).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
                        var restored=new Identity(saved.initial().transactionId(),saved.initial().operation(),first);
                        int n=0; for (var p:saved.participants()) if (p.intent().captureId()==null || !p.intent().prepareControl().equals(restored.control(n)) || !p.intent().finalizeControl().equals(restored.control(64+n++))) throw new IOException("Journal differs from remote atomic identity block");
                        restored.requireHome(saved.lastHome()); if (saved.pendingRecovery()==null && saved.pendingHome()!=null) restored.requireHome(saved.pendingHome().command().operationId());
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
            var accepted=new HashMap<UUID,DistributedCommand>(); var prepares=new HashMap<UUID,OperationId>(); var finalizes=new HashMap<UUID,OperationId>(); var leases=new HashMap<UUID,UUID>(); int n=0;
            for (var part:plan.partitions()) {
                task.check(); var group=part.route().groupId(); var capture=captures.get(group);
                if (capture==null) { capture=data.openTransaction(part.scope(),ttl); if (!capture.descriptor().equals(part.route())) { capture.close(); throw new IOException("Atomic directory changed while opening its capture"); } captures.put(group,capture); }
                task.check();
                if (!checkedConditions.contains(group)) {
                    for (var predicate:part.command().predicates()) {
                        byte[] value=capture.get(predicate.columnId(),predicate.key()); task.check();
                        if (predicate.bucket()) value=schema.decodeBucket(predicate.columnId(),predicate.key(),value).get(predicate.memberKeys());
                        if (!Arrays.equals(value,predicate.expectedValue())) { rejectedRequest=requestDigest(plan); publishRejection(); return; }
                    }
                    checkedConditions.add(group);
                }
                var staged=capture.stage(part.command().mutations(),part.command().predicates());
                task.check();
                accepted.put(group,capture.participantIntent(part.command(),staged)); leases.put(group,capture.captureId()); prepares.put(group,identity.control(n)); finalizes.put(group,identity.control(64+n++));
            }
            task.check(); initialization=plan.accept(identity.transaction(),accepted,prepares,finalizes,leases);
            registration=homes.prepare(identity.control(128),null,initialization.initial()); publish();
        }
        private void publish() throws IOException {
            if (java.nio.file.Files.exists(path)) {
                var candidate=RatisDurableTransaction.reopen(path,homes,data);
                var saved=candidate.snapshot();
                boolean same=saved.initial().equals(initialization.initial()) && saved.participants().size()==initialization.intents().size();
                for (int n=0;same && n<saved.participants().size();n++) same=sameIntent(saved.participants().get(n).intent(),initialization.intents().get(n));
                if (!same) { candidate.close(); throw new IOException("Uncertain atomic publication differs from frozen request"); }
                journal=candidate;
            } else journal=RatisDurableTransaction.create(path,homes,data,initialization.initial(),registration,initialization.intents());
            plan=null; initialization=null; registration=null;
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
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; if (active!=null || closingOperation) throw new IOException("Atomic work active; ownership retained"); closeRequested=true; closingOperation=true; }
            try { closeCaptures(); if (journal!=null) journal.close(); if (rejected!=null) rejected.close(); synchronized (this) { closed=true; } release(this); }
            finally { synchronized (this) { closingOperation=false; } }
        }
    }
    private final class Task<T> implements Runnable,RWScheduler.RejectionAwareTask {
        final Operation operation; final Kind kind; final long deadline; final Work<T> work;
        final CompletableFuture<T> result=new CompletableFuture<>(); final AtomicInteger phase=new AtomicInteger();
        int views; volatile Throwable rejection;
        Task(Operation operation,Kind kind,long deadline,Work<T> work) { this.operation=operation; this.kind=kind; this.deadline=deadline; this.work=work; }
        synchronized CompletableFuture<T> view() {
            if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Atomic waiter capacity exhausted"));
            if (!result.isDone()) views++; return result.copy();
        }
        void check() throws IOException {
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
