package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Scoped consistent snapshots with frozen native owners. Borrowed infrastructure remains caller-owned. */
public final class RatisSnapshotClient implements AutoCloseable {
    public static final int MAX_LEASES=16,MAX_READS=32,MAX_WAITERS=16,MAX_CURSORS=128;
    public static final int CONTROL_SLOTS=512;
    public static final long MAX_BYTES=256L*1024*1024;
    private static final long POINT_BYTES=8L*1024*1024,PAGE_BYTES=WorkloadCost.MAX_ESTIMATED_BYTES;
    public record Limits(int leases,long bytes) {
        public Limits { if (leases<1 || leases>MAX_LEASES || bytes<1 || bytes>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot client limits"); }
        public static Limits defaults() { return new Limits(MAX_LEASES,128L*1024*1024); }
    }
    public record Stats(int leases,int reads,int cursors,long bytes) {}
    /** Exclusive caller reservation in one issued grant, including enough HOME progress slots for 64 groups. */
    public record Identity(UUID fence,OperationId business,OperationId firstControl) {
        public Identity {
            Objects.requireNonNull(fence); Objects.requireNonNull(business); Objects.requireNonNull(firstControl);
            if (business.epoch()<1 || !business.sessionId().equals(firstControl.sessionId()) || business.epoch()!=firstControl.epoch() || firstControl.sequence()<=business.sequence() || firstControl.sequence()>Long.MAX_VALUE-CONTROL_SLOTS) throw new IllegalArgumentException("Invalid reserved snapshot identity block");
        }
        public OperationId lastControl() { return new OperationId(firstControl.sessionId(),firstControl.epoch(),firstControl.sequence()+CONTROL_SLOTS-1); }
    }
    public RatisRoutedClient.SnapshotTarget restoreExportTarget(SnapshotFence fence,RatisExportJournal.SourceOwner owner,SnapshotFenceStatus.Capture capture) {
        return data.restoreSnapshotTarget(fence,owner.route(),owner.replica(),capture.ownerIncarnation());
    }
    private final DistributedSchema schema;
    private final RatisSnapshotFences homes;
    private final RatisRoutedClient data;
    private final RWScheduler scheduler;
    private final Limits limits;
    private final RatisSnapshotCaptureRequest.Mode mode;
    private final Map<Path,Lease> leases=new LinkedHashMap<>();
    private long chargedBytes;
    private int reads,cursors;
    private boolean closing,closed;
    public RatisSnapshotClient(DistributedSchema schema,RatisSnapshotFences homes,RatisRoutedClient data,RWScheduler scheduler,Limits limits) {
        this(schema,homes,data,scheduler,limits,RatisSnapshotCaptureRequest.Mode.READ_ONLY);
    }
    RatisSnapshotClient(DistributedSchema schema,RatisSnapshotFences homes,RatisRoutedClient data,RWScheduler scheduler,Limits limits,RatisSnapshotCaptureRequest.Mode mode) {
        this.mode=Objects.requireNonNull(mode); this.schema=Objects.requireNonNull(schema); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data); this.scheduler=Objects.requireNonNull(scheduler); this.limits=Objects.requireNonNull(limits);
        schema.requireMatches(data.directory()); homes.requireDatabaseBinding(schema.clusterId(),schema.databaseId());
    }
    RatisSnapshotCaptureRequest.Mode captureMode() { return mode; }
    public byte[] identityDigest() {
        var bytes=java.nio.ByteBuffer.allocate(97).put(data.sessionIdentityDigest()).put(homes.identityDigest()).put(schema.digest()).put((byte)(mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY ? 0 : 1)).array();
        try { return java.security.MessageDigest.getInstance("SHA-256").digest(bytes); }
        catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    /** Frozen capture definition is checked against cached placement before any source RPC. */
    public Lease openFrozen(Path path,SnapshotFence expected,Duration ttl,RequestContext context) throws IOException {
        Objects.requireNonNull(expected); validateTtl(ttl); long deadline=scheduler.resolveMonotonicDeadline(context);
        var home=homes.route(expected.fenceId());
        if (!home.groupId().equals(expected.homeGroup()) || home.rangeEpoch()!=expected.homeEpoch()) throw new IOException("Frozen source HOME placement changed");
        final RatisSnapshotPlan plan;
        try { plan=RatisSnapshotPlan.frozen(data.directory(),expected); }
        catch (IllegalArgumentException invalid) { throw new IOException("Frozen source definition differs from current cached projection",invalid); }
        long bytes=64*1024L+3L*expected.encode().length+128*1024L*plan.routes().size(); for (var route:plan.routes()) bytes+=2L*SessionWindowJournal.routeSize(route);
        if (bytes>RatisDurableSnapshot.MAX_BYTES) throw new IOException("Snapshot retained metadata exceeds journal capacity");
        var lease=admit(path,plan,ttl,bytes,expected); lease.beginAcquire(context,deadline); return lease;
    }
    public Lease reopenFrozen(Path path,SnapshotFence expected,RequestContext context) throws IOException {
        Objects.requireNonNull(expected);
        if (mode!=RatisSnapshotCaptureRequest.Mode.READ_ONLY) throw new IOException("Mutable frontend cannot reopen its prefix");
        long deadline=scheduler.resolveMonotonicDeadline(context); var lease=admit(path,null,null,RatisDurableSnapshot.MAX_BYTES,expected); lease.beginAcquire(context,deadline); return lease;
    }
    public Lease open(Path path,Identity identity,TransactionScope scope,Duration ttl,RequestContext context) throws IOException {
        long deadline=scheduler.resolveMonotonicDeadline(context); Objects.requireNonNull(identity); validateTtl(ttl);
        var home=homes.route(identity.fence());
        var plan=RatisSnapshotPlan.create(data.directory(),scope,identity.fence(),identity.business(),home.groupId(),home.rangeEpoch(),identity.firstControl());
        long bytes=64*1024L+3L*plan.fence().encode().length+128*1024L*plan.routes().size(); for (var route:plan.routes()) bytes+=2L*SessionWindowJournal.routeSize(route);
        if (bytes>RatisDurableSnapshot.MAX_BYTES) throw new IOException("Snapshot retained metadata exceeds journal capacity");
        var lease=admit(path,plan,ttl,bytes,null); lease.beginAcquire(context,deadline); return lease;
    }
    public Lease reopen(Path path,RequestContext context) throws IOException {
        if (mode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED) throw new IOException("Writable frontend prefixes are ephemeral; resume the original lease");
        long deadline=scheduler.resolveMonotonicDeadline(context); var lease=admit(path,null,null,RatisDurableSnapshot.MAX_BYTES,null); lease.beginAcquire(context,deadline); return lease;
    }
    /** Recover only durable cleanup direction; no frontend prefix or read authority is reconstructed. */
    Lease reopenForRelease(Path path,RequestContext context) throws IOException {
        requireOpen(); Objects.requireNonNull(context);
        var lease=admit(path,null,null,RatisDurableSnapshot.MAX_BYTES,null); lease.release(context); return lease;
    }
    Lease reopenForReleaseFrozen(Path path,SnapshotFence expected,RequestContext context) throws IOException {
        Objects.requireNonNull(expected); requireOpen(); var lease=admit(path,null,null,RatisDurableSnapshot.MAX_BYTES,expected); lease.release(context); return lease;
    }
    public synchronized Lease lease(Path path) { return leases.get(path.toAbsolutePath().normalize()); }
    public synchronized Stats stats() { return new Stats(leases.size(),reads,cursors,chargedBytes); }
    private synchronized void requireOpen() throws IOException { if (closing || closed) throw new IOException("Snapshot client closing/closed"); }
    private synchronized Lease admit(Path file,RatisSnapshotPlan plan,Duration ttl,long bytes,SnapshotFence expectedFence) throws IOException {
        requireOpen(); Path path=Objects.requireNonNull(file).toAbsolutePath().normalize();
        if (leases.containsKey(path)) throw new IOException("Snapshot journal already owned; reuse its lease");
        if (leases.size()>=limits.leases() || bytes>limits.bytes()-chargedBytes) throw new IOException("Snapshot lease/byte capacity exhausted");
        var lease=new Lease(path,plan,ttl,bytes,expectedFence); leases.put(path,lease); chargedBytes+=bytes; return lease;
    }
    private synchronized void reserve(long bytes,boolean read,boolean cursor) throws IOException {
        requireOpen(); if (bytes>limits.bytes()-chargedBytes || read && reads>=MAX_READS || cursor && cursors>=MAX_CURSORS) throw new IOException("Snapshot reader/cursor/byte capacity exhausted");
        chargedBytes+=bytes; if (read) reads++; if (cursor) cursors++;
    }
    private synchronized void refund(long bytes,boolean read,boolean cursor) { chargedBytes-=bytes; if (read) reads--; if (cursor) cursors--; }
    private synchronized void remove(Lease lease) { if (leases.remove(lease.path,lease)) chargedBytes-=lease.bytes; }
    private static void validateTtl(Duration ttl) { if (Objects.requireNonNull(ttl).isZero() || ttl.isNegative() || ttl.compareTo(Duration.ofSeconds(60))>0 || !ttl.equals(Duration.ofMillis(ttl.toMillis()))) throw new IllegalArgumentException("Snapshot TTL must be 1..60000 whole milliseconds"); }
    /** Per-group headers are not duplicated in the global result's single eight-byte header. */
    static long checkedResultBytes(long total,CommandResult result) throws IOException {
        if (total<8 || total>CommandResult.MAX_BYTES) throw new IllegalArgumentException("Invalid aggregate staged-result accounting");
        long next=total+Objects.requireNonNull(result).encode().length-8;
        if (next>CommandResult.MAX_BYTES) throw new IOException("Aggregate staged result exceeds transaction result budget; prefix remains uncertain");
        return next;
    }
    private enum Kind { ACQUIRE, RELEASE, STAGE, GET, PAGE }
    @FunctionalInterface private interface Work<T> { T run(Task<T> task) throws Exception; }
    public final class Lease implements AutoCloseable {
        private final Path path; private final long bytes;
        private RatisSnapshotPlan plan; private Duration ttl;
        private final SnapshotFence expectedFence;
        private volatile RatisDurableSnapshot journal;
        private record ReadBinding(TransactionScope scope,List<RatisLogicalSnapshotPages.Owner> owners,RatisLogicalSnapshotPages.OwnerReader reader,Map<UUID,RatisRoutedClient.ScopedTransaction> captures) {}
        private volatile ReadBinding binding;
        private volatile boolean completed,lost;
        private boolean releaseRequested,closeRequested,closingLease,closed,creationAttempted;
        private Task<?> control;
        private Task<Void> acquisition;
        private Task<Void> releasing;
        private Task<Map<UUID,CommandResult>> staging;
        private RatisTransactionPlan pendingPrefix,stagedPrefix;
        private final Map<UUID,CommandResult> pendingResults=new LinkedHashMap<>();
        private Map<UUID,CommandResult> stagedResults=Map.of();
        private long pendingPrefixBytes,stagedPrefixBytes,pendingResultBytes=8;
        private boolean commitFrozen,commitFinished;
        private final Set<Task<?>> reading=new HashSet<>();
        private final Set<Cursor> borrowedCursors=new HashSet<>();
        private Lease(Path path,RatisSnapshotPlan plan,Duration ttl,long bytes,SnapshotFence expectedFence) { this.path=path; this.plan=plan; this.ttl=ttl; this.bytes=bytes; this.expectedFence=expectedFence; }
        boolean initialized() { return journal!=null; }
        synchronized boolean uninitializedFailure() { return journal==null && control==null && acquisition!=null && acquisition.result.isCompletedExceptionally(); }
        public Path path() { return path; }
        public synchronized RatisDurableSnapshot.Snapshot snapshot() throws IOException { if (closed || closeRequested || journal==null) throw new IOException("Snapshot journal unavailable"); return journal.snapshot(); }
        public synchronized CompletableFuture<Void> completion() { return acquisition==null ? CompletableFuture.failedFuture(new IOException("Snapshot acquisition not admitted")) : acquisition.view(); }
        private synchronized void beginAcquire(RequestContext context,long deadline) { if (control!=null) return; var task=new Task<Void>(this,Kind.ACQUIRE,deadline,0,OperationFamily.MUTATION,this::acquire); acquisition=task; submitControl(task,context); }
        public synchronized CompletableFuture<Void> resume(RequestContext context) {
            if (releaseRequested) return release(context);
            if (pendingPrefix!=null) return CompletableFuture.failedFuture(new IOException("Resume the exact uncertain prefix through stagePrefix"));
            if (control!=null) return control.kind==Kind.ACQUIRE ? acquisition.view() : CompletableFuture.failedFuture(new IOException("Snapshot control already active"));
            beginAcquire(context,scheduler.resolveMonotonicDeadline(context)); return acquisition.view();
        }
        private void requireIdleControl() throws IOException { if (closed || closeRequested) throw new IOException("Snapshot lease closing/closed"); if (control!=null || !reading.isEmpty()) throw new IOException("Snapshot work active; ownership retained"); }
        private <T> void submitControl(Task<T> task,RequestContext context) {
            try { if (task.kind==Kind.RELEASE) { synchronized (RatisSnapshotClient.this) { if (closed) throw new IOException("Snapshot client closed"); } } else requireOpen(); requireIdleControl(); control=task; (task.kind==Kind.RELEASE ? scheduler.executor(WorkloadProfile.CONTROL,OperationFamily.CONTROL,task.deadline) : scheduler.executor(context.profile(),task.family,task.deadline)).execute(task,Math.min(bytes+task.bytes,WorkloadCost.MAX_ESTIMATED_BYTES)); }
            catch (Exception failure) { task.fail(failure); }
        }
        private <T> CompletableFuture<T> read(Kind kind,OperationFamily family,long cost,RequestContext context,long deadline,Work<T> work,Cursor cursor) {
            try { return startRead(kind,family,cost,context,deadline,work,cursor).view(); }
            catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private <T> Task<T> startRead(Kind kind,OperationFamily family,long cost,RequestContext context,long deadline,Work<T> work,Cursor cursor) throws IOException {
            requireOpen();
            Task<T> task;
            synchronized (this) {
                owners(); if (control!=null || reading.size()>=16) throw new IOException("Snapshot control or reader capacity busy"); reserve(cost,true,false);
                task=new Task<>(this,kind,deadline,cost,family,work); task.cursor=cursor; reading.add(task);
                try { scheduler.executor(context.profile(),family,task.deadline).execute(task,cost); } catch (RuntimeException failure) { task.fail(failure); }
            }
            return task;
        }
        private List<RatisLogicalSnapshotPages.Owner> owners() throws IOException {
            if (closed || closeRequested || releaseRequested || lost) throw new RatisSnapshotLostException("Snapshot lease closed, released or lost");
            if (pendingPrefix!=null || commitFrozen) throw new IOException("Writable prefix uncertain or commit frozen");
            if (!completed || journal==null) throw new IOException("Snapshot acquisition is not complete");
            var current=binding; if (current==null) throw new RatisSnapshotLostException("Original snapshot capture vector is unavailable"); return current.owners();
        }
        private ReadBinding binding(RatisDurableSnapshot.Snapshot saved) throws IOException {
            if (saved.captureMode()!=mode) throw new IOException("Snapshot journal capture mode differs from client");
            if (saved.releaseRequested() || saved.observedHome()==null || saved.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE) throw new RatisSnapshotLostException("Snapshot has no usable COMPLETE image");
            var owners=new ArrayList<RatisLogicalSnapshotPages.Owner>();
            for (var p:saved.participants()) {
                if (p.capture()==null || p.captureToken()==null || p.captureLost() || p.captureReleased() || p.captureReleaseLost()) throw new RatisSnapshotLostException("Original snapshot owner capture is unavailable");
                owners.add(new RatisLogicalSnapshotPages.Owner(p.capture().target().descriptor(),p.capture().target(),p.captureToken(),saved.initial().fence().participant(p.route().groupId()).scope()));
            }
            var captures=new LinkedHashMap<UUID,RatisRoutedClient.ScopedTransaction>();
            if (mode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED) for (var p:saved.participants()) {
                if (p.certifiedHandle()==null) throw new IOException("Writable capture has no original certified handle");
                captures.put(p.route().groupId(),data.capturedTransaction(p.capture(),new RatisSnapshotCaptureRequest.Result(p.captureToken(),p.certifiedHandle())));
            }
            var reader=new RatisLogicalSnapshotPages.OwnerReader() {
                public byte[] get(RatisLogicalSnapshotPages.Owner owner,UUID column,byte[] key) throws IOException {
                    return mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY ? data.snapshotGet(owner.target(),owner.token(),column,key) : captures.get(owner.route().groupId()).get(column,key);
                }
                public PhysicalRangePage page(RatisLogicalSnapshotPages.Owner owner,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException {
                    return mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY ? data.snapshotPage(owner.target(),owner.token(),column,lower,upper,reverse,resume,items,bytes) : captures.get(owner.route().groupId()).page(column,lower,upper,reverse,resume,items,bytes);
                }
            };
            return new ReadBinding(RatisSnapshotPlan.scope(saved.initial().fence()),List.copyOf(owners),reader,Map.copyOf(captures));
        }
        public CompletableFuture<Buf> get(long column,Keys key,RequestContext context) {
            long deadline=scheduler.resolveMonotonicDeadline(context); Keys frozen=freeze(Objects.requireNonNull(key));
            return read(Kind.GET,OperationFamily.POINT_LOOKUP,POINT_BYTES,context,deadline,task -> { task.check(); return RatisLogicalSnapshotPages.get(binding.reader(),owners(),scope(),schema,column,frozen); },null);
        }
        public CompletableFuture<RangePage<KV>> page(long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
            long deadline=scheduler.resolveMonotonicDeadline(context); Keys low=freeze(lower), high=freeze(upper), after=freeze(resume); Objects.requireNonNull(budget);
            return read(Kind.PAGE,OperationFamily.RANGE_PAGE,PAGE_BYTES,context,deadline,task -> { task.check(); return RatisLogicalSnapshotPages.read(binding.reader(),owners(),scope(),schema,column,low,high,reverse,after,budget); },null);
        }
        private TransactionScope scope() throws IOException { var current=binding; if (current==null) throw new RatisSnapshotLostException("Snapshot read scope unavailable"); return current.scope(); }
        private OperationId firstControl() throws IOException {
            var fence=journal.snapshot().initial().fence(); return fence.participants().stream().map(SnapshotFence.Participant::installOperation).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
        }
        private OperationId nextHome(OperationId last) throws IOException {
            var first=firstControl();
            if (!last.sessionId().equals(first.sessionId()) || last.epoch()!=first.epoch() || last.sequence()<first.sequence()+128 || last.sequence()>=first.sequence()+CONTROL_SLOTS-1) throw new IOException("Snapshot HOME control block exhausted or changed");
            return new OperationId(last.sessionId(),last.epoch(),last.sequence()+1);
        }
        private void validateIdentity(RatisDurableSnapshot candidate) throws IOException {
            if (candidate.mode()!=mode) throw new IOException("Snapshot journal capture mode differs from client");
            var saved=candidate.snapshot(); var fence=saved.initial().fence();
            if (expectedFence!=null && !expectedFence.equals(fence)) throw new IOException("Original source journal differs from frozen expected fence");
            if (!fence.clusterId().equals(schema.clusterId()) || !fence.databaseId().equals(schema.databaseId()) || !fence.domainId().equals(schema.domainId()) || fence.schemaEpoch()!=schema.schemaEpoch() || !Arrays.equals(fence.schemaDigest(),schema.digest())) throw new IOException("Snapshot journal schema differs from this client");
            var first=fence.participants().stream().map(SnapshotFence.Participant::installOperation).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow(); new Identity(fence.fenceId(),fence.operation(),first);
            int n=0; for (var p:fence.participants()) {
                OperationId install=new OperationId(first.sessionId(),first.epoch(),first.sequence()+n), release=new OperationId(first.sessionId(),first.epoch(),first.sequence()+64+n++);
                if (!p.installOperation().equals(install) || !p.releaseOperation().equals(release)) throw new IOException("Snapshot journal control identities changed");
            }
            var last=saved.lastHome(); if (!last.sessionId().equals(first.sessionId()) || last.epoch()!=first.epoch() || last.sequence()<first.sequence()+128 || last.sequence()>first.sequence()+CONTROL_SLOTS-1) throw new IOException("Snapshot HOME control outside reserved block");
            if (saved.requestedTtl()==null) throw new IOException("Snapshot client journal lacks its original declared lifetime"); RatisSnapshotPlan.scope(fence);
        }
        private void initialize() throws IOException {
            if (plan==null) {
                var candidate=RatisDurableSnapshot.reopen(path,homes,data);
                try { validateIdentity(candidate); journal=candidate; }
                catch (Exception failure) { try { candidate.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Invalid snapshot client journal",failure); }
            } else {
                if (!creationAttempted && Files.exists(path)) throw new IOException("Snapshot journal exists; reopen it"); creationAttempted=true;
                var initial=SnapshotFenceStatus.begin(plan.fence()); var first=plan.fence().participants().stream().map(SnapshotFence.Participant::installOperation).min(Comparator.comparingLong(OperationId::sequence)).orElseThrow();
                if (Files.exists(path)) {
                    var candidate=RatisDurableSnapshot.reopen(path,homes,data);
                    try { if (!candidate.snapshot().initial().equals(initial) || !ttl.equals(candidate.snapshot().requestedTtl())) throw new IOException("Uncertain snapshot publication changed its frozen definition/lifetime"); validateIdentity(candidate); journal=candidate; }
                    catch (Exception failure) { try { candidate.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } if (failure instanceof IOException io) throw io; throw new IOException("Invalid uncertain snapshot publication",failure); }
                } else journal=RatisDurableSnapshot.create(path,homes,data,initial,homes.prepare(new OperationId(first.sessionId(),first.epoch(),first.sequence()+128),null,initial),plan.routes(),ttl,mode);
                plan=null;
            }
            ttl=journal.snapshot().requestedTtl();
        }
        private Void acquire(Task<Void> task) throws Exception {
            task.check(); if (journal==null) initialize();
            try (var coordinator=new RatisSnapshotCoordinator(journal,homes,data,this::nextHome,UUID::randomUUID,ttl)) {
                for (int n=0;n<1024;n++) {
                    task.check(); if (coordinator.step().complete()) {
                        var saved=journal.snapshot(); if (saved.observedHome()==null || saved.observedHome().phase()!=SnapshotFenceStatus.Phase.COMPLETE) throw new RatisSnapshotLostException("Snapshot acquisition aborted or expired");
                        var acquired=binding(saved);
                        task.committed=() -> { binding=acquired; completed=true; }; return null;
                    }
                }
            }
            throw new IOException("Snapshot acquisition quantum exhausted; resume this journal");
        }
        /** Explicit durable close intent, gate cleanup, consumption and exact owner-pin release. */
        public synchronized CompletableFuture<Void> release(RequestContext context) {
            if (commitFrozen && !commitFinished) return CompletableFuture.failedFuture(new IOException("Atomic commit still owns captured native handles"));
            if (control!=null) return control.kind==Kind.RELEASE ? releasing.view() : CompletableFuture.failedFuture(new IOException("Snapshot control active"));
            if (!reading.isEmpty()) return CompletableFuture.failedFuture(new IOException("Snapshot readers active; ownership retained"));
            releaseRequested=true; for (var cursor:List.copyOf(borrowedCursors)) cursor.close();
            var task=new Task<Void>(this,Kind.RELEASE,scheduler.resolveMonotonicDeadline(context),0,OperationFamily.MUTATION,this::releasePins); releasing=task; submitControl(task,context); return task.view();
        }
        synchronized CompletableFuture<Void> releaseCompletion() {
            return releasing==null ? CompletableFuture.failedFuture(new IOException("Snapshot release not admitted")) : releasing.view();
        }
        private Void releasePins(Task<Void> task) throws Exception {
            task.check(); if (journal==null) initialize(); journal.requestRelease();
            try (var coordinator=new RatisSnapshotCoordinator(journal,homes,data,this::nextHome,UUID::randomUUID,ttl)) {
                boolean done=false; for (int n=0;n<1024;n++) { task.check(); if (coordinator.step().complete()) { done=true; break; } }
                if (!done) throw new IOException("Snapshot gate cleanup quantum exhausted");
            }
            task.check(); var saved=journal.snapshot();
            if (saved.homeRetirementProof()==null) {
                if (saved.observedHome()==null || !saved.observedHome().terminal()) throw new IOException("Snapshot release has no terminal HOME");
                if (!saved.observedHome().consumed()) { journal.consume(nextHome(saved.lastHome())); task.check(); if (journal.executeHome().status()!=CommandResult.Status.SUCCESS) throw new IOException("Snapshot consumption conflicted; retry release"); }
            }
            for (var participant:journal.snapshot().participants()) {
                task.check(); var result=journal.releaseCapture(participant.route().groupId());
                if (result==RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED) { task.check(); journal.releaseCapture(participant.route().groupId()); }
            }
            return null;
        }
        private long prefixBytes(RatisTransactionPlan prefix) throws IOException {
            boolean returning=prefix.partitions().stream().anyMatch(part -> !part.resultPositions().isEmpty());
            // One aggregate final result and one active reply, rather than a full reply per group.
            long bytes=65536L+136L*prefix.partitions().size()+(returning ? 2L*CommandResult.MAX_BYTES : 8);
            for (var part:prefix.partitions()) bytes+=2L*DistributedCommandCodec.encodedSize(part.command());
            if (bytes>2L*RatisTransactionPlan.MAX_BYTES+2L*CommandResult.MAX_BYTES+65536+136L*SnapshotFence.MAX_PARTICIPANTS) throw new IOException("Writable prefix retained budget exceeded");
            return bytes;
        }
        private boolean samePrefix(RatisTransactionPlan a,RatisTransactionPlan b) {
            if (!a.operation().equals(b.operation()) || !Arrays.equals(a.directory().payloadDigest(),b.directory().payloadDigest()) || a.partitions().size()!=b.partitions().size()) return false;
            for (int n=0;n<a.partitions().size();n++) {
                var x=a.partitions().get(n); var y=b.partitions().get(n);
                if (!x.route().equals(y.route()) || !x.resultPositions().equals(y.resultPositions()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(x.scope()),ScopedTransactionRpc.scopeBytes(y.scope())) || !Arrays.equals(DistributedCommandCodec.encode(x.command()),DistributedCommandCodec.encode(y.command()))) return false;
            }
            return true;
        }
        private boolean extendsPrefix(RatisTransactionPlan before,RatisTransactionPlan next) {
            if (!before.operation().equals(next.operation()) || before.partitions().size()!=next.partitions().size()) return false;
            for (int n=0;n<before.partitions().size();n++) {
                var a=before.partitions().get(n); var b=next.partitions().get(n);
                if (!a.route().equals(b.route()) || !prefix(a.command().mutations(),b.command().mutations()) || !prefix(a.command().predicates(),b.command().predicates()) || !prefix(a.resultPositions(),b.resultPositions())) return false;
            }
            return true;
        }
        private boolean prefix(List<?> before,List<?> next) { return next.size()>=before.size() && next.subList(0,before.size()).equals(before); }
        private void requireProjection(RatisTransactionPlan prefix) throws IOException {
            var mutations=new ArrayList<Mutation>(); var predicates=new ArrayList<KeyPredicate>();
            for (var part:prefix.partitions()) { mutations.addAll(part.command().mutations()); predicates.addAll(part.command().predicates()); }
            // Rebuild only for validating the complete original capture vector. Result positions are
            // caller-global ordering and need not follow the grouped flattened validation operands.
            var checked=RatisTransactionPlan.captured(prefix.directory(),prefix.operation(),mutations,predicates,snapshot());
            if (checked.partitions().size()!=prefix.partitions().size()) throw new IllegalArgumentException("Prefix omits original captured participants");
            for (int n=0;n<checked.partitions().size();n++) {
                var a=checked.partitions().get(n); var b=prefix.partitions().get(n);
                if (!a.route().equals(b.route()) || !Arrays.equals(ScopedTransactionRpc.scopeBytes(a.scope()),ScopedTransactionRpc.scopeBytes(b.scope())) || !Arrays.equals(DistributedCommandCodec.encode(a.command()),DistributedCommandCodec.encode(b.command()))) throw new IllegalArgumentException("Prefix changes original captured projection");
            }
        }
        synchronized CompletableFuture<Map<UUID,CommandResult>> stagePrefix(RatisTransactionPlan prefix,RequestContext context) {
            try {
                Objects.requireNonNull(prefix); if (mode!=RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED || commitFrozen) throw new IOException("Writable staging is unavailable");
                if (control!=null) {
                    if (control.kind==Kind.STAGE && samePrefix(pendingPrefix,prefix)) return staging.view();
                    throw new IOException("Snapshot control active or prefix changed");
                }
                if (closed || closeRequested || releaseRequested || lost || !completed || binding==null) throw new IOException("Writable lease unavailable");
                requireIdleControl(); requireProjection(prefix);
                if (pendingPrefix!=null && !samePrefix(pendingPrefix,prefix)) throw new IOException("Resume the exact uncertain full prefix");
                if (pendingPrefix==null) {
                    if (stagedPrefix!=null && samePrefix(stagedPrefix,prefix)) return CompletableFuture.completedFuture(stagedResults);
                    if (stagedPrefix!=null && !extendsPrefix(stagedPrefix,prefix)) throw new IllegalArgumentException("Staged prefix changed or regressed");
                    long bytes=prefixBytes(prefix); reserve(bytes,false,false); pendingPrefixBytes=bytes; pendingPrefix=prefix; pendingResults.clear(); pendingResultBytes=8;
                }
                var task=new Task<Map<UUID,CommandResult>>(this,Kind.STAGE,scheduler.resolveMonotonicDeadline(context),pendingPrefixBytes,OperationFamily.MUTATION,this::stagePending); staging=task; submitControl(task,context); return task.view();
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        private Map<UUID,CommandResult> stagePending(Task<Map<UUID,CommandResult>> task) throws Exception {
            for (var part:pendingPrefix.partitions()) {
                task.check(); UUID group=part.route().groupId(); if (pendingResults.containsKey(group)) continue;
                var result=part.command().readCertification() ? new CommandResult(CommandResult.Status.SUCCESS,List.of()) : binding.captures().get(group).stage(part.command().mutations(),part.command().predicates());
                synchronized (this) { pendingResultBytes=checkedResultBytes(pendingResultBytes,result); pendingResults.put(group,result); }
            }
            var results=Map.copyOf(pendingResults);
            task.committed=() -> {
                refund(stagedPrefixBytes,false,false); stagedPrefix=pendingPrefix; stagedResults=results; stagedPrefixBytes=pendingPrefixBytes;
                pendingPrefix=null; pendingPrefixBytes=0; pendingResults.clear(); pendingResultBytes=8; for (var cursor:List.copyOf(borrowedCursors)) cursor.close();
            };
            return results;
        }
        synchronized RatisTransactionPlan.Accepted preparedPrefix(RatisAtomicClient.Identity identity) throws IOException {
            if (mode!=RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED || commitFrozen) throw new IOException("Writable commit unavailable");
            owners(); requireIdleControl();
            var prefix=stagedPrefix==null ? RatisTransactionPlan.captured(data.directory(),identity.business(),List.of(),List.of(),snapshot()) : stagedPrefix;
            if (!prefix.operation().equals(identity.business())) throw new IllegalArgumentException("Commit business differs from staged prefix");
            var accepted=new LinkedHashMap<UUID,DistributedCommand>(); var prepares=new LinkedHashMap<UUID,OperationId>(); var finalizes=new LinkedHashMap<UUID,OperationId>(); int n=0;
            for (var part:prefix.partitions()) {
                var group=part.route().groupId(); var capture=binding.captures().get(group);
                var result=stagedResults.get(group);
                accepted.put(group,part.command().readCertification() ? part.command() : capture.participantIntent(part.command(),Objects.requireNonNull(result,"Missing settled stage result")));
                prepares.put(group,new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),identity.firstControl().sequence()+n));
                finalizes.put(group,new OperationId(identity.firstControl().sessionId(),identity.firstControl().epoch(),identity.firstControl().sequence()+64+n++));
            }
            var result=prefix.acceptCaptured(identity.transaction(),accepted,prepares,finalizes);
            commitFrozen=true; for (var cursor:List.copyOf(borrowedCursors)) cursor.close(); return result;
        }
        synchronized void undoCommitFreeze() { if (!commitFinished) commitFrozen=false; }
        synchronized void finishCommitOwnership() { if (!commitFrozen) throw new IllegalStateException("Commit was not frozen"); commitFinished=true; }
        public synchronized Cursor openCursor(long column,Keys lower,Keys upper,boolean reverse) throws IOException {
            owners(); if (control!=null) throw new IOException("Snapshot control active"); RatisLogicalClient.supported(schema,column);
            Keys low=freeze(lower),high=freeze(upper); RatisLogicalRangePages.validateBounds(scope(),schema,column,low,high); long bytes=256+keyBytes(low)+keyBytes(high)+TransactionScope.MAX_BOUND_BYTES+RangeBudget.DEFAULT_MAX_BYTES;
            reserve(bytes,false,true); var cursor=new Cursor(column,low,high,reverse,bytes); borrowedCursors.add(cursor); return cursor;
        }
        private long keyBytes(Keys keys) { long bytes=0; if (keys!=null) for (var key:keys.keys()) { bytes+=4L+Objects.requireNonNull(key).size(); if (bytes>TransactionScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Snapshot key bound exceeds limit"); } return bytes; }
        private Keys freeze(Keys keys) {
            if (keys==null) return null;
            if (keys.keys().length>TransactionScope.MAX_BOUND_BYTES/4) throw new IllegalArgumentException("Snapshot key component bound exceeded");
            var shallow=new Keys(keys.keys().clone()); keyBytes(shallow); return RatisLogicalRangePages.copy(shallow);
        }
        public final class Cursor implements AutoCloseable {
            private final long column,bytes; private final Keys lower,upper; private final boolean reverse;
            private Keys resume; private boolean exhausted,closed,active,refunded;
            private Task<RangePage<KV>> pending; private RangeBudget pendingBudget;
            private Cursor(long column,Keys lower,Keys upper,boolean reverse,long bytes) { this.column=column; this.lower=lower; this.upper=upper; this.reverse=reverse; this.bytes=bytes; }
            public CompletableFuture<RangePage<KV>> nextPage(RangeBudget budget,RequestContext context) {
                long deadline=scheduler.resolveMonotonicDeadline(context);
                synchronized (Lease.this) {
                    try {
                        owners(); if (closed) throw new IOException("Cursor closed");
                        if (active) { if (!Objects.equals(pendingBudget,budget)) throw new IOException("Pending cursor page budget changed"); return pending.view(); }
                        if (exhausted) return CompletableFuture.completedFuture(RangePage.empty()); active=true;
                    }
                    catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
                    Keys after=RatisLogicalRangePages.copy(resume);
                    try {
                        pendingBudget=Objects.requireNonNull(budget);
                        pending=Lease.this.<RangePage<KV>>startRead(Kind.PAGE,OperationFamily.RANGE_PAGE,PAGE_BYTES,context,deadline,task -> {
                            var result=RatisLogicalSnapshotPages.read(binding.reader(),owners(),scope(),schema,column,lower,upper,reverse,after,budget);
                            task.committed=() -> { if (!closed) { resume=RatisLogicalRangePages.copy(result.resumeAfter()); exhausted=!result.hasMore(); } };
                            return result;
                        },this);
                        return pending.view();
                    } catch (Exception failure) { active=false; if (closed) refundCursor(); return CompletableFuture.failedFuture(failure); }
                }
            }
            /** Recover the last actual page after cancelling its observer, without advancing again. */
            public CompletableFuture<RangePage<KV>> pendingPage() {
                synchronized (Lease.this) { return pending==null ? CompletableFuture.failedFuture(new IOException("Cursor has no pending page")) : pending.view(); }
            }
            private void finished() { active=false; if (closed) refundCursor(); }
            private void refundCursor() { if (!refunded && borrowedCursors.remove(this)) { refunded=true; pending=null; refund(bytes,false,true); } }
            @Override public void close() { synchronized (Lease.this) { closed=true; if (!active) refundCursor(); } }
        }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; if (control!=null || !reading.isEmpty() || closingLease || commitFrozen && !commitFinished) throw new IOException("Snapshot work active; ownership retained"); closeRequested=true; closingLease=true; for (var cursor:List.copyOf(borrowedCursors)) cursor.close(); }
            try { if (journal!=null) journal.close(); synchronized (this) { closed=true; refund(pendingPrefixBytes+stagedPrefixBytes,false,false); pendingPrefixBytes=stagedPrefixBytes=0; pendingPrefix=stagedPrefix=null; pendingResults.clear(); pendingResultBytes=8; stagedResults=Map.of(); } remove(this); }
            finally { synchronized (this) { closingLease=false; } }
        }
    }
    private final class Task<T> implements Runnable,RWScheduler.RejectionAwareTask {
        final Lease lease; final Kind kind; final long deadline,bytes; final OperationFamily family; final Work<T> work;
        final CompletableFuture<T> result=new CompletableFuture<>(); final AtomicInteger phase=new AtomicInteger();
        int views; volatile Throwable rejection; Lease.Cursor cursor; Runnable committed;
        Task(Lease lease,Kind kind,long deadline,long bytes,OperationFamily family,Work<T> work) { this.lease=lease; this.kind=kind; this.deadline=deadline; this.bytes=bytes; this.family=family; this.work=work; }
        synchronized CompletableFuture<T> view() {
            if (!result.isDone() && views>=MAX_WAITERS) return CompletableFuture.failedFuture(new RejectedExecutionException("Snapshot waiter capacity exhausted"));
            if (!result.isDone()) views++;
            return cursor==null ? result.copy() : result.thenApply(this::exposePage);
        }
        @SuppressWarnings("unchecked")
        private T exposePage(T value) {
            var page=(RangePage<KV>)value; var items=new ArrayList<KV>(page.items().size());
            for (var item:page.items()) items.add(new KV(RatisLogicalRangePages.copy(item.keys()),item.value()==null ? null : Buf.wrap(item.value().toByteArray())));
            return (T)new RangePage<>(items,RatisLogicalRangePages.copy(page.resumeAfter()),page.hasMore());
        }
        void check() throws IOException {
            if (rejection!=null) throw new IOException("Snapshot task rejected; ownership retained",rejection);
            if ((kind==Kind.GET || kind==Kind.PAGE) && lease.lost) throw new RatisSnapshotLostException("Original snapshot owner lost");
            if (deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Snapshot deadline expired; ownership retained",new TimeoutException());
        }
        public void run() {
            if (!phase.compareAndSet(0,1)) return; T value=null; Throwable failure=null;
            try { check(); value=work.run(this); check(); } catch (Throwable unavailable) { failure=unavailable; if (unavailable instanceof RatisSnapshotLostException && kind!=Kind.RELEASE) lease.lost=true; }
            phase.set(2); finish(value,failure); if (failure instanceof Error fatal) throw fatal;
        }
        void fail(Throwable failure) { if (phase.compareAndSet(0,2)) finish(null,failure); else if (phase.get()==1) rejection=failure; }
        public void reject(RuntimeException failure) { fail(failure); }
        void finish(T value,Throwable failure) {
            synchronized (lease) {
                if (failure==null) try { check(); if (committed!=null) committed.run(); } catch (Throwable lateFailure) { failure=lateFailure; }
                if (lease.control==this) lease.control=null; if (lease.reading.remove(this)) refund(bytes,true,false); if (cursor!=null) cursor.finished();
            }
            if (failure==null) result.complete(value); else result.completeExceptionally(failure);
        }
    }
    @Override public void close() throws IOException {
        List<Lease> owners; synchronized (this) { if (closed) return; closing=true; owners=List.copyOf(leases.values()); }
        IOException failure=null; for (var lease:owners) try { lease.close(); } catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
        if (failure!=null) throw failure; synchronized (this) { closed=true; }
    }
}
