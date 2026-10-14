package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

/** Caller-driven durable snapshot control. Saved control progress does not guarantee a volatile native pin still exists. */
public final class RatisDurableSnapshot implements AutoCloseable {
    public static final int MAX_BYTES=64*1024*1024;
    private static final long MAX_ROUTES=16L*1024*1024;
    private static final int MAGIC=0x53534a4c;
    public record ParticipantProgress(RangeDescriptor route,boolean installFrozen,RatisSnapshotRecord.Result installReceipt,
            SnapshotGate nativeGate,ApplyPosition readyProof,RatisRoutedClient.PreparedSnapshotCapture capture,SnapshotFenceStatus.Capture captureToken,ScopedTransactionRpc.Handle certifiedHandle,
            boolean captureLost,boolean captureReleased,boolean captureReleaseLost,boolean releaseFrozen,ApplyPosition releaseReceipt,SnapshotFenceProof retirementProof) {}
    public record Snapshot(SnapshotFenceStatus initial,SnapshotFenceStatus observedHome,RatisRoutedClient.PreparedCommand pendingHome,
            CommandResult homeResult,boolean homeSuperseded,OperationId lastHome,List<ParticipantProgress> participants,SnapshotHomeFenceProof homeRetirementProof,boolean releaseRequested,Duration requestedTtl,RatisSnapshotCaptureRequest.Mode captureMode) {
        public ParticipantProgress participant(UUID group) { return participants.stream().filter(p -> p.route().groupId().equals(group)).findFirst().orElseThrow(); }
    }
    private static final class Entry {
        final RangeDescriptor route;
        boolean installFrozen,captureLost,captureReleaseFrozen,captureReleased,captureReleaseLost,releaseFrozen;
        RatisSnapshotRecord.Result installReceipt;
        SnapshotGate gate;
        ApplyPosition ready,releaseReceipt;
        RatisRoutedClient.PreparedSnapshotCapture capture;
        SnapshotFenceStatus.Capture token;
        ScopedTransactionRpc.Handle certifiedHandle;
        SnapshotFenceProof retirementProof;
        Entry(RangeDescriptor route) { this.route=route; }
        ParticipantProgress progress() { return new ParticipantProgress(route,installFrozen,installReceipt,gate,ready,capture,token,certifiedHandle,captureLost,captureReleased,captureReleaseLost,releaseFrozen,releaseReceipt,retirementProof); }
    }
    @FunctionalInterface interface Persistence {void write(Path path,byte[] body,int maximum)throws IOException;}
    private final RatisJournalAccess journalAccess;
    Persistence persistence;
    private final Path path;
    private final RatisSnapshotFences homes;
    private final RatisRoutedClient data;
    private byte[] identity;private final byte[] homeIdentity;
    private final FileChannel channel;
    private final FileLock lock;
    private final LinkedHashMap<UUID,Entry> entries=new LinkedHashMap<>();
    private SnapshotFenceStatus initial,observed;
    private RangeDescriptor homeRoute;
    private RatisRoutedClient.PreparedCommand pending;
    private OperationId lastHome;
    private CommandResult homeResult;
    private SnapshotHomeFenceProof homeRetirementProof;
    private Duration requestedTtl;
    private RatisSnapshotCaptureRequest.Mode captureMode;
    private boolean homeSuperseded,active,closed,fenced,reclaimed,releaseRequested;

    public static RatisDurableSnapshot create(Path path,RatisSnapshotFences homes,RatisRoutedClient data,SnapshotFenceStatus initial,
            RatisRoutedClient.PreparedCommand registration,List<RangeDescriptor> routes) throws IOException {
        return create(path,homes,data,initial,registration,routes,null,RatisSnapshotCaptureRequest.Mode.READ_ONLY);
    }
    public static RatisDurableSnapshot create(Path path,RatisSnapshotFences homes,RatisRoutedClient data,SnapshotFenceStatus initial,
            RatisRoutedClient.PreparedCommand registration,List<RangeDescriptor> routes,Duration requestedTtl) throws IOException {
        return create(path,homes,data,initial,registration,routes,requestedTtl,RatisSnapshotCaptureRequest.Mode.READ_ONLY);
    }
    public static RatisDurableSnapshot create(Path path,RatisSnapshotFences homes,RatisRoutedClient data,SnapshotFenceStatus initial,
            RatisRoutedClient.PreparedCommand registration,List<RangeDescriptor> routes,RatisSnapshotCaptureRequest.Mode mode) throws IOException {
        return create(path,homes,data,initial,registration,routes,null,mode);
    }
    public static RatisDurableSnapshot create(Path path,RatisSnapshotFences homes,RatisRoutedClient data,SnapshotFenceStatus initial,
            RatisRoutedClient.PreparedCommand registration,List<RangeDescriptor> routes,Duration requestedTtl,RatisSnapshotCaptureRequest.Mode mode) throws IOException {
        return createOwned(path,homes,data,initial,registration,routes,requestedTtl,mode,RatisJournalAccess.standalone());
    }
    static RatisDurableSnapshot createOwned(Path path,RatisSnapshotFences homes,RatisRoutedClient data,SnapshotFenceStatus initial,RatisRoutedClient.PreparedCommand registration,List<RangeDescriptor> routes,Duration requestedTtl,RatisSnapshotCaptureRequest.Mode mode,RatisJournalAccess access)throws IOException {
        requireTtl(requestedTtl,mode);
        var journal=new RatisDurableSnapshot(path,homes,data,true,access);
        journal.requestedTtl=requestedTtl; journal.captureMode=mode;journal.identity=dataBinding(data,mode);
        try {
            journal.initial=Objects.requireNonNull(initial);
            if (initial.phase()!=SnapshotFenceStatus.Phase.INSTALLING || initial.generation()!=0) throw new IllegalArgumentException("Snapshot journal requires initial INSTALLING definition");
            homes.restore(registration.command(),registration.descriptor());
            if (expected(registration)!=null || !next(registration).equals(initial)) throw new IllegalArgumentException("Home registration differs from snapshot definition");
            journal.homeRoute=registration.descriptor(); journal.pending=registration; journal.lastHome=registration.command().operationId();
            var directory=data.directory(); if (directory.generation()!=initial.fence().directoryGeneration()) throw new IllegalArgumentException("Snapshot directory generation changed before freezing routes");
            if (routes.size()!=initial.fence().participants().size()) throw new IllegalArgumentException("Incomplete snapshot route set");
            var supplied=new HashMap<UUID,RangeDescriptor>(); for (var route:routes) if (supplied.putIfAbsent(route.groupId(),route)!=null) throw new IllegalArgumentException("Duplicate snapshot route");
            for (var participant:initial.fence().participants()) {
                var route=Objects.requireNonNull(supplied.get(participant.groupId()),"Missing participant route");
                var current=directory.ranges().stream().filter(r -> r.groupId().equals(participant.groupId())).findFirst().orElseThrow();
                if (!route.equals(current)) throw new IllegalArgumentException("Snapshot route differs from original directory");
                var covered=participant.scope().participants(directory,SnapshotFence.MAX_PARTICIPANTS);
                if (covered.size()!=1 || !covered.getFirst().equals(route)) throw new IllegalArgumentException("Snapshot scope crosses its frozen participant route");
                journal.entries.put(route.groupId(),new Entry(route));
            }
            journal.capacity(); journal.persist(); return journal;
        } catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public static RatisDurableSnapshot reopen(Path path,RatisSnapshotFences homes,RatisRoutedClient data) throws IOException { return new RatisDurableSnapshot(path,homes,data,false,RatisJournalAccess.standalone()); }
    static RatisDurableSnapshot reopenOwned(Path path,RatisSnapshotFences homes,RatisRoutedClient data,RatisJournalAccess access)throws IOException {return new RatisDurableSnapshot(path,homes,data,false,access);}
    private RatisDurableSnapshot(Path file,RatisSnapshotFences homes,RatisRoutedClient data,boolean create,RatisJournalAccess access) throws IOException {
        journalAccess=Objects.requireNonNull(access);persistence=access::write;access.register(file,MAX_BYTES);access.register(file.resolveSibling(file.getFileName()+".lock"),32);
        path=file.toAbsolutePath(); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data); identity=data.sessionIdentityDigest(); homeIdentity=homes.identityDigest();
        if (!Files.isDirectory(path.getParent())) throw new IOException("Snapshot journal parent is absent");
        channel=FileChannel.open(path.resolveSibling(path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Snapshot journal already owned",owned); }
            if (acquired==null) throw new IOException("Snapshot journal already owned"); lock=acquired;
            if (create==Files.exists(path)) throw new IOException(create ? "Snapshot journal already exists" : "Snapshot journal absent");
            if (!create) { byte[] body=journalAccess.read(path,MAX_BYTES); decode(body); capacity(); if (!Arrays.equals(body,encode())) throw new IOException("Noncanonical snapshot journal"); SessionJournalFiles.force(path); SessionJournalFiles.force(path.getParent()); }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    private static byte[] dataBinding(RatisRoutedClient data,RatisSnapshotCaptureRequest.Mode mode){return mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY?data.snapshotNamespaceDigest():data.sessionIdentityDigest();}
    synchronized void requireClients(RatisSnapshotFences expectedHomes,RatisRoutedClient expectedData) throws IOException {
        requireOpen(); if (!Arrays.equals(homeIdentity,Objects.requireNonNull(expectedHomes).identityDigest()) || !Arrays.equals(identity,dataBinding(Objects.requireNonNull(expectedData),captureMode))) throw new IllegalArgumentException("Snapshot clients differ from journal binding");
    }
    public synchronized Snapshot snapshot() throws IOException { requireOpen(); return new Snapshot(initial,observed,pending,homeResult,homeSuperseded,lastHome,entries.values().stream().map(Entry::progress).toList(),homeRetirementProof,releaseRequested,requestedTtl,captureMode); }
    public synchronized RatisSnapshotCaptureRequest.Mode mode() throws IOException { requireOpen(); return captureMode; }
    /** Explicit whole-lease release direction, forced once before any remote cleanup. */
    public synchronized void requestRelease() throws IOException {
        requireOpen(); if (active) throw new IOException("Snapshot journal RPC already active");
        if (!releaseRequested) { releaseRequested=true; persist(); }
    }
    public synchronized void queueHome(RatisRoutedClient.PreparedCommand request) throws IOException {
        idle(); homes.restore(request.command(),request.descriptor()); requireLineage(homeRoute,request.descriptor());
        if (pending!=null && homeResult==null && !homeSuperseded) { if (sameCommand(pending,request)) return; throw new IllegalStateException("Uncertain snapshot home request must retain exact bytes/route/identity"); }
        var before=expected(request); var next=next(request);
        if (observed==null) {
            if (before!=null || !next.equals(initial) || homeResult==null || homeResult.status()==CommandResult.Status.SUCCESS) throw new IllegalArgumentException("Registration retry requires definite no-effects home result");
        } else { if (!observed.equals(before)) throw new IllegalArgumentException("Home CAS differs from observed expected image"); next.requireSuccessorOf(observed); requireEvidence(next); }
        advance(request.command().operationId()); pending=request; homeResult=null; homeSuperseded=false; persist();
    }
    /** Queue explicit client consumption; execution remains a separate caller-driven quantum. */
    public synchronized void consume(OperationId operation) throws IOException {
        idle(); if (observed==null || !observed.terminal()) throw new IllegalStateException("Snapshot consumption requires observed terminal HOME");
        if (observed.consumed() || pending!=null && homeResult==null && !homeSuperseded && next(pending).consumed()) return;
        queueHome(homes.prepare(operation,observed,observed.consume()));
    }
    @FunctionalInterface private interface AsyncWork<T>{CompletableFuture<T> run()throws IOException;}
    @FunctionalInterface private interface CheckedMap<T,R>{R apply(T value)throws IOException;}
    private static <T,R> CompletableFuture<R> map(CompletableFuture<T> actual,CheckedMap<T,R> mapper){
        return actual.thenApply(value->{try{return mapper.apply(value);}catch(IOException failure){throw new CompletionException(failure);}});
    }
    /** Admission is already held; observer cancellation cannot release the actual journal owner. */
    private <T> CompletableFuture<T> finishAsync(AsyncWork<T> work){
        final CompletableFuture<T> actual;
        try{actual=work.run();}catch(Throwable failure){end();return CompletableFuture.failedFuture(failure);}
        var result=new CompletableFuture<T>();actual.whenComplete((value,failure)->{end();if(failure==null)result.complete(value);else result.completeExceptionally(failure);});return result.copy();
    }
    public CompletableFuture<CommandResult> executeHomeAsync()throws IOException{
        final RatisRoutedClient.PreparedCommand request;
        synchronized(this){idle();if(homeResult!=null)return CompletableFuture.completedFuture(homeResult);if(homeSuperseded)throw new IllegalStateException("Snapshot home request semantically superseded; exact result unknown");request=pending;active=true;}
        return finishAsync(()->homes.executeAsync(request).thenCompose(result->{
            CompletableFuture<SnapshotHomeFenceProof> retirement=result.status()==CommandResult.Status.SUCCESS?CompletableFuture.completedFuture(null):homes.fenceAsync(homes.identity(initial.fence(),homeRoute));
            return map(retirement,proof->{synchronized(this){
                if(proof!=null){mergeHomeRetirement(proof);persist();throw new RatisSnapshotHomeRetiredException(proof);}
                if(result.valueCount()!=0)throw new IOException("Snapshot home returned unexpected values");
                if(result.status()==CommandResult.Status.SUCCESS){var successful=next(request);if(observed!=null&&observed.generation()>successful.generation())monotone(successful,observed);else observe(successful);}
                homeResult=result;persist();return result;
            }});
        }));
    }
    public CompletableFuture<SnapshotFenceStatus> refreshHomeAsync()throws IOException{
        synchronized(this){idle();active=true;}
        return finishAsync(()->homes.readRegistrationAsync(initial.fence().fenceId()).thenCompose(read->{
            try{requireLineage(homeRoute,read.descriptor());}catch(IllegalArgumentException changed){return CompletableFuture.failedFuture(new IOException("Snapshot home incarnation/bounds changed",changed));}
            CompletableFuture<SnapshotHomeFenceProof> retirement=read.status()==null||!initial.fence().equals(read.status().fence())?homes.fenceAsync(homes.identity(initial.fence(),homeRoute)):CompletableFuture.completedFuture(null);
            return map(retirement,proof->{synchronized(this){
                if(proof!=null){mergeHomeRetirement(proof);persist();throw new RatisSnapshotHomeRetiredException(proof);}
                if(read.status()==null){if(observed!=null)throw new IOException("Snapshot home disappeared");return null;}
                if(!read.status().equals(observed)){observe(read.status());persist();}return observed;
            }});
        }));
    }
    public CommandResult executeHome() throws IOException {
        RatisRoutedClient.PreparedCommand request;
        synchronized (this) { idle(); if (homeResult!=null) return homeResult; if (homeSuperseded) throw new IllegalStateException("Snapshot home request semantically superseded; exact result unknown"); request=pending; active=true; }
        try {
            var result=homes.execute(request);
            if (result.status()!=CommandResult.Status.SUCCESS) {
                var retirement=homes.fence(homes.identity(initial.fence(),homeRoute));
                if (retirement!=null) throw new RatisSnapshotHomeRetiredException(retirement);
            }
            synchronized (this) {
                if (result.valueCount()!=0) throw new IOException("Snapshot home returned unexpected values");
                if (result.status()==CommandResult.Status.SUCCESS) { var successful=next(request); if (observed!=null && observed.generation()>successful.generation()) monotone(successful,observed); else observe(successful); }
                homeResult=result; persist(); return result;
            }
        } catch (RatisSnapshotHomeRetiredException retired) {
            synchronized (this) { mergeHomeRetirement(retired.proof()); persist(); } throw retired;
        } finally { end(); }
    }
    public SnapshotFenceStatus refreshHome() throws IOException {
        synchronized (this) { idle(); active=true; }
        try {
            var read=homes.readRegistration(initial.fence().fenceId());
            try { requireLineage(homeRoute,read.descriptor()); } catch (IllegalArgumentException changed) { throw new IOException("Snapshot home incarnation/bounds changed",changed); }
            if (read.status()==null || !initial.fence().equals(read.status().fence())) {
                var retirement=homes.fence(homes.identity(initial.fence(),homeRoute));
                if (retirement!=null) { synchronized (this) { mergeHomeRetirement(retirement); persist(); } throw new RatisSnapshotHomeRetiredException(retirement); }
            }
            synchronized (this) {
                if (read.status()==null) { if (observed!=null) throw new IOException("Snapshot home disappeared"); return null; }
                if (!read.status().equals(observed)) { observe(read.status()); persist(); } return observed;
            }
        } finally { end(); }
    }
    public synchronized boolean resolveHomeFromObserved() throws IOException {
        idle(); if (homeSuperseded) return true; if (homeResult!=null || !supersedes()) return false; homeSuperseded=true; persist(); return true;
    }
    private boolean supersedes() { var before=expected(pending); return observed!=null && initial.fence().equals(observed.fence()) && (before==null || observed.generation()>before.generation()); }
    public synchronized void freezeInstall(UUID group) throws IOException {
        idle(); var entry=entry(group); if (entry.installFrozen) return;
        if (observed==null || observed.terminal()) throw new IllegalStateException("INSTALL requires observed INSTALLING home");
        data.restoreSnapshotGate(RatisSnapshotRecord.install(initial.fence(),group),entry.route); entry.installFrozen=true; persist();
    }
    /** Abandon only unreceipted INSTALL authority; frozen bytes remain available for a later exact retry. */
    void cancelInstall(UUID group) throws IOException {
        Entry entry;
        synchronized (this) {
            idle(); entry=entry(group);
            if (!entry.installFrozen || entry.retirementProof!=null || entry.installReceipt!=null || entry.releaseReceipt!=null || entry.gate!=null && entry.gate.released()!=null) return;
            active=true;
        }
        try { data.cancelSnapshotGate(data.restoreSnapshotGate(RatisSnapshotRecord.install(initial.fence(),group),entry.route)); }
        finally { end(); }
    }
    private CompletableFuture<SnapshotGateResult> readGateResultAsync(UUID group){
        return data.snapshotGateAsync(initial.fence(),group).handle((gate,failure)->{
            if(failure==null&&gate!=null&&gate.fence().equals(initial.fence()))return CompletableFuture.completedFuture(SnapshotGateResult.retained(gate));
            return map(data.snapshotFenceAsync(initial.fence(),group),proof->{
                if(proof!=null)return SnapshotGateResult.retired(proof);
                if(failure!=null){Throwable cause=failure;while(cause instanceof CompletionException&&cause.getCause()!=null)cause=cause.getCause();if(cause instanceof IOException io)throw io;throw new IOException("Snapshot gate lookup failed",cause);}
                if(gate!=null)throw new IOException("Snapshot gate belongs to another unfenced business");return null;
            });
        }).thenCompose(future->future);
    }
    public CompletableFuture<RatisSnapshotRecord.Result> executeInstallAsync(UUID group)throws IOException{
        final Entry entry;
        synchronized(this){idle();entry=entry(group);if(!entry.installFrozen)throw new IllegalStateException("INSTALL is not frozen");if(entry.retirementProof!=null)return CompletableFuture.completedFuture(entry.installReceipt==null?RatisSnapshotRecord.Result.retired(entry.retirementProof):entry.installReceipt);active=true;}
        return finishAsync(()->{
            var receipt=entry.installReceipt==null?data.executeSnapshotGateAsync(data.restoreSnapshotGate(RatisSnapshotRecord.install(initial.fence(),group),entry.route)):CompletableFuture.completedFuture(entry.installReceipt);
            return map(receipt,value->{synchronized(this){if(value.retired()){mergeRetirement(entry,value.retirement());persist();return value;}if(entry.installReceipt==null){checkInstallReceipt(entry,value,entry.gate);entry.installReceipt=value;persist();}return value;}}).thenCompose(value->{
                if(value.retired())return CompletableFuture.completedFuture(value);
                return map(readGateResultAsync(group),nativeResult->{synchronized(this){
                    if(nativeResult!=null&&nativeResult.retired()){mergeRetirement(entry,nativeResult.retirement());persist();return entry.installReceipt==null?RatisSnapshotRecord.Result.retired(entry.retirementProof):entry.installReceipt;}
                    var gate=nativeResult==null?null:nativeResult.gate();if(gate==null)throw new IOException("INSTALL receipt has no native proof; retry lookup");mergeGate(entry,gate);persist();return entry.installReceipt;
                }});
            });
        });
    }
    public CompletableFuture<Boolean> verifyReadyAsync(UUID group)throws IOException{
        final Entry entry;synchronized(this){idle();entry=entry(group);active=true;}
        return finishAsync(()->map(data.snapshotReadinessAsync(initial.fence(),group),ready->{synchronized(this){
            if(!ready.binding().equals(binding())||!ready.group().equals(group))throw new IOException("Readiness binding changed");
            if(ready.retired()){mergeRetirement(entry,ready.retirement());persist();return false;}if(!ready.ready())return false;
            if(entry.installReceipt==null||entry.installReceipt.suppressed()||entry.gate==null||!entry.gate.active()||!entry.installReceipt.position().equals(entry.gate.installed())||!entry.gate.installed().equals(ready.installed()))throw new IOException("Readiness has no matching forced INSTALL receipt/native proof");
            if(entry.ready!=null&&!entry.ready.equals(ready.installed()))throw new IOException("Ready proof changed");if(entry.ready==null){entry.ready=ready.installed();persist();}return true;
        }}));
    }
    public CompletableFuture<RatisSnapshotRecord.Result> resolveReleaseAsync(UUID group)throws IOException{
        final Entry entry;synchronized(this){idle();entry=entry(group);if(observed==null||!observed.terminal())throw new IllegalStateException("RELEASE resolution requires terminal HOME");active=true;}
        return finishAsync(()->map(readGateResultAsync(group),nativeResult->{synchronized(this){
            var gate=nativeResult==null?null:nativeResult.gate();var retirement=nativeResult==null?null:nativeResult.retirement();
            if(retirement!=null){mergeRetirement(entry,retirement);persist();return RatisSnapshotRecord.Result.retired(entry.retirementProof);}
            if(!Objects.equals(gate,entry.gate)){mergeGate(entry,gate);persist();}return RatisSnapshotRecord.release(observed,group).receipt(gate);
        }}));
    }
    public CompletableFuture<RatisSnapshotRecord.Result> executeReleaseAsync(UUID group)throws IOException{
        final Entry entry;synchronized(this){idle();entry=entry(group);if(!entry.releaseFrozen)throw new IllegalStateException("RELEASE is not frozen");if(entry.retirementProof!=null)return CompletableFuture.completedFuture(entry.releaseReceipt==null?RatisSnapshotRecord.Result.retired(entry.retirementProof):new RatisSnapshotRecord.Result(entry.releaseReceipt));active=true;}
        return finishAsync(()->{
            var receipt=entry.releaseReceipt==null?data.executeSnapshotGateAsync(data.restoreSnapshotGate(RatisSnapshotRecord.release(observed,group),entry.route)):CompletableFuture.completedFuture(new RatisSnapshotRecord.Result(entry.releaseReceipt));
            return map(receipt,result->{synchronized(this){if(result.retired()){mergeRetirement(entry,result.retirement());persist();return result;}
                if(entry.releaseReceipt==null){var position=result.position();checkReleaseReceipt(entry,position);if(entry.gate!=null&&entry.gate.released()!=null&&!entry.gate.released().equals(position))throw new IOException("RELEASE receipt differs from verified native proof");entry.releaseReceipt=position;persist();}return result;
            }}).thenCompose(result->{if(result.retired())return CompletableFuture.completedFuture(result);return map(readGateResultAsync(group),nativeResult->{synchronized(this){
                if(nativeResult!=null&&nativeResult.retired()){mergeRetirement(entry,nativeResult.retirement());persist();return new RatisSnapshotRecord.Result(entry.releaseReceipt);}
                var gate=nativeResult==null?null:nativeResult.gate();if(gate==null||gate.released()==null)throw new IOException("RELEASE receipt has no native terminal proof; retry lookup");mergeGate(entry,gate);persist();return new RatisSnapshotRecord.Result(entry.releaseReceipt);
            }});});
        });
    }
    public RatisSnapshotRecord.Result executeInstall(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); if (!entry.installFrozen) throw new IllegalStateException("INSTALL is not frozen"); if (entry.retirementProof!=null) return entry.installReceipt==null ? RatisSnapshotRecord.Result.retired(entry.retirementProof) : entry.installReceipt; active=true; }
        try {
            if (entry.installReceipt==null) {
                var receipt=data.executeSnapshotGate(data.restoreSnapshotGate(RatisSnapshotRecord.install(initial.fence(),group),entry.route));
                if (receipt.retired()) { synchronized (this) { mergeRetirement(entry,receipt.retirement()); persist(); return receipt; } }
                synchronized (this) { checkInstallReceipt(entry,receipt,entry.gate); entry.installReceipt=receipt; persist(); }
            }
            var nativeResult=readGateResult(group);
            if (nativeResult!=null && nativeResult.retired()) { synchronized (this) { mergeRetirement(entry,nativeResult.retirement()); persist(); return entry.installReceipt==null ? RatisSnapshotRecord.Result.retired(entry.retirementProof) : entry.installReceipt; } }
            var gate=nativeResult==null ? null : nativeResult.gate();
            synchronized (this) { if (gate==null) throw new IOException("INSTALL receipt has no native proof; retry lookup"); mergeGate(entry,gate); persist(); return entry.installReceipt; }
        } finally { end(); }
    }
    public SnapshotGate resolveGate(UUID group) throws IOException {
        Entry entry; synchronized (this) { idle(); entry=entry(group); active=true; }
        try { var nativeResult=readGateResult(group);
            if (nativeResult!=null && nativeResult.retired()) { synchronized (this) { mergeRetirement(entry,nativeResult.retirement()); persist(); return entry.gate; } }
            var gate=nativeResult==null ? null : nativeResult.gate();
            synchronized (this) { if (!Objects.equals(entry.gate,gate)) { mergeGate(entry,gate); persist(); } return entry.gate; } }
        finally { end(); }
    }
    public boolean verifyReady(UUID group) throws IOException {
        Entry entry; synchronized (this) { idle(); entry=entry(group); active=true; }
        try {
            var ready=data.snapshotReadiness(initial.fence(),group);
            synchronized (this) {
                if (!ready.binding().equals(binding()) || !ready.group().equals(group)) throw new IOException("Readiness binding changed");
                if (ready.retired()) { mergeRetirement(entry,ready.retirement()); persist(); return false; }
                if (!ready.ready()) return false;
                if (entry.installReceipt==null || entry.installReceipt.suppressed() || entry.gate==null || !entry.gate.active()
                        || !entry.installReceipt.position().equals(entry.gate.installed()) || !entry.gate.installed().equals(ready.installed())) throw new IOException("Readiness has no matching forced INSTALL receipt/native proof");
                if (entry.ready!=null && !entry.ready.equals(ready.installed())) throw new IOException("Ready proof changed");
                if (entry.ready==null) { entry.ready=ready.installed(); persist(); } return true;
            }
        } finally { end(); }
    }
    public synchronized void freezeCapture(RatisRoutedClient.PreparedSnapshotCapture operation) throws IOException {
        if (Objects.requireNonNull(operation).request().mode()!=captureMode) throw new IllegalArgumentException("Capture differs from frozen snapshot mode");
        idle(); if (requestedTtl!=null && !requestedTtl.equals(operation.request().ttl())) throw new IllegalArgumentException("Capture lifetime differs from frozen requested TTL");
        var target=operation.target(); var entry=entry(target.descriptor().groupId());
        if (entry.captureLost || entry.captureReleaseFrozen) throw new RatisSnapshotLostException("Capture was lost or explicitly released");
        if (!initial.fence().equals(operation.request().fence()) || !initial.fence().equals(target.fence())) throw new IllegalArgumentException("Capture fence changed");
        requireLineage(entry.route,target.descriptor());
        var restoredTarget=data.restoreSnapshotTarget(initial.fence(),target.descriptor(),target.replica(),target.incarnation());
        var restored=data.restoreSnapshotCapture(restoredTarget,operation.request());
        if (entry.capture!=null) { if (!sameCapture(entry.capture,restored)) throw new IllegalArgumentException("Frozen capture request/owner/route changed"); return; }
        if (observed!=null && observed.progress(entry.route.groupId()).capture()!=null) { entry.captureLost=true; persist(); throw new RatisSnapshotLostException("Home capture has no locally frozen owner target"); }
        if (observed==null || observed.terminal() || !observed.allReady() || entry.ready==null || !entry.ready.equals(observed.progress(entry.route.groupId()).ready())) throw new IllegalStateException("Capture requires all home ready proofs and verified local readiness");
        entry.capture=restored; persist();
    }
    public CompletableFuture<SnapshotFenceStatus.Capture> executeCaptureAsync(UUID group)throws IOException{
        final Entry entry;synchronized(this){idle();entry=entry(group);if(entry.capture==null)throw new IllegalStateException("Capture is not frozen");if(entry.captureLost||entry.captureReleaseFrozen)throw new RatisSnapshotLostException("Capture was lost or explicitly released");active=true;}
        return finishAsync(()->map(data.executeCaptureAsync(entry.capture).handle((captured,failure)->{
            if(failure==null)return captured;
            Throwable cause=failure;while(cause instanceof CompletionException&&cause.getCause()!=null)cause=cause.getCause();
            if(cause instanceof RatisSnapshotLostException||cause.getCause() instanceof IllegalArgumentException){synchronized(this){try{entry.captureLost=true;persist();}catch(IOException force){cause.addSuppressed(force);}}throw new CompletionException(new RatisSnapshotLostException("Frozen owner capture lost or changed",cause));}
            throw new CompletionException(cause);
        }),captured->{synchronized(this){return acceptCaptured(entry,group,captured);}}));
    }
    public SnapshotFenceStatus.Capture executeCapture(UUID group) throws IOException {
        Entry entry;
        synchronized (this) { idle(); entry=entry(group); if (entry.capture==null) throw new IllegalStateException("Capture is not frozen"); if (entry.captureLost || entry.captureReleaseFrozen) throw new RatisSnapshotLostException("Capture was lost or explicitly released"); active=true; }
        try {
            final RatisSnapshotCaptureRequest.Result captured;
            try { captured=readCapture(entry); }
            catch (IOException failure) {
                if (failure instanceof RatisSnapshotLostException || failure.getCause() instanceof IllegalArgumentException) { synchronized (this) { entry.captureLost=true; persist(); } throw new RatisSnapshotLostException("Frozen owner capture lost or changed",failure); }
                throw failure;
            }
            synchronized(this){return acceptCaptured(entry,group,captured);}

        } finally { end(); }
    }
    /** Accepts the original cohort-owned result without any capture or HOME RPC. Request must already be forced. */
    public synchronized SnapshotFenceStatus.Capture acceptExternalCapture(UUID group,RatisRoutedClient.PreparedSnapshotCapture exactPrepared,RatisSnapshotCaptureRequest.Result result)throws IOException {
        idle();var entry=entry(group);Objects.requireNonNull(exactPrepared);Objects.requireNonNull(result);
        if(captureMode!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || exactPrepared.request().mode()!=captureMode || result.handle()!=null)throw new IllegalArgumentException("External cohort capture must retain read-only mode");
        if(entry.capture==null || !sameCapture(entry.capture,exactPrepared))throw new IllegalArgumentException("External capture changed its frozen request/owner/route");
        if(entry.captureLost || entry.captureReleaseFrozen)throw new RatisSnapshotLostException("Capture was lost or explicitly released");return acceptCaptured(entry,group,result);
    }
    private SnapshotFenceStatus.Capture acceptCaptured(Entry entry,UUID group,RatisSnapshotCaptureRequest.Result captured)throws IOException {
        var token=captured.token();var request=entry.capture.request();var homeToken=observed==null ? null : observed.progress(group).capture();
        if((captureMode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED)!=(captured.handle()!=null) || !token.ownerIncarnation().equals(request.owner()) || !token.snapshotId().equals(request.snapshotId()) || entry.ready==null || !atLeast(token.position(),entry.ready)
                || entry.token!=null && !entry.token.equals(token) || entry.certifiedHandle!=null && !entry.certifiedHandle.equals(captured.handle()) || homeToken!=null && !homeToken.equals(token)) {
            entry.captureLost=true;persist();throw new RatisSnapshotLostException("Capture token changed its frozen identity/cut/mode");
        }
        if(entry.token==null){entry.token=token;entry.certifiedHandle=captured.handle();persist();}return token;
    }
    private RatisSnapshotCaptureRequest.Result readCapture(Entry entry) throws IOException {
        return captureMode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED ? data.executeWritableSnapshotCapture(entry.capture)
                : new RatisSnapshotCaptureRequest.Result(data.executeSnapshotCapture(entry.capture),null);
    }
    public enum CaptureReleaseProgress { RECOVERED, RELEASED, LOST }
    /** One owner RPC per call: recover an unknown original token after HOME consumption, then explicitly release it. */
    public CompletableFuture<CaptureReleaseProgress> releaseCaptureAsync(UUID group)throws IOException{
        final Entry entry;final boolean recover;
        synchronized(this){requireOpen();if(active)throw new IOException("Snapshot journal RPC already active");entry=entry(group);
            if(entry.captureReleased)return CompletableFuture.completedFuture(CaptureReleaseProgress.RELEASED);if(entry.captureReleaseLost)return CompletableFuture.completedFuture(CaptureReleaseProgress.LOST);
            if(observed!=null&&!observed.terminal()&&pending!=null&&homeResult==null&&!homeSuperseded&&next(pending).phase()==SnapshotFenceStatus.Phase.COMPLETE)throw new IllegalStateException("Resolve pending COMPLETE before releasing its capture pin");
            if(entry.capture==null&&entry.token==null&&(observed==null||observed.progress(group).capture()==null))return CompletableFuture.completedFuture(CaptureReleaseProgress.RELEASED);
            if(entry.capture==null)throw new IllegalStateException("Capture release requires its frozen original owner target");recover=entry.token==null;
            if(recover&&!captureRecoveryAllowed())throw new IllegalStateException("Unknown capture token replay requires forced consumed HOME or matching retirement proof");
            if(!entry.captureReleaseFrozen){entry.captureReleaseFrozen=true;entry.captureLost=true;persist();}active=true;
        }
        return finishAsync(()->{
            CompletableFuture<CaptureReleaseProgress> actual=recover?map(data.executeCaptureAsync(entry.capture),captured->{synchronized(this){
                var token=captured.token();var request=entry.capture.request();var homeToken=observed==null?null:observed.progress(group).capture();
                if(!token.ownerIncarnation().equals(request.owner())||!token.snapshotId().equals(request.snapshotId())||entry.ready==null||!atLeast(token.position(),entry.ready)||homeToken!=null&&!homeToken.equals(token))throw new IOException("Recovered capture token changed its original identity/cut");
                entry.token=token;entry.certifiedHandle=captured.handle();persist();return CaptureReleaseProgress.RECOVERED;
            }}):map(data.releaseSnapshotAsync(entry.capture.target(),entry.token),ignored->{synchronized(this){entry.captureReleased=true;persist();return CaptureReleaseProgress.RELEASED;}});
            return actual.handle((progress,failure)->{if(failure==null)return progress;Throwable cause=failure;while(cause instanceof CompletionException&&cause.getCause()!=null)cause=cause.getCause();
                if(cause instanceof RatisSnapshotLostException){synchronized(this){try{entry.captureReleaseLost=true;entry.captureLost=true;persist();return CaptureReleaseProgress.LOST;}catch(IOException force){throw new CompletionException(force);}}}
                throw new CompletionException(cause);
            });
        });
    }
    public CaptureReleaseProgress releaseCapture(UUID group) throws IOException {
        Entry entry; boolean recover;
        synchronized (this) {
            requireOpen(); if (active) throw new IOException("Snapshot journal RPC already active"); entry=entry(group);
            if (entry.captureReleased) return CaptureReleaseProgress.RELEASED;
            if (entry.captureReleaseLost) return CaptureReleaseProgress.LOST;
            if (observed!=null && !observed.terminal() && pending!=null && homeResult==null && !homeSuperseded && next(pending).phase()==SnapshotFenceStatus.Phase.COMPLETE) throw new IllegalStateException("Resolve pending COMPLETE before releasing its capture pin");
            if (entry.capture==null && entry.token==null && (observed==null || observed.progress(group).capture()==null)) return CaptureReleaseProgress.RELEASED;
            if (entry.capture==null) throw new IllegalStateException("Capture release requires its frozen original owner target");
            recover=entry.token==null;
            if (recover && !captureRecoveryAllowed()) throw new IllegalStateException("Unknown capture token replay requires forced consumed HOME or matching retirement proof");
            if (!entry.captureReleaseFrozen) { entry.captureReleaseFrozen=true; entry.captureLost=true; persist(); }
            active=true;
        }
        try {
            try {
                if (recover) {
                    var captured=readCapture(entry); var token=captured.token();
                    synchronized (this) {
                        var request=entry.capture.request(); var homeToken=observed==null ? null : observed.progress(group).capture();
                        if (!token.ownerIncarnation().equals(request.owner()) || !token.snapshotId().equals(request.snapshotId()) || entry.ready==null || !atLeast(token.position(),entry.ready) || homeToken!=null && !homeToken.equals(token)) throw new IOException("Recovered capture token changed its original identity/cut");
                        entry.token=token; entry.certifiedHandle=captured.handle(); persist(); return CaptureReleaseProgress.RECOVERED;
                    }
                }
                data.releaseSnapshot(entry.capture.target(),entry.token);
            } catch (RatisSnapshotLostException lost) {
                synchronized (this) { entry.captureReleaseLost=true; entry.captureLost=true; persist(); }
                return CaptureReleaseProgress.LOST;
            }
            synchronized (this) { entry.captureReleased=true; persist(); }
            return CaptureReleaseProgress.RELEASED;
        } finally { end(); }
    }
    private boolean captureRecoveryAllowed() {
        return homeRetirementProof!=null || observed!=null && observed.terminal() && observed.consumed() && initial.fence().equals(observed.fence());
    }
    /** Remove only a locally consumed, durably HOME-retired journal after every possible owner pin is settled. */
    public void reclaimRetired() throws IOException {
        final byte[] body;
        synchronized (this) {
            requireOpen(); if (active) throw new IOException("Snapshot journal RPC active; file ownership retained");
            if (observed==null || !observed.terminal() || !observed.consumed() || homeRetirementProof==null
                    || !homes.identity(initial.fence(),homeRoute).equals(homeRetirementProof.identity())) throw new IllegalStateException("Snapshot journal reclamation requires saved consumed HOME and matching retirement proof");
            for (var entry:entries.values()) if ((entry.capture!=null || entry.token!=null || observed.progress(entry.route.groupId()).capture()!=null) && !entry.captureReleased && !entry.captureReleaseLost) throw new IllegalStateException("Snapshot capture pin is still owned or unresolved");
            body=encode(); active=true;
        }
        try { journalAccess.remove(path,body,MAX_BYTES,SessionJournalFiles.FILESYSTEM); synchronized (this) { reclaimed=true; } }
        catch (IOException | RuntimeException failure) { synchronized (this) { fenced=true; } if (failure instanceof IOException io) throw io; throw new IOException("Snapshot journal reclamation failed; reopen it",failure); }
        finally { end(); }
    }
    public synchronized void markCaptureLost(UUID group) throws IOException { idle(); var entry=entry(group); if (!entry.captureLost) { entry.captureLost=true; persist(); } }
    public synchronized void freezeRelease(UUID group) throws IOException {
        idle(); var entry=entry(group); if (entry.releaseFrozen) return;
        if (observed==null || !observed.terminal()) throw new IllegalStateException("RELEASE requires observed terminal home");
        data.restoreSnapshotGate(RatisSnapshotRecord.release(observed,group),entry.route); entry.releaseFrozen=true; persist();
    }
    /** Fresh native RELEASE or atomic fenced absence; neither lookup fabricates an append receipt. */
    public RatisSnapshotRecord.Result resolveRelease(UUID group) throws IOException {
        Entry entry; synchronized (this) { idle(); entry=entry(group); if (observed==null || !observed.terminal()) throw new IllegalStateException("RELEASE resolution requires terminal HOME"); active=true; }
        try {
            var nativeResult=readGateResult(group); var gate=nativeResult==null ? null : nativeResult.gate();
            var retirement=nativeResult!=null ? nativeResult.retirement() : null;
            synchronized (this) {
                if (retirement!=null) { mergeRetirement(entry,retirement); persist(); return RatisSnapshotRecord.Result.retired(entry.retirementProof); }
                if (!Objects.equals(gate,entry.gate)) { mergeGate(entry,gate); persist(); }
                return RatisSnapshotRecord.release(observed,group).receipt(gate);
            }
        } finally { end(); }
    }
    public RatisSnapshotRecord.Result executeRelease(UUID group) throws IOException {
        Entry entry; synchronized (this) {
            idle(); entry=entry(group); if (!entry.releaseFrozen) throw new IllegalStateException("RELEASE is not frozen");
            if (entry.retirementProof!=null) return entry.releaseReceipt==null ? RatisSnapshotRecord.Result.retired(entry.retirementProof) : new RatisSnapshotRecord.Result(entry.releaseReceipt);
            active=true;
        }
        try {
            if (entry.releaseReceipt==null) {
                var result=data.executeSnapshotGate(data.restoreSnapshotGate(RatisSnapshotRecord.release(observed,group),entry.route));
                synchronized (this) {
                    if (result.retired()) { mergeRetirement(entry,result.retirement()); persist(); return result; }
                    var receipt=result.position(); checkReleaseReceipt(entry,receipt);
                    if (entry.gate!=null && entry.gate.released()!=null && !entry.gate.released().equals(receipt)) throw new IOException("RELEASE receipt differs from verified native proof");
                    entry.releaseReceipt=receipt; persist();
                }
            }
            var nativeResult=readGateResult(group);
            if (nativeResult!=null && nativeResult.retired()) { synchronized (this) { mergeRetirement(entry,nativeResult.retirement()); persist(); return new RatisSnapshotRecord.Result(entry.releaseReceipt); } }
            var gate=nativeResult==null ? null : nativeResult.gate();
            synchronized (this) { if (gate==null || gate.released()==null) throw new IOException("RELEASE receipt has no native terminal proof; retry lookup"); mergeGate(entry,gate); persist(); return new RatisSnapshotRecord.Result(entry.releaseReceipt); }
        } finally { end(); }
    }
    private SnapshotGateResult readGateResult(UUID group) throws IOException {
        final SnapshotGate gate;
        try { gate=data.snapshotGate(initial.fence(),group); }
        catch (IOException unavailableOrChanged) {
            final SnapshotFenceProof proof;
            try { proof=data.snapshotFence(initial.fence(),group); }
            catch (IOException unavailableFence) { unavailableOrChanged.addSuppressed(unavailableFence); throw unavailableOrChanged; }
            if (proof==null) throw unavailableOrChanged;
            return SnapshotGateResult.retired(proof);
        }
        if (gate==null || !gate.fence().equals(initial.fence())) {
            var proof=data.snapshotFence(initial.fence(),group); if (proof!=null) return SnapshotGateResult.retired(proof);
            if (gate!=null) throw new IOException("Snapshot gate belongs to another unfenced business");
        }
        return gate==null ? null : SnapshotGateResult.retained(gate);
    }
    private void mergeRetirement(Entry entry,SnapshotFenceProof proof) throws IOException {
        if (observed==null || !observed.terminal() || !observed.consumed() || !initial.fence().equals(proof.fence()) || !entry.route.groupId().equals(proof.participantGroup())
                || entry.retirementProof!=null && !proof.covers(entry.retirementProof)
                || entry.releaseReceipt!=null && !atLeast(proof.position(),entry.releaseReceipt)) throw new IOException("Snapshot retirement lacks consumed HOME or matching monotone DATA proof");
        entry.retirementProof=proof;
    }
    private void mergeGate(Entry entry,SnapshotGate gate) throws IOException {
        if (gate==null) { if (entry.gate!=null) throw new IOException("Previously verified native gate disappeared"); return; }
        validateGate(entry,gate);
        if (entry.gate!=null) {
            var old=entry.gate;
            if (old.installed()!=null && !old.installed().equals(gate.installed()) || old.released()!=null && (!old.released().equals(gate.released())
                    || !Objects.equals(old.installed(),gate.installed()) || !Arrays.equals(old.decisionDigest(),gate.decisionDigest()))) throw new IOException("Native gate proof changed or regressed");
        }
        entry.gate=gate;
    }
    private void validateGate(Entry entry,SnapshotGate gate) throws IOException {
        if (!gate.fence().equals(initial.fence()) || !gate.participantGroup().equals(entry.route.groupId())) throw new IOException("Native gate binding changed");
        checkInstallReceipt(entry,entry.installReceipt,gate);
        if (entry.ready!=null && !entry.ready.equals(gate.installed())) throw new IOException("Native install differs from saved ready proof");
        if (entry.releaseReceipt!=null && gate.released()!=null && !entry.releaseReceipt.equals(gate.released())) throw new IOException("Native release differs from saved append receipt");
        if (gate.released()!=null) {
            if (observed==null || !observed.terminal()) throw new IOException("Native release lacks verified terminal home");
            try {
                var expected=gate.installed()==null ? SnapshotGate.released(observed,entry.route.groupId(),gate.released())
                        : SnapshotGate.install(initial.fence(),entry.route.groupId(),gate.installed()).release(observed,gate.released());
                if (!expected.equals(gate)) throw new IllegalArgumentException("Native decision digest changed");
            } catch (IllegalArgumentException invalid) { throw new IOException("Native release proof contradicts home",invalid); }
        }
    }
    private static void checkInstallReceipt(Entry entry,RatisSnapshotRecord.Result receipt,SnapshotGate gate) throws IOException {
        if (receipt==null || gate==null) return;
        if (receipt.retired()) throw new IOException("Retirement cannot be an INSTALL receipt");
        if (!Objects.equals(receipt.position(),gate.installed()) || receipt.suppressed() && gate.released()==null) throw new IOException("Native installation differs from saved append receipt");
    }
    private void requireEvidence(SnapshotFenceStatus next) {
        if (releaseRequested && !observed.terminal() && next.phase()==SnapshotFenceStatus.Phase.COMPLETE) throw new IllegalArgumentException("Snapshot release already requested");
        for (var proof:next.progress()) {
            var before=observed.progress(proof.groupId()); var entry=entry(proof.groupId());
            if (before.ready()==null && proof.ready()!=null && !proof.ready().equals(entry.ready)) throw new IllegalArgumentException("Home READY lacks locally verified readiness");
            if (before.capture()==null && proof.capture()!=null && (entry.captureLost || !proof.capture().equals(entry.token))) throw new IllegalArgumentException("Home capture lacks original owner token");
            if (!observed.terminal() && next.phase()==SnapshotFenceStatus.Phase.COMPLETE && (entry.capture==null || entry.captureLost || entry.token==null || !entry.token.equals(proof.capture()) || !Objects.equals(entry.ready,proof.ready()))) throw new IllegalArgumentException("COMPLETE requires every matching local ready/capture proof");
        }
    }
    private void observe(SnapshotFenceStatus status) throws IOException {
        monotone(initial,status); if (observed!=null) monotone(observed,status);
        for (var entry:entries.values()) {
            var token=status.progress(entry.route.groupId()).capture();
            if (token!=null && (entry.capture==null || !entry.capture.request().owner().equals(token.ownerIncarnation())
                    || !entry.capture.request().snapshotId().equals(token.snapshotId()) || entry.token!=null && !entry.token.equals(token))) entry.captureLost=true;
        }
        observed=status;
    }
    private static void monotone(SnapshotFenceStatus before,SnapshotFenceStatus after) throws IOException {
        if (!before.fence().equals(after.fence()) || after.generation()<before.generation() || before.consumed() && !after.consumed() || before.terminal() && (!after.terminal() || !before.terminalDecision().equals(after.terminalDecision()))) throw new IOException("Snapshot home definition/decision/progress regressed");
        for (var old:before.progress()) {
            var next=after.progress(old.groupId());
            if (old.ready()!=null && !old.ready().equals(next.ready()) || old.capture()!=null && !old.capture().equals(next.capture())) throw new IOException("Snapshot home proof changed");
        }
    }
    private void advance(OperationId operation) {
        if (operation.epoch()<lastHome.epoch() || operation.epoch()==lastHome.epoch() && (!operation.sessionId().equals(lastHome.sessionId()) || operation.sequence()<=lastHome.sequence())) throw new IllegalArgumentException("Snapshot home operation identity did not advance");
        requireControl(operation); lastHome=operation;
    }
    private void requireControl(OperationId operation) {
        if (initial.fence().operation().equals(operation)) throw new IllegalArgumentException("Snapshot HOME control overlaps business retention anchor");
        for (var participant:initial.fence().participants()) if (participant.installOperation().equals(operation) || participant.releaseOperation().equals(operation)) throw new IllegalArgumentException("Snapshot home identity overlaps frozen gate control");
    }
    private static void requireLineage(RangeDescriptor original,RangeDescriptor next) {
        if (!original.rangeId().equals(next.rangeId()) || !original.groupId().equals(next.groupId()) || original.rangeEpoch()!=next.rangeEpoch()
                || !Arrays.equals(original.lowerInclusive(),next.lowerInclusive()) || !Arrays.equals(original.upperExclusive(),next.upperExclusive())
                || next.topologyState()!=RangeDescriptor.TopologyState.ACTIVE) throw new IllegalArgumentException("Frozen range incarnation/bounds changed");
    }
    private static boolean sameCommand(RatisRoutedClient.PreparedCommand a,RatisRoutedClient.PreparedCommand b) { return a.descriptor().equals(b.descriptor()) && Arrays.equals(DistributedCommandCodec.encode(a.command()),DistributedCommandCodec.encode(b.command())); }
    private static boolean sameCapture(RatisRoutedClient.PreparedSnapshotCapture a,RatisRoutedClient.PreparedSnapshotCapture b) { return a.target().descriptor().equals(b.target().descriptor()) && a.target().replica().equals(b.target().replica()) && Arrays.equals(a.bytes(),b.bytes()); }
    private static SnapshotFenceStatus expected(RatisRoutedClient.PreparedCommand request) { byte[] value=request.command().predicates().getFirst().expectedValue(); return value==null ? null : SnapshotFenceStatus.decode(value); }
    private static SnapshotFenceStatus next(RatisRoutedClient.PreparedCommand request) { return SnapshotFenceStatus.decode(request.command().mutations().getFirst().value()); }
    private Entry entry(UUID group) { var entry=entries.get(group); if (entry==null) throw new IllegalArgumentException("Unregistered snapshot group"); return entry; }
    private ScopedTransactionRpc.SnapshotBinding binding() { return ScopedTransactionRpc.SnapshotBinding.of(initial.fence()); }
    private static boolean atLeast(ApplyPosition after,ApplyPosition before) { return after!=null && before!=null && after.term()>=before.term() && after.index()>=before.index(); }
    private void mergeHomeRetirement(SnapshotHomeFenceProof proof) throws IOException {
        if (!homes.identity(initial.fence(),homeRoute).equals(proof.identity()) || homeRetirementProof!=null && !proof.covers(homeRetirementProof)) throw new IOException("Snapshot HOME retirement identity changed or regressed");
        homeRetirementProof=proof;
    }
    private void requireOpen() throws IOException { if (closed || reclaimed || fenced || !lock.isValid()) throw new IOException("Snapshot journal closed/fenced; reopen it"); }
    private void idle() throws IOException { requireOpen(); if (homeRetirementProof!=null) throw new RatisSnapshotHomeRetiredException(homeRetirementProof); if (active) throw new IOException("Snapshot journal RPC already active"); }
    private synchronized void end() { active=false; }
    private void persist() throws IOException {
        try { persistence.write(path,encode(),MAX_BYTES); }
        catch (IOException | RuntimeException failure) { fenced=true; if (failure instanceof IOException io) throw io; throw new IOException("Snapshot journal persistence failed; reopen it",failure); }
    }
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Snapshot RPC active; file ownership retained");
        if (closed && !channel.isOpen()) return; closed=true; try { if (lock.isValid()) lock.release(); } finally { channel.close(); }
    }
    private void capacity() throws IOException {
        if (entries.size()!=initial.fence().participants().size() || entries.isEmpty() || entries.size()>SnapshotFence.MAX_PARTICIPANTS) throw new IOException("Incomplete snapshot route set");
        requireControl(lastHome); requireLineage(homeRoute,pending.descriptor()); homes.restore(pending.command(),pending.descriptor());
        if (!initial.fence().homeGroup().equals(homeRoute.groupId()) || initial.fence().homeEpoch()!=homeRoute.rangeEpoch()) throw new IOException("Snapshot home route differs from definition");
        long routes=SessionWindowJournal.routeSize(homeRoute)+futureRoute(homeRoute);
        for (var entry:entries.values()) {
            data.restoreSnapshotGate(RatisSnapshotRecord.install(initial.fence(),entry.route.groupId()),entry.route); validateScope(entry.route);
            routes+=SessionWindowJournal.routeSize(entry.route)+futureRoute(entry.route);
        }
        if (routes>MAX_ROUTES) throw new IOException("Snapshot frozen/future routes exceed aggregate budget");
        long reserve=initial.fence().encode().length+routes+65536L+entries.size()*(1024L+ScopedTransactionRpc.HANDLE_BYTES+1);
        if (reserve+32>MAX_BYTES) throw new IOException("Snapshot future persistence capacity unavailable");
    }
    private static long futureRoute(RangeDescriptor route) { return 60L+(route.lowerInclusive()==null ? 0 : route.lowerInclusive().length)+(route.upperExclusive()==null ? 0 : route.upperExclusive().length)+RangeDirectoryCodec.MAX_REPLICAS*(57L+128+256); }
    private void validateScope(RangeDescriptor route) {
        var directory=data.directory(); int prefix=directory.routingPrefixBytes(); var participant=initial.fence().participant(route.groupId());
        byte[] routeLower=route.lowerInclusive(),routeUpper=route.upperExclusive();
        if (prefix>0 && (routeLower!=null && routeLower.length!=prefix || routeUpper!=null && routeUpper.length!=prefix)) throw new IllegalArgumentException("Snapshot route bounds differ from projection");
        for (var span:participant.scope().spans()) {
            if (!directory.columns().contains(span.columnId())) throw new IllegalArgumentException("Snapshot scope belongs to another column");
            byte[] lower=span.lowerInclusive(),upper=span.upperExclusive(),projectedLower=project(lower,prefix),projectedUpper=project(upper,prefix);
            if (span.isPoint()) {
                if (routeLower!=null && Arrays.compareUnsigned(projectedLower,routeLower)<0 || routeUpper!=null && Arrays.compareUnsigned(projectedLower,routeUpper)>=0) throw new IllegalArgumentException("Snapshot point crosses frozen route");
            } else if (routeLower!=null && (projectedLower==null || Arrays.compareUnsigned(projectedLower,routeLower)<0)
                    || routeUpper!=null && (projectedUpper==null || Arrays.compareUnsigned(projectedUpper,routeUpper)>0 || Arrays.equals(projectedUpper,routeUpper) && prefix>0 && upper.length>prefix)) throw new IllegalArgumentException("Snapshot scope crosses frozen route");
        }
    }
    private static byte[] project(byte[] value,int prefix) { if (value==null || prefix==0) return value; if (value.length<prefix) throw new IllegalArgumentException("Snapshot scope key shorter than projection"); return Arrays.copyOf(value,prefix); }
    /** The definition occurs once; home images and per-group native/request proofs contain only compact progress. */
    private byte[] encode() throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(MAGIC); out.write(identity); out.write(homeIdentity); out.writeByte(captureMode==RatisSnapshotCaptureRequest.Mode.READ_ONLY ? 0 : 1);
        blob(out,initial.fence().encode()); route(out,homeRoute); operation(out,lastHome); status(out,observed);
        route(out,pending.descriptor()); var command=pending.command(); uuid(out,command.mutations().getFirst().columnId()); out.write(command.schemaDigest()); operation(out,command.operationId()); out.writeLong(command.acknowledgedThrough());
        status(out,expected(pending)); status(out,next(pending)); out.writeByte(homeResult==null ? 0 : homeResult.status().ordinal()+1); out.writeBoolean(homeSuperseded);
        out.writeBoolean(homeRetirementProof!=null); if (homeRetirementProof!=null) { out.writeLong(homeRetirementProof.minimumEpoch()); positionValue(out,homeRetirementProof.position()); }
        for (var entry:entries.values()) {
            route(out,entry.route); out.writeBoolean(entry.installFrozen); out.writeByte(entry.installReceipt==null ? 0 : entry.installReceipt.suppressed() ? 1 : 2);
            if (entry.installReceipt!=null && !entry.installReceipt.suppressed()) positionValue(out,entry.installReceipt.position());
            out.writeBoolean(entry.gate!=null);
            if (entry.gate!=null) { position(out,entry.gate.installed()); position(out,entry.gate.released()); if (entry.gate.released()!=null) out.write(entry.gate.decisionDigest()); }
            position(out,entry.ready);
            if (entry.capture==null) out.writeByte(0);
            else {
                var target=entry.capture.target(); boolean original=entry.route.equals(target.descriptor()); out.writeByte(original ? 1 : 2); if (!original) route(out,target.descriptor());
                out.writeInt(target.descriptor().replicas().indexOf(target.replica())); uuid(out,target.incarnation()); uuid(out,entry.capture.request().snapshotId()); out.writeLong(entry.capture.request().ttl().toMillis());
            }
            token(out,entry.token); out.writeBoolean(entry.certifiedHandle!=null); if (entry.certifiedHandle!=null) { var handle=ByteBuffer.allocate(ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.writeHandle(handle,entry.certifiedHandle); out.write(handle.array()); } out.writeBoolean(entry.captureLost); out.writeBoolean(entry.captureReleaseFrozen); out.writeBoolean(entry.captureReleased); out.writeBoolean(entry.captureReleaseLost); out.writeBoolean(entry.releaseFrozen); position(out,entry.releaseReceipt);
            out.writeBoolean(entry.retirementProof!=null); if (entry.retirementProof!=null) { out.writeLong(entry.retirementProof.minimumEpoch()); positionValue(out,entry.retirementProof.position()); }
        }
        out.writeBoolean(releaseRequested); out.writeLong(requestedTtl==null ? -1 : requestedTtl.toMillis());
        out.flush(); if (bytes.size()>MAX_BYTES-32) throw new IOException("Snapshot journal exceeds byte bound"); return bytes.toByteArray();
    }
    private void decode(byte[] body) throws IOException {
        try {
            var in=ByteBuffer.wrap(body); if (in.getInt()!=MAGIC) throw new IOException("Invalid snapshot journal format");
            byte[] bound=new byte[32],homeBound=new byte[32]; in.get(bound).get(homeBound);
            int mode=in.get() & 255; if (mode>1) throw new IOException("Invalid frozen snapshot capture mode"); captureMode=mode==0 ? RatisSnapshotCaptureRequest.Mode.READ_ONLY : RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED;identity=dataBinding(data,captureMode);
            if (!Arrays.equals(bound,identity) || !Arrays.equals(homeBound,homeIdentity)) throw new IOException("Snapshot journal client identity changed");
            initial=SnapshotFenceStatus.begin(SnapshotFence.decode(blob(in,SnapshotFence.MAX_BYTES))); homeRoute=SessionWindowJournal.route(in); lastHome=operation(in); observed=status(in);
            if (observed!=null) monotone(initial,observed);
            var pendingRoute=SessionWindowJournal.route(in); requireLineage(homeRoute,pendingRoute); UUID column=uuid(in); byte[] schema=new byte[32]; in.get(schema); var operation=operation(in); long ack=in.getLong();
            var before=status(in); var after=status(in); if (after==null) throw new IOException("Snapshot home pending request has no next image");
            byte[] key=ByteBuffer.allocate(16).putLong(initial.fence().fenceId().getMostSignificantBits()).putLong(initial.fence().fenceId().getLeastSignificantBits()).array();
            pending=homes.restore(new DistributedCommand(pendingRoute.groupId(),pendingRoute.rangeEpoch(),schema,operation,List.of(new Mutation(Mutation.Kind.PUT,column,key,after.encode())),List.of(new KeyPredicate(column,key,before==null ? null : before.encode())),ack),pendingRoute);
            int result=in.get() & 255; if (result>CommandResult.Status.values().length) throw new IOException("Invalid snapshot home result kind");
            homeResult=result==0 ? null : new CommandResult(CommandResult.Status.values()[result-1],List.of()); homeSuperseded=flag(in);
            if (flag(in)) mergeHomeRetirement(new SnapshotHomeFenceProof(homes.identity(initial.fence(),homeRoute),in.getLong(),positionValue(in)));
            for (var participant:initial.fence().participants()) {
                var entry=new Entry(SessionWindowJournal.route(in)); if (!entry.route.groupId().equals(participant.groupId())) throw new IOException("Snapshot journal route order changed"); entries.put(participant.groupId(),entry);
                entry.installFrozen=flag(in); int receipt=in.get() & 255; if (receipt>2) throw new IOException("Invalid INSTALL receipt kind"); entry.installReceipt=receipt==0 ? null : new RatisSnapshotRecord.Result(receipt==1 ? null : positionValue(in));
                if (flag(in)) { var installed=position(in); var released=position(in); byte[] digest=null; if (released!=null) { digest=new byte[32]; in.get(digest); } entry.gate=new SnapshotGate(initial.fence(),entry.route.groupId(),installed,released,digest); }
                entry.ready=position(in); int capture=in.get() & 255; if (capture>2) throw new IOException("Invalid capture target kind");
                if (capture!=0) {
                    var captureRoute=capture==1 ? entry.route : SessionWindowJournal.route(in); requireLineage(entry.route,captureRoute); int member=in.getInt();
                    if (member<0 || member>=captureRoute.replicas().size()) throw new IOException("Invalid frozen capture member"); var owner=uuid(in); var snapshot=uuid(in); long ttl=in.getLong();
                    var target=data.restoreSnapshotTarget(initial.fence(),captureRoute,captureRoute.replicas().get(member),owner); entry.capture=data.restoreSnapshotCapture(target,new RatisSnapshotCaptureRequest(initial.fence(),owner,snapshot,Duration.ofMillis(ttl),captureMode));
                }
                entry.token=token(in); entry.certifiedHandle=flag(in) ? ScopedTransactionRpc.readHandle(in) : null; entry.captureLost=flag(in); entry.captureReleaseFrozen=flag(in); entry.captureReleased=flag(in); entry.captureReleaseLost=flag(in); entry.releaseFrozen=flag(in); entry.releaseReceipt=position(in);
                if (flag(in)) mergeRetirement(entry,new SnapshotFenceProof(initial.fence(),entry.route.groupId(),in.getLong(),positionValue(in)));
                validateEntry(entry);
            }
            releaseRequested=flag(in); long ttl=in.getLong(); requestedTtl=ttl== -1 ? null : Duration.ofMillis(ttl); requireTtl(requestedTtl,captureMode);
            if (requestedTtl!=null) for (var entry:entries.values()) if (entry.capture!=null && !requestedTtl.equals(entry.capture.request().ttl())) throw new IOException("Frozen capture differs from requested TTL");
            if (in.hasRemaining() || !lastHome.equals(pending.command().operationId())) throw new IOException("Invalid trailing snapshot journal/home identity");
            if (before!=null) { if (observed==null) throw new IOException("Pending home CAS lacks observed expected image"); monotone(before,observed); }
            if (homeSuperseded && (homeResult!=null || !supersedes())) throw new IOException("Snapshot home retirement lacks newer proof");
            if (homeResult!=null && homeResult.status()==CommandResult.Status.SUCCESS) { if (observed==null) throw new IOException("Successful home result lacks observed image"); monotone(after,observed); }
        } catch (RuntimeException invalid) { throw new IOException("Invalid snapshot journal",invalid); }
    }
    private static void requireTtl(Duration ttl,RatisSnapshotCaptureRequest.Mode mode) {
        Objects.requireNonNull(mode);if(ttl!=null)RatisSnapshotCaptureRequest.requireTtl(ttl,mode);
    }
    private void validateEntry(Entry entry) throws IOException {
        if (entry.capture!=null && entry.capture.request().mode()!=captureMode || captureMode==RatisSnapshotCaptureRequest.Mode.READ_ONLY && entry.certifiedHandle!=null
                || captureMode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED && (entry.token==null)!=(entry.certifiedHandle==null)) throw new IOException("Saved capture differs from frozen snapshot mode");
        if (entry.certifiedHandle!=null && (!entry.certifiedHandle.owner().equals(entry.token.ownerIncarnation()) || !entry.certifiedHandle.position().equals(entry.token.position()))) throw new IOException("Certified handle differs from original capture owner/cut");
        if (entry.captureReleased && entry.captureReleaseLost || entry.captureReleaseLost && !entry.captureLost) throw new IOException("Contradictory capture release settlement");
        if (entry.captureReleaseFrozen && (entry.capture==null || !entry.captureLost || entry.token==null && !captureRecoveryAllowed()) || (entry.captureReleased || entry.captureReleaseLost) && !entry.captureReleaseFrozen || entry.captureReleased && entry.token==null) throw new IOException("Capture release lacks its exact frozen target/token");
        if (entry.installReceipt!=null && !entry.installFrozen) throw new IOException("INSTALL receipt lacks frozen request");
        if (entry.gate!=null) validateGate(entry,entry.gate);
        if (entry.ready!=null && (entry.installReceipt==null || entry.installReceipt.suppressed() || !entry.ready.equals(entry.installReceipt.position())
                || entry.gate==null || !entry.ready.equals(entry.gate.installed()))) throw new IOException("Ready proof lacks forced INSTALL receipt/native proof");
        if (entry.capture!=null) {
            if (observed==null || !observed.allReady() || entry.ready==null || !entry.ready.equals(observed.progress(entry.route.groupId()).ready())) throw new IOException("Frozen capture lacks all-ready home/local proof");
            requireLineage(entry.route,entry.capture.target().descriptor());
        }
        if (entry.token!=null) {
            if (entry.capture==null || !entry.capture.request().owner().equals(entry.token.ownerIncarnation()) || !entry.capture.request().snapshotId().equals(entry.token.snapshotId()) || !atLeast(entry.token.position(),entry.ready)) throw new IOException("Saved capture token lacks its exact request/ready proof");
        }
        if (observed!=null && observed.progress(entry.route.groupId()).capture()!=null && !entry.captureLost) {
            var proof=observed.progress(entry.route.groupId()).capture();
            if (entry.capture==null || !entry.capture.request().owner().equals(proof.ownerIncarnation()) || !entry.capture.request().snapshotId().equals(proof.snapshotId()) || entry.token!=null && !entry.token.equals(proof)) throw new IOException("Home capture has no matching frozen target/token");
        }
        if (entry.releaseFrozen && (observed==null || !observed.terminal()) || entry.releaseReceipt!=null && !entry.releaseFrozen) throw new IOException("RELEASE state lacks frozen terminal request");
        if (entry.releaseReceipt!=null) checkReleaseReceipt(entry,entry.releaseReceipt);
    }
    private void checkReleaseReceipt(Entry entry,ApplyPosition receipt) throws IOException {
        if (receipt==null || receipt.term()<=0 || receipt.index()<=0) throw new IOException("Invalid RELEASE receipt");
        var progress=observed.progress(entry.route.groupId()); var before=progress.capture()==null ? progress.ready() : progress.capture().position();
        if (before!=null && (!atLeast(receipt,before) || receipt.index()<=before.index()) || entry.gate!=null && entry.gate.installed()!=null
                && (!atLeast(receipt,entry.gate.installed()) || receipt.index()<=entry.gate.installed().index())) throw new IOException("RELEASE receipt precedes saved install/home proof");
    }
    private void status(DataOutputStream out,SnapshotFenceStatus status) throws IOException {
        out.writeBoolean(status!=null); if (status==null) return;
        if (!initial.fence().equals(status.fence())) throw new IOException("Snapshot status definition changed"); out.writeByte(status.phase().ordinal()); out.writeBoolean(status.consumed());
        for (var progress:status.progress()) { position(out,progress.ready()); token(out,progress.capture()); }
    }
    private SnapshotFenceStatus status(ByteBuffer in) throws IOException {
        if (!flag(in)) return null; int phase=in.get() & 255; if (phase>=SnapshotFenceStatus.Phase.values().length) throw new IOException("Invalid compact snapshot phase"); boolean consumed=flag(in);
        var ready=new ArrayList<ApplyPosition>(); var tokens=new ArrayList<SnapshotFenceStatus.Capture>();
        for (int i=0;i<initial.fence().participants().size();i++) { ready.add(position(in)); tokens.add(token(in)); }
        var status=initial;
        for (int i=0;i<ready.size();i++) if (ready.get(i)!=null) status=status.ready(initial.fence().participants().get(i).groupId(),ready.get(i));
        for (int i=0;i<tokens.size();i++) if (tokens.get(i)!=null) status=status.capture(initial.fence().participants().get(i).groupId(),tokens.get(i));
        status=switch (SnapshotFenceStatus.Phase.values()[phase]) { case INSTALLING -> status; case COMPLETE -> status.complete(); case ABORT -> status.abort(); };
        return consumed ? status.consume() : status;
    }
    private static void route(DataOutputStream out,RangeDescriptor route) throws IOException { var b=ByteBuffer.allocate(Math.toIntExact(SessionWindowJournal.routeSize(route))); SessionWindowJournal.route(b,route); out.write(b.array()); }
    private static void operation(DataOutputStream out,OperationId operation) throws IOException { uuid(out,operation.sessionId()); out.writeLong(operation.epoch()); out.writeLong(operation.sequence()); }
    private static OperationId operation(ByteBuffer in) { return new OperationId(uuid(in),in.getLong(),in.getLong()); }
    private static void uuid(DataOutputStream out,UUID uuid) throws IOException { out.writeLong(uuid.getMostSignificantBits()); out.writeLong(uuid.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    private static void positionValue(DataOutputStream out,ApplyPosition position) throws IOException { out.writeLong(position.term()); out.writeLong(position.index()); }
    private static ApplyPosition positionValue(ByteBuffer in) { return new ApplyPosition(in.getLong(),in.getLong()); }
    private static void position(DataOutputStream out,ApplyPosition position) throws IOException { out.writeBoolean(position!=null); if (position!=null) positionValue(out,position); }
    private static ApplyPosition position(ByteBuffer in) throws IOException { return flag(in) ? positionValue(in) : null; }
    private static void token(DataOutputStream out,SnapshotFenceStatus.Capture token) throws IOException { out.writeBoolean(token!=null); if (token!=null) { uuid(out,token.ownerIncarnation()); uuid(out,token.snapshotId()); positionValue(out,token.position()); } }
    private static SnapshotFenceStatus.Capture token(ByteBuffer in) throws IOException { return flag(in) ? new SnapshotFenceStatus.Capture(uuid(in),uuid(in),positionValue(in)) : null; }
    private static boolean flag(ByteBuffer in) throws IOException { int flag=in.get() & 255; if (flag>1) throw new IOException("Invalid snapshot journal flag"); return flag==1; }
    private static void blob(DataOutputStream out,byte[] bytes) throws IOException { out.writeInt(bytes.length); out.write(bytes); }
    private static byte[] blob(ByteBuffer in,int maximum) throws IOException { int size=in.getInt(); if (size<0 || size>maximum || size>in.remaining()) throw new IOException("Invalid snapshot journal blob"); byte[] bytes=new byte[size]; in.get(bytes); return bytes; }
}
