package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** Global barrier and one bounded materialization wave. The borrowed cohort transport must qualify actual native owners. */
public final class RatisDatabaseBackupDriver implements AutoCloseable {
    public record Policy(Duration retention,Duration maximumMaterialization,int maximumCohorts) {
        public Policy {
            Objects.requireNonNull(retention);Objects.requireNonNull(maximumMaterialization);
            RatisSnapshotCaptureRequest.requireTtl(retention,RatisSnapshotCaptureRequest.Mode.READ_ONLY);
            if(maximumMaterialization.isZero() || maximumMaterialization.isNegative() || maximumMaterialization.compareTo(retention)>0
                    || !maximumMaterialization.equals(Duration.ofMillis(maximumMaterialization.toMillis())) || maximumCohorts<1 || maximumCohorts>1024)
                throw new IllegalArgumentException("Unsupported backup retention/cohort envelope");
        }
    }
    public record Vector(DatabaseDomainRoster roster,List<DatabaseBackupDefinition.Domain> domains) {public Vector{domains=List.copyOf(domains);}}
    @FunctionalInterface public interface Authority {Vector read()throws IOException;}
    /** No capture factory is permitted on retry: started requests must return original tokens or explicit loss. */
    public interface CohortTransport {
        /** Validate the complete declared work against actual registry/pool pin counts, bytes and retention before gating. */
        CompletableFuture<Void> requireSupported(DatabaseBackupDefinition definition,Policy policy)throws IOException;
        /** Fresh process/pool/configuration qualification, after every subfence is READY. */
        CompletableFuture<List<RatisDatabaseBackupJournal.CohortRequest>> qualify(DatabaseBackupDefinition definition,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen,Policy policy)throws IOException;
        CompletableFuture<Map<UUID,RatisSnapshotCaptureRequest.Result>> capture(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,RatisRoutedClient.PreparedSnapshotCapture> frozen)throws IOException;
        CompletableFuture<Void> requireLive(RatisDatabaseBackupJournal.CohortRequest request,Map<UUID,SnapshotFenceStatus.Capture> original)throws IOException;
        /** Must wait for actual capture/export actions to return before closing adopted native pins. */
        CompletableFuture<Void> cancelAndDrain(RatisDatabaseBackupJournal.CohortRequest request)throws IOException;
    }
    @FunctionalInterface public interface Operators {Path source(DistributedSchema.Operator operator)throws IOException;}
    public record Binding(RatisDurableSnapshot journal,RatisSnapshotCoordinator coordinator,RatisRoutedClient routed,
                          Path exportPath,RatisExportJournal.Definition exportDefinition,RatisExportClient.ChunkSource chunks) {
        public Binding {Objects.requireNonNull(journal);Objects.requireNonNull(coordinator);Objects.requireNonNull(routed);Objects.requireNonNull(exportPath);Objects.requireNonNull(exportDefinition);Objects.requireNonNull(chunks);exportPath=exportPath.toAbsolutePath().normalize();}
    }
    public record Progress(RatisDatabaseBackupJournal.Phase phase,int copiedChunks,boolean complete,boolean cleanupPending) {}
    private final RatisDatabaseBackupJournal journal;private final RatisDatabaseBackupArchive archive;private final RatisDatabaseCaptureCoordinator captures;
    private final CohortTransport transport;private final Operators operators;private final Map<UUID,Binding> bindings;
    private boolean active,closed,cohortsDrained;private volatile boolean cancelRequested;
    public RatisDatabaseBackupDriver(RatisDatabaseBackupJournal journal,RatisDatabaseBackupArchive archive,Authority authority,
            CohortTransport transport,Operators operators,Map<UUID,Binding> bindings)throws IOException {
        this.journal=Objects.requireNonNull(journal);this.archive=Objects.requireNonNull(archive);Objects.requireNonNull(authority);this.transport=Objects.requireNonNull(transport);this.operators=Objects.requireNonNull(operators);this.bindings=Map.copyOf(bindings);
        var expected=new HashSet<UUID>();for(var d:journal.definition().domains())for(var fence:d.fences()){
            expected.add(fence.fenceId());var b=this.bindings.get(fence.fenceId());if(b==null || !fence.equals(b.journal().snapshot().initial().fence()) || !fence.equals(b.exportDefinition().fence())
                    || b.journal().snapshot().captureMode()!=RatisSnapshotCaptureRequest.Mode.READ_ONLY || !journal.policy().retention().equals(b.journal().snapshot().requestedTtl()) || !journal.policy().retention().equals(b.exportDefinition().snapshotTtl())
                    || !new HashSet<>(b.exportDefinition().columns()).equals(d.directory().columns()))throw new IOException("Backup subjournal/export binding differs from original definition");
        }
        if(!expected.equals(this.bindings.keySet()))throw new IOException("Backup subjournal coverage differs");
        var captureBindings=new HashMap<UUID,RatisDatabaseCaptureCoordinator.CaptureBinding>();this.bindings.forEach((id,binding)->captureBindings.put(id,new RatisDatabaseCaptureCoordinator.CaptureBinding(binding.journal(),binding.coordinator(),binding.routed())));captures=new RatisDatabaseCaptureCoordinator(journal,authority,transport,captureBindings);
    }
    /** Bounded global capture dispatch; at most one HOME state change or materialization chunk per step. */
    public Progress step(RequestContext context)throws IOException {
        Objects.requireNonNull(context);synchronized(this){if(closed || active)throw new IOException("Backup driver closed or work active");active=true;}
        try {
            var saved=journal.snapshot();
            if(cancelRequested && saved.phase()!=RatisDatabaseBackupJournal.Phase.COMPLETE){abandonOwned(RatisDatabaseBackupJournal.Phase.CANCELLED);return progress();}
            if(saved.phase()!=RatisDatabaseBackupJournal.Phase.GATES && saved.phase()!=RatisDatabaseBackupJournal.Phase.MATERIALIZING && saved.phase()!=RatisDatabaseBackupJournal.Phase.COMPLETE && saved.phase()!=RatisDatabaseBackupJournal.Phase.CANCELLED && saved.phase()!=RatisDatabaseBackupJournal.Phase.LOST)checkDeadline(saved);
            switch(saved.phase()) {
                case GATES,REQUESTS,CAPTURED,REPORTED -> captures.step(context);
                case RELEASED -> journal.phase(RatisDatabaseBackupJournal.Phase.MATERIALIZING);
                case MATERIALIZING -> materialize(context);
                case COMPLETE -> {qualifyComplete();cleanupSources(context);}
                case CANCELLED -> abandonOwned(RatisDatabaseBackupJournal.Phase.CANCELLED);
                case LOST -> abandonOwned(RatisDatabaseBackupJournal.Phase.LOST);
            }
            return progress();
        }catch(RatisSnapshotLostException lost){if(journal.snapshot().phase()!=RatisDatabaseBackupJournal.Phase.COMPLETE)journal.phase(RatisDatabaseBackupJournal.Phase.LOST);throw lost;}
        finally{synchronized(this){active=false;notifyAll();}}
    }
    private void checkDeadline(RatisDatabaseBackupJournal.Snapshot saved)throws IOException{
        long now=System.currentTimeMillis(),started=saved.dispatchStartedMillis();
        if(started<=0 || now<started || now-started>journal.policy().maximumMaterialization().toMillis())throw new RatisSnapshotLostException("Backup exceeded original dispatch deadline; reopen never renews retention");
    }
    private Progress progress()throws IOException{var s=journal.snapshot();boolean pending=s.phase()==RatisDatabaseBackupJournal.Phase.COMPLETE?cleanupPending():(s.phase()==RatisDatabaseBackupJournal.Phase.CANCELLED || s.phase()==RatisDatabaseBackupJournal.Phase.LOST) && abandonmentPending();return new Progress(s.phase(),s.chunks().size(),s.phase()==RatisDatabaseBackupJournal.Phase.COMPLETE && !pending,pending);}
    private List<Binding> ordered(){return bindings.entrySet().stream().sorted(Map.Entry.comparingByKey(RangeDirectoryCodec.UUID_ORDER)).map(Map.Entry::getValue).toList();}
    private void materialize(RequestContext context)throws IOException{
        // Cold continuation can verify independently copied full coverage without touching volatile source pins.
        try{var manifest=journal.manifest();completeArchive(manifest);journal.phase(RatisDatabaseBackupJournal.Phase.COMPLETE);return;}catch(IllegalArgumentException incomplete){}
        checkDeadline(journal.snapshot());
        for(var b:ordered()){
            boolean existed=Files.exists(b.exportPath(),LinkOption.NOFOLLOW_LINKS);
            try(var export=existed?RatisExportJournal.reopen(b.exportPath()):RatisExportJournal.create(b.exportPath(),b.exportDefinition())){
                var state=export.snapshot();if(!state.definition().equals(b.exportDefinition())){
                    // Definition has defensive array accessors, so compare the authority-bearing fields explicitly.
                    if(!state.definition().jobId().equals(b.exportDefinition().jobId()) || !state.definition().fence().equals(b.exportDefinition().fence()) || !Arrays.equals(state.definition().sourceIdentity(),b.exportDefinition().sourceIdentity()) || !state.definition().sourceJournal().equals(b.exportDefinition().sourceJournal()) || !state.definition().columns().equals(b.exportDefinition().columns()) || !state.definition().limits().equals(b.exportDefinition().limits()) || !state.definition().snapshotTtl().equals(b.exportDefinition().snapshotTtl()))throw new IOException("Original export definition changed");
                }
                export.bindSource(b.journal().snapshot());if(export.snapshot().phase()==RatisExportJournal.Phase.EXPORTING || !export.snapshot().sourceCleanup().isEmpty())b.chunks().bind(export.snapshot());
                for(var chunk:export.snapshot().chunks())if(copy(export,b,chunk))return;
                if(export.snapshot().phase()==RatisExportJournal.Phase.EXPORTING){
                    var group=export.unfinishedGroup();var initial=export.snapshot();
                    try{var chunk=export.export(group,(artifact,intent)->await(b.chunks().write(new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(initial,group),intent),artifact,context)));copy(export,b,chunk);return;}
                    catch(IOException failure){throw failure;}catch(Exception failure){throw new IOException("Backup chunk failed",failure);}
                }
                if(!export.snapshot().sourceCleanup().isEmpty()){
                    UUID group=export.snapshot().sourceCleanup().iterator().next();await(b.chunks().release(RatisExportJournal.sourceDefinition(export.snapshot(),group),context));
                    export.sourceReleased(group);return;
                }
                // All local copies are durable; public pins remain owned until the whole global graph is complete.
            }
        }
        var seen=new HashSet<String>();for(var d:journal.definition().domains())for(var c:d.schema().columns())if(c.operator()!=null && seen.add(HexFormat.of().formatHex(c.operator().artifactSha256()))) {
            if(journal.snapshot().operators().stream().noneMatch(a->Arrays.equals(a.sha256(),c.operator().artifactSha256()))){UUID id=asset("operator/"+HexFormat.of().formatHex(c.operator().artifactSha256()));var copied=archive.copyOperator(id,operators.source(c.operator()),c.operator().artifactSha256());journal.operator(copied);return;}
        }
        var manifest=journal.manifest();completeArchive(manifest);journal.phase(RatisDatabaseBackupJournal.Phase.COMPLETE);
    }
    private void qualifyComplete()throws IOException {
        try {if(!Arrays.equals(journal.manifest().encode(),archive.reopenComplete().encode()))throw new IOException("Published archive differs from journal COMPLETE graph");}
        catch(IllegalArgumentException|org.rocksdb.RocksDBException malformed){throw new IOException("COMPLETE archive qualification failed",malformed);}
    }
    private void settleExport(RatisExportJournal export,Binding binding)throws IOException {
        binding.journal().requestRelease();
        if(export.snapshot().phase()==RatisExportJournal.Phase.MATERIALIZED || export.snapshot().phase()==RatisExportJournal.Phase.READY)export.markReady(binding.journal().snapshot());
        else export.confirmSourceSettlement(binding.journal().snapshot());
        var phase=export.snapshot().phase();
        if(phase==RatisExportJournal.Phase.READY){for(var chunk:export.snapshot().chunks())export.acknowledge(chunk);export.release();}
        else if(phase==RatisExportJournal.Phase.CANCELLED || phase==RatisExportJournal.Phase.SNAPSHOT_LOST)export.release();
        else if(phase!=RatisExportJournal.Phase.RELEASED)throw new IOException("Export settlement lacks a releasable terminal phase");
        export.reclaimArtifacts();
    }
    private boolean abandonmentPending()throws IOException {
        if(!cohortsDrained)return true;
        for(var b:ordered()){
            var snapshot=b.journal().snapshot();var home=snapshot.observedHome();
            for(var p:snapshot.participants()){
                boolean retired=home!=null && home.consumed() && p.retirementProof()!=null;
                boolean released=home!=null && home.terminal() && p.nativeGate()!=null && p.releaseReceipt()!=null
                        && p.releaseReceipt().equals(p.nativeGate().released()) && Arrays.equals(home.terminalDecision().digest(),p.nativeGate().decisionDigest());
                if(!retired && !released || p.captureToken()!=null && !p.captureReleased() && !p.captureReleaseLost())return true;
            }
        }return false;
    }
    private void completeArchive(DatabaseBackupManifest manifest)throws IOException {try{archive.complete(manifest);}catch(org.rocksdb.RocksDBException invalid){throw new IOException("Archive native qualification failed",invalid);}}
    private boolean cleanupPending()throws IOException {
        if(!cohortsDrained)return true;
        for(var b:ordered()){
            if(b.journal().snapshot().participants().stream().anyMatch(p->!p.captureReleased() && !p.captureReleaseLost()))return true;
            if(Files.exists(b.exportPath(),LinkOption.NOFOLLOW_LINKS))try(var export=RatisExportJournal.reopen(b.exportPath())){if(!export.snapshot().sourceCleanup().isEmpty() || export.snapshot().phase()!=RatisExportJournal.Phase.RELEASED || !export.snapshot().artifactsReclaimed())return true;}
        }return false;
    }
    /** Archive publication is independent of pins; resource completion still requires this actual cleanup drain. */
    private void cleanupSources(RequestContext context)throws IOException {
        // Finish every remote writer before closing any aliases of a shared native pool cohort.
        for(var b:ordered())if(Files.exists(b.exportPath(),LinkOption.NOFOLLOW_LINKS))try(var export=RatisExportJournal.reopen(b.exportPath())){
            if(!export.snapshot().sourceCleanup().isEmpty()){
                b.chunks().bind(export.snapshot());UUID group=export.snapshot().sourceCleanup().iterator().next();
                await(b.chunks().release(RatisExportJournal.sourceDefinition(export.snapshot(),group),context));export.sourceReleased(group);return;
            }
        }
        if(!cohortsDrained){for(var request:journal.snapshot().requests())await(transport.cancelAndDrain(request));cohortsDrained=true;}
        for(var b:ordered()){
            for(var p:b.journal().snapshot().participants())if(p.captureToken()!=null && !p.captureReleased() && !p.captureReleaseLost()){b.journal().releaseCapture(p.route().groupId());return;}
            if(Files.exists(b.exportPath(),LinkOption.NOFOLLOW_LINKS))try(var export=RatisExportJournal.reopen(b.exportPath())){settleExport(export,b);}
        }
    }
    private boolean copy(RatisExportJournal export,Binding binding,RatisExportJournal.Chunk chunk)throws IOException{
        long ordinal=export.snapshot().chunks().stream().filter(c->c.group().equals(chunk.group()) && c.column().equals(chunk.column()) && c.ordinal()<chunk.ordinal()).count();
        var old=journal.snapshot().chunks().stream().filter(c->c.groupId().equals(chunk.group()) && c.columnId().equals(chunk.column()) && c.ordinal()==ordinal).findFirst();if(old.isPresent())return false;
        UUID id=asset(binding.exportDefinition().jobId()+"/"+chunk.group()+"/"+chunk.ordinal());
        var source=new NativeRangeSstExport.Manifest(chunk.group(),chunk.rangeEpoch(),chunk.column(),binding.exportDefinition().fence().schemaDigest(),chunk.cut(),chunk.lowerInclusive(),chunk.upperExclusive(),chunk.rows(),chunk.payloadBytes(),chunk.fileBytes(),chunk.rows()==0?new byte[0]:chunk.sha256(),chunk.file());
        try{var copied=archive.copy(binding.exportDefinition().fence().domainId(),ordinal,id,source);journal.copied(copied);return true;}catch(org.rocksdb.RocksDBException failure){throw new IOException("Copied archive qualification failed",failure);}
    }
    private UUID asset(String key){return UUID.nameUUIDFromBytes((journal.definition().backupId()+"/"+key).getBytes(StandardCharsets.UTF_8));}
    private void abandonOwned(RatisDatabaseBackupJournal.Phase terminal)throws IOException{
        journal.phase(terminal);if(!cohortsDrained){for(var request:journal.snapshot().requests())await(transport.cancelAndDrain(request));cohortsDrained=true;}
        for(var b:ordered()){b.journal().requestRelease();b.coordinator().step(RatisSnapshotCoordinator.StopAt.RELEASED);for(var p:b.journal().snapshot().participants())if(p.captureToken()!=null && !p.captureReleased() && !p.captureReleaseLost())b.journal().releaseCapture(p.route().groupId());}
    }
    /** Cancellation is intent only while actual work runs; the borrowed backend owns actual drain. */
    public synchronized void cancel(){cancelRequested=true;}
    @Override public synchronized void close()throws IOException{if(active)throw new IOException("Backup actual work still active; ownership retained");closed=true;}
    private static <T>T await(CompletableFuture<T> future)throws IOException{boolean interrupted=false;try{while(true){try{return future.get();}catch(InterruptedException retry){interrupted=true;}catch(ExecutionException failed){var cause=failed.getCause();if(cause instanceof IOException io)throw io;throw new IOException("Actual export action failed",cause);}catch(CancellationException canceled){throw new IOException("Actual export action canceled",canceled);}}}finally{if(interrupted)Thread.currentThread().interrupt();}}
}
