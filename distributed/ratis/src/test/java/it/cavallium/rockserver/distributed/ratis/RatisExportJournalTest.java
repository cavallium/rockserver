package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Kernel artifacts are bounded immutable byte fixtures, not claims of native SST acceptance. */
class RatisExportJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID column=new UUID(0,1),job=new UUID(0,2),home=new UUID(0,3);
    RangeDirectory directory(int count) {
        var replica=new ReplicaDescriptor(new UUID(0,10),new UUID(0,11),new UUID(0,12),"peer","localhost:12345",ReplicaDescriptor.Role.VOTER); var ranges=new ArrayList<RangeDescriptor>();
        for (int n=0;n<count;n++) ranges.add(new RangeDescriptor(new UUID(0,20+n),new UUID(0,30+n),1,0,n==0 ? null : new byte[]{(byte)128},n==count-1 ? null : new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(replica)));
        return new RangeDirectory(new UUID(0,40),new UUID(0,41),new UUID(0,42),1,new byte[32],1,Set.of(column),0,ranges);
    }
    SnapshotFence fence(RangeDirectory directory) { return RatisSnapshotPlan.create(directory,new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),new UUID(0,50),new OperationId(new UUID(0,51),1,0),home,1,new OperationId(new UUID(0,51),1,1)).fence(); }
    RatisExportJournal.Definition definition(RangeDirectory directory) { return new RatisExportJournal.Definition(job,fence(directory),column,root.resolve("original-snapshot.journal"),new byte[32],RatisExportJournal.Limits.defaults()); }
    Map<UUID,SnapshotFenceStatus.Capture> tokens(SnapshotFence fence) { var tokens=new LinkedHashMap<UUID,SnapshotFenceStatus.Capture>(); int n=0; for (var p:fence.participants()) tokens.put(p.groupId(),new SnapshotFenceStatus.Capture(new UUID(0,60+n),new UUID(0,70+n),new ApplyPosition(1,5+n++))); return tokens; }
    NativeRangeSstExport.Chunk artifact(RatisExportJournal journal,RatisExportJournal.ChunkIntent intent,byte[] end,boolean more,boolean empty) throws Exception {
        Path file=empty ? null : journal.artifactPath(intent); byte[] body={1,2,3}; if (file!=null) Files.write(file,body,StandardOpenOption.CREATE_NEW);
        var p=journal.snapshot().definition().fence().participant(intent.group());
        return new NativeRangeSstExport.Chunk(new NativeRangeSstExport.Manifest(intent.group(),p.rangeEpoch(),column,p.schemaDigest(),intent.cut(),intent.scopeStart(),end,empty ? 0 : 1,empty ? 0 : 2,empty ? 0 : body.length,empty ? new byte[0] : MessageDigest.getInstance("SHA-256").digest(body),file),more);
    }
    RatisDurableSnapshot.Snapshot releasedSource(RangeDirectory directory,SnapshotFence fence,Map<UUID,SnapshotFenceStatus.Capture> tokens) throws Exception { return source(directory,fence,tokens,true); }
    RatisDurableSnapshot.Snapshot capturedSource(RangeDirectory directory,SnapshotFence fence,Map<UUID,SnapshotFenceStatus.Capture> tokens) throws Exception { return source(directory,fence,tokens,false); }
    RatisDurableSnapshot.Snapshot source(RangeDirectory directory,SnapshotFence fence,Map<UUID,SnapshotFenceStatus.Capture> tokens,boolean released) throws Exception {
        var initial=SnapshotFenceStatus.begin(fence); var status=initial; int n=0; for (var p:fence.participants()) status=status.ready(p.groupId(),new ApplyPosition(1,2+n++)); for (var p:fence.participants()) status=status.capture(p.groupId(),tokens.get(p.groupId())); status=status.complete(); var progress=new ArrayList<RatisDurableSnapshot.ParticipantProgress>();
        try (var data=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor route,UUID col,byte[] key) { throw new AssertionError(); } public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
        })) {
            for (var route:directory.ranges()) { var token=tokens.get(route.groupId()); var target=data.restoreSnapshotTarget(fence,route,route.replicas().getFirst(),token.ownerIncarnation()); var capture=data.prepareSnapshotCapture(target,token.snapshotId(),Duration.ofSeconds(30)); var ready=status.progress(route.groupId()).ready(); var release=new ApplyPosition(1,20); var gate=SnapshotGate.install(fence,route.groupId(),ready).release(status,release);
                progress.add(new RatisDurableSnapshot.ParticipantProgress(route,true,new RatisSnapshotRecord.Result(ready),gate,ready,capture,token,null,released,released,false,true,release,null)); }
        }
        return new RatisDurableSnapshot.Snapshot(initial,status,null,null,false,new OperationId(new UUID(0,51),1,129),progress,null,released,Duration.ofSeconds(30),RatisSnapshotCaptureRequest.Mode.READ_ONLY);
    }
    @Test void contiguousMaterializationRequiresReleasedSourceBeforeReadyAndExactDestinationAcknowledgements() throws Exception {
        var directory=directory(2); var definition=definition(directory); var path=root.resolve("complete.export"); var captures=tokens(definition.fence()); List<RatisExportJournal.Chunk> chunks;
        try (var journal=RatisExportJournal.create(path,definition)) {
            assertThrows(IOException.class,() -> RatisExportJournal.reopen(path)); assertThrows(IllegalArgumentException.class,() -> journal.bindSource(releasedSource(directory,definition.fence(),captures))); journal.bindSource(capturedSource(directory,definition.fence(),captures)); var left=directory.ranges().getFirst().groupId(); var right=directory.ranges().getLast().groupId();
            var first=journal.freezeChunk(left); var chunk=journal.publish(first,artifact(journal,first,new byte[]{8},true,false)); assertEquals(RatisExportJournal.Phase.EXPORTING,journal.snapshot().phase()); assertThrows(IllegalStateException.class,() -> journal.acknowledge(chunk));
            var tail=journal.freezeChunk(left); assertArrayEquals(new byte[]{8},tail.scopeStart()); journal.publish(tail,artifact(journal,tail,new byte[]{(byte)128},false,true)); var last=journal.freezeChunk(right); journal.publish(last,artifact(journal,last,null,false,false));
            assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); journal.markReady(releasedSource(directory,definition.fence(),captures)); assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); chunks=journal.snapshot().chunks(); assertThrows(IllegalStateException.class,journal::release);
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); assertThrows(IllegalStateException.class,() -> journal.acknowledge(chunks.getLast())); for (var chunk:chunks) { journal.acknowledge(chunk); journal.acknowledge(chunk); } journal.release(); }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); assertEquals(3,journal.snapshot().acknowledged().size()); }
    }
    @Test void orphanSidecarRecoversOnlyExactPendingIntentWithoutRerunningArtifactCreation() throws Exception {
        var directory=directory(1); var path=root.resolve("orphan.export"); long checkpoint; var definition=definition(directory);
        try (var journal=RatisExportJournal.create(path,definition)) { journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence()))); var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); checkpoint=Files.size(path); journal.publish(intent,artifact(journal,intent,null,false,false)); }
        try (var file=FileChannel.open(path,StandardOpenOption.WRITE)) { file.truncate(checkpoint); file.force(true); }
        try (var journal=RatisExportJournal.reopen(path)) { assertNotNull(journal.snapshot().pending()); assertNotNull(journal.recoverChunk()); assertNull(journal.recoverChunk()); assertEquals(1,journal.snapshot().chunks().size()); assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); }
    }
    @Test void immutableReadyArtifactAndExactSourceBindingsRejectMissingChangedOrBlindExistingFiles() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("changed.export"); Path artifact;
        try (var journal=RatisExportJournal.create(path,definition)) {
            var tokens=tokens(definition.fence()); journal.bindSource(capturedSource(directory,definition.fence(),tokens));
            var invalidSource=source(directory,definition.fence(),tokens,true); assertDoesNotThrow(() -> journal.bindSource(invalidSource)); var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); artifact=journal.artifactPath(intent); Files.write(artifact,new byte[]{7},StandardOpenOption.CREATE_NEW); assertThrows(IOException.class,journal::recoverChunk);
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertThrows(IOException.class,journal::recoverChunk); }
        Files.delete(artifact);
        try (var journal=RatisExportJournal.reopen(path)) { var intent=journal.snapshot().pending(); journal.publish(intent,artifact(journal,intent,null,false,false)); journal.markReady(releasedSource(directory,definition.fence(),tokens(definition.fence()))); }
        Files.write(artifact,new byte[]{9,9,9},StandardOpenOption.TRUNCATE_EXISTING); assertThrows(IOException.class,() -> RatisExportJournal.reopen(path));
    }
    @Test void actualChunkWriterOwnershipPreventsCloseOrAnotherWriterUntilReturnAndLossNeverRecaptures() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("ownership.export"); var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence()))); var future=CompletableFuture.supplyAsync(() -> { try { return journal.export(directory.ranges().getFirst().groupId(),(file,intent) -> { entered.countDown(); if (!resume.await(5,TimeUnit.SECONDS)) throw new IOException("Writer barrier timed out"); throw new RatisSnapshotLostException("Original source unavailable"); }); } catch (Exception failure) { throw new CompletionException(failure); } });
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,journal::close); assertThrows(IOException.class,() -> RatisExportJournal.reopen(path)); } finally { resume.countDown(); }
            assertThrows(ExecutionException.class,() -> future.get(5,TimeUnit.SECONDS)); journal.failSnapshotLost(); assertThrows(IOException.class,() -> journal.freezeChunk(directory.ranges().getFirst().groupId())); journal.release();
        } finally { resume.countDown(); }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); }
    }
    @Test void incompleteFinalChunkAppendTruncatesToForcedIntentAndRecoversOnlyItsDurableSidecar() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("torn.export"); long checkpoint;
        try (var journal=RatisExportJournal.create(path,definition)) { journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence()))); var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); checkpoint=Files.size(path); journal.publish(intent,artifact(journal,intent,null,false,false)); }
        try (var file=FileChannel.open(path,StandardOpenOption.WRITE)) { file.truncate(checkpoint+20); file.force(true); }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(checkpoint,Files.size(path)); assertNotNull(journal.snapshot().pending()); assertNotNull(journal.recoverChunk()); assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); }
        byte[] corrupted=Files.readAllBytes(path); corrupted[corrupted.length-1]^=1; Files.write(path,corrupted,StandardOpenOption.TRUNCATE_EXISTING); assertThrows(IOException.class,() -> RatisExportJournal.reopen(path));
    }
    @Test void adjacentChosenSpansInferExactProjectionButOverlapGapAndNullInteriorRejectBeforeCapture() {
        var directory=directory(1); var old=fence(directory); var local=old.participants().getFirst();
        for (var spans:List.of(
                List.of(TransactionScope.Span.range(column,null,new byte[]{4}),TransactionScope.Span.range(column,new byte[]{4},null)),
                List.of(TransactionScope.Span.range(column,null,new byte[]{4}),TransactionScope.Span.range(column,new byte[]{3},null)),
                List.of(TransactionScope.Span.range(column,null,new byte[]{4}),TransactionScope.Span.range(column,new byte[]{5},null)),
                List.of(TransactionScope.Span.range(column,null,null),TransactionScope.Span.range(column,new byte[]{4},null)))) {
            var changed=new SnapshotFence(old.clusterId(),old.databaseId(),old.domainId(),old.fenceId(),old.homeGroup(),old.homeEpoch(),old.operation(),old.directoryGeneration(),old.schemaEpoch(),old.schemaDigest(),List.of(new SnapshotFence.Participant(local.rangeId(),local.groupId(),local.rangeEpoch(),local.schemaDigest(),local.installOperation(),local.releaseOperation(),new TransactionScope(spans))));
            if (Arrays.equals(spans.getFirst().upperExclusive(),spans.getLast().lowerInclusive())) assertDoesNotThrow(() -> new RatisExportJournal.Definition(job,changed,column,root.resolve("source"),new byte[32],RatisExportJournal.Limits.defaults()));
            else assertThrows(IllegalArgumentException.class,() -> new RatisExportJournal.Definition(job,changed,column,root.resolve("source"),new byte[32],RatisExportJournal.Limits.defaults()));
        }
    }

    @Test void definiteSourceLossAfterBareFilePublicationIsForcedWithoutPromotingOrRecapturingIt() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("bare-loss.export"); Path artifact;
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence()))); var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); artifact=journal.artifactPath(intent);
            assertThrows(RatisSnapshotLostException.class,() -> journal.export(intent.group(),(file,original) -> { Files.write(file,new byte[]{1,2,3},StandardOpenOption.CREATE_NEW); throw new RatisSnapshotLostException("Source lost after native publication"); }));
            assertThrows(IOException.class,journal::recoverChunk); journal.failSnapshotLost(); assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,journal.snapshot().phase()); assertEquals(3,journal.snapshot().pendingArtifactBytes()); assertTrue(journal.snapshot().chunks().isEmpty());
            var late=new NativeRangeSstExport.Chunk(new NativeRangeSstExport.Manifest(intent.group(),1,column,new byte[32],intent.cut(),intent.scopeStart(),null,1,2,3,MessageDigest.getInstance("SHA-256").digest(new byte[]{1,2,3}),artifact),false);
            assertThrows(IOException.class,() -> journal.publish(intent,late)); assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,journal.snapshot().phase()); assertTrue(journal.snapshot().chunks().isEmpty());
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,journal.snapshot().phase()); assertNotNull(journal.snapshot().pending()); assertThrows(IOException.class,() -> journal.freezeChunk(directory.ranges().getFirst().groupId())); journal.release(); }
        assertTrue(Files.exists(artifact)); try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); assertEquals(3,journal.snapshot().pendingArtifactBytes()); }
    }

}
