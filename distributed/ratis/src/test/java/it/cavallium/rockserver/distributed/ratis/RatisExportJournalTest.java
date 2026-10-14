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
    RatisExportJournal.Definition definition(RangeDirectory directory) { return new RatisExportJournal.Definition(job,fence(directory),List.of(column),root.resolve("original-snapshot.journal"),new byte[32],RatisExportJournal.Limits.defaults(),java.time.Duration.ofSeconds(30)); }
    Map<UUID,SnapshotFenceStatus.Capture> tokens(SnapshotFence fence) { var tokens=new LinkedHashMap<UUID,SnapshotFenceStatus.Capture>(); int n=0; for (var p:fence.participants()) tokens.put(p.groupId(),new SnapshotFenceStatus.Capture(new UUID(0,60+n),new UUID(0,70+n),new ApplyPosition(1,5+n++))); return tokens; }
    NativeRangeSstExport.Chunk artifact(RatisExportJournal journal,RatisExportJournal.ChunkIntent intent,byte[] end,boolean more,boolean empty) throws Exception {
        Path file=empty ? null : journal.artifactPath(intent); byte[] body={1,2,3}; if (file!=null) Files.write(file,body,StandardOpenOption.CREATE_NEW);
        var p=journal.snapshot().definition().fence().participant(intent.group());
        return new NativeRangeSstExport.Chunk(new NativeRangeSstExport.Manifest(intent.group(),p.rangeEpoch(),intent.column(),p.schemaDigest(),intent.cut(),intent.scopeStart(),end,empty ? 0 : 1,empty ? 0 : 2,empty ? 0 : body.length,empty ? new byte[0] : MessageDigest.getInstance("SHA-256").digest(body),file),more);
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
            assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); for (var group:journal.snapshot().sourceCleanup()) journal.sourceReleased(group); journal.markReady(releasedSource(directory,definition.fence(),captures)); assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); chunks=journal.snapshot().chunks(); assertThrows(IllegalStateException.class,journal::release);
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
        try (var journal=RatisExportJournal.reopen(path)) { var intent=journal.snapshot().pending(); journal.publish(intent,artifact(journal,intent,null,false,false)); for (var group:journal.snapshot().sourceCleanup()) journal.sourceReleased(group); journal.markReady(releasedSource(directory,definition.fence(),tokens(definition.fence()))); }
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
            if (Arrays.equals(spans.getFirst().upperExclusive(),spans.getLast().lowerInclusive())) assertDoesNotThrow(() -> new RatisExportJournal.Definition(job,changed,List.of(column),root.resolve("source"),new byte[32],RatisExportJournal.Limits.defaults(),java.time.Duration.ofSeconds(30)));
            else assertThrows(IllegalArgumentException.class,() -> new RatisExportJournal.Definition(job,changed,List.of(column),root.resolve("source"),new byte[32],RatisExportJournal.Limits.defaults(),java.time.Duration.ofSeconds(30)));
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

    @Test void explicitCancellationFencesPendingPublicationAndReclaimsItsPrivateArtifacts() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("cancel.export"); Path artifact;
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence())));
            var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); artifact=journal.artifactPath(intent);
            var result=artifact(journal,intent,null,false,false);
            var partial=artifact.resolveSibling(artifact.getFileName()+".manifest.pending"); Files.write(partial,new byte[]{1,2},StandardOpenOption.CREATE_NEW); var building=NativeRangeSstExport.buildingPath(artifact); Files.write(building,new byte[]{3,4},StandardOpenOption.CREATE_NEW);
            journal.cancel(); journal.cancel(); assertEquals(RatisExportJournal.Phase.CANCELLED,journal.snapshot().phase());
            assertThrows(IOException.class,() -> journal.publish(intent,result)); assertThrows(IllegalStateException.class,journal::reclaimArtifacts);
            journal.release(); journal.reclaimArtifacts(); journal.reclaimArtifacts();
            assertTrue(journal.snapshot().artifactsReclaimed()); assertFalse(Files.exists(artifact)); assertFalse(Files.exists(partial)); assertFalse(Files.exists(building)); assertEquals(0,journal.snapshot().pendingArtifactBytes());
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); assertTrue(journal.snapshot().artifactsReclaimed()); }
    }
    @Test void acknowledgedReclamationRecoversAfterDeletionBeforeCompletionMarker() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("reclaim.export"); long released; Path artifact;
        try (var journal=RatisExportJournal.create(path,definition)) {
            var captures=tokens(definition.fence()); journal.bindSource(capturedSource(directory,definition.fence(),captures));
            var intent=journal.freezeChunk(directory.ranges().getFirst().groupId()); var chunk=journal.publish(intent,artifact(journal,intent,null,false,false)); artifact=chunk.file();
            for (var group:journal.snapshot().sourceCleanup()) journal.sourceReleased(group); journal.markReady(releasedSource(directory,definition.fence(),captures)); journal.acknowledge(chunk); journal.release(); released=Files.size(path);
            journal.reclaimArtifacts(); assertFalse(Files.exists(artifact));
        }
        // Keep only the forced reclamation intent; the data removals already happened.
        try (var channel=FileChannel.open(path,StandardOpenOption.WRITE)) { channel.truncate(released+69); channel.force(true); }
        try (var journal=RatisExportJournal.reopen(path)) {
            assertTrue(journal.snapshot().reclaiming()); assertFalse(journal.snapshot().artifactsReclaimed()); journal.reclaimArtifacts(); assertTrue(journal.snapshot().artifactsReclaimed());
        }
        assertTrue(Files.exists(path.resolveSibling(path.getFileName()+".lock")));
    }

    @Test void reclamationCannotDeleteArtifactsOwnedByAnotherJournalInTheSameDirectory() throws Exception {
        var directory=directory(1); var definition=definition(directory); var captures=tokens(definition.fence()); var group=directory.ranges().getFirst().groupId();
        try (var first=RatisExportJournal.create(root.resolve("first.export"),definition); var second=RatisExportJournal.create(root.resolve("second.export"),definition)) {
            first.bindSource(capturedSource(directory,definition.fence(),captures)); second.bindSource(capturedSource(directory,definition.fence(),captures));
            var a=first.freezeChunk(group); var b=second.freezeChunk(group); assertNotEquals(first.artifactPath(a),second.artifactPath(b));
            var c=first.publish(a,artifact(first,a,null,false,false)); var d=second.publish(b,artifact(second,b,null,false,false));
            for (var cleanupGroup:first.snapshot().sourceCleanup()) first.sourceReleased(cleanupGroup); first.markReady(releasedSource(directory,definition.fence(),captures)); first.acknowledge(c); first.release(); first.reclaimArtifacts();
            assertFalse(Files.exists(c.file())); assertTrue(Files.exists(d.file())); assertEquals(1,second.snapshot().chunks().size());
        }
        try (var remaining=RatisExportJournal.reopen(root.resolve("second.export"))) { assertEquals(RatisExportJournal.Phase.MATERIALIZED,remaining.snapshot().phase()); assertTrue(Files.exists(remaining.snapshot().chunks().getFirst().file())); }
    }

    @Test void forcedDownloadDescriptorRecoversBeforeArtifactRenameAndKeepsOriginalOwner() throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("download.export"); var group=directory.ranges().getFirst().groupId(); var captures=tokens(definition.fence());
        RatisExportJournal.SourceOwner original;
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,definition.fence(),captures)); original=journal.snapshot().owners().get(group); var intent=journal.freezeChunk(group); var artifact=journal.artifactPath(intent); byte[] body={1,2,3};
            Files.write(RatisExportJournal.download(artifact),body,StandardOpenOption.CREATE_NEW);
            var p=definition.fence().participant(group); var request=new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(journal.snapshot(),group),intent);
            var descriptor=new RatisExportSourceRpc.Descriptor(request.definition().digest(),intent.ordinal(),group,p.rangeEpoch(),p.schemaDigest(),column,intent.cut(),intent.scopeStart(),null,MessageDigest.getInstance("SHA-256").digest(body),1,2,3,false);
            RatisExportJournal.publishDownload(request,descriptor,artifact,RatisJournalAccess.standalone());
            Files.move(artifact,RatisExportJournal.download(artifact)); // Exact interrupted state: sidecar forced, download still named.
        }
        try (var journal=RatisExportJournal.reopen(path)) {
            assertEquals(original,journal.snapshot().owners().get(group)); assertTrue(journal.snapshot().sourceCleanup().contains(group));
            var recovered=journal.recoverChunk(); assertNotNull(recovered); assertTrue(Files.exists(recovered.file())); assertFalse(Files.exists(RatisExportJournal.download(recovered.file())));
            assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); journal.sourceReleased(group);
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertTrue(journal.snapshot().sourceCleanup().isEmpty()); assertEquals(1,journal.snapshot().chunks().size()); }
    }

    @Test void cancellationAccountsRetainedFailedDownload() throws Exception { retainedDownloadBytes(false); }
    @Test void sourceLossAccountsRetainedFailedDownload() throws Exception { retainedDownloadBytes(true); }
    private void retainedDownloadBytes(boolean lost) throws Exception {
        var directory=directory(1); var definition=definition(directory); var path=root.resolve("partial.export"); var group=directory.ranges().getFirst().groupId();
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,definition.fence(),tokens(definition.fence()))); var intent=journal.freezeChunk(group); var artifact=journal.artifactPath(intent);
            Files.write(RatisExportJournal.download(artifact),new byte[]{1,2,3},StandardOpenOption.CREATE_NEW);
            Files.write(artifact,new byte[]{4,5},StandardOpenOption.CREATE_NEW); // Count both names conservatively, including possible publication residue.
            if (lost) journal.failSnapshotLost(); else journal.cancel();
            assertEquals(5,journal.snapshot().pendingArtifactBytes());
        }
        try (var journal=RatisExportJournal.reopen(path)) {
            assertEquals(5,journal.snapshot().pendingArtifactBytes()); assertEquals(lost ? RatisExportJournal.Phase.SNAPSHOT_LOST : RatisExportJournal.Phase.CANCELLED,journal.snapshot().phase());
            journal.sourceReleased(group); journal.release(); journal.reclaimArtifacts(); assertEquals(0,journal.snapshot().pendingArtifactBytes());
        }
    }

    @Test void canonicalSelectionProjectsJobsButRetainsAllOriginalPinsAndReplaysColumnBoundary() throws Exception {
        var base=directory(2); UUID second=new UUID(Long.MIN_VALUE,1),unselected=new UUID(0,99);
        var directory=new RangeDirectory(base.clusterId(),base.databaseId(),base.domainId(),base.schemaEpoch(),base.schemaDigest(),base.generation(),Set.of(column,second,unselected),base.routingPrefixBytes(),base.ranges());
        var original=fence(directory); var participants=new ArrayList<SnapshotFence.Participant>();
        for (int n=0;n<original.participants().size();n++) {
            var p=original.participants().get(n);
            var scope=n==0 ? new TransactionScope(List.of(TransactionScope.Span.range(column,null,new byte[]{4}),TransactionScope.Span.range(column,new byte[]{4},new byte[]{(byte)128}),TransactionScope.Span.range(second,null,new byte[]{(byte)128}))) : new TransactionScope(List.of(TransactionScope.Span.range(unselected,new byte[]{(byte)128},null)));
            participants.add(new SnapshotFence.Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.installOperation(),p.releaseOperation(),scope));
        }
        var fence=new SnapshotFence(original.clusterId(),original.databaseId(),original.domainId(),original.fenceId(),original.homeGroup(),original.homeEpoch(),original.operation(),original.directoryGeneration(),original.schemaEpoch(),original.schemaDigest(),participants);
        var selection=new ArrayList<>(List.of(second,column)); var definition=new RatisExportJournal.Definition(job,fence,selection,root.resolve("multi-source"),new byte[32],RatisExportJournal.Limits.defaults(),java.time.Duration.ofSeconds(30)); selection.clear();
        assertEquals(List.of(column,second),definition.columns()); assertThrows(UnsupportedOperationException.class,() -> definition.columns().clear());
        var group=participants.getFirst().groupId(); var skipped=participants.getLast().groupId();
        assertEquals(List.of(group),definition.groups()); assertTrue(definition.columnsForGroup(skipped).isEmpty());
        assertThrows(IllegalArgumentException.class,() -> new RatisExportJournal.Definition(job,fence,List.of(column,column),root.resolve("s"),new byte[32],definition.limits(),java.time.Duration.ofSeconds(30)));
        assertThrows(IllegalArgumentException.class,() -> new RatisExportJournal.Definition(job,fence,List.of(new UUID(0,199)),root.resolve("s"),new byte[32],definition.limits(),java.time.Duration.ofSeconds(30)));
        var tooFew=new RatisExportJournal.Limits(1,definition.limits().maximumJournalBytes(),definition.limits().maximumDiskBytes(),definition.limits().chunk());
        assertThrows(IllegalArgumentException.class,() -> new RatisExportJournal.Definition(job,fence,definition.columns(),root.resolve("s"),new byte[32],tooFew,java.time.Duration.ofSeconds(30)));
        var path=root.resolve("multi-export"); var tokens=tokens(fence);
        try (var journal=RatisExportJournal.create(path,definition)) {
            journal.bindSource(capturedSource(directory,fence,tokens)); assertEquals(2,journal.snapshot().captures().size()); assertEquals(Set.of(group),journal.snapshot().sourceCleanup());
            assertThrows(IllegalArgumentException.class,() -> RatisExportJournal.sourceDefinition(journal.snapshot(),skipped));
            var intent=journal.freezeChunk(group); assertEquals(column,intent.column()); journal.publish(intent,artifact(journal,intent,new byte[]{(byte)128},false,true));
            assertEquals(RatisExportJournal.Phase.EXPORTING,journal.snapshot().phase()); assertThrows(IOException.class,() -> journal.sourceReleased(group));
        }
        try (var journal=RatisExportJournal.reopen(path)) {
            assertEquals(group,journal.unfinishedGroup()); var intent=journal.freezeChunk(group); assertEquals(second,intent.column()); assertEquals(1,intent.ordinal()); assertNull(intent.scopeStart());
            var wrong=new RatisExportJournal.ChunkIntent(group,column,intent.cut(),intent.scopeStart(),intent.ordinal(),intent.limits()); assertThrows(IOException.class,() -> journal.publish(wrong,artifact(journal,intent,new byte[]{(byte)128},false,true)));
            journal.publish(intent,artifact(journal,intent,new byte[]{(byte)128},false,true)); assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); journal.sourceReleased(group); journal.markReady(releasedSource(directory,fence,tokens));
            for (var chunk:journal.snapshot().chunks()) journal.acknowledge(chunk);
        }
        try (var journal=RatisExportJournal.reopen(path)) { assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); assertEquals(2,journal.snapshot().acknowledged().size()); }
    }

    @Test void sourceLifetimeMismatchIsRejectedBeforeBinding() throws Exception {
        var directory=directory(1); var original=definition(directory);
        var declared=new RatisExportJournal.Definition(original.jobId(),original.fence(),original.columns(),original.sourceJournal(),original.sourceIdentity(),original.limits(),Duration.ofSeconds(60));
        try (var journal=RatisExportJournal.create(root.resolve("ttl-mismatch.export"),declared)) {
            var captured=capturedSource(directory,declared.fence(),tokens(declared.fence()));
            assertThrows(IllegalArgumentException.class,() -> journal.bindSource(captured));
            assertEquals(RatisExportJournal.Phase.CAPTURING,journal.snapshot().phase()); assertTrue(journal.snapshot().captures().isEmpty());
        }
    }
    @Test void extendedReadonlyRetentionRemainsExactAcrossJournalReopen() throws Exception {
        var original=definition(directory(1));var ttl=Duration.ofHours(2);
        var declared=new RatisExportJournal.Definition(original.jobId(),original.fence(),original.columns(),original.sourceJournal(),original.sourceIdentity(),original.limits(),ttl);
        Path path=root.resolve("long-retention.export");
        try(var journal=RatisExportJournal.create(path,declared)){assertEquals(ttl,journal.snapshot().definition().snapshotTtl());}
        try(var journal=RatisExportJournal.reopen(path)){assertEquals(ttl,journal.snapshot().definition().snapshotTtl());}
        assertThrows(IllegalArgumentException.class,()->new RatisExportJournal.Definition(original.jobId(),original.fence(),original.columns(),original.sourceJournal(),original.sourceIdentity(),original.limits(),Duration.ofHours(24).plusMillis(1)));
    }

}
