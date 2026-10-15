package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import static org.junit.jupiter.api.Assertions.*;

/** Independent source metadata recovery must never import, clear, or admit descendant USER data. */
class NativeMetadataRecoveryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest nativeTest=new NativeTopologyCdcTest();
    final NativeTopologySeedTest helper=nativeTest.helper;
    NativeMetadataRecoveryRegistration registration(NativeTopologyCdcTest.Fixture source,NativeRangeImage.Manifest image) throws Exception {
        var cfg=source.parent.configuration();
        return new NativeMetadataRecoveryRegistration(new UUID(0,1100),nativeTest.catalogue,new ApplyPosition(1,20),source.definition.sourceDirectory(),source.freeze,helper.topology.replica,helper.topology.replica,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of());
    }
    ReplicatedRangeStore.TopologyOperationOutcome outcome(ReplicatedRangeStore target,TopologyChange.Definition definition,OperationId id,byte[] digest)throws Exception {
        var schema=helper.outer.schema();return target.readTopologyOperationOutcome(schema.clusterId(),schema.databaseId(),schema.domainId(),nativeTest.lineage(definition.parents().getFirst()),id,digest);
    }
    void configure(ReplicatedRangeStore target,TopologyChange.Definition definition)throws Exception {
        target.enableSessionControls();target.enableCdc(definition.parents().getFirst().rangeId());
    }
    @Test void absentHistoricalMemberInstallsOnlyMetadataThenQualifiesActualTailAndReopens()throws Exception {
        Path image=root.resolve("source-metadata"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original,ack;byte[] result;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {
            original=source.write(source.parent,0,9);result=source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;
            registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));assertEquals(registration,NativeMetadataRecoveryRegistration.decode(registration.encode()));
            ack=new DistributedCommand(original.groupId(),1,original.schemaDigest(),new OperationId(original.operationId().sessionId(),1,1),List.of(),List.of(),0);source.parent.apply(new ApplyPosition(1,3),ack);
        }
        try(var pool=helper.pool(path)) {
            var child=helper.open(pool,definition.targets().getFirst());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.apply(new ApplyPosition(3,1),helper.put(child.ownership().owner().groupId(),0,1,7));
            assertTrue(pool.topologySeeds().isEmpty());var target=pool.openMetadataRecovery(registration);configure(target,definition);
            assertTrue(target.userHandlesDetached());assertTrue(target.metadataRecoveryPending());assertFalse(target.metadataRecoveryImageInstalled());assertFalse(target.admissionOpen());
            assertEquals(Set.of(registration.groupId(),child.ownership().owner().groupId()),pool.persistedRangeGroups(8));
            assertTrue(pool.retainedMetadataOwners().stream().anyMatch(owner->owner.owner().groupId().equals(registration.groupId()) && owner.retired() && owner.pending()));
            assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            target.installMetadataSnapshot(image,nativeTest.limits);assertEquals(registration.imageCut(),target.applied());assertTrue(target.metadataRecoveryImageInstalled());assertTrue(target.metadataRecoveryPending());
            assertThrows(IllegalStateException.class,()->target.operationResult(original.operationId(),original.receiptDigest()));
            assertThrows(IllegalStateException.class,()->target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration()));
            // Committed source tail may replay while every public authoritative outcome remains fenced.
            target.apply(new ApplyPosition(1,3),ack);assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));
            var wrong=new ReplicatedRangeStore.Configuration(0,-1,new byte[]{2});assertThrows(IllegalArgumentException.class,()->target.completeMetadataRecovery(registration,target.applied(),wrong));
            target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration());assertFalse(target.metadataRecoveryPending());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            assertNull(outcome(target,definition,new OperationId(new UUID(0,1101),2,0),new byte[32]).result());
            var page=target.pollRetiredCdc(nativeTest.lineage(definition.parents().getFirst()),DistributedCdcCheckpoint.Cursor.beginning(),256,8<<20);assertEquals(2,page.records().size());assertEquals(registration.freeze(),page.records().getLast().barrier());
            assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertEquals(new ApplyPosition(3,1),child.applied());assertEquals(0,target.activeSnapshotPins());assertTrue(pool.topologySeeds().isEmpty());
            assertThrows(IllegalStateException.class,()->helper.open(pool,definition.parents().getFirst()));
        }
        try(var pool=helper.pool(path)) {
            assertEquals(List.of(registration),pool.metadataRecoveries());assertEquals(Set.of(registration.groupId(),definition.targets().getFirst().groupId()),pool.persistedRangeGroups(8));assertTrue(pool.retainedMetadataOwners().stream().anyMatch(owner->owner.owner().groupId().equals(registration.groupId()) && owner.retired() && !owner.pending()));var target=pool.openMetadataRecovery(registration);configure(target,definition);assertFalse(target.metadataRecoveryPending());assertEquals(new ApplyPosition(1,3),target.applied());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            var child=helper.open(pool,definition.targets().getFirst());assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));assertTrue(target.userHandlesDetached());
        }
    }
    @Test void interruptedMetadataInstallReopensFencedAndRetriesExactOriginalImage()throws Exception {
        Path image=root.resolve("metadata"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original;byte[] result;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {original=source.write(source.parent,0,9);result=source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));}
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);target.installMetadataSnapshot(image,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertFalse(target.metadataRecoveryPending());assertThrows(IOException.class,()->target.installMetadataSnapshot(image,nativeTest.limits,batches->{throw new IOException("Lost install completion");}));assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));}
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());target.installMetadataSnapshot(image,nativeTest.limits);assertFalse(target.pendingInstallation());target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertArrayEquals(result,outcome(target,definition,original.operationId(),original.receiptDigest()).result());}
    }
    @Test void completedMetadataOwnerRestoresOwnCheckpointWithExactInterruptedColdRetry()throws Exception {
        Path initial=root.resolve("initial-meta"),earlier=root.resolve("earlier-own-meta"),latest=root.resolve("latest-own-meta"),receiver=root.resolve("own-meta-receiver");
        NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;DistributedCommand original;NativeRangeImage.Manifest checkpoint;
        try(var source=nativeTest.new Fixture(root.resolve("own-meta-source"))) {
            original=source.write(source.parent,0,9);source.parent.apply(new ApplyPosition(1,1),original);source.freeze();source.seed();definition=source.definition;
            registration=registration(source,NativeRangeImage.writeMetadata(initial,source.parent,nativeTest.limits));
        }
        try(var pool=helper.pool(receiver)) {
            pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));
            var child=helper.open(pool,definition.targets().getFirst());child.initializeConfiguration(new byte[]{1});child.applyNoop(new ApplyPosition(3,0));child.apply(new ApplyPosition(3,1),helper.put(child.ownership().owner().groupId(),0,1,7));
            var target=pool.openMetadataRecovery(registration);configure(target,definition);target.installMetadataSnapshot(initial,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());
            target.applyNoop(new ApplyPosition(1,3),new byte[]{2});var first=NativeRangeImage.writeMetadata(earlier,target,nativeTest.limits);
            assertEquals(first.position(),target.validateMetadataRecoveryImage(earlier,nativeTest.limits).position());target.validateMetadataRecoveryImage(initial,nativeTest.limits);
            var receipt=new ArrayList<byte[]>();NativeRangeImage.visitMetadata(earlier,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.RECEIPTS && receipt.isEmpty())receipt.add(value.clone());});assertFalse(receipt.isEmpty());
            byte[] replacement=receipt.getFirst().clone();replacement[replacement.length-1]^=1;Path forged=replaceMetadataBytes(earlier,root.resolve("forged-own-meta"),receipt.getFirst(),replacement);
            NativeRangeImage.inspectMetadata(forged,nativeTest.limits);assertThrows(IOException.class,()->target.validateMetadataRecoveryImage(forged,nativeTest.limits));assertThrows(IOException.class,()->target.installMetadataSnapshot(forged,nativeTest.limits));assertFalse(target.pendingInstallation());
            try(var freshPool=helper.pool(root.resolve("fresh-meta-owner"))) {
                var fresh=freshPool.openMetadataRecovery(registration);configure(fresh,definition);assertThrows(IOException.class,()->fresh.installMetadataSnapshot(earlier,nativeTest.limits));assertNull(fresh.applied());
                fresh.installMetadataSnapshot(initial,nativeTest.limits);fresh.completeMetadataRecovery(registration,registration.imageCut(),fresh.configuration());assertThrows(IOException.class,()->fresh.installMetadataSnapshot(earlier,nativeTest.limits));assertEquals(registration.imageCut(),fresh.applied());
            }
            var ack=new DistributedCommand(original.groupId(),1,original.schemaDigest(),new OperationId(original.operationId().sessionId(),1,1),List.of(),List.of(),0);target.apply(new ApplyPosition(1,4),ack);
            checkpoint=NativeRangeImage.writeMetadata(latest,target,nativeTest.limits);target.validateMetadataRecoveryImage(earlier,nativeTest.limits);assertThrows(IOException.class,()->target.installMetadataSnapshot(earlier,nativeTest.limits));
            target.installMetadataSnapshot(latest,nativeTest.limits);assertFalse(target.metadataRecoveryPending());assertEquals(checkpoint.position(),target.applied());
            assertThrows(IOException.class,()->target.installMetadataSnapshot(latest,nativeTest.limits,batch->{throw new IOException("Interrupt own metadata replacement");}));assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));
        }
        try(var pool=helper.pool(receiver)) {
            pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,8L<<30,java.time.Duration.ofSeconds(60),0));
            var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.pendingInstallation());assertTrue(target.metadataRecoveryPending());assertNull(target.applied());assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            var cfg=new ArrayList<byte[]>();NativeRangeImage.visitMetadata(latest,nativeTest.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==1)cfg.add(value.clone());});assertEquals(1,cfg.size());byte[] changedCfg=cfg.getFirst().clone();changedCfg[changedCfg.length-1]^=1;
            Path changed=replaceMetadataBytes(latest,root.resolve("changed-pending-meta"),cfg.getFirst(),changedCfg);assertThrows(IOException.class,()->target.installMetadataSnapshot(changed,nativeTest.limits));
            target.installMetadataSnapshot(latest,nativeTest.limits);assertFalse(target.pendingInstallation());assertFalse(target.metadataRecoveryPending());assertEquals(checkpoint.position(),target.metadataOutcomeState().applied());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(outcome(target,definition,original.operationId(),original.receiptDigest()).result()).status());
            var child=helper.open(pool,definition.targets().getFirst());assertArrayEquals(helper.helper.key(7),child.readCurrentPoint(helper.outer.fixed,helper.helper.key(1)));NativeRangeImage.writeMetadata(root.resolve("cold-own-meta-reexport"),target,nativeTest.limits);assertTrue(pool.topologySeeds().isEmpty());
        }
    }
    private Path replaceMetadataBytes(Path source,Path destination,byte[] before,byte[] after)throws Exception {
        assertEquals(before.length,after.length);byte[] bytes=java.nio.file.Files.readAllBytes(source);int offset=-1;
        for(int i=0;i<=bytes.length-before.length-32;i++)if(Arrays.equals(bytes,i,i+before.length,before,0,before.length)){offset=i;break;}
        assertTrue(offset>=0);System.arraycopy(after,0,bytes,offset,after.length);byte[] checksum=java.security.MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes,bytes.length-32));System.arraycopy(checksum,0,bytes,bytes.length-32,32);java.nio.file.Files.write(destination,bytes);return destination;
    }

    @Test void uncertainRegistrationRequiresExactRetryAndColdRegistryDoesNotInventReadiness()throws Exception {
        Path image=root.resolve("metadata");NativeMetadataRecoveryRegistration registration;TopologyChange.Definition definition;
        try(var source=nativeTest.new Fixture(root.resolve("source"))) {source.parent.apply(new ApplyPosition(1,1),source.write(source.parent,0,9));source.freeze();source.seed();definition=source.definition;registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));}
        for(boolean after:List.of(false,true)) {
            Path path=root.resolve("receiver-"+after);
            try(var pool=helper.pool(path)) {
                pool.metadataRecoveryForceObserver(boundary->{if(boundary==after)throw new RocksDBException("Uncertain registration force");});
                assertThrows(RocksDBException.class,()->pool.openMetadataRecovery(registration));assertTrue(pool.metadataRecoveries().isEmpty());
                var other=new NativeMetadataRecoveryRegistration(new UUID(0,1102),registration.catalogue(),registration.catalogueCut(),registration.sourceDirectory(),registration.freeze(),registration.receiver(),registration.sourceOwner(),registration.configurationTerm(),registration.configurationIndex(),registration.sourceConfiguration(),registration.imageCut(),registration.imageHash(),registration.mergeColumns());
                assertThrows(IllegalStateException.class,()->pool.openMetadataRecovery(other));assertThrows(IllegalStateException.class,()->helper.open(pool,definition.parents().getFirst()));
                pool.metadataRecoveryForceObserver(boundary->{});var target=pool.openMetadataRecovery(registration);assertSame(target,pool.openMetadataRecovery(registration));assertTrue(target.metadataRecoveryPending());
            }
            try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(registration);configure(target,definition);assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::retiredOutcomeState);target.installMetadataSnapshot(image,nativeTest.limits);target.completeMetadataRecovery(registration,registration.imageCut(),target.configuration());assertFalse(target.metadataRecoveryPending());}
        }
    }
    @Test void sourceCreditPublicationAndAckTailStayAuthoritativeAcrossNewMetadataImage()throws Exception {
        var promises=new NativeSourceOutcomePublicationTest();Path first=root.resolve("credit-image"),latest=root.resolve("ack-image"),path=root.resolve("receiver");NativeMetadataRecoveryRegistration next;NativeSourceOutcomeCredit.Request request;
        try(var source=nativeTest.new Fixture(root.resolve("source"));var home=promises.home.new Fixture(root.resolve("home"));var pool=helper.pool(path)) {
            source.freeze();source.parent.configureSourceOutcomeCapacity(4,352);request=promises.reserve(source,home,1,1);var credit=source.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);source.seed();
            var original=registration(source,NativeRangeImage.writeMetadata(first,source.parent,nativeTest.limits));var target=pool.openMetadataRecovery(original);configure(target,source.definition);target.installMetadataSnapshot(first,nativeTest.limits);target.configureSourceOutcomeCapacity(4,352);
            assertThrows(IllegalStateException.class,()->target.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));
            var replay=target.readCommittedSourceOutcome(request);assertEquals(credit,replay.credit());assertNull(replay.publication());assertFalse(replay.fenced());
            target.completeMetadataRecovery(original,original.imageCut(),target.configuration());assertEquals(credit,target.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));
            byte[] result=promises.success();var publication=source.parent.publishSourceOutcome(new ApplyPosition(1,3),request,result);var ack=new DistributedCommand(request.original().groupId(),1,source.parent.schemaDigest(),new OperationId(request.original().operationId().sessionId(),1,2),List.of(),List.of(),1);source.parent.apply(new ApplyPosition(1,4),ack);
            var saved=NativeRangeImage.writeMetadata(latest,source.parent,nativeTest.limits);var cfg=source.parent.configuration();next=new NativeMetadataRecoveryRegistration(new UUID(0,1103),original.catalogue(),new ApplyPosition(1,21),source.definition.sourceDirectory(),source.freeze,original.receiver(),original.sourceOwner(),cfg.term(),cfg.index(),cfg.data(),saved.position(),saved.sha256(),Set.of());
            assertSame(target,pool.openMetadataRecovery(next));assertTrue(target.metadataRecoveryPending());assertThrows(IllegalStateException.class,target::sourceOutcomeCredits);
            // The current source legitimately consumed the credit and fenced the published result.
            // Replacing metadata must carry that state forward instead of requiring an obsolete credit row.
            target.installMetadataSnapshot(latest,nativeTest.limits);target.completeMetadataRecovery(next,next.imageCut(),target.configuration());assertTrue(target.sourceOutcomeCredits().isEmpty());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(target.operationResult(request.original().operationId(),request.original().receiptDigest())).status());
            assertEquals(source.parent.sourceOutcomePublication(request,result),target.sourceOutcomePublication(request,result));assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,publication.outcome());
        }
        try(var pool=helper.pool(path)) {var target=pool.openMetadataRecovery(next);target.configureSourceOutcomeCapacity(4,352);assertFalse(target.metadataRecoveryPending());assertTrue(target.sourceOutcomeCredits().isEmpty());assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(target.operationResult(request.original().operationId(),request.original().receiptDigest())).status());}
    }

    @Test void committedCreditAndPublicationTailReplayDoesNotOpenFreshAuthority()throws Exception {
        var promises=new NativeSourceOutcomePublicationTest();Path image=root.resolve("metadata");
        try(var source=nativeTest.new Fixture(root.resolve("source"));var home=promises.home.new Fixture(root.resolve("home"));var pool=helper.pool(root.resolve("receiver"))) {
            source.freeze();source.seed();var registration=registration(source,NativeRangeImage.writeMetadata(image,source.parent,nativeTest.limits));
            source.parent.configureSourceOutcomeCapacity(4,352);var request=promises.reserve(source,home,1,1);var credit=source.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);byte[] result=promises.success();var publication=source.parent.publishSourceOutcome(new ApplyPosition(1,3),request,result);
            var target=pool.openMetadataRecovery(registration);configure(target,source.definition);target.installMetadataSnapshot(image,nativeTest.limits);target.configureSourceOutcomeCapacity(4,352);
            assertNull(target.readCommittedSourceOutcome(request).credit());assertEquals(credit,target.applySourceOutcomeCredit(new ApplyPosition(1,2),request));assertEquals(credit,target.applySourceOutcomeCredit(new ApplyPosition(1,2),request));
            assertEquals(credit,target.readCommittedSourceOutcome(request).credit());assertThrows(IllegalStateException.class,()->target.estimateSourceOutcomeCredit(request));
            assertEquals(publication,target.publishSourceOutcome(new ApplyPosition(1,3),request,result));var replay=target.readCommittedSourceOutcome(request);assertNull(replay.credit());assertEquals(publication.publication(),replay.publication());assertArrayEquals(result,replay.result());
            assertThrows(IllegalStateException.class,()->target.sourceOutcomePublication(request,result));assertThrows(IllegalStateException.class,()->target.operationResult(request.original().operationId(),request.original().receiptDigest()));
            target.completeMetadataRecovery(registration,new ApplyPosition(1,3),target.configuration());assertArrayEquals(result,target.operationResult(request.original().operationId(),request.original().receiptDigest()));
        }
    }

}
