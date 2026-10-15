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
            assertTrue(target.userHandlesDetached());assertTrue(target.metadataRecoveryPending());assertFalse(target.admissionOpen());
            assertEquals(Set.of(registration.groupId(),child.ownership().owner().groupId()),pool.persistedRangeGroups(8));
            assertTrue(pool.retainedMetadataOwners().stream().anyMatch(owner->owner.owner().groupId().equals(registration.groupId()) && owner.retired() && owner.pending()));
            assertThrows(IllegalStateException.class,()->outcome(target,definition,original.operationId(),original.receiptDigest()));assertThrows(IllegalStateException.class,target::metadataOutcomeState);
            target.installMetadataSnapshot(image,nativeTest.limits);assertEquals(registration.imageCut(),target.applied());assertTrue(target.metadataRecoveryPending());
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
