package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native source bookkeeping, with a real prior HOME reservation; no child execution. */
class NativeSourceOutcomePublicationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeTopologyCdcTest topology=new NativeTopologyCdcTest();
    final NativeHomeRecoveryTest home=new NativeHomeRecoveryTest();
    byte[] hash(byte[] value)throws Exception { return MessageDigest.getInstance("SHA-256").digest(value); }
    NativeSourceOutcomeCredit.Request reserve(NativeTopologyCdcTest.Fixture f,NativeHomeRecoveryTest.Fixture h,long sequence,int homeIndex)throws Exception {
        return reserveOriginal(f,h,f.write(f.parent,sequence,8),homeIndex);
    }
    NativeSourceOutcomeCredit.Request reserveOriginal(NativeTopologyCdcTest.Fixture f,NativeHomeRecoveryTest.Fixture h,DistributedCommand original,int homeIndex)throws Exception {
        long sequence=original.operationId().sequence();var schema=topology.helper.outer.schema();var directory=f.definition.targetDirectory();
        var origin=new OperationRerouteOrigin(schema.clusterId(),schema.databaseId(),schema.domainId(),topology.lineage(f.definition.parents().getFirst()),original.operationId(),original.receiptDigest(),hash(f.freeze.encode()),f.parent.applied(),directory.payloadDigest(),new byte[32],2,new OperationId(home.user,5,sequence*256));
        var reservation=TransactionStatus.reserve(origin, DomainBinding.of(directory));h.store.configureHomeRecoveryReservations(schema.clusterId(),schema.databaseId(),home.column);
        assertEquals(CommandResult.Status.SUCCESS,home.apply(h,homeIndex,home.cas(origin.firstControl(),null,reservation)).status());
        return new NativeSourceOutcomeCredit.Request(original,origin,directory,f.freeze,new HomeIdentity(home.group,1,new byte[32],home.column,reservation));
    }
    byte[] success() { return new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode(); }
    @Test void reservedCapacityPublishesOriginalReceiptWithoutUserEffectsAndLostReplyIsExact()throws Exception {
        try(var f=topology.new Fixture(root.resolve("source"));var h=home.new Fixture(root.resolve("home"))) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();f.parent.configureSourceOutcomeCapacity(2,176);
            var request=reserve(f,h,1,1);assertEquals(88,request.reservedBytes());var credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),request);assertEquals(1,f.parent.sourceOutcomeCredits().size());
            var outcome=f.parent.readTopologyOperationOutcome(request.origin().clusterId(),request.origin().databaseId(),request.origin().domainId(),request.origin().source(),request.original().operationId(),request.original().receiptDigest());assertEquals(credit,outcome.credit());assertNull(outcome.result());
            var another=reserve(f,h,2,2);assertThrows(IllegalStateException.class,()->f.parent.estimateSourceOutcomeCredit(another));assertEquals(new ApplyPosition(1,3),f.parent.applied());
            var result=success();var published=f.parent.publishSourceOutcome(new ApplyPosition(1,4),request,result);assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,published.outcome());assertTrue(f.parent.sourceOutcomeCredits().isEmpty());assertArrayEquals(result,f.parent.operationResult(request.original().operationId(),request.original().receiptDigest()));
            assertEquals(published,f.parent.sourceOutcomePublication(request,result));assertEquals(published,f.parent.publishSourceOutcome(new ApplyPosition(2,5),request,result));assertEquals(new ApplyPosition(1,4),published.publication().position());
            assertThrows(IllegalArgumentException.class,()->f.parent.publishSourceOutcome(new ApplyPosition(2,6),request,new CommandResult(CommandResult.Status.CONFLICT,List.of()).encode()));assertEquals(new ApplyPosition(2,5),f.parent.applied());
            try(var pin=f.parent.captureForRecovery()) { assertArrayEquals(topology.helper.helper.key(9),pin.get(topology.helper.outer.fixed,topology.helper.helper.key(1))); }
        }
    }
    @Test void pendingCreditAndPublishedOriginalProofSurviveReopenAndPortableImage()throws Exception {
        Path path=root.resolve("source"),image=root.resolve("source-image");NativeSourceOutcomeCredit.Request request;NativeSourceOutcomeCredit credit;RangeDescriptor parent;
        try(var f=topology.new Fixture(path);var h=home.new Fixture(root.resolve("home"))) { f.freeze();f.parent.configureSourceOutcomeCapacity(2,176);parent=f.definition.parents().getFirst();request=reserve(f,h,1,1);credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);NativeRangeImage.write(image,f.parent,topology.limits); }
        NativeRangeImage.inspect(image,topology.limits);
        for(Path target:List.of(path,root.resolve("target")))try(var pool=topology.helper.pool(target)) {
            var store=topology.helper.open(pool,parent);
            store.initializeConfiguration(new byte[]{1});store.enableCdc(request.origin().source().rangeId());
            if(!target.equals(path)) { store.fence();store.installSnapshot(image,topology.limits);store.activate(); }
            store.configureSourceOutcomeCapacity(2,176);assertEquals(credit,store.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));
            var result=store.publishSourceOutcome(new ApplyPosition(2,3),request,success());assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,result.outcome());assertEquals(credit.position(),result.publication().creditPosition());
        }
        try(var pool=topology.helper.pool(path)) { var store=topology.helper.open(pool,parent);assertEquals(new ApplyPosition(2,3),store.sourceOutcomePublication(request,success()).publication().position());assertTrue(store.sourceOutcomeCredits().isEmpty()); }
    }
    @Test void ackCannotDonatePendingPromiseAndFencedPublicationDoesNotResurrectOutcome()throws Exception {
        try(var f=topology.new Fixture(root.resolve("source"));var h=home.new Fixture(root.resolve("home"))) {
            f.freeze();f.parent.configureSourceOutcomeCapacity(2,176);var request=reserve(f,h,1,1);f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);
            var ack=new DistributedCommand(request.original().groupId(),1,f.parent.schemaDigest(),new OperationId(request.original().operationId().sessionId(),1,2),List.of(),List.of(),1);
            f.parent.apply(new ApplyPosition(1,3),ack);assertNotNull(f.parent.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));assertNull(f.parent.sourceOutcomePublication(request,success()));
            var publication=f.parent.publishSourceOutcome(new ApplyPosition(1,4),request,success());assertEquals(NativeSourceOutcomePublication.Outcome.FENCED,publication.outcome());assertTrue(f.parent.sourceOutcomeCredits().isEmpty());assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(f.parent.operationResult(request.original().operationId(),request.original().receiptDigest())).status());
            assertEquals(publication,f.parent.publishSourceOutcome(new ApplyPosition(2,5),request,success()));assertEquals(1,f.parent.receiptUsage().retainedCount());
        }
    }
    @Test void epochFenceKeepsPendingRecoveryCreditUntilTerminalPublication()throws Exception {
        try(var f=topology.new Fixture(root.resolve("source"));var h=home.new Fixture(root.resolve("home"))) {
            f.freeze();f.parent.configureSourceOutcomeCapacity(2,176);var request=reserve(f,h,1,1);var credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);
            var floor=DistributedCommand.advanceSessionEpochFloor(request.original().groupId(),1,f.parent.schemaDigest(),new OperationId(home.admin,2,0),2);f.parent.apply(new ApplyPosition(1,3),floor);
            assertEquals(credit,f.parent.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));assertTrue(f.parent.estimateSourceOutcomeCredit(request).fenced());assertNull(f.parent.sourceOutcomePublication(request,success()));
            assertEquals(NativeSourceOutcomePublication.Outcome.FENCED,f.parent.publishSourceOutcome(new ApplyPosition(1,4),request,success()).outcome());assertTrue(f.parent.sourceOutcomeCredits().isEmpty());
            var next=DistributedCommand.advanceSessionEpochFloor(request.original().groupId(),1,f.parent.schemaDigest(),new OperationId(home.admin,3,0),3);f.parent.apply(new ApplyPosition(1,5),next);
            assertNull(f.parent.sourceOutcomePublication(request,success()).publication());assertEquals(1,f.parent.receiptUsage().retainedCount());
        }
    }

    @Test void pendingOriginalIdentityRejectsAckPoisoningInBothOrders()throws Exception {
        for(boolean creditFirst:List.of(true,false))try(var f=topology.new Fixture(root.resolve("source-"+creditFirst));var h=home.new Fixture(root.resolve("home-"+creditFirst))) {
            f.freeze();f.parent.configureSourceOutcomeCapacity(4,368);var request=reserve(f,h,1,1);var ack=new DistributedCommand(request.original().groupId(),1,f.parent.schemaDigest(),request.original().operationId(),List.of(),List.of(),0);
            if(creditFirst) {
                var credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),request);assertThrows(IllegalArgumentException.class,()->f.parent.validate(ack));assertThrows(IllegalArgumentException.class,()->f.parent.estimateReceiptRetention(ack));assertThrows(IllegalArgumentException.class,()->f.parent.apply(new ApplyPosition(1,3),ack));
                assertEquals(new ApplyPosition(1,2),f.parent.applied());assertEquals(credit,f.parent.sourceOutcomeCredit(request.original().operationId(),request.original().receiptDigest()));assertTrue(f.parent.estimateSourceOutcomeCredit(request).metadataUsageBytes()>0);assertThrows(IllegalStateException.class,()->f.parent.apply(new ApplyPosition(1,3),request.original()));
                assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,f.parent.publishSourceOutcome(new ApplyPosition(1,3),request,success()).outcome());
            } else {
                f.parent.apply(new ApplyPosition(1,2),ack);assertThrows(IllegalArgumentException.class,()->f.parent.estimateSourceOutcomeCredit(request));assertThrows(IllegalArgumentException.class,()->f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),request));assertTrue(f.parent.sourceOutcomeCredits().isEmpty());assertEquals(new ApplyPosition(1,2),f.parent.applied());
            }
        }
    }
    @Test void creditCannotPrecedeItsAbsenceEvidenceAndAcceptedOriginalReservesOnlyNoEffectResult()throws Exception {
        try(var f=topology.new Fixture(root.resolve("source"));var h=home.new Fixture(root.resolve("home"))) {
            f.freeze();var request=reserve(f,h,1,1);assertThrows(IllegalArgumentException.class,()->new NativeSourceOutcomeCredit(request,request.origin().absenceCut()));
            var valid=new NativeSourceOutcomeCredit(request,new ApplyPosition(1,2));byte[] forged=valid.encode();java.nio.ByteBuffer.wrap(forged,12,8).putLong(0);assertThrows(IllegalArgumentException.class,()->NativeSourceOutcomeCredit.decode(forged));
            var mutation=new Mutation(Mutation.Kind.PUT,topology.helper.outer.fixed,topology.helper.helper.key(1),topology.helper.helper.key(8),Mutation.ReturnMode.CURRENT);var base=new DistributedCommand(request.original().groupId(),1,f.parent.schemaDigest(),new OperationId(topology.helper.outer.session,1,2),List.of(mutation));
            var result=new CommandResult(CommandResult.Status.SUCCESS,List.of(topology.helper.helper.key(8))).encode();var accepted=DistributedCommand.acceptedTransaction(base,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,77),new TransactionScope(List.of(TransactionScope.Span.point(mutation.columnId(),mutation.key()))),result));
            var old=request.origin();var origin=new OperationRerouteOrigin(old.clusterId(),old.databaseId(),old.domainId(),old.source(),accepted.operationId(),accepted.receiptDigest(),old.absenceCommitment(),old.absenceCut(),old.targetDirectoryDigest(),old.planDigest(),old.targetParticipantCount(),new OperationId(home.user,5,512));var reservation=TransactionStatus.reserve(origin, request.home().definition().domains().getFirst());
            var acceptedRequest=new NativeSourceOutcomeCredit.Request(accepted,origin,request.targetDirectory(),request.freeze(),new HomeIdentity(home.group,1,new byte[32],home.column,reservation));assertEquals(8,acceptedRequest.maximumResultBytes());assertEquals(88,acceptedRequest.reservedBytes());
        }
    }

    @Test void durableSourceAckPromiseWinsRaceWithOrdinaryNamespaceGrowthAndAllowsTerminalPublication() throws Exception {
        try(var f=topology.new Fixture(root.resolve("aux-source"));var h=home.new Fixture(root.resolve("aux-home"))) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();f.parent.configureSourceOutcomeCapacity(100,100*88);f.parent.configureSessionMetadataCapacity(2);
            var original=f.write(f.parent,1,8);original=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),original.operationId(),original.mutations(),List.of(),0);
            var request=reserveOriginal(f,h,original,1);assertEquals(1,f.parent.estimateSourceSessionMetadata(request).additionalNamespaces());
            f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),request);assertEquals(0,f.parent.estimateSourceSessionMetadata(request).additionalNamespaces());
            var issuer=new UUID(0,8000);var competitor=new UUID(0,8001);
            var ack=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),new OperationId(issuer,1,1),List.of(),List.of(),0);
            assertEquals(1,f.parent.estimateSessionMetadata(ack).reservedNamespaces());assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,4),ack)).status());
            var rival=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),new OperationId(competitor,1,1),List.of(),List.of(),0);
            assertEquals(1,f.parent.estimateSessionMetadata(rival).reservedNamespaces());assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.parent.apply(new ApplyPosition(1,5),rival)).status());
            assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,f.parent.publishSourceOutcome(new ApplyPosition(1,6),request,success()).outcome());
            assertEquals(2,f.parent.estimateSessionMetadata(rival).retainedNamespaces());assertEquals(0,f.parent.estimateSessionMetadata(rival).reservedNamespaces());
            var known=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),new OperationId(issuer,1,2),List.of(),List.of(),1);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,7),known)).status());
            try(var pin=f.parent.captureForRecovery()){assertArrayEquals(topology.helper.helper.key(9),pin.get(topology.helper.outer.fixed,topology.helper.helper.key(1)));}
            var floor=DistributedCommand.advanceSessionEpochFloor(original.groupId(),original.rangeEpoch(),original.schemaDigest(),new OperationId(new UUID(0,8002),2,0),2);assertEquals(2,f.parent.estimateSessionMetadata(floor).removedNamespaces());assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,8),floor)).status());
            assertEquals(0,f.parent.estimateSessionMetadata(floor).retainedNamespaces());
        }
    }

    @Test void receiverBudgetIncludesActualFloorsAndUnconsumedSourceNamespacePromises() throws Exception {
        var image=root.resolve("promised.image");
        try(var f=topology.new Fixture(root.resolve("promise-donor"));var h=home.new Fixture(root.resolve("promise-home"))) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();f.parent.configureSourceOutcomeCapacity(100,100*88);
            var original=f.write(f.parent,1,8);original=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),original.operationId(),original.mutations(),List.of(),0);
            var request=reserveOriginal(f,h,original,1);f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),request);
            for(int n=0;n<2;n++){var acknowledgement=new DistributedCommand(original.groupId(),original.rangeEpoch(),original.schemaDigest(),new OperationId(new UUID(0,8100+n),1,1),List.of(),List.of(),0);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,4+n),acknowledgement)).status());}
            assertThrows(IllegalStateException.class,()->f.parent.configureSessionMetadataCapacity(2));
            try(var pin=f.parent.captureForRecovery()){NativeRangeImage.write(image,pin,pin.interval(),topology.limits);}
            assertDoesNotThrow(()->NativeRangeImage.inspectSessionMetadata(image,topology.limits,3));assertThrows(java.io.IOException.class,()->NativeRangeImage.inspectSessionMetadata(image,topology.limits,2));
        }
        try(var f=topology.new Fixture(root.resolve("promise-receiver"))){f.parent.configureSessionMetadataCapacity(2);var cut=f.parent.applied();f.parent.fence();assertThrows(java.io.IOException.class,()->f.parent.installSnapshot(image,topology.limits));assertEquals(cut,f.parent.applied());assertFalse(f.parent.pendingInstallation());}
    }

    @Test void creditFreeColdCapacityAndTighterReceiverValidateExistingReceiptsBeforePublication() throws Exception {
        var path=root.resolve("ordinary-source");var image=root.resolve("ordinary-receipts.image");
        try(var f=topology.new Fixture(path)) {
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.parent.apply(new ApplyPosition(1,2),f.write(f.parent,1,8));
            assertTrue(f.parent.sourceOutcomeCredits().isEmpty());assertEquals(2,f.parent.receiptUsage().retainedCount());
            assertThrows(IllegalStateException.class,()->f.parent.configureSourceOutcomeCapacity(1,88));f.parent.configureSourceOutcomeCapacity(2,176);
            try(var pin=f.parent.capture()){NativeRangeImage.write(image,pin,pin.interval(),topology.limits);}
        }
        try(var f=topology.new Fixture(path)){assertTrue(f.parent.sourceOutcomeCredits().isEmpty());assertThrows(IllegalStateException.class,()->f.parent.configureSourceOutcomeCapacity(1,176));assertThrows(IllegalStateException.class,()->f.parent.configureSourceOutcomeCapacity(2,88));f.parent.configureSourceOutcomeCapacity(2,176);}
        try(var f=topology.new Fixture(root.resolve("tighter-target"))){f.parent.configureSourceOutcomeCapacity(1,88);var cut=f.parent.applied();f.parent.fence();assertThrows(java.io.IOException.class,()->f.parent.installSnapshot(image,topology.limits));assertEquals(cut,f.parent.applied());assertFalse(f.parent.pendingInstallation());}
    }

}
