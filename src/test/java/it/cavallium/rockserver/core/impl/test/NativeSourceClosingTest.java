package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native closing/accepted debt, with trusted controller inputs; no terminal deletion authority. */
class NativeSourceClosingTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSourceOutcomePublicationTest outcomes=new NativeSourceOutcomePublicationTest();
    final NativeTopologyCdcTest topology=outcomes.topology;
    NativeSourceClosing.Request request(NativeTopologyCdcTest.Fixture f)throws Exception{var cfg=f.parent.configuration();return new NativeSourceClosing.Request(f.definition.header(),f.parent.ownership().owner().groupId(),topology.catalogue,new ApplyPosition(1,10),cfg.term(),cfg.index(),cfg.data(),OperatorUpload.sha256(f.definition.header().encode()));}
    @Test void closingRejectsNewCreditAndAbsenceButExactAcceptedPublicationAndChildrenSurvive()throws Exception {
        try(var f=topology.new Fixture(root.resolve("source"));var home=outcomes.home.new Fixture(root.resolve("home"))){
            f.parent.apply(new ApplyPosition(1,1),f.write(f.parent,0,9));f.freeze();f.seed();f.parent.configureSourceOutcomeCapacity(3,1024);
            var accepted=outcomes.reserve(f,home,1,1);var unaccepted=outcomes.reserve(f,home,2,2);var credit=f.parent.applySourceOutcomeCredit(new ApplyPosition(1,3),accepted);
            var closingRequest=request(f);assertEquals(closingRequest,NativeSourceClosing.Request.decode(closingRequest.encode()));
            var closed=f.parent.applySourceClosing(new ApplyPosition(1,4),closingRequest);assertEquals(closed,NativeSourceClosing.decode(closed.encode()));
            assertTrue(f.parent.sourceClosingAdmission());assertEquals(credit,f.parent.sourceOutcomeCredit(accepted.original().operationId(),accepted.original().receiptDigest()));assertEquals(1,f.parent.sourceOutcomeCredits().size());
            assertThrows(ReplicatedRangeStore.SourceClosing.class,()->f.parent.estimateSourceOutcomeCredit(unaccepted));
            var origin=unaccepted.origin();assertThrows(ReplicatedRangeStore.SourceClosing.class,()->f.parent.readTopologyOperationOutcome(origin.clusterId(),origin.databaseId(),origin.domainId(),origin.source(),origin.operation(),origin.originalReceiptDigest()));
            assertNull(f.parent.applySourceOutcomeCredit(new ApplyPosition(1,5),unaccepted),"Committed stale CREDIT must reject without poisoning the native lane");assertEquals(1,f.parent.sourceOutcomeCredits().size());
            var publication=f.parent.publishSourceOutcome(new ApplyPosition(1,6),accepted,outcomes.success());assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,publication.outcome());assertTrue(f.parent.sourceOutcomeCredits().isEmpty());
            assertEquals(credit,f.parent.applySourceOutcomeCredit(new ApplyPosition(1,7),accepted));assertArrayEquals(outcomes.success(),f.parent.operationResult(accepted.original().operationId(),accepted.original().receiptDigest()));
            assertEquals(NativeSourceOutcomePublication.Outcome.CLOSED,f.parent.publishSourceOutcome(new ApplyPosition(1,8),unaccepted,outcomes.success()).outcome());
            assertArrayEquals(topology.helper.helper.key(9),f.child.readCurrentPoint(topology.helper.outer.fixed,topology.helper.helper.key(1)));
            try(var pin=f.parent.captureMetadataForRecovery()){
                Path file=root.resolve("closing-meta");NativeRangeImage.writeMetadataAdmitted(file,pin,pin.interval(),topology.limits,NativeRangeImage.IoAdmission.UNLIMITED);
                var count=new java.util.concurrent.atomic.AtomicInteger();NativeRangeImage.visitMetadata(file,topology.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17 && key[16]==0x76){assertEquals(closed,NativeSourceClosing.decode(value));count.incrementAndGet();}assertNotEquals(NativeRangeImage.Section.USER,section);});assertEquals(1,count.get());
            }
        }
    }
    @Test void originalBusinessResultRemainsDebtAfterCloseUntilActualAckWhileControlOutcomeSurvivesCold()throws Exception{
        var path=root.resolve("original-results");DistributedCommand original;RangeDescriptor parent;NativeSourceClosing closed;
        try(var f=topology.new Fixture(path)){
            original=f.write(f.parent,0,9);f.parent.apply(new ApplyPosition(1,1),original);f.freeze();f.seed();parent=f.definition.parents().getFirst();closed=f.parent.applySourceClosing(new ApplyPosition(1,3),request(f));
            var before=f.parent.sourceResultDebt(16,4096);assertEquals(1,before.businessResults());assertEquals(original.operationId(),before.firstBusiness());assertEquals(0,before.controlResults());assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.operationResult(original.operationId(),original.receiptDigest())).status());
            var id=original.operationId();var acknowledgement=new DistributedCommand(parent.groupId(),parent.rangeEpoch(),f.parent.schemaDigest(),new OperationId(id.sessionId(),id.epoch(),1),List.of(),List.of(),0);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.parent.apply(new ApplyPosition(1,4),acknowledgement)).status());var after=f.parent.sourceResultDebt(16,4096);assertEquals(0,after.businessResults());assertNull(after.firstBusiness());assertEquals(1,after.controlResults());assertEquals(89,after.controlBytes());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(f.parent.operationResult(original.operationId(),original.receiptDigest())).status(),"Only actual ACK authorizes this explicit retired outcome");
            try(var pin=f.parent.captureMetadataForRecovery()){var image=root.resolve("control-receipt-meta");NativeRangeImage.writeMetadataAdmitted(image,pin,pin.interval(),topology.limits,NativeRangeImage.IoAdmission.UNLIMITED);var count=new java.util.concurrent.atomic.AtomicInteger();NativeRangeImage.visitMetadata(image,topology.limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.RECEIPTS){assertEquals(NativeOperationReceipt.Kind.CONTROL,NativeOperationReceipt.validate(value));count.incrementAndGet();}});assertEquals(1,count.get());}
            assertArrayEquals(topology.helper.helper.key(9),f.child.readCurrentPoint(topology.helper.outer.fixed,topology.helper.helper.key(1)));
        }
        try(var pool=topology.helper.pool(path)){var store=topology.helper.open(pool,parent);assertEquals(closed,store.sourceClosing());var debt=store.sourceResultDebt(16,4096);assertEquals(0,debt.businessResults());assertEquals(1,debt.controlResults());assertEquals(89,debt.controlBytes());assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(store.operationResult(original.operationId(),original.receiptDigest())).status());}
    }
    @Test void exactClosingForceBeforeAndAfterAckLossColdRetryNeverResurrectsAdmission()throws Exception {
        for(boolean persisted:List.of(false,true)){
            Path path=root.resolve("source-"+persisted);NativeSourceClosing.Request request;NativeSourceOutcomeCredit.Request accepted;RangeDescriptor parent;NativeSourceClosing expected;
            try(var f=topology.new Fixture(path);var home=outcomes.home.new Fixture(root.resolve("home-"+persisted))){
                f.freeze();f.seed();f.parent.configureSourceOutcomeCapacity(2,1024);accepted=outcomes.reserve(f,home,1,1);f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),accepted);parent=f.definition.parents().getFirst();request=request(f);expected=new NativeSourceClosing(request,new ApplyPosition(1,3));
                var once=new java.util.concurrent.atomic.AtomicBoolean();f.parent.sourceClosingForceObserver(after->{if(after==persisted && once.compareAndSet(false,true))throw new org.rocksdb.RocksDBException("Lost original CLOSE force observation");});
                assertThrows(org.rocksdb.RocksDBException.class,()->f.parent.applySourceClosing(new ApplyPosition(1,3),request));assertTrue(f.parent.sourceClosingAdmission());assertEquals(persisted?new ApplyPosition(1,3):new ApplyPosition(1,2),f.parent.applied());assertEquals(1,f.parent.sourceOutcomeCredits().size());
            }
            try(var pool=topology.helper.pool(path)){
                var store=topology.helper.open(pool,parent);store.initializeConfiguration(new byte[]{1});store.enableCdc(parent.rangeId());store.configureSourceOutcomeCapacity(2,1024);
                assertEquals(persisted,store.sourceClosingAdmission());assertEquals(expected,store.applySourceClosing(new ApplyPosition(1,3),request));assertTrue(store.sourceClosingAdmission());
                assertEquals(expected,store.applySourceClosing(new ApplyPosition(2,4),request));assertEquals(expected.position(),store.sourceClosing().position());assertEquals(1,store.sourceOutcomeCredits().size());
                assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,store.publishSourceOutcome(new ApplyPosition(2,5),accepted,outcomes.success()).outcome());
            }
            try(var pool=topology.helper.pool(path)){
                var store=topology.helper.open(pool,parent);assertEquals(expected,store.sourceClosing());assertTrue(store.sourceClosingAdmission());assertEquals(new ApplyPosition(2,5),store.applied());
            }
        }
    }
}
