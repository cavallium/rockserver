package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native HOME reservation accounting only; child capture/admission belongs to the adapter. */
class NativeHomeReservationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    byte[] digest(int tag) { byte[] value=new byte[32];value[0]=(byte)tag;return value; }
    OperationRerouteOrigin origin(int sequence,int count,long epoch) {
        var initial=helper.initial(0);return new OperationRerouteOrigin(initial.clusterId(),initial.databaseId(),initial.domainId(),new DistributedCdcCheckpoint.Lineage(new UUID(0,50),new UUID(0,51),1),new OperationId(initial.operation().sessionId(),epoch,sequence),digest(1),digest(2),new ApplyPosition(1,9),digest(3),digest(4),count,new OperationId(helper.user,5,sequence*256L));
    }
    TransactionStatus reserve(int sequence,int count) { return TransactionStatus.reserve(origin(sequence,count,1),1,new byte[32]); }
    TransactionStatus promoted(TransactionStatus reserve) {
        var participants=new ArrayList<TransactionStatus.Participant>();for(int n=0;n<reserve.origin().targetParticipantCount();n++)participants.add(new TransactionStatus.Participant(new UUID(0,100+n),new UUID(0,200+n),1,new byte[32],digest(10+n),null,false));
        return reserve.promote(TransactionStatus.begin(reserve.clusterId(),reserve.databaseId(),reserve.domainId(),reserve.transactionId(),reserve.operation(),reserve.schemaEpoch(),reserve.schemaDigest(),digest(9),participants));
    }
    void configure(NativeHomeRecoveryTest.Fixture f) { var initial=helper.initial(0);f.store.configureHomeRecoveryReservations(initial.clusterId(),initial.databaseId(),helper.column); }
    DistributedCommand cas(TransactionStatus before,TransactionStatus next,int step) { return helper.cas(new OperationId(next.origin().firstControl().sessionId(),next.origin().firstControl().epoch(),next.origin().firstControl().sequence()+step),before,next); }
    HomeRecovery abort(TransactionStatus before,long sequence) { return new HomeRecovery(HomeRecovery.Action.ABORT,helper.cas(new OperationId(helper.recoverySession,0,sequence),before,before.decide(TransactionStatus.Phase.ABORT)),null); }
    TransactionStatus row(NativeHomeRecoveryTest.Fixture f,TransactionStatus status)throws Exception { return TransactionStatus.decode(f.store.readCurrentPoint(helper.column,helper.key(status))); }
    @Test void ordinaryHomeCannotRegisterExpiredBusinessUnderFreshControlGrant() throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);f.store.enableSessionControls();var initial=helper.initial(0);
            var ledger=SessionEpochLedger.initial(initial.clusterId(),initial.databaseId(),0)
                    .issue(helper.admin,1000).issue(initial.operation().sessionId(),100).issue(helper.user,1000);
            var grant=ledger.grants().get(1);var control=ledger.grants().getLast();
            var business=TransactionStatus.begin(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.transactionId(),
                    new OperationId(grant.sessionId(),grant.epoch(),0),initial.schemaEpoch(),initial.schemaDigest(),initial.planDigest(),initial.participants());
            helper.apply(f,1,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),0),RangeSessionControl.bind(ledger,grant,32),-1));
            helper.apply(f,2,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),1),RangeSessionControl.bind(ledger,control,32),0));
            helper.apply(f,3,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,control.epoch(),2),RangeSessionControl.advance(ledger.advanceTime(200)),1));
            var command=helper.cas(new OperationId(control.sessionId(),control.epoch(),0),null,business);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,4,command).status());
            assertNull(f.store.readCurrentPoint(helper.column,helper.key(business)));
            assertEquals(0,f.store.homeRecoveryCapacity().reservedCount());
        }
    }
    @Test void registrationReservesWholeFutureStatusAndPromotionCannotDonateItsCreditToStaleAbort()throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);var reserved=reserve(0,2);var registration=cas(null,reserved,0);var estimate=f.store.estimateHomeRegistration(registration);assertEquals(4,estimate.additionalCount());assertTrue(estimate.additionalBytes()>=4L*ReplicatedRangeStore.HomeRecoveryCredit.TRANSITION_BYTES+33+128+2L*TransactionStatus.MAX_BYTES);assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,1,registration).status());var capacity=f.store.homeRecoveryCapacity();assertEquals(4,capacity.reservedCount());assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,2,registration).status());assertEquals(capacity.reservedBytes(),f.store.homeRecoveryCapacity().reservedBytes());
            var preparing=promoted(reserved);var promote=cas(reserved,preparing,1);var growth=f.store.estimateHomeRegistration(promote);assertEquals(0,growth.additionalCount());assertEquals(0,growth.additionalBytes());assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,3,promote).status());assertEquals(preparing,row(f,reserved));assertEquals(capacity.reservedBytes(),f.store.homeRecoveryCapacity().reservedBytes());
            var stale=abort(reserved,0);assertFalse(f.store.estimateHomeRecovery(stale).creditCovered());assertEquals(CommandResult.Status.CONFLICT,f.store.applyHomeRecovery(new ApplyPosition(1,4),stale).receipt().result().status());assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());assertEquals(1,f.store.homeRecoveryCapacity().count());assertEquals(preparing,row(f,reserved));
            assertEquals(CommandResult.Status.CONFLICT,f.store.applyHomeRecovery(new ApplyPosition(2,5),stale).receipt().result().status());assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());assertEquals(1,f.store.homeRecoveryCapacity().count());
        }
    }
    @Test void reservationAndPromotedCreditsSurviveReopenAndQualifiedHomeImage()throws Exception {
        Path source=root.resolve("source"),reservedImage=root.resolve("reserved-image"),promotedImage=root.resolve("promoted-image");var reserved=reserve(0,2);var preparing=promoted(reserved);
        try(var f=helper.new Fixture(source)) { configure(f);helper.apply(f,1,cas(null,reserved,0));try(var pin=f.store.capture()) { NativeRangeImage.write(reservedImage,pin,pin.interval(),helper.limits); }helper.apply(f,2,cas(reserved,preparing,1));try(var pin=f.store.capture()) { NativeRangeImage.write(promotedImage,pin,pin.interval(),helper.limits); } }
        NativeRangeImage.inspectHomeRecoveryReservations(reservedImage,helper.limits,reserved.clusterId(),reserved.databaseId(),helper.column);NativeRangeImage.inspectHomeRecoveryReservations(promotedImage,helper.limits,reserved.clusterId(),reserved.databaseId(),helper.column);
        try(var f=helper.new Fixture(source)) { configure(f);assertEquals(preparing,row(f,reserved));assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());assertEquals(0,f.store.estimateHomeRegistration(cas(reserved,preparing,1)).additionalBytes()); }
        for(boolean promotion:List.of(false,true))try(var f=helper.new Fixture(root.resolve("target-"+promotion))) { configure(f);f.store.fence();f.store.installSnapshot(promotion?promotedImage:reservedImage,helper.limits);f.store.activate();assertEquals(promotion?preparing:reserved,row(f,reserved));assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());if(!promotion)assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,2,cas(reserved,preparing,1)).status()); }
    }
    @Test void emptyReservedAbortMustPrecedeConsumptionAndRetirementAndAckNeverApplies()throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);var reserved=reserve(0,2);helper.apply(f,1,cas(null,reserved,0));helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,6,0),6));assertFalse(reserved.gcEligible());assertTrue(f.store.retirementCandidates(false,null,16).candidate()==null);
            var identity=new HomeIdentity(helper.group,1,new byte[32],helper.column,reserved);assertThrows(IllegalArgumentException.class,()->f.store.estimateHomeRetirement(new HomeRetirement(identity,null,List.of())));assertThrows(IllegalArgumentException.class,()->reserved.finalized(new UUID(0,200)));
            var recovery=abort(reserved,0);assertTrue(f.store.estimateHomeRecovery(recovery).creditCovered());var receipt=f.store.applyHomeRecovery(new ApplyPosition(1,3),recovery).receipt();assertEquals(CommandResult.Status.SUCCESS,receipt.result().status());assertEquals(1,f.store.homeRecoveryCapacity().reservedCount());var terminal=reserved.decide(TransactionStatus.Phase.ABORT);assertEquals(terminal,row(f,reserved));assertTrue(terminal.fullyFinalized());assertFalse(terminal.gcEligible());assertTrue(f.store.retirementCandidates(false,null,16).candidate()==null);
            var consumed=terminal.consume();var consume=new HomeRecovery(HomeRecovery.Action.CONSUME,helper.cas(new OperationId(helper.recoverySession,0,1),terminal,consumed),null);assertTrue(f.store.estimateHomeRecovery(consume).creditCovered());assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,4),consume).receipt().result().status());assertEquals(0,f.store.homeRecoveryCapacity().reservedCount());var retirement=new HomeRetirement(identity,consumed,List.of());assertTrue(f.store.estimateHomeRetirement(retirement));f.store.applyHomeRetirement(new ApplyPosition(1,5),retirement);assertTrue(f.store.homeRetired(retirement));assertTrue(f.store.applyHomeRecovery(new ApplyPosition(2,6),recovery).retired());assertEquals(0,f.store.homeRecoveryCapacity().effectiveCount());
        }
    }
    @Test void businessExpiryBlocksRegistrationAndPromotionDespiteFreshControlGrant()throws Exception {
        for(boolean individual:List.of(false,true))try(var f=helper.new Fixture(root.resolve("expiry-"+individual))) {
            configure(f);f.store.enableSessionControls();var business=helper.initial(0);var ledger=SessionEpochLedger.initial(business.clusterId(),business.databaseId(),0);if(individual)ledger=ledger.issue(helper.admin,1000);ledger=ledger.issue(business.operation().sessionId(),100).issue(helper.user,1000);var businessGrant=ledger.grants().get(individual?1:0);var controlGrant=ledger.grants().getLast();
            var first=DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,controlGrant.epoch(),0),RangeSessionControl.bind(ledger,businessGrant,4),-1);helper.apply(f,1,first);helper.apply(f,2,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,controlGrant.epoch(),1),RangeSessionControl.bind(ledger,controlGrant,4),0));
            var source=origin(0,2,businessGrant.epoch());var bound=new OperationRerouteOrigin(source.clusterId(),source.databaseId(),source.domainId(),source.source(),source.operation(),source.originalReceiptDigest(),source.absenceCommitment(),source.absenceCut(),source.targetDirectoryDigest(),source.planDigest(),2,new OperationId(helper.user,controlGrant.epoch(),0));var reserved=TransactionStatus.reserve(bound,1,new byte[32]);assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,3,cas(null,reserved,0)).status());
            var expired=ledger.advanceTime(200);helper.apply(f,4,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,controlGrant.epoch(),2),RangeSessionControl.advance(expired),1));assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,5,cas(reserved,promoted(reserved),1)).status());assertEquals(reserved,row(f,reserved));assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());
            var another=origin(1,2,businessGrant.epoch());var replacement=TransactionStatus.reserve(new OperationRerouteOrigin(another.clusterId(),another.databaseId(),another.domainId(),another.source(),another.operation(),another.originalReceiptDigest(),another.absenceCommitment(),another.absenceCut(),another.targetDirectoryDigest(),another.planDigest(),2,new OperationId(helper.user,controlGrant.epoch(),2)),1,new byte[32]);assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,6,cas(null,replacement,0)).status());assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());assertTrue(f.store.estimateHomeRecovery(abort(reserved,0)).creditCovered());
            var expiredDecision=reserved.abortReservation(TransactionStatus.NoEffectOutcome.SESSION_EXPIRED);var expireRecovery=new HomeRecovery(HomeRecovery.Action.ABORT,helper.cas(new OperationId(helper.recoverySession,0,0),reserved,expiredDecision),null);if(individual)assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,7),expireRecovery).receipt().result().status());else assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,7,cas(reserved,expiredDecision,3)).status());assertEquals(CommandResult.Status.SESSION_EXPIRED,row(f,reserved).terminalBusinessResult().status());
        }
    }
    @Test void missingBusinessBindingRejectsWithoutConsumingReservationControl()throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);f.store.enableSessionControls();var business=helper.initial(0);
            var ledger=SessionEpochLedger.initial(business.clusterId(),business.databaseId(),0).issue(business.operation().sessionId(),1000).issue(helper.user,1000);
            var businessGrant=ledger.grants().getFirst();var controlGrant=ledger.grants().getLast();
            helper.apply(f,1,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,controlGrant.epoch(),0),RangeSessionControl.bind(ledger,controlGrant,4),-1));
            var source=origin(0,2,businessGrant.epoch());var bound=new OperationRerouteOrigin(source.clusterId(),source.databaseId(),source.domainId(),source.source(),source.operation(),source.originalReceiptDigest(),source.absenceCommitment(),source.absenceCut(),source.targetDirectoryDigest(),source.planDigest(),2,new OperationId(helper.user,controlGrant.epoch(),0));
            var reserved=TransactionStatus.reserve(bound,1,new byte[32]);var command=cas(null,reserved,0);var capacity=f.store.homeRecoveryCapacity();var usage=f.store.receiptUsage();
            assertThrows(IllegalStateException.class,()->helper.apply(f,2,command));assertEquals(new ApplyPosition(1,1),f.store.applied());assertEquals(capacity,f.store.homeRecoveryCapacity());assertEquals(usage,f.store.receiptUsage());assertNull(f.store.readCurrentPoint(helper.column,helper.key(reserved)));
            helper.apply(f,2,DistributedCommand.sessionControl(helper.group,1,new byte[32],new OperationId(helper.admin,controlGrant.epoch(),1),RangeSessionControl.bind(ledger,businessGrant,4),0));
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,3,command).status());assertEquals(reserved,row(f,reserved));assertEquals(4,f.store.homeRecoveryCapacity().reservedCount());
        }
    }
    @Test void capacityAndOriginIdentityRejectBeforeAnyReservationOrParticipantChange()throws Exception {
        try(var f=helper.new Fixture(root)) {
            configure(f);int index=0,registered=0;for(int n=0;n<64;n++){var reserved=reserve(n,64);var command=cas(null,reserved,0);try{f.store.estimateHomeRegistration(command);}catch(IllegalStateException full){break;}assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,command).status());registered++;}assertTrue(registered>0);assertTrue(registered<64);var capacity=f.store.homeRecoveryCapacity();assertTrue(capacity.effectiveCount()<=ReplicatedRangeStore.MAX_HOME_RECOVERIES);assertTrue(capacity.effectiveBytes()<=ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES);
            var next=reserve(100,64);int position=index+1;assertThrows(IllegalStateException.class,()->helper.apply(f,position,cas(null,next,0)));assertEquals(capacity,f.store.homeRecoveryCapacity());
        }
        try(var f=helper.new Fixture(root.resolve("identity"))) { configure(f);var reserved=reserve(0,2);helper.apply(f,1,cas(null,reserved,0));var origin=reserved.origin();var changed=new OperationRerouteOrigin(origin.clusterId(),origin.databaseId(),origin.domainId(),origin.source(),origin.operation(),origin.originalReceiptDigest(),origin.absenceCommitment(),origin.absenceCut(),digest(99),origin.planDigest(),origin.targetParticipantCount(),origin.firstControl());var other=TransactionStatus.reserve(changed,1,new byte[32]);assertEquals(reserved.transactionId(),other.transactionId());assertThrows(IllegalArgumentException.class,()->helper.apply(f,2,cas(reserved,promoted(other),1)));assertEquals(reserved,row(f,reserved));assertEquals(new ApplyPosition(1,1),f.store.applied()); }
    }
    @Test void expiredNoEffectReasonRequiresNativeEvidenceAndPersistsThroughConsumptionReopenAndImage()throws Exception {
        Path path=root.resolve("expiry-reason"),image=root.resolve("expiry-reason-image");var reserved=reserve(0,2);var terminal=reserved.abortReservation(TransactionStatus.NoEffectOutcome.SESSION_EXPIRED);var consumed=terminal.consume();
        var recovery=new HomeRecovery(HomeRecovery.Action.ABORT,helper.cas(new OperationId(helper.recoverySession,0,0),reserved,terminal),null);
        try(var f=helper.new Fixture(path)) {
            configure(f);helper.apply(f,1,cas(null,reserved,0));assertThrows(IllegalArgumentException.class,()->f.store.estimateHomeRecovery(recovery));assertThrows(IllegalArgumentException.class,()->f.store.applyHomeRecovery(new ApplyPosition(1,2),recovery));
            var ordinary=cas(reserved,terminal,1);assertThrows(IllegalArgumentException.class,()->f.store.estimateHomeRegistration(ordinary));assertThrows(IllegalArgumentException.class,()->helper.apply(f,2,ordinary));assertEquals(reserved,row(f,reserved));assertEquals(new ApplyPosition(1,1),f.store.applied());
            helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,6,0),6));assertTrue(f.store.estimateHomeRecovery(recovery).creditCovered());var receipt=f.store.applyHomeRecovery(new ApplyPosition(1,3),recovery).receipt();assertEquals(CommandResult.Status.SUCCESS,receipt.result().status());assertEquals(terminal,row(f,reserved));assertEquals(CommandResult.Status.SESSION_EXPIRED,row(f,reserved).terminalBusinessResult().status());
            assertEquals(1,f.store.homeRecoveryCapacity().reservedCount());var consume=new HomeRecovery(HomeRecovery.Action.CONSUME,helper.cas(new OperationId(helper.recoverySession,0,1),terminal,consumed),null);assertTrue(f.store.estimateHomeRecovery(consume).creditCovered());var consumption=f.store.applyHomeRecovery(new ApplyPosition(1,4),consume).receipt();assertEquals(CommandResult.Status.SUCCESS,consumption.result().status());assertEquals(0,f.store.homeRecoveryCapacity().reservedCount());assertEquals(consumption,f.store.applyHomeRecovery(new ApplyPosition(2,5),consume).receipt());assertEquals(receipt,f.store.homeRecoveryResult(recovery).receipt());assertEquals(consumed,row(f,reserved));
            try(var pin=f.store.capture()) { NativeRangeImage.write(image,pin,pin.interval(),helper.limits); }
        }
        NativeRangeImage.inspectHomeRecoveryReservations(image,helper.limits,reserved.clusterId(),reserved.databaseId(),helper.column);
        try(var f=helper.new Fixture(path)) { configure(f);assertEquals(consumed,row(f,reserved));assertEquals(CommandResult.Status.SESSION_EXPIRED,row(f,reserved).terminalBusinessResult().status());assertEquals(CommandResult.Status.SUCCESS,f.store.homeRecoveryResult(recovery).receipt().result().status()); }
        try(var f=helper.new Fixture(root.resolve("restored-reason"))) { configure(f);f.store.fence();f.store.installSnapshot(image,helper.limits);f.store.activate();assertEquals(consumed,row(f,reserved));assertEquals(CommandResult.Status.SESSION_EXPIRED,row(f,reserved).terminalBusinessResult().status()); }
    }

}
