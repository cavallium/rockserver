package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RejectedParticipantTest {
    final PreparedParticipantTest helper=new PreparedParticipantTest();
    final ApplyPosition position=new ApplyPosition(2,3);
    final OperationId recovery=new OperationId(new UUID(0,90),3,0);
    ParticipantFinalization finalization(TransactionStatus status) { return new ParticipantFinalization(helper.home,1,helper.group,recovery,status); }
    RejectedParticipant rejected() { return RejectedParticipant.from(helper.preparation(),position); }
    @Test void compactCanonicalReceiptOwnsDigestAndRoundTrips() {
        var preparation=helper.preparation(); byte[] digest=preparation.digest();
        var rejected=new RejectedParticipant(helper.group,helper.home,1,helper.prepareId,preparation.registration(),digest,position);
        int hash=rejected.hashCode(); Arrays.fill(digest,(byte)4); Arrays.fill(rejected.preparationDigest(),(byte)5);
        assertArrayEquals(preparation.digest(),rejected.preparationDigest()); assertEquals(hash,rejected.hashCode());
        byte[] bytes=rejected.encode(); assertEquals(RejectedParticipant.HEADER_BYTES+preparation.registration().encode().length,bytes.length);
        assertTrue(bytes.length<=RejectedParticipant.MAX_BYTES); var decoded=RejectedParticipant.decode(bytes);
        assertEquals(rejected,decoded); assertEquals(hash,decoded.hashCode()); Arrays.fill(bytes,(byte)0); assertEquals(rejected,decoded);
        rejected.requirePreparation(preparation); assertEquals(helper.transaction,rejected.transactionId());
        assertTrue(rejected.ownsOperation(helper.prepareId)); assertTrue(rejected.ownsOperation(helper.businessId)); assertFalse(rejected.ownsOperation(recovery));
    }
    @Test void exactRetryCannotChangeControlHomeRegistrationOrDigest() {
        var p=helper.preparation(); var rejected=rejected();
        assertThrows(IllegalArgumentException.class,() -> rejected.requirePreparation(new PreparedParticipant.Preparation(helper.home,1,recovery,p.registration(),p.business())));
        assertThrows(IllegalArgumentException.class,() -> rejected.requirePreparation(new PreparedParticipant.Preparation(new UUID(0,91),1,p.operation(),p.registration(),p.business())));
        assertThrows(IllegalArgumentException.class,() -> rejected.requirePreparation(new PreparedParticipant.Preparation(helper.home,2,p.operation(),p.registration(),p.business())));
        byte[] changed=p.digest(); changed[0]^=1;
        var wrongDigest=new RejectedParticipant(helper.group,helper.home,1,p.operation(),p.registration(),changed,position);
        assertThrows(IllegalArgumentException.class,() -> wrongDigest.requirePreparation(p));
        var initial=p.registration(); var other=new TransactionStatus.Participant(new UUID(0,92),new UUID(0,92),1,new byte[32],new byte[32],null,false);
        var registration=TransactionStatus.begin(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.transactionId(),initial.operation(),initial.schemaEpoch(),initial.schemaDigest(),initial.planDigest(),List.of(initial.participants().getFirst(),other));
        var first=new PreparedParticipant.Preparation(helper.home,1,p.operation(),registration,p.business());
        var advanced=new PreparedParticipant.Preparation(helper.home,1,p.operation(),registration.prepared(other.groupId(),other.planDigest(),new ApplyPosition(1,1)),p.business());
        assertTrue(first.registration().sameDefinition(advanced.registration()));
        assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.from(first,position).requirePreparation(advanced));
    }
    @Test void onlyBoundAbortWithoutPositiveHomeVoteMayFinalizeRejection() {
        var p=helper.preparation(); var rejected=rejected(); var abort=p.registration().decide(TransactionStatus.Phase.ABORT);
        rejected.requireFinalization(finalization(abort));
        rejected.requireFinalization(finalization(abort.finalized(helper.group).consume()));
        var positive=p.registration().prepared(helper.group,p.business().payloadDigest(),new ApplyPosition(1,1));
        assertThrows(IllegalArgumentException.class,() -> rejected.requireFinalization(finalization(positive.decide(TransactionStatus.Phase.COMMIT))));
        assertThrows(IllegalArgumentException.class,() -> rejected.requireFinalization(finalization(positive.decide(TransactionStatus.Phase.ABORT))));
        assertThrows(IllegalArgumentException.class,() -> rejected.requireFinalization(new ParticipantFinalization(helper.home,1,helper.group,p.operation(),abort)));
        assertThrows(IllegalArgumentException.class,() -> rejected.requireFinalization(new ParticipantFinalization(new UUID(0,91),1,helper.group,recovery,abort)));
        assertThrows(IllegalArgumentException.class,() -> rejected.requireFinalization(new ParticipantFinalization(helper.home,2,helper.group,recovery,abort)));
    }
    @Test void negativeVoteFollowsItsAcceptedCutAndMayUseANewerTerm() {
        var original=helper.accepted().acceptance();
        var accepted=DistributedCommand.acceptedTransaction(helper.base(),new TransactionAcceptance(new ApplyPosition(2,3),original.leaderGeneration(),original.ownerIncarnation(),original.scope(),original.result()));
        var p=new PreparedParticipant.Preparation(helper.home,1,helper.prepareId,helper.registration(accepted.payloadDigest()),accepted);
        for (var invalid:List.of(new ApplyPosition(2,2),new ApplyPosition(2,3),new ApplyPosition(1,4))) {
            assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.from(p,invalid));
            var receipt=new RejectedParticipant(helper.group,helper.home,1,p.operation(),p.registration(),p.digest(),invalid);
            assertThrows(IllegalArgumentException.class,() -> receipt.requirePreparation(p));
        }
        RejectedParticipant.from(p,new ApplyPosition(2,4)).requirePreparation(p);
        RejectedParticipant.from(p,new ApplyPosition(3,4)).requirePreparation(p);
    }
    @Test void boundsMalformedFramesAndImpossibleRejectionsAreRejected() {
        byte[] bytes=rejected().encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.decode(Arrays.copyOf(bytes,bytes.length+1)));
        byte[] length=bytes.clone(); ByteBuffer.wrap(length).putInt(120,-1); assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.decode(length));
        byte[] zeroPosition=bytes.clone(); ByteBuffer.wrap(zeroPosition).putLong(112,0); assertThrows(IllegalArgumentException.class,() -> RejectedParticipant.decode(zeroPosition));
        var p=helper.preparation();
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(helper.group,helper.home,-1,p.operation(),p.registration(),p.digest(),position));
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(helper.group,helper.home,1,p.registration().operation(),p.registration(),p.digest(),position));
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(helper.group,helper.home,1,p.operation(),p.registration(),new byte[31],position));
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(new UUID(0,93),helper.home,1,p.operation(),p.registration(),p.digest(),position));
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(helper.group,helper.home,1,p.operation(),p.registration().decide(TransactionStatus.Phase.ABORT),p.digest(),position));
        var voted=p.registration().prepared(helper.group,p.business().payloadDigest(),new ApplyPosition(1,1));
        assertThrows(IllegalArgumentException.class,() -> new RejectedParticipant(helper.group,helper.home,1,p.operation(),voted,p.digest(),position));
    }
    @Test void abortFenceHasOnlyFinalizationAndPositionInCurrentFormat() {
        var abort=finalization(helper.preparation().registration().decide(TransactionStatus.Phase.ABORT));
        var fence=new AbortedParticipant(abort,position); assertEquals(16+abort.encode().length,fence.encode().length);
        assertEquals(fence,AbortedParticipant.decode(fence.encode()));
        assertTrue(fence.ownsOperation(recovery)); assertTrue(fence.ownsOperation(helper.businessId)); assertFalse(fence.ownsOperation(helper.prepareId));
        byte[] bytes=fence.encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> AbortedParticipant.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> AbortedParticipant.decode(Arrays.copyOf(bytes,bytes.length+1)));
    }
}
