package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CommittedParticipantTest {
    final PreparedParticipantTest helper=new PreparedParticipantTest();
    final PreparedParticipant prepared=new PreparedParticipant(helper.preparation(),new ApplyPosition(1,1));
    ParticipantFinalization intent() {
        var p=prepared.preparation(); var decision=p.registration().prepared(helper.group,p.business().payloadDigest(),prepared.position()).decide(TransactionStatus.Phase.COMMIT);
        return new ParticipantFinalization(helper.home,1,helper.group,new OperationId(new UUID(0,90),2,0),decision);
    }
    @Test void canonicalOwnedReceiptRetainsOriginalResultAndIndependentRecoveryIdentity() {
        var receipt=new CommittedParticipant(intent(),new ApplyPosition(2,2)); byte[] bytes=receipt.encode(); assertEquals(receipt,CommittedParticipant.decode(bytes));
        Arrays.fill(bytes,(byte)0); assertEquals(receipt,CommittedParticipant.decode(receipt.encode()));
        assertArrayEquals(helper.accepted().acceptance().result(),receipt.result(prepared)); byte[] result=receipt.result(prepared); Arrays.fill(result,(byte)0); assertArrayEquals(helper.accepted().acceptance().result(),receipt.result(prepared));
        assertTrue(receipt.ownsOperation(intent().operation())); assertTrue(receipt.ownsOperation(helper.businessId)); assertFalse(receipt.ownsOperation(helper.prepareId));
        var progress=intent().decision().finalized(helper.group).consume();
        assertEquals(receipt,new CommittedParticipant(new ParticipantFinalization(helper.home,1,helper.group,intent().operation(),progress),receipt.position()));
    }
    @Test void invalidPhasePositionBindingAndTruncatedReceiptAreRejected() {
        var receipt=new CommittedParticipant(intent(),new ApplyPosition(2,2)); byte[] bytes=receipt.encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> CommittedParticipant.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> CommittedParticipant.decode(Arrays.copyOf(bytes,bytes.length+1)));
        assertThrows(IllegalArgumentException.class,() -> new CommittedParticipant(intent(),new ApplyPosition(1,1)));
        var abort=new ParticipantFinalization(helper.home,1,helper.group,intent().operation(),helper.preparation().registration().decide(TransactionStatus.Phase.ABORT));
        assertThrows(IllegalArgumentException.class,() -> new CommittedParticipant(abort,new ApplyPosition(2,2)));
        assertThrows(IllegalArgumentException.class,() -> receipt.result(new PreparedParticipant(helper.preparation(),new ApplyPosition(1,2))));
    }
}
