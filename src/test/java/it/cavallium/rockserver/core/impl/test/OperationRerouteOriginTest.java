package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OperationRerouteOriginTest {
    static UUID id(int n) { return new UUID(0,n); }
    static byte[] digest(int n) { var bytes=new byte[32]; bytes[0]=(byte)n; return bytes; }
    static OperationRerouteOrigin origin(int participants) { return new OperationRerouteOrigin(id(1),id(2),id(3),new DistributedCdcCheckpoint.Lineage(id(20),id(21),7),new OperationId(id(5),1,0),digest(1),digest(2),new ApplyPosition(3,40),digest(3),digest(4),participants,new OperationId(id(30),2,10)); }
    @Test void originalIdentityOwnsOneHomeKeyAcrossPayloadEvidenceTargetPlanAndControlChanges() {
        var original=origin(2); var changed=new OperationRerouteOrigin(original.clusterId(),original.databaseId(),original.domainId(),original.source(),original.operation(),digest(9),digest(8),new ApplyPosition(4,50),digest(7),digest(6),1,new OperationId(id(31),3,11));
        assertEquals(original.transactionId(),changed.transactionId()); assertNotEquals(original,changed); assertFalse(Arrays.equals(original.digest(),changed.digest()));
        var epoch=new OperationRerouteOrigin(original.clusterId(),original.databaseId(),original.domainId(),new DistributedCdcCheckpoint.Lineage(original.source().rangeId(),original.source().groupId(),8),original.operation(),original.originalReceiptDigest(),original.absenceCommitment(),original.absenceCut(),original.targetDirectoryDigest(),original.planDigest(),2,original.firstControl()); assertNotEquals(original.transactionId(),epoch.transactionId());
        var operation=new OperationRerouteOrigin(original.clusterId(),original.databaseId(),original.domainId(),original.source(),new OperationId(original.operation().sessionId(),1,1),original.originalReceiptDigest(),original.absenceCommitment(),original.absenceCut(),original.targetDirectoryDigest(),original.planDigest(),2,original.firstControl()); assertNotEquals(original.transactionId(),operation.transactionId());
    }
    @Test void fixedCanonicalCodecOwnsDigestsAndRejectsMalformedBoundsBeforeAllocation() {
        var origin=origin(64); assertEquals(300,origin.encode().length); assertEquals(origin,OperationRerouteOrigin.decode(origin.encode())); assertEquals(origin.hashCode(),OperationRerouteOrigin.decode(origin.encode()).hashCode());
        byte[] bytes=origin.encode(); Arrays.fill(bytes,(byte)0); origin.planDigest()[0]=0; origin.originalReceiptDigest()[0]=0; origin.absenceCommitment()[0]=0; origin.targetDirectoryDigest()[0]=0; assertEquals(4,origin.planDigest()[0]); assertEquals(1,origin.originalReceiptDigest()[0]);
        for(int n=0;n<300;n++) { var truncated=Arrays.copyOf(origin.encode(),n); assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(new byte[301]));
        for(int count:List.of(0,65,-1)) { var invalid=origin.encode(); ByteBuffer.wrap(invalid).putInt(264,count); assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(invalid)); }
        var cut=origin.encode(); ByteBuffer.wrap(cut).putLong(184,0); assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(cut));
        var control=origin.encode(); ByteBuffer.wrap(control).putLong(284,0); assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(control));
        var last=origin.encode(); ByteBuffer.wrap(last).putLong(292,Long.MAX_VALUE-255); assertEquals(Long.MAX_VALUE-255,OperationRerouteOrigin.decode(last).firstControl().sequence()); ByteBuffer.wrap(last).putLong(292,Long.MAX_VALUE-254); assertThrows(IllegalArgumentException.class,()->OperationRerouteOrigin.decode(last));
    }
}
