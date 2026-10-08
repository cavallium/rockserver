package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SessionEpochLedgerTest {
    final UUID cluster=new UUID(1,1), database=new UUID(1,2), a=new UUID(2,1), b=new UUID(2,2);
    @Test void outOfOrderExpiryWaitsForContiguousPrefixAndClockRollbackCannotReactivate() {
        var initial=SessionEpochLedger.initial(cluster,database,10);
        var first=initial.issue(a,100); first.requireSuccessorOf(initial);
        var second=first.issue(b,50); second.requireSuccessorOf(first);
        assertEquals(1,second.minimumEpoch()); assertEquals(3,second.nextEpoch());
        var expired=second.advanceTime(50); expired.requireSuccessorOf(second);
        assertEquals(1,expired.minimumEpoch()); assertEquals(2,expired.grants().size());
        assertTrue(expired.isLive(new OperationId(a,1,0))); assertFalse(expired.isLive(new OperationId(b,2,0)));
        assertFalse(expired.isLive(new OperationId(b,1,0))); assertFalse(expired.isLive(new OperationId(a,3,0)));
        var rollback=expired.advanceTime(0); rollback.requireSuccessorOf(expired);
        assertEquals(50,rollback.logicalTimeMillis()); assertFalse(rollback.isLive(new OperationId(b,2,0)));
        var closed=rollback.advanceTime(100); closed.requireSuccessorOf(rollback);
        assertEquals(3,closed.minimumEpoch()); assertTrue(closed.grants().isEmpty());
        var later=closed.issue(a,200); later.requireSuccessorOf(closed);
        assertEquals(3,later.grants().getFirst().epoch()); assertFalse(later.isLive(new OperationId(a,1,0)));
    }
    @Test void canonicalCodecBoundsAndEveryTruncationAreValidated() {
        var ledger=SessionEpochLedger.initial(cluster,database,10).issue(a,100).issue(b,200);
        byte[] encoded=ledger.encode(); assertEquals(156,encoded.length);
        assertEquals(ledger,SessionEpochLedger.decode(encoded)); assertArrayEquals(encoded,SessionEpochLedger.decode(encoded).encode());
        for (int n=0;n<encoded.length;n++) {
            byte[] truncated=Arrays.copyOf(encoded,n);
            assertThrows(IllegalArgumentException.class,() -> SessionEpochLedger.decode(truncated));
        }
        assertThrows(IllegalArgumentException.class,() -> SessionEpochLedger.decode(Arrays.copyOf(encoded,encoded.length+1)));
        for (int count : new int[]{-1,1025,Integer.MAX_VALUE}) {
            byte[] bad=encoded.clone(); ByteBuffer.wrap(bad).putInt(72,count);
            assertThrows(IllegalArgumentException.class,() -> SessionEpochLedger.decode(bad));
        }
    }
    @Test void transitionsCannotRewriteHistorySkipEpochsOrResurrectExpiredGrants() {
        var initial=SessionEpochLedger.initial(cluster,database,10); initial.requireSuccessorOf(null);
        var issued=initial.issue(a,100);
        assertThrows(IllegalArgumentException.class,() -> issued.requireSuccessorOf(null));
        var rewritten=new SessionEpochLedger(cluster,database,2,10,1,2,List.of(new SessionEpochLedger.Grant(1,a,10,200)));
        assertThrows(IllegalArgumentException.class,() -> rewritten.requireSuccessorOf(issued));
        var expired=issued.advanceTime(100);
        var backwards=new SessionEpochLedger(cluster,database,expired.generation()+1,10,1,2,issued.grants());
        assertThrows(IllegalArgumentException.class,() -> backwards.requireSuccessorOf(expired));
        var foreign=new SessionEpochLedger(UUID.randomUUID(),database,1,10,1,1,List.of());
        assertThrows(IllegalArgumentException.class,() -> foreign.requireSuccessorOf(initial));
        assertThrows(IllegalArgumentException.class,() -> new SessionEpochLedger(cluster,database,0,10,9,9,List.of()).requireSuccessorOf(null));
    }
    @Test void finiteLifetimeCapacityAndArithmeticBoundariesFailExplicitly() {
        var ledger=SessionEpochLedger.initial(cluster,database,10);
        assertThrows(IllegalArgumentException.class,() -> ledger.issue(a,10));
        assertThrows(IllegalArgumentException.class,() -> ledger.issue(a,10+SessionEpochLedger.MAX_LIFETIME_MILLIS+1));
        var full=ledger;
        for (int i=0;i<SessionEpochLedger.MAX_GRANTS;i++) full=full.issue(new UUID(0,i),100);
        assertEquals(SessionEpochLedger.MAX_BYTES,full.encode().length);
        var captured=full; assertThrows(IllegalStateException.class,() -> captured.issue(a,100));
        var exhausted=new SessionEpochLedger(cluster,database,0,10,Long.MAX_VALUE,Long.MAX_VALUE,List.of());
        assertThrows(IllegalStateException.class,() -> exhausted.issue(a,100));
        var generationEnd=new SessionEpochLedger(cluster,database,Long.MAX_VALUE,10,1,1,List.of());
        assertThrows(ArithmeticException.class,() -> generationEnd.issue(a,100));
        assertThrows(ArithmeticException.class,() -> generationEnd.advanceTime(11));
    }
}
