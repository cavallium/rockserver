package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisMigrationControllerTest {
    final UUID pool=new UUID(0,1),a=new UUID(0,2),b=new UUID(0,3);
    final AtomicLong now=new AtomicLong();final AtomicReference<RatisDiskRunway.State> pressure=new AtomicReference<>(RatisDiskRunway.State.CLEAR);
    RatisMigrationController controller(){return new RatisMigrationController(new RatisMigrationController.Policy(100,100,100,10,200,100,2,4),Set.of(pool),now::get,pressure::get);}
    @Test void twoGroupsShareTheCeilingAndTakeTurnsAfterRefill()throws Exception{
        var c=controller();assertTrue(c.acquire(a,pool,50,false));assertTrue(c.acquire(b,pool,50,false));
        assertFalse(c.acquire(a,pool,50,false));assertFalse(c.acquire(b,pool,50,false));now.set(500_000_000L);
        assertFalse(c.acquire(b,pool,50,false));assertTrue(c.acquire(a,pool,50,false));assertFalse(c.acquire(a,pool,50,false));
        now.set(1_000_000_000L);assertFalse(c.acquire(a,pool,50,false));assertTrue(c.acquire(b,pool,50,false));assertFalse(c.acquire(a,pool,50,false));
    }
    @Test void unequalQuantaAreFairByBytesRatherThanRequestCount()throws Exception{
        var c=controller();assertTrue(c.acquire(a,pool,100,false));assertFalse(c.acquire(b,pool,10,false));assertFalse(c.acquire(a,pool,100,false));
        for(int n=1;n<=9;n++){now.set(n*100_000_000L);assertTrue(c.acquire(b,pool,10,false));assertFalse(c.acquire(a,pool,100,false));}
        for(int n=10;n<=19;n++){now.set(n*100_000_000L);assertFalse(c.acquire(b,pool,10,false));}assertTrue(c.acquire(a,pool,100,false));
    }
    @Test void twoPoolsCannotCreateCrossedFairnessHeads()throws Exception{
        var q=new UUID(0,5);var c=new RatisMigrationController(new RatisMigrationController.Policy(1000,1000,100,10,200,100,2,4),Set.of(pool,q),now::get,pressure::get);
        assertTrue(c.acquire(new UUID(0,6),q,10,false));now.set(100_000_000L);assertTrue(c.acquire(a,pool,100,false));
        assertFalse(c.acquire(a,pool,10,false));assertFalse(c.acquire(b,pool,50,false));now.set(200_000_000L);
        boolean first=c.acquire(a,pool,10,false);boolean second=c.acquire(b,pool,50,false);assertTrue(first || second);
    }
    @Test void tinyPayloadRefundPreservesActualRateAndOwnership()throws Exception{
        var c=controller();assertTrue(c.acquire(a,pool,100,false));c.refundUnused(a,pool,99);assertTrue(c.acquire(b,pool,99,false));assertFalse(c.acquire(b,pool,1,false));
    }
    @Test void recoveryOwnsFiniteCreditsWhenOrdinaryAndControlAreSaturated()throws Exception{
        var ordinary=new RatisNodeResourceBudget(new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(10,1),new RatisNodeResourceBudget.Lane(10,1)),Map.of(pool,new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(10,1),new RatisNodeResourceBudget.Lane(10,1))));
        try(var full=ordinary.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,10,1);var control=ordinary.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,10,1)){
            var c=controller();var lease=c.reserveRecovery(pool,100);assertEquals(100,c.usage().recoveryBytes());assertThrows(IOException.class,()->c.reserveRecovery(pool,1));
            assertEquals(10,ordinary.stats().protectedLane().bytes());lease.close();lease.close();assertEquals(0,c.usage().recoveryBytes());
        }
    }
    @Test void pressureReducesBulkButRecoveryStillHasFiniteProgress()throws Exception{
        var c=controller();assertTrue(c.acquire(a,pool,100,false));pressure.set(RatisDiskRunway.State.EMERGENCY);
        for(int n=0;n<10;n++){now.set(n*100_000_000L);assertFalse(c.acquire(a,pool,10,false));}now.set(1_000_000_000L);assertTrue(c.acquire(b,pool,10,true));assertFalse(c.acquire(b,pool,1,true));
        now.set(2_000_000_000L);assertTrue(c.acquire(b,pool,10,true));
    }
}
