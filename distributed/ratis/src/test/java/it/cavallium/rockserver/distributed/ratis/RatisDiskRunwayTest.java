package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisDiskRunwayTest {
    static final UUID POOL=new UUID(1,1);
    static RatisDiskRunway.Policy policy(){return new RatisDiskRunway.Policy(200,100,300,400,400,50,Duration.ofSeconds(1),16,64,2);}
    static Map<Long,RatisDiskRunway.Filesystem> free(long bytes){return Map.of(7L,new RatisDiskRunway.Filesystem(7,bytes));}
    static RatisDiskRunway.Census census(long time,long cut,long bytes){return new RatisDiskRunway.Census(time,cut,free(bytes),Map.of(POOL,Set.of(7L)),Set.of(7L),12);}
    static void observe(RatisDiskRunway runway,long time,long bytes)throws IOException {runway.observeFree(time,runway.beginFreeObservation(),free(bytes));}
    static void publish(RatisDiskRunway runway,long time,long bytes)throws IOException {runway.publish(census(time,runway.beginCensus(),bytes));observe(runway,time,bytes);}
    @Test void onlyFreshCompleteCensusOpensOrdinaryAndHysteresisCannotChatter()throws Exception {
        var clock=new AtomicLong(10);var runway=new RatisDiskRunway(policy(),clock::get);assertThrows(IOException.class,runway::requireOrdinary);publish(runway,10,400);runway.requireOrdinary();assertEquals(RatisDiskRunway.State.CLEAR,runway.state());
        observe(runway,10,50);assertEquals(RatisDiskRunway.State.EMERGENCY,runway.state());assertThrows(IOException.class,runway::requireOrdinary);observe(runway,10,150);assertEquals(RatisDiskRunway.State.EMERGENCY,runway.state());observe(runway,10,210);assertEquals(RatisDiskRunway.State.WARNING,runway.state());
        observe(runway,10,180);assertEquals(RatisDiskRunway.State.WARNING,runway.state());observe(runway,10,250);assertEquals(RatisDiskRunway.State.WARNING,runway.state());observe(runway,10,310);assertEquals(RatisDiskRunway.State.CLEAR,runway.state());clock.addAndGet(Duration.ofSeconds(2).toNanos());assertThrows(IOException.class,runway::requireOrdinary);
    }
    @Test void actualWriterAndLaterFreeSweepAreBothRequiredAndProtectedReserveRemainsIndependent()throws Exception {
        var clock=new AtomicLong(10);var runway=new RatisDiskRunway(policy(),clock::get);publish(runway,10,500);var ordinary=runway.reserveRaftAppend(POOL,400,false);assertThrows(IOException.class,()->runway.reserveRaftAppend(POOL,1,false));ordinary.close();publish(runway,10,500);assertEquals(400,runway.stats().pendingOrdinaryBytes());
        observe(runway,10,50);var protectedLease=runway.reserveRaftAppend(POOL,100,true);assertEquals(100,runway.stats().pendingProtectedBytes());long inProgress=runway.beginFreeObservation();ordinary.actualFinished();runway.observeFree(10,inProgress,free(500));assertFalse(ordinary.released());observe(runway,10,500);assertTrue(ordinary.released());assertFalse(protectedLease.released());assertEquals(0,runway.stats().pendingOrdinaryBytes());
    }
    @Test void unknownWriterRetainsReservationAndOldCensusCannotOverwriteNewEmergency()throws Exception {
        var clock=new AtomicLong(20);var runway=new RatisDiskRunway(policy(),clock::get);publish(runway,10,500);var unknown=runway.reserveRaftAppend(POOL,100,false);long old=runway.beginCensus();publish(runway,20,50);assertThrows(IOException.class,()->runway.publish(census(10,old,500)));assertEquals(RatisDiskRunway.State.EMERGENCY,runway.state());assertFalse(unknown.released());runway.invalidate();assertThrows(IOException.class,runway::requireOrdinary);runway.reserveRaftAppend(POOL,1,true);
    }
    @Test void actualRecoveryOwnerSurvivesObservationAndSerializesNativeWork()throws Exception {
        var clock=new AtomicLong(10);var runway=new RatisDiskRunway(policy(),clock::get);publish(runway,10,500);var owner=runway.reserveRecovery(POOL,50);assertThrows(IOException.class,()->runway.reserveRecovery(POOL,50));assertThrows(IOException.class,()->runway.awaitActualRecovery(Duration.ZERO));owner.actualFinished();owner.actualFinished();runway.awaitActualRecovery(Duration.ZERO);assertEquals(50,runway.stats().pendingProtectedBytes());observe(runway,10,500);assertEquals(0,runway.stats().pendingProtectedBytes());
    }
    @Test void sustainedSmallAppendsReconcileWithoutWaitingForMultiQuantumInventoryOrRetainingAnOwnerEach()throws Exception {
        var clock=new AtomicLong(10);var policy=new RatisDiskRunway.Policy(200,100,300,1<<20,400,50,Duration.ofMinutes(5),16,64,2);var runway=new RatisDiskRunway(policy,clock::get);publish(runway,clock.get(),2<<20);
        long inventory=runway.beginCensus(); // This inventory can require many rotating quanta.
        for(int round=0;round<10;round++){
            long sweep=runway.beginFreeObservation();
            for(int append=0;append<10000;append++)runway.reserveRaftAppend(POOL,8,false).actualFinished();
            assertEquals(80000,runway.stats().pendingOrdinaryBytes());assertEquals(0,runway.stats().owners());
            runway.observeFree(clock.get(),sweep,free(2<<20));assertEquals(80000,runway.stats().pendingOrdinaryBytes()); // Sweep began before these actual exits.
            clock.addAndGet(Duration.ofSeconds(1).toNanos());observe(runway,clock.get(),2<<20);assertEquals(0,runway.stats().pendingOrdinaryBytes());runway.requireOrdinary();
        }
        runway.publish(census(10,inventory,2<<20));runway.requireOrdinary();
        clock.addAndGet(Duration.ofSeconds(6).toNanos());assertThrows(IOException.class,runway::requireOrdinary);observe(runway,clock.get(),50);assertEquals(RatisDiskRunway.State.EMERGENCY,runway.state());
    }
}
