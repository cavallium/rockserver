package org.apache.ratis.server.leader;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.server.RaftConfiguration;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MigrationTransportOwnersTest {
    MigrationLogStateMachine.Lease lease(AtomicInteger closed){return new MigrationLogStateMachine.Lease(){public void materialized(long bytes){}public void close(){closed.incrementAndGet();}};}
    @Test void acknowledgementAndDrainRetainActualSendUntilItReturns(){
        var closed=new AtomicInteger();var owners=new MigrationTransportOwners(new Object(),()->{},()->{});owners.begin(1,lease(closed));
        owners.acknowledged(1);assertEquals(0,closed.get());owners.terminal();assertFalse(owners.drained().isDone());
        owners.sent(1);assertEquals(1,closed.get());assertTrue(owners.drained().isDone());owners.terminal();assertEquals(1,closed.get());
    }
    @Test void sentOwnerWaitsForAcknowledgementOrActualTerminal(){
        var closed=new AtomicInteger();var owners=new MigrationTransportOwners(new Object(),()->{},()->{});owners.begin(1,lease(closed));owners.sent(1);
        assertEquals(1,owners.size());assertEquals(0,closed.get());owners.terminal();assertEquals(1,closed.get());assertTrue(owners.drained().isDone());
    }
    @Test void oldAcknowledgementCannotRemoveANewOwnerFromResetStopDrain()throws Exception {
        var lock=new Object();var registry=new HashSet<MigrationTransportOwners>();var ref=new java.util.concurrent.atomic.AtomicReference<MigrationTransportOwners>();var closed=new AtomicInteger();
        var pruning=new java.util.concurrent.CountDownLatch(1);var unblock=new java.util.concurrent.CountDownLatch(1);
        var owners=new MigrationTransportOwners(lock,()->{pruning.countDown();try{assertTrue(unblock.await(5,java.util.concurrent.TimeUnit.SECONDS));}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}registry.remove(ref.get());},()->registry.add(ref.get()));ref.set(owners);
        owners.begin(1,lease(closed));owners.sent(1);
        try(var threads=java.util.concurrent.Executors.newFixedThreadPool(2)){
            var old=threads.submit(()->owners.acknowledged(1));assertTrue(pruning.await(5,java.util.concurrent.TimeUnit.SECONDS));var began=new java.util.concurrent.CountDownLatch(1);
            var next=threads.submit(()->{began.countDown();owners.begin(2,lease(closed));});assertTrue(began.await(5,java.util.concurrent.TimeUnit.SECONDS));assertFalse(next.isDone());
            unblock.countDown();old.get(5,java.util.concurrent.TimeUnit.SECONDS);next.get(5,java.util.concurrent.TimeUnit.SECONDS);
            java.util.concurrent.CompletableFuture<?> stop;synchronized(lock){assertTrue(registry.contains(owners));stop=java.util.concurrent.CompletableFuture.allOf(registry.stream().map(MigrationTransportOwners::drained).toArray(java.util.concurrent.CompletableFuture[]::new));}
            owners.terminal();assertFalse(stop.isDone());owners.sent(2);stop.get(5,java.util.concurrent.TimeUnit.SECONDS);assertEquals(2,closed.get());synchronized(lock){assertTrue(registry.isEmpty());}
        }finally{unblock.countDown();}
    }
    @Test void promotionInEitherConfigurationExemptsVoters(){
        var peer=RaftPeer.newBuilder().setId("learner").build();
        RaftConfiguration configuration=(RaftConfiguration)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RaftConfiguration.class},(proxy,method,args)->{
            if(method.getName().equals("getPeer")){var roles=(RaftPeerRole[])args[1];return Arrays.asList(roles).contains(RaftPeerRole.FOLLOWER)?peer:peer;}
            return null;
        });
        assertFalse(LogAppenderBase.migrationListener(configuration,peer.getId()));
        RaftConfiguration listener=(RaftConfiguration)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RaftConfiguration.class},(proxy,method,args)->method.getName().equals("getPeer") && Arrays.asList((RaftPeerRole[])args[1]).contains(RaftPeerRole.LISTENER)?peer:null);
        assertTrue(LogAppenderBase.migrationListener(listener,peer.getId()));
    }
}
