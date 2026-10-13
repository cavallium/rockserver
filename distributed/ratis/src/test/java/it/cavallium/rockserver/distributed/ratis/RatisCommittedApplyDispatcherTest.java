package it.cavallium.rockserver.distributed.ratis;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.impl.CommittedApplyStateMachine;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisCommittedApplyDispatcherTest {
    private static final UUID A=new UUID(0,1),B=new UUID(0,2);
    private static RatisCommittedApplyDispatcher.Limits limits(){return new RatisCommittedApplyDispatcher.Limits(new RatisCommittedApplyDispatcher.Lane(100,1),new RatisCommittedApplyDispatcher.Lane(100,1),new RatisCommittedApplyDispatcher.Lane(100,1),4);}
    private static CommittedApplyStateMachine.Descriptor descriptor(CommittedApplyStateMachine.Lane lane,long index){return new CommittedApplyStateMachine.Descriptor(1,index,1,lane,100,new byte[32],0);}
    private static RatisCommittedApplyDispatcher.Group group(RatisCommittedApplyDispatcher dispatcher,UUID id,UUID pool){return dispatcher.register(id,pool,100,100,8,entry->descriptor(CommittedApplyStateMachine.Lane.ORDINARY,entry.getIndex()));}
    @Test void defaultEnvelopesCoverLegalColdReadAndNativePlanWithoutAnIndependentReadQuota(){
        long entry=it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES+4096;
        long read=RatisRangeStateMachine.committedReadWeight(entry),execution=RatisRangeStateMachine.committedExecutionWeight(entry);
        var policy=RatisCommittedApplyDispatcher.Policy.defaults();
        assertTrue(read>=2*entry+65536);assertTrue(execution>=2*entry+65536);
        assertEquals(read,policy.pool().read().bytes());assertEquals(execution,policy.pool().ordinary().bytes());assertEquals(execution,policy.pool().protectedLane().bytes());
        assertEquals(2*read,policy.node().read().bytes());assertEquals(2*execution,policy.node().ordinary().bytes());
        try(var dispatcher=new RatisCommittedApplyDispatcher(policy.node(),Map.of(A,policy.pool()))){assertNotNull(dispatcher.register(new UUID(1,1),A,read,execution,RatisRangeStateMachine.COMMITTED_SEGMENT_BYTES,entryProto->descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1)));}
    }
    @Test void twoPoolsShareReadBoundBeforeAnyMaterialization()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits(),B,limits()));var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var a=group(dispatcher,new UUID(1,1),A);var b=group(dispatcher,new UUID(1,2),B);var read=a.awaitRead(1);var entered=new CountDownLatch(1);
            var pending=workers.submit(()->{entered.countDown();try(var permit=b.awaitRead(1)){return dispatcher.stats().reads();}});assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(100,dispatcher.stats().readBytes());assertFalse(pending.isDone());
            read.close();assertEquals(1,pending.get(5,TimeUnit.SECONDS));assertEquals(0,dispatcher.stats().readBytes());
        }
    }
    @Test void ordinaryExhaustionDoesNotHideProtectedHeadAndObserverCancellationCannotDonate()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits(),B,limits()));var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var a=group(dispatcher,new UUID(1,1),A);var b=group(dispatcher,new UUID(1,2),B);var c=group(dispatcher,new UUID(1,3),B);
            var d=descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1);var owner=a.awaitDispatch(d);a.offer(d,owner);assertSame(owner,a.claim(1,1,true));a.withdraw(d,owner);
            var waiting=workers.submit(()->b.awaitDispatch(d));assertTrue(waiting.cancel(false));assertEquals(100,dispatcher.stats().ordinaryBytes());
            try(var read=c.awaitRead(1);var completion=c.tryDispatch(descriptor(CommittedApplyStateMachine.Lane.PROTECTED,1))){assertNotNull(completion);assertEquals(1,dispatcher.stats().protectedOwners());}
            b.stop();owner.close();assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }
    @Test void stopWakesScalarWaiterWithoutRefundingTransferredNativeOwner()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits(),B,limits()));var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var a=group(dispatcher,new UUID(1,1),A);var b=group(dispatcher,new UUID(1,2),B);var d=descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1);var owner=a.awaitDispatch(d);owner.claim();
            var started=new CountDownLatch(1);var pending=workers.submit(()->{started.countDown();return b.awaitDispatch(d);});assertTrue(started.await(5,TimeUnit.SECONDS));b.stop();assertInstanceOf(CommittedApplyStateMachine.Stopped.class,assertThrows(ExecutionException.class,()->pending.get(5,TimeUnit.SECONDS)).getCause());
            dispatcher.close();assertEquals(100,dispatcher.stats().ordinaryBytes());owner.close();owner.close();assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }
    @Test void readinessOnlySignalsOriginalUpdaterAndRetainsCreditUntilActualActionFinishes()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits()));var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var group=group(dispatcher,new UUID(1,1),A);var ready=new CountDownLatch(1);var resume=new CountDownLatch(1);var actionThread=new AtomicReference<Thread>();var updaterThread=new AtomicReference<Thread>();var retained=new AtomicReference<CommittedApplyStateMachine.ApplyPermit>();
            var updater=workers.submit(()->{updaterThread.set(Thread.currentThread());try(var read=group.awaitRead(1)){}var owner=group.awaitDispatch(descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1));owner.claim();retained.set(owner);ready.countDown();assertTrue(resume.await(5,TimeUnit.SECONDS));try(var read=group.awaitRead(2)){}return null;});
            assertTrue(ready.await(5,TimeUnit.SECONDS));assertEquals(100,dispatcher.stats().ordinaryBytes());
            var future=group.executeNative(()->{actionThread.set(Thread.currentThread());group.stop();return Message.EMPTY;});assertFalse(future.isDone());resume.countDown();assertThrows(ExecutionException.class,()->updater.get(5,TimeUnit.SECONDS));assertSame(updaterThread.get(),actionThread.get());assertTrue(future.isDone());assertEquals(100,dispatcher.stats().ordinaryBytes());retained.get().close();assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }
    @Test void infeasiblePolicyAndChangedHandoffFailBeforeEntryOrCallbackAllocation(){
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits()))){assertThrows(IllegalArgumentException.class,()->dispatcher.register(new UUID(1,1),A,101,100,8,entry->descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1)));}
    }
    @Test void asynchronousCommittedTailSignalsAndPumpsWithoutReadingAnotherEntry()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits()));var worker=Executors.newVirtualThreadPerTaskExecutor()){
            var group=group(dispatcher,new UUID(1,1),A);var signals=new java.util.concurrent.atomic.AtomicInteger();var nativeCalls=new java.util.concurrent.atomic.AtomicInteger();group.bindWakeup(signals::incrementAndGet);
            try(var read=group.awaitRead(1)){}var owner=group.awaitDispatch(descriptor(CommittedApplyStateMachine.Lane.ORDINARY,1));owner.claim();
            var pending=worker.submit(()->group.executeNative(()->{nativeCalls.incrementAndGet();owner.close();return Message.EMPTY;})).get(5,TimeUnit.SECONDS);
            assertEquals(1,signals.get());assertEquals(0,nativeCalls.get());assertFalse(pending.isDone());assertEquals(100,dispatcher.stats().ordinaryBytes());
            group.pump();assertEquals(Message.EMPTY,pending.get(5,TimeUnit.SECONDS));assertEquals(1,nativeCalls.get());assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }

    @Test void pausedOfferAndClaimCannotDonateOrAdoptPreviousGeneration()throws Exception {
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits(),Map.of(A,limits()))){
            var group=group(dispatcher,new UUID(1,1),A);var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(1).build();CommittedApplyStateMachine.Descriptor d;
            try(var read=group.awaitRead(1)){d=group.describe(entry);}var oldDescriptor=d;var old=group.awaitDispatch(d);group.wakeup();group.resume();assertThrows(CommittedApplyStateMachine.Retry.class,()->group.offer(oldDescriptor,old));assertFalse(old.claimed());assertEquals(100,dispatcher.stats().ordinaryBytes());old.close();
            try(var read=group.awaitRead(1)){d=group.describe(entry);}var current=group.awaitDispatch(d);group.offer(d,current);group.wakeup();assertThrows(CommittedApplyStateMachine.Retry.class,()->group.claim(1,1,false));assertFalse(current.claimed());assertEquals(100,dispatcher.stats().ordinaryBytes());group.withdraw(d,current);current.close();assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }

}
