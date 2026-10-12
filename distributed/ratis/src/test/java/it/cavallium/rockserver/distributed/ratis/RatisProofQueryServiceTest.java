package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.Message;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Independent bounded proof executor; no server, native store or network fixture. */
class RatisProofQueryServiceTest {
    final UUID group=new UUID(1,1);
    @Test void cancelledViewsCoalesceAndStopRetainsRunningActionAndInputUntilReturn() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new AtomicInteger(); byte[] input={1,2,3};
        try {
            assertEquals(0,service.stats().threads());
            var first=service.submit(group,input,0,true,owned -> { calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); assertArrayEquals(new byte[]{1,2,3},owned); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var second=service.submit(group,input.clone(),0,true,owned -> { throw new AssertionError("Coalesced action changed"); }); assertTrue(first.cancel(false)); input[0]=9;
            assertEquals(1,service.stats().jobs()); assertEquals(input.length,service.stats().bytes());
            var stopped=service.stop(); assertFalse(stopped.isDone()); assertTrue(stopped.cancel(false)); var actual=service.stop(); assertFalse(actual.isDone());
            assertThrows(ExecutionException.class,() -> service.submit(group,new byte[]{4},0,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
            release.countDown(); actual.get(5,TimeUnit.SECONDS); assertEquals(Message.EMPTY,second.get(5,TimeUnit.SECONDS)); assertEquals(1,calls.get()); assertEquals(0,service.stats().jobs()); assertEquals(0,service.stats().bytes());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void fourThreadsAndSixteenJobsBoundRunningAndQueuedOwnership() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(4); var release=new CountDownLatch(1); var calls=new AtomicInteger(); var views=new ArrayList<CompletableFuture<Message>>();
        try {
            for (int n=0;n<16;n++) views.add(service.submit(group,ByteBuffer.allocate(4).putInt(n).array(),0,true,owned -> { calls.incrementAndGet(); entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; }));
            assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(4,service.stats().threads()); assertEquals(16,service.stats().jobs()); assertEquals(64,service.stats().bytes());
            assertThrows(ExecutionException.class,() -> service.submit(group,new byte[]{99},0,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
            var stopped=service.stop(); assertFalse(stopped.isDone()); assertEquals(4,service.stats().jobs()); assertEquals(16,service.stats().bytes()); assertEquals(4,calls.get());
            release.countDown(); stopped.get(5,TimeUnit.SECONDS); for (int n=0;n<4;n++) assertEquals(Message.EMPTY,views.get(n).get(5,TimeUnit.SECONDS));
            for (int n=4;n<16;n++) { var view=views.get(n); assertThrows(ExecutionException.class,() -> view.get(5,TimeUnit.SECONDS)); }
            assertEquals(0,service.stats().jobs()); assertEquals(0,service.stats().bytes());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void pendingResultAndStopViewsHaveFiniteBudgetsEvenWhenCancelled() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); CompletableFuture<Void> drain=null;
        try {
            byte[] input={3}; var first=service.submit(group,input,0,true,owned -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var views=new ArrayList<CompletableFuture<Message>>(); views.add(first);
            for (int n=1;n<RatisProofQueryService.MAX_WAITERS;n++) views.add(service.submit(group,input,0,true,owned -> { throw new AssertionError(); }));
            assertTrue(views.get(1).cancel(false)); assertThrows(ExecutionException.class,() -> service.submit(group,input,0,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
            drain=service.stop(); var stops=new ArrayList<CompletableFuture<Void>>(); stops.add(drain);
            for (int n=1;n<RatisProofQueryService.MAX_WAITERS;n++) stops.add(service.stop());
            assertTrue(stops.get(1).cancel(false)); assertThrows(ExecutionException.class,() -> service.stop().get(5,TimeUnit.SECONDS));
            release.countDown(); drain.get(5,TimeUnit.SECONDS); assertEquals(Message.EMPTY,first.get(5,TimeUnit.SECONDS)); service.stop().get(5,TimeUnit.SECONDS);
        } finally { release.countDown(); if (drain!=null) drain.get(5,TimeUnit.SECONDS); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void completedNativeActionDropsPayloadBeforeReentrantBlockedPublication() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var releaseAction=new CountDownLatch(1); var published=new CountDownLatch(1); var releaseCallback=new CountDownLatch(1);
        try {
            var view=service.submit(group,new byte[]{1,2,3},0,true,owned -> { entered.countDown(); if (!releaseAction.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); final Object task;
            synchronized (service) { var field=RatisProofQueryService.class.getDeclaredField("tasks"); field.setAccessible(true); task=((Map<?,?>)field.get(service)).values().iterator().next(); }
            var callback=view.thenRun(() -> {
                try { service.stop().get(5,TimeUnit.SECONDS); published.countDown(); if (!releaseCallback.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                catch (Exception failure) { throw new CompletionException(failure); }
            });
            releaseAction.countDown(); assertTrue(published.await(5,TimeUnit.SECONDS)); assertEquals(0,service.stats().jobs()); assertEquals(0,service.stats().bytes());
            for (String name:List.of("input","action")) { var field=task.getClass().getDeclaredField(name); field.setAccessible(true); assertNull(field.get(task)); }
            releaseCallback.countDown(); callback.get(5,TimeUnit.SECONDS);
        } finally { releaseAction.countDown(); releaseCallback.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void noncoalescingCaptureLikeDuplicateRejectsWithoutWaitingOrReplacingOwner() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); byte[] input={8};
        try {
            var first=service.submit(group,input,0,false,owned -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,() -> service.submit(group,input,0,false,owned -> { throw new AssertionError(); }).get(5,TimeUnit.SECONDS));
            assertFalse(first.isDone()); assertEquals(1,service.stats().jobs()); assertEquals(1,service.stats().bytes());
            release.countDown(); first.get(5,TimeUnit.SECONDS); assertEquals(Message.EMPTY,service.submit(group,input,0,false,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void queuedCancellationDrainsEveryJobBeforePublishingReentrantStopCallbacks() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(4); var release=new CountDownLatch(1);
        var field=RatisProofQueryService.class.getDeclaredField("executor"); field.setAccessible(true); var executor=(ThreadPoolExecutor)field.get(service);
        try {
            for (int n=0;n<4;n++) executor.execute(() -> { entered.countDown(); try { release.await(); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); } });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var callbacks=new ArrayList<CompletableFuture<Void>>();
            for (int n=0;n<2;n++) {
                var view=service.submit(group,new byte[]{(byte)n},0,true,owned -> { throw new AssertionError("Queued action must not run"); });
                callbacks.add(view.handle((message,failure) -> { assertNotNull(failure); try { service.stop().get(5,TimeUnit.SECONDS); } catch (Exception bad) { throw new CompletionException(bad); } return null; }));
            }
            service.stop().get(5,TimeUnit.SECONDS); for (var callback:callbacks) callback.get(5,TimeUnit.SECONDS); assertEquals(0,service.stats().jobs()); assertEquals(0,service.stats().bytes());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void actualEncodedBytesReserveBeforeDispatchAndFailuresPermitRetry() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try {
            byte[] maximum=new byte[Math.toIntExact(RatisProofQueryService.MAX_BYTES)];
            var running=service.submit(group,maximum,0,true,owned -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(RatisProofQueryService.MAX_BYTES,service.stats().bytes());
            assertThrows(ExecutionException.class,() -> service.submit(group,new byte[]{1},0,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
            release.countDown(); running.get(5,TimeUnit.SECONDS); assertEquals(0,service.stats().bytes());
            assertThrows(ExecutionException.class,() -> service.submit(group,new byte[]{2},0,true,owned -> { throw new IOException("Unavailable HOME"); }).get(5,TimeUnit.SECONDS));
            assertEquals(Message.EMPTY,service.submit(group,new byte[]{2},0,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS)); assertEquals(0,service.stats().jobs());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }
    @Test void expandedOwnershipIsChargedBeforeDispatchAndSurvivesCancelledViewsAndStop() throws Exception {
        var service=new RatisProofQueryService(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); byte[] input={42};
        try {
            assertThrows(ExecutionException.class,() -> service.submit(group,input,RatisProofQueryService.MAX_BYTES+1,true,owned -> { throw new AssertionError(); }).get(5,TimeUnit.SECONDS));
            var running=service.submit(group,input,RatisProofQueryService.MAX_BYTES,true,owned -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); return Message.EMPTY; });
            assertTrue(entered.await(5,TimeUnit.SECONDS)); var coalesced=service.submit(group,input,RatisProofQueryService.MAX_BYTES,true,owned -> { throw new AssertionError(); });
            assertTrue(running.cancel(false)); assertTrue(coalesced.cancel(false));
            assertEquals(RatisProofQueryService.MAX_BYTES,service.stats().bytes());
            assertThrows(ExecutionException.class,() -> service.submit(group,new byte[]{43},1,true,owned -> Message.EMPTY).get(5,TimeUnit.SECONDS));
            var stop=service.stop(); assertFalse(stop.isDone()); assertEquals(RatisProofQueryService.MAX_BYTES,service.stats().bytes());
            release.countDown(); stop.get(5,TimeUnit.SECONDS); assertEquals(0,service.stats().bytes());
        } finally { release.countDown(); service.stop().get(5,TimeUnit.SECONDS); }
    }

    @Test void topologyImageLaneRetainsFullChargeUntilActualWorkExitsAndOrdinaryControlRemainsIndependent()throws Exception{
        var image=RatisProofQueryService.topologyImageLane();var ordinary=new RatisProofQueryService();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var group=UUID.randomUUID();
        try{assertThrows(ExecutionException.class,()->image.submit(group,new byte[]{1},RatisProofQueryService.TOPOLOGY_IMAGE_BYTES+1,false,owned->{throw new AssertionError();}).get(5,TimeUnit.SECONDS));var running=image.submit(group,new byte[]{2},RatisProofQueryService.TOPOLOGY_IMAGE_BYTES,false,owned->{entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new IOException("Image fixture timeout");return Message.EMPTY;});assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(Message.EMPTY,ordinary.submit(group,new byte[]{3},1,false,owned->Message.EMPTY).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->image.submit(group,new byte[]{4},1,false,owned->Message.EMPTY).get(5,TimeUnit.SECONDS));running.cancel(false);var stop=image.stop();assertFalse(stop.isDone());assertEquals(RatisProofQueryService.TOPOLOGY_IMAGE_BYTES,image.stats().bytes());release.countDown();stop.get(5,TimeUnit.SECONDS);assertEquals(0,image.stats().bytes());assertEquals(0,image.stats().jobs());}finally{release.countDown();image.stop().get(5,TimeUnit.SECONDS);ordinary.stop().get(5,TimeUnit.SECONDS);}
    }

}
