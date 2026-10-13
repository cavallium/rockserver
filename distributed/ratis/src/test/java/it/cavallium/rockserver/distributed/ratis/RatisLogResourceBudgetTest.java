package it.cavallium.rockserver.distributed.ratis;

import org.apache.ratis.server.impl.LogResourceStateMachine;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisLogResourceBudgetTest {
    RatisLogResourceBudget.Limits limits(int owners,long resident,long headers) { return new RatisLogResourceBudget.Limits(owners,10000,4,resident,headers,4,1,4096,1,10000,2,10000,4); }
    @Test void sharedNodeIntakeAcrossTwoPoolsPreservesProtectedHeartbeatAndIndependentRead() throws Exception {
        var l=limits(1,4096,4); var node=new RatisLogResourceBudget.Shared(l);
        var first=new RatisLogResourceBudget(UUID.randomUUID(),node,new RatisLogResourceBudget.Shared(l),l,(bytes,protectedProgress)->()->{});
        var second=new RatisLogResourceBudget(UUID.randomUUID(),node,new RatisLogResourceBudget.Shared(l),l,(bytes,protectedProgress)->()->{});
        try(var intake=first.reserveIntake(100,false)) {
            assertThrows(IOException.class,()->second.reserveIntake(100,false));
            try(var heartbeat=second.reserveIntake(32,true);var read=second.reserveRead(1024);var reply=second.reserveReply(true)) { assertEquals(1,node.snapshot().intakeOwners()); assertEquals(1,node.snapshot().protectedOwners()); assertEquals(1,node.snapshot().readOwners()); }
        }
        assertEquals(0,node.snapshot().intakeOwners()); first.close(); second.close();
    }
    @Test void residentTransfersSurviveActualAppendCompletionAndPayloadEvictionKeepsHeaders() throws Exception {
        var l=limits(2,4096,4); var node=new RatisLogResourceBudget.Shared(l); var pool=new RatisLogResourceBudget.Shared(l); var finish=new AtomicInteger();
        var budget=new RatisLogResourceBudget(UUID.randomUUID(),node,pool,l,(bytes,protectedProgress)->finish::incrementAndGet);
        var append=budget.reserveAppend(new LogResourceStateMachine.AppendFootprint(100,2048,2,1,8192,false));
        var payload=append.transferResident(1024,0,0); var header=append.transferResident(0,1,0); var segment=append.transferResident(0,0,1);
        assertThrows(IllegalStateException.class,append::close); append.actualFinished(); append.close(); assertEquals(1,finish.get());
        assertEquals(1024,budget.snapshot().residentBytes()); assertEquals(1,budget.snapshot().headers()); assertEquals(1,budget.snapshot().segments());
        payload.close(); payload.close(); assertEquals(0,budget.snapshot().residentBytes()); assertEquals(1,budget.snapshot().headers()); assertThrows(IllegalStateException.class,budget::close);
        header.close(); segment.close(); budget.close(); assertEquals(0,node.snapshot().headers());
    }
    @Test void cancellationDoesNotSettleActualAppendOrDiskWriter() throws Exception {
        var l=limits(1,4096,4); var finished=new AtomicInteger(); var budget=new RatisLogResourceBudget(UUID.randomUUID(),new RatisLogResourceBudget.Shared(l),new RatisLogResourceBudget.Shared(l),l,(bytes,progress)->finished::incrementAndGet);
        var lease=budget.reserveAppend(new LogResourceStateMachine.AppendFootprint(100,1024,1,1,8192,false)); var actual=new CompletableFuture<Void>(); actual.whenComplete((value,error)->{ lease.actualFinished(); lease.close(); }); var observer=actual.copy(); observer.cancel(false);
        assertEquals(1,budget.snapshot().intakeOwners()); assertEquals(0,finished.get()); assertThrows(IOException.class,()->budget.reserveIntake(100,false)); actual.complete(null);
        assertEquals(1,finished.get()); assertEquals(0,budget.snapshot().residentBytes()); budget.close();
    }
    @Test void compoundResidentOrDiskFailureRollsBackBeforeAnyAcceptedOwner() throws Exception {
        var l=limits(1,1024,1); var diskCalls=new AtomicInteger(); var budget=new RatisLogResourceBudget(UUID.randomUUID(),new RatisLogResourceBudget.Shared(l),new RatisLogResourceBudget.Shared(l),l,(bytes,progress)->{ diskCalls.incrementAndGet(); throw new IOException("Runway refused"); });
        assertThrows(IOException.class,()->budget.reserveAppend(new LogResourceStateMachine.AppendFootprint(100,2048,1,1,8192,false))); assertEquals(0,diskCalls.get()); assertEquals(0,budget.snapshot().intakeOwners());
        assertThrows(IOException.class,()->budget.reserveAppend(new LogResourceStateMachine.AppendFootprint(100,1024,1,1,8192,false))); assertEquals(1,diskCalls.get()); assertEquals(0,budget.snapshot().residentBytes()); budget.close();
    }
    @Test void defaultReadEnvelopeAdmitsMarkedFrameAndParsedLargestLegalEntry() throws Exception {
        var l=RatisLogResourceBudget.Limits.defaults();
        var budget=new RatisLogResourceBudget(UUID.randomUUID(),new RatisLogResourceBudget.Shared(l),new RatisLogResourceBudget.Shared(l),l,(bytes,progress)->()->{});
        long envelope=org.apache.ratis.server.impl.LogResourceScope.coldReadBytes((64L<<20)+4096);
        try(var read=budget.reserveRead(envelope)) { assertEquals(envelope,budget.snapshot().readBytes());assertThrows(IllegalStateException.class,budget::close); }
        assertEquals(0,budget.snapshot().readBytes());budget.close();
    }
    @Test void coldHeaderAndSegmentInventoryOwnsCapacityUntilActualRemoval() throws Exception {
        var l=limits(1,1024,1); var budget=new RatisLogResourceBudget(UUID.randomUUID(),new RatisLogResourceBudget.Shared(l),new RatisLogResourceBudget.Shared(l),l,(bytes,progress)->()->{});
        var inventory=budget.reserveInventory(0,1,1); var header=inventory.transferResident(0,1,0); var segment=inventory.transferResident(0,0,1); inventory.close();
        assertThrows(IOException.class,()->budget.reserveInventory(0,1,1)); assertEquals(0,budget.snapshot().residentBytes()); assertEquals(1,budget.snapshot().headers()); header.close(); segment.close(); budget.close();
    }
    @Test void defaultBudgetTwoIdleGrpcStreamsPermitHeartbeatAndLargeArrivingFrame() throws Exception {
        var limits=RatisLogResourceBudget.Limits.defaults();var node=new RatisLogResourceBudget.Shared(limits);
        var budget=new RatisLogResourceBudget(UUID.randomUUID(),node,new RatisLogResourceBudget.Shared(limits),limits,(bytes,progress)->()->{});
        var group=org.apache.ratis.protocol.RaftGroupId.randomId();var pending=new java.util.HashMap<Long,CompletableFuture<org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto>>();
        var machine=java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{org.apache.ratis.server.impl.LogResourceStateMachine.class,org.apache.ratis.statemachine.StateMachine.class},(proxy,method,args)->switch(method.getName()){case "logResources"->budget;case "isProtectedLogEntry","areProtectedLogEntries"->false;default->throw new UnsupportedOperationException(method.getName());});
        var division=java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{org.apache.ratis.server.RaftServer.Division.class},(proxy,method,args)->{if(method.getName().equals("getStateMachine"))return machine;throw new UnsupportedOperationException(method.getName());});
        var server=(org.apache.ratis.server.RaftServer)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getProperties"->new org.apache.ratis.conf.RaftProperties();case "getDivision"->division;case "appendEntriesAsync"->{long call=((org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto)args[0]).getServerRequest().getCallId();yield pending.computeIfAbsent(call,key->new CompletableFuture<>());}default->throw new UnsupportedOperationException(method.getName());});
        class Response extends org.apache.ratis.thirdparty.io.grpc.stub.ServerCallStreamObserver<org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto> {
            int demand;Runnable cancel;
            public void disableAutoRequest(){}public void disableAutoInboundFlowControl(){}public void request(int count){demand+=count;}
            public boolean isCancelled(){return false;}public void setOnCancelHandler(Runnable handler){cancel=handler;}
            public boolean isReady(){return true;}public void setOnReadyHandler(Runnable handler){}public void setCompression(String compression){}public void setMessageCompression(boolean enabled){}
            public void onNext(org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto reply){}public void onError(Throwable error){throw new AssertionError(error);}public void onCompleted(){}
        }
        try(var registration=org.apache.ratis.server.impl.LogResourceStateMachine.bindServer(server,budget)){
            var type=Class.forName("org.apache.ratis.grpc.server.GrpcServerProtocolService");var ctor=type.getDeclaredConstructor(java.util.function.Supplier.class,org.apache.ratis.server.RaftServer.class);ctor.setAccessible(true);
            var service=ctor.newInstance((java.util.function.Supplier<org.apache.ratis.protocol.RaftPeerId>)()->org.apache.ratis.protocol.RaftPeerId.valueOf("budget-follower"),server);
            var open=type.getMethod("appendEntries",org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver.class);open.setAccessible(true);
            var deliver=type.getDeclaredMethod("admittedOnNext",org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver.class,org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto.class);deliver.setAccessible(true);
            var first=new Response();var second=new Response();var a=open.invoke(service,first);var b=open.invoke(service,second);
            assertEquals(1,first.demand);assertEquals(1,second.demand);assertEquals(0,node.snapshot().decodeBytes());
            var header=org.apache.ratis.proto.RaftProtos.RaftRpcRequestProto.newBuilder().setCallId(1).setRaftGroupId(org.apache.ratis.util.ProtoUtils.toRaftGroupIdProtoBuilder(group));
            var heartbeat=org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto.newBuilder().setServerRequest(header).build();deliver.invoke(service,b,heartbeat);pending.get(1L).complete(org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.getDefaultInstance());
            var large=org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto.newBuilder().setServerRequest(header.setCallId(2)).addEntries(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(new byte[(64<<20)-1024])))).build();
            deliver.invoke(service,a,large);assertEquals(0,node.snapshot().decodeBytes());assertTrue(node.snapshot().intakeBytes()>64L<<20);
            deliver.invoke(service,b,heartbeat.toBuilder().setServerRequest(header.setCallId(3)).build());pending.get(3L).complete(org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.getDefaultInstance());
            pending.get(2L).complete(org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.getDefaultInstance());first.cancel.run();second.cancel.run();assertEquals(0,node.snapshot().decodeOwners());assertEquals(0,node.snapshot().intakeOwners());
        }budget.close();
    }
    @Test void protectedControlsHaveFiniteIndependentCreditsAndUseProtectedDiskReservation() throws Exception {
        var limits=RatisLogResourceBudget.Limits.defaults();var calls=new AtomicInteger();
        var budget=new RatisLogResourceBudget(UUID.randomUUID(),new RatisLogResourceBudget.Shared(limits),new RatisLogResourceBudget.Shared(limits),limits,(bytes,progress)->{assertTrue(progress);calls.incrementAndGet();return ()->{};});
        try(var ordinary=budget.reserveIntake(70L<<20,false)) {
            var control=budget.reserveAppend(new LogResourceStateMachine.AppendFootprint(64L<<10,512,1,1,8192,true));assertEquals(1,calls.get());assertEquals(1,budget.snapshot().controlOwners());
            try(var heartbeat=budget.reserveIntake(64,true)){assertEquals(1,budget.snapshot().protectedOwners());}
            control.actualFinished();control.close();assertEquals(0,budget.snapshot().controlOwners());
            assertThrows(IOException.class,()->budget.reserveControlIntake(limits.intakeBytes()));
        }budget.close();
    }

    @Test void classificationScratchSharesDecodeCeilingAndDrainsOnlyAfterClassificationReturns() throws Exception {
        var limits=RatisLogResourceBudget.Limits.defaults();var node=new RatisLogResourceBudget.Shared(limits);var pool=new RatisLogResourceBudget.Shared(limits);
        var budget=new RatisLogResourceBudget(UUID.randomUUID(),node,pool,limits,(bytes,progress)->()->{});
        long sourceMaximum=RatisSourceOutcomeRecord.MAX_BYTES;
        long scratch=Math.addExact(65536,Math.multiplyExact(16,sourceMaximum));
        try(var frame=budget.reserveDecode(sourceMaximum)) {
            try(var classification=budget.reserveClassification(scratch)) {
                assertEquals(1,node.snapshot().decodeOwners());assertEquals(1,node.snapshot().classificationOwners());
                assertEquals(2*sourceMaximum+65536+scratch,node.snapshot().decodeBytes());
                assertEquals(scratch,pool.snapshot().decodeBytes());assertThrows(IllegalStateException.class,budget::close);
            }
            assertEquals(0,node.snapshot().classificationOwners());assertEquals(2*sourceMaximum+65536,node.snapshot().decodeBytes());
        }
        budget.close();
        var scratchOwner=new RatisLogResourceBudget(UUID.randomUUID(),node,pool,limits,(bytes,progress)->()->{});
        // Refusal precedes any full-copy work, and does not donate the existing frame owner.
        var other=new RatisLogResourceBudget(UUID.randomUUID(),node,new RatisLogResourceBudget.Shared(limits),limits,(bytes,progress)->()->{});
        try(var frame=other.reserveDecode(64L<<20)) {
            assertThrows(IOException.class,()->scratchOwner.reserveClassification(scratch));
            assertEquals(1,node.snapshot().decodeOwners());assertEquals(0,node.snapshot().classificationOwners());
        }
        assertEquals(0,node.snapshot().decodeBytes());other.close();scratchOwner.close();
    }

}
