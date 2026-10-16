package org.apache.ratis.server.impl;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.LongConsumer;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real no-socket division/protocol/updater close; this grants no native or CAT deletion authority. */
class RecoverySourceRetirementTest {
    @TempDir Path root;
    static class Machine extends org.apache.ratis.statemachine.impl.BaseStateMachine implements RecoveryEligibilityStateMachine, RockserverRaftServers.SourceRetirementStateMachine {
        final Gate gate=new Gate();final org.apache.ratis.statemachine.impl.SimpleStateMachineStorage storage=new org.apache.ratis.statemachine.impl.SimpleStateMachineStorage();
        final CountDownLatch entered=new CountDownLatch(1),returnObserver=new CountDownLatch(1);final CompletableFuture<Message> nativeFinished=new CompletableFuture<>();
        final List<Long> selected=new CopyOnWriteArrayList<>();CompletableFuture<Message> prior=CompletableFuture.completedFuture(Message.EMPTY);boolean failClose;CompletableFuture<Void> actualDrain=CompletableFuture.completedFuture(null);
        Machine(){gate.publish(Evidence.normal());}
        public CompletableFuture<Void> drainSourceRetirement(){return actualDrain;}
        public Gate recoveryEligibilityGate(){return gate;}
        public org.apache.ratis.statemachine.StateMachineStorage getStateMachineStorage(){return storage;}
        public void initialize(org.apache.ratis.server.RaftServer server,RaftGroupId group,org.apache.ratis.server.storage.RaftStorage raftStorage)throws IOException{super.initialize(server,group,raftStorage);storage.init(raftStorage);getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.STARTING);getLifeCycle().transition(org.apache.ratis.util.LifeCycle.State.RUNNING);}
        public synchronized CompletableFuture<Message> applyTransaction(org.apache.ratis.statemachine.TransactionContext transaction){
            long index=transaction.getLogEntry().getIndex();selected.add(index);entered.countDown();
            try{if(!returnObserver.await(5,TimeUnit.SECONDS))throw new IOException("Selected source callback was not released");}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
            prior=prior.thenCompose(ignored->nativeFinished).thenApply(result->{updateLastAppliedTermIndex(transaction.getLogEntry().getTerm(),index);return result;});return prior;
        }
        public void close()throws IOException{if(failClose)throw new IOException("Actual source SM close failed");super.close();}
    }
    static final class QueryMachine extends Machine {
        final CountDownLatch queryEntered=new CountDownLatch(1);
        final CompletableFuture<Message> actualQuery=new CompletableFuture<>();
        @Override public CompletableFuture<Message> query(Message request){queryEntered.countDown();return actualQuery;}
    }
    RecoveryEligibilityTest.Fixture open(String name,Machine machine)throws Exception{return new RecoveryEligibilityTest().new Fixture(root.resolve(name),machine);}
    static RaftClientRequest read(){return RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RecoveryEligibilityTest.SELF).setGroupId(RecoveryEligibilityTest.GROUP).setCallId(91).setType(RaftClientRequest.staleReadRequestType(0)).setMessage(Message.valueOf("owned source query")).build();}
    static AppendEntriesRequestProto next(long index){var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setClientId(ClientId.randomId().toByteString()).setCallId(index).setLogData(ByteString.copyFromUtf8("retained-"+index))).build();return ServerProtoUtils.toAppendEntriesRequestProto(RaftGroupMemberId.valueOf(RecoveryEligibilityTest.LEADER,RecoveryEligibilityTest.GROUP),RecoveryEligibilityTest.SELF,1,List.of(entry),index,false,TermIndex.valueOf(1,index-1),List.of(),40+index,null);}
    static void append(RecoveryEligibilityTest.Fixture fixture)throws Exception{
        var configuration=RaftConfigurationProto.newBuilder().addAllPeers(RecoveryEligibilityTest.PEERS.stream().map(RaftPeer::getRaftPeerProto).toList()).build();
        var entries=List.of(LogEntryProto.newBuilder().setTerm(1).setIndex(0).setConfigurationEntry(configuration).build(),
                LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setClientId(ClientId.randomId().toByteString()).setCallId(1).setLogData(ByteString.copyFromUtf8("selected-1"))).build());
        var request=ServerProtoUtils.toAppendEntriesRequestProto(RaftGroupMemberId.valueOf(RecoveryEligibilityTest.LEADER,RecoveryEligibilityTest.GROUP),RecoveryEligibilityTest.SELF,1,entries,1,false,null,List.of(),31,null);
        assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,fixture.server.appendEntries(request).getResult());
        assertEquals(0,fixture.server.getRaftConf().getLogEntryIndex(),"The terminal proof binds a genuinely appended full CFG");
    }
    static void applied(RaftServerImpl server,long index)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(server.getStateMachine().getLastAppliedTermIndex().getIndex()<index && System.nanoTime()<end)Thread.sleep(1);assertEquals(index,server.getStateMachine().getLastAppliedTermIndex().getIndex());}
    @Test void agreedSealJoinsSelectedNativeFutureAndAppliedCallbackBeforeCheckedActualClose()throws Exception{
        var machine=new Machine();
        try(var fixture=open("selected",machine);var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var updater=fixture.server.getState().getStateMachineUpdater();var field=StateMachineUpdater.class.getDeclaredField("appliedIndexConsumer");field.setAccessible(true);var original=(LongConsumer)field.get(updater);
            var callbackEntered=new CountDownLatch(1);var callbackReturn=new CountDownLatch(1);field.set(updater,(LongConsumer)index->{original.accept(index);if(index==1){callbackEntered.countDown();try{if(!callbackReturn.await(5,TimeUnit.SECONDS))throw new AssertionError("Source applied callback timed out");}catch(InterruptedException e){Thread.currentThread().interrupt();throw new AssertionError(e);}}});
            try{
                append(fixture);assertTrue(machine.entered.await(5,TimeUnit.SECONDS));
                var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);var expected=owner.observe();var oldExport=machine.gate.snapshotPermit();var sealed=owner.sealAt(expected);
                assertFalse(sealed.isDone());assertThrows(IOException.class,owner::requireHeld);assertThrows(IOException.class,()->fixture.server.appendEntries(next(2)));assertFalse(fixture.vote(false).getServerReply().getSuccess());assertThrows(IllegalStateException.class,oldExport::requireCurrent);
                var cancelled=owner.drained();assertTrue(cancelled.cancel(false));assertFalse(owner.drained().isDone());machine.returnObserver.countDown();
                var completed=workers.submit(()->machine.nativeFinished.complete(Message.EMPTY));assertTrue(callbackEntered.await(5,TimeUnit.SECONDS));assertFalse(owner.drained().isDone(),"Native-future completion is not applied-callback retirement");
                callbackReturn.countDown();completed.get(5,TimeUnit.SECONDS);assertEquals(expected,sealed.get(5,TimeUnit.SECONDS));owner.requireHeld();assertEquals(org.apache.ratis.util.LifeCycle.State.RUNNING,fixture.server.getInfo().getLifeCycleState(),"A sealed ingress owner does not pretend the live SM was paused");assertEquals(1,updater.getLastAppliedIndex());assertEquals(List.of(1L),machine.selected);
                owner.issuingTerminalReceipt(expected);assertThrows(IOException.class,owner::resumeCatchup);owner.closeChecked();assertTrue(owner.closed());assertTrue(Files.exists(root.resolve("selected")),"Checked close does not claim physical deletion");
            }finally{machine.returnObserver.countDown();callbackReturn.countDown();machine.nativeFinished.complete(Message.EMPTY);field.set(updater,original);}
        }
    }
    @Test void preReceiptCatchupKeepsReplicationUsableAndCanReagreeWithoutReusingOldExport()throws Exception{
        var machine=new Machine();machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);
        try(var fixture=open("catchup",machine)){
            append(fixture);applied(fixture.server,1);var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);var old=owner.observe();var export=machine.gate.snapshotPermit();
            assertEquals(old,owner.sealAt(old).get(5,TimeUnit.SECONDS));owner.resumeCatchup().get(5,TimeUnit.SECONDS);assertFalse(machine.gate.sourceRetirementPending());assertThrows(IllegalStateException.class,export::requireCurrent);
            assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,fixture.server.appendEntries(next(2)).getResult());applied(fixture.server,2);var current=owner.observe();assertEquals(2,current.cut().getIndex());
            assertEquals(current,owner.sealAt(current).get(5,TimeUnit.SECONDS));assertThrows(IOException.class,()->owner.issuingTerminalReceipt(old));owner.issuingTerminalReceipt(current);assertThrows(IOException.class,owner::resumeCatchup);owner.closeChecked();
        }
    }
    @Test void cancelledReadObserverKeepsActualAdmittedQueryUntilReturnBeforeSeal()throws Exception{
        var machine=new QueryMachine();machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);
        try(var fixture=open("query",machine)){
            try{
                append(fixture);applied(fixture.server,1);
                var request=read();var query=fixture.server.submitClientRequestAsync(request);assertTrue(machine.queryEntered.await(5,TimeUnit.SECONDS));assertTrue(query.cancel(false));
                var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);assertThrows(IOException.class,owner::observe);assertThrows(IOException.class,()->fixture.server.submitClientRequestAsync(request));assertFalse(owner.drained().isDone());
                machine.actualQuery.complete(Message.EMPTY);long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!fixture.server.sourceProtocolsDrained(owner) && System.nanoTime()<end)Thread.sleep(1);assertTrue(fixture.server.sourceProtocolsDrained(owner));
                var frontier=owner.observe();assertEquals(frontier,owner.sealAt(frontier).get(5,TimeUnit.SECONDS));owner.issuingTerminalReceipt(frontier);owner.closeChecked();assertTrue(query.isCancelled());
            }finally{machine.actualQuery.complete(Message.EMPTY);}
        }
    }
    @Test void timedOutProtocolIsInsufficientUntilAdapterActualNativeWorkRetires()throws Exception{
        var machine=new Machine();machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);machine.actualDrain=new CompletableFuture<>();
        try(var fixture=open("native-drain",machine)){
            try{
                append(fixture);applied(fixture.server,1);
                var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);var frontier=owner.observe();var observed=owner.sealAt(frontier);
                assertThrows(TimeoutException.class,()->observed.get(100,TimeUnit.MILLISECONDS));
                assertThrows(IOException.class,owner::requireHeld);assertThrows(IOException.class,()->owner.issuingTerminalReceipt(frontier));
                assertTrue(observed.cancel(false));assertFalse(owner.drained().isDone());
                machine.actualDrain.complete(null);assertEquals(frontier,owner.drained().get(5,TimeUnit.SECONDS));owner.issuingTerminalReceipt(frontier);owner.closeChecked();
            }finally{machine.actualDrain.complete(null);}
        }
    }
    @Test void cancelledResumeObservationCannotResetReceiptOrDiscardActualDrain()throws Exception{
        var machine=new Machine();machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);machine.actualDrain=new CompletableFuture<>();
        try(var fixture=open("resume-drain",machine)){
            try{
                append(fixture);applied(fixture.server,1);var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);var frontier=owner.observe();owner.sealAt(frontier);
                long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(owner.nativeDrained==null && System.nanoTime()<end)Thread.sleep(1);assertNotNull(owner.nativeDrained);
                var resumed=owner.resumeCatchup();assertTrue(resumed.cancel(false));assertTrue(machine.gate.sourceRetirementPending());assertThrows(IOException.class,()->owner.issuingTerminalReceipt(frontier));
                machine.actualDrain.complete(null);end=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(machine.gate.sourceRetirementPending() && System.nanoTime()<end)Thread.sleep(1);assertFalse(machine.gate.sourceRetirementPending());
                var next=owner.observe();assertEquals(next,owner.sealAt(next).get(5,TimeUnit.SECONDS));owner.issuingTerminalReceipt(next);assertThrows(IOException.class,owner::resumeCatchup);owner.closeChecked();
            }finally{machine.actualDrain.complete(null);}
        }
    }
    @Test void actualStateMachineCloseFailureNeverAcknowledgesTerminalCoreClosure()throws Exception{
        var machine=new Machine();machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);
        try(var fixture=open("failed-close",machine)){
            append(fixture);applied(fixture.server,1);var owner=RockserverRaftServers.beginSourceRetirement(fixture.server,machine);var frontier=owner.observe();owner.sealAt(frontier).get(5,TimeUnit.SECONDS);owner.issuingTerminalReceipt(frontier);machine.failClose=true;
            assertThrows(IOException.class,owner::closeChecked);assertFalse(owner.closed());assertThrows(IOException.class,owner::resumeCatchup);assertTrue(Files.exists(root.resolve("failed-close")));
        }
    }

}
