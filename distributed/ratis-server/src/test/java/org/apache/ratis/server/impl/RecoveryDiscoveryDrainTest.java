package org.apache.ratis.server.impl;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongConsumer;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real socket-free selection/apply futures; cancellation never substitutes for actual callback return. */
class RecoveryDiscoveryDrainTest {
  @TempDir Path root;
  static class Machine extends BaseStateMachine implements RecoveryEligibilityStateMachine {
    final Gate gate=new Gate();final SimpleStateMachineStorage storage=new SimpleStateMachineStorage();
    final CountDownLatch entered=new CountDownLatch(1),returnObserver=new CountDownLatch(1);
    final CompletableFuture<Message> nativeFinished=new CompletableFuture<>();final List<Long> selected=new CopyOnWriteArrayList<>();
    Machine(){gate.publish(Evidence.normal());}
    public Gate recoveryEligibilityGate(){return gate;}
    public StateMachineStorage getStateMachineStorage(){return storage;}
    public void initialize(RaftServer server,RaftGroupId group,RaftStorage raftStorage)throws IOException {
      super.initialize(server,group,raftStorage);storage.init(raftStorage);getLifeCycle().transition(LifeCycle.State.STARTING);getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
    public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
      long index=transaction.getLogEntry().getIndex();selected.add(index);entered.countDown();
      try{if(!returnObserver.await(5,TimeUnit.SECONDS))throw new IOException("Selected callback was not released");}catch(Exception failure){return CompletableFuture.failedFuture(failure);}
      return nativeFinished.thenApply(result->{updateLastAppliedTermIndex(transaction.getLogEntry().getTerm(),index);gate.publish(Evidence.normal());return result;});
    }
  }
  static final class PumpedMachine extends Machine implements CommittedApplyStateMachine {
    final AtomicBoolean ready=new AtomicBoolean();final AtomicInteger pumps=new AtomicInteger(),nativeOwners=new AtomicInteger();volatile ApplyPermit selectedPermit;
    final Admission admission=new Admission() {
      public ReadPermit awaitRead(long index){return new ReadPermit(){public long generation(){return 0;}public void close(){}};}
      public Descriptor describe(LogEntryProto entry){return new Descriptor(entry.getTerm(),entry.getIndex(),entry.getLogEntryBodyCase().getNumber(),Lane.ORDINARY,1,new byte[32],0);}
      public ApplyPermit tryDispatch(Descriptor descriptor){return new ApplyPermit(){boolean claimed,closed;public void claim(){claimed=true;nativeOwners.incrementAndGet();}public boolean claimed(){return claimed;}public void close(){if(!closed){closed=true;if(claimed)nativeOwners.decrementAndGet();}}};}
      public ApplyPermit awaitDispatch(Descriptor descriptor){return tryDispatch(descriptor);}
      public void validate(Descriptor descriptor){}public void offer(Descriptor descriptor,ApplyPermit permit){selectedPermit=permit;}public void withdraw(Descriptor descriptor,ApplyPermit permit){}
      public void bindWakeup(Runnable wakeup){}public void pump(){pumps.incrementAndGet();if(ready.get()){if(selectedPermit!=null)selectedPermit.close();nativeFinished.complete(Message.EMPTY);}}
      public void wakeup(){}public void stop(){}public void validateSegmentBytes(long bytes){}
    };
    public Admission committedApplyAdmission(){return admission;}
    public CompletableFuture<Message> applyTransaction(TransactionContext transaction){try{selectedPermit.claim();}catch(IOException failure){return CompletableFuture.failedFuture(failure);}return super.applyTransaction(transaction);}
  }
  RecoveryEligibilityTest.Fixture open(String name,Machine machine)throws Exception{return new RecoveryEligibilityTest().new Fixture(root.resolve(name),machine);}
  static void append(RecoveryEligibilityTest.Fixture fixture)throws Exception {
    var entries=new ArrayList<LogEntryProto>();for(int i=0;i<2;i++)entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(i).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setClientId(ClientId.randomId().toByteString()).setCallId(i).setLogData(ByteString.copyFromUtf8("selected-"+i))).build());
    var request=ServerProtoUtils.toAppendEntriesRequestProto(RaftGroupMemberId.valueOf(RecoveryEligibilityTest.LEADER,RecoveryEligibilityTest.GROUP),RecoveryEligibilityTest.SELF,1,entries,1,false,null,List.of(),31,null);
    assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,fixture.server.appendEntries(request).getResult());
  }
  @Test void currentHoldJoinsSelectedFutureAndAppliedCallbackWithOrWithoutNativePump()throws Exception {
    for(boolean pump:List.of(false,true)) {
      var machine=pump?new PumpedMachine():new Machine();
      try(var fixture=open("actual-"+pump,machine);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
        var updater=fixture.server.getState().getStateMachineUpdater();var field=StateMachineUpdater.class.getDeclaredField("appliedIndexConsumer");field.setAccessible(true);var prior=(LongConsumer)field.get(updater);
        var callbackEntered=new CountDownLatch(1);var callbackReturn=new CountDownLatch(1);
        field.set(updater,(LongConsumer)index->{prior.accept(index);if(index==0){callbackEntered.countDown();try{if(!callbackReturn.await(5,TimeUnit.SECONDS))throw new AssertionError("Applied callback was not released");}catch(InterruptedException failure){Thread.currentThread().interrupt();throw new AssertionError(failure);}}});
        try {
          append(fixture);assertTrue(machine.entered.await(5,TimeUnit.SECONDS));var hold=RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.CURRENT);machine.returnObserver.countDown();
          machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());assertFalse(hold.drained().isDone());assertThrows(IOException.class,hold::requireHeld);var canceled=hold.drained();assertTrue(canceled.cancel(false));assertFalse(hold.drained().isDone());if(machine instanceof PumpedMachine pumped)assertEquals(1,pumped.nativeOwners.get());
          CompletableFuture<?> completing;if(machine instanceof PumpedMachine pumped){pumped.ready.set(true);updater.notifyUpdater();completing=CompletableFuture.completedFuture(null);}else completing=CompletableFuture.runAsync(()->machine.nativeFinished.complete(Message.EMPTY),workers);
          assertTrue(callbackEntered.await(5,TimeUnit.SECONDS));assertFalse(hold.drained().isDone(),"Selected future completion does not retire its applied callback");assertEquals(List.of(0L),machine.selected);
          callbackReturn.countDown();completing.get(5,TimeUnit.SECONDS);hold.drained().get(5,TimeUnit.SECONDS);hold.requireHeld();assertEquals(machine.gate.revision(),hold.revision());if(machine instanceof PumpedMachine pumped){assertTrue(pumped.pumps.get()>0);assertEquals(0,pumped.nativeOwners.get());}
          assertTrue(canceled.isCancelled());assertEquals(List.of(0L),machine.selected);
          machine.gate.deferDiscovery();hold.release().get(5,TimeUnit.SECONDS);assertTrue(machine.gate.discoveryPending());assertEquals(List.of(0L),machine.selected);
        }finally{machine.returnObserver.countDown();callbackReturn.countDown();if(machine instanceof PumpedMachine pumped){pumped.ready.set(true);updater.notifyUpdater();}machine.nativeFinished.complete(Message.EMPTY);field.set(updater,prior);}
      }
    }
  }
  @Test void currentAbortAndStopWaitForSelectedApplicationWithoutDispatchingNext()throws Exception {
    for(boolean stop:List.of(false,true)) {
      var machine=new Machine();try(var fixture=open("retire-"+stop,machine);var workers=Executors.newVirtualThreadPerTaskExecutor()) {
        try {
          append(fixture);assertTrue(machine.entered.await(5,TimeUnit.SECONDS));var hold=RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.CURRENT);machine.returnObserver.countDown();
          var retired=hold.abort();Future<?> closing=stop?workers.submit(fixture.server::close):null;
          assertThrows(TimeoutException.class,()->retired.get(100,TimeUnit.MILLISECONDS));assertFalse(hold.drained().isDone());assertEquals(List.of(0L),machine.selected);
          if(stop){long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!fixture.server.getState().getStateMachineUpdater().isStopRequested() && System.nanoTime()<deadline)Thread.sleep(1);assertTrue(fixture.server.getState().getStateMachineUpdater().isStopRequested());}
          machine.nativeFinished.complete(Message.EMPTY);if(stop){assertThrows(ExecutionException.class,()->retired.get(5,TimeUnit.SECONDS));closing.get(5,TimeUnit.SECONDS);}else retired.get(5,TimeUnit.SECONDS);
          assertTrue(machine.gate.discoveryPending());assertThrows(ExecutionException.class,()->hold.drained().get(5,TimeUnit.SECONDS));assertEquals(List.of(0L),machine.selected);
        }finally{machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);}
      }
    }
  }
  @Test void selectedApplicationFailureNeverAcknowledgesDiscoveryOrDispatchesAnotherEntry()throws Exception {
    var machine=new Machine();try(var fixture=open("failed-application",machine)) {
      try {
        append(fixture);assertTrue(machine.entered.await(5,TimeUnit.SECONDS));var hold=RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.CURRENT);machine.returnObserver.countDown();assertFalse(hold.drained().isDone());
        machine.nativeFinished.completeExceptionally(new IOException("Actual selected application failed"));assertThrows(ExecutionException.class,()->hold.drained().get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->hold.abort().get(5,TimeUnit.SECONDS));assertTrue(machine.gate.discoveryPending());assertEquals(List.of(0L),machine.selected);
      }finally{machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);}
    }
  }
  @Test void missingAndAlreadyPendingCurrentOwnersNeedNoNativeReadinessButRevocationStillFailsClosed()throws Exception {
    for(boolean pending:List.of(false,true)) {
      var machine=new Machine();try(var fixture=open("missing-"+pending,machine)) {
        if(pending)machine.gate.deferDiscovery();else machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());
        var hold=RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.CURRENT);hold.drained().get(5,TimeUnit.SECONDS);hold.requireHeld();assertEquals(-1,machine.getLastAppliedTermIndex().getIndex());hold.abort().get(5,TimeUnit.SECONDS);assertTrue(machine.gate.discoveryPending());
      }
    }
    var machine=new Machine();try(var fixture=open("revoked",machine)) {
      try {
        append(fixture);assertTrue(machine.entered.await(5,TimeUnit.SECONDS));var hold=RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.CURRENT);
        machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);
        assertThrows(ExecutionException.class,()->hold.drained().get(5,TimeUnit.SECONDS));assertTrue(machine.gate.discoveryPending());assertEquals(List.of(0L),machine.selected);
      }finally{machine.returnObserver.countDown();machine.nativeFinished.complete(Message.EMPTY);}
    }
  }
}
