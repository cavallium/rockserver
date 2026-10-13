package org.apache.ratis.server.impl;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.proto.RaftProtos.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CommittedApplyAdmissionTest {
  static final class Permit implements CommittedApplyStateMachine.ApplyPermit {
    boolean claimed,closed;
    public void claim(){claimed=true;} public boolean claimed(){return claimed;}public void close(){closed=true;}
  }
  static class Admission implements CommittedApplyStateMachine.Admission {
    final CountDownLatch allowRead=new CountDownLatch(1),allowDispatch=new CountDownLatch(1),waiting=new CountDownLatch(1);
    final Permit permit=new Permit();boolean readClosed,offered,immediate;
    public CommittedApplyStateMachine.ReadPermit awaitRead(long index)throws InterruptedException{assertTrue(allowRead.await(5,TimeUnit.SECONDS));return new CommittedApplyStateMachine.ReadPermit(){public long generation(){return 0;}public void close(){readClosed=true;}};}
    public CommittedApplyStateMachine.Descriptor describe(LogEntryProto entry){var digest=new byte[32];digest[0]=(byte)entry.getTerm();return new CommittedApplyStateMachine.Descriptor(entry.getTerm(),entry.getIndex(),entry.getLogEntryBodyCase().getNumber(),CommittedApplyStateMachine.Lane.ORDINARY,100,digest,0);}
    public CommittedApplyStateMachine.ApplyPermit tryDispatch(CommittedApplyStateMachine.Descriptor d)throws IOException{return immediate?permit:null;}
    public CommittedApplyStateMachine.ApplyPermit awaitDispatch(CommittedApplyStateMachine.Descriptor d)throws InterruptedException {assertTrue(readClosed);waiting.countDown();assertTrue(allowDispatch.await(5,TimeUnit.SECONDS));return permit;}
    public void validate(CommittedApplyStateMachine.Descriptor d){}
    public void offer(CommittedApplyStateMachine.Descriptor d,CommittedApplyStateMachine.ApplyPermit p){offered=true;}
    public void withdraw(CommittedApplyStateMachine.Descriptor d,CommittedApplyStateMachine.ApplyPermit p){}
    public void bindWakeup(Runnable wakeup){}public void pump(){}public void wakeup(){}public void stop(){}public void validateSegmentBytes(long bytes){}
  }
  static LogEntryProto entry(long term){return LogEntryProto.newBuilder().setTerm(term).setIndex(1).setMetadataEntry(MetadataProto.newBuilder()).build();}
  @Test void firstMaterializationWaitsForReadCreditAndExecutionWaitRereadsExactHead()throws Exception {
    var a=new Admission();var reads=new AtomicInteger();
    try(var worker=Executors.newVirtualThreadPerTaskExecutor()){
      var started=new CountDownLatch(1);var result=worker.submit(()->{started.countDown();return StateMachineUpdater.readHead(a,index->{reads.incrementAndGet();return entry(1);},1);});assertTrue(started.await(5,TimeUnit.SECONDS));assertEquals(0,reads.get());
      a.allowRead.countDown();assertTrue(a.waiting.await(5,TimeUnit.SECONDS));assertEquals(1,reads.get());assertTrue(a.readClosed);assertFalse(a.offered);a.allowDispatch.countDown();
      var head=result.get(5,TimeUnit.SECONDS);assertEquals(2,reads.get());assertTrue(a.offered);assertSame(a.permit,head.permit());head.permit().close();
    }
  }
  @Test void replacedHeadReturnsExecutionCreditBeforeContextHandoff()throws Exception {
    var a=new Admission();a.allowRead.countDown();a.allowDispatch.countDown();var reads=new AtomicInteger();var head=StateMachineUpdater.readHead(a,index->entry(reads.incrementAndGet()),1);
    assertNull(head.entry());assertFalse(a.offered);assertTrue(a.permit.closed);assertEquals(2,reads.get());
  }
  @Test void synchronousAdmittedHeadRetainsFastSingleRead()throws Exception {
    var a=new Admission();a.immediate=true;a.allowRead.countDown();var reads=new AtomicInteger();var head=StateMachineUpdater.readHead(a,index->{reads.incrementAndGet();return entry(1);},1);
    assertEquals(1,reads.get());assertTrue(a.offered);assertFalse(a.permit.closed);head.permit().claim();assertTrue(head.permit().claimed());head.permit().close();
  }
  @Test void missingHeadNeverRequestsExecution()throws Exception {
    var a=new Admission();a.allowRead.countDown();var head=StateMachineUpdater.readHead(a,index->null,1);assertNull(head.entry());assertTrue(a.readClosed);assertFalse(a.offered);assertFalse(a.permit.closed);
  }
  @Test void snapshotTriggerDuringDelayedReadinessDefersAndKeepsPumping()throws Exception {
    var done=new CompletableFuture<Void>();var ready=new java.util.concurrent.atomic.AtomicBoolean();var actions=new AtomicInteger();var admission=new Admission(){@Override public void pump(){actions.incrementAndGet();if(ready.get())done.complete(null);}};
    assertFalse(StateMachineUpdater.applicationsReady(done,admission));assertFalse(done.isDone());ready.set(true);assertTrue(StateMachineUpdater.applicationsReady(done,admission));assertEquals(2,actions.get());
  }
  @Test void directCommittedStopSettlesTailObserverWithoutWaitingOrDonatingNativeOwnership()throws Exception {
    var tail=new CompletableFuture<Void>();var nativeOwner=new Permit();nativeOwner.claim();var closes=new AtomicInteger();var machine=new org.apache.ratis.statemachine.impl.BaseStateMachine(){@Override public void close(){closes.incrementAndGet();tail.completeExceptionally(new IOException("Stopped"));}};
    StateMachineUpdater.closeCommittedMachine(machine,new Admission());assertTrue(tail.isCompletedExceptionally());assertEquals(1,closes.get());assertFalse(nativeOwner.closed);nativeOwner.close();assertTrue(nativeOwner.closed);
  }
  @Test void pauseDuringMaterializationRejectsOldHandoffThenReplaysBelowSnapshotCut()throws Exception {
    var generation=new AtomicInteger();var admission=new Admission(){
      @Override public CommittedApplyStateMachine.Descriptor describe(LogEntryProto e){var d=super.describe(e);return new CommittedApplyStateMachine.Descriptor(d.term(),d.index(),d.bodyCase(),d.lane(),d.bytes(),d.digest(),0);}
      @Override public CommittedApplyStateMachine.ApplyPermit tryDispatch(CommittedApplyStateMachine.Descriptor d)throws IOException {if(d.generation()!=generation.get())throw new CommittedApplyStateMachine.Retry();return permit;}
    };
    admission.allowRead.countDown();assertThrows(CommittedApplyStateMachine.Retry.class,()->StateMachineUpdater.readHead(admission,index->{generation.incrementAndGet();return entry(1);},1));assertFalse(admission.offered);assertFalse(admission.permit.claimed);
    // Snapshot cut below this head leaves the same durable entry eligible for its original replay.
    generation.set(2);var fresh=new Admission(){@Override public CommittedApplyStateMachine.ReadPermit awaitRead(long index){return new CommittedApplyStateMachine.ReadPermit(){public long generation(){return 2;}public void close(){readClosed=true;}};}};fresh.immediate=true;var replay=StateMachineUpdater.readHead(fresh,index->entry(1),1);assertEquals(1,replay.entry().getIndex());assertEquals(2,replay.descriptor().generation());assertTrue(fresh.offered);replay.permit().close();
  }

}
