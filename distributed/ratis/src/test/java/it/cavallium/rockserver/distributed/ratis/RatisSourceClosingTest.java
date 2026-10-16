package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI + committed adapter records; controlled catalogue/leadership, no terminal deletion grant. */
class RatisSourceClosingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSourceOutcomeTest helper=new RatisSourceOutcomeTest();
    RatisSourceClosingRecord record(RatisSourceOutcomeTest.Fixture f)throws Exception{
        var source=f.home.source;var cfg=source.store.configuration();return new RatisSourceClosingRecord(new NativeSourceClosing.Request(source.definition.header(),source.groupId,source.catalogue,new ApplyPosition(1,10),cfg.term(),cfg.index(),cfg.data(),OperatorUpload.sha256(RatisTopologyRecoveryObligations.reserve(source.definition.header()).encode())));
    }
    NativeSourceClosing close(RatisSourceOutcomeTest.Fixture f,RatisSourceClosingRecord record)throws Exception{
        f.machine.prepareSourceClosing(record);
        var source=f.home.source;var tx=f.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(source.peer.getId()).setGroupId(source.group.getGroupId()).setCallId(701).setType(RaftClientRequest.writeRequestType()).setMessage(RatisSourceOutcomeTest.message(record.encode())).build());
        assertNull(tx.getException());f.machine.preAppendTransaction(tx);tx.initLogEntry(source.leader.get().term(),++f.index);
        return NativeSourceClosing.decode(f.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray());
    }
    @Test void preparedCreditCannotCrossClosingAndCommittedStaleCreditReturnsClosedIdentity()throws Exception{
        try(var f=helper.new Fixture(root,false)){
            var proposal=new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.CREDIT,f.request,List.of());var credit=f.prepare(proposal).record();assertEquals(1,f.machine.sourceOutcomeStats().holds());
            var record=record(f);assertEquals(record,RatisSourceClosingRecord.decode(record.encode()));assertThrows(IllegalArgumentException.class,()->RatisSourceClosingRecord.decode(Arrays.copyOf(record.encode(),record.encode().length-1)));
            var closed=close(f,record);assertEquals(record.request(),closed.request());assertEquals(0,f.machine.sourceOutcomeStats().holds());
            assertThrows(Exception.class,()->f.prepare(proposal));
            byte[] bytes=RatisMetadataOutcomeReplayTest.replay(f.machine,credit.encode(),++f.index);var rejected=RatisSourceOutcomeRpc.applied(bytes,credit);assertEquals(NativeSourceOutcomePublication.Outcome.CLOSED,rejected.outcome());assertNull(rejected.proof());assertArrayEquals(credit.digest(),rejected.recordDigest());assertTrue(f.home.source.store.sourceOutcomeCredits().isEmpty());
            var duplicate=RatisSourceOutcomeRpc.applied(RatisMetadataOutcomeReplayTest.replay(f.machine,credit.encode(),++f.index),credit);assertEquals(rejected.outcome(),duplicate.outcome());assertArrayEquals(rejected.recordDigest(),duplicate.recordDigest());assertNull(duplicate.proof());
            assertThrows(IllegalArgumentException.class,()->RatisSourceOutcomeRpc.applied(RatisSourceOutcomeRpc.applied(credit,NativeSourceOutcomePublication.Outcome.CLOSED,new byte[]{1}),credit));
        }
    }
    @Test void acceptedCreditDrainsThroughExactPublicationAfterCommittedClose()throws Exception{
        try(var f=helper.new Fixture(root,false)){
            var credit=f.prepare(new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.CREDIT,f.request,List.of())).record();var original=f.apply(credit);assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,original.outcome());
            var record=record(f);var closed=close(f,record);assertEquals(1,f.home.source.store.sourceOutcomeCredits().size());
            f.abortHome(TransactionStatus.NoEffectOutcome.CONFLICT);
            var publication=f.prepare(new RatisSourceOutcomeRpc.Proposal(RatisSourceOutcomeRecord.Kind.PUBLISH,f.request,List.of())).record();var published=f.apply(publication);assertEquals(NativeSourceOutcomePublication.Outcome.RETAINED,published.outcome());assertTrue(f.home.source.store.sourceOutcomeCredits().isEmpty());
            assertEquals(record.request(),close(f,record).request());assertEquals(closed.position(),f.home.source.store.sourceClosing().position());
            f.beforeHome=()->{throw new AssertionError("Exact credited replay cannot read new HOME authority");};assertEquals(credit,f.prepare(new RatisSourceOutcomeRpc.Proposal(credit.kind(),f.request,List.of(),credit)).record());assertEquals(original.proof().length,f.apply(credit).proof().length);
            assertArrayEquals(publication.result(),f.home.source.store.operationResult(f.request.original().operationId(),f.request.original().receiptDigest()));
        }
    }
    private static Object field(Object owner,String name)throws Exception{var field=RatisRangeStateMachine.class.getDeclaredField(name);field.setAccessible(true);return field.get(owner);}
    private static int slots(RatisRangeStateMachine machine)throws Exception{return (int)field(machine,"topologySlots");}
    @Test void submittedTruncatedClosingWaitsForNewCompletedNativeTermThenRetriesExactRecipe()throws Exception{
        try(var f=helper.new Fixture(root,false)){
            var record=record(f);var source=f.home.source;f.machine.prepareSourceClosing(record);
            var tx=f.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(source.peer.getId()).setGroupId(source.group.getGroupId()).setCallId(702).setType(RaftClientRequest.writeRequestType()).setMessage(RatisSourceOutcomeTest.message(record.encode())).build());
            assertNull(tx.getException());f.machine.preAppendTransaction(tx);long discardedIndex=f.index+1;tx.initLogEntry(1,discardedIndex);
            f.machine.notifyNotLeader(List.of(tx));source.leader.set(new RatisRangeStateMachine.Leadership(2,true));
            assertNull(source.store.sourceClosingAttempt());assertNotNull(field(f.machine,"sourceClosingHold"));assertEquals(1,slots(f.machine));
            assertThrows(java.io.IOException.class,()->f.machine.prepareSourceClosing(record),"Leadership alone cannot exclude the possibly appended old suffix");
            assertNotNull(field(f.machine,"sourceClosingHold"));assertFalse(source.store.sourceClosingAdmission());
            // Controlled log replacement: a genuine completed new-term NOOP occupies the discarded slot.
            source.store.applyNoop(new ApplyPosition(2,discardedIndex));f.index=discardedIndex;f.machine.notifyTermIndexUpdated(2,f.index);
            assertFalse(source.store.sourceClosingAdmission());var closed=close(f,record);
            assertEquals(record.request(),closed.request());assertEquals(2,closed.position().term());assertEquals(discardedIndex+1,closed.position().index());assertEquals(0,slots(f.machine));
            assertEquals(closed,close(f,record));assertEquals(record.request(),source.store.sourceClosingAttempt().request());assertEquals(0,slots(f.machine));
        }
    }
    @Test void invalidationCannotReleaseClosingOutsideAdmissionsAndProofFinallyRetiresOnce()throws Exception{
        try(var f=helper.new Fixture(root,false)){
            var record=record(f);f.machine.prepareSourceClosing(record);Object admissions=field(f.machine,"admissions");
            var failure=new java.util.concurrent.atomic.AtomicReference<Throwable>();Thread invalidator;
            synchronized(admissions){
                invalidator=Thread.ofPlatform().daemon(true).name("closing-invalidation").start(()->{try{f.machine.notifyNotLeader(List.of());}catch(Throwable error){failure.set(error);}});
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                while(invalidator.getState()!=Thread.State.BLOCKED && invalidator.isAlive() && System.nanoTime()<deadline)Thread.onSpinWait();
                assertEquals(Thread.State.BLOCKED,invalidator.getState(),"Invalidation must join the admissions ownership monitor");assertNotNull(field(f.machine,"sourceClosingHold"));assertEquals(1,slots(f.machine));
            }
            invalidator.join(5000);assertFalse(invalidator.isAlive());assertNull(failure.get());assertNull(field(f.machine,"sourceClosingHold"));assertEquals(0,slots(f.machine));
            var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
            f.beforeClosing=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Closing proof barrier timed out");}catch(InterruptedException error){Thread.currentThread().interrupt();throw new AssertionError(error);}};
            var preparing=java.util.concurrent.CompletableFuture.runAsync(()->{try{f.machine.prepareSourceClosing(record);}catch(Exception expected){throw new java.util.concurrent.CompletionException(expected);}});
            try{
                assertTrue(entered.await(5,TimeUnit.SECONDS));f.machine.notifyNotLeader(List.of());f.machine.notifyNotLeader(List.of());
                synchronized(admissions){assertNotNull(field(f.machine,"sourceClosingHold"));assertEquals(1,slots(f.machine),"Invalidated reading owner keeps its graph through actual proof return");}
            }finally{release.countDown();}
            assertThrows(java.util.concurrent.ExecutionException.class,()->preparing.get(5,TimeUnit.SECONDS));assertNull(field(f.machine,"sourceClosingHold"));assertEquals(0,slots(f.machine));
            f.beforeClosing=()->{};assertEquals(record.request(),close(f,record).request());assertEquals(0,slots(f.machine));
        }
    }

}
