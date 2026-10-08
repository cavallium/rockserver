package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled admission/append contexts and native effects, without server processes or quorum claims. */
class RatisAcceptedTransactionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception {
        var f=new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults());
        f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); return f;
    }
    TransactionScope scope(RatisRetentionAdmissionTest.Fixture f,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(f.column,new byte[]{(byte)key}))); }
    DistributedCommand put(RatisRetentionAdmissionTest.Fixture f,long sequence,int key,Mutation.ReturnMode mode) {
        return new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,sequence),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)key},new byte[]{7},mode)));
    }
    @Test void frozenPlanTransfersExclusiveOwnershipAndReplaysItsSavedReturn() throws Exception {
        try (var f=fixture("commit"); var pin=f.store.captureTracked(scope(f,0))) {
            var disjoint=f.propose(put(f,2,2,Mutation.ReturnMode.NONE));
            try (var prepared=f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.PREVIOUS))) {
                var command=prepared.command(); assertEquals(DistributedCommand.Kind.COMMIT_ACCEPTED,command.kind()); assertEquals(0,f.store.activeTrackedSnapshotPins());
                assertNotNull(f.propose(put(f,0,9,Mutation.ReturnMode.NONE)).getException(),"Prepared identity also excludes disjoint payload reuse");
                assertNull(CommandResult.decode(command.acceptance().result()).value(0)); assertFalse(prepared.submitted());
                assertNotNull(f.propose(put(f,3,0,Mutation.ReturnMode.NONE)).getException());
                var transaction=f.propose(command); assertNull(transaction.getException()); assertTrue(prepared.submitted()); prepared.close();
                assertNotNull(f.propose(put(f,3,0,Mutation.ReturnMode.NONE)).getException());
                assertEquals(CommandResult.Status.SUCCESS,f.apply(disjoint,1)); assertEquals(CommandResult.Status.SUCCESS,f.apply(transaction,2));
                assertEquals(0,f.machine.proposalFootprintStats().candidates());
                var later=f.propose(put(f,4,0,Mutation.ReturnMode.NONE)); assertEquals(CommandResult.Status.SUCCESS,f.apply(later,3));
                var retry=f.propose(command); assertNull(retry.getException()); f.machine.preAppendTransaction(retry); retry.initLogEntry(1,4);
                var result=CommandResult.decode(f.machine.applyTransaction(retry).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                assertNull(result.value(0)); assertArrayEquals(command.acceptance().result(),result.encode()); f.assertReleased();
            }
        }
    }
    @Test void overlappingPredecessorRefusesButDisjointTransactionsRemainPipelined() throws Exception {
        try (var f=fixture("parallel"); var a=f.store.captureTracked(scope(f,0)); var b=f.store.captureTracked(scope(f,1))) {
            var old=f.propose(put(f,10,0,Mutation.ReturnMode.NONE));
            assertThrows(IllegalStateException.class,() -> f.machine.prepareScopedCommit(a,put(f,0,0,Mutation.ReturnMode.NONE))); f.machine.cancelTransaction(old);
            try (var first=f.machine.prepareScopedCommit(a,put(f,0,0,Mutation.ReturnMode.NONE)); var second=f.machine.prepareScopedCommit(b,put(f,1,1,Mutation.ReturnMode.NONE))) {
                var x=f.propose(first.command()); var y=f.propose(second.command()); assertNull(x.getException()); assertNull(y.getException());
                assertEquals(2,f.machine.proposalFootprintStats().candidates()); assertNotNull(f.propose(f.ack(20,0)).getException());
                var unrelated=f.propose(put(f,3,3,Mutation.ReturnMode.NONE)); assertNull(unrelated.getException()); f.machine.cancelTransaction(unrelated);
                assertEquals(CommandResult.Status.SUCCESS,f.apply(x,1)); assertNotNull(f.propose(f.ack(20,0)).getException());
                assertEquals(CommandResult.Status.SUCCESS,f.apply(y,2)); var ack=f.propose(f.ack(20,0)); assertNull(ack.getException()); f.machine.cancelTransaction(ack); f.assertReleased();
            }
        }
    }
    @Test void abaPhantomAndOutsideScopeNeverAcquireAcceptedAuthority() throws Exception {
        try (var f=fixture("conflicts"); var pin=f.store.captureTracked(scope(f,0))) {
            var change=f.propose(put(f,10,0,Mutation.ReturnMode.NONE)); f.apply(change,1);
            var restore=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,11),List.of(new Mutation(Mutation.Kind.DELETE,f.column,new byte[]{0},null)));
            f.apply(f.propose(restore),2);
            assertThrows(IllegalStateException.class,() -> f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.NONE)));
            assertEquals(0,f.machine.proposalFootprintStats().candidates()); assertEquals(RangeMutationHistory.Status.CONFLICT,pin.status());
            assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes()); pin.close(); pin.close();
            try (var narrow=f.store.captureTracked(scope(f,1))) {
                assertThrows(IllegalArgumentException.class,() -> f.machine.prepareScopedCommit(narrow,put(f,1,2,Mutation.ReturnMode.NONE))); assertEquals(0,f.machine.proposalFootprintStats().candidates());
            }
            var range=new TransactionScope(List.of(TransactionScope.Span.range(f.column,new byte[]{4},new byte[]{8})));
            try (var phantom=f.store.captureTracked(range)) {
                f.apply(f.propose(put(f,12,5,Mutation.ReturnMode.NONE)),3);
                assertThrows(IllegalStateException.class,() -> f.machine.prepareScopedCommit(phantom,put(f,2,6,Mutation.ReturnMode.NONE))); assertEquals(0,f.machine.proposalFootprintStats().candidates());
            }
            f.assertReleased();
        }
    }
    @Test void newMetadataWaitsForAmbiguousAcceptedEntryAndGenerationDoesNotReleaseIt() throws Exception {
        try (var f=fixture("ambiguous"); var pin=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.NONE))) {
            var transaction=f.propose(plan.command()); f.machine.preAppendTransaction(transaction); transaction.initLogEntry(1,1); f.machine.cancelTransaction(transaction);
            plan.close(); f.machine.notifyNotLeader(List.of()); assertEquals(1,f.machine.proposalFootprintStats().candidates());
            assertNotNull(f.propose(f.ack(2,0)).getException()); assertNotNull(f.propose(put(f,3,0,Mutation.ReturnMode.NONE)).getException());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertEquals(0,f.machine.proposalFootprintStats().candidates()); var ack=f.propose(f.ack(2,0)); assertNull(ack.getException()); f.machine.cancelTransaction(ack); f.assertReleased();
        }
    }
    @Test void abandonedInvalidatedOrForgedPlansCannotInstallUserEffects() throws Exception {
        try (var f=fixture("authority"); var pin=f.store.captureTracked(scope(f,0)); var prepared=f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.NONE))) {
            var command=prepared.command();
            var forged=DistributedCommand.acceptedTransaction(put(f,1,0,Mutation.ReturnMode.NONE),command.acceptance());
            assertEquals(CommandResult.Status.CONFLICT,f.apply(f.propose(forged),1));
            f.machine.notifyNotLeader(List.of()); assertEquals(CommandResult.Status.CONFLICT,f.apply(f.propose(command),2));
            assertEquals(0,f.machine.proposalFootprintStats().candidates()); prepared.close(); f.assertReleased();
            try (var again=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(again,put(f,2,0,Mutation.ReturnMode.NONE))) {
                var abandoned=plan.command(); plan.close(); assertEquals(CommandResult.Status.CONFLICT,f.apply(f.propose(abandoned),3));
            }
            try (var read=f.store.capture()) { assertNull(read.get(f.column,new byte[]{0})); }
            f.assertReleased();
        }
    }
    @Test void metadataPredecessorsAndAcknowledgingPlansHaveExplicitOwnership() throws Exception {
        try (var f=fixture("metadata"); var pin=f.store.captureTracked(scope(f,1))) {
            var metadata=f.propose(f.ack(5,0)); assertNull(metadata.getException());
            assertThrows(java.io.IOException.class,() -> f.machine.prepareScopedCommit(pin,put(f,1,1,Mutation.ReturnMode.NONE))); f.machine.cancelTransaction(metadata);
            var base=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,5),put(f,5,1,Mutation.ReturnMode.NONE).mutations(),List.of(),0);
            try (var plan=f.machine.prepareScopedCommit(pin,base); var other=f.store.captureTracked(scope(f,2))) {
                assertThrows(java.io.IOException.class,() -> f.machine.prepareScopedCommit(other,put(f,2,2,Mutation.ReturnMode.NONE)));
                var transaction=f.propose(plan.command()); assertNull(transaction.getException()); assertEquals(CommandResult.Status.SUCCESS,f.apply(transaction,1)); f.assertReleased();
            }
            try (var fresh=f.store.captureTracked(scope(f,2)); var plan=f.machine.prepareScopedCommit(fresh,put(f,2,2,Mutation.ReturnMode.NONE))) { assertNotNull(plan); }
            assertEquals(0,f.machine.proposalFootprintStats().candidates());
        }
    }
    @Test void preparedPayloadQuotaIncludesPlanningAndReopensAfterAbandonment() throws Exception {
        try (var f=fixture("payload-budget")) {
            var plans=new ArrayList<RatisRangeStateMachine.ScopedCommit>();
            try {
                byte[] value=new byte[DistributedCommandCodec.MAX_ENCODED_BYTES-512];
                for (int i=0;i<8;i++) try (var pin=f.store.captureTracked(scope(f,i))) {
                    var command=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,i),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)i},value)));
                    plans.add(f.machine.prepareScopedCommit(pin,command));
                }
                assertTrue(f.counter("preparedCommitBytes")>15L*1024*1024); assertEquals(8,f.machine.proposalFootprintStats().candidates());
                try (var pin=f.store.captureTracked(scope(f,9))) {
                    var command=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,9),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{9},value)));
                    assertThrows(IllegalStateException.class,() -> f.machine.prepareScopedCommit(pin,command)); assertEquals(8,f.machine.proposalFootprintStats().candidates());
                    plans.removeFirst().close(); try (var reopened=f.machine.prepareScopedCommit(pin,command)) { assertNotNull(reopened); }
                }
            } finally { plans.forEach(RatisRangeStateMachine.ScopedCommit::close); }
            f.assertReleased(); assertEquals(0,f.machine.proposalFootprintStats().candidates()); assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
    @Test void producedEnvelopeMustPassAdapterValidationBeforeItBecomesAuthorized() throws Exception {
        try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("adapter-shape"),RatisRangeStateMachine.RetentionLimits.defaults(),command -> {
            if (command.acceptance()!=null) throw new IllegalArgumentException("This range does not accept transaction plans");
        })) {
            f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            try (var pin=f.store.captureTracked(scope(f,0))) {
                assertThrows(IllegalArgumentException.class,() -> f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.NONE)));
                assertEquals(0,f.machine.proposalFootprintStats().candidates()); assertEquals(RangeMutationHistory.Status.ACTIVE,pin.status()); f.assertReleased();
            }
            assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
    @Test void invalidatedNativePlannerKeepsItsPayloadChargeUntilItActuallyReturns() throws Exception {
        try (var f=fixture("retired-planner"); var pin=f.store.captureTracked(scope(f,0)); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var field=f.store.getClass().getDeclaredField("applyLane"); field.setAccessible(true);
            var lane=(java.util.concurrent.locks.ReentrantLock)field.get(f.store);
            var thread=new java.util.concurrent.atomic.AtomicReference<Thread>();
            lane.lock(); java.util.concurrent.Future<?> prepare=null,loss=null;
            try {
                prepare=executor.submit(() -> { thread.set(Thread.currentThread()); return f.machine.prepareScopedCommit(pin,put(f,0,0,Mutation.ReturnMode.NONE)); });
                await(() -> thread.get()!=null && lane.hasQueuedThread(thread.get()));
                long charged=f.counter("preparedCommitBytes"); assertTrue(charged>0);
                loss=executor.submit(() -> f.machine.notifyNotLeader(List.of()));
                await(() -> f.machine.proposalFootprintStats().candidates()==0);
                assertFalse(prepare.isDone()); assertEquals(charged,f.counter("preparedCommitBytes"),"Generation invalidation cannot donate payload still owned by native work");
            } finally { lane.unlock(); }
            assertNotNull(prepare); assertNotNull(loss); var pending=prepare;
            assertThrows(java.util.concurrent.ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
            loss.get(5,TimeUnit.SECONDS); f.assertReleased(); assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
    private static void await(java.util.function.BooleanSupplier condition) {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while (!condition.getAsBoolean() && System.nanoTime()<deadline) java.util.concurrent.locks.LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(condition.getAsBoolean(),"Controlled ownership transition timed out");
    }
}
