package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisNodeResourceBudgetTest {
    private static RatisNodeResourceBudget.Limits limits(long bytes,int owners) {
        return new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(bytes,owners),new RatisNodeResourceBudget.Lane(8192,2));
    }
    @Test void groupsAndPoolsShareNodeCapsWhileProtectedProgressRemainsAdmissible() throws Exception {
        var a=UUID.randomUUID(); var b=UUID.randomUUID();
        var budget=new RatisNodeResourceBudget(limits(100,2),Map.of(a,limits(80,2),b,limits(80,2)));
        var first=budget.reserve(a,RatisNodeResourceBudget.Kind.ORDINARY,60,1);
        assertThrows(IOException.class,()->budget.reserve(a,RatisNodeResourceBudget.Kind.ORDINARY,21,1));
        var second=budget.reserve(b,RatisNodeResourceBudget.Kind.ORDINARY,40,1);
        assertThrows(IOException.class,()->budget.reserve(b,RatisNodeResourceBudget.Kind.ORDINARY,1,1));
        try(var recovery=budget.reserve(a,RatisNodeResourceBudget.Kind.PROTECTED,32,1)) {
            assertEquals(100,budget.stats().ordinary().bytes());
            assertEquals(32,budget.stats().protectedLane().bytes());
        }
        first.close(); first.close(); second.close();
        assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats().ordinary());
    }
    @Test void observerCancellationDoesNotReleaseActualOwnership() throws Exception {
        var pool=UUID.randomUUID(); var budget=new RatisNodeResourceBudget(limits(64,1),Map.of(pool,limits(64,1)));
        var lease=budget.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,64,1);
        var observer=new CompletableFuture<Void>(); var actualDrain=new CompletableFuture<Void>();
        actualDrain.whenComplete((ignored,failure)->lease.close());
        observer.cancel(false);
        assertThrows(IOException.class,()->budget.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,1,1));
        assertFalse(lease.released()); actualDrain.complete(null);
        assertTrue(lease.released());
        try(var next=budget.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,64,1)) {assertEquals(64,budget.stats(pool).ordinary().bytes());}
    }
    @Test void shrinkOnlyRefundsKnownFreedBytesAndUndeclaredPoolsReject() throws Exception {
        var pool=UUID.randomUUID(); var budget=new RatisNodeResourceBudget(limits(64,2),Map.of(pool,limits(64,2)));
        try(var lease=budget.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,64,1)) {
            lease.shrinkTo(16,1);
            assertEquals(16,budget.stats().ordinary().bytes());
            assertThrows(IllegalArgumentException.class,()->lease.shrinkTo(17,1));
            assertThrows(IllegalArgumentException.class,()->budget.reserve(UUID.randomUUID(),RatisNodeResourceBudget.Kind.ORDINARY,1,1));
        }
        assertEquals(0,budget.stats().ordinary().owners());
    }
    @org.junit.jupiter.api.io.TempDir(factory=RatisDiskTestDirectory.class) java.nio.file.Path root;
    private RatisRetentionAdmissionTest.Fixture fixture(String name,RatisNodeResourceBudget budget,UUID pool,java.util.concurrent.atomic.AtomicLong clock) throws Exception {
        return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> {},clock::get,null,machine -> machine.nodeResources(budget,pool));
    }
    @Test void actualMachinesShareProposalOwnersAndRetainUnknownAppendUntilTermExclusion() throws Exception {
        var a=UUID.randomUUID(); var b=UUID.randomUUID(); var clock=new java.util.concurrent.atomic.AtomicLong(100);
        var budget=new RatisNodeResourceBudget(limits(1024*1024,1),Map.of(a,limits(1024*1024,1),b,limits(1024*1024,1)));
        try(var first=fixture("first",budget,a,clock);var second=fixture("second",budget,b,clock)) {
            var pending=first.propose(first.put(0)); assertNull(pending.getException());
            assertThrows(IOException.class,()->second.propose(second.put(0)));
            // Protected ACKs remain available with the shared ordinary owner occupied.
            var ack=second.propose(second.ack(1,0)); assertNull(ack.getException());
            second.machine.cancelTransaction(ack);
            pending.initLogEntry(1,1); first.machine.cancelTransaction(pending);
            assertEquals(1,budget.stats().ordinary().owners());
            assertThrows(IOException.class,()->second.propose(second.put(0)));
            // Actual committed completion settles that exact original, without a volatile apply check.
            first.machine.applyTransaction(pending).get(5,java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(0,budget.stats().ordinary().owners());
            var next=second.propose(second.put(0)); assertNull(next.getException()); second.machine.cancelTransaction(next);
            assertEquals(0,budget.stats().ordinary().bytes());
        }
    }
    @Test void canceledNativeStagingRetainsSharedBytesUntilActualReturn() throws Exception {
        var a=UUID.randomUUID(); var b=UUID.randomUUID(); var clock=new java.util.concurrent.atomic.AtomicLong(100);
        var capacity=RatisScopedTransactions.STAGING_BUILD_BYTES+8192;
        var lane=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(capacity,8),new RatisNodeResourceBudget.Lane(8192,4));
        var budget=new RatisNodeResourceBudget(lane,Map.of(a,lane,b,lane));
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);
        try(var first=fixture("staging-first",budget,a,clock);var second=fixture("staging-second",budget,b,clock);
            var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
            var registry=new RatisScopedTransactions(first.machine,RatisScopedTransactions.Limits.defaults(),clock::get,first.machine::prepareScopedCommit,(pin,command)-> {
                var actual=pin.stage(command); entered.countDown();
                try { if(!release.await(5,java.util.concurrent.TimeUnit.SECONDS))throw new IOException("Native staging release timeout"); return actual; }
                catch(Exception failure) { actual.close();throw failure; }
            });var other=new RatisScopedTransactions(second.machine,RatisScopedTransactions.Limits.defaults(),clock::get)) {
            var scope=new it.cavallium.rockserver.core.common.distributed.TransactionScope(java.util.List.of(it.cavallium.rockserver.core.common.distributed.TransactionScope.Span.point(first.column,new byte[]{0})));
            var handle=registry.open(UUID.randomUUID(),scope,java.time.Duration.ofMillis(1));
            var otherScope=new it.cavallium.rockserver.core.common.distributed.TransactionScope(java.util.List.of(it.cavallium.rockserver.core.common.distributed.TransactionScope.Span.point(second.column,new byte[]{0})));
            var otherHandle=other.open(UUID.randomUUID(),otherScope,java.time.Duration.ofSeconds(1));
            var command=new it.cavallium.rockserver.core.common.distributed.DistributedCommand(first.groupId,1,new byte[32],new it.cavallium.rockserver.core.common.distributed.OperationId(handle.id(),0,0),java.util.List.of(new it.cavallium.rockserver.core.common.distributed.Mutation(it.cavallium.rockserver.core.common.distributed.Mutation.Kind.PUT,first.column,new byte[]{0},new byte[]{7})));
            var pending=executor.submit(()->registry.stage(handle,command));
            try {
                assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS));
                clock.addAndGet(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(2)); registry.expire();
                assertFalse(registry.releaseAndDrain(handle));
                assertTrue(budget.stats().ordinary().bytes()>=RatisScopedTransactions.STAGING_BUILD_BYTES);
                var otherCommand=new it.cavallium.rockserver.core.common.distributed.DistributedCommand(second.groupId,1,new byte[32],new it.cavallium.rockserver.core.common.distributed.OperationId(otherHandle.id(),0,0),java.util.List.of(new it.cavallium.rockserver.core.common.distributed.Mutation(it.cavallium.rockserver.core.common.distributed.Mutation.Kind.PUT,second.column,new byte[]{0},new byte[]{8})));
                assertThrows(IOException.class,()->other.stage(otherHandle,otherCommand));
                var protectedAck=second.propose(second.ack(1,0)); assertNull(protectedAck.getException()); second.machine.cancelTransaction(protectedAck);
            } finally {release.countDown();}
            assertThrows(java.util.concurrent.ExecutionException.class,()->pending.get(5,java.util.concurrent.TimeUnit.SECONDS));
            other.release(otherHandle); assertEquals(0,budget.stats().ordinary().bytes());
            assertEquals(0,first.store.activeSnapshotPins());
        }
    }

}
