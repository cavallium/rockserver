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
    @Test void imageMetadataUsesActualSharedProtectedWorkspaceWithoutChargingUserIo()throws Exception {
        var pool=UUID.randomUUID();var budget=new RatisNodeResourceBudget(limits(100,2),Map.of(pool,limits(100,2)));
        var bytes=new java.util.concurrent.atomic.AtomicLong();var admission=budget.imageMetadataAdmission(pool,bytes::addAndGet);
        admission.beforeBytes(1024*1024);assertEquals(1024*1024,bytes.get());assertEquals(0,budget.stats().protectedLane().bytes());
        try(var workspace=admission.reserveMetadata(128)) {
            assertEquals(6144,budget.stats().protectedLane().bytes());assertEquals(6144,budget.stats(pool).protectedLane().bytes());
            assertThrows(IOException.class,()->admission.reserveMetadata(1));
        }
        assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats().protectedLane());
        assertThrows(IOException.class,()->admission.reserveMetadata(1<<20));assertEquals(0,budget.stats().protectedLane().owners());
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
    @Test void growthAdmitsOnlyTheByteDeltaAndDenialPreservesBothCounters()throws Exception {
        var a=UUID.randomUUID();var b=UUID.randomUUID();
        var budget=new RatisNodeResourceBudget(limits(100,2),Map.of(a,limits(80,1),b,limits(80,1)));
        var first=budget.reserve(a,RatisNodeResourceBudget.Kind.ORDINARY,40,1);
        try(var second=budget.reserve(b,RatisNodeResourceBudget.Kind.ORDINARY,20,1)) {
            first.growTo(80);first.growTo(80);
            assertEquals(new RatisNodeResourceBudget.Usage(100,2),budget.stats().ordinary());
            assertEquals(new RatisNodeResourceBudget.Usage(80,1),budget.stats(a).ordinary());
            assertThrows(IOException.class,()->first.growTo(81));
            assertThrows(IOException.class,()->second.growTo(21));
            assertEquals(new RatisNodeResourceBudget.Usage(100,2),budget.stats().ordinary());
            assertEquals(new RatisNodeResourceBudget.Usage(20,1),budget.stats(b).ordinary());
            second.shrinkTo(10,1);
            assertThrows(IOException.class,()->first.growTo(81)); // Pool limit still applies with node space available.
            assertEquals(new RatisNodeResourceBudget.Usage(90,2),budget.stats().ordinary());
            assertThrows(IllegalArgumentException.class,()->first.growTo(79));
            assertThrows(IllegalArgumentException.class,()->first.growTo(RatisNodeResourceBudget.MAX_BYTES+1));
            first.shrinkTo(50,1);first.growTo(60);
            assertEquals(new RatisNodeResourceBudget.Usage(70,2),budget.stats().ordinary());
            assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats().protectedLane());
        } finally {first.close();}
        assertThrows(IllegalStateException.class,()->first.growTo(1));
        assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats().ordinary());
        assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats(a).ordinary());
        assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats(b).ordinary());
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
    @Test void pointConstructionRejectsBeforeDecodeAndNativeReadWhenSharedGraphIsFull() throws Exception {
        var pool=UUID.randomUUID();var clock=new java.util.concurrent.atomic.AtomicLong(100);
        long capacity=16L<<20;var lane=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(capacity,8),new RatisNodeResourceBudget.Lane(8192,4));
        var budget=new RatisNodeResourceBudget(lane,Map.of(pool,lane));
        try(var first=fixture("read-first",budget,pool,clock);var second=fixture("read-second",budget,pool,clock)) {
            assertEquals(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,second.apply(second.propose(second.put(0)),1));
            var request=org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(ScopedTransactionRpc.pointRequest(second.column,new byte[]{0})));
            try(var occupied=budget.reserve(pool,RatisNodeResourceBudget.Kind.ORDINARY,capacity,1)) {
                var failure=assertThrows(java.util.concurrent.ExecutionException.class,()->second.machine.query(request).get(5,java.util.concurrent.TimeUnit.SECONDS));
                assertInstanceOf(IOException.class,failure.getCause());
                // A valid envelope with an undecodable body is rejected by admission first.
                var malformed=org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,new byte[0])));
                var rejected=assertThrows(java.util.concurrent.ExecutionException.class,()->first.machine.query(malformed).get(5,java.util.concurrent.TimeUnit.SECONDS));
                assertInstanceOf(IOException.class,rejected.getCause());assertEquals(capacity,budget.stats().ordinary().bytes());
            }
            assertArrayEquals(new byte[]{7},ScopedTransactionRpc.pointValue(second.machine.query(request).get(5,java.util.concurrent.TimeUnit.SECONDS).getContent().toByteArray(),16));
            assertEquals(new RatisNodeResourceBudget.Usage(0,0),budget.stats().ordinary());
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

    @Test void protectedFinalizationCompletesWithOrdinaryFullAndDuplicateInputsDoNotDonateCharge() throws Exception {
        var pool=UUID.randomUUID();
        var budget=new RatisNodeResourceBudget(new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1024*1024,1),new RatisNodeResourceBudget.Lane(32L*1024*1024,8)),Map.of(pool,new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1024*1024,1),new RatisNodeResourceBudget.Lane(32L*1024*1024,8))));
        var helper=new RatisParticipantPrepareTest(); helper.root=root; helper.dataInitializer=machine->machine.nodeResources(budget,pool);
        try(var home=helper.new Node("protected-home",true);var data=helper.new Node("protected-data",false);var other=helper.new Node("ordinary-full",false);
            var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            var abort=initial.decide(it.cavallium.rockserver.core.common.distributed.TransactionStatus.Phase.ABORT);
            assertEquals(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,home.statuses.execute(home.statuses.prepare(new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,4),1,1),initial,abort)).status());
            var ordinary=other.f.propose(other.f.put(0)); assertNull(ordinary.getException());
            assertThrows(IOException.class,()->data.f.propose(data.f.put(8)));
            try(var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),new it.cavallium.rockserver.core.common.distributed.OperationId(UUID.randomUUID(),3,0))) {
                var first=data.wire.propose(plan.bytes()); var duplicate=data.wire.propose(plan.bytes());
                assertNull(first.getException()); assertNull(duplicate.getException());
                var charged=budget.stats().protectedLane(); assertEquals(3,charged.owners());
                data.f.machine.cancelTransaction(first);
                assertEquals(charged,budget.stats().protectedLane());
                data.f.machine.preAppendTransaction(duplicate); duplicate.initLogEntry(1,1);
                assertNotNull(data.f.machine.applyTransaction(duplicate).get(5,java.util.concurrent.TimeUnit.SECONDS));
                assertNotNull(data.routed.finalizedParticipant(data.f.groupId,initial.transactionId()));
                assertEquals(0,budget.stats().protectedLane().bytes()); assertEquals(1,budget.stats().ordinary().owners());
            } finally { other.f.machine.cancelTransaction(ordinary); }
            assertEquals(0,budget.stats().ordinary().bytes());
        }
    }
    @Test void canceledBlockedHomeLookupKeepsProtectedGraphUntilActualLookupExits() throws Exception {
        var pool=UUID.randomUUID(); var lane=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1024*1024,1),new RatisNodeResourceBudget.Lane(32L*1024*1024,8));
        var budget=new RatisNodeResourceBudget(lane,Map.of(pool,lane));
        var helper=new RatisParticipantPrepareTest(); helper.root=root; helper.dataInitializer=machine->machine.nodeResources(budget,pool);
        var entered=new java.util.concurrent.CountDownLatch(1);var release=new java.util.concurrent.CountDownLatch(1);var drained=new CompletableFuture<Void>();
        try(var home=helper.new Node("lookup-home",true);var data=helper.new Node("lookup-data",false);
            var pin=data.f.store.captureTracked(helper.scope(data,1,4));var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            var abort=initial.decide(it.cavallium.rockserver.core.common.distributed.TransactionStatus.Phase.ABORT);
            assertEquals(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,home.statuses.execute(home.statuses.prepare(new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,4),1,1),initial,abort)).status());
            home.wire.afterRead=()->{entered.countDown();try {if(!release.await(5,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("HOME lookup timeout");}catch(InterruptedException failure){throw new AssertionError(failure);}};
            var recovery=new it.cavallium.rockserver.core.common.distributed.OperationId(UUID.randomUUID(),3,0);
            var observer=executor.submit(()->{try(var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery)){return plan.bytes();}finally{drained.complete(null);}});
            try {
                assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS));
                var charged=budget.stats().protectedLane(); assertTrue(charged.bytes()>0); assertEquals(1,charged.owners());
                assertTrue(observer.cancel(false)); data.f.machine.notifyNotLeader(java.util.List.of());
                assertFalse(drained.isDone()); assertEquals(charged,budget.stats().protectedLane());
                assertThrows(IllegalStateException.class,()->data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery));
                assertEquals(charged,budget.stats().protectedLane());
            } finally {release.countDown();}
            drained.get(5,java.util.concurrent.TimeUnit.SECONDS); home.wire.afterRead=null;
            assertEquals(0,budget.stats().protectedLane().bytes()); assertEquals(0,data.f.machine.protectedFinalizationSlots());
        }
    }

    private final class BudgetSnapshotHome implements AutoCloseable {
        final RatisRetentionAdmissionTest.Fixture f; final RatisParticipantPrepareTest.Wire wire;
        final RatisRoutedClient routed; final RatisSnapshotFences homes; long sequence;
        BudgetSnapshotHome(RatisParticipantPrepareTest helper,RatisNodeResourceBudget budget,UUID pool) throws Exception {
            var validation=new java.util.concurrent.atomic.AtomicReference<java.util.function.Consumer<it.cavallium.rockserver.core.common.distributed.DistributedCommand>>(command->{});
            var column=UUID.randomUUID();
            var interval=new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(UUID.randomUUID(),java.util.Set.of(column),null,null);
            f=new RatisRetentionAdmissionTest.Fixture(root.resolve("budget-snapshot-home"),RatisRangeStateMachine.RetentionLimits.defaults(),command->validation.get().accept(command),System::nanoTime,interval,machine->{
                machine.nodeResources(budget,pool);
                machine.role(RatisNodePlan.Role.SNAPSHOT_HOME,(key,value)->RatisSnapshotFences.validateRecord(helper.cluster,helper.database,machine.getGroupId().getUuid(),1,key,value));
                machine.snapshotHomeStorage(helper.cluster,helper.database,column);
            });
            var replica=new it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:12345",it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor.Role.VOTER);
            var route=new it.cavallium.rockserver.core.common.distributed.RangeDescriptor(f.groupId,f.groupId,1,0,null,null,it.cavallium.rockserver.core.common.distributed.RangeDescriptor.TopologyState.ACTIVE,java.util.List.of(replica));
            var directory=new it.cavallium.rockserver.core.common.distributed.RangeDirectory(helper.cluster,helper.database,interval.domainId(),1,new byte[32],0,java.util.Set.of(column),0,java.util.List.of(route));
            wire=new RatisParticipantPrepareTest.Wire(f); routed=new RatisRoutedClient(directory,wire); homes=new RatisSnapshotFences(routed,column);
            validation.set(RatisSnapshotFences.validator(helper.cluster,helper.database,f.groupId,1,new byte[32],column));
        }
        void advance(it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus before,it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus next) throws Exception {
            var id=new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,7100),1,sequence++);
            assertEquals(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,homes.execute(homes.prepare(id,before,next)).status());
        }
        public void close() throws Exception {routed.close();f.close();}
    }
    @Test void snapshotHomeProgressUsesProtectedLaneButFreshRegistrationStillRequiresOrdinaryCapacity() throws Exception {
        var pool=UUID.randomUUID(); var lane=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1024*1024,1),new RatisNodeResourceBudget.Lane(64L*1024*1024,16));
        var budget=new RatisNodeResourceBudget(lane,Map.of(pool,lane));
        var helper=new RatisParticipantPrepareTest(); helper.root=root; helper.dataInitializer=machine->machine.nodeResources(budget,pool);
        try(var home=new BudgetSnapshotHome(helper,budget,pool);var data=helper.new Node("snapshot-budget-data",false);var other=helper.new Node("snapshot-ordinary-full",false)) {
            var scope=helper.scope(data,1,4);
            var participant=new it.cavallium.rockserver.core.common.distributed.SnapshotFence.Participant(data.f.groupId,data.f.groupId,1,new byte[32],new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,7200),1,0),new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,7200),1,1),scope);
            var fence=new it.cavallium.rockserver.core.common.distributed.SnapshotFence(helper.cluster,helper.database,data.directory.domainId(),UUID.randomUUID(),home.f.groupId,1,new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,7300),1,0),0,1,new byte[32],java.util.List.of(participant));
            var initial=it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.begin(fence); home.advance(null,initial);
            var ordinary=other.f.propose(other.f.put(0)); assertNull(ordinary.getException());
            try {
                var another=new it.cavallium.rockserver.core.common.distributed.SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),UUID.randomUUID(),fence.homeGroup(),fence.homeEpoch(),new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,7301),1,0),0,1,new byte[32],fence.participants());
                assertThrows(IOException.class,()->home.advance(null,it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.begin(another)));
                it.cavallium.rockserver.core.common.distributed.ApplyPosition installed;
                try(var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { installed=RatisSnapshotRecord.Result.decode(data.wire.append(install.bytes())).position(); }
                var ready=initial.ready(data.f.groupId,installed); home.advance(initial,ready);
                it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus captured;
                try(var actual=data.f.machine.captureSnapshotGate(home.homes,fence)) {
                    captured=ready.capture(data.f.groupId,new it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),actual.position()));
                    home.advance(ready,captured);
                }
                var complete=captured.complete(); home.advance(captured,complete);
                try(var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) { assertNotNull(RatisSnapshotRecord.Result.decode(data.wire.append(release.bytes())).position()); }
                var consumed=complete.consume(); home.advance(complete,consumed);
                assertEquals(consumed,home.homes.read(fence.fenceId()));
                assertEquals(1,budget.stats().ordinary().owners()); assertEquals(0,budget.stats().protectedLane().bytes());
            } finally {other.f.machine.cancelTransaction(ordinary);}
            assertEquals(0,budget.stats().ordinary().bytes());
        }
    }

}
