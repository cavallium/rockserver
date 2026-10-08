package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import static org.junit.jupiter.api.Assertions.*;

/** Native admission/capacity checks; no transport or consensus acceptance assertion. */
class RatisRetentionAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void pendingReservationsDuplicatesCancellationAndAckAtCapacityConserveUsage() throws Exception {
        try (var f=new Fixture(root,new RatisRangeStateMachine.RetentionLimits(2,184,1,92))) {
            var zero=f.propose(f.put(0)); var one=f.propose(f.put(1));
            assertNull(zero.getException()); assertNull(one.getException());
            assertNotNull(f.propose(f.put(2)).getException()); assertEquals(0,f.store.receiptUsage().retainedCount());
            f.machine.cancelTransaction(one); assertEquals(1,f.counter("reservedReceiptCount"));
            var duplicate=f.propose(f.put(0)); assertNull(duplicate.getException());
            assertEquals(1,f.counter("reservedReceiptCount"));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(zero,1));
            assertEquals(0,f.counter("reservedReceiptCount"),"Applied result settles reservation despite duplicate RPC owner");
            var retryOne=f.propose(f.put(1)); assertNull(retryOne.getException());
            f.machine.cancelTransaction(duplicate);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(retryOne,2));
            assertEquals(2,f.store.receiptUsage().retainedCount()); assertNotNull(f.propose(f.put(2)).getException());
            var ack=f.propose(f.ack(3,1)); assertNull(ack.getException());
            assertNotNull(f.propose(f.ack(4,1)).getException(),"Overlapping maintenance cannot reuse removal credit");
            assertNotNull(f.propose(f.put(2)).getException(),"Pending deletion gives no negative credit to data");
            assertEquals(CommandResult.Status.SUCCESS,f.apply(ack,3));
            assertEquals(1,f.store.receiptUsage().retainedCount());
            var next=f.propose(f.put(2)); assertNull(next.getException());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(next,4));
            f.assertReleased(); assertEquals(2,f.store.receiptUsage().retainedCount());
        }
    }
    @Test void maintenanceHeadroomIsBoundedAndNoGrowthGcStillRunsAtHardCapacity() throws Exception {
        try (var f=new Fixture(root,new RatisRangeStateMachine.RetentionLimits(1,92,1,92))) {
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(f.put(0)),1));
            var first=DistributedCommand.advanceSessionEpochFloor(f.groupId,1,new byte[32],new OperationId(f.admin,2,0),1);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(first),2));
            assertEquals(2,f.store.receiptUsage().retainedCount());
            var redundant=DistributedCommand.advanceSessionEpochFloor(f.groupId,1,new byte[32],new OperationId(f.admin,2,1),1);
            assertNotNull(f.propose(redundant).getException(),"Hard maintenance ceiling also bounds no-op control receipts");
            var ack=f.propose(f.ack(2,0)); assertNull(ack.getException());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(ack,3));
            var expire=DistributedCommand.advanceSessionEpochFloor(f.groupId,1,new byte[32],new OperationId(f.admin,2,2),2);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(expire),4));
            assertEquals(2,f.store.receiptUsage().retainedCount()); f.assertReleased();
        }
    }
    @Test void maximumReturnedOutcomeIsReservedAndStaleTermReleasesWithoutStepDown() throws Exception {
        try (var f=new Fixture(root,new RatisRangeStateMachine.RetentionLimits(2,184,1,92))) {
            var returning=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,0),
                    List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{7},Mutation.ReturnMode.PREVIOUS)));
            assertNotNull(f.propose(returning).getException());
            var pending=f.propose(f.put(0)); assertNull(pending.getException());
            f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,1);
            var rejected=assertThrows(StateMachineException.class,() -> f.machine.preAppendTransaction(pending));
            assertFalse(rejected.leaderShouldStepDown());
            f.machine.cancelTransaction(pending); f.machine.notifyNotLeader(List.of(pending)); f.assertReleased();
            var next=f.propose(f.put(1)); assertNull(next.getException()); f.machine.cancelTransaction(next); f.assertReleased();
        }
    }
    @Test void snapshotCoveredMaintenanceSettlesRetentionBeforeDuplicatePayloadOwners() throws Exception {
        try (var f=new Fixture(root,new RatisRangeStateMachine.RetentionLimits(2,184,1,92))) {
            var command=f.ack(1,0);
            var covered=f.propose(command); var duplicate=f.propose(command);
            // Model the native result arriving through an installed image rather than this adapter's apply callback.
            f.store.apply(new ApplyPosition(1,1),command); assertEquals(1,f.machine.takeSnapshot());
            f.machine.pause();
            covered.initLogEntry(1,1); duplicate.initLogEntry(1,2);
            var first=f.machine.applyTransaction(covered); var second=f.machine.applyTransaction(duplicate);
            assertFalse(first.isDone()); assertFalse(second.isDone());
            f.machine.reinitialize();
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(first.get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertFalse(second.isDone()); assertEquals(1,f.counter("protectedAdmissions"));
            assertEquals(0,f.counter("reservedReceiptCount"));
            var next=f.propose(f.ack(3,1)); assertNull(next.getException(),"Recovered result releases maintenance ownership independently of duplicate RPCs");
            f.machine.cancelTransaction(next);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(duplicate).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertTrue(second.isDone()); f.assertReleased();
        }
    }
    @Test void pureAckHasAProtectedSlotWhenOrdinaryProposalQueueIsFull() throws Exception {
        try (var f=new Fixture(root,RatisRangeStateMachine.RetentionLimits.defaults())) {
            var pending=new ArrayList<TransactionContext>();
            for (int i=0;i<1024;i++) { var context=f.propose(f.put(i)); assertNull(context.getException()); pending.add(context); }
            assertNotNull(f.propose(f.put(1024)).getException());
            var ack=f.propose(f.ack(2000,0)); assertNull(ack.getException());
            f.machine.cancelTransaction(ack);
            for (var context:pending) f.machine.cancelTransaction(context);
            f.assertReleased();
        }
    }
    static final class Fixture implements AutoCloseable {
        final UUID groupId=UUID.randomUUID(), session=UUID.randomUUID(), admin=UUID.randomUUID();
        final UUID column;
        final RaftPeer peer=RaftPeer.newBuilder().setId("fixture").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final AtomicReference<RatisRangeStateMachine.Leadership> leadership=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final DistributedStoragePool pool; final RaftStorage storage; final ReplicatedRangeStore store; final RatisRangeStateMachine machine;
        Fixture(Path root,RatisRangeStateMachine.RetentionLimits limits) throws Exception {
            this(root,limits,command -> {});
        }
        Fixture(Path root,RatisRangeStateMachine.RetentionLimits limits,java.util.function.Consumer<DistributedCommand> validator) throws Exception {
            this(root,limits,validator,System::nanoTime);
        }
        Fixture(Path root,RatisRangeStateMachine.RetentionLimits limits,java.util.function.Consumer<DistributedCommand> validator,java.util.function.LongSupplier nanos) throws Exception {
            this(root,limits,validator,nanos,null);
        }
        Fixture(Path root,RatisRangeStateMachine.RetentionLimits limits,java.util.function.Consumer<DistributedCommand> validator,java.util.function.LongSupplier nanos,RangeOwnership.Interval interval) throws Exception {
            this(root,limits,validator,nanos,interval,machine -> {});
        }
        Fixture(Path root,RatisRangeStateMachine.RetentionLimits limits,java.util.function.Consumer<DistributedCommand> validator,java.util.function.LongSupplier nanos,RangeOwnership.Interval interval,java.util.function.Consumer<RatisRangeStateMachine> configure) throws Exception {
            column=interval==null ? UUID.randomUUID() : interval.columns().iterator().next();
            pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),interval==null ? new RangeOwnership.Interval(groupId,Set.of(column),null,null) : interval,new byte[32],Map.of(column,"data"),Set.of());
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
                if (method.getName().equals("getId")) return peer.getId();
                if (method.getName().equals("toString")) return "identity-fixture";
                throw new AssertionError("Unexpected server access "+method);
            });
            machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),validator,command -> {},limits,leadership::get);
            machine.transactionClock(nanos); configure.accept(machine);
            machine.initialize(identity,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
        }
        DistributedCommand put(long sequence) { return new DistributedCommand(groupId,1,new byte[32],new OperationId(session,1,sequence),
                List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)sequence},new byte[]{7}))); }
        DistributedCommand ack(long sequence,long through) { return new DistributedCommand(groupId,1,new byte[32],new OperationId(session,1,sequence),List.of(),List.of(),through); }
        TransactionContext propose(DistributedCommand command) throws Exception {
            return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId())
                    .setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
        }
        CommandResult.Status apply(TransactionContext transaction,long index) throws Exception {
            assertNull(transaction.getException()); machine.preAppendTransaction(transaction);
            transaction.initLogEntry(leadership.get().term(),index);
            return CommandResult.decode(machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status();
        }
        long counter(String name) throws Exception { var field=RatisRangeStateMachine.class.getDeclaredField(name); field.setAccessible(true); return ((Number)field.get(machine)).longValue(); }
        void assertReleased() throws Exception {
            assertEquals(0,counter("preparedCommitBytes"));
            assertEquals(0,counter("reservedReceiptCount")); assertEquals(0,counter("reservedReceiptBytes"));
            assertEquals(0,counter("dataAdmissions")); assertEquals(0,counter("protectedAdmissions")); assertEquals(0,counter("admittedBytes"));
            var field=RatisRangeStateMachine.class.getDeclaredField("maintenanceReservation"); field.setAccessible(true); assertNull(field.get(machine));
        }
        public void close() throws Exception { machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); storage.close(); pool.close(); }
    }
}
