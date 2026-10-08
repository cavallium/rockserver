package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Catalogue/native/adapter/client composition; no sockets, server process or consensus acceptance claim. */
class RatisSessionBindingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void routedBindingDataAckAndAllExpiredClockProgressUseOneMaintenanceOwner() throws Exception {
        try (var f=new Fixture(root)) {
            var client=new RatisSessionClient(f.routed,f.ledger,f.ledger.grants().getFirst(),new RatisSessionClient.Limits(2,1024,1024));
            var first=client.prepare(List.of(f.put(1)));
            assertThrows(IOException.class,() -> client.execute(first)); assertEquals(0,f.store.applied().index());
            int reads=f.catalogue.reads.get();
            var binding=client.prepareBinding(f.catalogue.epochs,f.admin(0),first,-1);
            assertEquals(reads+1,f.catalogue.reads.get());
            var owner=f.propose(binding.command()); assertNull(owner.getException());
            var duplicate=f.propose(binding.command()); assertNull(duplicate.getException());
            var extra=f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(1),-1);
            assertNotNull(f.propose(extra).getException(),"Binding and authority advance share the maintenance lane");
            f.catalogue.unavailable=true;
            assertEquals(CommandResult.Status.SUCCESS,f.apply(owner).status());
            f.machine.cancelTransaction(duplicate);
            reads=f.catalogue.reads.get();
            assertEquals(CommandResult.Status.SUCCESS,client.execute(first).status()); client.acknowledge(first);
            var second=client.prepare(List.of(f.put(2)));
            assertEquals(0,second.command().acknowledgedThrough());
            assertEquals(CommandResult.Status.SUCCESS,client.execute(second).status()); client.acknowledge(second);
            assertEquals(reads,f.catalogue.reads.get(),"Ordinary writes and piggyback ACK read no catalogue state");
            f.catalogue.unavailable=false;
            f.expire(100);
            var advance=f.routed.prepareSessionControl(f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(1),0));
            var pending=f.propose(advance.command()); assertNull(pending.getException());
            var ack=client.prepareAcknowledgement(f.outer.data,new byte[]{1});
            assertNotNull(f.propose(ack.command()).getException(),"ACK cannot overlap credited expiry removal");
            f.catalogue.unavailable=true; assertEquals(CommandResult.Status.SUCCESS,f.apply(pending).status());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,client.execute(ack).status()); client.acknowledge(ack);
            assertEquals(1,f.store.receiptUsage().retainedCount());
            f.catalogue.unavailable=false; f.expire(200);
            var later=f.routed.prepareSessionControl(f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(2),1));
            assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(later).status());
            assertEquals(1,f.store.receiptUsage().retainedCount());
            f.assertNoReservations();
        }
    }
    @Test void ackBlocksAuthorityUntilCancelledAndSnapshotReloadKeepsControlConfiguration() throws Exception {
        try (var f=new Fixture(root)) {
            var binding=f.catalogue.epochs.prepareSessionBinding(f.directory,f.outer.dataGroup,f.admin(0),f.ledger.grants().getFirst(),2,-1);
            assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(f.routed.prepareSessionControl(binding)).status());
            var data=new DistributedCommand(f.outer.dataGroup,1,new byte[32],new OperationId(f.outer.a,1,0),List.of(f.put(1)));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(data)).status());
            var ack=new DistributedCommand(f.outer.dataGroup,1,new byte[32],new OperationId(f.outer.a,1,1),List.of(),List.of(),0);
            var pending=f.propose(ack); assertNull(pending.getException());
            f.expire(100);
            var advance=f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(1),0);
            assertNotNull(f.propose(advance).getException()); f.machine.cancelTransaction(pending);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(advance)).status());
            long cut=f.machine.takeSnapshot(); f.machine.pause(); f.catalogue.unavailable=true; f.machine.reinitialize();
            assertEquals(cut,f.store.applied().index());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.apply(f.propose(data)).status());
            assertEquals(1,f.store.receiptUsage().retainedCount());
            f.catalogue.unavailable=false; f.expire(200);
            var fresh=f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(2),1);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(fresh)).status()); f.assertNoReservations();
        }
    }
    @Test void individualExpiryReturnsSavedTerminalOutcomeAndDrainsPiggybackAckAtCapacity() throws Exception {
        try (var f=new Fixture(root)) {
            var two=f.ledger.issue(f.outer.b,50);
            f.catalogue.epochs.execute(f.catalogue.epochs.prepare(f.outer.operation(2),f.ledger,two)); f.ledger=two;
            var client=new RatisSessionClient(f.routed,two,two.grants().getLast(),new RatisSessionClient.Limits(2,1024,CommandResult.MAX_BYTES));
            var first=client.prepare(List.of(f.put(1)));
            f.routed.execute(client.prepareBinding(f.catalogue.epochs,f.admin(0),first,-1));
            assertEquals(CommandResult.Status.SUCCESS,client.execute(first).status()); client.acknowledge(first);
            f.expire(50); assertEquals(1,f.ledger.minimumEpoch(),"Earlier live grant pins the coarse floor");
            var advance=f.catalogue.epochs.prepareAuthorityAdvance(f.directory,f.outer.dataGroup,f.admin(1),0);
            assertEquals(CommandResult.Status.SUCCESS,f.routed.execute(f.routed.prepareSessionControl(advance)).status());
            assertEquals(2,f.store.receiptUsage().retainedCount());
            var returning=new Mutation(Mutation.Kind.PUT,f.outer.data,new byte[]{2},new byte[]{9},Mutation.ReturnMode.PREVIOUS);
            var expired=client.prepare(List.of(returning));
            var estimate=f.store.estimateReceiptRetention(expired.command(),true);
            assertTrue(estimate.newReceipt()); assertEquals(0,estimate.additionalCount()); assertEquals(0,estimate.additionalBytes());
            f.catalogue.unavailable=true;
            assertEquals(CommandResult.Status.SESSION_EXPIRED,client.execute(expired).status());
            assertEquals(2,f.store.receiptUsage().retainedCount());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,f.routed.execute(f.routed.prepare(expired.command().operationId(),List.of(returning),List.of(),0)).status());
            var swapped=f.routed.prepareAcknowledgement(expired.command().operationId(),f.outer.data,new byte[]{1},0);
            assertThrows(IOException.class,() -> f.routed.execute(swapped),"Saved hash prevents changing an expired write into an ACK with the same identity");
            var beyond=new DistributedCommand(f.outer.dataGroup,1,new byte[32],new OperationId(f.outer.b,2,50),List.of(f.put(3)));
            assertNotNull(f.propose(beyond).getException(),"Individual expiry cannot bypass the declared outstanding window");
            client.acknowledge(expired); assertEquals(0,client.outstanding()); assertEquals(1,client.acknowledgedThrough());
            try (var pin=f.store.capture()) {
                assertArrayEquals(new byte[]{7},pin.get(f.outer.data,new byte[]{1})); assertNull(pin.get(f.outer.data,new byte[]{2}));
            }
            var ack=client.prepareAcknowledgement(f.outer.data,new byte[]{1});
            assertEquals(CommandResult.Status.SUCCESS,client.execute(ack).status()); client.acknowledge(ack);
            assertEquals(2,f.store.receiptUsage().retainedCount()); f.assertNoReservations();
        }
    }

    @Test void bootstrapAndControlRoutingRejectWrongIdentityBeforeOwnershipOrEffects() throws Exception {
        try (var f=new Fixture(root)) {
            var bad=new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),UUID.randomUUID(),1,new byte[32],0,
                    f.directory.columns(),0,f.directory.ranges());
            assertThrows(IllegalArgumentException.class,() -> RatisRangeStateMachine.withSessions(f.store,f.group,Fixture.LIMITS,command -> {},
                    f.catalogue.epochs,bad,RatisRangeStateMachine.RetentionLimits.defaults()));
            var foreign=SessionEpochLedger.initial(UUID.randomUUID(),f.outer.database,10);
            var command=DistributedCommand.sessionControl(f.outer.dataGroup,1,new byte[32],f.admin(0),RangeSessionControl.advance(foreign),-1);
            assertThrows(IllegalArgumentException.class,() -> f.routed.prepareSessionControl(command));
            var ordinary=new DistributedCommand(f.outer.dataGroup,1,new byte[32],new OperationId(f.outer.a,1,0),List.of(f.put(1)));
            assertThrows(IllegalArgumentException.class,() -> f.routed.prepareSessionControl(ordinary));
            assertEquals(0,f.store.applied().index()); f.assertNoReservations();
        }
    }
    static final class Fixture implements AutoCloseable {
        static final NativeRangeImage.Limits LIMITS=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1);
        final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
        final RatisSessionEpochsTest.Fixture catalogue;
        SessionEpochLedger ledger;
        final RangeDirectory directory;
        final RaftPeer peer=RaftPeer.newBuilder().setId("data-fixture").build();
        final RaftGroup group;
        final RaftStorage storage; final ReplicatedRangeStore store; final RatisRangeStateMachine machine; final RatisRoutedClient routed;
        Fixture(Path root) throws Exception {
            catalogue=outer.new Fixture(root.resolve("catalogue")); directory=outer.directory(outer.cluster);
            var initial=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,initial));
            ledger=initial.issue(outer.a,100); catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(1),initial,ledger));
            group=RaftGroup.valueOf(RaftGroupId.valueOf(outer.dataGroup),peer);
            store=catalogue.pool.openRange(new RangeOwnership.Owner(outer.dataGroup,1),new RangeOwnership.Interval(directory.domainId(),Set.of(outer.data),null,null),
                    new byte[32],Map.of(outer.data,"data"),Set.of());
            machine=RatisRangeStateMachine.withSessions(store,group,LIMITS,command -> {},catalogue.epochs,directory,
                    new RatisRangeStateMachine.RetentionLimits(2,184,1,92),() -> new RatisRangeStateMachine.Leadership(1,true));
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                    .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
                if (method.getName().equals("getId")) return peer.getId();
                if (method.getName().equals("toString")) return "identity-fixture";
                throw new AssertionError("Unexpected server access "+method);
            });
            machine.initialize(identity,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
            routed=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    try { return apply(propose(command)); }
                    catch (IOException failure) { throw failure; }
                    catch (Exception failure) { throw new IOException(failure); }
                }
            });
        }
        OperationId admin(long sequence) { return new OperationId(outer.maintenance,2,sequence); }
        Mutation put(int key) { return new Mutation(Mutation.Kind.PUT,outer.data,new byte[]{(byte)key},new byte[]{7}); }
        void expire(long time) throws Exception {
            var next=ledger.advanceTime(time); catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(ledger.generation()+1),ledger,next)); ledger=next;
        }
        TransactionContext propose(DistributedCommand command) throws Exception {
            return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId())
                    .setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build());
        }
        CommandResult apply(TransactionContext context) throws Exception {
            if (context.getException()!=null) throw new IOException("Admission rejected",context.getException());
            machine.preAppendTransaction(context); context.initLogEntry(1,store.applied().index()+1);
            return CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray());
        }
        void assertNoReservations() throws Exception {
            for (String name:List.of("reservedReceiptCount","reservedReceiptBytes","dataAdmissions","protectedAdmissions")) {
                var field=RatisRangeStateMachine.class.getDeclaredField(name); field.setAccessible(true); assertEquals(0,((Number)field.get(machine)).longValue(),name);
            }
            var field=RatisRangeStateMachine.class.getDeclaredField("maintenanceReservation"); field.setAccessible(true); assertNull(field.get(machine));
        }
        public void close() throws Exception { routed.close(); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); storage.close(); catalogue.close(); }
    }
}
