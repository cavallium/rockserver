package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Native apply and real adapter admission, with an identity-only server and no socket or quorum. */
class RatisSchemaFenceAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int n) { return new UUID(0,n); }
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static final class Fixture implements AutoCloseable {
        final UUID groupId=id(9),column=id(4);
        final RaftPeer peer=RaftPeer.newBuilder().setId("fixture").setAddress("127.0.0.1:9999").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final DistributedSchema schema=new DistributedSchema(id(1),id(2),id(3),1,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),null)));
        final RangeDirectory directory=new RangeDirectory(id(1),id(2),id(3),1,schema.digest(),1,Set.of(column),0,List.of(new RangeDescriptor(id(8),groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(new ReplicaDescriptor(id(5),id(6),id(7),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER)))));
        final SchemaChange.Definition definition=new SchemaChange.Definition(id(11),new OperationId(id(10),1,1),directory,schema,new DistributedSchema(id(1),id(2),id(3),2,0,List.of()));
        final RatisSchemaFenceRecord record=new RatisSchemaFenceRecord(definition.header(),groupId);
        final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicReference<SchemaChange.Status> status=new AtomicReference<>(SchemaChange.Status.begin(definition.header()).beginFencing());
        final DistributedStoragePool pool; final RaftStorage storage; final ReplicatedRangeStore store; final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService();
        volatile Runnable beforeRead=() -> {};
        volatile RangeDirectory authorityDirectory=directory;
        Fixture(Path root) throws Exception { this(root,false); }
        Fixture(Path root,boolean catalogue) throws Exception {
            pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            if (catalogue) pool.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(1,1L<<30,java.time.Duration.ofSeconds(60),0));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(id(3),Set.of(column),null,null),schema.digest(),Map.of(column,"data"),Set.of()); store.enableLogicalSchema(schema);
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command -> {},command -> {},leader::get);
            machine.proofQueries(queries);
            if (catalogue) { machine.role(RatisNodePlan.Role.CATALOGUE); machine.schemaCatalogue(id(1),id(2),column); }
            else machine.schemaAuthority(header -> { beforeRead.run(); return new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(id(1),id(2),id(3)),id(20),new ApplyPosition(1,1),authorityDirectory,new RatisSchemaAdministration.Head(status.get(),1)); },directory);
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> { if (method.getName().equals("getId")) return peer.getId(); if (method.getName().equals("toString")) return "identity-fixture"; throw new AssertionError("Unexpected server access "+method); });
            machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
        }
        TransactionContext propose(byte[] bytes) throws Exception { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build()); }
        DistributedCommand putCommand(long sequence,int value) { return new DistributedCommand(groupId,1,schema.digest(),new OperationId(id(30),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{(byte)value}))); }
        TransactionContext put(long sequence) throws Exception { return propose(DistributedCommandCodec.encode(putCommand(sequence,2))); }
        CompletableFuture<Message> prepare() { return machine.queryLinearizable(message(RatisSchemaFenceRpc.prepareRequest(record))); }
        void cancel() throws Exception { machine.queryLinearizable(message(RatisSchemaFenceRpc.cancelRequest(record))).get(5,TimeUnit.SECONDS); }
        RatisSchemaFenceRpc.State state() throws Exception { return RatisSchemaFenceRpc.stateReply(machine.queryLinearizable(message(RatisSchemaFenceRpc.stateRequest(record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),ScopedTransactionRpc.SCHEMA_FENCE_STATE,record); }
        NativeSchemaFence apply(TransactionContext tx,long index) throws Exception { assertNull(tx.getException()); machine.preAppendTransaction(tx); tx.initLogEntry(leader.get().term(),index); return NativeSchemaFence.decode(machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray()); }
        public void close() throws Exception { queries.stop().get(5,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); storage.close(); pool.close(); }
    }
    @Test void catalogueAuthorityReadsServerBoundDirectoryAndHeadWithoutSnapshotQuota() throws Exception {
        try (var f=new Fixture(root,true)) {
            byte[] directoryKey=java.nio.ByteBuffer.allocate(16).putLong(id(3).getMostSignificantBits()).putLong(id(3).getLeastSignificantBits()).array();
            var prepared=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(f.definition.header()),1);
            var initial=new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(id(30),1,0),List.of(
                    new Mutation(Mutation.Kind.PUT,f.column,directoryKey,RangeDirectoryCodec.encode(f.directory)),
                    new Mutation(Mutation.Kind.PUT,f.column,RatisSchemaAdministration.headKey(id(3)),prepared.encode())));
            var tx=f.propose(DistributedCommandCodec.encode(initial)); assertNull(tx.getException()); f.machine.preAppendTransaction(tx); tx.initLogEntry(1,1);
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            var request=new RatisSchemaAuthority.Request(id(1),id(2),id(3)); byte[] encoded=RatisSchemaAuthority.request(request);
            assertEquals(ScopedTransactionRpc.HEADER_BYTES+48,encoded.length,"Wire request carries database/domain identity and no column selector");
            try (var pin=f.store.capture()) {
                assertEquals(1,f.store.activeSnapshotPins()); assertThrows(RuntimeException.class,f.store::capture);
                var view=RatisSchemaAuthority.decodeReply(f.machine.queryLinearizable(message(encoded)).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request,f.groupId);
                assertEquals(new ApplyPosition(1,1),view.position()); assertArrayEquals(RangeDirectoryCodec.encode(f.directory),RangeDirectoryCodec.encode(view.directory())); assertArrayEquals(prepared.encode(),view.head().encode());
                assertEquals(1,f.store.activeSnapshotPins(),"Compound authority query allocates no native pin");
                var fencing=new RatisSchemaAdministration.Head(prepared.status().beginFencing(),1);
                var update=new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(id(30),1,1),List.of(new Mutation(Mutation.Kind.PUT,f.column,RatisSchemaAdministration.headKey(id(3)),fencing.encode())));
                var next=f.propose(DistributedCommandCodec.encode(update)); assertNull(next.getException()); f.machine.preAppendTransaction(next); next.initLogEntry(1,2);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(next).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
                var current=RatisSchemaAuthority.decodeReply(f.machine.queryLinearizable(message(encoded)).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request,f.groupId);
                assertEquals(new ApplyPosition(1,2),current.position()); assertArrayEquals(fencing.encode(),current.head().encode()); assertArrayEquals(RangeDirectoryCodec.encode(f.directory),RangeDirectoryCodec.encode(current.directory()));
                assertArrayEquals(prepared.encode(),pin.get(f.column,RatisSchemaAdministration.headKey(id(3))),"Held historical pin does not dictate authority cut");
                assertEquals(1,f.store.activeSnapshotPins());
                assertThrows(ExecutionException.class,() -> f.machine.query(message(encoded)).get(5,TimeUnit.SECONDS));
                for (var foreign:List.of(new RatisSchemaAuthority.Request(id(99),id(2),id(3)),new RatisSchemaAuthority.Request(id(1),id(99),id(3))))
                    assertThrows(ExecutionException.class,() -> f.machine.queryLinearizable(message(RatisSchemaAuthority.request(foreign))).get(5,TimeUnit.SECONDS));
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void lookupReservesGateBeforeNetworkAndPredecessorsRetainDrainRights() throws Exception {
        try (var f=new Fixture(root)) {
            var old=f.put(0); assertNull(old.getException()); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("lookup stalled"); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var pending=f.prepare();
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertNotNull(f.put(1).getException()); assertNotNull(f.propose(f.record.encode()).getException()); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
            assertNotNull(f.put(2).getException(),"Qualified gate stays closed while old accepted proposal drains");
            f.machine.cancelTransaction(old); f.beforeRead=() -> {}; f.prepare().get(5,TimeUnit.SECONDS);
            assertNull(f.state().fence()); f.cancel(); var next=f.put(3); assertNull(next.getException()); f.machine.cancelTransaction(next);
        }
    }
    @Test void irreversibleAuthorityNativeProofPinsHealthSnapshotAndRetry() throws Exception {
        try (var f=new Fixture(root)) {
            assertNotNull(f.propose(f.record.encode()).getException());
            f.status.set(SchemaChange.Status.begin(f.definition.header())); assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS));
            f.status.set(SchemaChange.Status.begin(f.definition.header()).abort()); assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS));
            var wrongDefinition=new SchemaChange.Definition(id(99),new OperationId(id(10),1,2),f.directory,f.schema,f.definition.targetSchema());
            f.status.set(SchemaChange.Status.begin(wrongDefinition.header()).beginFencing()); assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS));
            f.status.set(SchemaChange.Status.begin(f.definition.header()).beginFencing());
            f.authorityDirectory=new RangeDirectory(id(1),id(2),id(3),1,f.schema.digest(),2,Set.of(f.column),0,f.directory.ranges());
            assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS)); f.authorityDirectory=f.directory;
            try (var pin=f.store.capture()) {
                f.prepare().get(5,TimeUnit.SECONDS); var proof=f.apply(f.propose(f.record.encode()),1); assertEquals(new ApplyPosition(1,1),proof.position());
                assertEquals(proof,f.state().fence()); assertFalse(f.state().ready()); assertEquals(1,f.state().snapshotPins());
                assertThrows(IllegalStateException.class,f.store::capture); assertNotNull(f.put(1).getException());
                assertNotNull(f.machine.queryLinearizable(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))).get(5,TimeUnit.SECONDS));
                assertEquals(1,f.machine.takeSnapshot());
                f.beforeRead=() -> { throw new AssertionError("Native retry must not recapture authority"); }; f.prepare().get(5,TimeUnit.SECONDS);
                assertEquals(proof,f.apply(f.propose(f.record.encode()),2));
            }
            assertTrue(f.state().ready());
            f.machine.pause(); f.machine.reinitialize(); assertEquals(new ApplyPosition(1,1),f.state().fence().position());
        }
    }
    @Test void addressOnlyConfigurationChangeRejectsFreshFencePreparation() throws Exception {
        try (var f=new Fixture(root)) {
            var changed=RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(f.peer.getId()).setAddress("127.0.0.1:10000").build().getRaftPeerProto()).build();
            f.machine.notifyConfigurationChanged(1,1,changed); f.machine.notifyTermIndexUpdated(1,1);
            var failure=assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS));
            assertTrue(failure.getCause().getMessage().contains("peer address/role configuration changed"));
            assertNull(f.store.schemaFence());
            var restored=RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build();
            f.machine.notifyConfigurationChanged(1,2,restored); f.machine.notifyTermIndexUpdated(1,2);
            f.prepare().get(5,TimeUnit.SECONDS);
        }
    }
    @Test void queuedConfigurationInvalidatesFenceAuthorityBeforeNativeApply() throws Exception {
        try (var f=new Fixture(root.resolve("fence"))) {
            f.prepare().get(5,TimeUnit.SECONDS); var accepted=f.propose(f.record.encode()); assertNull(accepted.getException());
            var changed=RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto())
                    .addPeers(RaftPeer.newBuilder().setId("replacement").build().getRaftPeerProto()).build();
            long previousConfigurationIndex=f.store.configuration().index();
            f.machine.notifyConfigurationChanged(1,1,changed);
            assertEquals(previousConfigurationIndex,f.store.configuration().index(),"Queued configuration has not reached native apply");
            assertNotNull(f.propose(f.record.encode()).getException());
            assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,() -> f.machine.preAppendTransaction(accepted));
            assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS));
            assertNull(f.store.schemaFence());
            f.machine.notifyTermIndexUpdated(1,1); assertEquals(1,f.store.configuration().index());
            assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS),"Old qualified intent cannot fence new native membership");
            assertNull(f.store.schemaFence());
        }
        try (var f=new Fixture(root.resolve("ordinary"))) {
            var changed=RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto())
                    .addPeers(RaftPeer.newBuilder().setId("replacement").build().getRaftPeerProto()).build();
            var old=f.put(0); assertNull(old.getException()); f.machine.notifyConfigurationChanged(1,1,changed);
            f.machine.preAppendTransaction(old); // The queue-time stamp is schema authority only.
            f.machine.cancelTransaction(old); var fresh=f.put(1); assertNull(fresh.getException()); f.machine.cancelTransaction(fresh);
        }
    }
    @Test void lostAppliedDataReplyRetriesThroughAdmissionAfterNativeFence() throws Exception {
        try (var f=new Fixture(root)) {
            var original=f.putCommand(0,2); var accepted=f.propose(DistributedCommandCodec.encode(original)); assertNull(accepted.getException());
            f.machine.preAppendTransaction(accepted); accepted.initLogEntry(1,1);
            byte[] retained=f.machine.applyTransaction(accepted).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(retained).status());
            // The DATA reply was lost; its native receipt is the only authority for this retry.
            f.prepare().get(5,TimeUnit.SECONDS); f.apply(f.propose(f.record.encode()),2);
            var retry=f.propose(DistributedCommandCodec.encode(original)); assertNull(retry.getException());
            f.machine.preAppendTransaction(retry); retry.initLogEntry(1,3);
            assertArrayEquals(retained,f.machine.applyTransaction(retry).get(5,TimeUnit.SECONDS).getContent().toByteArray());
            assertNotNull(f.propose(DistributedCommandCodec.encode(f.putCommand(0,3))).getException(),"Same operation with changed payload cannot borrow the retained receipt");
            assertNotNull(f.put(1).getException(),"Fresh DATA remains fenced");
            assertEquals(new ApplyPosition(1,2),f.state().fence().position());
        }
    }
    @Test void unknownAppendKeepsGateUntilStrictlyNewerReadyPrefix() throws Exception {
        try (var f=new Fixture(root)) {
            f.prepare().get(5,TimeUnit.SECONDS); var tx=f.propose(f.record.encode()); assertNull(tx.getException()); f.machine.preAppendTransaction(tx); tx.initLogEntry(1,1);
            f.machine.notifyNotLeader(List.of(tx)); f.cancel(); assertNotNull(f.put(0).getException());
            f.leader.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,1);
            var next=f.put(1); assertNull(next.getException()); f.machine.cancelTransaction(next);
        }
    }
    @Test void cancellingObserverDoesNotDonateTheRunningGate() throws Exception {
        try (var f=new Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeRead=() -> { entered.countDown(); try { release.await(5,TimeUnit.SECONDS); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var observer=f.prepare();
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(observer.cancel(false));
                assertNotNull(f.put(0).getException());
                assertThrows(ExecutionException.class,() -> f.machine.query(message(RatisSchemaFenceRpc.prepareRequest(f.record))).get(5,TimeUnit.SECONDS));
            } finally { release.countDown(); }
            for (int i=0;i<100 && f.queries.stats().jobs()!=0;i++) Thread.sleep(10);
            f.beforeRead=() -> {}; f.prepare().get(5,TimeUnit.SECONDS); f.cancel();
        }
    }
    @Test void cancelledLookupKeepsItsInputChargedUntilReturn() throws Exception {
        try (var f=new Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeRead=() -> { entered.countDown(); try { release.await(5,TimeUnit.SECONDS); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var pending=f.prepare();
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); f.cancel(); assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS)); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS));
            f.beforeRead=() -> {}; f.prepare().get(5,TimeUnit.SECONDS); f.cancel();
        }
    }
}
