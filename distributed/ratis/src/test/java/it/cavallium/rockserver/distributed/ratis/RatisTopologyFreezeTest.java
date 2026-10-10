package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native and Raft adapter lifetimes with controlled authority; no server sockets or quorum. */
class RatisTopologyFreezeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long n) { return new UUID(0,n); }
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static void await(CountDownLatch latch) { try { if(!latch.await(5,TimeUnit.SECONDS)) throw new AssertionError("Topology fixture timed out"); } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); } }
    static final class Fixture implements AutoCloseable {
        final UUID groupId=id(9),column=id(4);
        final ReplicaDescriptor local=new ReplicaDescriptor(id(5),id(6),id(7),"topology-parent","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        final RaftPeer peer=RaftPeer.newBuilder().setId(local.peerId()).setAddress(local.address()).build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final DistributedSchema schema=new DistributedSchema(id(1),id(2),id(3),1,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),null)));
        final RangeDescriptor parent=new RangeDescriptor(id(8),groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
        final RangeDirectory directory=new RangeDirectory(id(1),id(2),id(3),1,schema.digest(),1,Set.of(column),0,List.of(parent));
        final TopologyChange.Definition definition=new TopologyChange.Definition(id(70),directory,new RangeDirectory(id(1),id(2),id(3),1,schema.digest(),2,Set.of(column),0,List.of(new RangeDescriptor(id(81),id(91),1,1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(local)),new RangeDescriptor(id(82),id(92),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local)))));
        final RatisTopologyAuthority.View view=new RatisTopologyAuthority.View(id(20),new ApplyPosition(1,1),definition,TopologyChange.State.begin(definition.header()).beginFencing());
        final RatisTopologyFreezeRecord record=new RatisTopologyFreezeRecord(definition,groupId,view.admissionDigest());
        final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicInteger reads=new AtomicInteger();
        final DistributedStoragePool pool;
        final ReplicatedRangeStore store;
        final RaftStorage storage;
        final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService();
        volatile Runnable beforeRead=()->{};
        volatile Runnable afterProof=()->{};
        volatile Runnable beforeProof=()->{};
        volatile boolean unavailable;
        Fixture(Path root) throws Exception {
            pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(id(3),Set.of(column),null,null),schema.digest(),Map.of(column,"data"),Set.of()); store.enableLogicalSchema(schema); store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); storage.initialize();
            machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command->{},command->{},leader::get);
            machine.role(RatisNodePlan.Role.DATA); machine.proofQueries((groupId,input,weight,coalesce,action)->queries.submit(groupId,input,weight,coalesce,owned->{ beforeProof.run(); try { return action.run(owned); } finally { afterProof.run(); } }));
            machine.topologyAuthority((header,stage)->{ assertEquals(TopologyChange.Stage.FREEZE,stage); reads.incrementAndGet(); beforeRead.run(); if(unavailable)throw new java.io.IOException("Authority unavailable"); return view; },directory,local,pool);
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{ if(method.getName().equals("getId"))return peer.getId(); if(method.getName().equals("toString"))return "topology-freeze-fixture"; throw new AssertionError(method); });
            machine.initialize(identity,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
        }
        TransactionScope point() { return new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))); }
        TransactionScope range() { return new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{1},new byte[]{2}))); }
        DistributedCommand put(long sequence,int value) { return new DistributedCommand(groupId,1,schema.digest(),new OperationId(id(30),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{(byte)value}))); }
        TransactionContext propose(byte[] bytes) throws java.io.IOException { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build()); }
        CompletableFuture<Message> apply(TransactionContext transaction,long index) throws Exception { assertNull(transaction.getException()); machine.preAppendTransaction(transaction); transaction.initLogEntry(leader.get().term(),index); return machine.applyTransaction(transaction); }
        CompletableFuture<Message> replay(byte[] bytes,long term,long index) {
            var transaction=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(term).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(bytes))).build()).build(); return machine.applyTransaction(transaction);
        }
        CompletableFuture<Message> preparing() { return machine.queryLinearizable(message(RatisTopologyFreezeRpc.prepareRequest(record))); }
        RatisTopologyFreezeRpc.State prepare() throws Exception { return RatisTopologyFreezeRpc.stateReply(preparing().get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyFreezeRpc.PREPARE,record); }
        void cancel() throws Exception { RatisTopologyFreezeRpc.decodeCancelledReply(machine.queryLinearizable(message(RatisTopologyFreezeRpc.cancelRequest(record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),record); machine.topologyCleanup().get(5,TimeUnit.SECONDS); }
        NativeTopologyFreeze freeze(long index) throws Exception { return NativeTopologyFreeze.decode(apply(propose(record.encode()),index).get(5,TimeUnit.SECONDS).getContent().toByteArray()); }
        @Override public void close() throws Exception { machine.close(); machine.topologyCleanup().get(5,TimeUnit.SECONDS); queries.stop().get(5,TimeUnit.SECONDS); machine.awaitNativeReaders(Duration.ofSeconds(5)); storage.close(); pool.close(); }
    }
    @Test void preparedFreezeKeepsOriginalOutcomeAndRetryNeedsNoCatalogue() throws Exception {
        try(var f=new Fixture(root)) {
            var original=f.put(0,9); f.apply(f.propose(DistributedCommandCodec.encode(original)),1).get(5,TimeUnit.SECONDS);
            assertNotNull(f.propose(f.record.encode()).getException()); assertTrue(f.prepare().ready()); assertTrue(f.prepare().ready()); assertEquals(1,f.reads.get());
            var freeze=f.freeze(2); assertEquals(new ApplyPosition(1,2),freeze.position()); f.machine.topologyCleanup().get(5,TimeUnit.SECONDS);
            assertEquals(0,f.machine.topologyFreezeStats().holds()); assertNotNull(f.propose(DistributedCommandCodec.encode(f.put(1,8))).getException());
            f.unavailable=true; assertEquals(freeze,f.freeze(3)); assertEquals(1,f.reads.get());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(f.propose(DistributedCommandCodec.encode(original)),4).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertArrayEquals(new byte[]{9},f.store.readCurrentPoint(f.column,new byte[]{1}));
            assertThrows(ExecutionException.class,()->f.machine.query(message(RatisTopologyFreezeRpc.prepareRequest(f.record))).get(5,TimeUnit.SECONDS));
            var status=RatisTopologyFreezeRpc.stateReply(f.machine.query(message(RatisTopologyFreezeRpc.stateRequest(f.record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyFreezeRpc.STATE,f.record);
            assertEquals(freeze,status.freeze());
        }
    }
    @Test void acceptedDataPredecessorFinishesBeforeNativeGateAndStableFreezeCut() throws Exception {
        try(var f=new Fixture(root)) {
            var previous=f.propose(DistributedCommandCodec.encode(f.put(0,9))); assertNull(previous.getException());
            assertFalse(f.prepare().ready()); assertNotNull(f.propose(DistributedCommandCodec.encode(f.put(1,8))).getException());
            f.apply(previous,1).get(5,TimeUnit.SECONDS); assertTrue(f.prepare().ready()); var freeze=f.freeze(2);
            assertEquals(new ApplyPosition(1,2),freeze.position()); assertArrayEquals(new byte[]{9},f.store.readCurrentPoint(f.column,new byte[]{1}));
        }
    }
    @Test void cancellationDuringActualAuthorityLookupRetainsOwnershipUntilReturn() throws Exception {
        try(var f=new Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); f.beforeRead=()->{ entered.countDown(); await(release); };
            var preparing=f.preparing(); await(entered);
            try {
                var cancelled=f.machine.queryLinearizable(message(RatisTopologyFreezeRpc.cancelRequest(f.record))).get(5,TimeUnit.SECONDS); RatisTopologyFreezeRpc.decodeCancelledReply(cancelled.getContent().toByteArray(),f.record);
                assertEquals(1,f.machine.topologyFreezeStats().holds()); assertEquals(1,f.machine.topologyFreezeStats().reading()); assertTrue(f.machine.topologyFreezeStats().bytes()>0);
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,()->preparing.get(5,TimeUnit.SECONDS)); f.machine.topologyCleanup().get(5,TimeUnit.SECONDS); assertEquals(0,f.machine.topologyFreezeStats().holds());
            f.beforeRead=()->{}; assertTrue(f.prepare().ready()); f.cancel();
            assertNull(f.propose(DistributedCommandCodec.encode(f.put(0,8))).getException());
        }
    }
    @Test void changedConfigurationRejectsPreparedAppendAndReleasesUnsubmittedGate() throws Exception {
        try(var f=new Fixture(root)) {
            assertTrue(f.prepare().ready()); var transaction=f.propose(f.record.encode()); assertNull(transaction.getException());
            f.machine.notifyConfigurationChanged(1,1,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());
            assertThrows(java.io.IOException.class,()->f.machine.preAppendTransaction(transaction)); f.machine.topologyCleanup().get(5,TimeUnit.SECONDS);
            assertNull(f.store.topologyFreeze()); assertEquals(0,f.machine.topologyFreezeStats().holds());
        }
    }
    @Test void followerWaitsForActualBusyPinAndQueuesLaterApplyWithoutParkingProofLane() throws Exception {
        try(var f=new Fixture(root);var executor=Executors.newSingleThreadExecutor()) {
            f.apply(f.propose(DistributedCommandCodec.encode(f.put(0,9))),1).get(5,TimeUnit.SECONDS); var pin=f.store.captureTracked(f.range()); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            var read=executor.submit(()->{ pin.visitColumn(f.column,new byte[]{1},new byte[]{2},(key,value)->{ entered.countDown(); await(release); }); return null; }); await(entered);
            try {
                f.leader.set(new RatisRangeStateMachine.Leadership(2,false)); f.unavailable=true; var applied=f.replay(f.record.encode(),2,2); f.machine.notifyTermIndexUpdated(2,3); pin.close();
                assertFalse(applied.isDone()); assertEquals(new ApplyPosition(1,1),f.store.applied());
                f.queries.submit(f.groupId,new byte[]{1},1,false,bytes->Message.EMPTY).get(5,TimeUnit.SECONDS);
                release.countDown(); read.get(5,TimeUnit.SECONDS); var freeze=NativeTopologyFreeze.decode(applied.get(5,TimeUnit.SECONDS).getContent().toByteArray());
                assertEquals(new ApplyPosition(2,2),freeze.position()); assertEquals(new ApplyPosition(2,3),f.store.applied()); assertEquals(0,f.reads.get());
            } finally { release.countDown(); pin.close(); }
        }
    }
    @Test void protectedPreparedFinalizationDrainsBeforeFreezeCanAppend() throws Exception {
        try(var f=new Fixture(root)) {
            DistributedCommand accepted; try(var pin=f.store.captureTracked(f.point())) { accepted=f.store.planScopedCommit(pin,f.put(0,9),0,id(99)); }
            var participant=new TransactionStatus.Participant(id(8),f.groupId,1,f.schema.digest(),PreparedParticipant.Preparation.planDigest(accepted),null,false);
            var registration=TransactionStatus.begin(id(1),id(2),id(3),id(101),accepted.operationId(),1,f.schema.digest(),f.directory.payloadDigest(),List.of(participant));
            var preparation=new PreparedParticipant.Preparation(id(100),1,new OperationId(id(102),1,0),registration,accepted);
            f.replay(RatisParticipantRecord.encode(preparation),1,1).get(5,TimeUnit.SECONDS); assertFalse(f.prepare().ready()); assertNotNull(f.propose(f.record.encode()).getException());
            var decision=registration.prepared(f.groupId,participant.planDigest(),new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
            var finalize=new ParticipantFinalization(id(100),1,f.groupId,new OperationId(id(150),3,0),decision);
            f.replay(RatisParticipantRecord.encode(finalize),1,2).get(5,TimeUnit.SECONDS); assertTrue(f.prepare().ready()); var freeze=f.freeze(3);
            assertEquals(new ApplyPosition(1,3),freeze.position()); assertArrayEquals(new byte[]{9},f.store.readCurrentPoint(f.column,new byte[]{1}));
        }
    }
    @Test void receivedSnapshotRecoversCoveredFreezeOrReplaysUncoveredPendingFreeze() throws Exception {
        for(boolean covered:List.of(false,true)) {
            Path directory=root.resolve("snapshot-"+covered);
            try(var donor=new Fixture(directory.resolve("donor"));var receiver=new Fixture(directory.resolve("receiver"));var executor=Executors.newFixedThreadPool(2)) {
                donor.apply(donor.propose(DistributedCommandCodec.encode(donor.put(0,9))),1).get(5,TimeUnit.SECONDS);
                NativeTopologyFreeze expected=null;
                if(covered) { assertTrue(donor.prepare().ready()); expected=donor.freeze(2); donor.machine.notifyTermIndexUpdated(1,3); }
                long snapshotIndex=covered ? 3 : 1; assertEquals(snapshotIndex,donor.machine.takeSnapshot());
                receiver.apply(receiver.propose(DistributedCommandCodec.encode(receiver.put(0,9))),1).get(5,TimeUnit.SECONDS);
                assertArrayEquals(new byte[]{9},receiver.store.readCurrentPoint(receiver.column,new byte[]{1}));
                var pin=receiver.store.captureTracked(receiver.range()); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var firstApply=new CountDownLatch(1);
                receiver.afterProof=firstApply::countDown; receiver.unavailable=true;
                var reader=executor.submit(()->{ pin.visitColumn(receiver.column,new byte[]{1},new byte[]{2},(key,value)->{ entered.countDown(); await(release); }); return null; }); await(entered);
                try {
                    var pending=receiver.replay(receiver.record.encode(),1,2); await(firstApply); assertFalse(pending.isDone()); receiver.machine.notifyTermIndexUpdated(1,3);
                    var source=donor.machine.getStateMachineStorage().getSnapshotFile(1,snapshotIndex).toPath();
                    var destination=receiver.machine.getStateMachineStorage().getSnapshotFile(1,snapshotIndex).toPath(); java.nio.file.Files.copy(source,destination);
                    var paused=executor.submit(()->{ receiver.machine.pause(); return null; });
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                    while(receiver.machine.getLifeCycleState()==org.apache.ratis.util.LifeCycle.State.RUNNING && System.nanoTime()<deadline) Thread.onSpinWait();
                    assertNotEquals(org.apache.ratis.util.LifeCycle.State.RUNNING,receiver.machine.getLifeCycleState());
                    release.countDown(); reader.get(5,TimeUnit.SECONDS); paused.get(5,TimeUnit.SECONDS); pin.close(); receiver.machine.reinitialize();
                    var actual=NativeTopologyFreeze.decode(pending.get(5,TimeUnit.SECONDS).getContent().toByteArray());
                    if(covered) assertEquals(expected,actual); assertEquals(new ApplyPosition(1,2),actual.position()); assertEquals(new ApplyPosition(1,3),receiver.store.applied()); assertEquals(0,receiver.reads.get());
                    assertArrayEquals(new byte[]{9},receiver.store.readCurrentPoint(receiver.column,new byte[]{1}));
                    var state=RatisTopologyFreezeRpc.stateReply(receiver.machine.queryLinearizable(message(RatisTopologyFreezeRpc.stateRequest(receiver.record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyFreezeRpc.STATE,receiver.record);
                    assertEquals(actual,state.freeze()); assertTrue(state.ready());
                } finally { release.countDown(); pin.close(); }
            }
        }
    }
    @Test void delayedOldReadinessAndFailureCannotActOnReplacementWork() throws Exception {
        try(var f=new Fixture(root)) {
            assertEquals(0,f.machine.takeSnapshot()); var pin=f.store.captureTracked(f.point()); var first=new CountDownLatch(1); f.afterProof=first::countDown; f.unavailable=true;
            var pending=f.replay(f.record.encode(),1,1); await(first);
            var applyField=RatisRangeStateMachine.class.getDeclaredField("nativeApply"); applyField.setAccessible(true); var work=applyField.get(f.machine);
            var generationField=work.getClass().getDeclaredField("topologyGeneration"); generationField.setAccessible(true); long oldGeneration=generationField.getLong(work);
            f.machine.pause(); pin.close();
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(f.queries.stats().jobs()!=0 && System.nanoTime()<deadline) Thread.onSpinWait(); assertEquals(0,f.queries.stats().jobs());
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); f.beforeProof=()->{ entered.countDown(); await(release); };
            try {
                f.machine.reinitialize(); await(entered); assertSame(work,applyField.get(f.machine)); assertTrue(generationField.getLong(work)>oldGeneration);
                // Hold the replacement action and deliver callbacks carrying the old captured generation.
                var schedule=RatisRangeStateMachine.class.getDeclaredMethod("scheduleNativeApply",work.getClass(),boolean.class,long.class); schedule.setAccessible(true); schedule.invoke(f.machine,work,true,oldGeneration);
                var failure=RatisRangeStateMachine.class.getDeclaredMethod("failNativeApply",work.getClass(),Throwable.class,long.class); failure.setAccessible(true); failure.invoke(f.machine,work,new java.io.IOException("Old readiness cancellation"),oldGeneration);
                assertEquals(1,f.queries.stats().jobs()); assertFalse(pending.isDone()); assertNull(f.store.topologyFreeze());
            } finally { release.countDown(); }
            var freeze=NativeTopologyFreeze.decode(pending.get(5,TimeUnit.SECONDS).getContent().toByteArray()); assertEquals(new ApplyPosition(1,1),freeze.position());
            assertNull(f.store.readCurrentPoint(f.column,new byte[]{1}));
            var status=RatisTopologyFreezeRpc.stateReply(f.machine.queryLinearizable(message(RatisTopologyFreezeRpc.stateRequest(f.record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyFreezeRpc.STATE,f.record); assertEquals(freeze,status.freeze());
        }
    }
}
