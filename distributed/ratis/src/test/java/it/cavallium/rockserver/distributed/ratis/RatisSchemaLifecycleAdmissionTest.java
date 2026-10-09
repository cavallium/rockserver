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

/** Real native generation changes through the adapter; the Raft server supplies identity only. */
class RatisSchemaLifecycleAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int n) { return new UUID(0,n); }
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static DistributedSchema.Column column(long api,UUID id,String name) {
        return new DistributedSchema.Column(api,id,name,ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),null);
    }
    static final class Fixture implements AutoCloseable {
        final UUID groupId=id(9),column=id(4),added=id(40);
        final RaftPeer peer=RaftPeer.newBuilder().setId("fixture").setAddress("127.0.0.1:9999").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final DistributedSchema source=new DistributedSchema(id(1),id(2),id(3),1,0,List.of(column(1,column,"data")));
        final RangeDirectory sourceDirectory=new RangeDirectory(id(1),id(2),id(3),1,source.digest(),1,Set.of(column),0,List.of(new RangeDescriptor(id(8),groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(new ReplicaDescriptor(id(5),id(6),id(7),"fixture","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER)))));
        final DistributedSchema target=new DistributedSchema(id(1),id(2),id(3),2,0,List.of(column(1,column,"data"),column(2,added,"added")));
        final SchemaChange.Definition create=new SchemaChange.Definition(id(11),new OperationId(id(10),1,1),sourceDirectory,source,target);
        final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicReference<SchemaChange.Status> status=new AtomicReference<>(SchemaChange.Status.begin(create.header()).beginFencing(new byte[32]));
        final AtomicReference<RangeDirectory> directory=new AtomicReference<>(sourceDirectory);
        final DistributedStoragePool pool; final ReplicatedRangeStore store; final RaftStorage storage; final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService();
        final AtomicReference<ApplyPosition> authorityPosition=new AtomicReference<>(new ApplyPosition(1,10));
        volatile UUID authorityGroup=id(20);
        volatile Runnable beforeRead=() -> {};
        volatile Runnable onActivation=() -> {};
        final java.util.concurrent.atomic.AtomicInteger activations=new java.util.concurrent.atomic.AtomicInteger();
        long index;
        final AtomicReference<SessionEpochLedger> ledger=new AtomicReference<>(SessionEpochLedger.initial(id(1),id(2),10));
        final java.util.concurrent.atomic.AtomicInteger epochReads=new java.util.concurrent.atomic.AtomicInteger();
        Fixture(Path root) throws Exception { this(root,false); }
        Fixture(Path root,boolean sessions) throws Exception { this(root,sessions,java.util.function.UnaryOperator.identity()); }
        Fixture(Path root,boolean sessions,java.util.function.UnaryOperator<DistributedStoragePool> storageView) throws Exception {
            pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(id(3),Set.of(column),null,null),source.digest(),Map.of(column,"data"),Set.of()); store.enableLogicalSchema(source);
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            if (sessions) {
                var io=(org.apache.ratis.client.api.BlockingApi)Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args) -> {
                    if (!method.getName().equals("sendReadOnly")) throw new AssertionError("Unexpected epoch call "+method);
                    epochReads.incrementAndGet(); byte[] bytes=ledger.get().encode();
                    byte[] reply=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,java.nio.ByteBuffer.allocate(4+bytes.length).putInt(bytes.length).put(bytes).array());
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(RaftGroupId.valueOf(id(20))).setCallId(1).setSuccess(true).setMessage(message(reply)).build();
                });
                var client=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args) -> {
                    if (method.getName().equals("getGroupId")) return RaftGroupId.valueOf(id(20)); if (method.getName().equals("io")) return io;
                    throw new AssertionError("Unexpected epoch client call "+method);
                });
                var epochs=new RatisSessionEpochs(client,id(1),id(2),id(20),1,new byte[32],id(21));
                machine=RatisRangeStateMachine.withSessions(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),command -> {},epochs,sourceDirectory,RatisRangeStateMachine.RetentionLimits.defaults(),leader::get);
            } else machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),command -> {},command -> {},leader::get);
            machine.schemaStorage(storageView.apply(pool)); machine.proofQueries(queries);
            machine.schemaActivated((definition,proof) -> { activations.incrementAndGet(); onActivation.run(); });
            machine.schemaAuthority((header,digest) -> { beforeRead.run(); return new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(id(1),id(2),id(3)),authorityGroup,authorityPosition.get(),directory.get(),new RatisSchemaAdministration.Head(status.get(),2)); },sourceDirectory);
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> { if (method.getName().equals("getId")) return peer.getId(); if (method.getName().equals("toString")) return "identity-schema-fixture"; throw new AssertionError("Unexpected server access "+method); });
            machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
        }
        TransactionContext propose(byte[] bytes) throws Exception {
            return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build());
        }
        byte[] apply(byte[] bytes) throws Exception {
            var tx=propose(bytes); assertNull(tx.getException()); machine.preAppendTransaction(tx); tx.initLogEntry(leader.get().term(),++index);
            return machine.applyTransaction(tx).get(10,TimeUnit.SECONDS).getContent().toByteArray();
        }
        void query(byte[] bytes) throws Exception { machine.queryLinearizable(message(bytes)).get(10,TimeUnit.SECONDS); }
        NativeSchemaFence fence(SchemaChange.Definition definition) throws Exception {
            var record=new RatisSchemaFenceRecord(definition.header(),groupId); query(RatisSchemaFenceRpc.prepareRequest(record));
            var proof=NativeSchemaFence.decode(apply(record.encode())); status.set(status.get().fenced(groupId,proof.position())); return proof;
        }
        RatisSchemaInstallRecord installRecord(SchemaChange.Definition definition,NativeSchemaFence fence) {
            return new RatisSchemaInstallRecord(definition,groupId,fence.position(),definition.header().kind()==SchemaChange.Kind.CREATE_COLUMN ? "added-physical" : null,false);
        }
        NativeSchemaInstallProof install(RatisSchemaInstallRecord record) throws Exception {
            query(RatisSchemaInstallRpc.prepareRequest(record)); var proof=NativeSchemaInstallProof.decode(apply(record.encode())); status.set(status.get().installed(groupId,proof.position())); return proof;
        }
        RatisSchemaActivateRecord activateRecord(SchemaChange.Definition definition,NativeSchemaInstallProof install) throws Exception {
            status.set(status.get().publish()); directory.set(definition.targetDirectory());
            var request=new RatisSchemaActivateRpc.Request(definition.header(),groupId,install.position());
            var reply=machine.queryLinearizable(message(RatisSchemaActivateRpc.prepareRequest(request))).get(10,TimeUnit.SECONDS).getContent().toByteArray();
            return RatisSchemaActivateRpc.decodePreparedReply(reply,ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request);
        }
        DistributedCommand put(DistributedSchema schema,UUID column,long sequence,int value) {
            return new DistributedCommand(groupId,1,schema.digest(),new OperationId(id(30),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{(byte)value})));
        }
        public void close() throws Exception { queries.stop().get(10,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); storage.close(); pool.close(); }
    }
    @Test void frozenActivationSurvivesLostPreparationReplyAndLaterAuthorityCutWithoutChangingBytes() throws Exception {
        try (var f=new Fixture(root)) {
            var fence=f.fence(f.create); var installed=f.install(f.installRecord(f.create,fence));
            var original=f.activateRecord(f.create,installed); byte[] frozen=original.encode();
            var base=new RatisSchemaActivateRpc.Request(f.create.header(),f.groupId,installed.position());
            var exact=new RatisSchemaActivateRpc.Request(f.create.header(),f.groupId,installed.position(),original);
            f.authorityPosition.set(new ApplyPosition(1,12));
            byte[] compatible=f.machine.queryLinearizable(message(RatisSchemaActivateRpc.prepareRequest(exact))).get(10,TimeUnit.SECONDS).getContent().toByteArray();
            assertArrayEquals(frozen,RatisSchemaActivateRpc.decodePreparedReply(compatible,ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,exact).encode());
            f.query(RatisSchemaActivateRpc.cancelRequest(base)); f.machine.schemaCleanup().get(5,TimeUnit.SECONDS);
            f.authorityGroup=id(99); assertThrows(ExecutionException.class,() -> f.query(RatisSchemaActivateRpc.prepareRequest(exact)));
            f.authorityGroup=id(20); f.authorityPosition.set(new ApplyPosition(1,9)); assertThrows(ExecutionException.class,() -> f.query(RatisSchemaActivateRpc.prepareRequest(exact)));
            f.authorityPosition.set(new ApplyPosition(1,12));
            byte[] refreshed=f.machine.queryLinearizable(message(RatisSchemaActivateRpc.prepareRequest(exact))).get(10,TimeUnit.SECONDS).getContent().toByteArray();
            assertArrayEquals(frozen,RatisSchemaActivateRpc.decodePreparedReply(refreshed,ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,exact).encode());
            var proof=NativeSchemaActivationProof.decode(f.apply(frozen)); assertEquals(original.cataloguePosition(),proof.cataloguePosition());
            f.beforeRead=() -> { throw new AssertionError("Retained activation must not fetch new authority"); };
            f.query(RatisSchemaActivateRpc.prepareRequest(exact)); assertArrayEquals(proof.encode(),f.apply(frozen));
            var changed=new RatisSchemaActivateRecord(f.create.header(),f.groupId,installed.position(),id(20),new ApplyPosition(1,12));
            assertThrows(ExecutionException.class,() -> f.query(RatisSchemaActivateRpc.prepareRequest(new RatisSchemaActivateRpc.Request(f.create.header(),f.groupId,installed.position(),changed))));
        }
    }

    @Test void createActivateDeleteActivateAndExactOriginalRetries() throws Exception {
        try (var f=new Fixture(root)) {
            var original=f.put(f.source,f.column,0,4); byte[] receipt=f.apply(DistributedCommandCodec.encode(original));
            var firstFence=f.fence(f.create); var installRecord=f.installRecord(f.create,firstFence); var firstInstall=f.install(installRecord);
            assertNotNull(f.propose(DistributedCommandCodec.encode(f.put(f.target,f.added,1,9))).getException(),"Installed target remains closed until catalogue-qualified activation");
            var firstActivationRecord=f.activateRecord(f.create,firstInstall); var firstActivation=NativeSchemaActivationProof.decode(f.apply(firstActivationRecord.encode()));
            var added=f.put(f.target,f.added,1,9); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(added))).status());
            assertArrayEquals(new byte[]{9},f.store.readCurrentPoint(f.added,new byte[]{1}));
            assertArrayEquals(receipt,f.apply(DistributedCommandCodec.encode(original)),"Original DATA outcome survives target activation");
            var deleteTarget=new DistributedSchema(id(1),id(2),id(3),3,0,List.of(column(1,f.column,"data")));
            var delete=new SchemaChange.Definition(id(12),new OperationId(id(10),1,2),f.create.targetDirectory(),f.target,deleteTarget);
            f.status.set(SchemaChange.Status.begin(delete.header()).beginFencing(new byte[32]));
            var secondFence=f.fence(delete); var secondInstall=f.install(f.installRecord(delete,secondFence));
            var secondActivationRecord=f.activateRecord(delete,secondInstall); f.apply(secondActivationRecord.encode());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(f.put(deleteTarget,f.column,2,7)))).status());
            assertArrayEquals(new byte[]{7},f.store.readCurrentPoint(f.column,new byte[]{1}));
            f.beforeRead=() -> { throw new AssertionError("Historical retry must not recapture changed catalogue authority"); };
            assertEquals(firstFence,NativeSchemaFence.decode(f.apply(new RatisSchemaFenceRecord(f.create.header(),f.groupId).encode())));
            assertEquals(firstInstall,NativeSchemaInstallProof.decode(f.apply(installRecord.encode())));
            assertEquals(firstActivation,NativeSchemaActivationProof.decode(f.apply(firstActivationRecord.encode())));
            assertArrayEquals(receipt,f.apply(DistributedCommandCodec.encode(original)));
            var altered=new RatisSchemaInstallRecord(f.create,f.groupId,firstFence.position(),"changed-physical",false);
            assertNotNull(f.propose(altered.encode()).getException());
            assertThrows(ExecutionException.class,() -> f.query(RatisSchemaInstallRpc.stateRequest(altered)));
            assertArrayEquals(deleteTarget.digest(),f.store.schemaDigest());
            assertEquals(2,f.activations.get(),"Historical activation retries never refresh the current runtime again");
        }
    }
    @Test void coveringSnapshotRetainsRunningInstallOwnerAndLateCompletionPreservesHealth() throws Exception {
        var committed=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var donor=new Fixture(root.resolve("donor"));
             var receiver=new Fixture(root.resolve("receiver"),false,pool -> {
                 var borrowed=org.mockito.Mockito.mock(DistributedStoragePool.class,org.mockito.AdditionalAnswers.delegatesTo(pool));
                 try {
                     org.mockito.Mockito.doAnswer(invocation -> {
                         var installed=pool.installSchema(invocation.getArgument(0),invocation.getArgument(1),invocation.getArgument(2,DistributedStoragePool.SchemaInstallPlan.class));
                         committed.countDown();
                         if (!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("INSTALL return barrier timed out");
                         return installed;
                     }).when(borrowed).installSchema(org.mockito.ArgumentMatchers.any(ReplicatedRangeStore.class),org.mockito.ArgumentMatchers.any(ApplyPosition.class),org.mockito.ArgumentMatchers.any(DistributedStoragePool.SchemaInstallPlan.class));
                 } catch (org.rocksdb.RocksDBException failure) { throw new AssertionError(failure); }
                 return borrowed;
             })) {
            var donorFence=donor.fence(donor.create); var expected=donor.install(donor.installRecord(donor.create,donorFence));
            assertEquals(expected.position().index(),donor.machine.takeSnapshot());
            var fence=receiver.fence(receiver.create); var record=receiver.installRecord(receiver.create,fence);
            receiver.query(RatisSchemaInstallRpc.prepareRequest(record)); var tx=receiver.propose(record.encode());
            assertNull(tx.getException()); receiver.machine.preAppendTransaction(tx); tx.initLogEntry(1,++receiver.index);
            var future=receiver.machine.applyTransaction(tx);
            try {
                assertTrue(committed.await(5,TimeUnit.SECONDS)); assertFalse(future.isDone());
                var source=donor.machine.getStateMachineStorage().getSnapshotFile(1,expected.position().index()).toPath();
                var destination=receiver.machine.getStateMachineStorage().getSnapshotFile(1,expected.position().index()).toPath();
                java.nio.file.Files.copy(source,destination);
                receiver.machine.pause(); receiver.machine.reinitialize();
                assertEquals(expected,NativeSchemaInstallProof.decode(future.get(5,TimeUnit.SECONDS).getContent().toByteArray()));
                assertThrows(IllegalStateException.class,() -> { try (var ignored=receiver.store.captureForRecovery()) {} },"Covered running worker retains native capture admission until its actual return");
            } finally { release.countDown(); }
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while (receiver.queries.stats().jobs()!=0 && System.nanoTime()<deadline) Thread.onSpinWait();
            assertEquals(0,receiver.queries.stats().jobs());
            receiver.query(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]));
            receiver.status.set(receiver.status.get().installed(receiver.groupId,expected.position()));
            receiver.apply(receiver.activateRecord(receiver.create,expected).encode());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(receiver.apply(DistributedCommandCodec.encode(receiver.put(receiver.target,receiver.added,0,8)))).status());
            assertEquals(expected,NativeSchemaInstallProof.decode(receiver.apply(record.encode())));
        }
    }
    @Test void pendingSourceAndTargetMetadataControlsRequireAuthorityThenSourceBecomesStale() throws Exception {
        try (var f=new Fixture(root,true)) {
            var fence=f.fence(f.create); var install=f.install(f.installRecord(f.create,fence));
            var control=RangeSessionControl.advance(f.ledger.get());
            var source=DistributedCommand.sessionControl(f.groupId,1,f.source.digest(),new OperationId(id(50),1,0),control,-1);
            var target=DistributedCommand.sessionControl(f.groupId,1,f.target.digest(),new OperationId(id(50),1,1),control,-1);
            int reads=f.epochReads.get();
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(source))).status());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(target))).status());
            assertEquals(reads+2,f.epochReads.get(),"Both pending generations require fresh catalogue clock authority");
            f.apply(f.activateRecord(f.create,install).encode());
            var stale=DistributedCommand.sessionControl(f.groupId,1,f.source.digest(),new OperationId(id(50),1,2),control,-1);
            reads=f.epochReads.get();
            assertEquals(CommandResult.Status.STALE_SCHEMA,CommandResult.decode(f.apply(DistributedCommandCodec.encode(stale))).status());
            assertEquals(reads,f.epochReads.get(),"Historical no-effect outcome never consults current directory validator");
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(source))).status());
            assertEquals(reads,f.epochReads.get(),"Saved original SUCCESS remains unchanged");
            try (var pin=f.store.captureForRecovery()) { assertEquals(SessionAuthority.from(f.ledger.get()),pin.stateView().sessionAuthority()); }
        }
    }
    @Test void localActivationObserverFailureDoesNotLoseCommittedProof() throws Exception {
        try (var f=new Fixture(root)) {
            var fence=f.fence(f.create); var install=f.install(f.installRecord(f.create,fence));
            var activation=f.activateRecord(f.create,install); var problem=new IllegalStateException("local refresh failure");
            f.onActivation=() -> { throw problem; };
            var proof=NativeSchemaActivationProof.decode(f.apply(activation.encode()));
            assertEquals(install,proof.installation()); assertSame(problem,f.machine.schemaActivationObserverFailure());
            assertEquals(1,f.activations.get()); assertEquals(proof,NativeSchemaActivationProof.decode(f.apply(activation.encode())));
            assertEquals(1,f.activations.get());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(f.put(f.target,f.added,0,5)))).status());
        }
    }
    @Test void lastColumnDeletionAndNewColumnCreationKeepMetadataRangeLive() throws Exception {
        try (var f=new Fixture(root)) {
            var empty=new DistributedSchema(id(1),id(2),id(3),2,0,List.of());
            var delete=new SchemaChange.Definition(id(12),new OperationId(id(10),1,2),f.sourceDirectory,f.source,empty);
            f.status.set(SchemaChange.Status.begin(delete.header()).beginFencing(new byte[32]));
            var fence=f.fence(delete); var installed=f.install(f.installRecord(delete,fence));
            f.apply(f.activateRecord(delete,installed).encode());
            assertTrue(f.store.ownership().interval().columns().isEmpty());
            try (var pin=f.store.captureForRecovery()) { assertTrue(pin.columns().isEmpty()); }
            f.query(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]));
            var restored=new DistributedSchema(id(1),id(2),id(3),3,0,List.of(column(2,f.added,"added")));
            var create=new SchemaChange.Definition(id(13),new OperationId(id(10),1,3),delete.targetDirectory(),empty,restored);
            f.status.set(SchemaChange.Status.begin(create.header()).beginFencing(new byte[32]));
            var next=f.fence(create); var created=f.install(f.installRecord(create,next)); f.apply(f.activateRecord(create,created).encode());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(DistributedCommandCodec.encode(f.put(restored,f.added,0,5)))).status());
            assertArrayEquals(new byte[]{5},f.store.readCurrentPoint(f.added,new byte[]{1}));
        }
    }
    @Test void prepareCaptureGatePrecedesLookupAndCancellationRetainsRunningOwner() throws Exception {
        try (var f=new Fixture(root)) {
            var fence=f.fence(f.create); var record=f.installRecord(f.create,fence);
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var retained=f.store.captureForRecovery()) {
                f.beforeRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("authority timeout"); } catch (InterruptedException e) { throw new AssertionError(e); } };
                var observer=f.machine.queryLinearizable(message(RatisSchemaInstallRpc.prepareRequest(record)));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IllegalStateException.class,f.store::captureForRecovery);
                    observer.cancel(false); f.query(RatisSchemaInstallRpc.cancelRequest(record));
                    assertThrows(IllegalStateException.class,f.store::captureForRecovery,"Cancelled lookup retains capture admission until actual lookup exits");
                    assertNotNull(f.propose(record.encode()).getException());
                } finally { release.countDown(); }
            }
            f.beforeRead=() -> {};
            // Drain the cancelled actual lookup by submitting a distinct bounded job behind its completion.
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while (f.queries.stats().jobs()!=0 && System.nanoTime()<deadline) Thread.onSpinWait();
            assertEquals(0,f.queries.stats().jobs());
            f.machine.schemaCleanup().get(5,TimeUnit.SECONDS); f.install(record);
        }
    }
    @Test void followerInstallationWaitsForOwnedRecoveryPinWithoutFailingHealthOrReordering() throws Exception {
        try (var f=new Fixture(root)) {
            var fence=f.fence(f.create); var record=f.installRecord(f.create,fence);
            var tx=TransactionContext.newBuilder().setStateMachine(f.machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                    .setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(++f.index)
                    .setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record.encode()))).build()).build();
            CompletableFuture<Message> installation;
            try (var held=f.store.captureForRecovery()) {
                installation=f.machine.applyTransaction(tx);
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
                while (f.queries.stats().jobs()!=0 && System.nanoTime()<deadline) Thread.onSpinWait();
                assertFalse(installation.isDone()); assertEquals(fence.position(),f.store.applied());
                assertThrows(IllegalStateException.class,f.store::captureForRecovery);
                f.machine.notifyTermIndexUpdated(1,++f.index);
                assertEquals(fence.position(),f.store.applied(),"Later metadata cannot cross a waiting INSTALL");
                assertEquals(fence.position(),held.position());
            }
            var proof=NativeSchemaInstallProof.decode(installation.get(10,TimeUnit.SECONDS).getContent().toByteArray());
            assertEquals(fence.position().index()+1,proof.position().index()); assertEquals(f.index,f.store.applied().index());
        }
    }
    @Test void queuedMembershipAndUnknownAppendKeepInstallOwnership() throws Exception {
        try (var f=new Fixture(root)) {
            var fence=f.fence(f.create); var record=f.installRecord(f.create,fence); f.query(RatisSchemaInstallRpc.prepareRequest(record));
            var tx=f.propose(record.encode()); assertNull(tx.getException());
            var changed=RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build();
            f.machine.notifyConfigurationChanged(1,f.index+1,changed);
            var stale=tx; assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,() -> f.machine.preAppendTransaction(stale));
            assertNull(f.store.lookupSchemaInstallation(f.create.header()));
            f.machine.notifyTermIndexUpdated(1,++f.index); f.machine.schemaCleanup().get(5,TimeUnit.SECONDS); f.query(RatisSchemaInstallRpc.prepareRequest(record));
            tx=f.propose(record.encode()); assertNull(tx.getException()); f.machine.preAppendTransaction(tx); tx.initLogEntry(1,f.index+1);
            f.machine.notifyNotLeader(List.of(tx)); f.query(RatisSchemaInstallRpc.cancelRequest(record));
            assertThrows(IllegalStateException.class,f.store::captureForRecovery,"Unknown append retains recovery capture gate");
            f.leader.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,++f.index);
            assertThrows(ExecutionException.class,() -> f.query(RatisSchemaInstallRpc.prepareRequest(record)),"Fresh leader qualification reclaims the excluded older-term owner and waits for its actual cleanup");
            f.machine.schemaCleanup().get(5,TimeUnit.SECONDS);
            try (var fresh=f.store.captureForRecovery()) { assertEquals(2,fresh.position().term()); }
            f.query(RatisSchemaInstallRpc.prepareRequest(record));
        }
    }
}
