package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native catalogue preparation/append ownership, without server, network or quorum. */
class RatisRetainedProofAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private final UUID pool=UUID.randomUUID();
    private RatisNodeResourceBudget budget() {
        var lane=new RatisNodeResourceBudget.Lane(64L<<20,32);
        var limits=new RatisNodeResourceBudget.Limits(lane,lane);
        return new RatisNodeResourceBudget(limits,Map.of(pool,limits));
    }
    private RatisImportPublicationAdmissionTest helper(String name) {
        var helper=new RatisImportPublicationAdmissionTest();helper.root=root.resolve(name);return helper;
    }
    @Test void preparedNativePublicationKeepsGraphAfterActualQueryDrainAndExactRetryReusesIt() throws Exception {
        var budget=budget();var checks=new AtomicInteger();var helper=helper("prepared");
        try(var fixture=helper.new Fixture(command->checks.incrementAndGet(),machine->machine.nodeResources(budget,pool))) {
            fixture.prepare().get(5,TimeUnit.SECONDS);
            long expected=256L+4L*DistributedCommandCodec.MAX_ENCODED_BYTES;
            assertEquals(expected,budget.stats().protectedLane().bytes());assertEquals(1,budget.stats().protectedLane().owners());
            fixture.prepare().get(5,TimeUnit.SECONDS);assertEquals(1,checks.get());
            assertEquals(expected,budget.stats().protectedLane().bytes());
            fixture.queries.stop().get(5,TimeUnit.SECONDS);assertEquals(0,fixture.queries.stats().bytes());
            assertEquals(expected,budget.stats().protectedLane().bytes());
            fixture.cancel();assertEquals(0,budget.stats().protectedLane().bytes());assertEquals(0,fixture.machine.importPublicationStats().holds());
        }
    }
    @Test void exhaustedGraphRejectsConstructionBeforeTrustedLookupOrNativePublication() throws Exception {
        var budget=budget();var checks=new AtomicInteger();var helper=helper("full");
        try(var fixture=helper.new Fixture(command->checks.incrementAndGet(),machine->machine.nodeResources(budget,pool));
            var occupied=budget.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,64L<<20,1)) {
            var rejected=assertThrows(ExecutionException.class,()->fixture.prepare().get(5,TimeUnit.SECONDS));
            assertInstanceOf(IOException.class,rejected.getCause());assertEquals(0,checks.get());
            assertEquals(0,fixture.machine.importPublicationStats().holds());assertEquals(0,fixture.queries.stats().bytes());
            assertEquals(64L<<20,budget.stats().protectedLane().bytes());
        }
    }
    @Test void canceledBlockedLookupRetainsItsGraphUntilActualLookupFinallyExits() throws Exception {
        var budget=budget();var helper=helper("blocked");var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var fixture=helper.new Fixture(command->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new IOException("Lookup timeout");}catch(InterruptedException failure){throw new IOException(failure);}},machine->machine.nodeResources(budget,pool))) {
            var observer=fixture.prepare();
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(observer.cancel(false));fixture.cancel();
                assertEquals(256L+4L*DistributedCommandCodec.MAX_ENCODED_BYTES,budget.stats().protectedLane().bytes());
                assertEquals(1,fixture.machine.importPublicationStats().reading());
            } finally {release.countDown();}
            fixture.queries.stop().get(5,TimeUnit.SECONDS);
            assertEquals(0,budget.stats().protectedLane().bytes());assertEquals(0,fixture.machine.importPublicationStats().holds());
        }
    }
    @Test void duplicateAppendOwnersRetainUnknownInputAndProofUntilStrictNewTermExclusion() throws Exception {
        var budget=budget();var helper=helper("unknown");
        try(var fixture=helper.new Fixture(command->{},machine->machine.nodeResources(budget,pool))) {
            fixture.prepare().get(5,TimeUnit.SECONDS);
            var first=fixture.propose();var duplicate=fixture.propose();assertNull(first.getException());assertNull(duplicate.getException());
            var proof=budget.stats().protectedLane();var inputs=budget.stats().ordinary();assertEquals(2,inputs.owners());
            first.initLogEntry(1,fixture.catalogue.nativeCatalogue.next.get());fixture.machine.cancelTransaction(first);
            fixture.machine.cancelTransaction(duplicate);fixture.machine.notifyNotLeader(List.of());
            assertEquals(proof,budget.stats().protectedLane());assertEquals(inputs,budget.stats().ordinary());
            assertEquals(1,fixture.machine.importPublicationStats().holds());
            fixture.term.set(2);fixture.machine.notifyTermIndexUpdated(2,fixture.catalogue.nativeCatalogue.next.getAndIncrement());
            assertNotNull(fixture.propose().getException());
            assertEquals(0,budget.stats().ordinary().bytes());assertEquals(0,budget.stats().protectedLane().bytes());
            assertEquals(0,fixture.machine.importPublicationStats().holds());
        }
    }
    private final class TopologyFixture implements AutoCloseable {
        final RatisTopologyFreezeTest.Fixture models;
        final it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool storagePool;
        final it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore store;
        final org.apache.ratis.server.storage.RaftStorage raft;
        final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService(RatisProofFixtures.admission());
        final RatisTopologyFreezeRecord record;
        final org.apache.ratis.protocol.RaftGroup group;
        final AtomicInteger reads=new AtomicInteger();
        TopologyFixture(RatisNodeResourceBudget budget) throws Exception {
            // Reuse the established native topology model; its independent owner is closed first.
            models=new RatisTopologyFreezeTest.Fixture(root.resolve("topology-model"));
            record=models.record;group=models.group;var schema=models.schema;var local=models.local;var directory=models.directory;var view=models.view;
            models.close();
            storagePool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(root.resolve("topology-native"),it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()));
            store=storagePool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(record.groupId(),1),new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(directory.domainId(),directory.columns(),null,null),schema.digest(),Map.of(models.column,"data"),java.util.Set.of());
            store.enableLogicalSchema(schema);store.enableScopedHistory(it.cavallium.rockserver.core.common.distributed.RangeMutationHistory.Limits.defaults());
            raft=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("topology-raft").toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();raft.initialize();
            machine=new RatisRangeStateMachine(store,group,new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(4*1024*1024,16*1024*1024,1),command->{},command->{},()->new RatisRangeStateMachine.Leadership(1,true));
            machine.nodeResources(budget,pool);machine.proofQueries(queries);machine.topologyAuthority((header,stage)->{reads.incrementAndGet();return view;},directory,local,storagePool);
            var identity=(org.apache.ratis.server.RaftServer)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return models.peer.getId();if(method.getName().equals("toString"))return "retained-topology";throw new AssertionError(method);});
            machine.initialize(identity,group.getGroupId(),raft);machine.notifyTermIndexUpdated(1,0);
        }
        org.apache.ratis.statemachine.TransactionContext propose() throws IOException {
            return machine.startTransaction(org.apache.ratis.protocol.RaftClientRequest.newBuilder().setClientId(org.apache.ratis.protocol.ClientId.randomId()).setServerId(models.peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType()).setMessage(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(record.encode()))).build());
        }
        void prepare() throws Exception {machine.queryLinearizable(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);}
        public void close() throws Exception {machine.close();machine.topologyCleanup().get(5,TimeUnit.SECONDS);queries.stop().get(5,TimeUnit.SECONDS);machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));raft.close();storagePool.close();}
    }
    @Test void customTopologyAppendCopiesRetainTheirOwnGraphThroughNativeCleanup() throws Exception {
        var budget=budget();
        try(var fixture=new TopologyFixture(budget)) {
            fixture.prepare();assertEquals(256L+4L*RatisTopologyFreezeRecord.MAX_BYTES,budget.stats().protectedLane().bytes());
            fixture.prepare();assertEquals(1,fixture.reads.get());
            var first=fixture.propose();var duplicate=fixture.propose();assertNull(first.getException());assertNull(duplicate.getException());
            var charged=budget.stats().protectedLane();assertEquals(3,charged.owners());
            fixture.machine.cancelTransaction(first);assertEquals(charged,budget.stats().protectedLane());
            fixture.machine.preAppendTransaction(duplicate);duplicate.initLogEntry(1,1);
            fixture.machine.applyTransaction(duplicate).get(5,TimeUnit.SECONDS);fixture.machine.topologyCleanup().get(5,TimeUnit.SECONDS);
            assertEquals(0,budget.stats().protectedLane().bytes());assertEquals(0,fixture.machine.topologyFreezeStats().holds());
        }
    }

    private final class SchemaFixture implements AutoCloseable {
        final UUID groupId=new UUID(0,9),column=new UUID(0,4),added=new UUID(0,40);
        final org.apache.ratis.protocol.RaftPeer peer=org.apache.ratis.protocol.RaftPeer.newBuilder().setId("fixture").setAddress("127.0.0.1:9999").build();
        final org.apache.ratis.protocol.RaftGroup group=org.apache.ratis.protocol.RaftGroup.valueOf(org.apache.ratis.protocol.RaftGroupId.valueOf(groupId),peer);
        final it.cavallium.rockserver.core.common.distributed.DistributedSchema source=new it.cavallium.rockserver.core.common.distributed.DistributedSchema(new UUID(0,1),new UUID(0,2),new UUID(0,3),1,0,List.of(RatisSchemaLifecycleAdmissionTest.column(1,column,"data")));
        final it.cavallium.rockserver.core.common.distributed.RangeDirectory directory=new it.cavallium.rockserver.core.common.distributed.RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,source.digest(),1,java.util.Set.of(column),0,List.of(new it.cavallium.rockserver.core.common.distributed.RangeDescriptor(new UUID(0,8),groupId,1,1,null,null,it.cavallium.rockserver.core.common.distributed.RangeDescriptor.TopologyState.ACTIVE,List.of(new it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor(new UUID(0,5),new UUID(0,6),new UUID(0,7),"fixture","127.0.0.1:9999",it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor.Role.VOTER)))));
        final it.cavallium.rockserver.core.common.distributed.DistributedSchema target=new it.cavallium.rockserver.core.common.distributed.DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,0,List.of(RatisSchemaLifecycleAdmissionTest.column(1,column,"data"),RatisSchemaLifecycleAdmissionTest.column(2,added,"added")));
        final it.cavallium.rockserver.core.common.distributed.SchemaChange.Definition definition=new it.cavallium.rockserver.core.common.distributed.SchemaChange.Definition(new UUID(0,11),new it.cavallium.rockserver.core.common.distributed.OperationId(new UUID(0,10),1,1),directory,source,target);
        final java.util.concurrent.atomic.AtomicReference<it.cavallium.rockserver.core.common.distributed.SchemaChange.Status> status=new java.util.concurrent.atomic.AtomicReference<>(it.cavallium.rockserver.core.common.distributed.SchemaChange.Status.begin(definition.header()).beginFencing(new byte[32]));
        final it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool storagePool;
        final it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore store;
        final org.apache.ratis.server.storage.RaftStorage raft;
        final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService(RatisProofFixtures.admission());
        SchemaFixture(RatisNodeResourceBudget budget) throws Exception {
            storagePool=it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.open(root.resolve("cleanup-native"),it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()));
            store=storagePool.openRange(new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Owner(groupId,1),new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(source.domainId(),java.util.Set.of(column),null,null),source.digest(),Map.of(column,"data"),java.util.Set.of());store.enableLogicalSchema(source);
            raft=org.apache.ratis.server.storage.RaftStorage.newBuilder().setDirectory(root.resolve("cleanup-raft").toFile()).setOption(org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();raft.initialize();
            machine=new RatisRangeStateMachine(store,group,new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(4*1024*1024,16*1024*1024,2),command->{},command->{},()->new RatisRangeStateMachine.Leadership(1,true));
            machine.nodeResources(budget,pool);machine.schemaStorage(storagePool);machine.proofQueries(queries);
            machine.schemaAuthority((header,digest)->new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(source.clusterId(),source.databaseId(),source.domainId()),new UUID(0,20),new it.cavallium.rockserver.core.common.distributed.ApplyPosition(1,10),directory,new RatisSchemaAdministration.Head(status.get(),2)),directory);
            var identity=(org.apache.ratis.server.RaftServer)java.lang.reflect.Proxy.newProxyInstance(org.apache.ratis.server.RaftServer.class.getClassLoader(),new Class<?>[]{org.apache.ratis.server.RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "retained-schema";throw new AssertionError(method);});
            machine.initialize(identity,group.getGroupId(),raft);machine.notifyTermIndexUpdated(1,0);
        }
        org.apache.ratis.protocol.Message message(byte[] bytes){return org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(bytes));}
        void query(byte[] bytes) throws Exception {machine.queryLinearizable(message(bytes)).get(5,TimeUnit.SECONDS);}
        org.apache.ratis.statemachine.TransactionContext propose(byte[] bytes) throws IOException {return machine.startTransaction(org.apache.ratis.protocol.RaftClientRequest.newBuilder().setClientId(org.apache.ratis.protocol.ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build());}
        byte[] append(byte[] bytes,long index) throws Exception {var context=propose(bytes);assertNull(context.getException());machine.preAppendTransaction(context);context.initLogEntry(1,index);return machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray();}
        Object hold() throws Exception {var field=RatisRangeStateMachine.class.getDeclaredField("schemaHold");field.setAccessible(true);return field.get(machine);}
        public void close() throws Exception {queries.stop().get(5,TimeUnit.SECONDS);machine.close();machine.schemaCleanup().get(5,TimeUnit.SECONDS);machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));raft.close();storagePool.close();}
    }
    @Test void exactSavedInstallReplaysWhileOriginalNativePlanCleanupIsBlockedWithoutDonatingGraph() throws Exception {
        var budget=budget();
        try(var fixture=new SchemaFixture(budget)) {
            var fenceRecord=new RatisSchemaFenceRecord(fixture.definition.header(),fixture.groupId);
            fixture.query(RatisSchemaFenceRpc.prepareRequest(fenceRecord));
            var fence=it.cavallium.rockserver.core.common.distributed.NativeSchemaFence.decode(fixture.append(fenceRecord.encode(),1));
            fixture.status.set(fixture.status.get().fenced(fixture.groupId,fence.position()));
            var install=new RatisSchemaInstallRecord(fixture.definition,fixture.groupId,fence.position(),"added-physical",false);
            fixture.query(RatisSchemaInstallRpc.prepareRequest(install));
            var held=fixture.hold();var planField=held.getClass().getDeclaredField("installPlan");planField.setAccessible(true);
            var plan=(it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool.SchemaInstallPlan)planField.get(held);
            var original=fixture.storagePool.installSchema(fixture.store,new it.cavallium.rockserver.core.common.distributed.ApplyPosition(1,2),plan).proof();
            fixture.machine.notifyTermIndexUpdated(1,2);
            var field=fixture.storagePool.getClass().getDeclaredField("nativeMutation");field.setAccessible(true);var mutation=(java.util.concurrent.Semaphore)field.get(fixture.storagePool);
            mutation.acquire();
            try {
                fixture.query(RatisSchemaInstallRpc.cancelRequest(install));
                var actualCleanup=fixture.machine.schemaCleanup();assertFalse(actualCleanup.isDone());
                long retained=256L+4L*((long)RatisSchemaInstallRecord.MAX_BYTES+it.cavallium.rockserver.core.common.distributed.NativeSchemaInstallation.MAX_BYTES);
                assertEquals(retained,budget.stats().protectedLane().bytes());assertSame(held,fixture.hold());
                var retry=fixture.propose(install.encode());assertNull(retry.getException());fixture.machine.preAppendTransaction(retry);retry.initLogEntry(1,3);
                var result=fixture.machine.applyTransaction(retry).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                assertArrayEquals(original.encode(),result);assertFalse(actualCleanup.isDone());assertSame(held,fixture.hold());
                assertTrue(budget.stats().protectedLane().bytes()>=retained);
            } finally {mutation.release();}
            fixture.machine.schemaCleanup().get(5,TimeUnit.SECONDS);
            fixture.queries.stop().get(5,TimeUnit.SECONDS);
            assertNull(fixture.hold());assertEquals(0,budget.stats().protectedLane().bytes());
        }
    }

}
