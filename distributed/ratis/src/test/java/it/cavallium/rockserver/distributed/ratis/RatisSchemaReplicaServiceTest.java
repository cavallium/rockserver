package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.*;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.jar.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaReplicaRpc.*;

/** Real scheduler/file/native preparation on an addressed voter or listener, with no socket or quorum. */
class RatisSchemaReplicaServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int value) { return new UUID(0,value); }
    public static class AppendOperator extends FFMByteArrayMergeOperator {
        public AppendOperator(String identity) { super(identity); }
        @Override public byte[] merge(byte[] key,byte[] existing,List<byte[]> operands) {
            var result=new ByteArrayOutputStream(); if (existing!=null) result.writeBytes(existing); operands.forEach(result::writeBytes); return result.toByteArray();
        }
    }
    static byte[] artifact() throws Exception {
        var output=new ByteArrayOutputStream();
        try (var jar=new JarOutputStream(output)) {
            String name=AppendOperator.class.getName().replace('.','/')+".class"; jar.putNextEntry(new JarEntry(name));
            try (var input=AppendOperator.class.getClassLoader().getResourceAsStream(name)) { jar.write(Objects.requireNonNull(input).readAllBytes()); } jar.closeEntry();
            byte[] padding=new byte[OperatorUpload.CHUNK_BYTES+4096]; new Random(12).nextBytes(padding);
            jar.putNextEntry(new JarEntry("padding")); jar.write(padding); jar.closeEntry();
        }
        return output.toByteArray();
    }
    static final class Fixture implements AutoCloseable {
        final UUID group=id(9),column=id(4);
        final ReplicaDescriptor voter=new ReplicaDescriptor(id(5),id(6),id(7),"voter","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
        final ReplicaDescriptor listener=new ReplicaDescriptor(id(15),id(16),id(17),"listener","127.0.0.1:9999",ReplicaDescriptor.Role.LISTENER);
        final ReplicaDescriptor local;
        final DistributedSchema source=new DistributedSchema(id(1),id(2),id(3),0,0,List.of());
        final RangeDirectory directory=new RangeDirectory(id(1),id(2),id(3),0,source.digest(),0,Set.of(),0,List.of(new RangeDescriptor(id(8),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter,listener))));
        final byte[] jar;
        final DistributedSchema.Operator operator;
        final SchemaChange.Definition definition;
        final Target target;
        final OperatorUpload upload;
        final AtomicReference<SchemaChange.Status> status;
        final DistributedStoragePool pool;
        final ReplicatedRangeStore store;
        final RWScheduler scheduler=new RWScheduler(3,3,"schema-replica-test");
        final RatisProofQueryService queries=new RatisProofQueryService();
        final RaftStorage storage;
        final RaftGroup raftGroup;
        final RaftServer server;
        RatisSchemaReplicaService service;
        RatisRangeStateMachine machine;
        volatile Runnable beforeAuthority=() -> {};
        volatile RatisSchemaReplicaService.OperatorPolicy policy;
        final Path poolPath,stagePath;
        Fixture(Path path,boolean onListener) throws Exception {
            local=onListener ? listener : voter; jar=artifact();
            operator=new DistributedSchema.Operator("append",1,AppendOperator.class.getName(),OperatorUpload.sha256(jar),1,1,"append-contract",1);
            var shape=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true);
            var targetSchema=new DistributedSchema(id(1),id(2),id(3),1,0,List.of(new DistributedSchema.Column(1,column,"added",new ColumnSchema(shape.keys(),shape.variableTailKeys(),true,operator.name(),operator.version(),operator.className()),operator)));
            definition=new SchemaChange.Definition(id(11),new OperationId(id(10),1,1),directory,source,targetSchema);
            target=new Target(new Binding(definition,group,"added-physical",true),local);
            upload=OperatorUpload.create(id(30),id(1),id(2),operator,jar); status=new AtomicReference<>(SchemaChange.Status.begin(definition.header()));
            policy=ignored -> new MergeOperatorRegistry.DeterministicContract("append-contract",1,true);
            poolPath=path.resolve("native"); stagePath=path.resolve("staging");
            pool=DistributedStoragePool.open(poolPath,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()),source,Map.of(),Map.of());
            store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(id(3),Set.of(),null,null),source.digest(),Map.of(),Set.of()); store.enableLogicalSchema(source);
            storage=RaftStorage.newBuilder().setDirectory(path.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            var voterPeer=RaftPeer.newBuilder().setId(voter.peerId()).setAddress(voter.address()).build();
            var listenerPeer=RaftPeer.newBuilder().setId(listener.peerId()).setAddress(listener.address()).setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
            raftGroup=RaftGroup.valueOf(RaftGroupId.valueOf(group),voterPeer,listenerPeer);
            server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> { if (method.getName().equals("getId")) return RaftPeerId.valueOf(local.peerId()); if (method.getName().equals("toString")) return "replica-identity-fixture"; throw new AssertionError("Unexpected server access "+method); });
            start(); machine.notifyTermIndexUpdated(1,0);
        }
        void start() throws Exception {
            service=new RatisSchemaReplicaService(poolPath,stagePath,pool,scheduler,operator -> policy.admit(operator),new RatisSchemaReplicaService.Limits(4,64L*1024*1024,192L*1024*1024,8,8,0));
            machine=new RatisRangeStateMachine(store,raftGroup,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),command -> {},command -> {},() -> new RatisRangeStateMachine.Leadership(1,false));
            machine.schemaStorage(pool); machine.proofQueries(queries);
            machine.schemaAuthority(header -> { beforeAuthority.run(); return new RatisSchemaAuthority.View(new RatisSchemaAuthority.Request(id(1),id(2),id(3)),id(20),new ApplyPosition(1,10),directory,new RatisSchemaAdministration.Head(status.get(),1)); },directory);
            machine.schemaReplicas(service,local); machine.initialize(server,raftGroup.getGroupId(),storage);
        }
        Request request(int kind) { return new Request(kind,target,kind<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE ? upload : null,-1,null); }
        Request chunk(int index) { int start=index*OperatorUpload.CHUNK_BYTES; return new Request(ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK,target,upload,index,Arrays.copyOfRange(jar,start,Math.min(jar.length,start+OperatorUpload.CHUNK_BYTES))); }
        CompletableFuture<org.apache.ratis.protocol.Message> submit(Request request) { return machine.query(Message.valueOf(ByteString.copyFrom(request.encode()))); }
        Reply call(Request request) throws Exception { return decodeReply(submit(request).get(10,TimeUnit.SECONDS).getContent().toByteArray(),request); }
        void restart() throws Exception { service.stop().get(10,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); start(); }
        void upload() throws Exception { call(request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)); for (int i=0;i<upload.chunks().size();i++) call(chunk(i)); assertTrue(call(request(ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH)).artifactReady()); }
        @Override public void close() throws Exception { service.stop().get(10,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); queries.stop().get(10,TimeUnit.SECONDS); storage.close(); scheduler.dispose(); pool.close(); }
    }
    @Test void listenerUploadForcedProgressReopenReserveAndAuthoritativeAbortCleanup() throws Exception {
        try (var f=new Fixture(root,true)) {
            assertEquals(2,f.upload.chunks().size());
            var absent=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)); assertEquals(CapacityState.NONE,absent.capacityState()); assertFalse(absent.forcedDurable());
            assertFalse(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)).artifactReady());
            f.call(f.chunk(0)); // Lost reply: recovery uses forced original progress rather than appending again.
            f.restart(); var progress=f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)); assertTrue(progress.chunks().get(0)); assertFalse(progress.chunks().get(1));
            f.call(f.chunk(0)); f.call(f.chunk(1)); assertTrue(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH)).artifactReady());
            f.restart(); assertTrue(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).artifactReady());
            var reserve=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)); assertTrue(reserve.capacityActive()); assertTrue(reserve.forcedDurable()); assertNull(f.store.schemaFence());
            assertEquals(reserve.capacity(),f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacity());
            f.status.set(f.status.get().beginFencing()); assertThrows(ExecutionException.class,() -> f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE)));
            f.status.set(SchemaChange.Status.begin(f.definition.header()).abort());
            var released=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE)); assertEquals(CapacityState.ABORTED,released.capacityState()); assertFalse(released.capacityActive());
            assertEquals(reserve.capacity(),released.capacity()); assertFalse(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).artifactReady());
            assertFalse(f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacityActive());
        }
    }
    @Test void configuredPolicyAndExactReplicaIdentityCannotBeSelectedByRequest() throws Exception {
        try (var f=new Fixture(root,false)) {
            f.policy=operator -> new MergeOperatorRegistry.DeterministicContract("append-contract",1,false);
            assertThrows(ExecutionException.class,() -> f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)));
            var foreign=new Target(f.target.binding(),f.listener); var request=new Request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN,foreign,f.upload,-1,null);
            assertThrows(ExecutionException.class,() -> f.call(request));
            f.policy=operator -> new MergeOperatorRegistry.DeterministicContract("append-contract",1,true);
            f.upload();
            f.policy=operator -> null; assertThrows(ExecutionException.class,() -> f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)));
            f.policy=operator -> new MergeOperatorRegistry.DeterministicContract("append-contract",1,true);
            var first=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE));
            var changed=new Target(new Binding(f.definition,f.group,"changed-physical",true),f.local);
            assertThrows(ExecutionException.class,() -> f.call(new Request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE,changed,null,-1,null)));
            assertEquals(first.capacity(),f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacity());
        }
    }
    @Test void qualifiedCommittedFenceInstallActivationAndNativeMerge() throws Exception {
        try (var f=new Fixture(root,true)) {
            f.upload(); var capacity=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)); assertTrue(capacity.capacityActive());
            f.status.set(f.status.get().beginFencing());
            var record=new RatisSchemaFenceRecord(f.definition.header(),f.group);
            var fence=NativeSchemaFence.decode(apply(f,1,record.encode())); f.status.set(f.status.get().fenced(f.group,fence.position()));
            assertEquals(CapacityState.FENCED,f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)).capacityState());
            var installation=new RatisSchemaInstallRecord(f.definition,f.group,fence.position(),"added-physical",true);
            var installed=NativeSchemaInstallProof.decode(apply(f,2,installation.encode())); f.status.set(f.status.get().installed(f.group,installed.position()).publish());
            var consumed=f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)); assertEquals(CapacityState.CONSUMED,consumed.capacityState()); assertFalse(consumed.capacityActive());
            var activation=new RatisSchemaActivateRecord(f.definition.header(),f.group,installed.position(),id(20),new ApplyPosition(1,10)); apply(f,3,activation.encode());
            var merge=new DistributedCommand(f.group,1,f.definition.targetSchema().digest(),new OperationId(id(40),1,0),List.of(new Mutation(Mutation.Kind.MERGE,f.column,new byte[]{1},new byte[]{7})));
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(apply(f,4,DistributedCommandCodec.encode(merge))).status()); assertArrayEquals(new byte[]{7},f.store.readCurrentPoint(f.column,new byte[]{1}));
            assertEquals(capacity.capacity(),f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_RELEASE)).capacity());
            assertEquals(fence,NativeSchemaFence.decode(apply(f,5,record.encode())));
        }
    }
    static byte[] apply(Fixture f,long index,byte[] record) throws Exception {
        var tx=org.apache.ratis.statemachine.TransactionContext.newBuilder().setStateMachine(f.machine).setServerRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER)
                .setLogEntry(org.apache.ratis.proto.RaftProtos.LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record))).build()).build();
        return f.machine.applyTransaction(tx).get(10,TimeUnit.SECONDS).getContent().toByteArray();
    }
    @Test void addressOnlyChangeAndRestorationInvalidateInFlightReplicaAuthority() throws Exception {
        try (var f=new Fixture(root,true)) {
            var listener=RaftPeer.newBuilder().setId(f.listener.peerId()).setAddress(f.listener.address()).setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER).build();
            var changed=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(f.voter.peerId()).setAddress("127.0.0.1:10000").build().getRaftPeerProto()).addListeners(listener.getRaftPeerProto()).build();
            var restored=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(f.voter.peerId()).setAddress(f.voter.address()).build().getRaftPeerProto()).addListeners(listener.getRaftPeerProto()).build();
            f.machine.notifyConfigurationChanged(1,1,changed); f.machine.notifyTermIndexUpdated(1,1);
            var wrongAddress=assertThrows(ExecutionException.class,() -> f.call(f.request(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE)));
            assertTrue(wrongAddress.getCause().getMessage().contains("peer address/role configuration changed"));
            f.machine.notifyConfigurationChanged(1,2,restored); f.machine.notifyTermIndexUpdated(1,2);
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeAuthority=() -> { entered.countDown(); try { if (!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("authority timeout"); } catch (InterruptedException problem) { throw new AssertionError(problem); } };
            var pending=f.submit(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                f.machine.notifyConfigurationChanged(1,3,changed); f.machine.notifyTermIndexUpdated(1,3);
                f.machine.notifyConfigurationChanged(1,4,restored); f.machine.notifyTermIndexUpdated(1,4);
            } finally { release.countDown(); }
            var stale=assertThrows(ExecutionException.class,() -> pending.get(10,TimeUnit.SECONDS));
            assertTrue(stale.getCause().getMessage().contains("configuration changed")); f.beforeAuthority=() -> {};
            assertFalse(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).artifactReady());
            f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));
        }
    }
    @Test void unknownChunkForceMustBeRetriedBeforeProgressAndEmptyTerminalDirectoriesAreReclaimed() throws Exception {
        try (var f=new Fixture(root.resolve("fixture"),false)) {
            Path staging=root.resolve("forced"); var fail=new AtomicBoolean(false); var forcedOwners=new AtomicInteger();
            var limits=new RatisSchemaReplicaService.Limits(1,64L*1024*1024,64L*1024*1024,1,1,0);
            try (var registry=new RatisSchemaArtifactRegistry(staging,limits,path -> {
                if (Files.isDirectory(path) && Files.exists(path.resolve("chunk-0"))) { forcedOwners.incrementAndGet(); if (fail.get()) throw new IOException("unknown directory force"); }
                try (var channel=java.nio.channels.FileChannel.open(path,StandardOpenOption.READ)) { channel.force(true); }
            })) {
                var begin=f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN); registry.begin(begin); fail.set(true);
                assertThrows(IOException.class,() -> registry.chunk(f.chunk(0))); assertThrows(IOException.class,() -> registry.progress(begin));
                fail.set(false); assertTrue(registry.progress(begin).chunks().get(0)); assertTrue(forcedOwners.get()>=3);
                registry.release(f.target);
                Files.createDirectory(staging.resolve(RatisSchemaArtifactRegistry.key(begin))); // Crash after the final marker unlink, before owner-directory unlink.
            }
            try (var registry=new RatisSchemaArtifactRegistry(staging,limits)) {
                assertFalse(Files.exists(staging.resolve(RatisSchemaArtifactRegistry.key(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)))));
                registry.begin(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN)); assertFalse(registry.progress(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).ready());
            }
        }
    }
    @Test void finishAndReserveAccountForExpandedClassLoaderHeapBeforePolicyOrNativeIo() throws Exception {
        try (var f=new Fixture(root.resolve("fixture"),false)) {
            var policies=new AtomicInteger();
            var limited=new RatisSchemaReplicaService(f.poolPath,root.resolve("limited"),f.pool,f.scheduler,operator -> { policies.incrementAndGet(); return f.policy.admit(operator); },new RatisSchemaReplicaService.Limits(1,64L*1024*1024,64L*1024*1024,2,2,0));
            try {
                limited.register(f.group,f.store,f.local,header -> { throw new AssertionError("Rejected FINISH must not fetch authority"); },(target,currentSource) -> 0);
                for (int kind:List.of(ScopedTransactionRpc.SCHEMA_ARTIFACT_FINISH,ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE)) {
                    var failure=assertThrows(ExecutionException.class,() -> limited.submit(f.group,f.request(kind).encode()).get(5,TimeUnit.SECONDS));
                    assertTrue(failure.getCause().getMessage().contains("actual work capacity")); assertEquals(0,policies.get());
                }
                assertNull(f.pool.lookupSchemaCapacity(f.store,f.definition.header()),"Rejected RESERVE must not mutate native capacity");
                try (var files=Files.list(root.resolve("limited"))) { assertEquals(List.of("lock"),files.map(path -> path.getFileName().toString()).toList()); }
            } finally { limited.stop().get(5,TimeUnit.SECONDS); }
        }
    }
    @Test void cancelledObserverRetainsActualIoOwnerAndStopWaitsForReturn() throws Exception {
        try (var f=new Fixture(root,false)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeAuthority=() -> { entered.countDown(); try { if (!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("authority timeout"); } catch (InterruptedException problem) { throw new AssertionError(problem); } };
            var observer=f.submit(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_BEGIN));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); observer.cancel(false);
                var stopping=f.service.stop(); assertFalse(stopping.isDone()); stopping.cancel(false);
                assertThrows(IOException.class,() -> new RatisSchemaReplicaService(f.poolPath,f.stagePath,f.pool,f.scheduler,f.policy,RatisSchemaReplicaService.Limits.defaults()));
            } finally { release.countDown(); }
            f.service.stop().get(10,TimeUnit.SECONDS); f.beforeAuthority=() -> {}; f.machine.close(); f.start();
            assertFalse(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).artifactReady());
            f.call(f.chunk(0)); assertTrue(f.call(f.request(ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE)).chunks().get(0));
        }
    }
}
