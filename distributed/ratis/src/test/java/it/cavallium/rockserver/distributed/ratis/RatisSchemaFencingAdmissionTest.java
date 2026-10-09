package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
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

/** Fresh owner-specific readiness and catalogue gate ownership; no server process, network or quorum. */
class RatisSchemaFencingAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(int n) { return new UUID(0,n); }
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static final class Fixture implements AutoCloseable {
        final UUID catalogue=id(20),metadata=id(21),dataGroup=id(9),column=id(4);
        final ReplicaDescriptor voter=new ReplicaDescriptor(id(5),id(6),id(7),"voter","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
        final ReplicaDescriptor listener=new ReplicaDescriptor(id(15),id(16),id(17),"listener","127.0.0.1:9999",ReplicaDescriptor.Role.LISTENER);
        final DistributedSchema source=new DistributedSchema(id(1),id(2),id(3),1,0,List.of(RatisSchemaLifecycleAdmissionTest.column(1,column,"data")));
        final RangeDirectory directory=new RangeDirectory(id(1),id(2),id(3),1,source.digest(),1,Set.of(column),0,List.of(new RangeDescriptor(id(8),dataGroup,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter,listener))));
        final DistributedSchema target=new DistributedSchema(id(1),id(2),id(3),2,0,List.of(RatisSchemaLifecycleAdmissionTest.column(1,column,"data"),RatisSchemaLifecycleAdmissionTest.column(2,id(40),"added")));
        final SchemaChange.Definition definition=new SchemaChange.Definition(id(11),new OperationId(id(10),1,1),directory,source,target);
        final NativeSchemaCapacityReservation capacity=new NativeSchemaCapacityReservation(definition.header(),dataGroup,new byte[32],1024);
        final SchemaAdmissionManifest manifest=new SchemaAdmissionManifest(definition,List.of(new SchemaAdmissionManifest.Binding(dataGroup,"added-physical",false)),List.of(
                new SchemaAdmissionManifest.ReplicaAdmission(dataGroup,voter.replicaId(),capacity),new SchemaAdmissionManifest.ReplicaAdmission(dataGroup,listener.replicaId(),capacity)));
        final RatisSchemaAdministration.Head prepared=new RatisSchemaAdministration.Head(SchemaChange.Status.begin(definition.header()),2);
        final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicInteger calls=new AtomicInteger();
        final DistributedStoragePool pool; final ReplicatedRangeStore store; final RaftStorage storage; final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService(),authorityQueries=RatisProofQueryService.authorityLane(); final RatisSchemaReplicaClient replicas;
        final RaftPeer peer=RaftPeer.newBuilder().setId("catalogue").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(catalogue),peer);
        final DistributedCommand fencing;
        final RatisSchemaAdministration administration;
        volatile Runnable beforeCapacity=() -> {};
        volatile Runnable beforeAuthorityWork=() -> {};
        volatile String bad="";
        long index=1;
        Fixture(Path root) throws Exception {
            replicas=new RatisSchemaReplicaClient(2,(descriptor,owner) -> {
                var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
                    if (!method.getName().equals("sendStaleRead")) throw new AssertionError("Unexpected direct owner RPC "+method);
                    var request=RatisSchemaReplicaRpc.request(((Message)args[0]).getContent().toByteArray()); calls.incrementAndGet(); beforeCapacity.run();
                    assertEquals(ScopedTransactionRpc.SCHEMA_CAPACITY_STATE,request.kind()); assertEquals(owner,request.target().replica());
                    if (bad.equals("missing") && owner.equals(listener)) throw new IOException("Listener unavailable");
                    var evidence=bad.equals("wrong") && owner.equals(listener) ? new NativeSchemaCapacityReservation(definition.header(),dataGroup,new byte[32],2048) : capacity;
                    var state=bad.equals("terminal") && owner.equals(listener) ? RatisSchemaReplicaRpc.CapacityState.CONSUMED : RatisSchemaReplicaRpc.CapacityState.ACTIVE;
                    byte[] reply=RatisSchemaReplicaRpc.reply(request,new BitSet(),false,evidence,state,!bad.equals("nonforced") || !owner.equals(listener));
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(owner.peerId())).setGroupId(RaftGroupId.valueOf(dataGroup)).setCallId(1).setSuccess(true).setMessage(message(reply)).build();
                });
                return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> {
                    if (method.getName().equals("io")) return io; if (method.getName().equals("close")) return null;
                    throw new AssertionError("Unexpected owner client access "+method);
                });
            });
            pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(catalogue,1),new RangeOwnership.Interval(id(3),Set.of(metadata),null,null),new byte[32],Map.of(metadata,"catalogue"),Set.of());
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build(); storage.initialize();
            var validator=RatisSchemaAdministration.validator(id(1),id(2),catalogue,1,new byte[32],metadata,key -> {
                try { return store.readCurrentPoint(metadata,key); } catch (Exception failure) { throw new IllegalArgumentException(failure); }
            });
            machine=new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),command -> { if (RatisSchemaAdministration.isCommand(command)) validator.accept(command); },command -> {},leader::get);
            machine.role(RatisNodePlan.Role.CATALOGUE); machine.schemaCatalogue(id(1),id(2),metadata); machine.schemaFencingReplicaClient(replicas); machine.proofQueries(queries); machine.schemaAuthorityQueries((ownedGroup,input,estimate,coalesce,action) -> authorityQueries.submit(ownedGroup,input,estimate,coalesce,owned -> { beforeAuthorityWork.run(); return action.run(owned); }));
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
                if (method.getName().equals("getId")) return peer.getId(); if (method.getName().equals("toString")) return "catalogue-fencing-fixture"; throw new AssertionError(method);
            });
            machine.initialize(server,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
            byte[] directoryKey=ByteBuffer.allocate(16).putLong(0).putLong(3).array();
            store.apply(new ApplyPosition(1,1),new DistributedCommand(catalogue,1,new byte[32],new OperationId(id(30),1,0),List.of(
                    new Mutation(Mutation.Kind.PUT,metadata,directoryKey,RangeDirectoryCodec.encode(directory)),
                    new Mutation(Mutation.Kind.PUT,metadata,RatisSchemaAdministration.headKey(id(3)),prepared.encode()),
                    new Mutation(Mutation.Kind.PUT,metadata,RatisSchemaAdministration.definitionKey(definition.digest()),definition.encode()),
                    new Mutation(Mutation.Kind.PUT,metadata,RatisSchemaAdministration.admissionKey(manifest.digest()),manifest.encode()))));
            machine.notifyTermIndexUpdated(1,1);
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> {
                if (method.getName().equals("getGroupId")) return group.getGroupId(); throw new AssertionError("Preparation accessed transport");
            });
            administration=new RatisSchemaAdministration(client,id(1),id(2),catalogue,1,new byte[32],metadata);
            fencing=administration.prepareFencing(new OperationId(id(30),1,1),prepared,definition,manifest);
        }
        TransactionContext propose(DistributedCommand command) throws Exception { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build()); }
        CompletableFuture<Message> prepare() { return machine.queryLinearizable(message(RatisSchemaFencingRpc.prepareRequest(fencing))); }
        void cancel() throws Exception { machine.queryLinearizable(message(RatisSchemaFencingRpc.cancelRequest(fencing))).get(5,TimeUnit.SECONDS); }
        byte[] apply(TransactionContext tx) throws Exception { assertNull(tx.getException()); machine.preAppendTransaction(tx); tx.initLogEntry(leader.get().term(),++index); return machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
        @Override public void close() throws Exception { queries.stop().get(5,TimeUnit.SECONDS); authorityQueries.stop().get(5,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(java.time.Duration.ofSeconds(5)); replicas.close(); storage.close(); pool.close(); }
    }
    @Test void sameRuntimeAuthorityDependencyCompletesWithOrdinaryLaneFullyChargedAndDrainsActualOwners() throws Exception {
        try (var f=new Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeAuthorityWork=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var request=new RatisSchemaAuthority.Request(id(1),id(2),id(3),f.definition.header(),f.manifest.digest(),false);
            try {
                byte[] input={99};
                var waiting=f.queries.submit(f.catalogue,input,RatisProofQueryService.MAX_BYTES,true,
                        ignored -> f.machine.queryLinearizable(message(RatisSchemaAuthority.request(request))).get(5,TimeUnit.SECONDS));
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                var observer=f.queries.submit(f.catalogue,input,RatisProofQueryService.MAX_BYTES,true,ignored -> { throw new AssertionError("Coalesced owner replaced"); });
                assertEquals(RatisProofQueryService.MAX_BYTES,f.queries.stats().bytes()); assertEquals(RatisProofQueryService.MAX_BYTES,f.authorityQueries.stats().bytes()); assertEquals(1,f.authorityQueries.stats().threads());
                assertTrue(waiting.cancel(false)); var drain=f.queries.stop(); assertFalse(drain.isDone());
                assertEquals(RatisProofQueryService.MAX_BYTES,f.queries.stats().bytes()); assertEquals(RatisProofQueryService.MAX_BYTES,f.authorityQueries.stats().bytes());
                release.countDown(); drain.get(5,TimeUnit.SECONDS);
                var view=RatisSchemaAuthority.decodeReply(observer.get(5,TimeUnit.SECONDS).getContent().toByteArray(),request,f.catalogue); assertArrayEquals(f.manifest.encode(),view.admissions());
                f.authorityQueries.stop().get(5,TimeUnit.SECONDS); assertEquals(0,f.queries.stats().bytes()); assertEquals(0,f.authorityQueries.stats().bytes());
            } finally { release.countDown(); }
        }
    }
    @Test void authorityReturnsDefinitionQualifiedManifestAtOneCutBeforeHeadCommitment() throws Exception {
        try (var f=new Fixture(root)) {
            var request=new RatisSchemaAuthority.Request(id(1),id(2),id(3),f.definition.header(),f.manifest.digest(),false);
            byte[] reply=f.machine.queryLinearizable(message(RatisSchemaAuthority.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            var view=RatisSchemaAuthority.decodeReply(reply,request,f.catalogue);
            assertEquals(new ApplyPosition(1,1),view.position()); assertEquals(SchemaChange.Phase.PREPARED,view.head().status().phase());
            assertNull(view.head().status().admissionDigest()); assertArrayEquals(f.manifest.encode(),view.admissions());
            var missing=new RatisSchemaAuthority.Request(id(1),id(2),id(3),f.definition.header(),new byte[32],false);
            assertThrows(ExecutionException.class,() -> f.machine.queryLinearizable(message(RatisSchemaAuthority.request(missing))).get(5,TimeUnit.SECONDS));
            var other=new SchemaChange.Definition(id(12),new OperationId(id(10),1,2),f.directory,f.source,f.target);
            var changed=new RatisSchemaAuthority.Request(id(1),id(2),id(3),other.header(),f.manifest.digest(),false);
            assertThrows(ExecutionException.class,() -> f.machine.queryLinearizable(message(RatisSchemaAuthority.request(changed))).get(5,TimeUnit.SECONDS));
        }
    }
    @Test void everyVoterAndListenerMustHaveExactForcedActiveReservationBeforeAppend() throws Exception {
        for (String invalid:List.of("missing","nonforced","wrong","terminal")) try (var f=new Fixture(root.resolve(invalid))) {
            assertNotNull(f.propose(f.fencing).getException()); f.bad=invalid;
            assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS)); assertNotNull(f.propose(f.fencing).getException());
            assertEquals(0,f.machine.proposalGateStats().gates());
            f.bad=""; f.prepare().get(5,TimeUnit.SECONDS); assertEquals(1,f.machine.proposalGateStats().gates());
            var tx=f.propose(f.fencing); assertNull(tx.getException()); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(tx)).status());
            assertEquals(0,f.machine.proposalGateStats().gates()); int calls=f.calls.get();
            f.beforeCapacity=() -> { throw new AssertionError("Saved outcome must not query readiness"); };
            f.prepare().get(5,TimeUnit.SECONDS); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(f.propose(f.fencing))).status()); assertEquals(calls,f.calls.get());
        }
    }
    @Test void metadataPredecessorsDrainAndGateBlocksAbortUntilExactOwnerCancels() throws Exception {
        try (var f=new Fixture(root)) {
            var predecessor=f.administration.prepareAdvance(new OperationId(id(31),1,0),f.prepared,f.prepared.status());
            var old=f.propose(predecessor); assertNull(old.getException());
            assertThrows(ExecutionException.class,() -> f.prepare().get(5,TimeUnit.SECONDS)); assertEquals(1,f.machine.proposalGateStats().gates());
            var abort=f.administration.prepareAdvance(new OperationId(id(31),1,1),f.prepared,f.prepared.status().abort());
            var blocked=f.propose(abort); assertNotNull(blocked.getException()); assertEquals("Proposal overlaps a reserved transaction scope",blocked.getException().getMessage());
            f.machine.cancelTransaction(old); f.prepare().get(5,TimeUnit.SECONDS);
            var tx=f.propose(f.fencing); assertNull(tx.getException()); f.machine.cancelTransaction(tx); assertEquals(0,f.machine.proposalGateStats().gates());
            var permitted=f.propose(abort); assertNull(permitted.getException()); f.machine.cancelTransaction(permitted);
            f.prepare().get(5,TimeUnit.SECONDS); f.cancel(); assertEquals(0,f.machine.proposalGateStats().gates());
        }
    }
    @Test void cancelledPreparationRetainsActualPayloadUntilOutboundWorkDrains() throws Exception {
        try (var f=new Fixture(root)) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.beforeCapacity=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            try {
                var preparing=f.prepare(); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(RatisProofQueryService.MAX_BYTES,f.queries.stats().bytes());
                assertTrue(preparing.cancel(false)); f.cancel(); assertEquals(0,f.machine.proposalGateStats().gates());
                assertNotNull(f.propose(f.fencing).getException()); assertFalse(f.machine.schemaCleanup().isDone());
                release.countDown(); f.queries.stop().get(5,TimeUnit.SECONDS); f.machine.schemaCleanup().get(5,TimeUnit.SECONDS); assertEquals(0,f.queries.stats().bytes());
            } finally { release.countDown(); }
        }
    }
    @Test void queuedConfigurationAndNewTermInvalidateOwnedPreparation() throws Exception {
        try (var f=new Fixture(root.resolve("configuration"))) {
            f.prepare().get(5,TimeUnit.SECONDS); var tx=f.propose(f.fencing); assertNull(tx.getException());
            f.machine.notifyConfigurationChanged(1,2,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());
            assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,() -> f.machine.preAppendTransaction(tx)); assertNotNull(f.propose(f.fencing).getException()); assertEquals(0,f.machine.proposalGateStats().gates());
        }
        try (var f=new Fixture(root.resolve("term"))) {
            f.prepare().get(5,TimeUnit.SECONDS); f.machine.notifyNotLeader(List.of()); assertEquals(0,f.machine.proposalGateStats().gates());
            f.leader.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,2); f.index=2;
            assertNotNull(f.propose(f.fencing).getException()); f.prepare().get(5,TimeUnit.SECONDS); var fresh=f.propose(f.fencing); assertNull(fresh.getException()); f.machine.cancelTransaction(fresh);
        }
    }
}
