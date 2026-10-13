package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** One genuine local voter, native catalogue rows and controlled Raft calls; no socket or distributed quorum. */
class RatisTopologySeedRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final UUID COLUMN=new UUID(2,3),SESSION=new UUID(62,1);
    static DistributedSchema schema(){var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);return new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,1,List.of(new DistributedSchema.Column(1,COLUMN,"data",shape,null)));}
    static <T>T field(Object owner,String name,Class<T> type)throws Exception{var field=owner.getClass().getDeclaredField(name);field.setAccessible(true);return type.cast(field.get(owner));}
    static void field(Object owner,String name,Object value)throws Exception{var field=owner.getClass().getDeclaredField(name);field.setAccessible(true);field.set(owner,value);}
    final class Fixture implements AutoCloseable {
        final RatisNodeRuntimeTest.Env env;
        final RatisNodePlan plan;final RatisRoutedClient routed;
        final TopologyChange.Definition definition;final ReplicaDescriptor local;
        final AtomicInteger authorityReads=new AtomicInteger(),borrowedCloses=new AtomicInteger();
        final java.util.function.UnaryOperator<RatisNodeRuntime.Settings> configure;RatisNodeRuntimeTest.FakeServer server;RatisNodeRuntime node;boolean authorityUnavailable,crossedReply;
        TopologyChange.State catalogueState;
        Fixture()throws Exception{this(true);}
        Fixture(boolean recordFreezeProof)throws Exception{this(recordFreezeProof,16);}
        Fixture(boolean recordFreezeProof,int maximumGroups)throws Exception{this(recordFreezeProof,maximumGroups,java.util.function.UnaryOperator.identity());}
        Fixture(boolean recordFreezeProof,int maximumGroups,java.util.function.UnaryOperator<RatisNodeRuntime.Settings> configure)throws Exception{
            this.configure=configure;env=new RatisNodeRuntimeTest.Env(root,false,schema().digest());local=env.plan.domains().getFirst().directory().ranges().getFirst().replicas().stream().filter(owner->owner.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
            var parent=new RangeDescriptor(new UUID(6,2),new UUID(3,1),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var source=new RangeDirectory(schema().clusterId(),schema().databaseId(),schema().domainId(),1,schema().digest(),7,Set.of(COLUMN),1,List.of(parent));
            schema().requireMatches(source);
            plan=new RatisNodePlan(env.plan.nodeId(), env.plan.nodeIncarnation(), List.of(new RatisNodePlan.Domain(source, env.plan.columns(), env.plan.merges())), env.plan.catalogue(), List.of(), List.of(), Map.of(), maximumGroups,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(new RatisNodePlan.Domain(source, env.plan.columns(), env.plan.merges()))),RatisPoolFixtures.groups(env.plan.catalogue(),List.of()));
            routed=new RatisRoutedClient(source,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("Fixture routing transport is unused");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("Fixture routing transport is unused");}});
            var left=new RangeDescriptor(new UUID(63,1),new UUID(63,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,List.of(local));var right=new RangeDescriptor(new UUID(63,3),new UUID(63,4),1,0,new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,source.schemaDigest(),8,source.columns(),1,List.of(left,right));definition=new TopologyChange.Definition(new UUID(63,5),source,target);
            open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT);catalogueState=TopologyChange.State.begin(definition.header()).beginFencing();commit(catalogueState);
            var parentStore=parentStore();var command=new DistributedCommand(parent.groupId(),1,schema().digest(),new OperationId(SESSION,1,0),List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{42})));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(parentStore.apply(new ApplyPosition(1,1),command)).status());
            var digest=authority(definition.header(),TopologyChange.Stage.FREEZE).admissionDigest();var record=new RatisTopologyFreezeRecord(definition,parent.groupId(),digest);var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));
            machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);var transaction=machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(plan.peerId())).setGroupId(RaftGroupId.valueOf(parent.groupId())).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(record.encode()))).build());assertNull(transaction.getException());machine.preAppendTransaction(transaction);transaction.initLogEntry(1,2);var applied=machine.applyTransaction(transaction);machine.committedApplyAdmission().pump();var freeze=NativeTopologyFreeze.decode(applied.get(5,TimeUnit.SECONDS).getContent().toByteArray());machine.topologyCleanup().get(5,TimeUnit.SECONDS);if(recordFreezeProof){catalogueState=catalogueState.frozen(freeze.proof(local.replicaId()));commit(catalogueState);}
        }
        void open(RatisNodeRuntime.Startup startup)throws Exception{
            server=new RatisNodeRuntimeTest.FakeServer();node=RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(routed.directory().domainId(), routed), env.scheduler, env.timer, ()->20L, configure.apply(env.settings()), startup, server::create, List.of(schema()));field(node,"seedAuthority",(RatisNodeRuntime.SeedAuthorityReader)this::authority);
            for(var machine:server.machines.values())field(machine,"topologyAuthority",(RatisRangeStateMachine.TopologyAuthorityReader)this::authority);node.start();
        }
        RatisTopologyAuthority.View authority(TopologyChange.Header header,TopologyChange.Stage stage)throws IOException{authorityReads.incrementAndGet();if(authorityUnavailable)throw new IOException("Catalogue unavailable");header.requireDefinition(definition);return RatisTopologyAuthority.readNative(env.catalogue.store,env.outer.directoryColumn,plan.domains().getFirst().directory().clusterId(),plan.domains().getFirst().directory().databaseId(),RatisTopologyAuthority.Request.of(header,stage));}
        void commit(TopologyChange.State state)throws Exception{
            var directory=state.phase()==TopologyChange.Phase.PUBLISHED || state.phase()==TopologyChange.Phase.ACTIVE || state.phase()==TopologyChange.Phase.RETIRED ? definition.targetDirectory() : definition.sourceDirectory();byte[] domain=ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();
            var command=new DistributedCommand(env.outer.catalogue,1,new byte[32],new OperationId(new UUID(62,2),1,env.catalogue.next.get()),List.of(new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.definitionKey(definition.digest()),definition.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.headKey(directory.domainId()),state.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,RatisTopologyCatalogue.stateKey(definition.intentId()),state.encode()),new Mutation(Mutation.Kind.PUT,env.outer.directoryColumn,domain,RangeDirectoryCodec.encode(directory))));assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(env.catalogue.store.apply(new ApplyPosition(1,env.catalogue.next.getAndIncrement()),command)).status());catalogueState=state;
        }
        ReplicatedRangeStore parentStore()throws Exception{return field(server.machines.get(RaftGroupId.valueOf(definition.parents().getFirst().groupId())),"store",ReplicatedRangeStore.class);}
        DistributedStoragePool pool()throws Exception{return node.nativePool(RatisPoolFixtures.ID);}
        RatisTopologySeedRpc.Request request(){return new RatisTopologySeedRpc.Request(definition,plan.catalogue().groupId(),definition.parents().getFirst().groupId(),local.replicaId());}
        RaftClient borrowed(){var request=request();var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{if(!method.getName().equals("sendStaleRead"))throw new AssertionError(method);assertEquals(0L,args[1]);assertEquals(RaftPeerId.valueOf(local.peerId()),args[2]);var message=server.machines.get(RaftGroupId.valueOf(request.anchorGroup())).queryStale((Message)args[0],0).get(5,TimeUnit.SECONDS);return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(crossedReply ? "wrong-peer" : local.peerId())).setGroupId(RaftGroupId.valueOf(request.anchorGroup())).setCallId(1).setSuccess(true).setMessage(message).build();});return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(request.anchorGroup());case "io"->io;case "close"->{borrowedCloses.incrementAndGet();yield null;}default->throw new AssertionError(method);});}
        public void close()throws Exception{node.close();routed.close();env.close();}
    }
    @Test void localGroupCapacityRejectsBeforeNativeBirthOrRaftExposure()throws Exception{
        try(var f=new Fixture(true,1)){
            assertThrows(IOException.class,()->f.node.seedTopology(f.request()));
            assertNull(f.node.nativePool(RatisPoolFixtures.ID).topologySeed(f.definition.intentId()));assertEquals(1,f.server.storages.size());assertEquals(1,field(f.node,"groups",RatisRuntimeGroups.class).raftGroups().size());
        }
    }
    @Test void parentSeedRpcReturnsForcedRecipeAndLateExactRetryRetainsOriginalsAcrossColdOpen()throws Exception{
        try(var f=new Fixture();var client=new RatisTopologySeedClient(f.borrowed())){var parent=f.parentStore();assertArrayEquals(new byte[]{42},parent.readCurrentPoint(COLUMN,new byte[]{1}));byte[] identity=Files.readAllBytes(f.env.nodeRoot.resolve("node.identity"));var seed=client.seed(f.request());assertEquals(f.definition.targets().size()+1,f.server.storages.size());var parentColumns=field(parent,"columns",Map.class);
            var state=f.catalogueState;for(var target:f.definition.targets()){var child=field(f.server.machines.get(RaftGroupId.valueOf(target.groupId())),"store",ReplicatedRangeStore.class);assertTrue(child.topologyBirthPending());assertEquals(new ApplyPosition(1,0),child.applied());assertEquals(new ApplyPosition(0,0),seed.proof(target.groupId()).position());assertSame(parentColumns.get(COLUMN),field(child,"columns",Map.class).get(COLUMN),"SEED retains physical aliases rather than copying USER rows");state=state.seeded(seed.proof(target.groupId()));}f.commit(state.publish());var originalAuthority=seed.authority().encode();int reads=f.authorityReads.get();f.authorityUnavailable=true;assertArrayEquals(seed.encode(),client.seed(f.request()).encode());assertEquals(reads,f.authorityReads.get());assertArrayEquals(originalAuthority,seed.authority().encode());f.node.close();f.open(RatisNodeRuntime.Startup.RECOVER);assertArrayEquals(seed.encode(),f.node.seedTopology(f.request()).encode());assertArrayEquals(identity,Files.readAllBytes(f.env.nodeRoot.resolve("node.identity")));f.crossedReply=true;assertThrows(IOException.class,()->client.seed(f.request()));assertEquals(0,f.borrowedCloses.get());}
    }
    @Test void stageConfigurationAndLocalOwnerMismatchesRejectBeforeAnySeedBirth()throws Exception{
        try(var f=new Fixture(false)){assertThrows(IOException.class,()->f.node.seedTopology(f.request()));assertNull(f.pool().topologySeed(f.definition.intentId()));f.commit(f.catalogueState.frozen(f.parentStore().topologyFreeze().proof(f.local.replicaId())));f.server.wrongEndpoint=true;assertThrows(IOException.class,()->f.node.seedTopology(f.request()));f.server.wrongEndpoint=false;assertNull(f.pool().topologySeed(f.definition.intentId()));assertThrows(IllegalArgumentException.class,()->new RatisTopologySeedRpc.Request(f.definition,f.plan.catalogue().groupId(),f.definition.targets().getFirst().groupId(),f.local.replicaId()));assertThrows(IOException.class,()->f.node.seedTopology(new RatisTopologySeedRpc.Request(f.definition,new UUID(64,1),f.request().anchorGroup(),f.local.replicaId())));assertNull(f.pool().topologySeed(f.definition.intentId()));var foreign=new ReplicaDescriptor(new UUID(64,2),new UUID(64,3),new UUID(64,4),f.local.peerId(),f.local.address(),f.local.role());var source=f.definition.sourceDirectory();var parent=source.ranges().getFirst();var foreignParent=new RangeDescriptor(parent.rangeId(),parent.groupId(),parent.rangeEpoch(),parent.configurationVersion(),parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),List.of(foreign));var changedSource=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation(),source.columns(),source.routingPrefixBytes(),List.of(foreignParent));var target=f.definition.targetDirectory();var changedTarget=new RangeDirectory(target.clusterId(),target.databaseId(),target.domainId(),target.schemaEpoch(),target.schemaDigest(),target.generation(),target.columns(),target.routingPrefixBytes(),target.ranges().stream().map(range->new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),range.topologyState(),List.of(foreign))).toList());var changed=new TopologyChange.Definition(f.definition.intentId(),changedSource,changedTarget);assertThrows(IOException.class,()->f.node.seedTopology(new RatisTopologySeedRpc.Request(changed,f.plan.catalogue().groupId(),parent.groupId(),foreign.replicaId())));assertNull(f.pool().topologySeed(f.definition.intentId()));}
    }
    @Test void unknownForceKeepsOriginalAuthorityAndRetriesWithoutAnotherRead()throws Exception{
        try(var f=new Fixture()){byte[] authority=f.catalogueState.encode();var once=new java.util.concurrent.atomic.AtomicBoolean(true);f.pool().topologySeedForceObserver(after->{if(after && once.compareAndSet(true,false))throw new org.rocksdb.RocksDBException("Lost force reply");});assertThrows(IOException.class,()->f.node.seedTopology(f.request()));assertNull(f.pool().topologySeed(f.definition.intentId()));int reads=f.authorityReads.get();f.authorityUnavailable=true;var seed=f.node.seedTopology(f.request());assertEquals(reads,f.authorityReads.get());assertArrayEquals(authority,seed.authority().encode());assertArrayEquals(seed.encode(),f.node.seedTopology(f.request()).encode());}
    }
    @Test void seedArtifactDoesNotClaimChildProofBeforeOwnCommittedPrefix()throws Exception{
        try(var f=new Fixture();var client=new RatisTopologySeedClient(f.borrowed())){f.server.skipFirstNoop=true;var seed=client.seed(f.request());for(var target:f.definition.targets()){var machine=f.server.machines.get(RaftGroupId.valueOf(target.groupId()));var store=field(machine,"store",ReplicatedRangeStore.class);assertNull(store.applied());assertEquals(new ApplyPosition(0,0),seed.proof(target.groupId()).position());var request=new RatisTopologyOwnerRpc.Target(f.definition,TopologyChange.Stage.SEED,f.plan.catalogue().groupId(),target.groupId(),f.local.replicaId());assertThrows(ExecutionException.class,()->machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisTopologyOwnerRpc.request(request))),0).get(5,TimeUnit.SECONDS));assertTrue(store.topologyBirthPending());}}
    }

}
