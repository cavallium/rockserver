package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced native births and controlled local group management; no socket or quorum acceptance. */
class RatisTopologyRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    DistributedSchema schema(){var shape=it.cavallium.rockserver.core.common.ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),it.unimi.dsi.fastutil.objects.ObjectArrayList.of(),true);return new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(new DistributedSchema.Column(1,new UUID(2,3),"data",shape,null)));}
    RatisNodeRuntime open(RatisNodeRuntimeTest.Env env,RatisNodeRuntimeTest.FakeServer fake,RatisNodeRuntime.Startup startup)throws Exception{var plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),env.plan.domains(),env.plan.catalogue(),env.plan.homes(),env.plan.imports(),env.plan.streamAddresses(),4);return RatisNodeRuntime.open(plan, env.nodeRoot, ConfigParser.parseDefault(), env.catalogue.epochs, Map.of(env.routed.directory().domainId(), env.routed), env.scheduler, env.timer, ()->20L, env.settings(), startup, fake::create, List.of(schema()));}
    static <T>T field(Object owner,String name,Class<T> type)throws Exception{var field=owner.getClass().getDeclaredField(name);field.setAccessible(true);return type.cast(field.get(owner));}
    NativeTopologySeed force(RatisNodeRuntime node,RatisNodeRuntimeTest.Env env,RatisNodeRuntimeTest.FakeServer fake)throws Exception{
        var source=env.plan.domains().getFirst().directory();var parent=source.ranges().getFirst();var left=new RangeDescriptor(new UUID(51,1),new UUID(51,2),1,0,null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,parent.replicas());var right=new RangeDescriptor(new UUID(51,3),new UUID(51,4),1,0,new byte[]{64},parent.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),List.of(left,right,source.ranges().getLast()));var definition=new TopologyChange.Definition(new UUID(51,5),source,target);
        var store=field(fake.machines.get(RaftGroupId.valueOf(parent.groupId())),"store",ReplicatedRangeStore.class);var digest=new RatisTopologyAuthority.View(env.plan.catalogue().groupId(),new ApplyPosition(1,1),definition,TopologyChange.State.begin(definition.header()).beginFencing()).admissionDigest();
        NativeTopologyFreeze freeze;try(var gate=store.prepareTopologyFreeze(definition,digest)){assertTrue(gate.state().ready());freeze=store.applyTopologyFreeze(new ApplyPosition(1,store.applied().index()+1),definition,digest);}
        var authority=TopologyChange.State.begin(definition.header()).beginFencing();for(var owner:parent.replicas())authority=authority.frozen(freeze.proof(owner.replicaId()));
        var local=parent.replicas().stream().filter(replica->replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();return field(node,"pool",DistributedStoragePool.class).seedTopology(definition,authority,List.of(store),local);
    }
    @Test void forcedChildrenRegisterBeforeVotingAndUnknownReplyRetryKeepsIdentity()throws Exception{
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema().digest())){var fake=new RatisNodeRuntimeTest.FakeServer();try(var node=open(env,fake,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){
            node.start();byte[] identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));var seed=force(node,env,fake);int initialAdds=fake.adds;fake.loseAddReply=true;node.installTopologySeed(seed);assertEquals(initialAdds+2,fake.adds);
            for(var target:seed.definition().targets()){var id=RaftGroupId.valueOf(target.groupId());assertTrue(fake.storages.containsKey(id));var machine=fake.machines.get(id);assertNotNull(field(machine,"topologyAuthority",Object.class));assertNotNull(field(machine,"schemaReplicas",Object.class));assertNotNull(field(machine,"exportSources",Object.class));var store=field(machine,"store",ReplicatedRangeStore.class);assertTrue(store.topologyBirthPending());assertFalse(store.admissionOpen());assertEquals(RangeOwnership.State.FENCED,store.ownership().state());}
            field(node,"transactionExpiry",RatisTransactionExpiryService.class).tick().get(5,java.util.concurrent.TimeUnit.SECONDS);
            assertNull(node.transactionExpiryFailure(),"Pending child births cannot poison ordinary expiry work");
            var groups=field(node,"groups",RatisRuntimeGroups.class);
            assertTrue(groups.dataGroups().contains(seed.definition().parents().getFirst().groupId()),"Retired original outcome journals remain allowed");
            for(var target:seed.definition().targets())assertTrue(groups.dataGroups().contains(target.groupId()));
            node.installTopologySeed(seed);assertEquals(initialAdds+2,fake.adds);assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }}
    }
    @Test void coldVirginBirthDiscoveryIsAllowedButEstablishedMissingRaftMetadataFailsClosed()throws Exception{
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema().digest())){NativeTopologySeed seed;byte[] identity;var first=new RatisNodeRuntimeTest.FakeServer();try(var node=open(env,first,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){node.start();identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));seed=force(node,env,first);}
            var recovered=new RatisNodeRuntimeTest.FakeServer();try(var node=open(env,recovered,RatisNodeRuntime.Startup.RECOVER)){for(var target:seed.definition().targets())assertTrue(recovered.machines.containsKey(RaftGroupId.valueOf(target.groupId())));node.start();assertEquals(4,recovered.storages.size());assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));}
            var target=seed.definition().targets().getFirst();Files.delete(env.nodeRoot.resolve("raft").resolve(target.groupId().toString()).resolve("current/raft-meta"));var missing=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(env,missing,RatisNodeRuntime.Startup.RECOVER));assertFalse(missing.created);assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
        }
    }
    @Test void registryRejectsForeignLocalIdentityAndDoesNotChangeBootstrapIdentity()throws Exception{
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema().digest())){var fake=new RatisNodeRuntimeTest.FakeServer();try(var node=open(env,fake,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){node.start();var seed=force(node,env,fake);var remote=seed.definition().parents().getFirst().replicas().stream().filter(replica->!replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();var foreign=new NativeTopologySeed(seed.definition(),seed.authority(),remote,seed.parentFreezes(),seed.physicalNames(),seed.mergeColumns());var registry=new RatisRuntimeGroups(env.plan);assertThrows(IllegalArgumentException.class,()->registry.register(foreign));assertEquals(2,registry.raftGroups().size());assertThrows(IOException.class,()->node.installTopologySeed(foreign));assertEquals(2,fake.storages.size());}}
    }
    @Test void exposedBirthWithoutAnyNativeNoopCannotRecreateMissingRaftOwner()throws Exception{
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema().digest())){NativeTopologySeed seed;var fake=new RatisNodeRuntimeTest.FakeServer();try(var node=open(env,fake,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT)){node.start();seed=force(node,env,fake);fake.skipFirstNoop=true;node.installTopologySeed(seed);var child=seed.definition().targets().getFirst();assertNull(field(fake.machines.get(RaftGroupId.valueOf(child.groupId())),"store",ReplicatedRangeStore.class).applied());assertTrue(Files.isRegularFile(env.nodeRoot.resolve("topology-exposure").resolve(child.groupId().toString())));}
            var directory=env.nodeRoot.resolve("raft").resolve(seed.definition().targets().getFirst().groupId().toString());try(var files=Files.walk(directory)){for(var file:files.sorted(Comparator.reverseOrder()).toList())Files.delete(file);}var missing=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->open(env,missing,RatisNodeRuntime.Startup.RECOVER));assertFalse(missing.created);
        }
    }

}
