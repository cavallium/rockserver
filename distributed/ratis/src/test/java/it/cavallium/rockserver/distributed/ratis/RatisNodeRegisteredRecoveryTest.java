package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced receiver registrations are cold authority, not historical SEED/ACTIVATE artifacts. */
class RatisNodeRegisteredRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final UUID column=new UUID(2,3),pool=RatisPoolFixtures.ID,otherPool=new UUID(0,900002);
        final DistributedSchema schema=new DistributedSchema(new UUID(1,1),new UUID(1,2),new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,column,"data",ColumnSchema.of(IntArrayList.of(1),new ObjectArrayList<>(),true),null)));
        final RatisNodeRuntimeTest.Env env;
        final RatisNodePlan plan;
        final RangeDirectory original,current;
        final ReplicaDescriptor receiver,source;
        final TopologyChange.Definition split;
        final NativeMetadataRecoveryRegistration metadata;
        final List<NativeCurrentRangeRecovery> children=new ArrayList<>();
        final RatisRoutedClient routed;
        final NativeRangeImage.Limits images=new NativeRangeImage.Limits(4<<20,16<<20,4);
        Fixture(boolean twoPools)throws Exception {
            env=new RatisNodeRuntimeTest.Env(root,false,schema.digest());var first=env.routed.directory().ranges().getFirst();receiver=first.replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();source=first.replicas().stream().filter(r->!r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
            var parent=new RangeDescriptor(new UUID(980,1),new UUID(981,1),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(receiver,source));
            original=directory(schema,1,List.of(parent));current=directory(schema,2,List.of(range(2,null,new byte[]{(byte)128}),range(3,new byte[]{(byte)128},null)));split=new TopologyChange.Definition(new UUID(982,1),original,current);
            var domain=new RatisNodePlan.Domain(original,Map.of(column,"data"),Set.of());plan=new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),List.of(domain),env.plan.catalogue(),List.of(),List.of(),Map.of(),4,twoPools?List.of(RatisPoolFixtures.spec(pool,false),RatisPoolFixtures.spec(otherPool,false)):List.of(RatisPoolFixtures.spec(pool,false)),Map.of(schema.domainId(),pool),Map.of(env.plan.catalogue().groupId(),pool));
            routed=new RatisRoutedClient(original,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key){throw new AssertionError("No USER route before recovery");}public CommandResult command(RangeDescriptor descriptor,DistributedCommand command){throw new AssertionError("No USER route before recovery");}});
            NativeMetadataRecoveryRegistration meta;
            try(var nativeSource=sourcePool(root.resolve("source-parent"))){
                var store=openSource(nativeSource,parent);try(var gate=store.prepareTopologyFreeze(split,new byte[32])){assertTrue(gate.ready());var freeze=store.applyTopologyFreeze(new ApplyPosition(1,1),split,new byte[32]);var image=NativeRangeImage.writeMetadata(root.resolve("source-meta.image"),store,images);var cfg=store.configuration();meta=new NativeMetadataRecoveryRegistration(new UUID(983,1),plan.catalogue().groupId(),new ApplyPosition(1,40),original,freeze,receiver,source,cfg.term(),cfg.index(),cfg.data(),image.position(),image.sha256(),Set.of(),null);}
            }
            metadata=meta;
            try(var nativeSource=sourcePool(root.resolve("source-current"))){for(var range:current.ranges()){
                var store=openSource(nativeSource,range);byte key=range.lowerInclusive()==null?7:(byte)200;store.apply(new ApplyPosition(1,1),new DistributedCommand(range.groupId(),1,schema.digest(),new OperationId(new UUID(984,1),1,children.size()),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{key},new byte[]{44}))));
                var image=NativeRangeImage.write(root.resolve("current-"+children.size()+".image"),store,images);var cfg=store.configuration();children.add(new NativeCurrentRangeRecovery(new UUID(983,2+children.size()),plan.catalogue().groupId(),new ApplyPosition(1,40),current,schema,range.groupId(),receiver,source,image.position(),image.sha256(),cfg.term(),cfg.index(),cfg.data(),null,Map.of(column,"data"),Set.of(),Map.of()));
            }}
        }
        RangeDescriptor range(int id,byte[] lower,byte[] upper){return new RangeDescriptor(new UUID(980,id),new UUID(981,id),1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(receiver,source));}
        RangeDirectory directory(DistributedSchema schema,long generation,List<RangeDescriptor> ranges){return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),generation,schema.columnIds(),0,ranges);}
        DistributedStoragePool sourcePool(Path path)throws Exception {var result=DistributedStoragePool.open(path,ConfigParser.parseDefault(),org.slf4j.LoggerFactory.getLogger(getClass()));result.configureSnapshotAdmission(new PoolSnapshotAdmission.Limits(8,32L<<20,Duration.ofMinutes(1),0));return result;}
        ReplicatedRangeStore openSource(DistributedStoragePool sourcePool,RangeDescriptor range)throws Exception {var store=sourcePool.openRange(new RangeOwnership.Owner(range.groupId(),range.rangeEpoch()),new RangeOwnership.Interval(schema.domainId(),schema.columnIds(),range.lowerInclusive(),range.upperExclusive()),schema.digest(),Map.of(column,"data"),Set.of());store.enableLogicalSchema(schema);var cfg=RaftConfigurationProto.newBuilder();for(var replica:range.replicas())cfg.addPeers(RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).build().getRaftPeerProto());store.initializeConfiguration(cfg.build().toByteArray());store.applyNoop(new ApplyPosition(1,0));return store;}
        RatisNodeRuntime.Settings settings(){var old=env.settings();return new RatisNodeRuntime.Settings(images,old.retention(),old.clockPeriod(),1,4,Duration.ofSeconds(5),old.exports(),old.resources());}
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup,RatisNodeRuntimeTest.FakeServer server,RatisNodeRuntime.Settings settings)throws IOException {return RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(schema.domainId(),routed),env.scheduler,env.timer,()->20L,settings,startup,server::create,List.of(schema));}
        void force(boolean duplicatePool,boolean badMembership)throws Exception {
            try(var node=open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer(),settings())){node.start();var nativePool=node.nativePool(pool);nativePool.openMetadataRecovery(metadata);
                for(var child:children){var registration=child;if(badMembership){var cfg=RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(source.peerId()).setAddress(source.address()).build().getRaftPeerProto()).build().toByteArray();registration=new NativeCurrentRangeRecovery(child.transferId(),child.catalogue(),child.catalogueCut(),child.directory(),child.schema(),child.groupId(),child.receiver(),child.sourceOwner(),child.imageCut(),child.imageHash(),0,-1,cfg,null,child.physicalNames(),child.mergeColumns(),child.sourceSchemaRows());}nativePool.openCurrentRangeRecovery(registration);if(duplicatePool)node.nativePool(otherPool).openCurrentRangeRecovery(registration);}
            }
        }
        @Override public void close()throws Exception {routed.close();env.close();}
    }
    private static ReplicatedRangeStore store(RatisNodeRuntimeTest.FakeServer server,UUID group)throws Exception {var field=RatisRangeStateMachine.class.getDeclaredField("store");field.setAccessible(true);return (ReplicatedRangeStore)field.get(server.machines.get(RaftGroupId.valueOf(group)));}
    @Test void coldInventoryReopensCurrentChildrenAndMetadataAncestorWithoutInventedBirth()throws Exception {
        try(var fixture=new Fixture(false)){fixture.force(false,false);for(int attempt=0;attempt<2;attempt++){
            var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
            try(var node=fixture.open(RatisNodeRuntime.Startup.RECOVER,server,fixture.settings())){node.start();assertEquals(3,node.localGroups().size());assertTrue(node.nativePool(fixture.pool).topologySeeds().isEmpty());
                var ancestor=store(server,fixture.metadata.groupId());assertEquals(fixture.metadata,ancestor.metadataRecoveryRegistration());assertTrue(ancestor.userHandlesDetached());assertTrue(ancestor.metadataRecoveryPending());assertNull(ancestor.topologyBirth());assertNull(ancestor.topologyActivation());assertThrows(IllegalStateException.class,ancestor::capture);
                for(var child:fixture.children){assertEquals(fixture.pool,node.poolId(child.groupId()));assertEquals(child.range(),node.groupDescriptor(child.groupId()));var current=store(server,child.groupId());assertEquals(child,current.currentRangeRecoveryRegistration());assertTrue(current.topologyRecoveryOnly());assertFalse(current.currentRangeRecoveryReady());assertNull(current.topologyBirth());assertNull(current.topologyActivation());assertThrows(IllegalStateException.class,()->current.readCurrentPoint(fixture.column,new byte[]{7}));assertEquals(child.receiver().role()==ReplicaDescriptor.Role.VOTER?org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER:org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER,node.localGroups().stream().filter(group->group.getGroupId().getUuid().equals(child.groupId())).findFirst().orElseThrow().getPeer(RaftPeerId.valueOf(fixture.plan.peerId())).getStartupRole());assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,server.machines.get(RaftGroupId.valueOf(child.groupId())).recoveryEligibilityGate().snapshot().mode());}
            }
        }}
    }
    @Test void wrongPoolAndDuplicateNativeRegistrationsRejectBeforeServerConstruction()throws Exception {try(var fixture=new Fixture(true)){fixture.force(true,false);var server=new RatisNodeRuntimeTest.FakeServer();assertThrows(IOException.class,()->fixture.open(RatisNodeRuntime.Startup.RECOVER,server,fixture.settings()));assertFalse(server.created);}}
    @Test void sourceConfigurationMustMatchExactPublishedMembershipBeforeServerConstruction()throws Exception {try(var fixture=new Fixture(false)){fixture.force(false,true);var server=new RatisNodeRuntimeTest.FakeServer();assertThrows(IllegalArgumentException.class,()->fixture.open(RatisNodeRuntime.Startup.RECOVER,server,fixture.settings()));assertFalse(server.created);}}    @Test void foreignClusterAndDatabaseRegistrationsRejectBeforeRaftExposure()throws Exception {
        for(boolean foreignCluster:List.of(true,false)){
            Path caseRoot=Files.createDirectory(root.resolve(foreignCluster?"foreign-cluster":"foreign-database"));Path originalRoot=root;root=caseRoot;
            try(var fixture=new Fixture(false)){
                var child=fixture.children.getFirst();var schema=new DistributedSchema(foreignCluster?new UUID(985,1):fixture.schema.clusterId(),foreignCluster?fixture.schema.databaseId():new UUID(985,2),fixture.schema.domainId(),fixture.schema.schemaEpoch(),0,fixture.schema.columns());var directory=fixture.directory(schema,child.directory().generation(),child.directory().ranges());
                fixture.children.set(0,new NativeCurrentRangeRecovery(child.transferId(),child.catalogue(),child.catalogueCut(),directory,schema,child.groupId(),child.receiver(),child.sourceOwner(),child.imageCut(),child.imageHash(),child.configurationTerm(),child.configurationIndex(),child.sourceConfiguration(),null,child.physicalNames(),child.mergeColumns(),child.sourceSchemaRows()));
                fixture.force(false,false);var server=new RatisNodeRuntimeTest.FakeServer();var rejected=assertThrows(IOException.class,()->fixture.open(RatisNodeRuntime.Startup.RECOVER,server,fixture.settings()));assertTrue(rejected.getMessage().contains("namespace/domain pool"));assertFalse(server.created);assertFalse(Files.exists(fixture.env.nodeRoot.resolve("topology-exposure/registered")));
            }finally{root=originalRoot;}
        }
    }
    @Test void exactAdditionalExposureDebtRejectsBeforeRaftAndRetainsNativeRegistrationsForRetry()throws Exception {
        try(var fixture=new Fixture(false)){
            fixture.force(false,false);var original=fixture.settings();var policy=original.resources();var spec=fixture.plan.pools().get(fixture.pool);long baseline=RatisMetadataClaims.JOURNAL_BYTES+RatisMetadataClaims.POOL_BYTES+RatisMetadataClaims.RECOVERY_GRAPH_BYTES;
            for(var group:List.of(fixture.metadata.groupId(),fixture.children.get(0).groupId(),fixture.children.get(1).groupId()))baseline=Math.addExact(baseline,RatisMetadataClaims.claim(fixture.pool,group,RatisNodePlan.Role.DATA,original.retention(),spec.replicas()).bytes());
            long exposure=Math.multiplyExact(224L,fixture.children.size()+1);var bounded=new RatisNodeRuntime.ResourceLimits(policy.node(),policy.pool(),policy.maximumSourceImageDiskBytes(),Math.addExact(baseline,exposure)-1,policy.maximumOperatorCacheBytes(),policy.maximumNativeCacheBytes(),policy.committedApply(),policy.diskRunway(),policy.migration(),policy.nativeExtents());
            var settings=new RatisNodeRuntime.Settings(original.images(),original.retention(),original.clockPeriod(),original.propagationVisits(),original.maxPropagationRanges(),original.shutdownTimeout(),original.exports(),bounded);var deniedServer=new RatisNodeRuntimeTest.FakeServer();
            var denied=assertThrows(IOException.class,()->fixture.open(RatisNodeRuntime.Startup.RECOVER,deniedServer,settings));assertTrue(denied.getMessage().contains("recovery exposure exceeds node durable metadata ceiling"));assertFalse(deniedServer.created);assertFalse(Files.exists(fixture.env.nodeRoot.resolve("topology-exposure/registered")));
            var retry=new RatisNodeRuntimeTest.FakeServer();retry.skipFirstNoop=true;try(var node=fixture.open(RatisNodeRuntime.Startup.RECOVER,retry,original)){node.start();assertEquals(fixture.metadata,store(retry,fixture.metadata.groupId()).metadataRecoveryRegistration());for(var child:fixture.children)assertEquals(child,store(retry,child.groupId()).currentRangeRecoveryRegistration());}
        }
    }

}
