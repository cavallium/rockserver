package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Runtime provenance is independent of image readiness and survives native registry replay order. */
class RatisRuntimeGroupsTest {
    static UUID id(int n){return new UUID(13010,n);}
    final ReplicaDescriptor local=new ReplicaDescriptor(id(1),id(2),id(3),"local","127.0.0.1:7331",ReplicaDescriptor.Role.VOTER);
    final ReplicaDescriptor remote=new ReplicaDescriptor(id(4),id(5),id(6),"catalogue","127.0.0.1:7332",ReplicaDescriptor.Role.VOTER);
    final DistributedSchema schema=new DistributedSchema(id(7),id(8),id(9),1,0,List.of());
    final RangeDescriptor parent=range(10,11,null,null);
    final RangeDirectory directory=directory(1,List.of(parent));
    final TopologyChange.Definition definition=new TopologyChange.Definition(id(12),directory,directory(2,List.of(range(13,14,null,new byte[]{(byte)128}),range(15,16,new byte[]{(byte)128},null))));
    final RatisNodePlan.Catalogue catalogue=new RatisNodePlan.Catalogue(id(17),1,new byte[32],id(18),id(19),List.of(remote));
    final RatisNodePlan.Domain domain=new RatisNodePlan.Domain(directory,Map.of(),Set.of());
    final RatisNodePlan plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(catalogue,List.of()));
    RangeDescriptor range(int range,int group,byte[] lower,byte[] upper){return new RangeDescriptor(id(range),id(group),1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(local));}
    RangeDirectory directory(long generation,List<RangeDescriptor> ranges){return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),1,schema.digest(),generation,Set.of(),0,ranges);}
    byte[] configuration(){return RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(local.peerId()).setAddress(local.address()).build().getRaftPeerProto()).build().toByteArray();}
    NativeCurrentRangeRecovery current(){return new NativeCurrentRangeRecovery(id(20),catalogue.groupId(),new ApplyPosition(1,10),directory,schema,parent.groupId(),local,local,new ApplyPosition(1,2),new byte[32],0,-1,configuration(),null,Map.of(),Set.of(),Map.of());}
    NativeTopologyFreeze freeze(){return new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(1,3),NativeTopologyFreeze.boundsDigest(null,null),new byte[32]);}
    NativeRecoverySupersession fence(NativeCurrentRangeRecovery current,long marker,long catCut){
        var scope=new NativeRecoverySupersession.Scope(catalogue.groupId(),new ApplyPosition(1,catCut),definition.header(),directory,parent.groupId(),local,new byte[32],new byte[32],new byte[32]);
        return new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(current==null?null:current.digest(),current==null?null:current.imageCut(),current==null?null:current.imageHash(),null,null,marker==0?null:new ApplyPosition(1,marker),null));
    }
    NativeMetadataRecoveryRegistration metadata(UUID transfer,long image,long catCut,NativeCurrentRangeRecovery current){return new NativeMetadataRecoveryRegistration(transfer,catalogue.groupId(),new ApplyPosition(1,catCut),directory,freeze(),local,local,0,-1,configuration(),new ApplyPosition(1,image),new byte[32],Set.of(),current==null?null:NativeMetadataRecoveryRegistration.CurrentPredecessor.of(current));}

    @Test void fenceCreatesRetiredDataProvenanceWithoutCurrentMetadataOrConfigurationRecipe(){
        var groups=new RatisRuntimeGroups(plan);var value=fence(null,0,20);groups.preflight(value);groups.register(value);groups.register(NativeRecoverySupersession.decode(value.encode()));
        var entry=groups.require(parent.groupId());assertEquals(value,entry.supersession());assertTrue(entry.retired());assertEquals(RatisNodePlan.Role.DATA,entry.role());assertNull(entry.current());assertNull(entry.metadata());assertNull(entry.seed());assertEquals(directory,entry.directory());
        assertThrows(IllegalArgumentException.class,()->groups.register(current()));
        assertThrows(IllegalArgumentException.class,()->groups.register(fence(null,1,20)));assertEquals(value,groups.require(parent.groupId()).supersession());
        var old=value.scope();var foreign=new NativeRecoverySupersession.Scope(id(900),old.catalogueCut(),old.header(),old.sourceDirectory(),old.groupId(),old.receiver(),old.selectedMetadataRequestDigest(),old.originalCurrentRequestDigest(),old.effectiveCurrentRequestDigest());
        assertThrows(IllegalArgumentException.class,()->new RatisRuntimeGroups(plan).register(new NativeRecoverySupersession(foreign,value.captured())));
    }
    @Test void coldCurrentThenFenceThenSeedPreservesAllOriginalProvenanceAndRejectsCurrentRetry(){
        var groups=new RatisRuntimeGroups(plan);var current=current();groups.register(current);var value=fence(current,5,20);groups.register(value);
        var freeze=freeze();var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(local.replicaId()));
        var seed=new NativeTopologySeed(definition,authority,local,Map.of(parent.groupId(),freeze),Map.of(),Set.of());groups.register(seed);
        var entry=groups.require(parent.groupId());assertEquals(current,entry.current());assertEquals(value,entry.supersession());assertEquals(seed,groups.retirement(parent.groupId()));assertTrue(entry.retired());
        assertThrows(IllegalArgumentException.class,()->groups.preflight(current));assertThrows(IllegalArgumentException.class,()->groups.register(current));
        var metadata=metadata(id(21),6,21,current);groups.register(metadata);entry=groups.require(parent.groupId());
        assertEquals(metadata,entry.metadata());assertEquals(current,entry.current());assertEquals(value,entry.supersession());assertEquals(seed,groups.retirement(parent.groupId()));
        groups.register(metadata);groups.register(value);assertEquals(metadata,groups.require(parent.groupId()).metadata());
    }
    @Test void ordinaryCurrentToMetadataPermanentlyRejectsOldCurrentRetry(){
        var groups=new RatisRuntimeGroups(plan);var current=current();groups.register(current);
        var metadata=metadata(id(25),6,21,current);groups.register(metadata);groups.register(metadata);
        assertNull(groups.require(parent.groupId()).current());assertEquals(metadata,groups.require(parent.groupId()).metadata());
        assertThrows(IllegalArgumentException.class,()->groups.preflight(current));assertThrows(IllegalArgumentException.class,()->groups.register(current));
    }
    @Test void existingMetadataRecipeNeedsOnlyScopeWhileNewCandidateMustCoverCapturedNativeTail(){
        var groups=new RatisRuntimeGroups(plan);var retained=metadata(id(30),6,21,null);groups.register(retained);var value=fence(null,9,30);groups.register(value);
        groups.preflight(retained);groups.register(retained);assertEquals(value,groups.require(parent.groupId()).supersession());
        assertThrows(IllegalArgumentException.class,()->groups.register(metadata(id(31),7,31,null)));
        var next=metadata(id(32),10,31,null);groups.register(next);assertEquals(value,groups.require(parent.groupId()).supersession());assertEquals(next,groups.require(parent.groupId()).metadata());
        groups.register(value);assertEquals(next,groups.require(parent.groupId()).metadata());
    }
    @Test void nativeFenceCanMaterializeAnAbsentRuntimeGroupWithinConfiguredCapacity(){
        var extra=range(40,41,null,null);var source=directory(1,List.of(extra));var targets=directory(2,List.of(range(42,43,null,new byte[]{(byte)128}),range(44,45,new byte[]{(byte)128},null)));var split=new TopologyChange.Definition(id(46),source,targets);
        var scope=new NativeRecoverySupersession.Scope(catalogue.groupId(),new ApplyPosition(1,20),split.header(),source,extra.groupId(),local,new byte[32],new byte[32],new byte[32]);
        var value=new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,null));
        var groups=new RatisRuntimeGroups(plan);assertThrows(IllegalArgumentException.class,()->groups.require(extra.groupId()));groups.register(value);
        assertEquals(value,groups.require(extra.groupId()).supersession());assertEquals(2,groups.raftGroups().size());assertTrue(groups.dataGroups().contains(extra.groupId()));assertNull(groups.require(extra.groupId()).metadata());
    }
}
