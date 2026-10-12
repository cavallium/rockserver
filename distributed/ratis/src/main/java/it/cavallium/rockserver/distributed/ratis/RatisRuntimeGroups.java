package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;

/** Local bootstrap placement plus forced native seed metadata. Requests cannot extend this registry. */
final class RatisRuntimeGroups {
    record Entry(RaftGroup group,RatisNodePlan.Role role,RangeDirectory directory,RangeDescriptor descriptor,NativeTopologySeed seed,NativeTopologyLearnerRegistration learner,RatisTopologyRelocationRecord relocation,boolean retired) {}
    private final RatisNodePlan plan;
    private final Map<UUID,Entry> groups=new ConcurrentHashMap<>();
    private final Map<UUID,NativeTopologySeed> retirements=new ConcurrentHashMap<>();
    private volatile Set<UUID> dataGroups=Set.of();
    NativeTopologySeed retirement(UUID group){return retirements.get(group);}
    RatisRuntimeGroups(RatisNodePlan plan){this.plan=plan;for(var group:plan.localGroups()){var id=group.getGroupId().getUuid();var role=plan.role(id);groups.put(id,new Entry(group,role,role==RatisNodePlan.Role.CATALOGUE ? null : plan.groupDirectory(id),role==RatisNodePlan.Role.CATALOGUE ? null : plan.groupDescriptor(id),null,null,null,false));}dataGroups=snapshotDataGroups();}
    Entry require(UUID group){var entry=groups.get(group);if(entry==null)throw new IllegalArgumentException("Unregistered runtime group");return entry;}
    List<Entry> entries(){return groups.values().stream().sorted(Comparator.<Entry>comparingLong(e->e.seed()==null ? -1 : e.seed().definition().sourceDirectory().generation()).thenComparing(e->e.group().getGroupId().toString())).toList();}
    List<RaftGroup> raftGroups(){return entries().stream().map(Entry::group).toList();}
    Set<UUID> dataGroups(){return dataGroups;}
    private Set<UUID> snapshotDataGroups(){var ids=new HashSet<UUID>();for(var entry:groups.values())if(entry.role()==RatisNodePlan.Role.DATA)ids.add(entry.group().getGroupId().getUuid());return Set.copyOf(ids);}
    synchronized void register(NativeTopologySeed seed){
        var local=seed.localReplica();var source=seed.definition().sourceDirectory();var bootstrap=plan.directory();
        if(!local.nodeId().equals(plan.nodeId()) || !local.nodeIncarnation().equals(plan.nodeIncarnation()) || !local.peerId().equals(plan.peerId()) || !local.address().equals(plan.address())
                || !source.clusterId().equals(bootstrap.clusterId()) || !source.databaseId().equals(bootstrap.databaseId()) || !source.domainId().equals(bootstrap.domainId()))throw new IllegalArgumentException("Native topology seed local identity/namespace differs from bootstrap");
        for(var parent:seed.definition().parents()){var old=require(parent.groupId());if(old.role()!=RatisNodePlan.Role.DATA || !old.descriptor().equals(parent))throw new IllegalArgumentException("Topology seed parent differs from registered native placement");}
        for(var target:seed.definition().targets()){var old=groups.get(target.groupId());if(old!=null && (old.seed()==null || !old.seed().equals(seed) || !old.descriptor().equals(target)))throw new IllegalArgumentException("Topology child identity already registered differently");}
        for(var parent:seed.definition().parents()){retirements.put(parent.groupId(),seed);var old=require(parent.groupId());groups.put(parent.groupId(),new Entry(old.group(),old.role(),source,parent,old.seed(),old.learner(),old.relocation(),true));}
        for(var target:seed.definition().targets())groups.putIfAbsent(target.groupId(),new Entry(group(target),RatisNodePlan.Role.DATA,seed.definition().targetDirectory(),target,seed,null,null,false));
        dataGroups=snapshotDataGroups();
    }
    synchronized void register(NativeTopologyLearnerRegistration learner){
        var local=learner.receiver();var source=learner.definition().sourceDirectory();var bootstrap=plan.directory();
        if(!local.nodeId().equals(plan.nodeId()) || !local.nodeIncarnation().equals(plan.nodeIncarnation()) || !local.peerId().equals(plan.peerId()) || !local.address().equals(plan.address())
                || !learner.catalogue().equals(plan.catalogue().groupId()) || !source.clusterId().equals(bootstrap.clusterId()) || !source.databaseId().equals(bootstrap.databaseId()) || !source.domainId().equals(bootstrap.domainId()))throw new IllegalArgumentException("Learner local identity/namespace differs from bootstrap");
        var target=learner.definition().targets().getFirst();var old=groups.get(target.groupId());
        if(old!=null && (old.learner()==null || !old.learner().equals(learner)))throw new IllegalArgumentException("Learner cannot adopt an existing local group");
        groups.putIfAbsent(target.groupId(),new Entry(group(target),RatisNodePlan.Role.DATA,learner.definition().targetDirectory(),target,null,learner,null,false));dataGroups=snapshotDataGroups();
    }
    synchronized void relocation(RatisTopologyRelocationRecord record){var definition=record.definition();var id=definition.parents().getFirst().groupId();var old=require(id);if(old.role()!=RatisNodePlan.Role.DATA || old.retired() || !definition.header().clusterId().equals(plan.directory().clusterId()) || !definition.header().databaseId().equals(plan.directory().databaseId()) || !definition.header().domainId().equals(old.directory().domainId()))throw new IllegalArgumentException("Relocation changed registered namespace/role");var directory=record.action()==RatisTopologyRelocationRecord.Action.COMPLETE?definition.targetDirectory():definition.sourceDirectory();if(old.directory().generation()>directory.generation())return;var descriptor=record.action()==RatisTopologyRelocationRecord.Action.COMPLETE?definition.targets().getFirst():definition.parents().getFirst();if(old.descriptor().rangeEpoch()!=descriptor.rangeEpoch() || !old.descriptor().rangeId().equals(descriptor.rangeId()))throw new IllegalArgumentException("Relocation changed registered lineage");groups.put(id,new Entry(group(descriptor),old.role(),directory,descriptor,old.seed(),old.learner(),record,old.retired()));}
    synchronized RaftGroup reconcileRelocationConfiguration(UUID id,byte[] nativeConfiguration)throws java.io.IOException{
        var old=require(id);if(old.relocation()==null)return old.group();var record=old.relocation();boolean joint=record.requireNativeConfiguration(nativeConfiguration);RaftGroup raft;
        if(joint){var peers=new TreeMap<String,RaftPeer>();for(var range:List.of(record.definition().parents().getFirst(),record.definition().targets().getFirst()))for(var peer:group(range).getPeers())peers.merge(peer.getId().toString(),peer,(before,after)->before.getStartupRole()==RaftPeerRole.FOLLOWER?before:after);raft=RaftGroup.valueOf(old.group().getGroupId(),peers.values());}
        else raft=group(RatisTopologyRelocationRecord.sameConfiguration(nativeConfiguration,record.targetConfiguration())?record.definition().targets().getFirst():record.definition().parents().getFirst());
        groups.put(id,new Entry(raft,old.role(),old.directory(),old.descriptor(),old.seed(),old.learner(),record,old.retired()));return raft;
    }
    synchronized void schemaActivated(SchemaChange.Definition definition,NativeSchemaActivationProof proof){
        var old=require(proof.installation().fence().participant().groupId());if(old.role()!=RatisNodePlan.Role.DATA)return;proof.requireHeader(definition.header());var directory=definition.targetDirectory();var descriptor=directory.ranges().stream().filter(r->r.groupId().equals(proof.installation().fence().participant().groupId())).findFirst().orElseThrow();groups.put(proof.installation().fence().participant().groupId(),new Entry(old.group(),old.role(),directory,descriptor,old.seed(),old.learner(),old.relocation(),old.retired()));
    }
    synchronized void topologyActivated(TopologyChange.Definition definition,NativeTopologyActivation activation){
        var old=require(activation.birth().groupId());if(old.seed()==null || old.role()!=RatisNodePlan.Role.DATA)throw new IllegalArgumentException("Topology activation group is not a registered native birth");activation.requireSeed(old.seed());definition.header().requireDefinition(old.seed().definition());var directory=definition.targetDirectory();if(old.directory().generation()>directory.generation())return;var descriptor=definition.targets().stream().filter(target->target.groupId().equals(activation.birth().groupId())).findFirst().orElseThrow();groups.put(descriptor.groupId(),new Entry(old.group(),old.role(),directory,descriptor,old.seed(),old.learner(),old.relocation(),old.retired()));
    }
    RaftGroup group(RangeDescriptor descriptor){return RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),descriptor.replicas().stream().map(replica->{var builder=RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER);var stream=plan.streamAddresses().get(replica.peerId());if(stream!=null)builder.setDataStreamAddress(stream);return builder.build();}).toList());}
}
