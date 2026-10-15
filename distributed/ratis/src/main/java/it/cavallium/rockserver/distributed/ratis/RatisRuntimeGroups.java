package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;

/** Local bootstrap placement plus forced native seed metadata. Requests cannot extend this registry. */
final class RatisRuntimeGroups {
    record Entry(RaftGroup group,RatisNodePlan.Role role,RangeDirectory directory,RangeDescriptor descriptor,NativeTopologySeed seed,NativeTopologyLearnerRegistration learner,RatisTopologyRelocationRecord relocation,NativeCurrentRangeRecovery current,NativeMetadataRecoveryRegistration metadata,boolean retired) {}
    private final RatisNodePlan plan;
    private final Map<UUID,Entry> groups=new ConcurrentHashMap<>();
    private final Map<UUID,NativeTopologySeed> retirements=new ConcurrentHashMap<>();
    private volatile Set<UUID> dataGroups=Set.of();
    NativeTopologySeed retirement(UUID group){return retirements.get(group);}
    RatisRuntimeGroups(RatisNodePlan plan){this.plan=plan;for(var group:plan.localGroups()){var id=group.getGroupId().getUuid();var role=plan.role(id);groups.put(id,new Entry(group,role,role==RatisNodePlan.Role.CATALOGUE ? null : plan.groupDirectory(id),role==RatisNodePlan.Role.CATALOGUE ? null : plan.groupDescriptor(id),null,null,null,null,null,false));}dataGroups=snapshotDataGroups();}
    Entry require(UUID group){var entry=groups.get(group);if(entry==null)throw new IllegalArgumentException("Unregistered runtime group");return entry;}
    List<Entry> entries(){return groups.values().stream().sorted(Comparator.<Entry>comparingLong(e->e.seed()==null ? -1 : e.seed().definition().sourceDirectory().generation()).thenComparing(e->e.group().getGroupId().toString())).toList();}
    List<RaftGroup> raftGroups(){return entries().stream().map(Entry::group).toList();}
    Set<UUID> dataGroups(){return dataGroups;}
    private Set<UUID> snapshotDataGroups(){var ids=new HashSet<UUID>();for(var entry:groups.values())if(entry.role()==RatisNodePlan.Role.DATA)ids.add(entry.group().getGroupId().getUuid());return Set.copyOf(ids);}
    synchronized void requireCapacity(Collection<UUID> requested){
        var added=new HashSet<>(requested);added.removeAll(groups.keySet());
        if(added.size()>plan.maxLocalGroups()-groups.size())throw new IllegalArgumentException("Node local group capacity exhausted");
    }
    synchronized void register(NativeTopologySeed seed){
        var local=seed.localReplica();var source=seed.definition().sourceDirectory();var bootstrap=plan.directory(source.domainId());
        if(!local.nodeId().equals(plan.nodeId()) || !local.nodeIncarnation().equals(plan.nodeIncarnation()) || !local.peerId().equals(plan.peerId()) || !local.address().equals(plan.address())
                || !source.clusterId().equals(bootstrap.clusterId()) || !source.databaseId().equals(bootstrap.databaseId()) || !source.domainId().equals(bootstrap.domainId()))throw new IllegalArgumentException("Native topology seed local identity/namespace differs from bootstrap");
        for(var parent:seed.definition().parents()){var old=require(parent.groupId());if(old.role()!=RatisNodePlan.Role.DATA || !old.descriptor().equals(parent))throw new IllegalArgumentException("Topology seed parent differs from registered native placement");}
        for(var target:seed.definition().targets()){var old=groups.get(target.groupId());if(old!=null && !(old.metadata()!=null && old.seed()==null && sameRangeIdentity(old.descriptor(),target)) && (old.seed()==null || !old.seed().equals(seed) || !old.descriptor().equals(target)))throw new IllegalArgumentException("Topology child identity already registered differently");}
        requireCapacity(seed.definition().targets().stream().map(RangeDescriptor::groupId).toList());
        for(var parent:seed.definition().parents()){retirements.put(parent.groupId(),seed);var old=require(parent.groupId());groups.put(parent.groupId(),new Entry(old.group(),old.role(),source,parent,old.seed(),old.learner(),old.relocation(),old.current(),old.metadata(),true));}
        for(var target:seed.definition().targets()){var old=groups.get(target.groupId());if(old!=null && old.metadata()!=null)groups.put(target.groupId(),new Entry(old.group(),old.role(),old.directory(),old.descriptor(),seed,old.learner(),old.relocation(),old.current(),old.metadata(),true));else groups.putIfAbsent(target.groupId(),new Entry(group(target),RatisNodePlan.Role.DATA,seed.definition().targetDirectory(),target,seed,null,null,null,null,false));}
        dataGroups=snapshotDataGroups();
    }
    synchronized void register(NativeTopologyLearnerRegistration learner){
        var local=learner.receiver();var source=learner.definition().sourceDirectory();var bootstrap=plan.directory(source.domainId());
        if(!local.nodeId().equals(plan.nodeId()) || !local.nodeIncarnation().equals(plan.nodeIncarnation()) || !local.peerId().equals(plan.peerId()) || !local.address().equals(plan.address())
                || !learner.catalogue().equals(plan.catalogue().groupId()) || !source.clusterId().equals(bootstrap.clusterId()) || !source.databaseId().equals(bootstrap.databaseId()) || !source.domainId().equals(bootstrap.domainId()))throw new IllegalArgumentException("Learner local identity/namespace differs from bootstrap");
        var target=learner.definition().targets().getFirst();var old=groups.get(target.groupId());
        if(old!=null && (old.learner()==null || !old.learner().equals(learner)))throw new IllegalArgumentException("Learner cannot adopt an existing local group");
        requireCapacity(List.of(target.groupId()));
        groups.putIfAbsent(target.groupId(),new Entry(group(target),RatisNodePlan.Role.DATA,learner.definition().targetDirectory(),target,null,learner,null,null,null,false));dataGroups=snapshotDataGroups();
    }
    private static boolean sameRangeIdentity(RangeDescriptor a,RangeDescriptor b){return a.groupId().equals(b.groupId()) && a.rangeId().equals(b.rangeId()) && a.rangeEpoch()==b.rangeEpoch() && Arrays.equals(a.lowerInclusive(),b.lowerInclusive()) && Arrays.equals(a.upperExclusive(),b.upperExclusive());}
    private void recoveryIdentity(UUID catalogue,RangeDirectory directory,ReplicaDescriptor receiver,RangeDescriptor descriptor,byte[] configuration){
        var bootstrap=plan.directory(directory.domainId());
        if(!catalogue.equals(plan.catalogue().groupId()) || !directory.clusterId().equals(plan.clusterId()) || !directory.databaseId().equals(plan.databaseId())
                || !directory.domainId().equals(bootstrap.domainId()) || !receiver.nodeId().equals(plan.nodeId()) || !receiver.nodeIncarnation().equals(plan.nodeIncarnation())
                || !receiver.peerId().equals(plan.peerId()) || !receiver.address().equals(plan.address()) || !descriptor.replicas().contains(receiver))throw new IllegalArgumentException("Native recovery identity/namespace differs from configured node");
        try{
            var cfg=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(configuration);
            if(!cfg.getOldPeersList().isEmpty() || !cfg.getOldListenersList().isEmpty())throw new IllegalArgumentException("Native recovery requires exact stable published membership");
            for(var role:List.of(ReplicaDescriptor.Role.VOTER,ReplicaDescriptor.Role.LISTENER)){
                var expected=new TreeMap<String,String>();var actual=new TreeMap<String,String>();for(var replica:descriptor.replicas())if(replica.role()==role)expected.put(replica.peerId(),replica.address());
                for(var peer:role==ReplicaDescriptor.Role.VOTER?cfg.getPeersList():cfg.getListenersList())if(actual.put(peer.getId().toStringUtf8(),peer.getAddress())!=null)throw new IllegalArgumentException("Duplicate native recovery peer");
                if(!actual.equals(expected))throw new IllegalArgumentException("Native recovery configuration differs from published membership");
            }
        }catch(java.io.IOException invalid){throw new IllegalArgumentException("Invalid native recovery configuration",invalid);}
    }
    synchronized void preflight(NativeCurrentRangeRecovery registration){
        var directory=registration.directory();var descriptor=registration.range();recoveryIdentity(registration.catalogue(),directory,registration.receiver(),descriptor,registration.sourceConfiguration());
        var old=groups.get(registration.groupId());
        if(old!=null){if(old.current()!=null && old.current().equals(registration))return;if(old.role()!=RatisNodePlan.Role.DATA || old.current()!=null || old.metadata()!=null || old.seed()!=null || old.learner()!=null || old.retired() || !old.descriptor().equals(descriptor))throw new IllegalArgumentException("CURRENT recovery conflicts with an existing local registration");}
        requireCapacity(List.of(registration.groupId()));
    }
    synchronized void register(NativeCurrentRangeRecovery registration){
        var existing=groups.get(registration.groupId());if(existing!=null && registration.equals(existing.current()))return;
        preflight(registration);var directory=registration.directory();var descriptor=registration.range();groups.put(registration.groupId(),new Entry(group(descriptor),RatisNodePlan.Role.DATA,directory,descriptor,null,null,null,registration,null,false));dataGroups=snapshotDataGroups();
    }
    synchronized void preflight(NativeMetadataRecoveryRegistration registration){
        var directory=registration.sourceDirectory();var descriptor=registration.range();recoveryIdentity(registration.catalogue(),directory,registration.receiver(),descriptor,registration.sourceConfiguration());
        var old=groups.get(registration.groupId());
        if(old!=null){
            if(old.metadata()!=null){if(!old.metadata().equals(registration))registration.requireSuccessorOf(old.metadata());return;}
            if(old.current()!=null)registration.requireTransitionOf(old.current());
            if(old.role()!=RatisNodePlan.Role.DATA || old.learner()!=null || old.descriptor().rangeEpoch()!=descriptor.rangeEpoch() || !old.descriptor().rangeId().equals(descriptor.rangeId())
                    || !Arrays.equals(old.descriptor().lowerInclusive(),descriptor.lowerInclusive()) || !Arrays.equals(old.descriptor().upperExclusive(),descriptor.upperExclusive()) || !old.directory().domainId().equals(directory.domainId()))throw new IllegalArgumentException("META recovery conflicts with an existing local registration");
        }
        requireCapacity(List.of(registration.groupId()));
    }
    synchronized void register(NativeMetadataRecoveryRegistration registration){
        var existing=groups.get(registration.groupId());if(existing!=null && registration.equals(existing.metadata()))return;
        preflight(registration);var directory=registration.sourceDirectory();var descriptor=registration.range();var old=groups.get(registration.groupId());groups.put(registration.groupId(),new Entry(group(descriptor),RatisNodePlan.Role.DATA,directory,descriptor,old==null?null:old.seed(),null,old==null?null:old.relocation(),null,registration,true));dataGroups=snapshotDataGroups();
    }
    synchronized void relocation(RatisTopologyRelocationRecord record){var definition=record.definition();var id=definition.parents().getFirst().groupId();var old=require(id);if(old.role()!=RatisNodePlan.Role.DATA || old.retired() || !definition.header().clusterId().equals(plan.clusterId()) || !definition.header().databaseId().equals(plan.databaseId()) || !definition.header().domainId().equals(old.directory().domainId()))throw new IllegalArgumentException("Relocation changed registered namespace/role");var directory=record.action()==RatisTopologyRelocationRecord.Action.COMPLETE?definition.targetDirectory():definition.sourceDirectory();if(old.directory().generation()>directory.generation())return;var descriptor=record.action()==RatisTopologyRelocationRecord.Action.COMPLETE?definition.targets().getFirst():definition.parents().getFirst();if(old.descriptor().rangeEpoch()!=descriptor.rangeEpoch() || !old.descriptor().rangeId().equals(descriptor.rangeId()))throw new IllegalArgumentException("Relocation changed registered lineage");groups.put(id,new Entry(group(descriptor),old.role(),directory,descriptor,old.seed(),old.learner(),record,old.current(),old.metadata(),old.retired()));}
    synchronized RaftGroup reconcileRelocationConfiguration(UUID id,byte[] nativeConfiguration)throws java.io.IOException{
        var old=require(id);if(old.relocation()==null)return old.group();var record=old.relocation();boolean joint=record.requireNativeConfiguration(nativeConfiguration);RaftGroup raft;
        if(joint){var peers=new TreeMap<String,RaftPeer>();for(var range:List.of(record.definition().parents().getFirst(),record.definition().targets().getFirst()))for(var peer:group(range).getPeers())peers.merge(peer.getId().toString(),peer,(before,after)->before.getStartupRole()==RaftPeerRole.FOLLOWER?before:after);raft=RaftGroup.valueOf(old.group().getGroupId(),peers.values());}
        else raft=group(RatisTopologyRelocationRecord.sameConfiguration(nativeConfiguration,record.targetConfiguration())?record.definition().targets().getFirst():record.definition().parents().getFirst());
        groups.put(id,new Entry(raft,old.role(),old.directory(),old.descriptor(),old.seed(),old.learner(),record,old.current(),old.metadata(),old.retired()));return raft;
    }
    synchronized void schemaActivated(SchemaChange.Definition definition,NativeSchemaActivationProof proof){
        var old=require(proof.installation().fence().participant().groupId());if(old.role()!=RatisNodePlan.Role.DATA || old.metadata()!=null)return;proof.requireHeader(definition.header());var directory=definition.targetDirectory();var descriptor=directory.ranges().stream().filter(r->r.groupId().equals(proof.installation().fence().participant().groupId())).findFirst().orElseThrow();groups.put(proof.installation().fence().participant().groupId(),new Entry(old.group(),old.role(),directory,descriptor,old.seed(),old.learner(),old.relocation(),old.current(),old.metadata(),old.retired()));
    }
    synchronized void topologyActivated(TopologyChange.Definition definition,NativeTopologyActivation activation){
        var old=require(activation.birth().groupId());if(old.seed()==null || old.role()!=RatisNodePlan.Role.DATA)throw new IllegalArgumentException("Topology activation group is not a registered native birth");activation.requireSeed(old.seed());definition.header().requireDefinition(old.seed().definition());var directory=definition.targetDirectory();if(old.directory().generation()>directory.generation())return;var descriptor=definition.targets().stream().filter(target->target.groupId().equals(activation.birth().groupId())).findFirst().orElseThrow();groups.put(descriptor.groupId(),new Entry(old.group(),old.role(),directory,descriptor,old.seed(),old.learner(),old.relocation(),old.current(),old.metadata(),old.retired()));
    }
    RaftGroup group(RangeDescriptor descriptor){return RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),descriptor.replicas().stream().map(replica->{var builder=RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER);var stream=plan.streamAddresses().get(replica.peerId());if(stream!=null)builder.setDataStreamAddress(stream);return builder.build();}).toList());}
}
