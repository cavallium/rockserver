package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.util.Objects;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.util.LifeCycle;

/** Exact local inert disposition. It never changes native state or the sticky recovery gate. */
final class RatisLocalNoneDiscovery {
    private final RatisConfiguredRecoveryGraph.Snapshot graph;
    private final RatisRuntimeGroups.Entry entry;
    private final ReplicatedRangeStore store;
    private final RatisRangeStateMachine machine;
    private final RaftServer.Division division;
    private final RatisNodeReadiness.MachineStamp stamp;
    private final long revision;
    private final ApplyPosition applied;
    private final RangeOwnership.Ownership ownership;
    private final NativeCurrentRangeRecovery current;
    private final NativeMetadataRecoveryRegistration metadata;
    private final NativeTopologyLearnerRegistration learner;
    private final NativeTopologyBirth birth;
    private final NativeTopologyActivation activation;

    RatisLocalNoneDiscovery(RatisConfiguredRecoveryGraph.Snapshot graph,RatisRuntimeGroups.Entry entry,
                           ReplicatedRangeStore store,RatisRangeStateMachine machine,RaftServer.Division division,java.util.UUID catalogue)throws IOException {
        this.graph=Objects.requireNonNull(graph);this.entry=Objects.requireNonNull(entry);this.store=Objects.requireNonNull(store);this.machine=Objects.requireNonNull(machine);this.division=Objects.requireNonNull(division);
        var group=entry.group().getGroupId().getUuid();
        if(entry.role()!=RatisNodePlan.Role.DATA || graph.plan().localObligations().stream().anyMatch(obligation->obligation.range().groupId().equals(group)) || graph.activeRequests().containsKey(group))throw new IOException("NONE requires an absent local obligation in the complete forced graph");
        stamp=machine.readinessStamp();revision=machine.recoveryEligibilityGate().revision();applied=store.applied();ownership=store.ownership();
        current=store.currentRangeRecoveryRegistration();metadata=store.metadataRecoveryRegistration();learner=store.topologyLearnerRegistration();birth=store.topologyBirth();activation=store.topologyActivation();
        if(!ownership.owner().groupId().equals(group) || entry.descriptor()==null || ownership.owner().epoch()!=entry.descriptor().rangeEpoch() || !ownership.interval().domainId().equals(entry.directory().domainId()))throw new IOException("NONE native owner differs from the registered DATA group");
        var cut=graph.plan().catalogueCut();
        if(current!=null){requireCut(cut,current.catalogueCut(),catalogue,current.catalogue());if(current.origin()!=null)requireCut(cut,current.origin().activation().catalogueCut(),catalogue,current.origin().activation().catalogueGroup());}
        if(metadata!=null)requireCut(cut,metadata.catalogueCut(),catalogue,metadata.catalogue());
        if(learner!=null)requireCut(cut,learner.catalogueCut(),catalogue,learner.catalogue());
        if(activation!=null)requireCut(cut,activation.catalogueCut(),catalogue,activation.catalogueGroup());
        if(entry.relocation()!=null)requireCut(cut,entry.relocation().catalogueCut(),catalogue,entry.relocation().catalogue());
        if(!current(graph,entry,store,machine,division,stamp))throw new IOException("NONE lost its fenced local owner during qualification");
    }
    private static void requireCut(ApplyPosition graph,ApplyPosition nativeCut,java.util.UUID catalogue,java.util.UUID actual)throws IOException {
        if(!catalogue.equals(actual) || !RatisRelocationOwnerRpc.covers(graph,nativeCut))throw new IOException("NONE graph does not cover retained native catalogue authority");
    }
    boolean current(RatisConfiguredRecoveryGraph.Snapshot selected,RatisRuntimeGroups.Entry registered,ReplicatedRangeStore nativeStore,
                    RatisRangeStateMachine owner,RaftServer.Division actual,RatisNodeReadiness.MachineStamp observed) {
        return graph==selected && entry==registered && store==nativeStore && machine==owner && division==actual
                && stamp.equals(observed) && observed.discovery()==RatisNodeReadiness.Discovery.PENDING
                && revision==machine.recoveryEligibilityGate().revision() && machine.recoveryEligibilityGate().discoveryPending()
                && machine.localDiscoveryIdle() && machine.getLifeCycleState()==LifeCycle.State.RUNNING
                && division.getInfo().isAlive() && division.getInfo().getLifeCycleState()==LifeCycle.State.RUNNING
                && division.getStateMachine()==machine && division.getGroup().getGroupId().equals(entry.group().getGroupId())
                && !store.admissionOpen() && Objects.equals(applied,store.applied()) && ownership.equals(store.ownership())
                && current==store.currentRangeRecoveryRegistration() && metadata==store.metadataRecoveryRegistration()
                && learner==store.topologyLearnerRegistration() && birth==store.topologyBirth() && activation==store.topologyActivation();
    }
}
