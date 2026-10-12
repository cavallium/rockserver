package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Fresh DATA witnesses qualify persisted delivery and exact terminal-to-child frontier replacement. */
final class RatisCdcDeliveryAuthority {
    @FunctionalInterface interface DefinitionReader { TopologyChange.Definition read(TopologyChange.Header header) throws IOException; }
    @FunctionalInterface interface DirectoryReader { RangeDirectory read(UUID domain) throws IOException; }
    @FunctionalInterface interface LineageReader { RangeDirectory read(DistributedCdcSubscription.Definition definition,DistributedCdcCheckpoint.Lineage lineage) throws IOException; }
    private final RatisRoutedClient client;
    private final DirectoryReader directories;
    private final LineageReader lineages;
    private final RatisTopologyCatalogue.OwnerProofReader topologyOwners;
    private final DefinitionReader definitions;
    RatisCdcDeliveryAuthority(RatisRoutedClient client,DirectoryReader directories) { this(client,directories,null,null,null); }
    RatisCdcDeliveryAuthority(RatisRoutedClient client,DirectoryReader directories,LineageReader lineages) { this(client,directories,lineages,null,null); }
    RatisCdcDeliveryAuthority(RatisRoutedClient client,DirectoryReader directories,LineageReader lineages,RatisTopologyCatalogue.OwnerProofReader topologyOwners) { this(client,directories,lineages,topologyOwners,null); }
    RatisCdcDeliveryAuthority(RatisRoutedClient client,DirectoryReader directories,LineageReader lineages,RatisTopologyCatalogue.OwnerProofReader topologyOwners,DefinitionReader definitions) { this.client=Objects.requireNonNull(client); this.directories=Objects.requireNonNull(directories); this.lineages=lineages; this.topologyOwners=topologyOwners; this.definitions=definitions; }
    void validate(DistributedCommand command) throws IOException {
        if(!RatisCdcSubscriptionCatalogue.requiresOwnerAdmission(command)) throw new IOException("Expected new CDC delivery/handoff bounds");
        var next=DistributedCdcSubscription.decode(command.mutations().getFirst().value()); var old=DistributedCdcSubscription.decode(command.predicates().getFirst().expectedValue()); next.requireSuccessorOf(old);
        var definition=next.definition();
        if(!next.currentStarts().equals(old.currentStarts())) {
            if(command.predicates().size()!=3) throw new IOException("CDC handoff lacks committed topology witnesses");
            var state=TopologyChange.State.decode(command.predicates().get(2).expectedValue()); var handoff=next.latestHandoff();
            if(definitions==null) throw new IOException("CDC handoff immutable definition admission is not configured"); var topology=definitions.read(handoff.header()); if(topology==null) throw new IOException("CDC handoff immutable definition absent"); handoff.header().requireDefinition(topology); handoff.requireAuthority(topology,state);
            if(!Arrays.equals(command.predicates().get(1).expectedValue(),topology.digest()) || !topology.sourceDirectory().columns().containsAll(definition.columns())) throw new IOException("CDC handoff immutable identity/selected geometry changed");
            if(topologyOwners==null) throw new IOException("CDC handoff native owner admission is not configured");
            qualify(topology,state,TopologyChange.Stage.FREEZE); qualify(topology,state,TopologyChange.Stage.ACTIVATE);
            return;
        }
        for(var entry:next.delivered().cursors().entrySet()) {
            if(entry.getValue().equals(old.delivered().cursors().get(entry.getKey()))) continue;
            var directory=lineages==null ? directories.read(definition.domainId()) : lineages.read(definition,entry.getKey());
            witness(directory,definition,entry.getKey(),entry.getValue(),old.delivered().cursors().get(entry.getKey()));
        }
    }
    private void qualify(TopologyChange.Definition definition,TopologyChange.State state,TopologyChange.Stage stage) throws IOException {
        var proofs=stage==TopologyChange.Stage.FREEZE ? state.frozen() : state.activated();
        for(var participant:stage==TopologyChange.Stage.FREEZE ? state.header().parents() : state.header().targets()) {
            int ready=0; IOException unavailable=null;
            for(var expected:proofs.values()) if(expected.groupId().equals(participant.groupId()) && expected.owner().role()==ReplicaDescriptor.Role.VOTER) {
                try {
                    var actual=topologyOwners.read(definition,expected);
                    if(actual==null || !Arrays.equals(actual.proof().encode(),expected.encode())) throw new IOException("CDC handoff native owner changed original artifact/cut");
                    var cut=actual.completedCut(); var original=expected.position(); if(cut.term()<1 || cut.index()<original.index() || cut.term()<original.term() || cut.index()==original.index() && cut.term()!=original.term()) throw new IOException("CDC handoff original cut lacks completed DATA prefix"); ready++;
                } catch(IOException failure) { unavailable=failure; }
            }
            if(ready<participant.quorum()) throw new IOException("CDC handoff lacks fresh original owner quorum",unavailable);
        }
    }
    private void witness(RangeDirectory directory,DistributedCdcSubscription.Definition definition,DistributedCdcCheckpoint.Lineage lineage,DistributedCdcCheckpoint.Cursor cursor,DistributedCdcCheckpoint.Cursor previous) throws IOException {
        if(directory==null || !directory.clusterId().equals(definition.clusterId()) || !directory.databaseId().equals(definition.databaseId()) || !directory.domainId().equals(definition.domainId()) || !directory.columns().containsAll(definition.columns()) || directory.ranges().stream().noneMatch(r->r.rangeId().equals(lineage.rangeId()) && r.groupId().equals(lineage.groupId()) && r.rangeEpoch()==lineage.rangeEpoch())) throw new IOException("CDC lineage directory is absent/foreign");
        var result=client.pollCdcDirectory(directory,lineage,cursor,1,RatisCdcPollRpc.REPLY_BASE_BYTES+1);
        if(result.outcome()==RatisCdcPollRpc.Outcome.GAP || !result.fromVerified()) throw new IOException("CDC cursor has no retained/current DATA witness");
        if(previous==null || !previous.covers(result.floor())) throw new IOException("CDC transition crosses an undelivered retention gap");
    }
}
