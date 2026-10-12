package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TopologyChangeTest {
    static UUID id(int n) { return new UUID(0,n); }
    ReplicaDescriptor owner(int n,ReplicaDescriptor.Role role) { return new ReplicaDescriptor(id(1000+n),id(2000+n),id(3000+n),"peer"+n,"127.0.0.1:"+(9000+n),role); }
    List<ReplicaDescriptor> owners() { return List.of(owner(1,ReplicaDescriptor.Role.VOTER),owner(2,ReplicaDescriptor.Role.VOTER),owner(3,ReplicaDescriptor.Role.VOTER),owner(4,ReplicaDescriptor.Role.LISTENER)); }
    byte[] bound(int n) { return new byte[]{(byte)n,0}; }
    RangeDescriptor range(int id,int group,long epoch,byte[] lower,byte[] upper,List<ReplicaDescriptor> replicas) { return new RangeDescriptor(id(id),id(group),epoch,3,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas); }
    RangeDirectory directory(long generation,List<RangeDescriptor> ranges) { return new RangeDirectory(id(1),id(2),id(3),4,new byte[32],generation,Set.of(id(9)),2,ranges); }
    TopologyChange.Definition split(List<ReplicaDescriptor> owners) {
        var left=range(20,21,2,null,bound(64),owners); var parent=range(40,41,7,bound(64),bound(192),owners); var right=range(60,61,2,bound(192),null,owners);
        var source=directory(8,List.of(left,parent,right)); var target=directory(9,List.of(left,range(100,110,1,bound(64),bound(128),owners),range(101,111,1,bound(128),bound(192),owners),right));
        return new TopologyChange.Definition(id(70),TopologyChange.Kind.SPLIT,source,target);
    }
    TopologyChange.Proof proof(TopologyChange.State state,TopologyChange.Stage stage,TopologyChange.Participant participant,int ownerIndex) {
        byte[] predecessor=switch(stage) { case FREEZE -> state.header().sourceDirectoryDigest(); case SEED -> state.freezeDigest(); case ACTIVATE -> state.seeded().get(new TopologyChange.Key(participant.groupId(),participant.owners().get(ownerIndex).replicaId())).digest(); };
        var position=switch(stage) { case FREEZE -> new ApplyPosition(7,500); case SEED -> new ApplyPosition(0,0); case ACTIVATE -> new ApplyPosition(1,1); };
        return TopologyChange.Proof.of(stage,state.header(),participant.groupId(),participant.owners().get(ownerIndex).replicaId(),position,new byte[32],predecessor);
    }
    TopologyChange.State freeze(TopologyChange.Definition definition,boolean all) {
        var state=TopologyChange.State.begin(definition.header()).beginFencing();
        for(var participant:state.header().parents()) for(int i=0;i<(all ? participant.owners().size() : participant.quorum());i++) state=state.frozen(proof(state,TopologyChange.Stage.FREEZE,participant,i));
        return state;
    }
    TopologyChange.State seed(TopologyChange.State state,boolean all) {
        for(var participant:state.header().targets()) for(int i=0;i<(all ? participant.owners().size() : participant.quorum());i++) state=state.seeded(proof(state,TopologyChange.Stage.SEED,participant,i)); return state;
    }
    TopologyChange.State activate(TopologyChange.State state,boolean all) {
        for(var participant:state.header().targets()) for(int i=0;i<(all ? participant.owners().size() : participant.quorum());i++) state=state.activated(proof(state,TopologyChange.Stage.ACTIVATE,participant,i)); return state;
    }
    @Test void exactNonemptyPrefixSplitAndAdjacentMergeRetainUnrelatedDescriptors() {
        var split=split(owners()); assertEquals(TopologyChange.Kind.SPLIT,split.kind()); assertEquals(1,split.parents().size()); assertEquals(2,split.targets().size());
        assertEquals(split.sourceDirectory().ranges().getFirst(),split.targetDirectory().ranges().getFirst()); assertEquals(split.sourceDirectory().ranges().getLast(),split.targetDirectory().ranges().getLast());
        assertArrayEquals(split.encode(),TopologyChange.Definition.decode(split.encode()).encode()); assertArrayEquals(split.header().encode(),TopologyChange.Header.decode(split.header().encode()).encode()); split.header().requireDefinition(split);
        var target=directory(10,List.of(split.targetDirectory().ranges().getFirst(),range(200,201,1,bound(64),bound(192),owners()),split.targetDirectory().ranges().getLast()));
        var merge=new TopologyChange.Definition(id(71),split.targetDirectory(),target); assertEquals(TopologyChange.Kind.MERGE,merge.kind()); assertEquals(2,merge.parents().size()); assertArrayEquals(bound(64),merge.targets().getFirst().lowerInclusive());
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),TopologyChange.Kind.SPLIT,split.targetDirectory(),target));
        byte[] digest=split.digest(); digest[0]=1; assertFalse(Arrays.equals(digest,split.digest())); assertThrows(UnsupportedOperationException.class,()->split.header().parents().clear());
    }
    @Test void repartitionStaleSchemaProjectionIdentityAndMembershipForgeriesAreRejected() {
        var definition=split(owners()); var source=definition.sourceDirectory(); var target=definition.targetDirectory();
        var stale=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),5,source.schemaDigest(),9,source.columns(),2,target.ranges()); assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,stale));
        var wrongColumns=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),4,source.schemaDigest(),9,Set.of(id(99)),2,target.ranges()); assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,wrongColumns));
        var wrongProjection=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),4,source.schemaDigest(),9,source.columns(),0,target.ranges()); assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,wrongProjection));
        assertThrows(IllegalArgumentException.class,()->directory(9,List.of(range(100,110,1,null,new byte[]{1},owners()),range(101,111,1,new byte[]{1},null,owners()))));
        var unrelated=range(20,21,2,null,bound(32),owners()); var moved=directory(9,List.of(unrelated,range(100,110,1,bound(32),bound(128),owners()),range(101,111,1,bound(128),bound(192),owners()),target.ranges().getLast()));
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,moved));
        var changedOwners=new ArrayList<>(owners()); changedOwners.set(3,owner(5,ReplicaDescriptor.Role.LISTENER)); var changed=directory(9,List.of(target.ranges().getFirst(),range(100,110,1,bound(64),bound(128),changedOwners),target.ranges().get(2),target.ranges().getLast()));
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,changed));
        var reused=directory(9,List.of(target.ranges().getFirst(),range(40,41,8,bound(64),bound(128),owners()),target.ranges().get(2),target.ranges().getLast())); assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,reused));
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(71),source,directory(9,source.ranges())));
    }
    @Test void quorumReadinessDoesNotWaitForOfflineVoterOrListenerAndNeverCopiesParentCutToChild() {
        var definition=split(owners()); var initial=TopologyChange.State.begin(definition.header()); var fencing=initial.beginFencing(); fencing.requireSuccessorOf(initial); var parent=fencing.header().parents().getFirst();
        var listener=fencing.frozen(proof(fencing,TopologyChange.Stage.FREEZE,parent,3)); assertEquals(TopologyChange.Phase.FENCING,listener.phase());
        var one=listener.frozen(proof(listener,TopologyChange.Stage.FREEZE,parent,0)); assertEquals(TopologyChange.Phase.FENCING,one.phase());
        var fenced=one.frozen(proof(one,TopologyChange.Stage.FREEZE,parent,1)); fenced.requireSuccessorOf(one); assertEquals(TopologyChange.Phase.FENCED,fenced.phase()); assertEquals(3,fenced.frozen().size());
        var seeded=seed(fenced,false); assertEquals(TopologyChange.Phase.SEEDED,seeded.phase()); assertTrue(seeded.seeded().values().stream().allMatch(p->p.position().equals(new ApplyPosition(0,0))));
        var published=seeded.publish(); published.requireSuccessorOf(seeded); var active=activate(published,false); assertEquals(TopologyChange.Phase.ACTIVE,active.phase());
        var retired=active.retire(); retired.requireSuccessorOf(active); assertTrue(retired.terminal()); assertArrayEquals(retired.encode(),TopologyChange.State.decode(retired.encode()).encode());
        assertEquals(new ApplyPosition(7,500),retired.frozen().values().iterator().next().position()); assertEquals(new ApplyPosition(1,1),retired.activated().values().iterator().next().position());
    }
    @Test void proofBindingDependenciesLocalSeedAndForwardOnlyPhasesAreStrict() {
        var definition=split(owners()); var initial=TopologyChange.State.begin(definition.header()); var abort=initial.abort(); assertTrue(abort.terminal()); assertSame(abort,abort.abort()); abort.requireSuccessorOf(initial);
        var fencing=initial.beginFencing(); var parent=definition.header().parents().getFirst(); var freeze=proof(fencing,TopologyChange.Stage.FREEZE,parent,0);
        assertThrows(IllegalStateException.class,()->initial.frozen(freeze)); assertThrows(IllegalStateException.class,fencing::abort); assertThrows(IllegalStateException.class,fencing::publish);
        byte[] wrong=new byte[32]; wrong[0]=1; var forged=new TopologyChange.Proof(freeze.stage(),wrong,freeze.rangeId(),freeze.groupId(),freeze.rangeEpoch(),freeze.configurationVersion(),freeze.owner(),freeze.position(),freeze.artifactDigest(),freeze.predecessorDigest());
        assertThrows(IllegalArgumentException.class,()->fencing.frozen(forged));
        var wrongOwner=new TopologyChange.Owner(freeze.owner().replicaId(),ReplicaDescriptor.Role.VOTER,wrong); var stale=new TopologyChange.Proof(freeze.stage(),freeze.definitionDigest(),freeze.rangeId(),freeze.groupId(),freeze.rangeEpoch(),freeze.configurationVersion(),wrongOwner,freeze.position(),freeze.artifactDigest(),freeze.predecessorDigest()); assertThrows(IllegalArgumentException.class,()->fencing.frozen(stale));
        var first=fencing.frozen(freeze); assertSame(first,first.frozen(freeze)); var changedCut=TopologyChange.Proof.of(TopologyChange.Stage.FREEZE,first.header(),parent.groupId(),parent.owners().get(1).replicaId(),new ApplyPosition(7,501),new byte[32],first.header().sourceDirectoryDigest()); assertThrows(IllegalArgumentException.class,()->first.frozen(changedCut));
        var fenced=freeze(definition,false); var child=fenced.header().targets().getFirst(); var badSeed=TopologyChange.Proof.of(TopologyChange.Stage.SEED,fenced.header(),child.groupId(),child.owners().getFirst().replicaId(),new ApplyPosition(0,0),new byte[32],wrong); assertThrows(IllegalArgumentException.class,()->fenced.seeded(badSeed));
        var seeded=seed(fenced,false); assertThrows(IllegalStateException.class,seeded::retire); var published=seeded.publish();
        var missingOwner=child.owners().get(2); var unseededActivation=TopologyChange.Proof.of(TopologyChange.Stage.ACTIVATE,published.header(),child.groupId(),missingOwner.replicaId(),new ApplyPosition(1,1),new byte[32],wrong); assertThrows(IllegalArgumentException.class,()->published.activated(unseededActivation));
        assertThrows(IllegalArgumentException.class,()->seeded.requireSuccessorOf(fenced));
        byte[] proofBytes=freeze.encode(); assertArrayEquals(proofBytes,TopologyChange.Proof.decode(proofBytes).encode()); proofBytes[0]=0; assertThrows(IllegalArgumentException.class,()->TopologyChange.Proof.decode(proofBytes));
    }
    @Test void fullSixteenOwnerReferencesStayBoundedAndCanonicalWithOptionalProofs() {
        var replicas=new ArrayList<ReplicaDescriptor>(); for(int i=1;i<=16;i++) replicas.add(owner(i,i<=5 ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER));
        var definition=split(replicas); var frozen=freeze(definition,true); var seeded=seed(frozen,true); var active=activate(seeded.publish(),true); assertEquals(16,active.frozen().size()); assertEquals(32,active.seeded().size()); assertEquals(32,active.activated().size());
        assertTrue(definition.header().encode().length<=TopologyChange.MAX_HEADER_BYTES); assertTrue(active.encode().length<=TopologyChange.MAX_STATUS_BYTES); assertTrue(definition.encode().length<=TopologyChange.MAX_DEFINITION_BYTES);
        assertArrayEquals(active.encode(),TopologyChange.State.decode(active.encode()).encode()); var bytes=active.encode(); int generation=8+active.header().encode().length; ByteBuffer.wrap(bytes).putLong(generation,active.generation()+1); assertThrows(IllegalArgumentException.class,()->TopologyChange.State.decode(bytes));
        var header=definition.header().encode(); header[125]^=1; var changed=TopologyChange.Header.decode(header); assertThrows(IllegalArgumentException.class,()->changed.requireDefinition(definition));
        var truncated=Arrays.copyOf(active.encode(),active.encode().length-1); assertThrows(IllegalArgumentException.class,()->TopologyChange.State.decode(truncated));
        var badLength=definition.encode(); ByteBuffer.wrap(badLength).putInt(21,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->TopologyChange.Definition.decode(badLength));
    }
    @Test void relocationIntentSupportsOnlyOneStagedMembershipStepAndNeverPretendsToFreezeWrites() {
        var base=split(owners()).sourceDirectory(); var original=base.ranges().get(1); var replicas=new ArrayList<>(owners()); replicas.add(owner(5,ReplicaDescriptor.Role.LISTENER));
        var changed=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),4,original.lowerInclusive(),original.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,replicas);
        var target=directory(9,List.of(base.ranges().getFirst(),changed,base.ranges().getLast())); var definition=new TopologyChange.Definition(id(71),TopologyChange.Kind.RELOCATE,base,target); assertEquals(TopologyChange.Kind.RELOCATE,definition.kind());
        var prepared=TopologyChange.State.begin(definition.header()); assertThrows(UnsupportedOperationException.class,prepared::beginFencing); assertTrue(prepared.abort().terminal()); assertEquals(RangeDescriptor.TopologyState.ACTIVE,definition.sourceDirectory().ranges().get(1).topologyState());
        replicas.set(4,owner(5,ReplicaDescriptor.Role.VOTER)); var voter=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),4,original.lowerInclusive(),original.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,replicas);
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(72),base,directory(9,List.of(base.ranges().getFirst(),voter,base.ranges().getLast()))));
    }
    @Test void stagedListenerCanReplaceOneOfFiveVotersInOneBoundedConfigurationStep() {
        var peers=new ArrayList<ReplicaDescriptor>();for(int n=1;n<=5;n++)peers.add(owner(n,ReplicaDescriptor.Role.VOTER));peers.add(owner(6,ReplicaDescriptor.Role.LISTENER));
        var original=range(80,81,1,null,null,peers);var source=directory(8,List.of(original));
        var replacements=new ArrayList<ReplicaDescriptor>();for(int n=2;n<=6;n++)replacements.add(owner(n,ReplicaDescriptor.Role.VOTER));
        var targetRange=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),4,null,null,RangeDescriptor.TopologyState.ACTIVE,replacements);
        var definition=new TopologyChange.Definition(id(82),TopologyChange.Kind.RELOCATE,source,directory(9,List.of(targetRange)));
        assertEquals(5,definition.targetDirectory().ranges().getFirst().replicas().stream().filter(r->r.role()==ReplicaDescriptor.Role.VOTER).count());
        assertArrayEquals(definition.encode(),TopologyChange.Definition.decode(definition.encode()).encode());
        var unprepared=new ArrayList<>(replacements);unprepared.set(4,owner(7,ReplicaDescriptor.Role.VOTER));
        var unpreparedRange=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),4,null,null,RangeDescriptor.TopologyState.ACTIVE,unprepared);
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(83),source,directory(9,List.of(unpreparedRange))));
        var multipleRemovals=new ArrayList<>(replacements);multipleRemovals.removeFirst();
        var smaller=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),4,null,null,RangeDescriptor.TopologyState.ACTIVE,multipleRemovals);
        assertThrows(IllegalArgumentException.class,()->new TopologyChange.Definition(id(84),source,directory(9,List.of(smaller))));
    }
}
