package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Recipe/pointer codec and structural authority tests, not native pool seeding or activation acceptance. */
class NativeTopologyBirthTest {
    static UUID id(int n) { return new UUID(115,n); }
    ReplicaDescriptor owner(int n,ReplicaDescriptor.Role role) { return new ReplicaDescriptor(id(100+n),id(200+n),id(300+n),"peer"+n,"127.0.0.1:"+(9000+n),role); }
    List<ReplicaDescriptor> owners() { return List.of(owner(1,ReplicaDescriptor.Role.VOTER),owner(2,ReplicaDescriptor.Role.VOTER),owner(3,ReplicaDescriptor.Role.VOTER),owner(4,ReplicaDescriptor.Role.LISTENER)); }
    RangeDescriptor range(int range,int group,byte[] lower,byte[] upper) { return new RangeDescriptor(id(range),id(group),3,4,lower,upper,RangeDescriptor.TopologyState.ACTIVE,owners()); }
    RangeDirectory directory(long generation,Set<UUID> columns,List<RangeDescriptor> ranges) { return new RangeDirectory(id(1),id(2),id(3),4,new byte[32],generation,columns,0,ranges); }
    final class Fixture {
        final TopologyChange.Definition definition;
        final TopologyChange.State authority;
        final Map<UUID,NativeTopologyFreeze> freezes=new HashMap<>();
        final Map<UUID,String> names;
        Fixture(boolean merge,Set<UUID> columns,Map<UUID,String> names) {
            var source=directory(7,columns,merge ? List.of(range(12,13,null,new byte[]{(byte)128}),range(14,15,new byte[]{(byte)128},null)) : List.of(range(12,13,null,null)));
            var target=directory(8,columns,merge ? List.of(range(20,21,null,null)) : List.of(range(20,21,null,new byte[]{(byte)128}),range(22,23,new byte[]{(byte)128},null)));
            definition=new TopologyChange.Definition(id(11),source,target); var state=TopologyChange.State.begin(definition.header()).beginFencing(); this.names=names;
            for(var parent:definition.parents()) {
                var freeze=new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(7,parent.groupId().equals(id(13)) ? 100 : 200),NativeTopologyFreeze.boundsDigest(parent.lowerInclusive(),parent.upperExclusive()),new byte[32]); freezes.put(parent.groupId(),freeze);
                for(var replica:parent.replicas().stream().filter(r->r.role()==ReplicaDescriptor.Role.VOTER).limit(2).toList()) state=state.frozen(freeze.proof(replica.replicaId()));
            }
            authority=state;
        }
        NativeTopologySeed seed(ReplicaDescriptor local) { return new NativeTopologySeed(definition,authority,local,freezes,names,Set.of()); }
    }
    Fixture fixture(boolean merge) { return new Fixture(merge,Set.of(id(9),id(10)),Map.of(id(9),"first",id(10),"second")); }
    @Test void splitAndMergeRecipesRoundTripAndBirthIsOnlyCompactLocalGenesisPointer() {
        for(boolean merge:List.of(false,true)) {
            var f=fixture(merge); var seed=f.seed(owners().getFirst()); assertEquals(TopologyChange.Phase.FENCED,seed.authority().phase()); assertEquals(seed,NativeTopologySeed.decode(seed.encode())); assertEquals(seed.hashCode(),NativeTopologySeed.decode(seed.encode()).hashCode());
            for(var target:f.definition.targets()) {
                var birth=seed.birth(target.groupId()); assertEquals(68,birth.encode().length); assertEquals(birth,NativeTopologyBirth.decode(birth.encode())); birth.requireSeed(seed); assertEquals(target,birth.resolveDescriptor(seed));
                var proof=birth.proof(seed); assertEquals(TopologyChange.Stage.SEED,proof.stage()); assertEquals(new ApplyPosition(0,0),proof.position()); assertArrayEquals(birth.digest(),proof.artifactDigest()); assertArrayEquals(f.authority.freezeDigest(),proof.predecessorDigest()); assertEquals(TopologyChange.Owner.of(seed.localReplica()),proof.owner()); assertEquals(proof,seed.proof(target.groupId()));
                assertNotEquals(f.freezes.values().iterator().next().position(),proof.position());
            }
            assertThrows(IllegalArgumentException.class,()->seed.birth(f.definition.parents().getFirst().groupId()));
        }
    }
    @Test void exactOfflineVoterOrListenerMayCatchUpWithoutItsOwnQuorumReference() {
        var f=fixture(true);
        for(var local:List.of(owners().get(2),owners().get(3))) {
            assertFalse(f.authority.frozen().containsKey(new TopologyChange.Key(f.definition.parents().getFirst().groupId(),local.replicaId())));
            var seed=f.seed(local); assertEquals(local,NativeTopologySeed.decode(seed.encode()).localReplica()); assertEquals(local.replicaId(),seed.proof(f.definition.targets().getFirst().groupId()).owner().replicaId());
        }
        var local=owners().get(2); var changed=new ReplicaDescriptor(local.nodeId(),id(999),local.replicaId(),local.peerId(),local.address(),local.role()); assertThrows(IllegalArgumentException.class,()->f.seed(changed));
    }
    @Test void authorityCoverageParentCutAdmissionSchemaAndBoundsMustMatchOriginalArtifacts() {
        var f=fixture(true); var local=owners().getFirst();
        assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,TopologyChange.State.begin(f.definition.header()),local,f.freezes,f.names,Set.of()));
        assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,local,Map.of(),f.names,Set.of()));
        var parent=f.definition.parents().getFirst(); var original=f.freezes.get(parent.groupId()); var changed=new HashMap<>(f.freezes);
        changed.put(parent.groupId(),new NativeTopologyFreeze(f.definition.header(),parent.groupId(),new ApplyPosition(7,101),original.boundsDigest(),original.admissionDigest())); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,local,changed,f.names,Set.of()));
        byte[] digest=new byte[32]; digest[0]=1; var wrongAdmission=new HashMap<>(f.freezes); wrongAdmission.put(parent.groupId(),new NativeTopologyFreeze(f.definition.header(),parent.groupId(),original.position(),original.boundsDigest(),digest)); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,local,wrongAdmission,f.names,Set.of()));
        var wrongBounds=new HashMap<>(f.freezes); wrongBounds.put(parent.groupId(),new NativeTopologyFreeze(f.definition.header(),parent.groupId(),original.position(),digest,original.admissionDigest())); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,local,wrongBounds,f.names,Set.of()));
        var crossed=new HashMap<>(f.freezes); crossed.put(parent.groupId(),f.freezes.get(f.definition.parents().getLast().groupId())); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,local,crossed,f.names,Set.of()));
        var foreign=fixture(false); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,foreign.authority,local,f.freezes,f.names,Set.of()));
    }
    @Test void aliasesMergeFlagsAndSeedCollectionsAreExactBoundedAndImmutable() {
        var f=fixture(false); var names=new HashMap<>(f.names); var freezes=new HashMap<>(f.freezes); var merges=new HashSet<>(Set.of(id(10))); var seed=new NativeTopologySeed(f.definition,f.authority,owners().getFirst(),freezes,names,merges); names.clear(); freezes.clear(); merges.clear();
        assertEquals(2,seed.physicalNames().size()); assertEquals(1,seed.parentFreezes().size()); assertEquals(Set.of(id(10)),seed.mergeColumns());
        assertThrows(UnsupportedOperationException.class,()->seed.physicalNames().clear()); assertThrows(UnsupportedOperationException.class,()->seed.parentFreezes().clear()); assertThrows(UnsupportedOperationException.class,()->seed.mergeColumns().clear());
        byte[] bytes=seed.encode(); bytes[0]=0; assertEquals(seed,NativeTopologySeed.decode(seed.encode())); byte[] digest=seed.digest(); digest[0]^=1; assertFalse(Arrays.equals(digest,seed.digest()));
        assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,owners().getFirst(),f.freezes,Map.of(id(9),"same",id(10),"same"),Set.of()));
        for(var bad:List.of("","non"+(char)0+"nul","caf\u00e9","x".repeat(4097))) assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,owners().getFirst(),f.freezes,Map.of(id(9),bad,id(10),"second"),Set.of()));
        assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,owners().getFirst(),f.freezes,Map.of(id(9),"first"),Set.of())); assertThrows(IllegalArgumentException.class,()->new NativeTopologySeed(f.definition,f.authority,owners().getFirst(),f.freezes,f.names,Set.of(id(99))));
    }
    @Test void laterAuthorityIsStructurallyAllowedButCannotReplaceOriginalBirthSeedCommitment() {
        var f=fixture(false); var original=f.seed(owners().getFirst()); var state=f.authority;
        for(var target:f.definition.targets()) for(var local:owners().subList(0,2)) state=state.seeded(f.seed(local).proof(target.groupId()));
        assertEquals(TopologyChange.Phase.SEEDED,state.phase()); var later=new NativeTopologySeed(f.definition,state,owners().getFirst(),f.freezes,f.names,Set.of()); assertArrayEquals(original.authority().freezeDigest(),later.authority().freezeDigest()); assertFalse(Arrays.equals(original.digest(),later.digest()));
        var birth=original.birth(f.definition.targets().getFirst().groupId()); assertThrows(IllegalArgumentException.class,()->birth.requireSeed(later)); birth.requireSeed(original);
        byte[] wrong=new byte[32]; wrong[0]=1; assertThrows(IllegalArgumentException.class,()->new NativeTopologyBirth(f.definition.intentId(),birth.groupId(),wrong).requireSeed(original)); assertThrows(IllegalArgumentException.class,()->new NativeTopologyBirth(id(99),birth.groupId(),original.digest()).requireSeed(original));
    }
    @Test void maximumAliasMapAndMalformedCanonicalEncodingsRemainBounded() {
        var names=new HashMap<UUID,String>(); for(int i=0;i<RangeDirectoryCodec.MAX_COLUMNS;i++) names.put(id(1000+i),"n".repeat(4092)+String.format("%04d",i)); var f=new Fixture(false,names.keySet(),names); var seed=f.seed(owners().getFirst());
        assertTrue(seed.encode().length>1024*1024); assertTrue(seed.encode().length<=NativeTopologySeed.MAX_BYTES); assertEquals(seed,NativeTopologySeed.decode(seed.encode()));
        byte[] badLength=seed.encode(); ByteBuffer.wrap(badLength).putInt(4,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->NativeTopologySeed.decode(badLength));
        byte[] flag=seed.encode(); flag[flag.length-1]=2; assertThrows(IllegalArgumentException.class,()->NativeTopologySeed.decode(flag)); assertThrows(IllegalArgumentException.class,()->NativeTopologySeed.decode(Arrays.copyOf(seed.encode(),seed.encode().length-1))); assertThrows(IllegalArgumentException.class,()->NativeTopologySeed.decode(new byte[NativeTopologySeed.MAX_BYTES+1]));
        var birth=seed.birth(f.definition.targets().getFirst().groupId()); assertThrows(IllegalArgumentException.class,()->NativeTopologyBirth.decode(Arrays.copyOf(birth.encode(),67))); byte[] bad=birth.encode(); bad[0]=0; assertThrows(IllegalArgumentException.class,()->NativeTopologyBirth.decode(bad));
    }
}
