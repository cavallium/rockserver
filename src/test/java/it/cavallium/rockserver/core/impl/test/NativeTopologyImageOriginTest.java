package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Canonical provenance and receiver derivation only; no native image/import acceptance claim. */
class NativeTopologyImageOriginTest {
    static UUID id(long n) { return new UUID(0,n); }
    final ReplicaDescriptor source=new ReplicaDescriptor(id(1),id(2),id(3),"source","127.0.0.1:9998",ReplicaDescriptor.Role.VOTER);
    final ReplicaDescriptor receiver=new ReplicaDescriptor(id(4),id(5),id(6),"receiver","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
    RangeDescriptor range(long n,byte[] lower,byte[] upper) { return new RangeDescriptor(id(n),id(n+100),1,1,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(source,receiver)); }
    TopologyChange.Definition definition(long intent) {
        var parent=range(10,null,null); var left=range(11,null,new byte[]{(byte)128}); var right=range(12,new byte[]{(byte)128},null);
        return new TopologyChange.Definition(id(intent),new RangeDirectory(id(20),id(21),id(22),1,new byte[32],1,Set.of(),0,List.of(parent)),new RangeDirectory(id(20),id(21),id(22),1,new byte[32],2,Set.of(),0,List.of(left,right)));
    }
    NativeTopologySeed seed(TopologyChange.Definition definition,ReplicaDescriptor local) { return seed(definition,local,id(30)); }
    NativeTopologySeed seed(TopologyChange.Definition definition,ReplicaDescriptor local,UUID catalogue) {
        var parent=definition.parents().getFirst(); byte[] fencing=TopologyChange.State.begin(definition.header()).beginFencing().encode();
        byte[] admission=OperatorUpload.sha256(ByteBuffer.allocate(16+fencing.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(fencing).array());
        var freeze=new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(1,4),NativeTopologyFreeze.boundsDigest(parent.lowerInclusive(),parent.upperExclusive()),admission);
        var state=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(source.replicaId())).frozen(freeze.proof(receiver.replicaId()));
        return new NativeTopologySeed(definition,state,local,Map.of(parent.groupId(),freeze),Map.of(),Set.of());
    }
    NativeTopologyActivation activation(NativeTopologySeed seed) {
        var publication=NativeTopologyActivation.publicationCommitment(seed.definition(),seed.authority().freezeDigest(),id(30));
        return new NativeTopologyActivation(seed,seed.birth(seed.definition().targets().getFirst().groupId()),new ApplyPosition(2,9),id(30),new ApplyPosition(3,5),publication);
    }
    @Test void canonicalOriginOwnsBytesAndBindsCompleteSourceTargetAndPrefix() {
        var seed=seed(definition(40),source); var activation=activation(seed); var origin=new NativeTopologyImageOrigin(seed,activation);
        byte[] bytes=origin.encode(); assertTrue(bytes.length<=NativeTopologyImageOrigin.MAX_BYTES); assertEquals(origin,NativeTopologyImageOrigin.decode(bytes));
        bytes[0]^=1; assertNotEquals(bytes[0],origin.encode()[0]); var target=seed.definition().targets().getFirst();
        assertDoesNotThrow(()->origin.requireImage(target.groupId(),target.rangeEpoch(),seed.header().domainId(),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(3,10)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(id(99),target.rangeEpoch(),seed.header().domainId(),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(3,10)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(target.groupId(),2,seed.header().domainId(),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(3,10)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(target.groupId(),1,id(99),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(3,10)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(target.groupId(),1,seed.header().domainId(),new byte[]{1},target.upperExclusive(),new ApplyPosition(3,10)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(target.groupId(),1,seed.header().domainId(),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(1,9)));
        assertThrows(IllegalArgumentException.class,()->origin.requireImage(target.groupId(),1,seed.header().domainId(),target.lowerInclusive(),target.upperExclusive(),new ApplyPosition(2,8)));
    }
    @Test void receiverReceiptUsesLocalPointerButPreservesOriginalCommonDecisionAndPosition() {
        var definition=definition(40); var sourceSeed=seed(definition,source); var original=activation(sourceSeed); var origin=new NativeTopologyImageOrigin(sourceSeed,original); var local=seed(definition,receiver);
        var derived=origin.localActivation(local); derived.requireSeed(local);
        assertEquals(original.position(),derived.position()); assertEquals(original.catalogueGroup(),derived.catalogueGroup()); assertEquals(original.catalogueCut(),derived.catalogueCut()); assertArrayEquals(original.publicationCommitment(),derived.publicationCommitment());
        assertEquals(TopologyChange.Owner.of(receiver),derived.owner()); assertFalse(original.birth().equals(derived.birth())); assertFalse(Arrays.equals(original.digest(),derived.digest()));
        assertThrows(IllegalArgumentException.class,()->origin.localActivation(seed(definition(41),receiver)));
        assertThrows(IllegalArgumentException.class,()->new NativeTopologyImageOrigin(local,original));
    }
    @Test void sameDefinitionAndCutsCannotRebindForeignCatalogueParentAdmissions() {
        var definition=definition(40); var sourceSeed=seed(definition,source); var origin=new NativeTopologyImageOrigin(sourceSeed,activation(sourceSeed));
        var foreignLocal=seed(definition,receiver,id(31));
        assertArrayEquals(sourceSeed.definition().encode(),foreignLocal.definition().encode()); assertArrayEquals(sourceSeed.authority().freezeDigest(),foreignLocal.authority().freezeDigest());
        assertThrows(IllegalArgumentException.class,()->origin.localActivation(foreignLocal));
        var foreignSource=seed(definition,source,id(31));
        assertThrows(IllegalArgumentException.class,()->new NativeTopologyImageOrigin(foreignSource,activation(foreignSource)));
        assertDoesNotThrow(()->origin.localActivation(seed(definition,receiver)));
    }
    @Test void malformedFramingAndChangedActivationNeverDecodeAsOrigin() {
        var origin=new NativeTopologyImageOrigin(seed(definition(40),source),activation(seed(definition(40),source))); byte[] valid=origin.encode();
        for(int length:new int[]{0,11,valid.length-1}) assertThrows(IllegalArgumentException.class,()->NativeTopologyImageOrigin.decode(Arrays.copyOf(valid,length)));
        assertThrows(IllegalArgumentException.class,()->NativeTopologyImageOrigin.decode(Arrays.copyOf(valid,valid.length+1)));
        for(int length:new int[]{-1,0,Integer.MAX_VALUE,NativeTopologySeed.MAX_BYTES+1}) {
            byte[] malformed=valid.clone(); ByteBuffer.wrap(malformed).putInt(4,length); assertThrows(IllegalArgumentException.class,()->NativeTopologyImageOrigin.decode(malformed));
        }
        byte[] changed=valid.clone(); changed[changed.length-1]^=1; assertThrows(IllegalArgumentException.class,()->NativeTopologyImageOrigin.decode(changed));
        byte[] badTag=valid.clone(); badTag[0]^=1; assertThrows(IllegalArgumentException.class,()->NativeTopologyImageOrigin.decode(badTag));
    }
}
