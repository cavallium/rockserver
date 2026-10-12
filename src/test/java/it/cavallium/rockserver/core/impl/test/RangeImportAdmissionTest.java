package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeImportAdmissionTest {
    static UUID id(long value) { return new UUID(550,value); }
    static ReplicaDescriptor owner(long value,ReplicaDescriptor.Role role) { return new ReplicaDescriptor(id(value),id(value+100),id(value+200),"owner"+value,"127.0.0.1:"+(9000+value),role); }
    static RangeImportDefinition definition() {
        var voter=owner(1,ReplicaDescriptor.Role.VOTER); var listener=owner(2,ReplicaDescriptor.Role.LISTENER);
        var a=new RangeDescriptor(id(10),id(11),2,3,null,new byte[]{64},RangeDescriptor.TopologyState.FENCED,List.of(voter,listener));
        var b=new RangeDescriptor(id(12),id(13),4,5,new byte[]{64},null,RangeDescriptor.TopologyState.FENCED,List.of(voter,listener));
        var directory=new RangeDirectory(id(20),id(21),id(22),1,new byte[32],7,Set.of(id(23)),1,List.of(a,b)); UUID imported=id(24);
        var manifests=new ArrayList<NativeSstImportManifest>(); for(var range:directory.ranges()) manifests.add(new NativeSstImportManifest(imported,range.groupId(),range.rangeEpoch(),directory.domainId(),directory.schemaDigest(),range.lowerInclusive(),range.upperExclusive(),List.of(new NativeSstImportManifest.Column(id(23),"data",false,List.of())),NativeSstImportManifest.Limits.defaults()));
        return new RangeImportDefinition(imported,new OperationId(id(25),1,20),directory,manifests);
    }
    static List<RangeImportAdmission.OwnerReceipt> receipts(RangeImportDefinition definition) {
        var result=new ArrayList<RangeImportAdmission.OwnerReceipt>(); int group=0;
        for(var range:definition.sourceDirectory().ranges()) { var manifest=definition.manifest(range.groupId()); var position=new ApplyPosition(1+group,3+group++); var receipt=new NativeSstImportManifest.Receipt(definition.importId(),range.groupId(),range.rangeEpoch(),manifest.digest(),position); for(var owner:range.replicas()) result.add(new RangeImportAdmission.OwnerReceipt(range.groupId(),range.rangeId(),range.configurationVersion(),owner,receipt,new ApplyPosition(position.term()+1,position.index()+2))); } return result;
    }
    @Test void canonicalAdmissionCoversEveryFrozenVoterAndListenerAcrossIndependentGroups() {
        var definition=definition(); var entries=receipts(definition); var reversed=new ArrayList<>(entries); Collections.reverse(reversed); var admission=new RangeImportAdmission(definition,reversed);
        assertEquals(4,admission.owners().size()); assertArrayEquals(admission.encode(),new RangeImportAdmission(definition,entries).encode()); assertArrayEquals(admission.encode(),RangeImportAdmission.decode(admission.encode(),definition).encode());
        var bytes=admission.encode(); bytes[0]=0; assertThrows(IllegalArgumentException.class,() -> RangeImportAdmission.decode(bytes,definition)); assertNotEquals(0,admission.encode()[0]);
        assertArrayEquals(definition.digest(),RangeImportAdmission.definitionDigest(admission.encode()));
    }
    @Test void missingListenerWrongConfiguredIdentityAndDuplicateOwnerReject() {
        var definition=definition(); var entries=receipts(definition); assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission(definition,entries.subList(0,3)));
        var first=entries.getFirst(); var old=first.owner(); var changed=new ReplicaDescriptor(old.nodeId(),id(999),old.replicaId(),old.peerId(),old.address(),old.role()); var altered=new ArrayList<>(entries); altered.set(0,new RangeImportAdmission.OwnerReceipt(first.groupId(),first.rangeId(),first.configurationVersion(),changed,first.receipt(),first.completed())); assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission(definition,altered));
        altered.set(0,entries.get(1)); assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission(definition,altered));
    }
    @Test void sameGroupInstalledPositionMustAgreeAndCompletedCutMustCoverIt() {
        var definition=definition(); var entries=receipts(definition); var second=entries.get(1); var receipt=second.receipt(); var changed=new NativeSstImportManifest.Receipt(receipt.importId(),receipt.groupId(),receipt.rangeEpoch(),receipt.manifestDigest(),new ApplyPosition(receipt.position().term(),receipt.position().index()+1)); var altered=new ArrayList<>(entries); altered.set(1,new RangeImportAdmission.OwnerReceipt(second.groupId(),second.rangeId(),second.configurationVersion(),second.owner(),changed,second.completed())); assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission(definition,altered));
        assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission.OwnerReceipt(second.groupId(),second.rangeId(),second.configurationVersion(),second.owner(),receipt,new ApplyPosition(receipt.position().term(),receipt.position().index()-1)));
        assertThrows(IllegalArgumentException.class,() -> new RangeImportAdmission(definition,Collections.nCopies(RangeImportAdmission.MAX_OWNERS+1,second))); assertThrows(IllegalArgumentException.class,() -> RangeImportAdmission.decode(new byte[RangeImportAdmission.MAX_BYTES+1],definition));
    }
}
