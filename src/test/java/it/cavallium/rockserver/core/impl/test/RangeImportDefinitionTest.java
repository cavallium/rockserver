package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeImportDefinitionTest {
    final UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),job=UUID.randomUUID(),column=UUID.randomUUID(),left=UUID.randomUUID(),right=UUID.randomUUID();
    final OperationId operation=new OperationId(UUID.randomUUID(),1,0);
    RangeDirectory directory(RangeDescriptor.TopologyState state) {
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"peer","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(cluster,database,domain,1,new byte[32],3,Set.of(column),1,List.of(
                new RangeDescriptor(UUID.randomUUID(),left,1,2,null,new byte[]{100},state,List.of(replica)),
                new RangeDescriptor(UUID.randomUUID(),right,1,2,new byte[]{100},null,RangeDescriptor.TopologyState.FENCED,List.of(replica))));
    }
    NativeSstImportManifest manifest(UUID group,byte[] lower,byte[] upper) {
        return new NativeSstImportManifest(job,group,1,domain,new byte[32],lower,upper,List.of(new NativeSstImportManifest.Column(column,"data",false,List.of())),NativeSstImportManifest.Limits.defaults());
    }
    @Test void derivesOnlyTheExactSelectedSuccessorAndRetainsImmutableOriginalAuthority() {
        var source=directory(RangeDescriptor.TopologyState.FENCED); var manifest=manifest(left,null,new byte[]{100});
        var definition=new RangeImportDefinition(job,operation,source,List.of(manifest)); var target=definition.targetDirectory();
        assertEquals(4,target.generation()); assertEquals(RangeDescriptor.TopologyState.ACTIVE,target.ranges().getFirst().topologyState());
        assertEquals(source.ranges().getLast(),target.ranges().getLast());
        var old=source.ranges().getFirst(); var next=target.ranges().getFirst();
        assertEquals(old.rangeId(),next.rangeId()); assertEquals(old.groupId(),next.groupId()); assertEquals(old.rangeEpoch(),next.rangeEpoch());
        assertEquals(old.configurationVersion(),next.configurationVersion()); assertEquals(old.replicas(),next.replicas());
        var decoded=RangeImportDefinition.decode(definition.encode()); assertArrayEquals(definition.digest(),decoded.digest());
        byte[] exposed=definition.digest(); exposed[0]^=1; assertArrayEquals(decoded.digest(),definition.digest());
        var receipt=new NativeSstImportManifest.Receipt(job,left,1,manifest.digest(),new ApplyPosition(2,8)); definition.requireReceipt(receipt);
        assertThrows(IllegalArgumentException.class,() -> definition.requireReceipt(new NativeSstImportManifest.Receipt(job,left,1,new byte[32],receipt.position())));
    }
    @Test void activeWrongBoundForeignAndDuplicateTargetsReject() {
        var active=directory(RangeDescriptor.TopologyState.ACTIVE); var exact=manifest(left,null,new byte[]{100});
        assertThrows(IllegalArgumentException.class,() -> new RangeImportDefinition(job,operation,active,List.of(exact)));
        var fenced=directory(RangeDescriptor.TopologyState.FENCED);
        assertThrows(IllegalArgumentException.class,() -> new RangeImportDefinition(job,operation,fenced,List.of(manifest(left,null,new byte[]{99}))));
        assertThrows(IllegalArgumentException.class,() -> new RangeImportDefinition(job,operation,fenced,List.of(manifest(UUID.randomUUID(),null,new byte[]{100}))));
        assertThrows(IllegalArgumentException.class,() -> new RangeImportDefinition(job,operation,fenced,List.of(exact,exact)));
    }
    @Test void directoryOrderDeterminesCanonicalManifestOrderAndTrailingBytesReject() {
        var source=directory(RangeDescriptor.TopologyState.FENCED); var a=manifest(left,null,new byte[]{100}); var b=manifest(right,new byte[]{100},null);
        var first=new RangeImportDefinition(job,operation,source,List.of(b,a)); var second=new RangeImportDefinition(job,operation,source,List.of(a,b));
        assertArrayEquals(first.encode(),second.encode()); assertEquals(List.of(a,b),first.manifests());
        assertThrows(IllegalArgumentException.class,() -> RangeImportDefinition.decode(Arrays.copyOf(first.encode(),first.encode().length+1)));
    }
}
