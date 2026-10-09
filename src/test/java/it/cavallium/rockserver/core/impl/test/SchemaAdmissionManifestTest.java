package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SchemaAdmissionManifestTest {
    static UUID id(int value) { return new UUID(0,value); }
    SchemaChange.Definition definition(int groups,int replicas,boolean operator) {
        var source=new DistributedSchema(id(1),id(2),id(3),0,4,List.of());
        var op=operator ? new DistributedSchema.Operator("append",1,"trusted.Append",new byte[32],1,1,"append-contract",1) : null;
        var shape=new ColumnSchema(IntArrayList.of(4),ObjectArrayList.of(),true,op==null ? null : op.name(),op==null ? null : op.version(),op==null ? null : op.className());
        var target=new DistributedSchema(id(1),id(2),id(3),1,4,List.of(new DistributedSchema.Column(1,id(4),"data",shape,op)));
        var ranges=new ArrayList<RangeDescriptor>();
        for(int i=0;i<groups;i++) {
            var owners=new ArrayList<ReplicaDescriptor>();
            for(int r=0;r<replicas;r++) owners.add(new ReplicaDescriptor(id(4000+r),id(5000+r),id(6000+r),"peer"+r,"127.0.0.1:"+(9000+r),r<5 ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER));
            ranges.add(new RangeDescriptor(id(2000+i),id(1000+i),2,3,i==0 ? null : ByteBuffer.allocate(4).putInt(i).array(),i==groups-1 ? null : ByteBuffer.allocate(4).putInt(i+1).array(),RangeDescriptor.TopologyState.ACTIVE,owners));
        }
        var directory=new RangeDirectory(id(1),id(2),id(3),0,source.digest(),7,source.columnIds(),4,ranges);
        return new SchemaChange.Definition(id(7),new OperationId(id(8),1,0),directory,source,target);
    }
    List<SchemaAdmissionManifest.Binding> bindings(SchemaChange.Definition definition,String name,boolean merge) {
        return definition.header().participants().stream().map(p -> new SchemaAdmissionManifest.Binding(p.groupId(),name,merge)).toList();
    }
    List<SchemaAdmissionManifest.ReplicaAdmission> replicas(SchemaChange.Definition definition) {
        var result=new ArrayList<SchemaAdmissionManifest.ReplicaAdmission>();
        for(var range:definition.sourceDirectory().ranges()) for(var owner:range.replicas()) result.add(new SchemaAdmissionManifest.ReplicaAdmission(range.groupId(),owner.replicaId(),new NativeSchemaCapacityReservation(definition.header(),range.groupId(),new byte[32],4096)));
        return result;
    }
    @Test void maximumCompleteCoverageAnd4096ByteNamesFitCanonicalBudget() {
        var definition=definition(64,16,false); var bindings=new ArrayList<>(bindings(definition,"x".repeat(4096),false)); var admissions=new ArrayList<>(replicas(definition));
        var manifest=new SchemaAdmissionManifest(definition,bindings,admissions); Collections.reverse(bindings); Collections.reverse(admissions);
        assertArrayEquals(manifest.encode(),new SchemaAdmissionManifest(definition,bindings,admissions).encode()); assertTrue(manifest.encode().length<=SchemaAdmissionManifest.MAX_BYTES);
        assertEquals(1024,manifest.replicas().size()); assertEquals(64,manifest.bindings().size());
        for(var range:definition.sourceDirectory().ranges()) for(var owner:range.replicas()) assertEquals(owner,manifest.owner(range.groupId(),owner.replicaId()));
        assertArrayEquals(manifest.encode(),SchemaAdmissionManifest.decode(manifest.encode(),definition).encode());
        assertArrayEquals(manifest.encode(),SchemaAdmissionManifest.decode(manifest.encode(),definition.header(),definition.targetDirectory()).encode());
        byte[] digest=manifest.digest(); digest[0]^=1; assertFalse(Arrays.equals(digest,manifest.digest()));
        assertThrows(UnsupportedOperationException.class,() -> manifest.replicas().clear());
    }
    @Test void missingDuplicateForeignAndWrongGroupEvidenceAreRejected() {
        var definition=definition(2,6,false); var bindings=bindings(definition,"physical",false); var admissions=replicas(definition);
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,admissions.subList(0,admissions.size()-1)));
        var duplicate=new ArrayList<>(admissions); duplicate.set(1,duplicate.getFirst()); assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,duplicate));
        var foreign=new ArrayList<>(admissions); var first=foreign.getFirst(); foreign.set(0,new SchemaAdmissionManifest.ReplicaAdmission(first.groupId(),id(9999),first.reservation())); assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,foreign));
        var wrongGroup=new ArrayList<>(admissions); wrongGroup.set(0,new SchemaAdmissionManifest.ReplicaAdmission(first.groupId(),first.replicaId(),admissions.getLast().reservation())); assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,wrongGroup));
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,List.of(bindings.getFirst(),bindings.getFirst()),admissions));
        var changed=new SchemaChange.Definition(id(99),definition.beginOperation(),definition.sourceDirectory(),definition.sourceSchema(),definition.targetSchema());
        var wrongHeader=new ArrayList<>(admissions); wrongHeader.set(0,new SchemaAdmissionManifest.ReplicaAdmission(first.groupId(),first.replicaId(),new NativeSchemaCapacityReservation(changed.header(),first.groupId(),new byte[32],4096)));
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings,wrongHeader));
    }
    @Test void fullDefinitionEnforcesCreateMergeAndDeleteHasNoTargetBinding() {
        var definition=definition(1,2,true); var admissions=replicas(definition);
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(definition,bindings(definition,"physical",false),admissions));
        var manifest=new SchemaAdmissionManifest(definition,bindings(definition,"physical",true),admissions);
        assertArrayEquals(manifest.encode(),SchemaAdmissionManifest.decode(manifest.encode(),definition).encode());
        var empty=new DistributedSchema(id(1),id(2),id(3),2,4,List.of()); var delete=new SchemaChange.Definition(id(10),new OperationId(id(8),1,1),definition.targetDirectory(),definition.targetSchema(),empty);
        var deleted=new SchemaAdmissionManifest(delete,bindings(delete,null,false),replicas(delete)); assertNull(deleted.binding(id(1000)).changedPhysicalName());
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(delete,bindings(delete,"old",false),replicas(delete)));
        assertThrows(IllegalArgumentException.class,() -> new SchemaAdmissionManifest(delete,bindings(delete,null,true),replicas(delete)));
        assertArrayEquals(deleted.encode(),SchemaAdmissionManifest.decode(deleted.encode(),delete).encode());
    }
    @Test void decodeRejectsNoncanonicalOrderTruncationTrailingDataAndForeignAuthority() {
        var definition=definition(2,2,false); var manifest=new SchemaAdmissionManifest(definition,bindings(definition,"physical",false),replicas(definition)); byte[] bytes=manifest.encode();
        int start=12+definition.header().encode().length; int width=21+"physical".length();
        byte[] swapped=bytes.clone(); System.arraycopy(bytes,start+width,swapped,start,width); System.arraycopy(bytes,start,swapped,start+width,width);
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(swapped,definition));
        int owners=start+2*width+4; byte[] ownerSwap=bytes.clone(); System.arraycopy(bytes,owners+204,ownerSwap,owners,204); System.arraycopy(bytes,owners,ownerSwap,owners+204,204);
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(ownerSwap,definition));
        for(int size:new int[]{0,4,8,start,start+width,bytes.length-1}) assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(Arrays.copyOf(bytes,size),definition));
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(Arrays.copyOf(bytes,bytes.length+1),definition));
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(new byte[SchemaAdmissionManifest.MAX_BYTES+1],definition));
        var changed=new SchemaChange.Definition(id(99),definition.beginOperation(),definition.sourceDirectory(),definition.sourceSchema(),definition.targetSchema());
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(bytes,changed));
        var ranges=new ArrayList<>(definition.sourceDirectory().ranges()); var range=ranges.getFirst(); var ownersChanged=new ArrayList<>(range.replicas()); var owner=ownersChanged.getFirst();
        ownersChanged.set(0,new ReplicaDescriptor(owner.nodeId(),id(999),owner.replicaId(),owner.peerId(),owner.address(),owner.role()));
        ranges.set(0,new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),range.topologyState(),ownersChanged));
        var foreign=new RangeDirectory(id(1),id(2),id(3),0,definition.sourceSchema().digest(),7,definition.sourceSchema().columnIds(),4,ranges);
        assertThrows(IllegalArgumentException.class,() -> SchemaAdmissionManifest.decode(bytes,definition.header(),foreign));
    }
}
