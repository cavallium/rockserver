package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import java.nio.ByteBuffer;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class SchemaChangeTest {
    private static UUID id(int value) { return new UUID(0,value); }
    private static DistributedSchema.Column column(int value) {
        return new DistributedSchema.Column(value,id(value),"column"+value,ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null);
    }
    private static DistributedSchema schema(long epoch,DistributedSchema.Column... columns) {
        return new DistributedSchema(id(100),id(101),id(102),epoch,4,List.of(columns));
    }
    private static RangeDirectory directory(DistributedSchema schema,int count) {
        var ranges=new ArrayList<RangeDescriptor>();
        for (int i=0;i<count;i++) {
            var replica=new ReplicaDescriptor(id(200),id(201),id(202),"peer","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
            ranges.add(new RangeDescriptor(id(300+i),id(400+i),8,9,i==0 ? null : ByteBuffer.allocate(4).putInt(i).array(),
                    i==count-1 ? null : ByteBuffer.allocate(4).putInt(i+1).array(),RangeDescriptor.TopologyState.ACTIVE,List.of(replica)));
        }
        return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),7,schema.columnIds(),4,ranges);
    }
    private static SchemaChange.Definition definition(DistributedSchema source,DistributedSchema target,int count) {
        return new SchemaChange.Definition(id(500),new OperationId(id(501),2,3),directory(source,count),source,target);
    }
    @Test void createDeleteEmptyDomainRoundTripRetainsGroupsAndProjection() {
        var empty=schema(1); var created=schema(2,column(1)); var create=definition(empty,created,2);
        assertEquals(SchemaChange.Kind.CREATE_COLUMN,create.kind()); assertEquals(1,create.header().changedApiId());
        assertEquals(0,create.header().sourceMaximumApiId()); assertEquals(1,create.header().targetMaximumApiId());
        assertEquals(create.sourceDirectory().ranges(),create.targetDirectory().ranges());
        assertEquals(8,create.targetDirectory().generation()); created.requireMatches(create.targetDirectory());
        assertArrayEquals(create.encode(),SchemaChange.Definition.decode(create.encode()).encode());
        assertArrayEquals(create.header().encode(),SchemaChange.Header.decode(create.header().encode()).encode());
        assertArrayEquals(create.digest(),create.header().definitionDigest());
        var delete=definition(created,schema(3),2); assertEquals(SchemaChange.Kind.DELETE_COLUMN,delete.kind());
        assertTrue(delete.targetDirectory().columns().isEmpty()); assertEquals(2,delete.targetDirectory().ranges().size());
        assertArrayEquals(RangeDirectoryCodec.encode(delete.targetDirectory()),RangeDirectoryCodec.encode(RangeDirectoryCodec.decode(RangeDirectoryCodec.encode(delete.targetDirectory()))));
        assertThrows(IllegalArgumentException.class,() -> delete.targetDirectory().route(id(1),new byte[4]));
        delete.header().definitionDigest()[0]^=1; assertArrayEquals(delete.digest(),delete.header().definitionDigest());
    }
    @Test void exactOneColumnChangePreservesUnchangedContentAndIdentities() {
        var source=schema(1,column(1),column(3));
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(2,column(1),column(3),column(2)),1));
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(2,column(1)),1+SchemaChange.MAX_PARTICIPANTS));
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(2,column(4)),1));
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(3,column(1)),1));
        var renamed=new DistributedSchema.Column(1,id(1),"renamed",column(1).schema(),null);
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(2,renamed,column(3),column(4)),1));
        var recycledName=new DistributedSchema.Column(4,id(4),"column1",column(1).schema(),null);
        assertThrows(IllegalArgumentException.class,() -> definition(source,schema(2,column(1),column(3),recycledName),1));
        var wrongDirectory=directory(schema(2,column(1)),1);
        assertThrows(IllegalArgumentException.class,() -> new SchemaChange.Definition(id(500),new OperationId(id(501),0,0),wrongDirectory,source,schema(2,column(1))));
    }
    @Test void durableLifecycleRequiresAllFencesThenStrictInstallThenPublicationAndRetirement() {
        var definition=definition(schema(1,column(1)),schema(2,column(1),column(2)),2);
        var initial=SchemaChange.Status.begin(definition.header()); UUID first=id(400), second=id(401);
        var fencing=initial.beginFencing(); fencing.requireSuccessorOf(initial);
        assertSame(fencing,fencing.beginFencing());
        assertThrows(IllegalStateException.class,() -> initial.fenced(first,new ApplyPosition(2,10)));
        assertThrows(IllegalStateException.class,fencing::abort);
        var aborted=initial.abort(); aborted.requireSuccessorOf(initial);
        assertThrows(IllegalStateException.class,aborted::beginFencing);
        assertThrows(IllegalArgumentException.class,() -> fencing.requireSuccessorOf(aborted));
        assertThrows(IllegalArgumentException.class,() -> aborted.requireSuccessorOf(fencing));
        var partial=fencing.fenced(first,new ApplyPosition(2,10)); partial.requireSuccessorOf(fencing);
        assertEquals(SchemaChange.Phase.FENCING,partial.phase());
        assertThrows(IllegalArgumentException.class,() -> partial.requireSuccessorOf(initial));
        assertSame(partial,partial.fenced(first,new ApplyPosition(2,10)));
        assertThrows(IllegalStateException.class,() -> partial.abort());
        assertThrows(IllegalStateException.class,() -> partial.installed(first,new ApplyPosition(2,11)));
        var fenced=partial.fenced(second,new ApplyPosition(3,20)); fenced.requireSuccessorOf(partial);
        assertEquals(SchemaChange.Phase.FENCED,fenced.phase());
        assertThrows(IllegalArgumentException.class,() -> fenced.installed(first,new ApplyPosition(2,10)));
        assertThrows(IllegalArgumentException.class,() -> fenced.installed(first,new ApplyPosition(1,11)));
        assertThrows(IllegalArgumentException.class,() -> fenced.installed(first,new ApplyPosition(3,10)));
        var installing=fenced.installed(first,new ApplyPosition(3,11)); installing.requireSuccessorOf(fenced);
        assertThrows(IllegalStateException.class,installing::publish);
        var installed=installing.installed(second,new ApplyPosition(3,21)); installed.requireSuccessorOf(installing);
        assertThrows(IllegalArgumentException.class,() -> installed.requireSuccessorOf(fenced));
        var published=installed.publish(); published.requireSuccessorOf(installed);
        var retired=published.retire(); retired.requireSuccessorOf(published);
        assertSame(retired,retired.retire()); retired.requireSuccessorOf(retired);
        for (var status:List.of(initial,fencing,partial,fenced,installing,installed,published,retired,aborted)) {
            assertArrayEquals(status.encode(),SchemaChange.Status.decode(status.encode()).encode());
            assertTrue(status.encode().length<16*1024);
        }
        assertThrows(IllegalArgumentException.class,() -> retired.fenced(first,new ApplyPosition(4,30)));
        assertThrows(IllegalArgumentException.class,() -> retired.installed(first,new ApplyPosition(4,30)));
        assertThrows(IllegalArgumentException.class,() -> initial.fenced(id(999),new ApplyPosition(0,1)));
    }
    @Test void canonicalBoundsRejectTruncationTrailingDataAndMultiProofSuccessors() {
        var definition=definition(schema(1),schema(2,column(1)),64); var header=definition.header();
        var status=SchemaChange.Status.begin(header).beginFencing();
        for (var participant:header.participants()) status=status.fenced(participant.groupId(),new ApplyPosition(0,1));
        for (var participant:header.participants()) status=status.installed(participant.groupId(),new ApplyPosition(1,2));
        assertTrue(status.encode().length<=SchemaChange.MAX_STATUS_BYTES);
        byte[][] payloads={definition.encode(),header.encode(),status.encode()};
        for (int type=0;type<3;type++) {
            final int which=type; byte[] bytes=payloads[type];
            for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> decode(which,truncated)); }
            assertThrows(IllegalArgumentException.class,() -> decode(which,Arrays.copyOf(bytes,bytes.length+1)));
        }
        assertThrows(IllegalArgumentException.class,() -> SchemaChange.Definition.decode(new byte[SchemaChange.MAX_DEFINITION_BYTES+1]));
        assertThrows(IllegalArgumentException.class,() -> SchemaChange.Status.decode(new byte[SchemaChange.MAX_STATUS_BYTES+1]));
    }
    @Test void combinedDefinitionBudgetRejectsIndividuallyValidLargeSchemas() {
        var operator=new DistributedSchema.Operator("o".repeat(4096),1,"c".repeat(4096),new byte[32],1,1);
        var shape=ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true,operator.name(),operator.version(),operator.className());
        var columns=new ArrayList<DistributedSchema.Column>();
        for (int i=1;i<=128;i++) columns.add(new DistributedSchema.Column(i,id(i),"n".repeat(4090)+String.format("%06d",i),shape,operator));
        var source=new DistributedSchema(id(1000),id(1001),id(1002),1,4,columns);
        columns.add(new DistributedSchema.Column(129,id(129),"new",shape,operator));
        var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),2,4,columns);
        assertTrue(DistributedSchemaCodec.encode(source).length<DistributedSchemaCodec.MAX_BYTES);
        assertTrue(DistributedSchemaCodec.encode(target).length<DistributedSchemaCodec.MAX_BYTES);
        assertThrows(IllegalArgumentException.class,() -> definition(source,target,1));
    }
    private static void decode(int type,byte[] bytes) {
        switch(type) { case 0 -> SchemaChange.Definition.decode(bytes); case 1 -> SchemaChange.Header.decode(bytes); case 2 -> SchemaChange.Status.decode(bytes); default -> throw new AssertionError(); }
    }
}
