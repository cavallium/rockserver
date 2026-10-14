package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedSchemaTest {
    private static final UUID CLUSTER=new UUID(0,1), DATABASE=new UUID(0,2), DOMAIN=new UUID(0,3), A=new UUID(0,4), B=new UUID(Long.MIN_VALUE,5);
    private static ColumnSchema fixed(int... widths) { return ColumnSchema.of(IntArrayList.of(widths),ObjectArrayList.of(),true); }
    private static DistributedSchema.Column column(long api,UUID id,String name,ColumnSchema shape) { return new DistributedSchema.Column(api,id,name,shape,null); }
    private static DistributedSchema schema(int prefix,DistributedSchema.Column... columns) { return new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,prefix,List.of(columns)); }
    private static DistributedSchema.Operator operator(byte[] artifact) { return new DistributedSchema.Operator("sum",2,"example.Sum",artifact,3,4,"pure-sum",1); }
    private static ColumnSchema merged() { return ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true,"sum",2L,"example.Sum"); }

    @Test void canonicalRoundTripDeepCopiesAndStableExplicitLookups() {
        var widths=IntArrayList.of(4,8); var tails=ObjectArrayList.<ColumnHashType>of(); byte[] artifact=new byte[32];
        var first=column(17,A,"alpha",new ColumnSchema(widths,tails,true,null,null,null));
        var second=new DistributedSchema.Column(23,B,"βeta",merged(),operator(artifact));
        var columns=new ArrayList<>(List.of(second,first)); var original=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,1,4,columns);
        widths.clear(); tails.add(ColumnHashType.ALLSAME8); artifact[0]=1; columns.clear();
        assertEquals(List.of(4,8),original.column(A).schema().keys()); assertEquals(0,original.column(B).operator().artifactSha256()[0]);
        assertSame(first,original.column(17)); assertSame(second,original.column("βeta"));
        assertThrows(IllegalArgumentException.class,() -> original.column(99)); assertThrows(IllegalArgumentException.class,() -> original.column("missing"));
        assertThrows(UnsupportedOperationException.class,() -> original.columns().clear());
        assertThrows(UnsupportedOperationException.class,() -> original.columnIds().clear());
        assertThrows(UnsupportedOperationException.class,() -> original.column(A).schema().keys().clear());
        original.column(B).operator().artifactSha256()[0]=9; original.digest()[0]^=1;
        byte[] encoded=DistributedSchemaCodec.encode(original); var decoded=DistributedSchemaCodec.decode(encoded);
        assertArrayEquals(encoded,DistributedSchemaCodec.encode(decoded)); assertArrayEquals(original.digest(),decoded.digest());
        assertEquals(original.columns(),decoded.columns()); assertEquals(1,decoded.physicalKeyCodecVersion());
        assertEquals(A,new UUID(ByteBuffer.wrap(encoded).getLong(72),ByteBuffer.wrap(encoded).getLong(80)));
        assertArrayEquals(encoded,DistributedSchemaCodec.encode(schema(4,first,second)));
    }

    @Test void contentDigestBindsApiNamesWidthsHashesOperatorsAndSchemaScope() {
        var base=schema(0,column(1,A,"data",fixed(4))); byte[] digest=base.digest();
        for (var changed:List.of(schema(0,column(2,A,"data",fixed(4))),schema(0,column(1,A,"changed",fixed(4))),
                schema(0,column(1,A,"data",fixed(8))),schema(0,column(1,B,"data",fixed(4))),schema(4,column(1,A,"data",fixed(4))),
                new DistributedSchema(CLUSTER,DATABASE,DOMAIN,2,0,base.columns()),
                new DistributedSchema(new UUID(9,9),DATABASE,DOMAIN,1,0,base.columns()),
                schema(0,new DistributedSchema.Column(1,A,"data",merged(),operator(new byte[32]))))) {
            assertFalse(Arrays.equals(digest,changed.digest()));
        }
        byte[] artifact=new byte[32]; artifact[0]=1;
        var merged=schema(0,new DistributedSchema.Column(1,A,"data",merged(),operator(new byte[32])));
        var differentArtifact=schema(0,new DistributedSchema.Column(1,A,"data",merged(),operator(artifact)));
        assertFalse(Arrays.equals(merged.digest(),differentArtifact.digest()));
        var hashDigests=new HashSet<String>();
        for (var hash:ColumnHashType.values()) {
            var shape=ColumnSchema.of(IntArrayList.of(),ObjectArrayList.of(hash),true);
            var value=schema(0,column(1,A,"data",shape)); var copy=DistributedSchemaCodec.decode(DistributedSchemaCodec.encode(value));
            assertEquals(hash,copy.column(A).schema().variableTailKeys().getFirst()); hashDigests.add(HexFormat.of().formatHex(value.digest()));
        }
        assertEquals(4,hashDigests.size());
        var differentCodec=new DistributedSchema.Operator("sum",2,"example.Sum",new byte[32],4,4,"pure-sum",1);
        assertFalse(Arrays.equals(merged.digest(),schema(0,new DistributedSchema.Column(1,A,"data",merged(),differentCodec)).digest()));
    }

    @Test void fixedPhysicalKeysExactlyMatchExistingCodecForSignedMultipartAndZeroKeys() {
        for (int[] widths:new int[][] {{},{8},{4,8},{1,2,4}}) {
            var shape=fixed(widths); var schema=schema(0,column(7,A,"data",shape)); var keys=new Buf[widths.length];
            for (int i=0;i<widths.length;i++) { byte[] bytes=new byte[widths[i]]; Arrays.fill(bytes,(byte)(0x80+i)); keys[i]=Buf.wrap(bytes); }
            if (widths.length==2) { keys[0]=Buf.wrap(ByteBuffer.allocate(4).putInt(-123).array()); keys[1]=Buf.wrap(ByteBuffer.allocate(8).putLong(Long.MIN_VALUE).array()); }
            var logical=new Keys(keys); byte[] expected=new ColumnInstance(null,shape).calculateKey(keys).toByteArray();
            assertArrayEquals(expected,schema.encodeFixedKey(A,logical)); assertEquals(logical,schema.decodeFixedKey(A,expected));
            var decoded=schema.decodeFixedKey(A,expected); if (expected.length>0) expected[0]^=1;
            assertEquals(logical,decoded);
            assertThrows(IllegalArgumentException.class,() -> schema.decodeFixedKey(A,new byte[new ColumnInstance(null,shape).finalKeySizeBytes()+1]));
        }
        var schema=schema(0,column(1,A,"data",fixed(4)));
        assertThrows(IllegalArgumentException.class,() -> schema.encodeFixedKey(A,new Keys()));
        assertThrows(IllegalArgumentException.class,() -> schema.encodeFixedKey(A,new Keys(Buf.wrap(new byte[3]))));
        var variable=schema(0,column(1,A,"data",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(ColumnHashType.XXHASH32),true)));
        assertThrows(UnsupportedOperationException.class,() -> variable.encodeFixedKey(A,new Keys()));
        assertThrows(UnsupportedOperationException.class,() -> variable.decodeFixedKey(A,new byte[8]));
    }

    @Test void invalidIdentityBindingsOperatorsWidthsAndPrefixBoundariesAreRejected() {
        var first=column(1,A,"data",fixed(4,8));
        assertThrows(IllegalArgumentException.class,() -> schema(0,first,column(1,B,"other",fixed(4))));
        assertThrows(IllegalArgumentException.class,() -> schema(0,first,column(2,A,"other",fixed(4))));
        assertThrows(IllegalArgumentException.class,() -> schema(0,first,column(2,B,"data",fixed(4))));
        assertThrows(IllegalArgumentException.class,() -> column(0,A,"data",fixed(4)));
        assertThrows(IllegalArgumentException.class,() -> column(1,A,"",fixed(4)));
        assertThrows(IllegalArgumentException.class,() -> column(1,A,"a".repeat(4097),fixed(4)));
        assertThrows(IllegalArgumentException.class,() -> column(1,A,"\ud800",fixed(4)));
        assertThrows(IllegalArgumentException.class,() -> schema(2,first));
        assertThrows(IllegalArgumentException.class,() -> schema(20,first));
        assertThrows(IllegalArgumentException.class,() -> schema(4,first,column(2,B,"other",fixed(2,2,8))));
        assertThrows(IllegalArgumentException.class,() -> schema(4,column(1,A,"empty",fixed())));
        assertThrows(IllegalArgumentException.class,() -> column(1,A,"large",fixed(DistributedCommandCodec.MAX_ENCODED_BYTES,1)));
        int[] widths=new int[257]; Arrays.fill(widths,1);
        assertThrows(IllegalArgumentException.class,() -> column(1,A,"many",fixed(widths)));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Column(1,A,"data",merged(),null));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Column(1,A,"data",fixed(4),operator(new byte[32])));
        var partial=ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true,"sum",2L);
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Column(1,A,"data",partial,operator(new byte[32])));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Operator("sum",-1,"example.Sum",new byte[32],1,1,"pure-sum",1));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Operator("sum",1,"example.Sum",new byte[31],1,1,"pure-sum",1));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema.Operator("sum",1,"example.Sum",new byte[32],0,1,"pure-sum",1));
        assertDoesNotThrow(() -> schema(4,first,column(2,B,"variable",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(ColumnHashType.XXHASH8),true))));
    }

    @Test void decoderRejectsEveryTruncationHeadersCountsWidthsFlagsHashesAndTrailingBytes() {
        byte[] valid=DistributedSchemaCodec.encode(schema(0,column(1,A,"x",fixed(4))));
        for (int length=0;length<valid.length;length++) { byte[] shortBytes=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(shortBytes)); }
        assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(Arrays.copyOf(valid,valid.length+1)));
        for (int offset:new int[] {0,4,64,68,96,101,105,109}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(offset,Integer.MAX_VALUE);
            assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(bad));
        }
        for (int offset:new int[] {113,114}) { byte[] bad=valid.clone(); bad[offset]=2; assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(bad)); }
        byte[] badName=valid.clone(); badName[100]=(byte)0xff; assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(badName));
        byte[] negative=valid.clone(); ByteBuffer.wrap(negative).putLong(56,-1); assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(negative));
        byte[] variable=DistributedSchemaCodec.encode(schema(0,column(1,A,"x",ColumnSchema.of(IntArrayList.of(),ObjectArrayList.of(ColumnHashType.XXHASH32),true))));
        variable[113]=99; assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(variable));
        assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(new byte[DistributedSchemaCodec.MAX_BYTES+1]));
        byte[] two=DistributedSchemaCodec.encode(schema(0,column(1,A,"x",fixed()),column(2,B,"y",fixed())));
        System.arraycopy(two,72,two,111,16); assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(two));
    }

    @Test void combinedSchemaBudgetAndColumnCountsAreBoundedBeforeEncoding() {
        var empty=new DistributedSchema(CLUSTER,DATABASE,DOMAIN,0,0,List.of());
        assertArrayEquals(DistributedSchemaCodec.encode(empty),DistributedSchemaCodec.encode(DistributedSchemaCodec.decode(DistributedSchemaCodec.encode(empty))));
        var columns=new ArrayList<DistributedSchema.Column>();
        String operatorName="o".repeat(4096), className="c".repeat(4096);
        var operator=new DistributedSchema.Operator(operatorName,1,className,new byte[32],1,1,"pure-sum",1);
        var shape=ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true,operatorName,1L,className);
        for (int i=1;i<=RangeDirectoryCodec.MAX_COLUMNS;i++) {
            columns.add(new DistributedSchema.Column(i,new UUID(0,i),"n".repeat(4092)+String.format("%04d",i),shape,operator));
        }
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema(CLUSTER,DATABASE,DOMAIN,0,0,columns));
        var plain=new ArrayList<DistributedSchema.Column>();
        for (int i=1;i<=RangeDirectoryCodec.MAX_COLUMNS+1;i++) plain.add(column(i,new UUID(0,i),"c"+i,fixed()));
        assertThrows(IllegalArgumentException.class,() -> new DistributedSchema(CLUSTER,DATABASE,DOMAIN,0,0,plain));
    }

    @Test void routingPrefixUsesDirectoryCeilingWhileUnprojectedPhysicalKeysKeepCommandBudget() {
        int ceiling=RangeDirectoryCodec.MAX_ENCODED_BYTES;
        assertDoesNotThrow(() -> schema(ceiling,column(1,A,"boundary",fixed(ceiling))));
        assertDoesNotThrow(() -> schema(0,column(1,A,"physical",fixed(DistributedCommandCodec.MAX_ENCODED_BYTES))));
        var tooWide=column(1,A,"wide",fixed(ceiling+1));
        assertThrows(IllegalArgumentException.class,() -> schema(ceiling+1,tooWide));
        byte[] encoded=DistributedSchemaCodec.encode(schema(0,tooWide));
        ByteBuffer.wrap(encoded).putInt(64,ceiling+1);
        assertThrows(IllegalArgumentException.class,() -> DistributedSchemaCodec.decode(encoded));
    }

    @Test void directoryMustMatchFullScopeContentAndRoutingProjection() {
        var schema=schema(4,column(1,A,"data",fixed(4,8)));
        var replica=new ReplicaDescriptor(new UUID(1,1),new UUID(1,2),new UUID(1,3),"peer","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(1,4),new UUID(1,5),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,schema.digest(),0,Set.of(A),4,List.of(range));
        assertDoesNotThrow(() -> schema.requireMatches(directory));
        for (var mismatch:List.of(new RangeDirectory(CLUSTER,DATABASE,DOMAIN,2,schema.digest(),0,Set.of(A),4,List.of(range)),
                new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],0,Set.of(A),4,List.of(range)),
                new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,schema.digest(),0,Set.of(B),4,List.of(range)),
                new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,schema.digest(),0,Set.of(A),0,List.of(range)),
                new RangeDirectory(new UUID(9,9),DATABASE,DOMAIN,1,schema.digest(),0,Set.of(A),4,List.of(range)))) {
            assertThrows(IllegalArgumentException.class,() -> schema.requireMatches(mismatch));
        }
    }
    @Test void variablePhysicalKeysMatchExistingHashesAndRetainDistinctCollisionMembers() {
        for (var hash:ColumnHashType.values()) {
            var shape=ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(hash,ColumnHashType.ALLSAME8),true);
            var schema=schema(8,column(1,A,"bucket",shape));
            byte[] fixed=ByteBuffer.allocate(8).putLong(-42).array();
            byte[] variable=hash==ColumnHashType.FIXEDINTEGER32 ? ByteBuffer.allocate(4).putInt(-7).array() : new byte[]{7,8,9};
            var keys=new Keys(Buf.wrap(fixed),Buf.wrap(variable),Buf.wrap(new byte[]{1,2})); var encoded=schema.encodeLogicalKey(A,keys);
            assertArrayEquals(new ColumnInstance(null,shape).calculateKey(keys.keys()).toByteArray(),encoded.physicalKey());
            assertDoesNotThrow(() -> schema.requireBucketKey(A,encoded.physicalKey(),encoded.variableKeys()));
            var collided=schema.encodeLogicalKey(A,new Keys(Buf.wrap(fixed),Buf.wrap(variable),Buf.wrap(new byte[]{9,9,9})));
            assertArrayEquals(encoded.physicalKey(),collided.physicalKey()); assertNotEquals(encoded,collided);
            fixed[0]^=1; variable[0]^=1; encoded.physicalKey()[0]^=1; encoded.variableKeys().getFirst()[0]^=1;
            assertNotEquals(fixed[0],encoded.physicalKey()[0]); assertNotEquals(variable[0],encoded.variableKeys().getFirst()[0]);
            assertEquals(encoded,new DistributedSchema.EncodedKey(encoded.physicalKey(),encoded.variableKeys()));
        }
    }
    @Test void bucketHashVerificationRejectsMisroutedMalformedAndOversizedMemberKeys() {
        var shape=ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(ColumnHashType.XXHASH32),false);
        var schema=schema(0,column(1,A,"bucket",shape)); var encoded=schema.encodeLogicalKey(A,new Keys(Buf.wrap(new byte[]{7}),Buf.wrap(new byte[]{1,2})));
        byte[] wrong=encoded.physicalKey(); wrong[1]^=1;
        assertThrows(IllegalArgumentException.class,() -> schema.requireBucketKey(A,wrong,encoded.variableKeys()));
        assertThrows(IllegalArgumentException.class,() -> schema.requireBucketKey(A,new byte[1],encoded.variableKeys()));
        assertThrows(IllegalArgumentException.class,() -> schema.requireBucketKey(A,encoded.physicalKey(),List.of()));
        assertThrows(IllegalArgumentException.class,() -> schema.encodeLogicalKey(A,new Keys(Buf.wrap(new byte[]{1,2}),Buf.wrap(new byte[]{1}))));
        assertThrows(IllegalArgumentException.class,() -> schema.encodeLogicalKey(A,new Keys(Buf.wrap(new byte[]{7}),Buf.wrap(new byte[65536]))));
        var integerShape=ColumnSchema.of(IntArrayList.of(),ObjectArrayList.of(ColumnHashType.FIXEDINTEGER32),true);
        var integer=schema(0,column(1,A,"int",integerShape));
        assertThrows(IllegalArgumentException.class,() -> integer.encodeLogicalKey(A,new Keys(Buf.wrap(new byte[3]))));
        var fixed=schema(0,column(1,A,"fixed",fixed(4)));
        var key=new Keys(Buf.wrap(ByteBuffer.allocate(4).putInt(-1).array()));
        assertArrayEquals(fixed.encodeFixedKey(A,key),fixed.encodeLogicalKey(A,key).physicalKey()); assertTrue(fixed.encodeLogicalKey(A,key).variableKeys().isEmpty());
        assertThrows(IllegalArgumentException.class,() -> fixed.requireBucketKey(A,new byte[4],List.of()));
    }

}
