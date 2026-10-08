package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.DistributedBucket;
import it.cavallium.rockserver.core.common.distributed.DistributedCommandCodec;
import it.cavallium.rockserver.core.impl.Bucket;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedBucketTest {
    private static ColumnSchema schema(int tails,boolean value) {
        var hashes=new ObjectArrayList<ColumnHashType>(); for (int i=0;i<tails;i++) hashes.add(ColumnHashType.ALLSAME8);
        return ColumnSchema.of(IntArrayList.of(4),hashes,value);
    }
    private static Buf[] buffers(List<byte[]> tails) { return tails.stream().map(Buf::wrap).toArray(Buf[]::new); }
    private static List<byte[]> tails(int count,byte first) {
        var keys=new ArrayList<byte[]>(); for (int i=0;i<count;i++) keys.add(new byte[] {first,(byte)i}); return keys;
    }
    private static void same(Bucket legacy,DistributedBucket bucket,ColumnSchema schema) {
        assertArrayEquals(legacy.toSegment().toByteArray(),bucket.encode());
        assertArrayEquals(bucket.encode(),DistributedBucket.decode(schema,legacy.toSegment().toByteArray()).encode());
        assertArrayEquals(bucket.encode(),new Bucket(new ColumnInstance(null,schema),Buf.wrap(bucket.encode())).toSegment().toByteArray());
        for (var member:bucket.members()) assertArrayEquals(legacy.getElement(buffers(member.tails())).toByteArray(),bucket.get(member.tails()));
    }

    @Test void standaloneCollisionBucketsMatchForMultiTailsValuesReplacementDeletionAndNulls() {
        for (int count:new int[] {1,2}) for (boolean hasValue:new boolean[] {false,true}) {
            var schema=schema(count,hasValue); var column=new ColumnInstance(null,schema); var legacy=new Bucket(column); var bucket=DistributedBucket.empty(schema);
            same(legacy,bucket,schema);
            var first=tails(count,(byte)0xff); var second=tails(count,(byte)0x80); var absent=tails(count,(byte)0x7f);
            var completeFirst=new Buf[count+1]; var completeSecond=new Buf[count+1]; completeFirst[0]=completeSecond[0]=Buf.wrap(ByteBuffer.allocate(4).putInt(-123).array());
            System.arraycopy(buffers(first),0,completeFirst,1,count); System.arraycopy(buffers(second),0,completeSecond,1,count);
            assertArrayEquals(column.calculateKey(completeFirst).toByteArray(),column.calculateKey(completeSecond).toByteArray(),"Distinct originals share the existing ALLSAME8 physical key");
            byte[] value=hasValue ? new byte[] {(byte)0xff,2} : null;
            assertNull(legacy.addElement(buffers(first),value==null ? null : Buf.wrap(value))); bucket=bucket.put(first,value); same(legacy,bucket,schema);
            legacy.addElement(buffers(second),null); bucket=bucket.put(second,null); same(legacy,bucket,schema);
            byte[] changed=hasValue ? new byte[] {3,4,5} : new byte[0];
            legacy.addElement(buffers(first),Buf.wrap(changed)); bucket=bucket.put(first,changed); same(legacy,bucket,schema);
            assertEquals(new DistributedBucket.Member(first,changed),bucket.members().getFirst());
            assertNull(bucket.get(absent)); assertSame(bucket,bucket.remove(absent));
            legacy.removeElement(buffers(first)); bucket=bucket.remove(first); same(legacy,bucket,schema);
            legacy.removeElement(buffers(second)); bucket=bucket.remove(second); same(legacy,bucket,schema); assertTrue(bucket.members().isEmpty());
        }
    }

    @Test void memberAndBucketOwnAllKeysValuesAndSchemaShape() {
        var schema=schema(1,true); byte[] key={1,2}, value={3,4}; var keys=new ArrayList<>(List.of(key));
        var bucket=DistributedBucket.empty(schema).put(keys,value); byte[] encoded=bucket.encode();
        key[0]=99; value[0]=99; keys.clear(); schema.variableTailKeys().clear();
        assertArrayEquals(new byte[] {3,4},bucket.get(List.of(new byte[] {1,2})));
        var member=bucket.members().getFirst(); member.tails().getFirst()[0]=55; member.value()[0]=55; bucket.get(List.of(new byte[] {1,2}))[0]=55;
        assertThrows(UnsupportedOperationException.class,() -> bucket.members().clear()); assertThrows(UnsupportedOperationException.class,() -> member.tails().clear());
        assertArrayEquals(encoded,bucket.encode()); assertEquals(new DistributedBucket.Member(List.of(new byte[] {1,2}),new byte[] {3,4}),member);
        assertEquals(new DistributedBucket.Member(List.of(new byte[] {1,2}),new byte[] {3,4}).hashCode(),member.hashCode());
        var decoded=DistributedBucket.decode(schema(1,true),encoded); encoded[encoded.length-1]=0; assertArrayEquals(new byte[] {3,4},decoded.get(List.of(new byte[] {1,2})));
        assertNotSame(bucket,bucket.put(List.of(new byte[] {1,2}),new byte[] {9})); assertArrayEquals(new byte[] {3,4},bucket.get(List.of(new byte[] {1,2})));
    }

    @Test void emptyAndLegacyZeroCountNormalizeAndUnsignedShortTailsArePreserved() {
        var schema=schema(1,false);
        for (byte[] empty:new byte[][] {null,new byte[0],new byte[4]}) {
            var bucket=DistributedBucket.decode(schema,empty); assertTrue(bucket.members().isEmpty()); assertArrayEquals(new byte[0],bucket.encode());
        }
        for (int length:new int[] {0,32768,65535}) {
            byte[] tail=new byte[length]; Arrays.fill(tail,(byte)0xff); var bucket=DistributedBucket.empty(schema).put(List.of(tail),null);
            var legacy=new Bucket(new ColumnInstance(null,schema)); legacy.addElement(new Buf[] {Buf.wrap(tail)},null);
            same(legacy,bucket,schema); assertArrayEquals(new byte[0],bucket.get(List.of(tail)));
        }
        assertThrows(DistributedBucket.LimitExceededException.class,() -> DistributedBucket.empty(schema).put(List.of(new byte[65536]),null));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.empty(schema).put(List.of(new byte[0]),new byte[] {1}));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.empty(ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true)));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.empty(schema).get(List.of()));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.empty(schema).remove(Arrays.asList((byte[])null)));
    }

    @Test void malformedStoredLengthsCountsTrailingDuplicatesAndKeyOnlyValuesAreRejected() {
        var schema=schema(1,true); byte[] valid=DistributedBucket.empty(schema).put(List.of(new byte[] {1}),new byte[] {2})
                .put(List.of(new byte[] {3}),new byte[] {4}).encode();
        for (int length=1;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,truncated)); }
        for (int count:new int[] {-1,DistributedBucket.MAX_MEMBERS+1,Integer.MAX_VALUE}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(count); assertEquals(IllegalArgumentException.class,assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,bad)).getClass());
        }
        for (int size:new int[] {-1,0,Integer.MAX_VALUE}) {
            byte[] bad=valid.clone(); ByteBuffer.wrap(bad).putInt(4,size); assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,bad));
        }
        byte[] tail=valid.clone(); ByteBuffer.wrap(tail).putChar(8,(char)65535); assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,tail));
        byte[] duplicate=valid.clone(); duplicate[18]=duplicate[10]; assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,duplicate));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,Arrays.copyOf(valid,valid.length+1)));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,new byte[5]));
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema(1,false),valid));
        assertThrows(DistributedBucket.LimitExceededException.class,() -> DistributedBucket.decode(schema,new byte[DistributedBucket.MAX_BYTES+1]));
        var two=schema(2,true); byte[] missingSecond=ByteBuffer.allocate(10).putInt(1).putInt(2).putChar((char)0).array();
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(two,missingSecond));
    }

    @Test void policyRefusalsRemainDistinctFromStructurallyMalformedStoredData() {
        assertEquals(DistributedCommandCodec.MAX_ENCODED_BYTES-4,DistributedBucket.MAX_BYTES);
        var schema=schema(1,true);
        byte[] oversized=ByteBuffer.allocate(DistributedBucket.MAX_BYTES+1).putInt(1)
                .putInt(DistributedBucket.MAX_BYTES-7).putChar((char)0).array();
        assertThrows(DistributedBucket.LimitExceededException.class,() -> DistributedBucket.decode(schema,oversized));
        int count=DistributedBucket.MAX_MEMBERS+1;
        byte[] structurallyPossible=ByteBuffer.allocate(4+count*6).putInt(count).array();
        assertThrows(DistributedBucket.LimitExceededException.class,() -> DistributedBucket.decode(schema,structurallyPossible));
        byte[] impossible=ByteBuffer.allocate(4).putInt(count).array();
        assertEquals(IllegalArgumentException.class,assertThrows(IllegalArgumentException.class,() -> DistributedBucket.decode(schema,impossible)).getClass());
        assertThrows(IllegalArgumentException.class,() -> DistributedBucket.empty(schema(257,true)));
        assertThrows(DistributedBucket.LimitExceededException.class,() -> new DistributedBucket.Member(Collections.nCopies(257,new byte[0]),null));
    }

    @Test void exactByteAndMemberBudgetsAreBoundedWithoutQuadraticDuplicateScanning() {
        var schema=schema(1,true); byte[] value=new byte[DistributedBucket.MAX_BYTES-10];
        var maximum=DistributedBucket.empty(schema).put(List.of(new byte[0]),value); assertEquals(DistributedBucket.MAX_BYTES,maximum.encode().length);
        assertArrayEquals(maximum.encode(),DistributedBucket.decode(schema,maximum.encode()).encode());
        assertThrows(DistributedBucket.LimitExceededException.class,() -> maximum.put(List.of(new byte[] {1}),null));
        assertThrows(DistributedBucket.LimitExceededException.class,() -> DistributedBucket.empty(schema).put(List.of(new byte[0]),new byte[DistributedBucket.MAX_BYTES]));
        var keyOnly=schema(1,false); var output=ByteBuffer.allocate(4+DistributedBucket.MAX_MEMBERS*8).putInt(DistributedBucket.MAX_MEMBERS);
        for (int i=0;i<DistributedBucket.MAX_MEMBERS;i++) output.putInt(4).putChar((char)2).putShort((short)i);
        byte[] full=output.array(); var members=DistributedBucket.decode(keyOnly,full); assertEquals(DistributedBucket.MAX_MEMBERS,members.members().size()); assertArrayEquals(full,members.encode());
        assertThrows(DistributedBucket.LimitExceededException.class,() -> members.put(List.of(new byte[] {0,0,0}),null));
        assertArrayEquals(new byte[0],members.get(List.of(new byte[] {(byte)0xff,(byte)0xff})));
    }
}
