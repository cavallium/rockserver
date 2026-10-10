package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class BucketMergeOperandTest {
    @Test void orderedMemberOperandsAreCanonicalImmutableAndDistinctFromStoredValues() {
        byte[] key={1,2},value={3,4,5}; var members=new ArrayList<>(List.of(key));
        var operand=new BucketMergeOperand(BucketMergeOperand.Type.MERGE,members,value); byte[] encoded=operand.encode(); key[0]=9; value[0]=9; members.clear(); operand.memberKeys().getFirst()[0]=9; operand.value()[0]=9;
        assertArrayEquals(encoded,operand.encode()); assertEquals(operand,BucketMergeOperand.decode(encoded)); assertThrows(UnsupportedOperationException.class,() -> operand.memberKeys().clear());
        for(var type:BucketMergeOperand.Type.values()) { var item=new BucketMergeOperand(type,List.of(new byte[0],new byte[]{7}),type==BucketMergeOperand.Type.DELETE ? null : new byte[0]); assertEquals(item,BucketMergeOperand.decode(item.encode())); }
        assertThrows(IllegalArgumentException.class,() -> new BucketMergeOperand(BucketMergeOperand.Type.PUT,List.of(new byte[]{1}),null));
        assertThrows(IllegalArgumentException.class,() -> new BucketMergeOperand(BucketMergeOperand.Type.DELETE,List.of(new byte[]{1}),new byte[0]));
        assertThrows(IllegalArgumentException.class,() -> BucketMergeOperand.decode(new byte[]{0,0,0,0}));
    }
    @Test void exactByteBoundsAndMalformedMemberPayloadsRejectBeforeNativeUse() {
        int payload=BucketMergeOperand.MAX_BYTES-15; var maximal=new BucketMergeOperand(BucketMergeOperand.Type.MERGE,List.of(new byte[0]),new byte[payload]); assertEquals(BucketMergeOperand.MAX_BYTES,maximal.encode().length);
        assertEquals(maximal,BucketMergeOperand.decode(maximal.encode())); assertThrows(IllegalArgumentException.class,() -> new BucketMergeOperand(BucketMergeOperand.Type.MERGE,List.of(new byte[0]),new byte[payload+1]));
        byte[] valid=BucketMergeOperand.encode(BucketMergeOperand.Type.MERGE,List.of(new byte[]{1,2}),new byte[]{3});
        for(int length=0;length<valid.length;length++) { byte[] truncated=Arrays.copyOf(valid,length); assertThrows(IllegalArgumentException.class,() -> BucketMergeOperand.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> BucketMergeOperand.decode(Arrays.copyOf(valid,valid.length+1)));
        for(int offset:new int[]{0,5,13}) { byte[] altered=valid.clone(); ByteBuffer.wrap(altered).putInt(offset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> BucketMergeOperand.decode(altered)); }
        byte[] type=valid.clone(); type[4]=9; assertThrows(IllegalArgumentException.class,() -> BucketMergeOperand.decode(type));
        assertThrows(IllegalArgumentException.class,() -> new BucketMergeOperand(BucketMergeOperand.Type.PUT,List.of(new byte[65536]),new byte[0]));
    }
}
