package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint;
import it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Cursor;
import it.cavallium.rockserver.core.common.distributed.DistributedCdcCheckpoint.Lineage;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCdcCheckpointTest {
    private static Lineage lineage(long id,long epoch) { return new Lineage(new UUID(0,id),new UUID(1,id),epoch); }
    private static Cursor cursor(long term,long index,int ordinal) { return new Cursor(new ApplyPosition(term,index),ordinal); }

    @Test void beginningAndSnapshotCutDoNotInventAnEvent() {
        var lineage=lineage(1,1);
        var initial=new DistributedCdcCheckpoint(Map.of(lineage,Cursor.beginning()));
        assertEquals(initial,DistributedCdcCheckpoint.decode(initial.encode()));
        var first=initial.advance(lineage,cursor(1,0,0));
        assertThrows(IllegalArgumentException.class,()->first.advance(lineage,Cursor.beginning()));
        var snapshot=first.advance(lineage,Cursor.after(new ApplyPosition(2,10)));
        assertThrows(IllegalArgumentException.class,()->snapshot.advance(lineage,cursor(2,10,0)));
        assertEquals(cursor(2,11,0),snapshot.advance(lineage,cursor(2,11,0)).cursors().get(lineage));
        assertThrows(IllegalArgumentException.class,()->new Cursor(new ApplyPosition(0,1),-1));
    }

    @Test void independentRangesKeepIndependentProgressAndExactEntryIdentity() {
        var a=lineage(1,3); var b=lineage(2,4);
        var checkpoint=new DistributedCdcCheckpoint(Map.of(a,cursor(2,100,0),b,cursor(8,5,1)));
        var advanced=checkpoint.advance(b,cursor(9,6,0));
        assertEquals(cursor(2,100,0),advanced.cursors().get(a));
        assertEquals(cursor(9,6,0),advanced.cursors().get(b));
        assertEquals(cursor(8,5,1),checkpoint.cursors().get(b));
        assertSame(advanced,advanced.advance(b,cursor(9,6,0)));
        assertThrows(IllegalArgumentException.class,()->advanced.advance(a,cursor(2,99,0)));
        assertThrows(IllegalArgumentException.class,()->advanced.advance(a,cursor(3,100,0)));
        assertThrows(IllegalArgumentException.class,()->advanced.advance(b,cursor(8,7,0)));
        assertThrows(IllegalArgumentException.class,()->advanced.advance(b,cursor(10,5,0)));
    }

    @Test void restartCheckpointPreservesOrdinalAndCannotSubstituteSuccessorEpoch() {
        var original=lineage(1,3); var source=new HashMap<>(Map.of(original,cursor(7,40,2)));
        var checkpoint=new DistributedCdcCheckpoint(source); source.clear();
        var resumed=DistributedCdcCheckpoint.decode(checkpoint.encode());
        assertEquals(checkpoint,resumed);
        assertEquals(cursor(7,40,3),resumed.advance(original,cursor(7,40,3)).cursors().get(original));
        assertThrows(IllegalArgumentException.class,()->resumed.advance(original,cursor(7,40,1)));
        assertThrows(IllegalArgumentException.class,()->resumed.advance(lineage(1,4),cursor(7,40,3)));
        assertThrows(UnsupportedOperationException.class,()->resumed.cursors().clear());
    }

    @Test void canonicalBoundedEncodingRejectsDuplicateReorderedAndMalformedMaps() {
        var a=lineage(1,1); var b=lineage(2,1); var cursor=cursor(1,1,0);
        var reversed=new LinkedHashMap<Lineage,Cursor>(); reversed.put(b,cursor); reversed.put(a,cursor);
        byte[] encoded=new DistributedCdcCheckpoint(reversed).encode();
        assertArrayEquals(new DistributedCdcCheckpoint(Map.of(a,cursor,b,cursor)).encode(),encoded);
        var duplicate=encoded.clone(); System.arraycopy(duplicate,4,duplicate,4+DistributedCdcCheckpoint.ENTRY_BYTES,DistributedCdcCheckpoint.ENTRY_BYTES);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcCheckpoint.decode(duplicate));
        var reordered=encoded.clone(); System.arraycopy(encoded,4,reordered,4+DistributedCdcCheckpoint.ENTRY_BYTES,DistributedCdcCheckpoint.ENTRY_BYTES); System.arraycopy(encoded,4+DistributedCdcCheckpoint.ENTRY_BYTES,reordered,4,DistributedCdcCheckpoint.ENTRY_BYTES);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcCheckpoint.decode(reordered));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcCheckpoint.decode(ByteBuffer.allocate(4).putInt(Integer.MAX_VALUE).array()));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcCheckpoint.decode(new byte[3]));
        var tooMany=new HashMap<Lineage,Cursor>(); for(int i=0;i<=DistributedCdcCheckpoint.MAX_LINEAGES;i++) tooMany.put(lineage(i,0),cursor);
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcCheckpoint(tooMany));
        tooMany.remove(lineage(0,0)); var full=new DistributedCdcCheckpoint(tooMany);
        assertEquals(DistributedCdcCheckpoint.MAX_BYTES,full.encode().length);
        assertEquals(full,DistributedCdcCheckpoint.decode(full.encode()));
    }
}
