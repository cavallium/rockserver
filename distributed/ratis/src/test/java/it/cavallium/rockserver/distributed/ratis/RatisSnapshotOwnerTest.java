package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSnapshotOwnerTest {
    final UUID group=new UUID(0,1),incarnation=new UUID(0,2);
    RatisSnapshotOwner owner() { return new RatisSnapshotOwner(group,1,new byte[32],incarnation,"node-1"); }
    @Test void currentOwnerCodecOwnsSchemaAndBindsFullServingIdentity() {
        byte[] schema=new byte[32]; var owner=new RatisSnapshotOwner(group,1,schema,incarnation,"node-1"); schema[0]=1; owner.schemaDigest()[0]=2;
        assertEquals(owner(),owner); assertEquals(owner().hashCode(),owner.hashCode()); byte[] bytes=owner.encode(); assertEquals(76+"node-1".length(),bytes.length);
        var decoded=RatisSnapshotOwner.decode(bytes); Arrays.fill(bytes,(byte)0); assertEquals(owner,decoded); assertEquals(owner.hashCode(),decoded.hashCode());
        assertNotEquals(owner,new RatisSnapshotOwner(new UUID(0,3),1,new byte[32],incarnation,"node-1"));
        assertNotEquals(owner,new RatisSnapshotOwner(group,2,new byte[32],incarnation,"node-1"));
        byte[] changed=new byte[32]; changed[0]=1; assertNotEquals(owner,new RatisSnapshotOwner(group,1,changed,incarnation,"node-1"));
        assertNotEquals(owner,new RatisSnapshotOwner(group,1,new byte[32],new UUID(0,3),"node-1"));
        assertNotEquals(owner,new RatisSnapshotOwner(group,1,new byte[32],incarnation,"node-2"));
    }
    @Test void peerLengthAndAsciiBoundsAreCanonicalWithoutVersionReaders() {
        var minimum=new RatisSnapshotOwner(group,0,new byte[32],incarnation,"n"); assertEquals(RatisSnapshotOwner.HEADER_BYTES+1,minimum.encode().length); assertEquals(minimum,RatisSnapshotOwner.decode(minimum.encode()));
        var maximum=new RatisSnapshotOwner(group,1,new byte[32],incarnation,"n".repeat(128)); assertEquals(RatisSnapshotOwner.MAX_BYTES,maximum.encode().length); assertEquals(maximum,RatisSnapshotOwner.decode(maximum.encode()));
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,-1,new byte[32],incarnation,"n"));
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,1,new byte[31],incarnation,"n"));
        for (var peer:List.of("","n".repeat(129),"n-é")) assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,1,new byte[32],incarnation,peer));
    }
    @Test void malformedTruncatedAndNonAsciiOwnerFramesAreRejected() {
        byte[] bytes=owner().encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(Arrays.copyOf(bytes,bytes.length+1)));
        for (int size:new int[]{-1,0,129,Integer.MAX_VALUE}) { byte[] length=bytes.clone(); ByteBuffer.wrap(length).putInt(72,size); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(length)); }
        byte[] ascii=bytes.clone(); ascii[76]=(byte)255; assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(ascii));
        byte[] epoch=bytes.clone(); ByteBuffer.wrap(epoch).putLong(16,-1); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(epoch));
    }
}
