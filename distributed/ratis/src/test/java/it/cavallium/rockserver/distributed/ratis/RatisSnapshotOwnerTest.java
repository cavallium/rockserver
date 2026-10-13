package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSnapshotOwnerTest {
    final UUID group=new UUID(0,1),incarnation=new UUID(0,2);
    RatisSnapshotOwner owner() { return new RatisSnapshotOwner(group,1,new byte[32],incarnation,"node-1",null); }
    @Test void currentOwnerCodecOwnsSchemaAndBindsFullServingIdentity() {
        byte[] schema=new byte[32]; var owner=new RatisSnapshotOwner(group,1,schema,incarnation,"node-1",null); schema[0]=1; owner.schemaDigest()[0]=2;
        assertEquals(owner(),owner); assertEquals(owner().hashCode(),owner.hashCode()); byte[] bytes=owner.encode(); assertEquals(81+"node-1".length(),bytes.length);
        var decoded=RatisSnapshotOwner.decode(bytes); Arrays.fill(bytes,(byte)0); assertEquals(owner,decoded); assertEquals(owner.hashCode(),decoded.hashCode());
        assertNotEquals(owner,new RatisSnapshotOwner(new UUID(0,3),1,new byte[32],incarnation,"node-1",null));
        assertNotEquals(owner,new RatisSnapshotOwner(group,2,new byte[32],incarnation,"node-1",null));
        byte[] changed=new byte[32]; changed[0]=1; assertNotEquals(owner,new RatisSnapshotOwner(group,1,changed,incarnation,"node-1",null));
        assertNotEquals(owner,new RatisSnapshotOwner(group,1,new byte[32],new UUID(0,3),"node-1",null));
        assertNotEquals(owner,new RatisSnapshotOwner(group,1,new byte[32],incarnation,"node-2",null));
    }
    @Test void peerLengthAndAsciiBoundsAreCanonicalWithoutVersionReaders() {
        var minimum=new RatisSnapshotOwner(group,0,new byte[32],incarnation,"n",null); assertEquals(RatisSnapshotOwner.HEADER_BYTES+2,minimum.encode().length); assertEquals(minimum,RatisSnapshotOwner.decode(minimum.encode()));
        var maximum=new RatisSnapshotOwner(group,1,new byte[32],incarnation,"n".repeat(128),null); assertEquals(RatisSnapshotOwner.HEADER_BYTES+129,maximum.encode().length); assertEquals(maximum,RatisSnapshotOwner.decode(maximum.encode()));
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,-1,new byte[32],incarnation,"n",null));
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,1,new byte[31],incarnation,"n",null));
        for (var peer:List.of("","n".repeat(129),"n-é")) assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotOwner(group,1,new byte[32],incarnation,peer,null));
    }
    @Test void malformedTruncatedAndNonAsciiOwnerFramesAreRejected() {
        byte[] bytes=owner().encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(Arrays.copyOf(bytes,bytes.length+1)));
        for (int size:new int[]{-1,0,129,Integer.MAX_VALUE}) { byte[] length=bytes.clone(); ByteBuffer.wrap(length).putInt(72,size); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(length)); }
        byte[] ascii=bytes.clone(); ascii[76]=(byte)255; assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(ascii));
        byte[] epoch=bytes.clone(); ByteBuffer.wrap(epoch).putLong(16,-1); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotOwner.decode(epoch));
    }
    @Test void supportedCapabilitiesBindActualProcessPoolAndFinitePressureBudget() {
        var caps=new RatisSnapshotCohortRpc.Capabilities(group,incarnation,new UUID(1,1),new UUID(1,2),4,0,128L*1024*1024,0,16,0,16L*1024*1024,0,60000,1,0,1L<<30,0,60000,1024,1L<<30,0);
        var owner=new RatisSnapshotOwner(group,1,new byte[32],incarnation,"n".repeat(128),caps);assertEquals(RatisSnapshotOwner.MAX_BYTES,owner.encode().length);assertEquals(owner,RatisSnapshotOwner.decode(owner.encode()));assertNotEquals(owner,owner());
        byte[] shortCaps=Arrays.copyOf(RatisSnapshotCohortRpc.capabilities(caps),RatisSnapshotCohortRpc.MAX_CAPABILITIES_BYTES-1);assertThrows(IllegalArgumentException.class,()->RatisSnapshotCohortRpc.capabilities(shortCaps));
    }

}
