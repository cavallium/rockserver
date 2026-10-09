package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisColumnIdentitiesTest {
    @Test void canonicalMarkersOwnDigestAndBindDomainUuidAndPositiveApiId() {
        UUID domain=new UUID(1,2),column=new UUID(3,4); byte[] digest=new byte[32]; digest[0]=9;
        byte[] marker=RatisColumnIdentities.value(domain,column,7,digest); digest[0]=0;
        assertEquals(72,marker.length); byte[] issued=RatisColumnIdentities.requireOwned(marker,domain,column,7); assertEquals(9,issued[0]);
        issued[0]=0; assertEquals(9,RatisColumnIdentities.requireOwned(marker,domain,column,7)[0]);
        assertArrayEquals(ByteBuffer.allocate(17).put((byte)0x55).putLong(3).putLong(4).array(),RatisColumnIdentities.columnKey(column));
        assertArrayEquals(ByteBuffer.allocate(9).put((byte)0x49).putLong(7).array(),RatisColumnIdentities.apiKey(7));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.requireOwned(marker,column,column,7));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.requireOwned(marker,domain,domain,7));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.requireOwned(marker,domain,column,8));
        for (int length:new int[] {0,71,73}) assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.requireOwned(new byte[length],domain,column,7));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.apiKey(0));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.value(domain,column,-1,new byte[32]));
        assertThrows(IllegalArgumentException.class,() -> RatisColumnIdentities.value(domain,column,1,new byte[31]));
    }
}
