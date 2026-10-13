package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisNativePoolSpecTest {
    @Test void frozenEffectiveConfigurationAndResourcesRoundtripCanonically()throws Exception {
        var spec=RatisPoolFixtures.spec(new UUID(993,1),true);var encoded=spec.encode();var decoded=RatisNativePoolSpec.decode(encoded);assertTrue(decoded.config().global().spinning());assertArrayEquals(encoded,decoded.encode());var config=decoded.configuration();Arrays.fill(config,(byte)0);assertTrue(decoded.config().global().spinning());assertEquals(spec.resources(),decoded.resources());assertEquals(spec.pins(),decoded.pins());
    }
    @Test void oversizedConfigurationLengthAndTrailingBytesRejectBeforeAllocation() {
        var encoded=RatisPoolFixtures.spec(new UUID(993,2),false).encode();var oversized=encoded.clone();ByteBuffer.wrap(oversized).putInt(16,Integer.MAX_VALUE);assertThrows(IllegalArgumentException.class,()->RatisNativePoolSpec.decode(oversized));assertThrows(IllegalArgumentException.class,()->RatisNativePoolSpec.decode(Arrays.copyOf(encoded,encoded.length+1)));
    }
    @Test void nodePerPoolCeilingRemainsAuthoritative(){var spec=RatisPoolFixtures.spec(new UUID(993,3),false);var ceiling=new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(1024,2),new RatisNodeResourceBudget.Lane(512,1));assertEquals(ceiling,RatisNodeRuntime.poolResources(spec.resources(),ceiling));}

}
