package it.cavallium.rockserver.distributed.ratis;

import org.junit.jupiter.api.Test;
import java.util.Collections;
import java.util.Properties;
import static org.junit.jupiter.api.Assertions.*;

/** Runtime composition only; no server startup, transport or failure probes. */
class RatisReadRuntimeTest {
    @Test void optionalRuntimeContainsExactlyOneReviewedServerImplementation() throws Exception {
        var loader = getClass().getClassLoader();
        for (String name : new String[]{"org/apache/ratis/server/impl/LeaderStateImpl.class",
                "org/apache/ratis/server/impl/ReadIndexHeartbeats.class"}) {
            assertEquals(1, Collections.list(loader.getResources(name)).size(), name);
        }
        String marker = "META-INF/rockserver-ratis-read-safety.properties";
        assertEquals(1, Collections.list(loader.getResources(marker)).size());
        var properties = new Properties();
        try (var input = loader.getResourceAsStream(marker)) { assertNotNull(input); properties.load(input); }
        assertEquals("3.3.1-rockserver.1", properties.getProperty("patch.version"));
        assertEquals("a635a8c69b5c59089b026992178ecea95ead7d709c5ff4f94834e06eaa0c911d",
                properties.getProperty("base.server.sha256"));
        var leader = Class.forName("org.apache.ratis.server.impl.LeaderStateImpl", false, loader);
        var rounds = Class.forName("org.apache.ratis.server.impl.ReadIndexHeartbeats", false, loader);
        assertEquals(leader.getProtectionDomain().getCodeSource().getLocation(),
                rounds.getProtectionDomain().getCodeSource().getLocation());
        assertEquals(rounds, leader.getDeclaredField("readIndexHeartbeats").getType());
    }
}
