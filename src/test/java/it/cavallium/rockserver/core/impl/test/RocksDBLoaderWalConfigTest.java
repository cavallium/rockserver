package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.nio.file.Path;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;

class RocksDBLoaderWalConfigTest {

    private static final String WAL_TTL_SECONDS_PROPERTY = "it.cavallium.dbengine.wal.ttl.seconds";

    @AfterEach
    void clearOverride() {
        System.clearProperty(WAL_TTL_SECONDS_PROPERTY);
    }

    @Test
    void usesIncidentScaleDefault() throws Exception {
        assertEquals(86_400L,
                RocksDBLoader.resolveWalTtlSeconds(ConfigParser.parseDefault().global()));
    }

    @Test
    void boundsLiveWalDebtByDefault() throws Exception {
        assertEquals(4L * 1024 * 1024 * 1024,
                ConfigParser.parseDefault().global().maxTotalWalSize().longValue());
    }

    @Test
    void typedWalConfigurationReachesNativeOptionsAndStartupDiagnostics(@TempDir Path tempDir)
            throws Exception {
        var logger = mock(Logger.class);
        var loaded = RocksDBLoader.load(tempDir.resolve("wal-config-db"), ConfigParser.parseDefault(), logger);
        try {
            assertEquals(4L * 1024 * 1024 * 1024, loaded.dbOptions().maxTotalWalSize());
            assertEquals(86_400L, loaded.dbOptions().walTtlSeconds());
            verify(logger).info("Opened RocksDB with effective WAL configuration: "
                    + "max-total-wal-size=4GiB (4294967296 bytes), wal-ttl-seconds=86400");
        } finally {
            loaded.db().close();
            loaded.refs().close();
        }
    }

    @Test
    void systemPropertyOverridesConfiguredDefault() throws Exception {
        System.setProperty(WAL_TTL_SECONDS_PROPERTY, "172800");
        assertEquals(172_800L,
                RocksDBLoader.resolveWalTtlSeconds(ConfigParser.parseDefault().global()));
    }

    @Test
    void rejectsNonPositiveOrMalformedOverrides() throws Exception {
        var global = ConfigParser.parseDefault().global();
        System.setProperty(WAL_TTL_SECONDS_PROPERTY, "0");
        assertThrows(RocksDBException.class, () -> RocksDBLoader.resolveWalTtlSeconds(global));

        System.setProperty(WAL_TTL_SECONDS_PROPERTY, "not-a-number");
        assertThrows(RocksDBException.class, () -> RocksDBLoader.resolveWalTtlSeconds(global));
    }
}
