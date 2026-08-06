package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.nio.file.Files;
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
    void boundsArchivedWalByDefault() throws Exception {
        assertEquals(102_400L,
                RocksDBLoader.resolveWalSizeLimitMb(ConfigParser.parseDefault().global()));
    }

    @Test
    void typedWalConfigurationReachesNativeOptionsAndStartupDiagnostics(@TempDir Path tempDir)
            throws Exception {
        var logger = mock(Logger.class);
        var registry = new SimpleMeterRegistry();
        try {
            var loaded = RocksDBLoader.load(tempDir.resolve("wal-config-db"),
                    ConfigParser.parseDefault(), logger, registry, "wal-config-test");
            try {
                assertEquals(4L * 1024 * 1024 * 1024, loaded.dbOptions().maxTotalWalSize());
                assertEquals(86_400L, loaded.dbOptions().walTtlSeconds());
                assertEquals(102_400L, loaded.dbOptions().walSizeLimitMB());
                assertEquals(0.0d, registry.get("rocksdb.startup.native.open.active")
                        .tag("database", "wal-config-test").gauge().value());
                assertEquals(1.0d, registry.get("rocksdb.startup.wal.observed.progress.ratio")
                        .tag("database", "wal-config-test").gauge().value());
                assertEquals(1.0d, registry.get("rocksdb.startup.native.open.completions")
                        .tag("database", "wal-config-test")
                        .tag("outcome", "success")
                        .counter()
                        .count());
                verify(logger).info(startsWith("Starting RocksDB native open:"), any(Object[].class));
                verify(logger).info(startsWith("Completed RocksDB native open:"), any(Object[].class));
                verify(logger).info("Opened RocksDB with effective WAL configuration: "
                        + "max-total-wal-size=4GiB (4294967296 bytes), wal-ttl-seconds=86400, "
                        + "wal-size-limit-mb=102400");
            } finally {
                loaded.db().close();
                loaded.refs().close();
            }
        } finally {
            registry.close();
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

    @Test
    void archiveSizeLimitAllowsZeroButRejectsNegativeValues(@TempDir Path tempDir) throws Exception {
        Path disabled = Files.writeString(tempDir.resolve("disabled.conf"),
                "database.global.wal-size-limit-mb = 0\n");
        assertEquals(0L,
                RocksDBLoader.resolveWalSizeLimitMb(ConfigParser.parse(disabled).global()));

        Path invalid = Files.writeString(tempDir.resolve("invalid.conf"),
                "database.global.wal-size-limit-mb = -1\n");
        var invalidGlobal = ConfigParser.parse(invalid).global();
        assertThrows(RocksDBException.class,
                () -> RocksDBLoader.resolveWalSizeLimitMb(invalidGlobal));
    }
}
