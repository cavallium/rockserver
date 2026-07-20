package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBMetadata;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RocksDBMetadataTest {

    @Test
    void rocksDbVersionHashLoadsFromBuildProperties() {
        var versionHash = RocksDBMetadata.getRocksDBVersionHash();

        assertNotNull(versionHash);
        assertFalse(versionHash.isBlank(), "RocksDB version hash must be populated from build metadata");
    }
}
