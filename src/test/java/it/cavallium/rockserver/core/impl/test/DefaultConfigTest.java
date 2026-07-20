package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.DataSize;

import java.nio.file.Path;

import org.github.gestalt.config.exceptions.GestaltException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DefaultConfigTest {

    @Test
    public void test() throws GestaltException {
        var def = ConfigParser.parseDefault();
        var checksum = def.global().checksum();
        Assertions.assertTrue(checksum);
        var ingestBehind = def.global().ingestBehind();
        Assertions.assertFalse(ingestBehind);
        Assertions.assertEquals(Path.of("./volume"), def.global().columnOptions()[0].volumes()[0].volumePath());
        Assertions.assertEquals("10TiB", def.global().columnOptions()[0].volumes()[0].targetSize().toString());
        Assertions.assertEquals(new DataSize("32KiB"), def.global().fallbackColumnOptions().levels()[6].maxDictBytes());
        Assertions.assertFalse(def.replication().enabled());
        Assertions.assertEquals(Path.of("./replication"), def.replication().dataPath());
        Assertions.assertEquals(Path.of("./replication/metadata"), def.replication().metadataPath());
        Assertions.assertNull(def.replication().bootstrapManifestPath());
        Assertions.assertNull(def.replication().adapterHome());
        Assertions.assertNull(def.replication().adapterManifestPath());
        Assertions.assertNull(def.replication().adapterJavaExecutable());
        Assertions.assertNull(def.replication().adapterHealthEndpoint());
        Assertions.assertNull(def.replication().adapterRpcEndpoint());
        Assertions.assertEquals(0, def.replication().metadataPeers().length);
    }
}
