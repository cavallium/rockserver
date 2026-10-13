package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.ConfigPrinter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class FrozenPoolConfigTest {
    @Test void byteSourceFreezesInputAndEffectiveConfigurationRoundtrips()throws Exception {
        byte[] bytes="database.global.spinning=true\ndatabase.global.maximum-open-files=123".getBytes(StandardCharsets.UTF_8);
        var parser=new ConfigParser();parser.addSourceBytes(bytes);java.util.Arrays.fill(bytes,(byte)' ');var config=parser.parse();
        assertTrue(config.global().spinning());assertEquals(123,config.global().maximumOpenFiles());
        var frozen=("database="+ConfigPrinter.stringify(config)).getBytes(StandardCharsets.UTF_8);var reopened=new ConfigParser();reopened.addSourceBytes(frozen);
        assertEquals(ConfigPrinter.stringify(config),ConfigPrinter.stringify(reopened.parse()));
    }
    @Test void malformedOrUnboundedSourcesRejectBeforeParsing() {
        for(byte[] bytes:new byte[][]{new byte[0],new byte[1024*1024+1],new byte[]{(byte)0xc3,(byte)0x28}})
            assertThrows(IllegalArgumentException.class,()->new ConfigParser().addSourceBytes(bytes));
        assertThrows(IllegalArgumentException.class,()->new ConfigParser().addSourceBytes((byte[])null));
    }
}
