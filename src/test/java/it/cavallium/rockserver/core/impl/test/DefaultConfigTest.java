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
		Assertions.assertEquals(Path.of("./volume"), def.global().volumes()[0].volumePath());
		Assertions.assertEquals(new DataSize("32KiB"), def.global().fallbackColumnOptions().levels()[6].maxDictBytes());
	}
}