package it.cavallium.rockserver.core.resources;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.io.InputStream;
import org.jetbrains.annotations.NotNull;

public class DefaultConfig {

	@NotNull
	public static InputStream getDefaultConfig() {
		var stream = DefaultConfig.class.getResourceAsStream("default.conf");
		if (stream == null) {
			throw RocksDBException.of(RocksDBErrorType.CONFIG_ERROR, "Missing default config resource: default.conf");
		}
		return stream;
	}
}
