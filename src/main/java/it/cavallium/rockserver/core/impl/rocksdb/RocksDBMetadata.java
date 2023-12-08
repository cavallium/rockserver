package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class RocksDBMetadata {

	private static Exception LOAD_EXCEPTION = null;
	private static String VERSION_HASH = null;

	static {
		try {
			try (var is = RocksDBMetadata.class.getClassLoader().getResourceAsStream("it/cavallium/rockserver/core/resources/build.properties")) {
				assert is != null;
				var props = new Properties();
				props.load(is);
				VERSION_HASH = Objects.requireNonNull(props.getProperty("rockserver.core.rocksdb.version"));
			}
		} catch (IOException | NullPointerException e) {
			LOAD_EXCEPTION = e;
		}
	}

	public static String getRocksDBVersionHash() {
		if (LOAD_EXCEPTION != null) {
			throw new IllegalStateException(LOAD_EXCEPTION);
		}
		return VERSION_HASH;
	}
}
