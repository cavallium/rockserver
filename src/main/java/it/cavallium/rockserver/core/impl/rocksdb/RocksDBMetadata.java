package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.IOException;
import java.util.Properties;

public class RocksDBMetadata {

	private static final String BUILD_PROPERTIES_RESOURCE = "it/cavallium/rockserver/core/resources/build.properties";
	private static final String ROCKSDB_VERSION_PROPERTY = "rockserver.core.rocksdb.version";
	private static Exception LOAD_EXCEPTION = null;
	private static String VERSION_HASH = null;

	static {
		try {
			VERSION_HASH = loadVersionHash();
		} catch (IOException | IllegalStateException e) {
			LOAD_EXCEPTION = e;
		}
	}

	public static String getRocksDBVersionHash() {
		if (LOAD_EXCEPTION != null) {
			throw new IllegalStateException("Unable to load RocksDB build metadata", LOAD_EXCEPTION);
		}
		return VERSION_HASH;
	}

	private static String loadVersionHash() throws IOException {
		try (var input = RocksDBMetadata.class.getClassLoader().getResourceAsStream(BUILD_PROPERTIES_RESOURCE)) {
			if (input == null) {
				throw new IllegalStateException("Missing build metadata resource: " + BUILD_PROPERTIES_RESOURCE);
			}
			var props = new Properties();
			props.load(input);
			var versionHash = props.getProperty(ROCKSDB_VERSION_PROPERTY);
			if (versionHash == null || versionHash.isBlank()) {
				throw new IllegalStateException("Missing build metadata property: " + ROCKSDB_VERSION_PROPERTY);
			}
			return versionHash;
		}
	}
}
