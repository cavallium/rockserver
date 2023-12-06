package it.cavallium.rockserver.core.config;

import java.nio.file.Path;

public interface GlobalDatabaseConfig {

	boolean spinning();

	boolean checksum();

	boolean useDirectIo();

	boolean allowRocksdbMemoryMapping();

	int maximumOpenFiles();

	boolean optimistic();

	DataSize blockCache();

	DataSize writeBufferManager();

	Path logPath();

	FallbackColumnOptions fallbackColumnOptions();
	NamedColumnOptions[] columnOptions();

}
