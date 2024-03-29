package it.cavallium.rockserver.core.config;

import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;

public interface GlobalDatabaseConfig {

	boolean spinning() throws GestaltException;

	boolean checksum() throws GestaltException;

	boolean useDirectIo() throws GestaltException;

	boolean allowRocksdbMemoryMapping() throws GestaltException;

	@Nullable
	Integer maximumOpenFiles() throws GestaltException;

	boolean optimistic() throws GestaltException;

	@Nullable
	DataSize blockCache() throws GestaltException;

	@Nullable
	DataSize writeBufferManager() throws GestaltException;

	@Nullable
	Path logPath() throws GestaltException;

	@Nullable
	Path walPath() throws GestaltException;

	@Nullable
	Duration delayWalFlushDuration() throws GestaltException;

	boolean absoluteConsistency() throws GestaltException;

	VolumeConfig[] volumes() throws GestaltException;

	FallbackColumnConfig fallbackColumnOptions() throws GestaltException;
	NamedColumnConfig[] columnOptions() throws GestaltException;

}
