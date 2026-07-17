package it.cavallium.rockserver.core.config;

import java.time.Duration;
import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

import java.nio.file.Path;

public interface GlobalDatabaseConfig {

	boolean followRocksdbOptimizations() throws GestaltException;

	boolean paranoidChecks() throws GestaltException;

	boolean useClockCache() throws GestaltException;

	boolean spinning() throws GestaltException;

	boolean checksum() throws GestaltException;

	boolean useDirectIo() throws GestaltException;

	boolean allowRocksdbMemoryMapping() throws GestaltException;

	boolean allowRocksdbMmapWrites() throws GestaltException;

	@Nullable Integer maximumOpenFiles() throws GestaltException;

	boolean optimistic() throws GestaltException;

	@Nullable DataSize blockCache() throws GestaltException;

	@Nullable Double blockCacheHighPriorityRatio() throws GestaltException;

	@Nullable DataSize writeBufferManager() throws GestaltException;

	@Nullable DataSize databaseWriteBufferSize() throws GestaltException;

	@Nullable Path logPath() throws GestaltException;

	@Nullable Path walPath() throws GestaltException;

	@Nullable Long walTtlSeconds() throws GestaltException;

	@Nullable Path tempSstPath() throws GestaltException;

	@Nullable Duration delayWalFlushDuration() throws GestaltException;

	boolean absoluteConsistency() throws GestaltException;

	boolean enableFastGet() throws GestaltException;

	boolean ingestBehind() throws GestaltException;

	boolean unorderedWrite() throws GestaltException;

	boolean disableAutoCompactions() throws GestaltException;

	boolean disableWriteSlowdown() throws GestaltException;

	@Nullable Integer maxSubcompactions() throws GestaltException;

	@Nullable Integer maxBackgroundJobs() throws GestaltException;

	FallbackColumnConfig fallbackColumnOptions() throws GestaltException;

	NamedColumnConfig[] columnOptions() throws GestaltException;

}
