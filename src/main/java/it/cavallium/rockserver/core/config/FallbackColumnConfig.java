package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface FallbackColumnConfig {

	ColumnLevelConfig[] levels() throws GestaltException;

	@Nullable
	DataSize memtableMemoryBudgetBytes() throws GestaltException;

	@Nullable
	Boolean cacheIndexAndFilterBlocks() throws GestaltException;

	@Nullable
	Boolean partitionFilters() throws GestaltException;

	@Nullable
	BloomFilterConfig bloomFilter() throws GestaltException;

	@Nullable
	DataSize blockSize() throws GestaltException;

	@Nullable
	DataSize writeBufferSize() throws GestaltException;

}
