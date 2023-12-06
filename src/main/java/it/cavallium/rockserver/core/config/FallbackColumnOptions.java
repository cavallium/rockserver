package it.cavallium.rockserver.core.config;

public interface FallbackColumnOptions {

	DatabaseLevel[] levels();

	DataSize memtableMemoryBudgetBytes();

	boolean cacheIndexAndFilterBlocks();

	boolean partitionFilters();

	BloomFilterConfig bloomFilter();

	DataSize blockSize();

	DataSize writeBufferSize();

}
