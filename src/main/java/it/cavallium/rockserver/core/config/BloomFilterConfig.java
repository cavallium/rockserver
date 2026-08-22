package it.cavallium.rockserver.core.config;

import org.github.gestalt.config.exceptions.GestaltException;
import org.jetbrains.annotations.Nullable;

public interface BloomFilterConfig {

	int bitsPerKey() throws GestaltException;

	@Nullable
	Boolean useRibbon() throws GestaltException;

	/** Use Bloom filters below this level, then Ribbon. Null uses RocksDB's Ribbon default. */
	@Nullable
	Integer ribbonBloomBeforeLevel() throws GestaltException;

	@Nullable
	Boolean optimizeForHits() throws GestaltException;

}
