package it.cavallium.rockserver.core.config;

public interface BloomFilterConfig {

	int bitsPerKey();

	boolean optimizeForHits();

}
