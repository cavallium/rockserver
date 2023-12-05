package it.cavallium.rockserver.core.config;

public interface BloomFilterConfig {

	int bitsPerKey();

	boolean optimizeForHits();

	static String stringify(BloomFilterConfig o) {
		return """
        {
                "bits-per-key": %d,
                "optimize-for-hits": %b
              }\
        """.formatted(o.bitsPerKey(), o.optimizeForHits());
	}
}
