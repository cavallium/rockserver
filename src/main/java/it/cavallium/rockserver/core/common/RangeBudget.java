package it.cavallium.rockserver.core.common;

/** Per-request logical item and encoded key/value byte limits for a bounded range page. */
public record RangeBudget(int maxItems, long maxBytes) {

	public static final int DEFAULT_MAX_ITEMS = 4_096;
	public static final long DEFAULT_MAX_BYTES = 8L * 1024 * 1024;
	public static final RangeBudget DEFAULT = new RangeBudget(DEFAULT_MAX_ITEMS, DEFAULT_MAX_BYTES);

	public RangeBudget {
		if (maxItems <= 0) {
			throw new IllegalArgumentException("maxItems must be positive: " + maxItems);
		}
		if (maxBytes <= 0L) {
			throw new IllegalArgumentException("maxBytes must be positive: " + maxBytes);
		}
	}
}
