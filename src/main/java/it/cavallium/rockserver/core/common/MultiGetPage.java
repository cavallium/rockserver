package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Ordered prefix of current values, with null slots for missing keys. Resume by
 * dropping {@code values().size()} input keys; separate requests do not share a snapshot.
 * Budgets include conservative framing costs and physical bucket bytes, so an oversized
 * bucket can exceed the budget even when the selected logical value would fit.
 */
public record MultiGetPage(List<@Nullable Buf> values, boolean hasMore) {
	public static final int MAX_KEYS = 128;
	public static final long MAX_KEY_BYTES = 2L * 1024 * 1024;
	public static final long KEY_PART_BYTES = 16;
	public static final long RESPONSE_BASE_BYTES = 16;
	public static final long RESPONSE_ITEM_BYTES = 16;

	public MultiGetPage {
		Objects.requireNonNull(values, "values");
		if (values.size() > MAX_KEYS || hasMore && values.isEmpty()) {
			throw new IllegalArgumentException("Invalid MultiGet page size or non-progressing continuation");
		}
		values = Collections.unmodifiableList(new ArrayList<>(values));
	}

	public static MultiGetPage empty() {
		return new MultiGetPage(List.of(), false);
	}

	public static void validateLimits(int keyCount, RangeBudget budget) {
		Objects.requireNonNull(budget, "budget");
		if (keyCount < 0 || keyCount > MAX_KEYS || budget.maxItems() > MAX_KEYS
				|| budget.maxBytes() > RangeBudget.DEFAULT_MAX_BYTES) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"MultiGet requires at most 128 keys/items and an 8 MiB response budget");
		}
	}

	public static void validateKeyBytes(long bytes) {
		if (bytes > MAX_KEY_BYTES) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					"MultiGet key bytes exceed 2 MiB including key part references");
		}
	}

	public static void validateRequest(List<Keys> keys, RangeBudget budget) {
		Objects.requireNonNull(keys, "keys");
		validateLimits(keys.size(), budget);
		long bytes = 0;
		for (var key : keys) {
			var parts = Objects.requireNonNull(key, "key").keys();
			bytes += KEY_PART_BYTES * parts.length;
			validateKeyBytes(bytes);
			for (var part : parts) {
				bytes += Objects.requireNonNull(part, "key part").size();
				validateKeyBytes(bytes);
			}
		}
	}

	/** Reject malformed transport responses before exposing them to a caller. */
	public void validateResponse(int requestedKeys, RangeBudget budget) {
		if (values.size() > Math.min(requestedKeys, budget.maxItems())
				|| hasMore != (values.size() < requestedKeys)) {
			throw new IllegalStateException("Invalid MultiGet response cardinality");
		}
		long bytes = RESPONSE_BASE_BYTES;
		for (var value : values) {
			bytes += RESPONSE_ITEM_BYTES + (value == null ? 0L : value.size());
		}
		if (!values.isEmpty() && bytes > budget.maxBytes()) {
			throw new IllegalStateException("MultiGet response exceeds its byte budget");
		}
	}
}
