package it.cavallium.rockserver.core.common;

public record ColumnSchema(int[] keys, int variableLengthKeysCount, boolean hasValue) {
	public ColumnSchema {
		if (variableLengthKeysCount > keys.length) {
			throw new IllegalArgumentException("variable length keys count must be less or equal keysCount");
		}
		for (int i = 0; i < keys.length - variableLengthKeysCount; i++) {
			if (keys[i] <= 0) {
				throw new UnsupportedOperationException("Key length must be > 0");
			}
		}
		for (int i = keys.length - variableLengthKeysCount; i < keys.length; i++) {
			if (keys[i] <= 1) {
				throw new UnsupportedOperationException("Key hash length must be > 1");
			}
		}
	}

	/**
	 * Keys with their length
	 * @return an array with the length of each key, variable-length keys must have the length of their hash
	 */
	@Override
	public int[] keys() {
		return keys;
	}

	/**
	 * The last n keys that are variable-length
	 */
	@Override
	public int variableLengthKeysCount() {
		return variableLengthKeysCount;
	}
}
