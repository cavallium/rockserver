package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.MemorySegment;

public record ColumnSchema(IntList keys, ObjectList<ColumnHashType> variableTailKeys, boolean hasValue) {

	public static ColumnSchema of(IntList fixedKeys, ObjectList<ColumnHashType> variableTailKeys, boolean hasValue) {
		IntList keys;
		if (!variableTailKeys.isEmpty()) {
			keys = new IntArrayList(fixedKeys.size() + variableTailKeys.size());
			keys.addAll(fixedKeys);
			for (ColumnHashType variableTailKey : variableTailKeys) {
				keys.add(variableTailKey.bytesSize());
			}
		} else {
			keys = fixedKeys;
		}
		return new ColumnSchema(keys, variableTailKeys, hasValue);
	}

	public ColumnSchema {
		final int keysSize = keys.size();
		final int variableKeysSize = variableTailKeys.size();
		if (variableKeysSize > keysSize) {
			throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH, "variable length keys count must be less or equal keysCount");
		}
		for (int i = 0; i < keysSize - variableKeysSize; i++) {
			if (keys.getInt(i) <= 0) {
				throw RocksDBException.of(RocksDBErrorType.KEY_LENGTH_MISMATCH, "Key length must be > 0");
			}
		}
		for (int i = keysSize - variableKeysSize; i < keysSize; i++) {
			var hash = variableTailKeys.get(i - (keysSize - variableKeysSize));
			var keySize = keys.getInt(i);
			if (keySize != hash.bytesSize()) {
				throw RocksDBException.of(RocksDBErrorType.KEY_HASH_SIZE_MISMATCH, "Key hash length of type " + hash + " must be " + hash.bytesSize() + ", but it's defined as " + keySize);
			}
		}
	}

	/**
	 * Keys with their length
	 * @return an array with the length of each key, variable-length keys must have the length of their hash
	 */
	@Override
	public IntList keys() {
		return keys;
	}

	/**
	 * The last n keys that are variable-length
	 */
	public int variableLengthKeysCount() {
		return variableTailKeys.size();
	}

	/**
	 * The first n keys that are fixed
	 */
	public int fixedLengthKeysCount() {
		return keys.size() - variableTailKeys.size();
	}

	/**
	 * The first n keys that are fixed
	 */
	public int keysCount() {
		return keys.size();
	}

	public int key(int i) {
		return keys.getInt(i);
	}

	/**
	 * @param absoluteIndex index from the first fixed key
	 */
	public ColumnHashType variableTailKey(int absoluteIndex) {
		return variableTailKeys.get(absoluteIndex - fixedLengthKeysCount());
	}
}
