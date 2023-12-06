package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.toCharExact;
import static java.lang.Math.toIntExact;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout.OfByte;
import java.lang.foreign.ValueLayout.OfChar;
import java.lang.foreign.ValueLayout.OfInt;
import java.lang.foreign.ValueLayout.OfShort;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;

public record ColumnInstance(ColumnFamilyHandle cfh, ColumnSchema schema, int finalKeySizeBytes) implements AutoCloseable {

	public static final OfByte BIG_ENDIAN_BYTES = OfByte.JAVA_BYTE.withOrder(ByteOrder.BIG_ENDIAN);

	public static final OfInt BIG_ENDIAN_INT = OfByte.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN);

	public static final OfShort BIG_ENDIAN_SHORT = OfByte.JAVA_SHORT.withOrder(ByteOrder.BIG_ENDIAN);

	public static final OfShort BIG_ENDIAN_SHORT_UNALIGNED = OfByte.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

	public static final OfChar BIG_ENDIAN_CHAR = OfByte.JAVA_CHAR.withOrder(ByteOrder.BIG_ENDIAN);

	public static final OfChar BIG_ENDIAN_CHAR_UNALIGNED = OfByte.JAVA_CHAR_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
	public static final OfInt BIG_ENDIAN_INT_UNALIGNED = OfByte.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

	public ColumnInstance(ColumnFamilyHandle cfh, ColumnSchema schema) {
		this(cfh, schema, calculateFinalKeySizeBytes(schema));
	}

	private static int calculateFinalKeySizeBytes(ColumnSchema schema) {
		int total = 0;
		for (int i : schema.keys()) {
			total += i;
		}
		return total;
	}

	@Override
	public void close() {
		cfh.close();
	}

	public boolean requiresWriteTransaction() {
		return schema.variableLengthKeysCount() > 0;
	}

	public boolean hasBuckets() {
		return schema.variableLengthKeysCount() > 0;
	}

	@NotNull
	public MemorySegment calculateKey(Arena arena, MemorySegment[] keys) {
		validateKeyCount(keys);
		MemorySegment finalKey;
		if (keys.length == 0) {
			finalKey = MemorySegment.NULL;
		} else if(keys.length == 1 && !hasBuckets()) {
			finalKey = keys[0];
		} else {
			finalKey = arena.allocate(finalKeySizeBytes);
			long offsetBytes = 0;
			for (int i = 0; i < schema.keys().length; i++) {
				var computedKeyAtI = computeKeyAt(arena, i, keys);
				var computedKeyAtISize = computedKeyAtI.byteSize();
				MemorySegment.copy(computedKeyAtI, 0, finalKey, offsetBytes, computedKeyAtISize);
				offsetBytes += computedKeyAtISize;
			}
		}
		validateFinalKeySize(finalKey);
		return finalKey;
	}

	private MemorySegment computeKeyAt(Arena arena, int i, MemorySegment[] keys) {
		if (i < schema.keys().length - schema.variableLengthKeysCount()) {
			if (keys[i].byteSize() != schema.keys()[i]) {
				throw new RocksDBException(RocksDBErrorType.KEY_LENGTH_MISMATCH,
						"Key at index " + i + " has a different length than expected! Expected: " + schema.keys()[i]
								+ ", received: " + keys[i].byteSize());
			}
			return keys[i];
		} else {
			if (schema.keys()[i] != Integer.BYTES) {
				throw new RocksDBException(RocksDBErrorType.UNSUPPORTED_HASH_SIZE,
						"Hash size different than 32-bit is currently unsupported");
			} else {
				return XXHash32.getInstance().hash(arena, keys[i], 0, 0, 0);
			}
		}
	}

	private void validateFinalKeySize(MemorySegment key) {
		if (finalKeySizeBytes != key.byteSize()) {
			throw new RocksDBException(RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH,
					"Keys size must be equal to the column keys size. Expected: "
							+ finalKeySizeBytes + ", got: " + key.byteSize());
		}
	}

	private void validateKeyCount(MemorySegment[] keys) {
		if (schema.keys().length != keys.length) {
			throw new RocksDBException(RocksDBErrorType.KEYS_COUNT_MISMATCH,
					"Keys count must be equal to the column keys count. Expected: " + schema.keys().length
							+ ", got: " + keys.length);
		}
	}

	public MemorySegment computeBucketElementKey(Arena arena, MemorySegment[] variableKeys) {
		long totalSize = 0L;
		assert variableKeys.length == schema.variableLengthKeysCount();
		for (MemorySegment variableKey : variableKeys) {
			totalSize += Character.BYTES + variableKey.byteSize();
		}
		MemorySegment bucketElementKey = arena.allocate(totalSize);
		long offset = 0;
		for (MemorySegment keyI : variableKeys) {
			var keyISize = keyI.byteSize();
			bucketElementKey.set(BIG_ENDIAN_CHAR_UNALIGNED, offset, toCharExact(keyISize));
			offset += Character.BYTES;
			MemorySegment.copy(keyI, 0, bucketElementKey, offset, keyISize);
			offset += keyISize;
		}
		assert offset == totalSize;
		return bucketElementKey;
	}

	public MemorySegment computeBucketElementValue(@Nullable MemorySegment value) {
		checkNullableValue(value);
		if (value != null) {
			return value;
		} else {
			return MemorySegment.NULL;
		}
	}

	public void checkNullableValue(MemorySegment value) {
		if (schema.hasValue() == (value == null)) {
			if (schema.hasValue()) {
				throw new RocksDBException(RocksDBErrorType.UNEXPECTED_NULL_VALUE,
						"Schema expects a value, but a null value has been passed");
			} else {
				throw new RocksDBException(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"Schema expects no value, but a non-null value has been passed");
			}
		}
	}

	public MemorySegment computeBucketElementKeyValue(Arena arena, MemorySegment computedBucketElementKey,
			@Nullable MemorySegment computedBucketElementValue) {
		checkNullableValue(computedBucketElementValue);
		var keySize = computedBucketElementKey.byteSize();
		var valueSize = computedBucketElementValue != null ? computedBucketElementValue.byteSize() : 0;
		var totalSize = Integer.BYTES + keySize + valueSize;
		var computedBucketElementKV = arena.allocate(totalSize);
		computedBucketElementKV.set(BIG_ENDIAN_INT, 0, toIntExact(totalSize));
		MemorySegment.copy(computedBucketElementKey, 0, computedBucketElementKV, Integer.BYTES, keySize);
		if (computedBucketElementValue != null) {
			MemorySegment.copy(computedBucketElementValue, 0, computedBucketElementKV, Integer.BYTES + keySize, valueSize);
		}
		return computedBucketElementKV;
	}

	/**
	 * Get only the variable-length keys
	 */
	public MemorySegment[] getBucketElementKeys(MemorySegment[] keys) {
		assert keys.length == schema.keys().length;
		return Arrays.copyOfRange(keys,
				schema.keys().length - schema.variableLengthKeysCount(),
				schema.keys().length);
	}
}
