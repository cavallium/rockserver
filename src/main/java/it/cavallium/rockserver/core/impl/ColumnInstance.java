package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toCharExact;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.buffer.Buf;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.rocksdb.ColumnFamilyHandle;

public record ColumnInstance(ColumnFamilyHandle cfh, ColumnSchema schema, int finalKeySizeBytes) implements AutoCloseable {

	private static final Buf[] EMPTY_BUF_ARRAY = new Buf[0];

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
	public Buf calculateKey(Buf[] keys) {
		validateKeyCount(keys);
		Buf finalKey;
		if (keys.length == 0) {
			finalKey = emptyBuf();
		} else if(keys.length == 1 && !hasBuckets()) {
			finalKey = keys[0];
		} else {
			finalKey = Buf.createZeroes(finalKeySizeBytes);
			int offsetBytes = 0;
			for (int i = 0; i < schema.keysCount(); i++) {
				var computedKeyAtI = computeKeyAt(i, keys);
				var computedKeyAtISize = computedKeyAtI.size();
				finalKey.setBytesFromBuf(offsetBytes, computedKeyAtI, 0, computedKeyAtISize);
				offsetBytes += computedKeyAtISize;
			}
		}
		validateFinalKeySize(finalKey);
		return finalKey;
	}

	/**
	 * @param bucketValue pass this parameter only if the columnInstance has variable-length keys
	 */
	@NotNull
	public Buf[] decodeKeys(Buf calculatedKey, @Nullable Buf bucketValue) {
		validateFinalKeySize(calculatedKey);
		Buf[] finalKeys;
		if (calculatedKey.isEmpty()) {
			finalKeys = EMPTY_BUF_ARRAY;
		} else if (!hasBuckets()) {
			if (schema.keysCount() == 1) {
				finalKeys = new Buf[] {calculatedKey};
			} else {
				finalKeys = new Buf[schema.keysCount()];
				int offsetBytes = 0;
				for (int i = 0; i < schema.keysCount(); i++) {
					var keyLength = schema.key(i);
					var finalKey = finalKeys[i] = Buf.createZeroes(keyLength);
					finalKey.setBytesFromBuf(0, calculatedKey, offsetBytes, keyLength);
					offsetBytes += keyLength;
				}
			}
		} else {
			// todo: implement
			throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, "Unsupported bucket columns, implement them");
		}
		validateKeyCount(finalKeys);
		return finalKeys;
	}

	private Buf computeKeyAt(int i, Buf[] keys) {
		if (i < schema.keysCount() - schema.variableLengthKeysCount()) {
			if (keys[i].size() != schema.key(i)) {
				throw RocksDBException.of(RocksDBErrorType.KEY_LENGTH_MISMATCH,
						"Key at index " + i + " has a different length than expected! Expected: " + schema.key(i)
								+ ", received: " + keys[i].size());
			}
			return keys[i];
		} else {
			var tailKey = schema.variableTailKey(i);
			var hashResult = Buf.createZeroes(tailKey.bytesSize());
			tailKey.hash(keys[i], hashResult);
			return hashResult;
		}
	}

	private void validateFinalKeySize(Buf key) {
		if (finalKeySizeBytes != key.size()) {
			throw RocksDBException.of(RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH,
					"Keys size must be equal to the column keys size. Expected: "
							+ finalKeySizeBytes + ", got: " + key.size());
		}
	}

	private void validateKeyCount(Buf[] keys) {
		if (schema.keysCount() != keys.length) {
			throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH,
					"Keys count must be equal to the column keys count. Expected: " + schema.keysCount()
							+ ", got: " + keys.length);
		}
	}

	public Buf computeBucketElementKey(Buf[] variableKeys) {
		int totalSize = 0;
		assert variableKeys.length == schema.variableLengthKeysCount();
		for (Buf variableKey : variableKeys) {
			totalSize += Character.BYTES + variableKey.size();
		}
		var bucketElementKey = Buf.createZeroes(totalSize);
		int offset = 0;
		for (Buf keyI : variableKeys) {
			var keyISize = keyI.size();
			bucketElementKey.setChar(offset, toCharExact(keyISize));
			offset += Character.BYTES;
			bucketElementKey.setBytesFromBuf(offset, keyI, 0, keyISize);
			offset += keyISize;
		}
		assert offset == totalSize;
		return bucketElementKey;
	}

	public Buf computeBucketElementValue(@Nullable Buf value) {
		checkNullableValue(value);
		if (value != null) {
			return value;
		} else {
			return Utils.emptyBuf();
		}
	}

	public void checkNullableValue(Buf value) {
		if (schema.hasValue() == (value == null || value.isEmpty())) {
			if (schema.hasValue()) {
				throw RocksDBException.of(RocksDBErrorType.UNEXPECTED_NULL_VALUE,
						"Schema expects a value, but a null value has been passed");
			} else {
				throw RocksDBException.of(RocksDBErrorType.VALUE_MUST_BE_NULL,
						"Schema expects no value, but a non-null value has been passed");
			}
		}
	}

	public Buf computeBucketElementKeyValue(Buf computedBucketElementKey,
			@Nullable Buf computedBucketElementValue) {
		checkNullableValue(computedBucketElementValue);
		var keySize = computedBucketElementKey.size();
		var valueSize = computedBucketElementValue != null ? computedBucketElementValue.size() : 0;
		var totalSize = keySize + valueSize;
		var computedBucketElementKV = Buf.createZeroes(totalSize);
		computedBucketElementKV.setBytesFromBuf(0, computedBucketElementKey, 0, keySize);
		if (computedBucketElementValue != null) {
			computedBucketElementKV.setBytesFromBuf(keySize, computedBucketElementValue, 0, valueSize);
		}
		return computedBucketElementKV;
	}

	/**
	 * Get only the variable-length keys
	 */
	public Buf[] getBucketElementKeys(Buf[] keys) {
		assert keys.length == schema.keysCount();
		return Arrays.copyOfRange(keys,
				schema.keysCount() - schema.variableLengthKeysCount(),
				schema.keysCount());
	}
}
