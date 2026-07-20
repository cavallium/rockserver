package it.cavallium.rockserver.core.cluster;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toCharExact;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

import java.util.Arrays;

public final class RockserverKeyEncoder {

    private RockserverKeyEncoder() {
    }

    public static Buf calculateKey(ColumnSchema schema, Keys keys) {
        var requiredKeys = requireArgument(keys, "keys");
        return calculateKey(schema, requiredKeys.keys());
    }

    public static Buf calculateKey(ColumnSchema schema, Buf[] keys) {
        var requiredSchema = requireArgument(schema, "schema");
        var requiredKeys = requireArgument(keys, "keys");
        validateKeyCount(requiredSchema, requiredKeys);
        requireKeyEntries(requiredKeys);
        Buf finalKey;
        if (requiredKeys.length == 0) {
            finalKey = emptyBuf();
        } else if (requiredKeys.length == 1 && !hasVariableTailKeys(requiredSchema)) {
            finalKey = requiredKeys[0];
        } else {
            finalKey = Buf.createZeroes(finalKeySizeBytes(requiredSchema));
            int offsetBytes = 0;
            for (int i = 0; i < requiredSchema.keysCount(); i++) {
                var computedKey = computeKeyAt(requiredSchema, i, requiredKeys);
                var computedKeySize = computedKey.size();
                finalKey.setBytesFromBuf(offsetBytes, computedKey, 0, computedKeySize);
                offsetBytes += computedKeySize;
            }
        }
        validateFinalKeySize(requiredSchema, finalKey);
        return finalKey;
    }

    public static Buf calculateBucketElementKey(ColumnSchema schema, Keys keys) {
        var requiredKeys = requireArgument(keys, "keys");
        return calculateBucketElementKey(schema, variableTailKeys(schema, requiredKeys.keys()));
    }

    public static Buf calculateBucketElementKey(ColumnSchema schema, Buf[] variableKeys) {
        var requiredSchema = requireArgument(schema, "schema");
        var requiredVariableKeys = requireArgument(variableKeys, "variableKeys");
        if (requiredVariableKeys.length != requiredSchema.variableLengthKeysCount()) {
            throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH,
                    "Variable keys count must be equal to the column variable keys count. Expected: "
                            + requiredSchema.variableLengthKeysCount() + ", got: " + requiredVariableKeys.length);
        }

        int totalSize = 0;
        for (Buf variableKey : requiredVariableKeys) {
            var requiredVariableKey = requireArgument(variableKey, "variableKey");
            totalSize += Character.BYTES + requiredVariableKey.size();
        }

        var bucketElementKey = Buf.createZeroes(totalSize);
        int offset = 0;
        for (Buf variableKey : requiredVariableKeys) {
            var keySize = variableKey.size();
            bucketElementKey.setChar(offset, toCharExact(keySize));
            offset += Character.BYTES;
            bucketElementKey.setBytesFromBuf(offset, variableKey, 0, keySize);
            offset += keySize;
        }
        return bucketElementKey;
    }

    public static Keys decodeFixedLengthKeys(ColumnSchema schema, Buf calculatedKey) {
        var requiredSchema = requireArgument(schema, "schema");
        var requiredCalculatedKey = requireArgument(calculatedKey, "calculatedKey");
        if (hasVariableTailKeys(requiredSchema)) {
            throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
                    "replicated range reads cannot decode variable-tail keys without bucket metadata");
        }
        validateFinalKeySize(requiredSchema, requiredCalculatedKey);
        if (requiredSchema.keysCount() == 0) {
            return new Keys();
        }
        var calculatedKeyBytes = requiredCalculatedKey.toByteArray();
        if (requiredSchema.keysCount() == 1) {
            return new Keys(Buf.wrap(calculatedKeyBytes).freeze());
        }
        var decoded = new Buf[requiredSchema.keysCount()];
        int offset = 0;
        for (int i = 0; i < requiredSchema.keysCount(); i++) {
            int keySize = requiredSchema.key(i);
            decoded[i] = Buf.wrap(Arrays.copyOfRange(calculatedKeyBytes, offset, offset + keySize)).freeze();
            offset += keySize;
        }
        return new Keys(decoded);
    }

    public static Buf[] variableTailKeys(ColumnSchema schema, Buf[] keys) {
        var requiredSchema = requireArgument(schema, "schema");
        var requiredKeys = requireArgument(keys, "keys");
        validateKeyCount(requiredSchema, requiredKeys);
        return Arrays.copyOfRange(requiredKeys, requiredSchema.fixedLengthKeysCount(), requiredSchema.keysCount());
    }

    public static int compareCalculatedKeys(Buf left, Buf right) {
        var requiredLeft = requireArgument(left, "left");
        var requiredRight = requireArgument(right, "right");
        return Arrays.compareUnsigned(requiredLeft.toByteArray(), requiredRight.toByteArray());
    }

    public static int finalKeySizeBytes(ColumnSchema schema) {
        var requiredSchema = requireArgument(schema, "schema");
        int total = 0;
        for (int keySize : requiredSchema.keys()) {
            total += keySize;
        }
        return total;
    }

    public static boolean hasVariableTailKeys(ColumnSchema schema) {
        var requiredSchema = requireArgument(schema, "schema");
        return requiredSchema.variableLengthKeysCount() > 0;
    }

    private static Buf computeKeyAt(ColumnSchema schema, int index, Buf[] keys) {
        var key = requireArgument(keys[index], "key");
        if (index < schema.fixedLengthKeysCount()) {
            validateFixedKeySize(schema, index, key);
            return key;
        }
        var tailKey = schema.variableTailKey(index);
        var hashResult = Buf.createZeroes(tailKey.bytesSize());
        tailKey.hash(key, hashResult);
        return hashResult;
    }

    private static void validateFixedKeySize(ColumnSchema schema, int index, Buf key) {
        var requiredKey = requireArgument(key, "key");
        if (requiredKey.size() != schema.key(index)) {
            throw RocksDBException.of(RocksDBErrorType.KEY_LENGTH_MISMATCH,
                    "Key at index " + index + " has a different length than expected! Expected: "
                            + schema.key(index) + ", received: " + requiredKey.size());
        }
    }

    private static void validateFinalKeySize(ColumnSchema schema, Buf key) {
        var finalKeySizeBytes = finalKeySizeBytes(schema);
        if (finalKeySizeBytes != key.size()) {
            throw RocksDBException.of(RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH,
                    "Keys size must be equal to the column keys size. Expected: "
                            + finalKeySizeBytes + ", got: " + key.size());
        }
    }

    private static void validateKeyCount(ColumnSchema schema, Buf[] keys) {
        if (schema.keysCount() != keys.length) {
            throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH,
                    "Keys count must be equal to the column keys count. Expected: "
                            + schema.keysCount() + ", got: " + keys.length);
        }
    }

    private static void requireKeyEntries(Buf[] keys) {
        for (var key : keys) {
            requireArgument(key, "key");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
