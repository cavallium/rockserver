package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.Bucket;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class BucketedColumnTest {

    @Test
    void testDecodeBucketedKeys() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf fixedKey = Buf.wrap(new byte[]{0, 0, 0, 1});
        Buf varKey = Buf.wrap("variable-key-content".getBytes(StandardCharsets.UTF_8));
        Buf[] originalKeys = new Buf[]{fixedKey, varKey};

        Buf calculatedKey = col.calculateKey(originalKeys);
        assertEquals(8, calculatedKey.size());

        Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{varKey});
        Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

        assertEquals(2, decodedKeys.length);
        assertEquals(fixedKey, decodedKeys[0], "Fixed key mismatch");
        assertEquals(varKey, decodedKeys[1], "Variable key mismatch");
    }

    @Test
    void testDecodeBucketedKeysMultipleVariable() {
        // Schema: 1 Fixed Key (4 bytes), 2 Variable Keys
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf fixedKey = Buf.wrap(new byte[]{1, 2, 3, 4});
        Buf varKey1 = Buf.wrap("var1".getBytes(StandardCharsets.UTF_8));
        Buf varKey2 = Buf.wrap("variable2".getBytes(StandardCharsets.UTF_8));
        Buf[] originalKeys = new Buf[]{fixedKey, varKey1, varKey2};

        Buf calculatedKey = col.calculateKey(originalKeys);
        assertEquals(12, calculatedKey.size());

        Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{varKey1, varKey2});
        Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

        assertEquals(3, decodedKeys.length);
        assertEquals(fixedKey, decodedKeys[0]);
        assertEquals(varKey1, decodedKeys[1]);
        assertEquals(varKey2, decodedKeys[2]);
    }

    @Test
    void testDecodeBucketedKeysWithEmptyVariableKey() {
        // Schema: 1 Fixed Key, 1 Variable Key
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf fixedKey = Buf.wrap(new byte[]{0, 0, 0, 1});
        Buf varKey = Buf.wrap(new byte[0]);
        Buf[] originalKeys = new Buf[]{fixedKey, varKey};

        Buf calculatedKey = col.calculateKey(originalKeys);
        Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{varKey});

        Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

        assertEquals(2, decodedKeys.length);
        assertEquals(fixedKey, decodedKeys[0]);
        assertEquals(varKey, decodedKeys[1]);
        assertEquals(0, decodedKeys[1].size());
    }

    @Test
    void testDecodeBucketedKeysNullBucketValue() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf calculatedKey = Buf.createZeroes(8);

        RocksDBException ex = assertThrows(RocksDBException.class, () -> col.decodeKeys(calculatedKey, null));
        assertEquals(RocksDBException.RocksDBErrorType.NULL_ARGUMENT, ex.getErrorUniqueId());
    }

    @Test
    void testDecodeBucketedKeysCorruptedBucketValueShortLength() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf calculatedKey = Buf.createZeroes(8);
        Buf bucketValue = Buf.wrap(new byte[]{0});

        RocksDBException ex = assertThrows(RocksDBException.class, () -> col.decodeKeys(calculatedKey, bucketValue));
        assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, ex.getErrorUniqueId());
        assertTrue(ex.getMessage().contains("too short for key length"));
    }

    @Test
    void testDecodeBucketedKeysCorruptedBucketValueShortContent() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf calculatedKey = Buf.createZeroes(8);
        Buf bucketValue = Buf.wrap(new byte[]{0, 5, 1});

        RocksDBException ex = assertThrows(RocksDBException.class, () -> col.decodeKeys(calculatedKey, bucketValue));
        assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, ex.getErrorUniqueId());
        assertTrue(ex.getMessage().contains("too short for key content"));
    }

    @Test
    void bucketParserRejectsMalformedSegmentsWithExplicitDiagnostics() {
        var col = column(bucketedSchema(true));

        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment too short for element count",
                () -> new Bucket(col, Buf.wrap(new byte[]{1})));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment element count must be non-negative",
                () -> new Bucket(col, bucketCountSegment(-1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment too short for element size at index 0",
                () -> new Bucket(col, bucketCountSegment(1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket element size must be non-negative at index 0",
                () -> new Bucket(col, bucketElementSegment(-1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment too short for element payload at index 0",
                () -> new Bucket(col, bucketElementSegment(2, (byte) 1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment too short for key length for element 0 variable key 0",
                () -> new Bucket(col, bucketElementPayload((byte) 1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment too short for key content for element 0 variable key 0",
                () -> new Bucket(col, bucketElementPayload((byte) 0, (byte) 2, (byte) 1)));
        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket segment has 1 trailing bytes after 0 elements",
                () -> new Bucket(col, trailingEmptyBucketSegment()));
    }

    @Test
    void bucketParserRejectsValueBytesForValueLessSchema() {
        var col = column(bucketedSchema(false));

        assertRocks(RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
                "Bucket element 0 has 1 trailing value bytes for value-less schema",
                () -> new Bucket(col, bucketElementPayload((byte) 0, (byte) 0, (byte) 7)));
    }

    @Test
    void bucketParserRoundTripsSerializedElements() {
        var col = column(bucketedSchema(true));
        var bucket = new Bucket(col);
        var variableKey = Buf.wrap("round-trip-key".getBytes(StandardCharsets.UTF_8));
        var value = Buf.wrap("round-trip-value".getBytes(StandardCharsets.UTF_8));

        assertNull(bucket.addElement(new Buf[]{variableKey}, value));

        var parsed = new Bucket(col, bucket.toSegment());

        assertEquals(value, parsed.getElement(new Buf[]{variableKey}));
        assertEquals(1, parsed.getElements().size());
    }

    @Test
    void bucketOperationsRejectInvalidVariableKeysWithExplicitDiagnostics() {
        var col = column(bucketedSchema(true));
        var bucket = new Bucket(col);

        assertRocks(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                "Bucket variable keys must not be null",
                () -> bucket.addElement(null, Buf.wrap(new byte[]{1})));
        assertRocks(RocksDBException.RocksDBErrorType.KEYS_COUNT_MISMATCH,
                "Bucket variable keys count must be equal to the column variable-length keys count",
                () -> bucket.getElement(new Buf[0]));
        assertRocks(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                "Bucket variable key at index 0 must not be null",
                () -> bucket.removeElement(new Buf[]{null}));
    }

    @Test
    void columnBucketHelpersRejectInvalidKeyInputsWithExplicitDiagnostics() {
        var col = column(bucketedSchema(true));

        assertRocks(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                "variableKeys must not be null",
                () -> col.computeBucketElementKey(null));
        assertRocks(RocksDBException.RocksDBErrorType.KEYS_COUNT_MISMATCH,
                "Variable keys count must be equal to the column variable-length keys count",
                () -> col.computeBucketElementKey(new Buf[0]));
        assertRocks(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                "Variable key at index 0 must not be null",
                () -> col.computeBucketElementKey(new Buf[]{null}));
        assertRocks(RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
                "keys must not be null",
                () -> col.getBucketElementKeys(null));
        assertRocks(RocksDBException.RocksDBErrorType.KEYS_COUNT_MISMATCH,
                "Keys count must be equal to the column keys count",
                () -> col.getBucketElementKeys(new Buf[0]));
    }

    @Test
    void testMixedFixedAndVariableKeys() {
        var schema = ColumnSchema.of(
                IntArrayList.of(4, 2),
                ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
                true
        );

        var col = column(schema);
        Buf f1 = Buf.wrap(new byte[]{1, 1, 1, 1});
        Buf f2 = Buf.wrap(new byte[]{2, 2});
        Buf v1 = Buf.wrap("v1".getBytes(StandardCharsets.UTF_8));
        Buf v2 = Buf.wrap("v2".getBytes(StandardCharsets.UTF_8));
        Buf[] originalKeys = new Buf[]{f1, f2, v1, v2};

        Buf calculatedKey = col.calculateKey(originalKeys);
        Buf bucketElementKey = col.computeBucketElementKey(new Buf[]{v1, v2});
        Buf[] decodedKeys = col.decodeKeys(calculatedKey, bucketElementKey);

        assertEquals(4, decodedKeys.length);
        assertEquals(f1, decodedKeys[0]);
        assertEquals(f2, decodedKeys[1]);
        assertEquals(v1, decodedKeys[2]);
        assertEquals(v2, decodedKeys[3]);
    }

    private static ColumnInstance column(ColumnSchema schema) {
        // These tests exercise pure key encoding/decoding paths and never close or inspect the RocksDB handle.
        return new ColumnInstance(null, schema);
    }

    private static ColumnSchema bucketedSchema(boolean hasValue) {
        return ColumnSchema.of(
                IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32),
                hasValue
        );
    }

    private static Buf bucketCountSegment(int elementCount) {
        var segment = Buf.createZeroes(Integer.BYTES);
        segment.setInt(0, elementCount);
        return segment;
    }

    private static Buf bucketElementPayload(byte... payload) {
        return bucketElementSegment(payload.length, payload);
    }

    private static Buf bucketElementSegment(int declaredElementSize, byte... payload) {
        var segment = Buf.createZeroes(Integer.BYTES + Integer.BYTES + payload.length);
        segment.setInt(0, 1);
        segment.setInt(Integer.BYTES, declaredElementSize);
        if (payload.length > 0) {
            segment.setBytesFromBuf(Integer.BYTES + Integer.BYTES, Buf.wrap(payload), 0, payload.length);
        }
        return segment;
    }

    private static Buf trailingEmptyBucketSegment() {
        var segment = Buf.createZeroes(Integer.BYTES + 1);
        segment.setInt(0, 0);
        segment.setByte(Integer.BYTES, (byte) 1);
        return segment;
    }

    private static RocksDBException assertRocks(RocksDBException.RocksDBErrorType expectedType,
                                                String expectedMessagePart,
                                                org.junit.jupiter.api.function.Executable executable) {
        var failure = assertThrows(RocksDBException.class, executable);
        assertEquals(expectedType, failure.getErrorUniqueId());
        assertTrue(failure.getMessage().contains(expectedMessagePart),
                () -> "Expected message to contain \"" + expectedMessagePart + "\", got \"" + failure.getMessage() + "\"");
        return failure;
    }
}
