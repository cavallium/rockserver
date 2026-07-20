package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class RockserverKeyEncoderTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var fixedSchema = ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
        var variableSchema = ColumnSchema.of(IntArrayList.of(),
                ObjectArrayList.of(ColumnHashType.XXHASH8), true);
        var keys = new Keys(buf(1));

        assertIllegalArgument("keys must not be null", () -> RockserverKeyEncoder.calculateKey(fixedSchema,
                (Keys) null));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.calculateKey(null, keys));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.calculateKey(null,
                new Buf[]{buf(1)}));
        assertIllegalArgument("keys must not be null", () -> RockserverKeyEncoder.calculateKey(fixedSchema,
                (Buf[]) null));
        assertIllegalArgument("key must not be null", () -> RockserverKeyEncoder.calculateKey(fixedSchema,
                new Buf[]{null}));
        assertIllegalArgument("keys must not be null", () -> RockserverKeyEncoder.calculateBucketElementKey(
                variableSchema, (Keys) null));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.calculateBucketElementKey(
                null, keys));
        assertIllegalArgument("variableKeys must not be null",
                () -> RockserverKeyEncoder.calculateBucketElementKey(variableSchema, (Buf[]) null));
        assertIllegalArgument("variableKey must not be null",
                () -> RockserverKeyEncoder.calculateBucketElementKey(variableSchema, new Buf[]{null}));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.decodeFixedLengthKeys(null,
                buf(1)));
        assertIllegalArgument("calculatedKey must not be null",
                () -> RockserverKeyEncoder.decodeFixedLengthKeys(fixedSchema, null));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.variableTailKeys(null,
                new Buf[]{buf(1)}));
        assertIllegalArgument("keys must not be null", () -> RockserverKeyEncoder.variableTailKeys(fixedSchema,
                null));
        assertIllegalArgument("left must not be null", () -> RockserverKeyEncoder.compareCalculatedKeys(null,
                buf(1)));
        assertIllegalArgument("right must not be null", () -> RockserverKeyEncoder.compareCalculatedKeys(buf(1),
                null));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.finalKeySizeBytes(null));
        assertIllegalArgument("schema must not be null", () -> RockserverKeyEncoder.hasVariableTailKeys(null));
    }

    @Test
    void matchesColumnInstanceForFixedCompositeKey() {
        var schema = ColumnSchema.of(IntArrayList.of(2, 1), ObjectArrayList.of(), true);
        var keys = new Keys(buf(0x01, 0x02), buf(0xff));
        var calculatedKey = RockserverKeyEncoder.calculateKey(schema, keys);

        assertEquals(columnInstanceKey(schema, keys), calculatedKey);
        assertEquals(keys, RockserverKeyEncoder.decodeFixedLengthKeys(schema, calculatedKey));
    }

    @Test
    void matchesColumnInstanceForBucketedKey() {
        var schema = ColumnSchema.of(IntArrayList.of(4),
                ObjectArrayList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH8),
                true);
        var keys = new Keys(buf(0, 0, 0, 7), text("variable-1"), text("variable-2"));

        assertEquals(columnInstanceKey(schema, keys), RockserverKeyEncoder.calculateKey(schema, keys));
        assertEquals(columnInstanceBucketKey(schema, keys),
                RockserverKeyEncoder.calculateBucketElementKey(schema, keys));
    }

    @Test
    void comparesCalculatedKeysUsingUnsignedRocksDbOrder() {
        var low = buf(0x00);
        var high = buf(0xff);

        assertEquals(-255, RockserverKeyEncoder.compareCalculatedKeys(low, high));
        assertEquals(255, RockserverKeyEncoder.compareCalculatedKeys(high, low));
    }

    @Test
    void bucketedHashCollisionStaysInOneCalculatedKey() {
        var schema = ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(ColumnHashType.ALLSAME8), true);
        var first = new Keys(buf(7), text("collision-a"));
        var second = new Keys(buf(7), text("collision-b"));

        assertEquals(RockserverKeyEncoder.calculateKey(schema, first),
                RockserverKeyEncoder.calculateKey(schema, second));
        assertEquals(0, RockserverKeyEncoder.compareCalculatedKeys(
                RockserverKeyEncoder.calculateKey(schema, first),
                RockserverKeyEncoder.calculateKey(schema, second)));
        assertNotEquals(RockserverKeyEncoder.calculateBucketElementKey(schema, first),
                RockserverKeyEncoder.calculateBucketElementKey(schema, second));
    }

    @Test
    void matchesColumnInstanceForSingleFixedKeyLengthMismatch() {
        var schema = ColumnSchema.of(IntArrayList.of(4), ObjectArrayList.of(), true);
        var keys = new Keys(buf(1, 2));

        var expected = assertThrows(RocksDBException.class, () -> columnInstanceKey(schema, keys));
        var actual = assertThrows(RocksDBException.class, () -> RockserverKeyEncoder.calculateKey(schema, keys));

        assertEquals(expected.getErrorUniqueId(), actual.getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH, actual.getErrorUniqueId());
    }

    private static void assertIllegalArgument(String expectedMessage, org.junit.jupiter.api.function.Executable action) {
        var failure = assertThrows(IllegalArgumentException.class, action);

        assertEquals(expectedMessage, failure.getMessage());
    }

    @SuppressWarnings("resource")
    private static Buf columnInstanceKey(ColumnSchema schema, Keys keys) {
        return new ColumnInstance(null, schema).calculateKey(keys.keys());
    }

    @SuppressWarnings("resource")
    private static Buf columnInstanceBucketKey(ColumnSchema schema, Keys keys) {
        var column = new ColumnInstance(null, schema);
        return column.computeBucketElementKey(column.getBucketElementKeys(keys.keys()));
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
