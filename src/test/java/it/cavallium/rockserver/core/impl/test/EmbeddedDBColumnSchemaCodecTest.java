package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Test;

class EmbeddedDBColumnSchemaCodecTest {

    @Test
    void decodeColumnSchemaRejectsUnknownVersionWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "schema-codec-decode", null)) {
            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeColumnSchema", byte[].class, new byte[]{99}));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("Unknown column schema version: 99", failure.getMessage());
        }
    }

    @Test
    void decodeColumnSchemaRejectsNegativeFixedKeyCountWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "schema-codec-negative-fixed-keys", null)) {
            var payload = columnSchemaPayload(out -> {
                out.writeByte(3);
                out.writeInt(-1);
            });

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeColumnSchema", byte[].class, payload));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("Column schema fixed key count must not be negative: -1", failure.getMessage());
        }
    }

    @Test
    void decodeColumnSchemaRejectsInvalidHashTypeOrdinalWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "schema-codec-invalid-hash-type", null)) {
            var payload = columnSchemaPayload(out -> {
                out.writeByte(3);
                out.writeInt(0);
                out.writeInt(1);
                out.writeByte(255);
            });

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeColumnSchema", byte[].class, payload));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("Unknown column schema hash type ordinal: 255", failure.getMessage());
        }
    }

    @Test
    void encodeColumnSchemaRejectsOversizedUtfFieldWithPublicError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "schema-codec-encode", null)) {
            var schema = ColumnSchema.of(
                    IntArrayList.of(4),
                    ObjectArrayList.of(),
                    true,
                    "m".repeat(70_000),
                    null,
                    null);

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "encodeColumnSchema", ColumnSchema.class, schema));

            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, failure.getErrorUniqueId());
            assertEquals("Failed to encode column schema", failure.getMessage());
        }
    }

    private static byte[] columnSchemaPayload(ColumnSchemaPayloadWriter writer) throws IOException {
        try (var baos = new ByteArrayOutputStream(); var out = new DataOutputStream(baos)) {
            writer.write(out);
            return baos.toByteArray();
        }
    }

    private static Object invokePrivate(EmbeddedDB db, String methodName, Class<?> parameterType, Object argument) {
        try {
            var method = EmbeddedDB.class.getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return method.invoke(db, argument);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError("Unable to invoke EmbeddedDB." + methodName, e);
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AssertionError("Unexpected checked failure from EmbeddedDB." + methodName, cause);
        }
    }

    @FunctionalInterface
    private interface ColumnSchemaPayloadWriter {

        void write(DataOutputStream out) throws IOException;
    }
}
