package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Test;

class EmbeddedDBCdcMetaCodecTest {

    @Test
    void decodeCdcMetaRejectsNegativeFilterCountWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "cdc-meta-negative-filter-count", null)) {
            var payload = cdcMetaPayload(out -> {
                out.writeByte(2);
                out.writeLong(0L);
                out.writeBoolean(true);
                out.writeInt(-1);
            });

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeCdcMeta", byte[].class, payload));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("CDC metadata filter count must not be negative: -1", failure.getMessage());
        }
    }

    @Test
    void decodeCdcMetaRejectsOversizedFilterCountWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "cdc-meta-oversized-filter-count", null)) {
            var payload = cdcMetaPayload(out -> {
                out.writeByte(2);
                out.writeLong(0L);
                out.writeBoolean(true);
                out.writeInt(1);
            });

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeCdcMeta", byte[].class, payload));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("CDC metadata filter count 1 exceeds remaining payload capacity: 0", failure.getMessage());
        }
    }

    @Test
    void decodeCdcMetaRejectsTrailingBytesWithLoadError() throws Exception {
        try (var connection = new EmbeddedConnection(null, "cdc-meta-trailing-bytes", null)) {
            var payload = cdcMetaPayload(out -> {
                out.writeByte(2);
                out.writeLong(0L);
                out.writeBoolean(false);
                out.writeBoolean(false);
                out.writeByte(99);
            });

            var failure = assertThrows(RocksDBException.class,
                    () -> invokePrivate(connection.getInternalDB(), "decodeCdcMeta", byte[].class, payload));

            assertEquals(RocksDBException.RocksDBErrorType.ROCKSDB_LOAD_ERROR, failure.getErrorUniqueId());
            assertEquals("CDC metadata payload has trailing bytes: 1", failure.getMessage());
        }
    }

    private static byte[] cdcMetaPayload(CdcMetaPayloadWriter writer) throws IOException {
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
    private interface CdcMetaPayloadWriter {

        void write(DataOutputStream out) throws IOException;
    }
}
