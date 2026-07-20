package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import io.grpc.Status;
import io.grpc.StatusException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.TransportRequestLimits;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ServerTransportMappingBoundsTest {

    private static final int MAX_KEY_PARTS = TransportRequestLimits.MAX_KEY_PARTS;
    private static final int MAX_BATCH_ENTRIES = TransportRequestLimits.MAX_BATCH_ENTRIES;

    @Test
    void grpcKeyMappingRejectsOversizedPartCountBeforeGetterAccess() throws Exception {
        var getterAccessed = new AtomicBoolean();
        var method = grpcServerMapper("mapKeys");

        var failure = invokeRocksDB(method,
                MAX_KEY_PARTS + 1,
                (Int2ObjectFunction<Object>) _ -> {
                    getterAccessed.set(true);
                    return null;
                });

        assertInvalidRequest(failure);
        assertEquals("gRPC key parts count "
                        + (MAX_KEY_PARTS + 1)
                        + " exceeds limit "
                        + MAX_KEY_PARTS,
                failure.getMessage());
        assertFalse(getterAccessed.get());
    }

    @Test
    void grpcBatchMappingRejectsOversizedEntryCountBeforeGetterAccess() throws Exception {
        var getterAccessed = new AtomicBoolean();
        var method = grpcServerMapper("mapKVBatch");

        var failure = invokeRocksDB(method,
                MAX_BATCH_ENTRIES + 1,
                (Int2ObjectFunction<Object>) _ -> {
                    getterAccessed.set(true);
                    return null;
                });

        assertInvalidRequest(failure);
        assertEquals("gRPC batch entries count "
                        + (MAX_BATCH_ENTRIES + 1)
                        + " exceeds limit "
                        + MAX_BATCH_ENTRIES,
                failure.getMessage());
        assertFalse(getterAccessed.get());
    }

    @Test
    void grpcClientPeerKeyMappingRejectsOversizedPartCountBeforeGetterAccess() throws Exception {
        var getterAccessed = new AtomicBoolean();
        var method = GrpcConnection.class.getDeclaredMethod("mapKeys", int.class, Int2ObjectFunction.class);

        var failure = invokeRocksDB(method,
                MAX_KEY_PARTS + 1,
                (Int2ObjectFunction<Object>) _ -> {
                    getterAccessed.set(true);
                    return null;
                });

        assertInvalidRequest(failure);
        assertEquals("gRPC peer key parts count "
                        + (MAX_KEY_PARTS + 1)
                        + " exceeds limit "
                        + MAX_KEY_PARTS,
                failure.getMessage());
        assertFalse(getterAccessed.get());
    }

    @Test
    void grpcClientBatchMappingRejectsOversizedEntryCountBeforeListAccess() throws Exception {
        var keysAccessed = new AtomicBoolean();
        var valuesAccessed = new AtomicBoolean();
        var method = GrpcConnection.class.getDeclaredMethod("mapKVList", List.class, List.class);

        var failure = invokeRocksDB(method,
                hostileList(MAX_BATCH_ENTRIES + 1, keysAccessed),
                hostileList(MAX_BATCH_ENTRIES + 1, valuesAccessed));

        assertInvalidRequest(failure);
        assertEquals("gRPC client batch entries count "
                        + (MAX_BATCH_ENTRIES + 1)
                        + " exceeds limit "
                        + MAX_BATCH_ENTRIES,
                failure.getMessage());
        assertFalse(keysAccessed.get());
        assertFalse(valuesAccessed.get());
    }

    @Test
    void grpcSchemaVariableTailHashMappingRejectsUnrecognizedEnum() throws Exception {
        var method = grpcServerMapper("mapVariableTailKeys");

        var failure = invokeRocksDB(method,
                1,
                (Int2ObjectFunction<Object>) _ ->
                        it.cavallium.rockserver.core.common.api.proto.ColumnHashType.UNRECOGNIZED);

        assertInvalidRequest(failure);
        assertEquals("gRPC schema variable tail key hash type is unrecognized", failure.getMessage());
    }

    @Test
    void grpcClientPeerSchemaVariableTailHashMappingRejectsUnrecognizedEnum() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("unmapVariableTailKeys",
                int.class, Int2ObjectFunction.class);

        var failure = invokeRocksDB(method,
                1,
                (Int2ObjectFunction<Object>) _ ->
                        it.cavallium.rockserver.core.common.api.proto.ColumnHashType.UNRECOGNIZED);

        assertInvalidRequest(failure);
        assertEquals("gRPC peer schema variable tail key hash type is unrecognized", failure.getMessage());
    }

    @Test
    void grpcClientCdcEventMappingRejectsUnrecognizedOpEnum() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapCDCEvent", grpcProtoClass("CDCEvent"));
        var event = it.cavallium.rockserver.core.common.api.proto.CDCEvent.newBuilder()
                .setOpValue(999)
                .build();

        var failure = invokeRocksDB(method, event);

        assertInvalidRequest(failure);
        assertEquals("gRPC CDC event op is unrecognized", failure.getMessage());
    }

    @Test
    void grpcClientOptionalKeyMappingKeepsNullRangeBoundary() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapKeys", Keys.class);
        method.setAccessible(true);

        var iterable = assertInstanceOf(Iterable.class, method.invoke(null, (Object) null));

        assertFalse(iterable.iterator().hasNext());
    }

    @Test
    void grpcClientRequiredKeyMappingRejectsNullKeys() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("requiredMapKeys", String.class, Keys.class);

        var failure = invokeRocksDB(method, "gRPC client get keys", null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client get keys must not be null", failure.getMessage());
    }

    @Test
    void grpcClientValueMappingRejectsNullValue() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapValue", Buf.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client value must not be null", failure.getMessage());
    }

    @Test
    void grpcClientBatchMappingRejectsNullBatch() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapKVBatch", KVBatch.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client batch must not be null", failure.getMessage());
    }

    @Test
    void grpcClientBatchMappingRejectsNullKeysList() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapKVList", List.class, List.class);

        var failure = invokeRocksDB(method, null, List.of());

        assertInvalidRequest(failure);
        assertEquals("gRPC client batch keys must not be null", failure.getMessage());
    }

    @Test
    void grpcClientBatchMappingRejectsMismatchedEntryCounts() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapKVList", List.class, List.class);

        var failure = invokeRocksDB(method, List.of(new Keys(Buf.wrap(new byte[]{1}))), List.of());

        assertInvalidRequest(failure);
        assertEquals("gRPC client batch keys count 1 does not match values count 0", failure.getMessage());
    }

    @Test
    void grpcClientPutBatchModeMappingRejectsNullMode() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapPutBatchMode", PutBatchMode.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client put batch mode must not be null", failure.getMessage());
    }

    @Test
    void grpcClientMergeBatchModeMappingRejectsNullMode() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapMergeBatchMode", MergeBatchMode.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client merge batch mode must not be null", failure.getMessage());
    }

    @Test
    void grpcClientSchemaMappingRejectsNullSchema() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapColumnSchema", ColumnSchema.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC client column schema must not be null", failure.getMessage());
    }

    @Test
    void grpcClientPeerKvMappingRejectsNullKv() throws Exception {
        var method = GrpcConnection.class.getDeclaredMethod("mapKV", grpcProtoClass("KV"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("gRPC peer KV must not be null", failure.getMessage());
    }

    @Test
    void thriftKeyMappingRejectsOversizedPartCountBeforeListAccess() throws Exception {
        var listAccessed = new AtomicBoolean();
        var method = ThriftServer.class.getDeclaredMethod("keysToRecord", List.class);

        var failure = invokeRocksDB(method, hostileList(MAX_KEY_PARTS + 1, listAccessed));

        assertInvalidRequest(failure);
        assertEquals("Thrift key parts count "
                        + (MAX_KEY_PARTS + 1)
                        + " exceeds limit "
                        + MAX_KEY_PARTS,
                failure.getMessage());
        assertFalse(listAccessed.get());
    }

    @Test
    void thriftOptionalKeyMappingKeepsNullRangeBoundary() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("keysToRecord", List.class);
        method.setAccessible(true);

        assertNull(method.invoke(null, (Object) null));
    }

    @Test
    void thriftRequiredKeyMappingRejectsNullList() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("requiredKeysToRecord", String.class, List.class);

        var failure = invokeRocksDB(method, "Thrift put keys", null);

        assertInvalidRequest(failure);
        assertEquals("Thrift put keys must not be null", failure.getMessage());
    }

    @Test
    void thriftMultiKeyMappingRejectsNullList() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("keysToRecords", List.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift multi-key entries must not be null", failure.getMessage());
    }

    @Test
    void thriftMultiKeyMappingRejectsNullEntry() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("keysToRecords", List.class);
        var keys = new ArrayList<List<ByteBuffer>>();
        keys.add(null);

        var failure = invokeRocksDB(method, keys);

        assertInvalidRequest(failure);
        assertEquals("Thrift multi-key entry 0 must not be null", failure.getMessage());
    }

    @Test
    void thriftValueMappingRejectsNullEntry() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("keyToRecords", List.class);
        var values = new ArrayList<ByteBuffer>();
        values.add(null);

        var failure = invokeRocksDB(method, values);

        assertInvalidRequest(failure);
        assertEquals("Thrift value entry 0 must not be null", failure.getMessage());
    }

    @Test
    void thriftBatchMappingRejectsNullEntry() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("kvToBatch", List.class);
        var values = new ArrayList<>();
        values.add(null);

        var failure = invokeRocksDB(method, values);

        assertInvalidRequest(failure);
        assertEquals("Thrift batch entry 0 must not be null", failure.getMessage());
    }

    @Test
    void thriftColumnSchemaMappingRejectsNullSchema() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("columnSchemaToRecord", thriftApiClass("ColumnSchema"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift column schema must not be null", failure.getMessage());
    }

    @Test
    void thriftColumnSchemaHashMappingRejectsNullList() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("hashTypesToRecord", List.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift column schema variableTailKeys must not be null", failure.getMessage());
    }

    @Test
    void thriftPutBatchModeMappingRejectsNullMode() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("mapPutBatchMode", thriftApiClass("PutBatchMode"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift put batch mode must not be null", failure.getMessage());
    }

    @Test
    void thriftMergeBatchModeMappingRejectsNullMode() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("mapMergeBatchMode", thriftApiClass("MergeBatchMode"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift merge batch mode must not be null", failure.getMessage());
    }

    @Test
    void thriftBinaryMappingRejectsCustomNullPayload() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("keyToRecord", String.class, ByteBuffer.class);

        var failure = invokeRocksDB(method, "Thrift merge-operator jar payload", null);

        assertInvalidRequest(failure);
        assertEquals("Thrift merge-operator jar payload must not be null", failure.getMessage());
    }

    @Test
    void thriftClientKeyMappingRejectsOversizedPartCountBeforeValueAccess() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapKeys", Keys.class);

        var failure = invokeRocksDB(method, oversizedKeys());

        assertInvalidRequest(failure);
        assertEquals("Thrift client key parts count "
                        + (MAX_KEY_PARTS + 1)
                        + " exceeds limit "
                        + MAX_KEY_PARTS,
                failure.getMessage());
    }

    @Test
    void thriftClientOptionalKeyMappingKeepsNullRangeBoundary() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapKeys", Keys.class);
        method.setAccessible(true);

        assertNull(method.invoke(null, (Object) null));
    }

    @Test
    void thriftClientRequiredKeyMappingRejectsNullKeys() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("requiredMapKeys", String.class, Keys.class);

        var failure = invokeRocksDB(method, "Thrift client put keys", null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client put keys must not be null", failure.getMessage());
    }

    @Test
    void thriftClientMultiKeyMappingRejectsNullList() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapKeysList", List.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client multi-key entries must not be null", failure.getMessage());
    }

    @Test
    void thriftClientMultiKeyMappingRejectsNullEntry() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapKeysList", List.class);
        var keys = new ArrayList<Keys>();
        keys.add(null);

        var failure = invokeRocksDB(method, keys);

        assertInvalidRequest(failure);
        assertEquals("Thrift client multi-key entry 0 must not be null", failure.getMessage());
    }

    @Test
    void thriftClientValueMappingRejectsNullList() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapBufList", List.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client value entries must not be null", failure.getMessage());
    }

    @Test
    void thriftClientValueMappingRejectsNullEntry() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapBufList", List.class);
        var values = new ArrayList<Buf>();
        values.add(null);

        var failure = invokeRocksDB(method, values);

        assertInvalidRequest(failure);
        assertEquals("Thrift client value entry 0 must not be null", failure.getMessage());
    }

    @Test
    void thriftClientPeerResponseMappingRejectsOversizedEntryCountBeforeListAccess() throws Exception {
        var listAccessed = new AtomicBoolean();
        var method = ThriftConnection.class.getDeclaredMethod("mapOptionalBinaryList", List.class);

        var failure = invokeRocksDB(method, hostileList(MAX_BATCH_ENTRIES + 1, listAccessed));

        assertInvalidRequest(failure);
        assertEquals("Thrift peer response entries count "
                        + (MAX_BATCH_ENTRIES + 1)
                        + " exceeds limit "
                        + MAX_BATCH_ENTRIES,
                failure.getMessage());
        assertFalse(listAccessed.get());
    }

    @Test
    void thriftClientPeerResponseMappingRejectsNullList() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapOptionalBinaryList", List.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift peer response entries must not be null", failure.getMessage());
    }

    @Test
    void thriftClientPeerKvMappingRejectsNullKv() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapKV", thriftApiClass("KV"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift peer KV must not be null", failure.getMessage());
    }

    @Test
    void thriftClientPutBatchModeMappingRejectsNullMode() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapPutBatchMode", PutBatchMode.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client put batch mode must not be null", failure.getMessage());
    }

    @Test
    void thriftClientMergeBatchModeMappingRejectsNullMode() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapMergeBatchMode", MergeBatchMode.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client merge batch mode must not be null", failure.getMessage());
    }

    @Test
    void thriftClientSchemaMappingRejectsNullSchema() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapSchema", ColumnSchema.class);

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift client column schema must not be null", failure.getMessage());
    }

    @Test
    void thriftClientPeerSchemaMappingRejectsNullSchema() throws Exception {
        var method = ThriftConnection.class.getDeclaredMethod("mapSchemaFromThrift", thriftApiClass("ColumnSchema"));

        var failure = invokeRocksDB(method, (Object) null);

        assertInvalidRequest(failure);
        assertEquals("Thrift peer column schema must not be null", failure.getMessage());
    }

    @Test
    void thriftServerKvMappingPreservesNullableValue() throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("mapKV",
                it.cavallium.rockserver.core.common.KV.class);
        method.setAccessible(true);
        var source = new it.cavallium.rockserver.core.common.KV(
                new Keys(Buf.wrap(new byte[]{1})),
                null);

        var mapped = assertInstanceOf(thriftApiClass("KV"), method.invoke(null, source));
        var isSetValue = mapped.getClass().getMethod("isSetValue");

        assertFalse((Boolean) isSetValue.invoke(mapped));
    }

    @Test
    void thriftBatchMappingRejectsOversizedEntryCountBeforeListAccess() throws Exception {
        var listAccessed = new AtomicBoolean();
        var method = ThriftServer.class.getDeclaredMethod("kvToBatch", List.class);

        var failure = invokeRocksDB(method, hostileList(MAX_BATCH_ENTRIES + 1, listAccessed));

        assertInvalidRequest(failure);
        assertEquals("Thrift batch entries count "
                        + (MAX_BATCH_ENTRIES + 1)
                        + " exceeds limit "
                        + MAX_BATCH_ENTRIES,
                failure.getMessage());
        assertFalse(listAccessed.get());
    }

    @Test
    void grpcRequestErrorMappingPreservesNestedRocksDBExceptionStatus() throws Exception {
        var rocksFailure = RocksDBException.of(
                RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                "nested gRPC request failure"
        );
        var mapped = invokeGrpcMapRequestError(new CompletionException(new CompletionException(rocksFailure)));

        StepVerifier.create(mapped)
                .expectErrorSatisfies(error -> {
                    var status = assertInstanceOf(StatusException.class, error);
                    assertEquals(Status.Code.INTERNAL, status.getStatus().getCode());
                    assertEquals(rocksFailure.getLocalizedMessage(), status.getStatus().getDescription());
                    assertEquals(rocksFailure, status.getStatus().getCause());
                })
                .verify();
    }

    @Test
    void grpcRequestErrorMappingPropagatesNestedFatalFailureCause() throws Exception {
        var fatalFailure = new AssertionError("nested fatal request failure");
        var mapped = invokeGrpcMapRequestError(new CompletionException(new CompletionException(fatalFailure)));

        StepVerifier.create(mapped)
                .expectErrorSatisfies(error -> assertSame(fatalFailure, error))
                .verify();
    }

    @Test
    void grpcStreamingFirstValueGuardRejectsNullOnNextValue() throws Exception {
        var method = grpcServerImpl().getDeclaredMethod("requireFirstSignalValue", String.class, Object.class);

        var failure = invokeRocksDB(method, "putBatch", null);

        assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, failure.getErrorUniqueId());
        assertEquals("gRPC putBatch stream emitted a first onNext signal without a value", failure.getMessage());
    }

    @Test
    void grpcResponseErrorMappingPropagatesNestedFatalFailureCause() throws Exception {
        var fatalFailure = new AssertionError("nested fatal response failure");

        assertSame(fatalFailure,
                invokeGrpcMapResponseError(new CompletionException(new CompletionException(fatalFailure))));
    }

    private static Method grpcServerMapper(String name) throws ReflectiveOperationException {
        var method = grpcServerImpl()
                .getDeclaredMethod(name, int.class, Int2ObjectFunction.class);
        method.setAccessible(true);
        return method;
    }

    @SuppressWarnings("unchecked")
    private static Mono<Object> invokeGrpcMapRequestError(Throwable failure) throws ReflectiveOperationException {
        var method = grpcServerImpl().getDeclaredMethod("mapRequestError",
                String.class,
                Message.class,
                Throwable.class);
        method.setAccessible(true);
        return (Mono<Object>) method.invoke(null, "put", Empty.getDefaultInstance(), failure);
    }

    private static Throwable invokeGrpcMapResponseError(Throwable failure) throws ReflectiveOperationException {
        var method = grpcServerImpl().getDeclaredMethod("mapResponseError", Throwable.class);
        method.setAccessible(true);
        return (Throwable) method.invoke(null, failure);
    }

    private static Class<?> grpcServerImpl() throws ClassNotFoundException {
        return Class.forName(GrpcServer.class.getName() + "$GrpcServerImpl");
    }

    private static Class<?> thriftApiClass(String simpleName) throws ClassNotFoundException {
        return Class.forName("it.cavallium.rockserver.core.common.api." + simpleName);
    }

    private static Class<?> grpcProtoClass(String simpleName) throws ClassNotFoundException {
        return Class.forName("it.cavallium.rockserver.core.common.api.proto." + simpleName);
    }

    private static RocksDBException invokeRocksDB(Method method, Object... args) {
        method.setAccessible(true);
        var invocation = assertThrows(InvocationTargetException.class, () -> method.invoke(null, args));
        return assertInstanceOf(RocksDBException.class, invocation.getCause());
    }

    private static void assertInvalidRequest(RocksDBException failure) {
        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, failure.getErrorUniqueId());
    }

    private static Keys oversizedKeys() {
        return new Keys(new Buf[MAX_KEY_PARTS + 1]);
    }

    private static <T> List<T> hostileList(int size, AtomicBoolean accessed) {
        return new AbstractList<>() {
            @Override
            public T get(int index) {
                accessed.set(true);
                throw new AssertionError("list should not be accessed");
            }

            @Override
            public int size() {
                return size;
            }
        };
    }
}
