package it.cavallium.rockserver.core.impl.test;

import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.SerializedKVBatch;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class NonThriftRegressionProofTest {

    private Path configFile;
    private EmbeddedConnection db;
    private GrpcServer grpcServer;
    private GrpcConnection grpcClient;
    private ManagedChannel grpcChannel;

    @BeforeEach
    void setUp() throws IOException {
        System.setProperty("rockserver.core.print-config", "false");
        configFile = Files.createTempFile("non-thrift-regression", ".conf");
        Files.writeString(configFile, "database: { global: { ingest-behind: true, optimistic: false } }");
        db = new EmbeddedConnection(null, "non-thrift-regression", configFile);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (grpcClient != null) {
            grpcClient.close();
        }
        if (grpcChannel != null) {
            grpcChannel.shutdownNow();
            grpcChannel.awaitTermination(5, TimeUnit.SECONDS);
        }
        if (grpcServer != null) {
            grpcServer.close();
        }
        if (db != null) {
            db.closeTesting();
        }
        if (configFile != null) {
            Files.deleteIfExists(configFile);
        }
    }

    @Test
    void ownedBucketedPutDoesNotUnderflowPendingOps() {
        long colId = db.getSyncApi().createColumn("bucketed-put", bucketedSchema());
        var key = key(intBytes(1), bytes("a"));

        db.getSyncApi().put(0, colId, key, bytes("value"), RequestType.none());

        assertNoResourceDrift("owned bucketed put");
    }

    @Test
    void ownedBucketedDeleteAndMergeDoNotDriftResourceCounters() {
        long deleteColId = db.getSyncApi().createColumn("bucketed-delete", bucketedSchema());
        var deleteKey = key(intBytes(2), bytes("a"));
        db.getSyncApi().put(0, deleteColId, deleteKey, bytes("delete-me"), RequestType.none());
        assertNoResourceDrift("owned bucketed put before delete");

        Buf previous = db.getSyncApi().delete(0, deleteColId, deleteKey, RequestType.previous());
        assertBufEquals(bytes("delete-me"), previous);
        assertNoResourceDrift("owned bucketed delete with previous value");

        long mergeColId = db.getSyncApi().createColumn("bucketed-merge-balanced", bucketedSchemaWithMergeOperator());
        var mergeKey = key(intBytes(3), bytes("b"));
        db.getSyncApi().merge(0, mergeColId, mergeKey, bytes("one"), RequestType.none());
        assertNoResourceDrift("owned bucketed merge insert");

        Buf merged = db.getSyncApi().merge(0, mergeColId, mergeKey, bytes("two"), RequestType.merged());
        assertEquals("one,two", string(merged));
        assertNoResourceDrift("owned bucketed merge existing value");
    }

    @Test
    void ownedReturnValueOperationsDoNotDriftResourceCounters() {
        long colId = db.getSyncApi().createColumn("return-value-counter-balance", fixedSchemaWithMergeOperator());
        var key = key(intBytes(4));

        Buf firstPrevious = db.getSyncApi().put(0, colId, key, bytes("one"), RequestType.previous());
        assertNull(firstPrevious);
        assertNoResourceDrift("owned put previous on insert");

        Delta<Buf> delta = db.getSyncApi().put(0, colId, key, bytes("two"), RequestType.delta());
        assertBufEquals(bytes("one"), delta.previous());
        assertBufEquals(bytes("two"), delta.current());
        assertNoResourceDrift("owned put delta");

        Boolean changed = db.getSyncApi().put(0, colId, key, bytes("two"), RequestType.changed());
        assertFalse(changed);
        assertNoResourceDrift("owned put changed without data change");

        Buf merged = db.getSyncApi().merge(0, colId, key, bytes("three"), RequestType.merged());
        assertEquals("two,three", string(merged));
        assertNoResourceDrift("owned merge merged result");

        Boolean present = db.getSyncApi().delete(0, colId, key, RequestType.previousPresence());
        assertTrue(present);
        assertNoResourceDrift("owned delete previous presence");
    }

    @Test
    void bucketedMergeWithoutMergeOperatorFailsWithPutInvalidRequest() {
        long colId = db.getSyncApi().createColumn("bucketed-no-merge-op", bucketedSchema());
        var key = key(intBytes(2), bytes("a"));

        db.getSyncApi().merge(0, colId, key, bytes("first"), RequestType.none());

        var ex = assertThrows(RocksDBException.class,
                () -> db.getSyncApi().merge(0, colId, key, bytes("second"), RequestType.none()));
        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, ex.getErrorUniqueId());
        assertTrue(ex.getMessage().contains("merge operator"));
        assertNoResourceDrift("failed owned bucketed merge");
    }

    @Test
    void scanRawBucketedColumnReturnsOneDecodedEntryPerBucketElement() {
        long colId = db.getSyncApi().createColumn("bucketed-scan-raw", bucketedSchema());
        var keyA = key(intBytes(3), bytes("a"));
        var keyB = key(intBytes(3), bytes("b"));
        db.getSyncApi().put(0, colId, keyA, bytes("value-a"), RequestType.none());
        db.getSyncApi().put(0, colId, keyB, bytes("value-b"), RequestType.none());
        db.getSyncApi().flush();

        List<KV> entries;
        try (var stream = db.getSyncApi().scanRaw(colId, 0, 1)) {
            entries = stream.flatMap(SerializedKVBatch::decode).toList();
        }

        assertEquals(2, entries.size());
        Map<String, String> valuesByVariableKey = entries.stream().collect(Collectors.toMap(
                kv -> string(kv.keys().keys()[1]),
                kv -> {
                    var value = kv.value();
                    assertNotNull(value);
                    return string(value);
                }
        ));
        assertEquals(Map.of("a", "value-a", "b", "value-b"), valuesByVariableKey);
    }

    @Test
    void grpcPutMultiReturnTypesAreImplemented() throws Exception {
        startGrpc();
        long colId = grpcClient.getSyncApi().createColumn("grpc-put-multi", fixedSchema());
        var keys = List.of(key(intBytes(10)), key(intBytes(11)));
        var initialValues = List.of(bytes("one"), bytes("two"));
        var replacementValues = List.of(bytes("one-new"), bytes("two-new"));

        List<Buf> previous = grpcClient.getSyncApi().putMulti(0, colId, keys, initialValues, RequestType.previous());
        assertEquals(2, previous.size());
        assertNull(previous.get(0));
        assertNull(previous.get(1));

        List<Delta<Buf>> deltas = grpcClient.getSyncApi().putMulti(0, colId, keys, replacementValues, RequestType.delta());
        assertBufEquals(initialValues.get(0), deltas.get(0).previous());
        assertBufEquals(replacementValues.get(0), deltas.get(0).current());
        assertBufEquals(initialValues.get(1), deltas.get(1).previous());
        assertBufEquals(replacementValues.get(1), deltas.get(1).current());

        List<Boolean> changed = grpcClient.getSyncApi().putMulti(0, colId, keys, replacementValues, RequestType.changed());
        assertEquals(List.of(false, false), changed);

        List<Boolean> present = grpcClient.getSyncApi().putMulti(0, colId, keys, replacementValues, RequestType.previousPresence());
        assertEquals(List.of(true, true), present);
    }

    @Test
    void emptyGrpcStreamingWriteRequestsFailInsteadOfSucceeding() throws Exception {
        startGrpc();
        var stub = ReactorRocksDBServiceGrpc.newReactorStub(grpcChannel);

        assertGrpcFailure(stub.putBatch(Flux.empty()));
        assertGrpcFailure(stub.mergeBatch(Flux.empty()));
        assertGrpcFailure(stub.putMulti(Flux.empty()));
        assertGrpcFailure(stub.mergeMulti(Flux.empty()));
    }

    @Test
    void grpcStreamingBatchRequestsRejectUnrecognizedModesAsInvalidRequests() throws Exception {
        startGrpc();
        var stub = ReactorRocksDBServiceGrpc.newReactorStub(grpcChannel);

        var putRequest = PutBatchRequest.newBuilder()
                .setInitialRequest(PutBatchInitialRequest.newBuilder()
                        .setColumnId(4)
                        .setModeValue(999))
                .build();
        var mergeRequest = MergeBatchRequest.newBuilder()
                .setInitialRequest(MergeBatchInitialRequest.newBuilder()
                        .setColumnId(4)
                        .setModeValue(999))
                .build();

        assertGrpcInvalidRequest(stub.putBatch(Flux.just(putRequest)), "Unrecognized request \"mode\"");
        assertGrpcInvalidRequest(stub.mergeBatch(Flux.just(mergeRequest)), "Unrecognized request \"mode\"");
    }

    @Test
    void grpcStreamingMultiRequestsRejectMultipleInitialRequestsAsInvalidRequests() throws Exception {
        startGrpc();
        var stub = ReactorRocksDBServiceGrpc.newReactorStub(grpcChannel);
        var putInitial = PutMultiRequest.newBuilder()
                .setInitialRequest(PutMultiInitialRequest.newBuilder().setColumnId(4))
                .build();
        var mergeInitial = MergeMultiRequest.newBuilder()
                .setInitialRequest(MergeMultiInitialRequest.newBuilder().setColumnId(4))
                .build();

        assertGrpcInvalidRequest(stub.putMulti(Flux.just(putInitial, putInitial)), "Multiple initial requests");
        assertGrpcInvalidRequest(stub.mergeMulti(Flux.just(mergeInitial, mergeInitial)), "Multiple initial requests");
        assertGrpcInvalidRequest(stub.mergeMultiGetMerged(Flux.just(mergeInitial, mergeInitial))
                .then(Mono.just(Empty.getDefaultInstance())), "Multiple initial requests");
    }

    @Test
    void grpcCdcPollBatchAdvancesNextSeqWhenFiltersEmitNoEvents() throws Exception {
        startGrpc();
        long includedColId = grpcClient.getSyncApi().createColumn("cdc-included", fixedSchema());
        long excludedColId = grpcClient.getSyncApi().createColumn("cdc-excluded", fixedSchema());
        long startSeq = grpcClient.getSyncApi().cdcCreate("filtered-cdc", null, List.of(includedColId));

        grpcClient.getSyncApi().put(0, excludedColId, key(intBytes(30)), bytes("excluded"), RequestType.none());

        var directBatch = db.getAsyncApi()
                .cdcPollBatchAsync("filtered-cdc", startSeq, 10)
                .block(Duration.ofSeconds(5));
        assertNotNull(directBatch);
        assertTrue(directBatch.events().isEmpty());
        assertTrue(directBatch.nextSeq() > startSeq,
                "embedded CDC batch polling must advance the server cursor across filtered WAL"
                        + " (startSeq=" + startSeq + ", nextSeq=" + directBatch.nextSeq() + ")");

        var batch = grpcClient.getAsyncApi()
                .cdcPollBatchAsync("filtered-cdc", startSeq, 10)
                .block(Duration.ofSeconds(5));

        assertNotNull(batch);
        assertTrue(batch.events().isEmpty());
        assertTrue(batch.nextSeq() > startSeq,
                "gRPC CDC batch polling must preserve the server cursor even when no events pass the filter"
                        + " (startSeq=" + startSeq + ", nextSeq=" + batch.nextSeq() + ")");
    }

    private void startGrpc() throws IOException {
        grpcServer = new GrpcServer(db, new InetSocketAddress("127.0.0.1", 0));
        grpcServer.start();
        grpcClient = GrpcConnection.forHostAndPort("non-thrift-regression",
                new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
        grpcChannel = NettyChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
                .usePlaintext()
                .build();
    }

    private static ColumnSchema fixedSchema() {
        return ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
    }

    private static ColumnSchema fixedSchemaWithMergeOperator() {
        return ColumnSchema.of(IntList.of(Integer.BYTES),
                ObjectList.of(),
                true,
                null,
                null,
                MyStringAppendOperator.class.getName());
    }

    private static ColumnSchema bucketedSchema() {
        return ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(ColumnHashType.ALLSAME8), true);
    }

    private static ColumnSchema bucketedSchemaWithMergeOperator() {
        return ColumnSchema.of(IntList.of(Integer.BYTES),
                ObjectList.of(ColumnHashType.ALLSAME8),
                true,
                null,
                null,
                MyStringAppendOperator.class.getName());
    }

    private static Keys key(Buf... keys) {
        return new Keys(keys);
    }

    private static Buf intBytes(int value) {
        byte[] bytes = new byte[Integer.BYTES];
        bytes[0] = (byte) (value >>> 24);
        bytes[1] = (byte) (value >>> 16);
        bytes[2] = (byte) (value >>> 8);
        bytes[3] = (byte) value;
        return Buf.wrap(bytes);
    }

    private static Buf bytes(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String string(Buf value) {
        return new String(value.toByteArray(), StandardCharsets.UTF_8);
    }

    private static void assertBufEquals(Buf expected, Buf actual) {
        assertTrue(Utils.valueEquals(expected, actual),
                () -> "Expected " + string(expected) + ", got " + (actual == null ? "null" : string(actual)));
    }

    private static void assertGrpcFailure(Mono<Empty> call) {
        var ex = assertThrows(StatusRuntimeException.class, () -> call.block(Duration.ofSeconds(5)));
        assertNotEquals(Status.Code.OK, ex.getStatus().getCode());
    }

    private static void assertGrpcInvalidRequest(Mono<Empty> call, String message) {
        var ex = assertThrows(StatusRuntimeException.class, () -> call.block(Duration.ofSeconds(5)));
        assertEquals(Status.Code.INTERNAL, ex.getStatus().getCode());
        assertNotNull(ex.getStatus().getDescription());
        assertTrue(ex.getStatus().getDescription().contains("RocksDBError: [uid:PUT_INVALID_REQUEST]"));
        assertTrue(ex.getStatus().getDescription().contains(message));
    }

    private void assertNoResourceDrift(String scenario) {
        assertEquals(0, db.getInternalDB().getPendingOpsCount(), scenario + " must leave pending ops exactly balanced");
        assertEquals(0, db.getInternalDB().getOpenTransactionsCount(), scenario + " must not leave open transactions");
        assertEquals(0, db.getInternalDB().getOpenIteratorsCount(), scenario + " must not leave open iterators");
    }
}
