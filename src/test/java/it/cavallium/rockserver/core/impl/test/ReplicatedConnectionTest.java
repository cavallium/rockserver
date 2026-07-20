package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.ReplicatedConnection;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionPointReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteSubmitter;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandApplier;
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMembershipWorkflowPlanner;
import it.cavallium.rockserver.core.cluster.workflow.PartitionTopologyWorkflowPlanner;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.publisher.Flux;

import org.junit.jupiter.api.Test;

class ReplicatedConnectionTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ClientId CLIENT_ID = new ClientId(new UUID(0, 9));
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");
    private static final ShardId PARTITION = new ShardId(7);

    @Test
    void publicNoResultWriteRoutesThroughPartitionSubmitter() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.none());
        }

        assertEquals(1, resolver.client.proposals.size());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
        assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
        assertEquals(ReplicatedCommandKind.REPLICATED_WRITE, resolver.targets.getFirst().commandKind());
        assertTrue(resolver.closed);

        var command = PartitionWriteCommandCodec.decode(resolver.client.proposals.getFirst());
        assertEquals(CLIENT_ID, command.requestId().clientId());
        assertEquals(0, command.requestId().sequence());
        assertEquals(3, command.schemaEpoch());
        assertEquals(1, command.mutations().size());
        var mutation = command.mutations().getFirst();
        assertEquals(ReplicatedMutationOp.PUT, mutation.operation());
        assertEquals(4, mutation.columnId());
        assertEquals(keys("k"), mutation.keys());
        assertArrayEquals(bytes("value"), mutation.requiredValue().toByteArray());
    }

    @Test
    void publicResponseProducingSingleWritesDecodeCommittedResponses() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.proposalResponses.add(writeResponse(buf("previous")));
        resolver.client.proposalResponses.add(writeResponse(new Delta<>(buf("old"), buf("new"))));
        resolver.client.proposalResponses.add(writeResponse(true));
        resolver.client.proposalResponses.add(writeResponse(false));
        resolver.client.proposalResponses.add(writeResponse(buf("merged")));
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            Buf previous = api.put(0, 4, keys("p"), buf("value"), RequestType.previous());
            Delta<Buf> delta = api.put(0, 4, keys("d"), buf("new"), RequestType.delta());
            boolean changed = api.put(0, 4, keys("c"), buf("value"), RequestType.changed());
            boolean existed = api.delete(0, 4, keys("x"), RequestType.previousPresence());
            Buf merged = api.merge(0, 4, keys("m"), buf("operand"), RequestType.merged());
            assertNotNull(delta.previous());
            assertNotNull(delta.current());

            assertArrayEquals(bytes("previous"), previous.toByteArray());
            assertArrayEquals(bytes("old"), delta.previous().toByteArray());
            assertArrayEquals(bytes("new"), delta.current().toByteArray());
            assertTrue(changed);
            assertFalse(existed);
            assertArrayEquals(bytes("merged"), merged.toByteArray());
        }

        assertEquals(5, resolver.client.proposals.size());
        assertEquals(PartitionWriteResponseMode.PREVIOUS_VALUE, responseMode(resolver.client.proposals.getFirst()));
        assertEquals(PartitionWriteResponseMode.DELTA, responseMode(resolver.client.proposals.get(1)));
        assertEquals(PartitionWriteResponseMode.CHANGED, responseMode(resolver.client.proposals.get(2)));
        assertEquals(PartitionWriteResponseMode.PREVIOUS_PRESENCE, responseMode(resolver.client.proposals.get(3)));
        assertEquals(PartitionWriteResponseMode.MERGED_VALUE, responseMode(resolver.client.proposals.get(4)));
    }

    @Test
    void publicResponseProducingMultiWritesDecodeCommittedResponses() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.proposalResponses.add(writeResponse(buf("previous-a"), null));
        resolver.client.proposalResponses.add(writeResponse(true, false));
        resolver.client.proposalResponses.add(writeResponse(buf("merged-a"), buf("merged-b")));
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            var previous = api.putMulti(
                    0,
                    4,
                    List.of(keys("a"), keys("b")),
                    List.of(buf("value-a"), buf("value-b")),
                    RequestType.previous()
            );
            var existed = api.deleteMulti(
                    0,
                    4,
                    List.of(keys("a"), keys("b")),
                    RequestType.previousPresence()
            );
            var merged = api.mergeMulti(
                    0,
                    4,
                    List.of(keys("a"), keys("b")),
                    List.of(buf("operand-a"), buf("operand-b")),
                    RequestType.merged()
            );

            assertEquals(2, previous.size());
            assertArrayEquals(bytes("previous-a"), previous.getFirst().toByteArray());
            assertNull(previous.get(1));
            assertEquals(List.of(true, false), existed);
            assertEquals(2, merged.size());
            assertArrayEquals(bytes("merged-a"), merged.getFirst().toByteArray());
            assertArrayEquals(bytes("merged-b"), merged.get(1).toByteArray());
        }

        assertEquals(3, resolver.client.proposals.size());
        assertEquals(PartitionWriteResponseMode.PREVIOUS_VALUE, responseMode(resolver.client.proposals.getFirst()));
        assertEquals(PartitionWriteResponseMode.PREVIOUS_PRESENCE, responseMode(resolver.client.proposals.get(1)));
        assertEquals(PartitionWriteResponseMode.MERGED_VALUE, responseMode(resolver.client.proposals.get(2)));
    }

    @Test
    void publicAsyncResponseProducingWritesDecodeCommittedResponses() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.proposalResponses.add(writeResponse(buf("previous")));
        resolver.client.proposalResponses.add(writeResponse(true, false));
        try (var connection = connection(resolver)) {
            var api = connection.getAsyncApi();

            Buf previous = api.putAsync(0, 4, keys("p"), buf("value"), RequestType.previous())
                    .get(5, TimeUnit.SECONDS);
            List<Boolean> existed = api.deleteMultiAsync(
                            0,
                            4,
                            List.of(keys("a"), keys("b")),
                            RequestType.previousPresence()
                    )
                    .get(5, TimeUnit.SECONDS);

            assertArrayEquals(bytes("previous"), previous.toByteArray());
            assertEquals(List.of(true, false), existed);
        }

        assertEquals(2, resolver.client.proposals.size());
        assertEquals(PartitionWriteResponseMode.PREVIOUS_VALUE, responseMode(resolver.client.proposals.getFirst()));
        assertEquals(PartitionWriteResponseMode.PREVIOUS_PRESENCE, responseMode(resolver.client.proposals.get(1)));
    }

    @Test
    void publicResponseProducingWriteFailsWhenCommittedResponseIsMissing() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.previous()));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("without persisted response payload"));
        }
    }

    @Test
    void publicResponseProducingWriteFailsWhenCommittedResponseIsMalformed() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.proposalResponses.add(new byte[]{1, 2, 3});
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.previous()));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("invalid response payload"));
        }
    }

    @Test
    void publicAsyncResponseProducingWriteFailsWhenCommittedResponseIsMalformed() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.proposalResponses.add(new byte[]{1, 2, 3});
        try (var connection = connection(resolver)) {
            var failed = connection.getAsyncApi()
                    .putAsync(0, 4, keys("k"), buf("value"), RequestType.previous());

            var error = assertThrows(ExecutionException.class, () -> failed.get(5, TimeUnit.SECONDS));
            var cause = assertInstanceOf(RocksDBException.class, error.getCause());
            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED, cause.getErrorUniqueId());
            assertTrue(cause.getMessage().contains("invalid response payload"));
        }
    }

    @Test
    void publicWriteRejectsNullAdapterProposalFuture() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.returnNullProposal = true;
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.none()));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("proposal returned null future"));
        }

        assertEquals(1, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicWriteRejectsNullAdapterProposalCommit() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.returnNullCommit = true;
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.none()));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("proposal returned null commit"));
        }

        assertEquals(1, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicSafeBatchWritesExpandIntoReplicatedMutations() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();
            api.putBatch(4, Flux.just(batch(
                    List.of(keys("a"), keys("b")),
                    List.of(buf("value-a"), buf("value-b"))
            )), PutBatchMode.WRITE_BATCH);
            api.mergeBatch(4, Flux.just(batch(
                    List.of(keys("c")),
                    List.of(buf("merge-c"))
            )), MergeBatchMode.MERGE_WRITE_BATCH);
        }

        assertEquals(2, resolver.client.proposals.size());
        var putCommand = PartitionWriteCommandCodec.decode(resolver.client.proposals.getFirst());
        assertEquals(2, putCommand.mutations().size());
        var firstPutMutation = putCommand.mutations().getFirst();
        assertEquals(ReplicatedMutationOp.PUT, firstPutMutation.operation());
        assertEquals(keys("a"), firstPutMutation.keys());
        assertArrayEquals(bytes("value-a"), firstPutMutation.requiredValue().toByteArray());
        assertEquals(ReplicatedMutationOp.PUT, putCommand.mutations().get(1).operation());
        assertEquals(keys("b"), putCommand.mutations().get(1).keys());
        assertArrayEquals(bytes("value-b"), putCommand.mutations().get(1).requiredValue().toByteArray());

        var mergeCommand = PartitionWriteCommandCodec.decode(resolver.client.proposals.get(1));
        assertEquals(1, mergeCommand.mutations().size());
        var merge = mergeCommand.mutations().getFirst();
        assertEquals(ReplicatedMutationOp.MERGE, merge.operation());
        assertEquals(keys("c"), merge.keys());
        assertArrayEquals(bytes("merge-c"), merge.requiredValue().toByteArray());
    }

    @Test
    void publicNoopWritesReturnWithoutAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            var putResult = api.putMulti(0, 4, List.of(), List.of(), RequestType.previous());
            var deleteResult = api.deleteMulti(0, 4, List.of(), RequestType.previousPresence());
            var mergeResult = api.mergeMulti(0, 4, List.of(), List.of(), RequestType.merged());
            api.putBatch(4, Flux.empty(), PutBatchMode.WRITE_BATCH);
            api.mergeBatch(4, Flux.just(batch(List.of(), List.of())), MergeBatchMode.MERGE_WRITE_BATCH);

            assertEquals(List.of(), putResult);
            assertEquals(List.of(), deleteResult);
            assertEquals(List.of(), mergeResult);
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicNoopWriteToUnknownColumnFailsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            var multiError = assertThrows(RocksDBException.class,
                    () -> api.putMulti(0, 404, List.of(), List.of(), RequestType.none()));
            var batchError = assertThrows(RocksDBException.class,
                    () -> api.putBatch(404, Flux.empty(), PutBatchMode.WRITE_BATCH));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, multiError.getErrorUniqueId());
            assertTrue(multiError.getMessage().contains("unknown column: 404"));
            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, batchError.getErrorUniqueId());
            assertTrue(batchError.getMessage().contains("unknown column: 404"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicBatchPublisherFailureStaysTypedBeforeAdapterDispatch() throws Exception {
        var publisherFailure = new IllegalStateException("boom");
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().putBatch(
                            4,
                            Flux.error(publisherFailure),
                            PutBatchMode.WRITE_BATCH
                    ));

            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("replicated write batch publisher failed: boom"));
            assertSame(publisherFailure, error.getCause());
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicWriteNullFieldsStayTypedBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var syncError = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 4, null, buf("value"), RequestType.none()));
            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, syncError.getErrorUniqueId());
            assertTrue(syncError.getMessage().contains("put keys must not be null"));

            var asyncFailure = assertThrows(ExecutionException.class,
                    () -> connection.getAsyncApi()
                            .putAsync(0, 4, keys("a"), null, RequestType.none())
                            .get(5, TimeUnit.SECONDS));
            var asyncError = assertInstanceOf(RocksDBException.class, asyncFailure.getCause());
            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, asyncError.getErrorUniqueId());
            assertTrue(asyncError.getMessage().contains("put value must not be null"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCrossPartitionMultiKeyWriteFailsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadataWithTwoPartitions(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().putMulti(
                            0,
                            4,
                            List.of(keys("a"), keys("z")),
                            List.of(buf("left"), buf("right")),
                            RequestType.none()
                    ));

            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("routing constraints"));
            assertTrue(error.getMessage().contains("outside the target partition"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicWriteToUnknownColumnFailsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().put(0, 404, keys("k"), buf("value"), RequestType.none()));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("unknown column: 404"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicPointReadFromUnknownColumnFailsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().get(0, 404, keys("k"), RequestType.current()));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("unknown column: 404"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicRangeReadFromUnknownColumnFailsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi()
                            .reduceRange(0, 404, keys("a"), keys("z"), false, RequestType.entriesCount(), 1000));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("unknown column: 404"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void constructorRejectsNullInputsWithExplicitDiagnostics() {
        var resolver = new RecordingResolver();

        assertIllegalArgument("metadata must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                null,
                resolver,
                resolver,
                resolver
        ));
        assertIllegalArgument("writeResolver must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                null,
                resolver,
                resolver
        ));
        assertIllegalArgument("pointReadResolver must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                null,
                resolver
        ));
        assertIllegalArgument("rangeReadResolver must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                null
        ));
        assertIllegalArgument("clientId must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                null,
                3,
                () -> 0
        ));
        assertIllegalArgument("currentTimeMillis must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                null
        ));
        assertIllegalArgument("metadataUpdateListener must not be null", () -> new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                null
        ));
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void publicEntrypointsRejectNullInputsWithExplicitDiagnostics() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            assertIllegalArgument("req must not be null", () -> connection.getSyncApi().requestSync(null));
            assertIllegalArgument("plan must not be null", () -> connection.applyPartitionMembershipChange(null));
            assertIllegalArgument("plan must not be null", () -> connection.applyPartitionSplit(null));
            assertIllegalArgument("plan must not be null", () -> connection.applyPartitionMerge(null));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void publicAsyncSubmissionIsBoundedAndRecoversCapacity() throws Exception {
        var resolver = new BlockingResolver(64);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var api = connection.getAsyncApi();
            var active = new ArrayList<CompletableFuture<Void>>();
            for (var i = 0; i < 64; i++) {
                active.add(api.putAsync(0, 4, keys("k"), buf("value"), RequestType.none()));
            }
            assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

            assertEquals(4L, api.getColumnIdAsync("col").get(5, TimeUnit.SECONDS));
            assertEquals(ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                    api.getAllColumnDefinitionsAsync().get(5, TimeUnit.SECONDS).get("col"));
            assertNull(api.checkMergeOperatorAsync("merge", new byte[]{1}).get(5, TimeUnit.SECONDS));
            assertNull(api.getAsync(0, 4, keys("k"), RequestType.none()).get(5, TimeUnit.SECONDS));
            assertEquals(64, resolver.resolveCount.get());

            var rejected = api.putAsync(0, 4, keys("k"), buf("value"), RequestType.none());
            var error = assertThrows(ExecutionException.class, () -> rejected.get(5, TimeUnit.SECONDS));
            assertEquals("Replicated connection async request executor is saturated", error.getCause().getMessage());

            resolver.release.countDown();
            for (var request : active) {
                request.get(5, TimeUnit.SECONDS);
            }
            assertEquals(4L, api.getColumnIdAsync("col").get(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void unsupportedPublicAsyncOperationsFailBeforeExecutorAdmissionWhenSaturated() throws Exception {
        var resolver = new BlockingResolver(64);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var api = connection.getAsyncApi();
            var active = new ArrayList<CompletableFuture<Void>>();
            for (var i = 0; i < 64; i++) {
                active.add(api.putAsync(0, 4, keys("k"), buf("value"), RequestType.none()));
            }
            assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

            var openTransaction = api.openTransactionAsync(1000);
            var uploadMergeOperator = api.uploadMergeOperatorAsync("merge", "Operator", new byte[]{1});

            assertTrue(openTransaction.isDone());
            assertTrue(uploadMergeOperator.isDone());
            assertAsyncNotImplemented(openTransaction);
            assertAsyncNotImplemented(uploadMergeOperator);

            var invalidReduceRange = api.reduceRangeAsync(0, 4, keys("a"), keys("z"),
                    false, RequestType.entriesCount(), -1);
            var invalidReduceFailure = assertThrows(ExecutionException.class,
                    () -> invalidReduceRange.get(5, TimeUnit.SECONDS));
            assertInstanceOf(IllegalArgumentException.class, invalidReduceFailure.getCause());
            assertEquals("timeoutMs must be >= 0", invalidReduceFailure.getCause().getMessage());

            var invalidRangeFailure = assertThrows(IllegalArgumentException.class,
                    () -> Flux.from(api.getRangeAsync(0, 4, keys("a"), keys("z"),
                                    false, RequestType.allInRange(), -1))
                            .blockLast(java.time.Duration.ofSeconds(5)));
            assertEquals("timeoutMs must be >= 0", invalidRangeFailure.getMessage());
            assertEquals(64, resolver.resolveCount.get());

            resolver.release.countDown();
            for (var request : active) {
                request.get(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void publicAsyncRangeReadSubmissionIsBounded() throws Exception {
        var resolver = new BlockingResolver(64);
        var active = new ArrayList<reactor.core.Disposable>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var api = connection.getAsyncApi();
            for (var i = 0; i < 64; i++) {
                active.add(Flux.from(api.getRangeAsync(0, 4, keys("a"), keys("z"),
                        false, RequestType.allInRange(), 1000)).subscribe());
            }
            assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

            var rejected = assertThrows(IllegalStateException.class,
                    () -> Flux.from(api.getRangeAsync(0, 4, keys("a"), keys("z"),
                                    false, RequestType.allInRange(), 1000))
                            .blockLast(java.time.Duration.ofSeconds(5)));

            assertEquals("Replicated connection async request executor is saturated", rejected.getMessage());
            assertInstanceOf(java.util.concurrent.RejectedExecutionException.class, rejected.getCause());
        } finally {
            for (var subscription : active) {
                subscription.dispose();
            }
            resolver.release.countDown();
        }
    }

    @Test
    void closeStopsAsyncWorkAndRejectsLaterRequests() throws Exception {
        var resolver = new BlockingResolver(1, false);
        var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        );
        var pending = connection.getAsyncApi().putAsync(0, 4, keys("k"), buf("value"), RequestType.none());
        assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

        connection.close();
        connection.close();

        assertEquals(1, resolver.closeCount.get());
        assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS));
        var syncError = assertThrows(IllegalStateException.class,
                () -> connection.getSyncApi().getColumnId("col"));
        assertEquals("Replicated connection is closed", syncError.getMessage());
        var asyncError = assertThrows(ExecutionException.class,
                () -> connection.getAsyncApi().getColumnIdAsync("col").get(5, TimeUnit.SECONDS));
        assertEquals("Replicated connection is closed", asyncError.getCause().getMessage());
    }

    @Test
    void closeRejectsLaterAsyncRangeSubscriptionsBeforeDispatch() throws Exception {
        var resolver = new RecordingResolver();
        var connection = connection(resolver);

        connection.close();
        connection.close();
        var error = assertThrows(IllegalStateException.class,
                () -> Flux.from(connection.getAsyncApi()
                                .getRangeAsync(0, 4, keys("a"), keys("z"),
                                        false, RequestType.allInRange(), 1000))
                        .blockLast(java.time.Duration.ofSeconds(5)));

        assertEquals("Replicated connection is closed", error.getMessage());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void closeWaitsAcrossInterruptionForAsyncWorkerTerminationAndRestoresInterrupt() throws Exception {
        var resolver = new UninterruptibleBlockingResolver();
        var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        );
        var closeEntered = new CountDownLatch(1);
        var closeFailure = new AtomicReference<Throwable>();
        var interruptRestored = new AtomicBoolean();
        var pending = connection.getAsyncApi().putAsync(0, 4, keys("k"), buf("value"), RequestType.none());
        assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

        var closeThread = new Thread(() -> {
            closeEntered.countDown();
            try {
                connection.close();
            } catch (Throwable e) {
                closeFailure.set(e);
            }
            if (Thread.currentThread().isInterrupted()) {
                interruptRestored.set(true);
            }
        }, "replicated-connection-close-test");
        closeThread.start();
        try {
            assertTrue(closeEntered.await(1, TimeUnit.SECONDS));
            TimeUnit.MILLISECONDS.sleep(50);
            assertTrue(closeThread.isAlive());
            closeThread.interrupt();
            TimeUnit.MILLISECONDS.sleep(50);
            assertTrue(closeThread.isAlive(), "close returned before interrupted async worker shutdown completed");
        } finally {
            resolver.release.countDown();
        }
        closeThread.join(1_000);

        assertFalse(closeThread.isAlive());
        assertNull(closeFailure.get());
        assertTrue(interruptRestored.get());
        assertEquals(1, resolver.closeCount.get());
        assertTrue(pending.isDone());
        connection.close();
    }

    @Test
    void cancelingPublicAsyncWriteCancelsPendingProposal() throws Exception {
        var resolver = new PendingProposalResolver();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var pending = connection.getAsyncApi().putAsync(0, 4, keys("k"), buf("value"), RequestType.none());
            assertTrue(resolver.client.started.await(5, TimeUnit.SECONDS));

            assertTrue(pending.cancel(true));

            assertTrue(resolver.client.canceled.await(5, TimeUnit.SECONDS));
            assertTrue(resolver.client.proposal.isCancelled());
        }
    }

    @Test
    void cancelingPublicAsyncRangeReadCancelsPendingDispatch() throws Exception {
        var resolver = new BlockingResolver(1, false);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var subscription = Flux.from(connection.getAsyncApi()
                            .getRangeAsync(0, 4, keys("a"), keys("z"),
                                    false, RequestType.allInRange(), 1000))
                    .subscribe();
            assertTrue(resolver.started.await(5, TimeUnit.SECONDS));

            subscription.dispose();

            assertTrue(resolver.interrupted.await(5, TimeUnit.SECONDS));
        } finally {
            resolver.release.countDown();
        }
    }

    @Test
    void closeRetriesOwnedResourceRuntimeCleanupFailure() throws Exception {
        var resolver = new RecordingResolver();
        var cleanupFailure = new IllegalStateException("cleanup failed");
        resolver.closeFailure = cleanupFailure;
        var connection = connection(resolver);

        var failure = assertThrows(IOException.class, connection::close);

        assertEquals("Failed to close replicated connection", failure.getMessage());
        assertSame(cleanupFailure, failure.getCause());
        assertEquals(1, resolver.closeCount);

        resolver.closeFailure = null;
        connection.close();
        connection.close();

        assertEquals(2, resolver.closeCount);
    }

    @Test
    void closeRetriesOwnedResourceFatalCleanupFailure() throws Exception {
        var resolver = new RecordingResolver();
        var cleanupFailure = new AssertionError("cleanup failed");
        resolver.closeFailure = cleanupFailure;
        var connection = connection(resolver);

        assertSame(cleanupFailure, assertThrows(AssertionError.class, connection::close));
        assertEquals(1, resolver.closeCount);

        resolver.closeFailure = null;
        connection.close();
        connection.close();

        assertEquals(2, resolver.closeCount);
    }

    @Test
    void publicCurrentPointReadRoutesThroughRatisQueryPath() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(pointReadResponse(buf("value")));
        resolver.client.queryResponses.add(pointReadResponse(buf("value")));
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            Buf value = api.get(0, 4, keys("k"), RequestType.current());
            boolean exists = api.get(0, 4, keys("k"), RequestType.exists());

            assertArrayEquals(bytes("value"), value.toByteArray());
            assertTrue(exists);
            assertEquals(4, api.getColumnId("col"));
            assertEquals(ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                    api.getAllColumnDefinitions().get("col"));
        }

        assertEquals(2, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(ReplicatedCommandKind.ROUTED_READ, resolver.targets.getFirst().commandKind());

        var command = PartitionPointReadCommandCodec.decode(resolver.client.queries.getFirst());
        assertEquals(3, command.schemaEpoch());
        assertEquals(4, command.columnId());
        assertEquals(keys("k"), command.keys());
    }

    @Test
    void publicPointReadDistinguishesMissingFromEmptyValue() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(pointReadResponse(buf("")));
        resolver.client.queryResponses.add(pointReadResponse(buf("")));
        resolver.client.queryResponses.add(pointReadResponse(null));
        resolver.client.queryResponses.add(pointReadResponse(null));
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            Buf empty = api.get(0, 4, keys("e"), RequestType.current());
            boolean emptyExists = api.get(0, 4, keys("e"), RequestType.exists());
            Buf missing = api.get(0, 4, keys("m"), RequestType.current());
            boolean missingExists = api.get(0, 4, keys("m"), RequestType.exists());

            assertArrayEquals(new byte[0], empty.toByteArray());
            assertTrue(emptyExists);
            assertNull(missing);
            assertFalse(missingExists);
        }

        assertEquals(4, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void publicPointReadNullFieldsStayTypedBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            assertReadFailure(
                    () -> connection.getSyncApi().get(0, 4, null, RequestType.current()),
                    "point read keys must not be null"
            );

            var asyncNullRequestType = connection.getAsyncApi().getAsync(0, 4, keys("a"), null);
            assertAsyncReadFailure(asyncNullRequestType, "point read requestType must not be null");

            assertReadFailure(
                    () -> connection.getSyncApi().get(0, 4, new Keys((Buf) null), RequestType.exists()),
                    "point read keys part 0 must not be null"
            );
        }

        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicPointReadRejectsMissingAdapterResponsePayload() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().get(0, 4, keys("m"), RequestType.current()));

            assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("point read response payload is missing"));
        }

        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicPointReadRejectsNullAdapterQueryFuture() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.returnNullQuery = true;
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().get(0, 4, keys("m"), RequestType.current()));

            assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("returned null query future"));
        }

        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicRangeReadRoutesThroughRatisQueryPath() throws Exception {
        var resolver = new RecordingResolver();
        var left = kv("a", "left");
        var right = kv("b", "right");
        resolver.client.queryResponses.add(rangeReadResponse(left, right));
        resolver.client.queryResponses.add(rangeReadResponse(left, right));
        try (var connection = connection(resolver)) {
            var syncEntries = connection.getSyncApi()
                    .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)
                    .toList();
            var asyncEntries = Flux.from(connection.getAsyncApi()
                            .getRangeAsync(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000))
                    .collectList()
                    .block(java.time.Duration.ofSeconds(5));

            assertEquals(List.of(left, right), syncEntries);
            assertEquals(List.of(left, right), asyncEntries);
        }

        assertEquals(2, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(ReplicatedCommandKind.ROUTED_READ, resolver.targets.getFirst().commandKind());

        var command = PartitionRangeReadCommandCodec.decode(resolver.client.queries.getFirst());
        assertEquals(3, command.schemaEpoch());
        assertEquals(4, command.columnId());
        assertEquals(keys("a"), command.startKeysInclusive());
        assertEquals(keys("z"), command.endKeysExclusive());
        assertFalse(command.reverse());
    }

    @Test
    void publicRangeReadStreamsMultiPartitionSegmentsLazily() throws Exception {
        var resolver = new RecordingResolver();
        var left = kv("a", "left");
        var right = kv("z", "right");
        var rightPartition = new ShardId(8);
        resolver.client.queryResponses.add(rangeReadResponse(left));
        resolver.client.queryResponses.add(rangeReadResponse(right));
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadataWithTwoPartitions(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        );
             var stream = connection.getSyncApi()
                     .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)) {
            assertEquals(0, resolver.client.queries.size());

            var iterator = stream.iterator();
            assertTrue(iterator.hasNext());
            assertEquals(left, iterator.next());
            assertEquals(1, resolver.client.queries.size());
            assertEquals(PARTITION, resolver.targets.getFirst().shardId());

            assertTrue(iterator.hasNext());
            assertEquals(right, iterator.next());
            assertEquals(2, resolver.client.queries.size());
            assertEquals(rightPartition, resolver.targets.get(1).shardId());
            assertFalse(iterator.hasNext());
            assertThrows(NoSuchElementException.class, iterator::next);
        }

        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicRangeReadStreamRejectsFetchAfterConnectionClose() throws Exception {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(rangeReadResponse(kv("a", "left")));
        try (var connection = connection(resolver);
             var stream = connection.getSyncApi()
                     .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)) {
            connection.close();

            var iterator = stream.iterator();
            var error = assertThrows(IllegalStateException.class, iterator::hasNext);
            assertEquals("Replicated connection is closed", error.getMessage());
        }

        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(1, resolver.closeCount);
    }

    @Test
    void publicRangeReadTimeoutsAreRelativeDurations() throws Exception {
        var resolver = new RecordingResolver();
        var left = kv("a", "left");
        var right = kv("b", "right");
        resolver.client.queryResponses.add(rangeReadResponse(left, right));
        resolver.client.queryResponses.add(rangeReadResponse(left, right));
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 10_000
        )) {
            var entries = connection.getSyncApi()
                    .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)
                    .toList();
            var count = connection.getSyncApi()
                    .reduceRange(0, 4, keys("a"), keys("z"), false, RequestType.entriesCount(), 1000);

            assertEquals(List.of(left, right), entries);
            assertEquals(2L, count);
        }

        assertEquals(2, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void publicRangeReadNullFieldsStayTypedBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            assertReadFailure(
                    () -> connection.getSyncApi()
                            .reduceRange(0, 4, keys("a"), keys("z"), false, null, 1000),
                    "reduceRange requestType must not be null"
            );

            assertReadFailure(
                    () -> connection.getSyncApi()
                            .getRange(0, 4, new Keys((Buf[]) null), keys("z"),
                                    false, RequestType.allInRange(), 1000),
                    "range read start keys parts must not be null"
            );

            var asyncNullRequestType = assertThrows(RocksDBException.class,
                    () -> Flux.from(connection.getAsyncApi()
                                    .getRangeAsync(0, 4, keys("a"), keys("z"), false, null, 1000))
                            .blockLast(java.time.Duration.ofSeconds(5)));
            assertEquals(RocksDBException.RocksDBErrorType.GET_1, asyncNullRequestType.getErrorUniqueId());
            assertTrue(asyncNullRequestType.getMessage().contains("getRange requestType must not be null"));

            assertReadFailure(
                    () -> connection.getSyncApi()
                            .getRange(0, 4, keys("a"), new Keys((Buf) null),
                                    false, RequestType.allInRange(), 1000),
                    "range read end keys part 0 must not be null"
            );

            assertReadFailure(
                    () -> connection.getSyncApi()
                            .getRange(0, -1, keys("a"), keys("z"),
                                    false, RequestType.allInRange(), 1000),
                    "columnId must be >= 0"
            );
        }

        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicRangeReadRejectsMissingAdapterResponsePayload() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi()
                            .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)
                            .toList());

            assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("range read segment response payload is missing"));
        }

        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicAsyncRangeReadRejectsMissingAdapterResponsePayload() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> Flux.from(connection.getAsyncApi()
                                    .getRangeAsync(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000))
                            .collectList()
                            .block(java.time.Duration.ofSeconds(5)));

            assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("range read segment response payload is missing"));
        }

        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicRangeReadRejectsSegmentAboveRequestedEntryLimit() throws Exception {
        var resolver = new RecordingResolver();
        var entries = new ArrayList<KV>();
        for (int i = 0; i < 16_385; i++) {
            entries.add(kv("a", "value"));
        }
        resolver.client.queryResponses.add(rangeReadResponse(entries));
        try (var connection = connection(resolver)) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi()
                            .getRange(0, 4, keys("a"), keys("z"), false, RequestType.allInRange(), 1000)
                            .toList());

            assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("exceeds limit"));
        }

        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicReduceRangeUsesOrderedSegmentResponses() throws Exception {
        var resolver = new RecordingResolver();
        var first = kv("a", "left");
        var second = kv("b", "middle");
        var third = kv("c", "right");
        resolver.client.queryResponses.add(rangeReadResponse(first, second, third));
        resolver.client.queryResponses.add(rangeReadResponse(first, second, third));
        try (var connection = connection(resolver)) {
            var count = connection.getSyncApi()
                    .reduceRange(0, 4, keys("a"), keys("z"), false, RequestType.entriesCount(), 1000);
            var firstAndLast = connection.getSyncApi()
                    .reduceRange(0, 4, keys("a"), keys("z"), false, RequestType.firstAndLast(), 1000);

            assertEquals(3L, count);
            assertEquals(new FirstAndLast<>(first, third), firstAndLast);
        }

        assertEquals(2, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void publicCreateAndDeleteColumnCanUseInjectedMetadataSubmitter() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            var api = connection.getSyncApi();

            var columnId = api.createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            assertEquals(5, columnId);
            assertEquals(5, api.getColumnId("new-col"));

            api.deleteColumn(columnId);
            var missing = assertThrows(RocksDBException.class, () -> api.getColumnId("new-col"));
            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, missing.getErrorUniqueId());
        }

        assertEquals(2, submitted.size());
        var createColumn = (MetadataCommand.CreateColumn) submitted.getFirst();
        assertEquals(7, createColumn.expectedMetadataEpoch());
        assertEquals(5, createColumn.column().columnId());
        assertEquals("new-col", createColumn.column().name());
        var deleteColumn = (MetadataCommand.DeleteColumn) submitted.get(1);
        assertEquals(8, deleteColumn.expectedMetadataEpoch());
        assertEquals(5, deleteColumn.columnId());
        assertEquals(2, refreshed.size());
        assertEquals(committed[0], refreshed.getLast());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void publicCreateColumnInvalidFieldsFailBeforeMetadataSubmission() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    submitted.add(command);
                    return MetadataCommandApplier.apply(metadata(), command);
                }
        )) {
            var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
            var localMergeOperatorSchema = ColumnSchema.of(
                    IntList.of(1),
                    ObjectList.of(),
                    true,
                    null,
                    null,
                    "it.cavallium.LocalMergeOperator"
            );

            assertColumnCreateFailure(
                    () -> connection.getSyncApi().createColumn(null, schema),
                    "column name must not be null"
            );
            assertColumnCreateFailure(
                    () -> connection.getSyncApi().createColumn(" ", schema),
                    "column name must not be blank"
            );
            assertColumnCreateFailure(
                    () -> connection.getSyncApi().createColumn("bad\nname", schema),
                    "column name must be a single line"
            );
            assertColumnCreateFailure(
                    () -> connection.getSyncApi().createColumn("local-merge", localMergeOperatorSchema),
                    "local mergeOperatorClass is not allowed in replicated metadata"
            );

            var asyncNullSchema = connection.getAsyncApi().createColumnAsync("null-schema", null);
            assertAsyncColumnCreateFailure(asyncNullSchema, "column schema must not be null");
        }

        assertEquals(0, submitted.size());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnPropagatesWrappedFatalMetadataSubmitterFailure() throws Exception {
        var resolver = new RecordingResolver();
        var fatalFailure = new AssertionError("fatal metadata submit failed");
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    assertInstanceOf(MetadataCommand.CreateColumn.class, command);
                    throw new CompletionException(new CompletionException(fatalFailure));
                }
        )) {
            var failure = assertThrows(AssertionError.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertSame(fatalFailure, failure);
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnWrapsCheckedMetadataSubmitterFailureWithDiagnostic() throws Exception {
        var resolver = new RecordingResolver();
        var checkedFailure = new IOException("checked metadata submit failed");
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    assertInstanceOf(MetadataCommand.CreateColumn.class, command);
                    throw new CompletionException(new CompletionException(checkedFailure));
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("replicated metadata command failed"));
            var cause = assertInstanceOf(CompletionException.class, error.getCause());
            assertEquals("replicated metadata command failed", cause.getMessage());
            assertSame(checkedFailure, cause.getCause());
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicMetadataSubmitterNullCommitFailsWithExplicitContractDiagnostic() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        var refreshed = new ArrayList<ClusterMetadata>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    return null;
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("metadata submitter committed metadata must not be null"));
            assertInstanceOf(IllegalStateException.class, error.getCause());
        }

        assertEquals(1, submitted.size());
        assertInstanceOf(MetadataCommand.CreateColumn.class, submitted.getFirst());
        assertEquals(List.of(), refreshed);
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnMapsCommittedDuplicateNameToColumnExists() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    submitted.add(command);
                    throw new IllegalArgumentException("duplicate column name in cluster metadata: new-col");
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_EXISTS, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("column already exists: new-col"));
        }

        assertEquals(1, submitted.size());
        assertInstanceOf(MetadataCommand.CreateColumn.class, submitted.getFirst());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnMapsWrappedCommittedDuplicateNameToColumnExists() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        var duplicateFailure = new IllegalArgumentException("duplicate column name in cluster metadata: new-col");
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    submitted.add(command);
                    throw new CompletionException(new CompletionException(duplicateFailure));
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_EXISTS, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("column already exists: new-col"));
            assertSame(duplicateFailure, error.getCause());
        }

        assertEquals(1, submitted.size());
        assertInstanceOf(MetadataCommand.CreateColumn.class, submitted.getFirst());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicDeleteColumnMapsCommittedMissingColumnToColumnNotFound() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                },
                command -> {
                    submitted.add(command);
                    throw new IllegalArgumentException("unknown column: 4");
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi().deleteColumn(4));

            assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("unknown column: 4"));
        }

        assertEquals(1, submitted.size());
        assertInstanceOf(MetadataCommand.DeleteColumn.class, submitted.getFirst());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnPropagatesWrappedFatalMetadataRefreshFailure() throws Exception {
        var resolver = new RecordingResolver();
        var committed = new ClusterMetadata[]{metadata()};
        var fatalFailure = new AssertionError("fatal metadata refresh failed");
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                    throw new CompletionException(new CompletionException(fatalFailure));
                },
                command -> {
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            var failure = assertThrows(AssertionError.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertSame(fatalFailure, failure);
            assertEquals(5, committed[0].requireColumn(5).columnId());
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCreateColumnRefreshFailureKeepsCommitFailureKind() throws Exception {
        var resolver = new RecordingResolver();
        var committed = new ClusterMetadata[]{metadata()};
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                    throw new IllegalArgumentException("duplicate column name in cluster metadata: new-col");
                },
                command -> {
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.getSyncApi()
                    .createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("duplicate column name in cluster metadata: new-col"));
            assertEquals(5, committed[0].requireColumn(5).columnId());
            assertThrows(RocksDBException.class, () -> connection.getSyncApi().getColumnId("new-col"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicRegisterNodeAndCreatePartitionCanUseInjectedMetadataSubmitterAndRefreshRoutes() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        var committed = new ClusterMetadata[]{metadataWithoutPartitions()};
        var refreshed = new ArrayList<ClusterMetadata>();
        PartitionDescriptor createdPartition;
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            connection.registerNode(
                    N2,
                    URI.create("http://n2.public:10000"),
                    URI.create("grpc://127.0.0.2:19002"),
                    "rack-b"
            );
            createdPartition = connection.createPartition(
                    PARTITION,
                    null,
                    null,
                    1,
                    "group-7",
                    List.of(N1, N2),
                    N1,
                    Map.of("placement", "initial")
            );
            connection.getSyncApi().put(0, 4, keys("k"), buf("value"), RequestType.none());
        }

        assertEquals(2, submitted.size());
        var registerNode = (MetadataCommand.RegisterNode) submitted.getFirst();
        assertEquals(11, registerNode.expectedMetadataEpoch());
        assertEquals(N2, registerNode.nodeId());
        var createPartition = (MetadataCommand.CreatePartition) submitted.get(1);
        assertEquals(12, createPartition.expectedMetadataEpoch());
        assertEquals(PARTITION, createPartition.partition().partitionId());
        assertEquals("group-7", createPartition.partition().ratisGroupId());
        assertEquals(createdPartition, createPartition.partition());
        assertEquals(13, committed[0].epoch());
        assertEquals(N2, committed[0].requireNode(N2).nodeId());
        assertEquals(createdPartition, committed[0].requirePartition(PARTITION));
        assertEquals(createdPartition.peers(), committed[0].requireGroup(PARTITION).members());
        assertEquals(2, refreshed.size());
        assertEquals(committed[0], refreshed.getLast());
        assertEquals(1, resolver.client.proposals.size());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
    }

    @Test
    void publicMetadataWorkflowAcceptsLaterCommittedSnapshotsWhenCommandEffectIsPresent() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        var committed = new ClusterMetadata[]{metadataWithoutPartitions()};
        var refreshed = new ArrayList<ClusterMetadata>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    var applied = MetadataCommandApplier.apply(committed[0], command);
                    committed[0] = metadataAtEpoch(applied, applied.epoch() + 1);
                    return committed[0];
                }
        )) {
            connection.registerNode(
                    N2,
                    URI.create("http://n2.public:10000"),
                    URI.create("grpc://127.0.0.2:19002"),
                    "rack-b"
            );
        }

        assertEquals(1, submitted.size());
        assertEquals(13, committed[0].epoch());
        assertEquals(N2, committed[0].requireNode(N2).nodeId());
        assertEquals(List.of(13L), refreshed.stream()
                .map(ClusterMetadata::epoch)
                .toList());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicMetadataWorkflowRejectsCommittedSnapshotsWithoutCommandEffects() throws Exception {
        var resolver = new RecordingResolver();
        var unchanged = metadataWithoutPartitions();
        var refreshed = new ArrayList<ClusterMetadata>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                unchanged,
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> metadataAtEpoch(unchanged, command.expectedMetadataEpoch() + 1)
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.registerNode(
                    N2,
                    URI.create("http://n2.public:10000"),
                    URI.create("grpc://127.0.0.2:19002"),
                    "rack-b"
            ));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("REGISTER_NODE effect"));
            assertTrue(error.getMessage().contains("registered node was not committed"));
        }
        assertEquals(List.of(), refreshed);

        try (var connection = new ReplicatedConnection(
                "replicated-test",
                unchanged,
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> metadataAtEpoch(unchanged, command.expectedMetadataEpoch() + 1)
        )) {
            var error = assertThrows(RocksDBException.class, () -> connection.createPartition(
                    PARTITION,
                    null,
                    null,
                    1,
                    "group-7",
                    List.of(N1),
                    N1,
                    Map.of()
            ));

            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("CREATE_PARTITION effect"));
            assertTrue(error.getMessage().contains("partition was not committed"));
        }

        assertEquals(List.of(), refreshed);
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicPartitionMembershipChangeCanUseInjectedMetadataSubmitterAndRefreshRoutes() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionMembershipWorkflowPlanner();
        var submitted = new ArrayList<MetadataCommand>();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        PartitionDescriptor updatedPartition;
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            connection.registerNode(
                    N2,
                    URI.create("http://n2.public:10000"),
                    URI.create("grpc://127.0.0.2:19002"),
                    "rack-b"
            );
            var learner = new ReplicaMember(
                    N2,
                    URI.create("grpc://127.0.0.2:19002"),
                    ReplicaRole.LEARNER,
                    0,
                    0,
                    false
            );
            var plan = planner.planAddReplica(committed[0], committed[0].epoch(), PARTITION, 1, learner);
            updatedPartition = connection.applyPartitionMembershipChange(plan);
            connection.getSyncApi().put(0, 4, keys("m"), buf("value"), RequestType.none());
        }

        assertEquals(2, submitted.size());
        var changePeers = (MetadataCommand.ChangePartitionPeers) submitted.get(1);
        assertEquals(8, changePeers.expectedMetadataEpoch());
        assertEquals(1, changePeers.expectedPartitionEpoch());
        assertEquals(2, updatedPartition.epoch());
        assertEquals(List.of(N1, N2), updatedPartition.peers().stream()
                .map(ReplicaMember::nodeId)
                .toList());
        assertEquals(updatedPartition, committed[0].requirePartition(PARTITION));
        assertEquals(updatedPartition.peers(), committed[0].requireGroup(PARTITION).members());
        assertEquals(2, refreshed.size());
        assertEquals(committed[0], refreshed.getLast());
        assertEquals(1, resolver.client.proposals.size());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
    }

    @Test
    void publicPartitionSplitAndMergeCanUseInjectedMetadataSubmitterAndRefreshRoutes() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var submitted = new ArrayList<MetadataCommand>();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var mergedPartition = new ShardId(10);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    submitted.add(command);
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    return committed[0];
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed[0],
                    committed[0].epoch(),
                    PARTITION,
                    1,
                    leftPartition,
                    rightPartition,
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed[0].epoch(), PARTITION, 1, 10, 0)
            );
            connection.applyPartitionSplit(splitPlan);
            connection.getSyncApi().put(0, 4, keys("a"), buf("left"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right"), RequestType.none());

            var mergePlan = planner.planMerge(
                    committed[0],
                    committed[0].epoch(),
                    leftPartition,
                    rightPartition,
                    mergedPartition,
                    "group-merged",
                    new ReplicatedCdcToken(committed[0].epoch(), leftPartition, 2, 11, 0),
                    new ReplicatedCdcToken(committed[0].epoch(), rightPartition, 2, 12, 0)
            );
            connection.applyPartitionMerge(mergePlan);
            connection.getSyncApi().put(0, 4, keys("n"), buf("merged"), RequestType.none());
        }

        assertEquals(4, submitted.size());
        var split = (MetadataCommand.SplitPartition) submitted.getFirst();
        assertEquals(7, split.expectedMetadataEpoch());
        assertEquals(PARTITION, split.sourcePartitionId());
        assertEquals(leftPartition, split.left().partitionId());
        assertEquals(rightPartition, split.right().partitionId());
        var splitTranslation = (MetadataCommand.RecordCdcTokenTranslation) submitted.get(1);
        assertEquals(8, splitTranslation.expectedMetadataEpoch());
        assertEquals(8, splitTranslation.translation().metadataEpoch());
        var merge = (MetadataCommand.MergePartitions) submitted.get(2);
        assertEquals(8, merge.expectedMetadataEpoch());
        assertEquals(leftPartition, merge.leftPartitionId());
        assertEquals(rightPartition, merge.rightPartitionId());
        assertEquals(mergedPartition, merge.merged().partitionId());
        var mergeTranslation = (MetadataCommand.RecordCdcTokenTranslation) submitted.get(3);
        assertEquals(9, mergeTranslation.expectedMetadataEpoch());
        assertEquals(9, mergeTranslation.translation().metadataEpoch());
        assertEquals(9, committed[0].epoch());
        assertTrue(committed[0].findPartition(PARTITION).isEmpty());
        assertTrue(committed[0].findPartition(leftPartition).isEmpty());
        assertTrue(committed[0].findPartition(rightPartition).isEmpty());
        assertEquals(mergedPartition, committed[0].requirePartition(mergedPartition).partitionId());
        assertEquals(splitTranslation.translation(), committed[0].requireCdcTokenTranslation(8));
        assertEquals(mergeTranslation.translation(), committed[0].requireCdcTokenTranslation(9));
        assertEquals(List.of(8L, 9L), refreshed.stream()
                .map(ClusterMetadata::epoch)
                .toList());
        assertEquals(List.of(leftPartition, rightPartition, mergedPartition), resolver.targets.stream()
                .map(RouteTarget::shardId)
                .toList());
        assertEquals(3, resolver.client.proposals.size());
    }

    @Test
    void publicPartitionSplitAcceptsLaterCommittedSnapshotAfterCdcTranslation() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    committed[0] = MetadataCommandApplier.apply(committed[0], command);
                    if (command instanceof MetadataCommand.RecordCdcTokenTranslation) {
                        committed[0] = metadataAtEpoch(committed[0], committed[0].epoch() + 1);
                    }
                    return committed[0];
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed[0],
                    committed[0].epoch(),
                    PARTITION,
                    1,
                    leftPartition,
                    rightPartition,
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed[0].epoch(), PARTITION, 1, 10, 0)
            );
            connection.applyPartitionSplit(splitPlan);
            connection.getSyncApi().put(0, 4, keys("a"), buf("left"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right"), RequestType.none());
        }

        assertEquals(9, committed[0].epoch());
        assertEquals(List.of(9L), refreshed.stream()
                .map(ClusterMetadata::epoch)
                .toList());
        assertEquals(List.of(leftPartition, rightPartition), resolver.targets.stream()
                .map(RouteTarget::shardId)
                .toList());
        assertEquals(2, resolver.client.proposals.size());
    }

    @Test
    void publicPartitionSplitDoesNotRefreshRoutesBeforeCdcTranslationCommits() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    if (command instanceof MetadataCommand.SplitPartition) {
                        committed[0] = MetadataCommandApplier.apply(committed[0], command);
                        return committed[0];
                    }
                    if (command instanceof MetadataCommand.RecordCdcTokenTranslation) {
                        throw new IllegalStateException("CDC translation commit failed");
                    }
                    throw new AssertionError("unexpected command: " + command.kind());
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed[0],
                    committed[0].epoch(),
                    PARTITION,
                    1,
                    new ShardId(8),
                    new ShardId(9),
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed[0].epoch(), PARTITION, 1, 10, 0)
            );

            var error = assertThrows(RocksDBException.class, () -> connection.applyPartitionSplit(splitPlan));
            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("CDC translation commit failed"));
            assertEquals(List.of(), refreshed);

            connection.getSyncApi().put(0, 4, keys("a"), buf("old-route"), RequestType.none());
        }

        assertEquals(1, resolver.client.proposals.size());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
    }

    @Test
    void publicPartitionMergeDoesNotRefreshRoutesBeforeCdcTranslationCommits() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        var translationCommits = new AtomicInteger();
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var mergedPartition = new ShardId(10);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    if (command instanceof MetadataCommand.SplitPartition
                            || command instanceof MetadataCommand.MergePartitions) {
                        committed[0] = MetadataCommandApplier.apply(committed[0], command);
                        return committed[0];
                    }
                    if (command instanceof MetadataCommand.RecordCdcTokenTranslation) {
                        if (translationCommits.incrementAndGet() == 1) {
                            committed[0] = MetadataCommandApplier.apply(committed[0], command);
                            return committed[0];
                        }
                        throw new IllegalStateException("merge CDC translation commit failed");
                    }
                    throw new AssertionError("unexpected command: " + command.kind());
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed[0],
                    committed[0].epoch(),
                    PARTITION,
                    1,
                    leftPartition,
                    rightPartition,
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed[0].epoch(), PARTITION, 1, 10, 0)
            );
            connection.applyPartitionSplit(splitPlan);
            connection.getSyncApi().put(0, 4, keys("a"), buf("left-before-merge"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right-before-merge"), RequestType.none());

            var mergePlan = planner.planMerge(
                    committed[0],
                    committed[0].epoch(),
                    leftPartition,
                    rightPartition,
                    mergedPartition,
                    "group-merged",
                    new ReplicatedCdcToken(committed[0].epoch(), leftPartition, 2, 11, 0),
                    new ReplicatedCdcToken(committed[0].epoch(), rightPartition, 2, 12, 0)
            );
            var error = assertThrows(RocksDBException.class, () -> connection.applyPartitionMerge(mergePlan));
            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("merge CDC translation commit failed"));
            assertEquals(List.of(8L), refreshed.stream()
                    .map(ClusterMetadata::epoch)
                    .toList());

            connection.getSyncApi().put(0, 4, keys("a"), buf("left-after-merge-failure"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right-after-merge-failure"), RequestType.none());
        }

        assertEquals(4, resolver.client.proposals.size());
        assertEquals(List.of(leftPartition, rightPartition, leftPartition, rightPartition), resolver.targets.stream()
                .map(RouteTarget::shardId)
                .toList());
    }

    @Test
    void publicPartitionSplitRejectsCommittedSnapshotsWithoutRouteGroups() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var committed = metadata();
        var refreshed = new ArrayList<ClusterMetadata>();
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed,
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    if (command instanceof MetadataCommand.SplitPartition split) {
                        return metadataWithoutRouteGroups(
                                committed,
                                split.expectedMetadataEpoch() + 1,
                                Map.of(
                                        split.left().partitionId(), split.left(),
                                        split.right().partitionId(), split.right()
                                )
                        );
                    }
                    throw new AssertionError("unexpected command: " + command.kind());
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed,
                    committed.epoch(),
                    PARTITION,
                    1,
                    leftPartition,
                    rightPartition,
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed.epoch(), PARTITION, 1, 10, 0)
            );

            var error = assertThrows(RocksDBException.class, () -> connection.applyPartitionSplit(splitPlan));
            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("SPLIT_PARTITION effect"));
            assertTrue(error.getMessage().contains("partition route group was not committed"));
            assertEquals(List.of(), refreshed);

            connection.getSyncApi().put(0, 4, keys("a"), buf("old-route"), RequestType.none());
        }

        assertEquals(1, resolver.client.proposals.size());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
    }

    @Test
    void publicPartitionMergeRejectsCommittedSnapshotsWithoutRouteGroups() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionTopologyWorkflowPlanner();
        var committed = new ClusterMetadata[]{metadata()};
        var refreshed = new ArrayList<ClusterMetadata>();
        var leftPartition = new ShardId(8);
        var rightPartition = new ShardId(9);
        var mergedPartition = new ShardId(10);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                committed[0],
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add,
                command -> {
                    if (command instanceof MetadataCommand.SplitPartition
                            || command instanceof MetadataCommand.RecordCdcTokenTranslation) {
                        committed[0] = MetadataCommandApplier.apply(committed[0], command);
                        return committed[0];
                    }
                    if (command instanceof MetadataCommand.MergePartitions merge) {
                        return metadataWithoutRouteGroups(
                                committed[0],
                                merge.expectedMetadataEpoch() + 1,
                                Map.of(merge.merged().partitionId(), merge.merged())
                        );
                    }
                    throw new AssertionError("unexpected command: " + command.kind());
                }
        )) {
            var splitPlan = planner.planSplit(
                    committed[0],
                    committed[0].epoch(),
                    PARTITION,
                    1,
                    leftPartition,
                    rightPartition,
                    buf("m"),
                    "group-left",
                    "group-right",
                    new ReplicatedCdcToken(committed[0].epoch(), PARTITION, 1, 10, 0)
            );
            connection.applyPartitionSplit(splitPlan);
            connection.getSyncApi().put(0, 4, keys("a"), buf("left-before-merge"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right-before-merge"), RequestType.none());

            var mergePlan = planner.planMerge(
                    committed[0],
                    committed[0].epoch(),
                    leftPartition,
                    rightPartition,
                    mergedPartition,
                    "group-merged",
                    new ReplicatedCdcToken(committed[0].epoch(), leftPartition, 2, 11, 0),
                    new ReplicatedCdcToken(committed[0].epoch(), rightPartition, 2, 12, 0)
            );

            var error = assertThrows(RocksDBException.class, () -> connection.applyPartitionMerge(mergePlan));
            assertEquals(RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("MERGE_PARTITIONS effect"));
            assertTrue(error.getMessage().contains("partition route group was not committed"));
            assertEquals(List.of(8L), refreshed.stream()
                    .map(ClusterMetadata::epoch)
                    .toList());

            connection.getSyncApi().put(0, 4, keys("a"), buf("left-after-merge-failure"), RequestType.none());
            connection.getSyncApi().put(0, 4, keys("z"), buf("right-after-merge-failure"), RequestType.none());
        }

        assertEquals(4, resolver.client.proposals.size());
        assertEquals(List.of(leftPartition, rightPartition, leftPartition, rightPartition), resolver.targets.stream()
                .map(RouteTarget::shardId)
                .toList());
    }

    @Test
    void unsupportedPublicOperationsFailBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        var planner = new PartitionMembershipWorkflowPlanner();
        var learner = new ReplicaMember(
                N2,
                URI.create("grpc://127.0.0.2:19002"),
                ReplicaRole.LEARNER,
                0,
                0,
                false
        );
        var membershipMetadata = MetadataCommandApplier.apply(metadata(), new MetadataCommand.RegisterNode(
                7,
                N2,
                URI.create("http://n2.public:10000"),
                URI.create("grpc://127.0.0.2:19002"),
                "rack-b"
        ));
        var membershipPlan = planner.planAddReplica(membershipMetadata, 8, PARTITION, 1, learner);
        var topologyPlanner = new PartitionTopologyWorkflowPlanner();
        var splitPlan = topologyPlanner.planSplit(
                metadata(),
                7,
                PARTITION,
                1,
                new ShardId(8),
                new ShardId(9),
                buf("m"),
                "group-left",
                "group-right",
                new ReplicatedCdcToken(7, PARTITION, 1, 10, 0)
        );
        var mergeMetadata = MetadataCommandApplier.apply(metadata(), splitPlan.splitCommand());
        var mergePlan = topologyPlanner.planMerge(
                mergeMetadata,
                8,
                new ShardId(8),
                new ShardId(9),
                new ShardId(10),
                "group-merged",
                new ReplicatedCdcToken(8, new ShardId(8), 2, 11, 0),
                new ReplicatedCdcToken(8, new ShardId(9), 2, 12, 0)
        );
        try (var connection = connection(resolver)) {
            var api = connection.getSyncApi();

            assertNotImplemented(() -> api.openTransaction(1000));
            assertNotImplemented(() -> api.createColumn("new-col", ColumnSchema.of(IntList.of(1), ObjectList.of(), true)));
            assertNotImplemented(() -> connection.registerNode(
                    N2,
                    URI.create("http://n2.public:10000"),
                    URI.create("grpc://127.0.0.2:19002"),
                    "rack-b"
            ));
            assertNotImplemented(() -> connection.createPartition(
                    PARTITION,
                    null,
                    null,
                    1,
                    "group-7",
                    List.of(N1),
                    N1,
                    Map.of()
            ));
            assertNotImplemented(() -> connection.applyPartitionMembershipChange(membershipPlan));
            assertNotImplemented(() -> connection.applyPartitionSplit(splitPlan));
            assertNotImplemented(() -> connection.applyPartitionMerge(mergePlan));
            assertNull(api.checkMergeOperator("merge", new byte[]{1}));
            assertNotImplemented(() -> api.uploadMergeOperator("merge", "Operator", new byte[]{1}));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicUploadMergeOperatorFailsBeforeMetadataSubmissionWhenSubmitterIsWired() throws Exception {
        var resolver = new RecordingResolver();
        var submitted = new ArrayList<MetadataCommand>();
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                ignored -> {
                },
                command -> {
                    submitted.add(command);
                    return MetadataCommandApplier.apply(metadata(), command);
                }
        )) {
            var error = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().uploadMergeOperator("merge", "Operator", new byte[]{1}));

            assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
            assertTrue(error.getMessage().contains("replicated merge-operator artifact upload requires"));
        }

        assertEquals(0, submitted.size());
        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCheckMergeOperatorReadsActiveMetadataWithoutRatisQuery() throws Exception {
        var resolver = new RecordingResolver();
        var jarPayload = bytes("operator jar");
        var hash = sha256(jarPayload);
        var activation = new MergeOperatorActivation("merge", 5, Buf.wrap(hash));
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadataWithActiveMergeOperator(activation),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        )) {
            var api = connection.getSyncApi();

            assertEquals(5L, api.checkMergeOperator("merge", hash));
            assertEquals(5L, api.ensureMergeOperator("merge", "Operator", jarPayload));
            assertNull(api.checkMergeOperator("merge", sha256(bytes("other jar"))));
            assertNull(api.checkMergeOperator("other", hash));
            assertNotImplemented(() -> api.ensureMergeOperator("other", "Operator", jarPayload));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void publicCheckMergeOperatorRejectsNullPublicFieldsBeforeAdapterDispatch() throws Exception {
        var resolver = new RecordingResolver();
        try (var connection = connection(resolver)) {
            var nullName = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().checkMergeOperator(null, new byte[]{1}));
            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, nullName.getErrorUniqueId());
            assertTrue(nullName.getMessage().contains("merge operator name must not be null"));

            var nullHash = assertThrows(RocksDBException.class,
                    () -> connection.getSyncApi().checkMergeOperator("merge", null));
            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, nullHash.getErrorUniqueId());
            assertTrue(nullHash.getMessage().contains("merge operator hash must not be null"));

            var asyncFailure = assertThrows(ExecutionException.class,
                    () -> connection.getAsyncApi()
                            .checkMergeOperatorAsync("merge", null)
                            .get(5, TimeUnit.SECONDS));
            var asyncError = assertInstanceOf(RocksDBException.class, asyncFailure.getCause());
            assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, asyncError.getErrorUniqueId());
            assertTrue(asyncError.getMessage().contains("merge operator hash must not be null"));
        }

        assertEquals(0, resolver.client.proposals.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void metadataUpdateListenerRunsBeforeRouteCacheSwap() throws Exception {
        var resolver = new RecordingResolver();
        var refreshed = new ArrayList<ClusterMetadata>();
        var updated = metadata(8, 5, "updated-col", 4);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                refreshed::add
        )) {
            connection.updateMetadata(updated);

            assertEquals(List.of(updated), refreshed);
            assertEquals(5, connection.getSyncApi().getColumnId("updated-col"));
        }
    }

    @Test
    void metadataUpdateListenerFailureLeavesRouteCacheUnchanged() throws Exception {
        var resolver = new RecordingResolver();
        var updated = metadata(8, 5, "updated-col", 4);
        try (var connection = new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0,
                ignored -> {
                    throw new IllegalStateException("refresh failed");
                }
        )) {
            assertThrows(IllegalStateException.class, () -> connection.updateMetadata(updated));

            assertEquals(4, connection.getSyncApi().getColumnId("col"));
        }
    }

    private static ReplicatedConnection connection(RecordingResolver resolver) {
        return new ReplicatedConnection(
                "replicated-test",
                metadata(),
                resolver,
                resolver,
                resolver,
                resolver,
                CLIENT_ID,
                3,
                () -> 0
        );
    }

    private static void assertIllegalArgument(String expectedMessage, ThrowingRunnable runnable) {
        var error = assertThrows(IllegalArgumentException.class, runnable::run);
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static void assertNotImplemented(ThrowingRunnable runnable) {
        var error = assertThrows(RocksDBException.class, runnable::run);
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
    }

    private static void assertAsyncNotImplemented(CompletableFuture<?> future) {
        var error = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        var rocksDBException = assertInstanceOf(RocksDBException.class, error.getCause());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, rocksDBException.getErrorUniqueId());
    }

    private static void assertReadFailure(ThrowingRunnable runnable, String expectedMessage) {
        var error = assertThrows(RocksDBException.class, runnable::run);
        assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static void assertAsyncReadFailure(CompletableFuture<?> future, String expectedMessage) {
        var failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        var error = assertInstanceOf(RocksDBException.class, failure.getCause());
        assertEquals(RocksDBException.RocksDBErrorType.GET_1, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static void assertColumnCreateFailure(ThrowingRunnable runnable, String expectedMessage) {
        var error = assertThrows(RocksDBException.class, runnable::run);
        assertEquals(RocksDBException.RocksDBErrorType.COLUMN_CREATE_FAIL, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static void assertAsyncColumnCreateFailure(CompletableFuture<?> future, String expectedMessage) {
        var failure = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
        var error = assertInstanceOf(RocksDBException.class, failure.getCause());
        assertEquals(RocksDBException.RocksDBErrorType.COLUMN_CREATE_FAIL, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains(expectedMessage));
    }

    private static ClusterMetadata metadata() {
        return metadata(7, 4, "col", 3);
    }

    private static ClusterMetadata metadataWithoutPartitions() {
        var node = new ClusterNodeMetadata(
                N1,
                URI.create("http://n1.public:10000"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        );
        var column = new ClusterColumnMetadata(
                4,
                "col",
                ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                3
        );
        return new ClusterMetadata(
                CLUSTER_ID,
                11,
                Map.of(),
                Map.of(column.columnId(), column),
                Map.of(),
                Map.of(N1, node)
        );
    }

    private static ClusterMetadata metadataAtEpoch(ClusterMetadata metadata, long epoch) {
        return new ClusterMetadata(
                metadata.clusterId(),
                epoch,
                metadata.shards(),
                metadata.columnsById(),
                metadata.partitionsById(),
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata metadataWithoutRouteGroups(ClusterMetadata metadata,
                                                              long epoch,
                                                              Map<ShardId, PartitionDescriptor> partitionsById) {
        return new ClusterMetadata(
                metadata.clusterId(),
                epoch,
                Map.of(),
                metadata.columnsById(),
                partitionsById,
                metadata.nodesById(),
                metadata.cdcTokenTranslationsByEpoch(),
                metadata.schemaChangesById(),
                metadata.backupManifestsById()
        );
    }

    private static ClusterMetadata metadata(long metadataEpoch, long columnId, String columnName, long schemaEpoch) {
        var member = new ReplicaMember(
                N1,
                URI.create("grpc://127.0.0.1:19001"),
                ReplicaRole.VOTER,
                10,
                10,
                true
        );
        var node = new ClusterNodeMetadata(
                N1,
                URI.create("http://n1.public:10000"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        );
        var column = new ClusterColumnMetadata(
                columnId,
                columnName,
                ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                schemaEpoch
        );
        var partition = new PartitionDescriptor(
                PARTITION,
                null,
                null,
                1,
                "group-7",
                List.of(member),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        var group = new ReplicaGroupMetadata(PARTITION, 1, N1, List.of(member), 1);
        return new ClusterMetadata(
                CLUSTER_ID,
                metadataEpoch,
                Map.of(PARTITION, group),
                Map.of(columnId, column),
                Map.of(PARTITION, partition),
                Map.of(N1, node)
        );
    }

    private static ClusterMetadata metadataWithTwoPartitions() {
        var member = new ReplicaMember(
                N1,
                URI.create("grpc://127.0.0.1:19001"),
                ReplicaRole.VOTER,
                10,
                10,
                true
        );
        var node = new ClusterNodeMetadata(
                N1,
                URI.create("http://n1.public:10000"),
                URI.create("grpc://127.0.0.1:19001"),
                "rack-a"
        );
        var column = new ClusterColumnMetadata(
                4,
                "col",
                ColumnSchema.of(IntList.of(1), ObjectList.of(), true),
                3
        );
        var left = new PartitionDescriptor(
                PARTITION,
                null,
                buf("m"),
                1,
                "group-left",
                List.of(member),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        var rightId = new ShardId(8);
        var right = new PartitionDescriptor(
                rightId,
                buf("m"),
                null,
                1,
                "group-right",
                List.of(member),
                N1,
                Map.of(),
                PartitionSplitState.ACTIVE
        );
        return new ClusterMetadata(
                CLUSTER_ID,
                7,
                Map.of(
                        PARTITION, new ReplicaGroupMetadata(PARTITION, 1, N1, List.of(member), 1),
                        rightId, new ReplicaGroupMetadata(rightId, 1, N1, List.of(member), 1)
                ),
                Map.of(4L, column),
                Map.of(PARTITION, left, rightId, right),
                Map.of(N1, node)
        );
    }

    private static ClusterMetadata metadataWithActiveMergeOperator(MergeOperatorActivation activation) {
        var prepared = MetadataCommandApplier.apply(metadata(), new MetadataCommand.PrepareSchemaChange(
                7,
                101,
                SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                4,
                4,
                null,
                activation
        ));
        var activated = MetadataCommandApplier.apply(prepared,
                new MetadataCommand.ActivateSchemaChange(8, 101, 4, 4));
        return MetadataCommandApplier.apply(activated,
                new MetadataCommand.FenceSchemaChange(9, 101, 4, 4));
    }

    private static Keys keys(String key) {
        return new Keys(buf(key));
    }

    private static Buf buf(String value) {
        return Buf.wrap(bytes(value));
    }

    private static byte[] pointReadResponse(Buf value) {
        return PartitionWriteResponseCodec.encodeResponses(java.util.Collections.singletonList(value));
    }

    private static byte[] writeResponse(Object... responses) {
        return PartitionWriteResponseCodec.encodeResponses(Arrays.asList(responses));
    }

    private static PartitionWriteResponseMode responseMode(byte[] proposal) {
        return PartitionWriteCommandCodec.decode(proposal).responseMode();
    }

    private static byte[] rangeReadResponse(KV... entries) {
        return PartitionRangeReadResponseCodec.encodeEntries(List.of(entries));
    }

    private static byte[] rangeReadResponse(List<KV> entries) {
        return PartitionRangeReadResponseCodec.encodeEntries(entries);
    }

    private static KV kv(String key, String value) {
        return new KV(keys(key), buf(value));
    }

    private static KVBatch batch(List<Keys> keys, List<Buf> values) {
        return new KVBatch.KVBatchRef(keys, values);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError(e);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        var interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class RecordingResolver implements PartitionWriteSubmitter.LogClientResolver,
            PartitionPointReadSubmitter.RatisClientResolver,
            PartitionRangeReadSubmitter.RatisClientResolver,
            AutoCloseable {

        private final RecordingPartitionClient client = new RecordingPartitionClient();
        private final ArrayList<RouteTarget> targets = new ArrayList<>();
        private Throwable closeFailure;
        private int closeCount;
        private boolean closed;

        @Override
        public RatisPartitionClient resolve(RouteTarget target) {
            targets.add(target);
            return client;
        }

        @Override
        public void close() {
            closeCount++;
            closed = true;
            if (closeFailure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (closeFailure instanceof Error error) {
                throw error;
            }
        }
    }

    private static final class RecordingPartitionClient implements RatisPartitionClient {

        private final ArrayList<byte[]> proposals = new ArrayList<>();
        private final ArrayList<byte[]> queries = new ArrayList<>();
        private final Queue<byte[]> proposalResponses = new ArrayDeque<>();
        private final Queue<byte[]> queryResponses = new ArrayDeque<>();
        private boolean returnNullProposal;
        private boolean returnNullCommit;
        private boolean returnNullQuery;

        @Override
        public RatisPartitionDescriptor descriptor() {
            return new RatisPartitionDescriptor(
                    "7",
                    "group-7",
                    "n1",
                    List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                    Path.of("/tmp/rockserver-replicated-connection-test")
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            proposals.add(payload.clone());
            if (returnNullProposal) {
                return null;
            }
            if (returnNullCommit) {
                return CompletableFuture.completedFuture(null);
            }
            var response = proposalResponses.poll();
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(
                    proposals.size(),
                    true,
                    response == null ? null : response.clone()
            ));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            queries.add(request.clone());
            if (returnNullQuery) {
                return null;
            }
            var response = queryResponses.poll();
            return CompletableFuture.completedFuture(response == null ? null : response.clone());
        }

        @Override
        public void close() {
        }
    }

    private static final class BlockingResolver implements PartitionWriteSubmitter.LogClientResolver,
            PartitionPointReadSubmitter.RatisClientResolver,
            PartitionRangeReadSubmitter.RatisClientResolver,
            AutoCloseable {

        private final RatisPartitionClient client = new BlockingPartitionClient();
        private final CountDownLatch started;
        private final CountDownLatch interrupted = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger resolveCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();
        private final boolean releaseOnClose;

        private BlockingResolver(int expectedStarted) {
            this(expectedStarted, true);
        }

        private BlockingResolver(int expectedStarted, boolean releaseOnClose) {
            this.started = new CountDownLatch(expectedStarted);
            this.releaseOnClose = releaseOnClose;
        }

        @Override
        public RatisPartitionClient resolve(RouteTarget target) {
            resolveCount.incrementAndGet();
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.countDown();
                Thread.currentThread().interrupt();
                throw new CompletionException(e);
            }
            return client;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            if (releaseOnClose) {
                release.countDown();
            }
        }
    }

    private static final class BlockingPartitionClient implements RatisPartitionClient {

        @Override
        public RatisPartitionDescriptor descriptor() {
            return new RatisPartitionDescriptor(
                    "7",
                    "group-7",
                    "n1",
                    List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                    Path.of("/tmp/rockserver-replicated-connection-blocking-test")
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(1, true));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }

    private static final class UninterruptibleBlockingResolver implements PartitionWriteSubmitter.LogClientResolver,
            PartitionPointReadSubmitter.RatisClientResolver,
            PartitionRangeReadSubmitter.RatisClientResolver,
            AutoCloseable {

        private final RatisPartitionClient client = new BlockingPartitionClient();
        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicInteger closeCount = new AtomicInteger();

        @Override
        public RatisPartitionClient resolve(RouteTarget target) {
            started.countDown();
            awaitUninterruptibly(release);
            return client;
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class PendingProposalResolver implements PartitionWriteSubmitter.LogClientResolver,
            PartitionPointReadSubmitter.RatisClientResolver,
            PartitionRangeReadSubmitter.RatisClientResolver,
            AutoCloseable {

        private final PendingProposalPartitionClient client = new PendingProposalPartitionClient();

        @Override
        public RatisPartitionClient resolve(RouteTarget target) {
            return client;
        }

        @Override
        public void close() {
        }
    }

    private static final class PendingProposalPartitionClient implements RatisPartitionClient {

        private final CountDownLatch started = new CountDownLatch(1);
        private final CountDownLatch canceled = new CountDownLatch(1);
        private final CompletableFuture<ReplicatedLogCommit> proposal = new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                var result = super.cancel(mayInterruptIfRunning);
                canceled.countDown();
                return result;
            }
        };

        @Override
        public RatisPartitionDescriptor descriptor() {
            return new RatisPartitionDescriptor(
                    "7",
                    "group-7",
                    "n1",
                    List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                    Path.of("/tmp/rockserver-replicated-connection-pending-proposal-test")
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            started.countDown();
            return proposal;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void close() {
        }
    }
}
