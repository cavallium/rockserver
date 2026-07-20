package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCommandEncoder;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class ReplicatedMutationCommandEncoderTest {

    @Test
    @SuppressWarnings("DataFlowIssue")
    void rejectsNullInputsWithExplicitDiagnostics() {
        var mutation = ReplicatedMutation.put(4, keys("a"), buf("value"));

        assertIllegalArgument("mutations must not be null",
                () -> new ReplicatedMutationCommandEncoder.EncodedPartitionWrite(null,
                        PartitionWriteResponseMode.NONE));
        assertIllegalArgument("responseMode must not be null",
                () -> new ReplicatedMutationCommandEncoder.EncodedPartitionWrite(List.of(mutation), null));
        assertIllegalArgument("mutation must not be null",
                () -> new ReplicatedMutationCommandEncoder.EncodedPartitionWrite(Collections.singletonList(null),
                        PartitionWriteResponseMode.NONE));
        assertIllegalArgument("command must not be null",
                () -> ReplicatedMutationCommandEncoder.encodePartitionWrite(null));
    }

    @Test
    void emptyMultiWritesEncodeAsNoops() {
        assertNoop(new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(
                0,
                4,
                List.<Keys>of(),
                List.<Buf>of(),
                RequestType.previous()
        ), PartitionWriteResponseMode.PREVIOUS_VALUE);
        assertNoop(new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(
                0,
                4,
                List.<Keys>of(),
                RequestType.previousPresence()
        ), PartitionWriteResponseMode.PREVIOUS_PRESENCE);
        assertNoop(new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(
                0,
                4,
                List.<Keys>of(),
                List.<Buf>of(),
                RequestType.merged()
        ), PartitionWriteResponseMode.MERGED_VALUE);
    }

    @Test
    void emptySafeBatchWritesEncodeAsNoops() {
        var put = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.empty(),
                PutBatchMode.WRITE_BATCH
        );
        var merge = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
                4,
                Flux.just(batch(List.of(), List.of())),
                MergeBatchMode.MERGE_WRITE_BATCH
        );

        assertNoop(put, PartitionWriteResponseMode.NONE);
        assertNoop(merge, PartitionWriteResponseMode.NONE);
        assertEquals(List.of(), ReplicatedMutationCommandEncoder.encodeAll(put));
        assertEquals(List.of(), ReplicatedMutationCommandEncoder.encodeAll(merge));
    }

    @Test
    void singleMutationEncodingRejectsNoopCommands() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.empty(),
                PutBatchMode.WRITE_BATCH
        );

        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encode(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains("replicated command produced no mutations"));
    }

    @Test
    void safePutBatchExpandsPublisherBatchesIntoOrderedMutations() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.just(
                        batch(List.of(keys("a")), List.of(buf("value-a"))),
                        batch(List.of(keys("b"), keys("c")), List.of(buf("value-b"), buf("value-c")))
                ),
                PutBatchMode.WRITE_BATCH
        );

        var mutations = ReplicatedMutationCommandEncoder.encodeAll(command);

        assertEquals(3, mutations.size());
        assertEquals(ReplicatedMutationOp.PUT, mutations.getFirst().operation());
        assertEquals(keys("a"), mutations.getFirst().keys());
        assertArrayEquals(bytes("value-a"), mutations.getFirst().requiredValue().toByteArray());
        assertEquals(ReplicatedMutationOp.PUT, mutations.get(1).operation());
        assertEquals(keys("b"), mutations.get(1).keys());
        assertArrayEquals(bytes("value-b"), mutations.get(1).requiredValue().toByteArray());
        assertEquals(ReplicatedMutationOp.PUT, mutations.get(2).operation());
        assertEquals(keys("c"), mutations.get(2).keys());
        assertArrayEquals(bytes("value-c"), mutations.get(2).requiredValue().toByteArray());
    }

    @Test
    void safeMergeBatchExpandsPublisherBatchesIntoOrderedMutations() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
                4,
                Flux.just(batch(List.of(keys("a"), keys("b")), List.of(buf("merge-a"), buf("merge-b")))),
                MergeBatchMode.MERGE_WRITE_BATCH
        );

        var mutations = ReplicatedMutationCommandEncoder.encodeAll(command);

        assertEquals(2, mutations.size());
        assertEquals(ReplicatedMutationOp.MERGE, mutations.getFirst().operation());
        assertEquals(keys("a"), mutations.getFirst().keys());
        assertArrayEquals(bytes("merge-a"), mutations.getFirst().requiredValue().toByteArray());
        assertEquals(ReplicatedMutationOp.MERGE, mutations.get(1).operation());
        assertEquals(keys("b"), mutations.get(1).keys());
        assertArrayEquals(bytes("merge-b"), mutations.get(1).requiredValue().toByteArray());
    }

    @Test
    void unsafeBatchModesRemainRejectedBeforeEncoding() {
        var put = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.empty(),
                PutBatchMode.WRITE_BATCH_NO_WAL
        );
        var merge = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
                4,
                Flux.empty(),
                MergeBatchMode.MERGE_SST_INGESTION
        );

        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(put))
                        .getErrorUniqueId());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(merge))
                        .getErrorUniqueId());
    }

    @Test
    void oversizedBatchIsRejectedBeforeProposalEncoding() {
        var keyList = new ArrayList<Keys>();
        var values = new ArrayList<Buf>();
        for (int i = 0; i < 16_385; i++) {
            keyList.add(keys("a"));
            values.add(buf("value"));
        }
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.just(batch(keyList, values)),
                PutBatchMode.WRITE_BATCH
        );

        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
    }

    @Test
    void oversizedMultiKeyWritesAreRejectedBeforeProposalEncoding() {
        var keyList = new ArrayList<Keys>();
        var values = new ArrayList<Buf>();
        for (int i = 0; i < 16_385; i++) {
            keyList.add(keys("a"));
            values.add(buf("value"));
        }

        assertOversized(new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(
                0,
                4,
                keyList,
                values,
                RequestType.none()
        ));
        assertOversized(new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(
                0,
                4,
                keyList,
                RequestType.none()
        ));
        assertOversized(new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(
                0,
                4,
                keyList,
                values,
                RequestType.none()
        ));
    }

    @Test
    void mismatchedMultiKeyWritesAreTypedInvalidRequests() {
        assertInvalidRequest(new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(
                0,
                4,
                List.of(keys("a"), keys("b")),
                List.of(buf("value-a")),
                RequestType.none()
        ));
        assertInvalidRequest(new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(
                0,
                4,
                List.of(keys("a"), keys("b")),
                List.of(buf("delta-a")),
                RequestType.none()
        ));
    }

    @Test
    void mismatchedBatchPayloadsAreTypedInvalidRequests() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.just(batch(List.of(keys("a"), keys("b")), List.of(buf("value-a")))),
                PutBatchMode.WRITE_BATCH
        );

        assertInvalidRequest(command);
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void nullSingleWriteFieldsAreTypedInvalidRequests() {
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(
                0,
                4,
                null,
                buf("value"),
                RequestType.none()
        ), "put keys must not be null");
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.Merge<>(
                0,
                4,
                keys("a"),
                null,
                RequestType.none()
        ), "merge value must not be null");
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.Delete<>(
                0,
                4,
                keys("a"),
                null
        ), "delete requestType must not be null");
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(
                0,
                4,
                new Keys((Buf) null),
                buf("value"),
                RequestType.none()
        ), "put keys part 0 must not be null");
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void nullMultiWriteFieldsAreTypedInvalidRequests() {
        var nullValues = new ArrayList<Buf>();
        nullValues.add(null);
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<>(
                0,
                4,
                List.of(keys("a")),
                nullValues,
                RequestType.none()
        ), "putMulti value 0 must not be null");

        var nullKeys = new ArrayList<Keys>();
        nullKeys.add(null);
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<>(
                0,
                4,
                nullKeys,
                RequestType.none()
        ), "deleteMulti key 0 must not be null");

        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<>(
                0,
                4,
                List.of(new Keys((Buf) null)),
                List.of(buf("merge")),
                RequestType.none()
        ), "mergeMulti key 0 part 0 must not be null");
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void invalidColumnIdsAndNullBatchModesAreTypedInvalidRequests() {
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(
                0,
                -1,
                keys("a"),
                buf("value"),
                RequestType.none()
        ), "columnId must be >= 0");
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.empty(),
                null
        ), "putBatch mode must not be null");
        assertInvalidRequestMessage(new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
                4,
                Flux.empty(),
                null
        ), "mergeBatch mode must not be null");
    }

    @Test
    void batchPublisherFailuresAreTypedInvalidRequests() {
        var publisherFailure = new IllegalStateException("boom");
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch(
                4,
                Flux.error(publisherFailure),
                PutBatchMode.WRITE_BATCH
        );

        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains("replicated write batch publisher failed: boom"));
        assertSame(publisherFailure, error.getCause());
    }

    @Test
    void nullBatchValuesAreTypedInvalidRequests() {
        var values = new ArrayList<Buf>();
        values.add(null);
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch(
                4,
                Flux.just(batch(List.of(keys("a")), values)),
                MergeBatchMode.MERGE_WRITE_BATCH
        );

        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains("replicated write batch value 0 must not be null"));
    }

    @Test
    void responseProducingWritesUsePartitionEncodingBoundary() {
        var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Put<>(
                0,
                4,
                keys("a"),
                buf("value"),
                RequestType.previous()
        );

        var encoded = ReplicatedMutationCommandEncoder.encodePartitionWrite(command);
        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(PartitionWriteResponseMode.PREVIOUS_VALUE, encoded.responseMode());
        assertEquals(1, encoded.mutations().size());
        assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains("encodeAll only accepts no-result replicated writes"));
    }

    private static void assertOversized(RocksDBAPICommand<?, ?, ?> command) {
        assertInvalidRequest(command);
    }

    private static void assertInvalidRequest(RocksDBAPICommand<?, ?, ?> command) {
        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains("keys and values must have the same size")
                || error.getMessage().contains("mutations per command"));
    }

    private static void assertInvalidRequestMessage(RocksDBAPICommand<?, ?, ?> command, String messagePart) {
        var error = assertThrows(RocksDBException.class, () -> ReplicatedMutationCommandEncoder.encodeAll(command));

        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, error.getErrorUniqueId());
        assertTrue(error.getMessage().contains(messagePart));
    }

    private static void assertNoop(RocksDBAPICommand<?, ?, ?> command, PartitionWriteResponseMode responseMode) {
        var encoded = ReplicatedMutationCommandEncoder.encodePartitionWrite(command);

        assertTrue(encoded.isNoop());
        assertEquals(responseMode, encoded.responseMode());
        assertEquals(List.of(), encoded.mutations());
    }

    private static void assertIllegalArgument(String expectedMessage, Runnable action) {
        var failure = assertThrows(IllegalArgumentException.class, action::run);

        assertEquals(expectedMessage, failure.getMessage());
    }

    private static KVBatch batch(List<Keys> keys, List<Buf> values) {
        return new KVBatch.KVBatchRef(keys, values);
    }

    private static Keys keys(String key) {
        return new Keys(buf(key));
    }

    private static Buf buf(String value) {
        return Buf.wrap(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
