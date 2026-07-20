package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationApplier;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCodec;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class ReplicatedMutationApplierTest {

    @Test
    @SuppressWarnings("DataFlowIssue")
    void rejectsNullInputsWithExplicitDiagnostics() {
        var mutation = ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"));
        var applier = new ReplicatedMutationApplier(new RecordingApi());

        assertIllegalArgument("api must not be null", () -> new ReplicatedMutationApplier(null));
        assertIllegalArgument("command must not be null", () -> applier.apply((PartitionWriteCommand) null));
        assertIllegalArgument("mutations must not be null", () -> applier.apply((List<ReplicatedMutation>) null));
        assertIllegalArgument("mutations must not be null", () -> applier.apply(null, PartitionWriteResponseMode.NONE));
        assertIllegalArgument("responseMode must not be null", () -> applier.apply(List.of(mutation), null));
        assertIllegalArgument("mutation must not be null", () -> applier.apply((ReplicatedMutation) null));
        assertIllegalArgument("mutation must not be null",
                () -> applier.apply(Collections.singletonList(null), PartitionWriteResponseMode.NONE));
    }

    @Test
    void appliesPutThroughSyncApi() {
        var api = new RecordingApi();
        var mutation = ReplicatedMutation.put(4, new Keys(buf("k1"), buf("k2")), buf("value"));

        new ReplicatedMutationApplier(api).apply(mutation);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Put.class, api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(4, command.columnId());
        assertEquals(mutation.keys(), command.keys());
        assertArrayEquals(mutation.requiredValue().toByteArray(), command.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesDeleteThroughSyncApi() {
        var api = new RecordingApi();
        var mutation = ReplicatedMutation.delete(6, new Keys(buf("k")));

        new ReplicatedMutationApplier(api).apply(mutation);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Delete.class, api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(6, command.columnId());
        assertEquals(mutation.keys(), command.keys());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesMergeThroughSyncApi() {
        var api = new RecordingApi();
        var mutation = ReplicatedMutation.merge(8, new Keys(buf("counter")), buf("delta"));

        new ReplicatedMutationApplier(api).apply(mutation);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Merge.class, api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(8, command.columnId());
        assertEquals(mutation.keys(), command.keys());
        assertArrayEquals(mutation.requiredValue().toByteArray(), command.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesHomogeneousBatchThroughMultiWriteApi() {
        var api = new RecordingApi();
        var mutations = List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.put(4, new Keys(buf("key-2")), buf("value-2"))
        );

        new ReplicatedMutationApplier(api).apply(mutations);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class, api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(4, command.columnId());
        assertEquals(List.of(mutations.getFirst().keys(), mutations.get(1).keys()), command.keys());
        assertArrayEquals(mutations.getFirst().requiredValue().toByteArray(),
                assertInstanceOf(Buf.class, command.values().getFirst()).toByteArray());
        assertArrayEquals(mutations.get(1).requiredValue().toByteArray(),
                assertInstanceOf(Buf.class, command.values().get(1)).toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesHomogeneousDeleteBatchThroughMultiWriteApi() {
        var api = new RecordingApi();
        var mutations = List.of(
                ReplicatedMutation.delete(4, new Keys(buf("key-1"))),
                ReplicatedMutation.delete(4, new Keys(buf("key-2")))
        );

        new ReplicatedMutationApplier(api).apply(mutations);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti.class,
                api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(4, command.columnId());
        assertEquals(List.of(mutations.getFirst().keys(), mutations.get(1).keys()), command.keys());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesHomogeneousMergeBatchThroughMultiWriteApi() {
        var api = new RecordingApi();
        var mutations = List.of(
                ReplicatedMutation.merge(4, new Keys(buf("key-1")), buf("delta-1")),
                ReplicatedMutation.merge(4, new Keys(buf("key-2")), buf("delta-2"))
        );

        new ReplicatedMutationApplier(api).apply(mutations);

        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti.class,
                api.singleCommand());
        assertEquals(0, command.transactionOrUpdateId());
        assertEquals(4, command.columnId());
        assertEquals(List.of(mutations.getFirst().keys(), mutations.get(1).keys()), command.keys());
        assertArrayEquals(mutations.getFirst().requiredValue().toByteArray(),
                assertInstanceOf(Buf.class, command.values().getFirst()).toByteArray());
        assertArrayEquals(mutations.get(1).requiredValue().toByteArray(),
                assertInstanceOf(Buf.class, command.values().get(1)).toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesMixedBatchInPayloadOrder() {
        var api = new RecordingApi();
        var mutations = List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.delete(4, new Keys(buf("key-2")))
        );

        new ReplicatedMutationApplier(api).apply(mutations);

        assertEquals(2, api.commands.size());
        var put = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Put.class, api.commands.getFirst());
        assertEquals(mutations.getFirst().keys(), put.keys());
        var delete = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Delete.class, api.commands.get(1));
        assertEquals(mutations.get(1).keys(), delete.keys());
    }

    @Test
    void appliesPutWithPreviousValueResponseRequest() {
        var api = new ResponseApi(buf("previous"));
        var mutation = ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"));

        var responses = new ReplicatedMutationApplier(api).apply(PartitionWriteCommand.single(requestId(), 3,
                mutation, PartitionWriteResponseMode.PREVIOUS_VALUE));

        assertEquals(1, responses.size());
        var previous = assertInstanceOf(Buf.class, responses.getFirst());
        assertArrayEquals(buf("previous").toByteArray(), previous.toByteArray());
        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Put.class, api.singleCommand());
        assertEquals(RequestType.RequestTypeId.PREVIOUS, command.requestType().getRequestTypeId());
    }

    @Test
    void preservesNullPreviousValueResponses() {
        var api = new ResponseApi(null);
        var mutation = ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"));

        var responses = new ReplicatedMutationApplier(api).apply(PartitionWriteCommand.single(requestId(), 3,
                mutation, PartitionWriteResponseMode.PREVIOUS_VALUE));

        assertEquals(1, responses.size());
        assertNull(responses.getFirst());
    }

    @Test
    void appliesDeleteWithPreviousPresenceResponseRequest() {
        var api = new ResponseApi(Boolean.TRUE);
        var mutation = ReplicatedMutation.delete(4, new Keys(buf("key")));

        var responses = new ReplicatedMutationApplier(api).apply(PartitionWriteCommand.single(requestId(), 3,
                mutation, PartitionWriteResponseMode.PREVIOUS_PRESENCE));

        assertEquals(List.of(Boolean.TRUE), responses);
        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Delete.class, api.singleCommand());
        assertEquals(RequestType.RequestTypeId.PREVIOUS_PRESENCE, command.requestType().getRequestTypeId());
    }

    @Test
    void appliesResponseProducingPutBatchThroughMultiWriteApi() {
        var responses = new ArrayList<Buf>();
        responses.add(buf("previous-1"));
        responses.add(buf("previous-2"));
        var api = new ResponseApi(responses);
        var mutations = List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.put(4, new Keys(buf("key-2")), buf("value-2"))
        );

        var appliedResponses = new ReplicatedMutationApplier(api).apply(new PartitionWriteCommand(requestId(), 3,
                mutations, PartitionWriteResponseMode.PREVIOUS_VALUE));

        assertEquals(2, appliedResponses.size());
        assertArrayEquals(buf("previous-1").toByteArray(),
                assertInstanceOf(Buf.class, appliedResponses.getFirst()).toByteArray());
        assertArrayEquals(buf("previous-2").toByteArray(),
                assertInstanceOf(Buf.class, appliedResponses.get(1)).toByteArray());
        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class,
                api.singleCommand());
        assertEquals(RequestType.RequestTypeId.PREVIOUS, command.requestType().getRequestTypeId());
    }

    @Test
    void responseProducingBatchMustReturnOneResponsePerMutation() {
        var api = new ResponseApi(List.of(buf("previous-1")));
        var mutations = List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.put(4, new Keys(buf("key-2")), buf("value-2"))
        );

        var failure = assertThrows(IllegalStateException.class, () -> new ReplicatedMutationApplier(api)
                .apply(new PartitionWriteCommand(requestId(), 3, mutations, PartitionWriteResponseMode.PREVIOUS_VALUE)));

        assertEquals("partition write batch response count 1 does not match mutation count 2", failure.getMessage());
        assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class, api.singleCommand());
    }

    @Test
    void responseProducingBatchMustReturnAResponseList() {
        var api = new ResponseApi(null);
        var mutations = List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.put(4, new Keys(buf("key-2")), buf("value-2"))
        );

        var failure = assertThrows(IllegalStateException.class, () -> new ReplicatedMutationApplier(api)
                .apply(new PartitionWriteCommand(requestId(), 3, mutations, PartitionWriteResponseMode.PREVIOUS_VALUE)));

        assertEquals("responses must not be null", failure.getMessage());
        assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class, api.singleCommand());
    }

    @Test
    void appliesMergeWithMergedResponseRequest() {
        var api = new ResponseApi(buf("merged"));
        var mutation = ReplicatedMutation.merge(4, new Keys(buf("key")), buf("delta"));

        var responses = new ReplicatedMutationApplier(api).apply(PartitionWriteCommand.single(requestId(), 3,
                mutation, PartitionWriteResponseMode.MERGED_VALUE));

        assertEquals(1, responses.size());
        var merged = assertInstanceOf(Buf.class, responses.getFirst());
        assertArrayEquals(buf("merged").toByteArray(), merged.toByteArray());
        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Merge.class, api.singleCommand());
        assertEquals(RequestType.RequestTypeId.MERGED, command.requestType().getRequestTypeId());
    }

    @Test
    void mutationListEntriesMustNotBeNull() {
        var mutations = new ArrayList<ReplicatedMutation>();
        mutations.add(null);

        var failure = assertThrows(IllegalArgumentException.class, () -> new ReplicatedMutationApplier(new RecordingApi())
                .apply(mutations, PartitionWriteResponseMode.NONE));

        assertEquals("mutation must not be null", failure.getMessage());
    }

    private static void assertIllegalArgument(String expectedMessage, Runnable action) {
        var failure = assertThrows(IllegalArgumentException.class, action::run);

        assertEquals(expectedMessage, failure.getMessage());
    }

    @Test
    void malformedPayloadDoesNotDecode() {
        var api = new RecordingApi();
        var payload = ReplicatedMutationCodec.encode(ReplicatedMutation.put(1, new Keys(buf("k")), buf("v")));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> ReplicatedMutationCodec.decodeBatch(payload));
        assertEquals(0, api.commands.size());
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), 1);
    }

    private static class RecordingApi implements RocksDBSyncAPI {

        private final List<RocksDBAPICommand<?, ?, ?>> commands = new ArrayList<>();

        @Override
        @SuppressWarnings("unchecked")
        public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
            commands.add(req);
            return (RS) resultFor(req);
        }

        protected RocksDBAPICommand<?, ?, ?> singleCommand() {
            assertEquals(1, commands.size());
            return commands.getFirst();
        }

        protected Object resultFor(RocksDBAPICommand<?, ?, ?> req) {
            return null;
        }
    }

    private static final class ResponseApi extends RecordingApi {

        private final Object response;

        private ResponseApi(Object response) {
            this.response = response;
        }

        @Override
        protected Object resultFor(RocksDBAPICommand<?, ?, ?> req) {
            return response;
        }
    }
}
