package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteApplier;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationApplier;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.replication.ratis.RockserverRatisStateMachine;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class RockserverRatisStateMachineTest {

    @Test
    void appliesCommittedMutationPayloadAndTracksLastAppliedIndex() {
        var api = new RecordingApi();
        var stateMachine = new RockserverRatisStateMachine(api);
        var writeCommand = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.put(4, new Keys(buf("key-2")), buf("value-2"))
        ));
        var payload = PartitionWriteCommandCodec.encode(writeCommand);

        var result = stateMachine.applyCommitted(1, payload);

        assertTrue(result.applied());
        assertEquals(2, result.mutationsApplied());
        assertEquals(1, result.logIndex());
        assertEquals(1, stateMachine.lastAppliedIndex());
        var command = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti.class,
                api.singleCommand());
        assertEquals(4, command.columnId());
    }

    @Test
    void duplicateOrOlderLogIndexDoesNotApplyTwice() {
        var api = new RecordingApi();
        var stateMachine = new RockserverRatisStateMachine(api);
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.delete(4, new Keys(buf("key")))));

        stateMachine.applyCommitted(2, payload);
        var duplicate = stateMachine.applyCommitted(2, payload);
        var older = stateMachine.applyCommitted(1, payload);

        assertFalse(duplicate.applied());
        assertFalse(older.applied());
        assertEquals(0, duplicate.mutationsApplied());
        assertEquals(0, older.mutationsApplied());
        assertEquals(2, stateMachine.lastAppliedIndex());
        assertEquals(1, api.commands.size());
    }

    @Test
    void appliesBlindPutAndDeletePayloadInLogOrder() {
        var api = new RecordingApi();
        var stateMachine = new RockserverRatisStateMachine(api);
        var putMutation = ReplicatedMutation.put(4, new Keys(buf("put-key")), buf("put-value"));
        var deleteMutation = ReplicatedMutation.delete(4, new Keys(buf("delete-key")));
        var payload = PartitionWriteCommandCodec.encode(new PartitionWriteCommand(requestId(), 3, List.of(
                putMutation,
                deleteMutation
        )));

        var result = stateMachine.applyCommitted(1, payload);

        assertTrue(result.applied());
        assertEquals(2, result.mutationsApplied());
        assertEquals(2, api.commands.size());

        var put = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Put.class, api.commands.getFirst());
        assertEquals(0, put.transactionOrUpdateId());
        assertEquals(4, put.columnId());
        assertEquals(putMutation.keys(), put.keys());
        assertArrayEquals(putMutation.requiredValue().toByteArray(), put.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.NOTHING, put.requestType().getRequestTypeId());

        var delete = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Delete.class, api.commands.get(1));
        assertEquals(0, delete.transactionOrUpdateId());
        assertEquals(4, delete.columnId());
        assertEquals(deleteMutation.keys(), delete.keys());
        assertEquals(RequestType.RequestTypeId.NOTHING, delete.requestType().getRequestTypeId());
    }

    @Test
    void appliesMergeWithMergedResponseInsideStateMachine() {
        var api = new ResponseApi(buf("merged-value"));
        var stateMachine = new RockserverRatisStateMachine(api);
        var mergeMutation = ReplicatedMutation.merge(4, new Keys(buf("merge-key")), buf("delta"));
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3, mergeMutation,
                PartitionWriteResponseMode.MERGED_VALUE));

        var result = stateMachine.applyCommitted(1, payload);

        assertTrue(result.applied());
        assertEquals(1, result.mutationsApplied());
        assertEquals(1, result.responses().size());
        var response = assertInstanceOf(Buf.class, result.responses().getFirst());
        assertArrayEquals(buf("merged-value").toByteArray(), response.toByteArray());

        var merge = assertInstanceOf(RocksDBAPICommand.RocksDBAPICommandSingle.Merge.class, api.singleCommand());
        assertEquals(0, merge.transactionOrUpdateId());
        assertEquals(4, merge.columnId());
        assertEquals(mergeMutation.keys(), merge.keys());
        assertArrayEquals(mergeMutation.requiredValue().toByteArray(), merge.value().toByteArray());
        assertEquals(RequestType.RequestTypeId.MERGED, merge.requestType().getRequestTypeId());
    }

    @Test
    void malformedPayloadDoesNotApplyOrAdvanceLastApplied() {
        var api = new RecordingApi();
        var stateMachine = new RockserverRatisStateMachine(api);
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"))));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> stateMachine.applyCommitted(1, payload));

        assertEquals(0, stateMachine.lastAppliedIndex());
        assertEquals(0, api.commands.size());
    }

    @Test
    void validatorRejectionDoesNotApplyOrAdvanceLastApplied() {
        var api = new RecordingApi();
        var stateMachine = new RockserverRatisStateMachine(new ReplicatedMutationApplier(api),
                ignored -> {
                    throw new IllegalArgumentException("rejected");
                });
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"))));

        assertThrows(IllegalArgumentException.class, () -> stateMachine.applyCommitted(1, payload));

        assertEquals(0, stateMachine.lastAppliedIndex());
        assertEquals(0, api.commands.size());
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var api = new RecordingApi();
        var replicatedApplier = new ReplicatedMutationApplier(api);
        var stateMachine = new RockserverRatisStateMachine(api);
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"))));

        assertRejectsNull("api must not be null",
                () -> new RockserverRatisStateMachine((RocksDBSyncAPI) null));
        assertRejectsNull("applier must not be null",
                () -> new RockserverRatisStateMachine((ReplicatedMutationApplier) null));
        assertRejectsNull("applier must not be null",
                () -> new RockserverRatisStateMachine((ReplicatedMutationApplier) null, _ -> {
                }));
        assertRejectsNull("validator must not be null",
                () -> new RockserverRatisStateMachine(replicatedApplier, null));
        assertRejectsNull("applier must not be null",
                () -> new RockserverRatisStateMachine((PartitionWriteApplier) null, _ -> {
                }));
        assertRejectsNull("validator must not be null",
                () -> new RockserverRatisStateMachine((_, _) -> List.of(), null));
        assertRejectsNull("failureInjector must not be null",
                () -> new RockserverRatisStateMachine(
                        (_, _) -> List.of(),
                        _ -> {
                        },
                        (ReplicationFailureInjector) null
                ));
        assertRejectsNull("payload must not be null", () -> stateMachine.applyCommitted(1, null));

        var nullResponseStateMachine = new RockserverRatisStateMachine((_, _) -> null, _ -> {
        });
        assertRejectsNull("responses must not be null",
                () -> nullResponseStateMachine.applyCommitted(1, payload));
        assertEquals(0, nullResponseStateMachine.lastAppliedIndex());

        assertRejectsNull("responses must not be null",
                () -> new RockserverRatisStateMachine.ApplyResult(1, 0, true, null));
    }

    @Test
    void applyResultPreservesNullResponses() {
        var responses = new ArrayList<>();
        responses.add(null);

        var result = new RockserverRatisStateMachine.ApplyResult(1, 1, true, responses);

        assertEquals(1, result.responses().size());
        assertNull(result.responses().getFirst());
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

        private final Buf response;

        private ResponseApi(Buf response) {
            this.response = response;
        }

        @Override
        protected Object resultFor(RocksDBAPICommand<?, ?, ?> req) {
            return response;
        }
    }

    private static void assertRejectsNull(String expectedMessage, Executable executable) {
        var error = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(expectedMessage, error.getMessage());
    }
}
