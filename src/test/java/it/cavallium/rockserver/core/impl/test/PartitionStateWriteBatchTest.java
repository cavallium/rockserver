package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeState;
import it.cavallium.rockserver.core.cluster.command.ClientDedupeStateCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionStateMetadataCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionStateWrite;
import it.cavallium.rockserver.core.cluster.command.PartitionStateWriteBatch;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEvent;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionStateWriteBatchTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var userWrite = PartitionStateWrite.put(text("user-key"), text("value"));
        var userWrites = List.of(userWrite);
        var dedupeKey = RockserverStorageKeyspace.dedupeResponse(requestId());
        var dedupeValue = PartitionWriteResponseCodec.encodeResponses(List.of());
        var clientDedupeStateKey = RockserverStorageKeyspace.dedupeClientState(requestId().clientId());
        var clientDedupeStateValue = ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requestId()));
        var lastAppliedKey = RockserverStorageKeyspace.lastAppliedIndex();
        var lastAppliedValue = PartitionStateMetadataCodec.encodeLastAppliedIndex(12);
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value"))
        ));

        assertIllegalArgument("operation must not be null",
                () -> new PartitionStateWrite(null, text("key"), text("value")));
        assertIllegalArgument("key must not be null",
                () -> new PartitionStateWrite(PartitionStateWrite.Operation.PUT, null, text("value")));
        assertIllegalArgument("userWrites must not be null",
                () -> batch(null, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("userWrites[0] must not be null",
                () -> batch(Collections.singletonList(null), dedupeKey, dedupeValue, clientDedupeStateKey,
                        clientDedupeStateValue, lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("dedupeKey must not be null",
                () -> batch(userWrites, null, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("dedupeValue must not be null",
                () -> batch(userWrites, dedupeKey, null, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("clientDedupeStateKey must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, null, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("clientDedupeStateValue must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, null,
                        lastAppliedKey, lastAppliedValue, List.of()));
        assertIllegalArgument("lastAppliedKey must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        null, lastAppliedValue, List.of()));
        assertIllegalArgument("lastAppliedValue must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, null, List.of()));
        assertIllegalArgument("cdcEvents must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, null));
        assertIllegalArgument("cdcEvents[0] must not be null",
                () -> batch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey, clientDedupeStateValue,
                        lastAppliedKey, lastAppliedValue, Collections.singletonList(null)));
        assertIllegalArgument("metadata must not be null",
                () -> PartitionStateWriteBatch.fromAppliedCommand(null, command, List.of(), 12));
        assertIllegalArgument("command must not be null",
                () -> PartitionStateWriteBatch.fromAppliedCommand(metadata(), null, List.of(), 12));
        assertIllegalArgument("responses must not be null",
                () -> PartitionStateWriteBatch.fromAppliedCommand(metadata(), command, null, 12));
    }

    @Test
    @SuppressWarnings("DataFlowIssue")
    void lastAppliedIndexCodecRejectsMalformedInputsWithExplicitDiagnostics() {
        assertIllegalArgument("payload must not be null",
                () -> PartitionStateMetadataCodec.decodeLastAppliedIndex(null));
        assertIllegalArgument("Invalid lastAppliedIndex payload length: 0",
                () -> PartitionStateMetadataCodec.decodeLastAppliedIndex(new byte[0]));
        assertIllegalArgument("lastAppliedIndex must be >= 0",
                () -> PartitionStateMetadataCodec.encodeLastAppliedIndex(-1));
        assertIllegalArgument("lastAppliedIndex must be >= 0",
                () -> PartitionStateMetadataCodec.decodeLastAppliedIndex(
                        ByteBuffer.allocate(Long.BYTES).putLong(-1).array()));
    }

    @Test
    void materializesUserDedupeAndLastAppliedWrites() {
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")),
                ReplicatedMutation.delete(4, new Keys(buf(0x19))),
                ReplicatedMutation.merge(4, new Keys(buf(0x1a)), text("delta"))
        ));

        var batch = PartitionStateWriteBatch.fromAppliedCommand(metadata(), command, List.of(), 12);

        assertEquals(3, batch.userMutationCount());
        assertTrue(batch.containsUserWriteOperation(PartitionStateWrite.Operation.PUT));
        assertTrue(batch.containsUserWriteOperation(PartitionStateWrite.Operation.DELETE));
        assertTrue(batch.containsUserWriteOperation(PartitionStateWrite.Operation.MERGE));
        assertArrayEquals(RockserverStorageKeyspace.userData(4, buf(0x18)).toByteArray(),
                batch.userWrites().get(0).key().toByteArray());
        assertArrayEquals(RockserverStorageKeyspace.dedupeResponse(requestId()).toByteArray(),
                batch.dedupeKey().toByteArray());
        assertEquals(List.of(), PartitionWriteResponseCodec.decodeResponses(batch.dedupeValue()));
        assertArrayEquals(RockserverStorageKeyspace.dedupeClientState(requestId().clientId()).toByteArray(),
                batch.clientDedupeStateKey().toByteArray());
        assertEquals(ClientDedupeState.acknowledged(requestId()),
                ClientDedupeStateCodec.decode(batch.clientDedupeStateValue()));
        assertArrayEquals(RockserverStorageKeyspace.lastAppliedIndex().toByteArray(),
                batch.lastAppliedKey().toByteArray());
        assertEquals(12, PartitionStateMetadataCodec.decodeLastAppliedIndex(batch.lastAppliedValue()));
    }

    @Test
    void persistsResponsePayloadForResponseProducingCommands() {
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value"))
        ), PartitionWriteResponseMode.PREVIOUS_VALUE);

        var batch = PartitionStateWriteBatch.fromAppliedCommand(metadata(), command, List.of(text("previous")), 13);

        var decoded = PartitionWriteResponseCodec.decodeResponses(batch.dedupeValue());
        assertEquals(1, decoded.size());
        assertArrayEquals(text("previous").toByteArray(), ((Buf) decoded.getFirst()).toByteArray());
    }

    @Test
    void requiresMatchingResponseCountForResponseProducingCommands() {
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value"))
        ), PartitionWriteResponseMode.PREVIOUS_VALUE);

        assertThrows(IllegalArgumentException.class,
                () -> PartitionStateWriteBatch.fromAppliedCommand(metadata(), command, List.of(), 13));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static PartitionStateWriteBatch batch(List<PartitionStateWrite> userWrites,
                                                  Buf dedupeKey,
                                                  byte[] dedupeValue,
                                                  Buf clientDedupeStateKey,
                                                  byte[] clientDedupeStateValue,
                                                  Buf lastAppliedKey,
                                                  byte[] lastAppliedValue,
                                                  List<ReplicatedCdcEvent> cdcEvents) {
        return new PartitionStateWriteBatch(userWrites, dedupeKey, dedupeValue, clientDedupeStateKey,
                clientDedupeStateValue, lastAppliedKey, lastAppliedValue, 12, cdcEvents);
    }

    private static ClusterMetadata metadata() {
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(4L, column));
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), 7);
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
