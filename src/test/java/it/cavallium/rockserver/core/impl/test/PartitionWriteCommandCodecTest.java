package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSeal;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCodec;
import it.cavallium.rockserver.core.common.Keys;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.RecordComponent;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class PartitionWriteCommandCodecTest {

    private static final int MAGIC = 0x52535057;
    private static final int MUTATION_BATCH_MAGIC = 0x52534D42;

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var mutation = ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"));

        assertIllegalArgument("requestId must not be null",
                () -> new PartitionWriteCommand(null, 11, List.of(mutation)));
        assertIllegalArgument("mutations must not be null",
                () -> new PartitionWriteCommand(requestId(), 11, null));
        assertIllegalArgument("mutation must not be null",
                () -> new PartitionWriteCommand(requestId(), 11, Collections.singletonList(null)));
        assertIllegalArgument("responseMode must not be null",
                () -> new PartitionWriteCommand(requestId(), 11, List.of(mutation), null));
        assertIllegalArgument("mutation must not be null",
                () -> PartitionWriteCommand.single(requestId(), 11, null));
        assertIllegalArgument("partitionId must not be null",
                () -> new PartitionWriteCommandSeal(17, null, 19, null, null));
        assertIllegalArgument("calculatedKey must not be null",
                () -> new PartitionWriteCommandSeal(17, new ShardId(5), 19, null, null)
                        .containsCalculatedKey(null));
        assertIllegalArgument("mutations must not be null",
                () -> PartitionWriteResponseMode.NONE.validate(null));
        assertIllegalArgument("mutation must not be null",
                () -> PartitionWriteResponseMode.NONE.validate(Collections.singletonList(null)));
        assertIllegalArgument("command must not be null", () -> PartitionWriteCommandCodec.encode(null));
        assertIllegalArgument("payload must not be null", () -> PartitionWriteCommandCodec.decode(null));
    }

    @Test
    void roundTripsPartitionWriteCommand() {
        var command = new PartitionWriteCommand(requestId(), 11, List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.delete(4, new Keys(buf("key-2")))
        ));

        var decoded = PartitionWriteCommandCodec.decode(PartitionWriteCommandCodec.encode(command));

        assertEquals(command.requestId(), decoded.requestId());
        assertEquals(command.schemaEpoch(), decoded.schemaEpoch());
        assertEquals(command.mutations(), decoded.mutations());
        assertEquals(PartitionWriteResponseMode.NONE, decoded.responseMode());
    }

    @Test
    void roundTripsResponseProducingPartitionWriteCommand() {
        var command = new PartitionWriteCommand(requestId(), 11, List.of(
                ReplicatedMutation.merge(4, new Keys(buf("key")), buf("delta"))
                        .withCalculatedKey(buf("calculated-key"))
        ), PartitionWriteResponseMode.MERGED_VALUE,
                new PartitionWriteCommandSeal(17, new ShardId(5), 19, buf("a"), buf("z")));

        var decoded = PartitionWriteCommandCodec.decode(PartitionWriteCommandCodec.encode(command));

        assertEquals(command.requestId(), decoded.requestId());
        assertEquals(command.schemaEpoch(), decoded.schemaEpoch());
        assertEquals(command.mutations(), decoded.mutations());
        assertEquals(PartitionWriteResponseMode.MERGED_VALUE, decoded.responseMode());
        assertEquals(command.seal(), decoded.seal());
    }

    @Test
    void decodesLegacyVersionOneCommandWithoutResponseMode() {
        var command = new PartitionWriteCommand(requestId(), 11, List.of(
                ReplicatedMutation.put(4, new Keys(buf("key-1")), buf("value-1")),
                ReplicatedMutation.delete(4, new Keys(buf("key-2")))
        ));

        var decoded = PartitionWriteCommandCodec.decode(legacyVersionOnePayload(command));

        assertEquals(command.requestId(), decoded.requestId());
        assertEquals(command.schemaEpoch(), decoded.schemaEpoch());
        assertEquals(command.mutations(), decoded.mutations());
        assertEquals(PartitionWriteResponseMode.NONE, decoded.responseMode());
    }

    @Test
    void decodesLegacyVersionTwoCommandWithoutSeal() {
        var command = PartitionWriteCommand.single(requestId(), 11,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value")),
                PartitionWriteResponseMode.PREVIOUS_VALUE);

        var decoded = PartitionWriteCommandCodec.decode(legacyVersionTwoPayload(command));

        assertEquals(command.requestId(), decoded.requestId());
        assertEquals(command.schemaEpoch(), decoded.schemaEpoch());
        assertEquals(command.mutations(), decoded.mutations());
        assertEquals(PartitionWriteResponseMode.PREVIOUS_VALUE, decoded.responseMode());
        assertNull(decoded.seal());
    }

    @Test
    void rejectsUnsupportedFutureCommandVersion() {
        var payload = commandPayloadWithVersion(4);

        assertEquals("Unsupported partition write command version: 4",
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsUnsupportedPreCompatibilityCommandVersion() {
        var payload = commandPayloadWithVersion(0);

        assertEquals("Unsupported partition write command version: 0",
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void commandRecordContainsOnlyDeterministicReplayInputs() {
        var componentNames = Arrays.stream(PartitionWriteCommand.class.getRecordComponents())
                .map(RecordComponent::getName)
                .toList();

        assertEquals(List.of("requestId", "schemaEpoch", "mutations", "responseMode", "seal"), componentNames);
    }

    @Test
    void rejectsChecksumMismatch() {
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 1,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"))));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload));
    }

    @Test
    void rejectsTrailingBytes() {
        var payload = PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(requestId(), 1,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"))));
        var payloadWithTrailingByte = Arrays.copyOf(payload, payload.length + 1);

        assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payloadWithTrailingByte));
    }

    @Test
    void rejectsOversizedDecodedMutationPayloadLengthBeforeAllocation() {
        var payload = commandPayloadWithDeclaredMutationPayloadLength(Integer.MAX_VALUE);

        assertEquals("Invalid partition write command byte length: " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsWithinLimitDecodedMutationPayloadLengthBeyondRemainingPayloadBeforeAllocation() {
        var payload = commandPayloadWithDeclaredMutationPayloadLength(64 * 1024 * 1024);

        assertEquals("Unexpected end of partition write command payload",
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsDecodedMutationBatchAbovePartitionCommandLimitBeforePreallocation() {
        var payload = commandPayloadWithMutationPayload(
                mutationBatchPayloadWithDeclaredMutationCount(PartitionWriteCommand.MAX_MUTATIONS + 1));

        assertEquals("Replicated mutation batch size "
                        + (PartitionWriteCommand.MAX_MUTATIONS + 1)
                        + " exceeds limit "
                        + PartitionWriteCommand.MAX_MUTATIONS,
                assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommandCodec.decode(payload))
                        .getMessage());
    }

    @Test
    void rejectsEmptyMutationList() {
        assertThrows(IllegalArgumentException.class, () -> new PartitionWriteCommand(requestId(), 1, List.of()));
    }

    @Test
    void rejectsOversizedMutationList() {
        var mutation = ReplicatedMutation.put(4, new Keys(buf("key")), buf("value"));
        var mutations = Collections.nCopies(PartitionWriteCommand.MAX_MUTATIONS + 1, mutation);

        assertEquals("partition write command contains "
                        + (PartitionWriteCommand.MAX_MUTATIONS + 1)
                        + " mutations, above limit "
                        + PartitionWriteCommand.MAX_MUTATIONS,
                assertThrows(IllegalArgumentException.class,
                        () -> new PartitionWriteCommand(requestId(), 1, mutations))
                        .getMessage());
    }

    @Test
    void rejectsIncompatibleResponseMode() {
        assertThrows(IllegalArgumentException.class, () -> PartitionWriteCommand.single(requestId(), 1,
                ReplicatedMutation.put(4, new Keys(buf("key")), buf("value")),
                PartitionWriteResponseMode.MERGED_VALUE));
    }

    @Test
    void copiesMutationList() {
        var mutations = new java.util.ArrayList<ReplicatedMutation>();
        mutations.add(ReplicatedMutation.put(4, new Keys(buf("key")), buf("value")));

        var command = new PartitionWriteCommand(requestId(), 1, mutations);
        mutations.clear();

        assertEquals(1, command.mutations().size());
        assertThrows(UnsupportedOperationException.class, () -> command.mutations().clear());
        assertArrayEquals(buf("value").toByteArray(), command.mutations().getFirst().requiredValue().toByteArray());
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), 7);
    }

    private static byte[] legacyVersionOnePayload(PartitionWriteCommand command) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(1);
            var clientUuid = command.requestId().clientId().value();
            out.writeLong(clientUuid.getMostSignificantBits());
            out.writeLong(clientUuid.getLeastSignificantBits());
            out.writeLong(command.requestId().sequence());
            out.writeLong(command.schemaEpoch());
            writeBytes(out, ReplicatedMutationCodec.encodeBatch(command.mutations()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return wrapWithChecksum(bytes.toByteArray());
    }

    private static byte[] commandPayloadWithVersion(int version) {
        var body = ByteBuffer.allocate(Integer.BYTES * 2)
                .putInt(MAGIC)
                .putInt(version)
                .array();
        return wrapWithChecksum(body);
    }

    private static byte[] legacyVersionTwoPayload(PartitionWriteCommand command) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(2);
            var clientUuid = command.requestId().clientId().value();
            out.writeLong(clientUuid.getMostSignificantBits());
            out.writeLong(clientUuid.getLeastSignificantBits());
            out.writeLong(command.requestId().sequence());
            out.writeLong(command.schemaEpoch());
            out.writeInt(command.responseMode().code());
            writeBytes(out, ReplicatedMutationCodec.encodeBatch(command.mutations()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return wrapWithChecksum(bytes.toByteArray());
    }

    private static byte[] commandPayloadWithDeclaredMutationPayloadLength(int length) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(2);
            var clientUuid = requestId().clientId().value();
            out.writeLong(clientUuid.getMostSignificantBits());
            out.writeLong(clientUuid.getLeastSignificantBits());
            out.writeLong(requestId().sequence());
            out.writeLong(11);
            out.writeInt(PartitionWriteResponseMode.NONE.code());
            out.writeInt(length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return wrapWithChecksum(bytes.toByteArray());
    }

    private static byte[] commandPayloadWithMutationPayload(byte[] mutationPayload) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(2);
            var clientUuid = requestId().clientId().value();
            out.writeLong(clientUuid.getMostSignificantBits());
            out.writeLong(clientUuid.getLeastSignificantBits());
            out.writeLong(requestId().sequence());
            out.writeLong(11);
            out.writeInt(PartitionWriteResponseMode.NONE.code());
            writeBytes(out, mutationPayload);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return wrapWithChecksum(bytes.toByteArray());
    }

    private static byte[] mutationBatchPayloadWithDeclaredMutationCount(int mutationCount) {
        var body = ByteBuffer.allocate(3 * Integer.BYTES)
                .putInt(MUTATION_BATCH_MAGIC)
                .putInt(2)
                .putInt(mutationCount)
                .array();
        return wrapWithChecksum(body);
    }

    private static byte[] wrapWithChecksum(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(crc.getValue());
        return payload.array();
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static Buf buf(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
