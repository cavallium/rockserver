package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSeal;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSealer;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandValidator;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionWriteCommandValidatorTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = command(4, 3, buf(0x18));

        assertIllegalArgument("metadata must not be null",
                () -> PartitionWriteCommandValidator.validate(null, partition, command));
        assertIllegalArgument("partition must not be null",
                () -> PartitionWriteCommandValidator.validate(metadata, null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionWriteCommandValidator.validate(metadata, partition, null));
        assertIllegalArgument("metadata must not be null",
                () -> PartitionWriteCommandValidator.validateForReplay(null, partition, command));
        assertIllegalArgument("partition must not be null",
                () -> PartitionWriteCommandValidator.validateForReplay(metadata, null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionWriteCommandValidator.validateForReplay(metadata, partition, null));
        assertIllegalArgument("metadata must not be null",
                () -> PartitionWriteCommandValidator.validateForProposal(null, partition, command));
        assertIllegalArgument("partition must not be null",
                () -> PartitionWriteCommandValidator.validateForProposal(metadata, null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionWriteCommandValidator.validateForProposal(metadata, partition, null));
        assertIllegalArgument("metadata must not be null",
                () -> PartitionWriteCommandSealer.seal(null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionWriteCommandSealer.seal(metadata, null));
    }

    @Test
    void acceptsCommandForMatchingColumnEpochAndOwnedPartitionRange() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = command(4, 3, buf(0x18));

        PartitionWriteCommandValidator.validate(metadata, partition, command);
    }

    @Test
    void rejectsSchemaEpochMismatch() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = command(4, 4, buf(0x18));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    @Test
    void rejectsUnknownColumn() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = command(5, 3, buf(0x18));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    @Test
    void rejectsKeyOutsidePartitionRange() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = command(4, 3, buf(0x20));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    @Test
    void rejectsMultiKeyCommandWhenAnyKeyIsOutsidePartitionRange() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), buf(0x01)),
                ReplicatedMutation.put(4, new Keys(buf(0x20)), buf(0x02))
        ));

        assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));
    }

    @Test
    void rejectsCommandSpanningMultipleColumns() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(
                4L, new ClusterColumnMetadata(4, "accounts", schema(), 3),
                5L, new ClusterColumnMetadata(5, "events", schema(), 3)
        ));
        var partition = partition(buf(0x10), buf(0x20));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), buf(0x01)),
                ReplicatedMutation.put(5, new Keys(buf(0x19)), buf(0x02))
        ));

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));

        assertEquals("partition write command must target one column", thrown.getMessage());
    }

    @Test
    void rejectsSealedCalculatedKeyThatDoesNotMatchCurrentSchema() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), buf(0x01)).withCalculatedKey(buf(0x19))
        ), PartitionWriteResponseMode.NONE, new PartitionWriteCommandSeal(7, partition.partitionId(), 3,
                partition.startKeyInclusive(), partition.endKeyExclusive()));

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validate(metadata, partition, command));

        assertEquals("sealed partition write calculated key does not match column schema", thrown.getMessage());
    }

    @Test
    void rejectsHalfSealedMutationDuringReplay() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), buf(0x01))
        ), PartitionWriteResponseMode.NONE, new PartitionWriteCommandSeal(7, partition.partitionId(), 3,
                partition.startKeyInclusive(), partition.endKeyExclusive()));

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validateForReplay(metadata, partition, command));

        assertEquals("sealed partition write mutation is missing its calculated key", thrown.getMessage());
    }

    @Test
    void proposalRejectsLegacyUnsealedCommand() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validateForProposal(metadata, partition,
                        command(4, 3, buf(0x18))));

        assertEquals("partition write proposal must be sealed", thrown.getMessage());
    }

    @Test
    void rejectsSealedReplayCalculatedKeyOutsideAdmittedRange() {
        var metadata = metadata(4, schema(), 3);
        var partition = partition(buf(0x10), buf(0x20));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), buf(0x01)).withCalculatedKey(buf(0x20))
        ), PartitionWriteResponseMode.NONE, new PartitionWriteCommandSeal(7, partition.partitionId(), 3,
                partition.startKeyInclusive(), partition.endKeyExclusive()));

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> PartitionWriteCommandValidator.validateForReplay(metadata, partition, command));

        assertEquals("sealed partition write calculated key is outside admitted range", thrown.getMessage());
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static ClusterMetadata metadata(long columnId, ColumnSchema schema, long schemaEpoch) {
        var column = new ClusterColumnMetadata(columnId, "accounts", schema, schemaEpoch);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(columnId, column));
    }

    private static PartitionWriteCommand command(long columnId, long schemaEpoch, Buf key) {
        return PartitionWriteCommand.single(requestId(), schemaEpoch,
                ReplicatedMutation.put(columnId, new Keys(key), buf(0x01)));
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), 7);
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static PartitionDescriptor partition(Buf startKeyInclusive, Buf endKeyExclusive) {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                new ShardId(7),
                startKeyInclusive,
                endKeyExclusive,
                3,
                "group-7",
                List.of(new ReplicaMember(leader, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
                        true)),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }
}
