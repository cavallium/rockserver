package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandValidator;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionPointReadCommandValidatorTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = metadata();
        var partition = partition();
        var command = command(4, 3, 0x18);

        assertIllegalArgument("metadata must not be null",
                () -> PartitionPointReadCommandValidator.calculatedKey(null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionPointReadCommandValidator.calculatedKey(metadata, null));
        assertIllegalArgument("metadata must not be null",
                () -> PartitionPointReadCommandValidator.validateForPartition(null, partition, command));
        assertIllegalArgument("partition must not be null",
                () -> PartitionPointReadCommandValidator.validateForPartition(metadata, null, command));
        assertIllegalArgument("command must not be null",
                () -> PartitionPointReadCommandValidator.validateForPartition(metadata, partition, null));
    }

    @Test
    void returnsCalculatedKeyForMatchingColumnEpochAndOwnedPartitionRange() {
        var calculatedKey = PartitionPointReadCommandValidator.validateForPartition(
                metadata(), partition(), command(4, 3, 0x18));

        assertArrayEquals(buf(0x18).toByteArray(), calculatedKey.toByteArray());
    }

    @Test
    void rejectsSchemaEpochMismatch() {
        assertThrows(IllegalArgumentException.class,
                () -> PartitionPointReadCommandValidator.validateForPartition(
                        metadata(), partition(), command(4, 4, 0x18)));
    }

    @Test
    void rejectsUnknownColumn() {
        assertThrows(IllegalArgumentException.class,
                () -> PartitionPointReadCommandValidator.validateForPartition(
                        metadata(), partition(), command(5, 3, 0x18)));
    }

    @Test
    void rejectsKeyOutsidePartitionRange() {
        assertThrows(IllegalArgumentException.class,
                () -> PartitionPointReadCommandValidator.validateForPartition(
                        metadata(), partition(), command(4, 3, 0x20)));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static ClusterMetadata metadata() {
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        return new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of(), Map.of(4L, column));
    }

    private static PartitionPointReadCommand command(long columnId, long schemaEpoch, int key) {
        return new PartitionPointReadCommand(schemaEpoch, columnId, new Keys(buf(key)));
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static PartitionDescriptor partition() {
        var leader = NodeId.of("n1");
        return new PartitionDescriptor(
                new ShardId(7),
                buf(0x10),
                buf(0x20),
                3,
                "group-7",
                List.of(new ReplicaMember(leader, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 0, 0,
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
