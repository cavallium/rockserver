package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.RockserverKeyEncoder;
import it.cavallium.rockserver.core.cluster.ShardId;

import java.util.Arrays;

import org.jetbrains.annotations.Nullable;

public record PartitionWriteCommandSeal(long metadataEpoch,
                                        ShardId partitionId,
                                        long partitionEpoch,
                                        @Nullable Buf startKeyInclusive,
                                        @Nullable Buf endKeyExclusive) {

    public PartitionWriteCommandSeal {
        if (metadataEpoch < 0) {
            throw new IllegalArgumentException("metadataEpoch must be >= 0");
        }
        requireArgument(partitionId, "partitionId");
        if (partitionEpoch < 0) {
            throw new IllegalArgumentException("partitionEpoch must be >= 0");
        }
        startKeyInclusive = copy(startKeyInclusive);
        endKeyExclusive = copy(endKeyExclusive);
        if (startKeyInclusive != null
                && endKeyExclusive != null
                && RockserverKeyEncoder.compareCalculatedKeys(startKeyInclusive, endKeyExclusive) >= 0) {
            throw new IllegalArgumentException("partition seal start key must be lower than end key");
        }
    }

    @Override
    public @Nullable Buf startKeyInclusive() {
        return copy(startKeyInclusive);
    }

    @Override
    public @Nullable Buf endKeyExclusive() {
        return copy(endKeyExclusive);
    }

    public boolean containsCalculatedKey(Buf calculatedKey) {
        requireArgument(calculatedKey, "calculatedKey");
        return (startKeyInclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(calculatedKey, startKeyInclusive) >= 0)
                && (endKeyExclusive == null
                || RockserverKeyEncoder.compareCalculatedKeys(calculatedKey, endKeyExclusive) < 0);
    }

    public boolean hasSameRange(@Nullable Buf otherStartKeyInclusive, @Nullable Buf otherEndKeyExclusive) {
        return Arrays.equals(bytes(startKeyInclusive), bytes(otherStartKeyInclusive))
                && Arrays.equals(bytes(endKeyExclusive), bytes(otherEndKeyExclusive));
    }

    private static @Nullable Buf copy(@Nullable Buf value) {
        return value == null ? null : Buf.wrap(value.toByteArray()).freeze();
    }

    private static byte @Nullable [] bytes(@Nullable Buf value) {
        return value == null ? null : value.toByteArray();
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
