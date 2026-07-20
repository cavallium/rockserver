package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ClientRequestId;

import java.util.List;

import org.jetbrains.annotations.Nullable;

public record PartitionWriteCommand(ClientRequestId requestId,
                                    long schemaEpoch,
                                    List<ReplicatedMutation> mutations,
                                    PartitionWriteResponseMode responseMode,
                                    @Nullable PartitionWriteCommandSeal seal) {

    public static final int MAX_MUTATIONS = 16_384;

    public PartitionWriteCommand {
        requireArgument(requestId, "requestId");
        if (schemaEpoch < 0) {
            throw new IllegalArgumentException("schemaEpoch must be >= 0");
        }
        requireArgument(mutations, "mutations");
        if (mutations.isEmpty()) {
            throw new IllegalArgumentException("partition write command must contain at least one mutation");
        }
        if (mutations.size() > MAX_MUTATIONS) {
            throw new IllegalArgumentException("partition write command contains "
                    + mutations.size()
                    + " mutations, above limit "
                    + MAX_MUTATIONS);
        }
        for (var mutation : mutations) {
            requireArgument(mutation, "mutation");
        }
        mutations = List.copyOf(mutations);
        requireArgument(responseMode, "responseMode");
        responseMode.validate(mutations);
    }

    public PartitionWriteCommand(ClientRequestId requestId,
                                 long schemaEpoch,
                                 List<ReplicatedMutation> mutations,
                                 PartitionWriteResponseMode responseMode) {
        this(requestId, schemaEpoch, mutations, responseMode, null);
    }

    public PartitionWriteCommand(ClientRequestId requestId,
                                 long schemaEpoch,
                                 List<ReplicatedMutation> mutations) {
        this(requestId, schemaEpoch, mutations, PartitionWriteResponseMode.NONE);
    }

    public static PartitionWriteCommand single(ClientRequestId requestId,
                                               long schemaEpoch,
                                               ReplicatedMutation mutation) {
        return single(requestId, schemaEpoch, mutation, PartitionWriteResponseMode.NONE);
    }

    public static PartitionWriteCommand single(ClientRequestId requestId,
                                                long schemaEpoch,
                                                ReplicatedMutation mutation,
                                                PartitionWriteResponseMode responseMode) {
        return new PartitionWriteCommand(requestId, schemaEpoch, List.of(requireArgument(mutation, "mutation")),
                responseMode);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
