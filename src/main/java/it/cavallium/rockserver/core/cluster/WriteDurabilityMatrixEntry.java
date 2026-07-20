package it.cavallium.rockserver.core.cluster;

public record WriteDurabilityMatrixEntry(WriteDurability requestedDurability,
                                         WriteDurability effectiveAdapterDurability,
                                         RatisQuorumCondition ratisQuorumCondition,
                                         boolean modeRequiresLeaderApplyBeforeAck,
                                         boolean adapterWaitsForLeaderApplyBeforeAck,
                                         boolean modeRequiresRocksDbPersistenceBeforeAck,
                                         boolean requiresResponsePersistenceBeforeAck,
                                         boolean requiresExplicitFlushBeforeAck,
                                         boolean responseProducingWritesAllowed,
                                         boolean usesApplyBasedFallback) {

    public WriteDurabilityMatrixEntry {
        requireArgument(requestedDurability, "requestedDurability");
        requireArgument(effectiveAdapterDurability, "effectiveAdapterDurability");
        requireArgument(ratisQuorumCondition, "ratisQuorumCondition");
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }
}
