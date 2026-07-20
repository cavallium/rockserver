package it.cavallium.rockserver.core.cluster;

public enum WriteDurability {
    QUORUM_COMMIT(false),
    QUORUM_COMMIT_APPLY(true),
    QUORUM_COMMIT_APPLY_FLUSH(true);

    private final boolean waitsForLeaderApply;

    WriteDurability(boolean waitsForLeaderApply) {
        this.waitsForLeaderApply = waitsForLeaderApply;
    }

    public boolean waitsForLeaderApply() {
        return waitsForLeaderApply;
    }

    public boolean requiresExplicitFlushBeforeAck() {
        return this == QUORUM_COMMIT_APPLY_FLUSH;
    }

    public boolean requiresPreApplyCommitAckPath() {
        return this == QUORUM_COMMIT;
    }

    public WriteDurability applyBasedFallback() {
        return requiresPreApplyCommitAckPath() ? QUORUM_COMMIT_APPLY : this;
    }

    public WriteDurabilityMatrixEntry matrixEntry(boolean responseProducingWrite) {
        var effectiveDurability = applyBasedFallback();
        var responseProducingAllowed = this != QUORUM_COMMIT || !responseProducingWrite;
        return new WriteDurabilityMatrixEntry(
                this,
                effectiveDurability,
                RatisQuorumCondition.MAJORITY_COMMITTED,
                waitsForLeaderApply,
                effectiveDurability.waitsForLeaderApply(),
                this != QUORUM_COMMIT,
                responseProducingWrite && this != QUORUM_COMMIT,
                requiresExplicitFlushBeforeAck(),
                responseProducingAllowed,
                effectiveDurability != this
        );
    }
}
