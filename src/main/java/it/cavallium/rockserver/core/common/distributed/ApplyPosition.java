package it.cavallium.rockserver.core.common.distributed;

/** A nonnegative Raft term/index; absence of applied state is represented separately. */
public record ApplyPosition(long term, long index) {
    public ApplyPosition {
        if (term < 0 || index < 0) {
            throw new IllegalArgumentException("Position term and index must be nonnegative");
        }
    }
}
