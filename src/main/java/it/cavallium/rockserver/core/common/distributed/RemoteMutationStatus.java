package it.cavallium.rockserver.core.common.distributed;

/** Observation only. UNAVAILABLE never authorizes a fresh execution. */
public record RemoteMutationStatus(RemoteMutationToken token, State state, long appliedMutations, long copiedBatches, int retainedResults, byte[] resultDigest) {
    public enum State { PREPARED, PENDING, TERMINAL, ACKNOWLEDGED, EXPIRED, OWNER_UNAVAILABLE, UNAVAILABLE }
    public RemoteMutationStatus { resultDigest=resultDigest==null?null:resultDigest.clone(); }
    /** False means effects/progress are unknown, including expired metadata; never infer zero effects. */
    public boolean progressKnown(){return appliedMutations>=0 && copiedBatches>=0;}
    @Override public byte[] resultDigest() { return resultDigest==null?null:resultDigest.clone(); }
}
