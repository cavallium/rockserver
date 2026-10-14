package it.cavallium.rockserver.core.common.distributed;

/** Confirmed applied mutation records; PENDING may have additional unknown effects. UNAVAILABLE never authorizes fresh execution. */
public record RemoteMutationStatus(RemoteMutationToken token, State state, long appliedMutations, long copiedBatches, int retainedResults, CommandResult.Status outcome, byte[] resultDigest) {
    public enum State { PREPARED, PENDING, TERMINAL, ACKNOWLEDGED, EXPIRED, OWNER_UNAVAILABLE, UNAVAILABLE }
    public RemoteMutationStatus {
        java.util.Objects.requireNonNull(token);java.util.Objects.requireNonNull(state);
        if(appliedMutations< -1 || copiedBatches< -1 || (appliedMutations<0)!=(copiedBatches<0) || retainedResults<0 || retainedResults>256)throw new IllegalArgumentException("Invalid observed progress bounds");
        if((state==State.TERMINAL || state==State.ACKNOWLEDGED)!=(outcome!=null))throw new IllegalArgumentException("Terminal state requires an explicit outcome");
        if(outcome!=null && (appliedMutations<0 || resultDigest==null || resultDigest.length!=32))throw new IllegalArgumentException("Terminal outcome requires known progress and its digest");
        if(resultDigest!=null && resultDigest.length!=32)throw new IllegalArgumentException("Invalid result digest length");
        if(state==State.EXPIRED && (appliedMutations>=0 || copiedBatches>=0 || resultDigest!=null))throw new IllegalArgumentException("Expired metadata has unknown effects");
        resultDigest=resultDigest==null?null:resultDigest.clone(); }
    /** Whether confirmed count/prefix fields are known. PENDING still requires resolving possible further effects. */
    public boolean progressKnown(){return appliedMutations>=0 && copiedBatches>=0;}
    @Override public byte[] resultDigest() { return resultDigest==null?null:resultDigest.clone(); }
}
