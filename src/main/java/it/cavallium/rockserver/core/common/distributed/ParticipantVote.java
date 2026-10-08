package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;

/** PREPARED and REJECTED are real votes; RETIRED carries only fenced absence evidence. */
public record ParticipantVote(Kind kind,ApplyPosition position,ParticipantFenceProof retirementProof) {
    public enum Kind { PREPARED, REJECTED, RETIRED }
    public ParticipantVote {
        Objects.requireNonNull(kind);
        if (kind==Kind.PREPARED ? position==null || position.term()<=0 || position.index()<=0 || retirementProof!=null
                : position!=null || (kind==Kind.RETIRED)!=(retirementProof!=null)) throw new IllegalArgumentException("Invalid participant vote");
    }
    public boolean prepared() { return kind==Kind.PREPARED; }
    public boolean retired() { return kind==Kind.RETIRED; }
    public static ParticipantVote prepared(ApplyPosition position) { return new ParticipantVote(Kind.PREPARED,position,null); }
    public static ParticipantVote negative() { return new ParticipantVote(Kind.REJECTED,null,null); }
    public static ParticipantVote retired(ParticipantFenceProof proof) { return new ParticipantVote(Kind.RETIRED,null,Objects.requireNonNull(proof)); }
}
