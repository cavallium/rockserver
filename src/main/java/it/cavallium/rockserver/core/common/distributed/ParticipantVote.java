package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;

/** A definitive prepare outcome. Only a prepared vote carries an applied prepare position. */
public record ParticipantVote(boolean prepared,ApplyPosition position) {
    public ParticipantVote {
        if (prepared) { Objects.requireNonNull(position); if (position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid prepared position"); }
        else if (position!=null) throw new IllegalArgumentException("Negative prepare vote has no prepared position");
    }
    public static ParticipantVote prepared(ApplyPosition position) { return new ParticipantVote(true,position); }
    public static ParticipantVote negative() { return new ParticipantVote(false,null); }
}
