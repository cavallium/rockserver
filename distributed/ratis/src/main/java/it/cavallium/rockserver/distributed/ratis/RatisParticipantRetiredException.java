package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ParticipantFenceProof;
import java.io.IOException;
import java.util.Objects;

/** The original participant has expired; the witness does not reconstruct a result or receipt. */
public final class RatisParticipantRetiredException extends IOException {
    private final ParticipantFenceProof proof;
    public RatisParticipantRetiredException(ParticipantFenceProof proof) { super("Participant business epoch has retired"); this.proof=Objects.requireNonNull(proof); }
    public ParticipantFenceProof proof() { return proof; }
}
