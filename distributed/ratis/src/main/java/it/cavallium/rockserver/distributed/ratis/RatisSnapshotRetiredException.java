package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.SnapshotFenceProof;
import java.io.IOException;
import java.util.Objects;

/** The original snapshot gate has retired; this is not a READY, capture or RELEASE receipt. */
public final class RatisSnapshotRetiredException extends IOException {
    private final SnapshotFenceProof proof;
    public RatisSnapshotRetiredException(SnapshotFenceProof proof) { super("Snapshot gate business epoch has retired"); this.proof=Objects.requireNonNull(proof); }
    public SnapshotFenceProof proof() { return proof; }
}
