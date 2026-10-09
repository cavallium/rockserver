package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.SnapshotHomeFenceProof;
import java.io.IOException;
import java.util.Objects;

/** Original snapshot HOME expiry; no terminal decision or historical capture is reconstructed. */
public final class RatisSnapshotHomeRetiredException extends IOException {
    private final SnapshotHomeFenceProof proof;
    public RatisSnapshotHomeRetiredException(SnapshotHomeFenceProof proof) { super("Snapshot HOME business epoch has retired"); this.proof=Objects.requireNonNull(proof); }
    public SnapshotHomeFenceProof proof() { return proof; }
}
