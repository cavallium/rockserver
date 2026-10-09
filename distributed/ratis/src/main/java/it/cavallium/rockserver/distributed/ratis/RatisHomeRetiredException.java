package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.HomeFenceProof;
import java.io.IOException;
import java.util.Objects;

/** The original HOME is fenced absent; this does not reconstruct a decision or recovery receipt. */
public final class RatisHomeRetiredException extends IOException {
    private final HomeFenceProof proof;
    public RatisHomeRetiredException(HomeFenceProof proof) { super("Transaction HOME business epoch has retired"); this.proof=Objects.requireNonNull(proof); }
    public HomeFenceProof proof() { return proof; }
}
