package it.cavallium.rockserver.core.common.distributed;

import java.util.*;

/** A retained native gate or explicit fenced absence; retirement is never an INSTALL/RELEASE receipt. */
public record SnapshotGateResult(SnapshotGate gate,SnapshotFenceProof retirement) {
    public SnapshotGateResult { if ((gate==null)==(retirement==null)) throw new IllegalArgumentException("Invalid snapshot gate result"); }
    public static SnapshotGateResult retained(SnapshotGate gate) { return new SnapshotGateResult(Objects.requireNonNull(gate),null); }
    public static SnapshotGateResult retired(SnapshotFenceProof proof) { return new SnapshotGateResult(null,Objects.requireNonNull(proof)); }
    public boolean retired() { return retirement!=null; }
}
