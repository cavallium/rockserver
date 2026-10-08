package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;
import java.util.UUID;

/** Immutable catalogue cut; expiry at a range changes only when this cut is replicated there. */
public record SessionAuthority(UUID clusterId, UUID databaseId, long generation, long logicalTimeMillis, long minimumEpoch) {
    public SessionAuthority {
        Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId);
        if (generation < 0 || logicalTimeMillis < 0 || minimumEpoch < 1) throw new IllegalArgumentException("Invalid session authority cut");
    }
    public static SessionAuthority from(SessionEpochLedger ledger) {
        return new SessionAuthority(ledger.clusterId(),ledger.databaseId(),ledger.generation(),ledger.logicalTimeMillis(),ledger.minimumEpoch());
    }
    /** Jumps are allowed because intermediate catalogue transitions need not involve this range. */
    public void requireAtLeast(SessionAuthority previous) {
        if (!clusterId.equals(previous.clusterId) || !databaseId.equals(previous.databaseId)
                || generation < previous.generation || logicalTimeMillis < previous.logicalTimeMillis || minimumEpoch < previous.minimumEpoch
                || generation == previous.generation && !equals(previous)) throw new IllegalArgumentException("Regressing or conflicting authority cut");
    }
    public boolean permits(SessionEpochLedger.Grant grant) {
        return grant.epoch() >= minimumEpoch && grant.issuedAtMillis() <= logicalTimeMillis && grant.expiresAtMillis() > logicalTimeMillis;
    }
}
