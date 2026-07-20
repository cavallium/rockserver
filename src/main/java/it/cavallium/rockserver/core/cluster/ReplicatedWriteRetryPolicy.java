package it.cavallium.rockserver.core.cluster;

public enum ReplicatedWriteRetryPolicy {
    UNSUPPORTED(false, false),
    ACK_ONLY_DEDUPE(true, false),
    RESPONSE_DEDUPE(true, true);

    private final boolean requiresDedupeRecord;
    private final boolean requiresResponseRecord;

    ReplicatedWriteRetryPolicy(boolean requiresDedupeRecord, boolean requiresResponseRecord) {
        this.requiresDedupeRecord = requiresDedupeRecord;
        this.requiresResponseRecord = requiresResponseRecord;
    }

    public boolean requiresDedupeRecord() {
        return requiresDedupeRecord;
    }

    public boolean requiresResponseRecord() {
        return requiresResponseRecord;
    }
}
