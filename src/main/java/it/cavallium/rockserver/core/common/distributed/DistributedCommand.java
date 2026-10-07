package it.cavallium.rockserver.core.common.distributed;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable v1 atomic mutation envelope; it does not itself provide replication. */
public final class DistributedCommand {
    private final UUID groupId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final OperationId operationId;
    private final List<Mutation> mutations;

    public DistributedCommand(UUID groupId, long rangeEpoch, byte[] schemaDigest,
                              OperationId operationId, List<Mutation> mutations) {
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        if (rangeEpoch < 0) throw new IllegalArgumentException("Range epoch must be nonnegative");
        this.rangeEpoch = rangeEpoch;
        Objects.requireNonNull(schemaDigest, "schemaDigest");
        if (schemaDigest.length != 32) throw new IllegalArgumentException("Schema digest must be SHA-256");
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(mutations, "mutations");
        if (mutations.isEmpty() || mutations.size() > DistributedCommandCodec.MAX_MUTATIONS) {
            throw new IllegalArgumentException("Command requires 1..256 mutations");
        }
        this.mutations = List.copyOf(mutations);
        long size = DistributedCommandCodec.HEADER_BYTES;
        for (Mutation mutation : this.mutations) size += mutation.encodedSize();
        if (size > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Command exceeds encoded byte limit");
        }
        this.schemaDigest = schemaDigest.clone();
    }

    public int formatVersion() { return DistributedCommandCodec.FORMAT_VERSION; }
    public UUID groupId() { return groupId; }
    public long rangeEpoch() { return rangeEpoch; }
    /** Digest of the immutable schema and operator contract selected by admission. */
    public byte[] schemaDigest() { return schemaDigest.clone(); }
    public OperationId operationId() { return operationId; }
    public List<Mutation> mutations() { return mutations; }
    /** SHA-256 of the entire canonical encoding, including identity and ordered operands. */
    public byte[] payloadDigest() { return DistributedCommandCodec.digest(this); }
}
