package it.cavallium.rockserver.core.common.distributed;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable atomic mutation envelope; equality/absence predicates are not snapshot transaction history. */
public final class DistributedCommand {
    private final int formatVersion;
    private final UUID groupId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final OperationId operationId;
    private final List<Mutation> mutations;
    private final List<KeyPredicate> predicates;
    private final long acknowledgedThrough;

    public DistributedCommand(UUID groupId, long rangeEpoch, byte[] schemaDigest,
                              OperationId operationId, List<Mutation> mutations) {
        this(groupId, rangeEpoch, schemaDigest, operationId, mutations, List.of());
    }

    public DistributedCommand(UUID groupId, long rangeEpoch, byte[] schemaDigest,
                              OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates) {
        this(2, groupId, rangeEpoch, schemaDigest, operationId, mutations, predicates);
    }

    /** A v3 acknowledgement covers this issuing session/epoch only, strictly before this operation. */
    public DistributedCommand(UUID groupId, long rangeEpoch, byte[] schemaDigest, OperationId operationId,
                              List<Mutation> mutations, List<KeyPredicate> predicates, long acknowledgedThrough) {
        this(3, groupId, rangeEpoch, schemaDigest, operationId, mutations, predicates, acknowledgedThrough);
    }

    // Decoding retains the original version, including legacy payload/receipt identity.
    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates) {
        this(formatVersion, groupId, rangeEpoch, schemaDigest, operationId, mutations, predicates, -1);
    }

    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates,
                       long acknowledgedThrough) {
        if (formatVersion < 1 || formatVersion > 3) throw new IllegalArgumentException("Unsupported command version");
        this.formatVersion = formatVersion;
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        if (rangeEpoch < 0) throw new IllegalArgumentException("Range epoch must be nonnegative");
        this.rangeEpoch = rangeEpoch;
        Objects.requireNonNull(schemaDigest, "schemaDigest");
        if (schemaDigest.length != 32) throw new IllegalArgumentException("Schema digest must be SHA-256");
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(mutations, "mutations");
        Objects.requireNonNull(predicates, "predicates");
        if ((formatVersion == 3 ? acknowledgedThrough < 0 || acknowledgedThrough >= operationId.sequence() : acknowledgedThrough != -1)
                || (mutations.isEmpty() && (formatVersion != 3 || !predicates.isEmpty()))
                || (long) mutations.size() + predicates.size() > DistributedCommandCodec.MAX_ITEMS
                || (formatVersion == 1 && !predicates.isEmpty())) {
            throw new IllegalArgumentException("Invalid command items or acknowledgement; v1 has no predicates");
        }
        this.mutations = List.copyOf(mutations);
        this.predicates = List.copyOf(predicates);
        this.acknowledgedThrough = acknowledgedThrough;
        long size = DistributedCommandCodec.headerBytes(formatVersion);
        for (Mutation mutation : this.mutations) size += mutation.encodedSize();
        for (KeyPredicate predicate : this.predicates) size += predicate.encodedSize();
        if (size > DistributedCommandCodec.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException("Command exceeds encoded byte limit");
        }
        this.schemaDigest = schemaDigest.clone();
    }

    public int formatVersion() { return formatVersion; }
    public UUID groupId() { return groupId; }
    public long rangeEpoch() { return rangeEpoch; }
    /** Digest of the immutable schema and operator contract selected by admission. */
    public byte[] schemaDigest() { return schemaDigest.clone(); }
    public OperationId operationId() { return operationId; }
    public List<Mutation> mutations() { return mutations; }
    public List<KeyPredicate> predicates() { return predicates; }
    /** -1 for legacy commands without an acknowledgement. */
    public long acknowledgedThrough() { return acknowledgedThrough; }
    /** SHA-256 of the entire canonical encoding, including identity and ordered operands. */
    public byte[] payloadDigest() { return DistributedCommandCodec.digest(this); }
}
