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
    private final long minimumSessionEpoch;
    private final RangeSessionControl sessionControl;

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

    /** Committed administrative fence; admission must obtain catalogue lifetime authority. */
    public static DistributedCommand advanceSessionEpochFloor(UUID groupId, long rangeEpoch, byte[] schemaDigest,
                                                               OperationId operationId, long minimumEpoch) {
        return new DistributedCommand(4, groupId, rangeEpoch, schemaDigest, operationId, List.of(), List.of(), -1, minimumEpoch);
    }

    /** Control issuer is a separate maintenance identity; it need not be a live user grant. */
    public static DistributedCommand sessionControl(UUID groupId, long rangeEpoch, byte[] schemaDigest,
                                                     OperationId operationId, RangeSessionControl control, long acknowledgedThrough) {
        return new DistributedCommand(5,groupId,rangeEpoch,schemaDigest,operationId,List.of(),List.of(),acknowledgedThrough,
                Objects.requireNonNull(control).authority().minimumEpoch(),control);
    }

    /** v6 raw/bucket data command; an optional ACK belongs to the same issuing session/epoch. */
    public static DistributedCommand bucketData(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,
            List<Mutation> mutations,List<KeyPredicate> predicates,long acknowledgedThrough) {
        return new DistributedCommand(6,groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates,acknowledgedThrough);
    }

    // Decoding retains the original version, including legacy payload/receipt identity.
    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates) {
        this(formatVersion, groupId, rangeEpoch, schemaDigest, operationId, mutations, predicates, -1);
    }

    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates,
                       long acknowledgedThrough) {
        this(formatVersion, groupId, rangeEpoch, schemaDigest, operationId, mutations, predicates, acknowledgedThrough, -1);
    }

    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates,
                       long acknowledgedThrough, long minimumSessionEpoch) {
        this(formatVersion,groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates,acknowledgedThrough,minimumSessionEpoch,null);
    }

    DistributedCommand(int formatVersion, UUID groupId, long rangeEpoch, byte[] schemaDigest,
                       OperationId operationId, List<Mutation> mutations, List<KeyPredicate> predicates,
                       long acknowledgedThrough, long minimumSessionEpoch, RangeSessionControl sessionControl) {
        if (formatVersion < 1 || formatVersion > 6) throw new IllegalArgumentException("Unsupported command version");
        this.formatVersion = formatVersion;
        this.groupId = Objects.requireNonNull(groupId, "groupId");
        if (rangeEpoch < 0) throw new IllegalArgumentException("Range epoch must be nonnegative");
        this.rangeEpoch = rangeEpoch;
        Objects.requireNonNull(schemaDigest, "schemaDigest");
        if (schemaDigest.length != 32) throw new IllegalArgumentException("Schema digest must be SHA-256");
        this.operationId = Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(mutations, "mutations");
        Objects.requireNonNull(predicates, "predicates");
        if ((formatVersion == 3 ? acknowledgedThrough < 0 || acknowledgedThrough >= operationId.sequence()
                        : formatVersion == 5 || formatVersion == 6 ? acknowledgedThrough < -1 || acknowledgedThrough >= operationId.sequence() : acknowledgedThrough != -1)
                || (formatVersion == 4 || formatVersion == 5 ? minimumSessionEpoch < 0 || operationId.epoch() < minimumSessionEpoch
                        || !mutations.isEmpty() || !predicates.isEmpty() : minimumSessionEpoch != -1)
                || (mutations.isEmpty() && ((formatVersion != 3 && formatVersion != 4 && formatVersion != 5) || !predicates.isEmpty()))
                || (long) mutations.size() + predicates.size() > DistributedCommandCodec.MAX_ITEMS
                || (formatVersion == 1 && !predicates.isEmpty())
                || (formatVersion == 6 ? mutations.stream().noneMatch(Mutation::bucket) && predicates.stream().noneMatch(KeyPredicate::bucket)
                        : mutations.stream().anyMatch(Mutation::bucket) || predicates.stream().anyMatch(KeyPredicate::bucket))
                || (formatVersion == 5 ? sessionControl==null || minimumSessionEpoch!=sessionControl.authority().minimumEpoch() : sessionControl!=null)) {
            throw new IllegalArgumentException("Invalid command items or acknowledgement; v1 has no predicates");
        }
        if (formatVersion==6 && mutations.stream().anyMatch(mutation -> mutation.kind()==Mutation.Kind.DELETE_RANGE)) throw new IllegalArgumentException("V6 bucket point commands do not support range deletion");
        this.mutations = List.copyOf(mutations);
        this.predicates = List.copyOf(predicates);
        this.acknowledgedThrough = acknowledgedThrough;
        this.minimumSessionEpoch = minimumSessionEpoch;
        this.sessionControl = sessionControl;
        long size = DistributedCommandCodec.headerBytes(formatVersion) + (sessionControl==null ? 0 : sessionControl.encodedSize());
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
    public RangeSessionControl sessionControl() { return sessionControl; }
    /** -1 unless this is an administrative epoch-floor/session-authority command. */
    public long minimumSessionEpoch() { return minimumSessionEpoch; }
    /** SHA-256 of the entire canonical encoding, including identity and ordered operands. */
    public byte[] payloadDigest() { return DistributedCommandCodec.digest(this); }
}
