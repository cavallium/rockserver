package it.cavallium.rockserver.core.common.distributed;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

/** Immutable atomic envelope in the single current distributed command format. */
public final class DistributedCommand {
    public enum Kind { DATA, SESSION_FLOOR, SESSION_CONTROL, COMMIT_ACCEPTED, COMMIT_REJECTED }
    private final Kind kind;
    private final UUID groupId;
    private final long rangeEpoch;
    private final byte[] schemaDigest;
    private final OperationId operationId;
    private final List<Mutation> mutations;
    private final List<KeyPredicate> predicates;
    private final long acknowledgedThrough,minimumSessionEpoch;
    private final RangeSessionControl sessionControl;
    private final TransactionAcceptance acceptance;
    private final boolean hasBucketData;
    private final byte[] rejectionDigest;

    public DistributedCommand(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,List<Mutation> mutations) {
        this(groupId,rangeEpoch,schemaDigest,operationId,mutations,List.of());
    }
    public DistributedCommand(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,List<Mutation> mutations,List<KeyPredicate> predicates) {
        this(groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates,-1);
    }
    public DistributedCommand(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,List<Mutation> mutations,List<KeyPredicate> predicates,long acknowledgedThrough) {
        this(Kind.DATA,groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates,acknowledgedThrough,-1,null,null,null);
    }
    public static DistributedCommand advanceSessionEpochFloor(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,long minimumEpoch) {
        return new DistributedCommand(Kind.SESSION_FLOOR,groupId,rangeEpoch,schemaDigest,operationId,List.of(),List.of(),-1,minimumEpoch,null,null,null);
    }
    public static DistributedCommand sessionControl(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,RangeSessionControl control,long acknowledgedThrough) {
        return new DistributedCommand(Kind.SESSION_CONTROL,groupId,rangeEpoch,schemaDigest,operationId,List.of(),List.of(),acknowledgedThrough,
                Objects.requireNonNull(control).authority().minimumEpoch(),control,null,null);
    }
    public static DistributedCommand bucketData(UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,List<Mutation> mutations,List<KeyPredicate> predicates,long acknowledgedThrough) {
        if (mutations.stream().noneMatch(Mutation::bucket) && predicates.stream().noneMatch(KeyPredicate::bucket)) throw new IllegalArgumentException("Bucket data requires a bucket item");
        return new DistributedCommand(groupId,rangeEpoch,schemaDigest,operationId,mutations,predicates,acknowledgedThrough);
    }
    /** Origin audit does not constrain replay term; the ordered business plan is retained unchanged. */
    public static DistributedCommand acceptedTransaction(DistributedCommand base,TransactionAcceptance acceptance) {
        Objects.requireNonNull(base); Objects.requireNonNull(acceptance);
        if (base.kind!=Kind.DATA || base.mutations.isEmpty()) throw new IllegalArgumentException("Only business DATA can become an accepted transaction");
        return new DistributedCommand(Kind.COMMIT_ACCEPTED,base.groupId,base.rangeEpoch,base.schemaDigest,base.operationId,base.mutations,base.predicates,
                base.acknowledgedThrough,-1,null,acceptance,null);
    }
    /** Internal no-effects record retaining the accepted command's receipt identity, without its operands. */
    public static DistributedCommand rejectAcceptedTransaction(DistributedCommand original) {
        Objects.requireNonNull(original);
        if (original.kind!=Kind.COMMIT_ACCEPTED) throw new IllegalArgumentException("Only an accepted transaction can be rejected internally");
        return new DistributedCommand(Kind.COMMIT_REJECTED,original.groupId,original.rangeEpoch,original.schemaDigest,original.operationId,List.of(),List.of(),
                original.acknowledgedThrough,-1,null,null,original.payloadDigest());
    }
    DistributedCommand(Kind kind,UUID groupId,long rangeEpoch,byte[] schemaDigest,OperationId operationId,List<Mutation> mutations,List<KeyPredicate> predicates,
            long acknowledgedThrough,long minimumSessionEpoch,RangeSessionControl sessionControl,TransactionAcceptance acceptance,byte[] rejectionDigest) {
        this.kind=Objects.requireNonNull(kind); this.groupId=Objects.requireNonNull(groupId); this.operationId=Objects.requireNonNull(operationId);
        Objects.requireNonNull(schemaDigest); Objects.requireNonNull(mutations); Objects.requireNonNull(predicates);
        if (rangeEpoch<0 || schemaDigest.length!=32 || acknowledgedThrough< -1 || acknowledgedThrough>=operationId.sequence()
                || (long)mutations.size()+predicates.size()>DistributedCommandCodec.MAX_ITEMS) throw new IllegalArgumentException("Invalid command metadata/count/ACK");
        boolean data=kind==Kind.DATA || kind==Kind.COMMIT_ACCEPTED;
        if (!data && (!mutations.isEmpty() || !predicates.isEmpty())
                || kind==Kind.DATA && mutations.isEmpty() && (!predicates.isEmpty() || acknowledgedThrough<0)
                || kind==Kind.COMMIT_ACCEPTED && mutations.isEmpty()
                || kind==Kind.SESSION_FLOOR && acknowledgedThrough!=-1
                || (kind==Kind.SESSION_CONTROL)!=(sessionControl!=null)
                || (kind==Kind.COMMIT_ACCEPTED)!=(acceptance!=null)
                || (kind==Kind.COMMIT_REJECTED ? rejectionDigest==null || rejectionDigest.length!=32 : rejectionDigest!=null)) {
            throw new IllegalArgumentException("Command body differs from its semantic kind");
        }
        if (kind==Kind.SESSION_FLOOR || kind==Kind.SESSION_CONTROL) {
            if (minimumSessionEpoch<0 || operationId.epoch()<minimumSessionEpoch
                    || sessionControl!=null && minimumSessionEpoch!=sessionControl.authority().minimumEpoch()) throw new IllegalArgumentException("Invalid command epoch fence");
        } else if (minimumSessionEpoch!=-1) throw new IllegalArgumentException("Data/commit command cannot advance an epoch floor");
        this.mutations=List.copyOf(mutations); this.predicates=List.copyOf(predicates);
        hasBucketData=this.mutations.stream().anyMatch(Mutation::bucket) || this.predicates.stream().anyMatch(KeyPredicate::bucket);
        if (hasBucketData && this.mutations.stream().anyMatch(mutation -> mutation.kind()==Mutation.Kind.DELETE_RANGE)) throw new IllegalArgumentException("Bucket point commands do not support range deletion");
        this.rangeEpoch=rangeEpoch; this.schemaDigest=schemaDigest.clone(); this.acknowledgedThrough=acknowledgedThrough;
        this.minimumSessionEpoch=minimumSessionEpoch; this.sessionControl=sessionControl; this.acceptance=acceptance;
        this.rejectionDigest=rejectionDigest==null ? null : rejectionDigest.clone();
        if (acceptance!=null) acceptance.requireCovers(this.mutations,this.predicates);
        long size=DistributedCommandCodec.HEADER_BYTES;
        for (var mutation:this.mutations) size+=mutation.encodedSize();
        for (var predicate:this.predicates) size+=predicate.encodedSize();
        size+=switch(kind) {
            case DATA -> 0;
            case SESSION_FLOOR -> 8;
            case SESSION_CONTROL -> 4L+sessionControl.encodedSize();
            case COMMIT_ACCEPTED -> 4L+acceptance.encodedSize();
            case COMMIT_REJECTED -> 32;
        };
        if (size>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Command exceeds encoded byte limit");
    }
    public Kind kind() { return kind; }
    public UUID groupId() { return groupId; }
    public long rangeEpoch() { return rangeEpoch; }
    public byte[] schemaDigest() { return schemaDigest.clone(); }
    public OperationId operationId() { return operationId; }
    public List<Mutation> mutations() { return mutations; }
    public List<KeyPredicate> predicates() { return predicates; }
    public long acknowledgedThrough() { return acknowledgedThrough; }
    public long minimumSessionEpoch() { return minimumSessionEpoch; }
    public RangeSessionControl sessionControl() { return sessionControl; }
    public TransactionAcceptance acceptance() { return acceptance; }
    public boolean hasBucketData() { return hasBucketData; }
    public boolean transactionRejection() { return kind==Kind.COMMIT_REJECTED; }
    public byte[] rejectionDigest() { return rejectionDigest==null ? null : rejectionDigest.clone(); }
    public byte[] receiptDigest() { return transactionRejection() ? rejectionDigest.clone() : payloadDigest(); }
    public byte[] payloadDigest() { return DistributedCommandCodec.digest(this); }
}
