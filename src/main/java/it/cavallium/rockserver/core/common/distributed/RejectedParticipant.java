package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Permanent native negative PREPARE fact. The trusted apply seam supplies rejection authority; no accepted plan is retained. */
public record RejectedParticipant(UUID participantGroup,UUID homeGroup,long homeEpoch,OperationId operation,
                                  TransactionStatus registration,byte[] preparationDigest,ApplyPosition position) {
    public static final int HEADER_BYTES=124, MAX_BYTES=HEADER_BYTES+TransactionStatus.MAX_BYTES;
    public RejectedParticipant {
        Objects.requireNonNull(participantGroup); Objects.requireNonNull(homeGroup); Objects.requireNonNull(operation);
        Objects.requireNonNull(registration); Objects.requireNonNull(preparationDigest); Objects.requireNonNull(position);
        if (homeEpoch<0 || registration.phase()!=TransactionStatus.Phase.PREPARING || operation.equals(registration.operation())
                || preparationDigest.length!=32 || position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid rejected participant receipt");
        var participant=registration.participants().stream().filter(p -> p.groupId().equals(participantGroup)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Rejected participant is unregistered"));
        if (participant.prepared()!=null) throw new IllegalArgumentException("Rejected participant already has a positive home vote");
        preparationDigest=preparationDigest.clone();
    }
    public static RejectedParticipant from(PreparedParticipant.Preparation preparation,ApplyPosition position) {
        Objects.requireNonNull(preparation);
        var result=new RejectedParticipant(preparation.business().groupId(),preparation.homeGroup(),preparation.homeEpoch(),preparation.operation(),preparation.registration(),preparation.digest(),position);
        result.requirePreparation(preparation); return result;
    }
    @Override public byte[] preparationDigest() { return preparationDigest.clone(); }
    public UUID transactionId() { return registration.transactionId(); }
    public boolean ownsOperation(OperationId id) { return operation.equals(id) || registration.operation().equals(id); }
    public void requirePreparation(PreparedParticipant.Preparation preparation) {
        Objects.requireNonNull(preparation);
        var cut=preparation.business().acceptance().snapshot();
        if (position.index()<=cut.index() || position.term()<cut.term()) throw new IllegalArgumentException("Rejected position does not follow its accepted cut");
        if (!participantGroup.equals(preparation.business().groupId()) || !homeGroup.equals(preparation.homeGroup()) || homeEpoch!=preparation.homeEpoch()
                || !operation.equals(preparation.operation()) || !registration.equals(preparation.registration()) || !Arrays.equals(preparationDigest,preparation.digest())) throw new IllegalArgumentException("Rejected preparation payload changed");
    }
    public void requireFinalization(ParticipantFinalization finalization) {
        Objects.requireNonNull(finalization);
        if (finalization.decision().phase()!=TransactionStatus.Phase.ABORT || finalization.participant().prepared()!=null
                || !participantGroup.equals(finalization.participantGroup()) || !homeGroup.equals(finalization.homeGroup()) || homeEpoch!=finalization.homeEpoch()
                || !registration.sameDefinition(finalization.decision()) || ownsOperation(finalization.operation())) throw new IllegalArgumentException("Finalization contradicts rejected preparation");
    }
    public byte[] encode() {
        byte[] status=registration.encode(); var b=ByteBuffer.allocate(HEADER_BYTES+status.length);
        uuid(b,participantGroup); uuid(b,homeGroup); b.putLong(homeEpoch); uuid(b,operation.sessionId());
        b.putLong(operation.epoch()).putLong(operation.sequence()).put(preparationDigest).putLong(position.term()).putLong(position.index()).putInt(status.length).put(status);
        return b.array();
    }
    public static RejectedParticipant decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<HEADER_BYTES+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid rejected participant length");
        var b=ByteBuffer.wrap(bytes); UUID participant=uuid(b),home=uuid(b); long epoch=b.getLong(); var operation=new OperationId(uuid(b),b.getLong(),b.getLong());
        byte[] digest=new byte[32]; b.get(digest); var position=new ApplyPosition(b.getLong(),b.getLong()); int length=b.getInt();
        if (length!=b.remaining()) throw new IllegalArgumentException("Invalid rejected registration length"); byte[] status=new byte[length]; b.get(status);
        var result=new RejectedParticipant(participant,home,epoch,operation,TransactionStatus.decode(status),digest,position);
        if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical rejected participant"); return result;
    }
    @Override public boolean equals(Object other) {
        return other instanceof RejectedParticipant rejected && participantGroup.equals(rejected.participantGroup) && homeGroup.equals(rejected.homeGroup)
                && homeEpoch==rejected.homeEpoch && operation.equals(rejected.operation) && registration.equals(rejected.registration)
                && Arrays.equals(preparationDigest,rejected.preparationDigest) && position.equals(rejected.position);
    }
    @Override public int hashCode() { return Objects.hash(participantGroup,homeGroup,homeEpoch,operation,registration,Arrays.hashCode(preparationDigest),position); }
    private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
}
