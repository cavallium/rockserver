package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Frozen FINALIZE intent. Its home decision must come from a linearizable read or trusted committed cache.
 * Encoding and native matching validate binding, not the provenance of that read. */
public final class ParticipantFinalization {
    public static final int HEADER_BYTES=76, MAX_BYTES=HEADER_BYTES+TransactionStatus.MAX_BYTES;
    private final UUID homeGroup,participantGroup;
    private final long homeEpoch;
    private final OperationId operation;
    private final TransactionStatus decision;
    public ParticipantFinalization(UUID homeGroup,long homeEpoch,UUID participantGroup,OperationId operation,TransactionStatus decision) {
        this.homeGroup=Objects.requireNonNull(homeGroup); this.homeEpoch=homeEpoch; this.participantGroup=Objects.requireNonNull(participantGroup);
        this.operation=Objects.requireNonNull(operation); this.decision=Objects.requireNonNull(decision).terminalDecision();
        if (homeEpoch<0 || operation.equals(decision.operation())) throw new IllegalArgumentException("Finalization needs a separate recovery identity");
        participant();
    }
    public UUID homeGroup() { return homeGroup; }
    public long homeEpoch() { return homeEpoch; }
    public UUID participantGroup() { return participantGroup; }
    public OperationId operation() { return operation; }
    public TransactionStatus decision() { return decision; }
    public DomainBinding domainBinding(){return decision.participantDomain(participantGroup);}
    public UUID transactionId() { return decision.transactionId(); }
    public TransactionStatus.Participant participant() { return decision.participants().stream().filter(p -> p.groupId().equals(participantGroup)).findFirst().orElseThrow(() -> new IllegalArgumentException("Unregistered finalization participant")); }
    /** An ABORT without a recorded vote may fence a prepare which was applied but not reported to the home. */
    public void requirePrepared(PreparedParticipant prepared) {
        Objects.requireNonNull(prepared); requirePreparation(prepared.preparation());
        var vote=participant().prepared();
        if (vote!=null && !vote.equals(prepared.position())) throw new IllegalArgumentException("Home vote differs from native prepared position");
    }
    public void requirePreparation(PreparedParticipant.Preparation preparation) {
        if (!homeGroup.equals(preparation.homeGroup()) || homeEpoch!=preparation.homeEpoch() || !participantGroup.equals(preparation.business().groupId())
                || !decision.sameDefinition(preparation.registration()) || preparation.ownsOperation(operation)) throw new IllegalArgumentException("Finalization differs from frozen preparation");
    }
    /** The stable decision identity excludes recovery operation and ACK/consumption progress. */
    public byte[] decisionDigest() {
        byte[] status=decision.encode(); var b=ByteBuffer.allocate(24+status.length); uuid(b,homeGroup); b.putLong(homeEpoch).put(status);
        try { return MessageDigest.getInstance("SHA-256").digest(b.array()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public byte[] encode() {
        byte[] status=decision.encode(); var b=ByteBuffer.allocate(HEADER_BYTES+status.length); uuid(b,homeGroup); b.putLong(homeEpoch); uuid(b,participantGroup);
        uuid(b,operation.sessionId()); b.putLong(operation.epoch()).putLong(operation.sequence()).putInt(status.length).put(status); return b.array();
    }
    public static ParticipantFinalization decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<HEADER_BYTES+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid finalization size");
        var b=ByteBuffer.wrap(bytes); UUID home=uuid(b); long epoch=b.getLong(); UUID group=uuid(b); var operation=new OperationId(uuid(b),b.getLong(),b.getLong());
        int size=b.getInt(); if (size!=b.remaining()) throw new IllegalArgumentException("Invalid finalization decision length"); byte[] status=new byte[size]; b.get(status);
        var result=new ParticipantFinalization(home,epoch,group,operation,TransactionStatus.decode(status));
        if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical finalization decision"); return result;
    }
    @Override public boolean equals(Object other) { return other instanceof ParticipantFinalization f && Arrays.equals(encode(),f.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
    private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
}
