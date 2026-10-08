package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Durable ABORT fence, with an optional exact negative PREPARE receipt. No staged plan is needed for a negative vote. */
public record AbortedParticipant(ParticipantFinalization finalization,ApplyPosition position,Negative negative) {
    public static final int MAX_BYTES=17+ParticipantFinalization.MAX_BYTES+80;
    public record Negative(OperationId operation,byte[] digest,ApplyPosition position) {
        public Negative { Objects.requireNonNull(operation); Objects.requireNonNull(position); if (digest.length!=32 || position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid negative preparation receipt"); digest=digest.clone(); }
        @Override public byte[] digest() { return digest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Negative n && operation.equals(n.operation) && position.equals(n.position) && Arrays.equals(digest,n.digest); }
        @Override public int hashCode() { return Objects.hash(operation,position,Arrays.hashCode(digest)); }
    }
    public AbortedParticipant {
        Objects.requireNonNull(finalization); Objects.requireNonNull(position);
        if (finalization.decision().phase()!=TransactionStatus.Phase.ABORT || position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid participant ABORT fence");
        if (negative!=null && (negative.position().index()<=position.index() || negative.position().term()<position.term()
                || negative.operation().equals(finalization.operation()) || negative.operation().equals(finalization.decision().operation()))) throw new IllegalArgumentException("Invalid fenced prepare receipt");
    }
    public boolean ownsOperation(OperationId id) { return finalization.operation().equals(id) || finalization.decision().operation().equals(id) || negative!=null && negative.operation().equals(id); }
    public AbortedParticipant withNegative(PreparedParticipant.Preparation preparation,ApplyPosition applied) {
        finalization.requirePreparation(preparation);
        if (negative!=null) { requireNegative(preparation); return this; }
        return new AbortedParticipant(finalization,position,new Negative(preparation.operation(),preparation.digest(),applied));
    }
    public void requireNegative(PreparedParticipant.Preparation preparation) {
        finalization.requirePreparation(preparation);
        if (negative==null || !negative.operation().equals(preparation.operation()) || !Arrays.equals(negative.digest(),preparation.digest())) throw new IllegalArgumentException("Negative prepare payload changed");
    }
    public byte[] encode() {
        byte[] decision=finalization.encode(); var b=ByteBuffer.allocate(17+decision.length+(negative==null ? 0 : 80)).putLong(position.term()).putLong(position.index()).put((byte)(negative==null ? 0 : 1));
        if (negative!=null) { b.putLong(negative.operation().sessionId().getMostSignificantBits()).putLong(negative.operation().sessionId().getLeastSignificantBits()).putLong(negative.operation().epoch()).putLong(negative.operation().sequence()).put(negative.digest()).putLong(negative.position().term()).putLong(negative.position().index()); }
        return b.put(decision).array();
    }
    public static AbortedParticipant decode(byte[] bytes) {
        if (bytes.length<17 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid ABORT fence length"); var b=ByteBuffer.wrap(bytes); var position=new ApplyPosition(b.getLong(),b.getLong()); int flag=b.get() & 255;
        if (flag>1 || flag==1 && b.remaining()<80) throw new IllegalArgumentException("Invalid negative receipt flag/length");
        Negative negative=null;
        if (flag==1) { var operation=new OperationId(new UUID(b.getLong(),b.getLong()),b.getLong(),b.getLong()); byte[] digest=new byte[32]; b.get(digest); negative=new Negative(operation,digest,new ApplyPosition(b.getLong(),b.getLong())); }
        byte[] decision=new byte[b.remaining()]; b.get(decision); return new AbortedParticipant(ParticipantFinalization.decode(decision),position,negative);
    }
}
