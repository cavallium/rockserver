package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Durable ABORT fence. A negative PREPARE fact is retained separately in RejectedParticipant. */
public record AbortedParticipant(ParticipantFinalization finalization,ApplyPosition position) {
    public static final int MAX_BYTES=16+ParticipantFinalization.MAX_BYTES;
    public AbortedParticipant {
        Objects.requireNonNull(finalization); Objects.requireNonNull(position);
        if (finalization.decision().phase()!=TransactionStatus.Phase.ABORT || position.term()<=0 || position.index()<=0) throw new IllegalArgumentException("Invalid participant ABORT fence");
    }
    public boolean ownsOperation(OperationId id) { return finalization.operation().equals(id) || finalization.decision().operation().equals(id); }
    public byte[] encode() { byte[] decision=finalization.encode(); return ByteBuffer.allocate(16+decision.length).putLong(position.term()).putLong(position.index()).put(decision).array(); }
    public static AbortedParticipant decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<16 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid ABORT fence length");
        var b=ByteBuffer.wrap(bytes); var position=new ApplyPosition(b.getLong(),b.getLong()); byte[] decision=new byte[b.remaining()]; b.get(decision);
        var result=new AbortedParticipant(ParticipantFinalization.decode(decision),position);
        if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical ABORT fence"); return result;
    }
}
