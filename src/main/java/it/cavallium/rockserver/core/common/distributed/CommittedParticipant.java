package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Durable COMMIT finalization receipt. Original result bytes remain in the retained prepared plan. */
public record CommittedParticipant(ParticipantFinalization finalization,ApplyPosition position) {
    public static final int MAX_BYTES=16+ParticipantFinalization.MAX_BYTES;
    public CommittedParticipant {
        Objects.requireNonNull(finalization); Objects.requireNonNull(position); var vote=finalization.participant().prepared();
        if (finalization.decision().phase()!=TransactionStatus.Phase.COMMIT || vote==null || position.term()<vote.term() || position.index()<=vote.index()) throw new IllegalArgumentException("COMMIT finalization must follow its prepared vote");
    }
    public boolean ownsOperation(OperationId id) { return finalization.operation().equals(id) || finalization.decision().operation().equals(id); }
    public byte[] result(PreparedParticipant prepared) { finalization.requirePrepared(prepared); return prepared.preparation().business().acceptance().result(); }
    public byte[] encode() { byte[] decision=finalization.encode(); return ByteBuffer.allocate(16+decision.length).putLong(position.term()).putLong(position.index()).put(decision).array(); }
    public static CommittedParticipant decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<16 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid COMMIT receipt length");
        var b=ByteBuffer.wrap(bytes); var position=new ApplyPosition(b.getLong(),b.getLong()); byte[] decision=new byte[b.remaining()]; b.get(decision); return new CommittedParticipant(ParticipantFinalization.decode(decision),position);
    }
}
