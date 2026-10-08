package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Original applied finalization or explicit retirement; a floor cut is never an applied receipt. */
public record ParticipantFinalizationResult(ApplyPosition position,ParticipantFenceProof retirement) {
    public static final int MAX_BYTES=1+ParticipantFenceProof.MAX_BYTES;
    public ParticipantFinalizationResult {
        if ((position==null)==(retirement==null) || position!=null && (position.term()<=0 || position.index()<=0)) throw new IllegalArgumentException("Invalid finalization result");
    }
    public static ParticipantFinalizationResult applied(ApplyPosition position) { return new ParticipantFinalizationResult(Objects.requireNonNull(position),null); }
    public static ParticipantFinalizationResult retired(ParticipantFenceProof proof) { return new ParticipantFinalizationResult(null,Objects.requireNonNull(proof)); }
    public boolean retired() { return retirement!=null; }
    public byte[] encode() {
        if (!retired()) return ByteBuffer.allocate(17).put((byte)0).putLong(position.term()).putLong(position.index()).array();
        byte[] proof=retirement.encode(); return ByteBuffer.allocate(1+proof.length).put((byte)1).put(proof).array();
    }
    public static ParticipantFinalizationResult decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<1 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid finalization result size");
        var b=ByteBuffer.wrap(bytes); int kind=b.get()&255;
        if (kind==0 && b.remaining()==16) return applied(new ApplyPosition(b.getLong(),b.getLong()));
        if (kind!=1) throw new IllegalArgumentException("Invalid finalization result kind/size");
        byte[] proof=new byte[b.remaining()]; b.get(proof); return retired(ParticipantFenceProof.decode(proof));
    }
}
