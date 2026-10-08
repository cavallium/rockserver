package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Qualified consumed HOME status and exact native finalization to reclaim together. */
public record ParticipantRetirement(ParticipantFinalization finalization,ApplyPosition position,TransactionStatus consumed) {
    public static final int MAX_BYTES=24+ParticipantFinalization.MAX_BYTES+TransactionStatus.MAX_BYTES;
    public ParticipantRetirement {
        Objects.requireNonNull(finalization); Objects.requireNonNull(position); Objects.requireNonNull(consumed);
        if (!consumed.gcEligible() || !consumed.terminalDecision().equals(finalization.decision()) || position.term()<=0 || position.index()<=0)
            throw new IllegalArgumentException("Retirement requires matching consumed decision and finalization");
        var vote=finalization.participant().prepared();
        if (vote!=null && (vote.index()>=position.index() || vote.term()>position.term())) throw new IllegalArgumentException("Retirement finalization precedes preparation");
    }
    public ParticipantIdentity identity() { return new ParticipantIdentity(finalization.homeGroup(),finalization.homeEpoch(),finalization.participantGroup(),consumed); }
    public byte[] encode() {
        byte[] f=finalization.encode(),c=consumed.encode(); return ByteBuffer.allocate(24+f.length+c.length).putInt(f.length).put(f).putLong(position.term()).putLong(position.index()).putInt(c.length).put(c).array();
    }
    public static ParticipantRetirement decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<24 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid participant retirement size");
        var b=ByteBuffer.wrap(bytes); int n=b.getInt(); if (n<0 || n>ParticipantFinalization.MAX_BYTES || n>b.remaining()-20) throw new IllegalArgumentException("Invalid retirement finalization size");
        byte[] f=new byte[n]; b.get(f); var cut=new ApplyPosition(b.getLong(),b.getLong()); n=b.getInt(); if (n<0 || n!=b.remaining()) throw new IllegalArgumentException("Invalid consumed status size");
        byte[] c=new byte[n]; b.get(c); return new ParticipantRetirement(ParticipantFinalization.decode(f),cut,TransactionStatus.decode(c));
    }
}
