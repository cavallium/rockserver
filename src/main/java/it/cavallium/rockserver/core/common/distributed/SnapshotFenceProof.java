package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Atomic DATA evidence that the original snapshot business epoch is fenced and its gate family is absent. */
public record SnapshotFenceProof(SnapshotFence fence,UUID participantGroup,long minimumEpoch,ApplyPosition position) {
    public static final int MAX_BYTES=44+SnapshotFence.MAX_BYTES;
    public SnapshotFenceProof {
        Objects.requireNonNull(fence); Objects.requireNonNull(participantGroup); Objects.requireNonNull(position); fence.participant(participantGroup);
        if (minimumEpoch<=fence.operation().epoch() || position.term()<=0) throw new IllegalArgumentException("Invalid snapshot retention proof");
    }
    public boolean covers(SnapshotFenceProof previous) {
        return previous!=null && fence.equals(previous.fence) && participantGroup.equals(previous.participantGroup) && minimumEpoch>=previous.minimumEpoch
                && position.index()>=previous.position.index() && position.term()>=previous.position.term() && (position.index()!=previous.position.index() || position.term()==previous.position.term());
    }
    public byte[] encode() { byte[] definition=fence.encode(); return ByteBuffer.allocate(44+definition.length).putInt(definition.length).put(definition).putLong(participantGroup.getMostSignificantBits()).putLong(participantGroup.getLeastSignificantBits()).putLong(minimumEpoch).putLong(position.term()).putLong(position.index()).array(); }
    public static SnapshotFenceProof decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<44+SnapshotFence.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot retention proof size");
        var b=ByteBuffer.wrap(bytes); int n=b.getInt(); if (n!=b.remaining()-40) throw new IllegalArgumentException("Invalid snapshot proof definition length");
        byte[] definition=new byte[n]; b.get(definition); return new SnapshotFenceProof(SnapshotFence.decode(definition),new UUID(b.getLong(),b.getLong()),b.getLong(),new ApplyPosition(b.getLong(),b.getLong()));
    }
}
