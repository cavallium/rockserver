package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Atomic expired snapshot HOME absence, not reconstructed consumption or a terminal decision. */
public record SnapshotHomeFenceProof(SnapshotHomeIdentity identity,long minimumEpoch,ApplyPosition position) {
    public static final int MAX_BYTES=28+SnapshotHomeIdentity.MAX_BYTES;
    public SnapshotHomeFenceProof {
        Objects.requireNonNull(identity); Objects.requireNonNull(position);
        if (minimumEpoch<=identity.business().epoch() || position.term()<=0) throw new IllegalArgumentException("Invalid snapshot HOME fence proof");
    }
    public boolean covers(SnapshotHomeFenceProof previous) {
        return previous!=null && identity.equals(previous.identity) && minimumEpoch>=previous.minimumEpoch && position.index()>=previous.position.index() && position.term()>=previous.position.term()
                && (position.index()!=previous.position.index() || position.term()==previous.position.term());
    }
    public byte[] encode() { byte[] id=identity.encode(); return ByteBuffer.allocate(28+id.length).putInt(id.length).put(id).putLong(minimumEpoch).putLong(position.term()).putLong(position.index()).array(); }
    public static SnapshotHomeFenceProof decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<28+76+SnapshotFence.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot HOME proof size");
        var b=ByteBuffer.wrap(bytes); int n=b.getInt(); if (n!=b.remaining()-24) throw new IllegalArgumentException("Invalid snapshot HOME identity length"); byte[] id=new byte[n]; b.get(id);
        return new SnapshotHomeFenceProof(SnapshotHomeIdentity.decode(id),b.getLong(),new ApplyPosition(b.getLong(),b.getLong()));
    }
}
