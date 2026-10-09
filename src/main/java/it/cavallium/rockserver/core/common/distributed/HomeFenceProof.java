package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Atomic HOME evidence: this business epoch is fenced and its USER record is absent. */
public record HomeFenceProof(HomeIdentity identity,long minimumEpoch,ApplyPosition position) {
    public static final int MAX_BYTES=28+HomeIdentity.MAX_BYTES;
    public HomeFenceProof {
        Objects.requireNonNull(identity); Objects.requireNonNull(position);
        if (minimumEpoch<=identity.business().epoch() || position.term()<=0) throw new IllegalArgumentException("Invalid HOME fence proof");
    }
    public boolean covers(HomeFenceProof original) {
        return original!=null && identity.equals(original.identity) && minimumEpoch>=original.minimumEpoch && position.index()>=original.position.index()
                && position.term()>=original.position.term() && (position.index()!=original.position.index() || position.term()==original.position.term());
    }
    public byte[] encode() {
        byte[] id=identity.encode(); return ByteBuffer.allocate(28+id.length).putInt(id.length).put(id).putLong(minimumEpoch).putLong(position.term()).putLong(position.index()).array();
    }
    public static HomeFenceProof decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<28+76+TransactionStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME fence proof size");
        var b=ByteBuffer.wrap(bytes); int n=b.getInt(); if (n!=b.remaining()-24) throw new IllegalArgumentException("Invalid HOME fence identity size");
        byte[] id=new byte[n]; b.get(id); return new HomeFenceProof(HomeIdentity.decode(id),b.getLong(),new ApplyPosition(b.getLong(),b.getLong()));
    }
}
