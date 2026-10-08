package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Retained protected receipt; user-grant expiration and ordinary receipt ACK/GC must not reclaim it. */
public record HomeRecoveryReceipt(HomeRecovery recovery,ApplyPosition position,CommandResult result) {
    public static final int MAX_BYTES=HomeRecovery.MAX_BYTES+28;
    public HomeRecoveryReceipt {
        Objects.requireNonNull(recovery); Objects.requireNonNull(position); Objects.requireNonNull(result);
        if (position.term()<=0 || position.index()<=0 || result.valueCount()!=0 || result.status()!=CommandResult.Status.SUCCESS && result.status()!=CommandResult.Status.CONFLICT) throw new IllegalArgumentException("Invalid HOME recovery receipt");
    }
    public void requireCut(ApplyPosition cut) {
        if (cut==null || position.term()>cut.term() || position.index()>cut.index() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("HOME recovery receipt exceeds completed cut");
    }
    @Override public boolean equals(Object other) { return other instanceof HomeRecoveryReceipt r && recovery.equals(r.recovery) && position.equals(r.position) && Arrays.equals(result.encode(),r.result.encode()); }
    @Override public int hashCode() { return Objects.hash(recovery,position,Arrays.hashCode(result.encode())); }
    public byte[] encode() {
        byte[] request=recovery.encode(),outcome=result.encode(); return ByteBuffer.allocate(20+request.length+outcome.length).putLong(position.term()).putLong(position.index()).putInt(request.length).put(request).put(outcome).array();
    }
    public static HomeRecoveryReceipt decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<28 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME recovery receipt length");
        var in=ByteBuffer.wrap(bytes); var position=new ApplyPosition(in.getLong(),in.getLong()); int size=in.getInt();
        if (size<0 || size>HomeRecovery.MAX_BYTES || size!=in.remaining()-8) throw new IllegalArgumentException("Invalid HOME recovery receipt request size");
        byte[] request=new byte[size],result=new byte[8]; in.get(request).get(result); return new HomeRecoveryReceipt(HomeRecovery.decode(request),position,CommandResult.decode(result));
    }
}
