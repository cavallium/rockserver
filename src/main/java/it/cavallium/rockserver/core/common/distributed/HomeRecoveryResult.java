package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Actual protected recovery receipt or explicit fenced HOME absence, never a replacement receipt. */
public record HomeRecoveryResult(HomeRecoveryReceipt receipt,HomeFenceProof retirement) {
    public static final int MAX_BYTES=1+Math.max(HomeRecoveryReceipt.MAX_BYTES,HomeFenceProof.MAX_BYTES);
    public HomeRecoveryResult { if ((receipt==null)==(retirement==null)) throw new IllegalArgumentException("Invalid HOME recovery result"); }
    public static HomeRecoveryResult applied(HomeRecoveryReceipt receipt) { return new HomeRecoveryResult(Objects.requireNonNull(receipt),null); }
    public static HomeRecoveryResult retired(HomeFenceProof proof) { return new HomeRecoveryResult(null,Objects.requireNonNull(proof)); }
    public boolean retired() { return retirement!=null; }
    public byte[] encode() { byte[] body=retired() ? retirement.encode() : receipt.encode(); return ByteBuffer.allocate(1+body.length).put((byte)(retired() ? 1 : 0)).put(body).array(); }
    public static HomeRecoveryResult decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<1 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid HOME recovery result size");
        int kind=bytes[0]&255; byte[] body=Arrays.copyOfRange(bytes,1,bytes.length);
        if (kind==0) return applied(HomeRecoveryReceipt.decode(body)); if (kind==1) return retired(HomeFenceProof.decode(body)); throw new IllegalArgumentException("Invalid HOME recovery result kind");
    }
}
