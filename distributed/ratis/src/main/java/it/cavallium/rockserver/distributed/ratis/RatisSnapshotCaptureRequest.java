package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.time.Duration;
import java.util.*;

/** Frozen owner-directed volatile capture request. Retain it before the first RPC. */
public record RatisSnapshotCaptureRequest(SnapshotFence fence,UUID owner,UUID snapshotId,Duration ttl) {
    public static final int HEADER_BYTES=44, MAX_BYTES=HEADER_BYTES+SnapshotFence.MAX_BYTES;
    public RatisSnapshotCaptureRequest {
        Objects.requireNonNull(fence); Objects.requireNonNull(owner); Objects.requireNonNull(snapshotId); Objects.requireNonNull(ttl);
        if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(Duration.ofSeconds(60))>0) throw new IllegalArgumentException("Snapshot capture TTL exceeds bound");
        long millis=ttl.toMillis(); if (millis<1 || millis>60000 || !ttl.equals(Duration.ofMillis(millis))) throw new IllegalArgumentException("Snapshot capture TTL must be 1..60000 whole milliseconds");
    }
    public ScopedTransactionRpc.SnapshotBinding binding() {
        try { return new ScopedTransactionRpc.SnapshotBinding(fence.fenceId(),MessageDigest.getInstance("SHA-256").digest(fence.encode())); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public byte[] encode() {
        byte[] definition=fence.encode(); var output=ByteBuffer.allocate(HEADER_BYTES+definition.length);
        ScopedTransactionRpc.uuid(output,owner); ScopedTransactionRpc.uuid(output,snapshotId); return output.putLong(ttl.toMillis()).putInt(definition.length).put(definition).array();
    }
    public static RatisSnapshotCaptureRequest decode(byte[] bytes) {
        if (bytes.length<HEADER_BYTES+SnapshotFence.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid capture request length");
        var input=ByteBuffer.wrap(bytes); UUID owner=ScopedTransactionRpc.uuid(input),snapshot=ScopedTransactionRpc.uuid(input); long ttl=input.getLong(); int size=input.getInt();
        if (size!=input.remaining()) throw new IllegalArgumentException("Invalid capture fence length"); byte[] definition=new byte[size]; input.get(definition);
        return new RatisSnapshotCaptureRequest(SnapshotFence.decode(definition),owner,snapshot,Duration.ofMillis(ttl));
    }
    public byte[] reply(SnapshotFenceStatus.Capture token) {
        if (token!=null && (!token.ownerIncarnation().equals(owner) || !token.snapshotId().equals(snapshotId))) throw new IllegalArgumentException("Capture result identity changed");
        var output=ByteBuffer.allocate(token==null ? 81 : 97); ScopedTransactionRpc.snapshotBinding(output,binding()); ScopedTransactionRpc.uuid(output,owner); ScopedTransactionRpc.uuid(output,snapshotId);
        output.put((byte)(token==null ? 1 : 0)); if (token!=null) output.putLong(token.position().term()).putLong(token.position().index()); return output.array();
    }
    public SnapshotFenceStatus.Capture result(byte[] bytes) throws RatisSnapshotLostException {
        if (bytes.length!=81 && bytes.length!=97) throw new IllegalArgumentException("Invalid capture reply length");
        var input=ByteBuffer.wrap(bytes); var binding=ScopedTransactionRpc.snapshotBinding(input); UUID actualOwner=ScopedTransactionRpc.uuid(input),actualSnapshot=ScopedTransactionRpc.uuid(input); int lost=input.get() & 255;
        if (!binding.equals(this.binding()) || !actualOwner.equals(owner) || !actualSnapshot.equals(snapshotId) || lost>1 || (lost==1 ? input.hasRemaining() : input.remaining()!=16)) throw new IllegalArgumentException("Capture reply binding changed");
        if (lost==1) throw new RatisSnapshotLostException("Capture unavailable on its frozen owner");
        return new SnapshotFenceStatus.Capture(owner,snapshotId,new ApplyPosition(input.getLong(),input.getLong()));
    }
}
