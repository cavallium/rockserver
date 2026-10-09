package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Qualified client consumption and exact local RELEASE position to reclaim. */
public record SnapshotGateRetirement(SnapshotFenceStatus consumed,UUID participantGroup,ApplyPosition released) {
    public static final int MAX_BYTES=36+SnapshotFenceStatus.MAX_BYTES;
    public SnapshotGateRetirement {
        Objects.requireNonNull(consumed); Objects.requireNonNull(participantGroup); Objects.requireNonNull(released);
        if (!consumed.terminal() || !consumed.consumed() || released.term()<=0 || released.index()<=0) throw new IllegalArgumentException("Snapshot retirement requires consumed terminal status and real RELEASE");
        var progress=consumed.progress(participantGroup); var before=progress.capture()==null ? progress.ready() : progress.capture().position();
        if (before!=null && (released.index()<=before.index() || released.term()<before.term())) throw new IllegalArgumentException("Snapshot RELEASE must follow its READY/capture proof");
    }
    public SnapshotFence fence() { return consumed.fence(); }
    public byte[] encode() { byte[] status=consumed.encode(); return ByteBuffer.allocate(36+status.length).putInt(status.length).put(status).putLong(participantGroup.getMostSignificantBits()).putLong(participantGroup.getLeastSignificantBits()).putLong(released.term()).putLong(released.index()).array(); }
    public static SnapshotGateRetirement decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<36+SnapshotFenceStatus.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot retirement size");
        var b=ByteBuffer.wrap(bytes); int n=b.getInt(); if (n!=b.remaining()-32) throw new IllegalArgumentException("Invalid snapshot retirement status size"); byte[] status=new byte[n]; b.get(status);
        return new SnapshotGateRetirement(SnapshotFenceStatus.decode(status),new UUID(b.getLong(),b.getLong()),new ApplyPosition(b.getLong(),b.getLong()));
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
