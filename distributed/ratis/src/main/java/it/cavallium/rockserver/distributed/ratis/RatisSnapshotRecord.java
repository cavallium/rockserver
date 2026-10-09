package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current committed snapshot gate command. Fresh home authority is supplied by leader admission. */
public record RatisSnapshotRecord(SnapshotFence fence,UUID participantGroup,SnapshotFenceStatus terminal) {
    private static final int MAGIC=0x52534754, INSTALL=1, RELEASE=2, HEADER_BYTES=24;
    public RatisSnapshotRecord {
        Objects.requireNonNull(fence); Objects.requireNonNull(participantGroup); fence.participant(participantGroup);
        if (terminal!=null) terminal=terminal.terminalDecision();
        if (terminal!=null && (!terminal.terminal() || !terminal.fence().equals(fence))) throw new IllegalArgumentException("Gate release lacks a bound terminal decision");
    }
    public static RatisSnapshotRecord install(SnapshotFence fence,UUID group) { return new RatisSnapshotRecord(fence,group,null); }
    public static RatisSnapshotRecord release(SnapshotFenceStatus terminal,UUID group) { return new RatisSnapshotRecord(terminal.fence(),group,terminal); }
    public boolean isRelease() { return terminal!=null; }
    public OperationId operation() { var participant=fence.participant(participantGroup); return isRelease() ? participant.releaseOperation() : participant.installOperation(); }
    public byte[] encode() {
        byte[] body=isRelease() ? terminal.encode() : fence.encode();
        if (body.length>DistributedCommandCodec.MAX_ENCODED_BYTES-HEADER_BYTES) throw new IllegalArgumentException("Snapshot gate record exceeds command budget");
        return ByteBuffer.allocate(HEADER_BYTES+body.length).putInt(MAGIC).putInt(isRelease() ? RELEASE : INSTALL).putLong(participantGroup.getMostSignificantBits()).putLong(participantGroup.getLeastSignificantBits()).put(body).array();
    }
    public static boolean isSnapshot(byte[] bytes) { return bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisSnapshotRecord decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<HEADER_BYTES || bytes.length>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Invalid snapshot gate record size");
        var b=ByteBuffer.wrap(bytes); int magic=b.getInt(),kind=b.getInt();
        if (magic!=MAGIC || kind!=INSTALL && kind!=RELEASE) throw new IllegalArgumentException("Unknown snapshot gate record kind");
        var group=new UUID(b.getLong(),b.getLong()); byte[] body=new byte[b.remaining()]; b.get(body);
        return kind==INSTALL ? install(SnapshotFence.decode(body),group) : release(SnapshotFenceStatus.decode(body),group);
    }
    public byte[] digest() {
        try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    /** Null means this command has no durable outcome yet; suppressed INSTALL is a durable no-effects result. */
    public Result receipt(SnapshotGate gate) {
        if (gate==null) return null;
        if (!gate.participantGroup().equals(participantGroup) || !gate.fence().equals(fence)) throw new IllegalArgumentException("Snapshot gate receipt binding changed");
        if (!isRelease()) return new Result(gate.installed());
        if (gate.released()==null) return null;
        if (!Arrays.equals(gate.decisionDigest(),terminal.digest())) throw new IllegalArgumentException("Snapshot gate release decision changed");
        return new Result(gate.released());
    }
    public Result receipt(SnapshotGateResult result) {
        if (result==null) return null;
        if (!result.retired()) return receipt(result.gate());
        var proof=result.retirement();
        if (!proof.fence().equals(fence) || !proof.participantGroup().equals(participantGroup)) throw new IllegalArgumentException("Snapshot retirement binding changed");
        return Result.retired(proof);
    }
    public record Result(ApplyPosition position,SnapshotFenceProof retirement) {
        public static final int MAX_BYTES=1+SnapshotFenceProof.MAX_BYTES;
        public Result(ApplyPosition position) { this(position,null); }
        public Result {
            if (position!=null && (retirement!=null || position.term()<=0 || position.index()<=0)) throw new IllegalArgumentException("Invalid snapshot gate receipt position");
        }
        public static Result retired(SnapshotFenceProof proof) { return new Result(null,Objects.requireNonNull(proof)); }
        public boolean retired() { return retirement!=null; }
        public boolean suppressed() { return position==null && retirement==null; }
        public byte[] encode() {
            if (retired()) { byte[] proof=retirement.encode(); return ByteBuffer.allocate(1+proof.length).put((byte)2).put(proof).array(); }
            return suppressed() ? new byte[]{0} : ByteBuffer.allocate(17).put((byte)1).putLong(position.term()).putLong(position.index()).array();
        }
        public static Result decode(byte[] bytes) {
            Objects.requireNonNull(bytes); if (bytes.length<1 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot gate reply size");
            var b=ByteBuffer.wrap(bytes); int kind=b.get()&255;
            if (kind==0 && !b.hasRemaining()) return new Result(null);
            if (kind==1 && b.remaining()==16) return new Result(new ApplyPosition(b.getLong(),b.getLong()));
            if (kind!=2) throw new IllegalArgumentException("Invalid snapshot gate reply kind/size");
            byte[] proof=new byte[b.remaining()]; b.get(proof); return retired(SnapshotFenceProof.decode(proof));
        }
    }
}
