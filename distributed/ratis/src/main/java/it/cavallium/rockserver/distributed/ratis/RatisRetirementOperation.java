package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;

/** One immutable current native cleanup frame, including its original control and business identities. */
public final class RatisRetirementOperation {
    public static final int MAX_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES;
    public enum Type { PARTICIPANT, HOME, SNAPSHOT_GATE, SNAPSHOT_HOME, RELEASE }
    private final Type type;
    private final UUID targetGroup;
    private final Object value;
    private final byte[] bytes;

    private RatisRetirementOperation(Type type,UUID targetGroup,Object value,byte[] bytes) {
        this.type=type; this.targetGroup=targetGroup; this.value=value;
        if (bytes.length>MAX_BYTES) throw new IllegalArgumentException("Retirement frame exceeds command budget");
        this.bytes=bytes;
    }
    public static RatisRetirementOperation participant(ParticipantRetirement value) {
        Objects.requireNonNull(value); return new RatisRetirementOperation(Type.PARTICIPANT,value.identity().participantGroup(),value,RatisParticipantRecord.encode(value));
    }
    public static RatisRetirementOperation home(HomeRetirement value) {
        Objects.requireNonNull(value); return new RatisRetirementOperation(Type.HOME,value.identity().groupId(),value,RatisHomeRetirementRecord.encode(value));
    }
    public static RatisRetirementOperation snapshotGate(SnapshotGateRetirement value) {
        Objects.requireNonNull(value); return new RatisRetirementOperation(Type.SNAPSHOT_GATE,value.participantGroup(),value,RatisSnapshotRetirementRecord.encode(value));
    }
    public static RatisRetirementOperation snapshotHome(SnapshotHomeRetirement value) {
        Objects.requireNonNull(value); return new RatisRetirementOperation(Type.SNAPSHOT_HOME,value.identity().groupId(),value,RatisSnapshotHomeRetirementRecord.encode(value));
    }
    public static RatisRetirementOperation release(RatisSnapshotRecord value) {
        Objects.requireNonNull(value); if (!value.isRelease()) throw new IllegalArgumentException("Cleanup requires a snapshot RELEASE");
        return new RatisRetirementOperation(Type.RELEASE,value.participantGroup(),value,value.encode());
    }
    public Type type() { return type; }
    public UUID targetGroup() { return targetGroup; }
    public byte[] bytes() { return bytes.clone(); }
    int encodedSize() { return bytes.length; }
    public ParticipantRetirement participant() { require(Type.PARTICIPANT); return (ParticipantRetirement)value; }
    public HomeRetirement home() { require(Type.HOME); return (HomeRetirement)value; }
    public SnapshotGateRetirement snapshotGate() { require(Type.SNAPSHOT_GATE); return (SnapshotGateRetirement)value; }
    public SnapshotHomeRetirement snapshotHome() { require(Type.SNAPSHOT_HOME); return (SnapshotHomeRetirement)value; }
    public RatisSnapshotRecord release() { require(Type.RELEASE); return (RatisSnapshotRecord)value; }
    private void require(Type expected) { if (type!=expected) throw new IllegalStateException("Retirement operation is "+type+", not "+expected); }
    public static RatisRetirementOperation decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<4 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid retirement frame size");
        try {
            final RatisRetirementOperation operation;
            if (RatisParticipantRecord.isRetirement(bytes)) operation=participant(RatisParticipantRecord.decodeRetirement(bytes));
            else if (RatisHomeRetirementRecord.isRetirement(bytes)) operation=home(RatisHomeRetirementRecord.decode(bytes));
            else if (RatisSnapshotRetirementRecord.isRetirement(bytes)) operation=snapshotGate(RatisSnapshotRetirementRecord.decode(bytes));
            else if (RatisSnapshotHomeRetirementRecord.isRetirement(bytes)) operation=snapshotHome(RatisSnapshotHomeRetirementRecord.decode(bytes));
            else if (RatisSnapshotRecord.isSnapshot(bytes)) operation=release(RatisSnapshotRecord.decode(bytes));
            else throw new IllegalArgumentException("Unknown retirement frame kind");
            if (!Arrays.equals(bytes,operation.bytes)) throw new IllegalArgumentException("Noncanonical retirement frame"); return operation;
        } catch (RuntimeException invalid) {
            if (invalid instanceof IllegalArgumentException argument) throw argument;
            throw new IllegalArgumentException("Invalid retirement frame",invalid);
        }
    }
    @Override public boolean equals(Object other) { return other instanceof RatisRetirementOperation operation && Arrays.equals(bytes,operation.bytes); }
    @Override public int hashCode() { return Arrays.hashCode(bytes); }
    @Override public String toString() { return type+" "+targetGroup; }
}
