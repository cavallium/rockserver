package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Immutable snapshot definition in its dedicated HOME placement. */
public record SnapshotHomeIdentity(UUID groupId,long rangeEpoch,byte[] schemaDigest,UUID columnId,SnapshotFence fence) {
    public static final int MAX_BYTES=76+SnapshotFence.MAX_BYTES;
    public SnapshotHomeIdentity {
        Objects.requireNonNull(groupId); Objects.requireNonNull(columnId); Objects.requireNonNull(schemaDigest); Objects.requireNonNull(fence);
        if (rangeEpoch<0 || schemaDigest.length!=32 || !groupId.equals(fence.homeGroup()) || rangeEpoch!=fence.homeEpoch()) throw new IllegalArgumentException("Invalid snapshot HOME identity");
        schemaDigest=schemaDigest.clone();
    }
    @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    public OperationId business() { return fence.operation(); }
    public UUID snapshotId() { return fence.fenceId(); }
    public byte[] encode() {
        byte[] definition=fence.encode(); return ByteBuffer.allocate(76+definition.length).putLong(groupId.getMostSignificantBits()).putLong(groupId.getLeastSignificantBits()).putLong(rangeEpoch).put(schemaDigest)
                .putLong(columnId.getMostSignificantBits()).putLong(columnId.getLeastSignificantBits()).putInt(definition.length).put(definition).array();
    }
    public static SnapshotHomeIdentity decode(byte[] bytes) {
        Objects.requireNonNull(bytes); if (bytes.length<76+SnapshotFence.HEADER_BYTES || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot HOME identity size");
        var b=ByteBuffer.wrap(bytes); UUID group=new UUID(b.getLong(),b.getLong()); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema); UUID column=new UUID(b.getLong(),b.getLong());
        int n=b.getInt(); if (n!=b.remaining()) throw new IllegalArgumentException("Invalid snapshot HOME definition size"); byte[] definition=new byte[n]; b.get(definition); return new SnapshotHomeIdentity(group,epoch,schema,column,SnapshotFence.decode(definition));
    }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return this==other || other instanceof SnapshotHomeIdentity id && groupId.equals(id.groupId) && rangeEpoch==id.rangeEpoch && columnId.equals(id.columnId) && Arrays.equals(schemaDigest,id.schemaDigest) && fence.equals(id.fence); }
    @Override public int hashCode() { return Objects.hash(groupId,rangeEpoch,columnId,Arrays.hashCode(schemaDigest),fence); }
}
