package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Frozen serving state-machine identity. The peer is resolved against the client's frozen replica descriptor. */
public record RatisSnapshotOwner(UUID groupId,long rangeEpoch,byte[] schemaDigest,UUID incarnation,String peerId,RatisSnapshotCohortRpc.Capabilities cohort) {
    public static final int HEADER_BYTES=80, MAX_PEER_BYTES=128, MAX_BYTES=HEADER_BYTES+MAX_PEER_BYTES+RatisSnapshotCohortRpc.MAX_CAPABILITIES_BYTES;
    public RatisSnapshotOwner {
        Objects.requireNonNull(groupId); Objects.requireNonNull(schemaDigest); Objects.requireNonNull(incarnation);
        if (rangeEpoch<0 || schemaDigest.length!=32 || peerId==null || peerId.isEmpty() || peerId.length()>MAX_PEER_BYTES) throw new IllegalArgumentException("Invalid snapshot owner identity");
        for (int i=0;i<peerId.length();i++) if (peerId.charAt(i)>127) throw new IllegalArgumentException("Snapshot owner peer must be ASCII");
        schemaDigest=schemaDigest.clone();
    }
    @Override public byte[] schemaDigest() { return schemaDigest.clone(); }
    public byte[] encode() {
        byte[] peer=peerId.getBytes(StandardCharsets.US_ASCII),capabilities=RatisSnapshotCohortRpc.capabilities(cohort); var b=ByteBuffer.allocate(HEADER_BYTES+peer.length+capabilities.length);
        uuid(b,groupId); b.putLong(rangeEpoch).put(schemaDigest); uuid(b,incarnation); return b.putInt(peer.length).put(peer).putInt(capabilities.length).put(capabilities).array();
    }
    public static RatisSnapshotOwner decode(byte[] bytes) {
        Objects.requireNonNull(bytes);
        if (bytes.length<HEADER_BYTES+2 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot owner length");
        var b=ByteBuffer.wrap(bytes); UUID group=uuid(b); long epoch=b.getLong(); byte[] schema=new byte[32]; b.get(schema); UUID incarnation=uuid(b); int length=b.getInt();
        if (length<1 || length>MAX_PEER_BYTES || length>b.remaining()-5) throw new IllegalArgumentException("Invalid snapshot owner peer length");
        byte[] peer=new byte[length]; b.get(peer); for (byte value:peer) if (value<0) throw new IllegalArgumentException("Snapshot owner peer must be ASCII");
        int size=b.getInt();if(size<1 || size>RatisSnapshotCohortRpc.MAX_CAPABILITIES_BYTES || size!=b.remaining())throw new IllegalArgumentException("Invalid snapshot cohort capabilities");byte[] capabilities=new byte[size];b.get(capabilities);var owner=new RatisSnapshotOwner(group,epoch,schema,incarnation,new String(peer,StandardCharsets.US_ASCII),RatisSnapshotCohortRpc.capabilities(capabilities));
        if (!Arrays.equals(bytes,owner.encode())) throw new IllegalArgumentException("Noncanonical snapshot owner"); return owner;
    }
    @Override public boolean equals(Object other) { return other instanceof RatisSnapshotOwner owner && groupId.equals(owner.groupId) && rangeEpoch==owner.rangeEpoch && Arrays.equals(schemaDigest,owner.schemaDigest) && incarnation.equals(owner.incarnation) && peerId.equals(owner.peerId) && Objects.equals(cohort,owner.cohort); }
    @Override public int hashCode() { return Objects.hash(groupId,rangeEpoch,Arrays.hashCode(schemaDigest),incarnation,peerId,cohort); }
    private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
}
