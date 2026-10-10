package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Compact local birth pointer. Resolve its retained seed recipe; it grants no activation authority. */
public final class NativeTopologyBirth {
    private static final int MAGIC=0x4e544252;
    public static final int BYTES=68;
    private final UUID intent,group;
    private final byte[] seedDigest;
    public NativeTopologyBirth(UUID intentId,UUID groupId,byte[] seedDigest) {
        intent=Objects.requireNonNull(intentId); group=Objects.requireNonNull(groupId); if(seedDigest==null || seedDigest.length!=32) throw new IllegalArgumentException("Invalid topology birth seed commitment"); this.seedDigest=seedDigest.clone();
    }
    public UUID intentId() { return intent; }
    public UUID groupId() { return group; }
    public byte[] seedDigest() { return seedDigest.clone(); }
    public void requireSeed(NativeTopologySeed seed) {
        if(!intent.equals(seed.definition().intentId()) || !Arrays.equals(seedDigest,seed.digest()) || seed.definition().targets().stream().noneMatch(r->r.groupId().equals(group))) throw new IllegalArgumentException("Topology birth crossed intent/seed/target group");
    }
    public RangeDescriptor resolveDescriptor(NativeTopologySeed seed) { requireSeed(seed); return seed.definition().targets().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow(); }
    public TopologyChange.Proof proof(NativeTopologySeed seed) { requireSeed(seed); return TopologyChange.Proof.of(TopologyChange.Stage.SEED,seed.header(),group,seed.localReplica().replicaId(),new ApplyPosition(0,0),digest(),seed.authority().freezeDigest()); }
    public byte[] encode() { return ByteBuffer.allocate(BYTES).putInt(MAGIC).putLong(intent.getMostSignificantBits()).putLong(intent.getLeastSignificantBits()).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put(seedDigest).array(); }
    public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(encode()); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    public static NativeTopologyBirth decode(byte[] bytes) {
        if(bytes==null || bytes.length!=BYTES) throw new IllegalArgumentException("Invalid topology birth size"); var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid topology birth tag"); UUID intent=new UUID(in.getLong(),in.getLong()),group=new UUID(in.getLong(),in.getLong()); byte[] digest=new byte[32]; in.get(digest); return new NativeTopologyBirth(intent,group,digest);
    }
    @Override public boolean equals(Object other) { return other instanceof NativeTopologyBirth birth && intent.equals(birth.intent) && group.equals(birth.group) && Arrays.equals(seedDigest,birth.seedDigest); }
    @Override public int hashCode() { return 31*Objects.hash(intent,group)+Arrays.hashCode(seedDigest); }
}
