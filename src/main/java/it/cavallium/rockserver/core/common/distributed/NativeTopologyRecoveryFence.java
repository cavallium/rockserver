package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Receiver-local recovery admission fence. Trusted replay may proceed; original bytes never authorize serving. */
public record NativeTopologyRecoveryFence(TopologyChange.Header header,UUID groupId,TopologyChange.Owner owner,
        ApplyPosition catalogueCut,byte[] anchorDigest) {
    public static final int MAX_BYTES=TopologyChange.MAX_HEADER_BYTES+128;
    public NativeTopologyRecoveryFence {
        Objects.requireNonNull(header);Objects.requireNonNull(groupId);Objects.requireNonNull(owner);Objects.requireNonNull(catalogueCut);
        if(catalogueCut.term()<1 || catalogueCut.index()<1 || anchorDigest==null || anchorDigest.length!=32)throw new IllegalArgumentException("Invalid topology recovery fence");
        anchorDigest=anchorDigest.clone();
        if(!participant(header,groupId).owner(owner.replicaId()).equals(owner))throw new IllegalArgumentException("Recovery receiver differs from original placement");
    }
    private static TopologyChange.Participant participant(TopologyChange.Header header,UUID group){return java.util.stream.Stream.concat(header.parents().stream(),header.targets().stream()).filter(p->p.groupId().equals(group)).findFirst().orElseThrow(()->new IllegalArgumentException("Recovery group is outside original lineage"));}
    public void requireGroup(UUID group,long epoch){if(!groupId.equals(group) || participant(header,group).rangeEpoch()!=epoch)throw new IllegalArgumentException("Recovery fence crossed group/epoch");}
    public NativeTopologyRecoveryFence forGroup(UUID group){return new NativeTopologyRecoveryFence(header,group,owner,catalogueCut,anchorDigest);}
    @Override public byte[] anchorDigest(){return anchorDigest.clone();}
    public byte[] encode(){byte[] h=header.encode();var out=ByteBuffer.allocate(121+h.length).putInt(0x54524643).putInt(h.length).put(h);uuid(out,groupId);uuid(out,owner.replicaId());out.put((byte)owner.role().ordinal()).put(owner.identityDigest()).putLong(catalogueCut.term()).putLong(catalogueCut.index()).put(anchorDigest);return out.array();}
    public static NativeTopologyRecoveryFence decode(byte[] bytes){if(bytes==null || bytes.length<121 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid recovery fence length");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=0x54524643)throw new IllegalArgumentException("Invalid recovery fence tag");int size=in.getInt();if(size<1 || size>TopologyChange.MAX_HEADER_BYTES || size!=in.remaining()-113)throw new IllegalArgumentException("Invalid recovery header length");byte[] h=new byte[size];in.get(h);var group=uuid(in);var replica=uuid(in);int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IllegalArgumentException("Invalid recovery role");byte[] identity=new byte[32];in.get(identity);var cut=new ApplyPosition(in.getLong(),in.getLong());byte[] digest=new byte[32];in.get(digest);var result=new NativeTopologyRecoveryFence(TopologyChange.Header.decode(h),group,new TopologyChange.Owner(replica,ReplicaDescriptor.Role.values()[role],identity),cut,digest);if(in.hasRemaining() || !Arrays.equals(bytes,result.encode()))throw new IllegalArgumentException("Noncanonical recovery fence");return result;}
    private static void uuid(ByteBuffer out,UUID id){out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());}
    private static UUID uuid(ByteBuffer in){return new UUID(in.getLong(),in.getLong());}
    @Override public boolean equals(Object other){return other instanceof NativeTopologyRecoveryFence f && Arrays.equals(encode(),f.encode());}
    @Override public int hashCode(){return Arrays.hashCode(encode());}
}
