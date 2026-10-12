package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Bounded frontier replacement derived from committed original topology cuts. Structural evidence is not fresh admission. */
public record DistributedCdcBarrierHandoff(TopologyChange.Header header,DistributedCdcCheckpoint parentTerminal,DistributedCdcCheckpoint childStarts) {
    public static final int MAX_BYTES=12+TopologyChange.MAX_HEADER_BYTES+2*(4+2*DistributedCdcCheckpoint.ENTRY_BYTES);
    public DistributedCdcBarrierHandoff {
        Objects.requireNonNull(header); Objects.requireNonNull(parentTerminal); Objects.requireNonNull(childStarts);
        if(header.kind()==TopologyChange.Kind.RELOCATE) throw new IllegalArgumentException("CDC relocation does not replace lineages");
        requireParticipants(header.parents(),parentTerminal); requireParticipants(header.targets(),childStarts);
    }
    private static void requireParticipants(List<TopologyChange.Participant> participants,DistributedCdcCheckpoint checkpoint) {
        var keys=new HashSet<DistributedCdcCheckpoint.Lineage>();
        for(var participant:participants) keys.add(lineage(participant));
        if(!keys.equals(checkpoint.cursors().keySet())) throw new IllegalArgumentException("CDC barrier participant coverage changed");
        for(var cursor:checkpoint.cursors().values()) if(cursor.position().term()<1 || cursor.position().index()<1 || cursor.mutationOrdinal()!=DistributedCommandCodec.MAX_MUTATIONS) throw new IllegalArgumentException("CDC handoff requires original committed terminal/activation cuts");
    }
    public static DistributedCdcBarrierHandoff from(TopologyChange.Definition definition,TopologyChange.State state) {
        state.requireDefinition(definition); return from(definition.header(),state);
    }
    public static DistributedCdcBarrierHandoff from(TopologyChange.Header header,TopologyChange.State state) {
        if(!Arrays.equals(header.encode(),state.header().encode())) throw new IllegalArgumentException("CDC topology header differs from committed intent");
        if(state.phase()!=TopologyChange.Phase.ACTIVE && state.phase()!=TopologyChange.Phase.RETIRED) throw new IllegalArgumentException("CDC children lack committed activation quorums");
        return new DistributedCdcBarrierHandoff(header,cuts(header.parents(),state.frozen()),cuts(header.targets(),state.activated()));
    }
    private static DistributedCdcCheckpoint cuts(List<TopologyChange.Participant> participants,Map<TopologyChange.Key,TopologyChange.Proof> proofs) {
        var result=new TreeMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
        for(var participant:participants) {
            ApplyPosition cut=null; int voters=0;
            for(var proof:proofs.values()) if(proof.groupId().equals(participant.groupId())) {
                if(cut!=null && !cut.equals(proof.position())) throw new IllegalArgumentException("CDC topology owners disagree on original cut"); cut=proof.position();
                if(participant.owner(proof.owner().replicaId()).role()==ReplicaDescriptor.Role.VOTER) voters++;
            }
            if(voters<participant.quorum()) throw new IllegalArgumentException("CDC original cut lacks voter quorum"); result.put(lineage(participant),DistributedCdcCheckpoint.Cursor.after(cut));
        }
        return new DistributedCdcCheckpoint(result);
    }
    public void requireAuthority(TopologyChange.Definition definition,TopologyChange.State state) {
        if(!equals(from(definition,state))) throw new IllegalArgumentException("CDC handoff differs from committed immutable topology authority");
    }
    private static DistributedCdcCheckpoint.Lineage lineage(TopologyChange.Participant participant) { return new DistributedCdcCheckpoint.Lineage(participant.rangeId(),participant.groupId(),participant.rangeEpoch()); }
    public byte[] encode() {
        byte[] h=header.encode(),p=parentTerminal.encode(),c=childStarts.encode();
        return ByteBuffer.allocate(12+h.length+p.length+c.length).putInt(h.length).put(h).putInt(p.length).put(p).putInt(c.length).put(c).array();
    }
    public static DistributedCdcBarrierHandoff decode(byte[] bytes) {
        if(bytes==null || bytes.length<12 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid CDC handoff size"); var in=ByteBuffer.wrap(bytes);
        var result=new DistributedCdcBarrierHandoff(TopologyChange.Header.decode(blob(in,TopologyChange.MAX_HEADER_BYTES)),DistributedCdcCheckpoint.decode(blob(in,124)),DistributedCdcCheckpoint.decode(blob(in,124)));
        if(in.hasRemaining() || !Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical CDC handoff"); return result;
    }
    private static byte[] blob(ByteBuffer in,int max) { if(in.remaining()<4) throw new IllegalArgumentException("Truncated CDC handoff"); int size=in.getInt(); if(size<0 || size>max || size>in.remaining()) throw new IllegalArgumentException("Invalid CDC handoff field"); byte[] bytes=new byte[size]; in.get(bytes); return bytes; }
    @Override public boolean equals(Object other) { return other instanceof DistributedCdcBarrierHandoff h && Arrays.equals(header.encode(),h.header.encode()) && parentTerminal.equals(h.parentTerminal) && childStarts.equals(h.childStarts); }
    @Override public int hashCode() { return Objects.hash(Arrays.hashCode(header.encode()),parentTerminal,childStarts); }
}
