package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Recovery binding shared by an immutable reservation row and a separate monotone completion row.
 * Completion never releases retained lineage or source images. */
public final class RatisTopologyRecoveryObligations {
    static final int COMPLETION_BYTES=97,OWNER_COMPLETION_BYTES=32+COMPLETION_BYTES;
    public static final int MAX_BYTES=TopologyChange.MAX_HEADER_BYTES+12+3*RangeDirectoryCodec.MAX_REPLICAS*OWNER_COMPLETION_BYTES;
    private static final int MAGIC=0x54524f42;
    public enum ProofKind { CURRENT_REGISTRATION, METADATA_REGISTRATION, LOCAL_METADATA }
    public record Completion(UUID transferId,ProofKind kind,byte[] requestDigest,byte[] proofDigest,ApplyPosition completedCut) {
        public Completion {Objects.requireNonNull(transferId);Objects.requireNonNull(kind);Objects.requireNonNull(completedCut);if(requestDigest.length!=32 || proofDigest.length!=32 || completedCut.term()<1 || completedCut.index()<0)throw new IllegalArgumentException("Invalid recovery completion");requestDigest=requestDigest.clone();proofDigest=proofDigest.clone();}
        public boolean metadata(){return kind!=ProofKind.CURRENT_REGISTRATION;}
        @Override public byte[] requestDigest(){return requestDigest.clone();}
        @Override public byte[] proofDigest(){return proofDigest.clone();}
        @Override public boolean equals(Object other){return other instanceof Completion c && transferId.equals(c.transferId) && kind==c.kind && Arrays.equals(requestDigest,c.requestDigest) && Arrays.equals(proofDigest,c.proofDigest) && completedCut.equals(c.completedCut);}
        @Override public int hashCode(){return Objects.hash(transferId,kind,Arrays.hashCode(requestDigest),Arrays.hashCode(proofDigest),completedCut);}
    }
    private static final Comparator<TopologyChange.Key> ORDER=Comparator.comparing(TopologyChange.Key::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(TopologyChange.Key::replicaId,RangeDirectoryCodec.UUID_ORDER);
    private final TopologyChange.Header header;
    private final SortedMap<TopologyChange.Key,Completion> completions;
    private RatisTopologyRecoveryObligations(TopologyChange.Header header,Map<TopologyChange.Key,Completion> completions){this.header=Objects.requireNonNull(header);var copy=new TreeMap<TopologyChange.Key,Completion>(ORDER);copy.putAll(completions);for(var entry:copy.entrySet()){requireOwner(entry.getKey());requirePurpose(entry.getKey(),entry.getValue());}this.completions=Collections.unmodifiableSortedMap(copy);}
    public static RatisTopologyRecoveryObligations reserve(TopologyChange.Header header){return new RatisTopologyRecoveryObligations(header,Map.of());}
    public TopologyChange.Header header(){return header;}
    /** Includes every original source and target, including owners absent before FREEZE. */
    public Set<TopologyChange.Key> owners(){var result=new TreeSet<TopologyChange.Key>(ORDER);for(var participants:List.of(header.parents(),header.targets()))for(var range:participants)for(var owner:range.owners())result.add(new TopologyChange.Key(range.groupId(),owner.replicaId()));return Collections.unmodifiableSet(result);}
    public Map<TopologyChange.Key,Completion> completions(){return completions;}
    public void requireHeader(TopologyChange.Header expected){if(!Arrays.equals(header.encode(),expected.encode()))throw new IllegalArgumentException("Recovery reservation changed original topology/source authority");}
    void requireOwner(TopologyChange.Key key){if(!owners().contains(key))throw new IllegalArgumentException("Completion has no original owner obligation");}
    private void requirePurpose(TopologyChange.Key key,Completion completion){if(header.parents().stream().anyMatch(parent->parent.groupId().equals(key.groupId())) && !completion.metadata())throw new IllegalArgumentException("Parent recovery obligation requires META completion");}
    public RatisTopologyRecoveryObligations complete(TopologyChange.Key key,Completion completion){requireOwner(key);requirePurpose(key,completion);var copy=new TreeMap<TopologyChange.Key,Completion>(ORDER);copy.putAll(completions);var previous=copy.putIfAbsent(key,Objects.requireNonNull(completion));if(previous!=null && !previous.equals(completion))throw new IllegalArgumentException("Recovery completion retry changed original receipt");return new RatisTopologyRecoveryObligations(header,copy);}
    static byte[] encodeCompletion(Completion c){var out=ByteBuffer.allocate(COMPLETION_BYTES);ScopedTransactionRpc.uuid(out,c.transferId());return out.put((byte)c.kind().ordinal()).put(c.requestDigest()).put(c.proofDigest()).putLong(c.completedCut().term()).putLong(c.completedCut().index()).array();}
    static Completion decodeCompletion(ByteBuffer in){if(in.remaining()!=COMPLETION_BYTES)throw new IllegalArgumentException("Invalid recovery receipt size");UUID transfer=ScopedTransactionRpc.uuid(in);int kind=in.get()&255;if(kind>=ProofKind.values().length)throw new IllegalArgumentException("Invalid recovery receipt proof kind");byte[] request=new byte[32],proofDigest=new byte[32];in.get(request).get(proofDigest);return new Completion(transfer,ProofKind.values()[kind],request,proofDigest,new ApplyPosition(in.getLong(),in.getLong()));}
    public byte[] encode(){byte[] h=header.encode();var out=ByteBuffer.allocate(12+h.length+OWNER_COMPLETION_BYTES*completions.size()).putInt(MAGIC).putInt(h.length).put(h).putInt(completions.size());for(var entry:completions.entrySet()){ScopedTransactionRpc.uuid(out,entry.getKey().groupId());ScopedTransactionRpc.uuid(out,entry.getKey().replicaId());var c=entry.getValue();ScopedTransactionRpc.uuid(out,c.transferId());out.put((byte)c.kind().ordinal()).put(c.requestDigest()).put(c.proofDigest()).putLong(c.completedCut().term()).putLong(c.completedCut().index());}return out.array();}
    public static RatisTopologyRecoveryObligations decode(byte[] bytes){if(bytes==null || bytes.length<12 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid recovery reservation bound");var in=ByteBuffer.wrap(bytes);if(in.getInt()!=MAGIC)throw new IllegalArgumentException("Invalid recovery reservation");int length=in.getInt();if(length<1 || length>TopologyChange.MAX_HEADER_BYTES || length>in.remaining()-4)throw new IllegalArgumentException("Invalid recovery header length");byte[] header=new byte[length];in.get(header);int count=in.getInt();if(count<0 || (long)count*OWNER_COMPLETION_BYTES!=in.remaining())throw new IllegalArgumentException("Invalid recovery completion count");var completed=new TreeMap<TopologyChange.Key,Completion>(ORDER);for(int i=0;i<count;i++){var key=new TopologyChange.Key(ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in));UUID transfer=ScopedTransactionRpc.uuid(in);int kind=in.get()&255;byte[] request=new byte[32],proofDigest=new byte[32];in.get(request).get(proofDigest);if(kind>=ProofKind.values().length || completed.putIfAbsent(key,new Completion(transfer,ProofKind.values()[kind],request,proofDigest,new ApplyPosition(in.getLong(),in.getLong())))!=null)throw new IllegalArgumentException("Invalid duplicate recovery completion");}var result=new RatisTopologyRecoveryObligations(TopologyChange.Header.decode(header),completed);if(!Arrays.equals(bytes,result.encode()))throw new IllegalArgumentException("Noncanonical recovery reservation");return result;}
}
