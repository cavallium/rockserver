package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Immutable local seed recipe, not USER payload or activation authority. The pool verifies actual local native state. */
public final class NativeTopologySeed {
    private static final int MAGIC=0x4e545344;
    public static final int MAX_NAME_BYTES=4096;
    public static final int MAX_BYTES=36+TopologyChange.MAX_DEFINITION_BYTES+TopologyChange.MAX_STATUS_BYTES+2*(20+NativeTopologyFreeze.MAX_BYTES)+RangeDirectoryCodec.MAX_COLUMNS*(21+MAX_NAME_BYTES);
    private final TopologyChange.Definition definition;
    private final TopologyChange.Header header;
    private final TopologyChange.State authority;
    private final ReplicaDescriptor local;
    private final Map<UUID,NativeTopologyFreeze> freezes;
    private final Map<UUID,String> names;
    private final Set<UUID> merges;
    private final byte[] encoded,digest;
    public NativeTopologySeed(TopologyChange.Definition definition,TopologyChange.State frozenAuthority,ReplicaDescriptor local,
            Map<UUID,NativeTopologyFreeze> parentFreezes,Map<UUID,String> physicalNames,Set<UUID> mergeColumns) {
        this.definition=Objects.requireNonNull(definition); header=definition.header(); authority=Objects.requireNonNull(frozenAuthority); this.local=Objects.requireNonNull(local); authority.requireDefinition(definition);
        if(definition.kind()==TopologyChange.Kind.RELOCATE || authority.phase()==TopologyChange.Phase.PREPARED || authority.phase()==TopologyChange.Phase.FENCING || authority.phase()==TopologyChange.Phase.ABORTED) throw new IllegalArgumentException("Topology seed requires split/merge freeze quorums");
        var placement=definition.parents().getFirst().replicas();
        for(var parent:definition.parents()) if(!parent.replicas().equals(placement) || !parent.replicas().contains(local)) throw new IllegalArgumentException("Local seed replica differs from all parent placements");
        for(var target:definition.targets()) if(!target.replicas().equals(placement) || !target.replicas().contains(local)) throw new IllegalArgumentException("Local seed replica differs from target placement");
        freezes=Map.copyOf(parentFreezes); names=Map.copyOf(physicalNames); merges=Set.copyOf(mergeColumns);
        var parents=new HashSet<UUID>(); definition.parents().forEach(parent->parents.add(parent.groupId()));
        if(!freezes.keySet().equals(parents)) throw new IllegalArgumentException("Seed must cover the exact full parent set");
        if(!names.keySet().equals(definition.sourceDirectory().columns()) || !names.keySet().containsAll(merges)) throw new IllegalArgumentException("Seed CF/merge coverage differs from frozen schema columns");
        var aliases=new HashSet<String>(); for(var name:names.values()) { requireName(name); if(!aliases.add(name)) throw new IllegalArgumentException("Duplicate physical CF alias in topology seed"); }
        byte[] commonAdmission=null;
        for(var parent:definition.parents()) {
            var freeze=freezes.get(parent.groupId()); if(!parent.groupId().equals(freeze.groupId())) throw new IllegalArgumentException("Seed parent map crossed native freeze group");
            freeze.requireRequest(header,freeze.admissionDigest()); var source=definition.sourceDirectory(); freeze.requireParent(parent.groupId(),parent.rangeEpoch(),source.domainId(),source.schemaDigest(),parent.lowerInclusive(),parent.upperExclusive());
            if(commonAdmission!=null && !Arrays.equals(commonAdmission,freeze.admissionDigest())) throw new IllegalArgumentException("Parent freezes disagree on original admission"); commonAdmission=freeze.admissionDigest();
            var observed=authority.frozen().values().stream().filter(p->p.groupId().equals(parent.groupId())).toList();
            if(observed.isEmpty()) throw new IllegalArgumentException("Seed parent lacks frozen quorum references");
            for(var proof:observed) if(!proof.position().equals(freeze.position()) || !Arrays.equals(proof.artifactDigest(),freeze.digest())) throw new IllegalArgumentException("Local parent artifact differs from qualified authority cut/artifact");
            // An offline/listener owner need not be in the quorum map; its exact shared native artifact must still match.
            var own=authority.frozen().get(new TopologyChange.Key(parent.groupId(),local.replicaId()));
            if(own!=null && !own.owner().equals(TopologyChange.Owner.of(local))) throw new IllegalArgumentException("Seed local identity differs from original freeze owner");
        }
        byte[] d=definition.encode(),a=authority.encode(); var body=new HashMap<UUID,byte[]>(); long size=36L+d.length+a.length;
        for(var entry:freezes.entrySet()) { byte[] bytes=entry.getValue().encode(); body.put(entry.getKey(),bytes); size+=20L+bytes.length; }
        for(var name:names.values()) size+=21L+name.length(); if(size>MAX_BYTES) throw new IllegalArgumentException("Topology seed exceeds artifact bound");
        var out=ByteBuffer.allocate((int)size).putInt(MAGIC); blob(out,d); blob(out,a); uuid(out,local.replicaId()); out.putInt(freezes.size());
        for(var group:freezes.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()) { uuid(out,group); blob(out,body.get(group)); }
        out.putInt(names.size()); for(var column:names.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()) { uuid(out,column); blob(out,names.get(column).getBytes(StandardCharsets.US_ASCII)); out.put((byte)(merges.contains(column) ? 1 : 0)); }
        encoded=out.array(); digest=hash(encoded);
    }
    public TopologyChange.Definition definition() { return definition; }
    public TopologyChange.Header header() { return header; }
    public TopologyChange.State authority() { return authority; }
    public ReplicaDescriptor localReplica() { return local; }
    public Map<UUID,NativeTopologyFreeze> parentFreezes() { return freezes; }
    public Map<UUID,String> physicalNames() { return names; }
    public Set<UUID> mergeColumns() { return merges; }
    public int encodedSize() { return encoded.length; }
    public byte[] encode() { return encoded.clone(); }
    public byte[] digest() { return digest.clone(); }
    public NativeTopologyBirth birth(UUID targetGroup) { var birth=new NativeTopologyBirth(definition.intentId(),targetGroup,digest); birth.requireSeed(this); return birth; }
    public TopologyChange.Proof proof(UUID targetGroup) { return birth(targetGroup).proof(this); }
    public static NativeTopologySeed decode(byte[] bytes) {
        if(bytes==null || bytes.length<36 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid topology seed byte bound"); var in=ByteBuffer.wrap(bytes);
        if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid topology seed tag"); var definition=TopologyChange.Definition.decode(blob(in,TopologyChange.MAX_DEFINITION_BYTES)); var authority=TopologyChange.State.decode(blob(in,TopologyChange.MAX_STATUS_BYTES)); UUID replica=uuid(in);
        if(in.remaining()<4) throw new IllegalArgumentException("Truncated parent count"); int count=in.getInt(); if(count<1 || count>2 || count!=definition.parents().size() || 20L*count>in.remaining()) throw new IllegalArgumentException("Invalid topology seed parent count"); var freezes=new HashMap<UUID,NativeTopologyFreeze>();
        for(int i=0;i<count;i++) { UUID group=uuid(in); var freeze=NativeTopologyFreeze.decode(blob(in,NativeTopologyFreeze.MAX_BYTES)); if(freezes.put(group,freeze)!=null) throw new IllegalArgumentException("Duplicate topology seed parent"); }
        if(in.remaining()<4) throw new IllegalArgumentException("Truncated CF count"); count=in.getInt(); if(count<0 || count>RangeDirectoryCodec.MAX_COLUMNS || 21L*count>in.remaining()) throw new IllegalArgumentException("Invalid topology seed CF count"); var names=new HashMap<UUID,String>(); var merges=new HashSet<UUID>();
        for(int i=0;i<count;i++) { UUID column=uuid(in); byte[] name=blob(in,MAX_NAME_BYTES); for(byte value:name) if(value<=0) throw new IllegalArgumentException("CF alias is not non-NUL ASCII"); if(in.remaining()<1) throw new IllegalArgumentException("Truncated merge flag"); int merge=in.get()&255; if(merge>1 || names.put(column,new String(name,StandardCharsets.US_ASCII))!=null) throw new IllegalArgumentException("Invalid CF alias/merge flag"); if(merge==1) merges.add(column); }
        if(in.hasRemaining()) throw new IllegalArgumentException("Trailing topology seed bytes");
        var local=definition.parents().getFirst().replicas().stream().filter(r->r.replicaId().equals(replica)).findFirst().orElseThrow(()->new IllegalArgumentException("Unknown seed local replica"));
        var seed=new NativeTopologySeed(definition,authority,local,freezes,names,merges); if(!Arrays.equals(bytes,seed.encoded)) throw new IllegalArgumentException("Noncanonical topology seed encoding"); return seed;
    }
    private static void requireName(String name) { if(name==null || name.isEmpty() || name.length()>MAX_NAME_BYTES) throw new IllegalArgumentException("Invalid CF alias size"); for(int i=0;i<name.length();i++) if(name.charAt(i)==0 || name.charAt(i)>127) throw new IllegalArgumentException("CF alias is not non-NUL ASCII"); }
    private static byte[] blob(ByteBuffer in,int maximum) { if(in.remaining()<4) throw new IllegalArgumentException("Truncated topology seed field"); int n=in.getInt(); if(n<0 || n>maximum || n>in.remaining()) throw new IllegalArgumentException("Invalid topology seed field size"); byte[] bytes=new byte[n]; in.get(bytes); return bytes; }
    private static void blob(ByteBuffer out,byte[] bytes) { out.putInt(bytes.length).put(bytes); }
    private static UUID uuid(ByteBuffer in) { if(in.remaining()<16) throw new IllegalArgumentException("Truncated topology seed UUID"); return new UUID(in.getLong(),in.getLong()); }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return other instanceof NativeTopologySeed seed && Arrays.equals(encoded,seed.encoded); }
    @Override public int hashCode() { return Arrays.hashCode(encoded); }
}
