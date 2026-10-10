package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Forced local child receipt for a trusted common publication decision. */
public final class NativeTopologyActivation {
    private static final int MAGIC=0x4e544143;
    public static final int MAX_BYTES=237+TopologyChange.MAX_HEADER_BYTES;
    private final TopologyChange.Header header;
    private final NativeTopologyBirth birth;
    private final TopologyChange.Owner owner;
    private final ApplyPosition position,catalogueCut;
    private final UUID catalogueGroup;
    private final byte[] freeze,publication;
    public NativeTopologyActivation(NativeTopologySeed seed,NativeTopologyBirth birth,ApplyPosition position,UUID catalogueGroup,ApplyPosition catalogueCut,byte[] publication) {
        this(seed.header(),birth,TopologyChange.Owner.of(seed.localReplica()),position,catalogueGroup,catalogueCut,seed.authority().freezeDigest(),publication);
        birth.requireSeed(seed);
    }
    private NativeTopologyActivation(TopologyChange.Header header,NativeTopologyBirth birth,TopologyChange.Owner owner,ApplyPosition position,UUID catalogueGroup,ApplyPosition catalogueCut,byte[] freeze,byte[] publication) {
        this.header=Objects.requireNonNull(header); this.birth=Objects.requireNonNull(birth); this.owner=Objects.requireNonNull(owner); this.position=Objects.requireNonNull(position); this.catalogueGroup=Objects.requireNonNull(catalogueGroup); this.catalogueCut=Objects.requireNonNull(catalogueCut);
        if(header.kind()==TopologyChange.Kind.RELOCATE || !birth.intentId().equals(header.intentId()) || !header.participant(TopologyChange.Stage.ACTIVATE,birth.groupId()).owners().contains(owner) || position.term()<=0 || position.index()<=0 || catalogueCut.term()<=0 || catalogueCut.index()<=0) throw new IllegalArgumentException("Invalid topology activation identity/cut");
        this.freeze=exact(freeze); this.publication=exact(publication);
        if(!Arrays.equals(this.publication,commitment(header,this.freeze,catalogueGroup))) throw new IllegalArgumentException("Invalid topology publication commitment");
    }
    private static byte[] exact(byte[] value) { if(value==null || value.length!=32) throw new IllegalArgumentException("Invalid topology activation commitment"); return value.clone(); }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException e) { throw new AssertionError(e); } }
    private static byte[] commitment(TopologyChange.Header header,byte[] freeze,UUID catalogue) { return hash(ByteBuffer.allocate(144).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(hash(header.encode())).put(header.definitionDigest()).put(exact(freeze)).put(header.targetDirectoryDigest()).array()); }
    public static byte[] publicationCommitment(TopologyChange.Definition definition,byte[] commonFreezeDigest,UUID catalogueGroup) { return commitment(definition.header(),commonFreezeDigest,Objects.requireNonNull(catalogueGroup)); }
    public TopologyChange.Header header() { return header; }
    public NativeTopologyBirth birth() { return birth; }
    public TopologyChange.Owner owner() { return owner; }
    public ApplyPosition position() { return position; }
    public UUID catalogueGroup() { return catalogueGroup; }
    public ApplyPosition catalogueCut() { return catalogueCut; }
    public byte[] commonFreezeDigest() { return freeze.clone(); }
    public byte[] publicationCommitment() { return publication.clone(); }
    public void requireRequest(TopologyChange.Definition definition,byte[] commonFreezeDigest,UUID catalogue,ApplyPosition cut,byte[] commitment) { header.requireDefinition(definition); if(!Arrays.equals(freeze,commonFreezeDigest) || !catalogueGroup.equals(catalogue) || !catalogueCut.equals(cut) || !Arrays.equals(publication,commitment)) throw new IllegalArgumentException("Topology activation request changed"); }
    public void requireSeed(NativeTopologySeed seed) { birth.requireSeed(seed); header.requireDefinition(seed.definition()); if(!owner.equals(TopologyChange.Owner.of(seed.localReplica())) || !Arrays.equals(freeze,seed.authority().freezeDigest())) throw new IllegalArgumentException("Topology activation differs from retained local seed"); }
    public void requireCut(ApplyPosition cut) { if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Topology activation exceeds completed prefix"); }
    public TopologyChange.Proof proof() { var predecessor=TopologyChange.Proof.of(TopologyChange.Stage.SEED,header,birth.groupId(),owner.replicaId(),new ApplyPosition(0,0),birth.digest(),freeze); return TopologyChange.Proof.of(TopologyChange.Stage.ACTIVATE,header,birth.groupId(),owner.replicaId(),position,digest(),predecessor.digest()); }
    public byte[] digest() { return hash(encode()); }
    public byte[] encode() { byte[] h=header.encode(); var out=ByteBuffer.allocate(237+h.length).putInt(MAGIC).putInt(h.length).put(h).put(birth.encode()); uuid(out,owner.replicaId()); out.put((byte)owner.role().ordinal()).put(owner.identityDigest()).putLong(position.term()).putLong(position.index()); uuid(out,catalogueGroup); return out.putLong(catalogueCut.term()).putLong(catalogueCut.index()).put(freeze).put(publication).array(); }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
    public static NativeTopologyActivation decode(byte[] bytes) {
        if(bytes==null || bytes.length<237 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid topology activation size"); var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid topology activation tag"); int n=in.getInt(); if(n<0 || n>TopologyChange.MAX_HEADER_BYTES || n!=bytes.length-237) throw new IllegalArgumentException("Invalid activation header size"); byte[] h=new byte[n],b=new byte[NativeTopologyBirth.BYTES],identity=new byte[32],f=new byte[32],p=new byte[32]; in.get(h).get(b); UUID replica=uuid(in); int role=Byte.toUnsignedInt(in.get()); if(role>=ReplicaDescriptor.Role.values().length) throw new IllegalArgumentException("Invalid activation owner role"); in.get(identity); var position=new ApplyPosition(in.getLong(),in.getLong()); UUID catalogue=uuid(in); var cut=new ApplyPosition(in.getLong(),in.getLong()); in.get(f).get(p); return new NativeTopologyActivation(TopologyChange.Header.decode(h),NativeTopologyBirth.decode(b),new TopologyChange.Owner(replica,ReplicaDescriptor.Role.values()[role],identity),position,catalogue,cut,f,p);
    }
}
