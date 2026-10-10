package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** Original committed parent cut. This native evidence grants no catalogue/client admission authority. */
public final class NativeTopologyFreeze {
    private static final int MAGIC=0x4e544652;
    public static final int MAX_BYTES=104+TopologyChange.MAX_HEADER_BYTES;
    private final TopologyChange.Header header;
    private final UUID group;
    private final ApplyPosition position;
    private final byte[] boundsDigest,admissionDigest;
    public NativeTopologyFreeze(TopologyChange.Header header,UUID group,ApplyPosition position,byte[] boundsDigest,byte[] admissionDigest) {
        this.header=Objects.requireNonNull(header); this.group=Objects.requireNonNull(group); this.position=Objects.requireNonNull(position);
        if(header.kind()==TopologyChange.Kind.RELOCATE || position.term()==0 || position.index()==0 || boundsDigest==null || boundsDigest.length!=32 || admissionDigest==null || admissionDigest.length!=32) throw new IllegalArgumentException("Invalid native topology FREEZE binding/cut");
        header.participant(TopologyChange.Stage.FREEZE,group); this.boundsDigest=boundsDigest.clone(); this.admissionDigest=admissionDigest.clone();
    }
    public TopologyChange.Header header() { return header; }
    public UUID groupId() { return group; }
    public ApplyPosition position() { return position; }
    public byte[] boundsDigest() { return boundsDigest.clone(); }
    public byte[] admissionDigest() { return admissionDigest.clone(); }
    public void requireRequest(TopologyChange.Header expected,byte[] admission) {
        if(!Arrays.equals(header.encode(),expected.encode()) || !Arrays.equals(admissionDigest,admission)) throw new IllegalArgumentException("Native topology intent/admission changed");
    }
    public void requireParent(UUID group,long epoch,UUID domain,byte[] schema,byte[] lower,byte[] upper) {
        var parent=header.participant(TopologyChange.Stage.FREEZE,this.group);
        if(!this.group.equals(group) || parent.rangeEpoch()!=epoch || !header.domainId().equals(domain) || !Arrays.equals(header.schemaDigest(),schema) || !Arrays.equals(boundsDigest,boundsDigest(lower,upper))) throw new IllegalArgumentException("Native topology FREEZE differs from parent ownership/schema/bounds");
    }
    public void requireCut(ApplyPosition cut) {
        if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("Native topology FREEZE exceeds completed cut");
    }
    public TopologyChange.Proof proof(UUID replica) { return TopologyChange.Proof.of(TopologyChange.Stage.FREEZE,header,group,replica,position,digest(),header.sourceDirectoryDigest()); }
    public byte[] digest() { return hash(encode()); }
    public byte[] encode() {
        byte[] content=header.encode(); var out=ByteBuffer.allocate(104+content.length).putInt(MAGIC).putInt(content.length).put(content);
        out.putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).putLong(position.term()).putLong(position.index()).put(boundsDigest).put(admissionDigest); return out.array();
    }
    public static NativeTopologyFreeze decode(byte[] encoded) {
        if(encoded==null || encoded.length<104 || encoded.length>MAX_BYTES) throw new IllegalArgumentException("Invalid native topology FREEZE size");
        var in=ByteBuffer.wrap(encoded); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid native topology FREEZE encoding");
        int length=in.getInt(); if(length<0 || length!=in.remaining()-96) throw new IllegalArgumentException("Invalid native topology header length");
        byte[] header=new byte[length]; in.get(header); UUID group=new UUID(in.getLong(),in.getLong()); var position=new ApplyPosition(in.getLong(),in.getLong()); byte[] bounds=new byte[32],admission=new byte[32]; in.get(bounds).get(admission);
        return new NativeTopologyFreeze(TopologyChange.Header.decode(header),group,position,bounds,admission);
    }
    public static byte[] boundsDigest(byte[] lower,byte[] upper) {
        var bytes=ByteBuffer.allocate(8+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length));
        bytes.putInt(lower==null ? -1 : lower.length); if(lower!=null) bytes.put(lower); bytes.putInt(upper==null ? -1 : upper.length); if(upper!=null) bytes.put(upper); return hash(bytes.array());
    }
    private static byte[] hash(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    @Override public boolean equals(Object other) { return other instanceof NativeTopologyFreeze freeze && Arrays.equals(encode(),freeze.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
