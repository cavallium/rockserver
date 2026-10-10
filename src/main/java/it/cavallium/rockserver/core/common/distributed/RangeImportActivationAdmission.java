package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Structural all-owner activation evidence; configured fresh native reads authorize domain completion. */
public final class RangeImportActivationAdmission {
    public static final int MAX_BYTES=2*1024*1024;
    /** Canonical native activation bytes, without a common-to-native-implementation dependency. */
    public record Activation(NativeSstImportManifest.Receipt receipt,UUID catalogueGroup,ApplyPosition cataloguePosition,byte[] publicationDigest,ApplyPosition position) {
        public static final int BYTES=168;
        public Activation {
            Objects.requireNonNull(receipt); Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(cataloguePosition); Objects.requireNonNull(position);
            if(publicationDigest.length!=32 || cataloguePosition.term()<1 || position.index()<=receipt.position().index() || position.term()<receipt.position().term()) throw new IllegalArgumentException("Invalid native import activation"); publicationDigest=publicationDigest.clone();
        }
        @Override public byte[] publicationDigest() { return publicationDigest.clone(); }
        public byte[] encode() { return ByteBuffer.allocate(BYTES).put(receipt.encode()).putLong(catalogueGroup.getMostSignificantBits()).putLong(catalogueGroup.getLeastSignificantBits()).putLong(cataloguePosition.term()).putLong(cataloguePosition.index()).put(publicationDigest).putLong(position.term()).putLong(position.index()).array(); }
        public static Activation decode(byte[] bytes) { if(bytes.length!=BYTES) throw new IllegalArgumentException("Invalid activation length"); var in=ByteBuffer.wrap(bytes); byte[] receipt=new byte[NativeSstImportManifest.Receipt.BYTES],digest=new byte[32]; in.get(receipt); UUID group=new UUID(in.getLong(),in.getLong()); var cut=new ApplyPosition(in.getLong(),in.getLong()); in.get(digest); return new Activation(NativeSstImportManifest.Receipt.decode(receipt),group,cut,digest,new ApplyPosition(in.getLong(),in.getLong())); }
        public void requireCut(ApplyPosition completed) { receipt.requireCut(position); if(completed==null || position.term()>completed.term() || position.index()>completed.index() || position.index()==completed.index() && position.term()!=completed.term()) throw new IllegalArgumentException("Native activation exceeds/conflicts with completed cut"); }
    }
    public record OwnerActivation(UUID groupId,UUID rangeId,long configurationVersion,ReplicaDescriptor owner,Activation activation,ApplyPosition completed) {
        public OwnerActivation { Objects.requireNonNull(groupId); Objects.requireNonNull(rangeId); Objects.requireNonNull(owner); Objects.requireNonNull(activation); Objects.requireNonNull(completed); if(configurationVersion<0 || !groupId.equals(activation.receipt().groupId())) throw new IllegalArgumentException("Invalid configured activation owner"); activation.requireCut(completed); }
    }
    private final byte[] definitionDigest,installationDigest,publicationDigest,encoded,digest;
    private final UUID catalogueGroup;
    private final List<OwnerActivation> owners;
    public RangeImportActivationAdmission(RangeImportDefinition definition,RangeImportAdmission installations,UUID catalogueGroup,byte[] publicationDigest,Collection<OwnerActivation> entries) {
        Objects.requireNonNull(definition); Objects.requireNonNull(installations); Objects.requireNonNull(entries); this.catalogueGroup=Objects.requireNonNull(catalogueGroup); installations.requireDefinition(definition);
        if(publicationDigest.length!=32 || entries.size()!=installations.owners().size() || entries.isEmpty() || entries.size()>RangeImportAdmission.MAX_OWNERS) throw new IllegalArgumentException("Every imported owner requires activation");
        definitionDigest=definition.digest(); installationDigest=installations.digest(); this.publicationDigest=publicationDigest.clone(); long bytes=120;
        for(var entry:entries) { bytes=Math.addExact(bytes,281L+entry.owner().peerId().length()+entry.owner().address().length()); if(bytes>MAX_BYTES) throw new IllegalArgumentException("Activation admission exceeds bound"); }
        var map=new HashMap<String,OwnerActivation>(); var groupProofs=new HashMap<UUID,byte[]>();
        for(var entry:entries) {
            var installed=installations.owner(entry.groupId(),entry.owner().replicaId());
            if(!entry.owner().equals(installed.owner()) || !entry.rangeId().equals(installed.rangeId()) || entry.configurationVersion()!=installed.configurationVersion() || !Arrays.equals(entry.activation().receipt().encode(),installed.receipt().encode()) || !entry.activation().catalogueGroup().equals(catalogueGroup) || !Arrays.equals(entry.activation().publicationDigest(),publicationDigest)) throw new IllegalArgumentException("Activation differs from original configured publication/receipt");
            String key=entry.groupId()+"/"+entry.owner().replicaId(); if(map.put(key,entry)!=null) throw new IllegalArgumentException("Duplicate activation owner"); byte[] proof=entry.activation().encode(),prior=groupProofs.putIfAbsent(entry.groupId(),proof); if(prior!=null && !Arrays.equals(prior,proof)) throw new IllegalArgumentException("Owners disagree on original group ACTIVATE entry");
        }
        owners=map.values().stream().sorted(Comparator.comparing(OwnerActivation::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(e -> e.owner().replicaId(),RangeDirectoryCodec.UUID_ORDER)).toList();
        encoded=serialize(Math.toIntExact(bytes)); if(encoded.length>MAX_BYTES) throw new IllegalArgumentException("Activation admission exceeds bound"); digest=RangeImportAdmission.sha256(encoded);
    }
    public List<OwnerActivation> owners() { return owners; }
    public byte[] encode() { return encoded.clone(); }
    public byte[] digest() { return digest.clone(); }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public byte[] installationDigest() { return installationDigest.clone(); }
    public byte[] publicationDigest() { return publicationDigest.clone(); }
    public UUID catalogueGroup() { return catalogueGroup; }
    public void requirePublication(RangeImportDefinition definition,RangeImportAdmission installed,UUID group,byte[] publication) { if(!Arrays.equals(definitionDigest,definition.digest()) || !Arrays.equals(installationDigest,installed.digest()) || !catalogueGroup.equals(group) || !Arrays.equals(publicationDigest,publication)) throw new IllegalArgumentException("Activation publication commitment differs"); }
    public static byte[] definitionDigest(byte[] bytes) { if(bytes==null || bytes.length<120 || bytes.length>MAX_BYTES || ByteBuffer.wrap(bytes).getInt()!=0x52494143) throw new IllegalArgumentException("Invalid activation admission header"); return Arrays.copyOfRange(bytes,4,36); }
    public static byte[] installationDigest(byte[] bytes) { definitionDigest(bytes); return Arrays.copyOfRange(bytes,36,68); }
    private byte[] serialize(int size) { try { var bytes=new ByteArrayOutputStream(size); var out=new DataOutputStream(bytes); out.writeInt(0x52494143); out.write(definitionDigest); out.write(installationDigest); out.write(publicationDigest); uuid(out,catalogueGroup); out.writeInt(owners.size()); for(var entry:owners) { uuid(out,entry.groupId()); uuid(out,entry.rangeId()); out.writeLong(entry.configurationVersion()); var owner=entry.owner(); uuid(out,owner.nodeId()); uuid(out,owner.nodeIncarnation()); uuid(out,owner.replicaId()); text(out,owner.peerId()); text(out,owner.address()); out.writeByte(owner.role().ordinal()); out.write(entry.activation().encode()); out.writeLong(entry.completed().term()); out.writeLong(entry.completed().index()); } return bytes.toByteArray(); } catch(IOException impossible) { throw new AssertionError(impossible); } }
    public static RangeImportActivationAdmission decode(byte[] bytes,RangeImportDefinition definition,RangeImportAdmission installed,UUID group,byte[] publication) {
        if(bytes==null || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid activation admission bytes");
        try { var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x52494143 || !Arrays.equals(in.readNBytes(32),definition.digest()) || !Arrays.equals(in.readNBytes(32),installed.digest()) || !Arrays.equals(in.readNBytes(32),publication) || !uuid(in).equals(group)) throw new IOException("Wrong activation publication"); int count=in.readInt(); if(count<1 || count>RangeImportAdmission.MAX_OWNERS || 281L*count>in.available()) throw new IOException("Invalid activation count"); var entries=new ArrayList<OwnerActivation>(count);
            for(int i=0;i<count;i++) { UUID ownerGroup=uuid(in),range=uuid(in); long configuration=in.readLong(); UUID node=uuid(in),inc=uuid(in),replica=uuid(in); String peer=text(in,ReplicaDescriptor.MAX_PEER_BYTES),address=text(in,ReplicaDescriptor.MAX_ADDRESS_BYTES); int role=in.readUnsignedByte(); if(role>=ReplicaDescriptor.Role.values().length) throw new IOException("Invalid activation owner role"); entries.add(new OwnerActivation(ownerGroup,range,configuration,new ReplicaDescriptor(node,inc,replica,peer,address,ReplicaDescriptor.Role.values()[role]),Activation.decode(in.readNBytes(Activation.BYTES)),new ApplyPosition(in.readLong(),in.readLong()))); }
            if(in.read()!=-1) throw new IOException("Trailing activation admission bytes"); var result=new RangeImportActivationAdmission(definition,installed,group,publication,entries); if(!Arrays.equals(bytes,result.encode())) throw new IOException("Noncanonical activation admission"); return result;
        } catch(IOException|RuntimeException invalid) { throw new IllegalArgumentException("Invalid activation admission",invalid); }
    }
    private static void uuid(DataOutputStream out,UUID id)throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in)throws IOException { return new UUID(in.readLong(),in.readLong()); }
    private static void text(DataOutputStream out,String value)throws IOException { byte[] bytes=value.getBytes(StandardCharsets.US_ASCII); out.writeInt(bytes.length); out.write(bytes); }
    private static String text(DataInputStream in,int max)throws IOException { int length=in.readInt(); if(length<1 || length>max || length>in.available()) throw new IOException("Invalid activation owner text"); byte[] bytes=in.readNBytes(length); for(byte b:bytes) if(b<0)throw new IOException("Activation owner text not ASCII"); return new String(bytes,StandardCharsets.US_ASCII); }
}
