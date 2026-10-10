package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

/** Complete structural import evidence. Only a configured fresh owner reader can authorize publication. */
public final class RangeImportAdmission {
    public static final int MAX_BYTES=1024*1024,MAX_OWNERS=RangeImportDefinition.MAX_GROUPS*RangeDirectoryCodec.MAX_REPLICAS;
    public record OwnerReceipt(UUID groupId,UUID rangeId,long configurationVersion,ReplicaDescriptor owner,
                               NativeSstImportManifest.Receipt receipt,ApplyPosition completed) {
        public OwnerReceipt {
            Objects.requireNonNull(groupId); Objects.requireNonNull(rangeId); Objects.requireNonNull(owner); Objects.requireNonNull(receipt); Objects.requireNonNull(completed);
            if(configurationVersion<0 || !groupId.equals(receipt.groupId()) || receipt.position().term()<1 || completed.term()<receipt.position().term() || completed.index()<receipt.position().index()) throw new IllegalArgumentException("Import evidence requires a completed configured cut covering the receipt");
            receipt.requireCut(completed);
        }
    }
    private record OwnerKey(UUID group,UUID replica) {}
    private final UUID importId;
    private final byte[] definitionDigest,encoded,digest;
    private final List<OwnerReceipt> owners;
    private final Map<OwnerKey,OwnerReceipt> byOwner;
    public RangeImportAdmission(RangeImportDefinition definition,Collection<OwnerReceipt> entries) {
        Objects.requireNonNull(definition); Objects.requireNonNull(entries); importId=definition.importId(); definitionDigest=definition.digest();
        if(entries.isEmpty() || entries.size()>MAX_OWNERS) throw new IllegalArgumentException("Invalid import owner count");
        long encodedBytes=56;
        for(var entry:entries) { Objects.requireNonNull(entry); encodedBytes=Math.addExact(encodedBytes,201L+entry.owner().peerId().length()+entry.owner().address().length()); if(encodedBytes>MAX_BYTES) throw new IllegalArgumentException("Import admission exceeds byte bound"); }
        var expected=new HashMap<UUID,RangeDescriptor>(); int count=0;
        for(var manifest:definition.manifests()) { var range=definition.sourceDirectory().ranges().stream().filter(r -> r.groupId().equals(manifest.groupId())).findFirst().orElseThrow(); expected.put(range.groupId(),range); count+=range.replicas().size(); }
        if(entries.size()!=count) throw new IllegalArgumentException("Every imported voter/listener requires a receipt");
        var indexed=new HashMap<OwnerKey,OwnerReceipt>(); var installed=new HashMap<UUID,byte[]>();
        for(var entry:entries) {
            var range=expected.get(entry.groupId());
            if(range==null || !range.rangeId().equals(entry.rangeId()) || range.configurationVersion()!=entry.configurationVersion() || !range.replicas().contains(entry.owner())) throw new IllegalArgumentException("Import evidence differs from frozen owner/lineage/configuration");
            definition.requireReceipt(entry.receipt());
            byte[] receipt=entry.receipt().encode(),prior=installed.putIfAbsent(entry.groupId(),receipt);
            if(prior!=null && !Arrays.equals(prior,receipt)) throw new IllegalArgumentException("Owners disagree on the original installed group receipt");
            if(indexed.put(new OwnerKey(entry.groupId(),entry.owner().replicaId()),entry)!=null) throw new IllegalArgumentException("Duplicate imported owner receipt");
        }
        for(var range:expected.values()) for(var owner:range.replicas()) if(!indexed.containsKey(new OwnerKey(range.groupId(),owner.replicaId()))) throw new IllegalArgumentException("Missing imported owner");
        owners=indexed.values().stream().sorted(Comparator.comparing(OwnerReceipt::groupId,RangeDirectoryCodec.UUID_ORDER).thenComparing(e -> e.owner().replicaId(),RangeDirectoryCodec.UUID_ORDER)).toList(); byOwner=Map.copyOf(indexed);
        encoded=serialize(Math.toIntExact(encodedBytes)); if(encoded.length>MAX_BYTES) throw new IllegalArgumentException("Import admission exceeds byte bound"); digest=sha256(encoded);
    }
    public UUID importId() { return importId; }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public List<OwnerReceipt> owners() { return owners; }
    public OwnerReceipt owner(UUID group,UUID replica) { var value=byOwner.get(new OwnerKey(group,replica)); if(value==null) throw new IllegalArgumentException("Owner outside import admission"); return value; }
    public byte[] encode() { return encoded.clone(); }
    public byte[] digest() { return digest.clone(); }
    public void requireDefinition(RangeImportDefinition definition) { if(!importId.equals(definition.importId()) || !Arrays.equals(definitionDigest,definition.digest())) throw new IllegalArgumentException("Import admission definition changed"); }
    private byte[] serialize(int bytesCount) {
        try { var bytes=new ByteArrayOutputStream(bytesCount); var out=new DataOutputStream(bytes); out.writeInt(0x52494144); uuid(out,importId); out.write(definitionDigest); out.writeInt(owners.size());
            for(var entry:owners) { uuid(out,entry.groupId()); uuid(out,entry.rangeId()); out.writeLong(entry.configurationVersion()); var owner=entry.owner(); uuid(out,owner.nodeId()); uuid(out,owner.nodeIncarnation()); uuid(out,owner.replicaId()); text(out,owner.peerId()); text(out,owner.address()); out.writeByte(owner.role().ordinal()); out.write(entry.receipt().encode()); out.writeLong(entry.completed().term()); out.writeLong(entry.completed().index()); }
            return bytes.toByteArray();
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    /** Extract the commitment needed to load the immutable definition before full canonical decoding. */
    public static byte[] definitionDigest(byte[] bytes) {
        if(bytes==null || bytes.length<56 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid import admission header");
        var in=java.nio.ByteBuffer.wrap(bytes); if(in.getInt()!=0x52494144) throw new IllegalArgumentException("Invalid import admission tag"); in.position(20); byte[] digest=new byte[32]; in.get(digest); return digest;
    }
    public static RangeImportAdmission decode(byte[] bytes,RangeImportDefinition definition) {
        if(bytes==null || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid import admission bytes");
        try { var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x52494144) throw new IOException("Invalid import admission tag"); UUID id=uuid(in); byte[] digest=in.readNBytes(32); if(!id.equals(definition.importId()) || !Arrays.equals(digest,definition.digest())) throw new IOException("Import definition differs"); int count=in.readInt(); if(count<1 || count>MAX_OWNERS || 201L*count>in.available()) throw new IOException("Invalid import admission count"); var entries=new ArrayList<OwnerReceipt>(count);
            for(int i=0;i<count;i++) { UUID group=uuid(in),range=uuid(in); long configuration=in.readLong(); UUID node=uuid(in),incarnation=uuid(in),replica=uuid(in); String peer=text(in,ReplicaDescriptor.MAX_PEER_BYTES),address=text(in,ReplicaDescriptor.MAX_ADDRESS_BYTES); int role=in.readUnsignedByte(); if(role>=ReplicaDescriptor.Role.values().length) throw new IOException("Invalid import owner role"); var owner=new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]); var receipt=NativeSstImportManifest.Receipt.decode(in.readNBytes(NativeSstImportManifest.Receipt.BYTES)); entries.add(new OwnerReceipt(group,range,configuration,owner,receipt,new ApplyPosition(in.readLong(),in.readLong()))); }
            if(in.read()!=-1) throw new IOException("Trailing import admission bytes"); var result=new RangeImportAdmission(definition,entries); if(!Arrays.equals(bytes,result.encode())) throw new IOException("Noncanonical import admission"); return result;
        } catch(IOException|RuntimeException invalid) { throw new IllegalArgumentException("Invalid import admission",invalid); }
    }
    private static void uuid(DataOutputStream out,UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(),in.readLong()); }
    private static void text(DataOutputStream out,String value) throws IOException { byte[] bytes=value.getBytes(StandardCharsets.US_ASCII); out.writeInt(bytes.length); out.write(bytes); }
    private static String text(DataInputStream in,int maximum) throws IOException { int size=in.readInt(); if(size<1 || size>maximum || size>in.available()) throw new IOException("Invalid import owner text"); byte[] bytes=in.readNBytes(size); for(byte b:bytes) if(b<0) throw new IOException("Import owner text is not ASCII"); return new String(bytes,StandardCharsets.US_ASCII); }
    public static byte[] sha256(byte[] bytes) { try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
