package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.security.*;
import java.util.*;

/** Immutable import authority input. Publication still requires proofs from every configured target owner. */
public final class RangeImportDefinition {
    public static final int MAX_BYTES=16*1024*1024,MAX_GROUPS=64;
    private final UUID importId;
    private final OperationId operation;
    private final RangeDirectory source,target;
    private final List<NativeSstImportManifest> manifests;
    private final byte[] encoded,digest;

    public RangeImportDefinition(UUID importId,OperationId operation,RangeDirectory source,Collection<NativeSstImportManifest> manifests) {
        this.importId=Objects.requireNonNull(importId); this.operation=Objects.requireNonNull(operation); this.source=Objects.requireNonNull(source);
        if(source.generation()==Long.MAX_VALUE || manifests.isEmpty() || manifests.size()>MAX_GROUPS) throw new IllegalArgumentException("Invalid import group count/generation");
        var byGroup=new HashMap<UUID,NativeSstImportManifest>();
        long bytes=60L+RangeDirectoryCodec.encode(source).length;
        for(var manifest:manifests) {
            bytes+=4L+manifest.encode().length; if(bytes>MAX_BYTES) throw new IllegalArgumentException("Import definition exceeds byte bound");
            if(!importId.equals(manifest.importId()) || !source.domainId().equals(manifest.domainId())
                    || !Arrays.equals(source.schemaDigest(),manifest.schemaDigest()) || !source.columns().equals(manifest.physicalNames().keySet())
                    || byGroup.put(manifest.groupId(),manifest)!=null) throw new IllegalArgumentException("Import manifest namespace/schema/group differs");
        }
        var ordered=new ArrayList<NativeSstImportManifest>(); var next=new ArrayList<RangeDescriptor>();
        for(var range:source.ranges()) {
            var manifest=byGroup.remove(range.groupId());
            if(manifest==null) { next.add(range); continue; }
            if(range.topologyState()!=RangeDescriptor.TopologyState.FENCED || range.rangeEpoch()!=manifest.rangeEpoch()
                    || !Arrays.equals(range.lowerInclusive(),manifest.lowerInclusive()) || !Arrays.equals(range.upperExclusive(),manifest.upperExclusive()))
                throw new IllegalArgumentException("Import target must be the exact new fenced interval");
            ordered.add(manifest);
            next.add(new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),
                    range.lowerInclusive(),range.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,range.replicas()));
        }
        if(!byGroup.isEmpty()) throw new IllegalArgumentException("Import group outside frozen directory");
        this.manifests=List.copyOf(ordered);
        target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),
                source.generation()+1,source.columns(),source.routingPrefixBytes(),next);
        encoded=serialize(); if(encoded.length>MAX_BYTES) throw new IllegalArgumentException("Import definition exceeds byte bound");
        try { digest=MessageDigest.getInstance("SHA-256").digest(encoded); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
    public UUID importId() { return importId; }
    public OperationId operation() { return operation; }
    public RangeDirectory sourceDirectory() { return source; }
    public RangeDirectory targetDirectory() { return target; }
    public List<NativeSstImportManifest> manifests() { return manifests; }
    public NativeSstImportManifest manifest(UUID group) { return manifests.stream().filter(value -> value.groupId().equals(group)).findFirst().orElseThrow(() -> new IllegalArgumentException("Group outside import")); }
    public byte[] encode() { return encoded.clone(); }
    public byte[] digest() { return digest.clone(); }
    public void requireReceipt(NativeSstImportManifest.Receipt receipt) { receipt.requireManifest(manifest(receipt.groupId())); }

    private byte[] serialize() {
        try {
            var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(0x52494d50); uuid(out,importId);
            uuid(out,operation.sessionId()); out.writeLong(operation.epoch()); out.writeLong(operation.sequence());
            blob(out,RangeDirectoryCodec.encode(source)); out.writeInt(manifests.size()); for(var manifest:manifests) blob(out,manifest.encode());
            return bytes.toByteArray();
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    public static RangeImportDefinition decode(byte[] bytes) {
        if(bytes==null || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid import definition size");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=0x52494d50) throw new IOException("Invalid import definition tag");
            UUID id=uuid(in); var operation=new OperationId(uuid(in),in.readLong(),in.readLong());
            var source=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES)); int count=in.readInt();
            if(count<1 || count>MAX_GROUPS || 4L*count>in.available()) throw new IOException("Invalid import manifest count");
            var manifests=new ArrayList<NativeSstImportManifest>(count); for(int i=0;i<count;i++) manifests.add(NativeSstImportManifest.decode(blob(in,NativeSstImportManifest.MAX_BYTES)));
            if(in.available()!=0) throw new IOException("Trailing import definition bytes");
            var result=new RangeImportDefinition(id,operation,source,manifests); if(!Arrays.equals(bytes,result.encode())) throw new IOException("Noncanonical import definition"); return result;
        } catch(IOException|RuntimeException malformed) { throw new IllegalArgumentException("Invalid import definition",malformed); }
    }
    private static void uuid(DataOutputStream out,UUID id) throws IOException { out.writeLong(id.getMostSignificantBits()); out.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(),in.readLong()); }
    private static void blob(DataOutputStream out,byte[] value) throws IOException { out.writeInt(value.length); out.write(value); }
    private static byte[] blob(DataInputStream in,int bound) throws IOException { int size=in.readInt(); if(size<0 || size>bound || size>in.available()) throw new IOException("Import definition component exceeds bound"); return in.readNBytes(size); }
}
