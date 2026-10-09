package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/** Frozen, path-free upload identity. Admission is an external trusted decision, never a client flag. */
public final class OperatorUpload {
    public static final int CHUNK_BYTES=1024*1024, MAX_ARTIFACT_BYTES=8*CHUNK_BYTES, MAX_BYTES=16*1024;
    private static final int MAGIC=0x4f505550;
    public record Chunk(int length,byte[] sha256) {
        public Chunk {
            if (length<1 || length>CHUNK_BYTES || sha256.length!=32) throw new IllegalArgumentException("Invalid operator chunk");
            sha256=sha256.clone();
        }
        @Override public byte[] sha256() { return sha256.clone(); }
        @Override public boolean equals(Object other) { return other instanceof Chunk chunk && length==chunk.length && Arrays.equals(sha256,chunk.sha256); }
        @Override public int hashCode() { return 31*length+Arrays.hashCode(sha256); }
    }
    private final UUID uploadId,clusterId,databaseId;
    private final DistributedSchema.Operator operator;
    private final int artifactLength;
    private final List<Chunk> chunks;
    public OperatorUpload(UUID uploadId,UUID clusterId,UUID databaseId,DistributedSchema.Operator operator,int artifactLength,List<Chunk> chunks) {
        this.uploadId=Objects.requireNonNull(uploadId); this.clusterId=Objects.requireNonNull(clusterId); this.databaseId=Objects.requireNonNull(databaseId);
        this.operator=Objects.requireNonNull(operator); this.artifactLength=artifactLength; this.chunks=List.copyOf(chunks);
        if (artifactLength<1 || artifactLength>MAX_ARTIFACT_BYTES || chunks.size()!=(artifactLength+CHUNK_BYTES-1)/CHUNK_BYTES) throw new IllegalArgumentException("Invalid operator artifact bounds");
        for (int i=0;i<chunks.size();i++) if(chunks.get(i).length()!=Math.min(CHUNK_BYTES,artifactLength-i*CHUNK_BYTES)) throw new IllegalArgumentException("Invalid operator chunk layout");
        if (encode().length>MAX_BYTES) throw new IllegalArgumentException("Operator manifest exceeds budget");
    }
    public static OperatorUpload create(UUID uploadId,UUID clusterId,UUID databaseId,DistributedSchema.Operator operator,byte[] artifact) {
        if (artifact.length<1 || artifact.length>MAX_ARTIFACT_BYTES || !Arrays.equals(sha256(artifact),operator.artifactSha256())) throw new IllegalArgumentException("Operator artifact SHA or length mismatch");
        var chunks=new ArrayList<Chunk>();
        for (int i=0;i<artifact.length;i+=CHUNK_BYTES) { byte[] bytes=Arrays.copyOfRange(artifact,i,Math.min(artifact.length,i+CHUNK_BYTES)); chunks.add(new Chunk(bytes.length,sha256(bytes))); }
        return new OperatorUpload(uploadId,clusterId,databaseId,operator,artifact.length,chunks);
    }
    public UUID uploadId() { return uploadId; }
    public UUID clusterId() { return clusterId; }
    public UUID databaseId() { return databaseId; }
    public DistributedSchema.Operator operator() { return operator; }
    public int artifactLength() { return artifactLength; }
    public List<Chunk> chunks() { return chunks; }
    public byte[] digest() { return sha256(encode()); }
    public void requireChunk(int index,byte[] bytes) {
        if(index<0 || index>=chunks.size() || bytes.length!=chunks.get(index).length() || !Arrays.equals(sha256(bytes),chunks.get(index).sha256())) throw new IllegalArgumentException("Operator chunk content differs from manifest");
    }
    public void requireArtifact(List<byte[]> bytes) {
        if(bytes.size()!=chunks.size()) throw new IllegalArgumentException("Incomplete operator artifact");
        var digest=hash(); for(int i=0;i<bytes.size();i++) { requireChunk(i,bytes.get(i)); digest.update(bytes.get(i)); }
        if(!Arrays.equals(digest.digest(),operator.artifactSha256())) throw new IllegalArgumentException("Operator artifact SHA mismatch");
    }
    public byte[] encode() {
        try {
            var buffer=new ByteArrayOutputStream(); var out=new DataOutputStream(buffer); out.writeInt(MAGIC);
            uuid(out,uploadId); uuid(out,clusterId); uuid(out,databaseId); writeOperator(out,operator);
            out.writeInt(artifactLength); out.writeInt(chunks.size());
            for(var chunk:chunks) { out.writeInt(chunk.length()); out.write(chunk.sha256()); }
            return buffer.toByteArray();
        } catch(IOException impossible) { throw new AssertionError(impossible); }
    }
    public static OperatorUpload decode(byte[] bytes) {
        if(bytes==null || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Operator manifest exceeds budget");
        try {
            var in=new DataInputStream(new ByteArrayInputStream(bytes)); if(in.readInt()!=MAGIC) throw new IllegalArgumentException("Unknown operator upload");
            UUID upload=uuid(in),cluster=uuid(in),database=uuid(in); var operator=readOperator(in); int length=in.readInt(),count=in.readInt();
            if(count<1 || count>8) throw new IllegalArgumentException("Invalid operator chunk count");
            var chunks=new ArrayList<Chunk>(); for(int i=0;i<count;i++) chunks.add(new Chunk(in.readInt(),in.readNBytes(32)));
            var result=new OperatorUpload(upload,cluster,database,operator,length,chunks);
            if(in.available()!=0 || !Arrays.equals(result.encode(),bytes)) throw new IllegalArgumentException("Noncanonical operator upload"); return result;
        } catch(IOException invalid) { throw new IllegalArgumentException("Truncated operator upload",invalid); }
    }
    private static void writeOperator(DataOutputStream out,DistributedSchema.Operator operator) throws IOException {
        text(out,operator.name()); out.writeLong(operator.version()); text(out,operator.className()); out.write(operator.artifactSha256());
        out.writeInt(operator.valueCodecVersion()); out.writeInt(operator.operandCodecVersion()); text(out,operator.deterministicContract()); out.writeInt(operator.deterministicContractVersion());
    }
    private static DistributedSchema.Operator readOperator(DataInputStream in) throws IOException {
        return new DistributedSchema.Operator(text(in),in.readLong(),text(in),in.readNBytes(32),in.readInt(),in.readInt(),text(in),in.readInt());
    }
    private static void text(DataOutputStream out,String text) throws IOException { byte[] bytes=DistributedSchemaCodec.text(text); out.writeInt(bytes.length); out.write(bytes); }
    private static String text(DataInputStream in) throws IOException {
        int length=in.readInt(); if(length<1 || length>DistributedSchemaCodec.MAX_NAME_BYTES || length>in.available()) throw new IllegalArgumentException("Invalid operator text");
        byte[] bytes=in.readNBytes(length); String text=new String(bytes,StandardCharsets.UTF_8);
        if(!Arrays.equals(bytes,DistributedSchemaCodec.text(text))) throw new IllegalArgumentException("Noncanonical operator text"); return text;
    }
    private static void uuid(DataOutputStream out,UUID uuid) throws IOException { out.writeLong(uuid.getMostSignificantBits()); out.writeLong(uuid.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream in) throws IOException { return new UUID(in.readLong(),in.readLong()); }
    public static byte[] sha256(byte[] bytes) { return hash().digest(bytes); }
    private static MessageDigest hash() { try { return MessageDigest.getInstance("SHA-256"); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
}
