package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Exact original schema inputs and physical replica identity; no leader routing or policy authority on wire. */
public final class RatisSchemaReplicaRpc {
    public static final int MAX_BYTES=4*1024*1024, MAX_TARGET_BYTES=SchemaChange.MAX_DEFINITION_BYTES+8192;
    private RatisSchemaReplicaRpc() {}
    public record Binding(SchemaChange.Definition definition,UUID groupId,String changedPhysicalName,boolean mergeAdmission) {
        public Binding {
            Objects.requireNonNull(definition); Objects.requireNonNull(groupId);
            if (definition.sourceDirectory().ranges().stream().noneMatch(range -> range.groupId().equals(groupId))) throw new IllegalArgumentException("Schema group outside original source");
            if (definition.header().kind()==SchemaChange.Kind.CREATE_COLUMN) {
                if (changedPhysicalName==null || changedPhysicalName.isEmpty() || changedPhysicalName.length()>4096 || changedPhysicalName.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(changedPhysicalName)) throw new IllegalArgumentException("Invalid changed physical binding");
            } else if (changedPhysicalName!=null || mergeAdmission) throw new IllegalArgumentException("Deletion carries no changed binding");
        }
        public SchemaChange.Header header() { return definition.header(); }
        public RangeDescriptor source() { return definition.sourceDirectory().ranges().stream().filter(range -> range.groupId().equals(groupId)).findFirst().orElseThrow(); }
        @Override public boolean equals(Object other) { return other instanceof Binding b && groupId.equals(b.groupId) && Objects.equals(changedPhysicalName,b.changedPhysicalName) && mergeAdmission==b.mergeAdmission && Arrays.equals(definition.encode(),b.definition.encode()); }
        @Override public int hashCode() { return Objects.hash(groupId,changedPhysicalName,mergeAdmission,Arrays.hashCode(definition.encode())); }
    }
    public record Target(Binding binding,ReplicaDescriptor replica) {
        public Target { Objects.requireNonNull(binding); Objects.requireNonNull(replica); if (!binding.source().replicas().contains(replica)) throw new IllegalArgumentException("Schema replica differs from original membership"); }
        public byte[] encode() {
            byte[] definition=binding.definition().encode(),name=binding.changedPhysicalName()==null ? null : binding.changedPhysicalName().getBytes(StandardCharsets.US_ASCII),peer=replica.peerId().getBytes(StandardCharsets.US_ASCII),address=replica.address().getBytes(StandardCharsets.US_ASCII);
            var out=ByteBuffer.allocate(82+definition.length+(name==null ? 0 : name.length)+peer.length+address.length);
            ScopedTransactionRpc.putBlob(out,definition); ScopedTransactionRpc.uuid(out,binding.groupId()); ScopedTransactionRpc.putBlob(out,name); out.put((byte)(binding.mergeAdmission() ? 1 : 0));
            ScopedTransactionRpc.uuid(out,replica.nodeId()); ScopedTransactionRpc.uuid(out,replica.nodeIncarnation()); ScopedTransactionRpc.uuid(out,replica.replicaId());
            ScopedTransactionRpc.putBlob(out,peer); ScopedTransactionRpc.putBlob(out,address); out.put((byte)replica.role().ordinal()); return out.array();
        }
        public static Target decode(byte[] bytes) {
            if (bytes==null || bytes.length>MAX_TARGET_BYTES || bytes.length<82) throw new IllegalArgumentException("Invalid schema target size");
            var in=ByteBuffer.wrap(bytes); var definition=SchemaChange.Definition.decode(ScopedTransactionRpc.blob(in,SchemaChange.MAX_DEFINITION_BYTES,false));
            UUID group=ScopedTransactionRpc.uuid(in); byte[] name=ScopedTransactionRpc.blob(in,4096,true); boolean merge=flag(in);
            UUID node=ScopedTransactionRpc.uuid(in),incarnation=ScopedTransactionRpc.uuid(in),replica=ScopedTransactionRpc.uuid(in);
            String peer=text(in,128),address=text(in,256); if (in.remaining()!=1) throw new IllegalArgumentException("Invalid schema target tail"); int role=in.get() & 255;
            if (role>=ReplicaDescriptor.Role.values().length) throw new IllegalArgumentException("Invalid replica role");
            var result=new Target(new Binding(definition,group,name==null ? null : new String(name,StandardCharsets.US_ASCII),merge),new ReplicaDescriptor(node,incarnation,replica,peer,address,ReplicaDescriptor.Role.values()[role]));
            if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical schema target"); return result;
        }
    }
    public record Request(int kind,Target target,OperatorUpload upload,int chunkIndex,byte[] chunk) {
        public Request {
            Objects.requireNonNull(target); if (!ScopedTransactionRpc.schemaReplica(kind)) throw new IllegalArgumentException("Invalid schema replica operation");
            if (kind<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE) {
                if (upload==null) throw new IllegalArgumentException("Artifact request lacks manifest");
                var header=target.binding().header(); var schema=target.binding().definition().targetSchema();
                if (!upload.clusterId().equals(header.clusterId()) || !upload.databaseId().equals(header.databaseId()) || header.kind()!=SchemaChange.Kind.CREATE_COLUMN
                        || !upload.operator().equals(schema.column(header.changedColumnId()).operator())) throw new IllegalArgumentException("Artifact differs from schema operator");
                if (kind==ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK) { Objects.requireNonNull(chunk); upload.requireChunk(chunkIndex,chunk); chunk=chunk.clone(); }
                else if (chunkIndex!=-1 || chunk!=null) throw new IllegalArgumentException("Unexpected artifact chunk");
            } else if (upload!=null || chunkIndex!=-1 || chunk!=null) throw new IllegalArgumentException("Capacity operation carries no artifact");
        }
        @Override public byte[] chunk() { return chunk==null ? null : chunk.clone(); }
        public byte[] encode() { return request(this); }
        public byte[] digest() { return OperatorUpload.sha256(encode()); }
        @Override public boolean equals(Object other) { return other instanceof Request r && kind==r.kind && target.equals(r.target) && Arrays.equals(encode(),r.encode()); }
        @Override public int hashCode() { return Arrays.hashCode(encode()); }
    }
    public enum CapacityState { NONE, ACTIVE, FENCED, CONSUMED, ABORTED, RETIRED }
    public record Reply(Target target,int kind,byte[] requestDigest,BitSet chunks,boolean artifactReady,NativeSchemaCapacityReservation capacity,CapacityState capacityState,boolean forcedDurable) {
        public Reply {
            Objects.requireNonNull(target); Objects.requireNonNull(chunks); Objects.requireNonNull(capacityState);
            if (!ScopedTransactionRpc.schemaReplica(kind) || requestDigest==null || requestDigest.length!=32 || chunks.length()>8) throw new IllegalArgumentException("Invalid replica reply metadata");
            requestDigest=requestDigest.clone(); chunks=(BitSet)chunks.clone();
            if (capacity==null && forcedDurable) throw new IllegalArgumentException("Durable capacity lacks original proof");
            if ((capacity==null)!=(capacityState==CapacityState.NONE)) throw new IllegalArgumentException("Capacity state lacks original proof");
            if (capacity!=null) { capacity.requireDefinition(target.binding().definition()); if (!capacity.participant().groupId().equals(target.binding().groupId())) throw new IllegalArgumentException("Crossed capacity proof group"); }
            if (kind>=ScopedTransactionRpc.SCHEMA_CAPACITY_RESERVE && (artifactReady || !chunks.isEmpty())) throw new IllegalArgumentException("Capacity reply carries artifact readiness");
            if (kind<=ScopedTransactionRpc.SCHEMA_ARTIFACT_STATE && capacity!=null) throw new IllegalArgumentException("Artifact reply carries capacity proof");
        }
        @Override public byte[] requestDigest() { return requestDigest.clone(); }
        @Override public BitSet chunks() { return (BitSet)chunks.clone(); }
        public boolean capacityActive() { return capacityState==CapacityState.ACTIVE && forcedDurable; }
    }
    public static byte[] request(Request request) {
        byte[] target=request.target().encode(),manifest=request.upload()==null ? null : request.upload().encode(),chunk=request.chunk();
        var out=ByteBuffer.allocate(16+target.length+(manifest==null ? 0 : manifest.length)+(chunk==null ? 0 : chunk.length));
        ScopedTransactionRpc.putBlob(out,target); ScopedTransactionRpc.putBlob(out,manifest); out.putInt(request.chunkIndex()); ScopedTransactionRpc.putBlob(out,chunk); return frame(request.kind(),out.array());
    }
    public static Request request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); if (!ScopedTransactionRpc.schemaReplica(packet.kind())) throw new IllegalArgumentException("Expected schema replica request");
        var in=ByteBuffer.wrap(packet.body()); var target=Target.decode(ScopedTransactionRpc.blob(in,MAX_TARGET_BYTES,false)); byte[] manifest=ScopedTransactionRpc.blob(in,OperatorUpload.MAX_BYTES,true);
        if (in.remaining()<4) throw new IllegalArgumentException("Truncated chunk index"); int index=in.getInt(); byte[] chunk=ScopedTransactionRpc.blob(in,OperatorUpload.CHUNK_BYTES,true);
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing schema replica request"); return new Request(packet.kind(),target,manifest==null ? null : OperatorUpload.decode(manifest),index,chunk);
    }
    public static byte[] reply(Request request,BitSet chunks,boolean artifactReady,NativeSchemaCapacityReservation capacity,CapacityState capacityState,boolean forcedDurable) {
        return reply(new Reply(request.target(),request.kind(),request.digest(),chunks,artifactReady,capacity,capacityState,forcedDurable));
    }
    public static byte[] reply(Reply reply) {
        byte[] target=reply.target().encode(),proof=reply.capacity()==null ? null : reply.capacity().encode();
        var out=ByteBuffer.allocate(44+target.length+(proof==null ? 0 : proof.length)); ScopedTransactionRpc.putBlob(out,target); out.put(reply.requestDigest());
        int bits=0; for (int i=0;i<8;i++) if (reply.chunks().get(i)) bits|=1<<i; out.put((byte)bits).put((byte)(reply.artifactReady() ? 1 : 0));
        ScopedTransactionRpc.putBlob(out,proof); out.put((byte)reply.capacityState().ordinal()).put((byte)(reply.forcedDurable() ? 1 : 0)); return frame(reply.kind(),out.array());
    }
    public static Reply decodeReply(byte[] bytes,Request expected) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); if (packet.kind()!=expected.kind()) throw new IllegalArgumentException("Crossed schema replica reply operation");
        var in=ByteBuffer.wrap(packet.body()); var target=Target.decode(ScopedTransactionRpc.blob(in,MAX_TARGET_BYTES,false));
        if (!target.equals(expected.target()) || in.remaining()<34) throw new IllegalArgumentException("Crossed schema replica reply identity"); byte[] digest=new byte[32]; in.get(digest);
        if (!Arrays.equals(digest,expected.digest())) throw new IllegalArgumentException("Crossed schema request/chunk digest");
        int bits=in.get() & 255; boolean ready=flag(in); byte[] proof=ScopedTransactionRpc.blob(in,NativeSchemaCapacityReservation.BYTES,true);
        if (in.remaining()!=2) throw new IllegalArgumentException("Invalid replica reply tail"); int state=in.get() & 255;
        if (state>=CapacityState.values().length) throw new IllegalArgumentException("Invalid capacity state");
        BitSet chunks=BitSet.valueOf(new byte[]{(byte)bits});
        if (expected.upload()!=null && (chunks.length()>expected.upload().chunks().size() || ready && chunks.cardinality()!=expected.upload().chunks().size())) throw new IllegalArgumentException("Artifact readiness contradicts chunk coverage");
        return new Reply(target,packet.kind(),digest,chunks,ready,proof==null ? null : NativeSchemaCapacityReservation.decode(proof),CapacityState.values()[state],flag(in));
    }
    private static byte[] frame(int kind,byte[] body) { if ((long)body.length+ScopedTransactionRpc.HEADER_BYTES>MAX_BYTES) throw new IllegalArgumentException("Schema replica frame exceeds bound"); return ScopedTransactionRpc.frame(kind,body); }
    private static String text(ByteBuffer in,int maximum) { byte[] bytes=ScopedTransactionRpc.blob(in,maximum,false); String text=new String(bytes,StandardCharsets.US_ASCII); if (!Arrays.equals(bytes,text.getBytes(StandardCharsets.US_ASCII))) throw new IllegalArgumentException("Noncanonical ASCII text"); return text; }
    private static boolean flag(ByteBuffer in) { if (!in.hasRemaining()) throw new IllegalArgumentException("Truncated boolean"); int value=in.get() & 255; if (value>1) throw new IllegalArgumentException("Invalid boolean"); return value==1; }
}
