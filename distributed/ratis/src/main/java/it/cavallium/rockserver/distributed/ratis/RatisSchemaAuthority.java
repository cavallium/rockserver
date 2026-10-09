package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed catalogue client; each response is one completed cut of fixed server-owned metadata keys. */
public final class RatisSchemaAuthority {
    public static final int MAX_REQUEST_BYTES=ScopedTransactionRpc.HEADER_BYTES+16*1024;
    public static final int MAX_BYTES=MAX_REQUEST_BYTES+48+RatisCatalogue.MAX_DIRECTORY_BYTES+RatisSchemaAdministration.MAX_HEAD_BYTES+SchemaAdmissionManifest.MAX_BYTES;
    private final RaftClient client;
    private final UUID cluster,database,group;
    public record Request(UUID clusterId,UUID databaseId,UUID domainId,SchemaChange.Header expectedHeader,byte[] admissionDigest,boolean outcome) {
        public Request(UUID clusterId,UUID databaseId,UUID domainId) { this(clusterId,databaseId,domainId,null,null,false); }
        public Request {
            Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId);
            if (expectedHeader!=null && (!expectedHeader.clusterId().equals(clusterId) || !expectedHeader.databaseId().equals(databaseId) || !expectedHeader.domainId().equals(domainId))) throw new IllegalArgumentException("Expected schema definition belongs to another catalogue domain");
            if (admissionDigest!=null && (expectedHeader==null || admissionDigest.length!=32) || outcome && expectedHeader==null) throw new IllegalArgumentException("Expected immutable schema identity missing");
            admissionDigest=admissionDigest==null ? null : admissionDigest.clone();
        }
        @Override public byte[] admissionDigest() { return admissionDigest==null ? null : admissionDigest.clone(); }
        @Override public boolean equals(Object other) {
            return other instanceof Request r && clusterId.equals(r.clusterId) && databaseId.equals(r.databaseId) && domainId.equals(r.domainId) && outcome==r.outcome
                    && Arrays.equals(admissionDigest,r.admissionDigest) && Arrays.equals(headerBytes(expectedHeader),headerBytes(r.expectedHeader));
        }
        @Override public int hashCode() { return Objects.hash(clusterId,databaseId,domainId,outcome,Arrays.hashCode(admissionDigest),Arrays.hashCode(headerBytes(expectedHeader))); }
    }
    public record View(Request request,UUID catalogueGroup,ApplyPosition position,RangeDirectory directory,RatisSchemaAdministration.Head head,byte[] admissions) {
        public View(Request request,UUID catalogueGroup,ApplyPosition position,RangeDirectory directory,RatisSchemaAdministration.Head head) { this(request,catalogueGroup,position,directory,head,null); }
        public View {
            Objects.requireNonNull(request); Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(position);
            if (position.term()<1) throw new IllegalArgumentException("Schema authority requires a completed committed prefix");
            if (directory!=null && (!directory.clusterId().equals(request.clusterId()) || !directory.databaseId().equals(request.databaseId()) || !directory.domainId().equals(request.domainId()) || RangeDirectoryCodec.encodedSize(directory)>RatisCatalogue.MAX_DIRECTORY_BYTES)) throw new IllegalArgumentException("Schema authority directory identity/budget mismatch");
            if (head!=null) {
                var header=head.status().header();
                if (!header.clusterId().equals(request.clusterId()) || !header.databaseId().equals(request.databaseId()) || !header.domainId().equals(request.domainId())) throw new IllegalArgumentException("Schema authority head identity mismatch");
                if (request.outcome() && (!head.terminal() || !Arrays.equals(header.encode(),request.expectedHeader().encode()))) throw new IllegalArgumentException("Expected terminal schema outcome changed");
            }
            if (admissions!=null && (request.admissionDigest()==null || admissions.length>SchemaAdmissionManifest.MAX_BYTES || !Arrays.equals(OperatorUpload.sha256(admissions),request.admissionDigest()))) throw new IllegalArgumentException("Immutable schema admissions differ from expected commitment");
            admissions=admissions==null ? null : admissions.clone();
        }
        @Override public byte[] admissions() { return admissions==null ? null : admissions.clone(); }
        public SchemaAdmissionManifest manifest(SchemaChange.Definition definition) {
            if (admissions==null) return null; return SchemaAdmissionManifest.decode(admissions,definition);
        }
    }
    public RatisSchemaAuthority(RaftClient client,UUID cluster,UUID database,UUID catalogueGroup) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.group=Objects.requireNonNull(catalogueGroup);
        if (!client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Schema authority client group mismatch");
    }
    public View read(UUID domain) throws IOException { return read(new Request(cluster,database,domain)); }
    public View read(SchemaChange.Header header,byte[] digest,boolean outcome) throws IOException { return read(new Request(cluster,database,header.domainId(),header,digest,outcome)); }
    public View read(Request expected) throws IOException {
        if (!expected.clusterId().equals(cluster) || !expected.databaseId().equals(database)) throw new IllegalArgumentException("Schema authority client identity changed");
        var reply=RatisRoutedClient.rpc(() -> client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(request(expected)))));
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getRaftGroupId()==null || !reply.getRaftGroupId().getUuid().equals(group) || reply.getMessage().getContent().size()>MAX_BYTES) throw new IOException("Invalid schema authority reply",reply==null ? null : reply.getException());
        try { return decodeReply(reply.getMessage().getContent().toByteArray(),expected,group); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema authority reply",invalid); }
    }
    public static byte[] request(Request request) { return ScopedTransactionRpc.frame(ScopedTransactionRpc.SCHEMA_AUTHORITY,encodeRequest(request)); }
    public static Request request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);
        if (packet.kind()!=ScopedTransactionRpc.SCHEMA_AUTHORITY) throw new IllegalArgumentException("Expected schema authority request"); return decodeRequest(packet.body());
    }
    private static byte[] encodeRequest(Request request) {
        byte[] header=headerBytes(request.expectedHeader()),digest=request.admissionDigest();
        var out=ByteBuffer.allocate(57+(header==null ? 0 : header.length)+(digest==null ? 0 : digest.length));
        ScopedTransactionRpc.uuid(out,request.clusterId()); ScopedTransactionRpc.uuid(out,request.databaseId()); ScopedTransactionRpc.uuid(out,request.domainId());
        ScopedTransactionRpc.putBlob(out,header); ScopedTransactionRpc.putBlob(out,digest); out.put((byte)(request.outcome() ? 1 : 0));
        if (out.capacity()>MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Schema authority request exceeds bound"); return out.array();
    }
    private static Request decodeRequest(byte[] bytes) {
        if (bytes==null || bytes.length<57 || bytes.length>MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Invalid schema authority request size");
        var in=ByteBuffer.wrap(bytes); UUID cluster=ScopedTransactionRpc.uuid(in),database=ScopedTransactionRpc.uuid(in),domain=ScopedTransactionRpc.uuid(in);
        byte[] header=ScopedTransactionRpc.blob(in,16*1024,true),digest=ScopedTransactionRpc.blob(in,32,true);
        if (in.remaining()!=1) throw new IllegalArgumentException("Invalid schema authority request tail"); int outcome=in.get()&255; if (outcome>1) throw new IllegalArgumentException("Invalid schema authority outcome flag");
        var request=new Request(cluster,database,domain,header==null ? null : SchemaChange.Header.decode(header),digest,outcome==1);
        if (!Arrays.equals(bytes,encodeRequest(request))) throw new IllegalArgumentException("Noncanonical schema authority request"); return request;
    }
    public static byte[] reply(View view) {
        byte[] request=encodeRequest(view.request()),directory=view.directory()==null ? null : RangeDirectoryCodec.encode(view.directory()),head=view.head()==null ? null : view.head().encode(),admissions=view.admissions();
        int size=48+request.length+(directory==null ? 0 : directory.length)+(head==null ? 0 : head.length)+(admissions==null ? 0 : admissions.length);
        if (size>MAX_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Schema authority reply exceeds budget");
        var out=ByteBuffer.allocate(size); ScopedTransactionRpc.putBlob(out,request); ScopedTransactionRpc.uuid(out,view.catalogueGroup()); out.putLong(view.position().term()).putLong(view.position().index());
        ScopedTransactionRpc.putBlob(out,directory); ScopedTransactionRpc.putBlob(out,head); ScopedTransactionRpc.putBlob(out,admissions); return ScopedTransactionRpc.frame(ScopedTransactionRpc.SCHEMA_AUTHORITY,out.array());
    }
    public static View decodeReply(byte[] bytes,Request expected,UUID expectedGroup) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES); if (packet.kind()!=ScopedTransactionRpc.SCHEMA_AUTHORITY) throw new IllegalArgumentException("Expected schema authority reply");
        var in=ByteBuffer.wrap(packet.body()); var request=decodeRequest(ScopedTransactionRpc.blob(in,MAX_REQUEST_BYTES-ScopedTransactionRpc.HEADER_BYTES,false));
        if (in.remaining()<44) throw new IllegalArgumentException("Truncated schema authority reply"); var group=ScopedTransactionRpc.uuid(in);
        if (!request.equals(expected) || !group.equals(expectedGroup)) throw new IllegalArgumentException("Schema authority reply identity changed"); var position=new ApplyPosition(in.getLong(),in.getLong());
        byte[] directory=ScopedTransactionRpc.blob(in,RatisCatalogue.MAX_DIRECTORY_BYTES,true),head=ScopedTransactionRpc.blob(in,RatisSchemaAdministration.MAX_HEAD_BYTES,true),admissions=ScopedTransactionRpc.blob(in,SchemaAdmissionManifest.MAX_BYTES,true);
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing schema authority reply"); return new View(request,group,position,directory==null ? null : RangeDirectoryCodec.decode(directory),head==null ? null : RatisSchemaAdministration.Head.decode(head),admissions);
    }
    private static byte[] headerBytes(SchemaChange.Header header) { return header==null ? null : header.encode(); }
}
