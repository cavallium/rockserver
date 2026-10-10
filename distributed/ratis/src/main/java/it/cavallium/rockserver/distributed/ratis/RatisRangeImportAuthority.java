package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/** Published import and routing image from one completed native catalogue cut. */
public final class RatisRangeImportAuthority {
    public static final int KIND=76,REQUEST_BYTES=96,MAX_REPLY_BYTES=RangeDirectoryCodec.MAX_ENCODED_BYTES+512;
    private RatisRangeImportAuthority() {}
    public static final class Client implements RatisRangeImportApply.PublicationReader {
        private final org.apache.ratis.client.RaftClient client;
        private final UUID cluster,database,catalogue;
        public Client(org.apache.ratis.client.RaftClient borrowed,UUID cluster,UUID database,UUID catalogue) {
            client=Objects.requireNonNull(borrowed); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.catalogue=Objects.requireNonNull(catalogue);
            if(!client.getGroupId().getUuid().equals(catalogue)) throw new IllegalArgumentException("Import authority client group differs");
        }
        @Override public RatisRangeImportApply.PublicationView read(RangeImportDefinition definition) throws IOException {
            var request=Request.of(definition); if(!cluster.equals(request.cluster()) || !database.equals(request.database())) throw new IOException("Import authority client namespace differs");
            var response=RatisRoutedClient.rpc(() -> client.io().sendReadOnly(org.apache.ratis.protocol.Message.valueOf(org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(request(request)))));
            if(response==null || !response.isSuccess() || response.getMessage()==null || !response.getRaftGroupId().getUuid().equals(catalogue) || response.getMessage().getContent().size()>MAX_REPLY_BYTES) throw new IOException("Import authority reply failed or differs from catalogue",response==null ? null : response.getException());
            try { return decodeReply(response.getMessage().getContent().toByteArray(),request,catalogue); } catch(IllegalArgumentException malformed) { throw new IOException("Invalid import authority reply",malformed); }
        }
    }
    public record Request(UUID cluster,UUID database,UUID domain,UUID importId,byte[] definitionDigest) {
        public Request { Objects.requireNonNull(cluster); Objects.requireNonNull(database); Objects.requireNonNull(domain); Objects.requireNonNull(importId); if(definitionDigest.length!=32) throw new IllegalArgumentException("Invalid import authority digest"); definitionDigest=definitionDigest.clone(); }
        @Override public byte[] definitionDigest() { return definitionDigest.clone(); }
        public static Request of(RangeImportDefinition definition) { var d=definition.sourceDirectory(); return new Request(d.clusterId(),d.databaseId(),d.domainId(),definition.importId(),definition.digest()); }
    }
    public static byte[] request(Request request) { var out=ByteBuffer.allocate(REQUEST_BYTES); uuid(out,request.cluster()); uuid(out,request.database()); uuid(out,request.domain()); uuid(out,request.importId()); out.put(request.definitionDigest()); return ScopedTransactionRpc.frame(KIND,out.array()); }
    public static Request decodeRequest(byte[] encoded) {
        var packet=ScopedTransactionRpc.parse(encoded,REQUEST_BYTES+ScopedTransactionRpc.HEADER_BYTES); if(packet.kind()!=KIND || packet.body().length!=REQUEST_BYTES) throw new IllegalArgumentException("Invalid import authority request");
        var in=ByteBuffer.wrap(packet.body()); UUID cluster=uuid(in),database=uuid(in),domain=uuid(in),id=uuid(in); byte[] digest=new byte[32]; in.get(digest); return new Request(cluster,database,domain,id,digest);
    }
    /** Caller provides its configured namespace, column and a fresh read barrier; request bytes select neither connector nor CF. */
    public static RatisRangeImportApply.PublicationView readNative(ReplicatedRangeStore store,UUID column,UUID cluster,UUID database,Request request) throws IOException {
        if(!cluster.equals(request.cluster()) || !database.equals(request.database())) throw new IOException("Import authority namespace differs");
        try {
            byte[] domain=ByteBuffer.allocate(16).putLong(request.domain().getMostSignificantBits()).putLong(request.domain().getLeastSignificantBits()).array();
            var read=store.readSchemaAuthorityPoints(List.of(new ReplicatedRangeStore.PhysicalPoint(column,RatisRangeImportCatalogue.importKey(request.importId())),new ReplicatedRangeStore.PhysicalPoint(column,domain)));
            var values=read.values(); if(read.position()==null || values.get(0)==null || values.get(1)==null) throw new IOException("Import publication authority missing");
            var head=RatisRangeImportCatalogue.Head.decode(values.get(0)); var directory=RangeDirectoryCodec.decode(values.get(1));
            require(request,head,directory);
            return new RatisRangeImportApply.PublicationView(new RatisRangeImportRecord.Publication(store.ownership().owner().groupId(),read.position(),head),directory);
        } catch(org.rocksdb.RocksDBException|IllegalArgumentException invalid) { throw new IOException("Invalid native import publication authority",invalid); }
    }
    public static byte[] reply(RatisRangeImportApply.PublicationView view) {
        byte[] publication=view.publication().encode(),directory=RangeDirectoryCodec.encode(view.directory());
        var out=ByteBuffer.allocate(8+publication.length+directory.length).putInt(publication.length).put(publication).putInt(directory.length).put(directory); return ScopedTransactionRpc.frame(KIND,out.array());
    }
    public static RatisRangeImportApply.PublicationView decodeReply(byte[] encoded,Request request,UUID expectedCatalogue) {
        var packet=ScopedTransactionRpc.parse(encoded,MAX_REPLY_BYTES); if(packet.kind()!=KIND) throw new IllegalArgumentException("Wrong import authority reply");
        var in=ByteBuffer.wrap(packet.body()); byte[] publication=blob(in,512),directory=blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES); if(in.hasRemaining()) throw new IllegalArgumentException("Trailing import authority reply");
        var p=ByteBuffer.wrap(publication); UUID group=uuid(p); var cut=new ApplyPosition(p.getLong(),p.getLong()); byte[] head=blob(p,RatisRangeImportCatalogue.MAX_HEAD_BYTES); if(p.hasRemaining() || !group.equals(expectedCatalogue)) throw new IllegalArgumentException("Import authority catalogue differs");
        var h=RatisRangeImportCatalogue.Head.decode(head); var d=RangeDirectoryCodec.decode(directory); require(request,h,d); return new RatisRangeImportApply.PublicationView(new RatisRangeImportRecord.Publication(group,cut,h),d);
    }
    private static void require(Request request,RatisRangeImportCatalogue.Head head,RangeDirectory directory) {
        if(head.phase()!=RatisRangeImportCatalogue.Phase.PUBLISHED || !head.clusterId().equals(request.cluster()) || !head.databaseId().equals(request.database()) || !head.domainId().equals(request.domain()) || !head.importId().equals(request.importId()) || !Arrays.equals(head.definitionDigest(),request.definitionDigest()) || !directory.clusterId().equals(request.cluster()) || !directory.databaseId().equals(request.database()) || !directory.domainId().equals(request.domain()) || !Arrays.equals(directory.payloadDigest(),head.targetDigest())) throw new IllegalArgumentException("Import authority is not exact published target");
    }
    private static byte[] blob(ByteBuffer in,int limit) { if(in.remaining()<4) throw new IllegalArgumentException("Truncated import authority blob"); int n=in.getInt(); if(n<0 || n>limit || n>in.remaining()) throw new IllegalArgumentException("Import authority blob exceeds bound"); byte[] bytes=new byte[n]; in.get(bytes); return bytes; }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { if(in.remaining()<16) throw new IllegalArgumentException("Truncated import authority identity"); return new UUID(in.getLong(),in.getLong()); }
}
