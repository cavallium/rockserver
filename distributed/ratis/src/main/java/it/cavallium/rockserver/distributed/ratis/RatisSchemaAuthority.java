package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed catalogue client and current bounded same-cut authority query. Column binding belongs to the server. */
public final class RatisSchemaAuthority {
    public static final int MAX_BYTES=ScopedTransactionRpc.HEADER_BYTES+88+RatisCatalogue.MAX_DIRECTORY_BYTES+RatisSchemaAdministration.MAX_HEAD_BYTES;
    private final RaftClient client;
    private final UUID cluster,database,group;
    public record Request(UUID clusterId,UUID databaseId,UUID domainId) {
        public Request { Objects.requireNonNull(clusterId); Objects.requireNonNull(databaseId); Objects.requireNonNull(domainId); }
    }
    public record View(Request request,UUID catalogueGroup,ApplyPosition position,RangeDirectory directory,RatisSchemaAdministration.Head head) {
        public View {
            Objects.requireNonNull(request); Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(position);
            if (directory!=null && (!directory.clusterId().equals(request.clusterId()) || !directory.databaseId().equals(request.databaseId())
                    || !directory.domainId().equals(request.domainId()) || RangeDirectoryCodec.encodedSize(directory)>RatisCatalogue.MAX_DIRECTORY_BYTES))
                throw new IllegalArgumentException("Schema authority directory identity/budget mismatch");
            if (head!=null) {
                var header=head.status().header();
                if (!header.clusterId().equals(request.clusterId()) || !header.databaseId().equals(request.databaseId()) || !header.domainId().equals(request.domainId()))
                    throw new IllegalArgumentException("Schema authority head identity mismatch");
            }
        }
    }
    public RatisSchemaAuthority(RaftClient client,UUID cluster,UUID database,UUID catalogueGroup) {
        this.client=Objects.requireNonNull(client); this.cluster=Objects.requireNonNull(cluster); this.database=Objects.requireNonNull(database); this.group=Objects.requireNonNull(catalogueGroup);
        if (!client.getGroupId().getUuid().equals(group)) throw new IllegalArgumentException("Schema authority client group mismatch");
    }
    /** The caller invokes this synchronous network read outside admission/kernel monitors. */
    public View read(UUID domain) throws IOException {
        var expected=new Request(cluster,database,domain);
        var reply=RatisRoutedClient.rpc(() -> client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(request(expected)))));
        if (reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getRaftGroupId()==null
                || !reply.getRaftGroupId().getUuid().equals(group) || reply.getMessage().getContent().size()>MAX_BYTES)
            throw new IOException("Invalid schema authority reply",reply==null ? null : reply.getException());
        try { return decodeReply(reply.getMessage().getContent().toByteArray(),expected,group); }
        catch (IllegalArgumentException invalid) { throw new IOException("Invalid schema authority reply",invalid); }
    }
    public static byte[] request(Request request) {
        var out=ByteBuffer.allocate(48); identity(out,request); return ScopedTransactionRpc.frame(ScopedTransactionRpc.SCHEMA_AUTHORITY,out.array());
    }
    public static Request request(byte[] bytes) {
        var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.HEADER_BYTES+48);
        if (packet.kind()!=ScopedTransactionRpc.SCHEMA_AUTHORITY || packet.body().length!=48) throw new IllegalArgumentException("Invalid schema authority request");
        return identity(ByteBuffer.wrap(packet.body()));
    }
    public static byte[] reply(View view) {
        byte[] directory=view.directory()==null ? null : RangeDirectoryCodec.encode(view.directory()),head=view.head()==null ? null : view.head().encode();
        int size=88+(directory==null ? 0 : directory.length)+(head==null ? 0 : head.length);
        if (size>MAX_BYTES-ScopedTransactionRpc.HEADER_BYTES) throw new IllegalArgumentException("Schema authority reply exceeds budget");
        var out=ByteBuffer.allocate(size); identity(out,view.request()); ScopedTransactionRpc.uuid(out,view.catalogueGroup());
        out.putLong(view.position().term()).putLong(view.position().index()); ScopedTransactionRpc.putBlob(out,directory); ScopedTransactionRpc.putBlob(out,head);
        return ScopedTransactionRpc.frame(ScopedTransactionRpc.SCHEMA_AUTHORITY,out.array());
    }
    public static View decodeReply(byte[] bytes,Request expected,UUID expectedGroup) {
        var packet=ScopedTransactionRpc.parse(bytes,MAX_BYTES);
        if (packet.kind()!=ScopedTransactionRpc.SCHEMA_AUTHORITY || packet.body().length<88) throw new IllegalArgumentException("Invalid schema authority reply frame");
        var in=ByteBuffer.wrap(packet.body()); var request=identity(in); var group=ScopedTransactionRpc.uuid(in);
        if (!request.equals(expected) || !group.equals(expectedGroup)) throw new IllegalArgumentException("Schema authority reply identity changed");
        var position=new ApplyPosition(in.getLong(),in.getLong());
        byte[] directory=ScopedTransactionRpc.blob(in,RatisCatalogue.MAX_DIRECTORY_BYTES,true),head=ScopedTransactionRpc.blob(in,RatisSchemaAdministration.MAX_HEAD_BYTES,true);
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing schema authority reply");
        return new View(request,group,position,directory==null ? null : RangeDirectoryCodec.decode(directory),head==null ? null : RatisSchemaAdministration.Head.decode(head));
    }
    private static void identity(ByteBuffer out,Request request) { ScopedTransactionRpc.uuid(out,request.clusterId()); ScopedTransactionRpc.uuid(out,request.databaseId()); ScopedTransactionRpc.uuid(out,request.domainId()); }
    private static Request identity(ByteBuffer in) { return new Request(ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in),ScopedTransactionRpc.uuid(in)); }
}
