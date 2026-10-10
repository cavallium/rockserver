package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeTopologyFreeze;
import java.io.IOException;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed parent-group client. Returned artifacts are local evidence, not configured-replica qualification. */
public final class RatisTopologyFreezeClient {
    private final RaftClient client;
    public RatisTopologyFreezeClient(RaftClient client) { this.client=Objects.requireNonNull(client); }
    public RatisTopologyFreezeRpc.State prepare(RatisTopologyFreezeRecord record) throws IOException { return read(record,RatisTopologyFreezeRpc.PREPARE,RatisTopologyFreezeRpc.prepareRequest(record)); }
    public RatisTopologyFreezeRpc.State state(RatisTopologyFreezeRecord record) throws IOException { return read(record,RatisTopologyFreezeRpc.STATE,RatisTopologyFreezeRpc.stateRequest(record)); }
    public void cancel(RatisTopologyFreezeRecord record) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(message(RatisTopologyFreezeRpc.cancelRequest(record))));
        try { RatisTopologyFreezeRpc.decodeCancelledReply(content(reply,record,RatisTopologyFreezeRpc.MAX_REQUEST_BYTES),record); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology cancel reply",invalid); }
    }
    public NativeTopologyFreeze append(RatisTopologyFreezeRecord record) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().send(message(record.encode())));
        try { var freeze=NativeTopologyFreeze.decode(content(reply,record,NativeTopologyFreeze.MAX_BYTES)); record.requireFreeze(freeze); return freeze; }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid original topology FREEZE artifact",invalid); }
    }
    private RatisTopologyFreezeRpc.State read(RatisTopologyFreezeRecord record,int kind,byte[] request) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(message(request)));
        try { return RatisTopologyFreezeRpc.stateReply(content(reply,record,RatisTopologyFreezeRpc.MAX_REPLY_BYTES),kind,record); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology control reply",invalid); }
    }
    private void requireGroup(RatisTopologyFreezeRecord record) throws IOException { if(!client.getGroupId().getUuid().equals(record.groupId())) throw new IOException("Topology FREEZE client is bound to another parent group"); }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,RatisTopologyFreezeRecord record,int maximum) throws IOException {
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !RaftGroupId.valueOf(record.groupId()).equals(reply.getRaftGroupId()) || reply.getMessage().getContent().size()>maximum) throw new IOException("Topology FREEZE response failed/crossed group/exceeded bound",reply==null ? null : reply.getException());
        return reply.getMessage().getContent().toByteArray();
    }
}
