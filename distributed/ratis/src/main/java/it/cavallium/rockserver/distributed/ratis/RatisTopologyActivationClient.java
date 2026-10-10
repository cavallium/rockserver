package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.NativeTopologyActivation;
import java.io.IOException;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed child-group client. Returned artifacts are local evidence, not configured-replica qualification. */
public final class RatisTopologyActivationClient {
    private final RaftClient client;
    public RatisTopologyActivationClient(RaftClient client) { this.client=Objects.requireNonNull(client); }
    public RatisTopologyActivationRpc.State prepare(RatisTopologyActivationRecord record) throws IOException { return read(record,RatisTopologyActivationRpc.PREPARE,RatisTopologyActivationRpc.prepareRequest(record)); }
    public RatisTopologyActivationRpc.State state(RatisTopologyActivationRecord record) throws IOException { return read(record,RatisTopologyActivationRpc.STATE,RatisTopologyActivationRpc.stateRequest(record)); }
    public void cancel(RatisTopologyActivationRecord record) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(message(RatisTopologyActivationRpc.cancelRequest(record))));
        try { RatisTopologyActivationRpc.decodeCancelledReply(content(reply,record,RatisTopologyActivationRpc.MAX_REQUEST_BYTES),record); } catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology cancel reply",invalid); }
    }
    public NativeTopologyActivation append(RatisTopologyActivationRecord record) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().send(message(record.encode())));
        try { var freeze=NativeTopologyActivation.decode(content(reply,record,NativeTopologyActivation.MAX_BYTES)); RatisTopologyActivationRpc.requireActivation(record,freeze); return freeze; }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid original topology FREEZE artifact",invalid); }
    }
    private RatisTopologyActivationRpc.State read(RatisTopologyActivationRecord record,int kind,byte[] request) throws IOException {
        requireGroup(record); var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(message(request)));
        try { return RatisTopologyActivationRpc.stateReply(content(reply,record,RatisTopologyActivationRpc.MAX_REPLY_BYTES),kind,record); }
        catch(IllegalArgumentException invalid) { throw new IOException("Invalid topology control reply",invalid); }
    }
    private void requireGroup(RatisTopologyActivationRecord record) throws IOException { if(!client.getGroupId().getUuid().equals(record.groupId())) throw new IOException("Topology ACTIVATE client is bound to another child group"); }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    private static byte[] content(RaftClientReply reply,RatisTopologyActivationRecord record,int maximum) throws IOException {
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !RaftGroupId.valueOf(record.groupId()).equals(reply.getRaftGroupId()) || reply.getMessage().getContent().size()>maximum) throw new IOException("Topology ACTIVATE response failed/crossed group/exceeded bound",reply==null ? null : reply.getException());
        return reply.getMessage().getContent().toByteArray();
    }
}
