package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Borrowed transport. Unknown replies retry the same UUID and immutable request at its actual owner. */
public final class RatisTopologySourceSnapshotClient {
    @FunctionalInterface public interface ReplyAdmission {
        ReplyAdmission UNLIMITED=bytes->{};
        void beforeReply(int bytes)throws IOException;
    }
    private final RaftClient client;private final ReplyAdmission admission;
    public RatisTopologySourceSnapshotClient(RaftClient client,ReplyAdmission admission){this.client=Objects.requireNonNull(client);this.admission=Objects.requireNonNull(admission);}
    public RatisSourceHeadRpc.Head head(RatisSourceHeadRpc.Request request)throws IOException{
        var reply=call(RatisSourceHeadRpc.request(request),request.source(),RatisSourceHeadRpc.MAX_REPLY_BYTES);
        try{var head=RatisSourceHeadRpc.reply(reply.getMessage().getContent().toByteArray(),request);if(!RaftPeerId.valueOf(head.sourceOwner().peerId()).equals(reply.getServerId()))throw new IOException("Source head crossed actual source owner");return head;}catch(IllegalArgumentException malformed){throw new IOException("Malformed source head reply",malformed);}
    }
    public Prepared prepare(Request request)throws IOException{return prepared(PREPARE,request);}
    public Prepared transfer(Request request)throws IOException{return prepared(TRANSFER,request);}
    public void release(Request request,it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor owner)throws IOException{
        if(!request.sourceOwner(owner) || !client.getGroupId().equals(RaftGroupId.valueOf(request.sourceGroup())))throw new IOException("Source release owner/group differs from immutable transfer");
        byte[] input=request(RELEASE,request);var peer=RaftPeerId.valueOf(owner.peerId());
        var reply=RatisRoutedClient.rpc(()->client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(input)),0,peer));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || !peer.equals(reply.getServerId()) || reply.getMessage().getContent().size()>MAX_REQUEST_BYTES)throw new IOException("Source release did not acknowledge exact request at its original owner",reply==null?null:reply.getException());
        admission.beforeReply(reply.getMessage().getContent().size());if(!Arrays.equals(input,reply.getMessage().getContent().toByteArray()))throw new IOException("Source release changed exact request acknowledgement");
    }
    public void obsolete(RatisRecoveryObsoleteRpc.Request request,it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor sourceOwner)throws IOException {
        var original=request.original();if(!original.sourceOwner(sourceOwner) || !client.getGroupId().equals(RaftGroupId.valueOf(original.sourceGroup())))throw new IOException("Obsolete source owner/group differs from immutable CURRENT capture");
        var peer=RaftPeerId.valueOf(sourceOwner.peerId());byte[] input=RatisRecoveryObsoleteRpc.request(request);
        var reply=RatisRoutedClient.rpc(()->client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(input)),0,peer));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || !peer.equals(reply.getServerId()) || reply.getMessage().getContent().size()!=RatisRecoveryObsoleteRpc.MAX_REPLY_BYTES)throw new IOException("Obsolete source did not acknowledge exact request at original owner",reply==null?null:reply.getException());
        admission.beforeReply(reply.getMessage().getContent().size());try{RatisRecoveryObsoleteRpc.reply(reply.getMessage().getContent().toByteArray(),request);}catch(IllegalArgumentException invalid){throw new IOException("Malformed obsolete source acknowledgement",invalid);}
    }
    public RatisRecoveryImageRpc.Evidence imageInstalled(RatisRecoveryImageRpc.Request request)throws IOException {
        if(!client.getGroupId().equals(RaftGroupId.valueOf(request.source().sourceGroup())))throw new IOException("Image probe client belongs to another group");
        var receiver=RaftPeerId.valueOf(request.source().receiver().peerId());
        var reply=RatisRoutedClient.rpc(()->client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisRecoveryImageRpc.request(request))),0,receiver));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || !receiver.equals(reply.getServerId()) || reply.getMessage().getContent().size()>RatisRecoveryImageRpc.MAX_REPLY_BYTES)throw new IOException("Image probe did not reach exact receiver",reply==null?null:reply.getException());
        admission.beforeReply(reply.getMessage().getContent().size());
        try{return RatisRecoveryImageRpc.reply(reply.getMessage().getContent().toByteArray(),request);}catch(IllegalArgumentException malformed){throw new IOException("Malformed recovery image receipt",malformed);}
    }
    public void superseded(RatisRecoverySupersessionRpc.Request request,it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor receiver)throws IOException {
        if(!client.getGroupId().equals(RaftGroupId.valueOf(request.group())) || !request.receiver().equals(it.cavallium.rockserver.core.common.distributed.TopologyChange.Owner.of(receiver)))throw new IOException("Supersession probe client/receiver differs from exact fence");
        var peer=RaftPeerId.valueOf(receiver.peerId());
        var reply=RatisRoutedClient.rpc(()->client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisRecoverySupersessionRpc.request(request))),0,peer));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || !peer.equals(reply.getServerId()) || reply.getMessage().getContent().size()!=RatisRecoverySupersessionRpc.MAX_REPLY_BYTES)throw new IOException("Supersession probe did not acknowledge exact receiver fence",reply==null?null:reply.getException());
        admission.beforeReply(reply.getMessage().getContent().size());
        try{RatisRecoverySupersessionRpc.reply(reply.getMessage().getContent().toByteArray(),request);}catch(IllegalArgumentException malformed){throw new IOException("Malformed supersession acknowledgement",malformed);}
    }
    private Prepared prepared(int kind,Request request)throws IOException{
        var reply=call(request(kind,request),request,MAX_REPLY_BYTES);
        try{var result=RatisTopologySourceSnapshotRpc.reply(reply.getMessage().getContent().toByteArray(),kind,request);if(!RaftPeerId.valueOf(result.sourceOwner().peerId()).equals(reply.getServerId()))throw new IOException("Source image reply crossed actual source owner");return result;}catch(AbandonedException abandoned){if(!RaftPeerId.valueOf(abandoned.receipt().sourceOwner().peerId()).equals(reply.getServerId()))throw new IOException("Abandoned source reply crossed actual owner",abandoned);throw abandoned;}catch(IllegalArgumentException malformed){throw new IOException("Malformed source snapshot reply",malformed);}
    }
    private RaftClientReply call(byte[] input,Request request,int maximum)throws IOException{
        UUID group=request.sourceGroup();if(!client.getGroupId().equals(RaftGroupId.valueOf(group)))throw new IOException("Source snapshot client belongs to another group");
        var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(input))));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || reply.getMessage().getContent().size()>maximum)throw new IOException("Source snapshot action failed; exact request remains recoverable",reply==null?null:reply.getException());
        admission.beforeReply(reply.getMessage().getContent().size());return reply;
    }
}
