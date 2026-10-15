package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Borrowed leader transport. Unknown replies must retry the same UUID and immutable request. */
public final class RatisTopologySourceSnapshotClient {
    private final RaftClient client;
    public RatisTopologySourceSnapshotClient(RaftClient client){this.client=Objects.requireNonNull(client);}
    public Prepared prepare(Request request)throws IOException{return prepared(PREPARE,request);}
    public Prepared transfer(Request request)throws IOException{return prepared(TRANSFER,request);}
    public void release(Request request)throws IOException{byte[] input=request(RELEASE,request),reply=call(input,request,MAX_REQUEST_BYTES).getMessage().getContent().toByteArray();if(!Arrays.equals(input,reply))throw new IOException("Source release acknowledgement changed exact request");}
    private Prepared prepared(int kind,Request request)throws IOException{
        var reply=call(request(kind,request),request,MAX_REPLY_BYTES);
        try{var result=RatisTopologySourceSnapshotRpc.reply(reply.getMessage().getContent().toByteArray(),kind,request);if(!RaftPeerId.valueOf(result.sourceOwner().peerId()).equals(reply.getServerId()))throw new IOException("Source image reply crossed actual source owner");return result;}catch(AbandonedException abandoned){if(!RaftPeerId.valueOf(abandoned.receipt().sourceOwner().peerId()).equals(reply.getServerId()))throw new IOException("Abandoned source reply crossed actual owner",abandoned);throw abandoned;}catch(IllegalArgumentException malformed){throw new IOException("Malformed source snapshot reply",malformed);}
    }
    private RaftClientReply call(byte[] input,Request request,int maximum)throws IOException{
        UUID group=request.sourceGroup();if(!client.getGroupId().equals(RaftGroupId.valueOf(group)))throw new IOException("Source snapshot client belongs to another group");
        var reply=RatisRoutedClient.rpc(()->client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(input))));
        if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !client.getGroupId().equals(reply.getRaftGroupId()) || reply.getMessage().getContent().size()>maximum)throw new IOException("Source snapshot action failed; exact request remains recoverable",reply==null?null:reply.getException());
        return reply;
    }
}
