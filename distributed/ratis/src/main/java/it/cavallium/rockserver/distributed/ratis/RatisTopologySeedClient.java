package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Bounded node-local SEED calls addressed to the original parent peer, including listeners. */
public final class RatisTopologySeedClient implements AutoCloseable {
    public static final int MAX_CALLS=8;
    private final RatisClientPool clients;private final RaftClient borrowed;private int active;private boolean closing;
    public RatisTopologySeedClient(){this(RatisTopologySeedClient::connected);}
    public RatisTopologySeedClient(RaftClient borrowed){this.borrowed=Objects.requireNonNull(borrowed);clients=null;}
    RatisTopologySeedClient(RatisClientPool.OwnerFactory factory){borrowed=null;clients=new RatisClientPool(MAX_CALLS,range->{throw new IOException("SEED requires exact parent owner");},factory);}
    public NativeTopologySeed seed(RatisTopologySeedRpc.Request request)throws IOException{return execute(request).seed();}
    public RatisTopologySeedRpc.Reply execute(RatisTopologySeedRpc.Request request)throws IOException{
        Objects.requireNonNull(request);synchronized(this){if(closing || active>=MAX_CALLS)throw new IOException("SEED client capacity unavailable");active++;}
        try{if(borrowed!=null)return execute(borrowed,request);try(var lease=clients.acquireOwner(request.range(),request.owner())){return execute(lease.client(),request);}}
        finally{synchronized(this){active--;notifyAll();}}
    }
    private static RatisTopologySeedRpc.Reply execute(RaftClient client,RatisTopologySeedRpc.Request request)throws IOException{
        if(!RaftGroupId.valueOf(request.anchorGroup()).equals(client.getGroupId()))throw new IOException("SEED client is outside exact parent anchor");var result=RatisRoutedClient.rpc(()->client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisTopologySeedRpc.request(request))),0,RaftPeerId.valueOf(request.owner().peerId())));if(result==null || !result.isSuccess() || result.getMessage()==null || result.getMessage().getContent().size()>RatisTopologySeedRpc.MAX_REPLY_BYTES || !RaftGroupId.valueOf(request.anchorGroup()).equals(result.getRaftGroupId()) || !RaftPeerId.valueOf(request.owner().peerId()).equals(result.getServerId()))throw new IOException("SEED reply crossed parent owner/group or failed",result==null ? null : result.getException());try{return RatisTopologySeedRpc.decodeReply(result.getMessage().getContent().toByteArray(),request);}catch(IllegalArgumentException invalid){throw new IOException("Invalid SEED reply",invalid);}
    }
    private static RaftClient connected(RangeDescriptor range,ReplicaDescriptor owner){var peers=range.replicas().stream().map(replica->RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build()).toList();var properties=new RaftProperties();org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);RaftClientConfigKeys.Rpc.setRequestTimeout(properties,org.apache.ratis.util.TimeDuration.valueOf(3,TimeUnit.SECONDS));GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);return RaftClient.newBuilder().setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),peers)).setProperties(properties).setLeaderId(RaftPeerId.valueOf(owner.peerId())).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build();}
    @Override public void close()throws IOException{synchronized(this){closing=true;if(active!=0)throw new IOException("SEED calls remain active");}if(clients!=null)clients.close();}
}
