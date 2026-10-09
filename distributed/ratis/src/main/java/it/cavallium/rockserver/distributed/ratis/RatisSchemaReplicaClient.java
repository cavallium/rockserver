package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

/** Bounded owner-specific schema control. The caller retains immutable requests for uncertain outcomes. */
public final class RatisSchemaReplicaClient implements AutoCloseable {
    public static final int MAX_CALLS=32;
    private final RatisClientPool clients;
    private int active;
    private boolean closing;
    public RatisSchemaReplicaClient(int cachedClients) { this(cachedClients,RatisSchemaReplicaClient::connected); }
    RatisSchemaReplicaClient(int cachedClients,RatisClientPool.OwnerFactory factory) {
        if (cachedClients<1 || cachedClients>MAX_CALLS) throw new IllegalArgumentException("Invalid schema replica client budget");
        clients=new RatisClientPool(cachedClients,descriptor -> { throw new IOException("Schema controls require explicit replica ownership"); },factory);
    }
    public static List<RatisSchemaReplicaRpc.Target> targets(RatisSchemaReplicaRpc.Binding binding) { return binding.source().replicas().stream().map(replica -> new RatisSchemaReplicaRpc.Target(binding,replica)).toList(); }
    public static RatisSchemaReplicaRpc.Request artifact(int kind,RatisSchemaReplicaRpc.Target target,OperatorUpload upload) { return new RatisSchemaReplicaRpc.Request(kind,target,upload,-1,null); }
    public static RatisSchemaReplicaRpc.Request chunk(RatisSchemaReplicaRpc.Target target,OperatorUpload upload,int index,byte[] bytes) { return new RatisSchemaReplicaRpc.Request(ScopedTransactionRpc.SCHEMA_ARTIFACT_CHUNK,target,upload,index,bytes); }
    public static RatisSchemaReplicaRpc.Request capacity(int kind,RatisSchemaReplicaRpc.Target target) { return new RatisSchemaReplicaRpc.Request(kind,target,null,-1,null); }
    public RatisSchemaReplicaRpc.Reply execute(RatisSchemaReplicaRpc.Request request) throws IOException {
        Objects.requireNonNull(request); synchronized(this) { if (closing) throw new IOException("Schema replica client closing"); if (active>=MAX_CALLS) throw new IOException("Schema replica calls exhausted"); active++; }
        try {
            var target=request.target(); var source=target.binding().source(); var replica=target.replica(); byte[] bytes=request.encode();
            try (var lease=clients.acquireOwner(source,replica)) {
                var result=lease.client().io().sendStaleRead(Message.valueOf(ByteString.copyFrom(bytes)),0,RaftPeerId.valueOf(replica.peerId()));
                if (result==null || !result.isSuccess() || result.getMessage()==null || !RaftPeerId.valueOf(replica.peerId()).equals(result.getServerId()) || !RaftGroupId.valueOf(source.groupId()).equals(result.getRaftGroupId())) throw new IOException("Schema reply came from another replica/group or failed",result==null ? null : result.getException());
                var content=result.getMessage().getContent(); if (content==null || content.size()>RatisSchemaReplicaRpc.MAX_BYTES) throw new IOException("Schema replica reply exceeds budget");
                try { return RatisSchemaReplicaRpc.decodeReply(content.toByteArray(),request); } catch(IllegalArgumentException malformed) { throw new IOException("Invalid schema replica reply",malformed); }
            }
        } finally { synchronized(this) { active--; notifyAll(); } }
    }
    private static RaftClient connected(RangeDescriptor descriptor,ReplicaDescriptor owner) {
        var peers=descriptor.replicas().stream().map(replica -> RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER ? RaftPeerRole.FOLLOWER : RaftPeerRole.LISTENER).build()).toList();
        var properties=new RaftProperties(); RaftConfigKeys.Rpc.setType(properties,SupportedRpcType.GRPC); RaftClientConfigKeys.Rpc.setRequestTimeout(properties,TimeDuration.valueOf(3,TimeUnit.SECONDS)); GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
        return RaftClient.newBuilder().setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(descriptor.groupId()),peers)).setProperties(properties).setLeaderId(RaftPeerId.valueOf(owner.peerId())).setRetryPolicy(RetryPolicies.noRetry()).build();
    }
    @Override public void close() throws IOException { synchronized(this) { closing=true; if (active!=0) throw new IOException("Schema replica calls still active"); } clients.close(); }
}
