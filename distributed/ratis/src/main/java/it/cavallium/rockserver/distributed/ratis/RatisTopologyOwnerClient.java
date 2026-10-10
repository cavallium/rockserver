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

/** Node-shared bounded transport. Replica proofs always target the exact immutable owner. */
public final class RatisTopologyOwnerClient implements AutoCloseable {
    public static final int MAX_CALLS=32;
    private final UUID cluster,database,catalogue;
    private final RatisClientPool clients;
    private int active;private boolean closing;
    public RatisTopologyOwnerClient(UUID cluster,UUID database,UUID catalogue){this(cluster,database,catalogue,range->connected(range,null),RatisTopologyOwnerClient::connected);}
    RatisTopologyOwnerClient(UUID cluster,UUID database,UUID catalogue,RatisClientPool.Factory leaders,RatisClientPool.OwnerFactory owners){this.cluster=Objects.requireNonNull(cluster);this.database=Objects.requireNonNull(database);this.catalogue=Objects.requireNonNull(catalogue);clients=new RatisClientPool(MAX_CALLS,leaders,owners);}
    private synchronized void begin()throws IOException{if(closing || active>=MAX_CALLS)throw new IOException("Topology owner transport capacity unavailable");active++;}
    private synchronized void end(){active--;notifyAll();}
    private void requireNamespace(TopologyChange.Definition definition)throws IOException{if(!cluster.equals(definition.header().clusterId()) || !database.equals(definition.header().databaseId()))throw new IOException("Topology transport namespace differs");}
    public RatisTopologyOwnerRpc.Evidence read(TopologyChange.Definition definition,TopologyChange.Stage stage,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{
        requireNamespace(definition);var target=new RatisTopologyOwnerRpc.Target(definition,stage,catalogue,range.groupId(),owner.replicaId());
        if(!target.range().equals(range) || !target.owner().equals(owner))throw new IOException("Topology proof placement differs from immutable definition");
        begin();try(var lease=clients.acquireOwner(range,owner)){
            var reply=RatisRoutedClient.rpc(()->lease.client().io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisTopologyOwnerRpc.request(target))),0,RaftPeerId.valueOf(owner.peerId())));
            if(reply==null || !reply.isSuccess() || reply.getMessage()==null || !RaftGroupId.valueOf(range.groupId()).equals(reply.getRaftGroupId()) || !RaftPeerId.valueOf(owner.peerId()).equals(reply.getServerId()) || reply.getMessage().getContent().size()>RatisTopologyOwnerRpc.MAX_REPLY_BYTES)throw new IOException("Topology proof failed or crossed replica/group",reply==null?null:reply.getException());
            try{return RatisTopologyOwnerRpc.decodeReply(reply.getMessage().getContent().toByteArray(),target);}catch(IllegalArgumentException invalid){throw new IOException("Invalid topology owner evidence",invalid);}
        }finally{end();}
    }
    /** Borrowed catalogue authority client remains owned by the caller. */
    public RatisTopologyController.DataPlane dataPlane(RatisTopologyAuthority.Client authority){
        Objects.requireNonNull(authority);return new RatisTopologyController.DataPlane(){
            @Override public NativeTopologyFreeze freeze(RatisTopologyFreezeRecord record)throws IOException{
                requireNamespace(record.definition());var range=record.definition().parents().stream().filter(r->r.groupId().equals(record.groupId())).findFirst().orElseThrow();
                begin();try(var lease=clients.acquire(range)){var client=new RatisTopologyFreezeClient(lease.client());if(!client.prepare(record).ready())throw new IOException("Topology parent still drains predecessors");return client.append(record);}finally{end();}
            }
            @Override public NativeTopologySeed seed(RatisTopologySeedRpc.Request request)throws IOException{
                requireNamespace(request.definition());if(!catalogue.equals(request.catalogue()))throw new IOException("Topology seed catalogue binding differs");
                begin();try(var lease=clients.acquireOwner(request.range(),request.owner())){return new RatisTopologySeedClient(lease.client()).seed(request);}finally{end();}
            }
            @Override public RatisTopologyOwnerRpc.Evidence proof(TopologyChange.Definition definition,TopologyChange.Stage stage,RangeDescriptor range,ReplicaDescriptor owner)throws IOException{return read(definition,stage,range,owner);}
            @Override public RatisTopologyAuthority.View authority(TopologyChange.Header header,TopologyChange.Stage stage)throws IOException{begin();try{var view=authority.read(header,stage);requireNamespace(view.definition());if(!catalogue.equals(view.catalogue()))throw new IOException("Topology authority catalogue binding differs");return view;}finally{end();}}
            @Override public NativeTopologyActivation activate(RatisTopologyActivationRecord record)throws IOException{
                requireNamespace(record.definition());if(!catalogue.equals(record.catalogueGroup()))throw new IOException("Topology activation catalogue binding differs");var range=record.definition().targets().stream().filter(r->r.groupId().equals(record.groupId())).findFirst().orElseThrow();
                begin();try(var lease=clients.acquire(range)){var client=new RatisTopologyActivationClient(lease.client());if(!client.prepare(record).ready())throw new IOException("Topology child activation is still preparing");return client.append(record);}finally{end();}
            }
        };
    }
    private static RaftClient connected(RangeDescriptor range,ReplicaDescriptor owner){
        var peers=range.replicas().stream().map(replica->RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER?RaftPeerRole.FOLLOWER:RaftPeerRole.LISTENER).build()).toList();
        var properties=new RaftProperties();org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);RaftClientConfigKeys.Rpc.setRequestTimeout(properties,org.apache.ratis.util.TimeDuration.valueOf(3,TimeUnit.SECONDS));GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
        var builder=RaftClient.newBuilder().setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),peers)).setProperties(properties).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry());
        if(owner!=null)builder.setLeaderId(RaftPeerId.valueOf(owner.peerId()));return builder.build();
    }
    @Override public void close()throws IOException{synchronized(this){closing=true;if(active!=0)throw new IOException("Topology owner calls remain active");}clients.close();}
}
