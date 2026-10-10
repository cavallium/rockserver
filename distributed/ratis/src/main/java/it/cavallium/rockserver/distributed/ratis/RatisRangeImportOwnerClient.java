package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Bounded exact-owner reads, including listeners. No leader discovery or caller-selected connector. */
public final class RatisRangeImportOwnerClient implements AutoCloseable {
    public static final int MAX_CALLS=32;
    private final UUID cluster,database,catalogue;
    private final RatisClientPool clients;
    private int active; private boolean closing;
    public RatisRangeImportOwnerClient(UUID cluster,UUID database,UUID catalogue){this(cluster,database,catalogue,RatisRangeImportOwnerClient::connected);}
    RatisRangeImportOwnerClient(UUID cluster,UUID database,UUID catalogue,RatisClientPool.OwnerFactory factory){this.cluster=Objects.requireNonNull(cluster);this.database=Objects.requireNonNull(database);this.catalogue=Objects.requireNonNull(catalogue);clients=new RatisClientPool(MAX_CALLS,range->{throw new IOException("Import status requires exact owner");},factory);}
    public RatisRangeImportApply.Status read(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {
        var target=RatisRangeImportStatusRpc.Target.of(definition,range.groupId(),owner);
        if(!cluster.equals(target.cluster()) || !database.equals(target.database()) || !range.rangeId().equals(target.range()) || range.rangeEpoch()!=target.epoch() || range.configurationVersion()!=target.configuration())throw new IOException("Import status namespace/lineage differs");
        var frozen=definition.sourceDirectory().ranges().stream().filter(r->r.groupId().equals(target.group())).findFirst().orElseThrow();
        if(!range.replicas().equals(frozen.replicas()) || !Arrays.equals(range.lowerInclusive(),frozen.lowerInclusive()) || !Arrays.equals(range.upperExclusive(),frozen.upperExclusive()) || range.topologyState()!=frozen.topologyState())throw new IOException("Import status source placement differs");
        synchronized(this){if(closing || active>=MAX_CALLS)throw new IOException("Import owner status capacity unavailable");active++;}
        try(var lease=clients.acquireOwner(frozen,owner)) {
            var reply=lease.client().io().sendStaleRead(Message.valueOf(ByteString.copyFrom(RatisRangeImportStatusRpc.request(target))),0,RaftPeerId.valueOf(owner.peerId()));
            if(reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()>RatisRangeImportStatusRpc.MAX_REPLY_BYTES || !RaftGroupId.valueOf(range.groupId()).equals(reply.getRaftGroupId()) || !RaftPeerId.valueOf(owner.peerId()).equals(reply.getServerId()))throw new IOException("Import status reply crossed owner/group or failed",reply==null?null:reply.getException());
            try{return RatisRangeImportStatusRpc.decodeReply(reply.getMessage().getContent().toByteArray(),target);}catch(IllegalArgumentException invalid){throw new IOException("Invalid owner import status",invalid);}
        }finally{synchronized(this){active--;notifyAll();}}
    }
    public RangeImportAdmission.OwnerReceipt receipt(RangeImportDefinition definition,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {
        var status=read(definition,range,owner);if(status.receipt()==null)throw new IOException("Import owner has not durably installed its original SSTs");
        return new RangeImportAdmission.OwnerReceipt(range.groupId(),range.rangeId(),range.configurationVersion(),owner,status.receipt(),status.applied());
    }
    public RangeImportActivationAdmission.OwnerActivation activation(RangeImportDefinition definition,RatisRangeImportCatalogue.Head published,RangeDescriptor range,ReplicaDescriptor owner)throws IOException {
        published.requireDefinition(definition);var status=read(definition,range,owner);var activation=status.activation();
        if(activation==null || published.phase()!=RatisRangeImportCatalogue.Phase.PUBLISHED || !activation.catalogueGroup().equals(catalogue) || !Arrays.equals(activation.publicationDigest(),RangeImportAdmission.sha256(published.encode())))throw new IOException("Import owner lacks exact published native activation");
        return new RangeImportActivationAdmission.OwnerActivation(range.groupId(),range.rangeId(),range.configurationVersion(),owner,RangeImportActivationAdmission.Activation.decode(activation.encode()),status.applied());
    }
    private static RaftClient connected(RangeDescriptor range,ReplicaDescriptor owner) {
        var peers=range.replicas().stream().map(replica->RaftPeer.newBuilder().setId(replica.peerId()).setAddress(replica.address()).setStartupRole(replica.role()==ReplicaDescriptor.Role.VOTER?RaftPeerRole.FOLLOWER:RaftPeerRole.LISTENER).build()).toList();
        var properties=new RaftProperties();org.apache.ratis.RaftConfigKeys.Rpc.setType(properties,org.apache.ratis.rpc.SupportedRpcType.GRPC);RaftClientConfigKeys.Rpc.setRequestTimeout(properties,org.apache.ratis.util.TimeDuration.valueOf(3,TimeUnit.SECONDS));GrpcConfigKeys.Client.setWorkerGroupSize(properties,1);
        return RaftClient.newBuilder().setRaftGroup(RaftGroup.valueOf(RaftGroupId.valueOf(range.groupId()),peers)).setProperties(properties).setLeaderId(RaftPeerId.valueOf(owner.peerId())).setRetryPolicy(org.apache.ratis.retry.RetryPolicies.noRetry()).build();
    }
    @Override public void close()throws IOException{synchronized(this){closing=true;if(active!=0)throw new IOException("Import owner reads remain active");}clients.close();}
}
