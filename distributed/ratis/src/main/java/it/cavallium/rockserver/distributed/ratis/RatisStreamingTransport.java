package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.client.DataStreamClient;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/** Explicit operator endpoints and finite provider workers; never derives a second port from Raft. */
public final class RatisStreamingTransport {
    private RatisStreamingTransport() {}
    public static void server(RatisNodePlan plan,RaftProperties properties) throws IOException {
        String address=plan.streamAddresses().get(plan.peerId());
        if(address==null) {
            if(!plan.imports().isEmpty())throw new IOException("Native imports require explicit streaming endpoints");
            RaftConfigKeys.DataStream.setType(properties,SupportedDataStreamType.DISABLED); return;
        }
        RaftConfigKeys.DataStream.setType(properties,SupportedDataStreamType.NETTY);
        int colon=address.lastIndexOf(':'); String host=address.substring(0,colon);
        if(host.startsWith("["))host=host.substring(1,host.length()-1);
        NettyConfigKeys.DataStream.setHost(properties,host); NettyConfigKeys.DataStream.setPort(properties,Integer.parseInt(address.substring(colon+1)));
        NettyConfigKeys.DataStream.Server.setBossGroupSize(properties,1); NettyConfigKeys.DataStream.Server.setWorkerGroupSize(properties,2);
        RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolCached(properties,false);
        RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolSize(properties,2); RaftServerConfigKeys.DataStream.setAsyncWriteThreadPoolSize(properties,2);
        RaftServerConfigKeys.DataStream.setClientPoolSize(properties,2); client(properties);
    }
    public static void client(RaftProperties properties) {
        RaftConfigKeys.DataStream.setType(properties,SupportedDataStreamType.NETTY);
        NettyConfigKeys.DataStream.Client.setWorkerGroupSize(properties,1); NettyConfigKeys.DataStream.Client.setWorkerGroupShare(properties,false);
    }
    /** Caller owns the returned stream client; the control client remains borrowed. No fallback endpoint. */
    public static DataStreamClient connect(RatisNodePlan plan,RaftClient control,RaftPeerId primary,Parameters parameters) throws IOException {
        var group=control.getGroupId().getUuid();
        if(plan.role(group)!=RatisNodePlan.Role.DATA || plan.streamAddresses().isEmpty())throw new IOException("Streaming client requires a configured DATA group");
        var owner=plan.groupDescriptor(group).replicas().stream().filter(replica->replica.peerId().equals(primary.toString())
                && replica.role()==it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor.Role.VOTER).findFirst()
                .orElseThrow(()->new IOException("Stream primary is outside frozen voters"));
        var peer=RaftPeer.newBuilder().setId(primary).setAddress(owner.address()).setDataStreamAddress(plan.streamAddresses().get(owner.peerId())).build();
        var properties=new RaftProperties(); client(properties);
        return DataStreamClient.newBuilder(control).setDataStreamServer(peer).setProperties(properties).setParameters(parameters).build();
    }
}
