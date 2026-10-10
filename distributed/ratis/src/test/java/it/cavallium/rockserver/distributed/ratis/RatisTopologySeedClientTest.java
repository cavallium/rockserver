package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Pure codec and controlled client targeting. Model artifacts here make no native/quorum acceptance claim. */
class RatisTopologySeedClientTest {
    static UUID id(long n){return new UUID(0,n);}
    record Fixture(RatisTopologySeedRpc.Request request,NativeTopologySeed seed) {}
    Fixture fixture(){
        var local=new ReplicaDescriptor(id(4),id(5),id(6),"owner","127.0.0.1:4567",ReplicaDescriptor.Role.VOTER);var parent=new RangeDescriptor(id(7),id(8),1,2,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var source=new RangeDirectory(id(1),id(2),id(3),1,new byte[32],4,Set.of(),0,List.of(parent));var left=new RangeDescriptor(id(9),id(11),1,parent.configurationVersion(),null,new byte[]{1},RangeDescriptor.TopologyState.ACTIVE,List.of(local));var right=new RangeDescriptor(id(10),id(12),1,parent.configurationVersion(),new byte[]{1},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));var target=new RangeDirectory(id(1),id(2),id(3),1,new byte[32],5,Set.of(),0,List.of(left,right));var definition=new TopologyChange.Definition(id(13),source,target);var request=new RatisTopologySeedRpc.Request(definition,id(14),parent.groupId(),local.replicaId());var freeze=new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(1,1),NativeTopologyFreeze.boundsDigest(null,null),request.originalAdmissionDigest());var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(local.replicaId()));return new Fixture(request,new NativeTopologySeed(definition,authority,local,Map.of(parent.groupId(),freeze),Map.of(),Set.of()));
    }
    @Test void canonicalRequestReplyRoundtripAcceptsDistinctDefinitionInstances(){
        var f=fixture();var decoded=RatisTopologySeedRpc.decodeRequest(RatisTopologySeedRpc.request(f.request()));assertNotSame(f.request().definition(),decoded.definition());var reply=RatisTopologySeedRpc.decodeReply(RatisTopologySeedRpc.reply(new RatisTopologySeedRpc.Reply(decoded,f.seed())),f.request());assertArrayEquals(f.seed().encode(),reply.seed().encode());assertArrayEquals(RatisTopologySeedRpc.request(f.request()),RatisTopologySeedRpc.request(reply.request()));var crossed=new RatisTopologySeedRpc.Request(f.request().definition(),id(15),f.request().anchorGroup(),f.request().replica());assertThrows(IllegalArgumentException.class,()->RatisTopologySeedRpc.decodeReply(RatisTopologySeedRpc.reply(new RatisTopologySeedRpc.Reply(f.request(),f.seed())),crossed));
    }
    @Test void borrowedClientTargetsExactParentPeerAndRejectsCrossedServerOrGroup()throws Exception{
        var f=fixture();var closes=new AtomicInteger();var calls=new AtomicInteger();var wrongServer=new AtomicBoolean();var wrongGroup=new AtomicBoolean();
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            assertEquals("sendStaleRead",method.getName());assertEquals(0L,args[1]);assertEquals(RaftPeerId.valueOf(f.request().owner().peerId()),args[2]);calls.incrementAndGet();var decoded=RatisTopologySeedRpc.decodeRequest(((Message)args[0]).getContent().toByteArray());assertArrayEquals(RatisTopologySeedRpc.request(f.request()),RatisTopologySeedRpc.request(decoded));return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(wrongServer.get() ? "crossed-peer" : f.request().owner().peerId())).setGroupId(RaftGroupId.valueOf(wrongGroup.get() ? id(16) : f.request().anchorGroup())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(RatisTopologySeedRpc.reply(new RatisTopologySeedRpc.Reply(decoded,f.seed()))))).build();
        });
        var borrowed=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(f.request().anchorGroup());case "io"->io;case "close"->{closes.incrementAndGet();yield null;}default->throw new AssertionError(method);});
        try(var client=new RatisTopologySeedClient(borrowed)){assertArrayEquals(f.seed().encode(),client.seed(f.request()).encode());wrongServer.set(true);assertThrows(IOException.class,()->client.seed(f.request()));wrongServer.set(false);wrongGroup.set(true);assertThrows(IOException.class,()->client.seed(f.request()));assertEquals(3,calls.get());}assertEquals(0,closes.get());
    }
}
