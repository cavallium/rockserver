package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisTopologyOwnerClientTest {
    static final class Transport {
        final RatisTopologyOwnerRpcTest helper=new RatisTopologyOwnerRpcTest();
        final RatisTopologyOwnerRpcTest.Fixture fixture=helper.fixture();
        final RatisTopologyOwnerRpc.Evidence evidence=helper.evidence(fixture,TopologyChange.Stage.SEED);
        final AtomicInteger opens=new AtomicInteger(),closes=new AtomicInteger();
        volatile boolean crossedPeer;volatile Runnable beforeReply=()->{};
        RaftClient open(RangeDescriptor range,ReplicaDescriptor owner){
            opens.incrementAndGet();assertEquals(evidence.target().range(),range);assertEquals(fixture.owner(),owner);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(!method.getName().equals("sendStaleRead"))throw new AssertionError(method);
                assertEquals(0L,args[1]);assertEquals(RaftPeerId.valueOf(owner.peerId()),args[2]);
                var request=RatisTopologyOwnerRpc.decodeRequest(((Message)args[0]).getContent().toByteArray());assertArrayEquals(evidence.target().encode(),request.encode());beforeReply.run();
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(crossedPeer?"other-leader":owner.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(RatisTopologyOwnerRpc.reply(evidence)))).build();
            });
            return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "io"->io;case "close"->{closes.incrementAndGet();yield null;}default->throw new AssertionError(method);});
        }
        RatisTopologyOwnerClient client(){return new RatisTopologyOwnerClient(fixture.definition().header().clusterId(),fixture.definition().header().databaseId(),fixture.catalogue(),range->{throw new AssertionError("Proof cannot use leader discovery");},this::open);}
        RatisTopologyOwnerRpc.Evidence read(RatisTopologyOwnerClient client)throws IOException{return client.read(fixture.definition(),TopologyChange.Stage.SEED,evidence.target().range(),fixture.owner());}
    }
    @Test void exactPeerRequestsReuseOwnedClientAndCannotAcceptLeaderRelabeling()throws Exception{
        var transport=new Transport();try(var client=transport.client()){assertEquals(transport.evidence.proof(),transport.read(client).proof());assertEquals(transport.evidence.proof(),transport.read(client).proof());assertEquals(1,transport.opens.get());transport.crossedPeer=true;assertThrows(IOException.class,()->transport.read(client));assertEquals(0,transport.closes.get());}assertEquals(1,transport.closes.get());
    }
    @Test void shutdownRetainsActualInFlightOwnerUntilItsCallReturns()throws Exception{
        var transport=new Transport();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);transport.beforeReply=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Blocked proof timed out");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new AssertionError(interrupted);}};
        var client=transport.client();try(var executor=Executors.newSingleThreadExecutor()){
            var call=executor.submit(()->transport.read(client));assertTrue(entered.await(5,TimeUnit.SECONDS));
            try{assertThrows(IOException.class,client::close);assertEquals(0,transport.closes.get());assertThrows(IOException.class,()->transport.read(client));}finally{release.countDown();}
            assertEquals(transport.evidence.proof(),call.get(5,TimeUnit.SECONDS).proof());client.close();assertEquals(1,transport.closes.get());
        }finally{release.countDown();client.close();}
    }
    @Test void facadeAuthorityRetainsCallOwnershipAndNeverClosesBorrowedClient()throws Exception{
        var transport=new Transport();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var borrowedCloses=new AtomicInteger();
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Authority timed out");
            var f=transport.fixture;var view=new RatisTopologyAuthority.View(f.catalogue(),new ApplyPosition(1,4),f.definition(),f.seed().authority());
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("catalogue")).setGroupId(RaftGroupId.valueOf(f.catalogue())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(RatisTopologyAuthority.reply(view)))).build();
        });
        var borrowed=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(transport.fixture.catalogue());case "io"->io;case "close"->{borrowedCloses.incrementAndGet();yield null;}default->throw new AssertionError(method);});
        var f=transport.fixture;var client=transport.client();var data=client.dataPlane(new RatisTopologyAuthority.Client(borrowed,f.definition().header().clusterId(),f.definition().header().databaseId(),f.catalogue()));
        try(var executor=Executors.newSingleThreadExecutor()){
            var call=executor.submit(()->data.authority(f.definition().header(),TopologyChange.Stage.FREEZE));assertTrue(entered.await(5,TimeUnit.SECONDS));
            try{assertThrows(IOException.class,client::close);assertThrows(IOException.class,()->data.authority(f.definition().header(),TopologyChange.Stage.FREEZE));assertEquals(0,borrowedCloses.get());}finally{release.countDown();}
            assertEquals(f.catalogue(),call.get(5,TimeUnit.SECONDS).catalogue());client.close();assertThrows(IOException.class,()->data.authority(f.definition().header(),TopologyChange.Stage.FREEZE));assertEquals(0,borrowedCloses.get());
        }finally{release.countDown();client.close();}
    }
}
