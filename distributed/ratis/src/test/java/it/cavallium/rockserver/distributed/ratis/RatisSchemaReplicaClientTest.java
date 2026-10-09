package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Identity-only transports; no sockets, discovery, Raft server or quorum. */
class RatisSchemaReplicaClientTest {
    @FunctionalInterface interface Answer { byte[] reply(RatisSchemaReplicaRpc.Request request) throws IOException; }
    static RaftClient client(RangeDescriptor range,ReplicaDescriptor owner,Answer answer,AtomicInteger closes,List<byte[]> received,String returnedPeer,UUID returnedGroup) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
            assertEquals("sendStaleRead",method.getName(),"No leader discovery or fallback is permitted"); assertEquals(0L,args[1]); assertEquals(RaftPeerId.valueOf(owner.peerId()),args[2]);
            byte[] bytes=((Message)args[0]).getContent().toByteArray(); received.add(bytes); var request=RatisSchemaReplicaRpc.request(bytes);
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(returnedPeer))
                    .setGroupId(RaftGroupId.valueOf(returnedGroup)).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(answer.reply(request)))).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
            case "io" -> io; case "close" -> { closes.incrementAndGet(); yield null; } default -> throw new AssertionError("Unexpected client method "+method.getName());
        });
    }
    static byte[] empty(RatisSchemaReplicaRpc.Request request) { return RatisSchemaReplicaRpc.reply(request,new BitSet(),false,null,RatisSchemaReplicaRpc.CapacityState.NONE,false); }
    @Test void listenerAndVoterAreAddressedExactlyAndFrozenTargetsSurviveNewMembership() throws Exception {
        var f=RatisSchemaReplicaRpcTest.fixture(6); var old=RatisSchemaReplicaClient.targets(f.binding()); var newer=RatisSchemaReplicaClient.targets(RatisSchemaReplicaRpcTest.fixture(7).binding());
        var closes=new AtomicInteger(); var opened=new ArrayList<ReplicaDescriptor>(); var received=new ArrayList<byte[]>();
        try (var client=new RatisSchemaReplicaClient(4,(range,owner) -> { opened.add(owner); return client(range,owner,RatisSchemaReplicaClientTest::empty,closes,received,owner.peerId(),range.groupId()); })) {
            for (var target:old) client.execute(RatisSchemaReplicaClient.capacity(70,target));
            client.execute(RatisSchemaReplicaClient.capacity(70,newer.getFirst()));
            client.execute(RatisSchemaReplicaClient.capacity(70,old.getFirst()));
            assertEquals(List.of(old.getFirst().replica(),old.get(1).replica(),newer.getFirst().replica()),opened);
            assertEquals(ReplicaDescriptor.Role.LISTENER,opened.get(1).role());
            assertEquals(old.getFirst(),RatisSchemaReplicaRpc.request(received.get(3)).target());
        }
        assertEquals(3,closes.get());
    }
    @Test void unknownReplyRetriesIdenticalBytesWithoutFallbackOrFreshOwnerSelection() throws Exception {
        var f=RatisSchemaReplicaRpcTest.fixture(6); var target=RatisSchemaReplicaClient.targets(f.binding()).get(1); var request=RatisSchemaReplicaClient.chunk(target,f.upload(),0,Arrays.copyOf(f.bytes(),OperatorUpload.CHUNK_BYTES));
        var received=new ArrayList<byte[]>(); var attempts=new AtomicInteger(); var opens=new AtomicInteger(); var closes=new AtomicInteger();
        try (var client=new RatisSchemaReplicaClient(1,(range,owner) -> { opens.incrementAndGet(); return client(range,owner,input -> { if (attempts.getAndIncrement()==0) throw new IOException("Reply lost after owner accepted chunk"); return empty(input); },closes,received,owner.peerId(),range.groupId()); })) {
            byte[] frozen=request.encode(); assertThrows(IOException.class,() -> client.execute(request)); assertEquals(1,attempts.get());
            client.execute(request); assertEquals(2,attempts.get()); assertEquals(1,opens.get()); assertArrayEquals(frozen,received.getFirst()); assertArrayEquals(frozen,received.get(1));
        }
        assertEquals(1,closes.get());
    }
    @Test void wrongPeerGroupAndIncarnationRepliesReject() throws Exception {
        var f=RatisSchemaReplicaRpcTest.fixture(6); var target=RatisSchemaReplicaClient.targets(f.binding()).getFirst(); var request=RatisSchemaReplicaClient.capacity(70,target);
        for (int wrong=0;wrong<3;wrong++) {
            int mode=wrong; var received=new ArrayList<byte[]>();
            try (var client=new RatisSchemaReplicaClient(1,(range,owner) -> client(range,owner,input -> mode==2 ? empty(RatisSchemaReplicaClient.capacity(70,RatisSchemaReplicaClient.targets(RatisSchemaReplicaRpcTest.fixture(7).binding()).getFirst())) : empty(input),new AtomicInteger(),received,mode==0 ? "foreign" : owner.peerId(),mode==1 ? RatisSchemaReplicaRpcTest.id(99) : range.groupId()))) {
                assertThrows(IOException.class,() -> client.execute(request)); assertEquals(1,received.size());
            }
        }
    }
}
