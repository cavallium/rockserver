package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.*;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.proto.RaftProtos.RaftClientRequestProto.TypeCase;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Actual Ratis async client retry machinery with controlled RPCs; no sockets. */
class RatisAsyncMetadataLeaderTest {
    private final RaftPeer follower=RaftPeer.newBuilder().setId("cold-follower").setAddress("127.0.0.1:12001").build();
    private final RaftPeer leader=RaftPeer.newBuilder().setId("frozen-leader").setAddress("127.0.0.1:12002").build();
    private final RaftGroup group=RaftGroup.valueOf(RaftGroupId.randomId(),follower,leader);
    private RangeDescriptor route(){
        var replicas=List.of(follower,leader).stream().map(peer->new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),peer.getId().toString(),peer.getAddress(),ReplicaDescriptor.Role.VOTER)).toList();
        return new RangeDescriptor(UUID.randomUUID(),group.getGroupId().getUuid(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas);
    }
    private abstract static class Rpc implements RaftClientRpc {
        public RaftClientReply sendRequest(RaftClientRequest request)throws IOException{throw new IOException("Blocking transport must not run");}
        public CompletableFuture<RaftClientReply> sendRequestAsyncUnordered(RaftClientRequest request){return sendRequestAsync(request);}
        public void addRaftPeers(Collection<RaftPeer> peers){}
        public void close(){}
    }
    private static void assertHandshake(RaftClientRequest request){
        assertTrue(request.getType().is(TypeCase.WATCH));assertEquals(0,request.getType().getWatch().getIndex());
        assertEquals(org.apache.ratis.proto.RaftProtos.ReplicationLevel.MAJORITY,request.getType().getWatch().getReplication());
        assertEquals(Message.valueOf("DUMMY").getContent(),request.getMessage().getContent());
    }
    private String sequence(List<RaftClientRequest> requests){return requests.stream().map(request->request.getType().getTypeCase()+" cid="+request.getCallId()+" seq="+request.getSlidingWindowEntry().getSeqNum()+" peer="+request.getServerId()).toList().toString();}
    /** The pinned gRPC stream completes NLE replies exceptionally before OrderedAsync sees them. */
    private CompletableFuture<RaftClientReply> rejected(RaftClientRequest request,RaftPeer suggestion){
        return CompletableFuture.failedFuture(new NotLeaderException(RaftGroupMemberId.valueOf(request.getServerId(),group.getGroupId()),suggestion,group.getPeers()));
    }
    @Test void coldFollowerReadBarrierFindsLeaderBeforeTheSingleExactWrite()throws Exception{
        var requests=new CopyOnWriteArrayList<RaftClientRequest>();
        var rpc=new Rpc(){public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request){
            requests.add(request);
            if(request.getType().is(TypeCase.WATCH))assertHandshake(request);
            else assertTrue(request.getType().is(TypeCase.READ)||request.getType().is(TypeCase.WRITE),()->sequence(requests));
            if(request.getServerId().equals(follower.getId()))return rejected(request,leader);
            return CompletableFuture.completedFuture(RaftClientReply.newBuilder().setRequest(request).setSuccess().setMessage(Message.valueOf("forced-home-cas")).build());
        }};
        try(var client=RaftClient.newBuilder().setRaftGroup(group).setLeaderId(follower.getId()).setClientRpc(rpc).setProperties(new RaftProperties()).setRetryPolicy(RatisRoutedClient.snapshotRetryPolicy(route())).build()){
            var result=RatisRoutedClient.metadataWriteAfterLeaderBarrier(client,route(),Message.valueOf("original-durable-control")).get(10,TimeUnit.SECONDS);
            assertTrue(result.isSuccess(),()->sequence(requests));
            var handshakes=requests.stream().filter(request->request.getType().is(TypeCase.WATCH)).toList();
            var reads=requests.stream().filter(request->request.getType().is(TypeCase.READ)).toList();
            var writes=requests.stream().filter(request->request.getType().is(TypeCase.WRITE)).toList();
            assertEquals(2,handshakes.size(),()->sequence(requests));assertEquals(1,reads.size(),()->sequence(requests));assertEquals(1,writes.size(),()->sequence(requests));
            assertEquals(ScopedTransactionRpc.STATE,ScopedTransactionRpc.parse(reads.getFirst().getMessage().getContent().toByteArray(),ScopedTransactionRpc.MAX_REQUEST_BYTES).kind());
            assertEquals(follower.getId(),handshakes.getFirst().getServerId());assertEquals(leader.getId(),handshakes.getLast().getServerId());assertEquals(leader.getId(),writes.getFirst().getServerId());
            assertEquals(handshakes.getFirst().getCallId(),handshakes.getLast().getCallId());
            assertEquals(handshakes.getFirst().getSlidingWindowEntry().getSeqNum(),handshakes.getLast().getSlidingWindowEntry().getSeqNum());
            assertEquals(handshakes.getFirst().getClientId(),writes.getFirst().getClientId());
            assertEquals(Message.valueOf("original-durable-control").getContent(),writes.getFirst().getMessage().getContent());
        }
    }
    @Test void stepDownOrUnknownWriteAfterBarrierIsNeverRetried()throws Exception{
        for(boolean stepDown:List.of(false,true)){
            var writes=new AtomicInteger();var reads=new AtomicInteger();var handshakes=new AtomicInteger();
            var rpc=new Rpc(){public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request){
                if(request.getType().is(TypeCase.WATCH)){assertHandshake(request);handshakes.incrementAndGet();return CompletableFuture.completedFuture(RaftClientReply.newBuilder().setRequest(request).setSuccess().build());}
                if(request.getType().is(TypeCase.READ)){reads.incrementAndGet();return CompletableFuture.completedFuture(RaftClientReply.newBuilder().setRequest(request).setSuccess().setMessage(Message.valueOf("barrier")).build());}
                writes.incrementAndGet();
                if(stepDown)return rejected(request,leader);
                return CompletableFuture.failedFuture(new IOException("Unknown write outcome"));
            }};
            try(var client=RaftClient.newBuilder().setRaftGroup(group).setLeaderId(follower.getId()).setClientRpc(rpc).setProperties(new RaftProperties()).setRetryPolicy(RatisRoutedClient.snapshotRetryPolicy(route())).build()){
                assertThrows(ExecutionException.class,()->RatisRoutedClient.metadataWriteAfterLeaderBarrier(client,route(),Message.valueOf("original-durable-control")).get(10,TimeUnit.SECONDS));
                assertEquals(1,handshakes.get());assertEquals(1,reads.get());assertEquals(1,writes.get());
            }
        }
    }
    @Test void definitiveFollowerLoopIsBoundedAndForeignLeaderRejected()throws Exception{
        for(boolean foreign:List.of(false,true)){
            var requests=new CopyOnWriteArrayList<RaftClientRequest>();var suggestion=foreign?RaftPeer.newBuilder().setId("foreign").setAddress("127.0.0.1:12003").build():leader;
            var rpc=new Rpc(){public CompletableFuture<RaftClientReply> sendRequestAsync(RaftClientRequest request){
                requests.add(request);if(request.getType().is(TypeCase.WATCH))assertHandshake(request);
                else assertTrue(request.getType().is(TypeCase.READ),()->sequence(requests));
                return rejected(request,suggestion);
            }};
            try(var client=RaftClient.newBuilder().setRaftGroup(group).setLeaderId(follower.getId()).setClientRpc(rpc).setProperties(new RaftProperties()).setRetryPolicy(RatisRoutedClient.snapshotRetryPolicy(route())).build()){
                assertThrows(ExecutionException.class,()->RatisRoutedClient.metadataWriteAfterLeaderBarrier(client,route(),Message.valueOf("original-durable-control")).get(10,TimeUnit.SECONDS),()->sequence(requests));
                assertFalse(requests.stream().anyMatch(request->request.getType().is(TypeCase.WRITE)),()->sequence(requests));
                var attempts=requests.stream().collect(java.util.stream.Collectors.groupingBy(RaftClientRequest::getCallId,java.util.stream.Collectors.counting()));
                assertTrue(attempts.values().stream().allMatch(count->count<=5),()->sequence(requests));
                if(foreign){assertEquals(1,requests.size(),()->sequence(requests));assertEquals(follower.getId(),requests.getFirst().getServerId());}
                else{assertTrue(attempts.values().stream().anyMatch(count->count==5),()->sequence(requests));assertTrue(requests.stream().allMatch(request->request.getServerId().equals(follower.getId())||request.getServerId().equals(leader.getId())),()->sequence(requests));}
            }
        }
    }
}
