package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AsyncApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static it.cavallium.rockserver.distributed.ratis.RatisSnapshotCohortRpc.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisSnapshotCohortClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void actualNativeCaptureLostReplyAndColdStateCancelUseOriginalPreparedVoter()throws Exception {
        var helper=new RatisSnapshotCohortServiceTest();helper.root=root;
        try(var f=helper.new Fixture("cohort-client")) {
            var ranges=new ArrayList<RangeDescriptor>();for(int n=0;n<2;n++){var store=f.nativeState.stores.get(n);var group=store.ownership().owner().groupId();var member=f.member(group,null,false);ranges.add(new RangeDescriptor(group,group,1,0,store.ownership().interval().lowerInclusive(),store.ownership().interval().upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,List.of(member.replica())));}
            var fence=f.nativeState.fence;var directory=new RangeDirectory(fence.clusterId(),fence.databaseId(),fence.domainId(),1,new byte[32],0,Set.of(f.nativeState.column),0,ranges);
            var lose=new AtomicBoolean(true);var calls=new ArrayList<byte[]>();var discovery=new AtomicInteger();
            var transport=new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError("No latest read");}
                public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError("No business write");}
                public byte[] transaction(RangeDescriptor r,byte[] bytes,boolean fresh)throws IOException {assertTrue(fresh);assertEquals(ScopedTransactionRpc.SNAPSHOT_OWNER,ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES));discovery.incrementAndGet();try{return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,f.service.owner(r.groupId(),fence).encode());}catch(Exception failure){throw new IOException(failure);}}
                public CompletableFuture<byte[]> snapshot(RangeDescriptor r,ReplicaDescriptor owner,byte[] bytes)throws IOException {
                    var packet=ScopedTransactionRpc.parse(bytes,MAX_REQUEST_BYTES);
                    if(owner==null){assertEquals(ScopedTransactionRpc.SNAPSHOT_OWNER,packet.kind());discovery.incrementAndGet();try{return CompletableFuture.completedFuture(ScopedTransactionRpc.frame(packet.kind(),f.service.owner(r.groupId(),fence).encode()));}catch(Exception failure){return CompletableFuture.failedFuture(failure);}}
                    assertTrue(r.replicas().contains(owner));calls.add(bytes.clone());
                    return f.service.query(r.groupId(),packet.kind(),packet.body()).thenApply(value->{if(packet.kind()==CAPTURE && lose.getAndSet(false))throw new CompletionException(new IOException("Lost actual capture reply"));return ScopedTransactionRpc.frame(packet.kind(),value);});
                }
            };
            try(var routed=new RatisRoutedClient(directory,transport)) {
                var frozen=new HashMap<UUID,RatisRoutedClient.PreparedSnapshotCapture>();var routes=new HashMap<UUID,RatisRoutedClient>();for(int n=0;n<2;n++){var group=ranges.get(n).groupId();frozen.put(group,routed.prepareSnapshotCapture(routed.snapshotTargetAsync(fence,group).get(10,TimeUnit.SECONDS),f.nativeState.request(n).snapshotId(),f.nativeState.request(n).ttl()));routes.put(group,routed);}
                var request=f.request().cohort();var client=new RatisSnapshotCohortClient(routes,frozen::get);assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->client.capture(request,frozen).get(10,TimeUnit.SECONDS)).getCause());assertEquals(1,f.pool().snapshotAdmissionStats().pins());var result=client.capture(request,frozen).get(10,TimeUnit.SECONDS);assertArrayEquals(calls.get(0),calls.get(1));assertEquals(0,f.nativeState.factories.get());
                var cold=new RatisSnapshotCohortClient(routes,frozen::get);var tokens=new HashMap<UUID,SnapshotFenceStatus.Capture>();result.forEach((g,c)->tokens.put(g,c.token()));cold.requireLive(request,tokens).get(10,TimeUnit.SECONDS);cold.cancelAndDrain(request).get(10,TimeUnit.SECONDS);assertEquals(0,f.pool().snapshotAdmissionStats().pins());assertEquals(2,discovery.get());assertEquals(Phase.RELEASED,f.service.state(reference(request)).phase());
                var missing=new RatisSnapshotCohortClient(routes,g->null);assertThrows(IOException.class,()->missing.cancelAndDrain(request));
            }
        }
    }
    @Test void actualOwnerTransportUsesFreshQualificationAndPerKindBoundsWithoutReplyRedirection()throws Exception {
        var peer=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"owner","127.0.0.1:23456",ReplicaDescriptor.Role.VOTER);var route=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));var wrongPeer=new AtomicBoolean();var wrongGroup=new AtomicBoolean();var count=new AtomicInteger();var closes=new AtomicInteger();
        var io=(AsyncApi)Proxy.newProxyInstance(AsyncApi.class.getClassLoader(),new Class<?>[]{AsyncApi.class},(proxy,method,args)->{var bytes=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(bytes,MAX_REQUEST_BYTES);assertEquals(kind==OWNER?"sendReadOnly":"sendStaleRead",method.getName());if(kind!=OWNER){assertEquals(0L,args[1]);assertEquals(RaftPeerId.valueOf(peer.peerId()),args[2]);}count.incrementAndGet();return CompletableFuture.completedFuture(new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(wrongPeer.get()?"wrong":peer.peerId())).setGroupId(RaftGroupId.valueOf(wrongGroup.get()?UUID.randomUUID():route.groupId())).setCallId(count.get()).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,new byte[0])))).build());});
        var borrowed=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "async"->io;case "close"->{closes.incrementAndGet();yield null;}default->throw new AssertionError(method);});
        for(int kind:new int[]{OWNER,CAPTURE,STATE,CANCEL}){var body=new byte[kind==CAPTURE?ScopedTransactionRpc.MAX_REQUEST_BYTES:96];assertArrayEquals(ScopedTransactionRpc.frame(kind,new byte[0]),RatisRoutedClient.snapshotOnOwner(borrowed,route,peer,ScopedTransactionRpc.frame(kind,body)).get(10,TimeUnit.SECONDS));}
        wrongPeer.set(true);assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->RatisRoutedClient.snapshotOnOwner(borrowed,route,peer,ScopedTransactionRpc.frame(STATE,new byte[96])).get(10,TimeUnit.SECONDS)).getCause());wrongPeer.set(false);wrongGroup.set(true);assertInstanceOf(IOException.class,assertThrows(ExecutionException.class,()->RatisRoutedClient.snapshotOnOwner(borrowed,route,peer,ScopedTransactionRpc.frame(CANCEL,new byte[96])).get(10,TimeUnit.SECONDS)).getCause());assertEquals(6,count.get());assertEquals(0,closes.get());
    }
}
