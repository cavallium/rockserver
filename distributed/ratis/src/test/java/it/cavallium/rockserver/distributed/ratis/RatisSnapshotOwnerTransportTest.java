package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Owner-directed controlled transport and SDK call shape; no sockets, servers, or quorum probes. */
class RatisSnapshotOwnerTransportTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotPinsTest helper=new RatisSnapshotPinsTest();
    ScopedTransactionRpc.SnapshotBinding binding(SnapshotFence fence) throws Exception { return new ScopedTransactionRpc.SnapshotBinding(fence.fenceId(),java.security.MessageDigest.getInstance("SHA-256").digest(fence.encode())); }
    @Test void frozenReplicaServesHistoricalReadsAfterLeaderAndDirectoryChangeWithoutRediscovery() throws Exception {
        helper.root=root;
        try (var f=helper.new Fixture("routed")) {
            var original=f.data.directory.ranges().getFirst(); var source=original.replicas().getFirst();
            var owner=new ReplicaDescriptor(source.nodeId(),source.nodeIncarnation(),source.replicaId(),f.data.f.peer.getId().toString(),source.address(),ReplicaDescriptor.Role.VOTER);
            var replacement=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"replacement","127.0.0.1:23456",ReplicaDescriptor.Role.VOTER);
            var route=new RangeDescriptor(original.rangeId(),original.groupId(),original.rangeEpoch(),0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(owner,replacement));
            var directory=new RangeDirectory(f.data.directory.clusterId(),f.data.directory.databaseId(),f.data.directory.domainId(),1,new byte[32],0,f.data.directory.columns(),0,List.of(route));
            var discoveries=new AtomicInteger(); var reads=new AtomicInteger();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor r,UUID column,byte[] key) { throw new AssertionError("No latest read fallback"); }
                public CommandResult command(RangeDescriptor r,DistributedCommand command) { throw new AssertionError("No write fallback"); }
                public byte[] transaction(RangeDescriptor r,byte[] request,boolean fresh) throws IOException {
                    assertTrue(fresh); assertEquals(ScopedTransactionRpc.SNAPSHOT_OWNER,ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES)); discoveries.incrementAndGet();
                    return f.data.wire.transaction(r,request,true);
                }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor r,ReplicaDescriptor peer,byte[] request) throws IOException {
                    assertEquals(route,r);if(peer==null)discoveries.incrementAndGet();else{assertEquals(owner,peer);reads.incrementAndGet();}var message=Message.valueOf(ByteString.copyFrom(request));return (peer==null ? f.data.f.machine.queryLinearizable(message) : f.data.f.machine.queryStale(message,0)).thenApply(reply->reply.getContent().toByteArray());
                }
            };
            try (var client=new RatisRoutedClient(directory,transport)) {
                var target=client.snapshotTarget(f.fence,f.data.f.groupId); assertEquals(owner,target.replica()); assertEquals(f.owner,target.incarnation());
                var token=f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,target.incarnation(),UUID.randomUUID(),Duration.ofSeconds(60));
                f.complete(token); f.change();
                var secondFence=helper.helper.fence(f.home,f.data,2,helper.helper.scope(f.data,1,4)); var secondInitial=f.home.register(secondFence); ApplyPosition installed;
                try (var install=f.data.f.machine.prepareSnapshotInstall(f.home.homes,secondFence)) { installed=helper.helper.append(f.data,install).position(); }
                var secondReady=secondInitial.ready(f.data.f.groupId,installed); f.home.transition(secondInitial,secondReady);
                var secondTarget=client.snapshotTarget(secondFence,f.data.f.groupId);
                var secondToken=f.data.f.machine.captureSnapshotPin(f.home.homes,secondFence,secondTarget.incarnation(),UUID.randomUUID(),Duration.ofSeconds(60));
                assertThrows(RatisSnapshotLostException.class,() -> client.snapshotGet(target,secondToken,f.data.f.column,new byte[]{2}));
                assertThrows(RatisSnapshotLostException.class,() -> client.snapshotPage(target,secondToken,f.data.f.column,new byte[]{1},new byte[]{4},false,null,1,1024));
                assertThrows(RatisSnapshotLostException.class,() -> client.releaseSnapshot(target,secondToken));
                assertArrayEquals(new byte[]{9},client.snapshotGet(secondTarget,secondToken,f.data.f.column,new byte[]{2}));
                var changedFence=new SnapshotFence(f.fence.clusterId(), f.fence.databaseId(), f.fence.fenceId(), f.fence.homeGroup(), f.fence.homeEpoch(), f.fence.operation(), List.of(new DomainBinding(f.fence.domains().getFirst().domainId(), 1, new byte[32], f.fence.domains().getFirst().schemaEpoch(), f.fence.domains().getFirst().schemaDigest())), f.fence.participants());
                var changedTarget=client.restoreSnapshotTarget(changedFence,route,owner,target.incarnation());
                assertThrows(RatisSnapshotLostException.class,() -> client.snapshotGet(changedTarget,token,f.data.f.column,new byte[]{2}));
                client.releaseSnapshot(secondTarget,secondToken);
                assertArrayEquals(new byte[]{7},client.snapshotGet(target,token,f.data.f.column,new byte[]{2}));
                f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
                var next=new RangeDescriptor(route.rangeId(),route.groupId(),route.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replacement));
                client.refresh(new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),1,new byte[32],1,directory.columns(),0,List.of(next)));
                var restored=client.restoreSnapshotTarget(f.fence,target.descriptor(),target.replica(),target.incarnation()); assertEquals(0,restored.descriptor().configurationVersion());
                var page=client.snapshotPage(restored,token,f.data.f.column,new byte[]{1},new byte[]{4},false,null,1,PhysicalRangePage.MAX_BYTES); assertArrayEquals(new byte[]{1},page.rows().getFirst().key());
                var reverse=client.snapshotPage(restored,token,f.data.f.column,new byte[]{1},new byte[]{4},true,null,1,1024); assertArrayEquals(new byte[]{3},reverse.rows().getFirst().key());
                assertArrayEquals(new byte[]{7},client.snapshotGet(restored,token,f.data.f.column,new byte[]{2})); assertEquals(2,discoveries.get());
                int before=reads.get(); var wrong=new SnapshotFenceStatus.Capture(UUID.randomUUID(),token.snapshotId(),token.position());
                assertThrows(RatisSnapshotLostException.class,() -> client.snapshotGet(restored,wrong,f.data.f.column,new byte[]{2})); assertEquals(before,reads.get());
                var reincarnated=new ReplicaDescriptor(owner.nodeId(),UUID.randomUUID(),owner.replicaId(),owner.peerId(),owner.address(),owner.role());
                assertThrows(IllegalArgumentException.class,() -> client.restoreSnapshotTarget(f.fence,route,reincarnated,target.incarnation()));
                client.releaseSnapshot(restored,token); client.releaseSnapshot(restored,token);
                assertThrows(RatisSnapshotLostException.class,() -> client.snapshotGet(restored,token,f.data.f.column,new byte[]{2})); assertEquals(2,discoveries.get());
            }
        }
    }
    @Test void ownerDiscoveryIsFreshButPinnedLossIsExplicitEvenDuringStoragePause() throws Exception {
        helper.root=root;
        try (var f=helper.new Fixture("query")) {
            var owner=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0]);
            assertThrows(ExecutionException.class,() -> f.data.f.machine.query(Message.valueOf(ByteString.copyFrom(owner))).get(5,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,() -> f.data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(owner)),0).get(5,TimeUnit.SECONDS));
            var fresh=f.data.f.machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(owner))).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            var decoded=RatisSnapshotOwner.decode(ScopedTransactionRpc.parse(fresh,ScopedTransactionRpc.MAX_REPLY_BYTES).body()); assertEquals(f.owner,decoded.incarnation());
            var token=f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,Duration.ofSeconds(60));
            var binding=binding(f.fence); var body=ByteBuffer.allocate(117); ScopedTransactionRpc.snapshotBinding(body,binding); ScopedTransactionRpc.snapshotToken(body,token); ScopedTransactionRpc.uuid(body,f.data.f.column); ScopedTransactionRpc.putBlob(body,new byte[]{2});
            var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GET,body.array())));
            assertThrows(ExecutionException.class,() -> f.data.f.machine.queryStale(request,1).get(5,TimeUnit.SECONDS));
            f.data.f.machine.takeSnapshot(); f.data.f.machine.pause();
            var lost=f.data.f.machine.queryStale(request,0).get(5,TimeUnit.SECONDS).getContent().toByteArray(); var packet=ScopedTransactionRpc.parse(lost,ScopedTransactionRpc.MAX_REPLY_BYTES);
            assertThrows(RatisSnapshotLostException.class,() -> ScopedTransactionRpc.snapshotPayload(packet.body(),binding,token)); f.data.f.machine.reinitialize();
            assertEquals(0,f.data.f.store.activeSnapshotPins());
        }
    }
    @Test void sdkCallTargetsExactlyOnePeerAndRejectsReplyRedirection() throws Exception {
        var peer=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"owner","127.0.0.1:23456",ReplicaDescriptor.Role.VOTER);
        var route=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var token=new SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),new ApplyPosition(1,1)); var binding=new ScopedTransactionRpc.SnapshotBinding(UUID.randomUUID(),new byte[32]); var body=ByteBuffer.allocate(96); ScopedTransactionRpc.snapshotBinding(body,binding); ScopedTransactionRpc.snapshotToken(body,token);
        byte[] request=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_RELEASE,body.array()); byte[] response=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_RELEASE,ScopedTransactionRpc.snapshotReply(binding,token,new byte[0]));
        var responsePeer=new AtomicReference<>(RaftPeerId.valueOf(peer.peerId())); var calls=new AtomicInteger();
        var io=(org.apache.ratis.client.api.AsyncApi)Proxy.newProxyInstance(org.apache.ratis.client.api.AsyncApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.AsyncApi.class},(proxy,method,args) -> {
            assertEquals("sendStaleRead",method.getName()); assertEquals(0L,args[1]); assertEquals(RaftPeerId.valueOf(peer.peerId()),args[2]); calls.incrementAndGet();
            return java.util.concurrent.CompletableFuture.completedFuture(RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(responsePeer.get()).setGroupId(RaftGroupId.valueOf(route.groupId())).setCallId(1).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(response))).build());
        });
        var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> { assertEquals("async",method.getName()); return io; });
        assertArrayEquals(response,RatisRoutedClient.awaitSnapshot(RatisRoutedClient.snapshotOnOwner(client,route,peer,request))); assertEquals(1,calls.get());
        responsePeer.set(RaftPeerId.valueOf("replacement")); assertThrows(IOException.class,() -> RatisRoutedClient.awaitSnapshot(RatisRoutedClient.snapshotOnOwner(client,route,peer,request))); assertEquals(2,calls.get());
        assertThrows(IllegalArgumentException.class,() -> RatisRoutedClient.snapshotOnOwner(client,route,peer,ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_OWNER,new byte[0]))); assertEquals(2,calls.get());
    }
    @Test void snapshotReplyBindsFullTokenAndLossStatusHasNoHiddenPayload() {
        var token=new SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),new ApplyPosition(1,2)); var other=new SnapshotFenceStatus.Capture(token.ownerIncarnation(),token.snapshotId(),new ApplyPosition(1,3));
        var binding=new ScopedTransactionRpc.SnapshotBinding(UUID.randomUUID(),new byte[32]); byte[] reply=ScopedTransactionRpc.snapshotReply(binding,token,new byte[]{7});
        assertThrows(IllegalArgumentException.class,() -> ScopedTransactionRpc.snapshotPayload(reply,binding,other));
        byte[] lost=ScopedTransactionRpc.snapshotReply(binding,token,null); assertThrows(RatisSnapshotLostException.class,() -> ScopedTransactionRpc.snapshotPayload(lost,binding,token));
        assertThrows(IllegalArgumentException.class,() -> ScopedTransactionRpc.snapshotPayload(Arrays.copyOf(lost,lost.length+1),binding,token));
        byte[] bad=reply.clone(); bad[96]=2; assertThrows(IllegalArgumentException.class,() -> ScopedTransactionRpc.snapshotPayload(bad,binding,token));
    }
}
