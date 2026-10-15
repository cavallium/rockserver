package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

class RatisTopologySourceSnapshotClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void borrowedClientCarriesExactIdentityThroughControlledSourceActions()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var raft=mock(RaftClient.class);var io=mock(BlockingApi.class);when(raft.getGroupId()).thenReturn(RaftGroupId.valueOf(fixture.group));when(raft.io()).thenReturn(io);
            when(io.sendReadOnly(any())).thenAnswer(call->{var input=((Message)call.getArgument(0)).getContent().toByteArray();var request=request(input);int kind=ScopedTransactionRpc.kind(input,MAX_REQUEST_BYTES);byte[] result;
                if(kind==RELEASE){service.release(request);result=input;}else result=reply(kind,kind==PREPARE?service.prepare(request):service.transfer(request).get(5,java.util.concurrent.TimeUnit.SECONDS));
                return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(fixture.donor.peerId())).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(result))).build();});
            when(io.sendStaleRead(any(),eq(0L),eq(RaftPeerId.valueOf(fixture.donor.peerId())))).thenAnswer(call->{var input=((Message)call.getArgument(0)).getContent().toByteArray();service.release(request(input));return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(fixture.donor.peerId())).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(input))).build();});
            var client=new RatisTopologySourceSnapshotClient(raft);var request=fixture.request(50);var prepared=client.prepare(request);var transferred=client.transfer(request);assertArrayEquals(prepared.image().sha256(),transferred.image().sha256());client.release(request,prepared.sourceOwner());client.release(request,prepared.sourceOwner());assertEquals(1,fixture.captures.get());assertEquals(1,fixture.pinCloses.get());verify(raft,never()).close();
        }
    }
    @Test void malformedOrCrossedRepliesDoNotInventSuccess()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var raft=mock(RaftClient.class);var io=mock(BlockingApi.class);when(raft.getGroupId()).thenReturn(RaftGroupId.valueOf(fixture.group));when(raft.io()).thenReturn(io);var request=fixture.request(50);var prepared=service.prepare(request);
            var response=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("other")).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(reply(PREPARE,prepared)))).build();when(io.sendReadOnly(any())).thenReturn(response);
            var client=new RatisTopologySourceSnapshotClient(raft);assertThrows(IOException.class,()->client.prepare(request));assertEquals(0,fixture.pinCloses.get());assertThrows(IOException.class,()->client.release(request,fixture.receiver));verify(io,never()).sendStaleRead(any(),anyLong(),any());
            when(io.sendStaleRead(any(),eq(0L),eq(RaftPeerId.valueOf(fixture.donor.peerId())))).thenReturn(response);assertThrows(IOException.class,()->client.release(request,fixture.donor));
            when(raft.getGroupId()).thenReturn(RaftGroupId.randomId());assertThrows(IOException.class,()->client.prepare(request));verify(raft,never()).close();
        }
    }
}
