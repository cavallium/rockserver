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
            var client=new RatisTopologySourceSnapshotClient(raft,RatisTopologySourceSnapshotClient.ReplyAdmission.UNLIMITED);var request=fixture.request(50);var prepared=client.prepare(request);var transferred=client.transfer(request);assertArrayEquals(prepared.image().sha256(),transferred.image().sha256());client.release(request,prepared.sourceOwner());client.release(request,prepared.sourceOwner());assertEquals(1,fixture.captures.get());assertEquals(1,fixture.pinCloses.get());verify(raft,never()).close();
        }
    }
    @Test void replyAdmissionPrecedesPayloadCopyAndDecodeForEverySourceAction()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var request=fixture.request(50);var prepared=service.prepare(request);byte[] encoded=reply(PREPARE,prepared);var content=spy(ByteString.copyFrom(encoded));var raft=mock(RaftClient.class);var io=mock(BlockingApi.class);when(raft.getGroupId()).thenReturn(RaftGroupId.valueOf(fixture.group));when(raft.io()).thenReturn(io);
            var response=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(fixture.donor.peerId())).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(content)).build();when(io.sendReadOnly(any())).thenReturn(response);when(io.sendStaleRead(any(),anyLong(),any())).thenReturn(response);clearInvocations(content);
            var lengths=new ArrayList<Integer>();var rejected=new IOException("Controlled response admission denial");var denied=new RatisTopologySourceSnapshotClient(raft,bytes->{lengths.add(bytes);throw rejected;});
            assertSame(rejected,assertThrows(IOException.class,()->denied.prepare(request)));assertSame(rejected,assertThrows(IOException.class,()->denied.transfer(request)));assertSame(rejected,assertThrows(IOException.class,()->denied.head(new RatisSourceHeadRpc.Request(request,prepared.image().position()))));assertSame(rejected,assertThrows(IOException.class,()->denied.release(request,fixture.donor)));
            var current=new Request(new UUID(94,20),null,request.catalogue(),fixture.donor,Purpose.CURRENT_LEAF,fixture.group,fixture.definition.sourceDirectory());var image=new RatisRecoveryImageRpc.Request(current,prepared.image().position(),prepared.image().sha256());assertSame(rejected,assertThrows(IOException.class,()->denied.imageInstalled(image)));assertEquals(Collections.nCopies(5,encoded.length),lengths);verify(content,never()).toByteArray();
            var allowed=new RatisTopologySourceSnapshotClient(raft,bytes->{assertEquals(encoded.length,bytes);verify(content,never()).toByteArray();});assertArrayEquals(prepared.image().sha256(),allowed.prepare(request).image().sha256());verify(content,times(1)).toByteArray();
            // A valid Raft envelope with an invalid publication body must still hit admission first.
            byte[] malformed=ScopedTransactionRpc.frame(PREPARE,new byte[]{99});var malformedContent=spy(ByteString.copyFrom(malformed));var bad=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(fixture.donor.peerId())).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(malformedContent)).build();when(io.sendReadOnly(any())).thenReturn(bad);clearInvocations(malformedContent);
            assertSame(rejected,assertThrows(IOException.class,()->denied.prepare(request)));assertEquals(malformed.length,lengths.getLast().intValue());verify(malformedContent,never()).toByteArray();var malformedFailure=assertThrows(IOException.class,()->new RatisTopologySourceSnapshotClient(raft,RatisTopologySourceSnapshotClient.ReplyAdmission.UNLIMITED).prepare(request));assertEquals("Malformed source snapshot reply",malformedFailure.getMessage());verify(malformedContent,times(1)).toByteArray();
        }
    }
    @Test void malformedOrCrossedRepliesDoNotInventSuccess()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root);var service=new RatisTopologySourceSnapshotService(fixture,RatisTopologySourceSnapshotService.Limits.defaults(8*1024*1024),fixture.scheduler,fixture.transferAdmission())){
            var raft=mock(RaftClient.class);var io=mock(BlockingApi.class);when(raft.getGroupId()).thenReturn(RaftGroupId.valueOf(fixture.group));when(raft.io()).thenReturn(io);var request=fixture.request(50);var prepared=service.prepare(request);
            var response=RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("other")).setGroupId(raft.getGroupId()).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(reply(PREPARE,prepared)))).build();when(io.sendReadOnly(any())).thenReturn(response);
            var client=new RatisTopologySourceSnapshotClient(raft,RatisTopologySourceSnapshotClient.ReplyAdmission.UNLIMITED);assertThrows(IOException.class,()->client.prepare(request));assertEquals(0,fixture.pinCloses.get());assertThrows(IOException.class,()->client.release(request,fixture.receiver));verify(io,never()).sendStaleRead(any(),anyLong(),any());
            when(io.sendStaleRead(any(),eq(0L),eq(RaftPeerId.valueOf(fixture.donor.peerId())))).thenReturn(response);assertThrows(IOException.class,()->client.release(request,fixture.donor));
            when(raft.getGroupId()).thenReturn(RaftGroupId.randomId());assertThrows(IOException.class,()->client.prepare(request));verify(raft,never()).close();
        }
    }
}
