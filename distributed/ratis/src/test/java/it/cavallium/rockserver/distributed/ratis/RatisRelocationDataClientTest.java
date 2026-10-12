package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.AdminApi;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Exercises the real AdminApi argument builder and client, without a server or network. */
class RatisRelocationDataClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static RaftClientReply reply(UUID group,byte[] bytes){return RaftClientReply.newBuilder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("donor")).setGroupId(RaftGroupId.valueOf(group)).setCallId(0).setSuccess().setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();}
    @Test void exactConditionalConfigurationSurvivesLostReplyAndNeverFallsBackToUnconditional()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var definition=fixture.definition;var source=fixture.store.configuration().data();var target=RatisRelocationDataClient.targetConfiguration(definition.targets().getFirst(),source);var catalogue=new UUID(90,1);var phase=new java.util.concurrent.atomic.AtomicReference<>(TopologyChange.State.begin(definition.header()).beginJoining());
            var control=mock(RaftClient.class);var io=mock(BlockingApi.class);when(control.getGroupId()).thenReturn(RaftGroupId.valueOf(catalogue));when(control.io()).thenReturn(io);when(io.sendReadOnly(any())).thenAnswer(call->{var request=RatisTopologyJoinAuthority.decodeRequest(((Message)call.getArgument(0)).getContent().toByteArray());var view=new RatisTopologyJoinAuthority.View(catalogue,new ApplyPosition(1,7),definition,phase.get());return reply(catalogue,RatisTopologyJoinAuthority.reply(view));});var authority=new RatisTopologyJoinAuthority.Client(control,definition.header().clusterId(),definition.header().databaseId(),catalogue);
            var client=mock(RaftClient.class);var admin=mock(AdminApi.class);when(client.admin()).thenReturn(admin);var calls=new AtomicInteger();var configurations=new ArrayList<String>();
            when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenAnswer(call->{SetConfigurationRequest.Arguments args=call.getArgument(0);assertEquals(SetConfigurationRequest.Mode.COMPARE_AND_SET,args.getMode());assertEquals(List.of("donor"),args.getServersInCurrentConf().stream().map(p->p.getId().toString()).toList());assertTrue(args.getListenersInCurrentConf().isEmpty());assertEquals(List.of("donor"),args.getServersInNewConf().stream().map(p->p.getId().toString()).toList());assertEquals(List.of("receiver"),args.getPeersInNewConf(RaftPeerRole.LISTENER).stream().map(p->p.getId().toString()).toList());configurations.add(args.toString());if(calls.incrementAndGet()==1)throw new IOException("Controlled lost configuration reply");return reply(fixture.group,new byte[0]);});
            try(var data=new RatisRelocationDataClient(definition.header().clusterId(),definition.header().databaseId(),catalogue,r->client,(r,o)->client)){
                assertThrows(IOException.class,()->data.compareAndSet(definition,source,target,authority));data.compareAndSet(definition,source,target,authority);assertEquals(configurations.getFirst(),configurations.getLast());assertEquals(2,calls.get());
                assertThrows(IOException.class,()->data.compareAndSet(definition,target,target,authority));assertEquals(2,calls.get(),"Wrong current voter/listener sets must fail before AdminApi");phase.set(TopologyChange.State.begin(definition.header()));assertThrows(IOException.class,()->data.compareAndSet(definition,source,target,authority));assertEquals(2,calls.get(),"PREPARED cannot authorize configuration mutation");
            }
        }
    }
    @Test void targetConfigurationUsesExactDescriptorAddressesAndRecordCodecBindsBothSets()throws Exception {
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            byte[] source=fixture.store.configuration().data();var stale=RaftConfigurationProto.parseFrom(source).toBuilder();stale.setPeers(0,stale.getPeers(0).toBuilder().setAddress("127.0.0.1:9999").build());byte[] target=RatisRelocationDataClient.targetConfiguration(fixture.definition.targets().getFirst(),stale.build().toByteArray());assertEquals(fixture.donor.address(),RaftConfigurationProto.parseFrom(target).getPeers(0).getAddress());
            var record=new RatisTopologyRelocationRecord(RatisTopologyRelocationRecord.Action.AUTHORIZE,fixture.definition,new UUID(91,1),new ApplyPosition(1,8),source,target);assertEquals(record,RatisTopologyRelocationRecord.decode(record.encode()));var copy=record.sourceConfiguration();copy[0]^=1;assertArrayEquals(source,record.sourceConfiguration());
            var trailing=Arrays.copyOf(record.encode(),record.encode().length+1);assertThrows(IllegalArgumentException.class,()->RatisTopologyRelocationRecord.decode(trailing));var unknown=record.encode();unknown[4]=(byte)255;assertThrows(IllegalArgumentException.class,()->RatisTopologyRelocationRecord.decode(unknown));assertThrows(IllegalArgumentException.class,()->new RatisTopologyRelocationRecord(record.action(),record.definition(),record.catalogue(),record.catalogueCut(),target,source));
        }
    }
    @Test void blockedAuthorityReadsStayOwnedThroughCloseAndRespectSixteenCallBound()throws Exception{
        try(var fixture=new RatisTopologySourceSnapshotFixture(root)){
            var definition=fixture.definition;var catalogue=new UUID(92,1);var entered=new java.util.concurrent.CountDownLatch(16);var release=new java.util.concurrent.CountDownLatch(1);var authorityCalls=new AtomicInteger();var control=mock(RaftClient.class);var io=mock(BlockingApi.class);when(control.getGroupId()).thenReturn(RaftGroupId.valueOf(catalogue));when(control.io()).thenReturn(io);
            when(io.sendReadOnly(any())).thenAnswer(call->{authorityCalls.incrementAndGet();entered.countDown();if(!release.await(5,java.util.concurrent.TimeUnit.SECONDS))throw new IOException("Authority fixture timeout");return reply(catalogue,RatisTopologyJoinAuthority.reply(new RatisTopologyJoinAuthority.View(catalogue,new ApplyPosition(1,8),definition,TopologyChange.State.begin(definition.header()).beginJoining())));});
            var authority=new RatisTopologyJoinAuthority.Client(control,definition.header().clusterId(),definition.header().databaseId(),catalogue);var client=mock(RaftClient.class);when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(fixture.group));var admin=mock(AdminApi.class);when(client.admin()).thenReturn(admin);when(admin.setConfiguration(any(SetConfigurationRequest.Arguments.class))).thenReturn(reply(fixture.group,new byte[0]));var source=fixture.store.configuration().data();var target=RatisRelocationDataClient.targetConfiguration(definition.targets().getFirst(),source);var data=new RatisRelocationDataClient(definition.header().clusterId(),definition.header().databaseId(),catalogue,r->client,(r,o)->client);
            try(var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()){
                var futures=new ArrayList<java.util.concurrent.Future<?>>();for(int i=0;i<16;i++)futures.add(executor.submit(()->{data.compareAndSet(definition,source,target,authority);return null;}));
                try{assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS));assertThrows(IOException.class,()->data.compareAndSet(definition,source,target,authority));assertEquals(16,authorityCalls.get());assertThrows(IOException.class,data::close);verify(client,never()).close();}finally{release.countDown();}
                for(var future:futures)future.get(5,java.util.concurrent.TimeUnit.SECONDS);data.close();verify(client).close();
            }finally{release.countDown();data.close();}
        }
    }

}
