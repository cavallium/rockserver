package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RatisRangeImportOwnerClientTest {
    @Test void exactVoterAndListenerReadsProduceCompleteEvidenceAndRejectCrossedOwnerOrPublication() throws Exception {
        UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),domain=UUID.randomUUID(),group=UUID.randomUUID(),column=UUID.randomUUID(),job=UUID.randomUUID(),catalogue=UUID.randomUUID();
        var voter=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"voter","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var listener=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"listener","127.0.0.1:9998",ReplicaDescriptor.Role.LISTENER);
        var range=new RangeDescriptor(UUID.randomUUID(),group,1,7,null,null,RangeDescriptor.TopologyState.FENCED,List.of(voter,listener));
        var directory=new RangeDirectory(cluster,database,domain,0,new byte[32],0,Set.of(column),0,List.of(range));
        var manifest=new NativeSstImportManifest(job,group,1,domain,new byte[32],null,null,List.of(new NativeSstImportManifest.Column(column,"data",false,List.of())));
        var definition=new RangeImportDefinition(job,new OperationId(UUID.randomUUID(),1,0),directory,List.of(manifest));
        var receipt=new NativeSstImportManifest.Receipt(job,group,1,manifest.digest(),new ApplyPosition(1,3));var published=RatisRangeImportCatalogue.Head.prepared(definition).publish(new byte[32]);
        var installed=new RatisRangeImportApply.Status(group,1,definition.digest(),manifest.digest(),new ApplyPosition(1,3),RatisRangeImportApply.Phase.INSTALLED,receipt,null);
        var state=new AtomicReference<>(installed);var crossed=new AtomicBoolean();var routes=new ArrayList<ReplicaDescriptor>();var closes=new AtomicInteger();
        try(var client=new RatisRangeImportOwnerClient(cluster,database,catalogue,(frozen,owner)->{
            assertEquals(range,frozen); routes.add(owner);var connection=mock(RaftClient.class);var io=mock(BlockingApi.class);when(connection.io()).thenReturn(io);
            when(io.sendStaleRead(any(Message.class),eq(0L),eq(RaftPeerId.valueOf(owner.peerId())))).thenAnswer(call->{
                var target=RatisRangeImportStatusRpc.decodeRequest(((Message)call.getArgument(0)).getContent().toByteArray());assertEquals(owner,target.owner());target.requireBinding(definition,owner);
                return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(crossed.get()?"another":owner.peerId())).setGroupId(RaftGroupId.valueOf(group)).setCallId(1).setSuccess(true)
                        .setMessage(Message.valueOf(ByteString.copyFrom(RatisRangeImportStatusRpc.reply(target,state.get())))).build();
            });doAnswer(call->{closes.incrementAndGet();return null;}).when(connection).close();return connection;
        })) {
            var admissions=new RangeImportAdmission(definition,List.of(client.receipt(definition,range,voter),client.receipt(definition,range,listener)));
            assertEquals(2,admissions.owners().size());assertEquals(List.of(voter,listener),routes);
            var nativeActivation=new NativeRangeSstImport.Activation(receipt,catalogue,new ApplyPosition(2,10),RangeImportAdmission.sha256(published.encode()),new ApplyPosition(1,4));
            state.set(new RatisRangeImportApply.Status(group,1,definition.digest(),manifest.digest(),new ApplyPosition(1,4),RatisRangeImportApply.Phase.PUBLISHED,receipt,nativeActivation));
            var complete=new RangeImportActivationAdmission(definition,admissions,catalogue,RangeImportAdmission.sha256(published.encode()),List.of(client.activation(definition,published,range,voter),client.activation(definition,published,range,listener)));
            assertEquals(2,complete.owners().size());assertEquals(2,routes.size());
            crossed.set(true);assertThrows(java.io.IOException.class,()->client.receipt(definition,range,listener));crossed.set(false);
            byte[] changed=new byte[32];changed[0]=1;assertThrows(java.io.IOException.class,()->client.activation(definition,RatisRangeImportCatalogue.Head.prepared(definition).publish(changed),range,voter));
            state.set(new RatisRangeImportApply.Status(group,1,definition.digest(),manifest.digest(),new ApplyPosition(1,4),RatisRangeImportApply.Phase.FENCED,null,null));
            assertThrows(java.io.IOException.class,()->client.receipt(definition,range,listener));
        }
        assertEquals(2,closes.get());
    }
}
