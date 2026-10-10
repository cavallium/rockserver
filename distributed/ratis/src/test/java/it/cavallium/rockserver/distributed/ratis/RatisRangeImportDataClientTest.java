package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.nio.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.*;
import org.apache.ratis.client.api.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RatisRangeImportDataClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void fixedPlacementStableTransferIdentityAndFrozenNativeRecordsUseOwnedBoundedClients() throws Exception {
        UUID cluster=UUID.randomUUID(),database=UUID.randomUUID(),catalogue=UUID.randomUUID();
        var definition=RatisRangeImportCatalogueTest.definition(cluster,database,new OperationId(UUID.randomUUID(),0,1));
        var manifest=definition.manifests().getFirst();var range=definition.sourceDirectory().ranges().getFirst();var voter=range.replicas().stream().filter(r->r.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow();
        var binding=new RatisNodePlan.Catalogue(catalogue,1,new byte[32],UUID.randomUUID(),UUID.randomUUID(),List.of(voter));
        var plan=new RatisNodePlan(voter.nodeId(),voter.nodeIncarnation(),definition.sourceDirectory(),binding,manifest.physicalNames(),Set.of(),List.of(),List.of(definition),Map.of("owner1","127.0.0.1:9101","owner2","127.0.0.1:9102"),2);
        var catalogueClient=mock(RaftClient.class);when(catalogueClient.getGroupId()).thenReturn(RaftGroupId.valueOf(catalogue));
        UUID identity=UUID.randomUUID();var request=new RatisReferencedFile.Request(range.groupId(),identity,9,manifest.encode().length,manifest.digest());
        var reference=new RatisReferencedFile(range.groupId(),1,1,identity,9,request.length(),request.sha256());var recovered=new AtomicReference<RatisReferencedFile>();
        var receipt=new NativeSstImportManifest.Receipt(definition.importId(),range.groupId(),range.rangeEpoch(),manifest.digest(),new ApplyPosition(1,3));
        var publication=new RatisRangeImportRecord.Publication(catalogue,new ApplyPosition(2,20),RatisRangeImportCatalogue.Head.prepared(definition).publish(new byte[32]));
        var activation=new NativeRangeSstImport.Activation(receipt,catalogue,publication.completedCatalogueCut(),publication.digest(),new ApplyPosition(1,4));
        var opened=new ArrayList<ClientId>();var closes=new AtomicInteger();var streamsClosed=new AtomicInteger();
        RatisRangeImportDataClient.Connections connections=(frozen,id)->{
            assertEquals(range,frozen);opened.add(id);var client=mock(RaftClient.class);var io=mock(BlockingApi.class);when(client.getId()).thenReturn(id);when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(range.groupId()));when(client.getLeaderId()).thenReturn(RaftPeerId.valueOf(voter.peerId()));when(client.io()).thenReturn(io);
            when(io.sendReadOnly(any(Message.class))).thenAnswer(call->{Message message=call.getArgument(0);byte[] bytes=message.getContent().toByteArray();
                if(ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES)==RatisRangeImportFileResultRpc.KIND)message=Message.valueOf(ByteString.copyFrom(RatisRangeImportFileResultRpc.reply(request,new ApplyPosition(1,recovered.get()==null?0:1),recovered.get())));
                return new RaftClientReply.Builder().setClientId(id).setServerId(client.getLeaderId()).setGroupId(client.getGroupId()).setCallId(1).setSuccess(true).setMessage(message).build();});
            when(io.send(any(Message.class))).thenAnswer(call->{var record=RatisRangeImportRecord.decode(((Message)call.getArgument(0)).getContent().toByteArray());byte[] result=record instanceof RatisRangeImportRecord.Install?receipt.encode():activation.encode();return new RaftClientReply.Builder().setClientId(id).setServerId(client.getLeaderId()).setGroupId(client.getGroupId()).setCallId(1).setLogIndex(4).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(result))).build();});
            doAnswer(call->{closes.incrementAndGet();return null;}).when(client).close();return client;
        };
        RatisRangeImportDataClient.Streams streams=(control,primary)->{
            assertEquals(ClientId.valueOf(identity),control.getId());assertEquals(RaftPeerId.valueOf(voter.peerId()),primary);var transport=mock(DataStreamClient.class);var output=mock(DataStreamOutput.class);
            when(transport.stream(any(RaftClientRequest.class))).thenAnswer(call->{RaftClientRequest header=call.getArgument(0);assertEquals(request,RatisReferencedFile.Request.decode(header.getMessage().getContent().toByteArray()));return output;});
            when(output.writeAsync(any(ByteBuffer.class),any(Iterable.class))).thenAnswer(call->{int count=((ByteBuffer)call.getArgument(0)).remaining();var written=mock(DataStreamReply.class);when(written.isSuccess()).thenReturn(true);when(written.getBytesWritten()).thenReturn((long)count);return CompletableFuture.completedFuture(written);});
            var closed=mock(DataStreamReply.class);when(closed.isSuccess()).thenReturn(true);when(output.closeAsync()).thenReturn(CompletableFuture.completedFuture(closed));
            var finalReply=new RaftClientReply.Builder().setClientId(control.getId()).setServerId(primary).setGroupId(control.getGroupId()).setCallId(9).setLogIndex(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reference.encode()))).build();
            when(output.getRaftClientReplyFuture()).thenReturn(CompletableFuture.completedFuture(finalReply));
            doAnswer(call->{streamsClosed.incrementAndGet();return null;}).when(transport).close();return transport;
        };
        var owners=new RatisRangeImportOwnerClient(cluster,database,catalogue,(frozen,owner)->{throw new AssertionError("Unused owner RPC");});
        try(var client=new RatisRangeImportDataClient(plan,catalogueClient,connections,streams,owners)) {
            Path source=root.resolve("manifest");Files.write(source,manifest.encode());assertEquals(reference,client.transfer(definition,request,source));recovered.set(reference);
            assertEquals(reference,client.transfer(definition,request,root.resolve("absent")));assertEquals(1,opened.size());assertEquals(1,streamsClosed.get());
            var install=new RatisRangeImportRecord.Install(definition.importId(),range.groupId(),range.rangeEpoch(),definition.digest(),reference,Map.of());
            assertArrayEquals(receipt.encode(),client.install(definition,install).encode());assertArrayEquals(receipt.encode(),client.install(definition,install).encode());
            assertArrayEquals(activation.encode(),client.activate(definition,new RatisRangeImportRecord.Activate(receipt,publication)).encode());assertEquals(2,opened.size());
        }
        assertEquals(2,closes.get());
    }
}
