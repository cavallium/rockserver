package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.*;
import org.apache.ratis.client.api.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RatisRangeImportFileClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void exactStreamingRequestUsesBoundedSequentialChunksAndRejectsChangedSourceOrReply() throws Exception {
        byte[] bytes=new byte[3*RatisRangeImportFileClient.BUFFER_BYTES+17]; new Random(1).nextBytes(bytes); Path source=root.resolve("immutable"); Files.write(source,bytes);
        UUID group=UUID.randomUUID(),column=UUID.randomUUID(),domain=UUID.randomUUID(),job=UUID.randomUUID(); var clientId=ClientId.randomId();
        var request=new RatisReferencedFile.Request(group,clientId.getUuid(),71,bytes.length,MessageDigest.getInstance("SHA-256").digest(bytes));
        var manifest=new NativeSstImportManifest(job,group,1,domain,new byte[32],null,null,List.of(new NativeSstImportManifest.Column(column,"data",false,List.of(new NativeSstImportManifest.File(UUID.randomUUID(),bytes.length,request.sha256(),1,new byte[]{1},new byte[]{1})))));
        var owner=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"leader","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var directory=new RangeDirectory(UUID.randomUUID(),UUID.randomUUID(),domain,0,new byte[32],0,Set.of(column),0,List.of(new RangeDescriptor(UUID.randomUUID(),group,1,0,null,null,RangeDescriptor.TopologyState.FENCED,List.of(owner))));
        var definition=new RangeImportDefinition(job,new OperationId(UUID.randomUUID(),1,0),directory,List.of(manifest));
        var client=mock(RaftClient.class); var control=mock(BlockingApi.class); var streams=mock(DataStreamRpcApi.class); var output=mock(DataStreamOutput.class);
        when(client.getId()).thenReturn(clientId); when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(group)); when(client.getLeaderId()).thenReturn(RaftPeerId.valueOf("leader")); when(client.io()).thenReturn(control);
        var recovered=new java.util.concurrent.atomic.AtomicReference<RatisReferencedFile>();
        when(control.sendReadOnly(any())).thenAnswer(call->{Message requestMessage=call.getArgument(0);byte[] content=requestMessage.getContent().toByteArray();
            if(ScopedTransactionRpc.kind(content,1024)==RatisRangeImportFileResultRpc.KIND)requestMessage=Message.valueOf(ByteString.copyFrom(RatisRangeImportFileResultRpc.reply(request,new ApplyPosition(2,recovered.get()==null?89:90),recovered.get())));
            return new RaftClientReply.Builder().setClientId(clientId).setServerId(client.getLeaderId()).setGroupId(client.getGroupId()).setCallId(1).setSuccess(true).setMessage(requestMessage).build();});
        when(streams.stream(any(RaftClientRequest.class))).thenAnswer(call->{RaftClientRequest header=call.getArgument(0);assertEquals(request,RatisReferencedFile.Request.decode(header.getMessage().getContent().toByteArray()));assertEquals(71,header.getCallId());assertEquals(clientId,header.getClientId());return output;});
        var collected=new java.io.ByteArrayOutputStream(); var maximum=new java.util.concurrent.atomic.AtomicInteger();
        when(output.writeAsync(any(ByteBuffer.class),any(Iterable.class))).thenAnswer(call->{ByteBuffer chunk=call.getArgument(0);int count=chunk.remaining();maximum.accumulateAndGet(count,Math::max);byte[] part=new byte[count];chunk.get(part);collected.writeBytes(part);DataStreamReply reply=mock(DataStreamReply.class);when(reply.isSuccess()).thenReturn(true);when(reply.getBytesWritten()).thenReturn((long)count);return CompletableFuture.completedFuture(reply);});
        var close=mock(DataStreamReply.class); when(close.isSuccess()).thenReturn(true); when(output.closeAsync()).thenReturn(CompletableFuture.completedFuture(close));
        var reference=new RatisReferencedFile(group,2,90,clientId.getUuid(),71,bytes.length,request.sha256());
        var reply=new RaftClientReply.Builder().setClientId(clientId).setServerId(client.getLeaderId()).setGroupId(client.getGroupId()).setCallId(71).setLogIndex(90).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(reference.encode()))).build();
        when(output.getRaftClientReplyFuture()).thenReturn(CompletableFuture.completedFuture(reply));
        var transfer=new RatisRangeImportFileClient(client,streams); assertEquals(reference,transfer.transfer(definition,request,source));assertArrayEquals(bytes,collected.toByteArray());assertEquals(RatisRangeImportFileClient.BUFFER_BYTES,maximum.get());assertEquals(0,transfer.activeTransfers());
        verify(output,times(4)).writeAsync(any(ByteBuffer.class),any(Iterable.class));
        recovered.set(reference);Files.delete(source);assertEquals(reference,transfer.transfer(definition,request,source));verify(streams,times(1)).stream(any(RaftClientRequest.class));recovered.set(null);
        Files.write(source,new byte[bytes.length]); assertThrows(java.io.IOException.class,()->transfer.transfer(definition,request,source));verify(streams,times(1)).stream(any(RaftClientRequest.class));assertEquals(0,transfer.activeTransfers());
        Files.write(source,bytes); var wrong=new RaftClientReply.Builder().setClientId(clientId).setServerId(client.getLeaderId()).setGroupId(client.getGroupId()).setCallId(72).setLogIndex(90).setSuccess(true).setMessage(reply.getMessage()).build();
        when(output.getRaftClientReplyFuture()).thenReturn(CompletableFuture.completedFuture(wrong));assertThrows(java.io.IOException.class,()->transfer.transfer(definition,request,source));assertEquals(0,transfer.activeTransfers());
        when(output.getRaftClientReplyFuture()).thenReturn(CompletableFuture.completedFuture(reply));
        var pending=new CompletableFuture<DataStreamReply>(); var entered=new CountDownLatch(1); var borrowed=new java.util.concurrent.atomic.AtomicReference<ByteBuffer>();
        when(output.writeAsync(any(ByteBuffer.class),any(Iterable.class))).thenAnswer(call->{borrowed.set(call.getArgument(0));entered.countDown();return pending;});
        var outcome=new java.util.concurrent.atomic.AtomicReference<Throwable>();
        var worker=new Thread(()->{try{transfer.transfer(definition,request,source);}catch(Throwable failure){outcome.set(failure);}});
        worker.start();
        try {
            assertTrue(entered.await(5,TimeUnit.SECONDS)); worker.interrupt(); assertEquals(1,transfer.activeTransfers()); assertEquals(RatisRangeImportFileClient.BUFFER_BYTES,borrowed.get().remaining());
            assertTrue(worker.isAlive()); var written=mock(DataStreamReply.class); when(written.isSuccess()).thenReturn(true);when(written.getBytesWritten()).thenReturn((long)RatisRangeImportFileClient.BUFFER_BYTES);pending.complete(written);
            worker.join(5000); assertFalse(worker.isAlive()); assertInstanceOf(java.io.IOException.class,outcome.get());assertEquals(0,transfer.activeTransfers());
        } finally { pending.completeExceptionally(new java.io.IOException("Fixture drain"));worker.join(5000); }
    }
}
