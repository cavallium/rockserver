package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.RangeImportDefinition;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.*;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

/** Borrowed clients; caller retains the immutable source and exact request through uncertain outcomes. */
public final class RatisRangeImportFileClient {
    public static final int MAX_TRANSFERS=2,BUFFER_BYTES=64*1024;
    private final RaftClient client;
    private final DataStreamRpcApi streams;
    private final Semaphore slots=new Semaphore(MAX_TRANSFERS);
    public RatisRangeImportFileClient(RaftClient client,DataStreamRpcApi streams) {
        this.client=Objects.requireNonNull(client); this.streams=Objects.requireNonNull(streams);
    }
    public int activeTransfers() { return MAX_TRANSFERS-slots.availablePermits(); }
    public RatisReferencedFile transfer(RangeImportDefinition definition,RatisReferencedFile.Request request,Path immutableSource) throws IOException {
        RatisRangeImportRpc.requireFile(definition.manifest(request.groupId()),request);
        if(!client.getGroupId().getUuid().equals(request.groupId()) || !client.getId().getUuid().equals(request.clientId()))throw new IOException("File client group/identity differs from original request");
        if(!slots.tryAcquire())throw new IOException("File transfer capacity exhausted");
        DataStreamOutput stream=null; boolean drained=false,preparationAttempted=false; Throwable failure=null;
        try(var source=FileChannel.open(immutableSource,StandardOpenOption.READ,LinkOption.NOFOLLOW_LINKS)) {
            var buffer=ByteBuffer.allocate(BUFFER_BYTES); qualify(source,request,buffer);
            preparationAttempted=true; var prepared=client.io().sendReadOnly(message(RatisRangeImportRpc.prepare(request)));
            if(prepared==null || !prepared.isSuccess() || prepared.getMessage()==null || !client.getGroupId().equals(prepared.getRaftGroupId()) || prepared.getMessage().getContent().size()>RatisRangeImportRpc.MAX_BYTES)throw new IOException("File preparation failed",prepared==null ? null : prepared.getException());
            RatisRangeImportRpc.requireReply(prepared.getMessage().getContent().toByteArray(),ScopedTransactionRpc.IMPORT_APPLY_PREPARE,request);
            var leader=client.getLeaderId();
            if(leader==null || definition.sourceDirectory().ranges().stream().filter(range->range.groupId().equals(request.groupId())).flatMap(range->range.replicas().stream())
                    .noneMatch(owner->owner.peerId().equals(leader.toString()) && owner.role()==it.cavallium.rockserver.core.common.distributed.ReplicaDescriptor.Role.VOTER))throw new IOException("Stream target is not a frozen voter");
            var header=RaftClientRequest.newBuilder().setClientId(client.getId()).setServerId(leader).setGroupId(client.getGroupId()).setCallId(request.callId())
                    .setType(RaftClientRequest.dataStreamRequestType()).setMessage(message(request.encode())).build();
            stream=streams.stream(header); long remaining=request.length(); source.position(0);
            while(remaining>0) {
                buffer.clear().limit((int)Math.min(buffer.capacity(),remaining)); int count=source.read(buffer); if(count<0)throw new IOException("Immutable source truncated during transfer"); if(count==0)continue;
                buffer.flip(); var written=await(stream.writeAsync(buffer.asReadOnlyBuffer(),List.of()));
                if(written==null || !written.isSuccess() || written.getBytesWritten()!=count)throw new IOException("Stream chunk did not acknowledge exact bytes"); remaining-=count;
            }
            var closed=await(stream.closeAsync()); var reply=await(stream.getRaftClientReplyFuture()); drained=true;
            if(closed==null || !closed.isSuccess() || reply==null || !reply.isSuccess() || reply.getMessage()==null || reply.getMessage().getContent().size()!=RatisReferencedFile.BYTES
                    || !client.getGroupId().equals(reply.getRaftGroupId()) || !client.getId().equals(reply.getClientId()) || reply.getCallId()!=request.callId())throw new IOException("Streamed file outcome failed or crossed original identity",reply==null ? null : reply.getException());
            var reference=RatisReferencedFile.decode(reply.getMessage().getContent().toByteArray());
            if(!reference.request().equals(request) || reply.getLogIndex()!=reference.index())throw new IOException("File outcome differs from committed reference"); return reference;
        } catch(IOException|Error problem) { failure=problem; throw problem; }
        catch(RuntimeException problem) { var invalid=new IOException("Invalid streamed file response or transport failure",problem); failure=invalid; throw invalid; }
        finally {
            try {
                if(stream!=null && !drained) drain(stream);
                else if(stream==null && preparationAttempted && failure!=null) {
                    var cancelled=client.io().sendReadOnly(message(RatisRangeImportRpc.cancel(request)));
                    if(cancelled==null || !cancelled.isSuccess() || cancelled.getMessage()==null || !client.getGroupId().equals(cancelled.getRaftGroupId()) || cancelled.getMessage().getContent().size()>RatisRangeImportRpc.MAX_BYTES)throw new IOException("File preparation cancellation failed");
                    RatisRangeImportRpc.requireReply(cancelled.getMessage().getContent().toByteArray(),ScopedTransactionRpc.IMPORT_APPLY_CANCEL,request);
                }
            }
            catch(IOException|RuntimeException cleanup) { if(failure!=null)failure.addSuppressed(cleanup); else throw cleanup; }
            finally { slots.release(); }
        }
    }
    private static void drain(DataStreamOutput stream) throws IOException {
        IOException failure=null;
        try { await(stream.closeAsync()); } catch(IOException|RuntimeException problem) { failure=new IOException("Stream close failed",problem); }
        try { await(stream.getRaftClientReplyFuture()); } catch(IOException|RuntimeException problem) { if(failure==null)failure=new IOException("Stream outcome failed",problem);else failure.addSuppressed(problem); }
        if(failure!=null)throw failure;
    }
    private static void qualify(FileChannel source,RatisReferencedFile.Request request,ByteBuffer buffer) throws IOException {
        if(source.size()!=request.length())throw new IOException("Immutable source length differs"); final MessageDigest digest;
        try { digest=MessageDigest.getInstance("SHA-256"); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        while(true) { buffer.clear(); int count=source.read(buffer); if(count<0)break; buffer.flip(); digest.update(buffer); }
        if(!MessageDigest.isEqual(digest.digest(),request.sha256()))throw new IOException("Immutable source SHA differs");
    }
    /** Caller interruption never releases a buffer still borrowed by an actual stream write. */
    private static <T> T await(CompletableFuture<T> actual) throws IOException {
        boolean interrupted=false;
        try { while(true)try { var value=actual.get(); if(interrupted)throw new IOException("File transfer interrupted after actual I/O returned"); return value; }
            catch(InterruptedException stop) { interrupted=true; }
            catch(ExecutionException failed) { throw new IOException("File stream failed; retain original identity for outcome recovery",failed.getCause()); }
        } finally { if(interrupted)Thread.currentThread().interrupt(); }
    }
    private static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
}
