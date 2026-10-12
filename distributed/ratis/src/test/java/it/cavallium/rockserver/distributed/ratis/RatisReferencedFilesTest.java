package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisReferencedFilesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static LogEntryProto entry(UUID group,UUID client,long call,long index,byte[] bytes,boolean inline) throws Exception {
        var request=new RatisReferencedFile.Request(group,client,call,bytes.length,MessageDigest.getInstance("SHA-256").digest(bytes));
        var data=StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(request.encode())).setClientId(ClientId.valueOf(client).toByteString()).setCallId(call);
        data.setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(inline ? ByteString.copyFrom(bytes) : ByteString.EMPTY));
        return LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(data).build();
    }
    @Test void forcedWriteRestoresOriginalPayloadAndExactDescriptorAfterReopen() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3,4}; var original=entry(group,client,5,1,payload,true); var stripped=entry(group,client,5,1,payload,false); var path=root.resolve("group"); RatisReferencedFile reference;
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,RatisReferencedFiles.Limits.defaults(),executor)) {
            owner.write(original).get(10,TimeUnit.SECONDS); owner.flush(1).get(10,TimeUnit.SECONDS); reference=owner.reference(1,1);
            assertEquals(group,reference.groupId()); assertEquals(payload.length,reference.length()); assertEquals(reference,RatisReferencedFile.decode(reference.encode())); assertArrayEquals(payload,owner.read(stripped).get(10,TimeUnit.SECONDS).toByteArray());
            owner.drain().get(10,TimeUnit.SECONDS);
        }
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,RatisReferencedFiles.Limits.defaults(),executor)) {
            assertEquals(reference,owner.reference(1,1)); assertArrayEquals(payload,owner.read(stripped).get(10,TimeUnit.SECONDS).toByteArray()); owner.drain().get(10,TimeUnit.SECONDS);
        }
    }
    @Test void publicationObserverCancellationCannotReleaseActualFlushBarrier() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={9,8,7}; var entry=entry(group,client,6,1,payload,true); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(root.resolve("cancel"),group,RatisReferencedFiles.Limits.defaults(),executor)) {
            owner.publicationObserver(stage -> { if(stage==RatisReferencedFiles.PublishStage.CONTENT_PUBLISHED) { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new java.io.IOException("Barrier timeout"); } catch(InterruptedException interrupted) { throw new java.io.IOException(interrupted); } } });
            var observer=owner.write(entry); assertTrue(entered.await(10,TimeUnit.SECONDS)); var flush=owner.flush(1); observer.cancel(false); assertFalse(flush.isDone()); assertThrows(java.io.IOException.class,owner::close);
            release.countDown(); flush.get(10,TimeUnit.SECONDS); owner.drain().get(10,TimeUnit.SECONDS); assertArrayEquals(payload,owner.read(entry).get(10,TimeUnit.SECONDS).toByteArray()); owner.drain().get(10,TimeUnit.SECONDS);
        } finally { release.countDown(); }
    }
    static final class ControlledExecutor implements java.util.concurrent.Executor {
        final ArrayDeque<Runnable> jobs=new ArrayDeque<>(); boolean reject;
        public void execute(Runnable job) { if(reject) throw new RejectedExecutionException("Controlled I/O rejection"); jobs.add(job); }
        void run() { jobs.remove().run(); }
        void runAll() { while(!jobs.isEmpty()) run(); }
    }
    static org.apache.ratis.protocol.RaftClientRequest request(UUID group,UUID client,long call,byte[] bytes) throws Exception {
        var metadata=new RatisReferencedFile.Request(group,client,call,bytes.length,MessageDigest.getInstance("SHA-256").digest(bytes));
        return org.apache.ratis.protocol.RaftClientRequest.newBuilder().setClientId(ClientId.valueOf(client)).setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf("file-owner-test"))
                .setGroupId(org.apache.ratis.protocol.RaftGroupId.valueOf(group)).setCallId(call).setType(org.apache.ratis.protocol.RaftClientRequest.dataStreamRequestType()).setMessage(org.apache.ratis.protocol.Message.valueOf(ByteString.copyFrom(metadata.encode()))).build();
    }
    @Test void rejectedTruncateDoesNotKeepItsFlagAndRetryAfterHeldReadSucceeds() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3}; var entry=entry(group,client,1,1,payload,true); var executor=new ControlledExecutor();
        try(var owner=new RatisReferencedFiles(root.resolve("truncate-reject"),group,new RatisReferencedFiles.Limits(8,2,1,16,64),executor)) {
            var written=owner.write(entry); executor.run(); written.get(); var read=owner.read(entry); assertEquals(1,executor.jobs.size());
            assertThrows(ExecutionException.class,() -> owner.truncate(1).get()); assertThrows(java.io.IOException.class,owner::close);
            executor.run(); assertArrayEquals(payload,read.get().toByteArray());
            executor.reject=true; assertThrows(ExecutionException.class,() -> owner.truncate(1).get()); executor.reject=false;
            var retry=owner.truncate(1); executor.run(); retry.get(); assertThrows(java.io.IOException.class,() -> owner.reference(1,1)); owner.drain().get();
        }
    }
    @Test void truncatePinsSharedContentUntilLastReaderAndChargesItAcrossNewAdmission() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] shared={1,2,3},replacement={4,5,6}; var path=root.resolve("shared");
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,2,4,3,3),executor)) {
            owner.write(entry(group,client,1,1,shared,true)).get(); owner.write(entry(group,client,2,2,shared,true)).get();
            var reference=owner.reference(1,2); try(var reader=owner.pin(reference)) {
                owner.truncate(2).get(); try(var files=Files.list(path.resolve("content"))) { assertEquals(1,files.count()); } assertArrayEquals(shared,reader.read().toByteArray());
                owner.truncate(1).get(); assertArrayEquals(shared,reader.read().toByteArray()); assertThrows(java.io.IOException.class,owner::close);
                assertThrows(ExecutionException.class,() -> owner.write(entry(group,client,3,3,replacement,true)).get());
            }
            owner.truncate(3).get(); owner.write(entry(group,client,3,3,replacement,true)).get(); owner.drain().get();
        }
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,2,4,3,3),executor)) {
            assertThrows(java.io.IOException.class,() -> owner.reference(1,1)); assertThrows(java.io.IOException.class,() -> owner.reference(1,2)); assertArrayEquals(replacement,owner.read(entry(group,client,3,3,replacement,false)).get().toByteArray()); owner.drain().get();
        }
    }
    @Test void corruptOversizedReadAndCorruptReopenFailClosedWithinSmallBounds() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3}; var path=root.resolve("corrupt"); var limits=new RatisReferencedFiles.Limits(8,2,4,8,32); Path content;
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,limits,executor)) {
            var entry=entry(group,client,1,1,payload,true); owner.write(entry).get(); var reference=owner.reference(1,1); content=path.resolve("content").resolve(HexFormat.of().formatHex(reference.sha256())+".data");
            Files.write(content,new byte[16]); assertThrows(ExecutionException.class,() -> owner.read(entry).get()); owner.drain().get();
        }
        try(var executor=Executors.newSingleThreadExecutor()) { assertThrows(java.io.IOException.class,() -> new RatisReferencedFiles(path,group,limits,executor)); }
        Files.write(content,new byte[]{3,2,1}); try(var executor=Executors.newSingleThreadExecutor()) { assertThrows(java.io.IOException.class,() -> new RatisReferencedFiles(path,group,limits,executor)); }
        Files.delete(content); try(var executor=Executors.newSingleThreadExecutor()) { assertThrows(java.io.IOException.class,() -> new RatisReferencedFiles(path,group,limits,executor)); }
    }
    @Test void storedVerificationRetainsActualOwnershipAndRejectsChangedBytesAcrossBufferBoundaries() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID();byte[] payload=new byte[65537];Arrays.fill(payload,(byte)7);
        var executor=new ControlledExecutor();var path=root.resolve("stored-verification");
        var limits=new RatisReferencedFiles.Limits(8,2,4,payload.length,2L*payload.length);var entry=entry(group,client,1,1,payload,true);
        try(var owner=new RatisReferencedFiles(path,group,limits,executor)) {
            var first=owner.write(entry);executor.runAll();first.get();owner.drain().get();
        }
        try(var owner=new RatisReferencedFiles(path,group,limits,executor)) {
            var cancelled=owner.write(entry);cancelled.cancel(false);assertThrows(java.io.IOException.class,owner::close);
            executor.runAll();owner.drain().get();
            try(var reader=owner.pin(owner.reference(1,1))){reader.verify();}
        }
        try(var owner=new RatisReferencedFiles(path,group,limits,executor)) {
            var reference=owner.reference(1,1);var retry=owner.write(entry);payload[payload.length-1]^=1;
            Files.write(path.resolve("content").resolve(HexFormat.of().formatHex(reference.sha256())+".data"),payload);
            executor.runAll();assertThrows(ExecutionException.class,retry::get);owner.drain().get();
        }
    }
    @Test void canceledCreationAndExecutorRejectionReleaseUnexposedStreamOwnership() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3}; var executor=new ControlledExecutor(); var path=root.resolve("unexposed");
        try(var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,1,2,8,32),executor)) {
            var observer=owner.stream(request(group,client,1,payload)); observer.cancel(false); executor.reject=true; executor.run(); assertThrows(java.io.IOException.class,owner::close);
            executor.reject=false; var drained=owner.drain(); executor.runAll(); drained.get();
            executor.reject=true; assertThrows(ExecutionException.class,() -> owner.stream(request(group,client,2,payload)).get()); executor.reject=false;
            var created=owner.stream(request(group,client,3,payload)); executor.run(); var stream=created.get(); var cleaned=stream.cleanUp(); executor.runAll(); cleaned.get(); owner.drain().get();
        }
    }
    @Test void duplicateStreamsReleaseTheirIncomingOwnershipAfterCanonicalPublication() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3}; var path=root.resolve("duplicates");
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,2,4,8,32),executor)) {
            var one=owner.stream(request(group,client,1,payload)).get(); var two=owner.stream(request(group,client,1,payload)).get();
            for(var stream:List.of(one,two)) { var bytes=java.nio.ByteBuffer.wrap(payload); while(bytes.hasRemaining()) stream.getDataChannel().write(bytes); stream.getDataChannel().close(); }
            var entry=entry(group,client,1,1,payload,false); owner.link(one,entry).get(); owner.link(two,entry).get(); owner.flush(1).get(); owner.drain().get();
            try(var files=Files.list(path.resolve("incoming"))) { assertEquals(0,files.count()); }
            var next=owner.stream(request(group,client,2,payload)).get(); next.cleanUp().get(); owner.drain().get();
        }
    }
    @Test void replacementIsRegisteredWithoutBlockingBehindExactTruncateCompletion() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] old={1,2,3},replacement={4,5,6}; var executor=new ControlledExecutor();
        try(var owner=new RatisReferencedFiles(root.resolve("replacement"),group,new RatisReferencedFiles.Limits(8,2,4,8,32),executor)) {
            var first=owner.write(entry(group,client,1,1,old,true)); executor.run(); first.get();
            var truncated=owner.truncate(1); var secondEntry=entry(group,client,2,1,replacement,true).toBuilder().setTerm(2).build(); var second=owner.write(secondEntry); var flush=owner.flush(1);
            assertFalse(truncated.isDone()); assertFalse(second.isDone()); assertFalse(flush.isDone()); assertThrows(java.io.IOException.class,owner::close);
            executor.run(); truncated.get(); assertFalse(second.isDone()); executor.runAll(); second.get(); flush.get(); assertEquals(2,owner.reference(2,1).term());
            var read=owner.read(secondEntry); executor.run(); assertArrayEquals(replacement,read.get().toByteArray()); owner.drain().get();
        }
    }

    @Test void publicationFenceStillAllowsExistingStreamCleanupAndActualDrain() throws Exception {
        UUID group=UUID.randomUUID(),client=UUID.randomUUID(); byte[] payload={1,2,3}; var path=root.resolve("fenced-cleanup");
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,2,4,8,32),executor)) {
            var unlinked=owner.stream(request(group,client,1,payload)).get();
            owner.publicationObserver(stage -> { if(stage==RatisReferencedFiles.PublishStage.CONTENT_PUBLISHED) throw new java.io.IOException("Controlled uncertain publication"); });
            assertThrows(ExecutionException.class,() -> owner.write(entry(group,client,2,1,payload,true)).get());
            assertThrows(ExecutionException.class,() -> owner.flush(1).get()); assertThrows(java.io.IOException.class,owner::close);
            unlinked.cleanUp().get(); owner.drain().get();
        }
        try(var executor=Executors.newSingleThreadExecutor(); var owner=new RatisReferencedFiles(path,group,new RatisReferencedFiles.Limits(8,2,4,8,32),executor)) {
            assertThrows(java.io.IOException.class,() -> owner.reference(1,1)); try(var files=Files.list(path.resolve("content"))) { assertEquals(0,files.count()); }
        }
    }

}
