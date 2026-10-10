package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Released segmented-log hook ordering; no server, network, or quorum is started. */
class RatisReferencedFilesLogOrderingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    static final class LogOwner implements AutoCloseable {
        final RaftStorage storage;
        final SegmentedRaftLog log;
        final ConcurrentHashMap<ClientInvocationId,CompletableFuture<StateMachine.DataStream>> streams=new ConcurrentHashMap<>();
        LogOwner(Path root,RaftGroupId group,StateMachine.DataApi data) throws Exception {
            storage=RaftStorage.newBuilder().setDirectory(root.toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(SizeInBytes.valueOf(0)).build();
            storage.initialize();
            var member=RaftGroupMemberId.valueOf(RaftPeerId.valueOf("local-file-test"),group);
            var info=mock(DivisionInfo.class);
            when(info.getFollowerNextIndices()).thenReturn(new long[0]);
            when(info.getLastAppliedIndex()).thenReturn(-1L);
            var division=mock(RaftServer.Division.class);
            when(division.getMemberId()).thenReturn(member);
            when(division.getId()).thenReturn(member.getPeerId());
            when(division.getInfo()).thenReturn(info);
            when(division.getThreadGroup()).thenReturn(Thread.currentThread().getThreadGroup());
            when(division.getDataStreamMap()).thenReturn(new DataStreamMap() {
                public CompletableFuture<StateMachine.DataStream> computeIfAbsent(ClientInvocationId id,java.util.function.Function<ClientInvocationId,CompletableFuture<StateMachine.DataStream>> create) { return streams.computeIfAbsent(id,create); }
                public CompletableFuture<StateMachine.DataStream> remove(ClientInvocationId id) { return streams.remove(id); }
            });
            var machine=new BaseStateMachine() { @Override public StateMachine.DataApi data() { return data; } };
            var properties=RatisDurability.properties(root);
            RaftServerConfigKeys.Log.setForceSyncNum(properties,1);
            log=SegmentedRaftLog.newBuilder().setMemberId(member).setServer(division).setStateMachine(machine)
                    .setNotifyTruncatedLogEntry(entry -> {}).setGetTransactionContext((entry,create) -> null)
                    .setSubmitUpdateCommitEvent(() -> {}).setStorage(storage).setSnapshotIndexSupplier(() -> -1L)
                    .setProperties(properties).build();
            log.open(-1,entry -> {});
        }
        CompletableFuture<Long> append(ByteString command,StateMachine.DataStream stream) {
            return append(command,stream,ClientId.randomId(),1);
        }
        CompletableFuture<Long> append(ByteString command,StateMachine.DataStream stream,ClientId client,long call) {
            streams.put(ClientInvocationId.valueOf(client,call),CompletableFuture.completedFuture(stream));
            var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(0).setStateMachineLogEntry(
                    StateMachineLogEntryProto.newBuilder().setType(StateMachineLogEntryProto.Type.DATASTREAM)
                            .setClientId(client.toByteString()).setCallId(call).setLogData(command)
                            .setStateMachineEntry(StateMachineEntryProto.getDefaultInstance())).build();
            var futures=log.append(List.of(entry));
            assertEquals(1,futures.size()); return futures.getFirst();
        }
        List<CompletableFuture<Long>> appendInline(RatisReferencedFile.Request request,byte[] bytes,long term) {
            return log.append(List.of(LogEntryProto.newBuilder().setTerm(term).setIndex(0).setStateMachineLogEntry(
                    StateMachineLogEntryProto.newBuilder().setType(StateMachineLogEntryProto.Type.WRITE)
                            .setClientId(ClientId.valueOf(request.clientId()).toByteString()).setCallId(request.callId())
                            .setLogData(ByteString.copyFrom(request.encode())).setStateMachineEntry(
                                    StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFrom(bytes)))).build()));
        }
        @Override public void close() throws Exception { try { log.close(); } finally { storage.close(); } }
    }

    @Test void releasedStreamWrapperDoesNotAwaitNestedLinkFuture() throws Exception {
        var linked=new CompletableFuture<Void>(); var entered=new CountDownLatch(1);
        var data=new StateMachine.DataApi() {
            @Override public CompletableFuture<?> link(StateMachine.DataStream stream,LogEntryProto entry) { entered.countDown(); return linked; }
        };
        try(var owner=new LogOwner(root,RaftGroupId.randomId(),data)) {
            var append=owner.append(ByteString.copyFromUtf8("controlled-stream"),mock(StateMachine.DataStream.class));
            assertTrue(entered.await(5,TimeUnit.SECONDS));
            assertEquals(0L,append.get(5,TimeUnit.SECONDS));
            assertEquals(0,owner.log.getFlushIndex());
            assertFalse(linked.isDone(),"The negative control must expose the released nested-future ordering");
        } finally { linked.complete(null); }
    }

    @Test void realFileOwnerFlushBlocksAppendThroughDirectoryAndReferencePublication() throws Exception {
        var group=RaftGroupId.randomId(); var client=ClientId.randomId(); long call=7;
        byte[] payload="immutable streamed file".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        var request=new RatisReferencedFile.Request(group.getUuid(),client.getUuid(),call,payload.length,
                java.security.MessageDigest.getInstance("SHA-256").digest(payload));
        var executor=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(2),
                Thread.ofPlatform().daemon(true).name("referenced-file-log-test").factory(),new ThreadPoolExecutor.AbortPolicy());
        var published=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var files=new RatisReferencedFiles(root.resolve("files"),group.getUuid(),RatisReferencedFiles.Limits.defaults(),executor)) {
            files.publicationObserver(stage -> {
                if(stage==RatisReferencedFiles.PublishStage.CONTENT_PUBLISHED) {
                    published.countDown();
                    try { if(!release.await(10,TimeUnit.SECONDS)) throw new java.io.IOException("Publication barrier timed out"); }
                    catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.io.IOException(interrupted); }
                }
            });
            var command=ByteString.copyFrom(request.encode());
            var stream=files.stream(RaftClientRequest.newBuilder().setClientId(client).setServerId(RaftPeerId.valueOf("local-file-test"))
                    .setGroupId(group).setCallId(call).setType(RaftClientRequest.dataStreamRequestType()).setMessage(Message.valueOf(command)).build()).get(5,TimeUnit.SECONDS);
            var input=java.nio.ByteBuffer.wrap(payload);
            while(input.hasRemaining()) assertTrue(stream.getDataChannel().write(input)>0);
            stream.getDataChannel().close();
            try(var owner=new LogOwner(root.resolve("raft"),group,files)) {
                try {
                    var append=owner.append(command,stream,client,call);
                    assertTrue(published.await(5,TimeUnit.SECONDS));
                    assertFalse(append.isDone()); assertTrue(owner.log.getFlushIndex()<0);
                    assertFalse(files.flush(0).isDone());
                    assertThrows(java.io.IOException.class,files::close);
                    release.countDown(); assertEquals(0L,append.get(5,TimeUnit.SECONDS));
                    assertEquals(0,owner.log.getFlushIndex());
                    assertArrayEquals(payload,files.read(owner.log.get(0)).get(5,TimeUnit.SECONDS).toByteArray());
                    var restored=owner.log.getEntryWithData(0).getEntry(org.apache.ratis.util.TimeDuration.valueOf(5,TimeUnit.SECONDS));
                    assertArrayEquals(payload,restored.getStateMachineLogEntry().getStateMachineEntry().getStateMachineData().toByteArray());
                } finally { release.countDown(); files.drain().get(5,TimeUnit.SECONDS); }
            }
        } finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
    }

    @Test void realLogQueuesReplacementBehindItsAsynchronousSuffixTruncation() throws Exception {
        var group=RaftGroupId.randomId(); var client=ClientId.randomId(); byte[] before={1,2},after={3,4,5};
        var oldRequest=new RatisReferencedFile.Request(group.getUuid(),client.getUuid(),1,before.length,java.security.MessageDigest.getInstance("SHA-256").digest(before));
        var newRequest=new RatisReferencedFile.Request(group.getUuid(),client.getUuid(),2,after.length,java.security.MessageDigest.getInstance("SHA-256").digest(after));
        var executor=new ThreadPoolExecutor(1,1,0,TimeUnit.MILLISECONDS,new ArrayBlockingQueue<>(4),
                Thread.ofPlatform().daemon(true).name("referenced-file-replacement-test").factory(),new ThreadPoolExecutor.AbortPolicy());
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
        try(var files=new RatisReferencedFiles(root.resolve("replacement-files"),group.getUuid(),RatisReferencedFiles.Limits.defaults(),executor);
            var owner=new LogOwner(root.resolve("replacement-raft"),group,files)) {
            var first=owner.appendInline(oldRequest,before,1); assertEquals(1,first.size()); first.getFirst().get(5,TimeUnit.SECONDS);
            executor.execute(() -> { entered.countDown(); try { if(!release.await(10,TimeUnit.SECONDS)) throw new AssertionError("Truncate barrier timed out"); } catch(InterruptedException interrupted) { throw new AssertionError(interrupted); } });
            assertTrue(entered.await(5,TimeUnit.SECONDS));
            try {
                var replacement=owner.appendInline(newRequest,after,2); assertEquals(2,replacement.size(),"Real log must submit truncate then replacement");
                assertFalse(replacement.getFirst().isDone()); assertFalse(replacement.getLast().isDone());
                release.countDown(); for(var future:replacement) future.get(5,TimeUnit.SECONDS);
                files.flush(0).get(5,TimeUnit.SECONDS); assertEquals(2,files.reference(2,0).term());
                assertThrows(java.io.IOException.class,() -> files.reference(1,0));
                assertArrayEquals(after,files.read(owner.log.get(0)).get(5,TimeUnit.SECONDS).toByteArray());
            } finally { release.countDown(); files.drain().get(5,TimeUnit.SECONDS); }
        } finally { release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
    }
}
