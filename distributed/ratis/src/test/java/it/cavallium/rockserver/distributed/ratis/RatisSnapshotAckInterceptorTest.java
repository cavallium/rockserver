package it.cavallium.rockserver.distributed.ratis;

import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Protocol-boundary tests only; real file durability and remote recovery need process tests. */
class RatisSnapshotAckInterceptorTest {
    private static final RaftGroupId GROUP = RaftGroupId.randomId();

    @Test
    void finalSuccessIsForwardedOnlyAfterBarrierAndRepeatedIdentityIsSafe() {
        var events = new CopyOnWriteArrayList<String>();
        var count = new AtomicInteger();
        var interceptor = new RatisSnapshotAckInterceptor((group, term, index) -> {
            assertEquals(GROUP, group);
            assertEquals(3, term);
            assertEquals(9, index);
            events.add("sync");
            count.incrementAndGet();
        });
        var stream = new Stream(interceptor, events);
        stream.listener.onMessage(request(GROUP, 3, 9, 4, true));
        stream.listener.onMessage(request(GROUP, 3, 9, 4, true));
        var success = reply(GROUP, 4, RaftProtos.InstallSnapshotResult.SUCCESS);
        stream.reply(success);
        stream.reply(success);
        assertEquals(List.of("sync", "send", "send"), events);
        assertEquals(1, count.get());
        assertEquals(2, stream.received.get());
        assertEquals(List.of(success, success), stream.call.messages);
        stream.listener.onComplete();
    }

    @Test
    void nonfinalChunksNegativeRepliesAndUnrelatedMethodsPassUnchanged() {
        var count = new AtomicInteger();
        var interceptor = new RatisSnapshotAckInterceptor((group, term, index) -> count.incrementAndGet());
        var stream = new Stream(interceptor, new CopyOnWriteArrayList<>());
        stream.listener.onMessage(request(GROUP, 3, 9, 0, false));
        stream.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS));
        stream.listener.onMessage(request(GROUP, 3, 9, 1, true));
        stream.reply(reply(GROUP, 1, RaftProtos.InstallSnapshotResult.NOT_LEADER));
        assertEquals(2, stream.call.messages.size());
        assertEquals(0, count.get());
        var other = new FakeCall<>(RaftServerProtocolServiceGrpc.getAppendEntriesMethod(), new CopyOnWriteArrayList<>());
        var forwarded = new AtomicReference<ServerCall<RaftProtos.AppendEntriesRequestProto, RaftProtos.AppendEntriesReplyProto>>();
        interceptor.interceptCall(other, new Metadata(), (call, headers) -> {
            forwarded.set(call);
            return new ServerCall.Listener<>() {};
        });
        assertSame(other, forwarded.get());
        stream.listener.onComplete();
    }

    @Test
    void barrierFailureClosesWithGenericErrorAndSuppressesEveryLaterSuccess() {
        var stream = new Stream(new RatisSnapshotAckInterceptor((group, term, index) -> {
            throw new IOException("private filesystem path and other details");
        }), new CopyOnWriteArrayList<>());
        stream.listener.onMessage(request(GROUP, 3, 9, 0, true));
        stream.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS));
        stream.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS));
        assertTrue(stream.call.messages.isEmpty());
        assertEquals(Status.Code.INTERNAL, stream.call.status.get().getCode());
        assertEquals("Snapshot durability barrier failed", stream.call.status.get().getDescription());
        assertNull(stream.call.status.get().getCause());
        assertEquals(1, stream.call.closes.get());
    }

    @Test
    void changedFinalHeaderAndMismatchedReplyGroupAreRejected() {
        var interceptor = new RatisSnapshotAckInterceptor((group, term, index) -> fail("Must not sync mismatched metadata"));
        var stream = new Stream(interceptor, new CopyOnWriteArrayList<>());
        stream.listener.onMessage(request(GROUP, 3, 9, 0, true));
        stream.listener.onMessage(request(GROUP, 3, 10, 0, true));
        assertEquals(1, stream.received.get());
        assertEquals(Status.Code.INVALID_ARGUMENT, stream.call.status.get().getCode());
        stream.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS));
        assertTrue(stream.call.messages.isEmpty());
        var wrongReply = new Stream(interceptor, new CopyOnWriteArrayList<>());
        wrongReply.listener.onMessage(request(GROUP, 3, 9, 0, true));
        wrongReply.reply(reply(RaftGroupId.randomId(), 0, RaftProtos.InstallSnapshotResult.SUCCESS));
        assertEquals(Status.Code.INTERNAL, wrongReply.call.status.get().getCode());
        assertTrue(wrongReply.call.messages.isEmpty());
        var missingIndex = new Stream(interceptor, new CopyOnWriteArrayList<>());
        missingIndex.listener.onMessage(request(GROUP, 3, 9, 0, true));
        missingIndex.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS).toBuilder().clearRequestIndex().build());
        assertEquals(Status.Code.INTERNAL, missingIndex.call.status.get().getCode());
    }

    @Test
    void correlationMetadataIsBoundedAndCancellationCloseAndCompletionReleaseCapacity() {
        var interceptor = new RatisSnapshotAckInterceptor((group, term, index) -> {});
        var bounded = new Stream(interceptor, new CopyOnWriteArrayList<>());
        for (int i = 0; i < RatisSnapshotAckInterceptor.MAX_FINAL_REQUESTS; i++) {
            bounded.listener.onMessage(request(GROUP, 3, 9, i, true));
        }
        bounded.listener.onMessage(request(GROUP, 3, 9, RatisSnapshotAckInterceptor.MAX_FINAL_REQUESTS, true));
        assertEquals(RatisSnapshotAckInterceptor.MAX_FINAL_REQUESTS, bounded.received.get());
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, bounded.call.status.get().getCode());
        var streams = new ArrayList<Stream>();
        for (int i = 0; i < RatisSnapshotAckInterceptor.MAX_ACTIVE_STREAMS; i++) {
            streams.add(new Stream(interceptor, new CopyOnWriteArrayList<>()));
        }
        var refused = new Stream(interceptor, new CopyOnWriteArrayList<>());
        assertEquals(Status.Code.RESOURCE_EXHAUSTED, refused.call.status.get().getCode());
        streams.get(0).listener.onCancel();
        streams.get(0).listener.onComplete();
        streams.get(1).listener.onComplete();
        streams.get(2).wrapped.get().close(Status.OK, new Metadata());
        streams.get(2).wrapped.get().close(Status.OK, new Metadata());
        var replacement = new Stream(interceptor, new CopyOnWriteArrayList<>());
        assertNotNull(replacement.wrapped.get());
        assertNull(replacement.call.status.get());
        replacement.listener.onComplete();
        streams.forEach(stream -> stream.listener.onCancel());
        assertEquals(1, streams.get(2).call.closes.get());
    }

    @Test
    void blockedBarrierDoesNotLockOtherStreamsAndCancelCannotReleaseItsResourceSlotEarly() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var interceptor = new RatisSnapshotAckInterceptor((group, term, index) -> {
            if (index == 9) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new IOException("test timeout");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IOException(interrupted);
                }
            }
        });
        var first = new Stream(interceptor, new CopyOnWriteArrayList<>());
        var independent = new Stream(interceptor, new CopyOnWriteArrayList<>());
        first.listener.onMessage(request(GROUP, 3, 9, 0, true));
        independent.listener.onMessage(request(GROUP, 3, 10, 0, true));
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var blocked = executor.submit(() -> first.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS)));
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                executor.submit(() -> independent.reply(reply(GROUP, 0, RaftProtos.InstallSnapshotResult.SUCCESS)))
                        .get(1, TimeUnit.SECONDS);
                assertEquals(1, independent.call.messages.size());
                independent.listener.onComplete();
                first.listener.onCancel();
                var fill = new ArrayList<Stream>();
                for (int i = 1; i < RatisSnapshotAckInterceptor.MAX_ACTIVE_STREAMS; i++) {
                    fill.add(new Stream(interceptor, new CopyOnWriteArrayList<>()));
                }
                var refused = new Stream(interceptor, new CopyOnWriteArrayList<>());
                assertEquals(Status.Code.RESOURCE_EXHAUSTED, refused.call.status.get().getCode());
                fill.forEach(stream -> stream.listener.onCancel());
            } finally {
                release.countDown();
            }
            blocked.get(5, TimeUnit.SECONDS);
        }
        assertTrue(first.call.messages.isEmpty());
        var after = new Stream(interceptor, new CopyOnWriteArrayList<>());
        assertNotNull(after.wrapped.get());
        after.listener.onComplete();
    }

    @Test
    void publicParametersRegistersReleasedGrpcCustomizer() {
        var parameters = RatisSnapshotAckInterceptor.parameters((group, term, index) -> {});
        assertNotNull(org.apache.ratis.grpc.GrpcConfigKeys.Server.servicesCustomizer(parameters));
    }

    private static RaftProtos.RaftGroupIdProto groupProto(RaftGroupId group) {
        return RaftProtos.RaftGroupIdProto.newBuilder().setId(group.toByteString()).build();
    }
    private static RaftProtos.InstallSnapshotRequestProto request(RaftGroupId group, long term, long index, int request, boolean done) {
        return RaftProtos.InstallSnapshotRequestProto.newBuilder()
                .setServerRequest(RaftProtos.RaftRpcRequestProto.newBuilder().setRaftGroupId(groupProto(group)))
                .setSnapshotChunk(RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder()
                        .setDone(done).setRequestIndex(request)
                        .setTermIndex(RaftProtos.TermIndexProto.newBuilder().setTerm(term).setIndex(index))).build();
    }
    private static RaftProtos.InstallSnapshotReplyProto reply(RaftGroupId group, int request, RaftProtos.InstallSnapshotResult result) {
        return RaftProtos.InstallSnapshotReplyProto.newBuilder()
                .setServerReply(RaftProtos.RaftRpcReplyProto.newBuilder().setRaftGroupId(groupProto(group)).setSuccess(true))
                .setResult(result).setRequestIndex(request).build();
    }

    private static final class Stream {
        final FakeCall<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto> call;
        final AtomicReference<ServerCall<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>> wrapped = new AtomicReference<>();
        final AtomicInteger received = new AtomicInteger();
        final ServerCall.Listener<RaftProtos.InstallSnapshotRequestProto> listener;
        Stream(RatisSnapshotAckInterceptor interceptor, CopyOnWriteArrayList<String> events) {
            call = new FakeCall<>(RaftServerProtocolServiceGrpc.getInstallSnapshotMethod(), events);
            listener = interceptor.interceptCall(call, new Metadata(), (guarded, headers) -> {
                wrapped.set(guarded);
                return new ServerCall.Listener<>() {
                    @Override public void onMessage(RaftProtos.InstallSnapshotRequestProto message) { received.incrementAndGet(); }
                };
            });
        }
        void reply(RaftProtos.InstallSnapshotReplyProto reply) { wrapped.get().sendMessage(reply); }
    }

    private static final class FakeCall<Req, Resp> extends ServerCall<Req, Resp> {
        final MethodDescriptor<Req, Resp> method;
        final CopyOnWriteArrayList<Resp> messages = new CopyOnWriteArrayList<>();
        final AtomicReference<Status> status = new AtomicReference<>();
        final AtomicInteger closes = new AtomicInteger();
        final CopyOnWriteArrayList<String> events;
        FakeCall(MethodDescriptor<Req, Resp> method, CopyOnWriteArrayList<String> events) { this.method = method; this.events = events; }
        @Override public void request(int count) {}
        @Override public void sendHeaders(Metadata headers) {}
        @Override public void sendMessage(Resp message) { events.add("send"); messages.add(message); }
        @Override public void close(Status status, Metadata trailers) { this.status.set(status); closes.incrementAndGet(); }
        @Override public boolean isCancelled() { return false; }
        @Override public MethodDescriptor<Req, Resp> getMethodDescriptor() { return method; }
    }
}
