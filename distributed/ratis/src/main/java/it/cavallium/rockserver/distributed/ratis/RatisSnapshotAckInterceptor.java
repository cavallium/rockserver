package it.cavallium.rockserver.distributed.ratis;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Forces the received final snapshot before released Ratis sends its successful chunk reply.
 * The barrier owns image validation, file/directory durability and replica fencing on failure.
 * This transport hook does not prove snapshot installation or state-machine reload completion.
 */
public final class RatisSnapshotAckInterceptor implements ServerInterceptor {
    static final int MAX_ACTIVE_STREAMS = 128;
    static final int MAX_FINAL_REQUESTS = 16;
    private static final String INSTALL_METHOD = RaftServerProtocolServiceGrpc.getInstallSnapshotMethod().getFullMethodName();

    @FunctionalInterface
    public interface Barrier {
        void sync(RaftGroupId group, long term, long index) throws IOException;
    }

    private final Barrier barrier;
    private final AtomicInteger activeStreams = new AtomicInteger();

    public RatisSnapshotAckInterceptor(Barrier barrier) { this.barrier = Objects.requireNonNull(barrier, "barrier"); }

    /** Register the hook through the released shaded gRPC service customization seam. */
    public static Parameters parameters(Barrier barrier) {
        var interceptor = new RatisSnapshotAckInterceptor(barrier);
        var parameters = new Parameters();
        GrpcConfigKeys.Server.setServicesCustomizer(parameters, (builder, types) -> builder.intercept(interceptor));
        return parameters;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
            Metadata headers, ServerCallHandler<ReqT, RespT> next) {
        if (!INSTALL_METHOD.equals(call.getMethodDescriptor().getFullMethodName())) {
            return next.startCall(call, headers);
        }
        if (!reserveStream()) {
            call.close(Status.RESOURCE_EXHAUSTED.withDescription("Snapshot receive stream budget exhausted"), new Metadata());
            return new ServerCall.Listener<>() {};
        }
        var state = new StreamState();
        var guarded = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
            @Override public void sendMessage(RespT message) {
                if (state.finished.get()) return;
                if (message instanceof RaftProtos.InstallSnapshotReplyProto reply
                        && reply.getResult() == RaftProtos.InstallSnapshotResult.SUCCESS) {
                    Entry entry = state.find(reply.hasRequestIndex() ? reply.getRequestIndex() : null);
                    if (entry != null) {
                        if (!reply.hasServerReply() || !reply.getServerReply().hasRaftGroupId()
                                || !entry.identity.group().toByteString().equals(reply.getServerReply().getRaftGroupId().getId())) {
                            close(Status.INTERNAL.withDescription("Snapshot reply correlation failed"), new Metadata());
                            return;
                        }
                        int start = state.beginBarrier(entry);
                        if (start < 0) return;
                        if (start > 0) {
                            try {
                                barrier.sync(entry.identity.group(), entry.identity.term(), entry.identity.index());
                                entry.durable.complete(null);
                            } catch (IOException | RuntimeException failure) {
                                entry.durable.completeExceptionally(failure);
                                close(Status.INTERNAL.withDescription("Snapshot durability barrier failed"), new Metadata());
                                return;
                            } finally {
                                state.endBarrier();
                            }
                        }
                        try {
                            entry.durable.join();
                        } catch (CompletionException | CancellationException failed) {
                            return;
                        }
                    } else if (!reply.hasRequestIndex() && state.hasFinalRequests()) {
                        close(Status.INTERNAL.withDescription("Snapshot reply correlation failed"), new Metadata());
                        return;
                    }
                }
                if (!state.finished.get() && !isCancelled()) super.sendMessage(message);
            }

            @Override public void close(Status status, Metadata trailers) {
                state.finish();
                if (state.rpcClosed.compareAndSet(false, true)) super.close(status, trailers);
            }
        };
        ServerCall.Listener<ReqT> listener;
        try {
            listener = next.startCall(guarded, headers);
        } catch (RuntimeException | Error failure) {
            state.finish();
            throw failure;
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            @Override public void onMessage(ReqT message) {
                if (state.finished.get()) return;
                if (message instanceof RaftProtos.InstallSnapshotRequestProto request
                        && request.hasSnapshotChunk() && request.getSnapshotChunk().getDone()) {
                    Identity identity;
                    var chunk = request.getSnapshotChunk();
                    try {
                        if (!request.hasServerRequest() || !request.getServerRequest().hasRaftGroupId()
                                || request.getServerRequest().getRaftGroupId().getId().size() != 16
                                || !chunk.hasTermIndex() || chunk.getTermIndex().getTerm() < 0
                                || chunk.getTermIndex().getIndex() < 0) {
                            throw new IllegalArgumentException("Invalid final snapshot metadata");
                        }
                        identity = new Identity(RaftGroupId.valueOf(request.getServerRequest().getRaftGroupId().getId()),
                                chunk.getTermIndex().getTerm(), chunk.getTermIndex().getIndex());
                    } catch (IllegalArgumentException invalid) {
                        guarded.close(Status.INVALID_ARGUMENT.withDescription("Invalid final snapshot metadata"), new Metadata());
                        return;
                    }
                    Status rejected = state.track(chunk.getRequestIndex(), identity);
                    if (rejected != null) {
                        guarded.close(rejected, new Metadata());
                        return;
                    }
                }
                if (!state.finished.get()) super.onMessage(message);
            }
            @Override public void onCancel() { state.finish(); super.onCancel(); }
            @Override public void onComplete() { state.finish(); super.onComplete(); }
        };
    }

    private boolean reserveStream() {
        for (;;) {
            int count = activeStreams.get();
            if (count >= MAX_ACTIVE_STREAMS) return false;
            if (activeStreams.compareAndSet(count, count + 1)) return true;
        }
    }

    private record Identity(RaftGroupId group, long term, long index) {}
    private static final class Entry {
        final Identity identity;
        final AtomicBoolean started = new AtomicBoolean();
        final CompletableFuture<Void> durable = new CompletableFuture<>();
        Entry(Identity identity) { this.identity = identity; }
    }

    /** Only fixed-size final identity metadata is retained; no request or file payload is stored. */
    private final class StreamState {
        private final HashMap<Integer, Entry> finals = new HashMap<>();
        private Identity identity;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicBoolean rpcClosed = new AtomicBoolean();
        private final AtomicBoolean slotReleased = new AtomicBoolean();
        private int barriers;

        synchronized Status track(int requestIndex, Identity next) {
            if (finished.get()) return Status.CANCELLED;
            if (identity != null && !identity.equals(next)) {
                return Status.INVALID_ARGUMENT.withDescription("Final snapshot identity changed within stream");
            }
            if (!finals.containsKey(requestIndex)) {
                if (finals.size() >= MAX_FINAL_REQUESTS) {
                    return Status.RESOURCE_EXHAUSTED.withDescription("Snapshot final request budget exhausted");
                }
                finals.put(requestIndex, new Entry(next));
            }
            identity = next;
            return null;
        }

        synchronized Entry find(Integer index) { return index == null ? null : finals.get(index); }
        synchronized boolean hasFinalRequests() { return !finals.isEmpty(); }

        synchronized int beginBarrier(Entry entry) {
            if (finished.get()) return -1;
            if (!entry.started.compareAndSet(false, true)) return 0;
            barriers++;
            return 1;
        }

        void endBarrier() {
            boolean release;
            synchronized (this) { release = --barriers == 0 && finished.get(); }
            if (release) releaseSlot();
        }

        private void releaseSlot() {
            if (slotReleased.compareAndSet(false, true)) activeStreams.decrementAndGet();
        }

        void finish() {
            if (!finished.compareAndSet(false, true)) return;
            ArrayList<Entry> pending;
            boolean release;
            synchronized (this) {
                pending = new ArrayList<>(finals.values());
                finals.clear();
                identity = null;
                release = barriers == 0;
            }
            if (release) releaseSlot();
            for (Entry entry : pending) entry.durable.completeExceptionally(new CancellationException("Snapshot stream closed"));
        }
    }
}
