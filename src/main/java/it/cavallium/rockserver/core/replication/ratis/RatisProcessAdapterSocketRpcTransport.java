package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class RatisProcessAdapterSocketRpcTransport implements RatisProcessAdapterRpcTransport {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 64;
    private static final int IDLE_WORKER_TIMEOUT_SECONDS = 60;
    private static final int SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final InetSocketAddress address;
    private final int timeoutMillis;
    private final int maxMessageBytes;
    private final ThreadPoolExecutor requestExecutor;
    private final Set<PendingRequest> pendingRequests = ConcurrentHashMap.newKeySet();
    private final Set<Socket> pendingSocketCleanup = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RatisProcessAdapterSocketRpcTransport(InetSocketAddress address, Duration timeout) {
        this(address, timeout, RatisProcessAdapterSocketWire.DEFAULT_MAX_RPC_MESSAGE_BYTES);
    }

    public RatisProcessAdapterSocketRpcTransport(InetSocketAddress address, Duration timeout, int maxMessageBytes) {
        this(address, timeout, maxMessageBytes, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    public RatisProcessAdapterSocketRpcTransport(InetSocketAddress address,
                                                  Duration timeout,
                                                  int maxMessageBytes,
                                                  int maxConcurrentRequests) {
        this.address = RatisProcessAdapterSocketWire.requireNonNull(address, "address");
        this.timeoutMillis = RatisProcessAdapterSocketWire.requireSocketTimeoutMillis(timeout, "timeout");
        this.maxMessageBytes = RatisProcessAdapterSocketWire.requirePositiveMaxMessageBytes(maxMessageBytes);
        this.requestExecutor = boundedExecutor(maxConcurrentRequests);
    }

    @Override
    public CompletableFuture<RatisProcessAdapterRpcResponse> send(RatisProcessAdapterRpcRequest request) {
        RatisProcessAdapterSocketWire.requireNonNull(request, "request");
        var activeSocket = new AtomicReference<Socket>();
        var response = cancellationAwareResponse(activeSocket);
        var pendingRequest = new PendingRequest(response, activeSocket);
        response.whenComplete((_, _) -> pendingRequests.remove(pendingRequest));
        if (!register(pendingRequest)) {
            return response;
        }
        try {
            requestExecutor.execute(() -> {
                try {
                    if (!response.isDone()) {
                        response.complete(sendBlocking(request, response, activeSocket));
                    }
                } catch (Exception e) {
                    response.completeExceptionally(e);
                } catch (Error e) {
                    response.completeExceptionally(e);
                    throw e;
                }
            });
        } catch (RejectedExecutionException e) {
            response.completeExceptionally(closed.get()
                    ? closedFailure()
                    : new IOException("Ratis process adapter RPC client executor is saturated", e));
        } catch (RuntimeException | Error e) {
            response.completeExceptionally(e);
            throw e;
        }
        return response;
    }

    @Override
    public void close() {
        closed.set(true);
        Throwable failure = null;
        try {
            requestExecutor.shutdownNow();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        for (var pendingRequest : pendingRequests) {
            try {
                failClosed(pendingRequest);
            } catch (RuntimeException | Error e) {
                failure = merge(failure, e);
            }
        }
        failure = closePendingSockets(failure);
        try {
            awaitRequestExecutorTermination();
        } catch (RuntimeException | Error e) {
            failure = merge(failure, e);
        }
        failure = closePendingSockets(failure);
        if (!pendingSocketCleanup.isEmpty()) {
            failure = merge(failure, new IllegalStateException(
                    "Ratis process adapter RPC transport socket cleanup remains incomplete"
            ));
        }
        throwUnchecked(failure);
    }

    private Throwable closePendingSockets(Throwable failure) {
        for (var socket : pendingSocketCleanup) {
            try {
                closeTracked(socket);
            } catch (RuntimeException | Error e) {
                failure = merge(failure, e);
            }
        }
        return failure;
    }

    private RatisProcessAdapterRpcResponse sendBlocking(RatisProcessAdapterRpcRequest request,
                                                        CompletableFuture<?> response,
                                                        AtomicReference<Socket> activeSocket) throws IOException {
        var requestBytes = RatisProcessAdapterSocketWire.encodeUtf8(
                RatisProcessAdapterRpcCodec.encodeRequest(request),
                maxMessageBytes
        );
        if (response.isDone()) {
            return null;
        }
        var socket = new Socket();
        activeSocket.set(socket);
        try (socket) {
            if (response.isDone()) {
                return null;
            }
            var startNanos = System.nanoTime();
            socket.connect(address, RatisProcessAdapterSocketWire.remainingTimeoutMillis(
                    startNanos,
                    timeoutMillis,
                    "Ratis process adapter RPC connection"
            ));
            RatisProcessAdapterSocketWire.writeUtf8(
                    socket,
                    requestBytes,
                    startNanos,
                    timeoutMillis,
                    "Ratis process adapter RPC request"
            );
            socket.shutdownOutput();
            var responsePayload = RatisProcessAdapterSocketWire.readUtf8(
                    socket,
                    maxMessageBytes,
                    startNanos,
                    timeoutMillis,
                    "Ratis process adapter RPC response"
            );
            try {
                return RatisProcessAdapterRpcCodec.decodeResponse(responsePayload);
            } catch (RuntimeException e) {
                throw new IOException("Invalid Ratis process adapter RPC response", e);
            }
        } finally {
            activeSocket.compareAndSet(socket, null);
            updatePendingSocketCleanup(socket);
        }
    }

    private boolean register(PendingRequest pendingRequest) {
        if (closed.get()) {
            failClosed(pendingRequest);
            return false;
        }
        pendingRequests.add(pendingRequest);
        if (closed.get()) {
            failClosed(pendingRequest);
            return false;
        }
        return true;
    }

    private static ThreadPoolExecutor boundedExecutor(int maxConcurrentRequests) {
        if (maxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("maxConcurrentRequests must be positive");
        }
        return new ThreadPoolExecutor(
                0,
                maxConcurrentRequests,
                IDLE_WORKER_TIMEOUT_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                task -> {
                    var thread = new Thread(task, "rockserver-ratis-rpc-client");
                    thread.setDaemon(true);
                    return thread;
                }
        );
    }

    private CompletableFuture<RatisProcessAdapterRpcResponse> cancellationAwareResponse(
            AtomicReference<Socket> activeSocket) {
        RatisProcessAdapterSocketWire.requireNonNull(activeSocket, "activeSocket");
        return new CompletableFuture<>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                var canceled = super.cancel(mayInterruptIfRunning);
                if (canceled) {
                    closeCanceledSocket(activeSocket.get());
                }
                return canceled;
            }
        };
    }

    private void closeCanceledSocket(Socket socket) {
        closeTracked(socket);
    }

    private void awaitRequestExecutorTermination() {
        var interrupted = false;
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(SHUTDOWN_TIMEOUT_SECONDS);
        try {
            while (true) {
                try {
                    var remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        throw clientExecutorDidNotStopFailure();
                    }
                    if (requestExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                        return;
                    }
                    throw clientExecutorDidNotStopFailure();
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static IllegalStateException clientExecutorDidNotStopFailure() {
        return new IllegalStateException("Ratis process adapter RPC client executor did not stop within "
                + SHUTDOWN_TIMEOUT_SECONDS + " seconds");
    }

    private static void closeQuietly(Socket socket) {
        if (socket == null) {
            return;
        }
        try {
            socket.close();
        } catch (IOException ignored) {
            // Cleanup ownership remains tracked until Socket.isClosed() confirms completion.
        }
    }

    private void closeTracked(Socket socket) {
        try {
            closeQuietly(socket);
        } finally {
            updatePendingSocketCleanup(socket);
        }
    }

    private void updatePendingSocketCleanup(Socket socket) {
        if (socket == null) {
            return;
        }
        if (socket.isClosed()) {
            pendingSocketCleanup.remove(socket);
        } else {
            pendingSocketCleanup.add(socket);
        }
    }

    private void failClosed(PendingRequest pendingRequest) {
        Throwable failure = null;
        var socket = pendingRequest.activeSocket().get();
        try {
            closeTracked(socket);
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        try {
            pendingRequest.response().completeExceptionally(closedFailure());
        } catch (RuntimeException | Error e) {
            failure = merge(failure, e);
        }
        throwUnchecked(failure);
    }

    private static IOException closedFailure() {
        return new IOException("Ratis process adapter RPC transport is closed");
    }

    private static Throwable merge(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IllegalStateException("Unexpected unchecked RPC transport cleanup failure", failure);
        }
    }

    private record PendingRequest(CompletableFuture<RatisProcessAdapterRpcResponse> response,
                                  AtomicReference<Socket> activeSocket) {
    }

}
