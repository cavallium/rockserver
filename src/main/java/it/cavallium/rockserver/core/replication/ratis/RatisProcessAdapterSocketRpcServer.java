package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class RatisProcessAdapterSocketRpcServer implements AutoCloseable {

    private static final String INVALID_REQUEST_ID = "invalid";
    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 64;
    private static final int MAX_ERROR_RESPONSE_WRITE_TIMEOUT_MILLIS = 100;

    private final InetSocketAddress bindAddress;
    private final RatisProcessAdapterRpcHandler handler;
    private final int stopTimeoutMillis;
    private final int maxMessageBytes;
    private final ExecutorService acceptExecutor;
    private final ExecutorService requestExecutor;
    private final ThreadPoolExecutor operationExecutor;
    private final RatisProcessAdapterSocketWire.AcceptedSockets acceptedSockets =
            new RatisProcessAdapterSocketWire.AcceptedSockets();
    private final AtomicReference<IOException> acceptFailure = new AtomicReference<>();
    private final AtomicReference<IOException> requestFailure = new AtomicReference<>();
    private volatile boolean operationSaturated;
    private ServerSocket serverSocket;
    private volatile boolean running;
    private boolean closed;

    public RatisProcessAdapterSocketRpcServer(InetSocketAddress bindAddress,
                                              RatisProcessAdapterRpcHandler handler,
                                              Duration stopTimeout) {
        this(bindAddress, handler, stopTimeout, RatisProcessAdapterSocketWire.DEFAULT_MAX_RPC_MESSAGE_BYTES);
    }

    public RatisProcessAdapterSocketRpcServer(InetSocketAddress bindAddress,
                                              RatisProcessAdapterRpcHandler handler,
                                              Duration stopTimeout,
                                              int maxMessageBytes) {
        this(bindAddress, handler, stopTimeout, maxMessageBytes, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    public RatisProcessAdapterSocketRpcServer(InetSocketAddress bindAddress,
                                              RatisProcessAdapterRpcHandler handler,
                                               Duration stopTimeout,
                                               int maxMessageBytes,
                                               int maxConcurrentRequests) {
        this.bindAddress = RatisProcessAdapterSocketWire.requireNonNull(bindAddress, "bindAddress");
        this.handler = RatisProcessAdapterSocketWire.requireNonNull(handler, "handler");
        this.stopTimeoutMillis = RatisProcessAdapterSocketWire.requireSocketTimeoutMillis(stopTimeout, "stopTimeout");
        this.maxMessageBytes = RatisProcessAdapterSocketWire.requirePositiveMaxMessageBytes(maxMessageBytes);
        this.requestExecutor = boundedExecutor(maxConcurrentRequests, "rockserver-ratis-rpc-request");
        this.operationExecutor = boundedExecutor(maxConcurrentRequests, "rockserver-ratis-rpc-operation");
        this.acceptExecutor = Executors.newSingleThreadExecutor(task -> thread(task, "rockserver-ratis-rpc-accept"));
    }

    public synchronized void start() throws IOException {
        if (running) {
            throw new IllegalStateException("Ratis process adapter RPC server is already running");
        }
        if (closed) {
            throw new IllegalStateException("Ratis process adapter RPC server is closed");
        }
        if (acceptFailure.get() != null || requestFailure.get() != null) {
            throw new IllegalStateException("Ratis process adapter RPC server failed; construct a new server");
        }
        if (serverSocket != null) {
            throw new IllegalStateException("Ratis process adapter RPC listener cleanup is incomplete; close the server");
        }
        startListener(new ServerSocket());
    }

    @SuppressWarnings("DuplicatedCode")
    private void startListener(ServerSocket listener) throws IOException {
        serverSocket = RatisProcessAdapterSocketWire.requireNonNull(listener, "listener");
        try {
            serverSocket = RatisProcessAdapterSocketWire.bindAndStartListener(listener, bindAddress, boundListener -> {
                serverSocket = boundListener;
                running = true;
                acceptExecutor.execute(() -> acceptLoop(boundListener));
            });
        } catch (IOException | RuntimeException | Error e) {
            running = false;
            clearClosedListener(listener);
            throw e;
        }
    }

    public synchronized InetSocketAddress boundAddress() {
        if (serverSocket == null || !running) {
            throw new IllegalStateException("Ratis process adapter RPC server is not running");
        }
        return new InetSocketAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());
    }

    public synchronized boolean isRunning() {
        return running;
    }

    public synchronized RatisProcessAdapterHealth health() {
        var failure = acceptFailure.get();
        if (failure != null) {
            return RatisProcessAdapterHealth.degraded(
                    "Ratis process adapter RPC listener failed: "
                            + RatisProcessAdapterRpcLimits.sanitizeErrorMessage(failure)
            );
        }
        failure = requestFailure.get();
        if (failure != null) {
            return RatisProcessAdapterHealth.degraded(
                    "Ratis process adapter RPC request worker failed: "
                            + RatisProcessAdapterRpcLimits.sanitizeErrorMessage(failure)
            );
        }
        if (!running) {
            return RatisProcessAdapterHealth.degraded("Ratis process adapter RPC server is not running");
        }
        if (operationSaturated) {
            if (operationExecutor.getActiveCount() >= operationExecutor.getMaximumPoolSize()) {
                return RatisProcessAdapterHealth.degraded("Ratis process adapter RPC operation executor is saturated");
            }
            operationSaturated = false;
        }
        return RatisProcessAdapterHealth.ready();
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        running = false;
        var listener = serverSocket;
        try {
            RatisProcessAdapterSocketWire.shutdownListener(
                    listener,
                    acceptedSockets,
                    acceptExecutor,
                    requestExecutor,
                    operationExecutor,
                    stopTimeoutMillis,
                    "Ratis process adapter RPC server"
            );
        } finally {
            clearClosedListener(listener);
        }
    }

    private void clearClosedListener(ServerSocket listener) {
        if (listener == null || listener.isClosed()) {
            serverSocket = null;
        }
    }

    private void acceptLoop(ServerSocket listener) {
        try {
            while (running) {
                try {
                    var socket = listener.accept();
                    var startNanos = System.nanoTime();
                    if (acceptedSockets.rejectIfClosed(socket)) {
                        continue;
                    }
                    try {
                        requestExecutor.execute(() -> serve(socket, startNanos));
                    } catch (RejectedExecutionException e) {
                        rejectBusy(socket);
                    } catch (RuntimeException | Error e) {
                        var failure = new IOException("Ratis process adapter RPC listener handoff failed", e);
                        closeAfterFailedHandoff(socket, failure);
                        failAcceptLoop(listener, failure);
                    }
                } catch (IOException e) {
                    failAcceptLoop(listener, e);
                }
            }
        } catch (RuntimeException | Error e) {
            failAcceptLoop(listener, new IOException("Ratis process adapter RPC listener handoff failed", e));
        } finally {
            running = false;
        }
    }

    private void failAcceptLoop(ServerSocket listener, IOException failure) {
        if (!running) {
            return;
        }
        running = false;
        acceptFailure.compareAndSet(null, failure);
        closeListenerAfterFailure(listener, failure);
    }

    private void rejectBusy(Socket socket) {
        try (socket) {
            RatisProcessAdapterSocketWire.writeUtf8(
                    socket,
                    RatisProcessAdapterSocketWire.encodeUtf8(
                            RatisProcessAdapterRpcCodec.encodeResponse(RatisProcessAdapterRpcResponse.error(
                                    INVALID_REQUEST_ID,
                                    "BUSY",
                                    "Ratis process adapter RPC server is saturated"
                            )),
                            maxMessageBytes
                    ),
                    System.nanoTime(),
                    errorResponseWriteTimeoutMillis(),
                    "Ratis process adapter busy RPC response"
            );
        } catch (IOException ignored) {
            // A rejected caller will observe the transport failure or retry through the gateway.
        } finally {
            acceptedSockets.removeAfterWorkerClose(socket);
        }
    }

    private void closeAfterFailedHandoff(Socket socket, Throwable failure) {
        RatisProcessAdapterSocketWire.closeAfterFailedHandoff(socket, failure);
        acceptedSockets.removeAfterWorkerClose(socket);
    }

    private void serve(Socket socket, long startNanos) {
        try (socket) {
            var response = readAndHandle(socket, startNanos);
            writeResponse(socket, response, startNanos);
        } catch (IOException ignored) {
            // The caller owns retry behavior; a broken adapter RPC socket is reported by the transport.
        } catch (RuntimeException | Error e) {
            failRequestWorker(new IOException("Ratis process adapter RPC request worker failed", e));
            throw e;
        } finally {
            acceptedSockets.removeAfterWorkerClose(socket);
        }
    }

    private void failRequestWorker(IOException failure) {
        requestFailure.compareAndSet(null, failure);
        if (!running) {
            return;
        }
        running = false;
        closeListenerAfterFailure(serverSocket, failure);
    }

    private static void closeListenerAfterFailure(ServerSocket listener, IOException failure) {
        if (listener == null) {
            return;
        }
        try {
            listener.close();
        } catch (IOException | RuntimeException | Error closeFailure) {
            if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    private RatisProcessAdapterRpcResponse readAndHandle(Socket socket, long startNanos) {
        try {
            var requestPayload = RatisProcessAdapterSocketWire.readUtf8(
                    socket,
                    maxMessageBytes,
                    startNanos,
                    stopTimeoutMillis,
                    "Ratis process adapter RPC request"
            );
            return handle(requestPayload, startNanos);
        } catch (IOException e) {
            return RatisProcessAdapterRpcResponse.error(
                    INVALID_REQUEST_ID,
                    "BAD_REQUEST",
                    RatisProcessAdapterRpcLimits.sanitizeErrorMessage(e)
            );
        }
    }

    private void writeResponse(Socket socket,
                               RatisProcessAdapterRpcResponse response,
                               long requestStartNanos) throws IOException {
        byte[] payload;
        var errorResponse = !response.success();
        try {
            payload = RatisProcessAdapterSocketWire.encodeUtf8(
                    RatisProcessAdapterRpcCodec.encodeResponse(response),
                    maxMessageBytes
            );
        } catch (IOException e) {
            payload = RatisProcessAdapterSocketWire.encodeUtf8(
                    RatisProcessAdapterRpcCodec.encodeResponse(RatisProcessAdapterRpcResponse.error(
                            response.requestId(),
                            "ADAPTER_ERROR",
                            RatisProcessAdapterRpcLimits.sanitizeErrorMessage(e)
                    )),
                    maxMessageBytes
            );
            errorResponse = true;
        }
        RatisProcessAdapterSocketWire.writeUtf8(
                socket,
                payload,
                errorResponse ? System.nanoTime() : requestStartNanos,
                errorResponse ? errorResponseWriteTimeoutMillis() : stopTimeoutMillis,
                "Ratis process adapter RPC response"
        );
    }

    private RatisProcessAdapterRpcResponse handle(String requestPayload, long startNanos) {
        RatisProcessAdapterRpcRequest request;
        try {
            request = RatisProcessAdapterRpcCodec.decodeRequest(requestPayload);
        } catch (RuntimeException e) {
            return RatisProcessAdapterRpcResponse.error(
                    INVALID_REQUEST_ID,
                    "BAD_REQUEST",
                    RatisProcessAdapterRpcLimits.sanitizeErrorMessage(e)
            );
        }
        final Future<CompletableFuture<RatisProcessAdapterRpcResponse>> invocation;
        var pendingResponse = new PendingResponse();
        try {
            remainingRequestTimeoutMillis(startNanos);
            invocation = operationExecutor.submit(() -> pendingResponse.publish(handler.handle(request)));
        } catch (TimeoutException e) {
            return timedOut(request.requestId());
        } catch (RejectedExecutionException e) {
            operationSaturated = true;
            return RatisProcessAdapterRpcResponse.error(
                    request.requestId(),
                    "BUSY",
                    "Ratis process adapter RPC operation executor is saturated"
            );
        }
        try {
            var responseFuture = invocation.get(remainingRequestTimeoutMillis(startNanos), TimeUnit.MILLISECONDS);
            return responseFuture.get(remainingRequestTimeoutMillis(startNanos), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            cancel(invocation, pendingResponse);
            return RatisProcessAdapterRpcResponse.error(request.requestId(), "ADAPTER_ERROR", "interrupted");
        } catch (TimeoutException e) {
            cancel(invocation, pendingResponse);
            return timedOut(request.requestId());
        } catch (ExecutionException e) {
            var failure = unwrap(e.getCause());
            if (failure instanceof Error error) {
                throw error;
            }
            return RatisProcessAdapterRpcResponse.error(
                    request.requestId(),
                    "ADAPTER_ERROR",
                    RatisProcessAdapterRpcLimits.sanitizeErrorMessage(failure)
            );
        } catch (RuntimeException e) {
            return RatisProcessAdapterRpcResponse.error(
                    request.requestId(),
                    "ADAPTER_ERROR",
                    RatisProcessAdapterRpcLimits.sanitizeErrorMessage(e)
            );
        }
    }

    private void cancel(Future<?> invocation, PendingResponse pendingResponse) {
        operationSaturated = true;
        invocation.cancel(true);
        pendingResponse.cancel();
    }

    private int remainingRequestTimeoutMillis(long startNanos) throws TimeoutException {
        var remainingNanos = TimeUnit.MILLISECONDS.toNanos(stopTimeoutMillis) - (System.nanoTime() - startNanos);
        if (remainingNanos <= 0) {
            throw new TimeoutException("Ratis process adapter RPC request timed out");
        }
        return (int) Math.min(stopTimeoutMillis, TimeUnit.NANOSECONDS.toMillis(remainingNanos - 1) + 1);
    }

    private static RatisProcessAdapterRpcResponse timedOut(String requestId) {
        return RatisProcessAdapterRpcResponse.error(
                requestId,
                "ADAPTER_TIMEOUT",
                "Ratis process adapter RPC request timed out"
        );
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }

    private int errorResponseWriteTimeoutMillis() {
        return Math.min(stopTimeoutMillis, MAX_ERROR_RESPONSE_WRITE_TIMEOUT_MILLIS);
    }

    private static Thread thread(Runnable task, String name) {
        var thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    private static ThreadPoolExecutor boundedExecutor(int maxConcurrentRequests, String threadName) {
        if (maxConcurrentRequests <= 0) {
            throw new IllegalArgumentException("maxConcurrentRequests must be positive");
        }
        return new ThreadPoolExecutor(
                maxConcurrentRequests,
                maxConcurrentRequests,
                0L,
                TimeUnit.MILLISECONDS,
                new SynchronousQueue<>(),
                task -> thread(task, threadName)
        );
    }

    private final class PendingResponse {

        private final AtomicBoolean canceled = new AtomicBoolean();
        private final AtomicReference<CompletableFuture<RatisProcessAdapterRpcResponse>> response =
                new AtomicReference<>();

        private CompletableFuture<RatisProcessAdapterRpcResponse> publish(
                CompletableFuture<RatisProcessAdapterRpcResponse> response) {
            RatisProcessAdapterSocketWire.requireNonNull(response, "response");
            if (!this.response.compareAndSet(null, response)) {
                throw new IllegalStateException("Ratis process adapter RPC response published more than once");
            }
            if (canceled.get()) {
                cancelResponse(response);
            }
            return response;
        }

        private void cancel() {
            canceled.set(true);
            var response = this.response.get();
            if (response != null) {
                cancelResponse(response);
            }
        }

        private void cancelResponse(CompletableFuture<?> response) {
            try {
                response.cancel(true);
            } catch (RuntimeException | Error e) {
                failRequestWorker(new IOException("Ratis process adapter RPC response cancellation failed", e));
                throw e;
            }
        }
    }

}
