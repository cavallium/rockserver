package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public final class RatisProcessAdapterSocketHealthServer implements AutoCloseable {

    private static final int DEFAULT_MAX_CONCURRENT_REQUESTS = 8;
    private static final int MAX_DEGRADED_RESPONSE_WRITE_TIMEOUT_MILLIS = 100;

    private final InetSocketAddress bindAddress;
    private final int stopTimeoutMillis;
    private final int maxMessageBytes;
    private final Supplier<RatisProcessAdapterHealth> healthSupplier;
    private final ExecutorService acceptExecutor;
    private final ExecutorService requestExecutor;
    private final ExecutorService operationExecutor;
    private final RatisProcessAdapterSocketWire.AcceptedSockets acceptedSockets =
            new RatisProcessAdapterSocketWire.AcceptedSockets();
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicReference<IOException> acceptFailure = new AtomicReference<>();
    private ServerSocket serverSocket;
    private volatile boolean running;
    private boolean closed;

    public RatisProcessAdapterSocketHealthServer(InetSocketAddress bindAddress, Duration stopTimeout) {
        this(
                bindAddress,
                stopTimeout,
                RatisProcessAdapterSocketWire.DEFAULT_MAX_HEALTH_MESSAGE_BYTES
        );
    }

    public RatisProcessAdapterSocketHealthServer(InetSocketAddress bindAddress,
                                                 Duration stopTimeout,
                                                 int maxMessageBytes) {
        this(bindAddress, stopTimeout, maxMessageBytes, DEFAULT_MAX_CONCURRENT_REQUESTS);
    }

    public RatisProcessAdapterSocketHealthServer(InetSocketAddress bindAddress,
                                                 Duration stopTimeout,
                                                 Supplier<RatisProcessAdapterHealth> healthSupplier) {
        this(
                bindAddress,
                stopTimeout,
                RatisProcessAdapterSocketWire.DEFAULT_MAX_HEALTH_MESSAGE_BYTES,
                DEFAULT_MAX_CONCURRENT_REQUESTS,
                healthSupplier
        );
    }

    public RatisProcessAdapterSocketHealthServer(InetSocketAddress bindAddress,
                                                 Duration stopTimeout,
                                                 int maxMessageBytes,
                                                 int maxConcurrentRequests) {
        this(bindAddress, stopTimeout, maxMessageBytes, maxConcurrentRequests, RatisProcessAdapterHealth::ready);
    }

    public RatisProcessAdapterSocketHealthServer(InetSocketAddress bindAddress,
                                                 Duration stopTimeout,
                                                  int maxMessageBytes,
                                                  int maxConcurrentRequests,
                                                  Supplier<RatisProcessAdapterHealth> healthSupplier) {
        this.bindAddress = RatisProcessAdapterSocketWire.requireNonNull(bindAddress, "bindAddress");
        this.stopTimeoutMillis = RatisProcessAdapterSocketWire.requireSocketTimeoutMillis(stopTimeout, "stopTimeout");
        this.maxMessageBytes = RatisProcessAdapterSocketWire.requirePositiveMaxMessageBytes(maxMessageBytes);
        this.healthSupplier = RatisProcessAdapterSocketWire.requireNonNull(healthSupplier, "healthSupplier");
        this.requestExecutor = boundedExecutor(maxConcurrentRequests, "rockserver-ratis-health-request");
        this.operationExecutor = boundedExecutor(maxConcurrentRequests, "rockserver-ratis-health-operation");
        this.acceptExecutor = Executors.newSingleThreadExecutor(
                task -> thread(task, "rockserver-ratis-health-accept")
        );
    }

    public synchronized void start() throws IOException {
        if (running) {
            throw new IllegalStateException("Ratis process adapter health server is already running");
        }
        if (closed) {
            throw new IllegalStateException("Ratis process adapter health server is closed");
        }
        if (acceptFailure.get() != null) {
            throw new IllegalStateException("Ratis process adapter health server failed; construct a new server");
        }
        if (serverSocket != null) {
            throw new IllegalStateException("Ratis process adapter health listener cleanup is incomplete; close the server");
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
            throw new IllegalStateException("Ratis process adapter health server is not running");
        }
        return new InetSocketAddress(serverSocket.getInetAddress(), serverSocket.getLocalPort());
    }

    public void awaitClosed() throws IOException {
        try {
            stopped.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for Ratis process adapter health server", e);
        }
        var failure = acceptFailure.get();
        if (failure != null) {
            throw failure;
        }
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
                    "Ratis process adapter health server"
            );
        } finally {
            clearClosedListener(listener);
            stopped.countDown();
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
                        closeRejected(socket);
                    } catch (RuntimeException | Error e) {
                        var failure = new IOException("Ratis process adapter health listener handoff failed", e);
                        closeAfterFailedHandoff(socket, failure);
                        failAcceptLoop(listener, failure);
                    }
                } catch (IOException e) {
                    failAcceptLoop(listener, e);
                }
            }
        } catch (RuntimeException | Error e) {
            failAcceptLoop(listener, new IOException("Ratis process adapter health listener handoff failed", e));
        } finally {
            running = false;
            stopped.countDown();
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

    private void serve(Socket socket, long startNanos) {
        try (socket) {
            try {
                var request = RatisProcessAdapterSocketWire.readUtf8Line(
                        socket,
                        maxMessageBytes,
                        startNanos,
                        stopTimeoutMillis
                );
                if (!request.equals("HEALTH")) {
                    writeDegradedHealth(socket,
                            "Unsupported adapter health request: " + singleLine(request),
                            startNanos);
                    return;
                }
                writeHealth(socket, suppliedHealth(startNanos), startNanos);
            } catch (IOException | RuntimeException e) {
                try {
                    writeDegradedHealth(socket, singleLineMessage(e), startNanos);
                } catch (IOException ignored) {
                    // The caller will observe a closed or failed health socket.
                }
            }
        } catch (IOException ignored) {
            // The caller will observe a closed or failed health socket.
        } catch (RuntimeException | Error e) {
            failRequestWorker(new IOException("Ratis process adapter health request worker failed", e));
            throw e;
        } finally {
            acceptedSockets.removeAfterWorkerClose(socket);
        }
    }

    private void failRequestWorker(IOException failure) {
        if (!running) {
            return;
        }
        running = false;
        acceptFailure.compareAndSet(null, failure);
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

    private void writeDegradedHealth(Socket socket, String message, long startNanos) throws IOException {
        writeHealth(socket, RatisProcessAdapterHealth.degraded(singleLine(message)), startNanos);
    }

    private void writeHealth(Socket socket, RatisProcessAdapterHealth health, long startNanos) throws IOException {
        var degraded = !health.isReady();
        RatisProcessAdapterSocketWire.writeUtf8(
                socket,
                RatisProcessAdapterSocketWire.encodeUtf8(
                        RatisProcessAdapterHealthCodec.encode(health),
                        maxMessageBytes
                ),
                degraded ? System.nanoTime() : startNanos,
                degraded ? degradedResponseWriteTimeoutMillis() : stopTimeoutMillis,
                "Ratis process adapter health response"
        );
    }

    private RatisProcessAdapterHealth suppliedHealth(long startNanos) throws IOException {
        final Future<RatisProcessAdapterHealth> invocation;
        try {
            remainingHealthSupplierTimeoutMillis(startNanos);
            invocation = operationExecutor.submit(healthSupplier::get);
        } catch (SocketTimeoutException e) {
            return healthSupplierTimedOut();
        } catch (RejectedExecutionException e) {
            return RatisProcessAdapterHealth.degraded("Ratis process adapter health supplier executor is saturated");
        }
        try {
            var health = invocation.get(
                    remainingHealthSupplierTimeoutMillis(startNanos),
                    TimeUnit.MILLISECONDS
            );
            if (health == null) {
                return RatisProcessAdapterHealth.degraded("Ratis process adapter health supplier returned null");
            }
            return health;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            invocation.cancel(true);
            throw new IOException("Interrupted while invoking Ratis process adapter health supplier", e);
        } catch (SocketTimeoutException | TimeoutException e) {
            invocation.cancel(true);
            return healthSupplierTimedOut();
        } catch (ExecutionException e) {
            var failure = unwrap(e.getCause());
            if (failure instanceof Error error) {
                throw error;
            }
            return RatisProcessAdapterHealth.degraded(singleLineMessage(failure));
        }
    }

    private int remainingHealthSupplierTimeoutMillis(long startNanos) throws SocketTimeoutException {
        return RatisProcessAdapterSocketWire.remainingTimeoutMillis(
                startNanos,
                stopTimeoutMillis,
                "Ratis process adapter health supplier"
        );
    }

    private static RatisProcessAdapterHealth healthSupplierTimedOut() {
        return RatisProcessAdapterHealth.degraded("Ratis process adapter health supplier timed out");
    }

    private int degradedResponseWriteTimeoutMillis() {
        return Math.min(stopTimeoutMillis, MAX_DEGRADED_RESPONSE_WRITE_TIMEOUT_MILLIS);
    }

    private void closeRejected(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // A rejected caller will retry through the process health probe.
        } finally {
            acceptedSockets.removeAfterWorkerClose(socket);
        }
    }

    private void closeAfterFailedHandoff(Socket socket, Throwable failure) {
        RatisProcessAdapterSocketWire.closeAfterFailedHandoff(socket, failure);
        acceptedSockets.removeAfterWorkerClose(socket);
    }

    private static ExecutorService boundedExecutor(int maxConcurrentRequests, String threadName) {
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

    private static Thread thread(Runnable task, String name) {
        var thread = new Thread(task, name);
        thread.setDaemon(true);
        return thread;
    }

    private static String singleLineMessage(Throwable failure) {
        var message = failure == null ? null : failure.getMessage();
        var fallback = failure == null ? "unknown failure" : failure.getClass().getSimpleName();
        return RatisProcessAdapterText.sanitizeSingleLine(
                message,
                fallback,
                RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS
        );
    }

    private static String singleLine(String message) {
        return RatisProcessAdapterText.sanitizeSingleLine(
                message,
                "unknown failure",
                RatisProcessAdapterHealth.MAX_MESSAGE_CHARACTERS
        );
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }
}
