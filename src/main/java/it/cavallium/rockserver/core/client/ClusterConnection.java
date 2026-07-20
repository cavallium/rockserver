package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static it.cavallium.rockserver.core.common.FutureUtils.unwrapCompletionFailure;

public final class ClusterConnection extends BaseConnection {

    public enum ReadPolicy {
        PRIMARY,
        ROUND_ROBIN_REPLICAS
    }

    private final String name;
    private final List<Node> nodes;
    private final int primaryIndex;
    private final ReadPolicy readPolicy;
    private final long unhealthyCooldownNanos;
    private final AtomicInteger readCursor = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
        @Override
        public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
            return executeSync(req);
        }
    };

    private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
        @Override
        @SuppressWarnings("unchecked")
        public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle<?> single) {
                return (RA) executeAsyncSingle((RocksDBAPICommand.RocksDBAPICommandSingle<R>) single);
            }
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandStream<?> stream) {
                return (RA) executeAsyncStream((RocksDBAPICommand.RocksDBAPICommandStream<R>) stream);
            }
            return RocksDBAsyncAPI.super.requestAsync(req);
        }
    };

    public ClusterConnection(String name,
                             List<? extends RocksDBConnection> connections,
                             int primaryIndex,
                             ReadPolicy readPolicy) {
        this(name, connections, primaryIndex, readPolicy, Duration.ofSeconds(2));
    }

    public ClusterConnection(String name,
                             List<? extends RocksDBConnection> connections,
                             int primaryIndex,
                             ReadPolicy readPolicy,
                             Duration unhealthyCooldown) {
        super(name);
        if (connections == null || connections.isEmpty()) {
            throw new IllegalArgumentException("Cluster connection requires at least one endpoint");
        }
        if (primaryIndex < 0 || primaryIndex >= connections.size()) {
            throw new IllegalArgumentException("Primary index out of range: " + primaryIndex);
        }
        this.name = name == null ? "cluster" : name;
        this.primaryIndex = primaryIndex;
        this.readPolicy = requireArgument(readPolicy, "readPolicy");
        this.unhealthyCooldownNanos = Math.max(0L, requireArgument(unhealthyCooldown, "unhealthyCooldown").toNanos());
        this.nodes = new ArrayList<>(connections.size());
        for (RocksDBConnection connection : connections) {
            this.nodes.add(new Node(requireArgument(connection, "connection")));
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    public static ClusterConnection primaryWithReplicas(String name,
                                                        RocksDBConnection primary,
                                                        List<? extends RocksDBConnection> replicas,
                                                        ReadPolicy readPolicy) {
        var requiredReplicas = requireArgument(replicas, "replicas");
        var connections = new ArrayList<RocksDBConnection>(1 + requiredReplicas.size());
        connections.add(requireArgument(primary, "primary"));
        connections.addAll(requiredReplicas);
        return new ClusterConnection(name, connections, 0, readPolicy);
    }

    @Override
    public URI getUrl() {
        return URI.create("rockserver-cluster:///" + name);
    }

    @Override
    public RocksDBSyncAPI getSyncApi() {
        return syncApi;
    }

    @Override
    public RocksDBAsyncAPI getAsyncApi() {
        return asyncApi;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        for (Node node : nodes) {
            try {
                node.connection.close();
            } catch (IOException | RuntimeException | Error e) {
                failure = addFailure(failure, e);
            }
        }
        if (failure != null) {
            throwCloseFailure(failure);
        }
    }

    private <R, RS, RA> RS executeSync(RocksDBAPICommand<R, RS, RA> req) {
        requireOpen();
        var request = requireArgument(req, "req");
        if (mustUsePrimary(request)) {
            return invokeSync(primary(), request);
        }

        RuntimeException lastFailure = null;
        for (Node node : readOrder()) {
            try {
                return invokeSync(node, request);
            } catch (RuntimeException e) {
                var failure = endpointFailure(e);
                if (!failure.retryable()) {
                    throwEndpointFailure(failure.cause());
                }
                lastFailure = (RuntimeException) failure.cause();
                markUnhealthy(node);
            }
        }
        if (lastFailure != null) {
            throw lastFailure;
        }
        throw noClusterEndpointsAvailable();
    }

    private <R> CompletableFuture<R> executeAsyncSingle(RocksDBAPICommand.RocksDBAPICommandSingle<R> req) {
        var request = requireArgument(req, "req");
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        if (mustUsePrimary(request)) {
            return invokeAsyncSingle(primary(), request);
        }
        var result = new FailoverFuture<R>();
        executeAsyncSingle(request, readOrder(), 0, result, null);
        return result;
    }

    private <R> void executeAsyncSingle(RocksDBAPICommand.RocksDBAPICommandSingle<R> req,
                                        List<Node> order,
                                        int index,
                                        FailoverFuture<R> result,
                                        Throwable lastFailure) {
        if (result.isDone()) {
            return;
        }
        if (index >= order.size()) {
            result.completeExceptionally(lastFailure != null ? lastFailure : noClusterEndpointsAvailable());
            return;
        }
        Node node = order.get(index);
        CompletableFuture<R> endpointResult;
        try {
            endpointResult = invokeAsyncSingle(node, req);
        } catch (RuntimeException e) {
            if (result.isDone()) {
                return;
            }
            var failure = endpointFailure(e);
            if (completeIfNonRetryable(result, failure)) {
                return;
            }
            markUnhealthy(node);
            executeAsyncSingle(req, order, index + 1, result, failure.cause());
            return;
        }
        if (!result.publishAttempt(endpointResult)) {
            return;
        }
        endpointResult.whenComplete((value, error) -> {
            result.clearAttempt(endpointResult);
            if (result.isDone()) {
                return;
            }
            if (error == null) {
                result.complete(value);
            } else {
                var failure = endpointFailure(error);
                if (completeIfNonRetryable(result, failure)) {
                    return;
                }
                markUnhealthy(node);
                executeAsyncSingle(req, order, index + 1, result, failure.cause());
            }
        });
    }

    private <R> Publisher<R> executeAsyncStream(RocksDBAPICommand.RocksDBAPICommandStream<R> req) {
        var request = requireArgument(req, "req");
        if (closed.get()) {
            return Flux.error(closedException());
        }
        return invokeAsyncStream(selectForStream(request), request);
    }

    private <R, RS, RA> RS invokeSync(Node node, RocksDBAPICommand<R, RS, RA> req) {
        return req.handleSync(node.connection.getSyncApi());
    }

    private <R> CompletableFuture<R> invokeAsyncSingle(Node node, RocksDBAPICommand.RocksDBAPICommandSingle<R> req) {
        return req.handleAsync(node.connection.getAsyncApi());
    }

    private <R> Publisher<R> invokeAsyncStream(Node node, RocksDBAPICommand.RocksDBAPICommandStream<R> req) {
        return req.handleAsync(node.connection.getAsyncApi());
    }

    private Node selectForStream(RocksDBAPICommand<?, ?, ?> req) {
        if (mustUsePrimary(req)) {
            return primary();
        }
        List<Node> order = readOrder();
        return order.isEmpty() ? primary() : order.getFirst();
    }

    private boolean mustUsePrimary(RocksDBAPICommand<?, ?, ?> req) {
        if (!req.isReadOnly()) {
            return true;
        }
        if (readPolicy == ReadPolicy.PRIMARY) {
            return true;
        }
        return isEndpointLocal(req);
    }

    private boolean isEndpointLocal(RocksDBAPICommand<?, ?, ?> req) {
        return req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.SeekTo
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent
                || req instanceof RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll;
    }

    private Node primary() {
        return nodes.get(primaryIndex);
    }

    private void requireOpen() {
        if (closed.get()) {
            throw closedException();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException("Cluster connection is closed");
    }

    private static IllegalStateException noClusterEndpointsAvailable() {
        return new IllegalStateException("No cluster endpoints available");
    }

    private List<Node> readOrder() {
        int size = nodes.size();
        int start = Math.floorMod(readCursor.getAndIncrement(), size);
        long now = System.nanoTime();
        var healthy = new ArrayList<Node>(size);
        var unhealthy = new ArrayList<Node>(size);
        for (int i = 0; i < size; i++) {
            Node node = nodes.get((start + i) % size);
            if (node.unhealthyUntilNanos.get() <= now) {
                healthy.add(node);
            } else {
                unhealthy.add(node);
            }
        }
        if (!healthy.isEmpty()) {
            return healthy;
        }
        return unhealthy;
    }

    private void markUnhealthy(Node node) {
        if (unhealthyCooldownNanos > 0) {
            node.unhealthyUntilNanos.set(System.nanoTime() + unhealthyCooldownNanos);
        }
    }

    private static Throwable addFailure(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwCloseFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IOException("Unexpected cluster endpoint close failure", failure);
    }

    private static Throwable unwrapEndpointFailure(Throwable failure) {
        if (failure instanceof CompletionException completionException) {
            return unwrapCompletionFailure(completionException, "cluster endpoint request failed");
        }
        return failure;
    }

    private static RuntimeException retryableFailure(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        return new CompletionException("cluster endpoint request failed", failure);
    }

    private static EndpointFailure endpointFailure(Throwable failure) {
        var cause = unwrapEndpointFailure(failure);
        if (cause instanceof Error || isTerminalApiFailure(cause)) {
            return new EndpointFailure(cause, false);
        }
        return new EndpointFailure(retryableFailure(cause), true);
    }

    private static void throwEndpointFailure(Throwable failure) {
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        throw new CompletionException("cluster endpoint request failed", failure);
    }

    private static boolean completeIfNonRetryable(CompletableFuture<?> result, EndpointFailure failure) {
        if (failure.retryable()) {
            return false;
        }
        result.completeExceptionally(failure.cause());
        return true;
    }

    private static boolean isTerminalApiFailure(Throwable failure) {
        return failure instanceof RocksDBException rocksDBException
                && rocksDBException.getErrorUniqueId() != RocksDBException.RocksDBErrorType.INTERNAL_ERROR;
    }

    private record EndpointFailure(Throwable cause, boolean retryable) {
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static final class Node {
        private final RocksDBConnection connection;
        private final AtomicLong unhealthyUntilNanos = new AtomicLong();

        private Node(RocksDBConnection connection) {
            this.connection = connection;
        }
    }

    private static final class FailoverFuture<R> extends CompletableFuture<R> {
        private final AtomicReference<CompletableFuture<?>> activeAttempt = new AtomicReference<>();
        private final AtomicBoolean interruptOnCancel = new AtomicBoolean();

        private boolean publishAttempt(CompletableFuture<?> attempt) {
            activeAttempt.set(attempt);
            if (isCancelled()) {
                activeAttempt.compareAndSet(attempt, null);
                attempt.cancel(interruptOnCancel.get());
                return false;
            }
            return true;
        }

        private void clearAttempt(CompletableFuture<?> attempt) {
            activeAttempt.compareAndSet(attempt, null);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            if (mayInterruptIfRunning) {
                interruptOnCancel.set(true);
            }
            var canceled = super.cancel(mayInterruptIfRunning);
            if (canceled) {
                var attempt = activeAttempt.get();
                if (attempt != null) {
                    attempt.cancel(interruptOnCancel.get());
                }
            }
            return canceled;
        }
    }
}
