package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.Future;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class GrpcConnectionFutureBridgeTest {

    @Test
    void completableFutureBridgeCancellationReachesSource() {
        var source = new CompletableFuture<String>();
        var response = bridge(source);

        assertTrue(response.cancel(true));

        assertTrue(source.isCancelled());
    }

    @Test
    void listenableFutureBridgeCancellationReachesSource() {
        var source = new TestListenableFuture<String>();
        var response = bridge(source, Function.identity());

        assertTrue(response.cancel(true));

        assertTrue(source.isCancelled());
    }

    @Test
    void listenableFutureMapperRuntimeFailureCompletesResponseWithPublicInternalError() {
        var source = new TestListenableFuture<String>();
        var mappingFailure = new IllegalStateException("mapping failed");
        var response = bridge(source, _ -> {
            throw mappingFailure;
        });

        source.set("value");

        var failure = assertInstanceOf(RocksDBException.class,
                assertThrows(CompletionException.class, response::join).getCause());
        assertEquals(RocksDBException.RocksDBErrorType.INTERNAL_ERROR, failure.getErrorUniqueId());
        assertEquals("Failed to map gRPC response", failure.getMessage());
        assertSame(mappingFailure, failure.getCause());
    }

    @Test
    void listenableFutureMapperRocksDBFailureCompletesResponseExceptionally() {
        var source = new TestListenableFuture<String>();
        var mappingFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "bad response");
        var response = bridge(source, _ -> {
            throw mappingFailure;
        });

        source.set("value");

        assertSame(mappingFailure, assertThrows(CompletionException.class, response::join).getCause());
    }

    @Test
    void listenableFutureMapperNestedRocksDBFailureCompletesResponseExceptionally() {
        var source = new TestListenableFuture<String>();
        var mappingFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "bad response");
        var response = bridge(source, _ -> {
            throw new CompletionException(new CompletionException(mappingFailure));
        });

        source.set("value");

        assertSame(mappingFailure, assertThrows(CompletionException.class, response::join).getCause());
    }

    @Test
    void listenableFutureMapperFatalFailureCompletesResponseExceptionally() {
        var source = new TestListenableFuture<String>();
        var mappingFailure = new AssertionError("mapping failed");
        var response = bridge(source, _ -> {
            throw mappingFailure;
        });

        source.set("value");

        assertSame(mappingFailure, assertThrows(CompletionException.class, response::join).getCause());
    }

    @Test
    void listenableFutureMapperNestedFatalFailureCompletesResponseExceptionally() {
        var source = new TestListenableFuture<String>();
        var mappingFailure = new AssertionError("mapping failed");
        var response = bridge(source, _ -> {
            throw new CompletionException(new CompletionException(mappingFailure));
        });

        source.set("value");

        assertSame(mappingFailure, assertThrows(CompletionException.class, response::join).getCause());
    }

    @Test
    void completableFutureNestedGrpcStatusFailureMapsPublicRocksDBException() {
        var responseMessage = "nested completable response failure";
        var source = CompletableFuture.<String>failedFuture(
                new CompletionException(new CompletionException(publicGrpcFailure(responseMessage)))
        );
        var response = bridge(source);

        var failure = assertThrows(CompletionException.class, response::join).getCause();
        assertMappedGrpcFailure(failure, responseMessage);
    }

    @Test
    void listenableFutureNestedGrpcStatusFailureMapsPublicRocksDBException() {
        var responseMessage = "nested listenable response failure";
        var source = new TestListenableFuture<String>();
        var response = bridge(source, Function.identity());

        source.setFailure(new CompletionException(new CompletionException(publicGrpcFailure(responseMessage))));

        var failure = assertThrows(CompletionException.class, response::join).getCause();
        assertMappedGrpcFailure(failure, responseMessage);
    }

    @Test
    void reactiveFluxNestedGrpcStatusFailureMapsPublicRocksDBException() {
        var responseMessage = "nested reactive response failure";
        var response = bridge(Flux.error(new CompletionException(new CompletionException(publicGrpcFailure(responseMessage)))));

        StepVerifier.create(response)
                .expectErrorSatisfies(failure -> assertMappedGrpcFailure(failure, responseMessage))
                .verify();
    }

    @Test
    void synchronousWaitInterruptionCancelsSourceAndRestoresInterruptFlag() {
        var source = new CompletableFuture<String>();
        Thread.currentThread().interrupt();
        try {
            var failure = assertThrows(CompletionException.class, () -> awaitSync(source));

            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(source.isCancelled());
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void synchronousWaitPropagatesRuntimeFailureDirectly() {
        var failure = new IllegalStateException("request failed");

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> awaitSync(CompletableFuture.failedFuture(failure))));
    }

    @Test
    void synchronousWaitPropagatesFatalFailureDirectly() {
        var failure = new AssertionError("request failed");

        assertSame(failure, assertThrows(AssertionError.class,
                () -> awaitSync(CompletableFuture.failedFuture(failure))));
    }

    @Test
    void closeForcesGrpcChannelAndReportsUnterminatedShutdown() {
        var channel = new StuckManagedChannel();
        var connection = newGrpcConnection(channel);

        var failure = assertThrows(IllegalStateException.class, connection::close);

        assertEquals("gRPC channel did not terminate after forced shutdown", failure.getMessage());
        assertEquals(1, channel.shutdowns.get());
        assertEquals(1, channel.shutdownNowCalls.get());
        assertEquals(2, channel.awaitTerminationCalls.get());
    }

    @Test
    void closeRetriesChannelAwaitAfterInterruptionAndRestoresInterrupt() {
        var channel = new InterruptingThenTerminatingManagedChannel();
        var connection = newGrpcConnection(channel);

        try {
            connection.close();

            assertTrue(Thread.interrupted());
            assertEquals(1, channel.shutdowns.get());
            assertEquals(0, channel.shutdownNowCalls.get());
            assertEquals(2, channel.awaitTerminationCalls.get());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void closeReportsUnterminatedEventLoopShutdown() {
        var termination = nettyTermination(false, false, null);
        var connection = newGrpcConnection(new TerminatedManagedChannel(), eventLoopGroup(termination));

        var failure = assertThrows(IllegalStateException.class, connection::close);

        assertEquals("gRPC event loop group did not terminate after shutdown", failure.getMessage());
    }

    @Test
    void closeReportsEventLoopShutdownCause() {
        var eventLoopFailure = new IllegalStateException("event loop failed");
        var termination = nettyTermination(true, false, eventLoopFailure);
        var connection = newGrpcConnection(new TerminatedManagedChannel(), eventLoopGroup(termination));

        var failure = assertThrows(IllegalStateException.class, connection::close);

        assertSame(eventLoopFailure, failure);
    }

    @Test
    void closeSuppressesEventLoopFailureAfterChannelShutdownFailure() {
        var channelFailure = new IllegalStateException("channel failed");
        var eventLoopFailure = new IllegalStateException("event loop failed");
        var termination = nettyTermination(true, false, eventLoopFailure);
        var connection = newGrpcConnection(new FailingShutdownManagedChannel(channelFailure), eventLoopGroup(termination));

        var failure = assertThrows(IllegalStateException.class, connection::close);

        assertSame(channelFailure, failure);
        assertEquals(1, failure.getSuppressed().length);
        assertSame(eventLoopFailure, failure.getSuppressed()[0]);
    }

    @Test
    void closeIsIdempotentAndRejectsNewRequestsBeforeChannelUse() {
        var channel = new TerminatedManagedChannel();
        var connection = newGrpcConnection(channel);

        connection.close();
        connection.close();

        var syncError = assertThrows(IllegalStateException.class,
                () -> connection.getSyncApi().getAllColumnDefinitions());
        var asyncError = assertThrows(IllegalStateException.class,
                () -> connection.getAsyncApi().getAllColumnDefinitionsAsync());
        var unsupportedAsyncError = assertThrows(CompletionException.class,
                () -> connection.getAsyncApi()
                        .deleteAsync(0, 4, new Keys(Buf.wrap(new byte[]{1})), RequestType.none())
                        .join());

        assertEquals("gRPC connection is closed", syncError.getMessage());
        assertEquals("gRPC connection is closed", asyncError.getMessage());
        assertEquals("gRPC connection is closed", unsupportedAsyncError.getCause().getMessage());
        assertEquals(1, channel.shutdowns.get());
        assertEquals(0, channel.shutdownNowCalls.get());
    }

    @SuppressWarnings("unchecked")
    private static <T> CompletableFuture<T> bridge(CompletableFuture<T> source) {
        try {
            var method = GrpcConnection.class.getDeclaredMethod("toResponse", CompletableFuture.class);
            method.setAccessible(true);
            return (CompletableFuture<T>) method.invoke(null, source);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> Flux<T> bridge(Flux<T> source) {
        try {
            var method = GrpcConnection.class.getDeclaredMethod("toResponse", Flux.class);
            method.setAccessible(true);
            return (Flux<T>) method.invoke(null, source);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T, U> CompletableFuture<U> bridge(ListenableFuture<T> source, Function<T, U> mapper) {
        try {
            var method = GrpcConnection.class.getDeclaredMethod("toResponse", ListenableFuture.class, Function.class);
            method.setAccessible(true);
            return (CompletableFuture<U>) method.invoke(null, source, mapper);
        } catch (InvocationTargetException e) {
            throw new AssertionError(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T awaitSync(CompletableFuture<T> source) {
        try {
            var method = GrpcConnection.class.getDeclaredMethod("awaitSyncResponse", CompletableFuture.class);
            method.setAccessible(true);
            return (T) method.invoke(null, source);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (e.getCause() instanceof Error error) {
                throw error;
            }
            throw new AssertionError(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private static StatusRuntimeException publicGrpcFailure(String message) {
        return Status.INTERNAL
                .withDescription("RocksDBError: [uid:PUT_INVALID_REQUEST] " + message)
                .asRuntimeException();
    }

    private static void assertMappedGrpcFailure(Throwable failure, String message) {
        var rocksFailure = assertInstanceOf(RocksDBException.class, failure);
        assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, rocksFailure.getErrorUniqueId());
        assertEquals(message, rocksFailure.getMessage());
    }

    private static GrpcConnection newGrpcConnection(ManagedChannel channel) {
        return newGrpcConnection(channel, null);
    }

    private static GrpcConnection newGrpcConnection(ManagedChannel channel, EventLoopGroup eventLoopGroup) {
        try {
            var constructor = GrpcConnection.class.getDeclaredConstructor(
                    String.class,
                    URI.class,
                    ManagedChannel.class,
                    EventLoopGroup.class
            );
            constructor.setAccessible(true);
            return (GrpcConnection) constructor.newInstance(
                    "grpc-lifecycle-test",
                    URI.create("memory://grpc-lifecycle-test"),
                    channel,
                    eventLoopGroup
            );
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static EventLoopGroup eventLoopGroup(Future<?> termination) {
        return (EventLoopGroup) Proxy.newProxyInstance(
                EventLoopGroup.class.getClassLoader(),
                new Class<?>[]{EventLoopGroup.class},
                (_, method, args) -> {
                    if (method.getName().equals("shutdownGracefully") && method.getParameterCount() == 3) {
                        return termination;
                    }
                    return unsupportedProxyMethod("event loop group", method, args);
                }
        );
    }

    private static Future<?> nettyTermination(boolean awaited, boolean success, Throwable cause) {
        return (Future<?>) Proxy.newProxyInstance(
                Future.class.getClassLoader(),
                new Class<?>[]{Future.class},
                (_, method, args) -> switch (method.getName()) {
                    case "awaitUninterruptibly" -> awaited;
                    case "isSuccess" -> success;
                    case "cause" -> cause;
                    default -> unsupportedProxyMethod("event loop termination", method, args);
                }
        );
    }

    private static Object unsupportedProxyMethod(String target, Method method, Object[] args) {
        if (method.getDeclaringClass() == Object.class) {
            return switch (method.getName()) {
                case "toString" -> target;
                case "hashCode" -> System.identityHashCode(args);
                case "equals" -> false;
                default -> throw new UnsupportedOperationException(method.getName() + " is not used by this test");
            };
        }
        throw new UnsupportedOperationException(method.getName() + " is not used by this test");
    }

    private static final class TestListenableFuture<T> implements ListenableFuture<T> {
        private final CompletableFuture<T> delegate = new CompletableFuture<>();

        @Override
        public void addListener(Runnable listener, Executor executor) {
            delegate.whenComplete((_, _) -> executor.execute(listener));
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return delegate.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return delegate.isCancelled();
        }

        @Override
        public boolean isDone() {
            return delegate.isDone();
        }

        @Override
        public T get() throws InterruptedException, ExecutionException {
            return delegate.get();
        }

        @Override
        public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.get(timeout, unit);
        }

        private boolean set(T value) {
            return delegate.complete(value);
        }

        private boolean setFailure(Throwable failure) {
            return delegate.completeExceptionally(failure);
        }
    }

    private static final class StuckManagedChannel extends ManagedChannel {
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger shutdownNowCalls = new AtomicInteger();
        private final AtomicInteger awaitTerminationCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public ManagedChannel shutdown() {
            shutdowns.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return false;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                CallOptions callOptions) {
            throw new UnsupportedOperationException("newCall is not used by this test");
        }

        @Override
        public String authority() {
            return "grpc-lifecycle-test";
        }
    }

    private static final class TerminatedManagedChannel extends ManagedChannel {
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger shutdownNowCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public ManagedChannel shutdown() {
            shutdowns.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return true;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                CallOptions callOptions) {
            throw new UnsupportedOperationException("newCall is not used by this test");
        }

        @Override
        public String authority() {
            return "grpc-lifecycle-test";
        }
    }

    private static final class InterruptingThenTerminatingManagedChannel extends ManagedChannel {
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final AtomicInteger shutdownNowCalls = new AtomicInteger();
        private final AtomicInteger awaitTerminationCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public ManagedChannel shutdown() {
            shutdowns.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return awaitTerminationCalls.get() > 1;
        }

        @Override
        public ManagedChannel shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            shutdown = true;
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            if (awaitTerminationCalls.incrementAndGet() == 1) {
                throw new InterruptedException("await interrupted");
            }
            return true;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                CallOptions callOptions) {
            throw new UnsupportedOperationException("newCall is not used by this test");
        }

        @Override
        public String authority() {
            return "grpc-lifecycle-test";
        }
    }

    private static final class FailingShutdownManagedChannel extends ManagedChannel {
        private final RuntimeException failure;

        private FailingShutdownManagedChannel(RuntimeException failure) {
            this.failure = failure;
        }

        @Override
        public ManagedChannel shutdown() {
            throw failure;
        }

        @Override
        public boolean isShutdown() {
            return false;
        }

        @Override
        public boolean isTerminated() {
            return false;
        }

        @Override
        public ManagedChannel shutdownNow() {
            return this;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return false;
        }

        @Override
        public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
                MethodDescriptor<RequestT, ResponseT> methodDescriptor,
                CallOptions callOptions) {
            throw new UnsupportedOperationException("newCall is not used by this test");
        }

        @Override
        public String authority() {
            return "grpc-lifecycle-test";
        }
    }
}
