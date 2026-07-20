package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.server.ThriftServer;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

class ThriftServerLifecycleTest {

    @Test
    void closeWaitsForOwnedServeThreadAndWorkerExecutor() throws Exception {
        Thread thriftThread;
        ExecutorService thriftWorkerExecutor;
        try (var server = new ThriftServer(new TestConnection(), "127.0.0.1", 0)) {
            thriftThread = thriftThread(server);
            thriftWorkerExecutor = thriftWorkerExecutor(server);
            server.start();
            assertTrue(thriftThread.isAlive());
        }

        assertFalse(thriftThread.isAlive());
        assertTrue(thriftWorkerExecutor.isTerminated());
    }

    @Test
    void closeWithoutStartTerminatesOwnedWorkerExecutor() throws Exception {
        ExecutorService thriftWorkerExecutor;
        try (var server = new ThriftServer(new TestConnection(), "127.0.0.1", 0)) {
            thriftWorkerExecutor = thriftWorkerExecutor(server);
        }

        assertTrue(thriftWorkerExecutor.isTerminated());
    }

    @Test
    void startAfterCloseFailsClosed() throws Exception {
        var server = new ThriftServer(new TestConnection(), "127.0.0.1", 0);
        server.close();

        assertThrows(IllegalStateException.class, server::start);
    }

    @Test
    void awaitThreadTerminationReportsStuckThread() throws Exception {
        var release = new CountDownLatch(1);
        var thread = Thread.ofPlatform().start(() -> {
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        try {
            var failure = assertThrows(IllegalStateException.class,
                    () -> awaitThreadTermination(thread, 1L));
            assertEquals("Thrift server thread did not terminate after shutdown", failure.getMessage());
        } finally {
            release.countDown();
            thread.join();
        }
    }

    @Test
    void awaitThreadTerminationRetriesAfterInterruptionAndRestoresInterrupt() throws Exception {
        var thread = Thread.ofPlatform().start(() ->
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)));

        try {
            Thread.currentThread().interrupt();
            awaitThreadTermination(thread, TimeUnit.SECONDS.toMillis(2));

            assertTrue(Thread.interrupted());
            assertFalse(thread.isAlive());
        } finally {
            clearInterruptedStatus();
            thread.join();
        }
    }

    @Test
    @SuppressWarnings("resource")
    void closeThriftWorkerExecutorReportsUnterminatedForcedShutdown() {
        var executor = new NonTerminatingExecutor();

        var failure = assertThrows(IllegalStateException.class, () -> closeThriftWorkerExecutor(executor));

        assertEquals("Thrift worker executor did not terminate after forced shutdown", failure.getMessage());
        assertEquals(1, executor.shutdowns.get());
        assertEquals(1, executor.shutdownNowCalls.get());
        assertEquals(2, executor.awaitTerminationCalls.get());
    }

    @Test
    void closeThriftWorkerExecutorRetriesAfterInterruptionAndRestoresInterrupt() throws Exception {
        var executor = new InterruptingThenTerminatingExecutor();

        try {
            closeThriftWorkerExecutor(executor);

            assertTrue(Thread.interrupted());
            assertEquals(1, executor.shutdowns.get());
            assertEquals(1, executor.shutdownNowCalls.get());
            assertEquals(2, executor.awaitTerminationCalls.get());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void constructorRollbackSuppressesCleanupFailureOntoStartupFailure() throws Exception {
        var startupFailure = new AssertionError("startup failed");
        var cleanupFailure = new AssertionError("transport close failed");
        var method = ThriftServer.class.getDeclaredMethod(
                "suppressConstructorCleanupFailure",
                Throwable.class,
                Runnable.class
        );
        method.setAccessible(true);

        method.invoke(null, startupFailure, (Runnable) () -> {
            throw cleanupFailure;
        });

        assertEquals(1, startupFailure.getSuppressed().length);
        assertSame(cleanupFailure, startupFailure.getSuppressed()[0]);
    }

    private static void awaitThreadTermination(Thread thread, long timeoutMillis) throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("awaitThreadTermination", Thread.class, long.class);
        method.setAccessible(true);
        invokeReflected(() -> method.invoke(null, thread, timeoutMillis));
    }

    private static void closeThriftWorkerExecutor(ExecutorService executor) throws Exception {
        var method = ThriftServer.class.getDeclaredMethod("closeThriftWorkerExecutor", ExecutorService.class);
        method.setAccessible(true);
        invokeReflected(() -> method.invoke(null, executor));
    }

    private static void invokeReflected(ReflectiveInvocation invocation) throws Exception {
        try {
            invocation.invoke();
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    private static Thread thriftThread(ThriftServer server) throws Exception {
        var field = ThriftServer.class.getDeclaredField("thriftThread");
        field.setAccessible(true);
        return (Thread) field.get(server);
    }

    private static ExecutorService thriftWorkerExecutor(ThriftServer server) throws Exception {
        var field = ThriftServer.class.getDeclaredField("thriftWorkerExecutor");
        field.setAccessible(true);
        return (ExecutorService) field.get(server);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    @FunctionalInterface
    private interface ReflectiveInvocation {

        void invoke() throws ReflectiveOperationException;
    }

    private static final class TestConnection implements RocksDBConnection {

        private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
        };
        private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
        };

        @Override
        public URI getUrl() {
            return URI.create("memory://thrift-server-lifecycle-test");
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
        public void close() {
        }
    }

    private abstract static class TrackingExecutor extends AbstractExecutorService {

        protected final AtomicInteger shutdowns = new AtomicInteger();
        protected final AtomicInteger shutdownNowCalls = new AtomicInteger();
        protected final AtomicInteger awaitTerminationCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
            shutdowns.incrementAndGet();
            shutdown = true;
        }

        @Override
        public @NotNull List<Runnable> shutdownNow() {
            shutdownNowCalls.incrementAndGet();
            shutdown = true;
            return List.of();
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
        public void execute(@NotNull Runnable command) {
            throw new IllegalStateException("executor is closed");
        }
    }

    private static final class NonTerminatingExecutor extends TrackingExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) {
            awaitTerminationCalls.incrementAndGet();
            return false;
        }
    }

    private static final class InterruptingThenTerminatingExecutor extends TrackingExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
            if (awaitTerminationCalls.incrementAndGet() == 1) {
                throw new InterruptedException("await interrupted");
            }
            return true;
        }
    }
}
