package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.EmbeddedDB;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EmbeddedDBShutdownExecutorTest {

    @Test
    void shutdownExecutorRetriesAwaitAfterInterruptionAndRestoresInterrupt() throws Exception {
        var executor = new InterruptingThenTerminatingScheduledExecutor();

        try {
            shutdownExecutor(executor);

            assertTrue(Thread.interrupted());
            assertEquals(1, executor.shutdownNowCalls.get());
            assertEquals(2, executor.awaitTerminationCalls.get());
        } finally {
            clearInterruptedStatus();
        }
    }

    private static void shutdownExecutor(ScheduledExecutorService executor) throws Exception {
        var method = EmbeddedDB.class.getDeclaredMethod("shutdownExecutor",
                ScheduledExecutorService.class,
                Logger.class);
        method.setAccessible(true);
        try {
            method.invoke(null, executor, LoggerFactory.getLogger(EmbeddedDBShutdownExecutorTest.class));
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private abstract static class TrackingScheduledExecutor extends AbstractExecutorService
            implements ScheduledExecutorService {

        protected final AtomicInteger shutdownNowCalls = new AtomicInteger();
        protected final AtomicInteger awaitTerminationCalls = new AtomicInteger();
        private volatile boolean shutdown;

        @Override
        public void shutdown() {
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
            return shutdown && awaitTerminationCalls.get() > 1;
        }

        @Override
        public void execute(@NotNull Runnable command) {
            throw new UnsupportedOperationException("execute is not used by this test");
        }

        @Override
        public @NotNull ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException("schedule is not used by this test");
        }

        @Override
        public <V> @NotNull ScheduledFuture<V> schedule(@NotNull Callable<V> callable,
                                                        long delay,
                                                        @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException("schedule is not used by this test");
        }

        @Override
        public @NotNull ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command,
                                                               long initialDelay,
                                                               long period,
                                                               @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException("scheduleAtFixedRate is not used by this test");
        }

        @Override
        public @NotNull ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command,
                                                                  long initialDelay,
                                                                  long delay,
                                                                  @NotNull TimeUnit unit) {
            throw new UnsupportedOperationException("scheduleWithFixedDelay is not used by this test");
        }
    }

    private static final class InterruptingThenTerminatingScheduledExecutor extends TrackingScheduledExecutor {

        @Override
        public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
            if (awaitTerminationCalls.incrementAndGet() == 1) {
                throw new InterruptedException("await interrupted");
            }
            return true;
        }
    }

}
