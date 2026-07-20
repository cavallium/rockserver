package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RocksDBStatistics;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

class RocksDBStatisticsTest {

    @Test
    void awaitStatisticsThreadTerminationReportsStuckThread() throws Exception {
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
                    () -> awaitStatisticsThreadTermination(thread, 1L));

            assertEquals("Timed out while closing RocksDB statistics executor", failure.getMessage());
        } finally {
            release.countDown();
            thread.join();
        }
    }

    @Test
    void awaitStatisticsThreadTerminationRetriesAfterInterruptionAndRestoresInterrupt() throws Exception {
        var thread = Thread.ofPlatform().start(() ->
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100)));

        try {
            Thread.currentThread().interrupt();
            awaitStatisticsThreadTermination(thread, TimeUnit.SECONDS.toMillis(2));

            assertTrue(Thread.interrupted());
            assertFalse(thread.isAlive());
        } finally {
            clearInterruptedStatus();
            thread.join();
        }
    }

    @Test
    void statisticsSleepStopsAndRestoresInterruptWhenInterrupted() throws Exception {
        try {
            Thread.currentThread().interrupt();
            assertFalse(sleepUntilNextStatisticsCollection(0L, 1L));
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

    private static void awaitStatisticsThreadTermination(Thread thread, long timeoutMillis) throws Exception {
        var method = RocksDBStatistics.class.getDeclaredMethod("awaitStatisticsThreadTermination", Thread.class, long.class);
        method.setAccessible(true);
        invokeReflected(() -> method.invoke(null, thread, timeoutMillis));
    }

    private static boolean sleepUntilNextStatisticsCollection(long taskStartTime, long taskEndTime) throws Exception {
        var method = RocksDBStatistics.class.getDeclaredMethod(
                "sleepUntilNextStatisticsCollection",
                long.class,
                long.class
        );
        method.setAccessible(true);
        return (Boolean) invokeReflectedResult(() -> method.invoke(null, taskStartTime, taskEndTime));
    }

    private static void invokeReflected(ReflectiveInvocation invocation) throws Exception {
        try {
            invocation.invoke();
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw e;
        }
    }

    private static <T> T invokeReflectedResult(ReflectiveResultInvocation<T> invocation) throws Exception {
        try {
            return invocation.invoke();
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof Exception exception) {
                throw exception;
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

    @FunctionalInterface
    private interface ReflectiveInvocation {

        void invoke() throws Exception;
    }

    @FunctionalInterface
    private interface ReflectiveResultInvocation<T> {

        T invoke() throws Exception;
    }
}
