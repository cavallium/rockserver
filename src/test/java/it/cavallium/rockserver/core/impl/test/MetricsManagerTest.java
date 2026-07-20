package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.reactivex.rxjava3.core.Completable;
import it.cavallium.rockserver.core.impl.MetricsManager;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class MetricsManagerTest {

    @Test
    void rejectsNullConfigWithExplicitDiagnostics() {
        assertIllegalArgument("config must not be null", MetricsManagerTest::createWithNullConfig);
    }

    @Test
    void awaitCloseRejectsInvalidInputsWithExplicitDiagnostics() {
        assertIllegalArgument("closeOperation must not be null",
                () -> awaitClose(null, "metrics resource", 1L, TimeUnit.SECONDS));
        assertIllegalArgument("resourceName must not be null",
                () -> awaitClose(Completable.complete(), null, 1L, TimeUnit.SECONDS));
        assertIllegalArgument("unit must not be null",
                () -> awaitClose(Completable.complete(), "metrics resource", 1L, null));
        assertIllegalArgument("timeout must be positive",
                () -> awaitClose(Completable.complete(), "metrics resource", 0L, TimeUnit.SECONDS));
        assertIllegalArgument("timeout must be positive",
                () -> awaitClose(Completable.complete(), "metrics resource", -1L, TimeUnit.SECONDS));
    }

    @Test
    void awaitCloseAcceptsCompletedCloseOperation() {
        assertDoesNotThrow(() -> awaitClose(Completable.complete(), "metrics resource", 1L, TimeUnit.SECONDS));
    }

    @Test
    void awaitCloseReportsTimeout() {
        var failure = assertThrows(IllegalStateException.class,
                () -> awaitClose(Completable.never(), "metrics resource", 1L, TimeUnit.MILLISECONDS));

        assertEquals("metrics resource close did not complete within timeout", failure.getMessage());
    }

    @Test
	void awaitCloseRestoresInterruptWhenBlockingAwaitIsInterrupted() {
        try {
            Thread.currentThread().interrupt();
            var failure = assertThrows(RuntimeException.class,
                    () -> awaitClose(Completable.never(), "metrics resource", 1L, TimeUnit.SECONDS));

            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

	@SuppressWarnings("SameParameterValue")
	private static void awaitClose(Completable closeOperation, String resourceName, long timeout, TimeUnit unit)
			throws Exception {
        var method = MetricsManager.class.getDeclaredMethod(
                "awaitClose",
                Completable.class,
                String.class,
                long.class,
                TimeUnit.class
        );
        method.setAccessible(true);
        invokeReflected(() -> method.invoke(null, closeOperation, resourceName, timeout, unit));
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private static void assertIllegalArgument(String message, Executable executable) {
        var failure = assertThrows(IllegalArgumentException.class, executable);
        assertEquals(message, failure.getMessage());
    }

    private static void createWithNullConfig() {
        try (var ignored = new MetricsManager(null)) {
            throw new AssertionError("constructor should reject null config");
        }
    }

    @FunctionalInterface
    private interface ReflectiveInvocation {

        void invoke() throws Exception;
    }
}
