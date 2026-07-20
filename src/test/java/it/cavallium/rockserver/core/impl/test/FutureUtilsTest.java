package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.FutureUtils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

class FutureUtilsTest {

    @Test
    void propagateCancellationMirrorsSourceCancellation() {
        var source = new CompletableFuture<String>();
        var response = FutureUtils.propagateCancellation(source, source);

        assertTrue(source.cancel(true));

        assertTrue(response.isCancelled());
    }

    @Test
    void mapCancellationMirrorsSourceCancellationWithoutInvokingMapper() {
        var mapped = new AtomicBoolean();
        var source = new CompletableFuture<String>();
        var response = FutureUtils.mapCancellation(source, value -> {
            mapped.set(true);
            return value.length();
        });

        assertTrue(source.cancel(true));

        assertTrue(response.isCancelled());
        assertFalse(mapped.get());
    }

    @Test
    void composeCancellationMirrorsNextStageCancellation() {
        var next = new CompletableFuture<Integer>();
        var response = FutureUtils.composeCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> next
        );

        assertTrue(next.cancel(true));

        assertTrue(response.isCancelled());
    }

    @Test
    void composeCancellationCancelsActiveNextStage() {
        var next = new CompletableFuture<Integer>();
        var response = FutureUtils.composeCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> next
        );

        assertTrue(response.cancel(true));

        assertTrue(next.isCancelled());
    }

    @Test
    void composeCancellationMirrorsMapperCancellation() {
        var response = FutureUtils.composeCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> {
                    throw new CompletionException(new CancellationException("mapper cancelled"));
                }
        );

        assertTrue(response.isCancelled());
    }

    @Test
    void composeCancellationMirrorsNestedMapperCancellation() {
        var response = FutureUtils.composeCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> {
                    throw new CompletionException(new CompletionException(
                            new CancellationException("mapper cancelled")
                    ));
                }
        );

        assertTrue(response.isCancelled());
    }

    @Test
    void propagatedFailureFlattensNestedCompletionWrappers() {
        var source = new CompletableFuture<String>();
        var response = FutureUtils.propagateCancellation(source, source);
        var fatalFailure = new AssertionError("fatal source failure");

        source.completeExceptionally(new CompletionException(new CompletionException(fatalFailure)));

        var error = assertThrows(CompletionException.class, response::join);
        assertSame(fatalFailure, error.getCause());
    }

    @Test
    void awaitInterruptiblyPropagatesNestedFatalFailureCause() {
        var source = new CompletableFuture<String>();
        var fatalFailure = new AssertionError("fatal await failure");

        source.completeExceptionally(new CompletionException(new CompletionException(fatalFailure)));

        var error = assertThrows(AssertionError.class, () -> FutureUtils.awaitInterruptibly(source, "await"));
        assertSame(fatalFailure, error);
    }

    @Test
    void awaitInterruptiblyFlattensNestedCheckedFailureCause() {
        var source = new CompletableFuture<String>();
        var checkedFailure = new Exception("checked await failure");

        source.completeExceptionally(new CompletionException(new CompletionException(checkedFailure)));

        var error = assertThrows(CompletionException.class, () -> FutureUtils.awaitInterruptibly(source, "await"));
        assertEquals("await failed", error.getMessage());
        assertSame(checkedFailure, error.getCause());
    }

    @Test
    void unwrapCompletionExceptionWrapsCheckedCauseWithFallbackMessage() {
        var checkedFailure = new Exception("checked failure");

        var error = FutureUtils.unwrapCompletionException(
                new CompletionException(new CompletionException(checkedFailure)),
                "operation failed"
        );

        assertEquals("operation failed", error.getMessage());
        assertSame(checkedFailure, error.getCause());
    }

    @Test
    void unwrapCompletionExceptionPreservesExplicitCheckedFailureMessage() {
        var checkedFailure = new Exception("checked failure");

        var error = FutureUtils.unwrapCompletionException(
                new CompletionException("explicit operation failure", new CompletionException(checkedFailure)),
                "operation failed"
        );

        assertEquals("explicit operation failure", error.getMessage());
        assertSame(checkedFailure, error.getCause());
    }

    @Test
    void unwrapCompletionExceptionThrowsFatalCause() {
        var fatalFailure = new AssertionError("fatal failure");

        assertSame(fatalFailure, assertThrows(AssertionError.class, () -> {
            throw FutureUtils.unwrapCompletionException(new CompletionException(fatalFailure), "operation failed");
        }));
    }

    @Test
    void unwrapCompletionFailureReturnsFatalCause() {
        var fatalFailure = new AssertionError("fatal failure");

        assertSame(fatalFailure, FutureUtils.unwrapCompletionFailure(
                new CompletionException(new CompletionException(fatalFailure)),
                "operation failed"
        ));
    }

    @Test
    void mapperFailuresStillCompleteExceptionally() {
        var failure = new IllegalStateException("mapper failed");
        var response = FutureUtils.mapCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> {
                    throw failure;
                }
        );

        var error = assertThrows(CompletionException.class, response::join);

        assertSame(failure, error.getCause());
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        try (var executor = Executors.newSingleThreadExecutor()) {
            var future = new CompletableFuture<String>();
            var failure = new IllegalStateException("failure");

            assertIllegalArgument("source must not be null",
                    () -> FutureUtils.<String, String>mapCancellation(null, value -> value));
            assertIllegalArgument("mapper must not be null",
                    () -> FutureUtils.mapCancellation(CompletableFuture.completedFuture("value"), null));
            assertIllegalArgument("response must not be null",
                    () -> FutureUtils.propagateCancellation(null, future));
            assertIllegalArgument("cancellationTarget must not be null",
                    () -> FutureUtils.propagateCancellation(future, null));
            assertIllegalArgument("source must not be null",
                    () -> FutureUtils.<String, String>composeCancellation(null,
                            CompletableFuture::completedFuture));
            assertIllegalArgument("mapper must not be null",
                    () -> FutureUtils.composeCancellation(CompletableFuture.completedFuture("value"), null));
            assertIllegalArgument("executor must not be null",
                    () -> FutureUtils.submitCancellationAware(null, () -> "value"));
            assertIllegalArgument("supplier must not be null",
                    () -> FutureUtils.submitCancellationAware(executor, null));
            assertIllegalArgument("future must not be null",
                    () -> FutureUtils.awaitInterruptibly(null, "await"));
            assertIllegalArgument("operation must not be null",
                    () -> FutureUtils.awaitInterruptibly(future, null));
            assertIllegalArgument("failure must not be null",
                    () -> {
                        throw FutureUtils.unwrapCompletionException(null, "operation failed");
                    });
            assertIllegalArgument("fallbackMessage must not be null",
                    () -> {
                        throw FutureUtils.unwrapCompletionException(new CompletionException(failure), null);
                    });
            assertIllegalArgument("failure must not be null",
                    () -> unwrapCompletionFailureForValidation(null, "operation failed"));
            assertIllegalArgument("fallbackMessage must not be null",
                    () -> unwrapCompletionFailureForValidation(new CompletionException(failure), null));
            assertIllegalArgument("future must not be null",
                    () -> FutureUtils.cancelWithSuppressed(null, failure));
            assertIllegalArgument("failure must not be null",
                    () -> FutureUtils.cancelWithSuppressed(future, null));
        }
    }

    @SuppressWarnings("ThrowableNotThrown")
    private static void unwrapCompletionFailureForValidation(CompletionException failure, String fallbackMessage) {
        FutureUtils.unwrapCompletionFailure(failure, fallbackMessage);
    }

    @Test
    void composeCancellationRejectsNullMapperResultWithExplicitDiagnostic() {
        var response = FutureUtils.composeCancellation(
                CompletableFuture.completedFuture("value"),
                _ -> null
        );

        var error = assertThrows(CompletionException.class, response::join);
        var cause = assertInstanceOf(IllegalStateException.class, error.getCause());
        assertEquals("mapper result must not be null", cause.getMessage());
    }

    private static void assertIllegalArgument(String message, Executable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation);
        assertEquals(message, error.getMessage());
    }
}
