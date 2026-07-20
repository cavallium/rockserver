package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class EmbeddedDBBatchWriteFailureTest {

    @Test
    void awaitBatchWritePreservesNestedRocksDBExceptionCause() {
        var writeFailure = RocksDBException.of(
                RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                "invalid nested batch write"
        );
        var write = new CompletableFuture<Void>();
        write.completeExceptionally(new CompletionException(new CompletionException(writeFailure)));

        var failure = assertThrows(RocksDBException.class, () -> awaitBatchWrite(write));

        assertSame(writeFailure, failure);
    }

    @Test
    void awaitBatchWritePropagatesNestedFatalCompletionCause() {
        var writeFailure = new AssertionError("fatal nested batch write");
        var write = new CompletableFuture<Void>();
        write.completeExceptionally(new CompletionException(new CompletionException(writeFailure)));

        var failure = assertThrows(AssertionError.class, () -> awaitBatchWrite(write));

        assertSame(writeFailure, failure);
    }

    private static void awaitBatchWrite(CompletableFuture<Void> write) throws Throwable {
        var method = EmbeddedDB.class.getDeclaredMethod("awaitBatchWrite", CompletableFuture.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(null, write, "putBatch");
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
