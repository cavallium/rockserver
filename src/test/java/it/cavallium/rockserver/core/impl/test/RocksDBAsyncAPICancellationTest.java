package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

class RocksDBAsyncAPICancellationTest {

    @Test
    void cancelingEnsureMergeOperatorCancelsActiveCheck() {
        var check = new CompletableFuture<Long>();
        var api = new RecordingAPI(check, () -> CompletableFuture.completedFuture(3L));
        var response = api.ensureMergeOperatorAsync("operator", "Operator", new byte[] {1});

        assertTrue(response.cancel(true));

        assertTrue(check.isCancelled());
        assertEquals(0, api.uploads.get());
    }

    @Test
    void cancelingEnsureMergeOperatorCancelsActiveUpload() {
        var check = new CompletableFuture<Long>();
        var upload = new CompletableFuture<Long>();
        var api = new RecordingAPI(check, () -> upload);
        var response = api.ensureMergeOperatorAsync("operator", "Operator", new byte[] {1});
        check.complete(null);

        assertTrue(response.cancel(true));

        assertTrue(upload.isCancelled());
        assertEquals(1, api.uploads.get());
    }

    @Test
    void cancelingEnsureMergeOperatorCancelsLatePublishedUpload() throws Exception {
        var check = new CompletableFuture<Long>();
        var upload = new CompletableFuture<Long>();
        var uploadStarted = new CountDownLatch(1);
        var releaseUpload = new CountDownLatch(1);
        var api = new RecordingAPI(check, () -> {
            uploadStarted.countDown();
            await(releaseUpload);
            return upload;
        });
        var response = api.ensureMergeOperatorAsync("operator", "Operator", new byte[] {1});
        var completer = new Thread(() -> check.complete(null));
        completer.start();
        assertTrue(uploadStarted.await(5, TimeUnit.SECONDS));

        assertTrue(response.cancel(true));
        releaseUpload.countDown();
        completer.join(TimeUnit.SECONDS.toMillis(5));

        assertFalse(completer.isAlive());
        assertTrue(upload.isCancelled());
    }

    @Test
    void ensureMergeOperatorReturnsExistingVersionWithoutUpload() {
        var api = new RecordingAPI(CompletableFuture.completedFuture(7L),
                () -> CompletableFuture.completedFuture(3L));

        assertEquals(7L, api.ensureMergeOperatorAsync("operator", "Operator", new byte[] {1}).join());
        assertEquals(0, api.uploads.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CompletionException(e);
        }
    }

    private static final class RecordingAPI implements RocksDBAsyncAPI {
        private final CompletableFuture<Long> check;
        private final Supplier<CompletableFuture<Long>> upload;
        private final AtomicInteger uploads = new AtomicInteger();

        private RecordingAPI(CompletableFuture<Long> check, Supplier<CompletableFuture<Long>> upload) {
            this.check = check;
            this.upload = upload;
        }

        @Override
        public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
            return check;
        }

        @Override
        public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
            uploads.incrementAndGet();
            return upload.get();
        }
    }
}
