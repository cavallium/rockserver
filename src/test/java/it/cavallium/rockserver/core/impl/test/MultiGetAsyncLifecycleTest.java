package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class MultiGetAsyncLifecycleTest {

    @TempDir
    Path directory;

    @Test
    void queuedReadCopiesCallerKeysAndReleasesOwnershipBeforePublishing() throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-async-copy", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var expected = Buf.wrap(new byte[]{4, 5});
            api.put(0, column, key(1), expected, RequestType.none());
            var db = connection.getInternalDB();
            var pending = db.getPendingOpsCount();
            var executor = new ControlledExecutor();
            byte[] callerBytes = {1};
            Buf[] callerParts = {Buf.wrap(callerBytes)};
            var callerKeys = new ArrayList<>(List.of(new Keys(callerParts), key(2)));
            var request = db.getMultiAsyncInternal(column, callerKeys, new RangeBudget(2, 128),
                    WorkloadProfile.BATCH, executor, Long.MAX_VALUE);
            assertFalse(request.isDone());
            callerBytes[0] = 2;
            callerParts[0] = key(2).keys()[0];
            callerKeys.clear();
            var published = request.thenApply(page -> {
                assertEquals(pending, db.getPendingOpsCount(), "completion must follow native ownership release");
                return page;
            });
            executor.runNext();
            var page = published.join();
            assertEquals(java.util.Arrays.asList(expected, null), page.values());
            api.put(0, column, key(1), Buf.wrap(new byte[]{9}), RequestType.none());
            api.deleteColumn(column);
            assertEquals(expected, page.values().getFirst());
        }
    }

    @Test
    void queuedCancellationDeadlineAndRejectionLeaveNoPendingOperations() throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-async-stop", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            api.put(0, column, key(1), Buf.wrap(new byte[]{7}), RequestType.none());
            var db = connection.getInternalDB();
            var pending = db.getPendingOpsCount();
            var executor = new ControlledExecutor();
            var keys = List.of(key(1));
            var budget = new RangeBudget(1, 128);

            var cancelled = db.getMultiAsyncInternal(column, keys, budget,
                    WorkloadProfile.BATCH, executor, Long.MAX_VALUE);
            assertTrue(cancelled.cancel(true));
            assertThrows(CancellationException.class, cancelled::join);
            assertEquals(pending, db.getPendingOpsCount());
            executor.runNext();
            assertTrue(cancelled.isCancelled());
            assertEquals(pending, db.getPendingOpsCount());

            long deadline = db.getScheduler().bindTimeoutNanos(Duration.ofSeconds(1).toNanos());
            var expired = db.getMultiAsyncInternal(column, keys, budget,
                    WorkloadProfile.BATCH, executor, deadline);
            assertFalse(expired.isDone(), "the request must enter the queue before expiry");
            while (!db.getScheduler().isMonotonicDeadlineExpired(deadline)) {
                LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
            }
            executor.runNext();
            var completion = assertThrows(CompletionException.class, expired::join);
            var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
            assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure.getErrorUniqueId());
            assertEquals(pending, db.getPendingOpsCount());

            var rejection = new RejectedExecutionException("test queue full");
            var rejected = db.getMultiAsyncInternal(column, keys, budget, WorkloadProfile.BATCH,
                    task -> { throw rejection; }, Long.MAX_VALUE);
            assertSame(rejection, assertThrows(CompletionException.class, rejected::join).getCause());
            assertEquals(pending, db.getPendingOpsCount());

            // Repeated use after each terminal path still reads from the live column.
            for (int i = 0; i < 2; i++) {
                var next = db.getMultiAsyncInternal(column, keys, budget,
                        WorkloadProfile.BATCH, executor, Long.MAX_VALUE);
                executor.runNext();
                assertEquals(List.of(Buf.wrap(new byte[]{7})), next.join().values());
                assertEquals(pending, db.getPendingOpsCount());
            }
            api.deleteColumn(column);
        }
    }

    @Test
    void cancellationAndDeadlineAfterNativeReadReleaseOwnership() throws Exception {
        try (var connection = new EmbeddedConnection(directory.resolve("db"), "multi-get-late-stop", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var expected = Buf.wrap(new byte[]{7});
            api.put(0, column, key(1), expected, RequestType.none());
            var db = connection.getInternalDB();
            var pending = db.getPendingOpsCount();
            var keys = List.of(key(1));
            var budget = new RangeBudget(1, 128);
            var checks = new AtomicInteger();
            // Discover the final ownership-transfer checkpoint without hard-coding the
            // number of internal cancellation checks. One fitting key needs no retry.
            try (var result = db.getMultiPinned(column, keys, budget, WorkloadProfile.BATCH,
                    Long.MAX_VALUE, () -> { checks.incrementAndGet(); return false; })) {
                assertEquals(List.of(expected), result.page().values());
            }
            int finalCheck = checks.get();
            assertTrue(finalCheck > 1);
            checks.set(0);
            assertThrows(CancellationException.class, () -> db.getMultiPinned(column, keys, budget,
                    WorkloadProfile.BATCH, Long.MAX_VALUE,
                    () -> checks.incrementAndGet() == finalCheck));
            assertEquals(finalCheck, checks.get(), "cancellation must reach the checkpoint after native reading");
            assertEquals(pending, db.getPendingOpsCount());

            checks.set(0);
            long deadline = db.getScheduler().bindTimeoutNanos(Duration.ofSeconds(1).toNanos());
            var failure = assertThrows(RocksDBException.class, () -> db.getMultiPinned(column, keys, budget,
                    WorkloadProfile.BATCH, deadline, () -> {
                        // The next checkpoint checks time before invoking this supplier.
                        // Delay after native reading while its pins are still owned.
                        if (checks.incrementAndGet() == finalCheck - 1) {
                            while (!db.getScheduler().isMonotonicDeadlineExpired(deadline)) {
                                LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
                            }
                        }
                        return false;
                    }));
            assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure.getErrorUniqueId());
            assertEquals(finalCheck - 1, checks.get(), "expiry must be triggered after native reading");
            assertEquals(pending, db.getPendingOpsCount());
            assertEquals(List.of(expected), api.getMulti(column, keys, budget).values());
            api.deleteColumn(column);
            assertEquals(pending, db.getPendingOpsCount());
        }
    }

    private static Keys key(int value) {
        return new Keys(Buf.wrap(new byte[]{(byte) value}));
    }

    private static final class ControlledExecutor implements Executor {
        private final ArrayDeque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable task) {
            tasks.addLast(task);
        }

        void runNext() {
            tasks.removeFirst().run();
        }
    }
}
