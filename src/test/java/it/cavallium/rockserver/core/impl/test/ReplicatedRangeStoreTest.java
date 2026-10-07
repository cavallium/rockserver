package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class ReplicatedRangeStoreTest {
    static { RocksDB.loadLibrary(); }
    private static final UUID GROUP = UUID.fromString("a8800000-0000-0000-0000-000000000001");
    private static final UUID A = UUID.fromString("a8800000-0000-0000-0000-000000000002");
    private static final UUID B = UUID.fromString("a8800000-0000-0000-0000-000000000003");
    private static final UUID SESSION = UUID.fromString("a8800000-0000-0000-0000-000000000004");
    @TempDir(factory = DiskTestDirectory.class) Path path;

    @Test void duplicateReturnsOriginalResultWithoutRepeatingMergeAfterReopen() throws Exception {
        var command = command(0, new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("first")),
                new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("paired")));
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            assertArrayEquals(bytes("original"), pool.store.apply(new ApplyPosition(1, 1), command, bytes("original")));
            pool.store.apply(new ApplyPosition(1, 2), command(1,
                    new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("later"))), bytes("later-result"));
            pool.db.flushWal(true);
        }
        try (Pool pool = new Pool(path)) {
            assertEquals(new ApplyPosition(1, 2), pool.store.applied());
            assertArrayEquals(bytes("original"), pool.store.apply(new ApplyPosition(2, 3), command, bytes("must-not-replace")));
            assertArrayEquals(bytes("first,later"), pool.db.get(pool.handles.get(1), bytes("k")));
            assertArrayEquals(bytes("paired"), pool.db.get(pool.handles.get(2), bytes("k")));
            assertEquals(new ApplyPosition(2, 3), pool.store.applied());
            assertArrayEquals(bytes("original"), pool.store.apply(new ApplyPosition(1, 1), command, bytes("ignored")));
            assertEquals(new ApplyPosition(2, 3), pool.store.applied());
        }
    }

    @Test void snapshotPinsDataAndReceiptsAtSameCompletedMarker() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            var first = command(0, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("old-a")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("old-b")));
            pool.store.apply(new ApplyPosition(1, 1), first, bytes("old-result"));
            try (var pin = pool.store.capture()) {
                var later = command(1, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("new-a")),
                        new Mutation(Mutation.Kind.DELETE, B, bytes("k"), null));
                pool.store.apply(new ApplyPosition(1, 2), later, bytes("new-result"));
                assertEquals(new ApplyPosition(1, 1), pin.position());
                assertArrayEquals(bytes("old-a"), pin.get(A, bytes("k")));
                assertArrayEquals(bytes("old-b"), pin.get(B, bytes("k")));
                assertArrayEquals(bytes("old-result"), pin.operationResult(first.operationId(), first.payloadDigest()));
                assertNull(pin.operationResult(later.operationId(), later.payloadDigest()));
            }
        }
    }

    @Test void invalidBatchOrPayloadReusePublishesNoEffectsOrMarker() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            var first = command(0, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("first")));
            pool.store.apply(new ApplyPosition(1, 1), first, bytes("first-result"));
            var changed = command(0, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("different")));
            assertThrows(IllegalArgumentException.class, () -> pool.store.apply(new ApplyPosition(1, 2), changed, bytes("bad")));
            var invalid = command(1, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("partial")),
                    new Mutation(Mutation.Kind.PUT, UUID.randomUUID(), bytes("k"), bytes("bad")));
            assertThrows(IllegalArgumentException.class, () -> pool.store.apply(new ApplyPosition(1, 2), invalid, bytes("bad")));
            assertThrows(IllegalStateException.class, () -> pool.store.applyNoop(new ApplyPosition(1, 3)));
            assertEquals(new ApplyPosition(1, 1), pool.store.applied());
            assertArrayEquals(bytes("first"), pool.db.get(pool.handles.get(1), bytes("k")));
            assertNull(pool.store.operationResult(invalid.operationId(), invalid.payloadDigest()));
            var unsupportedMerge = command(2, new Mutation(Mutation.Kind.MERGE, B, bytes("k"), bytes("operand")));
            assertThrows(IllegalArgumentException.class, () -> pool.store.apply(new ApplyPosition(1, 2), unsupportedMerge));
            assertEquals(new ApplyPosition(1, 1), pool.store.applied());
        }
    }

    @Test void replicatedApplyNeverHonorsStandaloneNoWalProperty() throws Exception {
        String previous = System.getProperty("rocksdb.migrate");
        System.setProperty("rocksdb.migrate", "true");
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("logged"))), bytes("result"));
            try (var log = pool.db.getUpdatesSince(0)) {
                assertTrue(log.isValid(), "Replicated effects and marker must use native WAL");
            }
        } finally {
            if (previous == null) System.clearProperty("rocksdb.migrate");
            else System.setProperty("rocksdb.migrate", previous);
        }
    }

    @Test void orderedReturnValuesUseSameBatchOverlayAndDurableOriginalOutcome() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("seed"))));
            var update = command(1,
                    new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("first"), Mutation.ReturnMode.PREVIOUS),
                    new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("second"), Mutation.ReturnMode.CURRENT),
                    new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("paired"), Mutation.ReturnMode.PREVIOUS));
            byte[] original = pool.store.apply(new ApplyPosition(1, 2), update);
            var result = CommandResult.decode(original);
            assertEquals(CommandResult.Status.SUCCESS, result.status());
            assertEquals(3, result.valueCount());
            assertArrayEquals(bytes("seed"), result.value(0));
            assertArrayEquals(bytes("seed,first,second"), result.value(1));
            assertNull(result.value(2));
            pool.store.apply(new ApplyPosition(1, 3), command(2,
                    new Mutation(Mutation.Kind.DELETE, A, bytes("k"), null)));
            assertArrayEquals(original, pool.store.apply(new ApplyPosition(1, 4), update));
            assertNull(pool.db.get(pool.handles.get(1), bytes("k")));
        }
    }

    @Test void excessiveResultIsARecordedRejectionWithNoPartialEffects() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            byte[] large = new byte[1100 * 1024];
            pool.db.put(pool.handles.get(1), bytes("k"), large);
            pool.db.put(pool.handles.get(2), bytes("k"), large);
            var command = command(0,
                    new Mutation(Mutation.Kind.DELETE, A, bytes("k"), null, Mutation.ReturnMode.PREVIOUS),
                    new Mutation(Mutation.Kind.DELETE, B, bytes("k"), null, Mutation.ReturnMode.PREVIOUS));
            byte[] rejected = pool.store.apply(new ApplyPosition(1, 1), command);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE, CommandResult.decode(rejected).status());
            assertArrayEquals(large, pool.db.get(pool.handles.get(1), bytes("k")));
            assertArrayEquals(large, pool.db.get(pool.handles.get(2), bytes("k")));
            assertEquals(new ApplyPosition(1, 1), pool.store.applied());
            assertArrayEquals(rejected, pool.store.apply(new ApplyPosition(1, 2), command));
        }
    }

    @Test void closingExportPinDefersNativeReleaseUntilReaderHasReturned() throws Exception {
        try (Pool pool = new Pool(path); var executor = Executors.newSingleThreadExecutor()) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("old"))));
            var pin = pool.store.capture();
            var visiting = new CountDownLatch(1);
            var finish = new CountDownLatch(1);
            var export = executor.submit(() -> {
                pin.visitColumn(A, null, null, (key, value) -> {
                    assertArrayEquals(bytes("old"), value);
                    visiting.countDown();
                    try {
                        if (!finish.await(10, TimeUnit.SECONDS)) throw new java.io.IOException("Reader barrier timed out");
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new java.io.IOException(interrupted);
                    }
                });
                return null;
            });
            try {
                assertTrue(visiting.await(10, TimeUnit.SECONDS));
                pin.close();
                assertEquals(1, pool.store.activeSnapshotPins());
                assertThrows(IllegalStateException.class, () -> pin.get(A, bytes("k")));
                pool.store.apply(new ApplyPosition(1, 2), command(1,
                        new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("new"))));
            } finally {
                finish.countDown();
            }
            export.get(10, TimeUnit.SECONDS);
            pin.close();
            assertEquals(0, pool.store.activeSnapshotPins());
        }
    }

    private static DistributedCommand command(long sequence, Mutation... mutations) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }

    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain = new ColumnFamilyOptions();
        final ColumnFamilyOptions merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB db;
        final ReplicatedRangeStore store;
        Pool(Path path) throws Exception {
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(bytes("a"), merged), new ColumnFamilyDescriptor(bytes("b"), plain),
                    new ColumnFamilyDescriptor(bytes("state"), plain), new ColumnFamilyDescriptor(bytes("receipts"), plain));
            db = RocksDB.open(options, path.toString(), descriptors, handles);
            var guard = new RangeOwnership();
            var owner = new RangeOwnership.Owner(GROUP, 1);
            guard.register(owner, new RangeOwnership.Interval(GROUP, Set.of(A, B), null, null), RangeOwnership.State.ACTIVE);
            store = new ReplicatedRangeStore(db, guard, owner, new byte[32], Map.of(A, handles.get(1), B, handles.get(2)),
                    handles.get(3), handles.get(4), Set.of(A));
        }
        @Override public void close() {
            for (var handle : handles) handle.close();
            db.close();
            merged.close();
            plain.close();
            options.close();
        }
    }
}
