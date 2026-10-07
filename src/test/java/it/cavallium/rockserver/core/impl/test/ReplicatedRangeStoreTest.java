package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.nio.file.Files;
import java.time.Duration;
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

    @Test void conditionalMutationsAreAtomicAndConflictReceiptCannotBecomeASecondAttempt() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("seed")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("paired"))));
            var conditional = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 1),
                    List.of(new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("first"), Mutation.ReturnMode.CURRENT),
                            new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("next"), Mutation.ReturnMode.PREVIOUS)),
                    List.of(new KeyPredicate(A, bytes("k"), bytes("seed")), new KeyPredicate(B, bytes("k"), bytes("paired"))));
            assertThrows(IllegalArgumentException.class,
                    () -> pool.store.apply(new ApplyPosition(1, 2), conditional, new byte[0]));
            assertEquals(new ApplyPosition(1, 1), pool.store.applied());
            var result = CommandResult.decode(pool.store.apply(new ApplyPosition(1, 2), conditional));
            assertEquals(CommandResult.Status.SUCCESS, result.status());
            assertArrayEquals(bytes("seed,first"), result.value(0));
            assertArrayEquals(bytes("paired"), result.value(1));
            var rejected = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2),
                    List.of(new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("must-not-apply")),
                            new Mutation(Mutation.Kind.MERGE, A, bytes("k"), bytes("must-not-merge"))),
                    List.of(new KeyPredicate(A, bytes("k"), bytes("seed"))));
            byte[] conflict = pool.store.apply(new ApplyPosition(1, 3), rejected);
            assertEquals(CommandResult.Status.CONFLICT, CommandResult.decode(conflict).status());
            assertArrayEquals(bytes("next"), pool.db.get(pool.handles.get(2), bytes("k")));
            assertArrayEquals(bytes("seed,first"), pool.db.get(pool.handles.get(1), bytes("k")));
            pool.store.apply(new ApplyPosition(1, 4), command(3, new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("seed"))));
            assertArrayEquals(conflict, pool.store.apply(new ApplyPosition(1, 5), rejected));
            assertArrayEquals(bytes("next"), pool.db.get(pool.handles.get(2), bytes("k")));
            assertArrayEquals(bytes("seed"), pool.db.get(pool.handles.get(1), bytes("k")));
        }
    }

    @Test void predicatesDistinguishAnAbsentKeyFromAPresentEmptyValue() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0, new Mutation(Mutation.Kind.PUT, B, bytes("empty"), new byte[0])));
            var mutation = List.of(new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("value")));
            var absent = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 1), mutation,
                    List.of(new KeyPredicate(B, bytes("empty"), null)));
            assertEquals(CommandResult.Status.CONFLICT, CommandResult.decode(pool.store.apply(new ApplyPosition(1, 2), absent)).status());
            assertNull(pool.db.get(pool.handles.get(1), bytes("k")));
            var empty = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 2), mutation,
                    List.of(new KeyPredicate(B, bytes("empty"), new byte[0])));
            assertEquals(CommandResult.Status.SUCCESS, CommandResult.decode(pool.store.apply(new ApplyPosition(1, 3), empty)).status());
            assertArrayEquals(bytes("value"), pool.db.get(pool.handles.get(1), bytes("k")));
        }
    }

    @Test void scopedSnapshotRetainsOneCrossColumnCutAndCanCloseAndReopen() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("old-a")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("old-b"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.point(A, bytes("k")), TransactionScope.Span.point(B, bytes("k"))));
            var snapshot = pool.store.capture(scope);
            try {
                assertEquals(1, pool.store.activeSnapshotPins());
                pool.store.apply(new ApplyPosition(1, 2), command(1,
                        new Mutation(Mutation.Kind.PUT, A, bytes("k"), bytes("new-a")),
                        new Mutation(Mutation.Kind.PUT, B, bytes("k"), bytes("new-b"))));
                assertEquals(new ApplyPosition(1, 1), snapshot.position());
                assertArrayEquals(bytes("old-a"), snapshot.get(A, bytes("k")));
                assertArrayEquals(bytes("old-b"), snapshot.get(B, bytes("k")));
                assertThrows(IllegalArgumentException.class, () -> snapshot.get(A, bytes("outside")));
                assertThrows(IllegalArgumentException.class, () -> snapshot.visitColumn(A, bytes("k"), bytes("l"), (key, value) -> fail("Point scope grants no scan")));
            } finally { snapshot.close(); }
            snapshot.close();
            assertEquals(0, pool.store.activeSnapshotPins());
            assertThrows(IllegalStateException.class, () -> snapshot.get(A, bytes("k")));
            try (var reopened = pool.store.capture(scope)) {
                assertEquals(new ApplyPosition(1, 2), reopened.position());
                assertArrayEquals(bytes("new-a"), reopened.get(A, bytes("k")));
                assertArrayEquals(bytes("new-b"), reopened.get(B, bytes("k")));
            }
        }
    }

    @Test void scopedScansRequireCompleteCoverageAndReadTheirOriginalCutAfterFence() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, B, bytes("a"), bytes("first")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("b"), bytes("second")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("d"), bytes("outside"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(B, bytes("a"), bytes("b")),
                    TransactionScope.Span.range(B, bytes("b"), bytes("c"))));
            try (var snapshot = pool.store.capture(scope)) {
                pool.store.fence();
                var seen = new ArrayList<String>();
                snapshot.visitColumn(B, bytes("a"), bytes("c"), (key, value) -> seen.add(new String(key, StandardCharsets.UTF_8)));
                assertEquals(List.of("a", "b"), seen);
                assertThrows(IllegalArgumentException.class, () -> snapshot.visitColumn(B, null, null, (key, value) -> fail("No full-range scan granted")));
                assertThrows(IllegalArgumentException.class, () -> snapshot.get(A, bytes("a")));
                assertThrows(IllegalStateException.class, () -> pool.store.capture(scope));
                assertEquals(1, pool.store.activeSnapshotPins());
            }
            assertEquals(0, pool.store.activeSnapshotPins());
        }
    }

    @Test void scopeOutsideNativeOwnerIsRejectedBeforePinAllocation() throws Exception {
        try (Pool pool = new Pool(path, bytes("a"), bytes("m"))) {
            var inside = new TransactionScope(List.of(TransactionScope.Span.range(A, bytes("a"), bytes("m"))));
            try (var snapshot = pool.store.capture(inside)) { assertEquals(1, pool.store.activeSnapshotPins()); }
            var outside = List.of(TransactionScope.Span.point(A, bytes("m")), TransactionScope.Span.range(A, null, bytes("m")),
                    TransactionScope.Span.range(A, bytes("a"), null), TransactionScope.Span.point(UUID.randomUUID(), bytes("b")));
            for (var span : outside) {
                assertThrows(IllegalArgumentException.class, () -> pool.store.capture(new TransactionScope(List.of(span))));
                assertEquals(0, pool.store.activeSnapshotPins());
            }
        }
    }

    @Test void scopedPagesResumeExclusivelyInBothDirectionsAtTheirOriginalCut() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, B, bytes("a"), bytes("old-a")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("b"), bytes("old-b")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("c"), bytes("old-c")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("d"), bytes("outside"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(B, bytes("a"), bytes("d"))));
            try (var snapshot = pool.store.capture(scope)) {
                var first = snapshot.page(B, bytes("a"), bytes("d"), false, null, 2, 1024);
                assertEquals(List.of("a", "b"), physicalKeys(first));
                assertTrue(first.hasMore());
                byte[] resume = first.resumeAfter(); resume[0] = 'z';
                assertArrayEquals(bytes("b"), first.resumeAfter());
                first.rows().getLast().key()[0] = 'z';
                first.rows().getLast().value()[0] = 'X';
                assertArrayEquals(bytes("b"), first.resumeAfter());
                assertArrayEquals(bytes("old-b"), first.rows().getLast().value());
                assertThrows(UnsupportedOperationException.class, () -> first.rows().clear());
                pool.store.apply(new ApplyPosition(1, 2), command(1,
                        new Mutation(Mutation.Kind.DELETE, B, bytes("b"), null),
                        new Mutation(Mutation.Kind.PUT, B, bytes("c"), bytes("new-c"))));
                var last = snapshot.page(B, bytes("a"), bytes("d"), false, first.resumeAfter(), 2, 1024);
                assertEquals(List.of("c"), physicalKeys(last));
                assertArrayEquals(bytes("old-c"), last.rows().getFirst().value());
                assertFalse(last.hasMore());
                var reverse = snapshot.page(B, bytes("a"), bytes("d"), true, null, 2, 1024);
                assertEquals(List.of("c", "b"), physicalKeys(reverse));
                assertTrue(reverse.hasMore());
                var reverseLast = snapshot.page(B, bytes("a"), bytes("d"), true, reverse.resumeAfter(), 2, 1024);
                assertEquals(List.of("a"), physicalKeys(reverseLast));
                assertFalse(reverseLast.hasMore());
                assertEquals(List.of("c"), physicalKeys(snapshot.page(B, bytes("a"), bytes("d"), false, bytes("bb"), 2, 1024)));
                assertThrows(IllegalArgumentException.class, () -> snapshot.page(B, bytes("a"), bytes("d"), false, bytes("d"), 2, 1024));
                assertThrows(IllegalArgumentException.class, () -> snapshot.page(B, null, null, false, null, 2, 1024));
            }
            assertEquals(0, pool.store.activeSnapshotPins());
        }
    }

    @Test void pageByteLimitsIncludeContinuationAndOversizedFirstRowIsExplicit() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, B, new byte[0], new byte[0]),
                    new Mutation(Mutation.Kind.PUT, B, bytes("a"), bytes("value")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("b"), bytes("value"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(B, null, null)));
            try (var snapshot = pool.store.capture(scope)) {
                var emptyKey = snapshot.page(B, null, null, false, null, 10, 17);
                assertEquals(1, emptyKey.rows().size());
                assertEquals(17, emptyKey.payloadBytes());
                assertArrayEquals(new byte[0], emptyKey.resumeAfter());
                assertTrue(emptyKey.hasMore());
                var one = snapshot.page(B, null, null, false, emptyKey.resumeAfter(), 10, 24);
                assertEquals(List.of("a"), physicalKeys(one));
                assertEquals(24, one.payloadBytes());
                assertTrue(one.hasMore());
                var tooSmall = assertThrows(PhysicalRangePage.LimitExceededException.class,
                        () -> snapshot.page(B, null, null, false, emptyKey.resumeAfter(), 10, 23));
                assertEquals(24, tooSmall.minimumBytes());
                var last = snapshot.page(B, null, null, false, one.resumeAfter(), 10, 24);
                assertEquals(List.of("b"), physicalKeys(last));
                assertFalse(last.hasMore());
                var exhausted = snapshot.page(B, null, null, false, last.resumeAfter(), 10, 24);
                assertTrue(exhausted.rows().isEmpty()); assertNull(exhausted.resumeAfter()); assertFalse(exhausted.hasMore());
                try (var cursor = snapshot.openCursor(B, null, null, false)) {
                    assertArrayEquals(new byte[0], cursor.nextPage(10, 17).resumeAfter());
                    assertThrows(PhysicalRangePage.LimitExceededException.class, () -> cursor.nextPage(10, 23));
                    assertEquals(List.of("a"), physicalKeys(cursor.nextPage(10, 24)), "Budget rejection must not advance continuation");
                    assertEquals(List.of("b"), physicalKeys(cursor.nextPage(10, 24)));
                }
            }
        }
    }

    @Test void pageCursorBindsBoundsDirectionAndCutAndStopsWithItsParent() throws Exception {
        try (Pool pool = new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1, 0));
            pool.store.apply(new ApplyPosition(1, 1), command(0,
                    new Mutation(Mutation.Kind.PUT, B, bytes("a"), bytes("old-a")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("b"), bytes("old-b")),
                    new Mutation(Mutation.Kind.PUT, B, bytes("c"), bytes("old-c"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(B, bytes("a"), bytes("d"))));
            var snapshot = pool.store.capture(scope);
            try {
                byte[] lower = bytes("a"), upper = bytes("d");
                try (var forward = snapshot.openCursor(B, lower, upper, false);
                        var reverse = snapshot.openCursor(B, lower, upper, true)) {
                    lower[0] = 'z'; upper[0] = 'a';
                    assertEquals(List.of("a"), physicalKeys(forward.nextPage(1, 1024)));
                    assertEquals(List.of("c"), physicalKeys(reverse.nextPage(1, 1024)));
                    pool.store.apply(new ApplyPosition(1, 2), command(1,
                            new Mutation(Mutation.Kind.DELETE, B, bytes("b"), null),
                            new Mutation(Mutation.Kind.PUT, B, bytes("c"), bytes("new-c"))));
                    assertEquals(List.of("b"), physicalKeys(forward.nextPage(1, 1024)));
                    assertEquals(List.of("b"), physicalKeys(reverse.nextPage(1, 1024)));
                    var last = forward.nextPage(1, 1024);
                    assertEquals(List.of("c"), physicalKeys(last));
                    assertArrayEquals(bytes("old-c"), last.rows().getFirst().value());
                    assertFalse(last.hasMore());
                    assertTrue(forward.nextPage(1, 1024).rows().isEmpty());
                    assertThrows(IllegalArgumentException.class, () -> forward.nextPage(0, 1024));
                    assertEquals(1, pool.store.activeSnapshotPins(), "Both cursors borrow one native cut");
                    snapshot.close();
                    assertThrows(IllegalStateException.class, () -> forward.nextPage(1, 1024));
                    assertThrows(IllegalStateException.class, () -> reverse.nextPage(1, 1024));
                    assertThrows(IllegalStateException.class, () -> snapshot.openCursor(B, bytes("a"), bytes("d"), false));
                }
            } finally { snapshot.close(); }
            assertEquals(0, pool.store.activeSnapshotPins());
            try (var reopened = pool.store.capture(scope); var cursor = reopened.openCursor(B, bytes("a"), bytes("d"), false)) {
                assertEquals(List.of("a", "c"), physicalKeys(cursor.nextPage(3, 1024)));
                cursor.close();
                assertThrows(IllegalStateException.class, () -> cursor.nextPage(1, 1024));
                assertEquals(1, pool.store.activeSnapshotPins(), "Cursor close leaves its parent pin owned");
                assertArrayEquals(bytes("new-c"), reopened.get(B, bytes("c")));
            }
        }
    }

    @Test void nativeSstExportLoadsMaterializedSnapshotValuesWithColumnMergeOptions() throws Exception {
        try (Pool source = new Pool(path.resolve("source")); Pool target = new Pool(path.resolve("target"))) {
            source.store.applyNoop(new ApplyPosition(1,0));
            source.store.apply(new ApplyPosition(1,1), command(0,
                    new Mutation(Mutation.Kind.PUT,A,bytes("a"),bytes("old-a")),
                    new Mutation(Mutation.Kind.MERGE,A,bytes("b"),bytes("one")),
                    new Mutation(Mutation.Kind.MERGE,A,bytes("b"),bytes("two")),
                    new Mutation(Mutation.Kind.PUT,A,bytes("d"),bytes("outside"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(A,bytes("a"),bytes("d"))));
            try (var snapshot = source.store.capture(scope)) {
                source.store.apply(new ApplyPosition(1,2), command(1,
                        new Mutation(Mutation.Kind.PUT,A,bytes("a"),bytes("new-a")),
                        new Mutation(Mutation.Kind.MERGE,A,bytes("b"),bytes("later"))));
                var limits = new NativeRangeSstExport.Limits(2,1024,1024*1024,Duration.ofSeconds(10));
                var manifest = NativeRangeSstExport.write(path.resolve("column-a.sst"),snapshot,A,bytes("a"),bytes("d"),source.options,source.merged,limits);
                assertEquals(2,manifest.rows());
                assertEquals(new ApplyPosition(1,1),manifest.position());
                assertEquals(Files.size(manifest.file()),manifest.fileBytes());
                assertEquals(32,manifest.sha256().length);
                try (var ingest = new IngestExternalFileOptions().setMoveFiles(false)) {
                    target.db.ingestExternalFile(target.handles.get(1),List.of(manifest.file().toString()),ingest);
                }
                assertArrayEquals(bytes("old-a"),target.db.get(target.handles.get(1),bytes("a")));
                assertArrayEquals(bytes("one,two"),target.db.get(target.handles.get(1),bytes("b")));
                assertNull(target.db.get(target.handles.get(1),bytes("d")));
                target.db.merge(target.handles.get(1),bytes("b"),bytes("three"));
                assertArrayEquals(bytes("one,two,three"),target.db.get(target.handles.get(1),bytes("b")));
                assertThrows(java.nio.file.FileAlreadyExistsException.class, () -> NativeRangeSstExport.write(manifest.file(),snapshot,A,bytes("a"),bytes("d"),source.options,source.merged,limits));
            }
            assertEquals(0,source.store.activeSnapshotPins());
        }
    }

    @Test void nativeSstExportLeavesNoPublishedFileForEmptyOrOverBudgetJobs() throws Exception {
        try (Pool source = new Pool(path.resolve("source"))) {
            source.store.applyNoop(new ApplyPosition(1,0));
            source.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,B,bytes("a"),bytes("value"))));
            var scope = new TransactionScope(List.of(TransactionScope.Span.range(B,null,null)));
            try (var snapshot = source.store.capture(scope)) {
                var limits = new NativeRangeSstExport.Limits(1,1024,1024*1024,Duration.ofSeconds(10));
                var emptyPath = path.resolve("empty.sst");
                var empty = NativeRangeSstExport.write(emptyPath,snapshot,B,bytes("z"),null,source.options,source.plain,limits);
                assertEquals(0,empty.rows()); assertNull(empty.file()); assertFalse(Files.exists(emptyPath));
                var limitedPath = path.resolve("limited.sst");
                assertThrows(java.io.IOException.class, () -> NativeRangeSstExport.write(limitedPath,snapshot,B,null,null,source.options,source.plain,
                        new NativeRangeSstExport.Limits(1,1,1024*1024,Duration.ofSeconds(10))));
                assertFalse(Files.exists(limitedPath));
                try (var files = Files.list(path)) { assertTrue(files.noneMatch(file -> file.getFileName().toString().startsWith(".range-sst-building-"))); }
                assertEquals(1,source.store.activeSnapshotPins(),"Exporter retains but does not close the caller's snapshot");
            }
            assertEquals(0,source.store.activeSnapshotPins());
        }
    }

    private static List<String> physicalKeys(PhysicalRangePage page) {
        return page.rows().stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList();
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
        Pool(Path path) throws Exception { this(path, null, null); }
        Pool(Path path, byte[] lower, byte[] upper) throws Exception {
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(bytes("a"), merged), new ColumnFamilyDescriptor(bytes("b"), plain),
                    new ColumnFamilyDescriptor(bytes("state"), plain), new ColumnFamilyDescriptor(bytes("receipts"), plain));
            db = RocksDB.open(options, path.toString(), descriptors, handles);
            var guard = new RangeOwnership();
            var owner = new RangeOwnership.Owner(GROUP, 1);
            guard.register(owner, new RangeOwnership.Interval(GROUP, Set.of(A, B), lower, upper), RangeOwnership.State.ACTIVE);
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
