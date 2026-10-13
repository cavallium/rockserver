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

    @Test void cdcKeepsCommittedMergeOperandsInsteadOfReadingLaterResolvedValues() throws Exception {
        try(var pool=new Pool(path)) {
            pool.store.enableCdc(GROUP);
            var first=command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("first")));
            var second=command(1,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("later")));
            pool.store.apply(new ApplyPosition(1,0),first); pool.store.apply(new ApplyPosition(1,1),second);
            var page=pool.store.pollCdc(new DistributedCdcCheckpoint.Lineage(GROUP,GROUP,1),DistributedCdcCheckpoint.Cursor.beginning(),2,8*1024*1024);
            assertEquals(2,page.records().size());
            assertArrayEquals(bytes("first"),page.records().getFirst().events().getFirst().mutation().value());
            assertArrayEquals(bytes("later"),page.records().getLast().events().getFirst().mutation().value());
            assertEquals(Mutation.Kind.MERGE,page.records().getFirst().events().getFirst().mutation().kind());
            assertArrayEquals(bytes("first,later"),pool.db.get(pool.handles.get(1),bytes("k")));
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

    @Test void acknowledgementDeletesReceiptsAtomicallyAndCannotReplayMergesOrRegressFloor() throws Exception {
        try (Pool pool = new Pool(path)) {
            var first = command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("first")));
            pool.store.apply(new ApplyPosition(1,0),first);
            try (var old = pool.store.capture()) {
                var second = command(1,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("second")));
                pool.store.apply(new ApplyPosition(1,1),second);
                var ack = ack(2,0,List.of(),List.of());
                var originalAck = pool.store.apply(new ApplyPosition(1,2),ack);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(originalAck).status());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,first));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(old.operationResult(first.operationId(),first.payloadDigest())).status());
                try (var cut = pool.store.capture()) {
                    assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(cut.operationResult(first.operationId(),first.payloadDigest())).status());
                    var receipts = new ArrayList<byte[]>(); cut.visitReceipts((key,value) -> receipts.add(key));
                    assertEquals(2,receipts.size());
                }
                pool.store.apply(new ApplyPosition(1,3),ack(3,1,List.of(),List.of()));
                assertArrayEquals(originalAck,pool.store.apply(new ApplyPosition(1,4),ack));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,second));
                var conflict = ack(4,2,List.of(new Mutation(Mutation.Kind.PUT,A,bytes("k"),bytes("wrong"))),
                        List.of(new KeyPredicate(A,bytes("k"),bytes("not-current"))));
                assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(pool.store.apply(new ApplyPosition(1,5),conflict)).status());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,ack));
                pool.store.apply(new ApplyPosition(1,6),ack(5,0,List.of(),List.of()));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,second),"A new regressive ACK cannot lower the floor");
                var changed = command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("must-not-replay")));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(pool.store.apply(new ApplyPosition(2,7),changed)).status());
                assertArrayEquals(bytes("first,second"),pool.db.get(pool.handles.get(1),bytes("k")));
                assertEquals(new ApplyPosition(2,7),pool.store.applied());
                try (var cut = pool.store.capture()) {
                    var receipts = new ArrayList<byte[]>(); cut.visitReceipts((key,value) -> receipts.add(key));
                    assertEquals(3,receipts.size(),"Expired retry never creates a replacement receipt");
                }
            }
        }
    }

    @Test void acknowledgementSurvivesReopenAndPortableImageInstallWithNoReceiptResurrection() throws Exception {
        Path source = path.resolve("source"), target = path.resolve("target"), image = path.resolve("floor.image");
        var first = command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("first")));
        var limits = new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(2*1024*1024,16*1024*1024,2);
        try (Pool pool = new Pool(source)) {
            pool.store.apply(new ApplyPosition(1,0),first);
            pool.store.apply(new ApplyPosition(1,1),ack(1,0,List.of(),List.of()));
            try (var pin = pool.store.capture()) {
                it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.write(image,pin,pin.interval(),limits);
            }
        }
        try (Pool pool = new Pool(source)) {
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,first));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(pool.store.apply(new ApplyPosition(1,0),first)).status());
            assertEquals(new ApplyPosition(1,1),pool.store.applied());
            assertArrayEquals(bytes("first"),pool.db.get(pool.handles.get(1),bytes("k")));
        }
        try (Pool pool = new Pool(target)) {
            pool.store.fence(); pool.store.installSnapshot(image,limits); pool.store.activate();
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,first));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(pool.store.apply(new ApplyPosition(2,2),first)).status());
            assertArrayEquals(bytes("first"),pool.db.get(pool.handles.get(1),bytes("k")));
            pool.store.apply(new ApplyPosition(2,3),command(2,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("later"))));
            assertArrayEquals(bytes("first,later"),pool.db.get(pool.handles.get(1),bytes("k")));
        }
    }

    @Test void receiptDeletionIsLimitedToExactGroupSessionAndEpochEvenAtMaximumSequence() throws Exception {
        try (Pool pool = new Pool(path,null,bytes("m"))) {
            var owner = new RangeOwnership.Owner(UUID.randomUUID(),1);
            var interval = new RangeOwnership.Interval(GROUP,Set.of(A,B),bytes("m"),null);
            pool.guard.register(owner,interval,RangeOwnership.State.ACTIVE);
            var other = new ReplicatedRangeStore(pool.db,pool.guard,owner,new byte[32],Map.of(A,pool.handles.get(1),B,pool.handles.get(2)),
                    pool.handles.get(3),pool.handles.get(4),Set.of(A));
            try {
                var old = command(0,new Mutation(Mutation.Kind.PUT,A,bytes("a"),bytes("old")));
                var neighborSession = new DistributedCommand(GROUP,1,new byte[32],new OperationId(UUID.randomUUID(),1,0),
                        List.of(new Mutation(Mutation.Kind.PUT,A,bytes("b"),bytes("session"))));
                var neighborEpoch = new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,2,0),
                        List.of(new Mutation(Mutation.Kind.PUT,A,bytes("c"),bytes("epoch"))));
                var neighborGroup = new DistributedCommand(owner.groupId(),1,new byte[32],old.operationId(),
                        List.of(new Mutation(Mutation.Kind.PUT,A,bytes("z"),bytes("group"))));
                pool.store.apply(new ApplyPosition(1,0),old);
                pool.store.apply(new ApplyPosition(1,1),neighborSession);
                pool.store.apply(new ApplyPosition(1,2),neighborEpoch);
                other.apply(new ApplyPosition(1,0),neighborGroup);
                var max = ack(Long.MAX_VALUE,Long.MAX_VALUE-1,List.of(),List.of());
                pool.store.apply(new ApplyPosition(1,3),max);
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,old));
                assertEquals(CommandResult.Status.SUCCESS,status(pool.store,neighborSession));
                assertEquals(CommandResult.Status.SUCCESS,status(pool.store,neighborEpoch));
                assertEquals(CommandResult.Status.SUCCESS,status(other,neighborGroup));
                assertEquals(CommandResult.Status.SUCCESS,status(pool.store,max));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(pool.store.apply(new ApplyPosition(1,4),max)).status());
                var neverSubmitted = command(999,new Mutation(Mutation.Kind.PUT,A,bytes("a"),bytes("new")));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,neverSubmitted));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(pool.store.apply(new ApplyPosition(1,5),neverSubmitted)).status());
                assertArrayEquals(bytes("old"),pool.db.get(pool.handles.get(1),bytes("a")));
                pool.store.apply(new ApplyPosition(1,6),DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],new OperationId(UUID.randomUUID(),4,0),2));
                var cleaned = pool.store.reclaimExpiredSessions(null,4096,8*1024*1024);
                assertNull(cleaned.continuation());
                assertEquals(CommandResult.Status.SUCCESS,status(other,neighborGroup),"Another group's old epoch remains live");
                assertEquals(CommandResult.Status.SUCCESS,status(pool.store,neighborEpoch));
            } finally { assertTrue(other.close(Duration.ofSeconds(10))); }
        }
    }

    @Test void oversizedResultStillAcknowledgesPriorReceiptsWithoutPartialBusinessEffects() throws Exception {
        try (Pool pool = new Pool(path)) {
            var first = command(0,new Mutation(Mutation.Kind.PUT,B,bytes("a"),new byte[1024*1024]));
            var second = command(1,new Mutation(Mutation.Kind.PUT,B,bytes("b"),new byte[1024*1024]));
            pool.store.apply(new ApplyPosition(1,0),first); pool.store.apply(new ApplyPosition(1,1),second);
            var oversized = ack(2,1,List.of(new Mutation(Mutation.Kind.DELETE,B,bytes("a"),null,Mutation.ReturnMode.PREVIOUS),
                    new Mutation(Mutation.Kind.DELETE,B,bytes("b"),null,Mutation.ReturnMode.PREVIOUS)),List.of());
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,CommandResult.decode(pool.store.apply(new ApplyPosition(1,2),oversized)).status());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,first));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,second));
            assertEquals(1024*1024,pool.db.get(pool.handles.get(2),bytes("a")).length);
            assertEquals(1024*1024,pool.db.get(pool.handles.get(2),bytes("b")).length);
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,status(pool.store,oversized));
        }
    }

    @Test void statusRacingAcknowledgementNeverReportsMissingReceiptForCoveredOperation() throws Exception {
        try (Pool pool = new Pool(path); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var first = command(0,new Mutation(Mutation.Kind.PUT,B,bytes("k"),bytes("value")));
            pool.store.apply(new ApplyPosition(1,0),first);
            var start = new CountDownLatch(1);
            var results = new ArrayList<java.util.concurrent.Future<CommandResult.Status>>();
            for (int i=0;i<32;i++) results.add(executor.submit(() -> { start.await(); return status(pool.store,first); }));
            var gc = executor.submit(() -> { start.await(); pool.store.apply(new ApplyPosition(1,1),ack(1,0,List.of(),List.of())); return null; });
            start.countDown();
            for (var result : results) assertTrue(Set.of(CommandResult.Status.SUCCESS,CommandResult.Status.SESSION_EXPIRED).contains(result.get(5,TimeUnit.SECONDS)));
            gc.get(5,TimeUnit.SECONDS);
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,first));
        }
    }

    @Test void coarseFenceCleanupIsBoundedAndDoesNotChangeLogicalImageOrUserData() throws Exception {
        var limits = new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(2*1024*1024,16*1024*1024,2);
        var admin = new UUID(0,1);
        var old = command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("old")));
        var ack = ack(1,0,List.of(),List.of());
        var live = new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,2,0),
                List.of(new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("live"))));
        try (Pool pool = new Pool(path)) {
            pool.store.apply(new ApplyPosition(1,0),old); pool.store.apply(new ApplyPosition(1,1),ack);
            pool.store.apply(new ApplyPosition(1,2),live);
            try (var oldCut = pool.store.capture()) {
                var fence = DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],new OperationId(admin,4,0),2);
                var original = pool.store.apply(new ApplyPosition(1,3),fence);
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,ack));
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(oldCut.operationResult(ack.operationId(),ack.payloadDigest())).status());
                Path before = path.resolve("before-clean.image"), after = path.resolve("after-clean.image");
                try (var pin = pool.store.capture()) { it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.write(before,pin,pin.interval(),limits); }
                assertThrows(IllegalArgumentException.class,() -> pool.store.reclaimExpiredSessions(null,1,91));
                var page = pool.store.reclaimExpiredSessions(null,1,92);
                assertEquals(1,page.examined()); assertEquals(0,page.deleted()); assertEquals(92,page.scannedBytes());
                assertNotNull(page.continuation());
                int scanned = page.examined(), deleted = page.deleted(), pages = 1;
                while (page.continuation()!=null) {
                    page = pool.store.reclaimExpiredSessions(page.continuation(),1,1024);
                    assertTrue(page.examined()<=1); scanned+=page.examined(); deleted+=page.deleted();
                    assertTrue(++pages<10);
                }
                assertEquals(2,scanned); assertEquals(0,deleted);
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(oldCut.operationResult(ack.operationId(),ack.payloadDigest())).status(),
                        "Pre-fence native pin retains the physically collected receipt");
                assertEquals(new ApplyPosition(1,3),pool.store.applied());
                try (var pin = pool.store.capture()) { it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.write(after,pin,pin.interval(),limits); }
                assertArrayEquals(Files.readAllBytes(before),Files.readAllBytes(after));
                try (Pool target = new Pool(path.resolve("epoch-target"))) {
                    target.store.fence(); target.store.installSnapshot(after,limits); target.store.activate();
                    assertEquals(CommandResult.Status.SESSION_EXPIRED,status(target.store,ack));
                    assertEquals(CommandResult.Status.SUCCESS,status(target.store,live));
                    assertArrayEquals(bytes("old,live"),target.db.get(target.handles.get(1),bytes("k")));
                }
                assertEquals(CommandResult.Status.SUCCESS,status(pool.store,live));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,ack));
                assertArrayEquals(bytes("old,live"),pool.db.get(pool.handles.get(1),bytes("k")));
                assertArrayEquals(original,pool.store.apply(new ApplyPosition(1,4),fence));
                var regressive = DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],new OperationId(admin,4,1),1);
                pool.store.apply(new ApplyPosition(1,5),regressive);
                var oldEpochNewSequence = command(99,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("never")));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(pool.store.apply(new ApplyPosition(1,6),oldEpochNewSequence)).status());
                assertArrayEquals(bytes("old,live"),pool.db.get(pool.handles.get(1),bytes("k")));
            }
        }
        try (Pool reopened = new Pool(path)) {
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(reopened.store,ack));
            assertEquals(CommandResult.Status.SUCCESS,status(reopened.store,live));
            var page = reopened.store.reclaimExpiredSessions(null,4096,8*1024*1024);
            assertNull(page.continuation()); assertEquals(0,page.deleted());
        }
    }

    @Test void cleanupKeepsFixedCutoffAcrossNewFenceAndRejectsCursorAfterImageInstall() throws Exception {
        var limits = new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.Limits(2*1024*1024,16*1024*1024,2);
        var admin = new UUID(0,1); Path oldImage = path.resolve("old-cut.image");
        try (Pool pool = new Pool(path)) {
            var old = command(0,new Mutation(Mutation.Kind.PUT,B,bytes("k"),bytes("value")));
            pool.store.apply(new ApplyPosition(1,0),old);
            pool.store.apply(new ApplyPosition(1,1),ack(1,0,List.of(),List.of()));
            var epoch2 = new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,2,0),
                    List.of(new Mutation(Mutation.Kind.PUT,B,bytes("k"),bytes("two"))));
            pool.store.apply(new ApplyPosition(1,2),epoch2);
            try (var pin = pool.store.capture()) { it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage.write(oldImage,pin,pin.interval(),limits); }
            pool.store.apply(new ApplyPosition(1,3),DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],new OperationId(admin,4,0),2));
            var page = pool.store.reclaimExpiredSessions(null,1,1024); var stale = page.continuation();
            pool.store.apply(new ApplyPosition(1,4),DistributedCommand.advanceSessionEpochFloor(GROUP,1,new byte[32],new OperationId(admin,4,1),3));
            while (page.continuation()!=null) page = pool.store.reclaimExpiredSessions(page.continuation(),1,1024);
            assertEquals(2,page.minimumEpoch());
            var later = pool.store.reclaimExpiredSessions(null,4096,8*1024*1024);
            assertEquals(3,later.minimumEpoch()); assertEquals(0,later.deleted());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,status(pool.store,epoch2),"Committed epoch floor already physically reclaimed the receipt");
            pool.store.fence(); pool.store.installSnapshot(oldImage,limits); pool.store.activate();
            assertThrows(IllegalArgumentException.class,() -> pool.store.reclaimExpiredSessions(stale,1,1024));
            assertEquals(CommandResult.Status.SUCCESS,status(pool.store,epoch2));
        }
    }

    private static DistributedCommand ack(long sequence,long through,List<Mutation> mutations,List<KeyPredicate> predicates) {
        return new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,1,sequence),mutations,predicates,through);
    }
    private static CommandResult.Status status(ReplicatedRangeStore store,DistributedCommand command) throws Exception {
        return CommandResult.decode(store.operationResult(command.operationId(),command.payloadDigest())).status();
    }

    private static List<String> physicalKeys(PhysicalRangePage page) {
        return page.rows().stream().map(row -> new String(row.key(), StandardCharsets.UTF_8)).toList();
    }

    private static DistributedCommand command(long sequence, Mutation... mutations) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }

    @Test void acceptedMergePlanPreservesOperandsAndReplaysAfterReopenWithoutHistory() throws Exception {
        DistributedCommand accepted; byte[] outcome;
        try (var pool=new Pool(path)) {
            pool.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); pool.store.applyNoop(new ApplyPosition(1,0));
            pool.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("seed"))));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(A,bytes("k"))));
            try (var pin=pool.store.captureTracked(scope)) {
                var base=command(1,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("one"),Mutation.ReturnMode.PREVIOUS),
                        new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("two"),Mutation.ReturnMode.CURRENT));
                accepted=pool.store.planScopedCommit(pin,base,0,new UUID(0,9));
                assertEquals(Mutation.Kind.MERGE,accepted.mutations().getFirst().kind()); assertArrayEquals(bytes("one"),accepted.mutations().getFirst().value());
                var result=CommandResult.decode(accepted.acceptance().result()); assertArrayEquals(bytes("seed"),result.value(0)); assertArrayEquals(bytes("seed,one,two"),result.value(1));
            }
            outcome=pool.store.apply(new ApplyPosition(1,2),accepted); assertArrayEquals(accepted.acceptance().result(),outcome);
        }
        try (var reopened=new Pool(path)) {
            assertArrayEquals(outcome,reopened.store.apply(new ApplyPosition(1,2),accepted));
            reopened.store.apply(new ApplyPosition(2,3),command(2,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("later"))));
            assertArrayEquals(outcome,reopened.store.apply(new ApplyPosition(2,4),accepted)); assertArrayEquals(bytes("seed,one,two,later"),reopened.db.get(reopened.handles.get(1),bytes("k")));
        }
        try (var follower=new Pool(path.resolve("follower"))) {
            follower.store.applyNoop(new ApplyPosition(1,0)); follower.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.MERGE,A,bytes("k"),bytes("seed"))));
            assertArrayEquals(outcome,follower.store.apply(new ApplyPosition(1,2),DistributedCommandCodec.decode(DistributedCommandCodec.encode(accepted))));
            assertArrayEquals(bytes("seed,one,two"),follower.db.get(follower.handles.get(1),bytes("k")));
        }
    }

    @Test void catalogueCompactionCoversReadOnlyColumnsAndKeepsDataScopesBounded() throws Exception {
        var mutations=new ArrayList<Mutation>();
        for (int i=0;i<TransactionScope.MAX_SPANS;i++) mutations.add(new Mutation(Mutation.Kind.PUT,A,bytes("key"+i),bytes("v")));
        var bounded=new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,1,0),mutations);
        var large=new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,1,1),mutations,
                List.of(new KeyPredicate(B,bytes("read-only"),null)));
        try (var pool=new Pool(path)) {
            var exact=pool.store.commandAccessScope(bounded);
            assertEquals(TransactionScope.MAX_SPANS,exact.spans().size()); assertTrue(exact.spans().stream().allMatch(TransactionScope.Span::isPoint));
            assertFalse(exact.contains(A,bytes("other"))); assertFalse(exact.contains(B,bytes("read-only")));
            assertThrows(IllegalArgumentException.class,() -> pool.store.commandAccessScope(large));
            pool.store.enableCatalogueScopeCompaction(); pool.store.enableCatalogueScopeCompaction();
            assertTrue(pool.store.commandAccessScope(bounded).spans().stream().allMatch(TransactionScope.Span::isPoint));
            var compact=pool.store.commandAccessScope(large); assertEquals(2,compact.spans().size());
            assertTrue(compact.containsRange(A,null,null)); assertTrue(compact.containsRange(B,null,null));
            assertFalse(compact.intersects(new TransactionScope(List.of(TransactionScope.Span.point(UUID.randomUUID(),bytes("key"))))));
            assertThrows(IllegalStateException.class,() -> pool.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()));
        }
        try (var pool=new Pool(path.resolve("data"))) {
            pool.store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            assertThrows(IllegalStateException.class,pool.store::enableCatalogueScopeCompaction);
            assertThrows(IllegalArgumentException.class,() -> pool.store.commandAccessScope(large));
        }
        assertEquals(256,TransactionScope.MAX_SPANS);
    }

    @Test void schemaAuthorityReadHasSeparateAggregateBudgetAtOneCutWithoutSnapshotRetention() throws Exception {
        try(var pool=new Pool(path)) {
            pool.store.applyNoop(new ApplyPosition(1,0));
            byte[][] values={new byte[1_100_000],new byte[1_100_000],new byte[2_000_000],new byte[2_000_000]};
            var points=new ArrayList<ReplicatedRangeStore.PhysicalPoint>();
            for(int i=0;i<values.length;i++) {
                java.util.Arrays.fill(values[i],(byte)(i+1)); byte[] key=bytes("authority-"+i);
                pool.store.apply(new ApplyPosition(1,i+1),command(i,new Mutation(Mutation.Kind.PUT,B,key,values[i])));
                points.add(new ReplicatedRangeStore.PhysicalPoint(B,key));
            }
            var usage=pool.store.receiptUsage(); int pins=pool.store.activeSnapshotPins();
            assertThrows(IllegalStateException.class,() -> pool.store.readCurrentPoints(points.subList(0,2)));
            assertArrayEquals(values[0],pool.store.readCurrentPoints(points.subList(0,1)).values().getFirst());
            var authority=List.of(points.get(0),points.get(1),new ReplicatedRangeStore.PhysicalPoint(B,bytes("missing-definition")),new ReplicatedRangeStore.PhysicalPoint(B,bytes("missing-manifest")));
            var read=pool.store.readSchemaAuthorityPoints(authority);
            assertEquals(new ApplyPosition(1,4),read.position()); assertArrayEquals(values[0],read.values().getFirst()); assertArrayEquals(values[1],read.values().get(1));
            assertNull(read.values().get(2)); assertNull(read.values().get(3));
            assertThrows(IllegalStateException.class,() -> pool.store.readSchemaAuthorityPoints(points));
            assertThrows(IllegalArgumentException.class,() -> pool.store.readSchemaAuthorityPoints(java.util.Collections.nCopies(5,points.getFirst())));
            assertEquals(pins,pool.store.activeSnapshotPins()); assertEquals(usage,pool.store.receiptUsage());
            assertEquals(new ApplyPosition(1,4),pool.store.applied());
        }
    }

    private static final class Pool implements AutoCloseable {
        final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain = new ColumnFamilyOptions();
        final ColumnFamilyOptions merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles = new ArrayList<>();
        final RocksDB db;
        final ReplicatedRangeStore store;
        final RangeOwnership guard = new RangeOwnership();
        Pool(Path path) throws Exception { this(path, null, null); }
        Pool(Path path, byte[] lower, byte[] upper) throws Exception {
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(bytes("a"), merged), new ColumnFamilyDescriptor(bytes("b"), plain),
                    new ColumnFamilyDescriptor(bytes("state"), plain), new ColumnFamilyDescriptor(bytes("receipts"), plain));
            db = RocksDB.open(options, path.toString(), descriptors, handles);
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
