package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native-cut/history/resource ownership; validation is not distributed commit acceptance. */
class NativeScopedHistoryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    TransactionScope point(UUID column,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)key}))); }
    DistributedCommand raw(NativeBucketApplyTest.Fixture f,long sequence,int key,byte[] value) {
        return new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,sequence),List.of(new Mutation(value==null ? Mutation.Kind.DELETE : Mutation.Kind.PUT,outer.fixed,new byte[]{(byte)key},value)));
    }
    void enable(NativeBucketApplyTest.Fixture f,RangeMutationHistory.Limits limits) throws Exception {
        f.store.enableScopedHistory(limits); f.store.applyNoop(new ApplyPosition(1,0));
    }
    @Test void realSnapshotDetectsAbaSameValueAndAbsenceWritesButNotDuplicateConflictOrNoop() throws Exception {
        try (var f=outer.new Fixture(root.resolve("aba"),true)) {
            enable(f,RangeMutationHistory.Limits.defaults()); var first=raw(f,0,1,new byte[]{1}); f.apply(1,first);
            try (var ordinary=f.store.capture(); var pin=f.store.captureTracked(point(outer.fixed,1))) {
                assertEquals(new ApplyPosition(1,1),pin.position()); f.apply(2,raw(f,1,1,new byte[]{2})); f.apply(3,raw(f,2,1,new byte[]{1}));
                assertArrayEquals(new byte[]{1},pin.get(outer.fixed,new byte[]{1})); assertEquals(RangeMutationHistory.Status.CONFLICT,pin.validate());
                assertEquals(1,f.store.activeSnapshotPins(),"Ordinary read-only pin survives tracked invalidation"); assertArrayEquals(new byte[]{1},ordinary.get(outer.fixed,new byte[]{1}));
            }
            try (var absent=f.store.captureTracked(point(outer.fixed,2))) {
                assertNull(absent.get(outer.fixed,new byte[]{2})); f.apply(4,raw(f,3,2,null)); assertEquals(RangeMutationHistory.Status.CONFLICT,absent.validate());
            }
            try (var unchanged=f.store.captureTracked(point(outer.fixed,1))) { f.apply(5,raw(f,4,1,new byte[]{1})); assertEquals(RangeMutationHistory.Status.CONFLICT,unchanged.validate()); }
            try (var stable=f.store.captureTracked(point(outer.fixed,1))) {
                f.apply(6,first); f.store.applyNoop(new ApplyPosition(1,7));
                var failed=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,8),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},new byte[]{9})),List.of(new KeyPredicate(outer.fixed,new byte[]{1},new byte[]{8})));
                assertEquals(CommandResult.Status.CONFLICT,f.apply(8,failed).status()); assertEquals(RangeMutationHistory.Status.ACTIVE,stable.validate());
            }
            assertEquals(0,f.store.activeSnapshotPins()); assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes()); assertEquals(0,f.store.scopedHistoryStats().entries());
        }
    }
    @Test void declaredRangePhantomsAndDifferentMembersOfOneBucketConflict() throws Exception {
        try (var f=outer.new Fixture(root.resolve("phantom"),true)) {
            enable(f,RangeMutationHistory.Limits.defaults()); f.apply(1,raw(f,1,2,new byte[]{2}));
            var range=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,new byte[]{1},new byte[]{4})));
            try (var pin=f.store.captureTracked(range)) {
                assertEquals(1,pin.page(outer.fixed,new byte[]{1},new byte[]{4},false,null,16,1024).rows().size()); f.apply(2,raw(f,2,3,new byte[]{3}));
                assertEquals(1,pin.page(outer.fixed,new byte[]{1},new byte[]{4},false,null,16,1024).rows().size()); assertEquals(RangeMutationHistory.Status.CONFLICT,pin.validate());
            }
            f.apply(3,outer.command(f.schema,3,List.of(outer.put(outer.bucket,7,1,new byte[]{1},Mutation.ReturnMode.NONE)),List.of(),-1));
            var bucket=new TransactionScope(List.of(TransactionScope.Span.point(outer.bucket,outer.physical(7))));
            try (var pin=f.store.captureTracked(bucket)) {
                assertThrows(IllegalArgumentException.class,() -> pin.get(outer.fixed,new byte[]{1}));
                f.apply(4,outer.command(f.schema,4,List.of(outer.put(outer.bucket,7,2,new byte[]{2},Mutation.ReturnMode.NONE)),List.of(),-1));
                assertArrayEquals(new byte[]{1},f.schema.decodeBucket(outer.bucket,outer.physical(7),pin.get(outer.bucket,outer.physical(7))).get(outer.member(1)));
                assertEquals(RangeMutationHistory.Status.CONFLICT,pin.validate());
            }
        }
    }
    @Test void historyPressureExpiresOldestWithoutRejectingWritesAndAdmissionAcquiresNoExtraPin() throws Exception {
        try (var f=outer.new Fixture(root.resolve("pressure"),true)) {
            enable(f,new RangeMutationHistory.Limits(3,1,512,4096));
            try (var oldest=f.store.captureTracked(point(outer.fixed,1))) {
                f.apply(1,raw(f,1,2,new byte[]{2})); assertEquals(RangeMutationHistory.Status.ACTIVE,oldest.validate());
                try (var newer=f.store.captureTracked(point(outer.fixed,3))) {
                    f.apply(2,raw(f,2,4,new byte[]{4})); assertEquals(RangeMutationHistory.Status.EXPIRED,oldest.status()); assertEquals(1,f.store.activeSnapshotPins());
                    assertEquals(RangeMutationHistory.Status.ACTIVE,newer.validate()); f.apply(3,raw(f,3,5,new byte[]{5})); assertEquals(RangeMutationHistory.Status.EXPIRED,newer.status());
                    assertArrayEquals(new byte[]{5},f.raw(outer.fixed,new byte[]{5})); assertEquals(0,f.store.activeSnapshotPins()); assertEquals(0,f.store.scopedHistoryStats().historyBytes());
                }
            }
            var held=new ArrayList<ReplicatedRangeStore.ScopedHistoryPin>();
            try {
                for (int i=0;i<3;i++) held.add(f.store.captureTracked(point(outer.fixed,i)));
                assertThrows(IllegalStateException.class,() -> f.store.captureTracked(point(outer.fixed,4))); assertEquals(3,f.store.activeSnapshotPins());
            } finally { held.forEach(pin -> { pin.close(); pin.close(); }); }
            assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes());
            var spans=new ArrayList<TransactionScope.Span>(); for (int i=0;i<100;i++) spans.add(TransactionScope.Span.point(outer.fixed,new byte[]{(byte)i}));
            assertThrows(IllegalStateException.class,() -> f.store.captureTracked(new TransactionScope(spans))); assertEquals(0,f.store.activeSnapshotPins());
        }
        try (var f=outer.new Fixture(root.resolve("oversized"),true)) {
            enable(f,new RangeMutationHistory.Limits(2,16,128,4096));
            try (var pin=f.store.captureTracked(point(outer.fixed,1))) { assertEquals(CommandResult.Status.SUCCESS,f.apply(1,raw(f,1,1,new byte[]{1})).status()); assertEquals(RangeMutationHistory.Status.EXPIRED,pin.status()); }
        }
    }
    @Test void rangeDeletionFenceTermConfigurationAndShutdownNeverResurrectTrackedHandles() throws Exception {
        try (var f=outer.new Fixture(root.resolve("lifecycle"),true)) {
            enable(f,RangeMutationHistory.Limits.defaults());
            try (var raw=f.store.capture(); var pin=f.store.captureTracked(point(outer.fixed,1))) {
                var deletion=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,1),List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.fixed,new byte[]{100},new byte[]{110})));
                f.apply(1,deletion); assertEquals(RangeMutationHistory.Status.CONFLICT,pin.status()); assertEquals(1,f.store.activeSnapshotPins());
                var fenced=f.store.captureTracked(point(outer.fixed,1)); f.store.fence(); assertEquals(RangeMutationHistory.Status.INVALIDATED,fenced.status()); f.store.activate(); assertEquals(RangeMutationHistory.Status.INVALIDATED,fenced.validate()); fenced.close();
                var config=f.store.captureTracked(point(outer.fixed,1)); f.store.applyNoop(new ApplyPosition(1,2),new byte[]{1}); assertEquals(RangeMutationHistory.Status.INVALIDATED,config.status()); config.close();
                var term=f.store.captureTracked(point(outer.fixed,1)); f.store.applyNoop(new ApplyPosition(2,3)); assertEquals(RangeMutationHistory.Status.INVALIDATED,term.status()); term.close();
                var shutdown=f.store.captureTracked(point(outer.fixed,1)); assertFalse(f.store.close(Duration.ZERO)); assertEquals(RangeMutationHistory.Status.INVALIDATED,shutdown.status()); shutdown.close();
                assertNull(raw.get(outer.fixed,new byte[]{1}));
            }
            assertTrue(f.store.close(Duration.ofSeconds(1)));
        }
    }
    @Test void pressureInvalidationDefersSnapshotReleaseUntilBlockedNativeReaderFinishes() throws Exception {
        for (boolean countBound:new boolean[]{false,true}) try (var f=outer.new Fixture(root.resolve("reader-"+countBound),true); var executor=Executors.newSingleThreadExecutor()) {
            enable(f,new RangeMutationHistory.Limits(countBound ? 1 : 2,1,512,countBound ? 4096 : 194)); f.apply(1,raw(f,1,1,new byte[]{1}));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,new byte[]{1},new byte[]{4})));
            try (var pin=f.store.captureTracked(scope)) {
                var began=new CountDownLatch(1); var release=new CountDownLatch(1);
                var read=executor.submit(() -> { pin.visitColumn(outer.fixed,new byte[]{1},new byte[]{4},(key,value) -> {
                    began.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new java.io.IOException("fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new java.io.IOException(interrupted); }
                }); return null; });
                try {
                    assertTrue(began.await(5,TimeUnit.SECONDS)); f.apply(2,raw(f,2,2,new byte[]{2})); f.apply(3,raw(f,3,3,new byte[]{3}));
                    assertEquals(RangeMutationHistory.Status.EXPIRED,pin.status()); assertEquals(1,f.store.activeSnapshotPins()); assertThrows(IllegalStateException.class,() -> pin.get(outer.fixed,new byte[]{1}));
                    assertEquals(0,f.store.scopedHistoryStats().activePins()); assertEquals(1,f.store.activeTrackedSnapshotPins()); assertEquals(194,f.store.trackedSnapshotScopeBytes());
                    assertThrows(IllegalStateException.class,() -> { try (var unexpected=f.store.captureTracked(point(outer.fixed,1))) { fail("Retired reader must retain physical pin and scope-byte admission"); } });
                    if (countBound) assertFalse(f.store.close(Duration.ZERO));
                } finally { release.countDown(); }
                read.get(5,TimeUnit.SECONDS); assertEquals(0,f.store.activeSnapshotPins()); assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes());
                if (!countBound) try (var next=f.store.captureTracked(point(outer.fixed,1))) { assertEquals(RangeMutationHistory.Status.ACTIVE,next.validate()); }
            }
        }
    }
    @Test void postWritePublicationFailureAbortsOldPinsBeforeExposingCommittedData() throws Exception {
        try (var f=outer.new Fixture(root.resolve("publication"),true)) {
            enable(f,RangeMutationHistory.Limits.defaults());
            var field=ReplicatedRangeStore.class.getDeclaredField("scopedHistory"); field.setAccessible(true);
            try (var stale=f.store.captureTracked(point(outer.fixed,1))) {
                ((RangeMutationHistory)field.get(f.store)).advance(new ApplyPosition(1,100),List.of());
                assertThrows(IllegalArgumentException.class,() -> f.store.captureTracked(point(outer.fixed,2)));
                assertEquals(RangeMutationHistory.Status.INVALIDATED,stale.status()); assertEquals(0,f.store.activeTrackedSnapshotPins()); assertEquals(0,f.store.trackedSnapshotScopeBytes());
            }
            try (var pin=f.store.captureTracked(point(outer.fixed,1))) {
                ((RangeMutationHistory)field.get(f.store)).advance(new ApplyPosition(1,100),List.of()); // Inject an impossible publication watermark.
                assertEquals(CommandResult.Status.SUCCESS,f.apply(1,raw(f,1,1,new byte[]{9})).status()); assertEquals(new ApplyPosition(1,1),f.store.applied());
                assertEquals(RangeMutationHistory.Status.INVALIDATED,pin.validate()); assertEquals(0,f.store.activeSnapshotPins()); assertArrayEquals(new byte[]{9},f.raw(outer.fixed,new byte[]{1}));
            }
            try (var next=f.store.captureTracked(point(outer.fixed,1))) { assertEquals(RangeMutationHistory.Status.ACTIVE,next.validate()); f.apply(2,raw(f,2,1,new byte[]{9})); assertEquals(RangeMutationHistory.Status.CONFLICT,next.validate()); }
        }
    }
    @Test void snapshotInstallInvalidatesHistoryAndSeedsTheReplacementCut() throws Exception {
        try (var f=outer.new Fixture(root.resolve("install"),true)) {
            f.store.initializeConfiguration(new byte[]{1}); enable(f,RangeMutationHistory.Limits.defaults()); f.apply(1,raw(f,1,1,new byte[]{1}));
            Path image=root.resolve("image"); var limits=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);
            try (var source=f.store.capture()) { NativeRangeImage.write(image,source,f.store.ownership().interval(),limits); }
            try (var pin=f.store.captureTracked(point(outer.fixed,1))) {
                f.store.fence(); assertEquals(0,f.store.activeSnapshotPins()); f.store.installSnapshot(image,limits); f.store.activate(); assertEquals(RangeMutationHistory.Status.INVALIDATED,pin.validate());
            }
            try (var next=f.store.captureTracked(point(outer.fixed,1))) { assertEquals(new ApplyPosition(1,1),next.position()); f.store.applyNoop(new ApplyPosition(1,2)); assertEquals(RangeMutationHistory.Status.ACTIVE,next.validate()); }
        }
    }
}
