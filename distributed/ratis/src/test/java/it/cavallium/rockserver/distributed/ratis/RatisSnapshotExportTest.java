package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Exact owner-directed native export; no servers, network or quorum acceptance. */
class RatisSnapshotExportTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotPinsTest helper=new RatisSnapshotPinsTest();
    static final NativeRangeSstExport.Limits LIMITS=new NativeRangeSstExport.Limits(1,1024,1024*1024,Duration.ofSeconds(5));
    RatisSnapshotPinsTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name); }
    static void await(CountDownLatch latch) throws Exception { assertTrue(latch.await(5,TimeUnit.SECONDS)); }

    @Test void originalOwnerExportsHistoricalChunksAfterLiveMutationAndLeadershipChange() throws Exception {
        try (var f=fixture("owner-exports")) {
            var machine=f.data.f.machine;
            var token=machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,helper.ttl); f.complete(token); f.change();
            machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
            var first=machine.exportSnapshotPinChunk(token,pin -> f.data.f.pool.exportSstChunk(root.resolve("owner-first.sst"),pin,f.data.f.column,new byte[]{1},new byte[]{4},LIMITS));
            assertTrue(first.hasMore()); assertArrayEquals(new byte[]{2},first.manifest().upperExclusive()); assertEquals(token.position(),first.manifest().position());
            var last=machine.exportSnapshotPinChunk(token,pin -> f.data.f.pool.exportSstChunk(root.resolve("owner-last.sst"),pin,f.data.f.column,first.manifest().upperExclusive(),new byte[]{4},new NativeRangeSstExport.Limits(2,1024,1024*1024,Duration.ofSeconds(5))));
            assertFalse(last.hasMore()); assertArrayEquals(new byte[]{4},last.manifest().upperExclusive()); assertEquals(token.position(),last.manifest().position());
            try (var options=new Options().setCreateIfMissing(true); var target=RocksDB.open(options,root.resolve("owner-target").toString()); var ingest=new IngestExternalFileOptions().setMoveFiles(false)) {
                target.ingestExternalFile(List.of(first.manifest().file().toString(),last.manifest().file().toString()),ingest);
                assertArrayEquals(new byte[]{7},target.get(new byte[]{2}));
            }
            try (var current=f.data.f.store.capture(new TransactionScope(List.of(TransactionScope.Span.range(f.data.f.column,new byte[]{1},new byte[]{4}))))) { assertArrayEquals(new byte[]{9},current.get(f.data.f.column,new byte[]{2})); }
            assertEquals(1,machine.snapshotPinStats().pins()); machine.releaseSnapshotPin(token); assertEquals(0,machine.snapshotPinStats().pins());
        }
    }

    @Test void changedOwnerAndWidenedBoundsCannotPublishAnArtifact() throws Exception {
        try (var f=fixture("owner-bounds")) {
            var machine=f.data.f.machine; var token=machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,helper.ttl); f.complete(token);
            var foreign=new SnapshotFenceStatus.Capture(UUID.randomUUID(),token.snapshotId(),token.position());
            assertThrows(RatisSnapshotLostException.class,() -> machine.exportSnapshotPinChunk(foreign,pin -> f.data.f.pool.exportSstChunk(root.resolve("foreign.sst"),pin,f.data.f.column,new byte[]{1},new byte[]{4},LIMITS)));
            assertThrows(IllegalArgumentException.class,() -> machine.exportSnapshotPinChunk(token,pin -> f.data.f.pool.exportSstChunk(root.resolve("wide.sst"),pin,f.data.f.column,new byte[]{0},new byte[]{4},LIMITS)));
            assertFalse(Files.exists(root.resolve("foreign.sst"))); assertFalse(Files.exists(root.resolve("wide.sst")));
            assertArrayEquals(new byte[]{7},machine.snapshotPinGet(token,f.data.f.column,new byte[]{2})); machine.releaseSnapshotPin(token);
        }
    }

    @Test void releaseRetainsActualExporterAndItsNativePinUntilReturn() throws Exception {
        try (var f=fixture("owner-drain")) {
            var calls=new AtomicInteger(); var clock=new AtomicLong();
            try (var pins=f.registry(clock,calls); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,helper.ttl,() -> 0); f.complete(token);
                var entered=new CountDownLatch(1); var proceed=new CountDownLatch(1);
                var actual=executor.submit(() -> pins.export(token,pin -> {
                    entered.countDown(); if (!proceed.await(5,TimeUnit.SECONDS)) throw new IOException("Exporter was not unblocked");
                    return f.data.f.pool.exportSstChunk(root.resolve("draining.sst"),pin,f.data.f.column,new byte[]{1},new byte[]{4},LIMITS);
                }));
                try { await(entered); pins.release(token); assertEquals(1,pins.stats().pins()); assertEquals(1,f.data.f.store.activeSnapshotPins()); assertFalse(actual.isDone()); }
                finally { proceed.countDown(); }
                assertInstanceOf(RatisSnapshotLostException.class,assertThrows(ExecutionException.class,() -> actual.get(5,TimeUnit.SECONDS)).getCause());
                assertEquals(0,pins.stats().pins()); assertEquals(0,f.data.f.store.activeSnapshotPins()); assertEquals(1,calls.get());
            }
        }
    }
}
