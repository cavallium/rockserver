package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;

/** Actual original native cuts, SST delivery and asynchronous ownership; no network/server fixtures. */
class RatisExportClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T> T await(CompletableFuture<T> value) throws Exception { return value.get(10,TimeUnit.SECONDS); }
    static RatisSnapshotClient snapshots(Fixture f) { return new RatisSnapshotClient(f.schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults()); }
    static RatisExportJournal.Definition definition(Fixture f,RatisSnapshotClient source,Path journal) {
        var id=RatisExportWorkflowTest.identity(f); var column=f.schema.column(71).columnId(); var home=f.snapshots.route(id.fence());
        var plan=RatisSnapshotPlan.create(f.directory,new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),id.fence(),id.business(),home.groupId(),home.rangeEpoch(),id.firstControl());
        var limits=new RatisExportJournal.Limits(16,16L*1024*1024,16L*1024*1024,new NativeRangeSstExport.Limits(1,1024,1024*1024,Duration.ofSeconds(5)));
        return new RatisExportJournal.Definition(new UUID(270,1),plan.fence(),column,journal,source.identityDigest(),limits);
    }
    static void seed(Fixture f) throws Exception {
        RatisRocksDBAdapterTest.seed(f,71,key(2),value(1),1900); RatisRocksDBAdapterTest.seed(f,71,key(4),value(2),1901); RatisRocksDBAdapterTest.seed(f,71,key(200),value(3),1902);
    }
    static RatisExportClient.ChunkSource nativeWriter(Fixture f,RatisExportJournal.Definition definition,Executor executor) {
        return (file,intent,token,context) -> CompletableFuture.supplyAsync(() -> {
            try {
                var span=definition.fence().participant(intent.group()).scope().spans().stream().filter(value -> value.columnId().equals(definition.column())).findFirst().orElseThrow();
                return f.machines.get(intent.group()).exportSnapshotPinChunk(token,pin -> f.catalogue.pool.exportSstChunk(file,pin,intent.column(),intent.scopeStart(),span.upperExclusive(),intent.limits()));
            } catch (Exception failure) { throw new CompletionException(failure); }
        },executor);
    }
    static void ready(RatisExportClient.Job job) throws Exception {
        for (int n=0;n<16 && job.snapshot().phase()!=RatisExportJournal.Phase.READY;n++) await(job.step(RequestContext.batch()));
        assertEquals(RatisExportJournal.Phase.READY,job.snapshot().phase());
    }
    @Test void actualNativeReadyDeliveryReopensWithoutSourceAndAcknowledgesOnlyAfterDestinationDurability() throws Exception {
        Path jobRoot=Files.createDirectory(root.resolve("job")),path=jobRoot.resolve("export.log"),sourcePath=jobRoot.resolve("source.snapshot"); List<RatisExportJournal.Chunk> chunks;
        try (var f=new Fixture(root.resolve("source"),true); var source=snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f); var definition=definition(f,source,sourcePath);
            try (var client=new RatisExportClient(source,f.scheduler,nativeWriter(f,definition,executor),RatisExportClient.Limits.defaults()); var job=client.create(path,definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int captures=f.captureRequests.get();
                RatisRocksDBAdapterTest.seed(f,71,key(4),value(9),1903); ready(job); assertEquals(captures,f.captureRequests.get());
                for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
                var first=await(job.manifest(0,1,65536,RequestContext.batch())); assertEquals(1,first.chunks().size()); assertTrue(first.hasMore());
                chunks=job.snapshot().chunks(); assertEquals(3,chunks.size()); assertTrue(job.snapshot().acknowledged().isEmpty());
                assertThrows(ExecutionException.class,() -> await(job.acknowledge(chunks.getLast(),RequestContext.batch())));
            }
        }
        Files.delete(sourcePath);
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"offline-export-sdk");
        try (var client=new RatisExportClient(null,scheduler,(file,intent,token,context) -> { throw new AssertionError("READY cannot access the source"); },RatisExportClient.Limits.defaults());
             var job=client.reopen(path,RequestContext.batch()); var options=new Options().setCreateIfMissing(true); var destination=RocksDB.open(options,root.resolve("destination").toString())) {
            await(job.completion()); assertTrue(job.snapshot().acknowledged().isEmpty());
            for (var chunk:chunks) {
                Path received=root.resolve("received-"+chunk.group()+"-"+chunk.ordinal()+".sst");
                try (var out=Files.newOutputStream(received,StandardOpenOption.CREATE_NEW)) {
                    long offset=0; do { var page=await(job.read(chunk,offset,65536,RequestContext.batch())); out.write(page.bytes()); offset+=page.bytes().length; if (!page.hasMore()) break; } while (true);
                }
                assertArrayEquals(chunk.sha256(),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(received)));
                try (var ingest=new IngestExternalFileOptions().setMoveFiles(false); var flush=new FlushOptions().setWaitForFlush(true); var sync=new WriteOptions().setSync(true)) {
                    destination.ingestExternalFile(List.of(received.toString()),ingest); destination.flush(flush); destination.put(sync,RatisExportWorkflowTest.checkpoint(chunk),chunk.sha256());
                }
                assertArrayEquals(chunk.sha256(),destination.get(RatisExportWorkflowTest.checkpoint(chunk))); await(job.acknowledge(chunk,RequestContext.batch()));
            }
            assertArrayEquals(new byte[]{1},destination.get(new byte[]{2})); assertArrayEquals(new byte[]{2},destination.get(new byte[]{4})); assertArrayEquals(new byte[]{3},destination.get(new byte[]{(byte)200}));
            await(job.release(RequestContext.batch())); await(job.reclaim(RequestContext.batch())); assertTrue(job.snapshot().artifactsReclaimed());
        } finally { scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void cancellationWaitsForActualNativeExportAndCopiedObserversCannotDonateOwnership() throws Exception {
        try (var f=new Fixture(root.resolve("busy-source"),true); var source=snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f); var directory=Files.createDirectory(root.resolve("busy-job")); var definition=definition(f,source,directory.resolve("source.snapshot")); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            RatisExportClient.ChunkSource writer=(artifact,intent,token,context) -> CompletableFuture.supplyAsync(() -> {
                try { return f.machines.get(intent.group()).exportSnapshotPinChunk(token,pin -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); var span=definition.fence().participant(intent.group()).scope().spans().getFirst(); return f.catalogue.pool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),span.upperExclusive(),intent.limits()); }); }
                catch (Exception failure) { throw new CompletionException(failure); }
            },executor);
            try (var client=new RatisExportClient(source,f.scheduler,writer,RatisExportClient.Limits.defaults()); var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); var view=job.step(RequestContext.batch());
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long charged=client.stats().queuedBytes(); assertTrue(view.cancel(false));
                    for (int n=1;n<RatisExportClient.MAX_WAITERS;n++) assertFalse(job.step(RequestContext.batch()).isDone());
                    assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch())));
                    assertThrows(IOException.class,job::close); assertThrows(IOException.class,client::close);
                    var cancelled=job.cancel(RequestContext.batch()); assertFalse(cancelled.isDone()); assertTrue(cancelled.cancel(false)); var actual=job.cancel(RequestContext.batch()); assertFalse(actual.isDone());
                    assertTrue(client.stats().queuedBytes()>=charged); assertEquals(1,f.machines.get(f.groups.getFirst()).snapshotPinStats().pins());
                    release.countDown(); await(actual); assertEquals(RatisExportJournal.Phase.CANCELLED,job.snapshot().phase());
                    for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins()); assertEquals(0,client.stats().queuedBytes());
                    await(job.release(RequestContext.batch())); await(job.reclaim(RequestContext.batch())); assertTrue(job.snapshot().artifactsReclaimed());
                } finally { release.countDown(); }
            }
        }
    }
    @Test void rejectedDescriptorContinuationRetainsExactNativeOutcomeUntilRetryForcesIt() throws Exception { rejectedDescriptorWithTightBudget(false); }
    @Test void rejectedDescriptorCanCancelDirectlyWithOnlyOneStepOfDataCapacity() throws Exception { rejectedDescriptorWithTightBudget(true); }
    private void rejectedDescriptorWithTightBudget(boolean cancelDirectly) throws Exception {
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"export-descriptor-rejection");
        var unblock=new CountDownLatch(1);
        try (var f=new Fixture(root.resolve("descriptor-source"),true); var source=snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f); var directory=Files.createDirectory(root.resolve("descriptor-job")); var definition=definition(f,source,directory.resolve("source.snapshot"));
            var nativeDone=new CompletableFuture<NativeRangeSstExport.Chunk>(); var reply=new CompletableFuture<NativeRangeSstExport.Chunk>(); var builds=new AtomicInteger();
            var nativeSource=nativeWriter(f,definition,executor);
            RatisExportClient.ChunkSource writer=(artifact,intent,token,context) -> {
                builds.incrementAndGet(); nativeSource.write(artifact,intent,token,context).whenComplete((chunk,failure) -> { if (failure==null) nativeDone.complete(chunk); else nativeDone.completeExceptionally(failure); }); return reply;
            };
            long oneStep=definition.limits().chunk().maximumPayloadBytes()+definition.limits().chunk().maximumFileBytes()+8L*1024*1024;
            var limits=new RatisExportClient.Limits(1,RatisExportClient.Limits.defaults().metadataBytes(),oneStep);
            try (var client=new RatisExportClient(source,scheduler,writer,limits); var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int captures=f.captureRequests.get(); var first=job.step(RequestContext.batch()); var chunk=await(nativeDone);
                var occupied=new CountDownLatch(1); var drained=new CompletableFuture<Void>(); var lane=scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION);
                lane.execute(() -> { occupied.countDown(); try { if (!unblock.await(5,TimeUnit.SECONDS)) throw new AssertionError("Blocked fixture timeout"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } });
                assertTrue(occupied.await(5,TimeUnit.SECONDS)); lane.execute(() -> drained.complete(null));
                assertTrue(reply.complete(chunk)); assertThrows(ExecutionException.class,() -> await(first));
                assertEquals(1,builds.get()); assertTrue(Files.exists(chunk.manifest().file())); assertTrue(job.snapshot().chunks().isEmpty()); assertEquals(oneStep,client.stats().queuedBytes()); assertThrows(IOException.class,job::close);
                unblock.countDown(); await(drained);
                if (cancelDirectly) {
                    await(job.cancel(RequestContext.batch())); assertEquals(RatisExportJournal.Phase.CANCELLED,job.snapshot().phase()); assertTrue(job.snapshot().chunks().isEmpty());
                } else {
                    await(job.step(RequestContext.batch()));
                    assertEquals(chunk.manifest().file(),job.snapshot().chunks().getFirst().file()); assertArrayEquals(chunk.manifest().sha256(),job.snapshot().chunks().getFirst().sha256()); await(job.cancel(RequestContext.batch()));
                }
                assertEquals(1,builds.get()); assertEquals(captures,f.captureRequests.get()); assertEquals(0,client.stats().queuedBytes());
                for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
                await(job.release(RequestContext.batch())); await(job.reclaim(RequestContext.batch())); assertTrue(job.snapshot().artifactsReclaimed()); assertFalse(Files.exists(chunk.manifest().file()));
            }
        } finally { unblock.countDown(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    @Test void partialOriginalPinLossForcesFailureWithoutAnyReplacementCapture() throws Exception {
        try (var f=new Fixture(root.resolve("lost-source"),true); var source=snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f); var directory=Files.createDirectory(root.resolve("lost-job")); var definition=definition(f,source,directory.resolve("source.snapshot"));
            try (var client=new RatisExportClient(source,f.scheduler,nativeWriter(f,definition,executor),RatisExportClient.Limits.defaults()); var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int captures=f.captureRequests.get(); await(job.step(RequestContext.batch()));
                assertEquals(1,job.snapshot().chunks().size()); var group=f.groups.getFirst(); f.machines.get(group).releaseSnapshotPin(job.snapshot().captures().get(group));
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,job.snapshot().phase());
                assertEquals(captures,f.captureRequests.get()); assertEquals(1,job.snapshot().chunks().size()); await(job.cancel(RequestContext.batch()));
            }
        }
    }
    @Test void coldBoundSourceJournalLossFailsBeforeRpcAndNeverRecreatesTheCut() throws Exception {
        try (var f=new Fixture(root.resolve("missing-source"),true); var source=snapshots(f)) {
            seed(f); var directory=Files.createDirectory(root.resolve("missing-job")); var sourcePath=directory.resolve("source.snapshot"); var path=directory.resolve("export.log"); var definition=definition(f,source,sourcePath);
            byte[] original;
            try (var journal=RatisExportJournal.create(path,definition)) {
                var lease=source.openFrozen(sourcePath,definition.fence(),Duration.ofSeconds(30),RequestContext.batch()); await(lease.completion()); journal.bindSource(lease.snapshot());
                assertTrue(RatisExportWorkflowTest.export(journal,f,f.groups.getFirst()).hasMore()); original=Files.readAllBytes(sourcePath); lease.close();
            }
            Files.delete(sourcePath); int calls=f.rpcs.get(),captures=f.captureRequests.get();
            try (var client=new RatisExportClient(source,f.scheduler,(artifact,intent,token,context) -> { throw new AssertionError("Missing original source cannot build"); },RatisExportClient.Limits.defaults()); var job=client.reopen(path,RequestContext.batch())) {
                await(job.completion()); assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch())));
                assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,job.snapshot().phase()); assertEquals(calls,f.rpcs.get()); assertEquals(captures,f.captureRequests.get()); assertFalse(Files.exists(sourcePath));
                // Restore only the caller's saved original journal to settle its original owners.
                Files.write(sourcePath,original,StandardOpenOption.CREATE_NEW); await(job.cancel(RequestContext.batch())); assertEquals(captures,f.captureRequests.get());
            }
        }
    }
    @Test void jobAndSourcePathRolesAreDisjointAndFailedReopenCannotExecute() throws Exception {
        try (var f=new Fixture(root.resolve("roles-source"),true); var source=snapshots(f); var client=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults())) {
            var directory=Files.createDirectory(root.resolve("roles-job")); var sourcePath=directory.resolve("source.snapshot"); var definition=definition(f,source,sourcePath); var one=directory.resolve("one.log"); var two=directory.resolve("two.log");
            try (var first=client.create(one,definition,RequestContext.batch())) {
                await(first.completion()); byte[] original=Files.readAllBytes(one);
                assertThrows(IOException.class,() -> client.reopen(sourcePath,RequestContext.batch()));
                var overlapping=new RatisExportJournal.Definition(new UUID(270,3),definition.fence(),definition.column(),one,definition.sourceIdentity(),definition.limits());
                assertThrows(IOException.class,() -> client.create(two,overlapping,RequestContext.batch())); assertFalse(Files.exists(two)); assertArrayEquals(original,Files.readAllBytes(one));
                try (var saved=RatisExportJournal.create(two,new RatisExportJournal.Definition(new UUID(270,4),definition.fence(),definition.column(),sourcePath,definition.sourceIdentity(),definition.limits()))) { }
                int calls=f.rpcs.get(); var failed=client.reopen(two,RequestContext.batch()); assertThrows(ExecutionException.class,() -> await(failed.completion()));
                assertThrows(ExecutionException.class,() -> await(failed.step(RequestContext.batch()))); assertEquals(calls,f.rpcs.get()); failed.close(); assertEquals(1,client.stats().jobs());
            }
            assertEquals(0,client.stats().jobs()); assertEquals(0,client.stats().metadataBytes());
        }
    }
    @Test void wrongExistingSourceJournalIsRejectedBeforeAnySourceRpcWithoutLeakingOwnership() throws Exception {
        try (var f=new Fixture(root.resolve("mismatch-source"),true); var source=snapshots(f)) {
            seed(f); var directory=Files.createDirectory(root.resolve("mismatch-job")); var sourcePath=directory.resolve("source.snapshot");
            var lease=source.open(sourcePath,RatisSnapshotClientTest.snapshotIdentity(f,1),RatisSnapshotClientTest.scope(f),Duration.ofSeconds(30),RequestContext.batch()); await(lease.completion()); var original=lease.snapshot().initial().fence(); lease.close();
            var definition=definition(f,source,sourcePath); int calls=f.rpcs.get();
            try (var client=new RatisExportClient(source,f.scheduler,(artifact,intent,token,context) -> { throw new AssertionError("Wrong source cannot build"); },RatisExportClient.Limits.defaults())) {
                var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch()); await(job.completion());
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); assertEquals(calls,f.rpcs.get());
                job.close(); assertEquals(0,client.stats().jobs()); assertEquals(0,client.stats().metadataBytes()); assertNull(source.lease(sourcePath)); assertEquals(calls,f.rpcs.get());
            }
            // The SDK never released the unrelated old cut: its actual owner can still read and clean it.
            var retained=source.reopenFrozen(sourcePath,original,RequestContext.batch()); await(retained.completion()); assertArrayEquals(new byte[]{1},await(retained.get(71,key(2),RequestContext.batch())).toByteArray()); await(retained.release(RequestContext.batch())); retained.close();
        }
    }
    @Test void duplicateOrForeignSourceOwnersAreRejectedWithoutReleasingTheirNativeCuts() throws Exception {
        try (var f=new Fixture(root.resolve("exclusive-source"),true); var source=snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f); var directory=Files.createDirectory(root.resolve("exclusive-job")); var definition=definition(f,source,directory.resolve("source.snapshot"));
            try (var client=new RatisExportClient(source,f.scheduler,nativeWriter(f,definition,executor),RatisExportClient.Limits.defaults()); var job=client.create(directory.resolve("one.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int calls=f.rpcs.get();
                var duplicate=new RatisExportJournal.Definition(new UUID(270,2),definition.fence(),definition.column(),definition.sourceJournal(),definition.sourceIdentity(),definition.limits());
                assertThrows(IOException.class,() -> client.create(directory.resolve("two.log"),duplicate,RequestContext.batch())); assertEquals(calls,f.rpcs.get()); assertEquals(1,client.stats().jobs());
                assertArrayEquals(new byte[]{1},await(source.lease(definition.sourceJournal()).get(71,key(2),RequestContext.batch())).toByteArray()); await(job.cancel(RequestContext.batch()));
            }
            var foreignPath=directory.resolve("foreign.snapshot"); var foreign=source.open(foreignPath,RatisSnapshotClientTest.snapshotIdentity(f,1),RatisSnapshotClientTest.scope(f),Duration.ofSeconds(30),RequestContext.batch()); await(foreign.completion());
            var foreignDefinition=definition(f,source,foreignPath); int calls=f.rpcs.get();
            try (var client=new RatisExportClient(source,f.scheduler,nativeWriter(f,foreignDefinition,executor),RatisExportClient.Limits.defaults())) {
                assertThrows(IOException.class,() -> client.create(directory.resolve("foreign.log"),foreignDefinition,RequestContext.batch())); assertEquals(calls,f.rpcs.get()); assertEquals(0,client.stats().jobs());
            }
            assertArrayEquals(new byte[]{1},await(foreign.get(71,key(2),RequestContext.batch())).toByteArray()); await(foreign.release(RequestContext.batch())); foreign.close();
        }
    }
}
