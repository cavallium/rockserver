package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.common.export.*;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

class RatisExportBackendTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static ExportJobs.Create request() {
        return new ExportJobs.Create(new UUID(290,1),List.of(71L,72L,73L,74L),new ExportJobs.Limits(32,16*1024*1024,32*1024*1024,1,1024,1024*1024,Duration.ofSeconds(5),Duration.ofSeconds(30)));
    }
    static RatisExportClient.ChunkSource writer(RatisAtomicClientTest.Fixture f,Executor executor) {
        return new RatisExportClient.ChunkSource() {
            public void bind(RatisExportJournal.Snapshot snapshot) {}
            public CompletableFuture<it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Chunk> write(RatisExportSourceRpc.BuildRequest request,Path file,RequestContext context) {
                return CompletableFuture.supplyAsync(() -> { try { var intent=request.intent(); return f.machines.get(intent.group()).exportSnapshotPinChunk(request.definition().capture(),pin -> f.catalogue.pool.exportSstChunk(file,pin,intent.column(),intent.scopeStart(),request.definition().scope(intent.column()).upperExclusive(),intent.limits())); } catch (Exception failure) { throw new CompletionException(failure); } },executor);
            }
            public CompletableFuture<Void> release(RatisExportSourceRpc.JobDefinition definition,RequestContext context) { return CompletableFuture.completedFuture(null); }
        };
    }
    @Test void allColumnsTwoGroupsLostCreateReplyColdDeliveryAndExplicitDurableAck() throws Exception {
        Path backendRoot=root.resolve("jobs"); UUID jobId; int captures;
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(200),value(2),1901); seed(f,72,key(4),value(3),1902);
            var allocations=new Allocations(f);
            try (var sdk=new RatisExportClient(source,f.scheduler,writer(f,executor),RatisExportClient.Limits.defaults()); var backend=new RatisExportBackend(backendRoot,f.schema,source,f.scheduler,sdk,allocations,RatisExportBackend.Limits.defaults())) {
                var created=await(backend.createExport(request(),RequestContext.batch())); jobId=created.jobId();
                assertEquals(0,f.captureRequests.get()); assertEquals(1,allocations.count.get()); assertEquals(2,created.participantCount());
                assertEquals(jobId,await(backend.createExport(request(),RequestContext.batch())).jobId());
                var changed=new ExportJobs.Create(request().requestId(),List.of(71L),request().limits());
                assertThrows(ExecutionException.class,() -> await(backend.createExport(changed,RequestContext.batch()))); assertEquals(1,allocations.count.get());
                var status=await(backend.stepExport(jobId,RequestContext.batch()));
                var original=source.lease(allocations.issued.getFirst().snapshot()).snapshot();
                var tokens=original.participants().stream().collect(java.util.stream.Collectors.toMap(participant -> participant.route().groupId(),RatisDurableSnapshot.ParticipantProgress::captureToken));
                assertEquals(2,tokens.size()); for (var group:f.groups) assertEquals(1,f.machines.get(group).snapshotPinStats().pins());
                captures=f.captureRequests.get();
                for (int i=0;i<32 && status.phase()!=ExportJobs.Phase.READY;i++) status=await(backend.stepExport(jobId,RequestContext.batch()));
                assertEquals(ExportJobs.Phase.READY,status.phase()); assertEquals(captures,f.captureRequests.get());
                try (var settled=RatisDurableSnapshot.reopen(allocations.issued.getFirst().snapshot(),f.snapshots,f.data)) {
                    for (var participant:settled.snapshot().participants()) { assertEquals(tokens.get(participant.route().groupId()),participant.captureToken()); assertTrue(participant.captureReleased() || participant.captureReleaseLost()); }
                }
                for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
                assertEquals(0,status.acknowledgedCount()); assertEquals(List.of(71L,72L,73L,74L),status.columns());
                var entry=new RatisExportRequests(backendRoot,128,16*1024*1024).job(jobId); assertEquals(Duration.ofSeconds(30),entry.definition(f.schema.columns().stream().map(DistributedSchema.Column::columnId).toList()).snapshotTtl());
            }
            Files.delete(allocations.issued.getFirst().snapshot());
            Files.write(backendRoot.resolve("requests.pending"),new byte[]{1,2,3},StandardOpenOption.CREATE_NEW);
            try (var sdk=new RatisExportClient(null,f.scheduler,null,RatisExportClient.Limits.defaults()); var backend=new RatisExportBackend(backendRoot,f.schema,null,f.scheduler,sdk,null,RatisExportBackend.Limits.defaults()); var options=new Options().setCreateIfMissing(true); var destination=RocksDB.open(options,root.resolve("destination").toString())) {
                assertFalse(Files.exists(backendRoot.resolve("requests.pending")));
                assertEquals(jobId,await(backend.createExport(request(),RequestContext.batch())).jobId()); assertEquals(captures,f.captureRequests.get());
                var manifest=await(backend.exportManifest(new ExportJobs.ManifestRequest(jobId,0,128,4*1024*1024),RequestContext.batch())); assertFalse(manifest.hasMore()); assertEquals(8,manifest.chunks().size());
                var first=manifest.chunks().getFirst(); var id=first.id(); var invalid=new ExportJobs.ChunkId(id.jobId(),id.groupId(),new UUID(999,1),id.rangeEpoch(),id.snapshotTerm(),id.snapshotIndex(),id.ordinal(),id.contentHash());
                assertThrows(ExecutionException.class,() -> await(backend.readExport(new ExportJobs.ReadRequest(invalid,0,65536),RequestContext.batch())));
                assertThrows(ExecutionException.class,() -> await(backend.acknowledgeExport(invalid,RequestContext.batch())));
                assertThrows(ExecutionException.class,() -> await(backend.releaseExport(jobId,RequestContext.batch())));
                for (var chunk:manifest.chunks()) {
                    if (chunk.rows()>0) {
                        Path received=root.resolve("received-"+chunk.id().groupId()+"-"+chunk.id().ordinal()); long offset=0;
                        try (var out=FileChannel.open(received,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
                            while (true) { var page=await(backend.readExport(new ExportJobs.ReadRequest(chunk.id(),offset,65536),RequestContext.batch())); var bytes=java.nio.ByteBuffer.wrap(page.bytes()); while (bytes.hasRemaining()) out.write(bytes); offset+=page.bytes().length; if (!page.hasMore()) break; } out.force(true);
                        }
                        assertArrayEquals(chunk.id().contentHash(),java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(received)));
                        try (var ingest=new IngestExternalFileOptions().setMoveFiles(false); var flush=new FlushOptions().setWaitForFlush(true); var sync=new WriteOptions().setSync(true)) { destination.ingestExternalFile(List.of(received.toString()),ingest); destination.flush(flush); destination.put(sync,chunk.id().contentHash(),new byte[]{1}); }
                        assertArrayEquals(new byte[]{1},destination.get(chunk.id().contentHash()));
                    }
                    await(backend.acknowledgeExport(chunk.id(),RequestContext.batch()));
                }
                await(backend.releaseExport(jobId,RequestContext.batch()));
                try (var ownedFiles=Files.list(backendRoot.resolve(jobId.toString()))) { assertTrue(ownedFiles.noneMatch(path -> path.getFileName().toString().endsWith(".sst") || path.getFileName().toString().endsWith(".manifest") || path.getFileName().toString().endsWith(".download"))); }
                assertEquals(ExportJobs.Phase.RELEASED,await(backend.exportStatus(jobId,RequestContext.batch())).phase()); assertEquals(captures,f.captureRequests.get());
            }
        }
    }
    @Test void durablePendingAndBoundBeforeCaptureSurviveRestartWithoutChangingJobIdentity() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f)) {
            Path pendingRoot=Files.createDirectory(root.resolve("pending")); var ledger=new RatisExportRequests(pendingRoot,128,16*1024*1024); var pending=ledger.pending(request(),f.schema.digest(),source.identityDigest()); var allocations=new Allocations(f);
            try (var sdk=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults()); var backend=new RatisExportBackend(pendingRoot,f.schema,source,f.scheduler,sdk,allocations,RatisExportBackend.Limits.defaults())) {
                assertEquals(pending.job(),await(backend.createExport(request(),RequestContext.batch())).jobId()); assertEquals(1,allocations.count.get()); assertEquals(0,f.captureRequests.get());
            }
            Path boundRoot=Files.createDirectory(root.resolve("bound")); var second=new RatisExportRequests(boundRoot,128,16*1024*1024); var original=second.pending(request(),f.schema.digest(),source.identityDigest()); var allocation=allocations.issued.getFirst();
            var scope=new TransactionScope(f.schema.columns().stream().map(column -> TransactionScope.Span.range(column.columnId(),null,null)).toList()); second.bind(original,source.plan(allocation.identity().snapshot(),scope).fence(),allocation.snapshot());
            try (var sdk=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults()); var backend=new RatisExportBackend(boundRoot,f.schema,source,f.scheduler,sdk,context -> { throw new AssertionError("Bound nonce cannot allocate again"); },RatisExportBackend.Limits.defaults())) {
                assertEquals(original.job(),await(backend.createExport(request(),RequestContext.batch())).jobId()); assertEquals(0,f.captureRequests.get());
                assertThrows(IOException.class,() -> new RatisExportBackend(boundRoot,f.schema,source,f.scheduler,sdk,allocations,RatisExportBackend.Limits.defaults()));
            }
        }
    }
    @Test void cancelledObserverCannotReleaseActualAllocationOwnership() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f); var sdk=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults())) {
            var entered=new CountDownLatch(1); var allocation=new CompletableFuture<RatisRocksDBAdapter.Allocation>(); var allocations=new Allocations(f); Path jobs=root.resolve("jobs");
            var backend=new RatisExportBackend(jobs,f.schema,source,f.scheduler,sdk,context -> { entered.countDown(); return allocation; },RatisExportBackend.Limits.defaults());
            try { var observer=backend.createExport(request(),RequestContext.batch()); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(observer.cancel(false)); assertThrows(IOException.class,backend::close); var entry=new RatisExportRequests(jobs,128,16*1024*1024).nonce(request().requestId()); assertNotNull(entry); assertNull(entry.fence());
                var repeat=backend.createExport(request(),RequestContext.batch()); allocation.complete(await(allocations.allocate(RequestContext.batch()))); assertEquals(entry.job(),await(repeat).jobId());
            } finally { if (!allocation.isDone()) allocation.completeExceptionally(new IOException("Fixture cleanup")); backend.close(); }
        }
    }
    @Test void retiredNoncesRemainBoundedAndMissingUsedCatalogCannotBeRecreated() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f); var sdk=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults())) {
            Path jobs=root.resolve("jobs"); var allocations=new Allocations(f);
            var limits=new RatisExportBackend.Limits(1,65536,1,4);
            try (var backend=new RatisExportBackend(jobs,f.schema,source,f.scheduler,sdk,allocations,limits)) {
                var created=await(backend.createExport(request(),RequestContext.batch()));
                await(backend.cancelExport(created.jobId(),RequestContext.batch()));
                var other=new ExportJobs.Create(new UUID(290,2),request().columns(),request().limits());
                assertThrows(ExecutionException.class,() -> await(backend.createExport(other,RequestContext.batch())));
                assertEquals(1,allocations.count.get());
            }
            Files.delete(jobs.resolve("requests"));
            assertThrows(IOException.class,() -> new RatisExportBackend(jobs,f.schema,source,f.scheduler,sdk,allocations,limits));
            assertFalse(Files.exists(jobs.resolve("requests")));
        }
    }

    @Test void knownOwnedCancellationUsesReservedControlWhileBatchAndBulkAdmissionAreStalled() throws Exception {
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"public-export-control");
        var unblock=new CountDownLatch(1);
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f); var executor=Executors.newVirtualThreadPerTaskExecutor(); var sdk=new RatisExportClient(source,scheduler,writer(f,executor),RatisExportClient.Limits.defaults()); var backend=new RatisExportBackend(root.resolve("jobs"),f.schema,source,scheduler,sdk,new Allocations(f),new RatisExportBackend.Limits(128,16*1024*1024,8,1))) {
            var created=await(backend.createExport(request(),RequestContext.batch()));
            await(backend.stepExport(created.jobId(),RequestContext.batch()));
            for (var group:f.groups) assertEquals(1,f.machines.get(group).snapshotPinStats().pins());
            var occupied=new CountDownLatch(1); var lane=scheduler.executor(RequestContext.batch(),OperationFamily.MUTATION);
            lane.execute(() -> { occupied.countDown(); try { if (!unblock.await(10,TimeUnit.SECONDS)) throw new AssertionError("Blocked fixture timeout"); } catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); } });
            assertTrue(occupied.await(5,TimeUnit.SECONDS));
            var bulk=backend.exportStatus(created.jobId(),RequestContext.batch());
            assertFalse(bulk.isDone());
            assertThrows(ExecutionException.class,() -> await(backend.exportStatus(created.jobId(),RequestContext.batch())));
            await(backend.cancelExport(created.jobId(),RequestContext.batch()));
            for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
            assertEquals(0,sdk.stats().jobs()); assertFalse(bulk.isDone());
            assertThrows(IOException.class,backend::close);
            int settledCaptures=f.captureRequests.get(); unblock.countDown(); assertEquals(ExportJobs.Phase.CANCELLED,await(bulk).phase()); assertEquals(settledCaptures,f.captureRequests.get());
            await(backend.releaseExport(created.jobId(),RequestContext.batch())); assertEquals(ExportJobs.Phase.RELEASED,await(backend.exportStatus(created.jobId(),RequestContext.batch())).phase()); assertEquals(settledCaptures,f.captureRequests.get());
        } finally { unblock.countDown(); scheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }

    @Test void delayedAllocationForcesOriginalBindingWithoutResettingExpiredPublicDeadline() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f); var sdk=new RatisExportClient(source,f.scheduler,null,RatisExportClient.Limits.defaults())) {
            Path jobs=root.resolve("jobs"); var entered=new CountDownLatch(1); var allocation=new CompletableFuture<RatisRocksDBAdapter.Allocation>(); var allocationContext=new java.util.concurrent.atomic.AtomicReference<RequestContext>(); var allocations=new Allocations(f);
            try (var backend=new RatisExportBackend(jobs,f.schema,source,f.scheduler,sdk,context -> { allocationContext.set(context); entered.countDown(); return allocation; },RatisExportBackend.Limits.defaults())) {
                var result=backend.createExport(request(),new RequestContext(WorkloadProfile.BATCH,Duration.ofSeconds(10).toNanos()));
                assertTrue(entered.await(8,TimeUnit.SECONDS)); assertTrue(allocationContext.get().timeoutNanos()<=Duration.ofSeconds(10).toNanos());
                Thread.sleep(Duration.ofNanos(allocationContext.get().timeoutNanos()+Duration.ofMillis(20).toNanos()));
                assertFalse(result.isDone()); assertThrows(IOException.class,backend::close);
                var issued=await(allocations.allocate(RequestContext.batch())); allocation.complete(issued);
                assertThrows(ExecutionException.class,() -> await(result));
                var saved=new RatisExportRequests(jobs,128,16*1024*1024).nonce(request().requestId());
                assertNotNull(saved.fence()); assertEquals(issued.snapshot().toAbsolutePath(),saved.snapshot()); assertFalse(saved.initialized()); assertFalse(Files.exists(jobs.resolve(saved.job().toString()).resolve("export.log"))); assertEquals(0,f.captureRequests.get());
                assertEquals(saved.job(),await(backend.createExport(request(),RequestContext.batch())).jobId()); assertEquals(1,allocations.count.get()); assertEquals(0,f.captureRequests.get());
            }
        }
    }
    @Test void sourceSettlementReceiptRequiresActualOriginalGateAndPinRelease() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var source=RatisExportClientTest.snapshots(f)) {
            Path directory=Files.createDirectory(root.resolve("job")); var allocations=new Allocations(f); var allocation=await(allocations.allocate(RequestContext.batch()));
            var scope=new TransactionScope(f.schema.columns().stream().map(column -> TransactionScope.Span.range(column.columnId(),null,null)).toList());
            var ledger=new RatisExportRequests(directory,128,16*1024*1024); var pending=ledger.pending(request(),f.schema.digest(),source.identityDigest()); var bound=ledger.bind(pending,source.plan(allocation.identity().snapshot(),scope).fence(),allocation.snapshot()); var definition=bound.definition(f.schema.columns().stream().map(DistributedSchema.Column::columnId).toList());
            var lease=source.openFrozen(allocation.snapshot(),definition.fence(),definition.snapshotTtl(),RequestContext.batch());
            try {
                await(lease.completion());
                try (var journal=RatisExportJournal.create(directory.resolve("export.log"),definition)) {
                    journal.bindSource(lease.snapshot()); journal.cancel();
                    assertThrows(IllegalArgumentException.class,() -> journal.confirmSourceSettlement(lease.snapshot())); assertFalse(journal.sourceSettled());
                }
                try (var journal=RatisExportJournal.reopen(directory.resolve("export.log"))) {
                    assertFalse(journal.sourceSettled()); for (var group:f.groups) assertEquals(1,f.machines.get(group).snapshotPinStats().pins());
                    await(lease.release(RequestContext.batch())); journal.confirmSourceSettlement(lease.snapshot()); assertTrue(journal.sourceSettled());
                }
                try (var journal=RatisExportJournal.reopen(directory.resolve("export.log"))) { assertTrue(journal.sourceSettled()); }
            } finally { await(lease.release(RequestContext.batch())); lease.close(); }
        }
    }

}
