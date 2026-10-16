package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import org.rocksdb.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisAtomicClientTest.*;
import static it.cavallium.rockserver.distributed.ratis.RatisExportClientTest.*;

/** Controlled owner transport: no sockets, server, quorum, or replacement source capture. */
class RatisRemoteExportSourceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final class Proxy implements RatisRoutedClient.Transport {
        final Fixture fixture; final Path artifacts;
        final Map<String,RatisExportSourceRpc.Descriptor> descriptors=new HashMap<>();
        final Map<String,Path> files=new HashMap<>();
        boolean loseBuild=true,truncate,failRelease; int builds,releases;
        Proxy(Fixture fixture,Path artifacts) { this.fixture=fixture; this.artifacts=artifacts; }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
        String key(RatisExportSourceRpc.JobDefinition d,long ordinal) { return d.jobId()+"-"+d.group()+"-"+ordinal; }
        public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] frame) throws IOException {
            assertEquals(route.replicas().getFirst(),owner);
            var request=ScopedTransactionRpc.parse(frame,ScopedTransactionRpc.MAX_REQUEST_BYTES); int kind=request.kind();
            try {
                if (kind==ScopedTransactionRpc.EXPORT_BUILD) {
                    var build=RatisExportSourceRpc.BuildRequest.decode(request.body()); var d=build.definition(); var i=build.intent(); String key=key(d,i.ordinal());
                    var descriptor=descriptors.get(key);
                    if (descriptor==null) {
                        var file=artifacts.resolve(key+".sst");
                        var chunk=fixture.machines.get(d.group()).exportSnapshotPinChunk(d.capture(),pin -> fixture.catalogue.pool.exportSstChunk(file,pin,i.column(),i.scopeStart(),d.scope(i.column()).upperExclusive(),i.limits()));
                        var m=chunk.manifest(); descriptor=new RatisExportSourceRpc.Descriptor(d.digest(),i.ordinal(),m.groupId(),m.rangeEpoch(),m.schemaDigest(),m.columnId(),m.position(),m.lowerInclusive(),m.upperExclusive(),m.rows()==0 ? new byte[32] : m.sha256(),m.rows(),m.payloadBytes(),m.fileBytes(),chunk.hasMore());
                        descriptors.put(key,descriptor); files.put(key,m.file()); builds++;
                    }
                    if (loseBuild) { loseBuild=false; throw new IOException("Lost BUILD reply after durable artifact"); }
                    return java.util.concurrent.CompletableFuture.completedFuture(ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.BuildReply(build,descriptor).encode()));
                }
                if (kind==ScopedTransactionRpc.EXPORT_READ) {
                    var read=RatisExportSourceRpc.ReadRequest.decode(request.body()); String key=key(read.definition(),read.ordinal());
                    assertArrayEquals(descriptors.get(key).digest(),read.descriptorDigest()); assertTrue(read.maxBytes()<=1024*1024);
                    var output=ByteBuffer.allocate(read.maxBytes()); try (var input=FileChannel.open(files.get(key),StandardOpenOption.READ)) { input.position(read.offset()); while(output.hasRemaining() && input.read(output)>=0) {} }
                    byte[] bytes=Arrays.copyOf(output.array(),output.position()); if (truncate) { truncate=false; bytes=Arrays.copyOf(bytes,bytes.length-1); }
                    return java.util.concurrent.CompletableFuture.completedFuture(ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.ReadReply(read,bytes).encode()));
                }
                if (kind==ScopedTransactionRpc.EXPORT_RELEASE) {
                    var release=RatisExportSourceRpc.ReleaseRequest.decode(request.body()); if (failRelease) { failRelease=false; throw new IOException("Controlled RELEASE rejection"); }
                    releases++; return java.util.concurrent.CompletableFuture.completedFuture(ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.ReleaseReply(release).encode()));
                }
                throw new AssertionError("Unexpected owner query "+kind);
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
    }
    @Test void coldLostBuildReplyRecoversCompletedArtifactAfterOriginalPinLossThenFailsRemainingCoverageAndRetriesCleanup() throws Exception {
        try (var f=new Fixture(root.resolve("source"),true); var source=snapshots(f)) {
            seed(f); var directory=Files.createDirectory(root.resolve("job")); var definition=definition(f,source,directory.resolve("source.snapshot")); var path=directory.resolve("export.log");
            var proxy=new Proxy(f,Files.createDirectory(root.resolve("remote")));
            try (var routed=new RatisRoutedClient(f.directory,proxy)) {
                var remote=new RatisRemoteExportSource(source,routed,RatisJournalAccess.standalone()); UUID group;
                try (var journal=RatisExportJournal.create(path,definition)) {
                    var lease=source.openFrozen(definition.sourceJournal(),definition.fence(),definition.snapshotTtl(),RequestContext.batch()); await(lease.completion()); journal.bindSource(lease.snapshot()); remote.bind(journal.snapshot());
                    var saved=journal.snapshot(); var alteredLimits=new RatisExportJournal.Limits(definition.limits().maximumChunks()+1,definition.limits().maximumJournalBytes(),definition.limits().maximumDiskBytes(),definition.limits().chunk());
                    var changed=new RatisExportJournal.Definition(definition.jobId(),definition.fence(),definition.columns(),definition.sourceJournal(),definition.sourceIdentity(),alteredLimits,java.time.Duration.ofSeconds(60));
                    assertThrows(IOException.class,() -> remote.bind(new RatisExportJournal.Snapshot(changed,saved.phase(),saved.captures(),saved.owners(),saved.sourceCleanup(),saved.pending(),saved.chunks(),saved.acknowledged(),saved.pendingArtifactBytes(),saved.reclaiming(),saved.artifactsReclaimed())));
                    group=f.groups.getFirst(); var intent=journal.freezeChunk(group); var request=new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(journal.snapshot(),group),intent);
                    assertThrows(ExecutionException.class,() -> await(remote.write(request,journal.artifactPath(intent),RequestContext.batch()))); assertEquals(1,proxy.builds); assertFalse(Files.exists(journal.artifactPath(intent)));
                    f.machines.get(group).releaseSnapshotPin(journal.snapshot().captures().get(group)); lease.close();
                }
                int captures=f.captureRequests.get();
                try (var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed,RatisJournalAccess.standalone()),RatisExportClient.Limits.defaults()); var job=client.reopen(path,RequestContext.batch())) {
                    await(job.completion()); await(job.step(RequestContext.batch())); assertEquals(1,proxy.builds); assertEquals(1,job.snapshot().chunks().size()); assertEquals(captures,f.captureRequests.get()); assertEquals(0,proxy.releases);
                    assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,job.snapshot().phase());
                    proxy.failRelease=true; assertThrows(ExecutionException.class,() -> await(job.cancel(RequestContext.batch()))); assertFalse(job.snapshot().sourceCleanup().isEmpty()); assertThrows(IOException.class,job::close);
                    await(job.cancel(RequestContext.batch())); assertTrue(job.snapshot().sourceCleanup().isEmpty()); assertEquals(captures,f.captureRequests.get());
                }
            }
        }
    }
    @Test void truncatedDownloadDoesNotPublishOrReleaseAndRetryUsesExactCompletedBuild() throws Exception {
        try (var f=new Fixture(root.resolve("source"),true); var source=snapshots(f)) {
            seed(f); var directory=Files.createDirectory(root.resolve("job")); var definition=definition(f,source,directory.resolve("source.snapshot")); var proxy=new Proxy(f,Files.createDirectory(root.resolve("remote"))); proxy.loseBuild=false; proxy.truncate=true;
            try (var routed=new RatisRoutedClient(f.directory,proxy); var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed,RatisJournalAccess.standalone()),RatisExportClient.Limits.defaults()); var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); var captures=job.snapshot().captures();
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); var pending=job.snapshot().pending(); assertNotNull(pending); assertTrue(job.snapshot().chunks().isEmpty()); assertEquals(0,proxy.releases);
                try (var files=Files.list(directory)) { assertFalse(files.anyMatch(file -> file.getFileName().toString().endsWith(".sst") || file.getFileName().toString().endsWith(".manifest"))); }
                await(job.step(RequestContext.batch())); assertEquals(1,proxy.builds); assertEquals(captures,job.snapshot().captures()); assertEquals(1,job.snapshot().chunks().size()); assertEquals(0,proxy.releases);
                await(job.cancel(RequestContext.batch())); assertTrue(job.snapshot().sourceCleanup().isEmpty());
            }
        }
    }
    @Test void completeFourColumnNativeServiceWorkflowRetriesLostReleaseThenRoutesAcknowledgesAndReopensOffline() throws Exception {
        var sourceScheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,8,8,"remote-export-service");
        var directory=Files.createDirectory(root.resolve("complete-job")); var path=directory.resolve("export.log"); var destinationPath=root.resolve("destination"); List<RatisExportJournal.Chunk> chunks;
        try (var f=new Fixture(root.resolve("complete-source"),true,Map.of(73L,ColumnSchema.of(it.unimi.dsi.fastutil.ints.IntArrayList.of(1),new it.unimi.dsi.fastutil.objects.ObjectArrayList<>(),false)),
                new it.cavallium.rockserver.core.impl.rocksdb.PoolSnapshotAdmission.Limits(2,1L<<40,Duration.ofSeconds(60),0)); var source=snapshots(f)) {
            seed(f); RatisRocksDBAdapterTest.seed(f,73,key(2),null,1904); RatisRocksDBAdapterTest.seed(f,73,key(200),null,1905);
            RatisRocksDBAdapterTest.seed(f,74,member(2,"first"),value(4),1906); RatisRocksDBAdapterTest.seed(f,74,member(200,"second"),value(5),1907);
            var id=RatisExportWorkflowTest.identity(f); var spans=new ArrayList<TransactionScope.Span>();
            for (var column:f.schema.columnIds()) {
                if (column.equals(f.schema.column(71).columnId())) {
                    spans.add(TransactionScope.Span.range(column,null,new byte[]{4}));
                    spans.add(TransactionScope.Span.range(column,new byte[]{4},null));
                } else spans.add(TransactionScope.Span.range(column,null,null));
            }
            var home=f.snapshots.route(id.fence()); var plan=RatisSnapshotPlan.create(List.of(f.directory),new TransactionScope(spans),id.fence(),id.business(),home.groupId(),home.rangeEpoch(),id.firstControl());
            var baseline=definition(f,source,directory.resolve("source.snapshot"));
            var definition=new RatisExportJournal.Definition(baseline.jobId(),plan.fence(),new ArrayList<>(f.schema.columnIds()),baseline.sourceJournal(),source.identityDigest(),baseline.limits(),java.time.Duration.ofSeconds(60)); var builds=new java.util.concurrent.atomic.AtomicInteger();
            var builder=new RatisExportSourceRegistry.NativeBuilder() {
                public NativeRangeSstExport.Chunk build(RatisExportSourceRpc.JobDefinition d,RatisExportJournal.ChunkIntent intent,Path artifact,RatisExportSourceRegistry.Completion completion) throws Exception {
                    var machine=f.machines.get(d.group()); machine.requireExportPin(d); builds.incrementAndGet();
                    return machine.exportSnapshotPinChunk(d.capture(),pin -> { var chunk=f.catalogue.pool.exportSstChunk(artifact,pin,intent.column(),intent.scopeStart(),d.scope(intent.column()).upperExclusive(),intent.limits()); completion.persist(chunk); return chunk; });
                }
                public boolean tokenUsable(RatisExportSourceRpc.JobDefinition d) throws Exception { try { f.machines.get(d.group()).requireExportPin(d); return true; } catch (RatisSnapshotLostException lost) { return false; } }
            };
            var service=new RatisExportSourceService(root.resolve("service-artifacts"),RatisExportSourceRegistry.Budgets.defaults(),RatisExportSourceService.IoLimits.defaults(),builder,sourceScheduler);
            var releases=new ArrayList<byte[]>(); var loseRelease=new java.util.concurrent.atomic.AtomicBoolean(true);
            try (var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] frame) throws IOException {
                    assertEquals(route.replicas().getFirst(),owner);var packet=ScopedTransactionRpc.parse(frame,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if(packet.kind()==ScopedTransactionRpc.EXPORT_RELEASE){for(var group:f.groups)assertEquals(0,f.machines.get(group).snapshotPinStats().pins());releases.add(packet.body());}
                    return service.submit(route.groupId(),packet.kind(),packet.body()).thenApply(reply->{if(packet.kind()==ScopedTransactionRpc.EXPORT_RELEASE&&loseRelease.compareAndSet(true,false))throw new java.util.concurrent.CompletionException(new IOException("Lost RELEASE reply after durable cleanup"));return ScopedTransactionRpc.frame(packet.kind(),reply);});
                }
            }); var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed,RatisJournalAccess.standalone()),RatisExportClient.Limits.defaults()); var job=client.create(path,definition,RequestContext.batch()); var destination=new Destination(destinationPath,true,definition.columns())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int captures=f.captureRequests.get();
                assertEquals(2,f.catalogue.pool.snapshotAdmissionStats().pins());
                RatisRocksDBAdapterTest.seed(f,71,key(4),value(9),1903);
                while (job.snapshot().phase()==RatisExportJournal.Phase.EXPORTING) { for (var group:f.groups) assertEquals(1,f.machines.get(group).snapshotPinStats().pins()); await(job.step(RequestContext.batch())); }
                assertEquals(RatisExportJournal.Phase.MATERIALIZED,job.snapshot().phase()); assertEquals(9,builds.get()); assertTrue(releases.isEmpty()); for (var group:f.groups) assertEquals(1,f.machines.get(group).snapshotPinStats().pins());
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); assertEquals(RatisExportJournal.Phase.MATERIALIZED,job.snapshot().phase()); assertFalse(job.snapshot().sourceCleanup().isEmpty());
                await(job.step(RequestContext.batch())); assertEquals(RatisExportJournal.Phase.READY,job.snapshot().phase()); assertTrue(job.snapshot().sourceCleanup().isEmpty()); assertEquals(captures,f.captureRequests.get()); assertEquals(0,f.catalogue.pool.snapshotAdmissionStats().pins()); assertEquals(3,releases.size()); assertArrayEquals(releases.get(0),releases.get(1));
                chunks=job.snapshot().chunks(); assertEquals(4,chunks.stream().map(RatisExportJournal.Chunk::column).distinct().count()); assertEquals(2,chunks.stream().filter(chunk -> chunk.column().equals(f.schema.column(72).columnId()) && chunk.rows()==0).count()); assertEquals(2,chunks.stream().map(RatisExportJournal.Chunk::group).distinct().count());
                var manifest=await(job.manifest(0,128,65536,RequestContext.batch())); assertEquals(chunks,manifest.chunks()); assertFalse(manifest.hasMore());
                for (var chunk:chunks) {
                    if (chunk.rows()==0) { destination.checkpoint(chunk); await(job.acknowledge(chunk,RequestContext.batch())); continue; }
                    Path received=root.resolve("complete-received-"+chunk.group()+"-"+chunk.ordinal()+".sst");
                    try (var output=Files.newOutputStream(received,StandardOpenOption.CREATE_NEW)) { long offset=0; while (true) { var page=await(job.read(chunk,offset,65536,RequestContext.batch())); output.write(page.bytes()); offset+=page.bytes().length; if (!page.hasMore()) break; } }
                    assertArrayEquals(chunk.sha256(),java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(received)));
                    try (var ingest=new IngestExternalFileOptions().setMoveFiles(false); var flush=new FlushOptions().setWaitForFlush(true); var sync=new WriteOptions().setSync(true)) { destination.db.ingestExternalFile(destination.columns.get(chunk.column()),List.of(received.toString()),ingest); destination.db.flush(flush); destination.db.put(sync,RatisExportWorkflowTest.checkpoint(chunk),chunk.sha256()); }
                    await(job.acknowledge(chunk,RequestContext.batch()));
                }
                assertArrayEquals(new byte[]{2},destination.get(f.schema.column(71).columnId(),new byte[]{4}));
                assertArrayEquals(new byte[0],destination.get(f.schema.column(73).columnId(),new byte[]{2}));
                var bucketKey=f.schema.encodeLogicalKey(f.schema.column(74).columnId(),member(2,"first")).physicalKey(); var bucket=f.schema.decodeBucket(f.schema.column(74).columnId(),bucketKey,destination.get(f.schema.column(74).columnId(),bucketKey)); assertArrayEquals(new byte[]{4},bucket.members().getFirst().value());
                assertEquals(chunks.size(),job.snapshot().acknowledged().size());
            } finally { await(service.stop()); }
        } finally { sourceScheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
        var offlineScheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"remote-export-offline");
        try (var client=new RatisExportClient(null,offlineScheduler,null,RatisExportClient.Limits.defaults()); var job=client.reopen(path,RequestContext.batch()); var destination=new Destination(destinationPath,false,chunks.stream().map(RatisExportJournal.Chunk::column).distinct().toList())) {
            await(job.completion()); assertEquals(RatisExportJournal.Phase.READY,job.snapshot().phase()); assertEquals(chunks.size(),job.snapshot().acknowledged().size());
            for (var chunk:chunks) assertArrayEquals(chunk.sha256(),destination.db.get(RatisExportWorkflowTest.checkpoint(chunk)));
            assertArrayEquals(new byte[]{1},destination.get(new UUID(112,1),new byte[]{2})); assertArrayEquals(new byte[]{2},destination.get(new UUID(112,1),new byte[]{4})); assertArrayEquals(new byte[]{3},destination.get(new UUID(112,1),new byte[]{(byte)200}));
            await(job.release(RequestContext.batch())); await(job.reclaim(RequestContext.batch())); assertTrue(job.snapshot().artifactsReclaimed());
        } finally { offlineScheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }

    @Test void coldServiceRecoveryRetriesLostBuildAtColumnBoundaryAndLostReleaseWithoutRecapture() throws Exception {
        var lane=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,8,8,"column-boundary-recovery");
        var directory=Files.createDirectory(root.resolve("boundary-job")); var path=directory.resolve("export.log"); var artifacts=root.resolve("boundary-artifacts");
        try (var f=new Fixture(root.resolve("boundary-source"),true); var snapshots=snapshots(f)) {
            seed(f); var baseline=definition(f,snapshots,directory.resolve("source.snapshot")); var selected=List.of(f.schema.column(71).columnId(),f.schema.column(72).columnId());
            var id=RatisExportWorkflowTest.identity(f); var home=f.snapshots.route(id.fence()); var plan=RatisSnapshotPlan.create(List.of(f.directory),new TransactionScope(selected.stream().map(column -> TransactionScope.Span.range(column,null,null)).toList()),id.fence(),id.business(),home.groupId(),home.rangeEpoch(),id.firstControl());
            var definition=new RatisExportJournal.Definition(baseline.jobId(),plan.fence(),selected,baseline.sourceJournal(),snapshots.identityDigest(),baseline.limits(),java.time.Duration.ofSeconds(60)); var builds=new java.util.concurrent.atomic.AtomicInteger();
            var builder=new RatisExportSourceRegistry.NativeBuilder() {
                public NativeRangeSstExport.Chunk build(RatisExportSourceRpc.JobDefinition d,RatisExportJournal.ChunkIntent i,Path artifact,RatisExportSourceRegistry.Completion completion) throws Exception { builds.incrementAndGet(); return f.machines.get(d.group()).exportSnapshotPinChunk(d.capture(),pin -> { var chunk=f.catalogue.pool.exportSstChunk(artifact,pin,i.column(),i.scopeStart(),d.scope(i.column()).upperExclusive(),i.limits()); completion.persist(chunk); return chunk; }); }
                public boolean tokenUsable(RatisExportSourceRpc.JobDefinition d) throws Exception { try { f.machines.get(d.group()).requireExportPin(d); return true; } catch (RatisSnapshotLostException lost) { return false; } }
            };
            var service=new java.util.concurrent.atomic.AtomicReference<>(new RatisExportSourceService(artifacts,RatisExportSourceRegistry.Budgets.defaults(),RatisExportSourceService.IoLimits.defaults(),builder,lane));
            var loseBuild=new java.util.concurrent.atomic.AtomicBoolean(true); var loseRelease=new java.util.concurrent.atomic.AtomicBoolean(true); var releases=new ArrayList<byte[]>();
            try (var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); } public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] frame) throws IOException {
                    var packet=ScopedTransactionRpc.parse(frame,ScopedTransactionRpc.MAX_REQUEST_BYTES);return service.get().submit(route.groupId(),packet.kind(),packet.body()).thenApply(reply->{if(packet.kind()==ScopedTransactionRpc.EXPORT_BUILD&&RatisExportSourceRpc.BuildRequest.decode(packet.body()).intent().column().equals(selected.getLast())&&loseBuild.compareAndSet(true,false))throw new java.util.concurrent.CompletionException(new IOException("Lost boundary BUILD reply"));if(packet.kind()==ScopedTransactionRpc.EXPORT_RELEASE){releases.add(packet.body());if(loseRelease.compareAndSet(true,false))throw new java.util.concurrent.CompletionException(new IOException("Lost RELEASE reply"));}return ScopedTransactionRpc.frame(packet.kind(),reply);});
                }
            })) {
                var lease=snapshots.openFrozen(definition.sourceJournal(),definition.fence(),definition.snapshotTtl(),RequestContext.batch()); await(lease.completion()); int captures=f.captureRequests.get(); var group=f.groups.getFirst();
                try (var journal=RatisExportJournal.create(path,definition)) {
                    journal.bindSource(lease.snapshot()); var remote=new RatisRemoteExportSource(snapshots,routed,RatisJournalAccess.standalone()); remote.bind(journal.snapshot());
                    while(journal.snapshot().chunks().isEmpty() || journal.snapshot().chunks().getLast().hasMore()) remoteChunk(journal,remote,group);
                    var intent=journal.freezeChunk(group); assertEquals(selected.getLast(),intent.column()); assertEquals(2,intent.ordinal());
                    assertThrows(ExecutionException.class,() -> await(remote.write(new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(journal.snapshot(),group),intent),journal.artifactPath(intent),RequestContext.batch()))); assertEquals(3,builds.get());
                }
                await(service.get().stop()); service.set(new RatisExportSourceService(artifacts,RatisExportSourceRegistry.Budgets.defaults(),RatisExportSourceService.IoLimits.defaults(),builder,lane));
                try(var journal=RatisExportJournal.reopen(path)) {
                    var remote=new RatisRemoteExportSource(snapshots,routed,RatisJournalAccess.standalone()); remote.bind(journal.snapshot()); remoteChunk(journal,remote,group); assertEquals(3,builds.get()); assertEquals(captures,f.captureRequests.get());
                    while(journal.snapshot().phase()==RatisExportJournal.Phase.EXPORTING) remoteChunk(journal,remote,journal.unfinishedGroup());
                    assertEquals(5,builds.get()); for(var participant:f.groups) assertEquals(1,f.machines.get(participant).snapshotPinStats().pins());
                    await(lease.release(RequestContext.batch()));
                    assertThrows(ExecutionException.class,() -> await(remote.release(RatisExportJournal.sourceDefinition(journal.snapshot(),group),RequestContext.batch()))); assertTrue(journal.snapshot().sourceCleanup().contains(group));
                }
                await(service.get().stop()); service.set(new RatisExportSourceService(artifacts,RatisExportSourceRegistry.Budgets.defaults(),RatisExportSourceService.IoLimits.defaults(),builder,lane));
                try(var journal=RatisExportJournal.reopen(path)) {
                    var remote=new RatisRemoteExportSource(snapshots,routed,RatisJournalAccess.standalone()); remote.bind(journal.snapshot());
                    for(var participant:f.groups) if(journal.snapshot().sourceCleanup().contains(participant)) { await(remote.release(RatisExportJournal.sourceDefinition(journal.snapshot(),participant),RequestContext.batch())); journal.sourceReleased(participant); }
                    journal.markReady(lease.snapshot()); assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); assertEquals(captures,f.captureRequests.get()); assertArrayEquals(releases.getFirst(),releases.get(1));
                }
                lease.close();
            } finally { await(service.get().stop()); }
        } finally { lane.disposeGracefully().block(Duration.ofSeconds(5)); }
    }
    static void remoteChunk(RatisExportJournal journal,RatisRemoteExportSource remote,UUID group) throws Exception {
        var intent=journal.freezeChunk(group); var request=new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(journal.snapshot(),group),intent); journal.publish(intent,await(remote.write(request,journal.artifactPath(intent),RequestContext.batch())));
    }

    static final class Destination implements AutoCloseable {
        final DBOptions options; final ColumnFamilyOptions columnOptions=new ColumnFamilyOptions(); final List<ColumnFamilyHandle> handles=new ArrayList<>(); final Map<UUID,ColumnFamilyHandle> columns=new HashMap<>(); final RocksDB db;
        Destination(Path path,boolean create,List<UUID> selection) throws Exception {
            options=new DBOptions().setCreateIfMissing(create).setCreateMissingColumnFamilies(create); var descriptors=new ArrayList<ColumnFamilyDescriptor>(); descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,columnOptions));
            for (var column:selection) descriptors.add(new ColumnFamilyDescriptor(column.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8),columnOptions));
            db=RocksDB.open(options,path.toString(),descriptors,handles); for(int n=0;n<selection.size();n++) columns.put(selection.get(n),handles.get(n+1));
        }
        byte[] get(UUID column,byte[] key) throws Exception { return db.get(columns.get(column),key); }
        void checkpoint(RatisExportJournal.Chunk chunk) throws Exception { try(var sync=new WriteOptions().setSync(true)) { db.put(sync,RatisExportWorkflowTest.checkpoint(chunk),chunk.sha256()); } }
        public void close() { for(var handle:handles) handle.close(); db.close(); columnOptions.close(); options.close(); }
    }

}
