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
        public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] frame) throws IOException {
            assertEquals(route.replicas().getFirst(),owner);
            var request=ScopedTransactionRpc.parse(frame,ScopedTransactionRpc.MAX_REQUEST_BYTES); int kind=request.kind();
            try {
                if (kind==ScopedTransactionRpc.EXPORT_BUILD) {
                    var build=RatisExportSourceRpc.BuildRequest.decode(request.body()); var d=build.definition(); var i=build.intent(); String key=key(d,i.ordinal());
                    var descriptor=descriptors.get(key);
                    if (descriptor==null) {
                        var file=artifacts.resolve(key+".sst");
                        var chunk=fixture.machines.get(d.group()).exportSnapshotPinChunk(d.capture(),pin -> fixture.catalogue.pool.exportSstChunk(file,pin,d.column(),i.scopeStart(),d.scope().upperExclusive(),i.limits()));
                        var m=chunk.manifest(); descriptor=new RatisExportSourceRpc.Descriptor(d.digest(),i.ordinal(),m.groupId(),m.rangeEpoch(),m.schemaDigest(),m.columnId(),m.position(),m.lowerInclusive(),m.upperExclusive(),m.rows()==0 ? new byte[32] : m.sha256(),m.rows(),m.payloadBytes(),m.fileBytes(),chunk.hasMore());
                        descriptors.put(key,descriptor); files.put(key,m.file()); builds++;
                    }
                    if (loseBuild) { loseBuild=false; throw new IOException("Lost BUILD reply after durable artifact"); }
                    return ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.BuildReply(build,descriptor).encode());
                }
                if (kind==ScopedTransactionRpc.EXPORT_READ) {
                    var read=RatisExportSourceRpc.ReadRequest.decode(request.body()); String key=key(read.definition(),read.ordinal());
                    assertArrayEquals(descriptors.get(key).digest(),read.descriptorDigest()); assertTrue(read.maxBytes()<=1024*1024);
                    var output=ByteBuffer.allocate(read.maxBytes()); try (var input=FileChannel.open(files.get(key),StandardOpenOption.READ)) { input.position(read.offset()); while(output.hasRemaining() && input.read(output)>=0) {} }
                    byte[] bytes=Arrays.copyOf(output.array(),output.position()); if (truncate) { truncate=false; bytes=Arrays.copyOf(bytes,bytes.length-1); }
                    return ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.ReadReply(read,bytes).encode());
                }
                if (kind==ScopedTransactionRpc.EXPORT_RELEASE) {
                    var release=RatisExportSourceRpc.ReleaseRequest.decode(request.body()); if (failRelease) { failRelease=false; throw new IOException("Controlled RELEASE rejection"); }
                    releases++; return ScopedTransactionRpc.frame(kind,new RatisExportSourceRpc.ReleaseReply(release).encode());
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
                var remote=new RatisRemoteExportSource(source,routed); UUID group;
                try (var journal=RatisExportJournal.create(path,definition)) {
                    var lease=source.openFrozen(definition.sourceJournal(),definition.fence(),Duration.ofSeconds(30),RequestContext.batch()); await(lease.completion()); journal.bindSource(lease.snapshot()); remote.bind(journal.snapshot());
                    var saved=journal.snapshot(); var alteredLimits=new RatisExportJournal.Limits(definition.limits().maximumChunks()+1,definition.limits().maximumJournalBytes(),definition.limits().maximumDiskBytes(),definition.limits().chunk());
                    var changed=new RatisExportJournal.Definition(definition.jobId(),definition.fence(),definition.column(),definition.sourceJournal(),definition.sourceIdentity(),alteredLimits);
                    assertThrows(IOException.class,() -> remote.bind(new RatisExportJournal.Snapshot(changed,saved.phase(),saved.captures(),saved.owners(),saved.sourceCleanup(),saved.pending(),saved.chunks(),saved.acknowledged(),saved.pendingArtifactBytes(),saved.reclaiming(),saved.artifactsReclaimed())));
                    group=f.groups.getFirst(); var intent=journal.freezeChunk(group); var request=new RatisExportSourceRpc.BuildRequest(RatisExportJournal.sourceDefinition(journal.snapshot(),group),intent);
                    assertThrows(ExecutionException.class,() -> await(remote.write(request,journal.artifactPath(intent),RequestContext.batch()))); assertEquals(1,proxy.builds); assertFalse(Files.exists(journal.artifactPath(intent)));
                    f.machines.get(group).releaseSnapshotPin(journal.snapshot().captures().get(group)); lease.close();
                }
                int captures=f.captureRequests.get();
                try (var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed),RatisExportClient.Limits.defaults()); var job=client.reopen(path,RequestContext.batch())) {
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
            try (var routed=new RatisRoutedClient(f.directory,proxy); var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed),RatisExportClient.Limits.defaults()); var job=client.create(directory.resolve("export.log"),definition,RequestContext.batch())) {
                await(job.completion()); await(job.step(RequestContext.batch())); var captures=job.snapshot().captures();
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); var pending=job.snapshot().pending(); assertNotNull(pending); assertTrue(job.snapshot().chunks().isEmpty()); assertEquals(0,proxy.releases);
                try (var files=Files.list(directory)) { assertFalse(files.anyMatch(file -> file.getFileName().toString().endsWith(".sst") || file.getFileName().toString().endsWith(".manifest"))); }
                await(job.step(RequestContext.batch())); assertEquals(1,proxy.builds); assertEquals(captures,job.snapshot().captures()); assertEquals(1,job.snapshot().chunks().size()); assertEquals(0,proxy.releases);
                await(job.cancel(RequestContext.batch())); assertTrue(job.snapshot().sourceCleanup().isEmpty());
            }
        }
    }
    @Test void completeNativeServiceWorkflowRetriesLostReleaseThenDeliversAcknowledgesAndReopensOffline() throws Exception {
        var sourceScheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,8,8,"remote-export-service");
        var directory=Files.createDirectory(root.resolve("complete-job")); var path=directory.resolve("export.log"); var destinationPath=root.resolve("destination"); List<RatisExportJournal.Chunk> chunks;
        try (var f=new Fixture(root.resolve("complete-source"),true); var source=snapshots(f)) {
            seed(f); var definition=definition(f,source,directory.resolve("source.snapshot")); var builds=new java.util.concurrent.atomic.AtomicInteger();
            var builder=new RatisExportSourceRegistry.NativeBuilder() {
                public NativeRangeSstExport.Chunk build(RatisExportSourceRpc.JobDefinition d,RatisExportJournal.ChunkIntent intent,Path artifact,RatisExportSourceRegistry.Completion completion) throws Exception {
                    var machine=f.machines.get(d.group()); machine.requireExportPin(d); builds.incrementAndGet();
                    return machine.exportSnapshotPinChunk(d.capture(),pin -> { var chunk=f.catalogue.pool.exportSstChunk(artifact,pin,d.column(),intent.scopeStart(),d.scope().upperExclusive(),intent.limits()); completion.persist(chunk); return chunk; });
                }
                public boolean tokenUsable(RatisExportSourceRpc.JobDefinition d) throws Exception { try { f.machines.get(d.group()).requireExportPin(d); return true; } catch (RatisSnapshotLostException lost) { return false; } }
            };
            var service=new RatisExportSourceService(root.resolve("service-artifacts"),RatisExportSourceRegistry.Budgets.defaults(),builder,sourceScheduler);
            var releases=new ArrayList<byte[]>(); var loseRelease=new java.util.concurrent.atomic.AtomicBoolean(true);
            try (var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] frame) throws IOException {
                    assertEquals(route.replicas().getFirst(),owner); var packet=ScopedTransactionRpc.parse(frame,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    try {
                        if (packet.kind()==ScopedTransactionRpc.EXPORT_RELEASE) {
                            for (var group:f.groups) assertEquals(0,f.machines.get(group).snapshotPinStats().pins());
                            releases.add(packet.body());
                        }
                        var reply=await(service.submit(route.groupId(),packet.kind(),packet.body()));
                        if (packet.kind()==ScopedTransactionRpc.EXPORT_RELEASE && loseRelease.compareAndSet(true,false)) throw new IOException("Lost RELEASE reply after durable cleanup");
                        return ScopedTransactionRpc.frame(packet.kind(),reply);
                    } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
                }
            }); var client=new RatisExportClient(source,f.scheduler,new RatisRemoteExportSource(source,routed),RatisExportClient.Limits.defaults()); var job=client.create(path,definition,RequestContext.batch()); var options=new Options().setCreateIfMissing(true); var destination=RocksDB.open(options,destinationPath.toString())) {
                await(job.completion()); await(job.step(RequestContext.batch())); int captures=f.captureRequests.get();
                RatisRocksDBAdapterTest.seed(f,71,key(4),value(9),1903);
                while (job.snapshot().phase()==RatisExportJournal.Phase.EXPORTING) await(job.step(RequestContext.batch()));
                assertEquals(RatisExportJournal.Phase.MATERIALIZED,job.snapshot().phase()); assertEquals(3,builds.get()); assertTrue(releases.isEmpty());
                assertThrows(ExecutionException.class,() -> await(job.step(RequestContext.batch()))); assertEquals(RatisExportJournal.Phase.MATERIALIZED,job.snapshot().phase()); assertFalse(job.snapshot().sourceCleanup().isEmpty());
                await(job.step(RequestContext.batch())); assertEquals(RatisExportJournal.Phase.READY,job.snapshot().phase()); assertTrue(job.snapshot().sourceCleanup().isEmpty()); assertEquals(captures,f.captureRequests.get()); assertEquals(3,releases.size()); assertArrayEquals(releases.get(0),releases.get(1));
                chunks=job.snapshot().chunks(); assertEquals(2,chunks.stream().map(RatisExportJournal.Chunk::group).distinct().count());
                var manifest=await(job.manifest(0,128,65536,RequestContext.batch())); assertEquals(chunks,manifest.chunks()); assertFalse(manifest.hasMore());
                for (var chunk:chunks) {
                    Path received=root.resolve("complete-received-"+chunk.group()+"-"+chunk.ordinal()+".sst");
                    try (var output=Files.newOutputStream(received,StandardOpenOption.CREATE_NEW)) { long offset=0; while (true) { var page=await(job.read(chunk,offset,65536,RequestContext.batch())); output.write(page.bytes()); offset+=page.bytes().length; if (!page.hasMore()) break; } }
                    assertArrayEquals(chunk.sha256(),java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(received)));
                    try (var ingest=new IngestExternalFileOptions().setMoveFiles(false); var flush=new FlushOptions().setWaitForFlush(true); var sync=new WriteOptions().setSync(true)) { destination.ingestExternalFile(List.of(received.toString()),ingest); destination.flush(flush); destination.put(sync,RatisExportWorkflowTest.checkpoint(chunk),chunk.sha256()); }
                    await(job.acknowledge(chunk,RequestContext.batch()));
                }
                assertArrayEquals(new byte[]{2},destination.get(new byte[]{4})); assertEquals(chunks.size(),job.snapshot().acknowledged().size());
            } finally { await(service.stop()); }
        } finally { sourceScheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
        var offlineScheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"remote-export-offline");
        try (var client=new RatisExportClient(null,offlineScheduler,null,RatisExportClient.Limits.defaults()); var job=client.reopen(path,RequestContext.batch()); var options=new Options().setCreateIfMissing(false); var destination=RocksDB.open(options,destinationPath.toString())) {
            await(job.completion()); assertEquals(RatisExportJournal.Phase.READY,job.snapshot().phase()); assertEquals(chunks.size(),job.snapshot().acknowledged().size());
            for (var chunk:chunks) assertArrayEquals(chunk.sha256(),destination.get(RatisExportWorkflowTest.checkpoint(chunk)));
            assertArrayEquals(new byte[]{1},destination.get(new byte[]{2})); assertArrayEquals(new byte[]{2},destination.get(new byte[]{4})); assertArrayEquals(new byte[]{3},destination.get(new byte[]{(byte)200}));
            await(job.release(RequestContext.batch())); await(job.reclaim(RequestContext.batch())); assertTrue(job.snapshot().artifactsReclaimed());
        } finally { offlineScheduler.disposeGracefully().block(Duration.ofSeconds(5)); }
    }

}
