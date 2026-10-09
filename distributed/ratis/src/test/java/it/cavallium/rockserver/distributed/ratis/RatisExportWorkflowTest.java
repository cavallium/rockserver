package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisRocksDBAdapterTest.*;

/** Actual source capture, native SST artifacts, durable destination checkpoint and explicit export ACK. */
class RatisExportWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static <T> T await(CompletableFuture<T> result) throws Exception { return result.get(10,TimeUnit.SECONDS); }
    static RatisExportJournal.Definition definition(RatisAtomicClientTest.Fixture f,Path source) throws Exception {
        var identity=identity(f); var column=f.schema.column(71).columnId();
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))); var home=f.snapshots.route(identity.fence());
        var plan=RatisSnapshotPlan.create(f.directory,scope,identity.fence(),identity.business(),home.groupId(),home.rangeEpoch(),identity.firstControl());
        var chunk=new it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport.Limits(1,1024,1024*1024,Duration.ofSeconds(5));
        return new RatisExportJournal.Definition(new UUID(250,1),plan.fence(),column,source,MessageDigest.getInstance("SHA-256").digest(plan.fence().encode()),new RatisExportJournal.Limits(16,16*1024*1024,16*1024*1024,chunk));
    }
    static RatisSnapshotClient.Identity identity(RatisAtomicClientTest.Fixture f) { return new RatisSnapshotClient.Identity(new UUID(250,2),new OperationId(f.session,1,0),new OperationId(f.session,1,1)); }
    static RatisExportJournal.Chunk export(RatisExportJournal journal,RatisAtomicClientTest.Fixture f,UUID group) throws Exception {
        var definition=journal.snapshot().definition(); var span=definition.fence().participant(group).scope().spans().stream().filter(s -> s.columnId().equals(definition.column())).findFirst().orElseThrow();
        var token=journal.snapshot().captures().get(group);
        return journal.export(group,(artifact,intent) -> f.machines.get(group).exportSnapshotPinChunk(token,pin -> f.catalogue.pool.exportSstChunk(artifact,pin,definition.column(),intent.scopeStart(),span.upperExclusive(),intent.limits())));
    }
    static byte[] checkpoint(RatisExportJournal.Chunk chunk) {
        return java.nio.ByteBuffer.allocate(65).put((byte)0).putLong(chunk.jobId().getMostSignificantBits()).putLong(chunk.jobId().getLeastSignificantBits())
                .putLong(chunk.group().getMostSignificantBits()).putLong(chunk.group().getLeastSignificantBits()).putLong(chunk.rangeEpoch())
                .putLong(chunk.cut().term()).putLong(chunk.cut().index()).putLong(chunk.ordinal()).array();
    }
    static void destination(RocksDB db,RatisExportJournal.Chunk chunk,Path downloads) throws Exception {
        Path received=chunk.file()==null ? null : Files.createTempFile(Files.createDirectories(downloads),"received-export-",".sst");
        try (var ingest=new IngestExternalFileOptions().setMoveFiles(true); var flush=new FlushOptions().setWaitForFlush(true); var sync=new WriteOptions().setSync(true)) {
            if (received!=null) { Files.copy(chunk.file(),received,StandardCopyOption.REPLACE_EXISTING);
                assertArrayEquals(chunk.sha256(),MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(received)));
                db.ingestExternalFile(List.of(received.toString()),ingest); }
            db.flush(flush);
            db.put(sync,checkpoint(chunk),chunk.sha256());
        } finally { if (received!=null) Files.deleteIfExists(received); }
    }

    @Test void coldReadyReplayPreservesOldCutAndWritesDestinationBeforeAcknowledgement() throws Exception {
        Path directory=Files.createDirectories(root.resolve("job")),source=directory.resolve("source.snapshot"),journalFile=directory.resolve("export.log"),target=root.resolve("destination");
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("source"),true); var snapshots=new RatisSnapshotClient(f.schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults()); var journal=RatisExportJournal.create(journalFile,definition(f,source))) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(4),value(2),1901); seed(f,71,key(200),value(3),1902);
            var lease=snapshots.open(source,identity(f),RatisSnapshotPlan.scope(journal.snapshot().definition().fence()),Duration.ofSeconds(30),RequestContext.batch()); await(lease.completion()); journal.bindSource(lease.snapshot()); int captureRequests=f.captureRequests.get();
            seed(f,71,key(4),value(9),1903);
            for (var participant:journal.snapshot().definition().fence().participants()) {
                RatisExportJournal.Chunk chunk; do { chunk=export(journal,f,participant.groupId()); } while (chunk.hasMore());
            }
            assertEquals(RatisExportJournal.Phase.MATERIALIZED,journal.snapshot().phase()); assertEquals(3,journal.snapshot().chunks().size());
            await(lease.release(RequestContext.batch())); for (var group:journal.snapshot().sourceCleanup()) journal.sourceReleased(group); journal.markReady(lease.snapshot()); lease.close();
            assertEquals(RatisExportJournal.Phase.READY,journal.snapshot().phase()); assertEquals(captureRequests,f.captureRequests.get()); assertEquals(0,f.opens.get());
            for (var machine:f.machines.values()) assertEquals(0,machine.snapshotPinStats().pins());
            try (var options=new Options().setCreateIfMissing(true); var db=RocksDB.open(options,target.toString())) {
                destination(db,journal.snapshot().chunks().getFirst(),root.resolve("downloads")); assertTrue(journal.snapshot().acknowledged().isEmpty(),"Durable destination completion does not implicitly acknowledge a source chunk");
            }
        }
        Files.delete(source); // READY replay must not consult the closed source or its original snapshot journal.
        try (var journal=RatisExportJournal.reopen(journalFile); var options=new Options().setCreateIfMissing(false); var db=RocksDB.open(options,target.toString())) {
            assertTrue(journal.snapshot().acknowledged().isEmpty()); var first=journal.snapshot().chunks().getFirst();
            assertArrayEquals(first.sha256(),db.get(checkpoint(first))); journal.acknowledge(first); journal.acknowledge(first);
            for (var chunk:journal.snapshot().chunks().subList(1,journal.snapshot().chunks().size())) { destination(db,chunk,root.resolve("downloads")); journal.acknowledge(chunk); }
            assertArrayEquals(new byte[]{1},db.get(new byte[]{2})); assertArrayEquals(new byte[]{2},db.get(new byte[]{4})); assertArrayEquals(new byte[]{3},db.get(new byte[]{(byte)200}));
            assertEquals(3,journal.snapshot().acknowledged().size()); journal.release(); assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase());
        }
        try (var journal=RatisExportJournal.reopen(journalFile)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); }
    }

    @Test void lossOfAnUnfinishedOriginalPinFencesTheJobWithoutRecapturing() throws Exception {
        Path directory=Files.createDirectories(root.resolve("lost-job")),source=directory.resolve("source.snapshot"),journalFile=directory.resolve("export.log");
        try (var f=new RatisAtomicClientTest.Fixture(root.resolve("lost-source"),true); var snapshots=new RatisSnapshotClient(f.schema,f.snapshots,f.data,f.scheduler,RatisSnapshotClient.Limits.defaults()); var journal=RatisExportJournal.create(journalFile,definition(f,source))) {
            seed(f,71,key(2),value(1),1900); seed(f,71,key(4),value(2),1901); seed(f,71,key(200),value(3),1902);
            var lease=snapshots.open(source,identity(f),RatisSnapshotPlan.scope(journal.snapshot().definition().fence()),Duration.ofSeconds(30),RequestContext.batch()); await(lease.completion()); journal.bindSource(lease.snapshot()); int captureRequests=f.captureRequests.get();
            UUID group=f.groups.getFirst(); assertTrue(export(journal,f,group).hasMore()); f.machines.get(group).releaseSnapshotPin(journal.snapshot().captures().get(group));
            assertThrows(RatisSnapshotLostException.class,() -> export(journal,f,group)); journal.failSnapshotLost();
            assertEquals(RatisExportJournal.Phase.SNAPSHOT_LOST,journal.snapshot().phase()); assertEquals(1,journal.snapshot().chunks().size()); assertEquals(captureRequests,f.captureRequests.get()); assertEquals(0,f.opens.get());
            assertThrows(java.io.IOException.class,() -> journal.freezeChunk(group));
            await(lease.release(RequestContext.batch())); lease.close(); journal.release();
        }
        try (var journal=RatisExportJournal.reopen(journalFile)) { assertEquals(RatisExportJournal.Phase.RELEASED,journal.snapshot().phase()); assertEquals(1,journal.snapshot().chunks().size()); }
    }
}
