package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisDatabaseBackupArchiveTest {
    static { RocksDB.loadLibrary(); }
    @TempDir Path root;
    static final class Source implements AutoCloseable {
        final DBOptions options=new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions columns=new ColumnFamilyOptions();
        final List<ColumnFamilyHandle> handles=new ArrayList<>();final RocksDB db;final ReplicatedRangeStore store;
        Source(Path path,RatisDatabaseBackupFixture f)throws Exception{
            db=RocksDB.open(options,path.toString(),List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,columns),new ColumnFamilyDescriptor(new byte[]{1},columns),new ColumnFamilyDescriptor(new byte[]{2},columns),new ColumnFamilyDescriptor(new byte[]{3},columns)),handles);
            var range=f.directory.ranges().getFirst();var ownership=new RangeOwnership();var owner=new RangeOwnership.Owner(range.groupId(),1);ownership.register(owner,new RangeOwnership.Interval(range.groupId(),Set.of(f.column),null,null),RangeOwnership.State.ACTIVE);
            store=new ReplicatedRangeStore(db,ownership,owner,f.schema.digest(),Map.of(f.column,handles.get(1)),handles.get(2),handles.get(3),Set.of());
            store.applyNoop(new ApplyPosition(1,0));store.applyNoop(new ApplyPosition(1,1));store.applyNoop(new ApplyPosition(1,2));store.applyNoop(new ApplyPosition(1,3));store.apply(f.cut,new DistributedCommand(range.groupId(),1,f.schema.digest(),new OperationId(f.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{7},new byte[]{9}),new Mutation(Mutation.Kind.PUT,f.column,new byte[]{8},new byte[]{10}))));
        }
        NativeRangeSstExport.Manifest export(Path path,RatisDatabaseBackupFixture f)throws Exception{try(var snapshot=store.capture(new TransactionScope(List.of(TransactionScope.Span.range(f.column,null,null))))){return NativeRangeSstExport.write(path,snapshot,f.column,null,null,options,columns,new NativeRangeSstExport.Limits(10,1024,1024*1024,Duration.ofSeconds(10)));}}
        @Override public void close(){handles.forEach(ColumnFamilyHandle::close);db.close();columns.close();options.close();}
    }
    @Test void actualRetainedExportCopiedArchiveSurvivesSourceDeletionAndPlansFreshUserImport() throws Exception {
        var f=new RatisDatabaseBackupFixture(1,2*1024*1024);Path sourceRoot=root.resolve("source"),archiveRoot=root.resolve("archive");Files.createDirectories(sourceRoot);
        NativeRangeSstExport.Manifest exported;DatabaseBackupManifest saved;Path copied;
        try(var source=new Source(sourceRoot.resolve("db"),f);var archive=new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination)){
            exported=source.export(sourceRoot.resolve("user.sst"),f);var chunk=archive.copy(f.domain,0,UUID.randomUUID(),exported);saved=f.manifest(List.of(chunk));archive.complete(saved);copied=archive.objectPath(chunk.objectId(),false);assertFalse(Files.isSameFile(exported.file(),copied));
        }
        // Only fixture-owned source paths are deleted, after every native resource is closed.
        try(var paths=Files.walk(sourceRoot)){for(var path:paths.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}
        assertFalse(Files.exists(exported.file()));
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination)){
            var complete=archive.reopenComplete();assertArrayEquals(saved.encode(),complete.encode());
            var plan=RatisDatabaseRestorePlanner.plan(complete,UUID.randomUUID(),List.of(f.fresh()),UUID.randomUUID(),0);
            var manifest=plan.domains().getFirst().imports().getFirst().manifests().getFirst();var file=manifest.columns().getFirst().files().getFirst();assertArrayEquals(exported.sha256(),file.sha256());assertEquals(exported.fileBytes(),file.bytes());assertNotEquals(exported.groupId(),manifest.groupId());assertEquals(0,manifest.rangeEpoch());
            try(var options=new Options().setCreateIfMissing(true);var restored=RocksDB.open(options,root.resolve("restored-user-check").toString());var ingest=new IngestExternalFileOptions().setMoveFiles(false)){
                restored.ingestExternalFile(List.of(archive.objectPath(file.fileId(),false).toString()),ingest);assertArrayEquals(new byte[]{9},restored.get(new byte[]{7}));assertArrayEquals(new byte[]{10},restored.get(new byte[]{8}));
            }
        }
    }
    @Test void destinationQuotaRejectsBeforeCopyAndCompleteRejectsChangedOrMissingBytes()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1);Path sourceRoot=root.resolve("source");Files.createDirectories(sourceRoot);
        try(var source=new Source(sourceRoot.resolve("db"),f);var archive=new RatisDatabaseBackupArchive(root.resolve("quota"),f.definition,f.destination)){
            var exported=source.export(sourceRoot.resolve("user.sst"),f);assertThrows(IOException.class,()->archive.copy(f.domain,0,UUID.randomUUID(),exported));
            try(var paths=Files.list(root.resolve("quota"))){assertTrue(paths.noneMatch(p->p.getFileName().toString().endsWith(".sst") || p.getFileName().toString().endsWith(".pending")));}
        }
        var real=new RatisDatabaseBackupFixture(1,2*1024*1024);Path second=root.resolve("second");Files.createDirectories(second);
        try(var source=new Source(second.resolve("db"),real);var archive=new RatisDatabaseBackupArchive(root.resolve("corrupt"),real.definition,real.destination)){
            var chunk=archive.copy(real.domain,0,UUID.randomUUID(),source.export(second.resolve("user.sst"),real));var manifest=real.manifest(List.of(chunk));Files.write(archive.objectPath(chunk.objectId(),false),new byte[]{1},StandardOpenOption.TRUNCATE_EXISTING);assertThrows(IOException.class,()->archive.complete(manifest));assertFalse(Files.exists(root.resolve("corrupt/COMPLETE")));
        }
    }
    @Test void foreignDestinationIdentityFilesAndConcurrentOwnersAreRejected()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1024);Path archiveRoot=root.resolve("archive");
        try(var archive=new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination)){
            assertThrows(Exception.class,()->new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination));
            assertThrows(IOException.class,()->archive.complete(new DatabaseBackupManifest(f.definition,new DatabaseBackupManifest.Destination(UUID.randomUUID(),UUID.randomUUID()),f.held,List.of(f.cohort),f.emptyChunks(),List.of())));
        }
        assertThrows(IOException.class,()->new RatisDatabaseBackupArchive(archiveRoot,f.definition,new DatabaseBackupManifest.Destination(UUID.randomUUID(),UUID.randomUUID())));
        Files.write(archiveRoot.resolve("foreign"),new byte[]{1});assertThrows(IOException.class,()->new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination));
    }
    @Test void knownCopyFailureRemovesOnlyItsPendingFileAndColdExactRetrySucceeds()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,2*1024*1024);Path sourceRoot=root.resolve("failed-source"),archiveRoot=root.resolve("retry");Files.createDirectories(sourceRoot);UUID object=UUID.randomUUID();
        try(var source=new Source(sourceRoot.resolve("db"),f)){
            var exported=source.export(sourceRoot.resolve("user.sst"),f);byte[] wrong=exported.sha256();wrong[0]^=1;
            var bad=new NativeRangeSstExport.Manifest(exported.groupId(),exported.rangeEpoch(),exported.columnId(),exported.schemaDigest(),exported.position(),null,null,exported.rows(),exported.payloadBytes(),exported.fileBytes(),wrong,exported.file());
            try(var archive=new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination)){assertThrows(IOException.class,()->archive.copy(f.domain,0,object,bad));assertFalse(Files.exists(archiveRoot.resolve(object+".sst.pending")));}
            try(var archive=new RatisDatabaseBackupArchive(archiveRoot,f.definition,f.destination)){var chunk=archive.copy(f.domain,0,object,exported);archive.complete(f.manifest(List.of(chunk)));assertArrayEquals(exported.sha256(),archive.reopenComplete().chunks().getFirst().sha256());}
        }
    }

    @Test void boundedScalarArchiveRejectsMergeAndOversizedPublicRowsWithoutComplete() throws Exception {
        for(boolean merge:new boolean[]{true,false}) {
            var fixture=new RatisDatabaseBackupFixture(1,8*1024*1024);Path source=root.resolve("invalid-"+merge+".sst");
            int valueBytes=merge?1:DistributedCommandCodec.MAX_ENCODED_BYTES+1;
            try(var env=new EnvOptions();var operator=new StringAppendOperator();var options=new Options().setMergeOperator(operator);var writer=new SstFileWriter(env,options)) {
                writer.open(source.toString());if(merge)writer.merge(new byte[]{1},new byte[]{2});else writer.put(new byte[]{1},new byte[valueBytes]);writer.finish();
            }
            var range=fixture.directory.ranges().getFirst();
            var manifest=new NativeRangeSstExport.Manifest(range.groupId(),range.rangeEpoch(),fixture.column,fixture.schema.digest(),fixture.cut,null,null,1,valueBytes+1L,Files.size(source),java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(source)),source);
            Path destination=root.resolve("invalid-archive-"+merge);
            try(var archive=new RatisDatabaseBackupArchive(destination,fixture.definition,fixture.destination)) {
                assertThrows(IOException.class,()->archive.copy(fixture.domain,0,UUID.randomUUID(),manifest));
                assertFalse(Files.exists(destination.resolve("COMPLETE")));
            }
        }
    }

}
