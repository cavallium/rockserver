package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real materialized SST chunks from retained native cuts on the existing XFS test directory. */
class NativeRangeSstExportChunkTest {
    static { RocksDB.loadLibrary(); }
    @TempDir(factory=DiskTestDirectory.class) Path root;
    static final UUID GROUP=new UUID(140,1),MERGED=new UUID(140,2),PLAIN=new UUID(140,3),SESSION=new UUID(140,4);
    static byte[] bytes(String value) { return value.getBytes(StandardCharsets.UTF_8); }
    static NativeRangeSstExport.Limits limits(long rows,long payload) { return new NativeRangeSstExport.Limits(rows,payload,1024*1024,Duration.ofSeconds(10)); }
    static DistributedCommand command(long sequence,Mutation... mutations) { return new DistributedCommand(GROUP,1,new byte[32],new OperationId(SESSION,1,sequence),List.of(mutations)); }
    static final class Fixture implements AutoCloseable {
        final DBOptions options=new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        final ColumnFamilyOptions plain=new ColumnFamilyOptions(),merged=new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","));
        final List<ColumnFamilyHandle> handles=new ArrayList<>(); final RocksDB db; final ReplicatedRangeStore store;
        Fixture(Path path) throws Exception {
            var descriptors=List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,plain),new ColumnFamilyDescriptor(bytes("merged"),merged),new ColumnFamilyDescriptor(bytes("plain"),plain),new ColumnFamilyDescriptor(bytes("state"),plain),new ColumnFamilyDescriptor(bytes("receipts"),plain));
            db=RocksDB.open(options,path.toString(),descriptors,handles);
            var guard=new RangeOwnership(); var owner=new RangeOwnership.Owner(GROUP,1); guard.register(owner,new RangeOwnership.Interval(GROUP,Set.of(MERGED,PLAIN),null,null),RangeOwnership.State.ACTIVE);
            store=new ReplicatedRangeStore(db,guard,owner,new byte[32],Map.of(MERGED,handles.get(1),PLAIN,handles.get(2)),handles.get(3),handles.get(4),Set.of(MERGED));
            store.applyNoop(new ApplyPosition(1,0));
        }
        @Override public void close() { for (var handle:handles) handle.close(); db.close(); merged.close(); plain.close(); options.close(); }
    }
    @Test void exactChunkBoundariesPreserveOldMergedValuesAndDeletedRowsWithoutInventingSuccessors() throws Exception {
        try (var source=new Fixture(root.resolve("source")); var target=new Fixture(root.resolve("target"))) {
            source.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,MERGED,bytes("a"),bytes("old-a")),new Mutation(Mutation.Kind.MERGE,MERGED,bytes("b"),bytes("one")),new Mutation(Mutation.Kind.MERGE,MERGED,bytes("b"),bytes("two")),new Mutation(Mutation.Kind.PUT,MERGED,bytes("c"),bytes("deleted")),new Mutation(Mutation.Kind.DELETE,MERGED,bytes("c"),null),new Mutation(Mutation.Kind.PUT,MERGED,bytes("d"),bytes("old-d")),new Mutation(Mutation.Kind.PUT,MERGED,bytes("e"),bytes("old-e"))));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(MERGED,bytes("a"),bytes("f"))));
            try (var snapshot=source.store.capture(scope)) {
                source.store.apply(new ApplyPosition(1,2),command(1,new Mutation(Mutation.Kind.PUT,MERGED,bytes("a"),bytes("new-a")),new Mutation(Mutation.Kind.MERGE,MERGED,bytes("b"),bytes("later")),new Mutation(Mutation.Kind.PUT,MERGED,bytes("c"),bytes("new-c")),new Mutation(Mutation.Kind.DELETE,MERGED,bytes("d"),null)));
                var first=NativeRangeSstExport.writeChunk(root.resolve("first.sst"),snapshot,MERGED,bytes("a"),bytes("f"),source.options,source.merged,limits(2,1024));
                assertTrue(first.hasMore()); assertEquals(2,first.manifest().rows()); assertArrayEquals(bytes("d"),first.manifest().upperExclusive());
                var last=NativeRangeSstExport.writeChunk(root.resolve("last.sst"),snapshot,MERGED,first.manifest().upperExclusive(),bytes("f"),source.options,source.merged,limits(2,1024));
                assertFalse(last.hasMore()); assertEquals(2,last.manifest().rows()); assertArrayEquals(bytes("d"),last.manifest().lowerInclusive()); assertArrayEquals(bytes("f"),last.manifest().upperExclusive());
                for (var chunk:List.of(first,last)) {
                    var manifest=chunk.manifest(); assertEquals(new ApplyPosition(1,1),manifest.position()); assertEquals(GROUP,manifest.groupId()); assertEquals(1,manifest.rangeEpoch()); assertArrayEquals(new byte[32],manifest.schemaDigest()); assertEquals(Files.size(manifest.file()),manifest.fileBytes()); assertEquals(32,manifest.sha256().length); assertFalse(Files.exists(NativeRangeSstExport.buildingPath(manifest.file())));
                    try (var ingest=new IngestExternalFileOptions().setMoveFiles(false)) { target.db.ingestExternalFile(target.handles.get(1),List.of(manifest.file().toString()),ingest); }
                }
                assertArrayEquals(bytes("old-a"),target.db.get(target.handles.get(1),bytes("a"))); assertArrayEquals(bytes("one,two"),target.db.get(target.handles.get(1),bytes("b"))); assertNull(target.db.get(target.handles.get(1),bytes("c"))); assertArrayEquals(bytes("old-d"),target.db.get(target.handles.get(1),bytes("d")));
                target.db.merge(target.handles.get(1),bytes("b"),bytes("three")); assertArrayEquals(bytes("one,two,three"),target.db.get(target.handles.get(1),bytes("b")));
                assertEquals(1,source.store.activeSnapshotPins(),"Only the caller's original snapshot remains owned after publication");
            }
            assertEquals(0,source.store.activeSnapshotPins());
        }
    }
    @Test void payloadChunksAreBoundedAndEmptyCoverageRetainsTheDeclaredFinalUpper() throws Exception {
        try (var source=new Fixture(root.resolve("payload"))) {
            source.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,PLAIN,bytes("a"),bytes("12")),new Mutation(Mutation.Kind.PUT,PLAIN,bytes("b"),bytes("34")),new Mutation(Mutation.Kind.PUT,PLAIN,bytes("d"),bytes("56"))));
            try (var snapshot=source.store.capture(new TransactionScope(List.of(TransactionScope.Span.range(PLAIN,null,null))))) {
                var first=NativeRangeSstExport.writeChunk(root.resolve("payload-first.sst"),snapshot,PLAIN,bytes("a"),bytes("z"),source.options,source.plain,limits(100,6));
                assertTrue(first.hasMore()); assertEquals(6,first.manifest().payloadBytes()); assertEquals(2,first.manifest().rows()); assertArrayEquals(bytes("d"),first.manifest().upperExclusive());
                var last=NativeRangeSstExport.writeChunk(root.resolve("payload-last.sst"),snapshot,PLAIN,bytes("d"),bytes("z"),source.options,source.plain,limits(100,6));
                assertFalse(last.hasMore()); assertArrayEquals(bytes("z"),last.manifest().upperExclusive()); assertEquals(1,last.manifest().rows());
                var empty=NativeRangeSstExport.writeChunk(root.resolve("empty.sst"),snapshot,PLAIN,bytes("z"),null,source.options,source.plain,limits(1,6));
                assertFalse(empty.hasMore()); assertEquals(0,empty.manifest().rows()); assertArrayEquals(bytes("z"),empty.manifest().lowerInclusive()); assertNull(empty.manifest().upperExclusive()); assertNull(empty.manifest().file()); assertFalse(Files.exists(root.resolve("empty.sst"))); assertFalse(Files.exists(NativeRangeSstExport.buildingPath(root.resolve("empty.sst"))));
            }
        }
    }
    @Test void oversizedFirstRowAndFinalFileLimitNeverPublishPartialChunks() throws Exception {
        try (var source=new Fixture(root.resolve("limits"))) {
            source.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,PLAIN,bytes("a"),bytes("value"))));
            try (var snapshot=source.store.capture(new TransactionScope(List.of(TransactionScope.Span.range(PLAIN,null,null))))) {
                assertThrows(java.io.IOException.class,() -> NativeRangeSstExport.writeChunk(root.resolve("oversized.sst"),snapshot,PLAIN,null,null,source.options,source.plain,limits(1,1)));
                assertThrows(java.io.IOException.class,() -> NativeRangeSstExport.writeChunk(root.resolve("file-limit.sst"),snapshot,PLAIN,null,null,source.options,source.plain,new NativeRangeSstExport.Limits(1,1024,1,Duration.ofSeconds(10))));
                assertFalse(Files.exists(root.resolve("oversized.sst"))); assertFalse(Files.exists(root.resolve("file-limit.sst")));
                try (var files=Files.list(root)) { assertTrue(files.noneMatch(file -> file.getFileName().toString().endsWith(".sst.building"))); }
                assertEquals(1,source.store.activeSnapshotPins()); assertArrayEquals(bytes("value"),snapshot.get(PLAIN,bytes("a")));
            }
        }
    }
    @Test void poolChunkExportUsesTheOwnedOptionsAndFrozenSourceCut() throws Exception {
        try (var pool=DistributedStoragePool.open(root.resolve("pool"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()))) {
            var owner=new RangeOwnership.Owner(GROUP,1); var store=pool.openRange(owner,new RangeOwnership.Interval(GROUP,Set.of(PLAIN),null,null),new byte[32],Map.of(PLAIN,"chunk_data"),Set.of());
            store.applyNoop(new ApplyPosition(1,0)); store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,PLAIN,bytes("a"),bytes("old")),new Mutation(Mutation.Kind.PUT,PLAIN,bytes("b"),bytes("next"))));
            try (var snapshot=store.capture(new TransactionScope(List.of(TransactionScope.Span.range(PLAIN,null,null))))) {
                store.apply(new ApplyPosition(1,2),command(1,new Mutation(Mutation.Kind.PUT,PLAIN,bytes("a"),bytes("new"))));
                var chunk=pool.exportSstChunk(root.resolve("pool-chunk.sst"),snapshot,PLAIN,null,null,limits(1,1024)); assertTrue(chunk.hasMore()); assertEquals(new ApplyPosition(1,1),chunk.manifest().position()); assertArrayEquals(bytes("b"),chunk.manifest().upperExclusive());
                assertEquals(1,store.activeSnapshotPins());
                try (var options=new Options().setCreateIfMissing(true); var target=RocksDB.open(options,root.resolve("pool-target").toString()); var ingest=new IngestExternalFileOptions().setMoveFiles(false)) { target.ingestExternalFile(List.of(chunk.manifest().file().toString()),ingest); assertArrayEquals(bytes("old"),target.get(bytes("a"))); assertNull(target.get(bytes("b"))); }
            }
        }
    }
    @Test void exactOrphanBuildPreventsExportWithoutChangingItsBytesOrAnExistingStableArtifact() throws Exception {
        try (var source=new Fixture(root.resolve("orphan-build"))) {
            source.store.apply(new ApplyPosition(1,1),command(0,new Mutation(Mutation.Kind.PUT,PLAIN,bytes("a"),bytes("value"))));
            try (var snapshot=source.store.capture(new TransactionScope(List.of(TransactionScope.Span.range(PLAIN,null,null))))) {
                var target=root.resolve("orphan.sst"); var building=NativeRangeSstExport.buildingPath(target); byte[] original=bytes("another invocation's retained build"); Files.write(building,original,StandardOpenOption.CREATE_NEW);
                assertThrows(FileAlreadyExistsException.class,() -> NativeRangeSstExport.writeChunk(target,snapshot,PLAIN,null,null,source.options,source.plain,limits(1,1024))); assertArrayEquals(original,Files.readAllBytes(building)); assertFalse(Files.exists(target)); assertEquals(1,source.store.activeSnapshotPins());
                Files.delete(building); var published=NativeRangeSstExport.writeChunk(target,snapshot,PLAIN,null,null,source.options,source.plain,limits(1,1024)); byte[] stable=Files.readAllBytes(published.manifest().file()); assertFalse(Files.exists(building));
                Files.write(building,original,StandardOpenOption.CREATE_NEW);
                assertThrows(FileAlreadyExistsException.class,() -> NativeRangeSstExport.writeChunk(target,snapshot,PLAIN,null,null,source.options,source.plain,limits(1,1024))); assertArrayEquals(stable,Files.readAllBytes(target)); assertArrayEquals(original,Files.readAllBytes(building));
            }
            assertEquals(0,source.store.activeSnapshotPins());
        }
    }

}
