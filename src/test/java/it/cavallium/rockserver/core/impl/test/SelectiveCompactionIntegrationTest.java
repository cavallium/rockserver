package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.*;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.rocksdb.*;

@Timeout(60)
class SelectiveCompactionIntegrationTest {
    @TempDir Path temp;
    static final byte[] VALUE = new byte[4096];
    static { new Random(4).nextBytes(VALUE); }
    static byte[] rawKey(int n) { return ByteBuffer.allocate(4).putInt(n).array(); }
    static Keys key(int n) { return new Keys(Buf.wrap(rawKey(n))); }
    static final it.cavallium.rockserver.core.common.ColumnSchema SCHEMA =
            it.cavallium.rockserver.core.common.ColumnSchema.of(IntList.of(4),ObjectList.of(),true);

    private Path fixture(int level, boolean multipath) throws Exception {
        org.rocksdb.RocksDB.loadLibrary();
        Path dbPath=temp.resolve("db");
        List<DbPath> paths=new ArrayList<>();
        if(multipath) {
            Files.createDirectories(temp.resolve("hot"));Files.createDirectories(temp.resolve("capacity"));
            paths=List.of(new DbPath(temp.resolve("hot"),1L<<30),new DbPath(temp.resolve("capacity"),10L<<30));
        }
        try(var o=new Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
            var co=new ColumnFamilyOptions().setNumLevels(7).setDisableAutoCompactions(true).setCfPaths(paths);
            var db=org.rocksdb.RocksDB.open(o,dbPath.toString());
            var cf=db.createColumnFamily(new ColumnFamilyDescriptor("entries".getBytes(),co));
            var fo=new FlushOptions().setWaitForFlush(true);
            var compact=new CompactionOptions().setOutputFileSizeLimit(1L<<20).setMaxSubcompactions(1)) {
            for(int batch=0;batch<6;batch++) {
                for(int i=batch*32;i<(batch+1)*32;i++)db.put(cf,rawKey(i),VALUE);
                db.flush(fo,cf);
                var files=db.getColumnFamilyMetaData(cf).levels().get(0).files().stream().map(SstFileMetaData::fileName).toList();
                db.compactFiles(compact,cf,files,level,0,null);
            }
        }
        Path cfg=temp.resolve("config.conf");
        Files.writeString(cfg,"database.global.disable-auto-compactions=true\ndatabase.global.ingest-behind=false\ndatabase.global.fallback-column-options.volumes=[{volume-path=\".\",target-size=\"10GiB\"}]\n"
                +(multipath? "database.global.column-options=[{name=entries,volumes=[{volume-path=\""+temp.resolve("hot")+"\",target-size=1GiB},{volume-path=\""+temp.resolve("capacity")+"\",target-size=10GiB}]}]\n":""));
        return cfg;
    }
    private SstMaintenance.Request request(SstMaintenance.Metadata m,int level,int path,boolean execute) {
        return new SstMaintenance.Request(m.columnId(),m.session(),m.files().stream().map(SstMaintenance.File::name).toList(),
                level,path,8L<<20,8L<<20,1,execute);
    }

    @ParameterizedTest @CsvSource({"5,false","6,false","5,true","6,true"})
    void grpcMergesSameLevelAndPreservesSnapshotAndLatestWrites(int level,boolean multipath) throws Exception {
        Path config=fixture(level,multipath);
        try(var embedded=new EmbeddedConnection(temp.resolve("db"),"selective",config);
            var server=new GrpcServer(embedded,new InetSocketAddress("127.0.0.1",0))) {
            server.start();
            try(var client=GrpcConnection.forHostAndPort("selective-client",new Utils.HostAndPort("127.0.0.1",server.getPort()))) {
                var api=client.getSyncApi(RequestContext.batch());
                long col=api.createColumn("entries",SCHEMA);
                var metadata=api.getSstMetadata(col,level);
                assertEquals(6,metadata.files().size());assertEquals(7,metadata.numLevels());
                assertEquals(multipath?2:1,metadata.paths().size());
                var selected=request(metadata,level,multipath?1:0,false);
                assertFalse(api.compactFiles(selected).executed());
                assertEquals(metadata.files(),api.getSstMetadata(col,level).files());
                var nativeDb=embedded.getInternalDB().getDb().get();
                var cf=embedded.getInternalDB().getDb().getStartupColumns().entrySet().stream()
                        .filter(e->Arrays.equals(e.getKey().getName(),"entries".getBytes())).findFirst().orElseThrow().getValue();
                var snapshot=nativeDb.getSnapshot();
                try(var ro=new ReadOptions().setSnapshot(snapshot)) {
                    api.put(0,col,key(0),Buf.wrap(new byte[]{7}),RequestType.none());
                    api.delete(0,col,key(1),RequestType.none());
                    var result=api.compactFiles(request(metadata,level,multipath?1:0,true));
                    assertTrue(result.executed());assertEquals(6,result.inputFiles().size());assertEquals(1,result.outputFiles().size());
                    assertTrue(result.outputBytes()>0);assertTrue(result.elapsedNanos()>0);
                    var after=api.getSstMetadata(col,level);assertEquals(1,after.files().size());
                    assertEquals(multipath?1:0,after.files().getFirst().pathId());
                    for(int i=0;i<192;i++)assertArrayEquals(VALUE,nativeDb.get(cf,ro,rawKey(i)));
                    assertEquals(Buf.wrap(new byte[]{7}),api.get(0,col,key(0),RequestType.current()));
                    assertNull(api.get(0,col,key(1),RequestType.current()));
                    assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,()->api.compactFiles(selected));
                } finally { nativeDb.releaseSnapshot(snapshot); }
            }
        }
        try(var reopened=new EmbeddedConnection(temp.resolve("db"),"reopened",config)) {
            var api=reopened.getSyncApi(RequestContext.batch());long col=api.getColumnId("entries");
            assertEquals(Buf.wrap(new byte[]{7}),api.get(0,col,key(0),RequestType.current()));
            assertNull(api.get(0,col,key(1),RequestType.current()));
            assertEquals(1,api.getSstMetadata(col,level).files().size());
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void metadataUsesDefaultDiskAndMemoryPaths(boolean memory) throws Exception {
        try(var embedded=new EmbeddedConnection(memory?null:temp.resolve("fresh"),"default-paths",null)) {
            var api=embedded.getSyncApi(RequestContext.batch());long col=api.createColumn("entries",SCHEMA);
            api.put(0,col,key(0),Buf.wrap(VALUE),RequestType.none());api.flush();
            var metadata=api.getSstMetadata(col,-1);
            assertFalse(metadata.files().isEmpty());assertEquals(1,metadata.paths().size());
            assertTrue(metadata.files().stream().allMatch(f->f.pathId()==0));
            assertEquals(Buf.wrap(VALUE),api.get(0,col,key(0),RequestType.current()));
        }
    }

    @Test void cancellationRetainsColumnLeaseUntilNativeOperationReturns() throws Exception {
        Path config=fixture(6,false);
        try(var embedded=new EmbeddedConnection(temp.resolve("db"),"lifecycle",config)) {
            var api=embedded.getSyncApi(RequestContext.batch());long col=api.createColumn("entries",SCHEMA);
            var request=request(api.getSstMetadata(col,6),6,0,true);
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            var internal=embedded.getInternalDB();
            internal.setColumnMaintenanceObserverForTesting(()->{
                entered.countDown(); boolean interrupted=false;
                for(;;) try { release.await();break; } catch(InterruptedException e) { interrupted=true; }
                if(interrupted) Thread.currentThread().interrupt();
            });
            var compaction=embedded.getAsyncApi(RequestContext.batch()).compactFilesAsync(request);
            CompletableFuture<Void> deletion=null;
            try {
                assertTrue(entered.await(10,TimeUnit.SECONDS));compaction.cancel(false);
                // Foreground requests do not need the global column edit lock.
                assertEquals(Buf.wrap(VALUE),embedded.getSyncApi(RequestContext.latency(Duration.ofSeconds(5)))
                        .get(0,col,key(2),RequestType.current()));
                deletion=CompletableFuture.runAsync(()->internal.deleteColumn(col));
                final var pending=deletion;
                assertThrows(TimeoutException.class,()->pending.get(150,TimeUnit.MILLISECONDS));
                assertTrue(internal.getPendingOpsCount()>0);
            } finally { release.countDown();internal.setColumnMaintenanceObserverForTesting(null); }
            if(deletion!=null)deletion.get(10,TimeUnit.SECONDS);
            assertEquals(0,internal.getPendingOpsCount());
        }
    }
}
