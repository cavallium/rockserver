package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.SelectiveCompaction;
import it.cavallium.rockserver.core.common.SstMaintenance;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SelectiveCompactionBoundedInventoryTest {
    static { RocksDB.loadLibrary(); }
    @TempDir(factory=DiskTestDirectory.class) Path root;

    @Test void selectedLevelIsBoundedNativelyWithoutCopyingOtherLevels() throws Exception {
        try(var options=new Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
            var db=RocksDB.open(options,root.resolve("db").toString());
            var flush=new FlushOptions().setWaitForFlush(true)) {
            var column=db.getDefaultColumnFamily();
            for(int n=0;n<3;n++){db.put(new byte[]{(byte)n},new byte[]{1});db.flush(flush);}
            assertThrows(org.rocksdb.RocksDBException.class,()->SelectiveCompaction.boundedMetadata(db,column,List.of(),"session",1,-1,2,SstMaintenance.MAX_METADATA_RESPONSE_BYTES));
            var empty=SelectiveCompaction.boundedMetadata(db,column,List.of(),"session",1,6,1,SstMaintenance.MAX_METADATA_RESPONSE_BYTES);
            assertTrue(empty.files().isEmpty());assertEquals(7,empty.numLevels());
            var all=SelectiveCompaction.boundedMetadata(db,column,List.of(),"session",1,0,3,SstMaintenance.MAX_METADATA_RESPONSE_BYTES);
            assertEquals(3,all.files().size());assertEquals("default",all.columnName());
            assertEquals((int)db.getLongProperty(column,"rocksdb.base-level"),all.baseLevel());
            assertEquals(List.of("00","01","02"),all.files().stream().map(SstMaintenance.File::smallestKeyHex).toList());
        }
    }

    @Test void largeNativeKeysAreRejectedBeforeJavaResponseCopies() throws Exception {
        try(var options=new Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
            var db=RocksDB.open(options,root.resolve("large-key").toString());
            var flush=new FlushOptions().setWaitForFlush(true)) {
            db.put(new byte[1024*1024],new byte[]{1});db.flush(flush);
            var rejected=assertThrows(org.rocksdb.RocksDBException.class,()->SelectiveCompaction.boundedMetadata(db,db.getDefaultColumnFamily(),List.of(),"session",1,0,1,64*1024));
            assertEquals(Status.SubCode.MemoryLimit,rejected.getStatus().getSubCode());
        }
    }

    @Test void exactConfiguredPathAndNativePathIdArePreserved() throws Exception {
        var first=root.resolve("hot");var second=root.resolve("capacity");
        java.nio.file.Files.createDirectories(first);java.nio.file.Files.createDirectories(second);
        try(var options=new Options().setCreateIfMissing(true).setDisableAutoCompactions(true);
            var db=RocksDB.open(options,root.resolve("paths-db").toString());
            var cfOptions=new ColumnFamilyOptions().setDisableAutoCompactions(true).setCfPaths(List.of(new DbPath(first,1L<<30),new DbPath(second,1L<<30)));
            var column=db.createColumnFamily(new ColumnFamilyDescriptor(new byte[]{'c'},cfOptions));
            var flush=new FlushOptions().setWaitForFlush(true);
            var compact=new CompactionOptions()) {
            db.put(column,new byte[]{1},new byte[]{2});db.flush(flush,column);
            var input=db.getBoundedColumnFamilyInventory(column,0,1,1024*1024).files.stream().map(f->Path.of(f.name).getFileName().toString()).toList();
            db.compactFiles(compact,column,input,5,1,null);
            var paths=List.of(first.toString(),second.toString());
            var observed=SelectiveCompaction.boundedMetadata(db,column,paths,"session",1,5,1,SstMaintenance.MAX_METADATA_RESPONSE_BYTES);
            assertEquals(1,observed.files().size());assertEquals(1,observed.files().getFirst().pathId());assertEquals(5,observed.files().getFirst().level());
            assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,()->SelectiveCompaction.boundedMetadata(db,column,List.of(root.resolve("foreign").toString()),"session",1,5,1,SstMaintenance.MAX_METADATA_RESPONSE_BYTES));
        }
    }

    @Test void staticResponseQuotaFailsBeforeAnyNativeCall() {
        var db=mock(RocksDB.class);var column=mock(ColumnFamilyHandle.class);
        assertThrows(it.cavallium.rockserver.core.common.RocksDBException.class,()->SelectiveCompaction.boundedMetadata(db,column,List.of("/"+"p".repeat(128)),"session",1,0,1,256));
        verifyNoInteractions(db,column);
    }
}
