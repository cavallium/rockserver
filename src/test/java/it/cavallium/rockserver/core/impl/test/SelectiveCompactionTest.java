package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.SelectiveCompaction;

import it.cavallium.rockserver.core.common.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SelectiveCompactionTest {
    private SstMaintenance.File file(String name, String start, String end, boolean busy) {
        return new SstMaintenance.File(name, 6, 0, 100, start, end, busy);
    }
    private SstMaintenance.Metadata metadata(List<SstMaintenance.File> files) {
        return new SstMaintenance.Metadata("session",1,"entries",7,2,List.of("/capacity"),files);
    }
    private SstMaintenance.Request request(List<String> files) {
        return new SstMaintenance.Request(1,"session",files,6,0,1024,1000,1,false);
    }
    @Test void rejectsNonAdjacentFilesAndSharedBoundary() {
        var m=metadata(List.of(file("1.sst","01","02",false),file("2.sst","03","04",false),file("3.sst","05","06",false)));
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(m,request(List.of("1.sst","3.sst"))));
        var shared=metadata(List.of(file("1.sst","01","02",false),file("2.sst","02","03",false)));
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(shared,request(List.of("2.sst"))));
        assertEquals(2,SelectiveCompaction.validate(shared,request(List.of("1.sst","2.sst"))).size());
    }
    @Test void rejectsBusyMissingStaleAndOversized() {
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(metadata(List.of(file("1.sst","01","02",true))),request(List.of("1.sst"))));
        var m=metadata(List.of(file("1.sst","01","02",false)));
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(m,request(List.of("2.sst"))));
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(m,new SstMaintenance.Request(1,"old",List.of("1.sst"),6,0,1024,1000,1,false)));
        assertThrows(RocksDBException.class,()->SelectiveCompaction.validate(m,new SstMaintenance.Request(1,"session",List.of("1.sst"),6,0,1024,99,1,false)));
    }
    @Test void rejectsInvalidParametersAndCopiesInputs() {
        for(var files:List.of(List.<String>of(),List.of("../1.sst"),List.of("/1.sst"),List.of("1.sst","1.sst")))
            assertThrows(RocksDBException.class,()->request(files));
        assertThrows(RocksDBException.class,()->new SstMaintenance.Request(1,"session",List.of("1.sst"),0,0,1024,1000,1,false));
        assertThrows(RocksDBException.class,()->new SstMaintenance.Request(1,"session",List.of("1.sst"),6,-1,1024,1000,1,false));
        assertThrows(RocksDBException.class,()->new SstMaintenance.Request(1,"session",List.of("1.sst"),6,0,0,1000,1,false));
        var mutable=new java.util.ArrayList<>(List.of("1.sst"));var req=request(mutable);mutable.clear();assertEquals(1,req.files().size());
        assertEquals(OperationFamily.COMPACTION,new RocksDBAPICommand.CompactFiles(req).operationFamily());
        assertEquals(WorkloadProfile.PHYSICAL_MAINTENANCE,new RocksDBAPICommand.CompactFiles(req).protectedProfile());
    }
}
