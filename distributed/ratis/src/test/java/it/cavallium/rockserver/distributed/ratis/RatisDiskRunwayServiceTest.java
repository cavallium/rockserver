package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDiskRunwayServiceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void rotatingNativeWalAndRaftWalkMustCompleteBeforeOrdinaryAndDeduplicatesFilesystem()throws Exception {
        var clock=new AtomicLong(10);var runway=new RatisDiskRunway(RatisDiskRunwayTest.policy(),clock::get);var nativeRoot=Files.createDirectory(root.resolve("native"));var wal=Files.createDirectory(nativeRoot.resolve("wal"));var raft=Files.createDirectory(root.resolve("raft"));Files.write(wal.resolve("a.log"),new byte[7]);Files.write(raft.resolve("log_0-1"),new byte[11]);
        var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(4,1,1,16,16,"disk-census");var scopes=List.of(new RatisDiskRunwayService.Scope(RatisDiskRunwayTest.POOL,null,nativeRoot),new RatisDiskRunwayService.Scope(RatisDiskRunwayTest.POOL,null,wal),new RatisDiskRunwayService.Scope(null,new UUID(2,2),raft));var service=new RatisDiskRunwayService(runway,()->scopes,scheduler,path->new RatisDiskRunwayService.Observation(7,500),clock::get);
        try {service.refresh().get(5,TimeUnit.SECONDS);assertThrows(java.io.IOException.class,runway::requireOrdinary);for(int n=0;n<16 && runway.state()==RatisDiskRunway.State.UNKNOWN;n++)service.refresh().get(5,TimeUnit.SECONDS);runway.requireOrdinary();assertEquals(18,runway.stats().observedLogicalBytes());var lease=runway.reserveRaftAppend(RatisDiskRunwayTest.POOL,100,false);assertEquals(100,runway.stats().pendingOrdinaryBytes());lease.actualFinished();}
        finally{service.stop().get(5,TimeUnit.SECONDS);scheduler.dispose();}
    }
    @Test void foreignSymlinkMakesCensusIncompleteAndStopClosesIterator()throws Exception {
        var clock=new AtomicLong(10);var runway=new RatisDiskRunway(RatisDiskRunwayTest.policy(),clock::get);var nativeRoot=Files.createDirectory(root.resolve("native"));Files.createSymbolicLink(nativeRoot.resolve("foreign"),root.resolve("missing"));var scheduler=it.cavallium.rockserver.core.impl.RWScheduler.forTesting(4,1,1,16,16,"disk-bad-census");var service=new RatisDiskRunwayService(runway,()->List.of(new RatisDiskRunwayService.Scope(RatisDiskRunwayTest.POOL,null,nativeRoot)),scheduler,path->new RatisDiskRunwayService.Observation(7,500),clock::get);
        try {assertThrows(ExecutionException.class,()->service.refresh().get(5,TimeUnit.SECONDS));assertEquals(RatisDiskRunway.State.UNKNOWN,runway.state());assertThrows(java.io.IOException.class,runway::requireOrdinary);}
        finally{service.stop().get(5,TimeUnit.SECONDS);scheduler.dispose();}
    }
}
