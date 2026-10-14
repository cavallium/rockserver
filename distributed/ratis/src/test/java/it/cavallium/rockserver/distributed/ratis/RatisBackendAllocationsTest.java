package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisBackendAllocationsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void exhaustedWindowRotatesExclusivelyIssuedGrantAndColdReopenRetainsEveryOldRoot()throws Exception{
        try(var fixture=new RatisDatabaseBackupFactoryFixture(root.resolve("rotation"))){Path path=root.resolve("allocations");var scheduler=fixture.real.endpoints.getFirst().scheduler;var reservations=new ArrayList<RatisRocksDBAdapter.Allocation>();int perGrant=4096/RatisTransactionIdentities.BLOCK_SIZE;
            try(var owner=new RatisBackendAllocations(path,fixture.catalogue.epochs,scheduler,60000,2,RatisBackendSessionsTest.targets(fixture), RatisJournalAccess.standalone())){for(int i=0;i<perGrant+1;i++)reservations.add(owner.allocate(RequestContext.batch()).get(10,TimeUnit.SECONDS));}
            assertEquals(reservations.getFirst().identity().commit().business().sessionId(),reservations.get(perGrant-1).identity().commit().business().sessionId());assertNotEquals(reservations.getFirst().identity().commit().business().sessionId(),reservations.get(perGrant).identity().commit().business().sessionId());
            long epochs=fixture.catalogue.epochs.read().nextEpoch();try(var owner=new RatisBackendAllocations(path,fixture.catalogue.epochs,scheduler,60000,2,RatisBackendSessionsTest.targets(fixture), RatisJournalAccess.standalone())){for(int i=0;i<perGrant-1;i++)reservations.add(owner.allocate(RequestContext.batch()).get(10,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->owner.allocate(RequestContext.batch()).get(10,TimeUnit.SECONDS));assertEquals(epochs,fixture.catalogue.epochs.read().nextEpoch());}
            assertEquals(2L*perGrant,reservations.stream().map(a->a.identity().commit().business()).distinct().count());for(var allocation:reservations)assertTrue(Files.isDirectory(allocation.snapshot().getParent()));
            Files.delete(path.resolve("allocator.bin"));assertThrows(IOException.class,()->new RatisBackendAllocations(path,fixture.catalogue.epochs,scheduler,60000,2,RatisBackendSessionsTest.targets(fixture), RatisJournalAccess.standalone()));assertEquals(epochs,fixture.catalogue.epochs.read().nextEpoch());
        }
    }
}
