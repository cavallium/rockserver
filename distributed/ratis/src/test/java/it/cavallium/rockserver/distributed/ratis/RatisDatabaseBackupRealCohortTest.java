package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDatabaseBackupRealCohortTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void realServiceClientAndRemoteExportSixtyFiveGroupsNeedActualCohortDrainBeforeComplete()throws Exception{
        try(var fixture=new RatisDatabaseBackupRealCohortFixture(root.resolve("real-sixty-five"),32,33);var journal=fixture.base.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.base.root.resolve("archive"),fixture.base.definition,fixture.base.destination);var driver=fixture.driver(journal,archive)){
            driver.step(RequestContext.batch());fixture.loseReply=true;assertThrows(IOException.class,()->driver.step(RequestContext.batch()));assertEquals(1,fixture.base.pools.stream().mapToInt(p->p.nativePool.snapshotAdmissionStats().pins()).sum());
            driver.step(RequestContext.batch());assertEquals(2,fixture.base.pools.stream().mapToInt(p->p.nativePool.snapshotAdmissionStats().pins()).sum());fixture.denyDrain=true;
            boolean blocked=false;for(int i=0;i<512;i++){try{var progress=driver.step(RequestContext.batch());assertFalse(progress.complete());}catch(IOException expected){if(journal.snapshot().phase()==RatisDatabaseBackupJournal.Phase.COMPLETE){blocked=true;break;}throw expected;}}
            assertTrue(blocked);assertEquals(65,archive.reopenComplete().chunks().size());assertTrue(fixture.base.pools.stream().anyMatch(p->p.nativePool.snapshotAdmissionStats().pins()>0));fixture.denyDrain=false;
            var result=driver.step(RequestContext.batch());for(int i=0;i<128 && !result.complete();i++)result=driver.step(RequestContext.batch());assertTrue(result.complete());assertFalse(result.cleanupPending());for(var pool:fixture.base.pools)assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());
        }
    }
    @Test void authoritativeDrainedOldProcessSettlesLostAttemptWithoutAnyNewCapture()throws Exception{
        try(var fixture=new RatisDatabaseBackupRealCohortFixture(root.resolve("gone-cold"),1,1);var archive=new RatisDatabaseBackupArchive(fixture.base.root.resolve("archive"),fixture.base.definition,fixture.base.destination)){
            try(var journal=fixture.base.journal(false,false);var driver=fixture.driver(journal,archive)){
                driver.step(RequestContext.batch());driver.step(RequestContext.batch());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,journal.snapshot().phase());
                var request=journal.snapshot().requests().getFirst();var endpoint=fixture.endpoints.stream().filter(e->e.state.poolId.equals(request.poolId())).findFirst().orElseThrow();endpoint.restartAfterDrain(request);
                assertThrows(RatisSnapshotLostException.class,()->driver.step(RequestContext.batch()));assertEquals(RatisDatabaseBackupJournal.Phase.LOST,journal.snapshot().phase());
            }
            try(var journal=fixture.base.journal(true,false);var driver=fixture.driver(journal,archive)){
                var result=driver.step(RequestContext.batch());for(int i=0;i<8 && result.cleanupPending();i++)result=driver.step(RequestContext.batch());assertFalse(result.cleanupPending());assertFalse(result.complete());assertEquals(RatisDatabaseBackupJournal.Phase.LOST,result.phase());for(var p:fixture.base.pools)assertEquals(0,p.nativePool.snapshotAdmissionStats().pins());assertFalse(java.nio.file.Files.exists(fixture.base.root.resolve("archive/COMPLETE")));
            }
        }
    }

}
