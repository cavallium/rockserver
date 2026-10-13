package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual existing controlled native HOME/DATA adapters; no quorum or production endpoint claim. */
class RatisSnapshotExternalReportingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void matchingHelperCompleteReportsWithoutRecaptureAndCrossedTokenIsRejected()throws Exception{
        var tests=new RatisSnapshotCoordinatorTest();tests.root=root;
        try(var f=tests.new Fixture("external-report")){
            var initial=SnapshotFenceStatus.begin(f.fence);Path path=root.resolve("external.journal");
            try(var journal=RatisDurableSnapshot.create(path,f.home.homes,f.data,initial,f.home.homes.prepare(tests.control(20),null,initial),f.directory.ranges(),Duration.ofSeconds(10));var coordinator=f.driver(journal)){
                for(int i=0;i<32;i++)if(coordinator.step(RatisSnapshotCoordinator.StopAt.GATES_READY).action()==RatisSnapshotCoordinator.Action.GATES_READY)break;
                assertTrue(journal.snapshot().observedHome().allReady());var expected=journal.snapshot().observedHome();
                for(var p:expected.progress()){
                    var prepared=f.data.prepareSnapshotCapture(f.data.snapshotTarget(f.fence,p.groupId()),UUID.randomUUID(),Duration.ofSeconds(10));journal.freezeCapture(prepared);
                    var token=f.data.executeSnapshotCapture(prepared);journal.acceptExternalCapture(p.groupId(),prepared,new RatisSnapshotCaptureRequest.Result(token,null));expected=expected.capture(p.groupId(),token);
                }
                assertEquals(2,f.captures.get());for(int i=0;i<4;i++)if(coordinator.reportExternalCaptures(expected).participant()==null)break;
                var last=journal.snapshot().lastHome();var complete=journal.snapshot().observedHome().complete();f.home.homes.execute(f.home.homes.prepare(new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),journal.snapshot().observedHome(),complete));
                for(int i=0;i<8;i++)if(coordinator.step(RatisSnapshotCoordinator.StopAt.RELEASED).complete())break;
                assertNull(coordinator.reportExternalCaptures(expected).participant());assertEquals(2,f.captures.get());
                var crossed=SnapshotFenceStatus.begin(f.fence);for(var p:expected.progress())crossed=crossed.ready(p.groupId(),p.ready());for(var p:expected.progress())crossed=crossed.capture(p.groupId(),new SnapshotFenceStatus.Capture(p.capture().ownerIncarnation(),UUID.randomUUID(),p.capture().position()));
                var invalid=crossed;assertThrows(IOException.class,()->coordinator.reportExternalCaptures(invalid));assertEquals(2,f.captures.get());for(var p:journal.snapshot().participants())journal.releaseCapture(p.route().groupId());
            }
        }
    }
}
