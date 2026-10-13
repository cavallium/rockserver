package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native fixture backend; actual quorum, endpoint and production cohort wiring remain separate acceptance. */
class RatisDatabaseBackupDriverTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final RequestContext CONTEXT=RequestContext.batch();
    static void until(RatisDatabaseBackupDriver driver,RatisDatabaseBackupJournal journal,RatisDatabaseBackupJournal.Phase phase)throws Exception{for(int i=0;i<512 && journal.snapshot().phase()!=phase;i++)driver.step(CONTEXT);assertEquals(phase,journal.snapshot().phase());}
    @Test void twoDomainTwoPoolSixtyFiveGroupArchiveForcesWholeVectorBeforeAnyHomeReport()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("sixty-five"),32,33);var journal=fixture.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination);var driver=fixture.driver(journal,archive)){
            driver.step(CONTEXT);assertEquals(RatisDatabaseBackupJournal.Phase.REQUESTS,journal.snapshot().phase());assertEquals(0,fixture.physical.get());
            fixture.lostReply=true;assertThrows(IOException.class,()->driver.step(CONTEXT));assertEquals(1,fixture.physical.get());assertEquals(0,fixture.reports.get());assertFalse(fixture.vectorForced.get());
            var original=journal.snapshot().requests();driver.step(CONTEXT);assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,journal.snapshot().phase());assertEquals(2,fixture.physical.get());assertEquals(original,journal.snapshot().requests());assertEquals(0,fixture.reports.get());
            assertEquals(65,journal.snapshot().captured().stream().mapToInt(s->s.progress().size()).sum());for(var pool:fixture.pools)assertEquals(1,pool.nativePool.snapshotAdmissionStats().pins());
            RatisDatabaseBackupDriver.Progress progress=null;for(int i=0;i<512;i++){progress=driver.step(CONTEXT);if(progress.complete())break;}assertNotNull(progress);assertTrue(progress.complete());assertFalse(progress.cleanupPending());assertEquals(2,fixture.physical.get());
            var complete=archive.reopenComplete();assertEquals(65,complete.chunks().size());assertEquals(2,complete.cohorts().size());assertTrue(complete.heldCaptures().stream().allMatch(s->s.phase()==SnapshotFenceStatus.Phase.INSTALLING));for(var pool:fixture.pools)assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());
            var fresh=fixture.pools.getFirst().owner;var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"restored","localhost:29000",ReplicaDescriptor.Role.VOTER);
            var plan=RatisDatabaseRestorePlanner.plan(complete,UUID.randomUUID(),List.of(replica),UUID.randomUUID(),0);assertEquals(2,plan.domains().size());assertEquals(65,plan.domains().stream().mapToInt(d->d.finalDirectory().ranges().size()).sum());
        }
    }
    @Test void unknownGlobalForceFencesLiveDriverAndExactReopenRecoversOriginalCuts()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("unknown"),1,1);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination)){
            long started;try(var journal=fixture.journal(false,true);var driver=fixture.driver(journal,archive)){
                driver.step(CONTEXT);started=journal.snapshot().dispatchStartedMillis();assertThrows(IOException.class,()->driver.step(CONTEXT));assertEquals(2,fixture.physical.get());assertEquals(0,fixture.reports.get());assertThrows(IOException.class,()->driver.step(CONTEXT));
            }
            try(var journal=fixture.journal(true,false);var driver=fixture.driver(journal,archive)){
                assertEquals(started,journal.snapshot().dispatchStartedMillis());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,journal.snapshot().phase());driver.step(CONTEXT);assertTrue(fixture.reports.get()>0);assertEquals(2,fixture.physical.get());
            }
        }
    }
    @Test void coldLostOriginalPinCannotRecaptureOrPublishCompleteAndChangedVectorCannotReport()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("lost"),1,1);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination)){
            try(var journal=fixture.journal(false,false);var driver=fixture.driver(journal,archive)){
                driver.step(CONTEXT);fixture.lostReply=true;assertThrows(IOException.class,()->driver.step(CONTEXT));fixture.cancelAndDrain(journal.snapshot().requests().getFirst());assertEquals(1,fixture.physical.get());
            }
            try(var journal=fixture.journal(true,false);var driver=fixture.driver(journal,archive)){
                assertThrows(RatisSnapshotLostException.class,()->driver.step(CONTEXT));assertEquals(RatisDatabaseBackupJournal.Phase.LOST,journal.snapshot().phase());assertEquals(1,fixture.physical.get());assertEquals(0,fixture.reports.get());assertFalse(Files.exists(fixture.root.resolve("archive/COMPLETE")));
            }
        }
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("changed"),1,1);var journal=fixture.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination);var driver=fixture.driver(journal,archive)){
            driver.step(CONTEXT);fixture.changeVector=true;assertThrows(IllegalArgumentException.class,()->driver.step(CONTEXT));assertEquals(RatisDatabaseBackupJournal.Phase.REQUESTS,journal.snapshot().phase());assertEquals(0,fixture.reports.get());driver.cancel();driver.step(CONTEXT);
        }
    }
    @Test void cancellationWaitsForActualExportBeforePinsCanDrain()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("cancel"),1,1);var journal=fixture.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination);var driver=fixture.driver(journal,archive);var caller=Executors.newSingleThreadExecutor()){
            until(driver,journal,RatisDatabaseBackupJournal.Phase.MATERIALIZING);fixture.blockExport=true;var pending=caller.submit(()->driver.step(CONTEXT));assertTrue(fixture.exportEntered.await(10,TimeUnit.SECONDS));
            driver.cancel();assertThrows(IOException.class,driver::close);assertFalse(pending.isDone());for(var pool:fixture.pools)assertEquals(1,pool.nativePool.snapshotAdmissionStats().pins());
            fixture.exportExit.countDown();pending.get(10,TimeUnit.SECONDS);driver.step(CONTEXT);assertEquals(RatisDatabaseBackupJournal.Phase.CANCELLED,journal.snapshot().phase());for(var pool:fixture.pools)assertEquals(0,pool.nativePool.snapshotAdmissionStats().pins());assertFalse(Files.exists(fixture.root.resolve("archive/COMPLETE")));
        }
    }
    private static void rewriteOriginalStart(RatisDatabaseBackupDriverFixture fixture,long started)throws IOException{
        Path path=fixture.root.resolve("global");byte[] bytes=SessionJournalFiles.read(path,RatisDatabaseBackupJournal.MAX_BYTES);java.nio.ByteBuffer.wrap(bytes).putLong(60+fixture.definition.encode().length,started);SessionJournalFiles.write(path,bytes,RatisDatabaseBackupJournal.MAX_BYTES);
    }
    @Test void expiredOrRegressingOriginalDispatchFailsColdWithoutAnyRecapture()throws Exception{
        for(boolean future:List.of(false,true))try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("deadline-"+future),1,1);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination)){
            try(var journal=fixture.journal(false,false);var driver=fixture.driver(journal,archive)){driver.step(CONTEXT);}
            rewriteOriginalStart(fixture,future?System.currentTimeMillis()+1000000:1);
            try(var journal=fixture.journal(true,false);var driver=fixture.driver(journal,archive)){assertThrows(RatisSnapshotLostException.class,()->driver.step(CONTEXT));assertEquals(RatisDatabaseBackupJournal.Phase.LOST,journal.snapshot().phase());assertEquals(0,fixture.physical.get());assertEquals(0,fixture.reports.get());}
        }
    }
    @Test void coldCopiedCompleteGraphPublishesAfterDeadlineWithoutOriginalPins()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("copied-cold"),1,1);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination)){
            try(var journal=fixture.journal(false,false);var driver=fixture.driver(journal,archive)){
                until(driver,journal,RatisDatabaseBackupJournal.Phase.MATERIALIZING);for(int i=0;i<32 && journal.snapshot().chunks().size()<2;i++)driver.step(CONTEXT);assertEquals(2,journal.snapshot().chunks().size());
                for(var request:journal.snapshot().requests())fixture.cancelAndDrain(request);
            }
            rewriteOriginalStart(fixture,1);int physical=fixture.physical.get();
            try(var journal=fixture.journal(true,false);var driver=fixture.driver(journal,archive)){
                var result=driver.step(CONTEXT);assertEquals(RatisDatabaseBackupJournal.Phase.COMPLETE,result.phase());assertEquals(2,archive.reopenComplete().chunks().size());assertEquals(physical,fixture.physical.get());
                for(int i=0;i<8 && !result.complete();i++)result=driver.step(CONTEXT);assertTrue(result.complete());assertEquals(physical,fixture.physical.get());assertTrue(driver.step(CONTEXT).complete(),"Released exports retry settlement without a second READY-only ACK");
            }
        }
    }

    @Test void terminalCancellationAndLossStayPendingUntilEveryNativeGateIsReleased()throws Exception{
        for(boolean cancel:List.of(true,false))try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("pending-"+cancel),2,2);var journal=fixture.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination);var driver=fixture.driver(journal,archive)){
            driver.step(CONTEXT);if(cancel)driver.cancel();else journal.phase(RatisDatabaseBackupJournal.Phase.LOST);
            var result=driver.step(CONTEXT);assertTrue(result.cleanupPending());for(var pool:fixture.pools)for(var store:pool.stores.values())assertNull(store.snapshotGate(pool.fence.fenceId()).released());
            for(int i=0;i<8 && result.cleanupPending();i++)result=driver.step(CONTEXT);assertFalse(result.cleanupPending());assertFalse(result.complete());
            for(var pool:fixture.pools)for(var store:pool.stores.values())assertNotNull(store.snapshotGate(pool.fence.fenceId()).released());assertEquals(0,fixture.physical.get());
        }
    }
    @Test void writeCertifiedBindingsRejectBeforeAnyCoordinatorOrBackendGateCall()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("writable"),1,1);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination)){
            var shortPolicy=new RatisDatabaseBackupDriver.Policy(java.time.Duration.ofSeconds(30),java.time.Duration.ofSeconds(25),4);var bindings=new HashMap<UUID,RatisDatabaseBackupDriver.Binding>();
            for(var pool:fixture.pools){
                org.mockito.Mockito.when(pool.durable.snapshot()).thenAnswer(call->{var s=pool.snapshot();return new RatisDurableSnapshot.Snapshot(s.initial(),s.observedHome(),s.pendingHome(),s.homeResult(),s.homeSuperseded(),s.lastHome(),s.participants(),s.homeRetirementProof(),s.releaseRequested(),shortPolicy.retention(),RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);});
                var old=fixture.bindings.get(pool.fence.fenceId());var e=old.exportDefinition();var export=new RatisExportJournal.Definition(e.jobId(),e.fence(),e.columns(),e.sourceJournal(),e.sourceIdentity(),e.limits(),shortPolicy.retention());bindings.put(pool.fence.fenceId(),new RatisDatabaseBackupDriver.Binding(pool.durable,pool.coordinator,pool.routed,old.exportPath(),export,old.chunks()));
            }
            try(var journal=RatisDatabaseBackupJournal.create(fixture.root.resolve("writable-global"),fixture.definition,fixture.destination,shortPolicy)){
                assertThrows(IOException.class,()->new RatisDatabaseBackupDriver(journal,archive,()->new RatisDatabaseBackupDriver.Vector(fixture.definition.roster(),fixture.definition.domains()),fixture,op->{throw new IOException("unused");},bindings));
                assertEquals(0,fixture.physical.get());for(var pool:fixture.pools){assertEquals(0,pool.gateChecks);for(var store:pool.stores.values())assertEquals(new ApplyPosition(1,2),store.applied());}
            }
        }
    }
    @Test void completePhaseWithoutExactIndependentArchiveCannotReturnSuccessOrReleasePins()throws Exception{
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("missing-complete"),1,1);var journal=fixture.journal(false,false);var archive=new RatisDatabaseBackupArchive(fixture.root.resolve("archive"),fixture.definition,fixture.destination);var driver=fixture.driver(journal,archive)){
            until(driver,journal,RatisDatabaseBackupJournal.Phase.COMPLETE);int before=fixture.pools.stream().mapToInt(p->p.released.size()).sum();Files.delete(fixture.root.resolve("archive/COMPLETE"));
            assertThrows(IOException.class,()->driver.step(CONTEXT));assertEquals(before,fixture.pools.stream().mapToInt(p->p.released.size()).sum());assertEquals(RatisDatabaseBackupJournal.Phase.COMPLETE,journal.snapshot().phase());
            archive.complete(journal.manifest());var result=driver.step(CONTEXT);for(int i=0;i<8 && !result.complete();i++)result=driver.step(CONTEXT);assertTrue(result.complete());
        }
    }

}
