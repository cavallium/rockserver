package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisDatabaseBackupJournalTest {
    @TempDir Path root;
    final RatisDatabaseBackupDriver.Policy policy=new RatisDatabaseBackupDriver.Policy(Duration.ofSeconds(10),Duration.ofSeconds(8),4);
    RatisDatabaseBackupJournal.CohortRequest request(RatisDatabaseBackupFixture f){var p=f.held.getFirst().progress().getFirst();return new RatisDatabaseBackupJournal.CohortRequest(UUID.randomUUID(),f.owner.nodeIncarnation(),UUID.randomUUID(),UUID.randomUUID(),List.of(new RatisDatabaseBackupJournal.Member(f.held.getFirst().fence().fenceId(),p.groupId(),p.capture().snapshotId(),p.capture().ownerIncarnation(),f.owner.replicaId(),10000,new byte[32])));}
    @Test void completeCurrentJournalReopensExactVectorAndRejectsChangedCaptureAttempt()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1024);var request=request(f);Path path=root.resolve("backup");long started;
        try(var journal=RatisDatabaseBackupJournal.create(path,f.definition,f.destination,policy)){
            journal.freezeRequests(List.of(request));started=journal.snapshot().dispatchStartedMillis();assertTrue(started>0);
            var old=request.members().getFirst();var changed=new RatisDatabaseBackupJournal.Member(old.fenceId(),old.groupId(),UUID.randomUUID(),old.owner(),old.replica(),old.ttlMillis(),old.preparedDigest());
            assertThrows(IOException.class,()->journal.freezeRequests(List.of(new RatisDatabaseBackupJournal.CohortRequest(request.cohortId(),request.nodeIncarnation(),request.processIncarnation(),request.poolId(),List.of(changed)))));
            journal.captured(f.held);journal.phase(RatisDatabaseBackupJournal.Phase.REPORTED);journal.phase(RatisDatabaseBackupJournal.Phase.RELEASED);journal.phase(RatisDatabaseBackupJournal.Phase.MATERIALIZING);journal.copied(f.emptyChunks().getFirst());journal.phase(RatisDatabaseBackupJournal.Phase.COMPLETE);
        }
        try(var journal=RatisDatabaseBackupJournal.reopen(path,f.definition,f.destination,policy)){assertEquals(started,journal.snapshot().dispatchStartedMillis());assertEquals(RatisDatabaseBackupJournal.Phase.COMPLETE,journal.snapshot().phase());assertEquals(1,journal.manifest().chunks().size());assertThrows(IOException.class,()->journal.phase(RatisDatabaseBackupJournal.Phase.LOST));}
    }
    @Test void missingCohortCoverageAndOversizedDecoderCountFailBeforeAllocation()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1024);Path path=root.resolve("counts");
        try(var journal=RatisDatabaseBackupJournal.create(path,f.definition,f.destination,policy)){assertThrows(IOException.class,()->journal.freezeRequests(List.of()));journal.freezeRequests(List.of(request(f)));}
        byte[] bytes=SessionJournalFiles.read(path,RatisDatabaseBackupJournal.MAX_BYTES);ByteBuffer.wrap(bytes).putInt(72+f.definition.encode().length,Integer.MAX_VALUE);SessionJournalFiles.write(path,bytes,RatisDatabaseBackupJournal.MAX_BYTES);
        assertThrows(IOException.class,()->RatisDatabaseBackupJournal.reopen(path,f.definition,f.destination,policy));
    }
    @Test void extendedReadonlyPolicyStillRequiresActualNativeBackendCaps()throws Exception{
        var extended=new RatisDatabaseBackupDriver.Policy(Duration.ofHours(1),Duration.ofMinutes(50),4);assertEquals(Duration.ofHours(1),extended.retention());
        try(var fixture=new RatisDatabaseBackupDriverFixture(root.resolve("caps"),1,1)){assertThrows(IOException.class,()->fixture.requireSupported(fixture.definition,extended));assertEquals(0,fixture.physical.get());assertTrue(fixture.pools.stream().allMatch(p->p.gateChecks==0));}
    }
    @Test void everyPostCapturePhaseRejectsChecksummedMissingGlobalVector()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1024);Path path=root.resolve("missing-vector");
        try(var journal=RatisDatabaseBackupJournal.create(path,f.definition,f.destination,policy)){journal.freezeRequests(List.of(request(f)));}
        byte[] original=SessionJournalFiles.read(path,RatisDatabaseBackupJournal.MAX_BYTES);
        for(var phase:List.of(RatisDatabaseBackupJournal.Phase.CAPTURED,RatisDatabaseBackupJournal.Phase.REPORTED,RatisDatabaseBackupJournal.Phase.RELEASED,RatisDatabaseBackupJournal.Phase.MATERIALIZING,RatisDatabaseBackupJournal.Phase.COMPLETE)){
            byte[] changed=original.clone();ByteBuffer.wrap(changed).putInt(68+f.definition.encode().length,phase.ordinal());SessionJournalFiles.write(path,changed,RatisDatabaseBackupJournal.MAX_BYTES);
            assertThrows(IOException.class,()->RatisDatabaseBackupJournal.reopen(path,f.definition,f.destination,policy));
        }
    }
    @Test void renameBeforeDirectoryForceRequiresExactReforceBeforeReopenExposesVector()throws Exception{
        var f=new RatisDatabaseBackupFixture(1,1024);Path path=root.resolve("reforce");var calls=new java.util.concurrent.atomic.AtomicInteger();var denied=new java.util.concurrent.atomic.AtomicBoolean(true);
        RatisDatabaseBackupJournal.Force io=new RatisDatabaseBackupJournal.Force(){
            public void write(Path target,byte[] bytes)throws IOException{
                if(ByteBuffer.wrap(bytes).getInt(68+f.definition.encode().length)==RatisDatabaseBackupJournal.Phase.CAPTURED.ordinal()){
                    Path temporary=target.resolveSibling("owned-rename-stage");SessionJournalFiles.write(temporary,bytes,RatisDatabaseBackupJournal.MAX_BYTES);Files.move(temporary,target,StandardCopyOption.ATOMIC_MOVE,StandardCopyOption.REPLACE_EXISTING);throw new IOException("Directory force outcome unknown after rename");
                }SessionJournalFiles.write(target,bytes,RatisDatabaseBackupJournal.MAX_BYTES);
            }
            @Override public void reforce(Path target)throws IOException{calls.incrementAndGet();if(denied.get())throw new IOException("Directory reforce rejected");RatisDatabaseBackupJournal.Force.super.reforce(target);}
        };
        try(var journal=new RatisDatabaseBackupJournal(path,f.definition,f.destination,policy,false,io)){
            journal.freezeRequests(List.of(request(f)));assertThrows(IOException.class,()->journal.captured(f.held));assertThrows(IOException.class,journal::snapshot);
        }
        assertThrows(IOException.class,()->new RatisDatabaseBackupJournal(path,f.definition,f.destination,policy,true,io));assertEquals(1,calls.get());denied.set(false);
        try(var journal=new RatisDatabaseBackupJournal(path,f.definition,f.destination,policy,true,io)){assertEquals(2,calls.get());assertEquals(RatisDatabaseBackupJournal.Phase.CAPTURED,journal.snapshot().phase());}
    }

}
