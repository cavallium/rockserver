package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native HOME/DATA and journal recovery, without a running server or network. */
class RatisSnapshotHomeJournalRetirementTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    OperationId control(long sequence) { return new OperationId(new UUID(0,44000),1,sequence); }
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home home;
        final RatisParticipantPrepareTest.Node data;
        final SnapshotFence fence;
        final SnapshotFenceStatus initial;
        final Path path;
        RatisDurableSnapshot journal;
        Fixture(String name) throws Exception {
            helper.root=root; helper.helper.root=root; home=helper.new Home(name+"-home");
            data=helper.helper.new Node(name+"-data",false,null,home.homes,System::nanoTime);
            fence=helper.fence(home,data,81,helper.scope(data,1,4)); initial=SnapshotFenceStatus.begin(fence); path=root.resolve(name+".journal");
            journal=RatisDurableSnapshot.create(path,home.homes,RatisTransactionBindings.of(data.routed),initial,home.homes.prepare(control(0),null,initial),data.directory.ranges());
        }
        void reopen() throws Exception { journal.close(); journal=RatisDurableSnapshot.reopen(path,home.homes,RatisTransactionBindings.of(data.routed)); }
        RatisSnapshotCoordinator driver() throws Exception { return new RatisSnapshotCoordinator(journal,home.homes,RatisTransactionBindings.of(data.routed),last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10)); }
        long writes() { return home.wire.index+data.wire.index; }
        public void close() throws Exception { try { journal.close(); } finally { try { data.close(); } finally { home.close(); } } }
    }
    void floorHome(RatisSnapshotAdmissionTest.Home home) throws Exception {
        home.wire.command(home.routed.directory().ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(home.f.groupId,1,new byte[32],new OperationId(home.f.admin,2,0),2));
    }
    void retire(RatisSnapshotAdmissionTest.Home home,RatisParticipantPrepareTest.Node data,SnapshotFenceStatus consumed,ApplyPosition released) throws Exception {
        data.wire.command(data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,2,0),2));
        data.f.store.applySnapshotGateRetirement(new ApplyPosition(1,++data.wire.index),new SnapshotGateRetirement(consumed,data.f.groupId,released));
        var proof=data.routed.snapshotFence(consumed.fence(),data.f.groupId); assertNotNull(proof); floorHome(home);
        home.homes.retire(new SnapshotHomeRetirement(home.homes.identity(consumed.fence(),home.routed.directory().ranges().getFirst()),consumed,List.of(proof)));
        assertNull(home.homes.read(consumed.fence().fenceId()));
    }
    SnapshotFenceStatus foreignAbortAndConsume(Fixture f) throws Exception {
        var abort=f.initial.abort(); f.home.transition(f.initial,abort);
        var released=f.data.routed.executeSnapshotGate(f.data.routed.prepareSnapshotGate(RatisSnapshotRecord.release(abort,f.data.f.groupId))).position();
        var consumed=abort.consume(); f.home.transition(abort,consumed); retire(f.home,f.data,consumed,released); return consumed;
    }
    byte[] pending(RatisDurableSnapshot journal) throws Exception { return DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()); }
    void assertExpired(Fixture f,SnapshotFenceStatus saved) throws Exception {
        long writes=f.writes();
        try (var driver=f.driver()) {
            var step=driver.step(); assertEquals(RatisSnapshotCoordinator.Action.HOME_RETIRED,step.action()); assertTrue(step.complete()); assertEquals(step,driver.step());
        }
        assertNotNull(f.journal.snapshot().homeRetirementProof()); assertEquals(saved,f.journal.snapshot().observedHome());
        assertThrows(RatisSnapshotHomeRetiredException.class,f.journal::executeHome);
        assertThrows(RatisSnapshotHomeRetiredException.class,() -> f.journal.freezeInstall(f.data.f.groupId));
        assertThrows(RatisSnapshotHomeRetiredException.class,() -> f.journal.consume(control(100)));
        assertEquals(writes,f.writes());
    }

    @Test void neverRegisteredExpiredHomeCannotRegisterOrInventTerminalStatus() throws Exception {
        try (var f=new Fixture("absent")) {
            var original=pending(f.journal); floorHome(f.home); f.reopen(); assertExpired(f,null);
            assertArrayEquals(original,pending(f.journal)); assertNull(f.journal.snapshot().homeResult());
            var progress=f.journal.snapshot().participant(f.data.f.groupId); assertNull(progress.captureToken()); assertNull(progress.releaseReceipt()); assertNull(progress.retirementProof());
            f.reopen(); assertExpired(f,null); assertNull(f.home.homes.read(f.fence.fenceId())); assertNull(f.data.f.store.snapshotGate(f.fence.fenceId()));
        }
    }

    @Test void consumedDataAndHomeGcExpireOldJournalWithoutAdoptingMissingDecision() throws Exception {
        try (var f=new Fixture("consumed")) {
            f.journal.executeHome(); f.journal.freezeInstall(f.data.f.groupId); var original=pending(f.journal);
            foreignAbortAndConsume(f); f.reopen(); assertExpired(f,f.initial);
            assertArrayEquals(original,pending(f.journal)); assertEquals(SnapshotFenceStatus.Phase.INSTALLING,f.journal.snapshot().observedHome().phase());
            assertFalse(f.journal.snapshot().observedHome().consumed()); var progress=f.journal.snapshot().participant(f.data.f.groupId);
            assertNull(progress.nativeGate()); assertNull(progress.installReceipt()); assertNull(progress.releaseReceipt()); assertNull(progress.captureToken());
            f.reopen(); assertExpired(f,f.initial);
        }
    }

    @Test void unknownRegistrationReplyForcesExpiryWithoutReplacingFrozenRequest() throws Exception {
        try (var f=new Fixture("unknown-registration")) {
            f.home.wire.lose=true; assertThrows(IOException.class,f.journal::executeHome); assertNull(f.journal.snapshot().observedHome()); var original=pending(f.journal);
            foreignAbortAndConsume(f); f.reopen();
            var retired=assertThrows(RatisSnapshotHomeRetiredException.class,f.journal::executeHome); assertEquals(retired.proof(),f.journal.snapshot().homeRetirementProof());
            assertNull(f.journal.snapshot().observedHome()); assertNull(f.journal.snapshot().homeResult()); assertArrayEquals(original,pending(f.journal));
            f.reopen(); assertExpired(f,null); assertArrayEquals(original,pending(f.journal));
        }
    }

    @Test void genuinelyAppliedUnknownConsumptionCanRemainHistoricalSuccessBeforeExpiryRefresh() throws Exception {
        try (var f=new Fixture("unknown-consumption")) {
            f.journal.executeHome(); var abort=f.initial.abort(); f.journal.queueHome(f.home.homes.prepare(control(1),f.initial,abort)); f.journal.executeHome();
            var released=f.data.routed.executeSnapshotGate(f.data.routed.prepareSnapshotGate(RatisSnapshotRecord.release(abort,f.data.f.groupId))).position();
            var consume=new OperationId(new UUID(0,44001),2,0); f.journal.consume(consume); f.home.wire.lose=true; assertThrows(IOException.class,f.journal::executeHome);
            var original=pending(f.journal); assertFalse(f.journal.snapshot().observedHome().consumed());
            retire(f.home,f.data,abort.consume(),released); f.reopen();
            assertEquals(CommandResult.Status.SUCCESS,f.journal.executeHome().status()); assertTrue(f.journal.snapshot().observedHome().consumed());
            assertNull(f.journal.snapshot().homeRetirementProof()); assertArrayEquals(original,pending(f.journal));
            assertExpired(f,abort.consume()); f.reopen(); assertExpired(f,abort.consume()); assertArrayEquals(original,pending(f.journal));
        }
    }

    @Test void sameUuidReplacementCannotHideExpiryOrBeAdoptedByOldJournal() throws Exception {
        try (var f=new Fixture("reused")) {
            f.journal.executeHome(); foreignAbortAndConsume(f);
            var old=f.fence; var participant=old.participant(f.data.f.groupId);
            var replacement=new SnapshotFence(old.clusterId(), old.databaseId(), old.fenceId(), old.homeGroup(), old.homeEpoch(), new OperationId(old.operation().sessionId(),2,0), old.domains(), List.of(new SnapshotFence.Participant(participant.domainId(), participant.rangeId(), participant.groupId(), participant.rangeEpoch(), participant.schemaDigest(), new OperationId(participant.installOperation().sessionId(),2,0), new OperationId(participant.releaseOperation().sessionId(),2,1), participant.scope())));
            var next=SnapshotFenceStatus.begin(replacement);
            assertEquals(CommandResult.Status.SUCCESS,f.home.homes.execute(f.home.homes.prepare(new OperationId(new UUID(0,44002),2,0),null,next)).status());
            f.reopen(); assertExpired(f,f.initial); f.reopen(); assertExpired(f,f.initial);
            assertEquals(next,f.home.homes.read(old.fenceId())); assertNull(f.data.f.store.snapshotGate(old.fenceId()));
        }
    }

    @Test void cachedOriginalCaptureAndReleaseRemainAvailableAfterHomeExpiryWithoutRecapture() throws Exception {
        var fixtures=new RatisDurableSnapshotTest(); fixtures.root=root; var path=root.resolve("cached.journal");
        try (var f=fixtures.fixture("cached-home")) {
            var group=f.data.f.groupId; SnapshotFenceStatus saved; RatisRoutedClient.PreparedSnapshotCapture capture; SnapshotFenceStatus.Capture token; ApplyPosition released;
            try (var journal=fixtures.create(path,f); var driver=fixtures.coordinator(journal,f)) {
                boolean complete=false; for (int n=0;n<20 && !complete;n++) complete=driver.step().complete(); assertTrue(complete);
                var progress=journal.snapshot().participant(group); capture=progress.capture(); token=progress.captureToken(); released=progress.releaseReceipt();
                journal.consume(fixtures.control(100)); journal.executeHome(); saved=journal.snapshot().observedHome(); retire(f.home,f.data,saved,released);
            }
            long writes=f.home.wire.index+f.data.wire.index;
            for (int n=0;n<2;n++) try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,RatisTransactionBindings.of(f.client)); var driver=fixtures.coordinator(journal,f)) {
                var step=driver.step(); assertEquals(RatisSnapshotCoordinator.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
                assertEquals(saved,journal.snapshot().observedHome()); var progress=journal.snapshot().participant(group);
                assertEquals(token,progress.captureToken()); assertEquals(released,progress.releaseReceipt()); assertArrayEquals(capture.bytes(),progress.capture().bytes());
                assertThrows(RatisSnapshotHomeRetiredException.class,() -> journal.executeCapture(group)); assertThrows(RatisSnapshotHomeRetiredException.class,() -> journal.executeRelease(group));
                assertArrayEquals(new byte[]{7},f.client.snapshotGet(capture.target(),token,f.data.f.column,new byte[]{2}));
            }
            assertEquals(writes,f.home.wire.index+f.data.wire.index); assertEquals(1,f.data.f.machine.snapshotPinStats().pins());
        }
    }
}
