package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native journals and Ratis dispatch; no quorum or server lifecycle acceptance. */
class RatisHomeJournalRetirementTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisTransactionRecoveryTest fixtures=new RatisTransactionRecoveryTest();
    RatisTransactionRecoveryTest.Fixture fixture(String name) throws Exception { fixtures.root=root; return fixtures.new Fixture(name); }
    OperationId control(long n) { return new OperationId(new UUID(0,2990),0,n); }
    long writes(RatisTransactionRecoveryTest.Fixture f) { return f.home.wire.index+f.data.wire.index; }
    void consumeAndRetire(RatisTransactionRecoveryTest.Fixture f,ParticipantOutcome outcome,TransactionStatus acknowledged) throws Exception {
        var consumed=acknowledged.consume();
        f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,2991),2,0),acknowledged,consumed));
        f.data.wire.command(f.plan.intent().route(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
        f.data.f.store.applyParticipantRetirement(new ApplyPosition(1,++f.data.wire.index),new ParticipantRetirement(outcome.finalization(),outcome.position(),consumed));
        var participant=f.data.routed.participantFence(new ParticipantIdentity(f.home.f.groupId,1,f.data.f.groupId,f.plan.initial())); assertNotNull(participant);
        var route=f.plan.registration().descriptor();
        f.home.wire.command(route,DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(f.home.f.admin,2,0),2));
        f.home.statuses.retire(new HomeRetirement(f.home.statuses.identity(f.plan.initial(),route),consumed,List.of(participant)));
        assertNull(f.home.statuses.read(f.plan.initial().transactionId()));
    }
    ParticipantOutcome finalizeForeign(RatisTransactionRecoveryTest.Fixture f,TransactionStatus terminal) throws Exception {
        var finalization=new ParticipantFinalization(f.home.f.groupId,1,f.data.f.groupId,control(0),terminal);
        f.data.routed.executeFinalization(f.data.routed.restoreFinalization(finalization,f.plan.intent().route()));
        var outcome=f.data.routed.finalizedParticipant(f.data.f.groupId,f.plan.initial().transactionId());
        f.home.statuses.executeRecovery(f.home.statuses.prepareRecovery(control(1),terminal,terminal.finalized(f.data.f.groupId),outcome));
        return outcome;
    }
    void assertFullExpired(RatisTransactionRecoveryTest.Fixture f,TransactionStatus saved) throws Exception {
        long writes=writes(f);
        try (var recovery=f.recovery()) {
            var step=recovery.step(); assertEquals(RatisTransactionRecovery.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
            assertEquals(step,recovery.step());
        }
        assertNotNull(f.journal.snapshot().homeRetirementProof()); assertEquals(saved,f.journal.snapshot().observedHome());
        assertThrows(RatisHomeRetiredException.class,f.journal::executeHome);
        assertEquals(writes,writes(f));
    }

    @Test void neverRegisteredExpiredHomeCannotAllocateFreshRegistrationOrInventAStatus() throws Exception {
        try (var f=fixture("absent-home")) {
            f.pin.close(); var original=DistributedCommandCodec.encode(f.journal.snapshot().pendingHome().command());
            var route=f.plan.registration().descriptor();
            f.home.wire.command(route,DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(f.home.f.admin,2,0),2));
            f.reopen(); assertFullExpired(f,null); assertNull(f.journal.snapshot().homeResult()); assertNull(f.journal.snapshot().recoveryReceipt());
            assertArrayEquals(original,DistributedCommandCodec.encode(f.journal.snapshot().pendingHome().command()));
            assertThrows(RatisHomeRetiredException.class,() -> f.journal.queueHome(f.plan.registration()));
            assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
            f.reopen(); assertFullExpired(f,null);
            long writes=writes(f);
            try (var coordinator=new RatisTransactionCoordinator(f.journal,f.home.statuses,f.data.routed,
                    last -> { throw new AssertionError("Expired HOME must not allocate registration"); },
                    (tx,p) -> { throw new AssertionError("Expired HOME must not prepare effects"); })) {
                var step=coordinator.step(); assertEquals(RatisTransactionCoordinator.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
            }
            assertEquals(writes,writes(f)); assertNull(f.home.statuses.read(f.plan.initial().transactionId()));
            assertNull(f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()));
        }
    }

    @Test void consumedHomeGcReopensBothOldJournalsAsExpiredWithoutFabricatedCompletion() throws Exception {
        try (var f=fixture("consumed-home")) {
            var commit=f.commit(false); var localPath=root.resolve("consumed-home-local.bin");
            try (var local=RatisDurableParticipantRecovery.create(localPath,f.home.statuses,f.data.routed,
                    f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()),f.plan.intent().route())) {
                var outcome=finalizeForeign(f,commit); consumeAndRetire(f,outcome,commit.finalized(f.data.f.groupId));
            }
            f.reopen(); assertFullExpired(f,commit);
            assertNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
            f.reopen(); assertFullExpired(f,commit); long writes=writes(f);
            for (int n=0;n<2;n++) try (var local=RatisDurableParticipantRecovery.reopen(localPath,f.home.statuses,f.data.routed);
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) {
                var step=recovery.step(); assertEquals(RatisParticipantRecovery.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
                assertNotNull(local.snapshot().homeRetirementProof()); assertEquals(commit,local.snapshot().observedHome());
                assertNull(local.snapshot().outcome()); assertNull(local.snapshot().finalizationReceipt()); assertNull(local.snapshot().recoveryReceipt());
            }
            assertEquals(writes,writes(f)); assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2}));
        }
    }

    @Test void missingRawHomeWithoutAtomicFenceRemainsAnErrorForBothJournals() throws Exception {
        try (var f=fixture("ambiguous-home")) {
            var commit=f.commit(false); var path=root.resolve("ambiguous-home-local.bin");
            try (var local=RatisDurableParticipantRecovery.create(path,f.home.statuses,f.data.routed,
                    f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()),f.plan.intent().route())) {}
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { return null; }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Ambiguous HOME must not write"); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return f.home.wire.transaction(route,bytes,fresh); }
            };
            try (var routed=new RatisRoutedClient(f.home.directory,transport)) {
                var homes=new RatisTransactionStatuses(routed,f.home.f.column); f.journal.close(); f.journal=RatisDurableTransaction.reopen(f.path,homes,f.data.routed);
                long writes=writes(f); var failure=assertThrows(IOException.class,f.journal::refreshHome); assertFalse(failure instanceof RatisHomeRetiredException);
                assertNull(f.journal.snapshot().homeRetirementProof()); assertEquals(commit,f.journal.snapshot().observedHome());
                try (var local=RatisDurableParticipantRecovery.reopen(path,homes,f.data.routed)) {
                    var localFailure=assertThrows(IOException.class,local::refreshHome); assertFalse(localFailure instanceof RatisHomeRetiredException);
                    assertNull(local.snapshot().homeRetirementProof()); assertEquals(commit,local.snapshot().observedHome());
                }
                assertEquals(writes,writes(f));
            }
        }
    }

    @Test void reusedTransactionUuidDoesNotHideOldHomeExpiryOrMutateTheNewBusiness() throws Exception {
        try (var f=fixture("reused-home")) {
            var commit=f.commit(false); var localPath=root.resolve("reused-home-local.bin");
            try (var local=RatisDurableParticipantRecovery.create(localPath,f.home.statuses,f.data.routed,
                    f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()),f.plan.intent().route())) {
                var outcome=finalizeForeign(f,commit); consumeAndRetire(f,outcome,commit.finalized(f.data.f.groupId));
            }
            var original=f.plan.initial();
            var reused=TransactionStatus.begin(original.clusterId(), original.databaseId(), original.transactionId(), new OperationId(new UUID(0,2998),2,0), original.domains(), original.planDigest(), original.participants());
            assertEquals(CommandResult.Status.SUCCESS,f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,2999),2,0),null,reused)).status());
            f.reopen(); assertFullExpired(f,commit); long writes=writes(f);
            for (int n=0;n<2;n++) try (var local=RatisDurableParticipantRecovery.reopen(localPath,f.home.statuses,f.data.routed);
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) {
                var step=recovery.step(); assertEquals(RatisParticipantRecovery.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
                assertNotNull(local.snapshot().homeRetirementProof()); assertEquals(commit,local.snapshot().observedHome());
            }
            f.reopen(); assertFullExpired(f,commit); assertEquals(writes,writes(f));
            assertEquals(reused,f.home.statuses.read(original.transactionId())); assertFalse(reused.consumed());
            assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
        }
    }

    @Test void previouslyProvenCompleteBusinessResultRemainsExactAfterHomeGc() throws Exception {
        try (var f=fixture("cached-home")) {
            f.commit(false); try (var recovery=f.recovery()) { fixtures.drain(f,recovery); }
            var saved=f.journal.snapshot().observedHome(); var expected=f.journal.result().encode();
            var outcome=f.journal.snapshot().participant(f.data.f.groupId).outcome(); consumeAndRetire(f,outcome,saved);
            f.reopen(); assertFullExpired(f,saved); assertArrayEquals(expected,f.journal.result().encode());
            assertEquals(outcome,f.journal.snapshot().participant(f.data.f.groupId).outcome());
            f.reopen(); assertFullExpired(f,saved); assertArrayEquals(expected,f.journal.result().encode());
        }
    }

    @Test void previouslyProvenFencedAbortResultSurvivesHomeGcAndReopen() throws Exception {
        try (var f=fixture("cached-fenced-abort-home")) {
            f.journal.executeHome(); f.pin.close();
            f.data.wire.command(f.plan.intent().route(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
            try (var recovery=f.recovery()) { fixtures.drain(f,recovery); }
            var progress=f.journal.snapshot().participant(f.data.f.groupId); assertNotNull(progress.retirementProof()); assertNull(progress.outcome());
            var expected=f.journal.result().encode(); assertEquals(CommandResult.Status.CONFLICT,f.journal.result().status());
            var acknowledged=f.journal.snapshot().observedHome(); var consumed=acknowledged.consume();
            f.journal.queueRecovery(consumed); f.journal.executeHome();
            var route=f.plan.registration().descriptor();
            f.home.wire.command(route,DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(f.home.f.admin,2,0),2));
            f.home.statuses.retire(new HomeRetirement(f.home.statuses.identity(f.plan.initial(),route),consumed,List.of(progress.retirementProof())));
            assertNull(f.home.statuses.read(f.plan.initial().transactionId()));
            for (int n=0;n<2;n++) {
                f.reopen(); assertFullExpired(f,consumed); assertArrayEquals(expected,f.journal.result().encode());
                assertNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).retirementProof());
            }
            assertNull(f.data.routed.finalizedParticipant(f.data.f.groupId,f.plan.initial().transactionId()));
            assertNull(f.data.routed.read(f.data.f.column,new byte[]{2}));
        }
    }

    @Test void unknownProtectedAbortRetainsExactRequestAndForcesHomeExpiryBeforeExposure() throws Exception {
        try (var f=fixture("unknown-home-abort")) {
            f.journal.executeHome(); var saved=f.journal.snapshot().observedHome(); var abort=saved.decide(TransactionStatus.Phase.ABORT);
            f.journal.queueRecovery(abort); f.home.wire.lose=true; assertThrows(IOException.class,f.journal::executeHome);
            var pending=f.journal.snapshot().pendingRecovery(); assertNull(f.journal.snapshot().homeResult());
            var outcome=finalizeForeign(f,abort); consumeAndRetire(f,outcome,abort.finalized(f.data.f.groupId)); f.reopen();
            var retired=assertThrows(RatisHomeRetiredException.class,f.journal::executeHome);
            assertEquals(retired.proof(),f.journal.snapshot().homeRetirementProof()); assertEquals(saved,f.journal.snapshot().observedHome());
            assertNull(f.journal.snapshot().homeResult()); assertNull(f.journal.snapshot().recoveryReceipt());
            assertArrayEquals(pending.bytes(),f.journal.snapshot().pendingRecovery().bytes()); assertEquals(pending.descriptor(),f.journal.snapshot().pendingRecovery().descriptor());
            f.reopen(); assertFullExpired(f,saved); assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
            assertArrayEquals(pending.bytes(),f.journal.snapshot().pendingRecovery().bytes());
        }
    }

    @Test void unknownLocalAckRetainsItsActualOutcomeAndExactPendingBytesAfterHomeGc() throws Exception {
        try (var f=fixture("unknown-home-ack")) {
            var commit=f.commit(false); var path=root.resolve("unknown-home-local.bin"); RatisRoutedClient.PreparedHomeRecoveryOperation pending; ParticipantOutcome outcome;
            try (var local=RatisDurableParticipantRecovery.create(path,f.home.statuses,f.data.routed,
                    f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()),f.plan.intent().route());
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) {
                assertEquals(RatisParticipantRecovery.Action.FINALIZED,recovery.step().action());
                f.home.wire.lose=true; assertThrows(IOException.class,recovery::step);
                pending=local.snapshot().pendingRecovery(); outcome=local.snapshot().outcome();
                consumeAndRetire(f,outcome,commit.finalized(f.data.f.groupId));
            }
            try (var local=RatisDurableParticipantRecovery.reopen(path,f.home.statuses,f.data.routed)) {
                var retired=assertThrows(RatisHomeRetiredException.class,local::executeHome);
                assertEquals(retired.proof(),local.snapshot().homeRetirementProof()); assertEquals(commit,local.snapshot().observedHome());
                assertEquals(outcome,local.snapshot().outcome()); assertNull(local.snapshot().recoveryReceipt()); assertNull(local.snapshot().homeResult());
                assertArrayEquals(pending.bytes(),local.snapshot().pendingRecovery().bytes());
            }
            long writes=writes(f);
            try (var local=RatisDurableParticipantRecovery.reopen(path,f.home.statuses,f.data.routed);
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) {
                var step=recovery.step(); assertEquals(RatisParticipantRecovery.Action.HOME_RETIRED,step.action()); assertTrue(step.complete());
                assertArrayEquals(pending.bytes(),local.snapshot().pendingRecovery().bytes());
                assertThrows(RatisHomeRetiredException.class,() -> local.queueRecovery(commit.finalized(f.data.f.groupId)));
            }
            assertEquals(writes,writes(f));
        }
    }
}
