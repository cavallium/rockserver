package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Recovery takeover using controlled Ratis dispatch/native storage, not quorum/process acceptance. */
class RatisTransactionRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisDurableTransactionTest journals=new RatisDurableTransactionTest();
    final RatisParticipantPrepareTest helper=journals.helper;
    final class Fixture implements AutoCloseable {
        final RatisParticipantPrepareTest.Node home,data;
        final it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.ScopedHistoryPin pin;
        final RatisDurableTransactionTest.Plan plan; final Path path;
        RatisDurableTransaction journal;
        Fixture(String name) throws Exception {
            helper.root=root; home=helper.new Node(name+"-home",true); data=helper.new Node(name+"-data",false);
            pin=data.f.store.captureTracked(helper.scope(data,1,4)); plan=journals.plan(home,data,pin); path=root.resolve(name+".bin"); journal=journals.create(path,home,data,plan);
        }
        void reopen() throws Exception { journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,RatisTransactionBindings.of(data.routed)); }
        RatisTransactionRecovery recovery() throws IOException { return new RatisTransactionRecovery(journal,home.statuses,RatisTransactionBindings.of(data.routed),RatisTransactionRecoveryTest::next); }
        TransactionStatus commit(boolean lose) throws Exception {
            journal.executeHome();
            try (var token=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,plan.initial().transactionId())) { journal.freezePreparation(data.routed.prepareParticipant(token.preparation())); journal.executePreparation(data.f.groupId); }
            var vote=journal.snapshot().participant(data.f.groupId).vote(); var ready=plan.initial().prepared(data.f.groupId,plan.intent().acceptedBusiness().payloadDigest(),vote.position()); journal.queueHome(home.statuses.prepare(journals.homeOperation(1),plan.initial(),ready)); journal.executeHome();
            var commit=ready.decide(TransactionStatus.Phase.COMMIT); journal.queueHome(home.statuses.prepare(journals.homeOperation(2),ready,commit)); home.wire.lose=lose;
            if (lose) assertThrows(IOException.class,journal::executeHome); else journal.executeHome(); return commit;
        }
        public void close() throws Exception { try { journal.close(); } finally { try { pin.close(); } finally { try { data.close(); } finally { home.close(); } } } }
    }
    static OperationId next(OperationId last) { return new OperationId(last.sessionId(),last.epoch(),Math.addExact(last.sequence(),1)); }
    void drain(Fixture f,RatisTransactionRecovery recovery) throws Exception {
        for (int n=0;n<32;n++) { long before=f.home.wire.index+f.data.wire.index; var step=recovery.step(); assertTrue(f.home.wire.index+f.data.wire.index-before<=1,"Recovery step may issue at most one transaction write"); if (step.complete()) return; }
        fail("Recovery did not complete its bounded fixture");
    }
    @Test void aReopenedUnregisteredIntentIsRegisteredAbortedFinalizedAndAckedWithoutConsumption() throws Exception {
        try (var f=new Fixture("unregistered")) {
            f.pin.close(); f.reopen();
            try (var recovery=f.recovery()) { drain(f,recovery); assertTrue(recovery.step().complete()); }
            var home=f.home.statuses.read(f.plan.initial().transactionId()); assertEquals(TransactionStatus.Phase.ABORT,home.phase()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed());
            assertEquals(f.plan.initial().operation(),home.operation()); assertEquals(0,f.data.f.store.preparedParticipantCount()); assertNull(f.data.routed.read(f.data.f.column,new byte[]{2})); helper.released(f.data);
            f.reopen(); assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).outcome());
        }
    }
    @Test void neverPreparedExpiredBusinessAbortsAndAcknowledgesWithDurableFenceOnly() throws Exception {
        try (var f=new Fixture("expired-never-prepared")) {
            f.journal.executeHome(); f.pin.close();
            var preparation=new PreparedParticipant.Preparation(f.home.f.groupId,1,f.plan.intent().prepareControl(),f.plan.initial(),f.plan.intent().acceptedBusiness());
            var original=f.data.routed.restoreParticipant(preparation,f.plan.intent().route());
            var floor=DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2);
            f.data.wire.command(f.plan.intent().route(),floor); f.reopen();
            try (var recovery=f.recovery()) { drain(f,recovery); }
            var progress=f.journal.snapshot().participant(f.data.f.groupId);
            assertNotNull(progress.retirementProof()); assertNull(progress.vote()); assertNull(progress.outcome()); assertNull(progress.finalizationReceipt());
            assertTrue(f.journal.snapshot().observedHome().fullyFinalized()); assertFalse(f.journal.snapshot().observedHome().consumed());
            assertEquals(CommandResult.Status.CONFLICT,f.journal.result().status());
            assertTrue(f.journal.snapshot().pendingRecovery().recovery().proof().fenced());
            assertNull(f.data.routed.finalizedParticipant(f.data.f.groupId,f.plan.initial().transactionId()));
            f.reopen(); long writes=f.home.wire.index+f.data.wire.index;
            try (var recovery=f.recovery()) { assertTrue(recovery.step().complete()); }
            assertEquals(writes,f.home.wire.index+f.data.wire.index);
            var acknowledged=f.journal.snapshot().observedHome();
            f.journal.queueRecovery(acknowledged.consume()); f.journal.executeHome();
            f.reopen(); assertTrue(f.journal.snapshot().observedHome().gcEligible()); assertEquals(CommandResult.Status.CONFLICT,f.journal.result().status());
            var retired=f.data.routed.executeParticipant(original); assertTrue(retired.retired());
            assertNull(f.data.routed.preparedParticipant(f.data.f.groupId,f.plan.initial().transactionId()));
            assertNull(f.data.routed.finalizedParticipant(f.data.f.groupId,f.plan.initial().transactionId()));
            assertNull(f.data.routed.read(f.data.f.column,new byte[]{2}));
        }
    }
    @Test void consumedCommitRetirementReopensBothJournalsWithoutInventingNativeResults() throws Exception {
        try (var f=new Fixture("consumed-retirement")) {
            var commit=f.commit(false); var transaction=f.plan.initial().transactionId();
            var localPath=root.resolve("consumed-local.bin");
            try (var local=RatisDurableParticipantRecovery.create(localPath,f.home.statuses,f.data.routed,
                    f.data.routed.preparedParticipant(f.data.f.groupId,transaction),f.plan.intent().route())) {
                var foreign=new OperationId(new UUID(0,1991),0,0);
                var finalization=new ParticipantFinalization(f.home.f.groupId,1,f.data.f.groupId,foreign,commit);
                var frozen=f.data.routed.restoreFinalization(finalization,f.plan.intent().route());
                var position=f.data.routed.executeFinalization(frozen);
                var nativeOutcome=f.data.routed.finalizedParticipant(f.data.f.groupId,transaction);
                var acknowledged=commit.finalized(f.data.f.groupId);
                f.home.statuses.executeRecovery(f.home.statuses.prepareRecovery(new OperationId(new UUID(0,1992),0,0),commit,acknowledged,nativeOutcome));
                var consumed=acknowledged.consume();
                f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,1993),2,0),acknowledged,consumed));
                f.data.wire.command(f.plan.intent().route(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
                f.data.f.store.applyParticipantRetirement(new ApplyPosition(1,++f.data.wire.index),new ParticipantRetirement(finalization,position,consumed));
                assertNull(f.data.routed.preparedParticipant(f.data.f.groupId,transaction)); assertNull(f.data.routed.finalizedParticipant(f.data.f.groupId,transaction));
                assertThrows(RatisParticipantRetiredException.class,() -> f.data.routed.executeFinalization(frozen));
            }
            f.reopen(); long writes=f.home.wire.index+f.data.wire.index;
            try (var recovery=f.recovery()) { assertTrue(recovery.step().complete()); }
            var progress=f.journal.snapshot().participant(f.data.f.groupId);
            assertNotNull(progress.retirementProof()); assertNull(progress.outcome()); assertNull(progress.finalizationReceipt());
            assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
            f.reopen(); assertThrows(RatisTransactionOutcomeExpiredException.class,f.journal::result);
            try (var local=RatisDurableParticipantRecovery.reopen(localPath,f.home.statuses,f.data.routed);
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) {
                assertTrue(recovery.step().complete()); assertNotNull(local.snapshot().retirementProof()); assertNull(local.snapshot().outcome()); assertNull(local.snapshot().finalizationReceipt());
            }
            try (var local=RatisDurableParticipantRecovery.reopen(localPath,f.home.statuses,f.data.routed);
                    var recovery=new RatisParticipantRecovery(local,f.home.statuses,f.data.routed,false)) { assertTrue(recovery.step().complete()); }
            assertEquals(writes,f.home.wire.index+f.data.wire.index); assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2}));
        }
    }
    @Test void cachedNativeCommitResultSurvivesAuthorizedRetirement() throws Exception {
        try (var f=new Fixture("cached-consumed-retirement")) {
            f.commit(false); try (var recovery=f.recovery()) { drain(f,recovery); }
            var outcome=f.journal.snapshot().participant(f.data.f.groupId).outcome(); var expected=f.journal.result().encode();
            var home=f.home.statuses.read(f.plan.initial().transactionId()); var consumed=home.consume();
            f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,1994),2,0),home,consumed));
            f.data.wire.command(f.plan.intent().route(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
            f.data.f.store.applyParticipantRetirement(new ApplyPosition(1,++f.data.wire.index),new ParticipantRetirement(outcome.finalization(),outcome.position(),consumed));
            f.reopen(); try (var recovery=f.recovery()) { assertTrue(recovery.step().complete()); }
            assertEquals(outcome,f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertArrayEquals(expected,f.journal.result().encode());
        }
    }
    @Test void abandonedReadyPrepareDoesNotLeaveAVolatileScopeAfterRecoveredAbort() throws Exception {
        try (var f=new Fixture("ready")) {
            f.journal.executeHome();
            try (var abandoned=f.data.f.machine.prepareParticipant(f.pin,f.data.f.put(2),helper.control(0),f.home.statuses,f.plan.initial().transactionId())) {
                var original=f.data.routed.prepareParticipant(abandoned.preparation()); f.journal.freezePreparation(original); assertEquals(1,f.data.f.machine.proposalFootprintStats().candidates()); f.reopen();
                try (var recovery=f.recovery()) { drain(f,recovery); }
                assertEquals(0,f.data.f.machine.proposalFootprintStats().candidates()); assertThrows(IOException.class,() -> f.data.routed.executeParticipant(original));
                var write=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(0,1900),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9})));
                assertEquals(CommandResult.Status.SUCCESS,f.data.wire.command(f.data.directory.ranges().getFirst(),write).status()); assertArrayEquals(new byte[]{9},f.data.routed.read(f.data.f.column,new byte[]{2})); helper.released(f.data);
            }
        }
    }
    @Test void lostCommitReplyIsSupersededByItsObservedDecisionWithoutInventingAnExactReceipt() throws Exception {
        try (var f=new Fixture("commit")) {
            var commit=f.commit(true); long homeIndex=f.home.wire.index; f.reopen();
            try (var recovery=f.recovery()) { assertEquals(RatisTransactionRecovery.Action.FINALIZED,recovery.step().action()); }
            assertTrue(f.journal.snapshot().homeSuperseded()); assertNull(f.journal.snapshot().homeResult()); assertThrows(IllegalStateException.class,f.journal::executeHome); f.reopen();
            try (var recovery=f.recovery()) { drain(f,recovery); }
            assertEquals(homeIndex+1,f.home.wire.index,"Recovery only adds its finalization ACK, not another COMMIT retry");
            assertEquals(TransactionStatus.Phase.COMMIT,f.journal.snapshot().observedHome().phase()); assertFalse(f.journal.snapshot().observedHome().consumed());
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); assertArrayEquals(f.plan.intent().acceptedBusiness().acceptance().result(),f.journal.snapshot().participant(f.data.f.groupId).outcome().result()); assertTrue(commit.sameDefinition(f.journal.snapshot().observedHome())); helper.released(f.data);
        }
    }
    @Test void anotherCoordinatorsWinningFinalizationIsAProofWithoutClaimingOurRpcSucceeded() throws Exception {
        try (var f=new Fixture("foreign")) {
            var commit=f.commit(false); var foreign=new OperationId(new UUID(0,1901),4,0);
            try (var token=f.data.f.machine.prepareFinalization(f.home.statuses,f.plan.initial().transactionId(),foreign)) { f.data.routed.executeFinalization(f.data.routed.prepareFinalization(token.finalization())); }
            var ack=commit.finalized(f.data.f.groupId); f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,1902),2,0),commit,ack));
            f.reopen(); long writes=f.home.wire.index+f.data.wire.index;
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,RatisTransactionBindings.of(f.data.routed),RatisTransactionRecoveryTest::next)) { assertTrue(recovery.step().complete()); }
            var progress=f.journal.snapshot().participant(f.data.f.groupId); assertEquals(foreign,progress.outcome().finalization().operation()); assertNull(progress.finalization()); assertNull(progress.finalizationReceipt()); assertEquals(writes,f.home.wire.index+f.data.wire.index);
        }
    }
    @Test void aCompetingCommitWinsAnAbortCasAndRecoveryFollowsCommit() throws Exception {
        try (var f=new Fixture("race")) {
            f.journal.executeHome();
            try (var token=f.data.f.machine.prepareParticipant(f.pin,f.data.f.put(2),helper.control(0),f.home.statuses,f.plan.initial().transactionId())) { f.journal.freezePreparation(f.data.routed.prepareParticipant(token.preparation())); f.journal.executePreparation(f.data.f.groupId); }
            var vote=f.journal.snapshot().participant(f.data.f.groupId).vote(); var ready=f.plan.initial().prepared(f.data.f.groupId,f.plan.intent().acceptedBusiness().payloadDigest(),vote.position()); f.journal.queueHome(f.home.statuses.prepare(journals.homeOperation(1),f.plan.initial(),ready)); f.journal.executeHome();
            var raced=new java.util.concurrent.atomic.AtomicBoolean();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return f.home.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return f.home.wire.command(route,command); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.HOME_RECOVERY_PREPARE && !raced.getAndSet(true)) f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,1903),2,0),ready,ready.decide(TransactionStatus.Phase.COMMIT)));
                    return f.home.wire.transaction(route,bytes,fresh);
                }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return f.home.wire.participant(route,bytes); }
            };
            try (var routed=new RatisRoutedClient(f.home.directory,transport)) {
                var racedHomes=new RatisTransactionStatuses(routed,f.home.f.column); f.journal.close(); f.journal=RatisDurableTransaction.reopen(f.path,racedHomes,RatisTransactionBindings.of(f.data.routed));
                try (var recovery=new RatisTransactionRecovery(f.journal,racedHomes,RatisTransactionBindings.of(f.data.routed),RatisTransactionRecoveryTest::next)) {
                assertEquals(RatisTransactionRecovery.Action.ABORT_ATTEMPTED,recovery.step().action()); assertEquals(CommandResult.Status.CONFLICT,f.journal.snapshot().homeResult().status()); assertEquals(TransactionStatus.Phase.COMMIT,f.home.statuses.read(f.plan.initial().transactionId()).phase()); drain(f,recovery);
                }
            }
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); assertEquals(TransactionStatus.Phase.COMMIT,f.journal.snapshot().observedHome().phase());
        }
    }
    @Test void recoveryAbortDoesNotNeedOrdinaryControlGrantAndFailedRemoteAuthorizationCannotDispatch() throws Exception {
        try (var f=new Fixture("failures")) {
            f.journal.executeHome();
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,RatisTransactionBindings.of(f.data.routed),last -> { throw new AssertionError("Protected ABORT must not allocate ordinary control"); })) { assertEquals(RatisTransactionRecovery.Action.ABORT_ATTEMPTED,recovery.step().action()); }
            assertEquals(TransactionStatus.Phase.ABORT,f.home.statuses.read(f.plan.initial().transactionId()).phase()); assertEquals(0,f.data.wire.index);
            assertEquals(0,f.journal.snapshot().pendingRecovery().recovery().operation().epoch()); assertNotNull(f.journal.snapshot().recoveryReceipt());
            f.data.wire.beforeFinalization=() -> { throw new IllegalStateException("Qualified finalization RPC unavailable"); };
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,RatisTransactionBindings.of(f.data.routed),RatisTransactionRecoveryTest::next)) { assertThrows(IOException.class,recovery::step); }
            f.data.wire.beforeFinalization=null;
            assertEquals(0,f.data.wire.index); assertEquals(0,f.data.f.machine.protectedFinalizationSlots()); assertEquals(0,f.data.f.machine.reservedFinalizationCount());
            try (var recovery=f.recovery()) { drain(f,recovery); }
        }
    }
    @Test void aSubmittedUnknownFinalizerReattachesItsExactAcceptanceWithoutAnotherOwner() throws Exception {
        try (var f=new Fixture("unknown-finalize")) {
            f.commit(false);
            try (var token=f.data.f.machine.prepareFinalization(f.home.statuses,f.plan.initial().transactionId(),f.plan.intent().finalizeControl())) {
                var operation=f.data.routed.prepareFinalization(token.finalization()); f.journal.freezeFinalization(operation);
                var pending=f.data.wire.propose(operation.bytes()); assertNull(pending.getException()); f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,2); f.data.f.machine.cancelTransaction(pending);
                assertNull(f.data.f.store.finalizedParticipant(f.plan.initial().transactionId())); f.reopen();
                try (var recovery=f.recovery()) { assertEquals(RatisTransactionRecovery.Action.FINALIZED,recovery.step().action()); drain(f,recovery); }
                assertArrayEquals(operation.bytes(),f.journal.snapshot().participant(f.data.f.groupId).finalization().bytes()); assertEquals(new ApplyPosition(1,2),f.journal.snapshot().participant(f.data.f.groupId).outcome().position());
                assertEquals(0,f.data.f.machine.protectedFinalizationSlots()); helper.released(f.data);
            }
        }
    }

    @Test void anUnavailableParticipantDoesNotStarveAnotherGroupsFinalizationAndAck() throws Exception {
        helper.root=root; UUID column=new UUID(0,1970),domain=new UUID(0,1971); byte[] split={(byte)128};
        try (var home=helper.new Node("multi-home",true); var left=helper.new Node("multi-left",false,new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,Set.of(column),null,split)); var right=helper.new Node("multi-right",false,new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,Set.of(column),split,null))) {
            var lr=left.directory.ranges().getFirst(); var rr=right.directory.ranges().getFirst(); var l=new RangeDescriptor(lr.rangeId(),lr.groupId(),1,0,null,split,RangeDescriptor.TopologyState.ACTIVE,lr.replicas()); var r=new RangeDescriptor(rr.rangeId(),rr.groupId(),1,0,split,null,RangeDescriptor.TopologyState.ACTIVE,rr.replicas());
            var directory=new RangeDirectory(helper.cluster,helper.database,domain,1,new byte[32],0,Set.of(column),0,List.of(l,r)); var unavailable=new java.util.concurrent.atomic.AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport() {
                RatisParticipantPrepareTest.Wire wire(RangeDescriptor route) { return route.groupId().equals(l.groupId()) ? left.wire : right.wire; }
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { return wire(route).read(route,col,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return wire(route).command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return wire(route).participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { if (route.groupId().equals(l.groupId()) && unavailable.get()) throw new IOException("Left participant unavailable"); return wire(route).transaction(route,bytes,fresh); }
            };
            var business=new OperationId(new UUID(0,1972),1,0); var lb=new DistributedCommand(l.groupId(),1,new byte[32],business,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7}))); var rb=new DistributedCommand(r.groupId(),1,new byte[32],business,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)130},new byte[]{8})));
            try (var lp=left.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})))); var rp=right.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)130})))); var routed=new RatisRoutedClient(directory,transport)) {
                var la=left.f.machine.participantIntent(lp,lb); var ra=right.f.machine.participantIntent(rp,rb);
                var intents=List.of(new RatisDurableTransaction.Intent(l,la,helper.control(0),new OperationId(new UUID(0,1974),3,0),List.of()),new RatisDurableTransaction.Intent(r,ra,helper.control(1),new OperationId(new UUID(0,1975),3,0),List.of()));
                var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,1973), business, List.of(DomainBinding.of(directory)), RatisDurableTransaction.planDigest(intents), List.of(new TransactionStatus.Participant(domain, l.rangeId(), l.groupId(), 1, new byte[32], la.payloadDigest(), null, false),new TransactionStatus.Participant(domain, r.rangeId(), r.groupId(), 1, new byte[32], ra.payloadDigest(), null, false)));
                try (var journal=RatisDurableTransaction.create(root.resolve("multi-recovery.bin"),home.statuses,RatisTransactionBindings.of(routed),initial,home.statuses.prepare(journals.homeOperation(0),null,initial),intents); var recovery=new RatisTransactionRecovery(journal,home.statuses,RatisTransactionBindings.of(routed),RatisTransactionRecoveryTest::next)) {
                    journal.executeHome(); lp.close(); rp.close();
                    for (int n=0;n<12 && !journal.snapshot().observedHome().participants().stream().filter(p -> p.groupId().equals(r.groupId())).findFirst().orElseThrow().finalized();n++) { try { recovery.step(); } catch (IOException expected) { assertTrue(expected.getMessage().contains("Left participant unavailable")); } }
                    assertTrue(journal.snapshot().observedHome().participants().stream().filter(p -> p.groupId().equals(r.groupId())).findFirst().orElseThrow().finalized()); assertNull(journal.snapshot().participant(l.groupId()).outcome()); assertThrows(IllegalStateException.class,journal::result); unavailable.set(false);
                    boolean complete=false; for (int n=0;n<12 && !complete;n++) { long before=home.wire.index+left.wire.index+right.wire.index; complete=recovery.step().complete(); assertTrue(home.wire.index+left.wire.index+right.wire.index-before<=1); }
                    assertTrue(complete); assertEquals(CommandResult.Status.CONFLICT,journal.result().status()); assertEquals(0,journal.result().valueCount()); assertEquals(TransactionStatus.Phase.ABORT,journal.snapshot().observedHome().phase()); assertFalse(journal.snapshot().observedHome().consumed()); assertNull(routed.read(column,new byte[]{1})); assertNull(routed.read(column,new byte[]{(byte)130})); helper.released(left); helper.released(right);
                }
            }
        }
    }

}
