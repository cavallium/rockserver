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
        void reopen() throws Exception { journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed); }
        RatisTransactionRecovery.Authorization authorize(UUID group,UUID transaction,OperationId control) throws IOException {
            try { assertEquals(data.f.groupId,group); var token=data.f.machine.prepareFinalization(home.statuses,transaction,control); return new RatisTransactionRecovery.Authorization(token.finalization(),token); }
            catch (IOException e) { throw e; } catch (Exception e) { throw new IOException(e); }
        }
        RatisTransactionRecovery recovery() throws IOException { return new RatisTransactionRecovery(journal,home.statuses,data.routed,RatisTransactionRecoveryTest::next,this::authorize); }
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
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(g,t,c) -> { throw new AssertionError("Winning outcome needs no new authority"); })) { assertTrue(recovery.step().complete()); }
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
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
                    if (TransactionStatus.decode(command.mutations().getFirst().value()).phase()==TransactionStatus.Phase.ABORT && !raced.getAndSet(true)) f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,1903),2,0),ready,ready.decide(TransactionStatus.Phase.COMMIT)));
                    return f.home.wire.command(route,command);
                }
            };
            try (var routed=new RatisRoutedClient(f.home.directory,transport)) {
                var racedHomes=new RatisTransactionStatuses(routed,f.home.f.column); f.journal.close(); f.journal=RatisDurableTransaction.reopen(f.path,racedHomes,f.data.routed);
                try (var recovery=new RatisTransactionRecovery(f.journal,racedHomes,f.data.routed,RatisTransactionRecoveryTest::next,f::authorize)) {
                assertEquals(RatisTransactionRecovery.Action.ABORT_ATTEMPTED,recovery.step().action()); assertEquals(CommandResult.Status.CONFLICT,f.journal.snapshot().homeResult().status()); assertEquals(TransactionStatus.Phase.COMMIT,f.home.statuses.read(f.plan.initial().transactionId()).phase()); drain(f,recovery);
                }
            }
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); assertEquals(TransactionStatus.Phase.COMMIT,f.journal.snapshot().observedHome().phase());
        }
    }
    @Test void invalidAuthorityAndControlAllocationFailureCannotDispatchNewRequests() throws Exception {
        try (var f=new Fixture("failures")) {
            f.journal.executeHome();
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,f.data.routed,last -> { throw new IOException("No current control grant"); },f::authorize)) { assertThrows(IOException.class,recovery::step); }
            assertEquals(TransactionStatus.Phase.PREPARING,f.home.statuses.read(f.plan.initial().transactionId()).phase()); assertEquals(0,f.data.wire.index);
            try (var recovery=f.recovery()) { recovery.step(); }
            try (var recovery=new RatisTransactionRecovery(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(g,t,c) -> f.authorize(g,t,new OperationId(new UUID(0,1904),3,0)))) { assertThrows(IOException.class,recovery::step); }
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
                var la=left.f.machine.participantIntent(lp,lb); var ra=right.f.machine.participantIntent(rp,rb); var initial=TransactionStatus.begin(helper.cluster,helper.database,domain,new UUID(0,1973),business,1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(l.rangeId(),l.groupId(),1,new byte[32],la.payloadDigest(),null,false),new TransactionStatus.Participant(r.rangeId(),r.groupId(),1,new byte[32],ra.payloadDigest(),null,false)));
                var intents=List.of(new RatisDurableTransaction.Intent(l,la,helper.control(0),new OperationId(new UUID(0,1974),3,0)),new RatisDurableTransaction.Intent(r,ra,helper.control(1),new OperationId(new UUID(0,1975),3,0)));
                try (var journal=RatisDurableTransaction.create(root.resolve("multi-recovery.bin"),home.statuses,routed,initial,home.statuses.prepare(journals.homeOperation(0),null,initial),intents); var recovery=new RatisTransactionRecovery(journal,home.statuses,routed,RatisTransactionRecoveryTest::next,(g,t,c) -> {
                    try { var node=g.equals(l.groupId()) ? left : right; var token=node.f.machine.prepareFinalization(home.statuses,t,c); return new RatisTransactionRecovery.Authorization(token.finalization(),token); } catch (Exception e) { throw new IOException(e); }
                })) {
                    journal.executeHome(); lp.close(); rp.close();
                    for (int n=0;n<12 && !journal.snapshot().observedHome().participants().stream().filter(p -> p.groupId().equals(r.groupId())).findFirst().orElseThrow().finalized();n++) { try { recovery.step(); } catch (IOException expected) { assertTrue(expected.getMessage().contains("Left participant unavailable")); } }
                    assertTrue(journal.snapshot().observedHome().participants().stream().filter(p -> p.groupId().equals(r.groupId())).findFirst().orElseThrow().finalized()); assertNull(journal.snapshot().participant(l.groupId()).outcome()); unavailable.set(false);
                    boolean complete=false; for (int n=0;n<12 && !complete;n++) { long before=home.wire.index+left.wire.index+right.wire.index; complete=recovery.step().complete(); assertTrue(home.wire.index+left.wire.index+right.wire.index-before<=1); }
                    assertTrue(complete); assertEquals(TransactionStatus.Phase.ABORT,journal.snapshot().observedHome().phase()); assertFalse(journal.snapshot().observedHome().consumed()); assertNull(routed.read(column,new byte[]{1})); assertNull(routed.read(column,new byte[]{(byte)130})); helper.released(left); helper.released(right);
                }
            }
        }
    }

}
