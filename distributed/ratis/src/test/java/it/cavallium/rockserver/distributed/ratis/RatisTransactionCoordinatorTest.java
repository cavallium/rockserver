package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Healthy coordinator over controlled RPC/native fixtures, not public frontend or quorum acceptance. */
class RatisTransactionCoordinatorTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisTransactionRecoveryTest fixtures=new RatisTransactionRecoveryTest();
    RatisTransactionCoordinator.Authorization authorize(RatisTransactionRecoveryTest.Fixture f,UUID tx,RatisDurableTransaction.ParticipantProgress progress) throws IOException {
        try {
            var intent=progress.intent(); RatisRangeStateMachine.ParticipantProposal token;
            try { token=f.data.f.machine.reattachParticipant(tx,intent.prepareControl(),intent.acceptedBusiness()); }
            catch (RatisPreparationUnavailableException absent) {
                if (progress.preparation()!=null) throw absent;
                try { token=f.data.f.machine.prepareParticipant(f.pin,f.data.f.put(2),intent.prepareControl(),f.home.statuses,tx); }
                catch (IllegalStateException expired) { throw new RatisPreparationUnavailableException("Original read cut unavailable",expired); }
            }
            return new RatisTransactionCoordinator.Authorization(token.preparation(),token.negative(),token);
        } catch (IOException e) { throw e; } catch (Exception e) { throw new IOException(e); }
    }
    RatisTransactionCoordinator coordinator(RatisTransactionRecoveryTest.Fixture f) throws IOException {
        return new RatisTransactionCoordinator(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(tx,p) -> authorize(f,tx,p));
    }
    int drain(RatisTransactionRecoveryTest.Fixture f,RatisTransactionCoordinator coordinator) throws Exception {
        for (int n=0;n<32;n++) { long before=f.home.wire.index+f.data.wire.index; var step=coordinator.step(); assertTrue(f.home.wire.index+f.data.wire.index-before<=1); if (step.complete()) return n+1; }
        fail("Coordinator did not complete"); return -1;
    }
    @Test void healthyCommitRequiresEveryVoteFinalizeAndAckWithoutImplicitConsumptionOrDuplicateHomeReads() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("healthy"); var coordinator=coordinator(f)) {
            var reads=new java.util.concurrent.atomic.AtomicInteger(); f.home.wire.afterRead=reads::incrementAndGet;
            int steps=drain(f,coordinator); assertEquals(steps+2,reads.get(),"Only outer home reads plus the two authority reads are needed");
            var home=f.journal.snapshot().observedHome(); assertEquals(TransactionStatus.Phase.COMMIT,home.phase()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed());
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); assertArrayEquals(f.plan.intent().acceptedBusiness().acceptance().result(),f.journal.snapshot().participant(f.data.f.groupId).outcome().result()); fixtures.helper.released(f.data);
        }
    }
    @Test void lostPrepareReplyResolvesItsNativeVoteWithoutAnotherPreparationIdentity() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("lost"); var coordinator=coordinator(f)) {
            coordinator.step(); f.data.wire.lose=true; assertThrows(IOException.class,coordinator::step);
            assertNull(f.journal.snapshot().participant(f.data.f.groupId).vote()); assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).preparation());
            drain(f,coordinator); assertEquals(2,f.data.wire.index); assertEquals(TransactionStatus.Phase.COMMIT,f.journal.snapshot().observedHome().phase()); assertEquals(f.plan.intent().prepareControl(),f.journal.snapshot().participant(f.data.f.groupId).preparation().preparation().operation());
        }
    }
    @Test void readyAndSubmittedUnknownPreparationsReattachExactAuthorityAfterReopen() throws Exception {
        fixtures.root=root;
        for (boolean submitted:new boolean[]{false,true}) try (var f=fixtures.new Fixture("reattach-"+submitted)) {
            f.journal.executeHome();
            try (var original=f.data.f.machine.prepareParticipant(f.pin,f.data.f.put(2),f.plan.intent().prepareControl(),f.home.statuses,f.plan.initial().transactionId())) {
                var operation=f.data.routed.prepareParticipant(original.preparation());
                if (submitted) { f.journal.freezePreparation(operation); var pending=f.data.wire.propose(operation.bytes()); assertNull(pending.getException()); f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,1); f.data.f.machine.cancelTransaction(pending); }
                f.reopen();
                try (var coordinator=coordinator(f)) { assertEquals(RatisTransactionCoordinator.Action.PREPARED,coordinator.step().action()); drain(f,coordinator); }
                assertArrayEquals(operation.bytes(),f.journal.snapshot().participant(f.data.f.groupId).preparation().bytes()); assertEquals(new ApplyPosition(1,1),f.journal.snapshot().participant(f.data.f.groupId).vote().position()); fixtures.helper.released(f.data);
            }
        }
    }
    @Test void unavailableOriginalCutAbortsWhileGenericIoRemainsUncertain() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("unavailable")) {
            f.pin.close();
            try (var coordinator=coordinator(f)) { drain(f,coordinator); }
            assertEquals(TransactionStatus.Phase.ABORT,f.journal.snapshot().observedHome().phase()); assertEquals(0,f.data.f.store.preparedParticipantCount()); assertNull(f.data.routed.read(f.data.f.column,new byte[]{2})); assertEquals(1,f.data.wire.index);
        }
        try (var f=fixtures.new Fixture("io")) {
            f.journal.executeHome();
            try (var coordinator=new RatisTransactionCoordinator(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(tx,p) -> { throw new IOException("Unknown authorization transport result"); })) { assertThrows(IOException.class,coordinator::step); }
            assertEquals(TransactionStatus.Phase.PREPARING,f.home.statuses.read(f.plan.initial().transactionId()).phase()); assertEquals(0,f.data.wire.index);
            try (var coordinator=coordinator(f)) { drain(f,coordinator); } assertEquals(TransactionStatus.Phase.COMMIT,f.journal.snapshot().observedHome().phase());
        }
    }
    @Test void aConflictingWriteAbortsRatherThanRebasingTheFrozenReadCut() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("conflict"); var coordinator=coordinator(f)) {
            coordinator.step();
            var concurrent=new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(0,2000),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9}))); f.data.wire.command(f.data.directory.ranges().getFirst(),concurrent);
            assertEquals(RatisTransactionCoordinator.Action.ABORT_ATTEMPTED,coordinator.step().action()); drain(f,coordinator);
            assertEquals(TransactionStatus.Phase.ABORT,f.journal.snapshot().observedHome().phase()); assertArrayEquals(new byte[]{9},f.data.routed.read(f.data.f.column,new byte[]{2})); assertEquals(new ApplyPosition(1,0),f.journal.snapshot().participant(f.data.f.groupId).intent().acceptedBusiness().acceptance().snapshot()); assertEquals(0,f.data.f.store.preparedParticipantCount()); fixtures.helper.released(f.data);
        }
    }
    @Test void competingAbortWinsCommitCasWithoutExposingBusinessEffects() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("decision-race")) {
            try (var coordinator=coordinator(f)) { for (int n=0;n<16;n++) { coordinator.step(); var home=f.journal.snapshot().observedHome(); if (home!=null && home.participants().getFirst().prepared()!=null) break; } }
            var ready=f.journal.snapshot().observedHome(); var raced=new java.util.concurrent.atomic.AtomicBoolean();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return f.home.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { if (TransactionStatus.decode(command.mutations().getFirst().value()).phase()==TransactionStatus.Phase.COMMIT && !raced.getAndSet(true)) f.home.statuses.execute(f.home.statuses.prepare(new OperationId(new UUID(0,2001),2,0),ready,ready.decide(TransactionStatus.Phase.ABORT))); return f.home.wire.command(route,command); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return f.home.wire.transaction(route,bytes,fresh); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return f.home.wire.participant(route,bytes); }
            };
            try (var routed=new RatisRoutedClient(f.home.directory,transport)) {
                var homes=new RatisTransactionStatuses(routed,f.home.f.column); f.journal.close(); f.journal=RatisDurableTransaction.reopen(f.path,homes,f.data.routed);
                try (var coordinator=new RatisTransactionCoordinator(f.journal,homes,f.data.routed,RatisTransactionRecoveryTest::next,(tx,p) -> { throw new AssertionError("All prepares already verified"); })) {
                    assertEquals(RatisTransactionCoordinator.Action.COMMIT_ATTEMPTED,coordinator.step().action()); assertEquals(CommandResult.Status.CONFLICT,f.journal.snapshot().homeResult().status()); drain(f,coordinator);
                }
            }
            assertEquals(TransactionStatus.Phase.ABORT,f.journal.snapshot().observedHome().phase()); assertNull(f.data.routed.read(f.data.f.column,new byte[]{2})); assertFalse(f.journal.snapshot().observedHome().consumed());
        }
    }
    @Test void aLostFinalAckCannotReturnCompleteBeforeFreshHomeProof() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("ack"); var coordinator=coordinator(f)) {
            for (int n=0;n<16 && f.journal.snapshot().participant(f.data.f.groupId).outcome()==null;n++) coordinator.step();
            assertFalse(f.journal.snapshot().observedHome().fullyFinalized()); f.home.wire.lose=true; assertThrows(IOException.class,coordinator::step);
            assertFalse(f.journal.snapshot().observedHome().fullyFinalized()); assertTrue(coordinator.step().complete()); assertTrue(f.journal.snapshot().homeSuperseded()); assertNull(f.journal.snapshot().homeResult()); assertFalse(f.journal.snapshot().observedHome().consumed());
        }
    }
    @Test void cancelledStepViewsCannotReleaseActivePreparationOwnership() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("active"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            f.journal.executeHome(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var actualDone=new CountDownLatch(1);
            var coordinator=new RatisTransactionCoordinator(f.journal,f.home.statuses,f.data.routed,RatisTransactionRecoveryTest::next,(tx,p) -> {
                entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); } catch (InterruptedException e) { throw new IOException(e); } return authorize(f,tx,p);
            });
            try {
                var view=executor.submit(() -> { try { return coordinator.step(); } finally { actualDone.countDown(); } }); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,coordinator::close); release.countDown(); assertTrue(actualDone.await(5,TimeUnit.SECONDS));
                assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).vote());
            } finally { release.countDown(); coordinator.close(); }
            try (var resumed=coordinator(f)) { drain(f,resumed); } fixtures.helper.released(f.data);
        }
    }
    @Test void twoGroupsCommitAfterALostPrepareReplyAndStayLockedUntilTheirLocalFinalization() throws Exception {
        fixtures.root=root; var helper=fixtures.helper; helper.root=root; UUID column=new UUID(0,2010),domain=new UUID(0,2011); byte[] split={(byte)128};
        try (var home=helper.new Node("multi-home",true); var left=helper.new Node("multi-left",false,new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,Set.of(column),null,split)); var right=helper.new Node("multi-right",false,new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(domain,Set.of(column),split,null))) {
            var lr=left.directory.ranges().getFirst(); var rr=right.directory.ranges().getFirst(); var l=new RangeDescriptor(lr.rangeId(),lr.groupId(),1,0,null,split,RangeDescriptor.TopologyState.ACTIVE,lr.replicas()); var r=new RangeDescriptor(rr.rangeId(),rr.groupId(),1,0,split,null,RangeDescriptor.TopologyState.ACTIVE,rr.replicas());
            var directory=new RangeDirectory(helper.cluster,helper.database,domain,1,new byte[32],0,Set.of(column),0,List.of(l,r));
            var transport=new RatisRoutedClient.Transport() {
                RatisParticipantPrepareTest.Wire wire(RangeDescriptor route) { return route.groupId().equals(l.groupId()) ? left.wire : right.wire; }
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { return wire(route).read(route,col,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return wire(route).command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return wire(route).participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return wire(route).transaction(route,bytes,fresh); }
            };
            var operation=new OperationId(new UUID(0,2012),1,0); var lb=new DistributedCommand(l.groupId(),1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7},Mutation.ReturnMode.PREVIOUS))); var rb=new DistributedCommand(r.groupId(),1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)130},new byte[]{8},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)130},new byte[0],Mutation.ReturnMode.CURRENT)));
            try (var lp=left.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})))); var rp=right.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)130})))); var routed=new RatisRoutedClient(directory,transport)) {
                var la=left.f.machine.participantIntent(lp,lb); var ra=right.f.machine.participantIntent(rp,rb);
                var intents=List.of(new RatisDurableTransaction.Intent(l,la,helper.control(0),new OperationId(new UUID(0,2014),3,0),List.of(1)),new RatisDurableTransaction.Intent(r,ra,helper.control(1),new OperationId(new UUID(0,2015),3,0),List.of(0,2)));
                var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,2013), operation, List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), RatisDurableTransaction.planDigest(intents), List.of(new TransactionStatus.Participant(domain, l.rangeId(), l.groupId(), 1, new byte[32], la.payloadDigest(), null, false),new TransactionStatus.Participant(domain, r.rangeId(), r.groupId(), 1, new byte[32], ra.payloadDigest(), null, false)));
                try (var journal=RatisDurableTransaction.create(root.resolve("multi-coordinator.bin"),home.statuses,routed,initial,home.statuses.prepare(fixtures.journals.homeOperation(0),null,initial),intents); var coordinator=new RatisTransactionCoordinator(journal,home.statuses,routed,RatisTransactionRecoveryTest::next,(tx,p) -> {
                    var isLeft=p.intent().route().groupId().equals(l.groupId()); var node=isLeft ? left : right; RatisRangeStateMachine.ParticipantProposal token;
                    try {
                        try { token=node.f.machine.reattachParticipant(tx,p.intent().prepareControl(),p.intent().acceptedBusiness()); }
                        catch (RatisPreparationUnavailableException absent) { if (p.preparation()!=null) throw absent; token=node.f.machine.prepareParticipant(isLeft ? lp : rp,isLeft ? lb : rb,p.intent().prepareControl(),home.statuses,tx); }
                        return new RatisTransactionCoordinator.Authorization(token.preparation(),token.negative(),token);
                    } catch (IOException e) { throw e; } catch (Exception e) { throw new IOException(e); }
                })) {
                    assertArrayEquals(RatisDurableTransaction.planDigest(intents),RatisDurableTransaction.planDigest(List.of(intents.get(1),intents.get(0))));
                    var reorderedResults=List.of(new RatisDurableTransaction.Intent(l,la,intents.get(0).prepareControl(),intents.get(0).finalizeControl(),List.of(0)),new RatisDurableTransaction.Intent(r,ra,intents.get(1).prepareControl(),intents.get(1).finalizeControl(),List.of(1,2)));
                    assertFalse(Arrays.equals(RatisDurableTransaction.planDigest(intents),RatisDurableTransaction.planDigest(reorderedResults)));
                    assertThrows(IllegalStateException.class,journal::result); left.wire.lose=true; int lost=0; boolean partial=false,complete=false;
                    for (int n=0;n<64 && !complete;n++) {
                        long before=home.wire.index+left.wire.index+right.wire.index;
                        try { complete=coordinator.step().complete(); } catch (IOException expected) { assertTrue(expected.getMessage().contains("Lost applied participant reply")); lost++; }
                        assertTrue(home.wire.index+left.wire.index+right.wire.index-before<=1);
                        var state=journal.snapshot(); long finalized=state.participants().stream().filter(p -> p.outcome()!=null).count();
                        if (finalized==1) {
                            partial=true; assertFalse(complete); boolean leftDone=state.participant(l.groupId()).outcome()!=null;
                            assertArrayEquals(leftDone ? new byte[]{7} : new byte[0],routed.read(column,leftDone ? new byte[]{1} : new byte[]{(byte)130}));
                            assertThrows(IllegalStateException.class,journal::result);
                            assertThrows(IOException.class,() -> routed.read(column,leftDone ? new byte[]{(byte)130} : new byte[]{1}));
                        }
                    }
                    assertTrue(complete); assertTrue(partial); assertEquals(1,lost); assertEquals(TransactionStatus.Phase.COMMIT,journal.snapshot().observedHome().phase()); assertTrue(journal.snapshot().observedHome().fullyFinalized()); assertFalse(journal.snapshot().observedHome().consumed());
                    assertArrayEquals(la.acceptance().result(),journal.snapshot().participant(l.groupId()).outcome().result()); assertArrayEquals(ra.acceptance().result(),journal.snapshot().participant(r.groupId()).outcome().result()); assertArrayEquals(new byte[]{7},routed.read(column,new byte[]{1})); assertArrayEquals(new byte[0],routed.read(column,new byte[]{(byte)130})); assertEquals(2,left.wire.index); assertEquals(2,right.wire.index); helper.released(left); helper.released(right);
                    var result=journal.result(); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(3,result.valueCount()); assertArrayEquals(new byte[]{8},result.value(0)); assertNull(result.value(1)); assertArrayEquals(new byte[0],result.value(2)); assertFalse(journal.snapshot().observedHome().consumed());
                    journal.close();
                    try (var reopened=RatisDurableTransaction.reopen(root.resolve("multi-coordinator.bin"),home.statuses,routed)) { assertArrayEquals(result.encode(),reopened.result().encode()); assertEquals(List.of(0,2),reopened.snapshot().participant(r.groupId()).intent().resultPositions()); }
                }
            }
        }
    }

    @Test void forcedTerminalProofCanBeAckedWithoutAnotherParticipantRead() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("cached-proof")) {
            try (var coordinator=coordinator(f)) { for (int n=0;n<16 && f.journal.snapshot().participant(f.data.f.groupId).outcome()==null;n++) coordinator.step(); }
            assertNotNull(f.journal.snapshot().participant(f.data.f.groupId).outcome()); assertFalse(f.journal.snapshot().observedHome().fullyFinalized());
            var unavailable=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { throw new IOException("Participant unavailable"); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { throw new IOException("Participant unavailable"); }
                public byte[] transaction(RangeDescriptor route,byte[] body,boolean fresh) { throw new AssertionError("Saved terminal proof must not be queried again for ACK"); }
                public byte[] participant(RangeDescriptor route,byte[] body) { throw new AssertionError("Saved terminal proof must not be re-executed"); }
            };
            try (var routed=new RatisRoutedClient(f.data.directory,unavailable)) {
                f.journal.close(); f.journal=RatisDurableTransaction.reopen(f.path,f.home.statuses,routed);
                try (var coordinator=new RatisTransactionCoordinator(f.journal,f.home.statuses,routed,RatisTransactionRecoveryTest::next,(t,p) -> { throw new AssertionError("No prepare needed"); })) {
                    var step=coordinator.step(); assertEquals(RatisTransactionCoordinator.Action.ACK_ATTEMPTED,step.action()); assertTrue(step.complete()); assertFalse(f.journal.snapshot().observedHome().consumed());
                }
            }
        }
    }

    @Test void snapshotGateAuthorityFreezesANegativePrepareThenAbortsOnTheFollowingStep() throws Exception {
        fixtures.root=root;
        try (var f=fixtures.new Fixture("negative-gate"); var coordinator=coordinator(f)) {
            coordinator.step();
            var group=f.data.f.groupId;
            var gate=new SnapshotFence(fixtures.helper.cluster, fixtures.helper.database, new UUID(0,3010), new UUID(0,3011), 1, new OperationId(new UUID(0,3010),1,0), List.of(new DomainBinding(f.data.directory.domainId(), 0, f.data.directory.payloadDigest(), 1, new byte[32])), List.of(new SnapshotFence.Participant(f.data.directory.domainId(), group, group, 1, new byte[32], new OperationId(new UUID(0,3012),1,0), new OperationId(new UUID(0,3012),1,1), fixtures.helper.scope(f.data,1,4))));
            var install=RatisSnapshotRecord.install(gate,group);
            f.data.f.machine.applyTransaction(RatisSnapshotRecordTest.committed(f.data.f.machine,1,1,install)).get(5,TimeUnit.SECONDS); f.data.wire.index=1;
            long before=f.home.wire.index+f.data.wire.index;
            assertEquals(RatisTransactionCoordinator.Action.PREPARATION_REJECTED,coordinator.step().action()); assertEquals(1,f.home.wire.index+f.data.wire.index-before);
            var progress=f.journal.snapshot().participant(group); assertTrue(progress.preparation().negative()); assertTrue(RatisParticipantRecord.isNegative(progress.preparation().bytes())); assertEquals(ParticipantVote.negative(),progress.vote());
            assertEquals(TransactionStatus.Phase.PREPARING,f.journal.snapshot().observedHome().phase());
            assertEquals(RatisTransactionCoordinator.Action.ABORT_ATTEMPTED,coordinator.step().action()); drain(f,coordinator);
            var home=f.journal.snapshot().observedHome(); assertEquals(TransactionStatus.Phase.ABORT,home.phase()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed());
            assertEquals(ParticipantVote.negative(),f.journal.snapshot().participant(group).outcome().vote()); assertEquals(0,f.data.f.store.preparedParticipantCount()); assertNull(f.data.routed.read(f.data.f.column,new byte[]{2})); fixtures.helper.released(f.data);
        }
    }

}
