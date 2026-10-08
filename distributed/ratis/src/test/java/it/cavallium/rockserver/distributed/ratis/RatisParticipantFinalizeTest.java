package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled append/barrier/native fixtures, not quorum acceptance. */
class RatisParticipantFinalizeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    OperationId recovery(long n) { return new OperationId(new UUID(0,1001),3,n); }
    void homeDecision(RatisParticipantPrepareTest.Node home,TransactionStatus before,TransactionStatus after,long n) throws Exception { assertEquals(CommandResult.Status.SUCCESS,home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,n),before,after)).status()); }
    void released(RatisParticipantPrepareTest.Node data) throws Exception { helper.released(data); assertEquals(0,data.f.machine.reservedFinalizationCount()); assertEquals(0,data.f.machine.protectedFinalizationSlots()); }
    @Test void frozenFinalizeLostReplyExactRetryAndExpiredBusinessGrantsWorkForCommitAndBothAbortOrders() throws Exception {
        helper.root=root;
        for (int mode=0;mode<3;mode++) try (var home=helper.new Node("home-"+mode,true); var data=helper.new Node("data-"+mode,false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var initial=helper.register(home,data,pin,business,0);
            try (var preparation=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,initial.transactionId())) {
                var status=initial;
                if (mode!=2) {
                    var vote=data.routed.executeParticipant(data.routed.prepareParticipant(preparation.preparation()));
                    if (mode==0) { status=initial.prepared(data.f.groupId,preparation.preparation().business().payloadDigest(),vote.position()); homeDecision(home,initial,status,1); }
                }
                var terminal=status.decide(mode==0 ? TransactionStatus.Phase.COMMIT : TransactionStatus.Phase.ABORT); homeDecision(home,status,terminal,2);
                if (mode!=2) data.wire.command(data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,2,0),2));
                try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(mode))) {
                    var operation=data.routed.prepareFinalization(plan.finalization()); assertArrayEquals(plan.bytes(),operation.bytes());
                    data.wire.lose=true; assertThrows(IOException.class,() -> data.routed.executeFinalization(operation));
                    var outcome=data.routed.finalizedParticipant(data.f.groupId,initial.transactionId()); assertEquals(terminal.phase(),outcome.finalization().decision().phase());
                    preparation.close(); data.wire.command(data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,4,0),4));
                    assertEquals(outcome.position(),data.routed.executeFinalization(operation));
                    var old=operation.descriptor(); var changed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
                    data.routed.refresh(new RangeDirectory(helper.cluster,helper.database,data.directory.domainId(),1,new byte[32],1,data.directory.columns(),0,List.of(changed)));
                    var restored=data.routed.restoreFinalization(operation.finalization(),old); assertEquals(0,restored.descriptor().configurationVersion()); assertArrayEquals(operation.bytes(),restored.bytes());
                    assertEquals(outcome.position(),data.routed.executeFinalization(restored));
                    if (mode==0) { assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2})); assertArrayEquals(preparation.preparation().business().acceptance().result(),outcome.result()); }
                    else assertNull(data.routed.read(data.f.column,new byte[]{2}));
                    if (mode==2) preparation.close(); released(data);
                }
            }
        }
    }
    @Test void sharedAbortAtCapacityRetainsThePendingSlotAfterPrepareCloses() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("cap-home",true); var data=helper.new Node("cap-data",false)) {
            for (int n=0;n<63;n++) try (var pin=data.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{(byte)n}))))) {
                var business=data.f.put(n); var registration=helper.register(home,data,pin,business,n);
                try (var plan=data.f.machine.prepareParticipant(pin,business,helper.control(n),home.statuses,registration.transactionId())) { data.routed.executeParticipant(data.routed.prepareParticipant(plan.preparation())); }
            }
            try (var pin=data.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{63}))))) {
                var initial=helper.register(home,data,pin,data.f.put(63),63);
                try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(63),helper.control(63),home.statuses,initial.transactionId())) {
                    homeDecision(home,initial,initial.decide(TransactionStatus.Phase.ABORT),64);
                    try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) {
                        assertEquals(1,data.f.machine.reservedParticipantCount()); assertEquals(0,data.f.machine.reservedFinalizationCount()); preparation.close();
                        assertEquals(1,data.f.machine.reservedParticipantCount());
                        try (var extra=data.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{64}))))) {
                            assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipant(extra,data.f.put(64),helper.control(64),home.statuses,new UUID(0,999)));
                        }
                        data.routed.executeFinalization(data.routed.prepareFinalization(plan.finalization()));
                        assertEquals(64,data.f.store.preparedCapacity().count()); assertEquals(63,data.f.store.preparedParticipantCount()); released(data);
                    }
                }
            }
        }
    }
    @Test void abortPublicationTransfersSharedCapacityBeforeTheUncertainLatePrepareSettles() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("late-home",true); var data=helper.new Node("late-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId())) {
                var pending=data.wire.propose(preparation.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); data.f.machine.cancelTransaction(pending);
                homeDecision(home,initial,initial.decide(TransactionStatus.Phase.ABORT),1);
                try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) {
                    data.routed.executeFinalization(data.routed.prepareFinalization(plan.finalization())); assertEquals(0,data.f.machine.reservedParticipantCount());
                    pending.initLogEntry(1,++data.wire.index);
                    assertEquals(ParticipantVote.negative(),RatisParticipantRecord.result(data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray())); released(data);
                    assertNull(data.routed.read(data.f.column,new byte[]{2}));
                }
            }
        }
    }
    @Test void protectedFinalizeIgnoresUserSaturationAndOverlappingVolatileCandidates() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("sat-home",true); var data=helper.new Node("sat-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId())) {
                var vote=data.routed.executeParticipant(data.routed.prepareParticipant(preparation.preparation())); var ready=initial.prepared(data.f.groupId,preparation.preparation().business().payloadDigest(),vote.position()); homeDecision(home,initial,ready,1); homeDecision(home,ready,ready.decide(TransactionStatus.Phase.COMMIT),2);
                var pending=new ArrayList<TransactionContext>();
                try (var candidate=data.f.machine.reserveProposalScope(helper.scope(data,1,4))) {
                    assertNotNull(candidate);
                    try {
                        for (int n=0;n<1024;n++) { var command=new DistributedCommand(data.f.groupId,1,new byte[32],new OperationId(new UUID(0,1002),1,n),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{5},new byte[]{5}))); var context=data.f.propose(command); assertNull(context.getException()); pending.add(context); }
                        var rejected=data.f.propose(data.f.put(9)); assertNotNull(rejected.getException());
                        try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) { data.routed.executeFinalization(data.routed.prepareFinalization(plan.finalization())); assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2})); }
                    } finally { for (var context:pending) data.f.machine.cancelTransaction(context); }
                }
                released(data);
            }
        }
    }
    @Test void coveredSnapshotFinalizationSettlesOwnershipBeforeDeferredDuplicateSuffix() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("snap-home",true); var data=helper.new Node("snap-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId())) {
                var vote=data.routed.executeParticipant(data.routed.prepareParticipant(preparation.preparation())); var ready=initial.prepared(data.f.groupId,preparation.preparation().business().payloadDigest(),vote.position()); homeDecision(home,initial,ready,1); homeDecision(home,ready,ready.decide(TransactionStatus.Phase.COMMIT),2);
                try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) {
                    var first=data.wire.propose(plan.bytes()); var duplicate=data.wire.propose(plan.bytes()); assertNull(first.getException()); assertNull(duplicate.getException());
                    data.f.machine.preAppendTransaction(first); first.initLogEntry(1,2); data.f.machine.preAppendTransaction(duplicate); duplicate.initLogEntry(1,3);
                    data.f.store.applyCommit(new ApplyPosition(1,2),plan.finalization()); data.f.machine.takeSnapshot(); data.f.machine.pause();
                    var covered=data.f.machine.applyTransaction(first); var suffix=data.f.machine.applyTransaction(duplicate); data.f.machine.reinitialize();
                    assertEquals(new ApplyPosition(1,2),RatisParticipantRecord.finalizationResult(covered.get(5,TimeUnit.SECONDS).getContent().toByteArray())); assertFalse(suffix.isDone()); assertEquals(0,data.f.machine.reservedFinalizationCount());
                    assertEquals(new ApplyPosition(1,2),RatisParticipantRecord.finalizationResult(data.f.machine.applyTransaction(duplicate).get(5,TimeUnit.SECONDS).getContent().toByteArray()));
                    assertEquals(new ApplyPosition(1,2),RatisParticipantRecord.finalizationResult(suffix.get(5,TimeUnit.SECONDS).getContent().toByteArray())); data.wire.index=3; released(data);
                }
            }
        }
    }
    @Test void invalidatedBlockedHomeReadRetainsProtectedSlotUntilReturnAndClosedPlansCannotAppend() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("blocked-home",true); var data=helper.new Node("blocked-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4)); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var initial=helper.register(home,data,pin,data.f.put(2),0); homeDecision(home,initial,initial.decide(TransactionStatus.Phase.ABORT),1);
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } };
            var pending=executor.submit(() -> data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0)));
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); data.f.machine.notifyNotLeader(List.of()); assertEquals(1,data.f.machine.protectedFinalizationSlots()); assertFalse(pending.isDone()); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); released(data); home.wire.afterRead=null;
            byte[] bytes;
            try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) { bytes=plan.bytes(); assertEquals(1,data.f.machine.reservedFinalizationCount()); }
            assertThrows(IOException.class,() -> data.wire.participant(data.directory.ranges().getFirst(),bytes)); released(data); assertNull(data.routed.finalizedParticipant(data.f.groupId,initial.transactionId()));
        }
    }

    @Test void abortSharingDuringBlockedPreparePlanningTransfersCapacityWithoutNegativeCounters() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("planning-home",true); var data=helper.new Node("planning-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4)); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var initial=helper.register(home,data,pin,data.f.put(2),0); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var first=new java.util.concurrent.atomic.AtomicBoolean(true);
            home.wire.afterRead=() -> { if (!first.getAndSet(false)) return; entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException interrupted) { throw new AssertionError(interrupted); } };
            var pending=executor.submit(() -> data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId()));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); homeDecision(home,initial,initial.decide(TransactionStatus.Phase.ABORT),1);
                try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) { data.routed.executeFinalization(data.routed.prepareFinalization(plan.finalization())); }
                assertEquals(0,data.f.machine.reservedParticipantCount()); assertEquals(0,data.f.machine.reservedParticipantBytes());
            } finally { release.countDown(); }
            try (var preparation=pending.get(5,TimeUnit.SECONDS)) { assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(data.routed.prepareParticipant(preparation.preparation()))); }
            released(data);
        }
    }
    @Test void unknownSubmittedFinalizerKeepsFenceCapacityUntilACompletedNewerTerm() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("unknown-home",true); var data=helper.new Node("unknown-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0); homeDecision(home,initial,initial.decide(TransactionStatus.Phase.ABORT),1);
            try (var plan=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) {
                var pending=data.wire.propose(plan.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,1); data.f.machine.cancelTransaction(pending);
                data.f.machine.notifyNotLeader(List.of()); assertEquals(1,data.f.machine.reservedFinalizationCount());
                assertThrows(IllegalStateException.class,() -> data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0)));
                data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); data.f.machine.notifyTermIndexUpdated(2,1); data.wire.index=1;
                try (var fresh=data.f.machine.prepareFinalization(home.statuses,initial.transactionId(),recovery(0))) { assertEquals(new ApplyPosition(2,2),data.routed.executeFinalization(data.routed.prepareFinalization(fresh.finalization()))); }
                released(data); assertNull(data.routed.read(data.f.column,new byte[]{2}));
            }
        }
    }

    @Test void clientRejectsAnAppliedReplyWhichDoesNotFollowItsRegisteredPrepareVote() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("bad-reply-home",true); var data=helper.new Node("bad-reply-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var initial=helper.register(home,data,pin,data.f.put(2),0);
            try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId())) {
                var vote=data.routed.executeParticipant(data.routed.prepareParticipant(preparation.preparation())); var ready=initial.prepared(data.f.groupId,preparation.preparation().business().payloadDigest(),vote.position()); homeDecision(home,initial,ready,1); homeDecision(home,ready,ready.decide(TransactionStatus.Phase.COMMIT),2);
                var finalization=home.statuses.readFinalization(initial.transactionId(),data.f.groupId,recovery(0)); var reply=new java.util.concurrent.atomic.AtomicReference<>(vote.position());
                var transport=new RatisRoutedClient.Transport() {
                    public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { throw new IOException("Unused"); }
                    public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { throw new IOException("Unused"); }
                    public byte[] participant(RangeDescriptor route,byte[] bytes) { return RatisParticipantRecord.result(reply.get()); }
                };
                try (var routed=new RatisRoutedClient(data.directory,transport)) {
                    var operation=routed.prepareFinalization(finalization); assertThrows(IOException.class,() -> routed.executeFinalization(operation));
                    reply.set(new ApplyPosition(1,2)); assertEquals(new ApplyPosition(1,2),routed.executeFinalization(operation));
                }
            }
        }
    }

}
