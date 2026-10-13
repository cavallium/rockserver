package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Protected negative votes through controlled admission/apply; no network or quorum acceptance. */
class RatisNegativeParticipantAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    final RatisSnapshotAdmissionTest gates=new RatisSnapshotAdmissionTest();
    void initialize() { helper.root=root; gates.root=root; gates.helper.root=root; }
    void released(RatisParticipantPrepareTest.Node data) throws Exception {
        assertEquals(0,data.f.machine.negativeParticipantSlots()); assertEquals(0,data.f.machine.negativeParticipantInputBytes()); helper.released(data); gates.released(data);
    }
    @Test void volatileGateRejectsPrepareBeforeInstallWithoutTakingAnOrdinaryCandidate() throws Exception {
        initialize();
        try (var home=helper.new Node("tx-home",true); var data=helper.new Node("data",false); var gateHome=gates.new Home("gate-home"); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var accepted=data.f.machine.participantIntent(pin,business);
            var old=data.f.propose(data.f.put(258)); assertNull(old.getException());
            var fence=gates.fence(gateHome,data,1,helper.scope(data,1,4)); var initial=gateHome.register(fence);
            try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence)) {
                assertFalse(gate.drained());
                var raw=new PreparedParticipant.Preparation(home.f.groupId,1,helper.control(0),registration,accepted);
                assertNotNull(data.wire.propose(RatisParticipantRecord.encode(raw,true)).getException(),"Encoding cannot invent negative authority");
                try (var negative=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) {
                    assertTrue(negative.negative()); assertTrue(RatisParticipantRecord.isNegative(negative.bytes())); assertEquals(0,data.f.machine.proposalFootprintStats().candidates());
                    assertEquals(it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.FINALIZATION_RESERVATION,data.f.machine.reservedParticipantBytes());
                    var operation=data.routed.prepareParticipant(negative.preparation(),negative.negative());
                    assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(operation)); assertNull(data.f.store.preparedParticipant(registration.transactionId()));
                    assertFalse(gate.drained()); data.f.machine.cancelTransaction(old); assertTrue(gate.drained());
                    gates.append(data,gate); gateHome.transition(initial,initial.abort());
                    try (var release=data.f.machine.prepareSnapshotRelease(gateHome.homes,fence)) { gates.append(data,release); }
                    assertEquals(CommandResult.Status.SUCCESS,data.wire.command(data.directory.ranges().getFirst(),data.f.put(258)).status());
                    var floor=DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,2,0),2); data.wire.command(data.directory.ranges().getFirst(),floor);
                    assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(operation)); assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2}));
                }
                released(data);
            }
        }
    }
    @Test void releasedGateCannotTurnItsAlreadyAdmittedNegativeRequestPositive() throws Exception {
        initialize();
        try (var home=helper.new Node("overtake-tx",true); var data=helper.new Node("overtake-data",false); var gateHome=gates.new Home("overtake-gate"); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var fence=gates.fence(gateHome,data,2,helper.scope(data,1,4)); var initial=gateHome.register(fence);
            try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence); var negative=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) {
                var pending=data.wire.propose(negative.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending);
                gateHome.transition(initial,initial.abort()); try (var release=data.f.machine.prepareSnapshotRelease(gateHome.homes,fence)) { gates.append(data,release); }
                assertFalse(data.f.store.snapshotGate(fence.fenceId()).active()); assertTrue(data.f.machine.negativeParticipantInputBytes()>0);
                data.wire.command(data.directory.ranges().getFirst(),data.f.put(258));
                data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++data.wire.index);
                assertEquals(ParticipantVote.negative(),RatisParticipantRecord.result(data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray()));
                assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2})); released(data);
            }
        }
    }
    @Test void candidateAndOrdinaryUserSaturationCannotDonateOrBlockNegativeControlCapacity() throws Exception {
        initialize();
        for (boolean candidates:new boolean[]{false,true}) try (var home=helper.new Node("quota-tx-"+candidates,true); var data=helper.new Node("quota-data-"+candidates,false); var gateHome=gates.new Home("quota-gate-"+candidates); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var owners=new ArrayList<AutoCloseable>();
            try {
                if (candidates) for (int n=0;n<64;n++) {
                    var owner=data.f.machine.reserveProposalScope(new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{(byte)n})))); assertNotNull(owner); owners.add(owner);
                } else for (int n=0;n<1024;n++) {
                    var command=new DistributedCommand(data.f.groupId,1,new byte[32],new OperationId(new UUID(0,9000),1,n),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{9},new byte[]{7})));
                    var owner=data.f.propose(command); assertNull(owner.getException()); owners.add(() -> data.f.machine.cancelTransaction(owner));
                }
                var fence=gates.fence(gateHome,data,3,helper.scope(data,1,4)); gateHome.register(fence);
                try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence); var negative=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) {
                    assertTrue(negative.negative()); assertEquals(candidates ? 64 : 0,data.f.machine.proposalFootprintStats().candidates());
                    assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(data.routed.prepareParticipant(negative.preparation(),true)));
                }
            } finally { for (var owner:owners) owner.close(); }
            released(data);
        }
    }
    @Test void originalPositiveOwnershipWinsAndARejectionNeedsCurrentGateAuthority() throws Exception {
        initialize();
        try (var home=helper.new Node("positive-tx",true); var data=helper.new Node("positive-data",false); var gateHome=gates.new Home("positive-gate"); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var accepted=data.f.machine.participantIntent(pin,business);
            assertThrows(RatisPreparationUnavailableException.class,() -> data.f.machine.prepareParticipantRejection(accepted,helper.control(0),home.statuses,registration.transactionId()));
            try (var positive=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) {
                assertFalse(positive.negative()); var fence=gates.fence(gateHome,data,4,helper.scope(data,1,4)); gateHome.register(fence);
                try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence)) {
                    assertFalse(gate.drained()); assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipantRejection(accepted,helper.control(0),home.statuses,registration.transactionId()));
                    assertTrue(data.routed.executeParticipant(data.routed.prepareParticipant(positive.preparation())).prepared());
                    assertTrue(gate.drained()); assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipantRejection(accepted,helper.control(0),home.statuses,registration.transactionId()));
                }
            }
            released(data);
        }
    }
    @Test void aNewTermCanRejectTheFrozenOriginalCutWithoutReopeningIt() throws Exception {
        initialize();
        try (var home=helper.new Node("term-tx",true); var data=helper.new Node("term-data",false); var gateHome=gates.new Home("term-gate"); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var accepted=data.f.machine.participantIntent(pin,business); pin.close();
            data.f.machine.notifyNotLeader(List.of()); data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); data.f.machine.notifyTermIndexUpdated(2,1); data.wire.index=1;
            var fence=gates.fence(gateHome,data,5,helper.scope(data,1,4)); gateHome.register(fence);
            try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence); var negative=data.f.machine.prepareParticipantRejection(accepted,helper.control(0),home.statuses,registration.transactionId())) {
                assertTrue(negative.negative()); assertEquals(new ApplyPosition(1,0),negative.preparation().business().acceptance().snapshot());
                assertArrayEquals(accepted.payloadDigest(),negative.preparation().business().payloadDigest());
                assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(data.routed.prepareParticipant(negative.preparation(),true)));
                assertEquals(new ApplyPosition(2,2),data.f.store.rejectedParticipant(registration.transactionId()).position());
            }
            released(data);
        }
    }
    @Test void sharedFinalizationKeepsBlockedNegativePayloadChargedUntilItsActualReturn() throws Exception {
        initialize();
        try (var home=helper.new Node("shared-tx",true); var data=helper.new Node("shared-data",false); var gateHome=gates.new Home("shared-gate"); var pin=data.f.store.captureTracked(helper.scope(data,1,4)); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var fence=gates.fence(gateHome,data,6,helper.scope(data,1,4)); gateHome.register(fence);
            try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence)) {
                var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
                home.wire.afterRead=() -> { home.wire.afterRead=null; entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
                var negative=executor.submit(() -> data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId()));
                TransactionContext finalization=null;
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); var abort=registration.decide(TransactionStatus.Phase.ABORT);
                    home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,abort));
                    try (var proposal=data.f.machine.prepareFinalization(home.statuses,registration.transactionId(),new OperationId(new UUID(0,9001),2,0))) {
                        finalization=data.wire.propose(proposal.bytes()); assertNull(finalization.getException()); data.f.machine.preAppendTransaction(finalization); finalization.initLogEntry(1,1); data.f.machine.cancelTransaction(finalization);
                    }
                    data.f.machine.notifyNotLeader(List.of()); assertTrue(data.f.machine.negativeParticipantInputBytes()>0);
                } finally { release.countDown(); }
                assertThrows(ExecutionException.class,() -> negative.get(5,TimeUnit.SECONDS));
                assertEquals(0,data.f.machine.negativeParticipantSlots()); assertTrue(data.f.machine.negativeParticipantInputBytes()>0,"Shared FINALIZE still owns the retained preparation input allowance");
                data.f.machine.applyTransaction(finalization).get(5,TimeUnit.SECONDS); data.wire.index=1; pin.close(); released(data);
            }
        }
    }
    @Test void positiveNativeProofDoesNotSettleAnUnknownNegativeAdmission() throws Exception {
        initialize();
        try (var home=helper.new Node("unknown-tx",true); var data=helper.new Node("unknown-data",false); var gateHome=gates.new Home("unknown-gate"); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0); var fence=gates.fence(gateHome,data,7,helper.scope(data,1,4)); gateHome.register(fence);
            try (var gate=data.f.machine.prepareSnapshotInstall(gateHome.homes,fence); var negative=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) {
                var pending=data.wire.propose(negative.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,2); data.f.machine.cancelTransaction(pending);
                data.f.store.applyPrepared(new ApplyPosition(1,1),negative.preparation()); // Contradictory speculative acceptance, not a valid admitted history.
                data.f.machine.takeSnapshot(); data.f.machine.pause(); data.f.machine.reinitialize();
                assertTrue(data.f.machine.negativeParticipantInputBytes()>0,"A positive native vote is not a negative request's outcome");
                assertEquals(1,data.f.counter("protectedAdmissions"));
                data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); data.f.machine.notifyTermIndexUpdated(2,2); data.wire.index=2;
                var fresh=data.f.propose(data.f.put(9)); assertNull(fresh.getException()); data.f.machine.cancelTransaction(fresh); released(data);
            }
        }
    }
    @Test void positiveSnapshotEvidenceCannotCompleteACoveredNegativeLogRecord() throws Exception {
        initialize();
        try (var home=helper.new Node("coverage-tx",true); var data=helper.new Node("coverage-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(home,data,pin,business,0);
            PreparedParticipant.Preparation body;
            try (var positive=data.f.machine.prepareParticipant(pin,business,helper.control(0),home.statuses,registration.transactionId())) { body=positive.preparation(); data.routed.executeParticipant(data.routed.prepareParticipant(body)); }
            data.f.machine.takeSnapshot(); data.f.machine.pause();
            var record=TransactionContext.newBuilder().setStateMachine(data.f.machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(RatisParticipantRecord.encode(body,true)))).build()).build();
            var pending=data.f.machine.applyTransaction(record); assertFalse(pending.isDone());
            assertThrows(IOException.class,data.f.machine::reinitialize); assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertFalse(data.f.store.admissionOpen());
        }
    }
}
