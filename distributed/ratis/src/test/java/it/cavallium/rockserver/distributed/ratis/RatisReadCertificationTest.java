package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Tracked snapshot and explicit read-only participants over controlled native/proxy owners. */
class RatisReadCertificationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home snapshots;
        final RatisParticipantPrepareTest.Node transactions,data;
        final SnapshotFence fence;
        SnapshotFenceStatus ready;
        Fixture(String name) throws Exception {
            helper.root=root; helper.helper.root=root;
            snapshots=helper.new Home(name+"-snapshots"); transactions=helper.helper.new Node(name+"-transactions",true);
            data=helper.helper.new Node(name+"-data",false,null,snapshots.homes,System::nanoTime);
            data.wire.command(data.directory.ranges().getFirst(),data.f.put(2));
            fence=helper.fence(snapshots,data,1,helper.scope(data,1,4));
            var initial=snapshots.register(fence); ApplyPosition installed;
            try (var proposal=data.f.machine.prepareSnapshotInstall(snapshots.homes,fence)) { installed=helper.append(data,proposal).position(); }
            ready=initial.ready(data.f.groupId,installed); snapshots.transition(initial,ready);
        }
        ReplicatedRangeStore.ScopedHistoryPin capture() throws Exception { return data.f.machine.captureTrackedSnapshotGate(snapshots.homes,fence); }
        DistributedCommand intent(ReplicatedRangeStore.ScopedHistoryPin pin) throws Exception { return data.f.machine.participantReadIntent(pin,new OperationId(data.f.session,1,100)); }
        TransactionStatus register(DistributedCommand accepted,int id) throws Exception {
            var participant=new TransactionStatus.Participant(data.f.groupId,data.f.groupId,1,new byte[32],accepted.payloadDigest(),null,false);
            var status=TransactionStatus.begin(helper.helper.cluster,helper.helper.database,data.directory.domainId(),new UUID(120,id),accepted.operationId(),1,new byte[32],accepted.payloadDigest(),List.of(participant));
            transition(null,status,id*10); return status;
        }
        void transition(TransactionStatus before,TransactionStatus after,long sequence) throws Exception {
            assertEquals(CommandResult.Status.SUCCESS,transactions.statuses.execute(transactions.statuses.prepare(new OperationId(new UUID(121,1),1,sequence),before,after)).status());
        }
        void releaseGate() throws Exception {
            var abort=ready.abort(); snapshots.transition(ready,abort);
            try (var proposal=data.f.machine.prepareSnapshotRelease(snapshots.homes,fence)) { helper.append(data,proposal); }
        }
        @Override public void close() throws Exception { data.close(); transactions.close(); snapshots.close(); }
    }
    @Test void trackedGateCaptureFreezesFullOriginalReadScopeAndCommitsNoEffectsAfterRelease() throws Exception {
        try (var f=new Fixture("read-commit"); var pin=f.capture()) {
            var cut=pin.position(); assertArrayEquals(new byte[]{7},pin.get(f.data.f.column,new byte[]{2})); assertNull(pin.get(f.data.f.column,new byte[]{3}));
            var accepted=f.intent(pin); assertTrue(accepted.readCertification()); assertEquals(cut,accepted.acceptance().snapshot());
            assertArrayEquals(ScopedTransactionRpc.scopeBytes(pin.scope()),ScopedTransactionRpc.scopeBytes(accepted.acceptance().scope())); assertTrue(accepted.mutations().isEmpty()); assertTrue(accepted.predicates().isEmpty());
            assertEquals(0,CommandResult.decode(accepted.acceptance().result()).valueCount()); f.releaseGate();
            var initial=f.register(accepted,1); ParticipantVote vote;
            try (var proposal=f.data.f.machine.prepareHandleParticipant(pin,accepted,helper.helper.control(0),initial.transactionId(),null)) {
                assertFalse(proposal.negative()); assertArrayEquals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(proposal.preparation().business()));
                vote=f.data.routed.executeParticipant(f.data.routed.prepareParticipant(proposal.preparation()));
            }
            assertTrue(vote.prepared()); assertThrows(IOException.class,() -> f.data.wire.command(f.data.directory.ranges().getFirst(),f.data.f.put(3)));
            var prepared=initial.prepared(f.data.f.groupId,accepted.payloadDigest(),vote.position()); f.transition(initial,prepared,11);
            var committed=prepared.decide(TransactionStatus.Phase.COMMIT); f.transition(prepared,committed,12);
            try (var proposal=f.data.f.machine.prepareFinalization(f.transactions.statuses,initial.transactionId(),new OperationId(new UUID(122,1),0,0))) {
                f.data.routed.executeFinalization(f.data.routed.prepareFinalization(proposal.finalization()));
            }
            var result=CommandResult.decode(f.data.f.store.finalizedParticipant(initial.transactionId()).result()); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(0,result.valueCount());
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); assertNull(f.data.routed.read(f.data.f.column,new byte[]{3}));
            assertEquals(0,f.data.f.store.activeTrackedSnapshotPins()); helper.helper.released(f.data);
        }
    }
    @Test void newSnapshotGateRejectsReadParticipantUsingTheFrozenOriginalIntent() throws Exception {
        try (var f=new Fixture("read-negative"); var pin=f.capture()) {
            var accepted=f.intent(pin); var initial=f.register(accepted,2);
            try (var proposal=f.data.f.machine.prepareHandleParticipant(pin,accepted,helper.helper.control(1),initial.transactionId(),null)) {
                assertTrue(proposal.negative()); assertArrayEquals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(proposal.preparation().business()));
                var vote=f.data.routed.executeParticipant(f.data.routed.restoreParticipant(proposal.preparation(),f.data.directory.ranges().getFirst(),true)); assertFalse(vote.prepared());
            }
            assertNull(f.data.f.store.preparedParticipant(initial.transactionId())); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins());
            assertArrayEquals(new byte[]{7},f.data.routed.read(f.data.f.column,new byte[]{2})); helper.helper.released(f.data);
        }
    }
    @Test void insertionInOriginalRangeFailsReadCertificationWithoutRebasing() throws Exception {
        try (var f=new Fixture("read-phantom"); var pin=f.capture()) {
            var accepted=f.intent(pin); var initial=f.register(accepted,3); f.releaseGate();
            f.data.wire.command(f.data.directory.ranges().getFirst(),f.data.f.put(3));
            assertThrows(IllegalStateException.class,() -> f.data.f.machine.prepareHandleParticipant(pin,accepted,helper.helper.control(2),initial.transactionId(),null));
            assertEquals(RangeMutationHistory.Status.CONFLICT,pin.status()); assertNull(f.data.f.store.preparedParticipant(initial.transactionId()));
            assertEquals(accepted.acceptance().snapshot(),pin.position()); helper.helper.released(f.data);
        }
    }
    @Test void trackedCaptureRejectsChangedHomeAndLostOwnerWithoutLeakingReaders() throws Exception {
        try (var f=new Fixture("capture-owner")) {
            var changed=new SnapshotFence(f.fence.clusterId(),f.fence.databaseId(),f.fence.domainId(),f.fence.fenceId(),f.fence.homeGroup(),f.fence.homeEpoch(),f.fence.operation(),f.fence.directoryGeneration()+1,f.fence.schemaEpoch(),f.fence.schemaDigest(),f.fence.participants());
            assertThrows(IllegalArgumentException.class,() -> f.data.f.machine.captureTrackedSnapshotGate(f.snapshots.homes,changed));
            f.snapshots.wire.afterRead=() -> { f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false)); f.snapshots.wire.afterRead=null; };
            assertThrows(IOException.class,f::capture); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins()); assertEquals(0,f.data.f.store.activeSnapshotPins());
        }
    }
    @Test void foreignNativePinAndChangedAcceptanceOwnerCannotCreateReadAuthority() throws Exception {
        try (var f=new Fixture("read-identity"); var other=helper.helper.new Node("foreign-data",false); var pin=f.capture(); var foreign=other.f.store.captureTracked(helper.scope(other,1,4))) {
            assertThrows(IllegalArgumentException.class,() -> f.data.f.machine.participantReadIntent(foreign,new OperationId(f.data.f.session,1,101)));
            assertThrows(IllegalArgumentException.class,() -> f.data.f.machine.participantIntent(foreign,f.data.f.put(3)));
            var accepted=f.intent(pin); var initial=f.register(accepted,4); f.releaseGate();
            var original=accepted.acceptance(); var changed=DistributedCommand.readCertification(accepted.groupId(),accepted.rangeEpoch(),accepted.schemaDigest(),accepted.operationId(),
                    new TransactionAcceptance(original.snapshot(),original.leaderGeneration(),UUID.randomUUID(),original.scope(),original.result()));
            assertThrows(IllegalArgumentException.class,() -> f.data.f.machine.prepareHandleParticipant(pin,changed,helper.helper.control(3),initial.transactionId(),null));
            assertEquals(RangeMutationHistory.Status.ACTIVE,pin.status()); helper.helper.released(f.data);
        }
    }
}
