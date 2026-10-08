package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Local-only durable recovery through native groups and controlled dispatch, without the original full coordinator journal. */
class RatisParticipantRecoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCoordinatorTest helper=new RatisSnapshotCoordinatorTest();
    OperationId nextHome(OperationId last) { return helper.control(Math.max(42,last.sequence()+1)); }
    RatisSnapshotCoordinatorTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name); }
    RatisDurableParticipantRecovery create(Path path,RatisSnapshotCoordinatorTest.Fixture f,UUID transaction) throws Exception {
        var prepared=f.data.preparedParticipant(f.right.f.groupId,transaction);
        return RatisDurableParticipantRecovery.create(path,f.transactions.statuses,f.data,prepared,f.directory.ranges().getLast(),helper.control(40),helper.control(41));
    }
    RatisParticipantRecovery recovery(RatisDurableParticipantRecovery journal,RatisSnapshotCoordinatorTest.Fixture f,boolean takeover) throws Exception {
        return new RatisParticipantRecovery(journal,f.transactions.statuses,f.data,this::nextHome,(group,tx,control) -> {
            try { var proposal=f.node(group).f.machine.prepareFinalization(f.transactions.statuses,tx,control); return new RatisTransactionRecovery.Authorization(proposal.finalization(),proposal); }
            catch (Exception failure) { throw new IOException(failure); }
        },takeover);
    }
    void drain(RatisParticipantRecovery recovery,RatisSnapshotCoordinatorTest.Fixture f) throws Exception {
        boolean complete=false;
        for (int n=0;n<12 && !complete;n++) { long before=f.writes(); complete=recovery.step().complete(); assertTrue(f.writes()-before<=1); }
        assertTrue(complete);
    }
    @Test void lostFinalizeReplyReopensAndAcknowledgesOnlyThisParticipant() throws Exception {
        Path path=root.resolve("commit.journal");
        try (var f=fixture("local-commit")) {
            f.committedTransactionWithRightStillPrepared(); long applied;
            try (var journal=create(path,f,helper.tx); var recovery=recovery(journal,f,false)) {
                f.right.wire.lose=true; assertThrows(IOException.class,recovery::step); applied=f.right.wire.index;
                assertNotNull(journal.snapshot().finalization()); assertNull(journal.snapshot().outcome());
            }
            try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data); var recovery=recovery(journal,f,false)) {
                drain(recovery,f); assertEquals(applied,f.right.wire.index);
                var home=f.transactions.statuses.read(helper.tx); assertEquals(TransactionStatus.Phase.COMMIT,home.phase()); assertFalse(home.fullyFinalized()); assertFalse(home.consumed());
                assertTrue(home.participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized());
                assertFalse(home.participants().stream().filter(p -> p.groupId().equals(f.left.f.groupId)).findFirst().orElseThrow().finalized());
                assertArrayEquals(new byte[]{8},f.data.read(helper.column,new byte[]{(byte)130}));
            }
        }
    }
    @Test void lostLocalAckReplyReopensAndCompletesFromFreshHomeEvidence() throws Exception {
        Path path=root.resolve("ack.journal");
        try (var f=fixture("local-ack")) {
            f.committedTransactionWithRightStillPrepared();
            try (var journal=create(path,f,helper.tx); var recovery=recovery(journal,f,false)) {
                assertEquals(RatisParticipantRecovery.Action.FINALIZED,recovery.step().action());
                f.transactions.wire.lose=true; assertThrows(IOException.class,recovery::step); assertNull(journal.snapshot().homeResult());
            }
            try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data); var recovery=recovery(journal,f,false)) {
                long before=f.writes(); assertTrue(recovery.step().complete()); assertEquals(before,f.writes()); assertTrue(journal.snapshot().homeSuperseded()); assertFalse(journal.snapshot().observedHome().fullyFinalized());
            }
        }
    }
    @Test void forcedFinalizeReceiptReopensAndResolvesProofWithoutWriteAuthority() throws Exception {
        Path path=root.resolve("receipt.journal");
        try (var f=fixture("local-receipt")) {
            f.committedTransactionWithRightStillPrepared(); var failProof=new java.util.concurrent.atomic.AtomicBoolean();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { byte[] reply=f.node(route.groupId()).wire.participant(route,bytes); failProof.set(true); return reply; }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    byte[] reply=f.node(route.groupId()).wire.transaction(route,bytes,fresh);
                    if (ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.PARTICIPANT_OUTCOME && failProof.getAndSet(false)) throw new IOException("Lost native terminal proof reply"); return reply;
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport)) {
                var prepared=data.preparedParticipant(f.right.f.groupId,helper.tx);
                try (var journal=RatisDurableParticipantRecovery.create(path,f.transactions.statuses,data,prepared,f.directory.ranges().getLast(),helper.control(40),helper.control(41));
                        var recovery=new RatisParticipantRecovery(journal,f.transactions.statuses,data,this::nextHome,(g,t,c) -> {
                            try { var proposal=f.node(g).f.machine.prepareFinalization(f.transactions.statuses,t,c); return new RatisTransactionRecovery.Authorization(proposal.finalization(),proposal); } catch (Exception failure) { throw new IOException(failure); }
                        },false)) {
                    assertThrows(IOException.class,recovery::step); assertNotNull(journal.snapshot().finalizationReceipt()); assertNull(journal.snapshot().outcome());
                }
                try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,data);
                        var recovery=new RatisParticipantRecovery(journal,f.transactions.statuses,data,this::nextHome,(g,t,c) -> { throw new AssertionError("Forced receipt only needs native lookup"); },false)) {
                    drain(recovery,f); assertNotNull(journal.snapshot().outcome());
                }
            }
        }
    }
    @Test void savedReceiptCannotBeProvenByAnotherRecoveryIdentityAtTheSamePosition() throws Exception {
        Path path=root.resolve("crossed-receipt.journal");
        try (var f=fixture("crossed-receipt")) {
            f.committedTransactionWithRightStillPrepared(); var prepared=f.data.preparedParticipant(f.right.f.groupId,helper.tx);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return f.node(route.groupId()).wire.transaction(route,bytes,fresh); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
                    try { f.finalize(f.right,helper.control(7)); } catch (Exception failure) { throw new IOException(failure); }
                    var position=f.right.f.store.committedParticipant(helper.tx).position(); return RatisParticipantRecord.result(position);
                }
            };
            try (var data=new RatisRoutedClient(f.directory,transport)) {
                try (var journal=RatisDurableParticipantRecovery.create(path,f.transactions.statuses,data,prepared,f.directory.ranges().getLast(),helper.control(40),helper.control(41))) {
                    var finalization=new ParticipantFinalization(f.transactions.f.groupId,1,f.right.f.groupId,helper.control(41),journal.snapshot().observedHome());
                    journal.freezeFinalization(data.restoreFinalization(finalization,f.directory.ranges().getLast()));
                    assertThrows(IOException.class,journal::executeFinalization); assertNotNull(journal.snapshot().finalizationReceipt()); assertNull(journal.snapshot().outcome());
                }
                try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,data)) {
                    assertThrows(IOException.class,journal::resolveParticipant); assertNull(journal.snapshot().outcome());
                }
            }
        }
    }
    @Test void snapshotHelperUsesDurableLocalRecoveryWithoutTheFullCoordinatorJournal() throws Exception {
        try (var f=fixture("durable-snapshot-helper")) {
            f.committedTransactionWithRightStillPrepared();
            try (var local=create(root.resolve("helper-local.journal"),f,helper.tx); var recovery=recovery(local,f,false)) {
                var initial=SnapshotFenceStatus.begin(f.fence);
                try (var snapshots=RatisDurableSnapshot.create(root.resolve("helper-snapshot.journal"),f.home.homes,f.data,initial,f.home.homes.prepare(helper.control(50),null,initial),f.directory.ranges());
                        var coordinator=new RatisSnapshotCoordinator(snapshots,f.home.homes,f.data,last -> helper.control(last.sequence()+1),request -> {
                            try { var node=f.node(request.participantGroup()); var proposal=request.isRelease() ? node.f.machine.prepareSnapshotRelease(f.home.homes,f.fence) : node.f.machine.prepareSnapshotInstall(f.home.homes,f.fence);
                                return new RatisSnapshotCoordinator.Authorization(proposal.request(),proposal::valid,proposal::drained,proposal);
                            } catch (Exception failure) { throw new IOException(failure); }
                        },new RatisSnapshotReadinessHelper(f.fence,f.data,(group,prepared) -> {
                            assertEquals(local.snapshot().prepared(),prepared); return recovery.step().action()!=RatisParticipantRecovery.Action.WAITING;
                        }),UUID::randomUUID,java.time.Duration.ofSeconds(10))) {
                    boolean complete=false;
                    for (int n=0;n<32 && !complete;n++) { long before=f.writes(); complete=coordinator.step().complete(); assertTrue(f.writes()-before<=1); }
                    assertTrue(complete); assertNotNull(local.snapshot().outcome()); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,snapshots.snapshot().observedHome().phase());
                    assertFalse(local.snapshot().observedHome().participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized());
                    assertTrue(recovery.step().complete()); assertFalse(local.snapshot().observedHome().fullyFinalized());
                }
            }
        }
    }
    @Test void cancelledStepViewCannotReleaseJournalOrDriverOwnershipBeforeActualReturn() throws Exception {
        try (var f=fixture("active-owner"); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            f.committedTransactionWithRightStillPrepared();
            try (var journal=create(root.resolve("active.journal"),f,helper.tx); var recovery=recovery(journal,f,false)) {
                var entered=new java.util.concurrent.CountDownLatch(1); var release=new java.util.concurrent.CountDownLatch(1); var done=new java.util.concurrent.CountDownLatch(1);
                f.transactions.wire.afterRead=() -> {
                    entered.countDown(); try { if (!release.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                    catch (InterruptedException failure) { throw new AssertionError(failure); }
                };
                var view=executor.submit(() -> { try { return recovery.step(); } finally { done.countDown(); } });
                try {
                    assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(view.cancel(false));
                    assertThrows(IOException.class,recovery::close); assertThrows(IOException.class,journal::close);
                } finally { release.countDown(); }
                assertTrue(done.await(5,java.util.concurrent.TimeUnit.SECONDS)); f.transactions.wire.afterRead=null;
                assertNotNull(journal.snapshot().outcome()); drain(recovery,f);
            }
        }
    }
    @Test void undecidedHomeNeedsExplicitTakeoverAndLostAbortReplyNeverRecreatesRegistration() throws Exception {
        Path path=root.resolve("abort.journal");
        try (var f=fixture("local-abort")) {
            f.committedTransactionWithRightStillPrepared(); UUID tx=f.anotherPreparedRight();
            try (var journal=create(path,f,tx); var follower=recovery(journal,f,false)) {
                long before=f.writes(); assertEquals(RatisParticipantRecovery.Action.WAITING,follower.step().action()); assertEquals(before,f.writes());
            }
            try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data); var takeover=recovery(journal,f,true)) {
                f.transactions.wire.lose=true; assertThrows(IOException.class,takeover::step); assertNull(journal.snapshot().homeResult());
                assertEquals(TransactionStatus.Phase.ABORT,f.transactions.statuses.read(tx).phase());
            }
            try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data); var recovery=recovery(journal,f,true)) {
                drain(recovery,f); var home=f.transactions.statuses.read(tx); assertEquals(TransactionStatus.Phase.ABORT,home.phase()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed());
                assertNull(f.data.read(helper.column,new byte[]{(byte)131})); assertNotNull(journal.snapshot().outcome()); assertTrue(journal.snapshot().homeSuperseded() || journal.snapshot().homeResult()!=null);
            }
        }
    }
    @Test void commitWinningAgainstTakeoverAbortIsFollowedWithoutChangingItsDecision() throws Exception {
        try (var f=fixture("decision-race")) {
            f.committedTransactionWithRightStillPrepared(); UUID tx=f.anotherPreparedRight();
            try (var journal=create(root.resolve("race.journal"),f,tx); var recovery=recovery(journal,f,true)) {
                var before=journal.snapshot().observedHome(); var prepared=journal.snapshot().prepared();
                f.transactions.wire.afterRead=() -> {
                    f.transactions.wire.afterRead=null;
                    try {
                        var voted=before.prepared(f.right.f.groupId,prepared.preparation().business().payloadDigest(),prepared.position());
                        f.txHome(before,voted,35); f.txHome(voted,voted.decide(TransactionStatus.Phase.COMMIT),36);
                    } catch (Exception failure) { throw new AssertionError(failure); }
                };
                assertEquals(RatisParticipantRecovery.Action.ABORT_ATTEMPTED,recovery.step().action());
                assertEquals(CommandResult.Status.CONFLICT,journal.snapshot().homeResult().status());
                drain(recovery,f); assertEquals(TransactionStatus.Phase.COMMIT,journal.snapshot().observedHome().phase());
                assertArrayEquals(new byte[]{6},f.data.read(helper.column,new byte[]{(byte)131})); assertFalse(journal.snapshot().observedHome().consumed());
            }
        }
    }
    @Test void absentHomeCannotBeRegisteredByParticipantRecovery() throws Exception {
        try (var f=fixture("absent-home")) {
            f.committedTransactionWithRightStillPrepared(); var prepared=f.data.preparedParticipant(f.right.f.groupId,helper.tx); long before=f.writes();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { return null; }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Recovery must never create a missing home"); }
            };
            try (var client=new RatisRoutedClient(f.transactions.directory,transport)) {
                var homes=new RatisTransactionStatuses(client,f.transactions.f.column);
                assertThrows(IOException.class,() -> RatisDurableParticipantRecovery.create(root.resolve("absent.journal"),homes,f.data,prepared,f.directory.ranges().getLast(),helper.control(40),helper.control(41)));
                assertEquals(before,f.writes());
            }
        }
    }
    @Test void foreignFinalizationIsVerifiedWithoutAcquiringAnotherWriteAuthority() throws Exception {
        Path path=root.resolve("foreign.journal");
        try (var f=fixture("local-foreign")) {
            f.committedTransactionWithRightStillPrepared();
            try (var journal=create(path,f,helper.tx)) { f.finalize(f.right,helper.control(7)); }
            try (var journal=RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data);
                    var recovery=new RatisParticipantRecovery(journal,f.transactions.statuses,f.data,this::nextHome,(g,t,c) -> { throw new AssertionError("Native terminal proof needs no write authority"); },false)) {
                drain(recovery,f); assertEquals(helper.control(7),journal.snapshot().outcome().finalization().operation()); assertNull(journal.snapshot().finalization());
            }
        }
    }
    @Test void journalRejectsUnprovenAckAndForeignParticipantAckAndHasExclusiveOwnership() throws Exception {
        Path path=root.resolve("evidence.journal");
        try (var f=fixture("local-evidence")) {
            f.committedTransactionWithRightStillPrepared();
            try (var journal=create(path,f,helper.tx)) {
                var home=journal.snapshot().observedHome(); var prepared=journal.snapshot().prepared();
                var forged=new PreparedParticipant(prepared.preparation(),new ApplyPosition(prepared.position().term(),prepared.position().index()+1));
                assertThrows(IOException.class,() -> RatisDurableParticipantRecovery.create(root.resolve("forged.journal"),f.transactions.statuses,f.data,forged,f.directory.ranges().getLast(),helper.control(40),helper.control(41)));
                assertThrows(IOException.class,() -> RatisDurableParticipantRecovery.reopen(path,f.transactions.statuses,f.data));
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.transactions.statuses.prepare(helper.control(42),home,home.finalized(f.right.f.groupId))));
                f.finalize(f.right,helper.control(7)); assertNotNull(journal.resolveParticipant());
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.transactions.statuses.prepare(helper.control(42),home,home.finalized(f.left.f.groupId))));
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.transactions.statuses.prepare(helper.control(41),home,home.finalized(f.right.f.groupId))));
            }
        }
    }
}
