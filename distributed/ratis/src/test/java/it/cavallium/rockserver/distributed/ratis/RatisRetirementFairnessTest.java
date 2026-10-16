package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Fair discovery and pending replay over real native groups with controlled synchronous transport. */
class RatisRetirementFairnessTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisRetirementCoordinatorTest owner=new RatisRetirementCoordinatorTest();
    final RatisSnapshotCoordinatorTest fixtures=new RatisSnapshotCoordinatorTest();
    RatisSnapshotCoordinatorTest.Fixture fixture(String name) throws Exception { owner.root=root; fixtures.root=root; return fixtures.new Fixture(name); }
    OperationId protectedControl(long sequence) { return new OperationId(new UUID(121,1),0,sequence); }
    TransactionStatus consumeBoth(RatisSnapshotCoordinatorTest.Fixture f) throws Exception {
        f.committedTransactionWithRightStillPrepared(); f.finalize(f.right,fixtures.control(7));
        var status=f.transactions.statuses.read(fixtures.tx); int sequence=0;
        for (var participant:status.participants()) {
            var outcome=f.data.finalizedParticipant(participant.groupId(),fixtures.tx); assertNotNull(outcome);
            var next=status.finalized(participant.groupId());
            assertEquals(CommandResult.Status.SUCCESS,f.transactions.statuses.executeRecovery(f.transactions.statuses.prepareRecovery(protectedControl(sequence++),status,next,outcome)).result().status()); status=next;
        }
        var consumed=status.consume(); f.txHome(status,consumed,40); return consumed;
    }
    void floor(RatisParticipantPrepareTest.Node node) throws Exception {
        node.wire.command(node.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(node.f.groupId,1,new byte[32],new OperationId(node.f.admin,4,0),2));
    }
    void floors(RatisSnapshotCoordinatorTest.Fixture f) throws Exception { floor(f.left); floor(f.right); floor(f.transactions); }
    UUID first(TransactionStatus status) { return status.participants().get(Math.floorMod(status.transactionId().hashCode(),status.participants().size())).groupId(); }
    RatisRoutedClient routed(RatisSnapshotCoordinatorTest.Fixture f,AtomicReference<UUID> inaccessible,AtomicReference<UUID> failedAppend,AtomicInteger calls) {
        return new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
            void query(UUID group) throws IOException { calls.incrementAndGet(); if (group.equals(inaccessible.get())) throw new IOException("Unavailable DATA group"); }
            public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { query(route.groupId()); return f.node(route.groupId()).wire.read(route,column,key); }
            public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { query(route.groupId()); return f.node(route.groupId()).wire.command(route,command); }
            public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { query(route.groupId()); return f.node(route.groupId()).wire.transaction(route,bytes,fresh); }
            public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
                query(route.groupId()); if (route.groupId().equals(failedAppend.get())) throw new IOException("Unavailable retirement append");
                return f.node(route.groupId()).wire.participant(route,bytes);
            }
            public RangeStateView stateView(RangeDescriptor route) throws IOException {
                return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(route,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body());
            }
        });
    }
    int visit(RatisRetirementCoordinator driver,RatisSnapshotCoordinatorTest.Fixture f) throws Exception {
        long before=f.writes(); int attempts=driver.cycle(1); assertTrue(f.writes()-before<=1,"One mutation per retirement visit"); return attempts;
    }
    void drain(RatisRetirementCoordinator driver,RatisSnapshotCoordinatorTest.Fixture f) throws Exception {
        for (int n=0;n<24 && (f.transactions.statuses.read(fixtures.tx)!=null || driver.stats().count()!=0);n++) visit(driver,f);
        assertNull(f.transactions.statuses.read(fixtures.tx)); assertEquals(0,driver.stats().count()); assertNull(driver.lastFailure());
        assertNull(f.left.f.store.finalizedParticipant(fixtures.tx)); assertNull(f.right.f.store.finalizedParticipant(fixtures.tx));
        assertArrayEquals(new byte[]{7},f.data.read(fixtures.column,new byte[]{1})); assertArrayEquals(new byte[]{8},f.data.read(fixtures.column,new byte[]{(byte)130}));
    }
    RatisRoutedClient.PreparedHomeRecoveryOperation orphan(RatisParticipantPrepareTest.Node home,UUID cluster,UUID database,UUID domain) throws Exception {
        var absent=new UUID(121,2); var initial=TransactionStatus.begin(cluster, database, new UUID(121,3), new OperationId(new UUID(121,4),1,0), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], List.of(new TransactionStatus.Participant(domain, absent, absent, 1, new byte[32], new byte[32], null, false)));
        var request=home.statuses.prepareRecovery(new OperationId(new UUID(121,5),0,0),initial,initial.decide(TransactionStatus.Phase.ABORT),null);
        assertEquals(CommandResult.Status.CONFLICT,home.statuses.executeRecovery(request).result().status()); assertNull(home.statuses.read(initial.transactionId())); return request;
    }

    @Test void fullScanRotationSkipsAnUnavailableFirstDataGroupButHomeWaitsForEveryProof() throws Exception {
        try (var f=fixture("rotation")) {
            var consumed=consumeBoth(f); floors(f); UUID unavailable=first(consumed);
            UUID healthy=consumed.participants().stream().map(TransactionStatus.Participant::groupId).filter(group -> !group.equals(unavailable)).findFirst().orElseThrow();
            var blocked=new AtomicReference<>(unavailable); var failedAppend=new AtomicReference<UUID>(); var calls=new AtomicInteger();
            try (var data=routed(f,blocked,failedAppend,calls);
                    var driver=owner.coordinator("rotation-journal",f.transactions.statuses,null,data,List.of(owner.source(f.transactions.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,f.transactions.f.machine)))) {
                long homeIndex=f.transactions.wire.index; assertEquals(0,visit(driver,f)); assertNotNull(driver.lastFailure()); assertEquals(0,driver.stats().count());
                assertEquals(1,visit(driver,f)); assertNull(f.node(healthy).f.store.finalizedParticipant(fixtures.tx)); assertNotNull(f.node(unavailable).f.store.finalizedParticipant(fixtures.tx));
                for (int n=0;n<4;n++) { visit(driver,f); assertEquals(consumed,f.transactions.statuses.read(fixtures.tx)); assertEquals(homeIndex,f.transactions.wire.index); }
                blocked.set(null); drain(driver,f);
            }
        }
    }

    @Test void failedPendingDataTargetDoesNotBlockHealthyOrphanSourceOrLoseItsExactFrame() throws Exception {
        try (var f=fixture("pending-fairness")) {
            var consumed=consumeBoth(f); long ordinaryReceipts=f.transactions.f.store.homeRecoveryCapacity().count();
            var orphan=orphan(f.transactions,f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId()); floors(f); UUID unavailable=first(consumed);
            var blocked=new AtomicReference<UUID>(); var appendFailure=new AtomicReference<>(unavailable); var calls=new AtomicInteger();
            var sources=List.of(owner.source(f.transactions.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,f.transactions.f.machine),owner.source(f.transactions.f.groupId,RatisRetirementCoordinator.Mode.ORPHAN,f.transactions.f.machine));
            try (var data=routed(f,blocked,appendFailure,calls); var driver=owner.coordinator("pending-fairness-journal",f.transactions.statuses,null,data,sources)) {
                assertEquals(0,visit(driver,f)); assertNotNull(driver.lastFailure()); assertEquals(1,driver.stats().count());
                var path=root.resolve("pending-fairness-journal").resolve(unavailable+".bin"); byte[] frozen=SessionJournalFiles.read(path,RatisRetirementOperation.MAX_BYTES+64);
                assertEquals(unavailable,RatisRetirementOperation.decode(Arrays.copyOfRange(frozen,32,frozen.length)).targetGroup());
                assertEquals(0,visit(driver,f)); assertEquals(1,driver.stats().count());
                int before=calls.get(); long homeIndex=f.transactions.wire.index;
                assertEquals(1,visit(driver,f)); assertEquals(before,calls.get(),"Orphan discovery/retirement must not query DATA"); assertEquals(homeIndex+1,f.transactions.wire.index);
                assertEquals(ordinaryReceipts,f.transactions.f.store.homeRecoveryCapacity().count()); assertEquals(consumed,f.transactions.statuses.read(fixtures.tx));
                assertArrayEquals(frozen,SessionJournalFiles.read(path,RatisRetirementOperation.MAX_BYTES+64)); assertEquals(1,driver.stats().count());
                assertThrows(RatisHomeRetiredException.class,() -> f.transactions.statuses.executeRecovery(orphan));
                appendFailure.set(null); drain(driver,f);
            }
        }
    }

    @Test void nativeOrphanConflictDiscoveryRetiresWithoutAnyDataReadOrInventedDecision() throws Exception {
        owner.root=root; var helper=new RatisParticipantPrepareTest(); helper.root=root;
        try (var home=helper.new Node("native-orphan",true)) {
            var orphan=orphan(home,helper.cluster,helper.database,new UUID(121,6)); floor(home);
            var page=home.f.machine.retirementCandidates(true,null,RatisRetirementCoordinator.SCAN_ROWS);
            assertNotNull(page.candidate()); assertTrue(page.candidate().orphan());
            assertEquals(TransactionStatus.Phase.PREPARING,page.candidate().transaction().phase()); assertFalse(page.candidate().transaction().consumed());
            var noData=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError("Orphan cleanup has no DATA dependency"); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Orphan cleanup must not send DATA commands"); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) { throw new AssertionError("Orphan cleanup must not query DATA"); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) { throw new AssertionError("Orphan cleanup must not dispatch DATA records"); }
            };
            try (var data=new RatisRoutedClient(home.directory,noData); var driver=owner.coordinator("native-orphan-journal",home.statuses,null,data,List.of(owner.source(home.f.groupId,RatisRetirementCoordinator.Mode.ORPHAN,home.f.machine)))) {
                long before=home.wire.index; assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure()); assertEquals(before+1,home.wire.index);
                assertEquals(0,driver.stats().count()); assertEquals(0,home.f.store.homeRecoveryCapacity().count()); assertNull(home.statuses.read(orphan.recovery().before().transactionId()));
                assertEquals(0,driver.cycle(4)); assertEquals(before+1,home.wire.index); assertThrows(RatisHomeRetiredException.class,() -> home.statuses.executeRecovery(orphan));
            }
        }
    }
}
