package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual native retirement through controlled transports; no servers or network. */
class RatisRetirementCoordinatorTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRetirementCoordinator.Source source(UUID group,RatisRetirementCoordinator.Mode mode,RatisRangeStateMachine machine) {
        return new RatisRetirementCoordinator.Source(group,mode,(after,scans) -> machine.retirementCandidates(mode==RatisRetirementCoordinator.Mode.ORPHAN,after,scans));
    }
    RatisRoutedClient data(RatisParticipantPrepareTest.Node node) {
        return new RatisRoutedClient(node.directory,new RatisRoutedClient.Transport() {
            public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return node.wire.command(route,command); }
            public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return node.wire.read(route,column,key); }
            public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return node.wire.participant(route,record); }
            public byte[] transaction(RangeDescriptor route,byte[] record,boolean fresh) throws IOException { return node.wire.transaction(route,record,fresh); }
            public RangeStateView stateView(RangeDescriptor route) throws IOException {
                return RangeStateView.decode(ScopedTransactionRpc.parse(transaction(route,ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]),true),ScopedTransactionRpc.MAX_REPLY_BYTES).body());
            }
        });
    }
    RatisRetirementCoordinator coordinator(String path,RatisTransactionStatuses homes,RatisSnapshotFences snapshots,RatisRoutedClient data,List<RatisRetirementCoordinator.Source> sources) throws IOException {
        return new RatisRetirementCoordinator(root.resolve(path), homes, snapshots, Map.of(data.directory().domainId(), data), sources, RatisRetirementJournal.Limits.defaults());
    }
    @Test void consumedCommitAndAbortRetireDataThenHomeOneMutationPerVisit() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        for (boolean commit:new boolean[]{false,true}) try (var x=owner.new Fixture("auto-"+commit,commit); var data=data(x.f.data)) {
            x.participant.floor(); x.floor(); var home=x.f.home; var id=x.f.initial.transactionId();
            try (var driver=coordinator("journal-"+commit,home.statuses,null,data,List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine)))) {
                long dataIndex=x.f.data.wire.index,homeIndex=home.wire.index;
                assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure());
                assertEquals(dataIndex+1,x.f.data.wire.index); assertEquals(homeIndex,home.wire.index);
                assertNotNull(home.statuses.read(id)); assertNull(data.finalizedParticipant(x.f.data.f.groupId,id));
                assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure());
                assertEquals(dataIndex+1,x.f.data.wire.index); assertEquals(homeIndex+1,home.wire.index);
                assertNull(home.statuses.read(id)); assertEquals(0,driver.stats().count());
                assertEquals(0,driver.cycle(4)); assertNull(driver.lastFailure());
                assertArrayEquals(commit ? new byte[]{7} : null,data.read(x.f.data.f.column,new byte[]{2})); x.released();
            }
        }
    }
    @Test void unconsumedOutcomesAreNeverConsumedOrRetiredAndBelowFloorWaits() throws Exception {
        var owner=new RatisParticipantRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("unconsumed-auto",true,false); var data=data(x.f.data)) {
            x.floor(); var home=x.f.home; long homeIndex=home.wire.index,dataIndex=x.f.data.wire.index;
            try (var driver=coordinator("unconsumed-journal",home.statuses,null,data,List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine)))) {
                assertEquals(0,driver.cycle(8)); assertNull(driver.lastFailure()); assertFalse(home.statuses.read(x.f.initial.transactionId()).consumed());
                assertNotNull(data.finalizedParticipant(x.f.data.f.groupId,x.f.initial.transactionId())); assertEquals(homeIndex,home.wire.index); assertEquals(dataIndex,x.f.data.wire.index);
            }
        }
        try (var x=owner.new Fixture("unfenced-auto",true,true); var data=data(x.f.data)) {
            var home=x.f.home;
            try (var driver=coordinator("unfenced-journal",home.statuses,null,data,List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine)))) {
                long index=x.f.data.wire.index; assertEquals(0,driver.cycle(4)); assertNull(driver.lastFailure()); assertEquals(index,x.f.data.wire.index); assertEquals(0,driver.stats().count());
                x.floor(); assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure()); assertNotNull(home.statuses.read(x.f.initial.transactionId()));
            }
        }
    }
    @Test void consumedActiveSnapshotReleasesThenRetiresDataAndHome() throws Exception {
        var owner=new RatisSnapshotGateRpcTest(); owner.root=root;
        try (var f=owner.new Fixture("active-auto"); var data=data(f.data)) {
            data.executeSnapshotGate(f.install); var terminal=f.initial.abort(); f.home.transition(f.initial,terminal); var consumed=terminal.consume(); f.home.transition(terminal,consumed);
            f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(111,1),3,0),3));
            f.home.wire.command(f.home.routed.directory().ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(new UUID(111,2),4,0),4));
            try (var driver=coordinator("snapshot-journal",null,f.home.homes,data,List.of(source(f.home.f.groupId,RatisRetirementCoordinator.Mode.SNAPSHOT,f.home.f.machine)))) {
                long di=f.data.wire.index,hi=f.home.wire.index;
                assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure()); assertEquals(di+1,f.data.wire.index); assertEquals(hi,f.home.wire.index); assertFalse(data.snapshotGate(f.fence,f.data.f.groupId).active());
                assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure()); assertEquals(di+2,f.data.wire.index); assertEquals(hi,f.home.wire.index); assertNotNull(data.snapshotFence(f.fence,f.data.f.groupId));
                assertEquals(1,driver.cycle(1)); assertNull(driver.lastFailure()); assertEquals(di+2,f.data.wire.index); assertEquals(hi+1,f.home.wire.index); assertNull(f.home.homes.read(f.fence.fenceId())); assertEquals(0,driver.stats().count());
                owner.helper.released(f.data);
            }
        }
    }
    @Test void lostReplyReopensExactIntentBeforeFurtherDiscoveryAndAcceptsSameFamilyRouteRefresh() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("lost-auto",true); var data=data(x.f.data)) {
            x.participant.floor(); x.floor(); var home=x.f.home; var sources=List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine));
            try (var driver=coordinator("lost-journal",home.statuses,null,data,sources)) {
                x.f.data.wire.lose=true; driver.cycle(1); assertNotNull(driver.lastFailure()); assertEquals(1,driver.stats().count());
                assertNotNull(data.participantFence(x.participant.retirement.identity()));
            }
            long index=x.f.data.wire.index; var d=data.directory(); var old=d.ranges().getFirst(); var route=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion()+1,old.lowerInclusive(),old.upperExclusive(),old.topologyState(),old.replicas());
            data.refresh(new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),d.schemaEpoch(),d.schemaDigest(),d.generation()+1,d.columns(),d.routingPrefixBytes(),List.of(route)));
            try (var driver=coordinator("lost-journal",home.statuses,null,data,sources)) {
                assertEquals(1,driver.stats().count()); driver.cycle(4); assertNull(driver.lastFailure()); assertEquals(index,x.f.data.wire.index); assertEquals(0,driver.stats().count()); assertNull(home.statuses.read(x.f.initial.transactionId()));
            }
            x.released();
        }
    }
    @Test void sourceFailureDoesNotBlockHealthyDiscovery() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("fair-auto",false); var data=data(x.f.data)) {
            x.participant.floor(); x.floor(); var home=x.f.home;
            var broken=new RatisRetirementCoordinator.Source(UUID.randomUUID(),RatisRetirementCoordinator.Mode.TRANSACTION,(after,scans) -> { throw new IOException("Unavailable hint source"); });
            try (var driver=coordinator("fair-journal",home.statuses,null,data,List.of(broken,source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine)))) {
                assertEquals(2,driver.cycle(4)); assertNotNull(driver.lastFailure()); assertNull(home.statuses.read(x.f.initial.transactionId())); assertEquals(0,driver.stats().count()); x.released();
            }
        }
    }
    @Test void uncertainHomeAdmissionKeepsExactProofCutAcrossNativeProgressAndRestart() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("proof-auto",true); var data=data(x.f.data)) {
            x.ready(); var home=x.f.home; var sources=List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine));
            var reads=new java.util.concurrent.atomic.AtomicInteger();
            x.f.data.wire.beforeFence=() -> { if (reads.incrementAndGet()==2) throw new IllegalStateException("Lost HOME authority read before append"); };
            byte[] frozen;
            try (var driver=coordinator("proof-journal",home.statuses,null,data,sources)) {
                long index=home.wire.index; driver.cycle(1); assertNotNull(driver.lastFailure()); assertEquals(1,driver.stats().count()); assertEquals(index,home.wire.index);
                frozen=java.nio.file.Files.readAllBytes(root.resolve("proof-journal").resolve(home.f.groupId+".bin"));
            }
            x.f.data.wire.beforeFence=null;
            x.f.data.wire.command(x.f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(x.f.data.f.groupId,1,new byte[32],new OperationId(new UUID(112,1),6,0),6));
            assertArrayEquals(frozen,java.nio.file.Files.readAllBytes(root.resolve("proof-journal").resolve(home.f.groupId+".bin")));
            try (var driver=coordinator("proof-journal",home.statuses,null,data,sources)) {
                driver.cycle(2); assertNull(driver.lastFailure()); assertEquals(0,driver.stats().count()); assertNull(home.statuses.read(x.f.initial.transactionId()));
            }
            x.released();
        }
    }
    @Test void activeRpcRetainsCoordinatorAndJournalOwnershipUntilActualReturn() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("blocked-auto",true); var data=data(x.f.data); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            x.participant.floor(); x.floor(); var home=x.f.home; var sources=List.of(source(home.f.groupId,RatisRetirementCoordinator.Mode.TRANSACTION,home.f.machine));
            var driver=coordinator("blocked-journal",home.statuses,null,data,sources); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("HOME wait timed out"); } catch (InterruptedException e) { throw new AssertionError(e); } };
            try {
                var running=executor.submit(() -> driver.cycle(1)); assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,driver.stats().count());
                assertThrows(IOException.class,driver::close); assertThrows(IOException.class,() -> driver.cycle(1)); assertThrows(IOException.class,() -> coordinator("blocked-journal",home.statuses,null,data,sources));
                release.countDown(); assertEquals(1,running.get(5,TimeUnit.SECONDS)); assertNull(driver.lastFailure());
            } finally { release.countDown(); driver.close(); }
            try (var reopened=coordinator("blocked-journal",home.statuses,null,data,sources)) { assertEquals(1,reopened.cycle(1)); assertNull(reopened.lastFailure()); assertNull(home.statuses.read(x.f.initial.transactionId())); }
        }
    }
    @Test void discoveryPausesWithoutTheCurrentReadyNativeOwner() throws Exception {
        var owner=new RatisHomeRetirementRpcTest(); owner.root=root;
        try (var x=owner.new Fixture("owner-auto",false)) {
            var home=x.f.home; assertNotNull(home.f.machine.retirementCandidates(false,null,16).candidate());
            home.f.leadership.set(new RatisRangeStateMachine.Leadership(1,false)); assertNull(home.f.machine.retirementCandidates(false,null,16));
            home.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); assertNull(home.f.machine.retirementCandidates(false,null,16));
            home.f.machine.notifyTermIndexUpdated(2,++home.wire.index); assertNotNull(home.f.machine.retirementCandidates(false,null,16).candidate());
            home.f.machine.takeSnapshot(); home.f.machine.pause(); assertNull(home.f.machine.retirementCandidates(false,null,16));
            home.f.machine.reinitialize(); assertNotNull(home.f.machine.retirementCandidates(false,null,16).candidate());
        }
    }
}
