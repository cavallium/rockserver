package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Automatic native discovery with controlled group/native fixtures, no server, network or quorum acceptance. */
class RatisNativeParticipantDiscoveryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCoordinatorTest helper=new RatisSnapshotCoordinatorTest();
    final RatisParticipantRecoveryRegistryTest registries=new RatisParticipantRecoveryRegistryTest();
    RatisSnapshotCoordinatorTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name); }
    RatisNativeParticipantDiscovery discovery(RatisParticipantRecoveryRegistry registry,RatisSnapshotCoordinatorTest.Fixture f) {
        return new RatisNativeParticipantDiscovery(registry,List.of(new RatisNativeParticipantDiscovery.Source(f.left.f.groupId,f.left.f.machine::preparedRecoveries),new RatisNativeParticipantDiscovery.Source(f.right.f.groupId,f.right.f.machine::preparedRecoveries)));
    }
    @Test void nativeCommitAndMissingForeignAckAreRecoveredWithoutAnyInitialJournal() throws Exception {
        Path path=Files.createDirectory(root.resolve("native-commit"));
        try (var f=fixture("native-commit")) {
            f.committedTransactionWithRightStillPrepared(); assertFalse(f.transactions.statuses.read(helper.tx).fullyFinalized());
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(2,4,96L*1024*1024))) {
                var discovery=discovery(registry,f); assertEquals(0,registry.stats().persistedFiles());
                for (int n=0;n<16;n++) { long before=f.writes(); discovery.cycle(1); assertTrue(f.writes()-before<=1); }
                assertEquals(2,registry.stats().persistedFiles()); assertEquals(0,registry.stats().openWorkers());
                var home=f.transactions.statuses.read(helper.tx); assertEquals(TransactionStatus.Phase.COMMIT,home.phase()); assertTrue(home.fullyFinalized()); assertFalse(home.consumed());
                assertArrayEquals(new byte[]{7},f.data.read(helper.column,new byte[]{1})); assertArrayEquals(new byte[]{8},f.data.read(helper.column,new byte[]{(byte)130}));
                var reads=new java.util.concurrent.atomic.AtomicInteger(); f.transactions.wire.afterRead=reads::incrementAndGet; int allocations=registries.allocations.get();
                for (int n=0;n<8;n++) assertEquals(0,discovery.cycle(1));
                assertEquals(0,reads.get()); assertEquals(allocations,registries.allocations.get());
            }
        }
    }
    @Test void snapshotReloadDiscoversRetainedVotesBeforeAnyCoordinatorJournal() throws Exception {
        Path path=Files.createDirectory(root.resolve("reloaded"));
        try (var f=fixture("reloaded")) {
            f.committedTransactionWithRightStillPrepared(); f.left.f.machine.takeSnapshot(); f.right.f.machine.takeSnapshot();
            f.left.f.machine.pause(); f.right.f.machine.pause(); f.left.f.machine.reinitialize(); f.right.f.machine.reinitialize();
            assertEquals(1,f.right.f.store.preparedRecoveries(null,1).records().size());
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(2,4,96L*1024*1024))) {
                var discovery=discovery(registry,f);
                for (int n=0;n<16;n++) { long before=f.writes(); discovery.cycle(1); assertTrue(f.writes()-before<=1); }
                assertTrue(f.transactions.statuses.read(helper.tx).fullyFinalized()); assertEquals(0,registry.stats().openWorkers());
                assertArrayEquals(new byte[]{8},f.data.read(helper.column,new byte[]{(byte)130}));
            }
        }
    }
    @Test void explicitTakeoverFindsUndecidedNativePreparationAndAbortsThenAcknowledges() throws Exception {
        Path path=Files.createDirectory(root.resolve("native-abort"));
        try (var f=fixture("native-abort")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight();
            try (var registry=registries.registry(path,f,true,new RatisParticipantRecoveryRegistry.Limits(2,4,96L*1024*1024))) {
                var discovery=discovery(registry,f);
                for (int n=0;n<24;n++) { long before=f.writes(); discovery.cycle(1); assertTrue(f.writes()-before<=1); }
                var abort=f.transactions.statuses.read(other); assertEquals(TransactionStatus.Phase.ABORT,abort.phase()); assertTrue(abort.fullyFinalized()); assertFalse(abort.consumed());
                assertNull(f.data.read(helper.column,new byte[]{(byte)131})); assertTrue(f.transactions.statuses.read(helper.tx).fullyFinalized()); assertEquals(0,registry.stats().openWorkers());
            }
        }
    }
    @Test void unavailableSourceDoesNotStarveOtherGroupsOrAlreadyQueuedRecovery() throws Exception {
        Path path=Files.createDirectory(root.resolve("unavailable"));
        try (var f=fixture("unavailable")) {
            f.committedTransactionWithRightStillPrepared();
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                var discovery=new RatisNativeParticipantDiscovery(registry,List.of(new RatisNativeParticipantDiscovery.Source(f.left.f.groupId,(after,limit) -> { throw new IOException("Local discovery unavailable"); }),new RatisNativeParticipantDiscovery.Source(f.right.f.groupId,f.right.f.machine::preparedRecoveries)));
                int failed=0;
                for (int n=0;n<12;n++) { long before=f.writes(); try { discovery.cycle(1); } catch (IOException failure) { assertEquals("Local discovery unavailable",failure.getMessage()); failed++; } assertTrue(f.writes()-before<=1); }
                assertEquals(6,failed); var home=f.transactions.statuses.read(helper.tx); assertFalse(home.fullyFinalized());
                assertTrue(home.participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized()); assertEquals(0,registry.stats().openWorkers());
            }
        }
    }
    @Test void followerAndUncompletedNewTermDoNotAdmitUntilTheNativeOwnerIsReady() throws Exception {
        Path path=Files.createDirectory(root.resolve("owner"));
        try (var f=fixture("owner")) {
            f.committedTransactionWithRightStillPrepared();
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                var discovery=new RatisNativeParticipantDiscovery(registry,List.of(new RatisNativeParticipantDiscovery.Source(f.right.f.groupId,f.right.f.machine::preparedRecoveries)));
                f.right.f.machine.notifyNotLeader(List.of()); f.right.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
                assertEquals(0,discovery.cycle(1)); assertEquals(0,registry.stats().persistedFiles());
                f.right.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); assertEquals(0,discovery.cycle(1)); assertEquals(0,registry.stats().persistedFiles());
                f.right.f.machine.notifyTermIndexUpdated(2,++f.right.wire.index);
                for (int n=0;n<6;n++) { long before=f.writes(); discovery.cycle(1); assertTrue(f.writes()-before<=1); }
                assertEquals(1,registry.stats().persistedFiles()); assertTrue(f.transactions.statuses.read(helper.tx).participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized());
            }
        }
    }
    @Test void unavailablePersistedJournalDoesNotBlockHealthyNativeOrphanDiscovery() throws Exception {
        Path path=Files.createDirectory(root.resolve("startup-failure"));
        try (var f=fixture("startup-failure")) {
            f.committedTransactionWithRightStillPrepared(); var limits=new RatisParticipantRecoveryRegistry.Limits(2,4,96L*1024*1024);
            try (var registry=registries.registry(path,f,false,limits)) { registry.register(f.left.f.groupId,f.data.preparedParticipant(f.left.f.groupId,helper.tx)); }
            var controls=registries.controls();
            try (var registry=new RatisParticipantRecoveryRegistry(path,f.transactions.statuses,f.data,(group,tx) -> { if (group.equals(f.left.f.groupId)) throw new IOException("Persisted worker grant unavailable"); return controls.create(group,tx); },(group,tx,control) -> {
                try { var proposal=f.node(group).f.machine.prepareFinalization(f.transactions.statuses,tx,control); return new RatisTransactionRecovery.Authorization(proposal.finalization(),proposal); } catch (Exception failure) { throw new IOException(failure); }
            },false,Set.of(f.left.f.groupId,f.right.f.groupId),limits)) {
                var discovery=new RatisNativeParticipantDiscovery(registry,List.of(new RatisNativeParticipantDiscovery.Source(f.right.f.groupId,f.right.f.machine::preparedRecoveries)));
                for (int n=0;n<12;n++) { long before=f.writes(); try { discovery.cycle(1); } catch (IOException expected) { assertEquals("Persisted worker grant unavailable",expected.getMessage()); } assertTrue(f.writes()-before<=1); }
                var home=f.transactions.statuses.read(helper.tx); assertFalse(home.fullyFinalized()); assertTrue(home.participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized());
            }
        }
    }
    @Test void pausedGroupDoesNotBlockHealthyRecoveryWhenEveryGroupIsVisited() throws Exception {
        Path path=Files.createDirectory(root.resolve("paused"));
        try (var f=fixture("paused")) {
            f.committedTransactionWithRightStillPrepared(); f.left.f.machine.pause();
            assertNull(f.left.f.machine.preparedRecoveries(null,1));
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                var discovery=discovery(registry,f);
                for (int n=0;n<8;n++) { long before=f.writes(); discovery.cycle(2); assertTrue(f.writes()-before<=2); }
                var home=f.transactions.statuses.read(helper.tx); assertFalse(home.fullyFinalized()); assertTrue(home.participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized());
            }
        }
    }
    @Test void nativePaginationIncludesTerminalVotesAndRejectsInvalidCutsAndCompletedIdentityReuse() throws Exception {
        Path path=Files.createDirectory(root.resolve("pages"));
        try (var f=fixture("pages")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight();
            var first=f.right.f.store.preparedRecoveries(null,1); assertEquals(helper.tx,first.records().getFirst().preparation().transactionId()); assertTrue(first.more());
            var last=f.right.f.store.preparedRecoveries(helper.tx,1); assertEquals(other,last.records().getFirst().preparation().transactionId()); assertFalse(last.more());
            assertTrue(f.right.f.store.preparedRecoveries(other,1).records().isEmpty()); assertEquals(1,f.left.f.store.preparedRecoveries(null,1).records().size());
            assertThrows(IllegalArgumentException.class,() -> f.right.f.store.preparedRecoveries(null,17));
            var vote=first.records().getFirst(); assertThrows(IllegalArgumentException.class,() -> new ReplicatedRangeStore.PreparedRecoveryPage(new ApplyPosition(0,vote.position().index()),List.of(vote),false));
            try (var registry=registries.registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                long before=f.writes(); assertTrue(registry.register(f.right.f.groupId,vote)); assertEquals(1,registries.allocations.get()); assertEquals(before,f.writes());
                for (int n=0;n<4;n++) registry.cycle(1);
                var changed=new PreparedParticipant(vote.preparation(),new ApplyPosition(vote.position().term(),vote.position().index()+1));
                assertThrows(IOException.class,() -> registry.register(f.right.f.groupId,changed));
                assertFalse(registry.register(f.right.f.groupId,vote));
            }
        }
    }
}
