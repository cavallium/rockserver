package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Durable registry ownership/discovery with native groups and controlled dispatch; no server or network fixture. */
class RatisParticipantRecoveryRegistryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCoordinatorTest helper=new RatisSnapshotCoordinatorTest();
    RatisSnapshotCoordinatorTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name); }
    RatisParticipantRecoveryRegistry registry(Path path,RatisSnapshotCoordinatorTest.Fixture f,boolean takeover,RatisParticipantRecoveryRegistry.Limits limits) throws Exception {
        return new RatisParticipantRecoveryRegistry(path,f.transactions.statuses,f.data,takeover,Set.of(f.left.f.groupId,f.right.f.groupId),limits);
    }
    @Test void helperCreatesDurableWorkerAndRestartRetainsCompletedProofWithoutPolling() throws Exception {
        Path path=Files.createDirectory(root.resolve("completed"));
        try (var f=fixture("completed")) {
            f.committedTransactionWithRightStillPrepared(); var prepared=f.data.preparedParticipant(f.right.f.groupId,helper.tx); long bytes;
            try (var registry=registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                long before=f.writes(); assertTrue(registry.step(f.right.f.groupId,prepared)); assertEquals(1,f.writes()-before);
                assertEquals(1,registry.stats().openWorkers()); assertEquals(1,registry.stats().persistedFiles());
                assertEquals(1,registry.cycle(1)); assertEquals(0,registry.stats().openWorkers()); bytes=registry.stats().reservedBytes(); assertTrue(bytes>0 && bytes<RatisDurableParticipantRecovery.MAX_BYTES);
                assertFalse(f.transactions.statuses.read(helper.tx).fullyFinalized());
            }
            try (var registry=registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                registry.discover(); long before=f.writes(); for (int n=0;n<4;n++) registry.cycle(1);
                assertEquals(0,registry.stats().openWorkers()); assertEquals(1,registry.stats().persistedFiles()); assertEquals(bytes,registry.stats().reservedBytes()); assertEquals(before,f.writes());
                assertArrayEquals(new byte[]{8},f.data.read(helper.column,new byte[]{(byte)130}));
            }
        }
    }
    @Test void restartDiscoversPendingAbortAndFinishesLocalFinalizeAndAck() throws Exception {
        Path path=Files.createDirectory(root.resolve("pending"));
        try (var f=fixture("pending")) {
            f.committedTransactionWithRightStillPrepared(); UUID tx=f.anotherPreparedRight(); var prepared=f.data.preparedParticipant(f.right.f.groupId,tx);
            try (var registry=registry(path,f,true,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                assertTrue(registry.step(f.right.f.groupId,prepared)); assertEquals(TransactionStatus.Phase.ABORT,f.transactions.statuses.read(tx).phase());
            }
            try (var registry=registry(path,f,true,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) {
                registry.discover(); for (int n=0;n<8;n++) { long before=f.writes(); registry.cycle(1); assertTrue(f.writes()-before<=1); }
                assertEquals(0,registry.stats().openWorkers()); var home=f.transactions.statuses.read(tx); assertTrue(home.fullyFinalized()); assertFalse(home.consumed()); assertNull(f.data.read(helper.column,new byte[]{(byte)131}));
            }
        }
    }
    @Test void discoveryRotatesMorePendingJournalsThanTheOpenWorkerLimit() throws Exception {
        Path path=Files.createDirectory(root.resolve("rotation"));
        try (var f=fixture("rotation")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight(); var limits=new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024);
            try (var registry=registry(path,f,true,limits)) {
                registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,helper.tx)); registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,other));
                assertEquals(1,registry.stats().openWorkers()); assertEquals(2,registry.stats().persistedFiles());
            }
            try (var registry=registry(path,f,true,limits)) {
                registry.discover();
                for (int n=0;n<12;n++) { long before=f.writes(); registry.cycle(1); assertTrue(f.writes()-before<=1); assertTrue(registry.stats().openWorkers()<=1); }
                assertEquals(0,registry.stats().openWorkers()); assertEquals(2,registry.stats().persistedFiles());
                var commit=f.transactions.statuses.read(helper.tx); assertTrue(commit.participants().stream().filter(p -> p.groupId().equals(f.right.f.groupId)).findFirst().orElseThrow().finalized()); assertFalse(commit.fullyFinalized());
                var abort=f.transactions.statuses.read(other); assertEquals(TransactionStatus.Phase.ABORT,abort.phase()); assertTrue(abort.fullyFinalized()); assertFalse(abort.consumed());
            }
        }
    }
    @Test void capacityRejectsNewEffectsAndForeignGroupsBeforeCreatingFiles() throws Exception {
        Path path=Files.createDirectory(root.resolve("capacity"));
        try (var f=fixture("capacity")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight();
            try (var registry=registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,1,RatisDurableParticipantRecovery.MAX_BYTES))) {
                registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,helper.tx)); long before=f.writes();
                assertThrows(IOException.class,() -> registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,other))); assertEquals(before,f.writes()); assertEquals(1,registry.stats().persistedFiles());
                assertThrows(IllegalArgumentException.class,() -> registry.step(UUID.randomUUID(),f.data.preparedParticipant(f.right.f.groupId,other)));
            }
        }
    }
    @Test void registryOwnsTheDirectoryAndRetainsItThroughCancelledRunningViews() throws Exception {
        Path path=Files.createDirectory(root.resolve("ownership"));
        try (var f=fixture("ownership"); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            f.committedTransactionWithRightStillPrepared(); var prepared=f.data.preparedParticipant(f.right.f.groupId,helper.tx);
            var limits=new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024);
            try (var registry=registry(path,f,false,limits)) {
                assertThrows(IOException.class,() -> registry(path,f,false,limits));
                var entered=new java.util.concurrent.CountDownLatch(1); var release=new java.util.concurrent.CountDownLatch(1); var done=new java.util.concurrent.CountDownLatch(1);
                f.transactions.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
                var view=executor.submit(() -> { try { return registry.step(f.right.f.groupId,prepared); } finally { done.countDown(); } });
                try { assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,registry::close); assertThrows(IOException.class,() -> registry(path,f,false,limits)); }
                finally { release.countDown(); }
                assertTrue(done.await(5,java.util.concurrent.TimeUnit.SECONDS)); f.transactions.wire.afterRead=null;
                assertThrows(IOException.class,() -> registry.cycle(1));
            }
            try (var registry=registry(path,f,false,limits)) { registry.discover(); for (int n=0;n<4;n++) registry.cycle(1); assertEquals(0,registry.stats().openWorkers()); }
        }
    }
    @Test void unpublishedAdmissionFailureDoesNotConsumeThePersistentFileBudget() throws Exception {
        Path path=Files.createDirectory(root.resolve("unpublished"));
        try (var f=fixture("unpublished")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight();
            try (var registry=new RatisParticipantRecoveryRegistry(path,f.transactions.statuses,f.data,false,Set.of(f.right.f.groupId),new RatisParticipantRecoveryRegistry.Limits(1,1,RatisDurableParticipantRecovery.MAX_BYTES))) {
                f.transactions.wire.readFailure=new IOException("Qualified HOME read unavailable");
                long before=f.writes(); assertThrows(IOException.class,() -> registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,helper.tx)));
                f.transactions.wire.readFailure=null;
                assertEquals(0,registry.stats().persistedFiles()); assertEquals(0,registry.stats().openWorkers()); assertEquals(0,registry.stats().reservedBytes());
                assertFalse(registry.step(f.right.f.groupId,f.data.preparedParticipant(f.right.f.groupId,other))); assertEquals(1,registry.stats().persistedFiles()); assertEquals(before,f.writes());
            }
        }
    }
    @Test void repeatedUnpublishedNativeProofFailuresDoNotAccumulateLockSidecars() throws Exception {
        Path path=Files.createDirectory(root.resolve("failed-sidecars"));
        try (var f=fixture("failed-sidecars")) {
            f.committedTransactionWithRightStillPrepared(); var nativePrepared=f.data.preparedParticipant(f.right.f.groupId,helper.tx); var original=nativePrepared.preparation(); var before=original.registration();
            var limits=new RatisParticipantRecoveryRegistry.Limits(1,1,RatisDurableParticipantRecovery.MAX_BYTES);
            try (var registry=registry(path,f,false,limits)) {
                for (int n=0;n<24;n++) {
                    var definition=TransactionStatus.begin(before.clusterId(),before.databaseId(),before.domainId(),new UUID(99,n),before.operation(),before.schemaEpoch(),before.schemaDigest(),before.planDigest(),before.participants());
                    var forged=new PreparedParticipant(new PreparedParticipant.Preparation(original.homeGroup(),original.homeEpoch(),original.operation(),definition,original.business()),nativePrepared.position());
                    assertThrows(IOException.class,() -> registry.step(f.right.f.groupId,forged)); assertEquals(0,registry.stats().persistedFiles()); assertEquals(0,registry.stats().reservedBytes());
                    try (var files=Files.list(path)) { assertEquals(1,files.count(),"Only the owned registry lock remains"); }
                }
            }
            try (var registry=registry(path,f,false,limits)) { assertEquals(0,registry.discover()); assertEquals(0,registry.cycle(1)); }
        }
    }
    @Test void malformedOrUnplannedPersistedNamesAndOversizedFilesFailDiscovery() throws Exception {
        try (var f=fixture("discovery")) {
            for (String name:List.of("wrong.bin",UUID.randomUUID()+"-"+UUID.randomUUID()+".bin",f.right.f.groupId+"-"+helper.tx+".bin.pending")) {
                Path path=Files.createDirectory(root.resolve("invalid-"+name)); Files.write(path.resolve(name),new byte[1]);
                try (var registry=registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) { assertThrows(IOException.class,registry::discover); }
            }
            Path path=Files.createDirectory(root.resolve("oversize"));
            try (var file=java.nio.channels.FileChannel.open(path.resolve(f.right.f.groupId+"-"+helper.tx+".bin"),StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) { file.position(RatisDurableParticipantRecovery.MAX_BYTES); file.write(java.nio.ByteBuffer.wrap(new byte[1])); }
            try (var registry=registry(path,f,false,new RatisParticipantRecoveryRegistry.Limits(1,4,64L*1024*1024))) { assertThrows(IOException.class,registry::discover); }
        }
    }
}
