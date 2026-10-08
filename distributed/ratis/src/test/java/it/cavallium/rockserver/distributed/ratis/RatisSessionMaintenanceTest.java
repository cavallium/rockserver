package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

/** Durable journal + real native adapter composition; no transport/process/power-loss acceptance claim. */
class RatisSessionMaintenanceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void repeatedClockPropagationAndReopenKeepOneReceiptAndRotateOnlyAtCoarseFloor() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            Path journal=root.resolve("maintenance.bin"); UUID session; long sequence;
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                var binding=owner.prepareBinding(f.ledger.grants().getFirst(),2); session=binding.operationId().sessionId();
                assertEquals(1,binding.operationId().epoch());
                assertThrows(IllegalStateException.class,owner::prepareAdvance);
                assertThrows(IOException.class,() -> new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup));
                assertEquals(CommandResult.Status.SUCCESS,owner.execute().status());
                for (int time=20;time<32;time++) {
                    f.expire(time); var command=owner.prepareAdvance();
                    assertEquals(session,command.operationId().sessionId()); assertEquals(1,command.operationId().epoch());
                    assertEquals(command.operationId().sequence()-1,command.acknowledgedThrough());
                    assertEquals(CommandResult.Status.SUCCESS,owner.execute().status());
                    assertEquals(1,f.store.receiptUsage().retainedCount());
                }
                assertEquals(SessionAuthority.from(f.ledger),owner.propagated()); sequence=owner.prepareAdvance().operationId().sequence();
            }
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                assertEquals(session,owner.pendingCommand().operationId().sessionId());
                assertEquals(sequence,owner.pendingCommand().operationId().sequence());
                // Unrelated catalogue change cannot strand an immutable clock prepared before it.
                f.expire(50); assertEquals(CommandResult.Status.SUCCESS,owner.execute().status());
                assertEquals(31,owner.propagated().logicalTimeMillis(),"Completion proves the frozen cut, not the latest catalogue state");
                owner.prepareAdvance(); owner.execute(); assertEquals(SessionAuthority.from(f.ledger),owner.propagated());
                f.expire(100); var rotated=owner.prepareAdvance();
                assertEquals(session,rotated.operationId().sessionId()); assertEquals(2,rotated.operationId().epoch());
                assertEquals(0,rotated.operationId().sequence()); assertEquals(-1,rotated.acknowledgedThrough());
                owner.execute(); assertEquals(1,f.store.receiptUsage().retainedCount());
            }
        }
    }
    @Test void lostReplyReopenRetriesOriginalPayloadAndPersistsCompletionBeforeAllocatingNext() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            var lose=new AtomicBoolean(true); var sent=new ArrayList<byte[]>(); var routes=new ArrayList<RangeDescriptor>();
            try (var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    sent.add(DistributedCommandCodec.encode(command)); routes.add(descriptor);
                    var result=f.routed.execute(f.routed.prepareSessionControl(command));
                    if (lose.getAndSet(false)) throw new IOException("reply lost after native apply"); return result;
                }
            })) {
                Path journal=root.resolve("lost.bin"); byte[] original;
                try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup)) {
                    original=DistributedCommandCodec.encode(owner.prepareAdvance());
                    assertThrows(IOException.class,owner::execute); assertNull(owner.propagated());
                    assertThrows(IllegalStateException.class,owner::prepareAdvance);
                }
                var previous=f.directory.ranges().getFirst(); var replica=previous.replicas().getFirst();
                var changedReplica=new ReplicaDescriptor(replica.nodeId(),replica.nodeIncarnation(),replica.replicaId(),replica.peerId(),"127.0.0.1:2",replica.role());
                var changed=new RangeDescriptor(previous.rangeId(),previous.groupId(),previous.rangeEpoch(),previous.configurationVersion()+1,
                        previous.lowerInclusive(),previous.upperExclusive(),previous.topologyState(),List.of(changedReplica));
                routed.refresh(new RangeDirectory(f.directory.clusterId(),f.directory.databaseId(),f.directory.domainId(),f.directory.schemaEpoch(),
                        f.directory.schemaDigest(),f.directory.generation()+1,f.directory.columns(),f.directory.routingPrefixBytes(),List.of(changed)));
                try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup)) {
                    assertArrayEquals(original,DistributedCommandCodec.encode(owner.pendingCommand()));
                    assertEquals(CommandResult.Status.SUCCESS,owner.execute().status());
                    assertEquals(SessionAuthority.from(f.ledger),owner.propagated()); assertNull(owner.pendingCommand());
                }
                try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup)) {
                    assertNull(owner.pendingCommand()); assertNotNull(owner.propagated());
                    var next=owner.prepareAdvance(); assertEquals(1,next.operationId().sequence()); assertEquals(0,next.acknowledgedThrough());
                    owner.execute(); assertEquals(1,f.store.receiptUsage().retainedCount());
                }
                assertArrayEquals(sent.get(0),sent.get(1));
                assertEquals(routes.get(0),routes.get(1),"Journal recovery retains the old peer route after refresh");
                assertNotEquals(routes.get(1).replicas(),routes.get(2).replicas());
            }
        }
    }
    @Test void failedCompletionWriteFencesOwnerAndRecoveredPendingResolvesNativeResult() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            Path journal=root.resolve("failure.bin"); byte[] pending;
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                owner.prepareAdvance(); pending=Files.readAllBytes(journal);
                Files.delete(journal); Files.createDirectory(journal); Files.writeString(journal.resolve("block"),"fixture");
                assertThrows(IOException.class,owner::execute);
                assertEquals(1,f.store.receiptUsage().retainedCount(),"Native apply preceded failed completion persistence");
                assertThrows(IOException.class,owner::prepareAdvance);
            }
            Files.delete(journal.resolve("block")); Files.delete(journal); Files.write(journal,pending);
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                assertEquals(CommandResult.Status.SUCCESS,owner.execute().status()); assertNotNull(owner.propagated());
                assertEquals(1,f.store.receiptUsage().retainedCount());
            }
        }
    }
    @Test void activeExecutionKeepsJournalOwnershipUntilItsActualCompletion() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            var started=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var routed=new RatisRoutedClient(f.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    started.countDown();
                    try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                    return f.routed.execute(f.routed.prepareSessionControl(command));
                }
            }); var executor=Executors.newSingleThreadExecutor()) {
                Path journal=root.resolve("active.bin");
                try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup)) {
                    owner.prepareAdvance(); var executing=executor.submit(owner::execute);
                    try {
                        assertTrue(started.await(5,TimeUnit.SECONDS));
                        assertThrows(IOException.class,owner::close);
                        assertThrows(IOException.class,() -> new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup));
                        assertThrows(IllegalStateException.class,owner::prepareAdvance);
                        assertThrows(IllegalStateException.class,owner::execute);
                    } finally { release.countDown(); }
                    assertEquals(CommandResult.Status.SUCCESS,executing.get(5,TimeUnit.SECONDS).status());
                }
                try (var recovered=new RatisSessionMaintenance(journal,f.catalogue.epochs,routed,f.outer.dataGroup)) { assertNotNull(recovered.propagated()); }
            }
        }
    }
    @Test void separateClockDriverFencesAnExpiredFrozenBindingWithoutDroppingItsIdentity() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root);
                var binding=new RatisSessionMaintenance(root.resolve("binding.bin"),f.catalogue.epochs,f.routed,f.outer.dataGroup);
                var clock=new RatisSessionMaintenance(root.resolve("clock.bin"),f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
            var original=binding.prepareBinding(f.ledger.grants().getFirst(),2); byte[] bytes=DistributedCommandCodec.encode(original);
            f.expire(100);
            assertThrows(IOException.class,binding::execute); assertNotNull(binding.pendingCommand());
            assertThrows(IllegalStateException.class,binding::prepareAdvance);
            clock.prepareAdvance(); assertEquals(CommandResult.Status.SUCCESS,clock.execute().status());
            assertArrayEquals(bytes,DistributedCommandCodec.encode(binding.pendingCommand()));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,binding.execute().status());
            assertNull(binding.pendingCommand()); assertNull(binding.propagated(),"Expired result is not propagation completion");
            var fresh=binding.prepareAdvance(); assertEquals(2,fresh.operationId().epoch());
            assertEquals(CommandResult.Status.SUCCESS,binding.execute().status()); assertNotNull(binding.propagated());
        }
    }
    @Test void lostLockChannelMakesOldOwnerUnusableBeforeAnotherOwnerRecovers() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            Path journal=root.resolve("lock-loss.bin");
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                owner.prepareAdvance();
                var field=RatisSessionMaintenance.class.getDeclaredField("lockChannel"); field.setAccessible(true);
                ((java.nio.channels.FileChannel)field.get(owner)).close();
                owner.close();
                assertThrows(IOException.class,owner::execute); assertThrows(IOException.class,owner::prepareAdvance);
                try (var next=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) {
                    assertEquals(CommandResult.Status.SUCCESS,next.execute().status());
                    assertThrows(IOException.class,owner::execute);
                }
            }
        }
    }
    @Test void corruptedAndForeignJournalsFailWithoutReplacingTheirState() throws Exception {
        try (var f=new RatisSessionBindingTest.Fixture(root)) {
            Path journal=root.resolve("corrupt.bin");
            try (var owner=new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup)) { owner.prepareAdvance(); }
            byte[] original=Files.readAllBytes(journal);
            assertThrows(IOException.class,() -> new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,UUID.randomUUID()));
            assertArrayEquals(original,Files.readAllBytes(journal));
            byte[] corrupt=original.clone(); corrupt[corrupt.length/2]^=1; Files.write(journal,corrupt);
            assertThrows(IOException.class,() -> new RatisSessionMaintenance(journal,f.catalogue.epochs,f.routed,f.outer.dataGroup));
            assertArrayEquals(corrupt,Files.readAllBytes(journal));
        }
    }
}
