package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native retained outcomes and controlled file/RPC ownership; no servers or network. */
class RatisTransactionJournalReclamationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisTransactionRecoveryTest fixtures=new RatisTransactionRecoveryTest();
    RatisTransactionRecoveryTest.Fixture fixture(String name) throws Exception { fixtures.root=root; return fixtures.new Fixture(name); }
    long writes(RatisTransactionRecoveryTest.Fixture f) { return f.home.wire.index+f.data.wire.index; }
    TransactionStatus completed(RatisTransactionRecoveryTest.Fixture f,boolean commit) throws Exception {
        if (commit) f.commit(false); else { f.journal.executeHome(); f.pin.close(); }
        try (var recovery=f.recovery()) { fixtures.drain(f,recovery); }
        assertTrue(f.journal.snapshot().observedHome().fullyFinalized()); return f.journal.snapshot().observedHome();
    }
    TransactionStatus consume(RatisTransactionRecoveryTest.Fixture f,TransactionStatus acknowledged) throws Exception {
        var consumed=acknowledged.consume();
        f.journal.queueRecovery(consumed);
        assertEquals(CommandResult.Status.SUCCESS,f.journal.executeHome().status()); return consumed;
    }
    void retire(RatisTransactionRecoveryTest.Fixture f,TransactionStatus consumed) throws Exception {
        var progress=f.journal.snapshot().participant(f.data.f.groupId); var outcome=progress.outcome();
        f.pin.close(); f.data.wire.command(f.plan.intent().route(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
        if (outcome!=null) f.data.routed.retireParticipant(new ParticipantRetirement(outcome.finalization(),outcome.position(),consumed));
        var proof=f.data.routed.participantFence(new ParticipantIdentity(f.home.f.groupId,1,f.data.f.groupId,f.plan.initial())); assertNotNull(proof);
        var route=f.plan.registration().descriptor(); f.home.wire.command(route,DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(f.home.f.admin,2,0),2));
        f.home.statuses.retire(new HomeRetirement(f.home.statuses.identity(f.plan.initial(),route),consumed,List.of(proof)));
        assertThrows(RatisHomeRetiredException.class,f.journal::refreshHome); assertNotNull(f.journal.snapshot().homeRetirementProof());
    }
    void ready(RatisTransactionRecoveryTest.Fixture f) throws Exception { retire(f,consume(f,completed(f,true))); assertTrue(f.journal.reclaimable()); }
    void missing(RatisTransactionRecoveryTest.Fixture f) {
        assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(f.path,f.home.statuses,f.data.routed));
    }

    @Test void explicitReclamationDiscardsConsumedSavedResultsAndRetainsTheStableLock() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var f=fixture("reclaim-"+commit)) {
            assertFalse(f.journal.reclaimable()); assertThrows(IOException.class,f.journal::reclaimRetired);
            var acknowledged=completed(f,commit); byte[] result=f.journal.result().encode();
            assertFalse(f.journal.reclaimable()); assertThrows(IOException.class,f.journal::reclaimRetired);
            var consumed=consume(f,acknowledged); assertFalse(f.journal.reclaimable()); assertThrows(IOException.class,f.journal::reclaimRetired);
            retire(f,consumed); f.reopen(); assertTrue(f.journal.reclaimable()); assertArrayEquals(result,f.journal.result().encode());
            Path lock=f.path.resolveSibling(f.path.getFileName()+".lock"),pending=f.path.resolveSibling(f.path.getFileName()+".pending");
            Object inode=Files.readAttributes(lock,BasicFileAttributes.class).fileKey(); Files.write(pending,new byte[]{1,2,3});
            long before=writes(f); f.journal.reclaimRetired(); assertFalse(Files.exists(f.path)); assertFalse(Files.exists(pending)); assertTrue(Files.exists(lock));
            assertEquals(inode,Files.readAttributes(lock,BasicFileAttributes.class).fileKey()); missing(f);
            assertThrows(IOException.class,f.journal::result); assertThrows(IOException.class,f.journal::snapshot); assertThrows(IOException.class,f.journal::executeHome);
            f.journal.close(); missing(f); assertEquals(before,writes(f));
        }
    }
    @Test void HomeRetirementCannotInventLocalConsumptionOrDiscardCachedOutcome() throws Exception {
        var helper=new RatisHomeJournalRetirementTest(); helper.root=root;
        try (var f=fixture("foreign-consumption")) {
            var acknowledged=completed(f,true); byte[] result=f.journal.result().encode();
            helper.consumeAndRetire(f,f.journal.snapshot().participant(f.data.f.groupId).outcome(),acknowledged);
            assertThrows(RatisHomeRetiredException.class,f.journal::refreshHome); assertFalse(f.journal.snapshot().observedHome().consumed());
            assertFalse(f.journal.reclaimable()); assertThrows(IOException.class,f.journal::reclaimRetired); assertArrayEquals(result,f.journal.result().encode());
            f.reopen(); assertFalse(f.journal.reclaimable()); assertArrayEquals(result,f.journal.result().encode()); assertTrue(Files.exists(f.path));
        }
        try (var f=fixture("never-registered")) {
            f.pin.close(); var route=f.plan.registration().descriptor(); f.home.wire.command(route,DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(f.home.f.admin,2,0),2));
            assertThrows(RatisHomeRetiredException.class,f.journal::refreshHome); assertFalse(f.journal.reclaimable()); assertThrows(IOException.class,f.journal::reclaimRetired); assertTrue(Files.exists(f.path));
        }
    }
    @Test void failedDeletionFencesTheOwnerAndReopenCanRetryTheExactQualifiedPayload() throws Exception {
        try (var f=fixture("delete-failure")) {
            ready(f); byte[] body=SessionJournalFiles.read(f.path,RatisDurableTransaction.MAX_BYTES); long before=writes(f);
            assertThrows(IOException.class,() -> f.journal.reclaimRetired(path -> { throw new IOException("Controlled unlink failure"); }));
            assertArrayEquals(body,SessionJournalFiles.read(f.path,RatisDurableTransaction.MAX_BYTES)); assertThrows(IOException.class,f.journal::snapshot);
            missing(f); f.reopen(); assertTrue(f.journal.reclaimable()); f.journal.reclaimRetired(); assertFalse(Files.exists(f.path)); assertEquals(before,writes(f));
        }
    }
    @Test void directoryForceFailureAfterUnlinkRetainsOwnershipAndCannotRecreateRequests() throws Exception {
        try (var f=fixture("force-failure")) {
            ready(f); long before=writes(f);
            var io=new SessionJournalFiles.Removal() {
                public void delete(Path path) throws IOException { Files.delete(path); }
                public void force(Path directory) throws IOException { throw new IOException("Controlled directory force failure"); }
            };
            assertThrows(IOException.class,() -> f.journal.reclaimRetired(io)); assertFalse(Files.exists(f.path));
            assertThrows(IOException.class,f.journal::snapshot); assertThrows(IOException.class,f.journal::executeHome); missing(f);
            f.journal.close(); missing(f); assertEquals(before,writes(f));
        }
    }
    @Test void changedOrCorruptOwnedFileIsNeverDeleted() throws Exception {
        for (boolean checksum:new boolean[]{false,true}) try (var f=fixture("changed-"+checksum)) {
            ready(f); byte[] exact=SessionJournalFiles.read(f.path,RatisDurableTransaction.MAX_BYTES);
            if (checksum) { byte[] encoded=Files.readAllBytes(f.path); encoded[encoded.length-1]^=1; Files.write(f.path,encoded); }
            else { byte[] changed=exact.clone(); changed[0]^=1; SessionJournalFiles.write(f.path,changed,RatisDurableTransaction.MAX_BYTES); }
            assertThrows(IOException.class,f.journal::reclaimRetired); assertTrue(Files.exists(f.path)); assertThrows(IOException.class,f.journal::result);
            f.journal.close(); missing(f); SessionJournalFiles.write(f.path,exact,RatisDurableTransaction.MAX_BYTES); f.reopen(); f.journal.reclaimRetired(); assertFalse(Files.exists(f.path));
        }
    }
    @Test void cancelledReclaimerRetainsOwnershipThroughUnlinkAndDirectoryForce() throws Exception {
        try (var f=fixture("active-reclaim"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            ready(f); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var returned=new CountDownLatch(1); var failure=new java.util.concurrent.atomic.AtomicReference<Throwable>();
            var io=new SessionJournalFiles.Removal() {
                public void delete(Path path) throws IOException { Files.delete(path); }
                public void force(Path directory) throws IOException {
                    entered.countDown(); for (;;) try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Force wait timed out"); break; } catch (InterruptedException cancelled) { /* Actual filesystem work still owns the journal. */ }
                    SessionJournalFiles.force(directory);
                }
            };
            var task=executor.submit(() -> { try { f.journal.reclaimRetired(io); } catch (Throwable problem) { failure.set(problem); } finally { returned.countDown(); } });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(task.cancel(true)); assertFalse(f.journal.reclaimable());
                assertThrows(IOException.class,f.journal::close); assertThrows(IOException.class,f.journal::reclaimRetired); missing(f);
            } finally { release.countDown(); }
            assertTrue(returned.await(5,TimeUnit.SECONDS)); assertNull(failure.get()); assertFalse(Files.exists(f.path)); f.journal.close(); missing(f);
        }
    }
}
