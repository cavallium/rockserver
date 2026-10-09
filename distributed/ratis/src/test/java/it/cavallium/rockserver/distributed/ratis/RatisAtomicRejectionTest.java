package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.OperationId;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Local durable rejection ownership; no accepted plan, transaction HOME, native pin or RPC is created. */
class RatisAtomicRejectionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final byte[] binding=new byte[32],digest=new byte[32];
    RatisAtomicClient.Identity identity() {
        var session=new UUID(0,1); return new RatisAtomicClient.Identity(new UUID(0,2),new OperationId(session,1,0),new OperationId(session,1,1));
    }
    RatisAtomicRejection create(Path path) throws IOException { return RatisAtomicRejection.create(path,binding,identity(),digest); }
    RatisAtomicRejection reopen(Path path) throws IOException { return RatisAtomicRejection.reopen(path,binding); }

    @Test void currentCanonicalRecordOwnsValuesAndConsumptionSurvivesReopen() throws Exception {
        var path=root.resolve("rejection.bin"); byte[] suppliedBinding=binding.clone(),suppliedDigest=digest.clone(); suppliedDigest[0]=7;
        try (var record=RatisAtomicRejection.create(path,suppliedBinding,identity(),suppliedDigest)) {
            suppliedBinding[0]=9; suppliedDigest[0]=9; assertEquals(identity(),record.identity()); assertEquals(7,record.requestDigest()[0]); record.requestDigest()[0]=10; assertEquals(7,record.requestDigest()[0]);
            assertFalse(record.consumed()); assertEquals(149,RatisAtomicRejection.BODY_BYTES); assertEquals(181,Files.size(path)); assertTrue(RatisAtomicRejection.isRejection(path));
            byte[] body=SessionJournalFiles.read(path,RatisAtomicRejection.MAX_BYTES); assertEquals(149,body.length); assertEquals(0,body[body.length-1]);
            record.consume(); assertTrue(record.consumed()); byte[] consumed=Files.readAllBytes(path);
            record.consume((file,bytes,maximum) -> { throw new AssertionError("Repeated consumption must not write"); }); assertArrayEquals(consumed,Files.readAllBytes(path));
        }
        try (var record=reopen(path)) { assertEquals(identity(),record.identity()); assertEquals(7,record.requestDigest()[0]); assertTrue(record.consumed()); }
    }

    @Test void exclusiveCreationAndSharedStablePathLockCannotOverwriteAnotherOutcome() throws Exception {
        var path=root.resolve("owned.bin");
        try (var record=create(path)) {
            byte[] original=Files.readAllBytes(path); assertThrows(IOException.class,() -> create(path)); assertThrows(IOException.class,() -> reopen(path)); assertArrayEquals(original,Files.readAllBytes(path));
            try (var channel=FileChannel.open(path.resolveSibling(path.getFileName()+".lock"),StandardOpenOption.WRITE)) { assertThrows(OverlappingFileLockException.class,channel::tryLock); }
        }
        byte[] original=Files.readAllBytes(path); assertThrows(IOException.class,() -> create(path)); assertArrayEquals(original,Files.readAllBytes(path));
        var accepted=root.resolve("accepted.bin"); var lockPath=accepted.resolveSibling(accepted.getFileName()+".lock");
        try (var channel=FileChannel.open(lockPath,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE); var lock=channel.tryLock()) {
            assertNotNull(lock); assertThrows(IOException.class,() -> create(accepted)); assertFalse(Files.exists(accepted));
        }
        byte[] body=ByteBuffer.allocate(RatisAtomicRejection.BODY_BYTES).putInt(0x54584a4c).array(); SessionJournalFiles.write(accepted,body,RatisAtomicRejection.MAX_BYTES);
        byte[] acceptedBytes=Files.readAllBytes(accepted); assertFalse(RatisAtomicRejection.isRejection(accepted)); assertThrows(IOException.class,() -> create(accepted)); assertThrows(IOException.class,() -> reopen(accepted)); assertArrayEquals(acceptedBytes,Files.readAllBytes(accepted));
    }

    @Test void boundedProbeAndReopenRejectMissingCorruptOrNoncanonicalRejectionRecords() throws Exception {
        assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(root.resolve("missing"))); assertThrows(IOException.class,() -> reopen(root.resolve("missing")));
        var path=root.resolve("checksum.bin"); try (var record=create(path)) {}
        byte[] original=Files.readAllBytes(path); byte[] broken=original.clone(); broken[40]^=1; Files.write(path,broken);
        assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(path)); assertThrows(IOException.class,() -> reopen(path)); assertArrayEquals(broken,Files.readAllBytes(path));
        Files.write(path,original); byte[] wrongBinding=binding.clone(); wrongBinding[0]=1; assertThrows(IOException.class,() -> RatisAtomicRejection.reopen(path,wrongBinding)); assertTrue(RatisAtomicRejection.isRejection(path));
        byte[] body=SessionJournalFiles.read(path,RatisAtomicRejection.MAX_BYTES); body[body.length-1]=2; SessionJournalFiles.write(path,body,RatisAtomicRejection.MAX_BYTES);
        assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(path)); assertThrows(IOException.class,() -> reopen(path));
        Files.write(path,original); body=SessionJournalFiles.read(path,RatisAtomicRejection.MAX_BYTES); ByteBuffer.wrap(body).putLong(108,0); SessionJournalFiles.write(path,body,RatisAtomicRejection.MAX_BYTES);
        assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(path)); assertThrows(IOException.class,() -> reopen(path));
        Files.write(path,Arrays.copyOf(original,original.length-1)); assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(path)); assertThrows(IOException.class,() -> reopen(path));
        Files.write(path,original); body=SessionJournalFiles.read(path,RatisAtomicRejection.MAX_BYTES); ByteBuffer.wrap(body).putInt(0,0x01020304); SessionJournalFiles.write(path,body,RatisAtomicRejection.MAX_BYTES);
        assertThrows(IOException.class,() -> RatisAtomicRejection.isRejection(path)); assertThrows(IOException.class,() -> reopen(path));
        var transaction=root.resolve("large-transaction.bin");
        try (var file=FileChannel.open(transaction,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) { file.write(ByteBuffer.allocate(4).putInt(0x54584a4c).flip()); file.position(RatisDurableTransaction.MAX_BYTES-1L).write(ByteBuffer.wrap(new byte[]{0})); }
        assertFalse(RatisAtomicRejection.isRejection(transaction)); assertThrows(IOException.class,() -> reopen(transaction));
    }

    @Test void uncertainPublicationIsPreservedForIdentityComparisonOnReopen() throws Exception {
        var path=root.resolve("uncertain-create.bin");
        assertThrows(IOException.class,() -> RatisAtomicRejection.create(path,binding,identity(),digest,(file,body,maximum) -> {
            SessionJournalFiles.write(file,body,maximum); throw new IOException("Lost persistence completion");
        }));
        assertTrue(Files.exists(path)); try (var record=reopen(path)) { assertEquals(identity(),record.identity()); assertArrayEquals(digest,record.requestDigest()); assertFalse(record.consumed()); }
        try (var record=reopen(path)) {
            assertThrows(IOException.class,() -> record.consume((file,body,maximum) -> { SessionJournalFiles.write(file,body,maximum); throw new IOException("Lost consumption completion"); }));
            assertThrows(IOException.class,record::consumed); assertThrows(IOException.class,record::consume); assertThrows(IOException.class,() -> reopen(path));
            assertEquals(identity(),record.identity()); assertArrayEquals(digest,record.requestDigest());
        }
        try (var record=reopen(path)) { assertTrue(record.consumed()); }
    }

    @Test void activeConsumptionIoRetainsTheLockUntilActualReturnEvenAfterCancellation() throws Exception {
        var path=root.resolve("blocked-consumption.bin"); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var returned=new CountDownLatch(1);
        try (var record=create(path); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var consuming=executor.submit(() -> {
                try { record.consume((file,body,maximum) -> {
                    entered.countDown(); for (;;) try { release.await(); break; } catch (InterruptedException cancelled) { /* Ownership lasts until file IO settles. */ }
                    SessionJournalFiles.write(file,body,maximum);
                }); return null; } finally { returned.countDown(); }
            });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(consuming.cancel(true)); assertFalse(record.consumed());
                assertThrows(IOException.class,record::close); assertThrows(IOException.class,record::consume); assertThrows(IOException.class,() -> reopen(path));
                assertEquals(identity(),record.identity());
            } finally { release.countDown(); }
            assertTrue(returned.await(5,TimeUnit.SECONDS)); assertTrue(record.consumed());
        }
        try (var record=reopen(path)) { assertTrue(record.consumed()); }
    }

    @Test void verifiedPublishedRecordRecoversPartialCrashSidecarBeforeConsumption() throws Exception {
        var path=root.resolve("crash-sidecar.bin"); try (var record=create(path)) {}
        byte[] published=Files.readAllBytes(path); var sidecar=path.resolveSibling(path.getFileName()+".pending"); Files.write(sidecar,new byte[]{1,2,3});
        try (var record=reopen(path)) {
            assertFalse(Files.exists(sidecar)); assertArrayEquals(published,Files.readAllBytes(path)); assertFalse(record.consumed());
            record.consume(); assertTrue(record.consumed());
        }
        try (var record=reopen(path)) { assertTrue(record.consumed()); }
        var live=root.resolve("live-sidecar.bin");
        try (var record=create(live)) {
            var pending=live.resolveSibling(live.getFileName()+".pending"); Files.write(pending,new byte[0]); record.consume(); assertTrue(record.consumed()); assertFalse(Files.exists(pending));
        }
    }

    @Test void sidecarRecoveryRequiresPublishedBindingAndPreservesInvalidSidecars() throws Exception {
        for (String kind:List.of("directory","oversize","link","binding","checksum")) {
            var path=root.resolve("invalid-sidecar-"+kind+".bin"); try (var record=create(path)) {}
            var sidecar=path.resolveSibling(path.getFileName()+".pending"); byte[] published=Files.readAllBytes(path);
            if (kind.equals("directory")) Files.createDirectory(sidecar);
            else if (kind.equals("oversize")) Files.write(sidecar,new byte[RatisAtomicRejection.MAX_BYTES+1]);
            else if (kind.equals("link")) { var target=root.resolve("sidecar-target"); Files.write(target,new byte[]{9}); Files.createSymbolicLink(sidecar,target); }
            else Files.write(sidecar,new byte[]{1});
            byte[] expected=published;
            if (kind.equals("checksum")) { expected=published.clone(); expected[40]^=1; Files.write(path,expected); }
            if (kind.equals("binding")) { byte[] wrong=binding.clone(); wrong[0]=1; assertThrows(IOException.class,() -> RatisAtomicRejection.reopen(path,wrong)); }
            else assertThrows(IOException.class,() -> reopen(path));
            assertTrue(Files.exists(sidecar,LinkOption.NOFOLLOW_LINKS)); assertArrayEquals(expected,Files.readAllBytes(path));
            if (kind.equals("link")) { assertTrue(Files.isSymbolicLink(sidecar)); assertArrayEquals(new byte[]{9},Files.readAllBytes(root.resolve("sidecar-target"))); }
        }
    }

    @Test void failedWritesAndInvalidLockFilesCannotOverwriteOrDiscardUncertainState() throws Exception {
        var path=root.resolve("sidecar.bin"); var sidecar=path.resolveSibling(path.getFileName()+".pending"); Files.write(sidecar,new byte[]{1,2,3});
        assertThrows(IOException.class,() -> create(path)); assertFalse(Files.exists(path)); assertArrayEquals(new byte[]{1,2,3},Files.readAllBytes(sidecar)); Files.delete(sidecar);
        try (var record=create(path)) {
            byte[] original=Files.readAllBytes(path); Files.createDirectory(sidecar); assertThrows(IOException.class,record::consume); assertArrayEquals(original,Files.readAllBytes(path)); assertTrue(Files.isDirectory(sidecar));
        }
        Files.delete(sidecar); try (var record=reopen(path)) { assertFalse(record.consumed()); }
        var invalid=root.resolve("invalid-lock.bin"); Files.write(invalid.resolveSibling(invalid.getFileName()+".lock"),new byte[]{1}); assertThrows(IOException.class,() -> create(invalid)); assertFalse(Files.exists(invalid));
        assertThrows(IllegalArgumentException.class,() -> RatisAtomicRejection.create(root.resolve("short-binding"),new byte[31],identity(),digest));
        assertThrows(IllegalArgumentException.class,() -> RatisAtomicRejection.create(root.resolve("short-digest"),binding,identity(),new byte[31]));
    }
}
