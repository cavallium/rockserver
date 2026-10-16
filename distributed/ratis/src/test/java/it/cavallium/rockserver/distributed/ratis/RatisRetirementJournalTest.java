package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRetirementJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID home=new UUID(0,1),data=new UUID(0,2),column=new UUID(0,3);
    final byte[] binding=new byte[32];
    TransactionStatus transaction(UUID group,long sequence) {
        return TransactionStatus.begin(new UUID(0,4), new UUID(0,5), new UUID(0,7), new OperationId(new UUID(0,8),1,sequence), List.of(new DomainBinding(new UUID(0,6), 0, new byte[32], 1, new byte[32])), new byte[32], List.of(new TransactionStatus.Participant(new UUID(0,6), group, group, 1, new byte[32], new byte[32], null, false)));
    }
    RatisRetirementOperation participant(UUID group,long sequence) {
        var terminal=transaction(group,sequence).decide(TransactionStatus.Phase.ABORT);
        var finalization=new ParticipantFinalization(home,1,group,new OperationId(new UUID(0,9),0,sequence),terminal);
        return RatisRetirementOperation.participant(new ParticipantRetirement(finalization,new ApplyPosition(2,3),terminal.finalized(group).consume()));
    }
    SnapshotFence fence() {
        return new SnapshotFence(new UUID(0,4), new UUID(0,5), new UUID(0,10), home, 1, new OperationId(new UUID(0,11),1,0), List.of(new DomainBinding(new UUID(0,6), 0, new byte[32], 1, new byte[32])), List.of(new SnapshotFence.Participant(new UUID(0,6), data, data, 1, new byte[32], new OperationId(new UUID(0,12),1,0), new OperationId(new UUID(0,12),1,1), new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))))));
    }
    List<RatisRetirementOperation> operations() {
        var participant=participant(data,0); var initial=transaction(data,0); var consumed=initial.decide(TransactionStatus.Phase.ABORT).finalized(data).consume();
        var proof=new ParticipantFenceProof(new ParticipantIdentity(home,1,data,initial),2,new ApplyPosition(3,4));
        var home=RatisRetirementOperation.home(new HomeRetirement(new HomeIdentity(this.home,1,new byte[32],column,initial),consumed,List.of(proof)));
        var fence=fence(); var snapshot=SnapshotFenceStatus.begin(fence).abort().consume();
        var gate=RatisRetirementOperation.snapshotGate(new SnapshotGateRetirement(snapshot,data,new ApplyPosition(2,3)));
        var snapshotHome=RatisRetirementOperation.snapshotHome(new SnapshotHomeRetirement(new SnapshotHomeIdentity(this.home,1,new byte[32],column,fence),snapshot,
                List.of(new SnapshotFenceProof(fence,data,2,new ApplyPosition(3,4)))));
        var release=RatisRetirementOperation.release(RatisSnapshotRecord.release(snapshot,data));
        return List.of(participant,home,gate,snapshotHome,release);
    }
    Path file(Path directory,RatisRetirementOperation operation) { return directory.resolve(operation.targetGroup()+".bin"); }
    long bytes(RatisRetirementOperation operation) { return 32L+operation.bytes().length; }
    RatisRetirementJournal open(Path directory) throws IOException { return RatisRetirementJournal.open(directory,binding,RatisRetirementJournal.Limits.defaults()); }
    void saved(Path directory,RatisRetirementOperation operation) throws IOException { try (var journal=open(directory)) { assertTrue(journal.reserve(operation)); } }

    @Test void allFiveCanonicalNativeFramesRoundTripAndOwnTheirBytes() throws Exception {
        var expected=List.of(RatisRetirementOperation.Type.PARTICIPANT,RatisRetirementOperation.Type.HOME,RatisRetirementOperation.Type.SNAPSHOT_GATE,RatisRetirementOperation.Type.SNAPSHOT_HOME,RatisRetirementOperation.Type.RELEASE);
        var directory=root.resolve("all-kinds"); var operations=operations();
        try (var journal=open(directory)) {
            for (int n=0;n<operations.size();n++) {
                var operation=operations.get(n); var decoded=RatisRetirementOperation.decode(operation.bytes());
                assertEquals(expected.get(n),operation.type()); assertEquals(operation,decoded); assertEquals(operation.hashCode(),decoded.hashCode());
                assertEquals(operation.type()==RatisRetirementOperation.Type.HOME || operation.type()==RatisRetirementOperation.Type.SNAPSHOT_HOME ? home : data,operation.targetGroup());
                byte[] supplied=operation.bytes(); Arrays.fill(supplied,(byte)0); assertEquals(operation,RatisRetirementOperation.decode(operation.bytes()));
                assertTrue(journal.reserve(operation)); byte[] body=SessionJournalFiles.read(file(directory,operation),RatisRetirementOperation.MAX_BYTES+64);
                assertArrayEquals(binding,Arrays.copyOf(body,32)); assertArrayEquals(operation.bytes(),Arrays.copyOfRange(body,32,body.length));
                assertEquals(new RatisRetirementJournal.Stats(1,body.length),journal.stats()); assertTrue(journal.remove(operation));
            }
        }
        assertEquals(operations.getFirst().participant(),RatisRetirementOperation.decode(operations.getFirst().bytes()).participant());
        assertEquals(operations.get(1).home(),RatisRetirementOperation.decode(operations.get(1).bytes()).home());
        assertEquals(operations.get(2).snapshotGate(),RatisRetirementOperation.decode(operations.get(2).bytes()).snapshotGate());
        assertEquals(operations.get(3).snapshotHome(),RatisRetirementOperation.decode(operations.get(3).bytes()).snapshotHome());
        assertEquals(operations.get(4).release(),RatisRetirementOperation.decode(operations.get(4).bytes()).release());
        assertThrows(IllegalStateException.class,operations.getFirst()::home);
    }

    @Test void onlyCanonicalRetirementOrOriginalReleaseFramesCanBeReserved() {
        var install=RatisSnapshotRecord.install(fence(),data); assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.release(install));
        assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(install.encode()));
        var ordinary=new DistributedCommand(data,1,new byte[32],new OperationId(new UUID(0,13),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2})));
        assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(DistributedCommandCodec.encode(ordinary)));
        var finalization=participant(data,0).participant().finalization(); assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(RatisParticipantRecord.encode(finalization)));
        var consumed=SnapshotFenceStatus.begin(fence()).abort().consume(); var release=RatisSnapshotRecord.release(consumed,data); byte[] status=consumed.encode();
        byte[] noncanonical=ByteBuffer.allocate(24+status.length).put(release.encode(),0,24).put(status).array();
        assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(noncanonical));
        for (var operation:operations()) {
            byte[] trailing=Arrays.copyOf(operation.bytes(),operation.bytes().length+1); assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(trailing));
            assertThrows(IllegalArgumentException.class,() -> RatisRetirementOperation.decode(Arrays.copyOf(operation.bytes(),4)));
        }
    }

    @Test void exclusiveLockAndUnresolvedFramesSurviveCloseAndReopen() throws Exception {
        var directory=root.resolve("restart"); var operation=participant(data,0);
        try (var journal=open(directory)) {
            assertThrows(IOException.class,() -> open(directory)); assertTrue(journal.reserve(operation)); assertTrue(journal.reserve(operation));
            assertEquals(List.of(operation),journal.pending()); assertThrows(UnsupportedOperationException.class,() -> journal.pending().clear());
        }
        try (var journal=open(directory)) {
            assertEquals(List.of(operation),journal.pending()); assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats());
            assertTrue(journal.remove(operation)); assertFalse(journal.remove(operation)); assertEquals(new RatisRetirementJournal.Stats(0,0),journal.stats());
        }
        try (var journal=open(directory)) { assertTrue(journal.pending().isEmpty()); }
    }

    @Test void rpcOwnershipSurvivesCancellationUntilTheActualCallbackReturns() throws Exception {
        var directory=root.resolve("active-rpc"); var operation=participant(data,0);
        var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var returned=new CountDownLatch(1);
        try (var journal=open(directory); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            assertTrue(journal.reserve(operation));
            var rpc=executor.submit(() -> {
                try {
                    journal.invoke(operation,() -> {
                        entered.countDown();
                        for (;;) try { release.await(); break; } catch (InterruptedException cancelled) { /* The RPC still owns its resources. */ }
                    });
                    return null;
                } finally { returned.countDown(); }
            });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertTrue(rpc.cancel(true));
                assertThrows(IOException.class,journal::close); assertThrows(IOException.class,() -> open(directory));
                assertThrows(IOException.class,() -> journal.reserve(operation)); assertThrows(IOException.class,() -> journal.remove(operation));
                assertEquals(List.of(operation),journal.pending()); assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats()); assertFalse(journal.fenced());
            } finally { release.countDown(); }
            assertTrue(returned.await(5,TimeUnit.SECONDS)); assertEquals(List.of(operation),journal.pending());
        }
        try (var journal=open(directory)) { assertEquals(List.of(operation),journal.pending()); assertTrue(journal.remove(operation)); }
    }

    @Test void invocationRequiresTheExactForcedFrameAndRemoteFailureRetainsIt() throws Exception {
        var directory=root.resolve("invoke"); var operation=participant(data,0); var changed=participant(data,1); var calls=new AtomicInteger();
        try (var journal=open(directory)) {
            assertThrows(IOException.class,() -> journal.invoke(operation,calls::incrementAndGet)); assertEquals(0,calls.get());
            assertTrue(journal.reserve(operation)); assertThrows(IOException.class,() -> journal.invoke(changed,calls::incrementAndGet)); assertEquals(0,calls.get());
            assertThrows(IOException.class,() -> journal.invoke(operation,() -> { calls.incrementAndGet(); throw new IOException("Lost remote reply"); }));
            assertEquals(1,calls.get()); assertFalse(journal.fenced()); assertEquals(List.of(operation),journal.pending());
            journal.invoke(operation,calls::incrementAndGet); assertEquals(2,calls.get()); assertEquals(List.of(operation),journal.pending()); assertTrue(journal.remove(operation));
        }
        var corrupt=root.resolve("invoke-corrupt");
        try (var journal=open(corrupt)) {
            assertTrue(journal.reserve(operation)); Files.write(file(corrupt,operation),new byte[]{1});
            assertThrows(IOException.class,() -> journal.invoke(operation,calls::incrementAndGet)); assertEquals(2,calls.get()); assertTrue(journal.fenced());
            assertEquals(List.of(operation),journal.pending());
        }
    }

    @Test void unpublishedCrashSidecarsAreRemovedButPublishedFramesRemainExact() throws Exception {
        var directory=root.resolve("crash-before-publication"); var operation=participant(data,0); saved(directory,operation);
        byte[] published=Files.readAllBytes(file(directory,operation)); var sameGroup=directory.resolve(operation.targetGroup()+".bin.pending"); var otherGroup=directory.resolve(new UUID(0,99)+".bin.pending");
        Files.write(sameGroup,new byte[]{1,2,3}); Files.write(otherGroup,new byte[0]);
        try (var journal=open(directory)) {
            assertFalse(Files.exists(sameGroup)); assertFalse(Files.exists(otherGroup)); assertArrayEquals(published,Files.readAllBytes(file(directory,operation)));
            assertEquals(List.of(operation),journal.pending()); assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats());
        }
        try (var journal=open(directory)) { assertEquals(List.of(operation),journal.pending()); }
        var wrongContext=root.resolve("sidecar-wrong-context"); saved(wrongContext,operation); var sidecar=wrongContext.resolve(new UUID(0,99)+".bin.pending"); Files.write(sidecar,new byte[]{1});
        byte[] wrong=binding.clone(); wrong[0]=1; assertThrows(IOException.class,() -> RatisRetirementJournal.open(wrongContext,wrong,RatisRetirementJournal.Limits.defaults()));
        assertTrue(Files.exists(sidecar)); assertTrue(Files.exists(file(wrongContext,operation)));
    }

    @Test void unpublishedSidecarCleanupRejectsUnownedNamesLinksAndExceededBounds() throws Exception {
        var badName=root.resolve("sidecar-name"); Files.createDirectory(badName); var malformed=badName.resolve("not-a-group.bin.pending"); Files.write(malformed,new byte[]{1});
        assertThrows(IOException.class,() -> open(badName)); assertTrue(Files.exists(malformed));
        var links=root.resolve("sidecar-link"); Files.createDirectory(links); var target=root.resolve("unowned-sidecar-target"); Files.write(target,new byte[]{1}); var link=links.resolve(data+".bin.pending"); Files.createSymbolicLink(link,target);
        assertThrows(IOException.class,() -> open(links)); assertTrue(Files.isSymbolicLink(link)); assertTrue(Files.exists(target));
        var count=root.resolve("sidecar-count"); Files.createDirectory(count);
        for (int n=0;n<=RatisRetirementJournal.MAX_PENDING;n++) Files.write(count.resolve(new UUID(0,100+n)+".bin.pending"),new byte[0]);
        assertThrows(IOException.class,() -> open(count));
        for (int n=0;n<=RatisRetirementJournal.MAX_PENDING;n++) assertTrue(Files.exists(count.resolve(new UUID(0,100+n)+".bin.pending")));
        var size=root.resolve("sidecar-size"); Files.createDirectory(size); var oversized=size.resolve(data+".bin.pending");
        try (var file=FileChannel.open(oversized,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) { file.position(64L+RatisRetirementOperation.MAX_BYTES).write(ByteBuffer.wrap(new byte[]{1})); }
        assertThrows(IOException.class,() -> open(size)); assertTrue(Files.exists(oversized));
        var aggregate=root.resolve("sidecar-total"); Files.createDirectory(aggregate);
        int sidecars=(int)((RatisRetirementJournal.MAX_BYTES+32L*RatisRetirementJournal.MAX_PENDING)/(64L+RatisRetirementOperation.MAX_BYTES))+1;
        assertTrue(sidecars<=RatisRetirementJournal.MAX_PENDING);
        for (int n=0;n<sidecars;n++) try (var file=FileChannel.open(aggregate.resolve(new UUID(0,100+n)+".bin.pending"),StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) {
            file.position(63L+RatisRetirementOperation.MAX_BYTES).write(ByteBuffer.wrap(new byte[]{1}));
        }
        assertThrows(IOException.class,() -> open(aggregate));
        for (int n=0;n<sidecars;n++) assertTrue(Files.exists(aggregate.resolve(new UUID(0,100+n)+".bin.pending")));
    }

    @Test void groupConflictsAndBothHardAndConfiguredQuotasRemainBounded() throws Exception {
        var operation=participant(data,0); var different=participant(data,1); var other=participant(new UUID(0,100),0);
        var directory=root.resolve("quota"); var limits=new RatisRetirementJournal.Limits(2,bytes(operation));
        try (var journal=RatisRetirementJournal.open(directory,binding,limits)) {
            assertTrue(journal.reserve(operation)); assertFalse(journal.reserve(different)); assertFalse(journal.remove(different)); assertFalse(journal.reserve(other));
            assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats()); assertTrue(journal.remove(operation)); assertTrue(journal.reserve(different));
        }
        try (var journal=RatisRetirementJournal.open(root.resolve("configured-count"),binding,new RatisRetirementJournal.Limits(1,RatisRetirementJournal.MAX_BYTES))) {
            assertTrue(journal.reserve(operation)); assertFalse(journal.reserve(other)); assertTrue(journal.reserve(operation)); assertFalse(journal.fenced());
        }
        var countDirectory=root.resolve("count");
        try (var journal=open(countDirectory)) {
            for (int n=0;n<RatisRetirementJournal.MAX_PENDING;n++) assertTrue(journal.reserve(participant(new UUID(0,100+n),0)));
            assertFalse(journal.reserve(participant(new UUID(0,999),0))); assertEquals(RatisRetirementJournal.MAX_PENDING,journal.stats().count());
        }
        try (var journal=open(countDirectory)) { assertEquals(RatisRetirementJournal.MAX_PENDING,journal.pending().size()); }
        assertThrows(IllegalArgumentException.class,() -> new RatisRetirementJournal.Limits(17,RatisRetirementJournal.MAX_BYTES));
        assertThrows(IllegalArgumentException.class,() -> new RatisRetirementJournal.Limits(16,1L+RatisRetirementJournal.MAX_BYTES));
        assertThrows(IllegalArgumentException.class,() -> new RatisRetirementJournal.Limits(0,1));
    }

    @Test void failedPersistenceCannotAuthorizeUseOrDropItsUncertainCharge() throws Exception {
        var directory=root.resolve("failed-reserve"); var operation=participant(data,0); Path temporary=file(directory,operation).resolveSibling(operation.targetGroup()+".bin.pending");
        try (var journal=open(directory)) {
            Files.createDirectory(temporary);
            assertThrows(IOException.class,() -> journal.reserve(operation)); assertFalse(Files.exists(file(directory,operation))); assertTrue(Files.isDirectory(temporary));
            assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats()); assertTrue(journal.fenced()); assertEquals(List.of(operation),journal.pending());
            assertThrows(IOException.class,() -> journal.reserve(operation));
        }
        assertThrows(IOException.class,() -> open(directory)); Files.delete(temporary);
        try (var journal=open(directory)) { assertTrue(journal.pending().isEmpty()); }
    }

    @Test void failedExactRemovalRetainsOwnershipUntilTheDurableFileCanBeReopened() throws Exception {
        var directory=root.resolve("failed-remove"); var operation=participant(data,0); var preserved=root.resolve("preserved.bin");
        try (var journal=open(directory)) {
            assertTrue(journal.reserve(operation)); Files.move(file(directory,operation),preserved); Files.createDirectory(file(directory,operation)); Files.write(file(directory,operation).resolve("unowned"),new byte[]{1});
            assertThrows(IOException.class,() -> journal.remove(operation)); assertEquals(new RatisRetirementJournal.Stats(1,bytes(operation)),journal.stats());
            assertTrue(Files.exists(file(directory,operation).resolve("unowned"))); assertTrue(journal.fenced()); assertEquals(List.of(operation),journal.pending());
        }
        Files.delete(file(directory,operation).resolve("unowned")); Files.delete(file(directory,operation)); Files.move(preserved,file(directory,operation));
        try (var journal=open(directory)) { assertEquals(List.of(operation),journal.pending()); assertTrue(journal.remove(operation)); }
    }

    @Test void loadingRejectsWrongBindingCorruptionAndCrossedOrNoncanonicalFilenames() throws Exception {
        var operation=participant(data,0); var context=root.resolve("context"); saved(context,operation); byte[] wrong=binding.clone(); wrong[0]=1;
        assertThrows(IOException.class,() -> RatisRetirementJournal.open(context,wrong,RatisRetirementJournal.Limits.defaults())); assertTrue(Files.exists(file(context,operation)));
        var corrupt=root.resolve("corrupt"); saved(corrupt,operation); byte[] bytes=Files.readAllBytes(file(corrupt,operation)); bytes[32]^=1; Files.write(file(corrupt,operation),bytes);
        assertThrows(IOException.class,() -> open(corrupt)); assertTrue(Files.exists(file(corrupt,operation)));
        var crossed=root.resolve("crossed"); saved(crossed,operation); var renamed=crossed.resolve(new UUID(0,999)+".bin"); Files.move(file(crossed,operation),renamed);
        assertThrows(IOException.class,() -> open(crossed)); assertTrue(Files.exists(renamed));
        var noncanonical=root.resolve("name"); saved(noncanonical,operation); Files.move(file(noncanonical,operation),noncanonical.resolve("2.bin")); assertThrows(IOException.class,() -> open(noncanonical));
        var unknown=root.resolve("kind"); Files.createDirectory(unknown); var install=RatisSnapshotRecord.install(fence(),data).encode();
        SessionJournalFiles.write(file(unknown,operation),ByteBuffer.allocate(32+install.length).put(binding).put(install).array(),RatisRetirementOperation.MAX_BYTES+64);
        assertThrows(IOException.class,() -> open(unknown)); assertTrue(Files.exists(file(unknown,operation)));
    }

    @Test void directoryCreationAndStartupFileSizesStayBoundedAndRejectLinks() throws Exception {
        assertThrows(IOException.class,() -> open(root.resolve("missing-parent").resolve("journal")));
        var link=root.resolve("link"); Files.createSymbolicLink(link,root); assertThrows(IOException.class,() -> open(link));
        var directory=root.resolve("oversized"); Files.createDirectory(directory); var path=file(directory,participant(data,0));
        try (var file=FileChannel.open(path,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)) { file.position(64L+RatisRetirementOperation.MAX_BYTES).write(ByteBuffer.wrap(new byte[]{1})); }
        assertThrows(IOException.class,() -> open(directory));
        var bindingDirectory=root.resolve("binding-owned"); byte[] supplied=binding.clone();
        try (var journal=RatisRetirementJournal.open(bindingDirectory,supplied,RatisRetirementJournal.Limits.defaults())) { supplied[0]=1; assertTrue(journal.reserve(participant(data,0))); }
        try (var journal=open(bindingDirectory)) { assertEquals(1,journal.pending().size()); }
    }
}
