package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Exact durable writable capture replay over native/proxy groups; no server or network fixtures. */
class RatisWritableSnapshotJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCoordinatorTest helper=new RatisSnapshotCoordinatorTest();
    static final Duration TTL=Duration.ofSeconds(10);
    RatisDurableSnapshot create(Path path,RatisSnapshotCoordinatorTest.Fixture f,RatisSnapshotCaptureRequest.Mode mode) throws Exception {
        var initial=SnapshotFenceStatus.begin(f.fence);
        return RatisDurableSnapshot.create(path,f.home.homes,f.data,initial,f.home.homes.prepare(helper.control(10),null,initial),f.directory.ranges(),TTL,mode);
    }
    static void drain(RatisSnapshotCoordinator coordinator) throws Exception {
        for (int n=0;n<64;n++) if (coordinator.step().complete()) return;
        throw new AssertionError("Snapshot coordinator did not complete");
    }
    static void release(RatisDurableSnapshot journal) throws Exception {
        journal.requestRelease(); journal.refreshHome(); var saved=journal.snapshot();
        journal.consume(new OperationId(saved.lastHome().sessionId(),saved.lastHome().epoch(),saved.lastHome().sequence()+1)); journal.executeHome();
        for (var participant:journal.snapshot().participants()) {
            var result=journal.releaseCapture(participant.route().groupId());
            if (result==RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED) result=journal.releaseCapture(participant.route().groupId());
            assertNotEquals(RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED,result);
        }
    }
    @Test void writableLostCaptureReplyReopensOriginalModeNonceCutAndCertifiedHandleAcrossTwoGroups() throws Exception {
        helper.root=root;
        try (var f=helper.new Fixture("write-replay")) {
            Path path=root.resolve("write.bin"); Map<UUID,byte[]> requests=new HashMap<>(); f.loseCapture=true;
            try (var journal=create(path,f,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED); var driver=f.driver(journal)) {
                boolean lost=false;
                for (int n=0;n<64 && !lost;n++) try { driver.step(); } catch (IOException failure) { assertTrue(failure.getMessage().contains("Lost published")); lost=true; }
                assertTrue(lost); assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,journal.mode());
                for (var participant:journal.snapshot().participants()) if (participant.capture()!=null) requests.put(participant.route().groupId(),participant.capture().bytes());
                assertFalse(requests.isEmpty());
            }
            Map<UUID,ScopedTransactionRpc.Handle> handles=new HashMap<>();
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.data); var driver=f.driver(journal)) {
                assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,journal.snapshot().captureMode()); drain(driver);
                for (var participant:journal.snapshot().participants()) {
                    assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,participant.capture().request().mode());
                    if (requests.containsKey(participant.route().groupId())) assertArrayEquals(requests.get(participant.route().groupId()),participant.capture().bytes());
                    assertNotNull(participant.certifiedHandle()); assertEquals(participant.captureToken().ownerIncarnation(),participant.certifiedHandle().owner()); assertEquals(participant.captureToken().position(),participant.certifiedHandle().position());
                    handles.put(participant.route().groupId(),participant.certifiedHandle());
                    assertEquals(participant.certifiedHandle(),f.data.executeWritableSnapshotCapture(participant.capture()).handle());
                }
                assertEquals(1,f.left.f.store.activeTrackedSnapshotPins()); assertEquals(1,f.right.f.store.activeTrackedSnapshotPins());
                assertThrows(IllegalArgumentException.class,() -> journal.freezeCapture(f.data.prepareSnapshotCapture(journal.snapshot().participants().getFirst().capture().target(),UUID.randomUUID(),TTL)));
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.data)) {
                for (var participant:journal.snapshot().participants()) assertEquals(handles.get(participant.route().groupId()),participant.certifiedHandle());
                release(journal); assertEquals(0,f.left.f.store.activeTrackedSnapshotPins()); assertEquals(0,f.right.f.store.activeTrackedSnapshotPins());
            }
        }
    }
    @Test void unknownWritableReleaseRecoversItsHandleAndExpiryNeverSubstitutesReadOnlyCapture() throws Exception {
        helper.root=root;
        try (var f=helper.new Fixture("unknown-release")) {
            Path path=root.resolve("unknown.bin"); f.loseCapture=true;
            try (var journal=create(path,f,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED); var driver=f.driver(journal)) {
                boolean lost=false;
                for (int n=0;n<64 && !lost;n++) try { driver.step(); } catch (IOException failure) { assertTrue(failure.getMessage().contains("Lost published")); lost=true; }
                assertTrue(lost); journal.refreshHome(); var saved=journal.snapshot(); var abort=saved.observedHome().abort();
                journal.queueHome(f.home.homes.prepare(new OperationId(saved.lastHome().sessionId(),saved.lastHome().epoch(),saved.lastHome().sequence()+1),saved.observedHome(),abort)); journal.executeHome(); drain(driver);
                journal.consume(new OperationId(journal.snapshot().lastHome().sessionId(),journal.snapshot().lastHome().epoch(),journal.snapshot().lastHome().sequence()+1)); journal.executeHome();
                var captured=journal.snapshot().participants().stream().filter(p -> p.capture()!=null).findFirst().orElseThrow();
                assertNull(captured.captureToken());
                assertEquals(RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED,journal.releaseCapture(captured.route().groupId()));
                var recovered=journal.snapshot().participant(captured.route().groupId()); assertNotNull(recovered.certifiedHandle());
                assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,recovered.capture().request().mode());
                assertEquals(RatisDurableSnapshot.CaptureReleaseProgress.RELEASED,journal.releaseCapture(captured.route().groupId()));
                assertEquals(0,f.left.f.store.activeTrackedSnapshotPins()+f.right.f.store.activeTrackedSnapshotPins());
            }
        }
        try (var f=helper.new Fixture("expired-write")) {
            Path path=root.resolve("expired.bin");
            try (var journal=create(path,f,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED); var driver=f.driver(journal)) {
                drain(driver); var original=journal.snapshot().participants().getFirst().certifiedHandle();
                f.clock.set(TTL.toNanos()); f.left.f.machine.expireTransactions(); f.right.f.machine.expireTransactions();
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(journal.snapshot().participants().getFirst().route().groupId()));
                assertEquals(original,journal.snapshot().participants().getFirst().certifiedHandle()); assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,journal.mode());
                release(journal); assertEquals(0,f.left.f.store.activeTrackedSnapshotPins()+f.right.f.store.activeTrackedSnapshotPins());
            }
        }
    }
    @Test void canonicalModeAndHandleBindingRejectCorruptedJournalAndReadOnlyClientCannotAcquireWriteMode() throws Exception {
        try (var f=new RatisAtomicClientTest.Fixture(root,true)) {
            var home=f.snapshotDirectory.ranges().getFirst(); var plan=RatisSnapshotPlan.create(f.directory,RatisSnapshotClientTest.scope(f),new UUID(130,1),new OperationId(f.session,1,513),home.groupId(),home.rangeEpoch(),new OperationId(f.session,1,514));
            var initial=SnapshotFenceStatus.begin(plan.fence()); Path path=root.resolve("bound-write.bin"); ScopedTransactionRpc.Handle handle;
            try (var journal=RatisDurableSnapshot.create(path,f.snapshots,f.data,initial,f.snapshots.prepare(new OperationId(f.session,1,642),null,initial),plan.routes(),TTL,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
                 var driver=new RatisSnapshotCoordinator(journal,f.snapshots,f.data,last -> new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,TTL)) {
                drain(driver); handle=journal.snapshot().participants().getFirst().certifiedHandle(); assertEquals(0,f.opens.get(),"Writable snapshot capture adopts tracked cuts without ordinary OPEN substitution");
            }
            int calls=f.rpcs.get();
            try (var client=RatisSnapshotClientTest.client(f); var lease=client.reopen(path,RequestContext.batch())) {
                var failure=assertThrows(ExecutionException.class,() -> lease.completion().get(5,TimeUnit.SECONDS)); assertInstanceOf(IOException.class,failure.getCause()); assertEquals(calls,f.rpcs.get());
            }
            byte[] original=SessionJournalFiles.read(path,RatisDurableSnapshot.MAX_BYTES); byte[] mode=original.clone(); mode[68]=0;
            SessionJournalFiles.write(path,mode,RatisDurableSnapshot.MAX_BYTES); assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.snapshots,f.data));
            var encoded=ByteBuffer.allocate(ScopedTransactionRpc.HANDLE_BYTES); ScopedTransactionRpc.writeHandle(encoded,handle); int offset=indexOf(original,encoded.array()); assertTrue(offset>=0);
            byte[] corrupt=original.clone(); corrupt[offset]^=1; SessionJournalFiles.write(path,corrupt,RatisDurableSnapshot.MAX_BYTES);
            assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.snapshots,f.data));
            SessionJournalFiles.write(path,original,RatisDurableSnapshot.MAX_BYTES);
            try (var journal=RatisDurableSnapshot.reopen(path,f.snapshots,f.data)) { release(journal); }
        }
    }
    static int indexOf(byte[] haystack,byte[] needle) {
        outer: for (int i=0;i<=haystack.length-needle.length;i++) { for (int j=0;j<needle.length;j++) if (haystack[i+j]!=needle[j]) continue outer; return i; }
        return -1;
    }
}
