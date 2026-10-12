package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native cuts with controlled clocks/read barriers; no network or quorum fixtures. */
class RatisSnapshotPinsTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    final Duration ttl=Duration.ofSeconds(10);
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home home;
        final RatisParticipantPrepareTest.Node data;
        final SnapshotFence fence;
        final SnapshotFenceStatus ready;
        final UUID owner,snapshot=UUID.randomUUID();
        Fixture(String name) throws Exception {
            helper.root=root; helper.helper.root=root;
            home=helper.new Home(name+"-home"); data=helper.helper.new Node(name+"-data",false);
            for (int n=1;n<=3;n++) data.wire.command(data.directory.ranges().getFirst(),data.f.put(n));
            fence=helper.fence(home,data,1,helper.scope(data,1,4)); var initial=home.register(fence); ApplyPosition installed;
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { installed=helper.append(data,install).position(); }
            ready=initial.ready(data.f.groupId,installed); home.transition(initial,ready); owner=data.f.machine.snapshotCaptureOwner();
        }
        RatisSnapshotPins registry(AtomicLong clock,AtomicInteger calls) {
            return new RatisSnapshotPins(owner,data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> { calls.incrementAndGet(); return data.f.machine.captureSnapshotGate(homes,fence); });
        }
        void complete(SnapshotFenceStatus.Capture token) throws Exception {
            var captured=ready.capture(data.f.groupId,token); home.transition(ready,captured); var terminal=captured.complete(); home.transition(captured,terminal);
            try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) { helper.append(data,release); }
        }
        void change() throws Exception {
            data.wire.command(data.directory.ranges().getFirst(),new DistributedCommand(data.f.groupId,1,new byte[32],new OperationId(new UUID(0,7000),1,0),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{2},new byte[]{9}))));
        }
        @Override public void close() throws Exception { data.close(); home.close(); }
    }
    @Test void configuredLongReadonlyCaptureUsesOriginalDeadlineAndNeverRenewsOnRetry()throws Exception {
        try(var f=new Fixture("long-readonly")) {
            var clock=new AtomicLong();var calls=new AtomicInteger();var requested=Duration.ofMinutes(2);try(var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,new RatisSnapshotPins.Limits(16,16L<<20,Duration.ofMinutes(5)),clock::get,(homes,fence)->{calls.incrementAndGet();return f.data.f.machine.captureSnapshotGate(homes,fence);})) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,requested,()->0);clock.set(Duration.ofSeconds(61).toNanos());assertEquals(token,pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,requested,()->0));assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));clock.set(requested.toNanos()-1);assertEquals(token,pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,requested,()->0));clock.set(requested.toNanos());pins.expire();assertThrows(RatisSnapshotLostException.class,()->pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,requested,()->0));assertEquals(1,calls.get());assertEquals(0,pins.stats().pins());
                var canceledFence=identity(f.fence,new UUID(0,10100),1,10);var canceledRequest=new RatisSnapshotCaptureRequest(canceledFence,f.owner,new UUID(0,10101),requested);var reserved=pins.reserveExternal(canceledRequest,()->0);pins.markPhysicalStarted(reserved);pins.cancelExternal(reserved);pins.failExternal(reserved);assertThrows(RatisSnapshotLostException.class,()->pins.capture(f.home.homes,canceledFence,f.owner,canceledRequest.snapshotId(),requested,()->0));assertEquals(1,calls.get());
                assertThrows(IllegalArgumentException.class,()->pins.capture(f.home.homes,f.fence,f.owner,UUID.randomUUID(),Duration.ofMinutes(2),()->0,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));
            }
        }
    }
    @Test void configuredServerLimitStillRejectsDeclaredReadonlyRetentionBeforeNativeCapture()throws Exception {
        helper.helper.dataInitializer=machine->machine.snapshotReadOnlyMaximumTtl(Duration.ofMinutes(3));try(var f=new Fixture("server-retention")) {
            assertThrows(IllegalArgumentException.class,()->f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,Duration.ofMinutes(4)));assertEquals(0,f.data.f.machine.snapshotPinStats().pins());var token=f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,Duration.ofMinutes(2));assertNotNull(token);f.data.f.machine.releaseSnapshotPin(token);
        }
    }
    @Test void wholeMillisecondCodecKeepsFiniteModeCeilingsAndRejectsOverflowBeforeConversion()throws Exception {
        try(var f=new Fixture("retention-codec")) {
            var longRead=new RatisSnapshotCaptureRequest(f.fence,f.owner,f.snapshot,Duration.ofHours(24));assertArrayEquals(longRead.encode(),RatisSnapshotCaptureRequest.decode(longRead.encode()).encode());assertEquals(Duration.ofHours(24),RatisSnapshotCaptureRequest.maximumTtl(RatisSnapshotCaptureRequest.Mode.READ_ONLY));assertEquals(Duration.ofSeconds(60),RatisSnapshotCaptureRequest.maximumTtl(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));assertEquals(Duration.ofSeconds(60),RatisSnapshotPins.Limits.defaults().maximumTtl());
            assertThrows(IllegalArgumentException.class,()->new RatisSnapshotCaptureRequest(f.fence,f.owner,f.snapshot,Duration.ofHours(24).plusMillis(1)));assertThrows(IllegalArgumentException.class,()->new RatisSnapshotCaptureRequest(f.fence,f.owner,f.snapshot,Duration.ofSeconds(Long.MAX_VALUE)));assertThrows(IllegalArgumentException.class,()->new RatisSnapshotCaptureRequest(f.fence,f.owner,f.snapshot,Duration.ofNanos(1)));assertThrows(IllegalArgumentException.class,()->new RatisSnapshotCaptureRequest(f.fence,f.owner,f.snapshot,Duration.ofSeconds(61),RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));var encoded=longRead.encode();encoded[40]=1;assertThrows(IllegalArgumentException.class,()->RatisSnapshotCaptureRequest.decode(encoded));assertThrows(IllegalArgumentException.class,()->new RatisSnapshotPins.Limits(16,16L<<20,Duration.ofHours(24).plusMillis(1)));
        }
    }
    @Test void exactCaptureRetryKeepsItsOriginalCutAndDeadlineThroughCompletionAndLeadershipLoss() throws Exception {
        try (var f=new Fixture("history")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); assertEquals(1,calls.get()); assertEquals(1,pins.stats().pins());
                f.complete(token); f.change(); clock.set(Duration.ofSeconds(9).toNanos());
                assertEquals(token,pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0)); assertEquals(1,calls.get());
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));
                f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));
                clock.set(ttl.toNanos()); pins.expire(); assertEquals(0,pins.stats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes());
                assertThrows(RatisSnapshotLostException.class,() -> pins.get(token,f.data.f.column,new byte[]{2}));
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0)); assertEquals(1,calls.get());
                pins.release(token); pins.release(token);
            }
        }
    }
    @Test void pointAndForwardReversePagesStayScopedAndRejectChangedCaptureIdentity() throws Exception {
        try (var f=new Fixture("pages")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0);
                var forward=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},false,null,1,1024); assertArrayEquals(new byte[]{1},forward.rows().getFirst().key());
                var second=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},false,forward.resumeAfter(),1,PhysicalRangePage.MAX_BYTES); assertArrayEquals(new byte[]{2},second.rows().getFirst().key());
                var reverse=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},true,null,1,1024); assertArrayEquals(new byte[]{3},reverse.rows().getFirst().key());
                var previous=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},true,reverse.resumeAfter(),1,1024); assertArrayEquals(new byte[]{2},previous.rows().getFirst().key());
                assertThrows(IllegalArgumentException.class,() -> pins.get(token,f.data.f.column,new byte[]{4}));
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,UUID.randomUUID(),ttl,() -> 0));
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,Duration.ofSeconds(11),() -> 0));
                var changed=new SnapshotFence(f.fence.clusterId(),f.fence.databaseId(),f.fence.domainId(),f.fence.fenceId(),f.fence.homeGroup(),f.fence.homeEpoch(),f.fence.operation(),1,f.fence.schemaEpoch(),f.fence.schemaDigest(),f.fence.participants());
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,changed,f.owner,f.snapshot,ttl,() -> 0));
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2})); pins.release(token);
                assertEquals(0,pins.stats().pins()); assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
            }
        }
    }
    @Test void ownerLossAndEntryBudgetNeverSubstituteANewCapture() throws Exception {
        try (var f=new Fixture("owner")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,new RatisSnapshotPins.Limits(1,1024*1024,ttl),clock::get,(homes,fence) -> { calls.incrementAndGet(); return f.data.f.machine.captureSnapshotGate(homes,fence); })) {
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,UUID.randomUUID(),f.snapshot,ttl,() -> 0)); assertEquals(0,pins.stats().entries()); assertEquals(0,calls.get());
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); var other=helper.fence(f.home,f.data,2,helper.scope(f.data,4,8));
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,other,f.owner,UUID.randomUUID(),ttl,() -> 0)); assertEquals(1,calls.get()); pins.release(token);
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,other,f.owner,UUID.randomUUID(),ttl,() -> 0)); assertEquals(1,calls.get());
                try (var restarted=new RatisSnapshotPins(UUID.randomUUID(),f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> { throw new AssertionError("Must not reopen at latest"); })) {
                    assertThrows(RatisSnapshotLostException.class,() -> restarted.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
                }
            }
        }
    }
    @Test void captureAndReadOwnershipOutliveExpiryUntilActualNativeReturn() throws Exception {
        for (boolean read:new boolean[]{false,true}) try (var f=new Fixture("busy-"+read); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var clock=new AtomicLong(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            RatisSnapshotPins.CaptureFactory factory=(homes,fence) -> {
                if (!read) { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                return f.data.f.machine.captureSnapshotGate(homes,fence);
            };
            RatisSnapshotPins.ValueReader reader=(pin,column,key) -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); return pin.get(column,key); };
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,factory,reader)) {
                SnapshotFenceStatus.Capture token=read ? pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0) : null;
                var pending=executor.submit(() -> read ? pins.get(token,f.data.f.column,new byte[]{2}) : pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=pins.stats().bytes(); assertTrue(bytes>RatisSnapshotPins.TOMBSTONE_BYTES);
                    clock.set(ttl.toNanos()); pins.expire(); pins.retireBefore(f.fence.operation().epoch()+1);
                    assertFalse(pending.isDone()); assertEquals(bytes,pins.stats().bytes());
                    if (read) assertEquals(1,pins.stats().pins());
                } finally { release.countDown(); }
                var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(RatisSnapshotLostException.class,failure.getCause());
                assertEquals(0,pins.stats().pins()); assertEquals(0,pins.stats().bytes()); assertEquals(0,pins.stats().entries()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
            }
        }
    }
    @Test void storageInvalidationAndShutdownKeepBusyReadChargesUntilReturn() throws Exception {
        for (boolean shutdown:new boolean[]{false,true}) try (var f=new Fixture("retire-"+shutdown); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var clock=new AtomicLong();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,f.data.f.machine::captureSnapshotGate,(pin,column,key) -> {
                entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); return pin.get(column,key);
            })) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); var pending=executor.submit(() -> pins.get(token,f.data.f.column,new byte[]{2}));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=pins.stats().bytes();
                    if (shutdown) pins.close(); else pins.invalidate();
                    assertFalse(pending.isDone()); assertEquals(1,pins.stats().pins()); assertEquals(bytes,pins.stats().bytes()); assertEquals(1,f.data.f.store.activeSnapshotPins());
                } finally { release.countDown(); }
                var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(RatisSnapshotLostException.class,failure.getCause());
                assertEquals(0,pins.stats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
            }
        }
    }
    @Test void failedUnpublishedCaptureCanRetryButNativeOwnerFailureNeverReopens() throws Exception {
        try (var f=new Fixture("failure")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> {
                if (calls.getAndIncrement()==0) throw new IOException("Home read unavailable before capture"); return f.data.f.machine.captureSnapshotGate(homes,fence);
            },(pin,column,key) -> { throw new IllegalStateException("Native owner lost"); })) {
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0)); assertEquals(0,pins.stats().entries()); assertEquals(0,pins.stats().bytes());
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); assertEquals(2,calls.get());
                assertThrows(RatisSnapshotLostException.class,() -> pins.get(token,f.data.f.column,new byte[]{2})); assertEquals(0,pins.stats().pins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0)); assertEquals(2,calls.get());
            }
        }
    }
    private SnapshotFence identity(SnapshotFence base,UUID fenceId,long epoch,long sequence) {
        return new SnapshotFence(base.clusterId(),base.databaseId(),base.domainId(),fenceId,base.homeGroup(),base.homeEpoch(),
                new OperationId(new UUID(0,9900),epoch,sequence),base.directoryGeneration(),base.schemaEpoch(),base.schemaDigest(),base.participants());
    }
    @Test void retiredClosedTombstonesRecycleBothIdentityMapsAndCannotReopenOldBusiness() throws Exception {
        try (var f=new Fixture("recycle")) {
            var calls=new AtomicInteger(); var clock=new AtomicLong(); var floor=new AtomicLong(1);
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> {
                calls.incrementAndGet(); return f.data.f.store.captureReadable(fence.participant(f.data.f.groupId).scope());
            })) {
                var old=identity(f.fence,f.fence.fenceId(),1,0);
                for (int i=0;i<16;i++) {
                    var fence=identity(f.fence,new UUID(0,10000+i),1,i); var token=pins.capture(f.home.homes,fence,f.owner,new UUID(0,11000+i),ttl,floor::get);
                    pins.release(token);
                }
                assertEquals(16,pins.stats().entries()); assertEquals(16L*RatisSnapshotPins.TOMBSTONE_BYTES,pins.stats().bytes());
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,old,f.owner,f.snapshot,ttl,floor::get));
                floor.set(2); pins.retireBefore(2); assertEquals(new RatisSnapshotPins.Stats(0,0,0),pins.stats());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,old,f.owner,f.snapshot,ttl,() -> 1));
                assertEquals(16,calls.get()); assertEquals(0,pins.stats().entries());
                for (int i=0;i<40;i++) {
                    long epoch=2L+i; var fence=identity(f.fence,new UUID(0,10000),epoch,i);
                    var token=pins.capture(f.home.homes,fence,f.owner,new UUID(0,11000),ttl,floor::get);
                    var changed=identity(fence,fence.fenceId(),epoch,i+1);
                    assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,changed,f.owner,token.snapshotId(),ttl,floor::get));
                    assertThrows(RatisSnapshotLostException.class,() -> pins.requireBinding(token,ScopedTransactionRpc.SnapshotBinding.of(old)));
                    pins.release(token); floor.set(epoch+1); pins.retireBefore(floor.get());
                    assertEquals(new RatisSnapshotPins.Stats(0,0,0),pins.stats());
                }
                assertEquals(56,calls.get()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(IllegalArgumentException.class,() -> pins.retireBefore(-1));
            }
        }
    }
    @Test void liveExactReplayBypassesFloorReadsAndRetainsTheOriginalDeadline() throws Exception {
        try (var f=new Fixture("floor-live")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0);
                pins.retireBefore(f.fence.operation().epoch()+1); clock.set(ttl.toNanos()-1);
                assertEquals(token,pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> { throw new AssertionError("Live replay must not read the native floor"); }));
                assertEquals(1,calls.get()); assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));
                clock.set(ttl.toNanos()); pins.expire(); assertEquals(new RatisSnapshotPins.Stats(0,0,0),pins.stats());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
                assertEquals(1,calls.get());
            }
        }
    }
    @Test void floorPreflightRunsOutsideMonitorAndRechecksConcurrentCapture() throws Exception {
        try (var f=new Fixture("floor-race"); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var clock=new AtomicLong(); var calls=new AtomicInteger(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var pins=f.registry(clock,calls)) {
                var pending=executor.submit(() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> {
                    assertFalse(Thread.holdsLock(pins)); entered.countDown();
                    try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                    return 0;
                }));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(0,pins.stats().entries());
                    var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0);
                    release.countDown(); assertEquals(token,pending.get(5,TimeUnit.SECONDS)); assertEquals(1,calls.get());
                } finally { release.countDown(); }
            }
        }
    }
    @Test void failedOrRunningNativeCloseRetainsFullChargeUntilSuccessfulReturn() throws Exception {
        for (boolean blocked:new boolean[]{false,true}) try (var f=new Fixture("close-"+blocked); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var fail=new AtomicBoolean(!blocked); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),() -> 0,
                    f.data.f.machine::captureSnapshotGate,it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.ScopedSnapshot::get,pin -> {
                        if (fail.get()) throw new IOException("Native close failed");
                        if (blocked) { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); }
                        pin.close();
                    })) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); long bytes=pins.stats().bytes();
                if (blocked) {
                    var closing=executor.submit(() -> { pins.release(token); return null; });
                    try {
                        assertTrue(entered.await(5,TimeUnit.SECONDS)); pins.retireBefore(f.fence.operation().epoch()+1);
                        assertFalse(closing.isDone()); assertEquals(bytes,pins.stats().bytes()); assertEquals(1,pins.stats().pins());
                    } finally { release.countDown(); }
                    closing.get(5,TimeUnit.SECONDS);
                } else {
                    assertThrows(IOException.class,() -> pins.release(token));
                    assertThrows(IOException.class,() -> pins.retireBefore(f.fence.operation().epoch()+1));
                    assertEquals(bytes,pins.stats().bytes()); assertEquals(1,pins.stats().pins());
                    fail.set(false); pins.retireBefore(f.fence.operation().epoch()+1);
                }
                assertEquals(new RatisSnapshotPins.Stats(0,0,0),pins.stats()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0));
            } finally { release.countDown(); }
        }
    }
    @Test void pauseInvalidatesPinsBeforeNativeDrainAndANewFenceCanCaptureAfterReload() throws Exception {
        try (var f=new Fixture("machine")) {
            var token=f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,ttl);
            assertEquals(token,f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
            f.complete(token); f.change(); f.data.f.machine.takeSnapshot(); f.data.f.machine.pause();
            assertThrows(RatisSnapshotLostException.class,() -> f.data.f.machine.snapshotPinGet(token,f.data.f.column,new byte[]{2}));
            assertEquals(0,f.data.f.store.activeSnapshotPins()); f.data.f.machine.reinitialize();
            assertThrows(RatisSnapshotLostException.class,() -> f.data.f.machine.captureSnapshotPin(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
            f.data.f.machine.releaseSnapshotPin(token); f.data.f.machine.releaseSnapshotPin(token);
            var next=helper.fence(f.home,f.data,2,helper.scope(f.data,1,4)); var initial=f.home.register(next); ApplyPosition installed;
            try (var install=f.data.f.machine.prepareSnapshotInstall(f.home.homes,next)) { installed=helper.append(f.data,install).position(); }
            f.home.transition(initial,initial.ready(f.data.f.groupId,installed));
            var fresh=f.data.f.machine.captureSnapshotPin(f.home.homes,next,f.data.f.machine.snapshotCaptureOwner(),UUID.randomUUID(),ttl);
            assertArrayEquals(new byte[]{9},f.data.f.machine.snapshotPinGet(fresh,f.data.f.column,new byte[]{2}));
            f.data.f.machine.releaseSnapshotPin(fresh); assertEquals(0,f.data.f.machine.snapshotPinStats().pins()); assertEquals(0,f.data.f.store.activeSnapshotPins());
        }
    }
    @Test void releasingLivePinBelowAdvancedFloorSettlesItsActualOwnerBeforeSuccess() throws Exception {
        try (var f=new Fixture("release-floor")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl,() -> 0); f.complete(token);
                pins.retireBefore(f.fence.operation().epoch()+1); assertEquals(1,pins.stats().pins());
                assertDoesNotThrow(() -> pins.release(token)); assertEquals(new RatisSnapshotPins.Stats(0,0,0),pins.stats()); assertEquals(0,f.data.f.store.activeSnapshotPins());
            }
        }
    }

}
