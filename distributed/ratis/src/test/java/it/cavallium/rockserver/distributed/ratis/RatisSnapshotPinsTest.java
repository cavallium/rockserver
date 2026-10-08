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
    @Test void exactCaptureRetryKeepsItsOriginalCutAndDeadlineThroughCompletionAndLeadershipLoss() throws Exception {
        try (var f=new Fixture("history")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl); assertEquals(1,calls.get()); assertEquals(1,pins.stats().pins());
                f.complete(token); f.change(); clock.set(Duration.ofSeconds(9).toNanos());
                assertEquals(token,pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl)); assertEquals(1,calls.get());
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));
                f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2}));
                clock.set(ttl.toNanos()); pins.expire(); assertEquals(0,pins.stats().pins()); assertEquals(128,pins.stats().bytes());
                assertThrows(RatisSnapshotLostException.class,() -> pins.get(token,f.data.f.column,new byte[]{2}));
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl)); assertEquals(1,calls.get());
                pins.release(token); pins.release(token);
            }
        }
    }
    @Test void pointAndForwardReversePagesStayScopedAndRejectChangedCaptureIdentity() throws Exception {
        try (var f=new Fixture("pages")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=f.registry(clock,calls)) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl);
                var forward=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},false,null,1,1024); assertArrayEquals(new byte[]{1},forward.rows().getFirst().key());
                var second=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},false,forward.resumeAfter(),1,PhysicalRangePage.MAX_BYTES); assertArrayEquals(new byte[]{2},second.rows().getFirst().key());
                var reverse=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},true,null,1,1024); assertArrayEquals(new byte[]{3},reverse.rows().getFirst().key());
                var previous=pins.page(token,f.data.f.column,new byte[]{1},new byte[]{4},true,reverse.resumeAfter(),1,1024); assertArrayEquals(new byte[]{2},previous.rows().getFirst().key());
                assertThrows(IllegalArgumentException.class,() -> pins.get(token,f.data.f.column,new byte[]{4}));
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,UUID.randomUUID(),ttl));
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,Duration.ofSeconds(11)));
                var changed=new SnapshotFence(f.fence.clusterId(),f.fence.databaseId(),f.fence.domainId(),f.fence.fenceId(),f.fence.homeGroup(),f.fence.homeEpoch(),1,f.fence.schemaEpoch(),f.fence.schemaDigest(),f.fence.participants());
                assertThrows(IllegalArgumentException.class,() -> pins.capture(f.home.homes,changed,f.owner,f.snapshot,ttl));
                assertArrayEquals(new byte[]{7},pins.get(token,f.data.f.column,new byte[]{2})); pins.release(token);
                assertEquals(0,pins.stats().pins()); assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
            }
        }
    }
    @Test void ownerLossAndEntryBudgetNeverSubstituteANewCapture() throws Exception {
        try (var f=new Fixture("owner")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,new RatisSnapshotPins.Limits(1,1024*1024,ttl),clock::get,(homes,fence) -> { calls.incrementAndGet(); return f.data.f.machine.captureSnapshotGate(homes,fence); })) {
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,UUID.randomUUID(),f.snapshot,ttl)); assertEquals(0,pins.stats().entries()); assertEquals(0,calls.get());
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl); var other=helper.fence(f.home,f.data,2,helper.scope(f.data,4,8));
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,other,f.owner,UUID.randomUUID(),ttl)); assertEquals(1,calls.get()); pins.release(token);
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,other,f.owner,UUID.randomUUID(),ttl)); assertEquals(1,calls.get());
                try (var restarted=new RatisSnapshotPins(UUID.randomUUID(),f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> { throw new AssertionError("Must not reopen at latest"); })) {
                    assertThrows(RatisSnapshotLostException.class,() -> restarted.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
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
                SnapshotFenceStatus.Capture token=read ? pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl) : null;
                var pending=executor.submit(() -> read ? pins.get(token,f.data.f.column,new byte[]{2}) : pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=pins.stats().bytes(); assertTrue(bytes>128);
                    clock.set(ttl.toNanos()); pins.expire(); assertFalse(pending.isDone()); assertEquals(bytes,pins.stats().bytes());
                    if (read) assertEquals(1,pins.stats().pins());
                } finally { release.countDown(); }
                var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(RatisSnapshotLostException.class,failure.getCause());
                assertEquals(0,pins.stats().pins()); assertEquals(128,pins.stats().bytes()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
            }
        }
    }
    @Test void storageInvalidationAndShutdownKeepBusyReadChargesUntilReturn() throws Exception {
        for (boolean shutdown:new boolean[]{false,true}) try (var f=new Fixture("retire-"+shutdown); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1); var clock=new AtomicLong();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,f.data.f.machine::captureSnapshotGate,(pin,column,key) -> {
                entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); return pin.get(column,key);
            })) {
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl); var pending=executor.submit(() -> pins.get(token,f.data.f.column,new byte[]{2}));
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=pins.stats().bytes();
                    if (shutdown) pins.close(); else pins.invalidate();
                    assertFalse(pending.isDone()); assertEquals(1,pins.stats().pins()); assertEquals(bytes,pins.stats().bytes()); assertEquals(1,f.data.f.store.activeSnapshotPins());
                } finally { release.countDown(); }
                var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(RatisSnapshotLostException.class,failure.getCause());
                assertEquals(0,pins.stats().pins()); assertEquals(128,pins.stats().bytes()); assertEquals(0,f.data.f.store.activeSnapshotPins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl));
            }
        }
    }
    @Test void failedUnpublishedCaptureCanRetryButNativeOwnerFailureNeverReopens() throws Exception {
        try (var f=new Fixture("failure")) {
            var clock=new AtomicLong(); var calls=new AtomicInteger();
            try (var pins=new RatisSnapshotPins(f.owner,f.data.f.groupId,RatisSnapshotPins.Limits.defaults(),clock::get,(homes,fence) -> {
                if (calls.getAndIncrement()==0) throw new IOException("Home read unavailable before capture"); return f.data.f.machine.captureSnapshotGate(homes,fence);
            },(pin,column,key) -> { throw new IllegalStateException("Native owner lost"); })) {
                assertThrows(IOException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl)); assertEquals(0,pins.stats().entries()); assertEquals(0,pins.stats().bytes());
                var token=pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl); assertEquals(2,calls.get());
                assertThrows(RatisSnapshotLostException.class,() -> pins.get(token,f.data.f.column,new byte[]{2})); assertEquals(0,pins.stats().pins());
                assertThrows(RatisSnapshotLostException.class,() -> pins.capture(f.home.homes,f.fence,f.owner,f.snapshot,ttl)); assertEquals(2,calls.get());
            }
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
}
