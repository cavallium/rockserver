package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Forced control recovery with real native state and controlled transport; no network/server fixture. */
class RatisDurableSnapshotTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCaptureRpcTest helper=new RatisSnapshotCaptureRpcTest();
    RatisSnapshotCaptureRpcTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name,true,false); }
    OperationId control(long sequence) { return new OperationId(new UUID(0,8100),1,sequence); }
    RatisDurableSnapshot create(Path path,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return RatisDurableSnapshot.create(path,f.home.homes,f.client,f.initial,f.home.homes.prepare(control(0),null,f.initial),f.data.directory.ranges());
    }
    void installed(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        assertEquals(f.initial,journal.refreshHome()); assertTrue(journal.resolveHomeFromObserved());
        journal.freezeInstall(f.data.f.groupId); assertNotNull(journal.executeInstall(f.data.f.groupId).position()); assertTrue(journal.verifyReady(f.data.f.groupId));
    }
    void transition(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f,SnapshotFenceStatus next,long sequence) throws Exception {
        journal.queueHome(f.home.homes.prepare(control(sequence),journal.snapshot().observedHome(),next));
        assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status());
    }
    RatisSnapshotCoordinator coordinator(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return new RatisSnapshotCoordinator(journal,f.home.homes,f.client,
                last -> new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10));
    }
    @Test void longReadonlyLifetimeReopensExactRequestAndModeWithoutChangingNativePin()throws Exception {
        helper.helper.helper.dataInitializer=machine->machine.snapshotReadOnlyMaximumTtl(Duration.ofMinutes(5));Path path=root.resolve("long-readonly.journal");var lifetime=Duration.ofMinutes(2);try(var f=fixture("long-readonly")) {
            var group=f.data.f.groupId;byte[] exact;SnapshotFenceStatus.Capture token;try(var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,f.initial,f.home.homes.prepare(control(0),null,f.initial),f.data.directory.ranges(),lifetime,RatisSnapshotCaptureRequest.Mode.READ_ONLY)){installed(journal,f);transition(journal,f,f.initial.ready(group,journal.snapshot().participant(group).readyProof()),1);var operation=f.client.prepareSnapshotCapture(f.client.snapshotTarget(f.fence,group),UUID.randomUUID(),lifetime);journal.freezeCapture(operation);exact=operation.bytes();token=journal.executeCapture(group);assertEquals(1,f.data.f.machine.snapshotPinStats().pins());}
            try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)){assertEquals(lifetime,journal.snapshot().requestedTtl());assertEquals(RatisSnapshotCaptureRequest.Mode.READ_ONLY,journal.mode());assertArrayEquals(exact,journal.snapshot().participant(group).capture().bytes());assertEquals(token,journal.executeCapture(group));assertEquals(1,f.data.f.machine.snapshotPinStats().pins());}f.data.f.machine.releaseSnapshotPin(token);
        }
    }
    @Test void modeBoundLifetimeRejectsBeforeJournalCreationAndOnColdCurrentFrameRead()throws Exception {
        try(var f=fixture("ttl-mode")) {
            var lifetime=Duration.ofMinutes(2);var invalid=root.resolve("write-long.journal");long home=f.home.wire.index;assertThrows(IllegalArgumentException.class,()->RatisDurableSnapshot.create(invalid,f.home.homes,f.client,f.initial,f.home.homes.prepare(control(0),null,f.initial),f.data.directory.ranges(),lifetime,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED));assertFalse(Files.exists(invalid));assertEquals(home,f.home.wire.index);
            Path path=root.resolve("mode-read.journal");try(var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,f.initial,f.home.homes.prepare(control(0),null,f.initial),f.data.directory.ranges(),lifetime,RatisSnapshotCaptureRequest.Mode.READ_ONLY)){assertEquals(lifetime,journal.snapshot().requestedTtl());}try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)){assertEquals(lifetime,journal.snapshot().requestedTtl());}
            byte[] current=SessionJournalFiles.read(path,RatisDurableSnapshot.MAX_BYTES);current[68]=1;SessionJournalFiles.write(path,current,RatisDurableSnapshot.MAX_BYTES);assertThrows(IOException.class,()->RatisDurableSnapshot.reopen(path,f.home.homes,f.client));assertEquals(home,f.home.wire.index);
        }
    }
    @Test void externalCapturedCutIsForcedAndColdReopenAcceptsExactResultWithoutCaptureRpc()throws Exception {
        Path path=root.resolve("external.journal");try(var f=fixture("external")) {
            var group=f.data.f.groupId;RatisRoutedClient.PreparedSnapshotCapture prepared;SnapshotFenceStatus.Capture token;
            try(var journal=create(path,f)) {installed(journal,f);transition(journal,f,f.initial.ready(group,journal.snapshot().participant(group).readyProof()),1);prepared=f.operation();journal.freezeCapture(prepared);
                try(var pin=f.data.f.machine.captureSnapshotGate(f.home.homes,f.fence)) {token=new SnapshotFenceStatus.Capture(prepared.request().owner(),prepared.request().snapshotId(),pin.position());int owners=f.owners.get(),homes=f.homes.get();assertEquals(token,journal.acceptExternalCapture(group,prepared,new RatisSnapshotCaptureRequest.Result(token,null)));assertEquals(owners,f.owners.get());assertEquals(homes,f.homes.get());assertArrayEquals(new byte[]{7},pin.get(f.data.f.column,new byte[]{2}));}
            }
            int owners=f.owners.get(),homes=f.homes.get();try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)){var saved=journal.snapshot().participant(group);assertArrayEquals(prepared.bytes(),saved.capture().bytes());assertEquals(token,saved.captureToken());assertEquals(token,journal.acceptExternalCapture(group,prepared,new RatisSnapshotCaptureRequest.Result(token,null)));assertEquals(owners,f.owners.get());assertEquals(homes,f.homes.get());var changed=f.client.prepareSnapshotCapture(prepared.target(),UUID.randomUUID(),Duration.ofSeconds(10));assertThrows(IllegalArgumentException.class,()->journal.acceptExternalCapture(group,changed,new RatisSnapshotCaptureRequest.Result(token,null)));}
        }
    }
    @Test void externalJournalForceUnknownFencesOriginalOwnerThenReopensExactForcedToken()throws Exception {
        Path path=root.resolve("external-force.journal");try(var f=fixture("external-force")) {
            var group=f.data.f.groupId;RatisRoutedClient.PreparedSnapshotCapture prepared;SnapshotFenceStatus.Capture token;
            try(var journal=create(path,f)){installed(journal,f);transition(journal,f,f.initial.ready(group,journal.snapshot().participant(group).readyProof()),1);prepared=f.operation();journal.freezeCapture(prepared);try(var pin=f.data.f.machine.captureSnapshotGate(f.home.homes,f.fence)){token=new SnapshotFenceStatus.Capture(prepared.request().owner(),prepared.request().snapshotId(),pin.position());journal.persistence=(file,body,maximum)->{SessionJournalFiles.write(file,body,maximum);throw new IOException("Unknown after external capture force");};var result=new RatisSnapshotCaptureRequest.Result(token,null);assertThrows(IOException.class,()->journal.acceptExternalCapture(group,prepared,result));assertThrows(IOException.class,()->journal.acceptExternalCapture(group,prepared,result));}}
            int owners=f.owners.get(),homes=f.homes.get();try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)){assertArrayEquals(prepared.bytes(),journal.snapshot().participant(group).capture().bytes());assertEquals(token,journal.snapshot().participant(group).captureToken());assertEquals(token,journal.acceptExternalCapture(group,prepared,new RatisSnapshotCaptureRequest.Result(token,null)));assertEquals(owners,f.owners.get());assertEquals(homes,f.homes.get());}
        }
    }
    @Test void coordinatorCompletesForcedWorkflowWithOneRaftWritePerStep() throws Exception {
        try (var f=fixture("driver"); var journal=create(root.resolve("driver.journal"),f);
                var driver=coordinator(journal,f)) {
            boolean complete=false;
            for (int n=0;n<20 && !complete;n++) {
                long before=f.data.wire.index+f.home.wire.index; var step=driver.step(); complete=step.complete();
                assertTrue(f.data.wire.index+f.home.wire.index-before<=1,"One Raft mutation per step");
                if (!journal.snapshot().observedHome().terminal()) assertTrue(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
            assertTrue(complete); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,journal.snapshot().observedHome().phase());
            var progress=journal.snapshot().participant(f.data.f.groupId);
            assertArrayEquals(new byte[]{7},f.client.snapshotGet(progress.capture().target(),progress.captureToken(),f.data.f.column,new byte[]{2}));
        }
    }
    void retireGate(RatisSnapshotCaptureRpcTest.Fixture f,SnapshotFenceStatus consumed,ApplyPosition released) throws Exception {
        f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(f.data.f.admin,2,0),2));
        f.data.f.store.applySnapshotGateRetirement(new ApplyPosition(1,++f.data.wire.index),new SnapshotGateRetirement(consumed,f.data.f.groupId,released));
        assertNull(f.data.f.store.snapshotGate(f.fence.fenceId()));
    }
    @Test void explicitConsumptionThenGcReopensAnUnknownReleaseWithRetirementOnly() throws Exception {
        Path path=root.resolve("retired-release.journal");
        try (var f=fixture("retired-release")) {
            var group=f.data.f.groupId; SnapshotFenceStatus consumed; byte[] releaseBytes; ApplyPosition installed;
            try (var journal=create(path,f)) {
                installed(journal,f); installed=journal.snapshot().participant(group).installReceipt().position();
                transition(journal,f,journal.snapshot().observedHome().abort(),1); var terminal=journal.snapshot().observedHome(); journal.freezeRelease(group);
                var operation=f.client.prepareSnapshotGate(RatisSnapshotRecord.release(terminal,group)); releaseBytes=operation.bytes();
                var released=f.client.executeSnapshotGate(operation).position();
                journal.consume(control(2)); assertFalse(journal.snapshot().observedHome().consumed());
                var frozenConsumption=DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()); journal.consume(control(3));
                assertArrayEquals(frozenConsumption,DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()));
                journal.executeHome(); consumed=journal.snapshot().observedHome();
                assertTrue(consumed.consumed()); assertArrayEquals(releaseBytes,RatisSnapshotRecord.release(consumed,group).encode());
                long writes=f.home.wire.index+f.data.wire.index; var last=journal.snapshot().lastHome();
                journal.consume(control(3)); journal.executeHome(); assertEquals(last,journal.snapshot().lastHome()); assertEquals(writes,f.home.wire.index+f.data.wire.index);
                retireGate(f,consumed,released);
            }
            long writes=f.home.wire.index+f.data.wire.index;
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                var step=driver.step(); assertEquals(RatisSnapshotCoordinator.Action.RETIRED,step.action()); assertTrue(step.complete());
                var progress=journal.snapshot().participant(group); assertNotNull(progress.retirementProof()); assertNull(progress.releaseReceipt());
                assertEquals(installed,progress.installReceipt().position()); assertNull(progress.captureToken());
                var retired=journal.executeRelease(group); assertTrue(retired.retired()); assertFalse(retired.suppressed()); assertNull(retired.position());
                assertEquals(consumed,journal.snapshot().observedHome()); assertTrue(driver.step().complete());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertNotNull(journal.snapshot().participant(group).retirementProof()); assertNull(journal.snapshot().participant(group).releaseReceipt()); assertTrue(driver.step().complete());
            }
            for (var request:List.of(RatisSnapshotRecord.install(f.fence,group),RatisSnapshotRecord.release(consumed,group))) {
                var result=f.client.executeSnapshotGate(f.client.prepareSnapshotGate(request)); assertTrue(result.retired()); assertFalse(result.suppressed()); assertNull(result.position());
            }
            var readiness=f.client.snapshotReadiness(f.fence,group); assertTrue(readiness.retired()); assertFalse(readiness.ready()); assertNull(readiness.installed());
            assertEquals(writes,f.home.wire.index+f.data.wire.index); assertNull(f.data.f.store.snapshotGate(f.fence.fenceId()));
        }
    }
    @Test void neverInstalledRetiredGateCannotInventReadyCaptureOrSuppressedInstall() throws Exception {
        var admission=helper.helper; admission.root=root; admission.helper.root=root; var path=root.resolve("retired-never-installed.journal");
        try (var home=admission.new Home("retired-never-installed-home");
                var data=admission.helper.new Node("retired-never-installed-data",false,null,home.homes,System::nanoTime)) {
            var fence=admission.fence(home,data,77,admission.scope(data,1,4)); var initial=SnapshotFenceStatus.begin(fence); var group=data.f.groupId;
            try (var journal=RatisDurableSnapshot.create(path,home.homes,data.routed,initial,home.homes.prepare(control(0),null,initial),data.directory.ranges())) {
                journal.executeHome(); journal.freezeInstall(group); var abort=initial.abort();
                journal.queueHome(home.homes.prepare(control(1),initial,abort)); journal.executeHome();
                var released=data.routed.executeSnapshotGate(data.routed.prepareSnapshotGate(RatisSnapshotRecord.release(abort,group))).position();
                var consumed=abort.consume(); home.homes.execute(home.homes.prepare(control(2),abort,consumed));
                data.wire.command(data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(group,1,new byte[32],new OperationId(data.f.admin,2,0),2));
                data.f.store.applySnapshotGateRetirement(new ApplyPosition(1,++data.wire.index),new SnapshotGateRetirement(consumed,group,released));
                assertThrows(IOException.class,() -> journal.executeInstall(group)); assertNull(journal.snapshot().participant(group).retirementProof());
                journal.refreshHome(); var resolved=journal.resolveRelease(group); assertTrue(resolved.retired()); assertNull(resolved.position());
                var install=journal.executeInstall(group); assertTrue(install.retired()); assertFalse(install.suppressed()); assertNull(install.position());
                journal.freezeRelease(group); assertTrue(journal.executeRelease(group).retired());
                var progress=journal.snapshot().participant(group); assertNull(progress.installReceipt()); assertNull(progress.readyProof()); assertNull(progress.captureToken()); assertNull(progress.nativeGate()); assertNull(progress.releaseReceipt());
            }
            long writes=home.wire.index+data.wire.index;
            try (var journal=RatisDurableSnapshot.reopen(path,home.homes,data.routed);
                    var driver=new RatisSnapshotCoordinator(journal,home.homes,data.routed,last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))) {
                assertTrue(driver.step().complete()); assertNotNull(journal.snapshot().participant(group).retirementProof());
                assertTrue(journal.executeInstall(group).retired()); assertTrue(journal.executeRelease(group).retired());
                assertEquals(writes,home.wire.index+data.wire.index); assertNull(data.f.store.snapshotGate(fence.fenceId()));
            }
        }
    }
    @Test void reusedFenceUuidRetiresOldJournalWithoutAdoptingOrChangingTheNewGate() throws Exception {
        Path path=root.resolve("retired-reused-gate.journal");
        try (var f=fixture("retired-reused-gate"); var newHome=helper.helper.new Home("reused-gate-home")) {
            var group=f.data.f.groupId; SnapshotFenceStatus consumed;
            try (var journal=create(path,f)) {
                installed(journal,f); transition(journal,f,journal.snapshot().observedHome().abort(),1); journal.freezeRelease(group);
                var released=f.client.executeSnapshotGate(f.client.prepareSnapshotGate(RatisSnapshotRecord.release(journal.snapshot().observedHome(),group))).position();
                journal.consume(control(2)); journal.executeHome(); consumed=journal.snapshot().observedHome(); retireGate(f,consumed,released);
            }
            var old=f.fence; var participant=old.participant(group);
            var newFence=new SnapshotFence(old.clusterId(), old.databaseId(), old.fenceId(), newHome.f.groupId, 1, new OperationId(old.operation().sessionId(),2,0), old.domains(), List.of(new SnapshotFence.Participant(participant.domainId(), participant.rangeId(), group, participant.rangeEpoch(), participant.schemaDigest(), new OperationId(participant.installOperation().sessionId(),2,0), new OperationId(participant.releaseOperation().sessionId(),2,1), participant.scope())));
            var newStatus=newHome.register(newFence);
            var newGate=f.data.f.store.applyGateInstall(new ApplyPosition(1,++f.data.wire.index),newFence).gate(); assertTrue(newGate.active());
            long writes=f.home.wire.index+f.data.wire.index+newHome.wire.index;
            for (int n=0;n<2;n++) try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertTrue(driver.step().complete()); assertEquals(consumed,journal.snapshot().observedHome());
                var progress=journal.snapshot().participant(group); assertNotNull(progress.retirementProof()); assertNull(progress.releaseReceipt());
                assertEquals(old,progress.nativeGate().fence()); assertTrue(journal.executeRelease(group).retired());
            }
            assertEquals(newGate,f.data.f.store.snapshotGate(old.fenceId())); assertEquals(newStatus,newHome.homes.read(old.fenceId()));
            assertEquals(writes,f.home.wire.index+f.data.wire.index+newHome.wire.index);
        }
    }
    @Test void consumedCompleteSnapshotKeepsOriginalReleaseAndLiveCaptureAcrossGcUntilOwnerExpiry() throws Exception {
        Path path=root.resolve("retired-capture.journal");
        try (var f=fixture("retired-capture")) {
            var group=f.data.f.groupId; SnapshotFenceStatus.Capture token; ApplyPosition released;
            try (var journal=create(path,f); var driver=coordinator(journal,f)) {
                boolean complete=false; for (int n=0;n<20 && !complete;n++) complete=driver.step().complete(); assertTrue(complete);
                token=journal.snapshot().participant(group).captureToken(); released=journal.snapshot().participant(group).releaseReceipt();
                journal.consume(control(100)); journal.executeHome(); retireGate(f,journal.snapshot().observedHome(),released);
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                var result=journal.executeRelease(group); assertFalse(result.retired()); assertEquals(released,result.position());
                assertNotNull(journal.snapshot().participant(group).retirementProof()); assertEquals(released,journal.snapshot().participant(group).releaseReceipt());
                assertEquals(token,journal.executeCapture(group));
                assertArrayEquals(new byte[]{7},f.client.snapshotGet(journal.snapshot().participant(group).capture().target(),token,f.data.f.column,new byte[]{2}));
                assertTrue(driver.step().complete());
                f.clock.set(Duration.ofSeconds(10).toNanos()); f.data.f.machine.expireTransactions();
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group)); assertEquals(0,f.data.f.store.activeSnapshotPins());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                assertEquals(token,journal.snapshot().participant(group).captureToken()); assertTrue(journal.snapshot().participant(group).captureLost());
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group)); assertNull(f.data.f.store.snapshotGate(f.fence.fenceId()));
            }
        }
    }
    @Test void coordinatorRetainsUndrainedInstallAuthorityAcrossWaitSteps() throws Exception {
        try (var f=fixture("waiting")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,19,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            try (var journal=RatisDurableSnapshot.create(root.resolve("waiting.journal"),f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges());
                    var driver=coordinator(journal,f)) {
                driver.step();
                assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(1,f.data.f.machine.snapshotPreparationSlots()); assertEquals(1,f.data.f.machine.proposalGateStats().gates());
                assertNotNull(f.data.f.propose(f.data.f.put(1)).getException());
                f.data.f.machine.notifyNotLeader(List.of());
                f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true));
                f.data.f.machine.notifyTermIndexUpdated(2,++f.data.wire.index);
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action());
                assertNotNull(f.data.f.store.snapshotGate(fence.fenceId()).installed());
            }
        }
    }
    @Test void coordinatorRetriesLostInstallReplyInsteadOfWaitingOnRetiredAuthority() throws Exception {
        Path path=root.resolve("install-retry.journal");
        try (var f=fixture("install-retry")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,20,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges()); var driver=coordinator(journal,f)) {
                driver.step(); f.data.wire.lose=true; assertThrows(IOException.class,driver::step);
                assertNull(journal.snapshot().participant(f.data.f.groupId).installReceipt());
            }
            long applied=f.data.wire.index;
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action()); assertEquals(applied,f.data.wire.index);
                assertNotNull(journal.snapshot().participant(f.data.f.groupId).installReceipt());
            }
        }
    }
    @Test void closeCancelsOnlyUndispatchedFrozenInstallAndReopenRetainsExactRequest() throws Exception {
        Path path=root.resolve("cancel-wait.journal");
        try (var f=fixture("cancel-wait")) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,21,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,f.client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges())) {
                var driver=coordinator(journal,f); driver.step(); assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                assertEquals(1,f.data.f.machine.proposalGateStats().gates()); driver.close(); assertEquals(0,f.data.f.machine.proposalGateStats().gates());
                assertTrue(journal.snapshot().participant(f.data.f.groupId).installFrozen()); assertNull(journal.snapshot().participant(f.data.f.groupId).installReceipt()); assertThrows(IOException.class,driver::step);
                f.data.f.machine.cancelTransaction(old);
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) { assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action()); }
        }
    }
    @Test void cancelledCloseViewKeepsJournalLockedUntilActualInstallCancellationReturns() throws Exception {
        Path path=root.resolve("cancel-owned.journal");
        try (var f=fixture("cancel-owned"); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.helper.append(f.data,release); }
            var fence=helper.helper.fence(f.home,f.data,22,helper.helper.scope(f.data,1,4)); var initial=SnapshotFenceStatus.begin(fence);
            var old=f.data.f.propose(f.data.f.put(3)); assertNull(old.getException());
            var entered=new java.util.concurrent.CountDownLatch(1); var release=new java.util.concurrent.CountDownLatch(1); var done=new java.util.concurrent.CountDownLatch(1);
            var cancelDrain=new java.util.concurrent.CompletableFuture<Void>();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return f.data.wire.participant(route,bytes); }
                public java.util.concurrent.CompletableFuture<byte[]> participantAsync(RangeDescriptor route,byte[] bytes)throws IOException{return f.data.wire.participantAsync(route,bytes);}
                public java.util.concurrent.CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] bytes,boolean fresh)throws IOException {
                    if(ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL){entered.countDown();return cancelDrain.thenCompose(ignored->{try{return f.data.wire.transactionAsync(route,bytes,fresh);}catch(IOException failure){return java.util.concurrent.CompletableFuture.failedFuture(failure);}});}
                    return f.data.wire.transactionAsync(route,bytes,fresh);
                }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.SNAPSHOT_GATE_CANCEL) {
                        entered.countDown(); try { if (!release.await(5,java.util.concurrent.TimeUnit.SECONDS)) throw new IOException("Fixture timeout"); } catch (InterruptedException failure) { throw new IOException(failure); }
                    }
                    return f.data.wire.transaction(route,bytes,fresh);
                }
            };
            try (var client=new RatisRoutedClient(f.data.directory,transport); var journal=RatisDurableSnapshot.create(path,f.home.homes,client,initial,f.home.homes.prepare(control(0),null,initial),f.data.directory.ranges());
                    var driver=new RatisSnapshotCoordinator(journal,f.home.homes,client,last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))) {
                driver.step(); assertEquals(RatisSnapshotCoordinator.Action.WAITING,driver.step().action());
                var view=executor.submit(() -> { try { driver.close(); return null; } finally { done.countDown(); } });
                try {
                    assertTrue(entered.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertTrue(view.cancel(false)); assertThrows(IOException.class,journal::close); assertThrows(IOException.class,driver::close);
                    assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,client));
                } finally { release.countDown(); cancelDrain.complete(null); }
                assertTrue(done.await(5,java.util.concurrent.TimeUnit.SECONDS)); assertEquals(0,f.data.f.machine.proposalGateStats().gates());
                f.data.f.machine.cancelTransaction(old); journal.close();
                try (var reopened=RatisDurableSnapshot.reopen(path,f.home.homes,client)) { assertTrue(reopened.snapshot().participant(f.data.f.groupId).installFrozen()); assertNull(reopened.snapshot().participant(f.data.f.groupId).installReceipt()); }
            } finally { release.countDown(); }
        }
    }
    @Test void observedForeignCaptureIsDurablyLostBeforeTheNextPolicyStep() throws Exception {
        Path path=root.resolve("foreign.journal");
        try (var f=fixture("foreign")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof());
                f.home.transition(f.initial,ready); var token=f.client.executeSnapshotCapture(f.operation()); f.home.transition(ready,ready.capture(group,token));
                journal.refreshHome(); assertTrue(journal.snapshot().participant(group).captureLost());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=coordinator(journal,f)) {
                assertTrue(journal.snapshot().participant(group).captureLost()); assertEquals(RatisSnapshotCoordinator.Action.ABORTED,driver.step().action());
                assertTrue(driver.step().complete()); assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
        }
    }
    RatisSnapshotCoordinator proofOnlyDriver(RatisDurableSnapshot journal,RatisSnapshotCaptureRpcTest.Fixture f) throws Exception {
        return new RatisSnapshotCoordinator(journal,f.home.homes,f.client,last -> control(last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10));
    }
    @Test void forcedReceiptsReopenAndResolveProofWithoutWriteAuthority() throws Exception {
        Path path=root.resolve("receipt-proof.journal");
        try (var f=fixture("receipt-proof")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                journal.refreshHome(); journal.resolveHomeFromObserved(); journal.freezeInstall(group); f.loseGateProof=true;
                assertThrows(IOException.class,() -> journal.executeInstall(group));
                assertNotNull(journal.snapshot().participant(group).installReceipt()); assertNull(journal.snapshot().participant(group).nativeGate());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=proofOnlyDriver(journal,f)) {
                assertEquals(RatisSnapshotCoordinator.Action.INSTALLED,driver.step().action());
                transition(journal,f,journal.snapshot().observedHome().abort(),1); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) {
                    f.loseGateProof=true; assertThrows(IOException.class,() -> journal.executeRelease(group));
                }
                assertNotNull(journal.snapshot().participant(group).releaseReceipt()); assertTrue(journal.snapshot().participant(group).nativeGate().active());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client); var driver=proofOnlyDriver(journal,f)) {
                assertTrue(driver.step().complete()); assertFalse(journal.snapshot().participant(group).nativeGate().active());
            }
        }
    }
    @Test void lostCaptureAndReleaseRepliesReopenWithExactRequestsAndKeepHistoricalReads() throws Exception {
        Path path=root.resolve("complete.journal");
        try (var f=fixture("complete")) {
            var group=f.data.f.groupId; byte[] frozen;
            try (var journal=create(path,f)) {
                installed(journal,f); var proof=journal.snapshot().participant(group).readyProof();
                transition(journal,f,f.initial.ready(group,proof),1);
                var operation=f.operation(); frozen=operation.bytes(); journal.freezeCapture(operation);
                f.lose=true; assertThrows(IOException.class,() -> journal.executeCapture(group));
                assertNull(journal.snapshot().participant(group).captureToken()); assertEquals(1,f.data.f.machine.snapshotPinStats().pins());
            }
            SnapshotFenceStatus.Capture token; RatisRoutedClient.SnapshotTarget target;
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                var operation=journal.snapshot().participant(group).capture(); assertArrayEquals(frozen,operation.bytes()); target=operation.target();
                token=journal.executeCapture(group); var ready=journal.snapshot().observedHome();
                transition(journal,f,ready.capture(group,token),2); transition(journal,f,journal.snapshot().observedHome().complete(),3);
                assertTrue(f.data.f.store.snapshotGate(f.fence.fenceId()).active()); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) {
                    f.data.wire.lose=true; assertThrows(IOException.class,() -> journal.executeRelease(group));
                }
                assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                assertNotNull(journal.executeRelease(group)); assertEquals(f.data.f.store.snapshotGate(f.fence.fenceId()).released(),journal.snapshot().participant(group).releaseReceipt());
                assertEquals(token,journal.executeCapture(group));
                assertArrayEquals(new byte[]{7},f.client.snapshotGet(target,token,f.data.f.column,new byte[]{2}));
            }
        }
    }
    @Test void expiredCaptureCannotBeRecreatedOrReportedAndAbortReleasesTheGate() throws Exception {
        Path path=root.resolve("lost.journal");
        try (var f=fixture("lost")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); transition(journal,f,f.initial.ready(group,journal.snapshot().participant(group).readyProof()),1);
                journal.freezeCapture(f.operation()); var token=journal.executeCapture(group);
                f.clock.set(Duration.ofSeconds(10).toNanos()); f.data.f.machine.expireTransactions();
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group));
                assertTrue(journal.snapshot().participant(group).captureLost());
                var captured=journal.snapshot().observedHome().capture(group,token);
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),journal.snapshot().observedHome(),captured)));
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                assertThrows(RatisSnapshotLostException.class,() -> journal.executeCapture(group));
                transition(journal,f,journal.snapshot().observedHome().abort(),2); journal.freezeRelease(group);
                try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { journal.executeRelease(group); }
                assertFalse(f.data.f.store.snapshotGate(f.fence.fenceId()).active()); assertEquals(0,f.data.f.store.activeSnapshotPins());
            }
        }
    }
    @Test void homeProgressNeedsVerifiedEvidenceAndJournalHasExclusiveOwnership() throws Exception {
        Path path=root.resolve("evidence.journal");
        try (var f=fixture("evidence"); var journal=create(path,f)) {
            var group=f.data.f.groupId; journal.refreshHome(); journal.resolveHomeFromObserved();
            var fake=f.initial.ready(group,f.data.f.store.snapshotGate(f.fence.fenceId()).installed());
            assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(1),f.initial,fake)));
            assertThrows(IOException.class,() -> RatisDurableSnapshot.reopen(path,f.home.homes,f.client));
            assertThrows(IllegalStateException.class,() -> journal.freezeRelease(group));
            journal.freezeInstall(group); journal.executeInstall(group); assertTrue(journal.verifyReady(group)); transition(journal,f,fake,1);
            var token=new SnapshotFenceStatus.Capture(UUID.randomUUID(),UUID.randomUUID(),journal.snapshot().participant(group).readyProof());
            assertThrows(IllegalArgumentException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),fake,fake.capture(group,token))));
        }
    }
    @Test void lostHomeReplyIsResolvedFromFreshObservedProgressBeforeAnotherCas() throws Exception {
        Path path=root.resolve("home.journal");
        try (var f=fixture("home")) {
            var group=f.data.f.groupId;
            try (var journal=create(path,f)) {
                installed(journal,f); var ready=f.initial.ready(group,journal.snapshot().participant(group).readyProof());
                journal.queueHome(f.home.homes.prepare(control(1),f.initial,ready)); f.home.wire.lose=true;
                assertThrows(IOException.class,journal::executeHome); assertNull(journal.snapshot().homeResult());
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.client)) {
                var frozen=journal.snapshot().pendingHome();
                assertThrows(IllegalStateException.class,() -> journal.queueHome(f.home.homes.prepare(control(2),f.initial,f.initial.abort())));
                journal.refreshHome(); assertTrue(journal.resolveHomeFromObserved()); assertTrue(journal.snapshot().homeSuperseded());
                assertArrayEquals(DistributedCommandCodec.encode(frozen.command()),DistributedCommandCodec.encode(journal.snapshot().pendingHome().command()));
                transition(journal,f,journal.snapshot().observedHome().abort(),2);
            }
        }
    }
}
