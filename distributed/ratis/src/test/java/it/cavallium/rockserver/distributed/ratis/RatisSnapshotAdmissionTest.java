package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled admission/native ownership. Does not establish quorum/network acceptance or complete snapshot coordination. */
class RatisSnapshotAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    final class Home implements AutoCloseable {
        final RatisRetentionAdmissionTest.Fixture f;
        final RatisParticipantPrepareTest.Wire wire;
        final RatisRoutedClient routed;
        final RatisSnapshotFences homes;
        long sequence;
        Home(String name) throws Exception {
            var validation=new AtomicReference<Consumer<DistributedCommand>>(command -> {});
            UUID column=UUID.randomUUID();
            var interval=new it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership.Interval(UUID.randomUUID(),Set.of(column),null,null);
            f=new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> validation.get().accept(command),System::nanoTime,interval,machine -> {
                machine.role(RatisNodePlan.Role.SNAPSHOT_HOME,(key,value) -> RatisSnapshotFences.validateRecord(helper.cluster,helper.database,machine.getGroupId().getUuid(),1,key,value));
                machine.snapshotHomeStorage(helper.cluster,helper.database,column);
                machine.snapshotHomeProofs((fence,group) -> {
                    var node=helper.nodes.get(group); if (node==null) throw new IOException("Unconfigured controlled snapshot DATA group");
                    return node.routed.snapshotFence(fence,group);
                });
            });
            var peer=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:12345",ReplicaDescriptor.Role.VOTER);
            var route=new RangeDescriptor(f.groupId,f.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
            var directory=new RangeDirectory(helper.cluster,helper.database,f.store.ownership().interval().domainId(),1,new byte[32],0,Set.of(f.column),0,List.of(route));
            wire=new RatisParticipantPrepareTest.Wire(f); routed=new RatisRoutedClient(directory,wire); homes=new RatisSnapshotFences(routed,f.column);
            validation.set(RatisSnapshotFences.validator(helper.cluster,helper.database,f.groupId,1,new byte[32],f.column));
        }
        void transition(SnapshotFenceStatus expected,SnapshotFenceStatus next) throws Exception {
            var operation=new OperationId(new UUID(0,2100),1,sequence++);
            assertEquals(CommandResult.Status.SUCCESS,homes.execute(homes.prepare(operation,expected,next)).status());
        }
        SnapshotFenceStatus register(SnapshotFence fence) throws Exception { var initial=SnapshotFenceStatus.begin(fence); transition(null,initial); return initial; }
        @Override public void close() throws Exception { routed.close(); f.close(); }
    }
    TransactionScope scope(RatisParticipantPrepareTest.Node data,int lower,int upper) { return helper.scope(data,lower,upper); }
    SnapshotFence fence(Home home,RatisParticipantPrepareTest.Node data,int id,TransactionScope scope) {
        return new SnapshotFence(helper.cluster,helper.database,data.directory.domainId(),new UUID(0,2200+id),home.f.groupId,1,new OperationId(new UUID(0,32000+id),1,0),data.directory.generation(),1,new byte[32],
                List.of(new SnapshotFence.Participant(data.f.groupId,data.f.groupId,1,new byte[32],new OperationId(new UUID(0,2300+id),1,0),new OperationId(new UUID(0,2300+id),1,1),scope)));
    }
    RatisSnapshotRecord.Result append(RatisParticipantPrepareTest.Node data,RatisRangeStateMachine.SnapshotProposal proposal) throws Exception {
        assertEquals(proposal.request(),RatisSnapshotRecord.decode(proposal.bytes()));
        return RatisSnapshotRecord.Result.decode(data.wire.append(proposal.bytes()));
    }
    void released(RatisParticipantPrepareTest.Node data) throws Exception {
        assertEquals(0,data.f.machine.snapshotPreparationSlots()); assertEquals(0,data.f.machine.reservedSnapshotGateCount()); assertEquals(0,data.f.machine.reservedSnapshotGateBytes());
        assertEquals(0,data.f.machine.proposalGateStats().gates()); assertEquals(0,data.f.counter("snapshotProposalBytes")); assertEquals(0,data.f.counter("snapshotInstallBytes")); assertEquals(0,data.f.counter("snapshotAdmittedBytes")); assertEquals(0,data.f.counter("snapshotInstallAdmittedBytes")); assertEquals(0,data.f.counter("snapshotAdmittedInstalls")); data.f.assertReleased();
    }
    @Test void installReservationBlocksNewConflictsButWaitsForExistingApplyOrCancellation() throws Exception {
        helper.root=root;
        for (boolean apply:new boolean[]{false,true}) try (var home=new Home("drain-home-"+apply); var data=helper.new Node("drain-data-"+apply,false)) {
            var fence=fence(home,data,0,scope(data,1,4)); var initial=home.register(fence); var old=data.f.propose(data.f.put(2)); assertNull(old.getException());
            try (var proposal=data.f.machine.prepareSnapshotInstall(home.homes,fence)) {
                assertFalse(proposal.drained()); assertEquals(1,data.f.machine.proposalGateStats().gates()); assertEquals(1,data.f.machine.reservedSnapshotGateCount());
                assertNotNull(data.f.propose(data.f.put(3)).getException());
                assertNotNull(data.wire.propose(proposal.bytes()).getException(),"Undrained encoded append must not consume or close its valid reservation");
                assertFalse(proposal.drained()); assertEquals(1,data.f.machine.reservedSnapshotGateCount());
                if (apply) { assertEquals(CommandResult.Status.SUCCESS,data.f.apply(old,++data.wire.index)); }
                else data.f.machine.cancelTransaction(old);
                assertTrue(proposal.drained()); var installed=append(data,proposal); assertNotNull(installed.position());
                assertTrue(data.f.machine.snapshotGateReady(fence)); assertNotNull(data.f.propose(data.f.put(3)).getException()); released(data);
                var ready=initial.ready(data.f.groupId,installed.position()); home.transition(initial,ready);
                try (var capture=data.f.machine.captureSnapshotGate(home.homes,fence)) { assertArrayEquals(apply ? new byte[]{7} : null,capture.get(data.f.column,new byte[]{2})); }
                var abort=ready.abort(); home.transition(ready,abort);
                try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) { assertNotNull(append(data,release).position()); }
                assertEquals(CommandResult.Status.SUCCESS,data.wire.command(data.directory.ranges().getFirst(),data.f.put(3)).status()); released(data);
            }
        }
    }
    @Test void freshHomeDefinitionAndTerminalDecisionAreRequiredAndUnsubmittedCloseReleasesOwnership() throws Exception {
        helper.root=root;
        try (var home=new Home("authority-home"); var data=helper.new Node("authority-data",false)) {
            var fence=fence(home,data,1,scope(data,1,4)); var initial=home.register(fence);
            var changed=new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),1,1,new byte[32],fence.participants());
            assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotInstall(home.homes,changed)); released(data);
            assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotRelease(home.homes,fence)); released(data);
            byte[] bytes;
            try (var proposal=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { assertTrue(proposal.drained()); bytes=proposal.bytes(); }
            released(data); assertNotNull(data.wire.propose(bytes).getException()); assertNull(data.f.store.snapshotGate(fence.fenceId()));
            assertEquals(CommandResult.Status.SUCCESS,data.wire.command(data.directory.ranges().getFirst(),data.f.put(2)).status());
            var abort=initial.abort(); home.transition(initial,abort);
            assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotInstall(home.homes,fence)); released(data);
            try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) { assertNotNull(append(data,release).position()); }
            assertNull(data.f.store.snapshotGate(fence.fenceId()).installed()); released(data);
        }
    }
    @Test void lostInstallReplyRetriesExactBytesAndCompleteReleaseLeavesCapturedCutReadable() throws Exception {
        helper.root=root;
        try (var home=new Home("capture-home"); var data=helper.new Node("capture-data",false,null,home.homes,System::nanoTime)) {
            var fence=fence(home,data,2,scope(data,1,4)); var initial=home.register(fence);
            RatisRoutedClient.PreparedSnapshotOperation operation;
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) {
                operation=data.routed.prepareSnapshotGate(install.request()); assertArrayEquals(install.bytes(),operation.bytes()); data.wire.lose=true;
                assertThrows(IOException.class,() -> data.routed.executeSnapshotGate(operation)); released(data);
            }
            var installed=data.routed.executeSnapshotGate(operation).position(); assertEquals(new ApplyPosition(1,1),installed); assertTrue(data.f.machine.snapshotGateReady(fence));
            assertThrows(IllegalStateException.class,() -> data.f.machine.captureSnapshotGate(home.homes,fence));
            var ready=initial.ready(data.f.groupId,installed); home.transition(initial,ready);
            try (var capture=data.f.machine.captureSnapshotGate(home.homes,fence)) {
                assertNull(capture.get(data.f.column,new byte[]{2}));
                var captured=ready.capture(data.f.groupId,new SnapshotFenceStatus.Capture(new UUID(0,2400),new UUID(0,2401),capture.position())); home.transition(ready,captured);
                var complete=captured.complete(); home.transition(captured,complete);
                assertThrows(IllegalStateException.class,() -> data.f.machine.captureSnapshotGate(home.homes,fence));
                try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) {
                    assertEquals(complete,release.request().terminal());
                    assertEquals(installed,data.routed.executeSnapshotGate(operation).position(),"A ready RELEASE cannot block an exact native INSTALL retry");
                    assertNotNull(append(data,release).position());
                }
                assertEquals(installed,data.routed.executeSnapshotGate(operation).position()); assertFalse(data.routed.snapshotGate(fence,data.f.groupId).active());
                assertEquals(CommandResult.Status.SUCCESS,data.wire.command(data.directory.ranges().getFirst(),data.f.put(2)).status());
                assertNull(capture.get(data.f.column,new byte[]{2}),"Native capture remains fixed after the gate is safely released");
            }
            released(data);
        }
    }
    @Test void captureWaitsForEveryHomeReadyProofAndRequiresTheExactLocalInstallPosition() throws Exception {
        helper.root=root;
        try (var home=new Home("all-ready-home"); var data=helper.new Node("all-ready-data",false)) {
            var base=fence(home,data,7,scope(data,1,4)); var other=new UUID(0,2700);
            var participants=List.of(base.participant(data.f.groupId),new SnapshotFence.Participant(other,other,1,new byte[32],new OperationId(other,1,0),new OperationId(other,1,1),scope(data,4,8)));
            var fence=new SnapshotFence(base.clusterId(),base.databaseId(),base.domainId(),base.fenceId(),base.homeGroup(),base.homeEpoch(),base.operation(),base.directoryGeneration(),base.schemaEpoch(),base.schemaDigest(),participants);
            var initial=home.register(fence); ApplyPosition installed;
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { installed=append(data,install).position(); }
            var local=initial.ready(data.f.groupId,installed); home.transition(initial,local);
            assertTrue(data.f.machine.snapshotGateReady(fence)); assertThrows(IllegalStateException.class,() -> data.f.machine.captureSnapshotGate(home.homes,fence));
            var ready=local.ready(other,new ApplyPosition(1,1)); home.transition(local,ready);
            try (var capture=data.f.machine.captureSnapshotGate(home.homes,fence)) { assertNull(capture.get(data.f.column,new byte[]{2})); }
            home.transition(ready,ready.abort()); try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) { append(data,release); } released(data);
            var mismatch=fence(home,data,8,scope(data,4,8)); var start=home.register(mismatch);
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,mismatch)) { installed=append(data,install).position(); }
            var incorrect=start.ready(data.f.groupId,new ApplyPosition(installed.term(),installed.index()+1)); home.transition(start,incorrect);
            assertThrows(IllegalStateException.class,() -> data.f.machine.captureSnapshotGate(home.homes,mismatch)); released(data);
        }
    }
    @Test void blockedHomeReadInvalidationRetainsPayloadIdentityAndCapacityUntilActualReturn() throws Exception {
        helper.root=root;
        try (var home=new Home("blocked-home"); var data=helper.new Node("blocked-data",false); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var fence=fence(home,data,3,scope(data,1,4)); home.register(fence); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var pending=executor.submit(() -> data.f.machine.prepareSnapshotInstall(home.homes,fence));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,data.f.machine.snapshotPreparationSlots()); assertEquals(1,data.f.machine.reservedSnapshotGateCount()); assertTrue(data.f.machine.reservedSnapshotGateBytes()>0);
                data.f.machine.notifyNotLeader(List.of()); assertFalse(pending.isDone());
                assertEquals(1,data.f.machine.snapshotPreparationSlots()); assertEquals(1,data.f.machine.reservedSnapshotGateCount()); assertTrue(data.f.counter("snapshotProposalBytes")>0);
                var stolen=new DistributedCommand(data.f.groupId,1,new byte[32],fence.participant(data.f.groupId).installOperation(),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{9},new byte[]{7})));
                assertNotNull(data.f.propose(stolen).getException(),"Invalidated planner still owns its original control identity until its home read returns");
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); released(data);
        }
    }
    @Test void unknownSubmittedInstallKeepsGateUntilCompletedNewerTermReconciliation() throws Exception {
        helper.root=root;
        try (var home=new Home("term-home"); var data=helper.new Node("term-data",false)) {
            var fence=fence(home,data,4,scope(data,1,4)); home.register(fence);
            try (var proposal=data.f.machine.prepareSnapshotInstall(home.homes,fence)) {
                var pending=data.wire.propose(proposal.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,1); data.f.machine.cancelTransaction(pending);
                data.f.machine.notifyNotLeader(List.of()); assertEquals(1,data.f.machine.reservedSnapshotGateCount()); assertEquals(1,data.f.machine.proposalGateStats().gates());
                assertNotNull(data.f.propose(data.f.put(2)).getException()); assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotInstall(home.homes,fence));
                data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true));
                assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotInstall(home.homes,fence)); assertEquals(1,data.f.machine.proposalGateStats().gates());
                data.f.machine.notifyTermIndexUpdated(2,1); data.wire.index=1;
                try (var fresh=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { assertTrue(fresh.drained()); assertEquals(new ApplyPosition(2,2),append(data,fresh).position()); }
                assertTrue(data.f.machine.snapshotGateReady(fence)); released(data);
            }
        }
    }
    @Test void terminalReleaseOvertakesUncertainInstallWithOneCapacityChargeAndSuppressesLateInstall() throws Exception {
        helper.root=root;
        try (var home=new Home("overtake-home"); var data=helper.new Node("overtake-data",false)) {
            for (int i=0;i<ReplicatedRangeStore.MAX_SNAPSHOT_GATES-1;i++) {
                var old=fence(home,data,100+i,scope(data,i,i+1)); var release=RatisSnapshotRecord.release(SnapshotFenceStatus.begin(old).abort(),data.f.groupId);
                data.f.machine.applyTransaction(RatisSnapshotRecordTest.committed(data.f.machine,1,++data.wire.index,release)).get(5,TimeUnit.SECONDS);
            }
            var fence=fence(home,data,5,scope(data,100,104)); var initial=home.register(fence);
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) {
                assertEquals(1,data.f.machine.reservedSnapshotGateCount()); long bytes=data.f.machine.reservedSnapshotGateBytes();
                var pending=data.wire.propose(install.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,data.wire.index+2); data.f.machine.cancelTransaction(pending);
                var abort=initial.abort(); home.transition(initial,abort);
                try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fence)) {
                    assertEquals(1,data.f.machine.reservedSnapshotGateCount()); assertEquals(bytes,data.f.machine.reservedSnapshotGateBytes()); assertNotNull(append(data,release).position());
                }
                var late=data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray(); data.wire.index++;
                assertTrue(RatisSnapshotRecord.Result.decode(late).suppressed()); assertNull(data.f.store.snapshotGate(fence.fenceId()).installed()); assertFalse(data.f.store.snapshotGate(fence.fenceId()).active()); released(data);
            }
        }
    }
    @Test void protectedReleaseProgressSurvivesFullOrdinaryAndInstallTaskQuotas() throws Exception {
        helper.root=root;
        try (var home=new Home("quota-home"); var data=helper.new Node("quota-data",false)) {
            var ordinary=new ArrayList<TransactionContext>(); var installs=new ArrayList<RatisRangeStateMachine.SnapshotProposal>();
            try {
                for (int i=0;i<1024;i++) { var pending=data.f.propose(data.f.put(i)); assertNull(pending.getException()); ordinary.add(pending); }
                assertNotNull(data.f.propose(data.f.put(1024)).getException());
                SnapshotFence first=null; SnapshotFenceStatus initial=null;
                for (int i=0;i<8;i++) {
                    var fence=fence(home,data,200+i,scope(data,i,i+1)); var registered=home.register(fence);
                    if (i==0) { first=fence; initial=registered; }
                    var proposal=data.f.machine.prepareSnapshotInstall(home.homes,fence); installs.add(proposal); assertFalse(proposal.drained());
                }
                var ninth=fence(home,data,208,scope(data,8,9)); home.register(ninth);
                assertThrows(IllegalStateException.class,() -> data.f.machine.prepareSnapshotInstall(home.homes,ninth));
                home.transition(initial,initial.abort());
                try (var release=data.f.machine.prepareSnapshotRelease(home.homes,first)) { assertNotNull(append(data,release).position()); }
                assertFalse(data.f.store.snapshotGate(first.fenceId()).active());
            } finally { for (var proposal:installs) proposal.close(); for (var pending:ordinary) data.f.machine.cancelTransaction(pending); }
            released(data);
        }
    }
    @Test void retainedInstallRetriesKeepTheirInputChargedAndLeaveReleaseHeadroom() throws Exception {
        helper.root=root;
        try (var home=new Home("retry-quota-home"); var data=helper.new Node("retry-quota-data",false)) {
            var fences=new ArrayList<SnapshotFence>(); var statuses=new ArrayList<SnapshotFenceStatus>(); var pending=new ArrayList<TransactionContext>();
            for (int i=0;i<9;i++) {
                var fence=fence(home,data,300+i,scope(data,i,i+1)); fences.add(fence); statuses.add(home.register(fence));
                try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { append(data,install); }
            }
            try {
                for (int i=0;i<8;i++) {
                    var retry=data.wire.propose(RatisSnapshotRecord.install(fences.get(i),data.f.groupId).encode()); assertNull(retry.getException()); pending.add(retry);
                }
                assertEquals(8,data.f.counter("snapshotAdmittedInstalls")); assertTrue(data.f.counter("snapshotAdmittedBytes")>0); assertTrue(data.f.counter("snapshotInstallAdmittedBytes")>0);
                assertNotNull(data.wire.propose(RatisSnapshotRecord.install(fences.get(8),data.f.groupId).encode()).getException());
                var initial=statuses.getFirst(); home.transition(initial,initial.abort());
                try (var release=data.f.machine.prepareSnapshotRelease(home.homes,fences.getFirst())) { append(data,release); }
                assertEquals(8,data.f.counter("snapshotAdmittedInstalls"),"Verified settlement does not donate still-owned input");
                assertFalse(data.f.store.snapshotGate(fences.getFirst().fenceId()).active());
            } finally { for (var retry:pending) data.f.machine.cancelTransaction(retry); }
            assertEquals(0,data.f.counter("snapshotAdmittedBytes")); assertEquals(0,data.f.counter("snapshotInstallAdmittedBytes")); released(data);
        }
    }
    @Test void existingPreparedParticipantBlocksReadyAndCaptureUntilItsDecisionDrains() throws Exception {
        helper.root=root;
        try (var home=new Home("blocker-home"); var txHome=helper.new Node("blocker-tx-home",true); var data=helper.new Node("blocker-data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=helper.register(txHome,data,pin,business,0);
            try (var prepare=data.f.machine.prepareParticipant(pin,business,helper.control(0),txHome.statuses,registration.transactionId())) {
                var vote=data.routed.executeParticipant(data.routed.prepareParticipant(prepare.preparation())); var voted=registration.prepared(data.f.groupId,prepare.preparation().business().payloadDigest(),vote.position());
                txHome.statuses.execute(txHome.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,voted));
                var commit=voted.decide(TransactionStatus.Phase.COMMIT); txHome.statuses.execute(txHome.statuses.prepare(new OperationId(new UUID(0,4),1,2),voted,commit));
                var fence=fence(home,data,6,scope(data,1,4)); var initial=home.register(fence); ApplyPosition installed;
                try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { assertTrue(install.drained()); installed=append(data,install).position(); }
                assertFalse(data.f.machine.snapshotGateReady(fence)); var ready=initial.ready(data.f.groupId,installed); home.transition(initial,ready);
                assertThrows(IllegalStateException.class,() -> data.f.machine.captureSnapshotGate(home.homes,fence));
                var recovery=new OperationId(new UUID(0,2600),2,0);
                try (var finalization=data.f.machine.prepareFinalization(txHome.statuses,registration.transactionId(),recovery)) { data.routed.executeFinalization(data.routed.prepareFinalization(finalization.finalization())); }
                assertTrue(data.f.machine.snapshotGateReady(fence));
                try (var capture=data.f.machine.captureSnapshotGate(home.homes,fence)) { assertArrayEquals(new byte[]{7},capture.get(data.f.column,new byte[]{2})); }
                released(data); helper.released(data);
            }
        }
    }
}
