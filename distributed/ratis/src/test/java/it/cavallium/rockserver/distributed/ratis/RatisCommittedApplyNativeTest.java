package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Two real native pools, controlled committed heads, no Raft server/network/quorum. */
class RatisCommittedApplyNativeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static void await(CountDownLatch latch){try{assertTrue(latch.await(5,TimeUnit.SECONDS));}catch(InterruptedException failed){throw new AssertionError(failed);}}
    private static TransactionContext transaction(RatisRangeStateMachine machine,long index,byte[] body){return TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(body))).build()).build();}
    @Test void canceledNativeHeadRetainsExecutionAndAnotherPoolProtectedHeadProgresses()throws Exception {
        UUID a=new UUID(10,1),b=new UUID(10,2);var limits=RatisCommittedApplyDispatcher.Policy.defaults().pool();
        try(var dispatcher=new RatisCommittedApplyDispatcher(RatisCommittedApplyDispatcher.Policy.defaults().node(),Map.of(a,limits,b,limits));
            var model=new RatisTopologyFreezeTest.Fixture(root.resolve("a"));
            var second=DistributedStoragePool.open(root.resolve("b"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            var workers=Executors.newVirtualThreadPerTaskExecutor()){
            var identity=model.machine.getServer().getNow(null);model.machine.close();model.queries.stop().get(5,TimeUnit.SECONDS);
            var machine=new RatisRangeStateMachine(model.store,model.group,new NativeRangeImage.Limits(4<<20,16<<20,1),command->{},command->{},model.leader::get);
            machine.role(RatisNodePlan.Role.DATA);machine.topologyAuthority((header,stage)->model.view,model.directory,model.local,model.pool);machine.committedApply(dispatcher,a,64L*1024*1024+4096);machine.initialize(identity,model.group.getGroupId(),model.storage);
            var otherId=new UUID(0,10);var other=second.openRange(new RangeOwnership.Owner(otherId,1),new RangeOwnership.Interval(model.directory.domainId(),Set.of(model.column),null,null),model.schema.digest(),Map.of(model.column,"data"),Set.of());
            var otherGroup=RaftGroup.valueOf(RaftGroupId.valueOf(otherId),model.peer);
            var otherStorage=RaftStorage.newBuilder().setDirectory(root.resolve("b-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();otherStorage.initialize();
            var otherMachine=new RatisRangeStateMachine(other,otherGroup,new NativeRangeImage.Limits(4<<20,16<<20,1));otherMachine.committedApply(dispatcher,b,64L*1024*1024+4096);otherMachine.initialize(identity,otherGroup.getGroupId(),otherStorage);otherMachine.notifyTermIndexUpdated(1,0);
            var pin=model.store.captureTracked(model.range());var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var headReady=new CountDownLatch(1);var pump=new CountDownLatch(1);
            try {
                // Establish USER data before taking the actual blocking tracked cut.
                pin.close();machine.applyTransaction(transaction(machine,1,DistributedCommandCodec.encode(model.put(0,9)))).get(5,TimeUnit.SECONDS);pin=model.store.captureTracked(model.range());var ownedPin=pin;
                var visitor=workers.submit(()->{ownedPin.visitColumn(model.column,new byte[]{1},new byte[]{2},(key,value)->{entered.countDown();await(release);});return null;});await(entered);
                var observer=new AtomicReference<CompletableFuture<Message>>();
                var updater=workers.submit(()->{var admission=machine.committedApplyAdmission();var tx=transaction(machine,2,model.record.encode());org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor description;
                    try(var read=admission.awaitRead(2)){description=admission.describe(tx.getLogEntry());}
                    var permit=admission.awaitDispatch(description);admission.offer(description,permit);try{observer.set(machine.applyTransaction(tx));}finally{admission.withdraw(description,permit);if(!permit.claimed())permit.close();}
                    assertTrue(((RatisCommittedApplyDispatcher.Group)admission).pumpNativeAction());headReady.countDown();await(pump);try(var read=admission.awaitRead(3)){}return null;});await(headReady);
                long held=dispatcher.stats().ordinaryBytes();assertTrue(held>0);assertTrue(observer.get().cancel(false));ownedPin.close();assertEquals(held,dispatcher.stats().ordinaryBytes());assertNull(model.store.lookupTopologyFreeze(model.record.header(),model.record.admissionDigest()));
                var protectedAdmission=otherMachine.committedApplyAdmission();var noop=LogEntryProto.newBuilder().setTerm(1).setIndex(1).setMetadataEntry(MetadataProto.newBuilder()).build();org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor descriptor;
                try(var read=protectedAdmission.awaitRead(1)){descriptor=protectedAdmission.describe(noop);}
                try(var completion=protectedAdmission.awaitDispatch(descriptor)){otherMachine.notifyTermIndexUpdated(1,1);}
                assertEquals(new ApplyPosition(1,1),other.applied());assertEquals(held,dispatcher.stats().ordinaryBytes());
                release.countDown();visitor.get(5,TimeUnit.SECONDS);assertNull(model.store.lookupTopologyFreeze(model.record.header(),model.record.admissionDigest()),"Ready callback must not perform JNI on visitor/CONTROL thread");pump.countDown();updater.get(5,TimeUnit.SECONDS);machine.topologyCleanup().get(5,TimeUnit.SECONDS);
                assertEquals(new ApplyPosition(1,2),model.store.lookupTopologyFreeze(model.record.header(),model.record.admissionDigest()).position());assertEquals(0,dispatcher.stats().ordinaryBytes());
            } finally {release.countDown();pump.countDown();pin.close();machine.close();machine.topologyCleanup().get(5,TimeUnit.SECONDS);machine.awaitNativeReaders(Duration.ofSeconds(5));otherMachine.close();otherMachine.awaitNativeReaders(Duration.ofSeconds(5));otherStorage.close();}
        }
    }
    private static java.util.function.UnaryOperator<DistributedStoragePool> blockInstalledReturn(CountDownLatch committed,CountDownLatch release){return pool->{
        var borrowed=org.mockito.Mockito.mock(DistributedStoragePool.class,org.mockito.AdditionalAnswers.delegatesTo(pool));
        try {org.mockito.Mockito.doAnswer(invocation->{var result=pool.installSchema(invocation.getArgument(0),invocation.getArgument(1),invocation.getArgument(2,DistributedStoragePool.SchemaInstallPlan.class));committed.countDown();await(release);return result;}).when(borrowed).installSchema(org.mockito.ArgumentMatchers.any(ReplicatedRangeStore.class),org.mockito.ArgumentMatchers.any(ApplyPosition.class),org.mockito.ArgumentMatchers.any(DistributedStoragePool.SchemaInstallPlan.class));}
        catch(org.rocksdb.RocksDBException failed){throw new AssertionError(failed);}return borrowed;
    };}
    private static Future<?> updater(ExecutorService workers,RatisRangeStateMachine machine,TransactionContext tx,AtomicReference<CompletableFuture<Message>> observer,CountDownLatch dispatched){return workers.submit(()->{
        var admission=machine.committedApplyAdmission();org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor descriptor;
        try(var read=admission.awaitRead(tx.getLogEntry().getIndex())){descriptor=admission.describe(tx.getLogEntry());}
        var permit=admission.awaitDispatch(descriptor);admission.offer(descriptor,permit);try{observer.set(machine.applyTransaction(tx));}finally{admission.withdraw(descriptor,permit);if(!permit.claimed())permit.close();}dispatched.countDown();
        for(;;){try(var read=admission.awaitRead(tx.getLogEntry().getIndex()+1)){return null;}catch(org.apache.ratis.server.impl.CommittedApplyStateMachine.Retry reload){/* Existing updater retries scalar read after reload. */}catch(org.apache.ratis.server.impl.CommittedApplyStateMachine.Stopped shutdown){return null;}}
    });}
    @Test void coveringSnapshotCompletesObserverWithoutRefundingBlockedActualNativeOwner()throws Exception {
        var poolId=new UUID(20,1);var limits=RatisCommittedApplyDispatcher.Policy.defaults();var committed=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits.node(),Map.of(poolId,limits.pool()));var workers=Executors.newVirtualThreadPerTaskExecutor();
            var donor=new RatisSchemaLifecycleAdmissionTest.Fixture(root.resolve("donor"));
            var receiver=new RatisSchemaLifecycleAdmissionTest.Fixture(root.resolve("receiver"),false,blockInstalledReturn(committed,release),machine->machine.committedApply(dispatcher,poolId,64L*1024*1024+4096))){
            var expected=donor.install(donor.installRecord(donor.create,donor.fence(donor.create)));assertEquals(expected.position().index(),donor.machine.takeSnapshot());
            var record=receiver.installRecord(receiver.create,receiver.fence(receiver.create));receiver.query(RatisSchemaInstallRpc.prepareRequest(record));var tx=receiver.propose(record.encode());assertNull(tx.getException());receiver.machine.preAppendTransaction(tx);tx.initLogEntry(1,++receiver.index);
            var observer=new AtomicReference<CompletableFuture<Message>>();var dispatched=new CountDownLatch(1);var actual=updater(workers,receiver.machine,tx,observer,dispatched);
            try {
                await(dispatched);await(committed);long held=dispatcher.stats().ordinaryBytes();assertTrue(held>0);assertFalse(observer.get().isDone());
                var source=donor.machine.getStateMachineStorage().getSnapshotFile(1,expected.position().index()).toPath();var destination=receiver.machine.getStateMachineStorage().getSnapshotFile(1,expected.position().index()).toPath();java.nio.file.Files.copy(source,destination);
                receiver.machine.pause();receiver.machine.reinitialize();assertEquals(expected,NativeSchemaInstallProof.decode(observer.get().get(5,TimeUnit.SECONDS).getContent().toByteArray()));
                assertEquals(held,dispatcher.stats().ordinaryBytes());assertThrows(java.io.IOException.class,()->dispatcher.awaitDrained(Duration.ofMillis(1)));assertThrows(IllegalStateException.class,()->{try(var ignored=receiver.store.captureForRecovery()){}},"Native gate remains owned through blocked actual return despite snapshot coverage");
            } finally {release.countDown();}
            actual.get(5,TimeUnit.SECONDS);dispatcher.awaitDrained(Duration.ofSeconds(5));assertEquals(0,dispatcher.stats().ordinaryBytes());assertEquals(expected,receiver.store.schemaInstallation().proof());
        }
    }
    @Test void shutdownFailsObserverButCannotRefundActualBlockedNativeReturn()throws Exception {
        var poolId=new UUID(20,2);var limits=RatisCommittedApplyDispatcher.Policy.defaults();var committed=new CountDownLatch(1);var release=new CountDownLatch(1);
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits.node(),Map.of(poolId,limits.pool()));var workers=Executors.newVirtualThreadPerTaskExecutor();
            var receiver=new RatisSchemaLifecycleAdmissionTest.Fixture(root.resolve("shutdown"),false,blockInstalledReturn(committed,release),machine->machine.committedApply(dispatcher,poolId,64L*1024*1024+4096))){
            var record=receiver.installRecord(receiver.create,receiver.fence(receiver.create));receiver.query(RatisSchemaInstallRpc.prepareRequest(record));var tx=receiver.propose(record.encode());assertNull(tx.getException());receiver.machine.preAppendTransaction(tx);tx.initLogEntry(1,++receiver.index);
            var observer=new AtomicReference<CompletableFuture<Message>>();var dispatched=new CountDownLatch(1);var actual=updater(workers,receiver.machine,tx,observer,dispatched);
            try {await(dispatched);await(committed);long held=dispatcher.stats().ordinaryBytes();assertTrue(held>0);receiver.machine.beginShutdown();assertThrows(ExecutionException.class,()->observer.get().get(5,TimeUnit.SECONDS));dispatcher.close();assertEquals(held,dispatcher.stats().ordinaryBytes());assertThrows(java.io.IOException.class,()->dispatcher.awaitDrained(Duration.ofMillis(1)));}
            finally {release.countDown();}
            actual.get(5,TimeUnit.SECONDS);dispatcher.awaitDrained(Duration.ofSeconds(5));assertEquals(0,dispatcher.stats().ordinaryBytes());assertNotNull(receiver.store.schemaInstallation());
        }
    }

    @Test void pauseAfterReadRejectsStaleHandoffAndBelowHeadSnapshotAllowsExactReplay()throws Exception {
        var poolId=new UUID(20,3);var limits=RatisCommittedApplyDispatcher.Policy.defaults();
        try(var dispatcher=new RatisCommittedApplyDispatcher(limits.node(),Map.of(poolId,limits.pool()));var fixture=new RatisSchemaLifecycleAdmissionTest.Fixture(root.resolve("handoff"),false,java.util.function.UnaryOperator.identity(),machine->machine.committedApply(dispatcher,poolId,64L*1024*1024+4096))){
            assertEquals(0,fixture.machine.takeSnapshot());var admission=fixture.machine.committedApplyAdmission();var original=DistributedCommandCodec.encode(fixture.put(fixture.source,fixture.column,0,9));var tx=transaction(fixture.machine,1,original);
            org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor stale;
            try(var read=admission.awaitRead(1)){stale=admission.describe(tx.getLogEntry());fixture.machine.pause();fixture.machine.reinitialize();assertThrows(org.apache.ratis.server.impl.CommittedApplyStateMachine.Retry.class,()->admission.tryDispatch(stale));}
            assertEquals(new ApplyPosition(1,0),fixture.store.applied());assertEquals(0,dispatcher.stats().ordinaryBytes());
            org.apache.ratis.server.impl.CommittedApplyStateMachine.Descriptor fresh;try(var read=admission.awaitRead(1)){fresh=admission.describe(tx.getLogEntry());}assertTrue(fresh.generation()>stale.generation());
            var permit=admission.awaitDispatch(fresh);admission.offer(fresh,permit);try{fixture.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS);}finally{admission.withdraw(fresh,permit);if(!permit.claimed())permit.close();}
            assertEquals(new ApplyPosition(1,1),fixture.store.applied());assertArrayEquals(new byte[]{9},fixture.store.readCurrentPoint(fixture.column,new byte[]{1}));assertEquals(0,dispatcher.stats().ordinaryBytes());
        }
    }

}
