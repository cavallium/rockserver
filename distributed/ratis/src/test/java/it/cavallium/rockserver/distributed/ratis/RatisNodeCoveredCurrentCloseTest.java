package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.LongConsumer;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Node close joins the actual covered updater owner; no sockets, fake native marker or quorum. */
class RatisNodeCoveredCurrentCloseTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void closeAtCoveredCursorAbortsBeforePublicationAndRetiresNativeOwners()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root,false)) {
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());var column=source.first.columnIds().iterator().next();
            var first=source.server.machines.get(id);var route=source.routes.get(directory.domainId());var catalogueBinding=source.plan.catalogue();
            var initialEpochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),catalogueBinding.groupId(),catalogueBinding.epoch(),catalogueBinding.schemaDigest(),catalogueBinding.sessionsColumn());
            ApplyPosition s,a;DistributedCommand applied,next;
            try(var window=RatisBackendSessions.window(root.resolve("covered-input"),route,initialEpochs,60000,new RatisSessionClient.Limits(4,1L<<20,1L<<20),List.of(new RatisBackendSessions.Target(route,range.groupId(),directory)),RatisJournalAccess.standalone())) {
                s=source.store(range.groupId()).applied();assertEquals(s.index(),first.takeSnapshot());
                var pending=window.prepare(List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{7},new byte[]{13})));assertEquals(CommandResult.Status.SUCCESS,window.execute(pending).status());applied=pending.command();a=source.store(range.groupId()).applied();assertEquals(s.index()+1,a.index());
                next=window.prepare(List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{7},new byte[]{14}))).command();
            }
            for(var entry:source.server.machines.entrySet())if(!entry.getKey().equals(id))assertTrue(entry.getValue().takeSnapshot()>=0);
            for(var entry:source.server.storages.entrySet())((org.apache.ratis.server.storage.RaftStorageImpl)entry.getValue()).writeBootstrapConfiguration(source.server.division(entry.getKey()).getRaftConf());
            var snapshotStorage=field(first,"snapshots",SimpleStateMachineStorage.class);var floor=snapshotStorage.getSnapshotFile(s.term(),s.index()).toPath();var unwanted=snapshotStorage.getSnapshotFile(a.term(),a.index()).toPath();byte[] original=Files.readAllBytes(floor);source.node.close();
            var borrowed=source.service.restoreResources();var c=source.plan.catalogue();var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            try(var node=RatisNodeRuntime.open(source.plan,root.resolve("node"),ConfigParser.parseDefault(),epochs,source.routes,borrowed.scheduler,borrowed.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.RECOVER,(plan,logs,machines)->{
                machines.get(id).deferRecoveryDiscovery();return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);
            },List.of(source.first,source.second))) {
                node.relocationTransport(target->{assertEquals(c.groupId(),target.groupId());return source.catalogue;},(target,owner)->{throw new AssertionError("Local CURRENT needs no source transfer");});node.start();
                var division=field(node,"server",RaftServer.class).getDivision(id);var machine=(RatisRangeStateMachine)division.getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);var resources=field(node,"resources",RatisNodeResourceBudget.class);var runway=field(node,"diskRunway",RatisDiskRunway.class);
                assertEquals(a,store.applied());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertEquals(s.index(),division.getRaftLog().getSnapshotIndex());
                var log=division.getRaftLog();for(var completion:log.append(List.of(data(a,applied),data(new ApplyPosition(a.term(),a.index()+1),next))))completion.get(5,TimeUnit.SECONDS);assertTrue(log.updateCommitIndex(a.index()+1,a.term(),false));
                var getState=division.getClass().getDeclaredMethod("getState");getState.setAccessible(true);var state=getState.invoke(division);var getUpdater=state.getClass().getDeclaredMethod("getStateMachineUpdater");getUpdater.setAccessible(true);var updater=getUpdater.invoke(state);
                var prior=field(updater,"appliedIndexConsumer",LongConsumer.class);var closing=new AtomicReference<CompletableFuture<Void>>();var callbackFailure=new AtomicReference<Throwable>();var pinsAtCursor=new AtomicInteger();var crossed=new CountDownLatch(1);
                field(updater,"appliedIndexConsumer",(LongConsumer)index->{
                    prior.accept(index);if(index!=a.index() || closing.get()!=null)return;
                    try {
                        pinsAtCursor.set(store.activeSnapshotPins());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());
                        closing.set(CompletableFuture.runAsync(()->{try{node.close();}catch(Exception failure){throw new CompletionException(failure);}}));
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(node.state()!=RatisNodeRuntime.State.STOPPING && System.nanoTime()<deadline)Thread.sleep(1);
                        assertEquals(RatisNodeRuntime.State.STOPPING,node.state());assertFalse(closing.get().isDone());
                    }catch(Throwable failure){callbackFailure.set(failure);}finally{crossed.countDown();}
                });
                try {
                    var observer=node.bootstrapDeferredRecovery();assertTrue(crossed.await(10,TimeUnit.SECONDS),"Actual covered cursor must reach native A");assertNull(callbackFailure.get());assertTrue(pinsAtCursor.get()>0);
                    assertNotNull(observer.handle((unused,failure)->failure).get(15,TimeUnit.SECONDS),"Node close must refuse final discovery publication");assertNotNull(closing.get());closing.get().get(15,TimeUnit.SECONDS);
                    assertEquals(RatisNodeRuntime.State.CLOSED,node.state());assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertEquals(a,store.applied(),"Committed tail must remain unapplied when Node close wins publication");
                    assertFalse(Files.exists(unwanted));assertArrayEquals(original,Files.readAllBytes(floor));assertEquals(0,store.activeSnapshotPins());assertEquals(0,resources.stats().protectedLane().bytes());assertEquals(0,runway.stats().owners());
                }finally{field(updater,"appliedIndexConsumer",prior);if(closing.get()!=null)closing.get().get(15,TimeUnit.SECONDS);}
            }
        }
    }
    private static LogEntryProto data(ApplyPosition cut,DistributedCommand command) {
        return LogEntryProto.newBuilder().setTerm(cut.term()).setIndex(cut.index()).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setClientId(ClientId.valueOf(command.operationId().sessionId()).toByteString()).setCallId(command.operationId().sequence()).setLogData(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build();
    }
}
