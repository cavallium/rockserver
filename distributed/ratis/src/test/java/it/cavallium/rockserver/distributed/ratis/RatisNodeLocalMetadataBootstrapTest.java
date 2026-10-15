package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Genuine native/CAT lineage and real socket-free divisions, without quorum or leader claims. */
class RatisNodeLocalMetadataBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void freshForcedGraphSettlesLocalMetadataThroughActualReloadAndColdRetry(boolean closeDuringReload)throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root,false)) {
            var consuming=source.split();var group=consuming.parents().getFirst().groupId();var id=RaftGroupId.valueOf(group);
            var cut=source.store(group).applied();assertTrue(source.store(group).topologyRetired());
            // The controlled source applies genuine committed records; persist their complete floors
            // before replacing its controlled division lifecycle with actual Ratis divisions.
            for(var entry:source.server.machines.entrySet())if(!entry.getKey().equals(id))assertTrue(entry.getValue().takeSnapshot()>=0);
            for(var entry:source.server.storages.entrySet())((org.apache.ratis.server.storage.RaftStorageImpl)entry.getValue()).writeBootstrapConfiguration(source.server.division(entry.getKey()).getRaftConf());
            source.node.close();
            var borrowed=source.service.restoreResources();var c=source.plan.catalogue();
            var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            for(int reopen=0;reopen<(closeDuringReload?1:2);reopen++) {
                try(var node=RatisNodeRuntime.open(source.plan,root.resolve("node"),ConfigParser.parseDefault(),epochs,source.routes,borrowed.scheduler,borrowed.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.RECOVER,(plan,logs,machines)->{
                    machines.get(id).deferRecoveryDiscovery();return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);
                },List.of(source.first,source.second))) {
                    node.relocationTransport(range->{assertEquals(c.groupId(),range.groupId());return source.catalogue;},(range,owner)->{throw new AssertionError("Local META bootstrap needs no DATA source");});
                    node.start();var server=field(node,"server",RaftServer.class);var machine=(RatisRangeStateMachine)server.getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);
                    var routes=new HashMap<UUID,byte[]>();source.routes.forEach((domain,route)->routes.put(domain,route.directory().payloadDigest()));
                    assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertTrue(machine.query(Message.EMPTY).isCompletedExceptionally());assertEquals(cut,store.applied());
                    if(closeDuringReload) {
                        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var prior=field(machine,"snapshotMetadataAdmission",NativeRangeImage.IoAdmission.class);
                        RatisTopologySeedRuntimeTest.field(machine,"snapshotMetadataAdmission",new NativeRangeImage.IoAdmission(){
                            public AutoCloseable reserveMetadata(int bytes)throws IOException{return prior.reserveMetadata(bytes);}
                            public void beforeBytes(int bytes)throws IOException {
                                if(Thread.currentThread().getName().contains("StateMachineUpdater")){entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Blocked local reload timed out");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}}
                                prior.beforeBytes(bytes);
                            }
                        });
                        CompletableFuture<Void> closing=null;
                        try {
                            var observer=node.bootstrapDeferredRecovery();assertTrue(entered.await(10,TimeUnit.SECONDS));assertTrue(observer.cancel(false));assertTrue(store.activeSnapshotPins()>0);
                            closing=CompletableFuture.runAsync(()->{try{node.close();}catch(IOException failure){throw new CompletionException(failure);}});
                            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);while(node.state()!=RatisNodeRuntime.State.STOPPING && System.nanoTime()<deadline)Thread.sleep(1);
                            assertEquals(RatisNodeRuntime.State.STOPPING,node.state());assertFalse(closing.isDone());assertTrue(machine.recoveryEligibilityGate().discoveryPending());release.countDown();closing.get(15,TimeUnit.SECONDS);
                            assertEquals(RatisNodeRuntime.State.CLOSED,node.state());assertTrue(machine.recoveryEligibilityGate().discoveryPending(),"Close must win final settlement after actual reload drains");
                        }finally{release.countDown();if(closing!=null)closing.get(15,TimeUnit.SECONDS);}
                        continue;
                    }
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);
                    var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();assertNotNull(graph);assertTrue(graph.activeRequests().get(group).metadataOnly());assertArrayEquals(consuming.digest(),graph.activeRequests().get(group).header().definitionDigest());
                    assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertEquals(cut,store.applied());assertEquals(cut.index(),server.getDivision(id).getRaftLog().getSnapshotIndex());assertEquals(cut.index(),machine.getLastAppliedTermIndex().getIndex());assertEquals(0,store.activeSnapshotPins());
                    var image=machine.getLatestSnapshot().getFiles().getFirst().getPath();assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(image,NativeRangeImage.IoAdmission.UNLIMITED));
                    assertTrue(machine.query(Message.EMPTY).isCompletedExceptionally(),"Retired META settlement grants no USER query");
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertEquals(0,store.activeSnapshotPins());
                    source.routes.forEach((domain,route)->assertArrayEquals(routes.get(domain),route.directory().payloadDigest()));assertFalse(field(node,"configuredRecoveryServiceStarted",Boolean.class));
                }
            }
        }
    }
}
