package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Real native source and receiver pools; actual CAT queries with controlled division transport. */
class RatisNodeRecoveryRegistrationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void registersMetadataThenAbsentBornCurrentBeforeExposureAndColdRetriesExactRecipe()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var split=source.split();var parent=split.parents().getFirst();var leaf=split.targets().getFirst();var catalogue=source.plan.catalogue();
            var helper=new RatisNodeRecoverySourceTest();var metaRequest=new RatisTopologySourceSnapshotRpc.Request(new UUID(990,1),split.header(),catalogue.groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory());var metadata=helper.prepare(source,metaRequest);
            var currentRequest=new RatisTopologySourceSnapshotRpc.Request(new UUID(990,2),null,catalogue.groupId(),leaf.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,leaf.groupId(),split.targetDirectory());var current=helper.prepare(source,currentRequest);
            try(var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
                var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"receiver-data"),Set.of());var pool=RatisPoolFixtures.ID;
                var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
                var route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER recovery admission");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER recovery admission");}});
                var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
                for(int round=0;round<2;round++) {
                    var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,server::create,List.of(source.first))) {
                        node.relocationTransport(range->sourceClient(source,range), (range,owner)->sourceClient(source,range));node.start();
                        if(round==0){var budget=field(node,"resources",RatisNodeResourceBudget.class);long available=plan.pools().get(pool).resources().protectedLane().bytes()-budget.stats(pool).protectedLane().bytes();try(var denied=budget.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,available,1)){assertThrows(IOException.class,()->node.registerRecovery(metadata));assertTrue(node.nativePool(pool).metadataRecoveries().isEmpty());}}
                        if(round==0){var originalMachine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));var pending=field(originalMachine,"pendingNoops",NavigableMap.class);pending.put(0L,1L);try{assertThrows(IOException.class,()->node.registerRecovery(metadata));assertEquals(org.apache.ratis.util.LifeCycle.State.RUNNING,originalMachine.getLifeCycleState(),"Busy registration must not suspend its original work");assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.NORMAL,originalMachine.recoveryEligibilityGate().snapshot().mode());assertTrue(node.nativePool(pool).metadataRecoveries().isEmpty());}finally{pending.clear();}}
                        ReplicatedRangeStore metadataStore;
                        if(round==0){var budget=field(node,"resources",RatisNodeResourceBudget.class);long available=plan.pools().get(pool).resources().protectedLane().bytes()-budget.stats(pool).protectedLane().bytes();try(var occupied=budget.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,available-(32L<<20),1)){metadataStore=node.registerRecovery(metadata);}}else metadataStore=node.registerRecovery(metadata);
                        assertNotNull(metadataStore.metadataRecoveryRegistration());assertTrue(metadataStore.metadataRecoveryPending());assertFalse(metadataStore.admissionOpen());assertMissing(server,parent.groupId());assertThrows(IllegalStateException.class,metadataStore::capture);
                        ApplyPosition frozenCut=null;
                        if(round==0) {
                            var once=new java.util.concurrent.atomic.AtomicBoolean(true);node.nativePool(pool).currentRecoveryForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Controlled lost forced CURRENT acknowledgement");});
                            assertThrows(IOException.class,()->node.registerRecovery(current));var attempt=field(node,"recoveryAttempt",Object.class);frozenCut=field(attempt,"current",NativeCurrentRangeRecovery.class).catalogueCut();assertThrows(IOException.class,()->node.registerRecovery(imageHash(current)));
                            var ledger=epochs.read();assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(new OperationId(new UUID(990,10),0,0),ledger,ledger.advanceTime(ledger.logicalTimeMillis()+1))).status());
                            assertTrue(epochs.catalogue(catalogue.directoryColumn()).readView(source.first.domainId()).cut().index()>frozenCut.index());node.nativePool(pool).currentRecoveryForceObserver(after->{});
                            server.rejectAdd=true;assertThrows(IOException.class,()->node.registerRecovery(current));assertEquals(1,node.nativePool(pool).currentRangeRecoveries().size());assertMissing(server,leaf.groupId());
                            var changed=imageHash(current);assertThrows(IOException.class,()->node.registerRecovery(changed));server.rejectAdd=false;
                        }
                        var store=node.registerRecovery(current);if(frozenCut!=null)assertEquals(frozenCut,store.currentRangeRecoveryRegistration().catalogueCut(),"Exact unknown-force retry must not rebase onto the newer CAT cut");assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());assertNull(store.topologyBirth());assertNull(store.topologyActivation());assertTrue(node.nativePool(pool).topologySeeds().isEmpty());assertMissing(server,leaf.groupId());assertNotNull(field(server.machines.get(RaftGroupId.valueOf(leaf.groupId())),"sourceSnapshots",RatisTopologySourceSnapshotService.class));
                        assertTrue(Files.isRegularFile(env.nodeRoot.resolve("topology-exposure/registered").resolve(leaf.groupId().toString())));assertSame(store,node.registerRecovery(current));assertEquals(2,node.localGroups().size());
                        var wrongRequest=new RatisTopologySourceSnapshotRpc.Request(new UUID(990,3),null,new UUID(990,4),currentRequest.receiver(),currentRequest.purpose(),leaf.groupId(),currentRequest.sourceDirectory());var wrong=new RatisTopologySourceSnapshotRpc.Prepared(wrongRequest,current.image(),current.schema(),current.sourceOwner(),current.sourceCfg(),current.schemaRows(),null);assertThrows(IOException.class,()->node.registerRecovery(wrong));assertEquals(1,node.nativePool(pool).currentRangeRecoveries().size());
                    }
                }
                route.close();
            }
        }
    }
    private static void assertMissing(RatisNodeRuntimeTest.FakeServer server,UUID group){var machine=server.machines.get(RaftGroupId.valueOf(group));assertNotNull(machine);assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());}
    private static RatisTopologySourceSnapshotRpc.Prepared imageHash(RatisTopologySourceSnapshotRpc.Prepared prepared){var image=prepared.image();var hash=image.sha256();hash[0]^=1;var changed=new NativeRangeImage.Manifest(image.owner(),image.interval(),image.schemaDigest(),image.position(),image.deterministicMergeColumns(),image.rows(),image.bytes(),hash,image.topologyOrigin());return new RatisTopologySourceSnapshotRpc.Prepared(prepared.request(),changed,prepared.schema(),prepared.sourceOwner(),prepared.sourceCfg(),prepared.schemaRows(),prepared.metadataFreeze());}
    private static RaftClient sourceClient(RatisTwoPoolWorkflowFixture source,RangeDescriptor range) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));var result=machine.queryLinearizable((Message)args[0]).get(10,TimeUnit.SECONDS);return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
