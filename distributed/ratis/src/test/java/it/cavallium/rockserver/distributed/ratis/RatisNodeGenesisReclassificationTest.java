package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.lang.reflect.*;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisNodeGenesisBootstrapTest.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Real native/updater handoff with controlled fresh CAT placement; no quorum claims. */
class RatisNodeGenesisReclassificationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    enum Selection { NONE, CURRENT, UNQUALIFIED_CURRENT, META }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.EnumSource(Selection.class)
    void issuedChangedGraphRetiresGenesisBeforeSelectedRoleAndColdRediscovery(Selection selection)throws Exception {
        boolean current=selection==Selection.CURRENT || selection==Selection.UNQUALIFIED_CURRENT,unqualified=selection==Selection.UNQUALIFIED_CURRENT || selection==Selection.META;
        var schema=new RatisDatabaseBootstrapTest().schema();
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            var base=originalPlan(env);var plan=selection==Selection.META?new RatisNodePlan(base.nodeId(),base.nodeIncarnation(),base.domains(),base.catalogue(),base.homes(),List.of(),base.streamAddresses(),4,base.pools().values(),base.domainPools(),base.groupPools()):base;
            var directory=plan.domains().getFirst().directory();var range=directory.ranges().getFirst();var id=RaftGroupId.valueOf(range.groupId());
            var selected=new AtomicReference<RangeDirectory>();var consuming=new AtomicReference<TopologyChange.Definition>();var catalogue=placementCatalogue(catClient(env,plan,new AtomicInteger()),selected,consuming);
            try(var route=route(directory)) {
                var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                try(var node=openSchemaNode(env,plan,route,machines,catalogue,RatisNodeRuntime.Startup.AUTHORIZED_GENESIS,schema)) {
                    node.start();var machine=machines.get(id);var store=field(machine,"store",ReplicatedRangeStore.class);
                    node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);appendOriginalMetadata(plan,range,machine,store);
                    assertTrue(machine.genesisActive());assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS,machine.recoveryEligibilityGate().snapshot().mode());
                    var roster=DatabaseDomainRoster.initial(plan.clusterId(),plan.databaseId(),List.of(directory.domainId()));
                    try(var publish=new RatisCatalogueGenesis(root.resolve("publish"),env.catalogue.client,plan,directory.domainId(),10,env.catalogue.epochs.read(),null,roster)){assertEquals(CommandResult.Status.SUCCESS,publish.execute().status());}
                    byte[] digest=directory.schemaDigest();if(selection==Selection.UNQUALIFIED_CURRENT)digest[0]^=1;
                    var owners=current?range.replicas():range.replicas().stream().filter(owner->!owner.nodeId().equals(plan.nodeId())).toList();
                    var target=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),selection==Selection.CURRENT?0:1,null,null,RangeDescriptor.TopologyState.ACTIVE,owners);
                    selected.set(new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),selection==Selection.UNQUALIFIED_CURRENT?directory.schemaEpoch()+1:directory.schemaEpoch(),digest,1,directory.columns(),directory.routingPrefixBytes(),List.of(target)));
                    if(selection==Selection.META){
                        var children=new ArrayList<RangeDescriptor>();
                        for(int child=0;child<2;child++){var group=new UUID(12080,child+1);children.add(new RangeDescriptor(group,group,1,0,child==0?null:new byte[]{(byte)128},child==0?new byte[]{(byte)128}:null,RangeDescriptor.TopologyState.ACTIVE,range.replicas()));}
                        var targetDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),1,directory.columns(),directory.routingPrefixBytes(),children);
                        consuming.set(new TopologyChange.Definition(new UUID(12080,20),directory,targetDirectory));selected.set(targetDirectory);
                    }
                    classify(node,unqualified);assertRole(node,machine,store,selection);assertEquals(1,actualUpdaterIndex(machine.getServer().join().getDivision(id)));
                    // Repeated discovery cannot restore the creation exemption from the retained original plan.
                    classify(node,unqualified);assertRole(node,machine,store,selection);
                }
                var recovered=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                try(var node=openSchemaNode(env,plan,route,recovered,catalogue,RatisNodeRuntime.Startup.RECOVER,schema)) {
                    node.start();var machine=recovered.get(id);var store=field(machine,"store",ReplicatedRangeStore.class);
                    assertFalse(machine.genesisActive());assertTrue(machine.recoveryEligibilityGate().discoveryPending());
                    classify(node,unqualified);assertRole(node,machine,store,selection);
                }
            }
        }
    }
    @org.junit.jupiter.api.Test void refusedPublicationRetiresActualHoldBeforeOrdinaryCurrentRetry()throws Exception {
        var schema=new RatisDatabaseBootstrapTest().schema();
        try(var env=new RatisNodeRuntimeTest.Env(root,false,schema.digest())) {
            var plan=originalPlan(env);var directory=plan.domains().getFirst().directory();var range=directory.ranges().getFirst();var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
            try(var route=route(directory);var node=openSchemaNode(env,plan,route,machines,catClient(env,plan,new AtomicInteger()),RatisNodeRuntime.Startup.AUTHORIZED_GENESIS,schema)) {
                node.start();var machine=machines.get(RaftGroupId.valueOf(range.groupId()));var store=field(machine,"store",ReplicatedRangeStore.class);
                node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);appendOriginalMetadata(plan,range,machine,store);
                var roster=DatabaseDomainRoster.initial(plan.clusterId(),plan.databaseId(),List.of(directory.domainId()));
                try(var publish=new RatisCatalogueGenesis(root.resolve("publish"),env.catalogue.client,plan,directory.domainId(),10,env.catalogue.epochs.read(),null,roster)){assertEquals(CommandResult.Status.SUCCESS,publish.execute().status());}
                node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS_SUSPENDED,machine.recoveryEligibilityGate().snapshot().mode());
                var failure=assertThrows(java.io.IOException.class,()->machine.abandonGenesis(publication->{throw new java.io.IOException("Node close rejected publication");}));
                assertEquals("Node close rejected publication",failure.getMessage());assertFalse(machine.genesisActive());assertTrue(machine.localDiscoveryIdle());
                assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertEquals(new ApplyPosition(1,1),store.applied());
                node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);
                assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertEquals(RecoveryEligibilityStateMachine.Mode.RECOVERY,machine.recoveryEligibilityGate().snapshot().mode());
                assertTrue(store.admissionOpen());assertFalse(machine.genesisActive());assertEquals(new ApplyPosition(1,1),store.applied());assertEquals(0,store.activeSnapshotPins());
            }
        }
    }
    static void classify(RatisNodeRuntime node,boolean unqualified)throws Exception {
        if(unqualified)assertThrows(ExecutionException.class,()->node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS));
        else node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);
    }
    static void assertRole(RatisNodeRuntime node,RatisRangeStateMachine machine,ReplicatedRangeStore store,Selection selection)throws Exception {
        if(selection==Selection.CURRENT){
            assertFalse(machine.genesisActive());assertTrue(machine.localDiscoveryIdle());assertFalse(machine.recoveryEligibilityGate().discoveryPending());
            assertEquals(RecoveryEligibilityStateMachine.Mode.RECOVERY,machine.recoveryEligibilityGate().snapshot().mode());assertTrue(store.admissionOpen());
            assertEquals(new ApplyPosition(1,1),store.applied());assertEquals(0,store.activeSnapshotPins());assertNotNull(machine.getLatestSnapshot());
            assertEquals(1,machine.getServer().join().getDivision(machine.getGroupId()).getRaftLog().getSnapshotIndex());
            assertNull(store.readCurrentPoint(store.ownership().interval().columns().iterator().next(),new byte[]{1}));return;
        }
        boolean current=selection!=Selection.NONE;
        assertFalse(machine.genesisActive());assertTrue(machine.localDiscoveryIdle());assertTrue(machine.recoveryEligibilityGate().discoveryPending());
        assertEquals(RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());
        assertFalse(store.admissionOpen());assertEquals(new ApplyPosition(1,1),store.applied());assertNull(machine.getLatestSnapshot());
        assertEquals(-1,machine.getServer().join().getDivision(machine.getGroupId()).getRaftLog().getSnapshotIndex());assertEquals(0,store.activeSnapshotPins());
        assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
        assertThrows(ExecutionException.class,()->machine.queryLinearizable(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))).get(5,TimeUnit.SECONDS));
        var status=node.readiness().groups().stream().filter(group->group.group().equals(machine.getGroupId().getUuid())).findFirst().orElseThrow();
        assertEquals(current?RatisNodeReadiness.Discovery.PENDING:RatisNodeReadiness.Discovery.NONE_AT_CUT,status.discovery());
        assertFalse(status.servingReady());assertEquals(!current,status.recoverySettled());
        var saved=field(node,"nodeIdentity",RatisNodeIdentity.class);assertFalse(saved.genesisOpen(store.ownership().interval().domainId()));
        if(selection==Selection.META)assertEquals(RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot().activeRequests().get(machine.getGroupId().getUuid()).purpose());
    }
    static RatisNodeRuntime openSchemaNode(RatisNodeRuntimeTest.Env env,RatisNodePlan plan,RatisRoutedClient route,Map<RaftGroupId,RatisRangeStateMachine> machines,RaftClient catalogue,RatisNodeRuntime.Startup startup,DistributedSchema schema)throws java.io.IOException {
        var node=RatisNodeRuntime.open(plan,env.nodeRoot,it.cavallium.rockserver.core.config.ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(route.directory().domainId(),route),env.scheduler,env.timer,()->20L,settings(env),startup,
                (configuration,logs,created)->{machines.putAll(created);return RatisLocalMetadataDiscoveryTest.noSocketServer(configuration,logs,created);},List.of(schema));
        node.relocationTransport(range->catalogue,(range,owner)->{throw new AssertionError("No remote capture during local reclassification");});return node;
    }
    static RaftClient placementCatalogue(RaftClient delegate,AtomicReference<RangeDirectory> selected,AtomicReference<TopologyChange.Definition> consuming) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            final Object result;try{result=method.invoke(delegate.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}
            if(!method.getName().equals("sendReadOnly") || selected.get()==null)return result;
            var query=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES);var reply=(RaftClientReply)result;byte[] wire;
            if(kind==RatisCatalogueViewRpc.KIND){var request=RatisCatalogueViewRpc.request(query);var view=RatisCatalogueViewRpc.reply(reply.getMessage().getContent().toByteArray(),request);wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,view.cut(),selected.get(),view.roster()));}
            else if(kind==RatisRecoveryLineageRpc.KIND && consuming.get()!=null){var request=RatisRecoveryLineageRpc.request(query);var definition=consuming.get();if(definition.targets().stream().noneMatch(range->range.groupId().equals(request.group())))return result;var birth=RatisRecoveryLineageRpc.reply(reply.getMessage().getContent().toByteArray(),request);wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,birth.catalogueCut(),definition));}
            else return result;
            return new RaftClientReply.Builder().setClientId(reply.getClientId()).setServerId(reply.getServerId()).setGroupId(reply.getRaftGroupId()).setCallId(reply.getCallId()).setSuccess(true).setMessage(message(wire)).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->delegate.getGroupId();case "io"->io;case "close"->null;default->throw new AssertionError(method);});
    }
}
