package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.conf.*;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.impl.RockserverRaftServers;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Genuine native provenance and single-voter socket-free Raft; no remote endpoint/quorum acceptance claim. */
class RatisLocalMetadataCompletionWorkflowTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private long operation;
    private final AtomicLong calls=new AtomicLong();
    private final AtomicReference<RaftServer> actual=new AtomicReference<>();
    private final AtomicBoolean self=new AtomicBoolean(),losePrepare=new AtomicBoolean(true);
    private final AtomicInteger metaPrepares=new AtomicInteger(),obsoleteCalls=new AtomicInteger();

    @Test void completedCurrentSourceCleansItsOldImageThenFinishesLocalMetaAndColdReopensTwice()throws Exception {
        try(var donor=new RatisTwoPoolWorkflowFixture(root.resolve("donor"),false)) {
            var directory=donor.firstDirectory;var parent=directory.ranges().getFirst();var group=parent.groupId();var id=RaftGroupId.valueOf(group);var c=donor.plan.catalogue();
            var original=new Request(new UUID(13410,1),null,c.groupId(),donor.local,Purpose.CURRENT_LEAF,group,directory);
            var prepared=new RatisNodeRecoverySourceTest().prepare(donor,original);
            var donorOwner=field(donor.node,"pools",Map.class).get(donor.fast);var donorArtifacts=field(donorOwner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);var image=donorArtifacts.lookup(original).image();
            var remote=new RangeDescriptor(parent.rangeId(),group,parent.rangeEpoch(),parent.configurationVersion(),parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),List.of(c.replicas().getFirst()));
            var bootstrap=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation()-1,directory.columns(),directory.routingPrefixBytes(),List.of(remote));
            var domain=new RatisNodePlan.Domain(bootstrap,Map.of(donor.first.columnIds().iterator().next(),"data"),Set.of());
            var homeId=new UUID(13410,2);var homeColumn=new UUID(13410,3);var homeOwner=new ReplicaDescriptor(donor.local.nodeId(),donor.local.nodeIncarnation(),new UUID(13410,4),donor.local.peerId(),donor.local.address(),ReplicaDescriptor.Role.VOTER);
            var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeOwner));
            var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
            var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"local-meta-home");
            var plan=new RatisNodePlan(donor.local.nodeId(),donor.local.nodeIncarnation(),List.of(domain),c,List.of(home),List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(c,List.of(home)));
            var nodeRoot=Files.createDirectory(root.resolve("node"));SessionJournalFiles.force(nodeRoot);SessionJournalFiles.force(root);
            var epochs=new RatisSessionEpochs(donor.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            try(var route=new RatisRoutedClient(bootstrap,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID column,byte[] key){throw new AssertionError("No routed USER read");}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError("No routed USER write");}})) {
                // Initial adoption uses controlled division delivery, real native image/receipts,
                // and the actual SM GROUP writer. Every transition below uses the real cold Core.
                var initial=new RatisNodeRuntimeTest.FakeServer();initial.skipFirstNoop=true;
                try(var node=open(donor,plan,nodeRoot,epochs,route,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,initial::create)) {
                    var store=node.registerRecovery(prepared);store.installSnapshot(image,donor.manifest.settings().images());store.completeCurrentRangeRecovery(store.currentRangeRecoveryRegistration(),store.applied(),store.configuration());
                    var machine=initial.machines.get(id);machine.notifyTermIndexUpdated(store.applied().term(),store.applied().index());
                    await(()->machine.getLastAppliedTermIndex().getIndex()==prepared.image().position().index(),"Initial genuine native prefix callback");
                    assertEquals(store.applied().index(),machine.takeSnapshot());assertTrue(store.currentRangeRecoveryReady());
                    var initialStorage=(org.apache.ratis.server.storage.RaftStorageImpl)initial.storages.get(id);
                    initialStorage.writeBootstrapConfiguration(initial.division(id).getRaftConf());
                    initialStorage.getMetadataFile().persist(org.apache.ratis.server.storage.RaftStorageMetadata.valueOf(prepared.image().position().term(),null));
                }
                self.set(true);var originalFakeMachines=donor.server.machines;donor.server.machines=new HashMap<>(originalFakeMachines);
                RatisNodeRuntime node=null;Throwable primary=null;
                try {
                    node=open(donor,plan,nodeRoot,epochs,route,RatisNodeRuntime.Startup.RECOVER,this::electedServer);
                    var server=actual.get();var machine=(RatisRangeStateMachine)server.getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);
                    bootstrap(node);awaitLeader(server,id,store);assertTrue(store.currentRangeRecoveryReady());assertArrayEquals(new byte[]{11},store.readCurrentPoint(donor.first.columnIds().iterator().next(),new byte[]{7}));
                    node.discoverConfiguredRecovery().get(20,TimeUnit.SECONDS);var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();requireRequest(original,graph.activeRequests().get(group));
                    node.recoverConfigured(original).get(20,TimeUnit.SECONDS);var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);
                    var lost=assertThrows(ExecutionException.class,()->manager.step().get(20,TimeUnit.SECONDS));if(losePrepare.get())throw new AssertionError("PREPARE did not reach controlled ACK loss",lost);
                    var owner=field(node,"pools",Map.class).get(RatisPoolFixtures.ID);var sources=field(owner,"sources",RatisTopologySourceSnapshotService.class);var artifacts=field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);
                    assertEquals(1,sources.stats().leases());assertNotNull(artifacts.lookup(original));assertEquals(0,store.activeSnapshotPins());
                    // The actual CAT owner-proof transport now addresses this actual Node's SMs.
                    donor.server.machines.put(id,machine);
                    var consuming=beginSplit(donor,directory,parent);var authority=new RatisTopologyAuthority.Client(donor.catalogue,directory.clusterId(),directory.databaseId(),c.groupId());
                    var frozenAuthority=authority.read(consuming.header(),TopologyChange.Stage.FREEZE);var freezeRecord=new RatisTopologyFreezeRecord(consuming,group,frozenAuthority.admissionDigest());
                    requireSuccess(submit(server,group,RatisTopologyFreezeRpc.prepareRequest(freezeRecord),false));
                    var freeze=NativeTopologyFreeze.decode(requireSuccess(submit(server,group,freezeRecord.encode(),true)).getMessage().getContent().toByteArray());
                    assertEquals(freeze.position(),store.applied());assertTrue(RockserverRaftServers.hasAppliedPrefix(server.getDivision(id),machine,TermIndex.valueOf(freeze.position().term(),freeze.position().index())));
                    var topology=topology(donor);command(donor,topology,topology.prepareProof(op(),consuming,topology.readHead(directory.domainId()),freeze.proof(donor.local.replicaId())));
                    var seed=node.seedTopology(new RatisTopologySeedRpc.Request(consuming,c.groupId(),group,donor.local.replicaId()));
                    for(var child:consuming.targets()){
                        var childId=RaftGroupId.valueOf(child.groupId());var childMachine=(RatisRangeStateMachine)server.getDivision(childId).getStateMachine();
                        awaitLeader(server,childId,field(childMachine,"store",ReplicatedRangeStore.class));donor.server.machines.put(childId,childMachine);
                    }
                    for(var child:consuming.targets())command(donor,topology,topology.prepareProof(op(),consuming,topology.readHead(directory.domainId()),seed.proof(child.groupId())));
                    command(donor,topology,topology.preparePublication(op(),consuming,topology.readHead(directory.domainId()),null,null));
                    assertTrue(store.topologyRetired());assertNotNull(node.nativePool(RatisPoolFixtures.ID).topologySeed(consuming.intentId()));assertNotNull(store.currentRangeRecoveryRegistration());assertTrue(store.currentRangeRecoveryReady());
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);var slot=field(manager,"groups",Map.class).get(group);var journal=field(slot,"directory",Path.class).resolve("recovery");
                    assertEquals("FENCE_RECEIVER",phase(journal));stepUntil(manager,journal,"CLEANUP_OLD");assertNotNull(store.acknowledgedRecoverySupersession());assertEquals(0,metaPrepares.get());
                    stepUntil(manager,journal,"PREPARE");assertTrue(obsoleteCalls.get()>0);assertEquals(0,sources.stats().leases());assertEquals(0,store.activeSnapshotPins());assertTrue(RatisSchemaClientJournal.read(journal).containsKey("cleanupScope"));
                    stepUntil(manager,journal,"LOCAL_DONE");assertEquals(0,metaPrepares.get(),"Local seed must not dispatch a remote META PREPARE");assertLocal(node,group,consuming);
                    var terminal=RatisSchemaClientJournal.read(journal);assertFalse(terminal.containsKey("prepared"));assertFalse(terminal.containsKey("registration"));assertTrue(terminal.containsKey("localCompletion"));
                    requireExactLocalReceiptHash(machine,terminal);
                    byte[] witness=terminal.get("cleanupScope");node.close();node=null;
                    for(int reopen=0;reopen<2;reopen++) {
                        node=open(donor,plan,nodeRoot,epochs,route,RatisNodeRuntime.Startup.RECOVER,this::electedServer);
                        var currentNode=node;bootstrap(currentNode);assertLocal(currentNode,group,consuming);assertEquals("LOCAL_DONE",phase(journal));assertArrayEquals(witness,RatisSchemaClientJournal.read(journal).get("cleanupScope"));
                        currentNode.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertLocal(currentNode,group,consuming);assertEquals(0,metaPrepares.get());node.close();node=null;
                    }
                }catch(Exception|Error failure){primary=failure;throw failure;}
                finally{
                    donor.server.machines=originalFakeMachines;
                    if(node!=null)try{node.close();}catch(Exception|Error cleanup){if(primary!=null)primary.addSuppressed(cleanup);else throw cleanup;}
                }
            }
        }
    }

    private RatisNodeRuntime open(RatisTwoPoolWorkflowFixture donor,RatisNodePlan plan,Path root,RatisSessionEpochs epochs,RatisRoutedClient route,RatisNodeRuntime.Startup startup,RatisNodeRuntime.ServerFactory factory)throws Exception {
        var borrowed=donor.service.restoreResources();var node=RatisNodeRuntime.open(plan,root,ConfigParser.parseDefault(),epochs,Map.of(donor.first.domainId(),route),borrowed.scheduler,borrowed.timer,()->20L,donor.manifest.settings(),startup,factory,List.of(donor.first));
        try{node.relocationTransport(range->client(donor,range),(range,peer)->client(donor,range));node.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);return node;}catch(Throwable failure){try{node.close();}catch(Throwable cleanup){failure.addSuppressed(cleanup);}throw failure;}
    }
    private RaftServer electedServer(RatisNodePlan plan,Path logs,Map<RaftGroupId,RatisRangeStateMachine> machines)throws IOException {
        var properties=RatisDurability.properties(logs);RaftConfigKeys.Rpc.setType(properties,new RatisLocalMetadataDiscoveryTest.NoSocketRpcType());RaftConfigKeys.DataStream.setType(properties,SupportedDataStreamType.DISABLED);
        var min=TimeDuration.valueOf(100,TimeUnit.MILLISECONDS);var max=TimeDuration.valueOf(200,TimeUnit.MILLISECONDS);RaftServerConfigKeys.Rpc.setTimeoutMin(properties,min);RaftServerConfigKeys.Rpc.setTimeoutMax(properties,max);RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties,min);RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties,max);RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties,TimeDuration.ZERO);
        var server=RockserverRaftServers.create(RaftPeerId.valueOf(plan.peerId()),plan.localGroups().getFirst(),RaftStorage.StartupOption.RECOVER,machines::get,properties,new Parameters());actual.set(server);return server;
    }
    private RaftClient client(RatisTwoPoolWorkflowFixture donor,RangeDescriptor range) {
        var fallback=RatisPendingCurrentMetadataRegistrationTest.client(donor,range);
        var io=(BlockingApi)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(!self.get() || range.groupId().equals(donor.plan.catalogue().groupId()))try{return method.invoke(fallback.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}
            byte[] query=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES);
            if(kind==PREPARE && RatisTopologySourceSnapshotRpc.request(query).metadataOnly())metaPrepares.incrementAndGet();if(kind==RatisRecoveryObsoleteRpc.KIND)obsoleteCalls.incrementAndGet();
            var reply=submit(actual.get(),range.groupId(),query,false,method.getName().equals("sendStaleRead"));
            if(kind==PREPARE && !RatisTopologySourceSnapshotRpc.request(query).metadataOnly() && losePrepare.compareAndSet(true,false))throw new IOException("Lost actual local source PREPARE acknowledgement");return reply;
        });return (RaftClient)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
    private RaftClientReply submit(RaftServer server,UUID group,byte[] bytes,boolean write)throws Exception{return submit(server,group,bytes,write,false);}
    private RaftClientReply submit(RaftServer server,UUID group,byte[] bytes,boolean write,boolean stale)throws Exception {
        return requireSuccess(server.submitClientRequestAsync(RaftClientRequest.newBuilder().setClientId(ClientId.valueOf(new UUID(13410,30))).setServerId(server.getId()).setGroupId(RaftGroupId.valueOf(group)).setCallId(calls.incrementAndGet()).setType(write?RaftClientRequest.writeRequestType():stale?RaftClientRequest.staleReadRequestType(0):RaftClientRequest.readRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build()).get(15,TimeUnit.SECONDS));
    }
    private static RaftClientReply requireSuccess(RaftClientReply reply)throws IOException{if(!reply.isSuccess())throw new IOException("Actual Raft request failed",reply.getException());return reply;}
    private TopologyChange.Definition beginSplit(RatisTwoPoolWorkflowFixture donor,RangeDirectory before,RangeDescriptor parent)throws Exception {
        var left=new RangeDescriptor(new UUID(13410,10),new UUID(13410,11),parent.rangeEpoch(),parent.configurationVersion(),null,new byte[]{64},RangeDescriptor.TopologyState.ACTIVE,parent.replicas());var right=new RangeDescriptor(new UUID(13410,12),new UUID(13410,13),parent.rangeEpoch(),parent.configurationVersion(),new byte[]{64},null,RangeDescriptor.TopologyState.ACTIVE,parent.replicas());
        var after=new RangeDirectory(before.clusterId(),before.databaseId(),before.domainId(),before.schemaEpoch(),before.schemaDigest(),before.generation()+1,before.columns(),before.routingPrefixBytes(),List.of(left,right));var definition=new TopologyChange.Definition(new UUID(13410,14),before,after);var topology=topology(donor);
        command(donor,topology,topology.prepareDefinition(op(),definition,topology.readCapacity()));command(donor,topology,topology.prepareBegin(op(),definition,topology.readHead(before.domainId()),null,null));command(donor,topology,topology.prepareFencing(op(),definition,topology.readHead(before.domainId()),null,null));return definition;
    }
    private RatisTopologyCatalogue topology(RatisTwoPoolWorkflowFixture donor){var c=donor.plan.catalogue();return new RatisTopologyCatalogue(donor.catalogue,donor.first.clusterId(),donor.first.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.directoryColumn());}
    private OperationId op(){return new OperationId(new UUID(13410,31),0,operation++);}
    private void command(RatisTwoPoolWorkflowFixture donor,RatisTopologyCatalogue topology,DistributedCommand command)throws Exception {
        if(RatisTopologyCatalogue.requiresOwnerAdmission(command)){byte[] answer=donor.catalogue.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(RatisTopologyExecutionRpc.prepareRequest(command)))).getMessage().getContent().toByteArray();RatisTopologyExecutionRpc.requireEcho(answer,RatisTopologyExecutionRpc.PREPARE,command);}assertEquals(CommandResult.Status.SUCCESS,topology.execute(command).status());
    }
    private static void assertLocal(RatisNodeRuntime node,UUID group,TopologyChange.Definition consuming)throws Exception {
        var server=field(node,"server",RaftServer.class);var division=server.getDivision(RaftGroupId.valueOf(group));var machine=(RatisRangeStateMachine)division.getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);
        assertNotNull(store.acknowledgedRecoverySupersession());assertNotNull(store.topologyFreeze());assertArrayEquals(consuming.header().encode(),store.topologyFreeze().header().encode());assertNull(store.metadataRecoveryRegistration());assertTrue(store.topologyRetired());assertFalse(store.admissionOpen());assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertTrue(machine.localMetadataBaselineReady());
        var a=store.applied();assertNotNull(a);assertTrue(RockserverRaftServers.hasAppliedPrefix(division,machine,TermIndex.valueOf(a.term(),a.index())));assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));
        assertThrows(IllegalStateException.class,store::capture);assertThrows(IllegalStateException.class,()->store.readCurrentPoint(consuming.sourceDirectory().columns().iterator().next(),new byte[]{7}));assertEquals(0,store.activeSnapshotPins());
    }
    private static String phase(Path journal)throws IOException{return new String(RatisSchemaClientJournal.read(journal).get("phase"),StandardCharsets.US_ASCII);}
    private static void requireExactLocalReceiptHash(RatisRangeStateMachine machine,Map<String,byte[]> journal)throws Exception {
        var selected=RatisTopologySourceSnapshotRpc.request(journal.get("request"));
        var decode=RatisConfiguredRecovery.class.getDeclaredMethod("decodeLocal",byte[].class,Request.class);decode.setAccessible(true);
        var receipt=(RatisConfiguredRecovery.LocalMetadataCompletion)decode.invoke(null,journal.get("localCompletion"),selected);
        assertNotNull(machine.confirmLocalMetadataCompletion(selected,receipt.head(),receipt));
        byte[] corrupt=journal.get("localCompletion").clone();corrupt[48]^=1;
        var forged=(RatisConfiguredRecovery.LocalMetadataCompletion)decode.invoke(null,corrupt,selected);
        var refused=assertThrows(IOException.class,()->machine.confirmLocalMetadataCompletion(selected,receipt.head(),forged));
        assertEquals("Cold local META checkpoint changed its exact equal-cut hash",refused.getMessage());
    }
    private static void stepUntil(RatisConfiguredRecoveries manager,Path journal,String phase)throws Exception {
        Throwable last=null;for(int n=0;n<48;n++){if(phase.equals(phase(journal)))return;try{manager.step().get(20,TimeUnit.SECONDS);}catch(ExecutionException pending){last=pending;}}
        fail("Expected parent "+phase+", actual="+phase(journal)+", failure="+last);
    }
    private static void bootstrap(RatisNodeRuntime node)throws Exception{Throwable last=null;for(int n=0;n<8;n++)try{node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);return;}catch(ExecutionException waitingForActualLeader){last=waitingForActualLeader;Thread.sleep(100);}throw new AssertionError("Cold local META did not settle",last);}
    private static void awaitLeader(RaftServer server,RaftGroupId group,ReplicatedRangeStore store)throws Exception {
        try{await(()->{var info=server.getDivision(group).getInfo();var a=store.applied();return info.isLeader() && info.isLeaderReady() && a!=null && a.term()==info.getCurrentTerm() && RockserverRaftServers.hasAppliedPrefix(server.getDivision(group),(RatisRangeStateMachine)server.getDivision(group).getStateMachine(),TermIndex.valueOf(a.term(),a.index()));},"Actual single-voter leader/native prefix");}
        catch(AssertionError failure){var division=server.getDivision(group);var sm=(RatisRangeStateMachine)division.getStateMachine();throw new AssertionError("Leader="+division.getInfo().isLeader()+", ready="+division.getInfo().isLeaderReady()+", term="+division.getInfo().getCurrentTerm()+", A="+store.applied()+", updater="+sm.getLastAppliedTermIndex()+", gate="+sm.recoveryEligibilityGate().snapshot()+", pending="+sm.recoveryEligibilityGate().discoveryPending()+", health="+sm.getLifeCycleState()+", conf="+division.getRaftConf(),failure);}
    }
    @FunctionalInterface private interface Condition{boolean ready()throws Exception;}
    private static void await(Condition condition,String description)throws Exception{long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(15);while(System.nanoTime()<end){if(condition.ready())return;Thread.sleep(5);}fail(description);}
}
