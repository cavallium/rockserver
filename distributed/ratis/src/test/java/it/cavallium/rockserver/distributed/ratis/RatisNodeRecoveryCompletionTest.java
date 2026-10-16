package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual Runtime/native image and own-tail completion with controlled Raft division transport. */
class RatisNodeRecoveryCompletionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void completionRequiresInstalledImageOwnTailAndRatisBaselineAndKeepsFrozenRecipeAcrossColdRetry()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var split=source.split();var parent=split.parents().getFirst();var leaf=split.targets().getFirst();var catalogue=source.plan.catalogue();var helper=new RatisNodeRecoverySourceTest();
            var metaRequest=new RatisTopologySourceSnapshotRpc.Request(new UUID(995,1),split.header(),catalogue.groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory());var metadata=prepareWithoutPin(source,metaRequest);var metaHead=helper.head(source,new RatisSourceHeadRpc.Request(metaRequest,metadata.image().position()));
            var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(995,2),null,catalogue.groupId(),leaf.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,leaf.groupId(),split.targetDirectory());var prepared=prepareWithoutPin(source,request);var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(leaf.groupId()));long required=prepared.image().position().index()+1;sourceMachine.notifyTermIndexUpdated(1,required);var frozen=helper.head(source,new RatisSourceHeadRpc.Request(request,prepared.image().position()));
            var artifacts=field(field(source.node,"pools",Map.class).get(source.fast),"artifacts",RatisTopologySourceSnapshotArtifacts.class);var metaImage=artifacts.lookup(metaRequest).image();var image=artifacts.lookup(request).image();
            try(var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
                var base=env.settings();var settings=new RatisNodeRuntime.Settings(new NativeRangeImage.Limits(base.images().maxRowBytes(),base.images().maxImageBytes(),2),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),base.shutdownTimeout(),base.exports(),base.resources());
                var pool=RatisPoolFixtures.ID;var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
                var route=spy(new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}}));var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
                for(int round=0;round<2;round++) {
                    var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,settings,round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,server::create,List.of(source.first))) {
                        node.relocationTransport(range->sourceClient(source,range),(range,owner)->sourceClient(source,range));node.start();var metaStore=node.registerRecovery(metadata);var store=node.registerRecovery(prepared);var machine=server.machines.get(RaftGroupId.valueOf(leaf.groupId()));var metaMachine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));
                        var wrongRequest=request.successor(new UUID(995,3));var wrongHead=new RatisSourceHeadRpc.Head(new RatisSourceHeadRpc.Request(wrongRequest,prepared.image().position()),frozen.catalogueCut(),frozen.currentDirectory(),frozen.sourceOwner(),frozen.completedCut(),frozen.configuration());assertThrows(IOException.class,()->node.completeRecovery(prepared,wrongHead));
                        if(round==0){
                            assertFalse(node.completeRecovery(metadata,metaHead));assertFalse(node.completeRecovery(prepared,frozen));assertFalse(store.admissionOpen());
                            install(metaMachine,metaImage,metadata.image());install(machine,image,prepared.image());
                            assertTrue(store.currentRangeRecoveryImageInstalled());assertFalse(node.completeRecovery(prepared,frozen));assertFalse(store.admissionOpen());
                            machine.notifyTermIndexUpdated(1,required);assertEquals(frozen.completedCut(),store.applied());assertFalse(node.completeRecovery(prepared,frozen),"SM snapshot alone cannot stand in for actual Ratis log floor");
                            when(server.division(RaftGroupId.valueOf(leaf.groupId())).getRaftLog().getSnapshotIndex()).thenReturn(prepared.image().position().index());when(server.division(RaftGroupId.valueOf(parent.groupId())).getRaftLog().getSnapshotIndex()).thenReturn(metadata.image().position().index());
                            assertTrue(node.completeRecovery(metadata,metaHead));assertFalse(metaStore.metadataRecoveryPending());assertFalse(metaStore.admissionOpen());
                            var once=new java.util.concurrent.atomic.AtomicBoolean(true);store.currentRecoveryCompletionForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost CURRENT completion acknowledgement");});
                            assertThrows(IOException.class,()->node.completeRecovery(prepared,frozen));assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());
                            sourceMachine.notifyTermIndexUpdated(1,required+1);var newer=helper.head(source,new RatisSourceHeadRpc.Request(request,prepared.image().position()));assertThrows(IOException.class,()->node.completeRecovery(prepared,newer));
                            long configurationIndex=required+2;var configuration=RaftConfigurationProto.parseFrom(store.configuration().data());sourceMachine.notifyConfigurationChanged(1,configurationIndex,configuration);sourceMachine.notifyTermIndexUpdated(1,configurationIndex);machine.notifyTermIndexUpdated(1,required+1);machine.notifyConfigurationChanged(1,configurationIndex,configuration);machine.notifyTermIndexUpdated(1,configurationIndex);assertEquals(configurationIndex,store.applied().index());assertEquals(configurationIndex,store.configuration().index());assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());
                            store.currentRecoveryCompletionForceObserver(after->{});
                            var lostRoute=new java.util.concurrent.atomic.AtomicBoolean(true);clearInvocations(route);doAnswer(call->{if(lostRoute.getAndSet(false))throw new IllegalStateException("Route refresh failed after native completion");return call.callRealMethod();}).when(route).refresh(any(RangeDirectory.class));
                            assertThrows(IOException.class,()->node.completeRecovery(prepared,frozen));assertTrue(store.currentRangeRecoveryReady());assertTrue(node.completeRecovery(prepared,frozen),"Exact retry settles routing after native completion and later own CFG progression");verify(route,times(2)).refresh(any(RangeDirectory.class));doCallRealMethod().when(route).refresh(any(RangeDirectory.class));assertTrue(store.admissionOpen());
                            assertTrue(node.completeRecovery(prepared,frozen),"Ready duplicate preserves original required cut/config despite later genuine config tail");
                            var replay=recordSchemaTail(source,leaf.groupId());assertEquals(3,replay.size(),"Actual FENCE, INSTALL and ACTIVATE records were captured");assertTrue(RatisSchemaFenceRecord.isFence(replay.get(0).bytes()));assertTrue(RatisSchemaInstallRecord.isInstall(replay.get(1).bytes()));assertTrue(RatisSchemaActivateRecord.isActivate(replay.get(2).bytes()));
                            // A normal ready receiver participates in the real schema capacity preflight.
                            // The matching aliases let its native reservation equal the captured all-owner admission.
                            var installation=RatisSchemaInstallRecord.decode(replay.get(1).bytes());var fence=RatisSchemaFenceRecord.decode(replay.get(0).bytes());var names=new HashMap<>(node.nativePool(pool).schemaPhysicalNames(store));names.put(installation.header().changedColumnId(),installation.changedPhysicalName());var reservation=node.nativePool(pool).reserveSchemaCapacity(store,installation.definition(),names,store.deterministicMergeColumns());var admitted=SchemaAdmissionManifest.decode(fence.admissions(),installation.definition());assertEquals(admitted.replica(leaf.groupId(),request.receiver().replicaId()).reservation(),reservation);
                            for(var record:replay){assertEquals(store.applied().index()+1,record.position().index());replay(source,machine,record);if(RatisSchemaFenceRecord.isFence(record.bytes())){var originalFence=store.schemaFence().encode();boolean admission=store.admissionOpen();source.server.leader=false;try{assertTrue(node.completeRecovery(prepared,frozen),"Historical retry requires no source and must preserve a later schema fence");assertEquals(admission,store.admissionOpen());assertArrayEquals(originalFence,store.schemaFence().encode());}finally{source.server.leader=true;}}}
                            assertEquals(source.store(leaf.groupId()).applied(),store.applied());assertArrayEquals(source.routes.get(source.first.domainId()).directory().schemaDigest(),store.schemaDigest());assertTrue(store.installedSchema().schemaEpoch()>prepared.schema().schemaEpoch());assertArrayEquals(new byte[]{11},store.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));assertTrue(node.completeRecovery(prepared,frozen),"Original completed head remains exact after actual schema activation");source.server.leader=false;try{assertTrue(node.completeRecovery(prepared,frozen),"Warm exact completion needs no available source after forced readiness");assertTrue(node.completeRecovery(metadata,metaHead));}finally{source.server.leader=true;}assertEquals(store.applied().index(),machine.takeSnapshot());
                            long metaNext=metaStore.applied().index()+1;source.server.machines.get(RaftGroupId.valueOf(parent.groupId())).notifyTermIndexUpdated(1,metaNext);metaMachine.notifyTermIndexUpdated(1,metaNext);assertEquals(metaNext,metaMachine.takeSnapshot());
                        } else {
                            when(server.division(RaftGroupId.valueOf(leaf.groupId())).getRaftLog().getSnapshotIndex()).thenReturn(machine.getLatestSnapshot().getIndex());when(server.division(RaftGroupId.valueOf(parent.groupId())).getRaftLog().getSnapshotIndex()).thenReturn(metaMachine.getLatestSnapshot().getIndex());
                            assertTrue(store.currentRangeRecoveryReady());source.server.leader=false;try{assertTrue(node.completeRecovery(prepared,frozen),"Cold exact completion needs no available source after forced readiness");assertTrue(node.completeRecovery(metadata,metaHead));}finally{source.server.leader=true;}var changed=helper.head(source,new RatisSourceHeadRpc.Request(request,prepared.image().position()));assertThrows(IOException.class,()->node.completeRecovery(prepared,changed),"Cold ready row binds the original required cut/full configuration");assertTrue(store.admissionOpen());assertFalse(metaStore.admissionOpen());assertTrue(store.installedSchema().schemaEpoch()>prepared.schema().schemaEpoch());assertArrayEquals(new byte[]{11},store.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));
                        }
                    }
                }
                route.close();
            }
        }
    }
    @Test void genuinePreConsumptionCurrentReceiptCannotAcknowledgeParentMetadataObligation()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("old-current-source"),false);var env=new RatisNodeRuntimeTest.Env(root.resolve("old-current-receiver"),false,source.first.digest())){
            var directory=source.firstDirectory;var range=directory.ranges().getFirst();var binding=source.plan.catalogue();var receiver=range.replicas().getFirst();
            var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(996,1),null,binding.groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);var prepared=prepareWithoutPin(source,request);var helper=new RatisNodeRecoverySourceTest();var head=helper.head(source,new RatisSourceHeadRpc.Request(request,prepared.image().position()));
            var artifacts=field(field(source.node,"pools",Map.class).get(source.fast),"artifacts",RatisTopologySourceSnapshotArtifacts.class);var image=artifacts.lookup(request).image();
            var remoteRange=new RangeDescriptor(range.rangeId(),range.groupId(),range.rangeEpoch(),range.configurationVersion(),range.lowerInclusive(),range.upperExclusive(),range.topologyState(),List.of(binding.replicas().getFirst()));var bootstrap=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation()-1,directory.columns(),directory.routingPrefixBytes(),List.of(remoteRange));
            var domain=new RatisNodePlan.Domain(bootstrap,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var homeId=new UUID(996,10);var homeColumn=new UUID(996,11);var homeOwner=new ReplicaDescriptor(receiver.nodeId(),receiver.nodeIncarnation(),new UUID(996,12),receiver.peerId(),receiver.address(),ReplicaDescriptor.Role.VOTER);var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeOwner));var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"old-current-home");
            var plan=new RatisNodePlan(receiver.nodeId(),receiver.nodeIncarnation(),List.of(domain),binding,List.of(home),List.of(),Map.of(),4,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(binding,List.of(home)));
            var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
            try(var route=new RatisRoutedClient(bootstrap,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError();}public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError();}})){
                var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
                try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server::create,List.of(source.first))){
                    node.relocationTransport(r->sourceClient(source,r),(r,o)->sourceClient(source,r));node.start();assertFalse(server.machines.containsKey(RaftGroupId.valueOf(range.groupId())));var store=node.registerRecovery(prepared);var machine=server.machines.get(RaftGroupId.valueOf(range.groupId()));install(machine,image,prepared.image());when(server.division(machine.getGroupId()).getRaftLog().getSnapshotIndex()).thenReturn(prepared.image().position().index());assertTrue(node.completeRecovery(prepared,head));
                    var old=RatisRecoveryObligationRpc.local(store);assertNotNull(old);assertFalse(old.completion().metadata());byte[] receipt=store.recoveryCompletionReceipt();
                    var split=source.split();var freeze=source.store(range.groupId()).topologyFreeze();assertTrue(freeze.position().index()>old.completion().completedCut().index());assertEquals(directory.schemaEpoch(),split.header().schemaEpoch());assertArrayEquals(directory.schemaDigest(),split.header().schemaDigest());
                    var key=new TopologyChange.Key(range.groupId(),receiver.replicaId());var target=new RatisRecoveryObligationRpc.Target(binding.groupId(),split.header(),key);
                    var denied=assertThrows(IOException.class,()->RatisRecoveryObligationRpc.evidence(store,target,receiver));assertTrue(denied.getMessage().contains("Parent recovery obligation requires exact consuming META/FREEZE"));
                    var wire=machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisRecoveryObligationRpc.request(target))),0);assertThrows(java.util.concurrent.ExecutionException.class,()->wire.get(10,TimeUnit.SECONDS));
                    var topology=epochs.topologyCatalogue(binding.directoryColumn());var reservation=topology.readRecovery(split.intentId());assertThrows(IllegalArgumentException.class,()->topology.prepareRecoveryCompletion(new OperationId(new UUID(996,2),0,0),reservation,key,old.completion()));assertFalse(topology.readRecovery(split.intentId()).completions().containsKey(key));assertArrayEquals(receipt,store.recoveryCompletionReceipt());assertTrue(store.currentRangeRecoveryReady());
                }
            }
        }
    }
    private static RatisTopologySourceSnapshotRpc.Prepared prepareWithoutPin(RatisTwoPoolWorkflowFixture source,RatisTopologySourceSnapshotRpc.Request request)throws Exception {
        var owner=field(source.node,"pools",Map.class).get(source.fast);var original=field(owner,"sources",RatisTopologySourceSnapshotService.class);
        try(var ephemeral=new RatisTopologySourceSnapshotService(field(original,"host",RatisTopologySourceSnapshotService.Host.class),field(original,"limits",RatisTopologySourceSnapshotService.Limits.class),source.scheduler(),field(original,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class))){return ephemeral.prepare(request);}
    }
    private record SchemaReplay(byte[] bytes,ApplyPosition position) {}
    private static List<SchemaReplay> recordSchemaTail(RatisTwoPoolWorkflowFixture source,UUID selected)throws Exception {
        var route=source.routes.get(source.first.domainId());var transport=RatisRoutedClient.class.getDeclaredField("transport");transport.setAccessible(true);var original=(RatisRoutedClient.Transport)transport.get(route);var records=Collections.synchronizedList(new ArrayList<SchemaReplay>());
        var capture=(RatisRoutedClient.Transport)Proxy.newProxyInstance(RatisRoutedClient.Transport.class.getClassLoader(),new Class<?>[]{RatisRoutedClient.Transport.class},(proxy,method,args)->{
            final Object result;try{result=method.invoke(original,args);}catch(InvocationTargetException failure){throw failure.getCause();}
            if((method.getName().equals("participant") || method.getName().equals("participantAsync")) && ((RangeDescriptor)args[0]).groupId().equals(selected)){
                byte[] record=((byte[])args[1]).clone();if(result instanceof CompletableFuture<?> future)return future.thenApply(reply->{try{records.add(new SchemaReplay(record,source.store(selected).applied()));return reply;}catch(Exception failure){throw new java.util.concurrent.CompletionException(failure);}});records.add(new SchemaReplay(record,source.store(selected).applied()));
            }
            return result;
        });
        transport.set(route,capture);try{source.createColumn(source.first.domainId(),"after-completion");}finally{transport.set(route,original);}return List.copyOf(records);
    }
    private static void replay(RatisTwoPoolWorkflowFixture source,RatisRangeStateMachine receiver,SchemaReplay record)throws Exception {
        var entry=LogEntryProto.newBuilder().setTerm(record.position().term()).setIndex(record.position().index()).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record.bytes()))).build();var transaction=TransactionContext.newBuilder().setStateMachine(receiver).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(entry).build();
        // Reuse the controlled updater's real read/dispatch/offer/withdraw/drain path.
        var apply=RatisTwoPoolWorkflowFixture.class.getDeclaredMethod("committedApply",RatisRangeStateMachine.class,TransactionContext.class);apply.setAccessible(true);try{apply.invoke(source,receiver,transaction);}catch(InvocationTargetException failure){if(failure.getCause() instanceof Exception cause)throw cause;throw failure;}
    }
    private static void install(RatisRangeStateMachine machine,Path image,NativeRangeImage.Manifest manifest)throws Exception {
        machine.prepareRecoveryReplacement();machine.pause();var target=machine.getStateMachineStorage().getSnapshotFile(manifest.position().term(),manifest.position().index()).toPath();Files.createDirectories(target.getParent());Files.copy(image,target);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath());SessionJournalFiles.force(target.getParent());machine.reinitialize();
    }
    private static RaftClient sourceClient(RatisTwoPoolWorkflowFixture source,RangeDescriptor range){
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);var result=source.server.machines.get(RaftGroupId.valueOf(range.groupId())).queryLinearizable((Message)args[0]).get(10,TimeUnit.SECONDS);return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result).build();});
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
