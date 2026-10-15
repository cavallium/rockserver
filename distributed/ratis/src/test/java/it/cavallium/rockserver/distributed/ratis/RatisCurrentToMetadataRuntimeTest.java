package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotResult;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode;
import org.apache.ratis.server.leader.RockserverSnapshotRequests;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySourceSnapshotRpc.*;

/** Actual Runtime/JNI GROUP adoption, later source sealing, META replacement, and cold ownership. */
class RatisCurrentToMetadataRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private long sequence;
    @Test void recoveredCurrentBecomesSealedMetadataWithoutReplayingItsOldGroupOrDeletingDescendantUserData()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false)) {
            var firstSplit=source.split();var rootRange=firstSplit.parents().getFirst();var parent=firstSplit.targets().getFirst();var catalogue=source.plan.catalogue();var helper=new RatisNodeRecoverySourceTest();
            var rootRequest=new Request(new UUID(1201,1),firstSplit.header(),catalogue.groupId(),rootRange.replicas().getFirst(),Purpose.ANCESTOR_METADATA,rootRange.groupId(),firstSplit.sourceDirectory());var rootMetadata=prepareWithoutPin(source,rootRequest);var rootHead=helper.head(source,new RatisSourceHeadRpc.Request(rootRequest,rootMetadata.image().position()));
            var currentRequest=new Request(new UUID(1201,2),null,catalogue.groupId(),parent.replicas().getFirst(),Purpose.CURRENT_LEAF,parent.groupId(),firstSplit.targetDirectory());var current=prepareWithoutPin(source,currentRequest);var sourceParent=source.server.machines.get(RaftGroupId.valueOf(parent.groupId()));sourceParent.notifyTermIndexUpdated(1,current.image().position().index()+1);var currentHead=helper.head(source,new RatisSourceHeadRpc.Request(currentRequest,current.image().position()));
            Prepared metadata=null,descendant=null;RatisSourceHeadRpc.Head metadataHead=null,descendantHead=null;byte[] groupFloor=null;long groupFloorIndex=-1;
            try(var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
                var pool=RatisPoolFixtures.ID;var domain=new RatisNodePlan.Domain(firstSplit.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),8,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
                var route=new RatisRoutedClient(firstSplit.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}});var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
                for(int round=0;round<3;round++) {
                    var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,server::create,List.of(source.first))) {
                        node.relocationTransport(range->sourceClient(source,range),(range,owner)->sourceClient(source,range));node.start();
                        if(round==0) {
                            var rootStore=node.registerRecovery(rootMetadata);var rootMachine=server.machines.get(RaftGroupId.valueOf(rootRange.groupId()));install(rootMachine,image(source,rootRequest),rootMetadata.image());floor(server,rootRange.groupId(),rootMetadata.image().position().index());assertTrue(node.completeRecovery(rootMetadata,rootHead));assertFalse(rootStore.admissionOpen());releaseInstalled(source,server,rootMetadata);
                            var store=node.registerRecovery(current);var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));install(machine,image(source,currentRequest),current.image());machine.notifyTermIndexUpdated(1,currentHead.completedCut().index());floor(server,parent.groupId(),current.image().position().index());assertTrue(node.completeRecovery(current,currentHead));assertTrue(store.currentRangeRecoveryReady());assertTrue(store.admissionOpen());assertArrayEquals(new byte[]{11},store.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));releaseInstalled(source,server,current);
                            assertEquals(store.applied().index(),machine.takeSnapshot());groupFloorIndex=machine.getLatestSnapshot().getIndex();floor(server,parent.groupId(),groupFloorIndex);var oldFloor=machine.getLatestSnapshot().getFiles().getFirst().getPath();assertEquals(NativeRangeImage.Kind.GROUP,NativeRangeImage.kind(oldFloor,NativeRangeImage.IoAdmission.UNLIMITED));groupFloor=Files.readAllBytes(oldFloor);
                            var consumed=splitSelected(source,firstSplit.targetDirectory(),parent);var request=new Request(new UUID(1201,3),consumed.header(),catalogue.groupId(),parent.replicas().getFirst(),Purpose.ANCESTOR_METADATA,parent.groupId(),consumed.sourceDirectory());metadata=prepareWithoutPin(source,request);sourceParent.notifyTermIndexUpdated(1,metadata.image().position().index()+1);metadataHead=helper.head(source,new RatisSourceHeadRpc.Request(request,metadata.image().position()));
                            var lost=new AtomicBoolean(true);node.nativePool(pool).metadataRecoveryForceObserver(after->{if(after && lost.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost CURRENT-to-META force acknowledgement");});var exact=metadata;assertThrows(IOException.class,()->node.registerRecovery(exact));assertEquals(Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());assertFalse(store.admissionOpen());assertThrows(IllegalStateException.class,store::capture);
                            node.nativePool(pool).metadataRecoveryForceObserver(after->{});assertSame(store,node.registerRecovery(metadata));assertPending(node,server,pool,parent.groupId());assertArrayEquals(groupFloor,Files.readAllBytes(oldFloor));assertEquals(groupFloorIndex,machine.getLatestSnapshot().getIndex());
                            assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));assertThrows(IllegalStateException.class,()->RockserverSnapshotRequests.open(server.division(RaftGroupId.valueOf(parent.groupId())),RaftPeerId.valueOf("blocked-outbound"),"old-group",machine.getLatestSnapshot(),1024));
                            var child=consumed.targets().getFirst();var childRequest=new Request(new UUID(1201,4),null,catalogue.groupId(),child.replicas().getFirst(),Purpose.CURRENT_LEAF,child.groupId(),consumed.targetDirectory());descendant=prepareWithoutPin(source,childRequest);descendantHead=helper.head(source,new RatisSourceHeadRpc.Request(childRequest,descendant.image().position()));
                        } else {
                            assertNotNull(metadata);var store=node.registerRecovery(metadata);var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));floor(server,parent.groupId(),machine.getLatestSnapshot().getIndex());
                            if(round==1) {
                                assertPending(node,server,pool,parent.groupId());assertEquals(groupFloorIndex,machine.getLatestSnapshot().getIndex());var retained=machine.getLatestSnapshot().getFiles().getFirst().getPath();assertArrayEquals(groupFloor,Files.readAllBytes(retained));assertEquals(NativeRangeImage.Kind.GROUP,NativeRangeImage.kind(retained,NativeRangeImage.IoAdmission.UNLIMITED));assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));
                                var childStore=node.registerRecovery(descendant);var childMachine=server.machines.get(RaftGroupId.valueOf(descendant.request().sourceGroup()));install(childMachine,image(source,descendant.request()),descendant.image());floor(server,descendant.request().sourceGroup(),descendant.image().position().index());assertTrue(node.completeRecovery(descendant,descendantHead));releaseInstalled(source,server,descendant);assertTrue(childStore.admissionOpen());assertArrayEquals(new byte[]{11},childStore.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));
                                install(machine,image(source,metadata.request()),metadata.image());assertTrue(store.metadataRecoveryImageInstalled());assertTrue(store.metadataRecoveryPending());machine.notifyTermIndexUpdated(1,metadataHead.completedCut().index());floor(server,parent.groupId(),metadata.image().position().index());assertTrue(node.completeRecovery(metadata,metadataHead));releaseInstalled(source,server,metadata);assertFalse(store.metadataRecoveryPending());assertFalse(store.admissionOpen());assertTrue(store.userHandlesDetached());assertNull(store.currentRangeRecoveryRegistration());assertFalse(store.currentRangeRecoveryReady());
                                assertEquals(store.applied().index(),machine.takeSnapshot());floor(server,parent.groupId(),store.applied().index());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertTrue(childStore.admissionOpen());assertArrayEquals(new byte[]{11},childStore.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));
                            } else {
                                assertTrue(store.metadataRecoveryImageInstalled());assertFalse(store.metadataRecoveryPending());assertFalse(store.admissionOpen());assertTrue(store.userHandlesDetached());assertNull(store.currentRangeRecoveryRegistration());assertTrue(node.completeRecovery(metadata,metadataHead));assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));
                                var childStore=node.registerRecovery(descendant);assertTrue(childStore.currentRangeRecoveryReady());assertTrue(childStore.admissionOpen());assertArrayEquals(new byte[]{11},childStore.readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));
                            }
                        }
                    }
                }
                route.close();
            }
        }
    }
    @Test void sealedMetaSupersedesUnfinishedCurrentWithoutRestoringItsObsoleteGroupImage()throws Exception {
        for(boolean installed:List.of(false,true))try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("unfinished-source-"+installed),false)) {
            var split=source.split();var parent=split.targets().getFirst();var rootRange=split.parents().getFirst();var catalogue=source.plan.catalogue();var helper=new RatisNodeRecoverySourceTest();
            var rootRequest=new Request(new UUID(1202,1),split.header(),catalogue.groupId(),rootRange.replicas().getFirst(),Purpose.ANCESTOR_METADATA,rootRange.groupId(),split.sourceDirectory());var rootMetadata=prepareWithoutPin(source,rootRequest);var rootHead=helper.head(source,new RatisSourceHeadRpc.Request(rootRequest,rootMetadata.image().position()));
            var currentRequest=new Request(new UUID(1202,2),null,catalogue.groupId(),parent.replicas().getFirst(),Purpose.CURRENT_LEAF,parent.groupId(),split.targetDirectory());var current=prepareWithoutPin(source,currentRequest);var currentHead=helper.head(source,new RatisSourceHeadRpc.Request(currentRequest,current.image().position()));var obsoleteImage=image(source,currentRequest);
            Prepared metadata=null;RatisSourceHeadRpc.Head metadataHead=null;
            try(var env=new RatisNodeRuntimeTest.Env(root.resolve("unfinished-receiver-"+installed),false,source.first.digest())) {
                var pool=RatisPoolFixtures.ID;var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),8,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
                var route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError();}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError();}});var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
                try {
                    for(int round=0;round<3;round++) {
                        var server=new RatisNodeRuntimeTest.FakeServer();server.skipFirstNoop=true;
                        try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,server::create,List.of(source.first))) {
                            node.relocationTransport(range->sourceClient(source,range),(range,owner)->sourceClient(source,range));node.start();
                            if(round==0) {
                                var rootStore=node.registerRecovery(rootMetadata);var rootMachine=server.machines.get(RaftGroupId.valueOf(rootRange.groupId()));install(rootMachine,image(source,rootRequest),rootMetadata.image());floor(server,rootRange.groupId(),rootMetadata.image().position().index());assertTrue(node.completeRecovery(rootMetadata,rootHead));assertFalse(rootStore.admissionOpen());releaseInstalled(source,server,rootMetadata);
                                var store=node.registerRecovery(current);var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));assertFalse(store.currentRangeRecoveryImageInstalled());assertFalse(store.currentRangeRecoveryReady());assertFalse(store.admissionOpen());assertNull(store.applied());assertNull(machine.getLatestSnapshot());
                                if(installed){install(machine,obsoleteImage,current.image());floor(server,parent.groupId(),current.image().position().index());assertTrue(store.currentRangeRecoveryImageInstalled());assertFalse(store.currentRangeRecoveryReady());}
                                var consumed=splitSelected(source,split.targetDirectory(),parent);var request=new Request(new UUID(1202,3),consumed.header(),catalogue.groupId(),parent.replicas().getFirst(),Purpose.ANCESTOR_METADATA,parent.groupId(),consumed.sourceDirectory());metadata=prepareWithoutPin(source,request);var sourceMachine=source.server.machines.get(RaftGroupId.valueOf(parent.groupId()));sourceMachine.notifyTermIndexUpdated(1,metadata.image().position().index()+1);metadataHead=helper.head(source,new RatisSourceHeadRpc.Request(request,metadata.image().position()));
                                assertSame(store,node.registerRecovery(metadata));assertPending(node,server,pool,parent.groupId());if(!installed){assertNull(store.applied());assertNull(machine.getLatestSnapshot());}assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));assertTrue(Files.isRegularFile(obsoleteImage),"Unfinished CURRENT has no fabricated receiver-install/release authority");
                            } else {
                                var store=node.registerRecovery(metadata);var machine=server.machines.get(RaftGroupId.valueOf(parent.groupId()));
                                if(round==1) {
                                    assertPending(node,server,pool,parent.groupId());if(!installed){assertNull(store.applied());assertNull(machine.getLatestSnapshot(),"Cold META must not restore the uninstalled obsolete GROUP image");}else assertEquals(NativeRangeImage.Kind.GROUP,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));
                                    assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));install(machine,image(source,metadata.request()),metadata.image());assertTrue(store.metadataRecoveryImageInstalled());assertTrue(store.metadataRecoveryPending());machine.notifyTermIndexUpdated(1,metadataHead.completedCut().index());floor(server,parent.groupId(),metadata.image().position().index());assertTrue(node.completeRecovery(metadata,metadataHead));releaseInstalled(source,server,metadata);assertEquals(store.applied().index(),machine.takeSnapshot());
                                } else {
                                    assertTrue(store.metadataRecoveryImageInstalled());assertFalse(store.metadataRecoveryPending());assertFalse(store.admissionOpen());assertTrue(store.userHandlesDetached());assertNull(store.currentRangeRecoveryRegistration());assertFalse(store.currentRangeRecoveryReady());assertTrue(node.completeRecovery(metadata,metadataHead));assertThrows(IOException.class,()->node.registerRecovery(current));assertThrows(IOException.class,()->node.completeRecovery(current,currentHead));assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));assertTrue(Files.isRegularFile(obsoleteImage));
                                }
                            }
                        }
                    }
                }finally{route.close();}
            }
        }
    }
    private static void assertPending(RatisNodeRuntime node,RatisNodeRuntimeTest.FakeServer server,UUID pool,UUID group)throws Exception {
        var store=field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);assertTrue(store.metadataRecoveryPending());assertFalse(store.metadataRecoveryImageInstalled());assertFalse(store.admissionOpen());assertTrue(store.userHandlesDetached());assertNull(store.currentRangeRecoveryRegistration());assertFalse(store.currentRangeRecoveryReady());assertTrue(node.nativePool(pool).currentRangeRecoveries().stream().noneMatch(current->current.groupId().equals(group)));assertEquals(Mode.MISSING,server.machines.get(RaftGroupId.valueOf(group)).recoveryEligibilityGate().snapshot().mode());assertThrows(IllegalStateException.class,store::capture);
    }
    private OperationId operation(){return new OperationId(new UUID(1201,20),0,sequence++);}
    private void topologyCommand(RatisTwoPoolWorkflowFixture source,RatisTopologyCatalogue topology,DistributedCommand command)throws Exception {
        if(RatisTopologyCatalogue.requiresOwnerAdmission(command)){byte[] reply=source.catalogue.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(RatisTopologyExecutionRpc.prepareRequest(command)))).getMessage().getContent().toByteArray();RatisTopologyExecutionRpc.requireEcho(reply,RatisTopologyExecutionRpc.PREPARE,command);}assertEquals(CommandResult.Status.SUCCESS,topology.execute(command).status());
    }
    private TopologyChange.Definition splitSelected(RatisTwoPoolWorkflowFixture source,RangeDirectory before,RangeDescriptor parent)throws Exception {
        var left=new RangeDescriptor(new UUID(1201,5),new UUID(1201,6),parent.rangeEpoch(),parent.configurationVersion(),parent.lowerInclusive(),new byte[]{32},RangeDescriptor.TopologyState.ACTIVE,parent.replicas());var right=new RangeDescriptor(new UUID(1201,7),new UUID(1201,8),parent.rangeEpoch(),parent.configurationVersion(),new byte[]{32},parent.upperExclusive(),RangeDescriptor.TopologyState.ACTIVE,parent.replicas());var ranges=new ArrayList<RangeDescriptor>();for(var range:before.ranges())if(range.groupId().equals(parent.groupId())){ranges.add(left);ranges.add(right);}else ranges.add(range);var after=new RangeDirectory(before.clusterId(),before.databaseId(),before.domainId(),before.schemaEpoch(),before.schemaDigest(),before.generation()+1,before.columns(),before.routingPrefixBytes(),ranges);var definition=new TopologyChange.Definition(new UUID(1201,9),before,after);var binding=source.plan.catalogue();var topology=new RatisTopologyCatalogue(source.catalogue,before.clusterId(),before.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.directoryColumn());
        topologyCommand(source,topology,topology.prepareDefinition(operation(),definition,topology.readCapacity()));topologyCommand(source,topology,topology.prepareBegin(operation(),definition,topology.readHead(before.domainId()),null,null));topologyCommand(source,topology,topology.prepareFencing(operation(),definition,topology.readHead(before.domainId()),null,null));
        var authority=new RatisTopologyAuthority.Client(source.catalogue,before.clusterId(),before.databaseId(),binding.groupId());var view=authority.read(definition.header(),TopologyChange.Stage.FREEZE);var freeze=new RatisTopologyFreezeRecord(definition,parent.groupId(),view.admissionDigest());var machine=source.server.machines.get(RaftGroupId.valueOf(parent.groupId()));machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyFreezeRpc.prepareRequest(freeze)))).get(5,TimeUnit.SECONDS);var frozen=NativeTopologyFreeze.decode(source.apply(parent.groupId(),freeze.encode()).getContent().toByteArray());topologyCommand(source,topology,topology.prepareProof(operation(),definition,topology.readHead(before.domainId()),frozen.proof(source.local.replicaId())));
        var seed=source.node.seedTopology(new RatisTopologySeedRpc.Request(definition,binding.groupId(),parent.groupId(),source.local.replicaId()));for(var child:definition.targets())topologyCommand(source,topology,topology.prepareProof(operation(),definition,topology.readHead(before.domainId()),seed.proof(child.groupId())));topologyCommand(source,topology,topology.preparePublication(operation(),definition,topology.readHead(before.domainId()),null,null));
        var published=authority.read(definition.header(),TopologyChange.Stage.ACTIVATE);for(var child:definition.targets()){var record=RatisTopologyActivationRecord.of(published,child.groupId());source.server.machines.get(RaftGroupId.valueOf(child.groupId())).queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologyActivationRpc.prepareRequest(record)))).get(5,TimeUnit.SECONDS);var activation=NativeTopologyActivation.decode(source.apply(child.groupId(),record.encode()).getContent().toByteArray());topologyCommand(source,topology,topology.prepareProof(operation(),definition,topology.readHead(before.domainId()),activation.proof()));}topologyCommand(source,topology,topology.prepareRetirement(operation(),definition,topology.readHead(before.domainId()),null,null));assertEquals(TopologyChange.Phase.RETIRED,topology.readHead(before.domainId()).phase());return definition;
    }
    /** Already-installed wire response is backed by the actual receiver's exact native image receipt. */
    private static void releaseInstalled(RatisTwoPoolWorkflowFixture source,RatisNodeRuntimeTest.FakeServer receiver,Prepared prepared)throws Exception {
        var owner=field(source.node,"pools",Map.class).get(source.fast);var original=field(owner,"sources",RatisTopologySourceSnapshotService.class);var host=field(original,"host",RatisTopologySourceSnapshotService.Host.class);
        var qualified=(RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(RatisTopologySourceSnapshotService.Host.class.getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
            if(method.getName().equals("send")) {
                var scope=(Request)args[0];requireRequest(prepared.request(),scope);var chunk=(InstallSnapshotRequestProto)args[1];var query=new RatisRecoveryImageRpc.Request(scope,prepared.image().position(),prepared.image().sha256());var evidence=receiver.machines.get(RaftGroupId.valueOf(scope.sourceGroup())).recoveryImageEvidence(query);evidence.require(query);assertTrue(evidence.imageInstalled());
                var rpc=chunk.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(chunk.getLeaderTerm()).setRequestIndex(chunk.getSnapshotChunk().getRequestIndex()).setResult(InstallSnapshotResult.ALREADY_INSTALLED).build();
            }
            if(method.getName().equals("requireReceiverInstalled")) {
                var lease=(RatisTopologySourceSnapshotService.Lease)args[0];requireRequest(prepared.request(),lease.prepared().request());var query=new RatisRecoveryImageRpc.Request(prepared.request(),prepared.image().position(),prepared.image().sha256());var evidence=receiver.machines.get(RaftGroupId.valueOf(prepared.request().sourceGroup())).recoveryImageEvidence(query);evidence.require(query);assertTrue(evidence.imageInstalled());return null;
            }
            try{return method.invoke(host,args);}catch(InvocationTargetException failed){throw failed.getCause();}
        });
        try(var transport=new RatisTopologySourceSnapshotService(qualified,field(original,"limits",RatisTopologySourceSnapshotService.Limits.class),source.scheduler(),field(original,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class))) {
            var request=prepared.request();assertArrayEquals(reply(PREPARE,prepared),reply(PREPARE,transport.prepare(request)));transport.transfer(request).get(15,TimeUnit.SECONDS);transport.release(request);assertTrue(host.isReleased(request));transport.release(request);
        }
        assertThrows(IOException.class,()->field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class).lookup(prepared.request()));
    }
    private static Prepared prepareWithoutPin(RatisTwoPoolWorkflowFixture source,Request request)throws Exception {
        var owner=field(source.node,"pools",Map.class).get(source.fast);var original=field(owner,"sources",RatisTopologySourceSnapshotService.class);try(var ephemeral=new RatisTopologySourceSnapshotService(field(original,"host",RatisTopologySourceSnapshotService.Host.class),field(original,"limits",RatisTopologySourceSnapshotService.Limits.class),source.scheduler(),field(original,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class))){return ephemeral.prepare(request);}
    }
    private static Path image(RatisTwoPoolWorkflowFixture source,Request request)throws Exception{return field(field(source.node,"pools",Map.class).get(source.fast),"artifacts",RatisTopologySourceSnapshotArtifacts.class).lookup(request).image();}
    private static void floor(RatisNodeRuntimeTest.FakeServer server,UUID group,long index){when(server.division(RaftGroupId.valueOf(group)).getRaftLog().getSnapshotIndex()).thenReturn(index);}
    private static void install(RatisRangeStateMachine machine,Path image,NativeRangeImage.Manifest manifest)throws Exception {
        machine.prepareRecoveryReplacement();machine.pause();var target=machine.getStateMachineStorage().getSnapshotFile(manifest.position().term(),manifest.position().index()).toPath();Files.createDirectories(target.getParent());Files.copy(image,target,StandardCopyOption.REPLACE_EXISTING);MD5FileUtil.saveMD5File(target.toFile(),MD5FileUtil.computeMd5ForFile(target.toFile()));SessionJournalFiles.force(target);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(target.toFile()).toPath());SessionJournalFiles.force(target.getParent());machine.reinitialize();
    }
    private static RaftClient sourceClient(RatisTwoPoolWorkflowFixture source,RangeDescriptor range) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);var result=source.server.machines.get(RaftGroupId.valueOf(range.groupId())).queryLinearizable((Message)args[0]).get(10,TimeUnit.SECONDS);return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result).build();});return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
