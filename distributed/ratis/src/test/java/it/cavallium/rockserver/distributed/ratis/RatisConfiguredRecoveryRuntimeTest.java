package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual source/receiver Runtime ports and native image chunks; only Raft wire delivery/log floor are controlled. */
class RatisConfiguredRecoveryRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void journaledDriverRecoversMetadataAndCurrentChildrenWithoutHistoricalUserImages()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false);var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
            var split=source.split();var catalogue=source.plan.catalogue();var pool=RatisPoolFixtures.ID;
            var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"receiver-data"),Set.of());
            var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
            var route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No external USER fixture transport");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No external USER fixture transport");}});
            var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
            var executor=Executors.newSingleThreadExecutor();
            try {
                for(int round=0;round<2;round++) {
                    var receiver=new RatisNodeRuntimeTest.FakeServer();receiver.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,receiver::create,List.of(source.first))) {
                        node.relocationTransport(range->client(source,range),(range,owner)->client(source,range));node.start();
                        var owners=field(source.node,"pools",Map.class);var owner=owners.get(source.fast);var prior=field(owner,"sources",RatisTopologySourceSnapshotService.class);var host=field(prior,"host",RatisTopologySourceSnapshotService.Host.class);var bytes=new ByteArrayOutputStream();
                        var delivery=(RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                            if(method.getName().equals("send")){
                                var scope=(RatisTopologySourceSnapshotRpc.Request)args[0];var chunk=(InstallSnapshotRequestProto)args[1];assertEquals(scope.transferId().toString(),chunk.getSnapshotChunk().getRequestId());if(chunk.getSnapshotChunk().getRequestIndex()==0)bytes.reset();for(var file:chunk.getSnapshotChunk().getFileChunksList())file.getData().writeTo(bytes);
                                if(chunk.getSnapshotChunk().getDone()){var machine=receiver.machines.get(RaftGroupId.valueOf(scope.sourceGroup()));var cut=chunk.getSnapshotChunk().getTermIndex();install(machine,bytes.toByteArray(),cut.getTerm(),cut.getIndex());when(receiver.division(machine.getGroupId()).getRaftLog().getSnapshotIndex()).thenReturn(cut.getIndex());}
                                var rpc=chunk.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(chunk.getLeaderTerm()).setRequestIndex(chunk.getSnapshotChunk().getRequestIndex()).setResult(InstallSnapshotResult.SUCCESS).build();
                            }
                            if(method.getName().equals("requireReceiverInstalled")){var lease=(RatisTopologySourceSnapshotService.Lease)args[0];var prepared=lease.prepared();var request=new RatisRecoveryImageRpc.Request(prepared.request(),prepared.image().position(),prepared.image().sha256());var evidence=receiver.machines.get(RaftGroupId.valueOf(prepared.request().sourceGroup())).recoveryImageEvidence(request);evidence.require(request);assertTrue(evidence.imageInstalled());return null;}
                            try{return method.invoke(host,args);}catch(InvocationTargetException failed){throw failed.getCause();}
                        });
                        var limits=field(prior,"limits",RatisTopologySourceSnapshotService.Limits.class);var admission=field(prior,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class);
                        try(var transport=new RatisTopologySourceSnapshotService(delivery,limits,source.scheduler(),admission)) {
                            var sourceField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");sourceField.setAccessible(true);var originals=new HashMap<RatisRangeStateMachine,Object>();for(var range:java.util.stream.Stream.concat(split.parents().stream(),split.targets().stream()).toList()){var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));originals.put(machine,sourceField.get(machine));sourceField.set(machine,transport);}
                            try {
                                var requests=new ArrayList<RatisTopologySourceSnapshotRpc.Request>();var parent=split.parents().getFirst();requests.add(new RatisTopologySourceSnapshotRpc.Request(new UUID(994,1),split.header(),catalogue.groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory()));int number=1;for(var child:split.targets())requests.add(new RatisTopologySourceSnapshotRpc.Request(new UUID(994,++number),null,catalogue.groupId(),child.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,child.groupId(),split.targetDirectory()));
                                for(var request:requests){var path=Files.createDirectories(root.resolve("receiver-attempts").resolve(pool.toString()).resolve(request.sourceGroup().toString()));var budget=field(node,"resources",RatisNodeResourceBudget.class);var lease=budget.reserve(pool,RatisNodeResourceBudget.Kind.PROTECTED,64L<<20,1);
                                    try(var driver=new RatisConfiguredRecovery(path,plan.identityDigest(),request,node.configuredRecoveryHost(),executor,RatisJournalAccess.standalone(),lease::close)) {if(round==1)assertEquals(RatisConfiguredRecovery.Phase.DONE,driver.phase());for(int steps=0;steps<16 && driver.phase()!=RatisConfiguredRecovery.Phase.DONE;steps++)driver.step().get(15,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.DONE,driver.phase());}
                                }
                            }finally{for(var entry:originals.entrySet())sourceField.set(entry.getKey(),entry.getValue());}
                        }
                        assertEquals(3,node.localGroups().size());assertTrue(node.nativePool(pool).topologySeeds().isEmpty());var ancestor=store(receiver,split.parents().getFirst().groupId());assertFalse(ancestor.metadataRecoveryPending());assertTrue(ancestor.userHandlesDetached());assertNull(ancestor.topologyBirth());
                        for(var child:split.targets()){var recovered=store(receiver,child.groupId());assertTrue(recovered.currentRangeRecoveryReady());assertTrue(recovered.admissionOpen());assertArrayEquals(source.store(child.groupId()).topologyBirth().encode(),recovered.topologyBirth().encode());assertArrayEquals(source.store(child.groupId()).topologyActivation().encode(),recovered.topologyActivation().encode());}
                        assertArrayEquals(new byte[]{11},store(receiver,split.targets().getFirst().groupId()).readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));assertEquals(0,source.store(split.parents().getFirst().groupId()).activeSnapshotPins());
                        assertArrayEquals(new byte[]{22},source.store(source.secondDirectory.ranges().getFirst().groupId()).readCurrentPoint(source.second.columnIds().iterator().next(),new byte[]{7}));
                    }
                }
            }finally{executor.shutdown();assertTrue(executor.awaitTermination(10,TimeUnit.SECONDS));route.close();}
        }
    }
    private static ReplicatedRangeStore store(RatisNodeRuntimeTest.FakeServer server,UUID group)throws Exception{return field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);}
    private static void install(RatisRangeStateMachine machine,byte[] image,long term,long index)throws Exception {
        machine.prepareRecoveryReplacement();machine.pause();Path file=machine.getStateMachineStorage().getSnapshotFile(term,index).toPath();Files.createDirectories(file.getParent());Files.write(file,image);MD5FileUtil.saveMD5File(file.toFile(),MD5FileUtil.computeMd5ForFile(file.toFile()));SessionJournalFiles.force(file);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());SessionJournalFiles.force(file.getParent());machine.syncReceivedSnapshot(term,index);machine.reinitialize();
    }
    private static RaftClient client(RatisTwoPoolWorkflowFixture source,RangeDescriptor range){
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));var request=(Message)args[0];var result=switch(method.getName()){case "sendReadOnly"->machine.queryLinearizable(request);case "sendStaleRead"->machine.queryStale(request,(long)args[1]);default->throw new AssertionError(method);};return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(range==null?null:RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result.get(15,TimeUnit.SECONDS)).build();});
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
