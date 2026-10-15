package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual Runtime source Host and native images; controlled divisions, no network transfer. */
class RatisNodeRecoverySourceTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisTopologySourceSnapshotRpc.Prepared prepare(RatisTwoPoolWorkflowFixture fixture,RatisTopologySourceSnapshotRpc.Request request)throws Exception {
        var machine=fixture.server.machines.get(RaftGroupId.valueOf(request.sourceGroup()));
        byte[] reply=machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request)))).get(10,TimeUnit.SECONDS).getContent().toByteArray();
        return RatisTopologySourceSnapshotRpc.reply(reply,RatisTopologySourceSnapshotRpc.PREPARE,request);
    }
    RatisSourceHeadRpc.Head head(RatisTwoPoolWorkflowFixture fixture,RatisSourceHeadRpc.Request request)throws Exception {
        var machine=fixture.server.machines.get(RaftGroupId.valueOf(request.source().sourceGroup()));
        return RatisSourceHeadRpc.reply(machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisSourceHeadRpc.request(request)))).get(10,TimeUnit.SECONDS).getContent().toByteArray(),request);
    }
    @Test void retainedImageSurvivesActualSchemaAndConfigurationTailWithFreshHead()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var directory=fixture.secondDirectory;var range=directory.ranges().getFirst();var receiver=range.replicas().getFirst();
            var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9410),null,fixture.plan.catalogue().groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);assertNull(request.header());
            var owners=field(fixture.node,"pools",Map.class);var owner=owners.get(fixture.archive);var original=field(owner,"sources",RatisTopologySourceSnapshotService.class);var actual=field(original,"host",RatisTopologySourceSnapshotService.Host.class);var received=new java.io.ByteArrayOutputStream();
            // Native source ownership/capture stays real. Only chunk delivery and its receiver receipt are controlled.
            var transport=(RatisTopologySourceSnapshotService.Host)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                if(method.getName().equals("requireReceiverInstalled"))return null;
                if(method.getName().equals("send")){var chunk=(org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto)args[1];for(var file:chunk.getSnapshotChunk().getFileChunksList())file.getData().writeTo(received);var rpc=chunk.getServerRequest();return org.apache.ratis.proto.RaftProtos.InstallSnapshotReplyProto.newBuilder().setServerReply(org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(chunk.getLeaderTerm()).setRequestIndex(chunk.getSnapshotChunk().getRequestIndex()).setResult(org.apache.ratis.proto.RaftProtos.InstallSnapshotResult.SUCCESS).build();}
                try{return method.invoke(actual,args);}catch(java.lang.reflect.InvocationTargetException failed){throw failed.getCause();}
            });
            var limits=field(original,"limits",RatisTopologySourceSnapshotService.Limits.class);var admission=field(original,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class);
            RatisTopologySourceSnapshotRpc.Prepared prepared;
            try(var service=new RatisTopologySourceSnapshotService(transport,limits,fixture.scheduler(),admission)){prepared=service.prepare(request);}
            assertEquals(0,fixture.store(range.groupId()).activeSnapshotPins());
            try(var service=new RatisTopologySourceSnapshotService(transport,limits,fixture.scheduler(),admission)) {
                assertArrayEquals(prepared.image().sha256(),service.prepare(request).image().sha256());var query=new RatisSourceHeadRpc.Request(request,prepared.image().position());var store=fixture.store(range.groupId());int pins=store.activeSnapshotPins();var before=head(fixture,query);assertEquals(pins,store.activeSnapshotPins());
                fixture.createColumn(directory.domainId(),"later",()->{assertNotNull(store.schemaFence());assertThrows(IllegalStateException.class,()->{try(var pin=store.capture()){fail("Native schema FENCE must reject USER capture");}});assertThrows(ExecutionException.class,()->head(fixture,query));assertArrayEquals(prepared.image().sha256(),service.prepare(request).image().sha256());assertArrayEquals(prepared.image().sha256(),service.transfer(request).get(10,TimeUnit.SECONDS).image().sha256());return null;});var machine=fixture.server.machines.get(RaftGroupId.valueOf(range.groupId()));long next=store.applied().index()+1;var cfg=org.apache.ratis.proto.RaftProtos.RaftConfigurationProto.parseFrom(store.configuration().data());machine.notifyConfigurationChanged(1,next,cfg);machine.notifyTermIndexUpdated(1,next);
                var after=head(fixture,query);assertTrue(after.currentDirectory().schemaEpoch()>directory.schemaEpoch());assertTrue(after.catalogueCut().index()>before.catalogueCut().index());assertEquals(store.applied(),after.completedCut());assertEquals(next,after.configuration().index());assertEquals(pins,store.activeSnapshotPins(),"Head allocates no image/pin");
                assertArrayEquals(prepared.image().sha256(),service.prepare(request).image().sha256());assertArrayEquals(prepared.image().sha256(),service.transfer(request).get(10,TimeUnit.SECONDS).image().sha256());assertArrayEquals(prepared.image().sha256(),OperatorUpload.sha256(received.toByteArray()));
                var freshId=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9411),null,request.catalogue(),receiver,request.purpose(),range.groupId(),directory);assertThrows(java.io.IOException.class,()->service.prepare(freshId),"A new capture cannot reuse stale placement");
            }
            try(var cold=new RatisTopologySourceSnapshotService(transport,limits,fixture.scheduler(),admission)){
                assertArrayEquals(prepared.image().sha256(),cold.prepare(request).image().sha256());cold.transfer(request).get(10,TimeUnit.SECONDS);
                fixture.server.leader=false;
                var machine=fixture.server.machines.get(RaftGroupId.valueOf(range.groupId()));var sourceField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");sourceField.setAccessible(true);var prior=sourceField.get(machine);sourceField.set(machine,cold);
                try {
                    assertThrows(ExecutionException.class,()->head(fixture,new RatisSourceHeadRpc.Request(request,prepared.image().position())));
                    byte[] release=RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.RELEASE,request);var message=Message.valueOf(ByteString.copyFrom(release));
                    assertThrows(ExecutionException.class,()->machine.queryStale(message,1).get(10,TimeUnit.SECONDS));
                    assertArrayEquals(release,machine.queryStale(message,0).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                    assertArrayEquals(release,machine.queryStale(message,0).get(10,TimeUnit.SECONDS).getContent().toByteArray());
                    assertEquals(0,cold.stats().leases());assertEquals(0,cold.stats().reservedImageBytes());assertEquals(0,fixture.store(range.groupId()).activeSnapshotPins());
                }finally{sourceField.set(machine,prior);fixture.server.leader=true;}
            }
        }
    }
    @Test void splitExportsCurrentLeafAndOriginalMetadataThroughActualHostAndColdArtifacts()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var split=fixture.split();var parent=split.parents().getFirst();var leaf=split.targets().getFirst();
            var current=fixture.routes.get(fixture.first.domainId()).directory();var receiver=leaf.replicas().getFirst();
            var user=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9400),null,fixture.plan.catalogue().groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,leaf.groupId(),current);
            var metadata=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9401),split.header(),fixture.plan.catalogue().groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory());
            fixture.createColumn(fixture.first.domainId(),"after-split");
            current=fixture.routes.get(fixture.first.domainId()).directory();
            user=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9400),null,fixture.plan.catalogue().groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,leaf.groupId(),current);
            var source=fixture.store(parent.groupId());var leafStore=fixture.store(leaf.groupId());var stale=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9402),null,user.catalogue(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,parent.groupId(),split.sourceDirectory());
            long before=source.activeSnapshotPins();assertThrows(ExecutionException.class,()->prepare(fixture,stale));assertThrows(ExecutionException.class,()->head(fixture,new RatisSourceHeadRpc.Request(stale,source.applied())));assertEquals(before,source.activeSnapshotPins());
            var userPrepared=prepare(fixture,user);var metadataPrepared=prepare(fixture,metadata);
            assertEquals(leaf.groupId(),userPrepared.image().owner().groupId());assertEquals(parent.groupId(),metadataPrepared.image().owner().groupId());
            assertEquals(fixture.store(leaf.groupId()).applied(),userPrepared.image().position());assertEquals(source.applied(),metadataPrepared.image().position());
            assertArrayEquals(fixture.store(leaf.groupId()).configuration().data(),userPrepared.sourceCfg().data());assertArrayEquals(source.configuration().data(),metadataPrepared.sourceCfg().data());
            assertNull(metadataPrepared.image().topologyOrigin());assertTrue(fixture.store(leaf.groupId()).activeSnapshotPins()>0);assertTrue(source.activeSnapshotPins()>0);
            var owners=field(fixture.node,"pools",Map.class);var owner=owners.get(fixture.fast);var artifacts=field(owner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);
            var userSaved=artifacts.lookup(user);var metadataSaved=artifacts.lookup(metadata);var limits=field(artifacts,"imageLimits",NativeRangeImage.Limits.class);
            assertArrayEquals(userPrepared.image().sha256(),NativeRangeImage.inspect(userSaved.image(),limits).sha256());assertArrayEquals(metadataPrepared.image().sha256(),NativeRangeImage.inspectMetadata(metadataSaved.image(),limits).sha256());
            assertThrows(java.io.IOException.class,()->NativeRangeImage.inspect(metadataSaved.image(),limits));assertThrows(java.io.IOException.class,()->NativeRangeImage.inspectMetadata(userSaved.image(),limits));
            var forged=new RangeDirectory(current.clusterId(),current.databaseId(),current.domainId(),current.schemaEpoch(),current.schemaDigest(),current.generation()+1,current.columns(),current.routingPrefixBytes(),current.ranges());
            var catalogue=new RatisCatalogue(fixture.catalogue,fixture.first.clusterId(),fixture.first.databaseId(),fixture.plan.catalogue().groupId(),fixture.plan.catalogue().epoch(),fixture.plan.catalogue().schemaDigest(),fixture.plan.catalogue().directoryColumn());
            var topology=new RatisTopologyCatalogue(fixture.catalogue,fixture.first.clusterId(),fixture.first.databaseId(),fixture.plan.catalogue().groupId(),fixture.plan.catalogue().epoch(),fixture.plan.catalogue().schemaDigest(),fixture.plan.catalogue().directoryColumn());
            var schemaAdministration=new RatisSchemaAdministration(fixture.catalogue,fixture.first.clusterId(),fixture.first.databaseId(),fixture.plan.catalogue().groupId(),fixture.plan.catalogue().epoch(),fixture.plan.catalogue().schemaDigest(),fixture.plan.catalogue().directoryColumn());
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAfterAdministration(new OperationId(new UUID(0,9403),0,0),current,forged,schemaAdministration.readHead(current.domainId()),null,topology.readHead(current.domainId()))).status());
            var fabricated=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9404),null,user.catalogue(),receiver,user.purpose(),leaf.groupId(),forged);
            int retainedPins=leafStore.activeSnapshotPins();assertThrows(ExecutionException.class,()->prepare(fixture,fabricated));assertEquals(retainedPins,leafStore.activeSnapshotPins());
            var directory=field(artifacts,"root",Path.class);long disk=field(artifacts,"maximumImageBytes",Long.class),memory=field(artifacts,"maximumMetadataBytes",Long.class);int slots=field(artifacts,"maximumArtifacts",Integer.class);
            fixture.service.close();fixture.service=null;assertEquals(0,source.activeSnapshotPins());assertEquals(0,leafStore.activeSnapshotPins());
            try(var cold=new RatisTopologySourceSnapshotArtifacts(directory,limits,disk,memory,slots)) {
                assertArrayEquals(userPrepared.image().sha256(),cold.lookup(user).prepared().image().sha256());assertArrayEquals(metadataPrepared.image().sha256(),cold.lookup(metadata).prepared().image().sha256());
                assertEquals(userPrepared.image().position(),cold.lookup(user).prepared().image().position());assertEquals(metadataPrepared.image().position(),cold.lookup(metadata).prepared().image().position());
            }
        }
    }
}
