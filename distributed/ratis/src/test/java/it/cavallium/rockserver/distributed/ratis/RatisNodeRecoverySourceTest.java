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
    @Test void splitExportsCurrentLeafAndOriginalMetadataThroughActualHostAndColdArtifacts()throws Exception {
        try(var fixture=new RatisTwoPoolWorkflowFixture(root,false)) {
            var split=fixture.split();var parent=split.parents().getFirst();var leaf=split.targets().getFirst();
            var current=fixture.routes.get(fixture.first.domainId()).directory();var receiver=leaf.replicas().getFirst();
            var user=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9400),null,fixture.plan.catalogue().groupId(),receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,leaf.groupId(),current);
            var metadata=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9401),split.header(),fixture.plan.catalogue().groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory());
            var source=fixture.store(parent.groupId());var leafStore=fixture.store(leaf.groupId());var stale=new RatisTopologySourceSnapshotRpc.Request(new UUID(0,9402),null,user.catalogue(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,parent.groupId(),split.sourceDirectory());
            long before=source.activeSnapshotPins();assertThrows(ExecutionException.class,()->prepare(fixture,stale));assertEquals(before,source.activeSnapshotPins());
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
            assertEquals(CommandResult.Status.SUCCESS,catalogue.execute(catalogue.prepareAfterAdministration(new OperationId(new UUID(0,9403),0,0),current,forged,null,null,topology.readHead(current.domainId()))).status());
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
