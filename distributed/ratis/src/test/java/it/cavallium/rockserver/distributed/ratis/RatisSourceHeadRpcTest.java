package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSourceHeadRpcTest {
    @Test void currentScopeHasNoSyntheticHeaderAndHeadBindsOriginalImageCutAndReceiver() {
        var schema=new RatisNodeServiceTest().schema();var group=new UUID(970,1);var voter=new ReplicaDescriptor(new UUID(970,2),new UUID(970,3),new UUID(970,4),"head-source","127.0.0.1:4567",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(970,5),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter));
        var original=new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),schema.schemaEpoch(),schema.digest(),1,schema.columnIds(),schema.routingPrefixBytes(),List.of(range));
        var source=new RatisTopologySourceSnapshotRpc.Request(new UUID(970,6),null,new UUID(970,7),voter,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,group,original);
        var request=new RatisSourceHeadRpc.Request(source,new ApplyPosition(1,5));var decoded=RatisSourceHeadRpc.request(RatisSourceHeadRpc.request(request));assertNull(decoded.source().header());assertEquals(request.minimumImageCut(),decoded.minimumImageCut());
        var newer=new RangeDirectory(original.clusterId(),original.databaseId(),original.domainId(),original.schemaEpoch()+1,new byte[32],2,original.columns(),original.routingPrefixBytes(),original.ranges());
        var configuration=new ReplicatedRangeStore.Configuration(1,6,RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(voter.peerId()).setAddress(voter.address()).build().getRaftPeerProto()).build().toByteArray());
        var head=new RatisSourceHeadRpc.Head(request,new ApplyPosition(1,20),newer,voter,new ApplyPosition(1,7),configuration);var bytes=RatisSourceHeadRpc.reply(head);var round=RatisSourceHeadRpc.reply(bytes,request);assertEquals(2,round.currentDirectory().generation());assertArrayEquals(configuration.data(),round.configuration().data());
        assertThrows(IllegalArgumentException.class,()->RatisSourceHeadRpc.reply(bytes,new RatisSourceHeadRpc.Request(source,new ApplyPosition(1,4))));
        var changedReceiver=new ReplicaDescriptor(voter.nodeId(),voter.nodeIncarnation(),voter.replicaId(),voter.peerId(),"127.0.0.1:4568",voter.role());
        var changedRange=new RangeDescriptor(range.rangeId(),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(changedReceiver));var wrong=new RangeDirectory(newer.clusterId(),newer.databaseId(),newer.domainId(),newer.schemaEpoch(),newer.schemaDigest(),newer.generation(),newer.columns(),newer.routingPrefixBytes(),List.of(changedRange));
        assertThrows(IllegalArgumentException.class,()->RatisSourceHeadRpc.requireContinuity(source,wrong));
        var unrelated=new RangeDescriptor(new UUID(970,8),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter));var wrongRange=new RangeDirectory(newer.clusterId(),newer.databaseId(),newer.domainId(),newer.schemaEpoch(),newer.schemaDigest(),newer.generation(),newer.columns(),newer.routingPrefixBytes(),List.of(unrelated));assertThrows(IllegalArgumentException.class,()->RatisSourceHeadRpc.requireContinuity(source,wrongRange));
        var catRequest=new RatisCatalogueViewRpc.Request(schema.clusterId(),schema.databaseId(),source.catalogue(),new UUID(970,9),schema.domainId());var view=new RatisCatalogueViewRpc.View(catRequest,new ApplyPosition(1,20),newer);assertArrayEquals(newer.payloadDigest(),RatisCatalogueViewRpc.reply(RatisCatalogueViewRpc.reply(view),catRequest).directory().payloadDigest());
        assertNull(RatisCatalogueViewRpc.reply(RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(catRequest,new ApplyPosition(1,21),null)),catRequest).directory());
    }
}
