package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisRelocationOwnerRpcTest {
    @Test void codecBindsWholeIntentOwnerAndCompletedNativeConfigurationWithoutClaimingEffects(){
        var voter=new ReplicaDescriptor(new UUID(1,1),new UUID(1,2),new UUID(1,3),"voter","127.0.0.1:9911",ReplicaDescriptor.Role.VOTER);var listener=new ReplicaDescriptor(new UUID(2,1),new UUID(2,2),new UUID(2,3),"listener","127.0.0.1:9912",ReplicaDescriptor.Role.LISTENER);var range=new RangeDescriptor(new UUID(3,1),new UUID(3,2),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter));var source=new RangeDirectory(new UUID(4,1),new UUID(4,2),new UUID(4,3),1,new byte[32],0,Set.of(new UUID(5,1)),0,List.of(range));var targetRange=new RangeDescriptor(range.rangeId(),range.groupId(),1,1,null,null,range.topologyState(),List.of(voter,listener));var target=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),1,new byte[32],1,source.columns(),0,List.of(targetRange));var definition=new TopologyChange.Definition(new UUID(6,1),source,target);var request=new RatisRelocationOwnerRpc.Request(definition,null,RatisRelocationOwnerRpc.Expected.TARGET,new ApplyPosition(2,5),null);assertArrayEquals(RatisRelocationOwnerRpc.request(request),RatisRelocationOwnerRpc.request(RatisRelocationOwnerRpc.decode(RatisRelocationOwnerRpc.request(request))));
        var cfg=RaftConfigurationProto.newBuilder().addPeers(RaftPeer.newBuilder().setId(voter.peerId()).setAddress(voter.address()).build().getRaftPeerProto()).addListeners(RaftPeer.newBuilder().setId(listener.peerId()).setAddress(listener.address()).build().getRaftPeerProto()).build().toByteArray();var evidence=new RatisRelocationOwnerRpc.Evidence(request.digest(),TopologyChange.Owner.of(voter),new ApplyPosition(2,8),new ReplicatedRangeStore.Configuration(2,6,cfg),null,true,true);var decoded=RatisRelocationOwnerRpc.decodeReply(RatisRelocationOwnerRpc.reply(evidence),request);assertArrayEquals(cfg,decoded.configuration().data());assertEquals(new ApplyPosition(2,6),decoded.proof(range.groupId(),request.requiredCut()).configurationPosition());
        var crossed=new RatisRelocationOwnerRpc.Request(definition,listener.replicaId(),RatisRelocationOwnerRpc.Expected.TARGET,request.requiredCut(),null);assertThrows(IllegalArgumentException.class,()->RatisRelocationOwnerRpc.decodeReply(RatisRelocationOwnerRpc.reply(evidence),crossed));assertThrows(IllegalArgumentException.class,()->new RatisRelocationOwnerRpc.Evidence(request.digest(),TopologyChange.Owner.of(voter),new ApplyPosition(2,6),new ReplicatedRangeStore.Configuration(1,6,cfg),null,true,true));var bytes=Arrays.copyOf(RatisRelocationOwnerRpc.request(request),RatisRelocationOwnerRpc.request(request).length+1);assertThrows(IllegalArgumentException.class,()->RatisRelocationOwnerRpc.decode(bytes));
    }
}
