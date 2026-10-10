package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Codec/replica binding fixtures; actual running division checks belong to state-machine tests. */
class RatisTopologyOwnerRpcTest {
    record Fixture(TopologyChange.Definition definition,UUID catalogue,ReplicaDescriptor owner,NativeTopologyFreeze freeze,NativeTopologySeed seed){}
    Fixture fixture(){
        var definition=new RatisTopologyActivationRecordTest().fixture().definition();var parent=definition.parents().getFirst();var owner=parent.replicas().getFirst();UUID catalogue=UUID.randomUUID();
        var view=new RatisTopologyAuthority.View(catalogue,new ApplyPosition(1,1),definition,TopologyChange.State.begin(definition.header()).beginFencing());
        var freeze=new NativeTopologyFreeze(definition.header(),parent.groupId(),new ApplyPosition(1,3),NativeTopologyFreeze.boundsDigest(parent.lowerInclusive(),parent.upperExclusive()),view.admissionDigest());
        var state=view.state().frozen(freeze.proof(owner.replicaId()));var names=new HashMap<UUID,String>();definition.sourceDirectory().columns().forEach(column->names.put(column,"data-"+column));
        return new Fixture(definition,catalogue,owner,freeze,new NativeTopologySeed(definition,state,owner,Map.of(parent.groupId(),freeze),names,Set.of()));
    }
    byte[] configuration(RangeDescriptor range){var builder=RaftConfigurationProto.newBuilder();for(var owner:range.replicas()){var peer=RaftPeer.newBuilder().setId(owner.peerId()).setAddress(owner.address()).build().getRaftPeerProto();if(owner.role()==ReplicaDescriptor.Role.VOTER)builder.addPeers(peer);else builder.addListeners(peer);}return builder.build().toByteArray();}
    RatisTopologyOwnerRpc.Evidence evidence(Fixture f,TopologyChange.Stage stage){
        UUID group=(stage==TopologyChange.Stage.FREEZE ? f.definition().parents() : f.definition().targets()).getFirst().groupId();var target=new RatisTopologyOwnerRpc.Target(f.definition(),stage,f.catalogue(),group,f.owner().replicaId());
        var activation=new NativeTopologyActivation(f.seed(),f.seed().birth(f.definition().targets().getFirst().groupId()),new ApplyPosition(1,1),f.catalogue(),new ApplyPosition(2,6),NativeTopologyActivation.publicationCommitment(f.definition(),f.seed().authority().freezeDigest(),f.catalogue()));
        byte[] artifact=switch(stage){case FREEZE->f.freeze().encode();case SEED->f.seed().encode();case ACTIVATE->activation.encode();};
        var proof=switch(stage){case FREEZE->f.freeze().proof(f.owner().replicaId());case SEED->f.seed().proof(group);case ACTIVATE->activation.proof();};
        return new RatisTopologyOwnerRpc.Evidence(target,f.owner(),proof,new ApplyPosition(2,10),0,-1,configuration(target.range()),2,artifact);
    }
    @Test void eachStageRoundTripsActualArtifactWithIndependentCompletedPrefix(){
        var f=fixture();for(var stage:TopologyChange.Stage.values()){var expected=evidence(f,stage);var target=expected.target();assertArrayEquals(target.encode(),RatisTopologyOwnerRpc.decodeRequest(RatisTopologyOwnerRpc.request(target)).encode());var actual=RatisTopologyOwnerRpc.decodeReply(RatisTopologyOwnerRpc.reply(expected),target);assertEquals(expected.proof(),actual.proof());assertEquals(expected.owner(),actual.owner());assertArrayEquals(expected.artifact(),actual.artifact());assertEquals(new ApplyPosition(2,10),actual.completedCut());assertEquals(-1,actual.configurationIndex());}
        var seed=evidence(f,TopologyChange.Stage.SEED);assertEquals(new ApplyPosition(0,0),seed.proof().position());assertNotEquals(seed.proof().position(),seed.completedCut());
    }
    @Test void crossedReplicaArtifactAndUnappliedOrJointMembershipFailClosed(){
        var f=fixture();var evidence=evidence(f,TopologyChange.Stage.SEED);var target=evidence.target();var listener=target.range().replicas().getLast();
        var otherCatalogue=new RatisTopologyOwnerRpc.Target(f.definition(),TopologyChange.Stage.SEED,UUID.randomUUID(),target.group(),target.replica());
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(otherCatalogue,f.owner(),evidence.proof(),evidence.completedCut(),0,-1,evidence.configuration(),2,evidence.artifact()));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,listener,evidence.proof(),evidence.completedCut(),0,-1,evidence.configuration(),2,evidence.artifact()));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,f.owner(),evidence.proof(),new ApplyPosition(0,0),0,-1,evidence.configuration(),0,evidence.artifact()));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,f.owner(),evidence.proof(),evidence.completedCut(),2,11,evidence.configuration(),2,evidence.artifact()));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,f.owner(),evidence.proof(),evidence.completedCut(),0,-1,evidence.configuration(),3,evidence.artifact()));
        var joined=RaftConfigurationProto.newBuilder().mergeFrom(parse(evidence.configuration())).addOldPeers(RaftPeer.newBuilder().setId("old").setAddress("127.0.0.1:9999").build().getRaftPeerProto()).build().toByteArray();
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,f.owner(),evidence.proof(),evidence.completedCut(),0,-1,joined,2,evidence.artifact()));
        var altered=evidence.artifact();altered[altered.length-1]^=1;assertThrows(IllegalArgumentException.class,()->new RatisTopologyOwnerRpc.Evidence(target,f.owner(),evidence.proof(),evidence.completedCut(),0,-1,evidence.configuration(),2,altered));
    }
    RaftConfigurationProto parse(byte[] bytes){try{return RaftConfigurationProto.parseFrom(bytes);}catch(java.io.IOException invalid){throw new AssertionError(invalid);}}
    @Test void replyCannotChangeExactTargetOrEscapeFramingAndAllocationBounds(){
        var f=fixture();var evidence=evidence(f,TopologyChange.Stage.FREEZE);var target=evidence.target();byte[] reply=RatisTopologyOwnerRpc.reply(evidence);
        var other=new RatisTopologyOwnerRpc.Target(f.definition(),target.stage(),UUID.randomUUID(),target.group(),target.replica());
        assertThrows(IllegalArgumentException.class,()->RatisTopologyOwnerRpc.decodeReply(reply,other));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyOwnerRpc.decodeReply(Arrays.copyOf(reply,reply.length-1),target));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyOwnerRpc.decodeReply(Arrays.copyOf(reply,reply.length+1),target));
        assertThrows(IllegalArgumentException.class,()->RatisTopologyOwnerRpc.decodeReply(new byte[RatisTopologyOwnerRpc.MAX_REPLY_BYTES+1],target));
        var malformed=RatisTopologyOwnerRpc.request(target);malformed[ScopedTransactionRpc.HEADER_BYTES+48]=(byte)255;assertThrows(IllegalArgumentException.class,()->RatisTopologyOwnerRpc.decodeRequest(malformed));
    }
}
