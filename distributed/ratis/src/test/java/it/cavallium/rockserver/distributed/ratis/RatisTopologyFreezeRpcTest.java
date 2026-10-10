package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Pure current-codec tests; no server, owner qualification or Raft execution. */
class RatisTopologyFreezeRpcTest {
    static UUID id(int n) { return new UUID(105,n); }
    static List<ReplicaDescriptor> owners() { return List.of(new ReplicaDescriptor(id(40),id(41),id(42),"voter","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER),new ReplicaDescriptor(id(43),id(44),id(45),"listener","127.0.0.1:9002",ReplicaDescriptor.Role.LISTENER)); }
    static RangeDescriptor range(int range,int group,byte[] lower,byte[] upper) { return new RangeDescriptor(id(range),id(group),3,4,lower,upper,RangeDescriptor.TopologyState.ACTIVE,owners()); }
    static RangeDirectory directory(long generation,List<RangeDescriptor> ranges) { return new RangeDirectory(id(1),id(2),id(3),4,new byte[32],generation,Set.of(id(10)),0,ranges); }
    static TopologyChange.Definition split() { return new TopologyChange.Definition(id(11),directory(1,List.of(range(12,13,null,null))),directory(2,List.of(range(20,21,null,new byte[]{(byte)128}),range(22,23,new byte[]{(byte)128},null)))); }
    static RatisTopologyFreezeRecord record() { var definition=split(); return new RatisTopologyFreezeRecord(definition,definition.parents().getFirst().groupId(),new byte[32]); }
    static NativeTopologyFreeze artifact(RatisTopologyFreezeRecord record) {
        var parent=record.definition().parents().stream().filter(p->p.groupId().equals(record.groupId())).findFirst().orElseThrow();
        return new NativeTopologyFreeze(record.header(),record.groupId(),new ApplyPosition(7,90),NativeTopologyFreeze.boundsDigest(parent.lowerInclusive(),parent.upperExclusive()),record.admissionDigest());
    }
    @Test void fullDefinitionRecordIsCanonicalImmutableAndMergeParentsRemainIndependentlyBound() {
        var record=record(); var decoded=RatisTopologyFreezeRecord.decode(record.encode()); assertEquals(record,decoded); assertEquals(record.hashCode(),decoded.hashCode()); assertTrue(RatisTopologyFreezeRecord.isFreeze(record.encode())); assertFalse(RatisTopologyFreezeRecord.isFreeze(new byte[3]));
        assertArrayEquals(record.definition().encode(),decoded.definition().encode()); assertArrayEquals(record.header().encode(),decoded.header().encode()); record.requireFreeze(artifact(record));
        byte[] digest=record.admissionDigest(); digest[0]=1; assertEquals(0,record.admissionDigest()[0]); byte[] bytes=record.encode(); bytes[0]=0; assertTrue(RatisTopologyFreezeRecord.isFreeze(record.encode()));
        var source=directory(1,List.of(range(12,13,null,new byte[]{(byte)128}),range(14,15,new byte[]{(byte)128},null))); var target=directory(2,List.of(range(20,21,null,null))); var merge=new TopologyChange.Definition(id(11),source,target);
        for(var parent:merge.parents()) { var merged=new RatisTopologyFreezeRecord(merge,parent.groupId(),new byte[32]); assertEquals(merged,RatisTopologyFreezeRecord.decode(merged.encode())); merged.requireFreeze(artifact(merged)); }
    }
    @Test void missingAdmissionRelocationAndNonParentCannotBecomeFreezeInput() {
        var definition=split(); assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRecord(definition,definition.parents().getFirst().groupId(),null)); assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRecord(definition,definition.parents().getFirst().groupId(),new byte[31])); assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRecord(definition,definition.targets().getFirst().groupId(),new byte[32]));
        var parent=definition.parents().getFirst(); var replicas=new ArrayList<>(parent.replicas()); replicas.add(new ReplicaDescriptor(id(50),id(51),id(52),"added","127.0.0.1:9003",ReplicaDescriptor.Role.LISTENER));
        var moved=new RangeDescriptor(parent.rangeId(),parent.groupId(),parent.rangeEpoch(),parent.configurationVersion()+1,null,null,RangeDescriptor.TopologyState.ACTIVE,replicas); var relocate=new TopologyChange.Definition(id(11),definition.sourceDirectory(),directory(2,List.of(moved)));
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRecord(relocate,parent.groupId(),new byte[32]));
        byte[] size=record().encode(); ByteBuffer.wrap(size).putInt(52,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRecord.decode(size)); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRecord.decode(Arrays.copyOf(record().encode(),record().encode().length-1))); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRecord.decode(new byte[RatisTopologyFreezeRecord.MAX_BYTES+1]));
    }
    @Test void prepareReadinessBeforeCommitAndCommittedDrainStateAreDistinct() {
        var record=record(); var pending=new RatisTopologyFreezeRpc.State(record,null,2,3,4,false); var ready=new RatisTopologyFreezeRpc.State(record,null,0,0,0,true); var committed=new RatisTopologyFreezeRpc.State(record,artifact(record),1,0,0,false);
        for(int kind:List.of(RatisTopologyFreezeRpc.PREPARE,RatisTopologyFreezeRpc.STATE)) for(var state:List.of(pending,ready,committed)) assertEquals(state,RatisTopologyFreezeRpc.stateReply(RatisTopologyFreezeRpc.reply(kind,state),kind,record));
        assertTrue(ready.ready()); assertNull(ready.freeze()); assertFalse(committed.ready()); assertNotNull(committed.freeze());
        assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRpc.State(record,null,1,0,0,true)); assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRpc.State(record,null,0,-1,0,false));
        for(var request:List.of(RatisTopologyFreezeRpc.prepareRequest(record),RatisTopologyFreezeRpc.stateRequest(record),RatisTopologyFreezeRpc.cancelRequest(record))) assertEquals(record,RatisTopologyFreezeRpc.request(request));
        assertEquals(record,RatisTopologyFreezeRpc.decodeCancelledReply(RatisTopologyFreezeRpc.cancelledReply(record),record));
    }
    @Test void repliesCannotCrossRecordDigestGroupKindBoundsOrReadinessFlag() {
        var record=record(); var state=new RatisTopologyFreezeRpc.State(record,artifact(record),0,0,0,true); byte[] reply=RatisTopologyFreezeRpc.reply(RatisTopologyFreezeRpc.STATE,state);
        byte[] changed=new byte[32]; changed[0]=1; var other=new RatisTopologyFreezeRecord(record.definition(),record.groupId(),changed);
        assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRpc.stateReply(reply,RatisTopologyFreezeRpc.STATE,other)); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRpc.stateReply(reply,RatisTopologyFreezeRpc.PREPARE,record)); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRpc.decodeCancelledReply(RatisTopologyFreezeRpc.prepareRequest(record),record));
        var wrongBounds=new NativeTopologyFreeze(record.header(),record.groupId(),new ApplyPosition(7,90),NativeTopologyFreeze.boundsDigest(new byte[]{1},null),record.admissionDigest()); assertThrows(IllegalArgumentException.class,()->new RatisTopologyFreezeRpc.State(record,wrongBounds,0,0,0,true));
        var wrongAdmission=new NativeTopologyFreeze(record.header(),record.groupId(),new ApplyPosition(7,90),artifact(record).boundsDigest(),changed); assertThrows(IllegalArgumentException.class,()->record.requireFreeze(wrongAdmission));
        reply[reply.length-1]=2; assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRpc.stateReply(reply,RatisTopologyFreezeRpc.STATE,record)); assertThrows(IllegalArgumentException.class,()->RatisTopologyFreezeRpc.stateReply(new byte[RatisTopologyFreezeRpc.MAX_REPLY_BYTES+1],RatisTopologyFreezeRpc.STATE,record));
    }
}
