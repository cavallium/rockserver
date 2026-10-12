package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisCdcSubscriptionBatchTest {
    static UUID id(int n) { return new UUID(0,n); }
    final UUID selected=id(1),other=id(2),group=id(3);
    final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(id(4),group,1);
    DistributedCdcSubscription state(DistributedCdcCheckpoint.Cursor from) {
        var definition=new DistributedCdcSubscription.Definition(id(5),id(6),id(7),id(8),id(9),new byte[32],List.of(selected),new DistributedCdcCheckpoint(Map.of(lineage,from)));
        return DistributedCdcSubscription.initial(definition);
    }
    RatisCdcPollRpc.Result result(DistributedCdcSubscription state,DistributedCdcRecord record) {
        var d=state.definition(); var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(d.clusterId(),d.databaseId(),d.domainId(),lineage),state.checkpoint().cursors().get(lineage),1,4096);
        return new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(1,1),DistributedCdcCheckpoint.Cursor.beginning(),record.end(),true,0,List.of(record));
    }
    @Test void selectedProjectionKeepsOriginalOrdinalsAndOperandsWithoutPredicatesOrUnselectedValues() {
        var state=state(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),0));
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(id(10),1,0),List.of(
                new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2}),new Mutation(Mutation.Kind.PUT,other,new byte[]{3},new byte[]{99}),
                new Mutation(Mutation.Kind.MERGE,selected,new byte[]{4},new byte[]{5})),List.of(new KeyPredicate(other,new byte[]{6},new byte[]{98})));
        var record=new DistributedCdcRecord(lineage,new ApplyPosition(1,1),command,null); var reply=result(state,record); var batch=new RatisCdcSubscriptionBatch(id(11),state,reply);
        assertEquals(1,batch.envelopes().size()); var envelope=batch.envelopes().getFirst(); assertEquals(1,envelope.events().size());
        assertEquals(2,envelope.events().getFirst().cursor().mutationOrdinal()); assertEquals(Mutation.Kind.MERGE,envelope.events().getFirst().mutation().kind());
        assertArrayEquals(new byte[]{5},envelope.events().getFirst().mutation().value()); assertEquals(record.end(),batch.next()); assertEquals(command.operationId(),envelope.operation());
        assertThrows(UnsupportedOperationException.class,()->envelope.events().clear());
        byte[] digest=envelope.schemaDigest(); digest[0]=1; assertEquals(0,envelope.schemaDigest()[0]);
    }
    @Test void batchesCannotBindClosedOrDifferentAcknowledgedState() {
        var state=state(DistributedCdcCheckpoint.Cursor.beginning());
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(id(10),1,0),List.of(new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2})));
        var record=new DistributedCdcRecord(lineage,new ApplyPosition(1,1),command,null); var reply=result(state,record);
        assertThrows(IllegalArgumentException.class,()->new RatisCdcSubscriptionBatch(id(11),state.close(),reply));
        var later=state.deliver(new DistributedCdcCheckpoint(Map.of(lineage,record.end()))).acknowledge(new DistributedCdcCheckpoint(Map.of(lineage,record.end())));
        assertThrows(IllegalArgumentException.class,()->new RatisCdcSubscriptionBatch(id(11),later,reply));
    }
    @Test void terminalBarrierProjectsWithoutColumnEventsAndEmptyMutationProjectionStillAdvances() {
        var state=state(DistributedCdcCheckpoint.Cursor.beginning()); var d=state.definition();
        var peer=new ReplicaDescriptor(id(101),id(102),id(103),"barrier","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var parent=new RangeDescriptor(lineage.rangeId(),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var source=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),1,new byte[32],1,Set.of(selected,other),1,List.of(parent));
        var left=new RangeDescriptor(id(104),id(105),1,1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var right=new RangeDescriptor(id(106),id(107),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var target=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),1,new byte[32],2,Set.of(selected,other),1,List.of(left,right));
        var freeze=new NativeTopologyFreeze(new TopologyChange.Definition(id(108),source,target).header(),group,new ApplyPosition(1,2),NativeTopologyFreeze.boundsDigest(null,null),new byte[32]);
        var barrier=DistributedCdcRecord.terminalBarrier(freeze);
        var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(d.clusterId(),d.databaseId(),d.domainId(),lineage),DistributedCdcCheckpoint.Cursor.beginning(),2,4096);
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(id(10),1,0),List.of(new Mutation(Mutation.Kind.PUT,other,new byte[]{1},new byte[]{99})));
        var mutation=new DistributedCdcRecord(lineage,new ApplyPosition(1,1),command,null);
        var empty=new RatisCdcSubscriptionBatch(id(11),state,result(state,mutation)).projected();
        assertTrue(empty.envelopes().isEmpty()); assertEquals(mutation.end(),empty.next());
        var reply=new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(1,3),DistributedCdcCheckpoint.Cursor.beginning(),barrier.end(),true,0,List.of(mutation,barrier));
        var projected=new RatisCdcSubscriptionBatch(id(12),state,reply).projected();
        assertEquals(1,projected.envelopes().size()); var envelope=projected.envelopes().getFirst();
        assertEquals(freeze,envelope.terminalBarrier()); assertTrue(envelope.events().isEmpty()); assertNull(envelope.operation()); assertNull(envelope.transaction()); assertEquals(barrier.end(),projected.next());
    }

    @Test void successorDeliveryProjectsCurrentStartsFromCommittedSubscriptionState() {
        var initial=state(DistributedCdcCheckpoint.Cursor.beginning()); var d=initial.definition();
        var peer=new ReplicaDescriptor(id(101),id(102),id(103),"successor","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var parent=new RangeDescriptor(lineage.rangeId(),group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var source=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),1,new byte[32],1,Set.of(selected),1,List.of(parent));
        var left=new RangeDescriptor(id(104),id(105),1,1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var right=new RangeDescriptor(id(106),id(107),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var target=new RangeDirectory(d.clusterId(),d.databaseId(),d.domainId(),1,new byte[32],2,Set.of(selected),1,List.of(left,right));
        var topology=new TopologyChange.Definition(id(108),source,target);
        var terminal=new DistributedCdcCheckpoint(Map.of(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2))));
        var child=new DistributedCdcCheckpoint.Lineage(left.rangeId(),left.groupId(),left.rangeEpoch());
        var sibling=new DistributedCdcCheckpoint.Lineage(right.rangeId(),right.groupId(),right.rangeEpoch());
        var start=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)); var starts=new DistributedCdcCheckpoint(Map.of(child,start,sibling,start));
        var transitioned=initial.deliver(terminal).acknowledge(terminal).lineageTransition(new DistributedCdcBarrierHandoff(topology.header(),terminal,starts));
        var command=new DistributedCommand(child.groupId(),child.rangeEpoch(),new byte[32],new OperationId(id(10),1,0),List.of(new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2})));
        var record=new DistributedCdcRecord(child,new ApplyPosition(1,5),command,null);
        var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(d.clusterId(),d.databaseId(),d.domainId(),child),start,1,4096);
        var reply=new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(1,6),start,record.end(),true,0,List.of(record));
        var projected=new RatisCdcSubscriptionBatch(id(12),transitioned,reply).projected();
        assertEquals(d,projected.definition()); assertFalse(d.start().cursors().containsKey(child)); assertEquals(starts,projected.currentStarts()); assertEquals(1,projected.envelopes().size());
    }

}
