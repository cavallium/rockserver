package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCdcSubscriptionTest {
    static UUID id(int n) { return new UUID(0,n); }
    final UUID column=id(10);
    final DistributedCdcCheckpoint.Lineage a=new DistributedCdcCheckpoint.Lineage(id(11),id(12),1),b=new DistributedCdcCheckpoint.Lineage(id(13),id(14),2);
    DistributedCdcCheckpoint checkpoint(DistributedCdcCheckpoint.Cursor first,DistributedCdcCheckpoint.Cursor second) { return new DistributedCdcCheckpoint(Map.of(a,first,b,second)); }
    DistributedCdcCheckpoint.Cursor cursor(long term,long index,int ordinal) { return new DistributedCdcCheckpoint.Cursor(new ApplyPosition(term,index),ordinal); }
    RangeDirectory directory() {
        var peer=new ReplicaDescriptor(id(20),id(21),id(22),"local","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(id(1),id(2),id(3),1,new byte[32],1,Set.of(column),0,List.of(
                new RangeDescriptor(a.rangeId(),a.groupId(),a.rangeEpoch(),1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(peer)),
                new RangeDescriptor(b.rangeId(),b.groupId(),b.rangeEpoch(),1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer))));
    }
    DistributedCdcSubscription initial() {
        var start=checkpoint(DistributedCdcCheckpoint.Cursor.beginning(),DistributedCdcCheckpoint.Cursor.beginning());
        return DistributedCdcSubscription.initial(DistributedCdcSubscription.Definition.create(id(4),id(5),directory(),List.of(column),start));
    }
    DistributedCdcSubscription generation(DistributedCdcSubscription state,long generation) {
        byte[] encoded=state.encode(); ByteBuffer.wrap(encoded).putLong(generation); return DistributedCdcSubscription.decode(encoded);
    }

    @Test void deliveryThenIndependentAcknowledgementsPreserveOtherProgressAndDuplicateMakesNoClaim() {
        var initial=initial(); assertEquals(initial.definition().start(),initial.delivered());
        var bounds=initial.delivered().advance(a,cursor(2,20,5)); var delivered=initial.deliver(bounds);
        delivered.requireSuccessorOf(initial); assertEquals(1,delivered.generation()); assertEquals(initial.checkpoint(),delivered.checkpoint()); assertSame(delivered,delivered.deliver(bounds));
        assertEquals(delivered,DistributedCdcSubscription.decode(delivered.encode()));
        var next=initial.checkpoint().advance(a,cursor(2,20,3)); var first=delivered.acknowledge(next);
        first.requireSuccessorOf(delivered); assertEquals(2,first.generation()); assertSame(first,first.acknowledge(next)); assertEquals(bounds,first.delivered());
        var secondDelivery=first.deliver(bounds.advance(b,cursor(7,9,2))); secondDelivery.requireSuccessorOf(first);
        var second=secondDelivery.acknowledge(next.advance(b,cursor(7,9,1))); second.requireSuccessorOf(secondDelivery);
        assertEquals(cursor(2,20,3),second.checkpoint().cursors().get(a)); assertEquals(cursor(7,9,1),second.checkpoint().cursors().get(b));
        assertEquals(initial.checkpoint(),initial.definition().start()); assertEquals(4,second.generation());
        var resumed=DistributedCdcSubscription.decode(second.encode()); assertEquals(second,resumed); assertEquals(second.delivered(),resumed.delivered());
    }

    @Test void persistedDeliveryBoundsAckAndRejectsRegressionOrLineageSubstitution() {
        var initial=initial(); var next=initial.checkpoint().advance(a,cursor(2,20,3));
        assertThrows(IllegalArgumentException.class,()->initial.acknowledge(next));
        var earlier=initial.deliver(initial.delivered().advance(a,cursor(2,20,2)));
        assertThrows(IllegalArgumentException.class,()->earlier.acknowledge(next));
        var delivered=initial.deliver(next); var first=delivered.acknowledge(next);
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(initial.checkpoint()));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(next.advance(a,cursor(2,21,0))));
        assertThrows(IllegalArgumentException.class,()->first.deliver(initial.delivered()));
        var foreign=new DistributedCdcCheckpoint.Lineage(a.rangeId(),a.groupId(),a.rangeEpoch()+1);
        var changed=new DistributedCdcCheckpoint(Map.of(foreign,cursor(2,21,0),b,DistributedCdcCheckpoint.Cursor.beginning()));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(changed)); assertThrows(IllegalArgumentException.class,()->first.deliver(changed));
        assertThrows(IllegalArgumentException.class,()->first.deliver(new DistributedCdcCheckpoint(Map.of(a,cursor(2,21,0)))));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(new DistributedCdcCheckpoint(Map.of(a,cursor(2,20,3)))));
        assertThrows(IllegalArgumentException.class,()->first.deliver(next.advance(a,cursor(3,20,3))));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(next.advance(a,cursor(3,20,3))));
    }

    @Test void successorRejectsStaleDeliveryAckClosureCombinedChangesAndNoopGeneration() {
        var initial=initial(); var first=initial.deliver(initial.delivered().advance(a,cursor(1,1,0)));
        var nextDelivery=first.deliver(first.delivered().advance(a,cursor(1,2,0))); var ack=first.acknowledge(first.delivered()); var closure=first.close();
        assertThrows(IllegalArgumentException.class,()->nextDelivery.requireSuccessorOf(ack));
        assertThrows(IllegalArgumentException.class,()->ack.requireSuccessorOf(nextDelivery));
        assertThrows(IllegalArgumentException.class,()->closure.requireSuccessorOf(nextDelivery));
        var combined=generation(nextDelivery.acknowledge(nextDelivery.delivered()),first.generation()+1);
        assertThrows(IllegalArgumentException.class,()->combined.requireSuccessorOf(first));
        var combinedClose=generation(nextDelivery.close(),first.generation()+1);
        assertThrows(IllegalArgumentException.class,()->combinedClose.requireSuccessorOf(first));
        var noop=generation(first,first.generation()+1); assertThrows(IllegalArgumentException.class,()->noop.requireSuccessorOf(first));
        assertThrows(IllegalArgumentException.class,()->first.requireSuccessorOf(first));
    }

    @Test void closurePreservesBothMapsAndIncarnationFencesOldProgressAcrossResume() {
        var initial=initial(); var delivered=initial.deliver(initial.delivered().advance(a,cursor(1,1,1))); var active=delivered.acknowledge(delivered.checkpoint().advance(a,cursor(1,1,0)));
        var closed=active.close(); closed.requireSuccessorOf(active); assertSame(closed,closed.close()); assertEquals(3,closed.generation());
        assertEquals(active.checkpoint(),closed.checkpoint()); assertEquals(active.delivered(),closed.delivered());
        var resumed=DistributedCdcSubscription.decode(closed.encode()); assertEquals(closed,resumed);
        assertThrows(IllegalStateException.class,()->resumed.acknowledge(active.checkpoint())); assertThrows(IllegalStateException.class,()->resumed.deliver(active.delivered()));
        var definition=initial.definition(); var other=new DistributedCdcSubscription.Definition(definition.clusterId(),definition.databaseId(),definition.domainId(),definition.subscriptionId(),id(99),definition.directoryDigest(),definition.columns(),definition.start());
        var replacement=DistributedCdcSubscription.initial(other).deliver(delivered.delivered());
        assertThrows(IllegalArgumentException.class,()->replacement.requireSuccessorOf(initial));
        assertEquals(initial.close(),DistributedCdcSubscription.decode(initial.close().encode())); assertEquals(delivered.close(),DistributedCdcSubscription.decode(delivered.close().encode()));
    }

    @Test void creationRequiresExactActiveDirectoryAndImmutableColumnSelection() {
        var start=initial().checkpoint(); var directory=directory();
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.Definition.create(id(4),id(5),directory,List.of(id(999)),start));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.Definition.create(id(4),id(5),directory,List.of(column),new DistributedCdcCheckpoint(Map.of(a,DistributedCdcCheckpoint.Cursor.beginning()))));
        var old=directory.ranges().getFirst(); var fenced=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),old.configurationVersion(),old.lowerInclusive(),old.upperExclusive(),RangeDescriptor.TopologyState.FENCED,old.replicas());
        var transitioning=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation(),directory.columns(),directory.routingPrefixBytes(),List.of(fenced,directory.ranges().getLast()));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.Definition.create(id(4),id(5),transitioning,List.of(column),start));
        var selected=new ArrayList<>(List.of(column)); byte[] digest=new byte[32];
        var definition=new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(4),id(5),digest,selected,start); selected.clear(); digest[0]=1;
        assertEquals(List.of(column),definition.columns()); assertEquals(0,definition.directoryDigest()[0]); assertThrows(UnsupportedOperationException.class,()->definition.columns().clear());
    }

    @Test void encodingRejectsMalformedLengthsBoundsCoverageAndImpossibleGenerations() {
        var initial=initial(); byte[] state=initial.encode(); assertEquals(initial,DistributedCdcSubscription.decode(state));
        assertThrows(IllegalArgumentException.class,()->generation(initial,1));
        var delivery=initial.deliver(initial.delivered().advance(a,cursor(1,1,0))); var ack=delivery.acknowledge(delivery.delivered());
        assertThrows(IllegalArgumentException.class,()->generation(delivery,0)); assertThrows(IllegalArgumentException.class,()->generation(ack,1)); assertThrows(IllegalArgumentException.class,()->generation(ack.close(),2));
        var length=state.clone(); ByteBuffer.wrap(length).putInt(9,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(length));
        int ackOffset=13+initial.definition().encode().length,deliveredOffset=ackOffset+4+initial.checkpoint().encode().length;
        var ackLength=state.clone(); ByteBuffer.wrap(ackLength).putInt(ackOffset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(ackLength));
        var deliveredLength=state.clone(); ByteBuffer.wrap(deliveredLength).putInt(deliveredOffset,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(deliveredLength));
        var uncovered=ack.encode(); System.arraycopy(initial.delivered().encode(),0,uncovered,deliveredOffset+4,initial.delivered().encode().length);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(uncovered));
        var foreign=state.clone(); ByteBuffer.wrap(foreign).putLong(deliveredOffset+4+4+32,99); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(foreign));
        var phase=state.clone(); phase[8]=(byte)255; assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(phase));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(Arrays.copyOf(state,state.length-1))); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(Arrays.copyOf(state,state.length+1)));
        var columns=initial.definition().encode(); ByteBuffer.wrap(columns).putInt(112,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.Definition.decode(columns));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(4),id(5),new byte[32],List.of(column,column),initial.checkpoint()));
    }

    @Test void maximumStateHasFixedCapacityAcrossDeliveryAcknowledgementAndClosure() {
        var start=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>(); var bounds=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
        for(int i=0;i<DistributedCdcCheckpoint.MAX_LINEAGES;i++) { var lineage=new DistributedCdcCheckpoint.Lineage(id(1000+i),id(3000+i),1); start.put(lineage,DistributedCdcCheckpoint.Cursor.beginning()); bounds.put(lineage,cursor(1,1,0)); }
        var columns=new ArrayList<UUID>(); for(int i=0;i<DistributedCdcSubscription.MAX_COLUMNS;i++) columns.add(id(5000+i));
        var definition=new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(4),id(5),new byte[32],columns,new DistributedCdcCheckpoint(start));
        var initial=DistributedCdcSubscription.initial(definition); assertEquals(DistributedCdcSubscription.MAX_BYTES,initial.encode().length);
        var next=new DistributedCdcCheckpoint(bounds); var delivered=initial.deliver(next); var active=delivered.acknowledge(next);
        assertEquals(initial.encode().length,delivered.encode().length); assertEquals(initial.encode().length,active.encode().length); assertEquals(initial.encode().length,active.close().encode().length);
        assertEquals(active.close(),DistributedCdcSubscription.decode(active.close().encode()));
    }
}
