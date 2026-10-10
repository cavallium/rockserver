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

    @Test void independentAcknowledgementsPreserveOtherProgressAndDuplicateMakesNoNewClaim() {
        var initial=initial(); var next=initial.checkpoint().advance(a,cursor(2,20,3));
        var first=initial.acknowledge(next,new DistributedCdcCheckpoint(Map.of(a,cursor(2,20,5))));
        first.requireSuccessorOf(initial,new DistributedCdcCheckpoint(Map.of(a,cursor(2,20,5))));
        assertEquals(1,first.generation()); assertSame(first,first.acknowledge(next,new DistributedCdcCheckpoint(Map.of())));
        var second=first.acknowledge(next.advance(b,cursor(7,9,1)),new DistributedCdcCheckpoint(Map.of(b,cursor(7,9,2))));
        assertEquals(cursor(2,20,3),second.checkpoint().cursors().get(a)); assertEquals(cursor(7,9,1),second.checkpoint().cursors().get(b));
        assertEquals(initial.checkpoint(),initial.definition().start()); assertEquals(2,second.generation());
        assertEquals(second,DistributedCdcSubscription.decode(second.encode()));
    }

    @Test void missingOrOldDeliveryCannotAdvanceAndNoLineageCanBeSubstituted() {
        var initial=initial(); var next=initial.checkpoint().advance(a,cursor(2,20,3));
        assertThrows(IllegalArgumentException.class,()->initial.acknowledge(next,new DistributedCdcCheckpoint(Map.of())));
        assertThrows(IllegalArgumentException.class,()->initial.acknowledge(next,new DistributedCdcCheckpoint(Map.of(a,cursor(2,20,2)))));
        var first=initial.acknowledge(next,next);
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(initial.checkpoint(),next));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(next.advance(a,cursor(2,21,0)),next));
        var foreign=new DistributedCdcCheckpoint.Lineage(a.rangeId(),a.groupId(),a.rangeEpoch()+1);
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(new DistributedCdcCheckpoint(Map.of(foreign,cursor(2,21,0),b,DistributedCdcCheckpoint.Cursor.beginning())),next));
        assertThrows(IllegalArgumentException.class,()->first.acknowledge(next,new DistributedCdcCheckpoint(Map.of(foreign,cursor(2,21,0)))));
        assertThrows(IllegalArgumentException.class,()->initial.acknowledge(next,new DistributedCdcCheckpoint(Map.of(a,cursor(3,20,3)))));
    }

    @Test void closureAndIncarnationFenceOldAcknowledgementsAcrossResume() {
        var initial=initial(); var active=initial.acknowledge(initial.checkpoint().advance(a,cursor(1,1,0)),new DistributedCdcCheckpoint(Map.of(a,cursor(1,1,0))));
        var closed=active.close(); closed.requireSuccessorOf(active,new DistributedCdcCheckpoint(Map.of()));
        assertSame(closed,closed.close()); assertEquals(2,closed.generation()); assertEquals(active.checkpoint(),closed.checkpoint());
        var resumed=DistributedCdcSubscription.decode(closed.encode()); assertEquals(closed,resumed);
        assertThrows(IllegalStateException.class,()->resumed.acknowledge(active.checkpoint(),active.checkpoint()));
        assertThrows(IllegalArgumentException.class,()->active.requireSuccessorOf(active,active.checkpoint()));
        var definition=initial.definition(); var other=new DistributedCdcSubscription.Definition(definition.clusterId(),definition.databaseId(),definition.domainId(),definition.subscriptionId(),id(99),definition.directoryDigest(),definition.columns(),definition.start());
        var replacement=DistributedCdcSubscription.initial(other).acknowledge(active.checkpoint(),active.checkpoint());
        assertThrows(IllegalArgumentException.class,()->replacement.requireSuccessorOf(initial,active.checkpoint()));
        assertEquals(initial.close(),DistributedCdcSubscription.decode(initial.close().encode()));
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
        assertEquals(List.of(column),definition.columns()); assertEquals(0,definition.directoryDigest()[0]);
        assertThrows(UnsupportedOperationException.class,()->definition.columns().clear());
    }

    @Test void canonicalEncodingRejectsMalformedLengthsAndImpossibleGenerations() {
        var initial=initial(); byte[] state=initial.encode();
        assertEquals(initial,DistributedCdcSubscription.decode(state));
        var generation=state.clone(); ByteBuffer.wrap(generation).putLong(1);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(generation));
        var length=state.clone(); ByteBuffer.wrap(length).putInt(9,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(length));
        var phase=state.clone(); phase[8]=(byte)255;
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(phase));
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.decode(Arrays.copyOf(state,state.length-1)));
        var columns=initial.definition().encode(); ByteBuffer.wrap(columns).putInt(112,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->DistributedCdcSubscription.Definition.decode(columns));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(4),id(5),new byte[32],List.of(column,column),initial.checkpoint()));
    }

    @Test void maximumStateHasFixedCapacityAcrossAcknowledgementAndClosure() {
        var start=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
        var delivered=new HashMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>();
        for(int i=0;i<DistributedCdcCheckpoint.MAX_LINEAGES;i++) {
            var lineage=new DistributedCdcCheckpoint.Lineage(id(1000+i),id(3000+i),1);
            start.put(lineage,DistributedCdcCheckpoint.Cursor.beginning()); delivered.put(lineage,cursor(1,1,0));
        }
        var columns=new ArrayList<UUID>(); for(int i=0;i<DistributedCdcSubscription.MAX_COLUMNS;i++) columns.add(id(5000+i));
        var definition=new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(4),id(5),new byte[32],columns,new DistributedCdcCheckpoint(start));
        var initial=DistributedCdcSubscription.initial(definition); assertEquals(DistributedCdcSubscription.MAX_BYTES,initial.encode().length);
        var next=new DistributedCdcCheckpoint(delivered); var active=initial.acknowledge(next,next);
        assertEquals(initial.encode().length,active.encode().length); assertEquals(initial.encode().length,active.close().encode().length);
        assertEquals(active.close(),DistributedCdcSubscription.decode(active.close().encode()));
    }
}
