package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.cdc.DistributedCdcSubscriptions;
import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCdcSubscriptionsTest {
    static UUID id(int n) { return new UUID(0,n); }
    final UUID column=id(10);
    final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(id(4),id(5),1);
    final OperationId operation=new OperationId(id(50),0,0);
    final DistributedCdcCheckpoint.Cursor beginning=DistributedCdcCheckpoint.Cursor.beginning();
    DistributedCdcSubscription.Definition definition() {
        return new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(6),id(7),new byte[32],List.of(column),new DistributedCdcCheckpoint(Map.of(lineage,beginning)));
    }
    DistributedCdcRecord.Event event(long index,int ordinal,UUID selected) { return new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,index),ordinal),new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2})); }
    DistributedCdcSubscriptions.Envelope envelope(long index) { return new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,index),new byte[32],operation,null,List.of(event(index,0,column))); }
    DistributedCdcSubscriptions.Batch batch(DistributedCdcCheckpoint.Cursor from,DistributedCdcCheckpoint.Cursor next,ApplyPosition cut,List<DistributedCdcSubscriptions.Envelope> envelopes) {
        return new DistributedCdcSubscriptions.Batch(id(8),definition(),lineage,from,next,cut,envelopes);
    }
    @Test void immutableProjectionRetainsAdvancedAckAndSparseOrdinalsWithoutOriginalCommand() {
        var events=new ArrayList<>(List.of(event(2,2,column))); byte[] schema=new byte[32];
        var envelope=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),schema,operation,null,events); events.clear(); schema[0]=1;
        var envelopes=new ArrayList<>(List.of(envelope)); var from=new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,2),0); var next=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2));
        var batch=batch(from,next,new ApplyPosition(1,3),envelopes); envelopes.clear();
        assertEquals(from,batch.from()); assertEquals(next,batch.next()); assertEquals(1,batch.envelopes().size()); assertEquals(2,batch.envelopes().getFirst().events().getFirst().cursor().mutationOrdinal());
        assertEquals(0,envelope.schemaDigest()[0]); byte[] returned=envelope.schemaDigest(); returned[0]=2; assertEquals(0,envelope.schemaDigest()[0]);
        byte[] value=envelope.events().getFirst().mutation().value(); value[0]=7; assertEquals(2,envelope.events().getFirst().mutation().value()[0]);
        assertThrows(UnsupportedOperationException.class,()->batch.envelopes().clear()); assertThrows(UnsupportedOperationException.class,()->envelope.events().clear());
        var same=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],operation,null,List.of(event(2,2,column))); assertEquals(envelope,same); assertEquals(envelope.hashCode(),same.hashCode());
        int charge=DistributedCdcSubscriptions.BATCH_HEADER_BYTES+definition().encode().length+definition().start().encode().length+DistributedCdcSubscriptions.ENVELOPE_HEADER_BYTES+4+26+1+1;
        assertEquals(charge,batch.budgetBytes());
    }
    @Test void lineageCursorSelectionAndEnvelopeOrderingMustMatchDefinition() {
        var end=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)); var cut=new ApplyPosition(1,3);
        var foreign=new DistributedCdcCheckpoint.Lineage(lineage.rangeId(),lineage.groupId(),2);
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Batch(id(8),definition(),foreign,beginning,end,cut,List.of()));
        var boundDefinition=new DistributedCdcSubscription.Definition(id(1),id(2),id(3),id(6),id(7),new byte[32],List.of(column),new DistributedCdcCheckpoint(Map.of(lineage,end)));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Batch(id(8),boundDefinition,lineage,beginning,end,cut,List.of()));
        assertThrows(IllegalArgumentException.class,()->batch(end,beginning,cut,List.of()));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)),cut,List.of()));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,end,cut,List.of(envelope(2),envelope(1))));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,end,cut,List.of(envelope(2),envelope(2))));
        assertThrows(IllegalArgumentException.class,()->batch(end,end,cut,List.of(envelope(2))));
        var unselected=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],operation,null,List.of(event(2,0,id(99))));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,end,cut,List.of(unselected)));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,2),1),cut,List.of(envelope(2))));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],operation,null,List.of(event(3,0,column))));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],operation,null,List.of(event(2,2,column),event(2,1,column))));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[31],operation,null,List.of(event(2,0,column))));
    }
    @Test void emptySelectedDataMayAdvanceButGapAndTooLargeNeverCarryBatch() {
        var cut=new ApplyPosition(1,3); var next=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)); var empty=batch(beginning,next,cut,List.of());
        assertTrue(empty.envelopes().isEmpty()); assertEquals(next,new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,empty,0,beginning,cut).batch().next());
        var gap=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.GAP,null,0,next,cut); assertNull(gap.batch());
        var large=new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.TOO_LARGE,null,4096,beginning,cut); assertEquals(4096,large.requiredBytes());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,null,0,beginning,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,empty,1,beginning,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.DATA,empty,0,next,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.GAP,empty,0,next,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.TOO_LARGE,null,0,beginning,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.TOO_LARGE,null,DistributedCdcSubscriptions.MAX_POLL_BYTES+1,beginning,cut));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Poll(lineage,DistributedCdcSubscriptions.Outcome.GAP,null,0,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,4)),cut));
    }
    @Test void localStatusDistinguishesPendingClosureAndRetainedConflict() {
        var initial=DistributedCdcSubscription.initial(definition()); var delivered=initial.deliver(initial.delivered().advance(lineage,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1))));
        assertEquals(initial,new DistributedCdcSubscriptions.Status(initial,DistributedCdcSubscriptions.Phase.CREATE).state());
        assertEquals(delivered,new DistributedCdcSubscriptions.Status(delivered,DistributedCdcSubscriptions.Phase.CLOSING).state());
        assertEquals(delivered.close(),new DistributedCdcSubscriptions.Status(delivered.close(),DistributedCdcSubscriptions.Phase.CLOSING).state());
        assertEquals(DistributedCdcSubscriptions.Phase.CONFLICT,new DistributedCdcSubscriptions.Status(initial,DistributedCdcSubscriptions.Phase.CONFLICT).phase());
        assertEquals(delivered.close(),new DistributedCdcSubscriptions.Status(delivered.close(),DistributedCdcSubscriptions.Phase.CLOSED).state());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Status(delivered,DistributedCdcSubscriptions.Phase.CREATE));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Status(delivered.close(),DistributedCdcSubscriptions.Phase.ACTIVE));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Status(delivered,DistributedCdcSubscriptions.Phase.CLOSED));
    }
    @Test void countAndByteBoundsIncludeProjectedOrdinalsAndDefinition() {
        var events=new ArrayList<DistributedCdcRecord.Event>(); for(int i=0;i<DistributedCdcSubscriptions.MAX_EVENTS_PER_ENVELOPE;i++) events.add(event(1,i,column));
        var full=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,1),new byte[32],operation,null,events); assertEquals(2048,full.events().size());
        events.add(event(1,2048,column)); assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,1),new byte[32],operation,null,events));
        var envelopes=new ArrayList<DistributedCdcSubscriptions.Envelope>(); for(int i=1;i<=DistributedCdcSubscriptions.MAX_RECORDS;i++) envelopes.add(envelope(i));
        var cut=new ApplyPosition(1,300); assertEquals(256,batch(beginning,DistributedCdcCheckpoint.Cursor.after(cut),cut,envelopes).envelopes().size());
        envelopes.add(envelope(257)); assertThrows(IllegalArgumentException.class,()->batch(beginning,DistributedCdcCheckpoint.Cursor.after(cut),cut,envelopes));
        var mutation=new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[DistributedCommandCodec.MAX_ENCODED_BYTES-DistributedCommandCodec.HEADER_BYTES-27]);
        var large=new ArrayList<DistributedCdcSubscriptions.Envelope>();
        for(int i=1;i<=5;i++) large.add(new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,i),new byte[32],operation,null,List.of(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,i),0),mutation))));
        var allowed=batch(beginning,DistributedCdcCheckpoint.Cursor.after(cut),cut,large); assertTrue(allowed.budgetBytes()>DistributedCdcSubscriptions.MAX_POLL_BYTES); assertTrue(allowed.budgetBytes()<=DistributedCdcSubscriptions.MAX_BATCH_BYTES);
        large.add(new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,6),new byte[32],operation,null,List.of(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,6),0),mutation))));
        assertThrows(IllegalArgumentException.class,()->batch(beginning,DistributedCdcCheckpoint.Cursor.after(cut),cut,large));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,1),new byte[32],operation,null,List.of(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),0),mutation),new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),1),mutation))));
    }
    @Test void transactionMetadataBindsCommitOperationNamespaceAndPreparedLineage() {
        var participant=new TransactionStatus.Participant(lineage.rangeId(),lineage.groupId(),1,new byte[32],new byte[32],null,false);
        var committed=TransactionStatus.begin(id(1),id(2),id(3),id(60),operation,1,new byte[32],new byte[32],List.of(participant)).prepared(lineage.groupId(),new byte[32],new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
        var transaction=new ParticipantFinalization(id(61),1,lineage.groupId(),new OperationId(id(62),0,0),committed);
        var envelope=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],operation,transaction,List.of(event(2,0,column)));
        assertSame(transaction,batch(beginning,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)),new ApplyPosition(1,3),List.of(envelope)).envelopes().getFirst().transaction());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,2),new byte[32],new OperationId(id(99),0,0),transaction,List.of(event(2,0,column))));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,1),new byte[32],operation,transaction,List.of(event(1,0,column))));
        var otherDefinition=new DistributedCdcSubscription.Definition(id(99),id(2),id(3),id(6),id(7),new byte[32],List.of(column),definition().start());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Batch(id(8),otherDefinition,lineage,beginning,DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2)),new ApplyPosition(1,3),List.of(envelope)));
    }
    static NativeTopologyFreeze freeze(UUID cluster,UUID database,UUID domain,DistributedCdcCheckpoint.Lineage lineage,UUID column) {
        var peer=new ReplicaDescriptor(id(101),id(102),id(103),"barrier","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var parent=new RangeDescriptor(lineage.rangeId(),lineage.groupId(),lineage.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var source=new RangeDirectory(cluster,database,domain,1,new byte[32],1,Set.of(column),1,List.of(parent));
        var left=new RangeDescriptor(id(104),id(105),1,1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var right=new RangeDescriptor(id(106),id(107),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        var target=new RangeDirectory(cluster,database,domain,1,new byte[32],2,Set.of(column),1,List.of(left,right));
        var topology=new TopologyChange.Definition(id(108),source,target);
        return new NativeTopologyFreeze(topology.header(),lineage.groupId(),new ApplyPosition(1,2),NativeTopologyFreeze.boundsDigest(null,null),new byte[32]);
    }
    @Test void terminalBarrierHasNoInventedOperationAndKeepsItsBoundedNativeCut() {
        var freeze=freeze(id(1),id(2),id(3),lineage,column);
        var envelope=new DistributedCdcSubscriptions.Envelope(freeze.position(),freeze.header().schemaDigest(),null,null,List.of(),freeze);
        var end=DistributedCdcCheckpoint.Cursor.after(freeze.position()); var value=batch(beginning,end,new ApplyPosition(1,3),List.of(envelope));
        assertNull(envelope.operation()); assertTrue(envelope.events().isEmpty()); assertEquals(freeze,envelope.terminalBarrier());
        assertEquals(DistributedCdcSubscriptions.ENVELOPE_HEADER_BYTES+freeze.encode().length,envelope.budgetBytes());
        assertEquals(value.envelopes(),batch(beginning,end,new ApplyPosition(1,3),List.of(envelope)).envelopes());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(freeze.position(),new byte[32],operation,null,List.of(),freeze));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(freeze.position(),new byte[32],null,null,List.of(event(2,0,column)),freeze));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(new ApplyPosition(1,3),new byte[32],null,null,List.of(),freeze));
        byte[] foreignSchema=new byte[32]; foreignSchema[0]=1;
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(freeze.position(),foreignSchema,null,null,List.of(),freeze));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Envelope(freeze.position(),new byte[32],null,null,List.of()));
        assertThrows(IllegalArgumentException.class,()->batch(end,end,new ApplyPosition(1,3),List.of(envelope)));
        var foreign=freeze(id(1),id(2),id(99),lineage,column);
        assertThrows(IllegalArgumentException.class,()->batch(beginning,end,new ApplyPosition(1,3),List.of(new DistributedCdcSubscriptions.Envelope(foreign.position(),new byte[32],null,null,List.of(),foreign))));
        var foreignLineage=new DistributedCdcCheckpoint.Lineage(id(98),lineage.groupId(),lineage.rangeEpoch());
        var wrongParent=freeze(id(1),id(2),id(3),foreignLineage,column);
        assertThrows(IllegalArgumentException.class,()->batch(beginning,end,new ApplyPosition(1,3),List.of(new DistributedCdcSubscriptions.Envelope(wrongParent.position(),new byte[32],null,null,List.of(),wrongParent))));
    }

    @Test void successorBatchUsesCommittedCurrentStartsWithoutChangingOriginalDefinition() {
        var child=new DistributedCdcCheckpoint.Lineage(id(104),id(105),2); var activation=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(2,4));
        var starts=new DistributedCdcCheckpoint(Map.of(child,activation)); var next=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(2,5));
        var envelope=new DistributedCdcSubscriptions.Envelope(new ApplyPosition(2,5),new byte[32],operation,null,List.of(new DistributedCdcRecord.Event(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(2,5),0),new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{2}))));
        var batch=new DistributedCdcSubscriptions.Batch(id(8),definition(),child,activation,next,new ApplyPosition(2,6),List.of(envelope),starts);
        assertEquals(definition(),batch.definition()); assertFalse(batch.definition().start().cursors().containsKey(child)); assertEquals(starts,batch.currentStarts());
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Batch(id(8),definition(),child,activation,next,new ApplyPosition(2,6),List.of(envelope)));
        assertThrows(IllegalArgumentException.class,()->new DistributedCdcSubscriptions.Batch(id(8),definition(),child,beginning,next,new ApplyPosition(2,6),List.of(envelope),starts));
        assertEquals(DistributedCdcSubscriptions.BATCH_HEADER_BYTES+definition().encode().length+starts.encode().length+envelope.budgetBytes(),batch.budgetBytes());
    }

}
