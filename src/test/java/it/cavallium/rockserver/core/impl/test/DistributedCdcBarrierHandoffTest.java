package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DistributedCdcBarrierHandoffTest {
    @Test void splitRequiresExplicitParentAckPreservesUnrelatedFrontierAndRejectsDelayedOldEpoch() {
        var helper=new TopologyChangeTest(); var topology=helper.split(helper.owners()); var active=helper.activate(helper.seed(helper.freeze(topology,false),false).publish(),false);
        var handoff=DistributedCdcBarrierHandoff.from(topology,active); assertEquals(handoff,DistributedCdcBarrierHandoff.decode(handoff.encode())); assertTrue(handoff.encode().length<=DistributedCdcBarrierHandoff.MAX_BYTES);
        var starts=new TreeMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>(); for(var range:topology.sourceDirectory().ranges()) starts.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning());
        var definition=DistributedCdcSubscription.Definition.create(TopologyChangeTest.id(900),TopologyChangeTest.id(901),topology.sourceDirectory(),topology.sourceDirectory().columns(),new DistributedCdcCheckpoint(starts)); var initial=DistributedCdcSubscription.initial(definition);
        assertThrows(IllegalArgumentException.class,()->initial.lineageTransition(handoff)); var parent=handoff.parentTerminal().cursors().entrySet().iterator().next(); var delivered=initial.deliver(initial.delivered().advance(parent.getKey(),parent.getValue())); assertThrows(IllegalArgumentException.class,()->delivered.lineageTransition(handoff));
        var ack=delivered.acknowledge(delivered.delivered()); var next=ack.lineageTransition(handoff); next.requireSuccessorOf(ack); assertEquals(next,DistributedCdcSubscription.decode(next.encode())); assertEquals(definition,next.definition()); assertEquals(handoff,next.latestHandoff());
        assertEquals(4,next.checkpoint().cursors().size()); assertFalse(next.currentStarts().cursors().containsKey(parent.getKey())); for(var entry:handoff.childStarts().cursors().entrySet()) { assertEquals(entry.getValue(),next.currentStarts().cursors().get(entry.getKey())); assertEquals(entry.getValue(),next.delivered().cursors().get(entry.getKey())); }
        assertThrows(IllegalArgumentException.class,()->next.acknowledge(ack.checkpoint())); assertThrows(IllegalArgumentException.class,()->next.deliver(ack.delivered())); assertThrows(IllegalArgumentException.class,()->next.lineageTransition(handoff));
        assertThrows(IllegalArgumentException.class,()->next.close().requireSuccessorOf(ack)); assertEquals(next.currentStarts(),next.close().currentStarts());
        var malformed=handoff.encode(); malformed[0]=127; assertThrows(IllegalArgumentException.class,()->DistributedCdcBarrierHandoff.decode(malformed)); assertThrows(IllegalArgumentException.class,()->DistributedCdcBarrierHandoff.from(topology,TopologyChange.State.begin(topology.header()).abort()));
    }
    @Test void mergeWaitsBothParentAcksAndOwnChildActivationCutIsNotParentCut() {
        var helper=new TopologyChangeTest(); var split=helper.split(helper.owners()); var left=split.targetDirectory().ranges().getFirst(); var right=split.targetDirectory().ranges().getLast();
        var merged=helper.range(500,501,1,helper.bound(64),helper.bound(192),helper.owners()); var target=helper.directory(10,List.of(left,merged,right)); var topology=new TopologyChange.Definition(TopologyChangeTest.id(502),split.targetDirectory(),target);
        var active=helper.activate(helper.seed(helper.freeze(topology,false),false).publish(),false); var handoff=DistributedCdcBarrierHandoff.from(topology,active);
        var starts=new TreeMap<DistributedCdcCheckpoint.Lineage,DistributedCdcCheckpoint.Cursor>(); for(var range:topology.sourceDirectory().ranges()) starts.put(new DistributedCdcCheckpoint.Lineage(range.rangeId(),range.groupId(),range.rangeEpoch()),DistributedCdcCheckpoint.Cursor.beginning());
        var initial=DistributedCdcSubscription.initial(DistributedCdcSubscription.Definition.create(TopologyChangeTest.id(900),TopologyChangeTest.id(901),topology.sourceDirectory(),topology.sourceDirectory().columns(),new DistributedCdcCheckpoint(starts)));
        var entries=new ArrayList<>(handoff.parentTerminal().cursors().entrySet()); var first=initial.deliver(initial.delivered().advance(entries.getFirst().getKey(),entries.getFirst().getValue())); var partial=first.acknowledge(first.delivered()); assertThrows(IllegalArgumentException.class,()->partial.lineageTransition(handoff));
        var all=partial.deliver(partial.delivered().advance(entries.getLast().getKey(),entries.getLast().getValue())); var ack=all.acknowledge(all.delivered()); var next=ack.lineageTransition(handoff); next.requireSuccessorOf(ack);
        assertEquals(new ApplyPosition(1,1),handoff.childStarts().cursors().values().iterator().next().position()); assertEquals(new ApplyPosition(7,500),handoff.parentTerminal().cursors().values().iterator().next().position()); assertEquals(3,next.checkpoint().cursors().size());
        assertThrows(IllegalArgumentException.class,()->DistributedCdcBarrierHandoff.from(topology,helper.seed(helper.freeze(topology,false),false).publish()));
    }
}
