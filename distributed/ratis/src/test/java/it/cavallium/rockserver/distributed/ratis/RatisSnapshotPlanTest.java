package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSnapshotPlanTest {
    final UUID column=new UUID(0,1),other=new UUID(0,2),session=new UUID(0,3);
    RangeDirectory directory(int groups,int prefix,RangeDescriptor.TopologyState state) {
        var replica=new ReplicaDescriptor(new UUID(0,10),new UUID(0,11),new UUID(0,12),"peer","localhost:12345",ReplicaDescriptor.Role.VOTER); var routes=new ArrayList<RangeDescriptor>();
        for (int n=0;n<groups;n++) routes.add(new RangeDescriptor(new UUID(0,100+n),new UUID(n==groups-1 ? Long.MIN_VALUE : 0,200+n),1,0,n==0 ? null : new byte[]{(byte)n},n==groups-1 ? null : new byte[]{(byte)(n+1)},state,List.of(replica)));
        return new RangeDirectory(new UUID(0,20),new UUID(0,21),new UUID(0,22),3,new byte[32],4,Set.of(column,other),prefix,routes);
    }
    RatisSnapshotPlan plan(RangeDirectory directory,TransactionScope scope) { return RatisSnapshotPlan.create(directory,scope,new UUID(0,30),new OperationId(session,1,0),new UUID(0,31),1,new OperationId(session,1,1)); }
    @Test void completePhysicalScopeClipsAcrossColumnsAndUnsignedGroupOrderAndReconstructsUnion() {
        var directory=directory(2,0,RangeDescriptor.TopologyState.ACTIVE);
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,null,null),TransactionScope.Span.range(column,new byte[]{0},new byte[]{2}),TransactionScope.Span.point(other,new byte[]{(byte)255})));
        var plan=plan(directory,scope); assertEquals(2,plan.routes().size()); assertSame(scope,plan.scope()); assertEquals(directory.generation(),plan.fence().directoryGeneration()); assertEquals(directory.schemaEpoch(),plan.fence().schemaEpoch());
        var first=plan.fence().participants().getFirst(); assertEquals(new UUID(0,200),first.groupId()); assertEquals(1,first.installOperation().sequence()); assertEquals(65,first.releaseOperation().sequence());
        assertArrayEquals(new byte[]{1},first.scope().spans().getFirst().upperExclusive());
        var second=plan.fence().participants().getLast(); assertEquals(Long.MIN_VALUE,second.groupId().getMostSignificantBits()); assertEquals(2,second.installOperation().sequence()); assertEquals(66,second.releaseOperation().sequence()); assertTrue(second.scope().contains(other,new byte[]{(byte)255}));
        var union=RatisSnapshotPlan.scope(SnapshotFence.decode(plan.fence().encode())); assertEquals(2,union.spans().size()); assertTrue(union.containsRange(column,null,null)); assertTrue(union.contains(other,new byte[]{(byte)255})); assertThrows(UnsupportedOperationException.class,() -> plan.routes().clear());
    }
    @Test void prefixRoutingKeepsWholePhysicalBucketPointsAndClipsPartiallyIncludedUpperPrefix() {
        var directory=directory(2,1,RangeDescriptor.TopologyState.ACTIVE); byte[] bucket={1,7,8,9};
        var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,bucket),TransactionScope.Span.range(other,new byte[]{0,9},new byte[]{1,2})));
        var plan=plan(directory,scope); bucket[0]=0;
        var right=plan.fence().participant(directory.ranges().getLast().groupId()); assertTrue(right.scope().contains(column,new byte[]{1,7,8,9})); assertFalse(right.scope().contains(column,new byte[]{1,7,8,10}));
        assertTrue(right.scope().containsRange(other,new byte[]{1},new byte[]{1,2})); assertTrue(plan.fence().participant(directory.ranges().getFirst().groupId()).scope().containsRange(other,new byte[]{0,9},new byte[]{1}));
        assertThrows(IllegalArgumentException.class,() -> plan(directory,new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[0])))));
    }
    @Test void participantIdentityOverflowAndAggregateBoundsAreRejectedBeforeAcquisition() {
        var all=new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))); assertThrows(IllegalArgumentException.class,() -> plan(directory(65,0,RangeDescriptor.TopologyState.ACTIVE),all));
        assertThrows(IllegalStateException.class,() -> plan(directory(2,0,RangeDescriptor.TopologyState.FENCED),all)); var directory=directory(2,0,RangeDescriptor.TopologyState.ACTIVE);
        for (var first:List.of(new OperationId(session,1,0),new OperationId(session,2,1),new OperationId(new UUID(0,99),1,1),new OperationId(session,1,Long.MAX_VALUE-255))) assertThrows(IllegalArgumentException.class,() -> RatisSnapshotPlan.create(directory,all,new UUID(0,30),new OperationId(session,1,0),new UUID(0,31),1,first));
        var large=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[TransactionScope.MAX_BOUND_BYTES]))); assertThrows(IllegalArgumentException.class,() -> plan(directory,large));
        var columns=new ArrayList<TransactionScope.Span>(); for (int n=0;n<256;n++) columns.add(TransactionScope.Span.range(column,null,null)); var many=plan(directory,new TransactionScope(columns)); assertEquals(1,RatisSnapshotPlan.scope(many.fence()).spans().size());
    }
    @Test void sixtyFourParticipantsLeaveEnoughDistinctHomeControlsForEveryReportDecisionAndConsumption() {
        var plan=plan(directory(64,0,RangeDescriptor.TopologyState.ACTIVE),new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))));
        assertEquals(64,plan.fence().participants().size()); var controls=new HashSet<OperationId>(); controls.add(plan.fence().operation());
        for (var participant:plan.fence().participants()) { assertTrue(controls.add(participant.installOperation())); assertTrue(controls.add(participant.releaseOperation())); }
        assertTrue(RatisSnapshotPlan.CONTROL_SLOTS-128>2*plan.fence().participants().size()+3);
        var directory=directory(64,0,RangeDescriptor.TopologyState.ACTIVE);
        assertDoesNotThrow(() -> RatisSnapshotPlan.create(directory,new TransactionScope(List.of(TransactionScope.Span.range(column,null,null))),new UUID(0,30),new OperationId(session,1,0),new UUID(0,31),1,new OperationId(session,1,Long.MAX_VALUE-RatisSnapshotPlan.CONTROL_SLOTS)));
    }
    @Test void frozenQualificationPreservesAdjacentSpansAndRejectsChangedAuthority() {
        var directory=directory(1,0,RangeDescriptor.TopologyState.ACTIVE);
        var original=plan(directory,new TransactionScope(List.of(
                TransactionScope.Span.range(column,new byte[]{1},new byte[]{2}),
                TransactionScope.Span.range(column,new byte[]{2},new byte[]{4}),
                TransactionScope.Span.point(other,new byte[]{9}))));
        var frozen=RatisSnapshotPlan.frozen(directory,original.fence());
        assertSame(original.fence(),frozen.fence());
        assertArrayEquals(original.fence().encode(),frozen.fence().encode());
        assertEquals(original.routes(),frozen.routes());
        assertEquals(2,frozen.scope().spans().size());
        var fence=original.fence(); var p=fence.participants().getFirst();
        var changed=new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),
                fence.homeGroup(),fence.homeEpoch(),fence.operation(),fence.directoryGeneration(),fence.schemaEpoch(),fence.schemaDigest(),
                List.of(new SnapshotFence.Participant(p.rangeId(),p.groupId(),p.rangeEpoch(),p.schemaDigest(),p.installOperation(),
                        new OperationId(p.releaseOperation().sessionId(),p.releaseOperation().epoch(),p.releaseOperation().sequence()+1),p.scope())));
        assertThrows(IllegalArgumentException.class,() -> RatisSnapshotPlan.frozen(directory,changed));
        var newer=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),
                directory.schemaDigest(),directory.generation()+1,directory.columns(),directory.routingPrefixBytes(),directory.ranges());
        assertThrows(IllegalArgumentException.class,() -> RatisSnapshotPlan.frozen(newer,fence));
    }

}
