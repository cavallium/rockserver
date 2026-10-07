package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionScopeTest {
    final UUID column = UUID.randomUUID(), other = UUID.randomUUID();
    byte[] b(int... values) { byte[] bytes = new byte[values.length]; for (int i=0;i<values.length;i++) bytes[i]=(byte)values[i]; return bytes; }

    @Test void declarationOwnsKeysAndDoesNotGrantOtherColumnsOrRanges() {
        byte[] key = b(128);
        var scope = new TransactionScope(List.of(TransactionScope.Span.point(column, key)));
        key[0]=1; scope.spans().getFirst().lowerInclusive()[0]=2;
        assertTrue(scope.contains(column,b(128)));
        assertFalse(scope.contains(column,b(1)));
        assertFalse(scope.contains(other,b(128)));
        assertFalse(scope.containsRange(column,b(128),b(129)));
        assertThrows(UnsupportedOperationException.class, () -> scope.spans().clear());
    }
    @Test void adjacentRangesCoverAccessButGapsAndExclusiveBoundsDoNot() {
        var scope = new TransactionScope(List.of(TransactionScope.Span.range(column,b(128),b(200)), TransactionScope.Span.range(column,b(1),b(128))));
        assertTrue(scope.containsRange(column,b(1),b(200)));
        assertFalse(scope.containsRange(column,b(0),b(200)));
        assertTrue(scope.contains(column,b(199)));
        assertFalse(scope.contains(column,b(200)));
        var gap = new TransactionScope(List.of(TransactionScope.Span.range(column,b(1),b(128)), TransactionScope.Span.range(column,b(129),b(200))));
        assertFalse(gap.containsRange(column,b(1),b(200)));
    }
    @Test void infiniteRangesAndPrefixCarryRetainPhysicalOrdering() {
        var all = new TransactionScope(List.of(TransactionScope.Span.range(column,null,null)));
        assertTrue(all.containsRange(column,null,null));
        assertTrue(all.contains(column,b()));
        var prefix = new TransactionScope(List.of(TransactionScope.Span.prefix(column,b(127,255))));
        assertTrue(prefix.contains(column,b(127,255,1)));
        assertFalse(prefix.contains(column,b(128)));
        assertArrayEquals(b(128),prefix.spans().getFirst().upperExclusive());
        var last = new TransactionScope(List.of(TransactionScope.Span.prefix(column,b(255))));
        assertTrue(last.contains(column,b(255,255)));
        assertFalse(last.contains(column,b(254,255)));
        assertTrue(new TransactionScope(List.of(TransactionScope.Span.prefix(column,b()))).containsRange(column,b(),null));
    }
    @Test void completeParticipantPlanHonorsBudgetAndColumnIdentity() {
        var replica = new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"node","localhost:1234",ReplicaDescriptor.Role.VOTER);
        var left = new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,0,null,b(128),RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var right = new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),1,0,b(128),null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory = new RangeDirectory(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),1,new byte[32],0,Set.of(column),0,List.of(right,left));
        var point = new TransactionScope(List.of(TransactionScope.Span.point(column,b(1)),TransactionScope.Span.point(column,b(2))));
        assertEquals(List.of(left),point.participants(directory,1));
        var both = new TransactionScope(List.of(TransactionScope.Span.range(column,null,null)));
        assertEquals(List.of(left,right),both.participants(directory,2));
        assertThrows(IllegalArgumentException.class, () -> both.participants(directory,1));
        assertThrows(IllegalArgumentException.class, () -> new TransactionScope(List.of(TransactionScope.Span.point(other,b(1)))).participants(directory,2));
    }
    @Test void scopeLimitsRejectOverlargeDeclarationsBeforeParticipantPlanning() {
        var point = TransactionScope.Span.point(column,b(1));
        assertThrows(IllegalArgumentException.class, () -> new TransactionScope(List.of()));
        assertThrows(IllegalArgumentException.class, () -> new TransactionScope(Collections.nCopies(TransactionScope.MAX_SPANS+1,point)));
        var large = TransactionScope.Span.point(column,new byte[TransactionScope.MAX_BOUND_BYTES/2+1]);
        assertThrows(IllegalArgumentException.class, () -> new TransactionScope(List.of(large,large)));
        assertThrows(IllegalArgumentException.class, () -> TransactionScope.Span.range(column,b(128),b(127)));
    }

}
