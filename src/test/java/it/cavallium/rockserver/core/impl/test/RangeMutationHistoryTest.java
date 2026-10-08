package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RangeMutationHistoryTest {
    private static final UUID COLUMN=new UUID(0,1), OTHER=new UUID(0,2);
    private static byte[] b(int... values) { byte[] bytes=new byte[values.length]; for (int i=0;i<values.length;i++) bytes[i]=(byte)values[i]; return bytes; }
    private static ApplyPosition p(long index) { return new ApplyPosition(1,index); }
    private static TransactionScope.Span point(int... key) { return TransactionScope.Span.point(COLUMN,b(key)); }
    private static TransactionScope scope(TransactionScope.Span... spans) { return new TransactionScope(List.of(spans)); }
    private static TransactionScope all() { return scope(TransactionScope.Span.range(COLUMN,null,null)); }
    private static RangeMutationHistory limited(int pins,int entries,long bytes,long scopes) {
        return new RangeMutationHistory(new RangeMutationHistory.Limits(pins,entries,bytes,scopes));
    }

    @Test void abaSameValueAndMissingDeleteAreWritesWhileDisjointPinsRemainActive() {
        var history=new RangeMutationHistory(); var changed=history.open(p(0),scope(point(1))); var disjoint=history.open(p(0),scope(point(2)));
        assertTrue(history.advance(p(1),List.of(point(1))).isEmpty());
        history.advance(p(2),List.of(point(1))); // Restoring a value, rewriting it, or deleting absence still stamps this physical key.
        assertEquals(1,history.stats().entries()); assertEquals(129,history.stats().historyBytes());
        assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(changed)); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(disjoint));
        history.close(changed); assertEquals(RangeMutationHistory.Status.CONFLICT,changed.status());
        history.close(disjoint); assertEquals(RangeMutationHistory.Status.CLOSED,disjoint.status()); assertEquals(new RangeMutationHistory.Stats(0,0,0,0),history.stats());
    }

    @Test void fullDeclaredScopeDetectsPhantomsAndPhysicalBucketChanges() {
        var history=new RangeMutationHistory();
        var pin=history.open(p(0),scope(point(5),TransactionScope.Span.range(COLUMN,b(10),b(20))));
        history.advance(p(1),List.of(point(15))); history.advance(p(2),List.of(point(15)));
        assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(pin),"Insert then delete in the declared scan conflicts even if the application read only its point");
        var bucket=history.open(p(2),scope(point(99))); history.advance(p(3),List.of(point(99)));
        assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(bucket),"Different logical bucket members share one physical history key");
    }

    @Test void rangesUseUnsignedInclusiveLowerExclusiveUpperAndPrefixCoverage() {
        var history=new RangeMutationHistory(); var prefix=history.open(p(0),scope(TransactionScope.Span.prefix(COLUMN,b(128))));
        history.advance(p(1),List.of(point(127,255),point(129),TransactionScope.Span.point(OTHER,b(128))));
        assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(prefix));
        history.advance(p(2),List.of(point(128,255))); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(prefix));
        var high=history.open(p(2),scope(TransactionScope.Span.prefix(COLUMN,b(255))));
        history.advance(p(3),List.of(point(254,255))); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(high));
        history.advance(p(4),List.of(point(255,0))); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(high));
        var bounds=history.open(p(4),scope(TransactionScope.Span.range(COLUMN,b(10),b(20))));
        history.advance(p(5),List.of(point(20))); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(bounds));
        history.advance(p(6),List.of(point(10))); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(bounds));
        var infinity=history.open(p(6),all()); history.advance(p(7),List.of(point())); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(infinity));
    }

    @Test void overwrittenLatestStampsCompactAndIndexGcPrunesOnlyPastOldestActiveCut() {
        var history=new RangeMutationHistory(); var old=history.open(p(0),all()); history.advance(p(1),List.of(point(1),point(1)));
        var middle=history.open(p(1),all()); history.advance(p(2),List.of(point(1))); var newest=history.open(p(2),all());
        assertEquals(1,history.stats().entries()); assertEquals(129,history.stats().historyBytes());
        history.close(old); assertEquals(1,history.stats().entries()); history.close(middle);
        assertEquals(0,history.stats().entries()); assertEquals(0,history.stats().historyBytes()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(newest));
        history.advance(p(3),List.of(point(2))); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(newest)); assertEquals(0,history.stats().entries());
    }

    @Test void pressureExpiresOldestPinsRepeatedlyWithoutRejectingCommittedWrites() {
        var history=limited(4,1,1024,2048); var first=history.open(p(0),all()); var second=history.open(p(0),all()); history.advance(p(1),List.of(point(1)));
        var recent=history.open(p(1),all()); var retired=history.advance(p(2),List.of(point(2)));
        assertEquals(List.of(first,second),retired); assertEquals(RangeMutationHistory.Status.EXPIRED,first.status()); assertEquals(RangeMutationHistory.Status.EXPIRED,second.status());
        assertEquals(RangeMutationHistory.Status.ACTIVE,recent.status()); assertEquals(new RangeMutationHistory.Stats(1,1,129,192),history.stats());
        assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(recent));
        var tiny=limited(2,10,128,1024); var a=tiny.open(p(0),all()); var c=tiny.open(p(0),scope(point(7)));
        assertEquals(List.of(a,c),tiny.advance(p(1),List.of(point(1)))); assertEquals(RangeMutationHistory.Status.EXPIRED,a.status());
        assertEquals(new RangeMutationHistory.Stats(0,0,0,0),tiny.stats());
        var noPins=new RangeMutationHistory(); assertTrue(noPins.advance(p(100),List.of(point(1))).isEmpty()); assertEquals(0,noPins.stats().entries());
    }

    @Test void pinAndScopeAdmissionRefuseBeforeCaptureAndNeverEvictExistingPins() {
        var one=limited(1,10,1024,1024); var pin=one.open(p(0),scope(point(1))); var before=one.stats();
        assertThrows(IllegalStateException.class,() -> one.checkOpenCapacity(all())); assertThrows(IllegalStateException.class,() -> one.open(p(0),all()));
        assertEquals(before,one.stats()); assertEquals(RangeMutationHistory.Status.ACTIVE,pin.status());
        var scopes=limited(4,10,1024,193); scopes.checkOpenCapacity(scope(point(1))); var scoped=scopes.open(p(0),scope(point(1)));
        assertEquals(193,scopes.stats().scopeBytes()); assertThrows(IllegalStateException.class,() -> scopes.checkOpenCapacity(scope(point(2))));
        assertEquals(RangeMutationHistory.Status.ACTIVE,scoped.status()); scopes.close(scoped); scopes.checkOpenCapacity(all());
        for (var invalid:List.of(new int[] {0,1},new int[] {4097,1},new int[] {1,0},new int[] {1,1_048_577})) {
            assertThrows(IllegalArgumentException.class,() -> new RangeMutationHistory.Limits(invalid[0],invalid[1],1024,1024));
        }
        assertThrows(IllegalArgumentException.class,() -> new RangeMutationHistory.Limits(1,1,0,1));
        assertThrows(IllegalArgumentException.class,() -> new RangeMutationHistory.Limits(1,1,1,256L*1024*1024+1));
    }

    @Test void termAdvanceRangeDeletionResetAndForeignOwnershipHaveExplicitTerminalReasons() {
        var history=new RangeMutationHistory(); var pin=history.open(p(0),all()); history.advance(p(1),List.of(point(1)));
        assertThrows(IllegalArgumentException.class,() -> history.open(new ApplyPosition(2,2),all())); assertEquals(RangeMutationHistory.Status.ACTIVE,pin.status());
        assertEquals(List.of(pin),history.advance(new ApplyPosition(2,2),List.of())); assertEquals(RangeMutationHistory.Status.INVALIDATED,pin.status());
        history.close(pin); assertEquals(RangeMutationHistory.Status.INVALIDATED,history.validate(pin));
        var next=history.open(new ApplyPosition(2,2),all());
        assertEquals(List.of(next),history.advance(new ApplyPosition(2,3),List.of(TransactionScope.Span.range(OTHER,b(1),b(2)))));
        assertEquals(RangeMutationHistory.Status.CONFLICT,next.status()); assertEquals(0,history.stats().entries());
        var reset=history.open(new ApplyPosition(2,3),all()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(reset));
        assertEquals(List.of(reset),history.reset()); assertEquals(RangeMutationHistory.Status.INVALIDATED,reset.status());
        var fresh=history.open(p(0),all()); var foreign=new RangeMutationHistory();
        assertThrows(IllegalArgumentException.class,() -> foreign.validate(fresh)); assertThrows(IllegalArgumentException.class,() -> foreign.close(fresh));
        assertThrows(IllegalArgumentException.class,() -> history.invalidateAll(RangeMutationHistory.Status.ACTIVE));
        assertEquals(List.of(fresh),history.invalidateAll(RangeMutationHistory.Status.CLOSED)); assertEquals(RangeMutationHistory.Status.CLOSED,fresh.status());
        assertDoesNotThrow(() -> history.open(new ApplyPosition(3,10),all()));
    }

    @Test void prewriteFootprintsOwnKeysRemainUnpublishedAndCannotCrossHistoryOwners() {
        var history=new RangeMutationHistory(); var pin=history.open(p(0),scope(point(1))); byte[] key=b(1);
        var actual=new ArrayList<>(List.of(TransactionScope.Span.point(COLUMN,key),point(1))); var before=history.stats();
        var prepared=history.prepare(actual); key[0]=2; actual.clear();
        assertEquals(before,history.stats()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(pin));
        assertThrows(IllegalArgumentException.class,() -> new RangeMutationHistory().advance(p(1),prepared));
        history.advance(p(1),prepared); assertEquals(1,history.stats().entries()); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(pin));
        var empty=history.prepare(List.of()); var next=history.open(p(1),all()); history.advance(p(2),empty);
        assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(next)); assertEquals(0,history.stats().entries());
        assertThrows(IllegalArgumentException.class,() -> history.prepare(Arrays.asList(point(1),null)));
    }

    @Test void invalidAdvanceArgumentsPublishNothingAndScopeOwnsItsBounds() {
        byte[] key=b(1); var declaration=scope(TransactionScope.Span.point(COLUMN,key)); var history=new RangeMutationHistory(); var pin=history.open(p(0),declaration);
        key[0]=2; pin.scope().spans().getFirst().lowerInclusive()[0]=3; assertTrue(pin.scope().contains(COLUMN,b(1)));
        assertThrows(UnsupportedOperationException.class,() -> pin.scope().spans().clear()); var before=history.stats();
        assertThrows(IllegalArgumentException.class,() -> history.advance(p(1),Arrays.asList(point(1),null)));
        assertThrows(IllegalArgumentException.class,() -> history.advance(p(0),List.of(point(1))));
        assertThrows(IllegalArgumentException.class,() -> history.advance(new ApplyPosition(0,1),List.of(point(1))));
        assertEquals(before,history.stats()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(pin));
        history.advance(p(1),List.of()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate(pin)); assertEquals(0,history.stats().entries());
        assertThrows(IllegalArgumentException.class,() -> history.open(p(0),all()));
        history.advance(p(2),List.of(point(1))); assertEquals(RangeMutationHistory.Status.CONFLICT,history.validate(pin));
    }
}
