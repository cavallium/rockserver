package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ProposalFootprintsTest {
    private static final UUID COLUMN=new UUID(0,1), OTHER=new UUID(0,2);
    private static byte[] b(int... values) { byte[] bytes=new byte[values.length]; for (int i=0;i<values.length;i++) bytes[i]=(byte)values[i]; return bytes; }
    private static TransactionScope.Span point(int... key) { return TransactionScope.Span.point(COLUMN,b(key)); }
    private static TransactionScope.Span range(byte[] lower,byte[] upper) { return TransactionScope.Span.range(COLUMN,lower,upper); }
    private static ProposalFootprints index() { return new ProposalFootprints(ProposalFootprints.Limits.defaults()); }
    private static ProposalFootprints.PreparedFootprint fp(ProposalFootprints index,TransactionScope.Span... spans) { return index.prepare(List.of(spans)); }

    @Test void ordinaryProposalsPipelineAndCandidatesExcludeBothKindsInEitherDirection() {
        var index=index(); var key=fp(index,point(1)); var first=index.registerOrdinary(key,1,0); var second=index.registerOrdinary(key,1,0);
        assertNotNull(first); assertNotNull(second); assertNull(index.reserveCandidate(key,1,0));
        index.release(first); assertNull(index.reserveCandidate(key,1,0)); index.release(second);
        var candidate=index.reserveCandidate(key,2,3); assertNotNull(candidate); assertEquals(2,candidate.term()); assertEquals(3,candidate.generation()); assertSame(key,candidate.footprint());
        assertNull(index.registerOrdinary(key,2,3)); assertNull(index.reserveCandidate(key,2,3));
        var disjoint=index.registerOrdinary(fp(index,point(2)),2,3); assertNotNull(disjoint);
        index.release(candidate); assertNotNull(index.reserveCandidate(key,2,3)); assertTrue(disjoint.active());
    }

    @Test void unsignedHalfOpenRangesPrefixesEmptyKeysAndColumnsHaveExactOverlap() {
        var index=index(); var candidate=index.reserveCandidate(fp(index,range(b(128),b(200))),1,0);
        assertNotNull(index.registerOrdinary(fp(index,point(127,255)),1,0)); assertNotNull(index.registerOrdinary(fp(index,point(200)),1,0));
        assertNotNull(index.registerOrdinary(fp(index,TransactionScope.Span.point(OTHER,b(150))),1,0));
        assertNull(index.registerOrdinary(fp(index,point(128)),1,0)); assertNull(index.registerOrdinary(fp(index,range(b(199),b(201))),1,0));
        assertNotNull(index.registerOrdinary(fp(index,range(b(1),b(128))),1,0)); index.release(candidate);
        var empty=index.reserveCandidate(fp(index,point()),1,0); assertNotNull(empty);
        assertNull(index.registerOrdinary(fp(index,range(null,b(0))),1,0)); assertNotNull(index.registerOrdinary(fp(index,range(b(0),b(1))),1,0));
        assertTrue(fp(index,range(null,b())).isEmpty()); index.release(empty);
        var high=index.reserveCandidate(fp(index,TransactionScope.Span.prefix(COLUMN,b(255))),1,0); assertNotNull(high);
        assertNull(index.registerOrdinary(fp(index,point(255,0)),1,0)); assertNotNull(index.registerOrdinary(fp(index,point(254,255)),1,0));
        assertNull(index.reserveCandidate(fp(index,range(null,null)),1,0)); index.release(high);
    }

    @Test void normalizationOwnsBoundsAndPreservesUnionWithCoveredAndDuplicatePointsRemoved() {
        var index=index(); byte[] lower=b(1), upper=b(5), key=b(10);
        var footprint=index.prepare(List.of(range(lower,b(3)),range(b(3),upper),range(b(2),b(4)),point(2),point(10),TransactionScope.Span.point(COLUMN,key)));
        lower[0]=99; upper[0]=99; key[0]=99;
        assertEquals(2,footprint.spans().size()); footprint.spans().getFirst().lowerInclusive()[0]=99;
        assertThrows(UnsupportedOperationException.class,() -> footprint.spans().clear());
        var owner=index.reserveCandidate(footprint,1,0); assertEquals(new ProposalFootprints.Stats(0,1,2,259),index.stats());
        assertNull(index.registerOrdinary(fp(index,point(1)),1,0)); assertNull(index.registerOrdinary(fp(index,point(4)),1,0));
        assertNotNull(index.registerOrdinary(fp(index,point(5)),1,0)); assertNull(index.registerOrdinary(fp(index,point(10)),1,0)); index.release(owner);
        var all=index.prepare(new TransactionScope(List.of(range(null,b(2)),range(b(1),null),point(5)))); assertEquals(1,all.spans().size());
        assertNull(all.spans().getFirst().lowerInclusive()); assertNull(all.spans().getFirst().upperExclusive());
        var empty=fp(index); assertTrue(empty.isEmpty()); assertNotNull(index.reserveCandidate(empty,1,0)); assertNotNull(index.registerOrdinary(empty,1,0));
    }

    @Test void independentQuotasRejectWithoutIndexMutationAndOldCallbacksCannotReleaseReplacement() {
        var index=new ProposalFootprints(new ProposalFootprints.Limits(1,1,2,1024)); var first=index.registerOrdinary(fp(index,point(1)),1,0); var before=index.stats();
        assertThrows(IllegalStateException.class,() -> index.registerOrdinary(fp(index,point(2)),1,0)); assertEquals(before,index.stats());
        var candidate=index.reserveCandidate(fp(index,point(2)),1,0); before=index.stats();
        assertThrows(IllegalStateException.class,() -> index.reserveCandidate(fp(index,point(3)),1,0)); assertEquals(before,index.stats());
        index.release(first); var replacement=index.registerOrdinary(fp(index,point(1)),2,1); index.release(first); assertTrue(replacement.active());
        assertNull(index.reserveCandidate(fp(index,point(1)),2,1)); index.release(candidate); var next=index.reserveCandidate(fp(index,point(2)),2,1);
        index.release(candidate); assertTrue(next.active()); assertNull(index.registerOrdinary(fp(index,point(2)),2,1));
        var spans=new ProposalFootprints(new ProposalFootprints.Limits(4,4,1,1024)); var one=spans.registerOrdinary(fp(spans,point(1)),1,0);
        assertThrows(IllegalStateException.class,() -> spans.reserveCandidate(fp(spans,point(2)),1,0)); assertEquals(1,spans.stats().spans()); spans.release(one);
        var bytes=new ProposalFootprints(new ProposalFootprints.Limits(4,4,4,193)); bytes.registerOrdinary(fp(bytes,point(1)),1,0);
        assertEquals(193,bytes.stats().bytes()); assertThrows(IllegalStateException.class,() -> bytes.reserveCandidate(fp(bytes),1,0));
    }

    @Test void candidateInvalidationRetainsOrdinaryOwnershipAndForeignObjectsAreRejected() {
        var index=index(); var ordinary=index.registerOrdinary(fp(index,point(1)),1,0); var candidate=index.reserveCandidate(fp(index,point(2)),1,0);
        index.invalidateCandidates(); assertFalse(candidate.active()); assertTrue(ordinary.active()); assertEquals(new ProposalFootprints.Stats(1,0,1,193),index.stats());
        assertNull(index.reserveCandidate(fp(index,point(1)),2,1)); var replacement=index.reserveCandidate(fp(index,point(2)),2,1); index.release(candidate); assertTrue(replacement.active());
        var foreign=index(); assertThrows(IllegalArgumentException.class,() -> foreign.release(ordinary)); assertThrows(IllegalArgumentException.class,() -> foreign.release(candidate));
        assertThrows(IllegalArgumentException.class,() -> foreign.reserveCandidate(fp(index,point(3)),1,0));
        assertThrows(IllegalArgumentException.class,() -> index.registerOrdinary(fp(index),0,0)); assertThrows(IllegalArgumentException.class,() -> index.reserveCandidate(fp(index),1,-1));
        index.release(ordinary); index.release(replacement); assertEquals(new ProposalFootprints.Stats(0,0,0,0),index.stats());
        assertThrows(IllegalArgumentException.class,() -> index.prepare(Collections.nCopies(257,point(1))));
        assertThrows(IllegalArgumentException.class,() -> index.prepare(Arrays.asList(point(1),null)));
        assertThrows(IllegalArgumentException.class,() -> index.prepare(List.of(TransactionScope.Span.point(COLUMN,new byte[TransactionScope.MAX_BOUND_BYTES]),point(1))));
        assertThrows(IllegalArgumentException.class,() -> new ProposalFootprints.Limits(0,1,1,1));
        assertThrows(IllegalArgumentException.class,() -> new ProposalFootprints.Limits(1,0,1,1));
        assertThrows(IllegalArgumentException.class,() -> new ProposalFootprints.Limits(1,1,0,1));
        assertThrows(IllegalArgumentException.class,() -> new ProposalFootprints.Limits(1,1,1,0));
    }

    @Test void orderedRangeIndexMatchesOracleForExplicitInsertionAndRemovalOrders() {
        var index=new ProposalFootprints(new ProposalFootprints.Limits(128,16,1024,1024*1024));
        var ordinary=new ArrayList<ProposalFootprints.OrdinaryLease>(); var candidates=new ArrayList<ProposalFootprints.CandidateLease>();
        for (int i=0;i<32;i++) ordinary.add(index.registerOrdinary(index.prepare(List.of(TransactionScope.Span.range(OTHER,b(2*i),b(2*i+1)))),1,0));
        for (int i=31;i>=0;i--) ordinary.add(index.registerOrdinary(index.prepare(List.of(TransactionScope.Span.point(COLUMN,b(2*i)))),1,0));
        candidates.add(index.reserveCandidate(index.prepare(List.of(TransactionScope.Span.range(COLUMN,b(100),b(120)))),1,0));
        for (int phase=0;phase<3;phase++) {
            for (UUID column:List.of(COLUMN,OTHER)) for (int key=0;key<66;key++) {
                for (var span:List.of(TransactionScope.Span.point(column,b(key)),TransactionScope.Span.range(column,b(key),b(key+1)))) {
                    var footprint=index.prepare(List.of(span));
                    boolean blocked=ordinary.stream().filter(ProposalFootprints.OrdinaryLease::active).anyMatch(lease -> overlaps(footprint,lease.footprint()))
                            || candidates.stream().filter(ProposalFootprints.CandidateLease::active).anyMatch(lease -> overlaps(footprint,lease.footprint()));
                    var lease=index.reserveCandidate(footprint,1,phase); assertEquals(blocked,lease==null);
                    if (lease!=null) { candidates.add(lease); assertAccounting(index,ordinary,candidates); index.release(lease); index.release(lease); }
                    assertAccounting(index,ordinary,candidates);
                }
            }
            if (phase==0) for (int i=0;i<ordinary.size();i+=2) index.release(ordinary.get(i));
            else if (phase==1) { for (int i=ordinary.size()-1;i>=0;i--) index.release(ordinary.get(i)); index.invalidateCandidates(); }
        }
        ordinary.forEach(index::release); candidates.forEach(index::release); assertEquals(new ProposalFootprints.Stats(0,0,0,0),index.stats());
    }
    @Test void submittedExclusiveLeaseSurvivesGenerationInvalidationUntilItsExactRelease() {
        var index=index(); var accepted=index.reserveCandidate(fp(index,point(1)),1,0); var pending=index.reserveCandidate(fp(index,point(2)),1,0);
        index.submit(accepted); index.submit(accepted); index.invalidateCandidates();
        assertTrue(accepted.active()); assertFalse(pending.active()); assertEquals(1,index.stats().candidates());
        assertNull(index.registerOrdinary(fp(index,point(1)),2,1)); assertNull(index.reserveCandidate(fp(index,point(1)),2,1));
        index.release(accepted); index.release(accepted); assertEquals(new ProposalFootprints.Stats(0,0,0,0),index.stats());
        assertThrows(IllegalStateException.class,() -> index.submit(accepted)); assertThrows(IllegalArgumentException.class,() -> index().submit(pending));
    }
    private static void assertAccounting(ProposalFootprints index,List<ProposalFootprints.OrdinaryLease> ordinary,List<ProposalFootprints.CandidateLease> candidates) {
        int owners=0, exclusive=0, count=0; long bytes=0;
        for (var lease:ordinary) if (lease.active()) { owners++; count+=lease.footprint().spans().size(); bytes+=accounted(lease.footprint()); }
        for (var lease:candidates) if (lease.active()) { exclusive++; count+=lease.footprint().spans().size(); bytes+=accounted(lease.footprint()); }
        assertEquals(new ProposalFootprints.Stats(owners,exclusive,count,bytes),index.stats());
    }
    private static long accounted(ProposalFootprints.PreparedFootprint footprint) {
        long bytes=128;
        for (var span:footprint.spans()) { byte[] lower=span.lowerInclusive(), upper=span.upperExclusive(); bytes+=64+(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length); }
        return bytes;
    }
    private static boolean overlaps(ProposalFootprints.PreparedFootprint first,ProposalFootprints.PreparedFootprint second) {
        for (var a:first.spans()) for (var b:second.spans()) if (a.columnId().equals(b.columnId())) {
            if (a.isPoint() && b.isPoint()) { if (Arrays.equals(a.lowerInclusive(),b.lowerInclusive())) return true; }
            else if (a.isPoint()) { if (contains(b,a.lowerInclusive())) return true; }
            else if (b.isPoint()) { if (contains(a,b.lowerInclusive())) return true; }
            else if ((a.upperExclusive()==null || b.lowerInclusive()==null || Arrays.compareUnsigned(a.upperExclusive(),b.lowerInclusive())>0)
                    && (b.upperExclusive()==null || a.lowerInclusive()==null || Arrays.compareUnsigned(b.upperExclusive(),a.lowerInclusive())>0)) return true;
        }
        return false;
    }
    private static boolean contains(TransactionScope.Span span,byte[] key) {
        return (span.lowerInclusive()==null || Arrays.compareUnsigned(key,span.lowerInclusive())>=0)
                && (span.upperExclusive()==null || Arrays.compareUnsigned(key,span.upperExclusive())<0);
    }
}
