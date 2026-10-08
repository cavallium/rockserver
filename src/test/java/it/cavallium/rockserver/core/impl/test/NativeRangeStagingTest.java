package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Ordered range tombstones, point resurrection and page seeks at an exact native cut. */
class NativeRangeStagingTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    byte[] key(int n) { return new byte[]{(byte)n}; }
    Mutation put(int n,int value,Mutation.ReturnMode mode) { return new Mutation(Mutation.Kind.PUT,outer.fixed,key(n),key(value),mode); }
    Mutation delete(int low,int high) { return new Mutation(Mutation.Kind.DELETE_RANGE,outer.fixed,key(low),key(high)); }
    DistributedCommand command(NativeBucketApplyTest.Fixture f,long n,List<Mutation> mutations) { return new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,n),mutations); }
    TransactionScope scope(UUID column,byte[] low,byte[] high) { return new TransactionScope(List.of(TransactionScope.Span.range(column,low,high))); }
    void enable(NativeBucketApplyTest.Fixture f) throws Exception { f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0)); }
    @Test void overlappingDeletesAndResurrectionsMatchPointReadsPagesAcceptedApplyAndReceiptRetry() throws Exception {
        try (var f=outer.new Fixture(root.resolve("ordered"),true)) {
            enable(f); var initial=new ArrayList<Mutation>(); for (int n=1;n<10;n++) initial.add(put(n,n,Mutation.ReturnMode.NONE)); f.apply(1,command(f,0,initial));
            var plan=command(f,1,List.of(put(2,20,Mutation.ReturnMode.PREVIOUS),delete(2,8),put(4,40,Mutation.ReturnMode.PREVIOUS),delete(3,6),put(5,50,Mutation.ReturnMode.CURRENT)));
            byte[] saved;
            try (var pin=f.store.captureTracked(scope(outer.fixed,key(1),key(10))); var staged=pin.stage(plan)) {
                assertArrayEquals(key(2),staged.result().value(0)); assertNull(staged.result().value(1)); assertArrayEquals(key(50),staged.result().value(2));
                for (int n:new int[]{2,3,4,6,7}) assertNull(staged.get(outer.fixed,key(n)));
                assertArrayEquals(key(50),staged.get(outer.fixed,key(5))); assertArrayEquals(key(8),staged.get(outer.fixed,key(8)));
                var first=staged.page(outer.fixed,key(1),key(10),false,null,2,1024); assertTrue(first.hasMore());
                assertEquals(List.of(new PhysicalRangePage.Row(key(1),key(1)),new PhysicalRangePage.Row(key(5),key(50))),first.rows());
                var rest=staged.page(outer.fixed,key(1),key(10),false,first.resumeAfter(),2,1024); assertFalse(rest.hasMore());
                assertEquals(List.of(new PhysicalRangePage.Row(key(8),key(8)),new PhysicalRangePage.Row(key(9),key(9))),rest.rows());
                var reverse=staged.page(outer.fixed,key(1),key(10),true,null,2,1024); assertTrue(reverse.hasMore()); assertArrayEquals(key(9),reverse.rows().getFirst().key());
                var prior=staged.page(outer.fixed,key(1),key(10),true,reverse.resumeAfter(),2,1024); assertFalse(prior.hasMore()); assertArrayEquals(key(5),prior.rows().getFirst().key());
                assertArrayEquals(key(2),pin.get(outer.fixed,key(2))); assertArrayEquals(key(4),f.raw(outer.fixed,key(4)));
                var accepted=f.store.planScopedCommit(pin,plan,0,new UUID(0,9)); assertEquals(plan.mutations(),accepted.mutations());
                saved=staged.result().encode(); assertArrayEquals(saved,accepted.acceptance().result()); assertArrayEquals(saved,f.store.apply(new ApplyPosition(1,2),accepted));
                f.apply(3,command(f,2,List.of(put(5,99,Mutation.ReturnMode.NONE)))); assertArrayEquals(saved,f.store.apply(new ApplyPosition(1,4),accepted));
            }
            assertNull(f.raw(outer.fixed,key(2))); assertNull(f.raw(outer.fixed,key(4))); assertArrayEquals(key(99),f.raw(outer.fixed,key(5))); assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void erasedOversizedRowsAndDeletedSuffixesDoNotConsumePageBudgetOrProduceFalseContinuation() throws Exception {
        try (var f=outer.new Fixture(root.resolve("seeks"),true)) {
            enable(f); f.apply(1,command(f,0,List.of(put(1,1,Mutation.ReturnMode.NONE),new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),new byte[1024*1024]),put(6,6,Mutation.ReturnMode.NONE),put(8,8,Mutation.ReturnMode.NONE))));
            var plan=command(f,1,List.of(delete(2,8),put(5,5,Mutation.ReturnMode.NONE)));
            try (var pin=f.store.captureTracked(scope(outer.fixed,key(1),key(10))); var staged=pin.stage(plan)) {
                var page=staged.page(outer.fixed,key(1),key(8),false,null,4,64); assertEquals(2,page.rows().size()); assertFalse(page.hasMore());
                assertArrayEquals(key(5),page.rows().getLast().key());
                var reverse=staged.page(outer.fixed,key(2),key(9),true,null,4,64); assertEquals(2,reverse.rows().size()); assertFalse(reverse.hasMore());
                assertArrayEquals(key(8),reverse.rows().getFirst().key()); assertArrayEquals(key(5),reverse.rows().getLast().key());
            }
            try (var pin=f.store.captureTracked(scope(outer.fixed,key(1),key(10))); var staged=pin.stage(command(f,2,List.of(delete(2,10))))) {
                var page=staged.page(outer.fixed,key(1),key(10),false,null,1,64); assertEquals(1,page.rows().size()); assertFalse(page.hasMore());
                assertFalse(staged.page(outer.fixed,key(2),key(10),true,null,1,64).hasMore());
                assertTrue(staged.page(outer.fixed,key(2),key(10),false,null,1,64).rows().isEmpty());
            }
            try (var pin=f.store.captureTracked(scope(outer.fixed,key(1),key(10))); var staged=pin.stage(command(f,3,List.of(delete(2,8),put(2,22,Mutation.ReturnMode.CURRENT))))) {
                var reverse=staged.page(outer.fixed,key(1),key(8),true,null,4,64);
                assertEquals(List.of(new PhysicalRangePage.Row(key(2),key(22)),new PhysicalRangePage.Row(key(1),key(1))),reverse.rows()); assertFalse(reverse.hasMore());
                var forward=staged.page(outer.fixed,key(1),key(8),false,null,4,64);
                assertEquals(List.of(new PhysicalRangePage.Row(key(1),key(1)),new PhysicalRangePage.Row(key(2),key(22))),forward.rows()); assertFalse(forward.hasMore());
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void resultOverflowAndOutOfScopeRangesNeverInstallPartialDeletesOrLeakTheStagedPin() throws Exception {
        try (var f=outer.new Fixture(root.resolve("rejection"),true)) {
            enable(f); byte[] large=new byte[1024*1024];
            f.apply(1,command(f,0,List.of(put(1,1,Mutation.ReturnMode.NONE),new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),large))));
            f.apply(2,command(f,1,List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,key(3),large))));
            var plan=command(f,2,List.of(delete(1,2),new Mutation(Mutation.Kind.DELETE,outer.fixed,key(2),null,Mutation.ReturnMode.PREVIOUS),new Mutation(Mutation.Kind.DELETE,outer.fixed,key(3),null,Mutation.ReturnMode.PREVIOUS)));
            try (var pin=f.store.captureTracked(scope(outer.fixed,key(1),key(4)))) {
                assertThrows(IllegalStateException.class,() -> pin.stage(plan));
                assertThrows(IllegalArgumentException.class,() -> pin.stage(command(f,3,List.of(delete(0,4)))));
                assertEquals(1,f.store.activeSnapshotPins()); assertArrayEquals(key(1),pin.get(outer.fixed,key(1)));
            }
            assertEquals(CommandResult.Status.RESULT_TOO_LARGE,f.apply(3,plan).status());
            assertArrayEquals(key(1),f.raw(outer.fixed,key(1))); assertArrayEquals(large,f.raw(outer.fixed,key(2))); assertArrayEquals(large,f.raw(outer.fixed,key(3))); assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void bucketRangesEraseWholeRowsAndOnlyLaterMemberMutationsSurvive() throws Exception {
        try (var f=outer.new Fixture(root.resolve("bucket-ranges"),true)) {
            enable(f); f.apply(1,command(f,0,List.of(outer.put(outer.bucket,7,1,key(1),Mutation.ReturnMode.NONE),outer.put(outer.bucket,8,1,key(8),Mutation.ReturnMode.NONE))));
            var mutations=List.of(outer.put(outer.bucket,7,2,key(2),Mutation.ReturnMode.CURRENT),
                    new Mutation(Mutation.Kind.DELETE_RANGE,outer.bucket,outer.physical(7),outer.physical(9)),
                    outer.put(outer.bucket,7,3,key(3),Mutation.ReturnMode.PREVIOUS),outer.put(outer.bucket,8,2,key(82),Mutation.ReturnMode.NONE),
                    new Mutation(Mutation.Kind.DELETE_RANGE,outer.bucket,outer.physical(8),outer.physical(9)));
            var plan=command(f,1,mutations); assertTrue(plan.hasBucketData()); assertEquals(mutations,DistributedCommandCodec.decode(DistributedCommandCodec.encode(plan)).mutations());
            try (var pin=f.store.captureTracked(scope(outer.bucket,outer.physical(7),outer.physical(9))); var staged=pin.stage(plan)) {
                assertArrayEquals(key(2),staged.result().value(0)); assertNull(staged.result().value(1));
                assertNull(staged.getMember(outer.bucket,outer.physical(7),outer.member(1))); assertNull(staged.getMember(outer.bucket,outer.physical(7),outer.member(2)));
                assertArrayEquals(key(3),staged.getMember(outer.bucket,outer.physical(7),outer.member(3))); assertNull(staged.get(outer.bucket,outer.physical(8)));
                var page=staged.page(outer.bucket,outer.physical(7),outer.physical(9),false,null,4,1024); assertEquals(1,page.rows().size()); assertFalse(page.hasMore());
                var accepted=f.store.planScopedCommit(pin,plan,0,new UUID(0,9)); assertArrayEquals(staged.result().encode(),f.store.apply(new ApplyPosition(1,2),accepted));
            }
            assertNull(f.raw(outer.bucket,outer.physical(8))); assertNull(f.row(7).get(outer.member(1))); assertNull(f.row(7).get(outer.member(2))); assertArrayEquals(key(3),f.row(7).get(outer.member(3)));
        }
    }
}
