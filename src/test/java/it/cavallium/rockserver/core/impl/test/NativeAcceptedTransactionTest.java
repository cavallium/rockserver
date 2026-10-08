package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeAcceptedTransactionTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    @Test void bucketAcceptedPlanReplaysOnFollowerWithoutNativeHistoryOrSnapshot() throws Exception {
        DistributedCommand accepted;
        var scope=new TransactionScope(List.of(TransactionScope.Span.point(outer.bucket,outer.physical(7)),TransactionScope.Span.point(outer.fixed,new byte[]{7})));
        try (var leader=outer.new Fixture(root.resolve("leader"),true)) {
            leader.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); leader.store.applyNoop(new ApplyPosition(1,0));
            var base=outer.command(leader.schema,0,List.of(outer.put(outer.bucket,7,1,new byte[]{1},Mutation.ReturnMode.PREVIOUS),
                    outer.put(outer.bucket,7,1,new byte[]{2},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{7},new byte[]{3},Mutation.ReturnMode.PREVIOUS)),
                    List.of(KeyPredicate.bucketMember(outer.bucket,outer.physical(7),outer.member(1),null)),-1);
            try (var pin=leader.store.captureTracked(scope)) { accepted=leader.store.planScopedCommit(pin,base,3,new UUID(0,9)); }
            assertArrayEquals(accepted.acceptance().result(),leader.store.apply(new ApplyPosition(1,1),accepted)); assertArrayEquals(new byte[]{2},leader.row(7).get(outer.member(1)));
        }
        try (var follower=outer.new Fixture(root.resolve("follower"),true)) {
            follower.store.applyNoop(new ApplyPosition(1,0));
            var replay=DistributedCommandCodec.decode(DistributedCommandCodec.encode(accepted));
            assertArrayEquals(accepted.acceptance().result(),follower.store.apply(new ApplyPosition(1,1),replay));
            assertArrayEquals(new byte[]{2},follower.row(7).get(outer.member(1))); assertArrayEquals(new byte[]{3},follower.raw(outer.fixed,new byte[]{7}));
        }
    }
    @Test void resultMismatchForeignPinAndInvalidAuditFailBeforeEffects() throws Exception {
        try (var f=outer.new Fixture(root.resolve("mismatch"),true); var other=outer.new Fixture(root.resolve("other"),true)) {
            f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0));
            other.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); other.store.applyNoop(new ApplyPosition(1,0));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(outer.fixed,new byte[]{1})));
            var base=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1},new byte[]{7},Mutation.ReturnMode.PREVIOUS)));
            try (var foreign=other.store.captureTracked(scope)) { assertThrows(IllegalArgumentException.class,() -> f.store.planScopedCommit(foreign,base,0,new UUID(0,1))); }
            var bad=DistributedCommand.acceptedTransaction(base,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,1),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{99})).encode()));
            assertThrows(IllegalStateException.class,() -> f.store.apply(new ApplyPosition(1,1),bad)); assertEquals(new ApplyPosition(1,0),f.store.applied()); assertNull(f.raw(outer.fixed,new byte[]{1})); assertEquals(0,f.store.receiptUsage().retainedCount());
            try (var pin=f.store.captureTracked(scope)) {
                var valid=f.store.planScopedCommit(pin,base,0,new UUID(0,1)); assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(2,1),valid));
                assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,1),valid,valid.acceptance().result()));
                assertArrayEquals(valid.acceptance().result(),f.store.apply(new ApplyPosition(1,1),valid));
            }
        }
    }
    @Test void qualifiedPlanShapeIsRejectedBeforeAcceptedBytesArePublished() throws Exception {
        try (var f=outer.new Fixture(root.resolve("qualified-shape"),true)) {
            f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0));
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,new byte[]{0},new byte[]{10})));
            var base=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,0),
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.fixed,new byte[]{1},new byte[]{9})));
            try (var pin=f.store.captureTracked(scope)) {
                assertThrows(IllegalArgumentException.class,() -> f.store.planScopedCommit(pin,base,0,new UUID(0,1)));
                assertEquals(new ApplyPosition(1,0),f.store.applied()); assertEquals(0,f.store.receiptUsage().retainedCount());
            }
        }
    }
}
