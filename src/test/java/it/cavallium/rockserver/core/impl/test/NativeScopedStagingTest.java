package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Exact native cut plus an immutable mutation prefix; no write RPC or speculative native commit. */
class NativeScopedStagingTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    byte[] key(int n) { return new byte[]{(byte)n}; }
    TransactionScope range(UUID column) { return new TransactionScope(List.of(TransactionScope.Span.range(column,key(1),key(10)))); }
    DistributedCommand command(NativeBucketApplyTest.Fixture f,long n,Mutation... mutations) {
        return new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,n),List.of(mutations));
    }
    Mutation put(UUID column,int key,int value) { return new Mutation(Mutation.Kind.PUT,column,key(key),key(value)); }
    void enable(NativeBucketApplyTest.Fixture f) throws Exception {
        f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0));
    }
    @Test void stagedPointsPagesAndReturnsUseOriginalCutAndLeaveDurableStateUntouched() throws Exception {
        try (var f=outer.new Fixture(root.resolve("points"),true)) {
            enable(f); f.apply(1,command(f,0,put(outer.fixed,1,11),put(outer.fixed,3,13),put(outer.fixed,5,15)));
            var scope=range(outer.fixed);
            try (var pin=f.store.captureTracked(scope)) {
                var base=command(f,1,new Mutation(Mutation.Kind.PUT,outer.fixed,key(1),key(21),Mutation.ReturnMode.PREVIOUS),
                        new Mutation(Mutation.Kind.DELETE,outer.fixed,key(3),null,Mutation.ReturnMode.PREVIOUS),
                        new Mutation(Mutation.Kind.PUT,outer.fixed,key(2),key(22),Mutation.ReturnMode.CURRENT),put(outer.fixed,9,29));
                try (var staged=pin.stage(base)) {
                    assertArrayEquals(key(11),staged.result().value(0)); assertArrayEquals(key(13),staged.result().value(1)); assertArrayEquals(key(22),staged.result().value(2));
                    assertArrayEquals(key(21),staged.get(outer.fixed,key(1))); assertNull(staged.get(outer.fixed,key(3)));
                    assertArrayEquals(key(11),pin.get(outer.fixed,key(1))); assertArrayEquals(key(13),f.raw(outer.fixed,key(3)));
                    f.apply(2,command(f,2,put(outer.fixed,5,99))); // The owned native cut remains unchanged; commit will conflict.
                    assertArrayEquals(key(15),staged.get(outer.fixed,key(5)));
                    var page=staged.page(outer.fixed,key(1),key(10),false,null,2,1024);
                    assertEquals(List.of(new PhysicalRangePage.Row(key(1),key(21)),new PhysicalRangePage.Row(key(2),key(22))),page.rows()); assertTrue(page.hasMore());
                    var rest=staged.page(outer.fixed,key(1),key(10),false,page.resumeAfter(),2,1024);
                    assertEquals(List.of(new PhysicalRangePage.Row(key(5),key(15)),new PhysicalRangePage.Row(key(9),key(29))),rest.rows()); assertFalse(rest.hasMore());
                    var reverse=staged.page(outer.fixed,key(2),key(9),true,null,8,1024);
                    assertEquals(List.of(new PhysicalRangePage.Row(key(5),key(15)),new PhysicalRangePage.Row(key(2),key(22))),reverse.rows()); assertFalse(reverse.hasMore());
                    assertThrows(IllegalArgumentException.class,() -> staged.get(outer.fixed,key(10)));
                    assertThrows(IllegalArgumentException.class,() -> staged.page(outer.fixed,key(0),key(10),false,null,1,1024));
                    assertThrows(PhysicalRangePage.LimitExceededException.class,() -> staged.page(outer.fixed,key(1),key(10),false,null,1,10));
                    assertEquals(RangeMutationHistory.Status.CONFLICT,pin.validate()); assertThrows(IllegalStateException.class,() -> staged.get(outer.fixed,key(1)));
                    assertEquals(1,f.store.activeSnapshotPins(),"Staging owns native use until its close, despite certification invalidation");
                }
                assertEquals(0,f.store.activeSnapshotPins());
            }
            assertEquals(new ApplyPosition(1,2),f.store.applied()); assertNull(f.raw(outer.fixed,key(2))); assertNull(f.raw(outer.fixed,key(9)));
        }
    }
    @Test void collidingBucketMembersShareStagingAndAcceptedPlanPreservesTheOriginalOperands() throws Exception {
        try (var f=outer.new Fixture(root.resolve("buckets"),true)) {
            enable(f); f.apply(1,command(f,0,outer.put(outer.bucket,7,1,key(11),Mutation.ReturnMode.NONE),outer.put(outer.bucket,7,2,key(12),Mutation.ReturnMode.NONE)));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(outer.bucket,outer.physical(7)),TransactionScope.Span.point(outer.fixed,key(7))));
            var base=command(f,1,outer.put(outer.bucket,7,1,key(21),Mutation.ReturnMode.PREVIOUS),
                    Mutation.bucketDelete(outer.bucket,outer.physical(7),outer.member(2),Mutation.ReturnMode.PREVIOUS),
                    outer.put(outer.bucket,7,3,key(23),Mutation.ReturnMode.CURRENT),put(outer.fixed,7,27));
            try (var pin=f.store.captureTracked(scope); var stage=pin.stage(base)) {
                assertArrayEquals(key(21),stage.getMember(outer.bucket,outer.physical(7),outer.member(1))); assertNull(stage.getMember(outer.bucket,outer.physical(7),outer.member(2)));
                assertArrayEquals(key(23),stage.getMember(outer.bucket,outer.physical(7),outer.member(3))); assertArrayEquals(key(27),stage.get(outer.fixed,key(7)));
                assertArrayEquals(key(11),f.row(7).get(outer.member(1)));
                var accepted=f.store.planScopedCommit(pin,base,0,new UUID(0,9));
                assertEquals(base.mutations(),accepted.mutations()); assertArrayEquals(stage.result().encode(),accepted.acceptance().result());
                assertArrayEquals(stage.result().encode(),f.store.apply(new ApplyPosition(1,2),accepted));
                assertArrayEquals(key(21),f.row(7).get(outer.member(1))); assertNull(f.row(7).get(outer.member(2))); assertArrayEquals(key(23),f.row(7).get(outer.member(3)));
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void failedPredicatesAndOutOfScopePlansReleaseTheirNativeUseAndPermitAnotherPrefix() throws Exception {
        try (var f=outer.new Fixture(root.resolve("rejected"),true)) {
            enable(f);
            try (var pin=f.store.captureTracked(range(outer.fixed))) {
                var bad=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,0),List.of(put(outer.fixed,1,1)),List.of(new KeyPredicate(outer.fixed,key(1),key(99))));
                assertThrows(IllegalStateException.class,() -> pin.stage(bad)); assertEquals(1,f.store.activeSnapshotPins());
                assertThrows(IllegalArgumentException.class,() -> pin.stage(command(f,1,put(outer.fixed,11,1))));
                try (var first=pin.stage(command(f,2,put(outer.fixed,1,1))); var next=pin.stage(command(f,2,put(outer.fixed,1,1),put(outer.fixed,2,2)))) {
                    assertNull(first.get(outer.fixed,key(2))); assertArrayEquals(key(2),next.get(outer.fixed,key(2)));
                    pin.close(); assertThrows(IllegalStateException.class,() -> next.get(outer.fixed,key(2)));
                    assertFalse(f.store.close(Duration.ZERO)); assertEquals(1,f.store.activeSnapshotPins());
                    first.close(); first.close(); next.close();
                    assertEquals(0,f.store.activeSnapshotPins()); assertTrue(f.store.close(Duration.ZERO));
                }
            }
        }
    }
    @Test void stagingOverflowAndBadLogicalShapesFailWithoutLeakingPinsOrPublishingEffects() throws Exception {
        try (var f=outer.new Fixture(root.resolve("bounds"),true)) {
            enable(f); byte[] value=new byte[1024*1024];
            f.apply(1,command(f,0,new Mutation(Mutation.Kind.PUT,outer.fixed,key(1),value)));
            try (var pin=f.store.captureTracked(range(outer.fixed))) {
                var tooLarge=command(f,1,new Mutation(Mutation.Kind.PUT,outer.fixed,key(1),value,Mutation.ReturnMode.PREVIOUS),
                        new Mutation(Mutation.Kind.DELETE,outer.fixed,key(1),null,Mutation.ReturnMode.PREVIOUS));
                assertThrows(IllegalStateException.class,() -> pin.stage(tooLarge));
                assertThrows(IllegalArgumentException.class,() -> pin.stage(command(f,2,new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{1,2},key(1)))));
                assertEquals(1,f.store.activeSnapshotPins()); assertEquals(new ApplyPosition(1,1),f.store.applied());
                try (var stage=pin.stage(command(f,3,put(outer.fixed,2,2)))) { assertArrayEquals(value,stage.get(outer.fixed,key(1))); }
                assertArrayEquals(value,f.raw(outer.fixed,key(1))); assertNull(f.raw(outer.fixed,key(2)));
            }
            assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void orderedNativeMergesReadTheirSnapshotBaseAndRemainOperandsAtCommit() throws Exception {
        RocksDB.loadLibrary(); UUID column=new UUID(0,1),group=new UUID(0,2),domain=new UUID(0,3),session=new UUID(0,4);
        for (boolean finalize:new boolean[]{false,true}) for (boolean ranges:new boolean[]{false,true}) try (var operator=new StringAppendOperator(","); var cf=new ColumnFamilyOptions().setMergeOperator(operator);
             var plain=new ColumnFamilyOptions(); var options=new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)) {
            var handles=new ArrayList<ColumnFamilyHandle>();
            try (var db=RocksDB.open(options,root.resolve("merge-"+ranges+"-"+finalize).toString(),List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,plain),
                    new ColumnFamilyDescriptor(key(1),cf),new ColumnFamilyDescriptor(key(2),plain),new ColumnFamilyDescriptor(key(3),plain)),handles)) {
                try {
                    var ownership=new RangeOwnership(); var owner=new RangeOwnership.Owner(group,1);
                    ownership.register(owner,new RangeOwnership.Interval(domain,Set.of(column),null,null),RangeOwnership.State.ACTIVE);
                    var store=new ReplicatedRangeStore(db,ownership,owner,new byte[32],Map.of(column,handles.get(1)),handles.get(2),handles.get(3),Set.of(column));
                    store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); store.applyNoop(new ApplyPosition(1,0));
                    var seed=new DistributedCommand(group,1,new byte[32],new OperationId(session,1,0),List.of(new Mutation(Mutation.Kind.PUT,column,key(1),"a".getBytes(StandardCharsets.UTF_8))));
                    store.apply(new ApplyPosition(1,1),seed);
                    var mutations=new ArrayList<>(List.of(
                            new Mutation(Mutation.Kind.MERGE,column,key(1),"b".getBytes(StandardCharsets.UTF_8),Mutation.ReturnMode.PREVIOUS),
                            new Mutation(Mutation.Kind.MERGE,column,key(1),"c".getBytes(StandardCharsets.UTF_8),Mutation.ReturnMode.CURRENT),
                            new Mutation(Mutation.Kind.DELETE,column,key(2),null),new Mutation(Mutation.Kind.MERGE,column,key(2),"z".getBytes(StandardCharsets.UTF_8))));
                    if (ranges) {
                        mutations.add(1,new Mutation(Mutation.Kind.DELETE_RANGE,column,key(1),key(2)));
                        mutations.add(3,new Mutation(Mutation.Kind.MERGE,column,key(1),"d".getBytes(StandardCharsets.UTF_8),Mutation.ReturnMode.CURRENT));
                    }
                    var base=new DistributedCommand(group,1,new byte[32],new OperationId(session,1,1),mutations);
                    String expected=ranges ? "c,d" : "a,b,c";
                    try (var pin=store.captureTracked(range(column)); var stage=pin.stage(base)) {
                        assertEquals("a",new String(stage.result().value(0),StandardCharsets.UTF_8)); assertEquals(expected,new String(stage.get(column,key(1)),StandardCharsets.UTF_8));
                        assertEquals("z",new String(stage.get(column,key(2)),StandardCharsets.UTF_8));
                        var page=stage.page(column,key(1),key(3),false,null,4,1024);
                        assertEquals(2,page.rows().size()); assertEquals(expected,new String(page.rows().getFirst().value(),StandardCharsets.UTF_8));
                        var accepted=store.planScopedCommit(pin,base,0,new UUID(0,9)); assertEquals(base.mutations(),accepted.mutations());
                        if (finalize) {
                            var registration=TransactionStatus.begin(new UUID(0,10), new UUID(0,11), new UUID(0,12), base.operationId(), List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), new byte[32], List.of(new TransactionStatus.Participant(domain, group, group, 1, new byte[32], accepted.payloadDigest(), null, false)));
                            var preparation=new PreparedParticipant.Preparation(new UUID(0,13),1,new OperationId(new UUID(0,14),1,0),registration,accepted);
                            var vote=store.applyPrepared(new ApplyPosition(1,2),preparation); var decision=registration.prepared(group,accepted.payloadDigest(),vote.position()).decide(TransactionStatus.Phase.COMMIT);
                            var intent=new ParticipantFinalization(preparation.homeGroup(),1,group,new OperationId(new UUID(0,15),2,0),decision);
                            var receipt=NativeParticipantRetirementTest.appliedCommit(store,new ApplyPosition(2,3),intent); assertArrayEquals(stage.result().encode(),store.committedResult(registration.transactionId(),accepted.payloadDigest()));
                            assertEquals(receipt,NativeParticipantRetirementTest.appliedCommit(store,new ApplyPosition(2,4),intent));
                        } else assertArrayEquals(stage.result().encode(),store.apply(new ApplyPosition(1,2),accepted));
                        try (var current=store.capture()) { assertEquals(expected,new String(current.get(column,key(1)),StandardCharsets.UTF_8)); }
                    }
                    assertTrue(store.close(Duration.ZERO));
                } finally { handles.forEach(ColumnFamilyHandle::close); }
            }
        }
    }
}
