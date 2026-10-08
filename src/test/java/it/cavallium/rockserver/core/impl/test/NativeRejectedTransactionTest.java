package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native ordered rejection/receipt recovery; no consensus or transport acceptance claims. */
class NativeRejectedTransactionTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeBucketApplyTest outer=new NativeBucketApplyTest();
    static final UUID INCARNATION=new UUID(0,9), ADMIN=new UUID(0,10), NEIGHBOR=new UUID(0,11);
    static final NativeRangeImage.Limits IMAGE_LIMITS=new NativeRangeImage.Limits(4*1024*1024,16*1024*1024,4);

    @Test void originalAndRejectionShareOneReceiptWhicheverCommitsFirst() throws Exception {
        for (boolean originalFirst:new boolean[]{true,false}) try (var f=outer.new Fixture(root.resolve("order-"+originalFirst),true)) {
            enableHistory(f);
            var base=outer.command(f.schema,0,List.of(
                    outer.put(outer.bucket,7,1,new byte[]{1},Mutation.ReturnMode.PREVIOUS),
                    outer.put(outer.bucket,7,1,new byte[]{2},Mutation.ReturnMode.CURRENT)),List.of(),-1);
            DistributedCommand original=plan(f,base), rejection=DistributedCommand.rejectAcceptedTransaction(original);
            assertTrue(rejection.transactionRejection()); assertArrayEquals(original.payloadDigest(),rejection.receiptDigest());
            assertFalse(Arrays.equals(rejection.payloadDigest(),rejection.receiptDigest()));
            assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,1),rejection,
                    new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            byte[] saved=f.apply(1,originalFirst ? original : rejection).encode();
            if (originalFirst) {
                assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(saved).status());
                assertNull(CommandResult.decode(saved).value(0)); assertArrayEquals(new byte[]{2},CommandResult.decode(saved).value(1));
                var later=outer.command(f.schema,1,List.of(outer.put(outer.bucket,7,1,new byte[]{9},Mutation.ReturnMode.NONE)),List.of(),-1);
                f.apply(2,later);
                assertArrayEquals(saved,f.apply(3,rejection).encode()); assertArrayEquals(saved,f.apply(4,original).encode());
                assertArrayEquals(new byte[]{9},f.row(7).get(outer.member(1)),"Neither identity may reapply the original bucket effects");
            } else {
                assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(saved).status());
                assertArrayEquals(saved,f.apply(2,original).encode()); assertArrayEquals(saved,f.apply(3,rejection).encode());
                assertNull(f.raw(outer.bucket,outer.physical(7)));
            }
            assertSavedIdentity(f,original,rejection,saved);
            for (var command:List.of(original,rejection)) {
                var estimate=f.store.estimateReceiptRetention(command);
                assertFalse(estimate.newReceipt()); assertEquals(0,estimate.additionalCount()); assertEquals(0,estimate.additionalBytes());
            }
        }
    }

    @Test void restartAndInstalledImagePreserveRejectedHashOutcomeMarkerAndUsageWithoutHistory() throws Exception {
        Path source=root.resolve("source"), image=root.resolve("image");
        DistributedCommand original,rejection; byte[] saved,receipt; ReceiptUsage usage;
        try (var f=outer.new Fixture(source,true)) {
            f.store.initializeConfiguration(new byte[]{1}); enableHistory(f);
            original=plan(f,bucketBase(f,0,-1)); rejection=DistributedCommand.rejectAcceptedTransaction(original);
            saved=f.apply(1,rejection).encode(); usage=f.store.receiptUsage();
            try (var pin=f.store.capture()) {
                receipt=receipt(pin,original.operationId());
                assertEquals(new ApplyPosition(1,1),pin.position());
                NativeRangeImage.write(image,pin,f.store.ownership().interval(),IMAGE_LIMITS);
            }
        }
        assertEquals(new ApplyPosition(1,1),NativeRangeImage.inspect(image,IMAGE_LIMITS).position());
        try (var reopened=outer.new Fixture(source,true)) {
            assertRecovered(reopened,original,rejection,saved,receipt,usage);
        }
        try (var target=outer.new Fixture(root.resolve("target"),true)) {
            target.store.fence(); target.store.installSnapshot(image,IMAGE_LIMITS); target.store.activate();
            assertRecovered(target,original,rejection,saved,receipt,usage);
        }
    }

    @Test void individuallyExpiredRejectionRetainsBusinessExpiryAndAppliesOnlyItsOwnAck() throws Exception {
        try (var f=outer.new Fixture(root.resolve("individual-expiry"),true)) {
            f.store.enableSessionControls(); f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            var ledger=SessionEpochLedger.initial(outer.cluster,outer.database,10).issue(NEIGHBOR,1000).issue(outer.session,100);
            var userGrant=ledger.grants().getLast(); assertEquals(2,userGrant.epoch());
            var binding=control(f,0,RangeSessionControl.bind(ledger,userGrant,2));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(0,binding).status());
            var seeded=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,2,0),
                    List.of(new Mutation(Mutation.Kind.PUT,outer.fixed,new byte[]{0},new byte[]{7})));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(1,seeded).status());
            byte[] seededReceipt;
            try (var pin=f.store.capture()) { seededReceipt=receipt(pin,seeded.operationId()); }
            assertNotNull(seededReceipt); assertEquals(92,48+seededReceipt.length);
            var base=DistributedCommand.bucketData(outer.group,1,f.schema.digest(),new OperationId(outer.session,2,1),
                    List.of(outer.put(outer.bucket,7,1,new byte[]{3},Mutation.ReturnMode.NONE)),List.of(),0);
            var original=plan(f,base); var rejection=DistributedCommand.rejectAcceptedTransaction(original);
            var expired=ledger.advanceTime(200); assertEquals(1,expired.minimumEpoch(),"Live neighboring epoch keeps the coarse floor below the expired user grant");
            assertEquals(CommandResult.Status.SUCCESS,f.apply(2,control(f,1,RangeSessionControl.advance(expired))).status());
            var estimate=f.store.estimateReceiptRetention(rejection);
            assertTrue(estimate.newReceipt()); assertTrue(estimate.maintenance());
            assertEquals(0,estimate.additionalCount()); assertEquals(0,estimate.additionalBytes());
            var before=f.store.receiptUsage();
            byte[] saved=f.apply(3,rejection).encode();
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(saved).status(),"Empty rejection body must not turn expired business data into a permitted ACK-only conflict");
            assertEquals(before.retainedCount(),f.store.receiptUsage().retainedCount());
            assertEquals(before.retainedBytes(),f.store.receiptUsage().retainedBytes());
            try (var pin=f.store.capture()) {
                assertNull(receipt(pin,seeded.operationId()),"ACK removes the issuing user's prior receipt");
                assertNotNull(receipt(pin,binding.operationId()),"ACK must not remove another issuing identity's receipt");
                assertEquals(1,pin.stateView().sessionAuthority().minimumEpoch());
                assertArrayEquals(new byte[]{7},pin.get(outer.fixed,new byte[]{0})); assertNull(pin.get(outer.bucket,outer.physical(7)));
            }
            assertSavedIdentity(f,original,rejection,saved);
            assertArrayEquals(saved,f.apply(4,original).encode()); assertNull(f.raw(outer.bucket,outer.physical(7)));
            assertEquals(CommandResult.Status.SESSION_EXPIRED,CommandResult.decode(f.store.operationResult(seeded.operationId(),seeded.payloadDigest())).status());
        }
    }

    @Test void rejectionConsumesOneNinetyTwoByteReceiptAndPublishesMarkerWithoutHistoryWrites() throws Exception {
        try (var f=outer.new Fixture(root.resolve("same-cut"),true)) {
            enableHistory(f); var original=plan(f,bucketBase(f,0,-1)); var rejection=DistributedCommand.rejectAcceptedTransaction(original);
            var estimate=f.store.estimateReceiptRetention(rejection);
            assertTrue(estimate.newReceipt()); assertEquals(1,estimate.additionalCount()); assertEquals(92,estimate.additionalBytes());
            try (var history=f.store.captureTracked(bucketScope()); var before=f.store.capture()) {
                assertEquals(new ApplyPosition(1,0),before.position()); assertNull(before.operationResult(original.operationId(),original.payloadDigest()));
                byte[] saved=f.apply(1,rejection).encode();
                assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(saved).status());
                assertEquals(new ReceiptUsage(new ApplyPosition(1,1),1,92),f.store.receiptUsage());
                assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate()); assertNull(history.get(outer.bucket,outer.physical(7)));
                assertEquals(new ApplyPosition(1,0),before.position()); assertNull(before.operationResult(original.operationId(),original.payloadDigest()));
                try (var after=f.store.capture()) {
                    assertEquals(new ApplyPosition(1,1),after.position());
                    assertArrayEquals(saved,after.operationResult(original.operationId(),original.payloadDigest()));
                    assertNull(after.get(outer.bucket,outer.physical(7)));
                    assertEquals(92,48+Objects.requireNonNull(receipt(after,original.operationId())).length);
                }
                assertArrayEquals(saved,f.apply(2,original).encode()); assertEquals(RangeMutationHistory.Status.ACTIVE,history.validate());
                assertEquals(new ReceiptUsage(new ApplyPosition(1,2),1,92),f.store.receiptUsage());
            }
            assertEquals(0,f.store.activeSnapshotPins()); assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }

    private void enableHistory(NativeBucketApplyTest.Fixture f) throws Exception {
        f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); f.store.applyNoop(new ApplyPosition(1,0));
    }
    private TransactionScope bucketScope() {
        return new TransactionScope(List.of(TransactionScope.Span.point(outer.bucket,outer.physical(7))));
    }
    private DistributedCommand bucketBase(NativeBucketApplyTest.Fixture f,long sequence,long ack) {
        return outer.command(f.schema,sequence,List.of(outer.put(outer.bucket,7,1,new byte[]{2},Mutation.ReturnMode.NONE)),List.of(),ack);
    }
    private DistributedCommand plan(NativeBucketApplyTest.Fixture f,DistributedCommand base) throws Exception {
        try (var pin=f.store.captureTracked(bucketScope())) { return f.store.planScopedCommit(pin,base,3,INCARNATION); }
    }
    private DistributedCommand control(NativeBucketApplyTest.Fixture f,long sequence,RangeSessionControl control) {
        return DistributedCommand.sessionControl(outer.group,1,f.schema.digest(),new OperationId(ADMIN,3,sequence),control,-1);
    }
    private void assertSavedIdentity(NativeBucketApplyTest.Fixture f,DistributedCommand original,DistributedCommand rejection,byte[] saved) throws Exception {
        assertArrayEquals(saved,f.store.operationResult(original.operationId(),original.payloadDigest()));
        assertArrayEquals(saved,f.store.operationResult(rejection.operationId(),rejection.receiptDigest()));
        assertThrows(IllegalArgumentException.class,() -> f.store.operationResult(rejection.operationId(),rejection.payloadDigest()));
        try (var pin=f.store.capture()) {
            byte[] receipt=Objects.requireNonNull(receipt(pin,original.operationId()));
            assertEquals(1,ByteBuffer.wrap(receipt).getInt());
            assertArrayEquals(original.payloadDigest(),Arrays.copyOfRange(receipt,4,36));
            assertArrayEquals(saved,Arrays.copyOfRange(receipt,36,receipt.length));
        }
    }
    private void assertRecovered(NativeBucketApplyTest.Fixture f,DistributedCommand original,DistributedCommand rejection,
            byte[] saved,byte[] receipt,ReceiptUsage usage) throws Exception {
        assertEquals(new ApplyPosition(1,1),f.store.applied()); assertEquals(usage,f.store.receiptUsage());
        try (var pin=f.store.capture()) { assertArrayEquals(receipt,receipt(pin,original.operationId())); }
        assertSavedIdentity(f,original,rejection,saved);
        assertArrayEquals(saved,f.apply(2,original).encode()); assertNull(f.raw(outer.bucket,outer.physical(7)));
        assertEquals(new ReceiptUsage(new ApplyPosition(1,2),usage.retainedCount(),usage.retainedBytes()),f.store.receiptUsage());
        assertFalse(f.store.estimateReceiptRetention(original).newReceipt()); assertFalse(f.store.estimateReceiptRetention(rejection).newReceipt());
    }
    private byte[] receipt(ReplicatedRangeStore.SnapshotPin pin,OperationId id) throws Exception {
        byte[] key=ByteBuffer.allocate(48).putLong(outer.group.getMostSignificantBits()).putLong(outer.group.getLeastSignificantBits())
                .putLong(id.sessionId().getMostSignificantBits()).putLong(id.sessionId().getLeastSignificantBits()).putLong(id.epoch()).putLong(id.sequence()).array();
        var found=new ArrayList<byte[]>();
        pin.visitReceipts((candidate,value) -> { if (Arrays.equals(key,candidate)) found.add(value.clone()); });
        assertTrue(found.size()<=1); return found.isEmpty() ? null : found.getFirst();
    }
}
