package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled Ratis contexts plus native receipts; no real append/quorum or server processes. */
class RatisRejectedTransactionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception { return fixture(name,RatisRangeStateMachine.RetentionLimits.defaults()); }
    RatisRetentionAdmissionTest.Fixture fixture(String name,RatisRangeStateMachine.RetentionLimits limits) throws Exception {
        return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),limits,command -> {},() -> 100L);
    }
    TransactionScope scope(RatisRetentionAdmissionTest.Fixture f,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(f.column,new byte[]{(byte)key}))); }
    DistributedCommand base(RatisRetentionAdmissionTest.Fixture f,long sequence,int key,byte[] value,Mutation.ReturnMode mode,long ack) {
        var mutations=List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)key},value,mode));
        return ack<0 ? new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,sequence),mutations)
                : new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,sequence),mutations,List.of(),ack);
    }
    DistributedCommand lost(RatisRetentionAdmissionTest.Fixture f,DistributedCommand base,int key) throws Exception {
        try (var pin=f.store.captureTracked(scope(f,key)); var plan=f.machine.prepareScopedCommit(pin,base)) { return plan.command(); }
    }
    DistributedCommand logged(TransactionContext transaction) { assertNull(transaction.getException()); return DistributedCommandCodec.decode(transaction.getStateMachineLogEntry().getLogData().toByteArray()); }

    @Test void duplicateOwnersFreezeCompactRejectionAndMayAppendInReverseOrder() throws Exception {
        try (var f=fixture("duplicates")) {
            var original=lost(f,f.put(0),0); var first=f.propose(original); var second=f.propose(original);
            assertTrue(logged(first).transactionRejection()); assertTrue(logged(second).transactionRejection()); assertSame(first.getStateMachineContext(),second.getStateMachineContext());
            assertArrayEquals(DistributedCommandCodec.encode(original),first.getClientRequest().getMessage().getContent().toByteArray());
            assertArrayEquals(first.getStateMachineLogEntry().getLogData().toByteArray(),second.getStateMachineLogEntry().getLogData().toByteArray());
            assertEquals(CommandResult.Status.CONFLICT,f.apply(second,1)); assertEquals(CommandResult.Status.CONFLICT,f.apply(first,2));
            assertEquals(1,f.store.receiptUsage().retainedCount()); try (var pin=f.store.capture()) { assertNull(pin.get(f.column,new byte[]{0})); }
            f.assertReleased();
        }
    }
    @Test void sameTermAmbiguousOriginalCannotBeConvertedToRejection() throws Exception {
        try (var f=fixture("ambiguous"); var pin=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(pin,f.put(0))) {
            var original=plan.command(); var transaction=f.propose(original); assertEquals(DistributedCommand.Kind.COMMIT_ACCEPTED,logged(transaction).kind());
            f.machine.preAppendTransaction(transaction); transaction.initLogEntry(1,1); f.machine.cancelTransaction(transaction); f.machine.notifyNotLeader(List.of());
            assertNotNull(f.propose(original).getException()); assertEquals(1,f.machine.proposalFootprintStats().candidates()); assertEquals(1,f.counter("reservedReceiptCount"));
            var result=CommandResult.decode(f.machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray()); assertEquals(CommandResult.Status.SUCCESS,result.status());
            var retry=f.propose(original); assertEquals(DistributedCommand.Kind.COMMIT_ACCEPTED,logged(retry).kind()); assertEquals(CommandResult.Status.SUCCESS,f.apply(retry,2)); f.assertReleased();
        }
    }
    @Test void rejectionAckRetainsMetadataExclusionAndUsesNormalInputSlots() throws Exception {
        try (var f=fixture("metadata")) {
            var original=lost(f,base(f,1,0,new byte[]{7},Mutation.ReturnMode.NONE,0),0);
            try (var other=f.machine.reserveProposalScope(scope(f,2))) { assertNotNull(other); assertNotNull(f.propose(original).getException()); }
            var transaction=f.propose(original); assertTrue(logged(transaction).transactionRejection());
            assertEquals(1,f.counter("dataAdmissions")); assertEquals(0,f.counter("protectedAdmissions")); assertEquals(DistributedCommandCodec.encodedSize(original),f.counter("dataProposalBytes"));
            assertEquals(0,f.machine.proposalFootprintStats().ordinary()); assertThrows(java.io.IOException.class,() -> f.machine.reserveProposalScope(scope(f,2)));
            assertEquals(CommandResult.Status.CONFLICT,f.apply(transaction,1)); try (var freed=f.machine.reserveProposalScope(scope(f,2))) { assertNotNull(freed); } f.assertReleased();
        }
    }
    @Test void ownAckCanResolveLargeReturningPlanAtTheReceiptCap() throws Exception {
        try (var f=fixture("quota",new RatisRangeStateMachine.RetentionLimits(1,92,1,92))) {
            byte[] prior=new byte[16384]; Arrays.fill(prior,(byte)3);
            f.apply(f.propose(base(f,0,0,prior,Mutation.ReturnMode.NONE,-1)),1); assertEquals(92,f.store.receiptUsage().retainedBytes());
            var original=lost(f,base(f,1,0,new byte[]{9},Mutation.ReturnMode.PREVIOUS,0),0); var rejection=DistributedCommand.rejectAcceptedTransaction(original);
            assertTrue(f.store.estimateReceiptRetention(original).additionalBytes()>184); assertEquals(0,f.store.estimateReceiptRetention(rejection).additionalBytes());
            var transaction=f.propose(original); assertTrue(logged(transaction).transactionRejection()); assertEquals(0,f.counter("reservedReceiptBytes"));
            assertEquals(CommandResult.Status.CONFLICT,f.apply(transaction,2)); assertEquals(1,f.store.receiptUsage().retainedCount()); assertEquals(92,f.store.receiptUsage().retainedBytes());
            try (var pin=f.store.capture()) { assertArrayEquals(prior,pin.get(f.column,new byte[]{0})); } f.assertReleased();
        }
    }
    @Test void snapshotCoveredDeferredRejectionUsesOriginalReceiptIdentity() throws Exception {
        try (var f=fixture("deferred")) {
            var original=lost(f,f.put(0),0); var transaction=f.propose(original); var rejection=logged(transaction); assertTrue(rejection.transactionRejection());
            f.machine.preAppendTransaction(transaction); transaction.initLogEntry(1,1); f.store.apply(new ApplyPosition(1,1),rejection);
            assertEquals(1,f.machine.takeSnapshot()); f.machine.pause(); var pending=f.machine.applyTransaction(transaction); assertFalse(pending.isDone());
            f.machine.reinitialize(); assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(pending.get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.operationResult(original.operationId(),original.payloadDigest())).status()); f.assertReleased();
        }
    }
    @Test void differentHashCannotPoisonAnAlreadyPreparedIdentity() throws Exception {
        try (var f=fixture("collision"); var pin=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(pin,f.put(0))) {
            var original=plan.command(); var altered=DistributedCommand.acceptedTransaction(base(f,0,0,new byte[]{9},Mutation.ReturnMode.NONE,-1),original.acceptance());
            assertNotNull(f.propose(altered).getException()); assertEquals(1,f.machine.proposalFootprintStats().candidates()); assertNull(f.store.operationResult(original.operationId(),original.payloadDigest()));
            var valid=f.propose(original); assertEquals(DistributedCommand.Kind.COMMIT_ACCEPTED,logged(valid).kind()); assertEquals(CommandResult.Status.SUCCESS,f.apply(valid,1));
            try (var read=f.store.capture()) { assertArrayEquals(new byte[]{7},read.get(f.column,new byte[]{0})); } f.assertReleased();
        }
    }
    @Test void ownerChangeResolvesMissingAuthorityButNeverOverwritesSavedSuccess() throws Exception {
        for (boolean saved:new boolean[]{false,true}) try (var f=fixture("owner-"+saved); var pin=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(pin,f.put(0))) {
            var original=plan.command(); long barrier=saved ? 2 : 1;
            if (saved) assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(original),1));
            f.machine.notifyNotLeader(List.of()); f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,barrier);
            var transaction=f.propose(original); assertEquals(!saved,logged(transaction).transactionRejection());
            var expected=saved ? CommandResult.Status.SUCCESS : CommandResult.Status.CONFLICT; assertEquals(expected,f.apply(transaction,barrier+1));
            assertEquals(expected,CommandResult.decode(f.store.operationResult(original.operationId(),original.payloadDigest())).status()); f.assertReleased();
        }
    }
    @Test void compactRecordsAreInternalAndRejectingOtherIdsDoesNotDrainUserScopes() throws Exception {
        try (var f=fixture("footprint"); var pin=f.store.captureTracked(scope(f,0)); var plan=f.machine.prepareScopedCommit(pin,f.put(0))) {
            var original=plan.command(); assertNotNull(f.propose(DistributedCommand.rejectAcceptedTransaction(original)).getException());
            var other=DistributedCommand.acceptedTransaction(base(f,1,0,new byte[]{9},Mutation.ReturnMode.NONE,-1),original.acceptance());
            var transaction=f.propose(other); assertTrue(logged(transaction).transactionRejection()); assertEquals(1,f.machine.proposalFootprintStats().candidates()); assertEquals(0,f.machine.proposalFootprintStats().ordinary());
            assertEquals(CommandResult.Status.CONFLICT,f.apply(transaction,1)); assertEquals(1,f.machine.proposalFootprintStats().candidates());
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(original),2)); f.assertReleased();
        }
    }
}
