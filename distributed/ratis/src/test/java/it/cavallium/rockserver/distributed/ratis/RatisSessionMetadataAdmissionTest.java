package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled native prefix and proposal ownership; no independent quorum or sockets. */
class RatisSessionMetadataAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private DistributedCommand ack(RatisRetentionAdmissionTest.Fixture f,UUID issuer,long epoch,long sequence,long through) {
        return new DistributedCommand(f.groupId,1,new byte[32],new OperationId(issuer,epoch,sequence),List.of(),List.of(),through);
    }
    @Test void saturatedNamespacesAdmitKnownIssuerAndNoGrowthThenFloorReclaimsCapacity() throws Exception {
        try(var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("saturated"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            f.store.configureSessionMetadataCapacity(1);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(ack(f,f.session,1,1,0)),1));
            var full=f.propose(ack(f,UUID.randomUUID(),1,1,0));
            assertNotNull(full.getException()); assertTrue(full.getException().getMessage().contains("namespace admission exhausted"));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(ack(f,f.session,1,2,1)),2));
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(f.put(3)),3));
            var floor=DistributedCommand.advanceSessionEpochFloor(f.groupId,1,new byte[32],new OperationId(f.admin,2,0),2);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(floor),4));
            var next=ack(f,UUID.randomUUID(),2,1,0);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(next),5));
            assertEquals(1,f.store.estimateSessionMetadata(next).retainedNamespaces()); f.assertReleased();
        }
    }
    @Test void duplicateProposalAndCanceledOwnerKeepNamespaceReservationUntilActualSettlement() throws Exception {
        try(var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("duplicates"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            f.store.configureSessionMetadataCapacity(1);
            var original=ack(f,f.session,1,1,0);
            var first=f.propose(original); var duplicate=f.propose(original);
            assertNull(first.getException()); assertNull(duplicate.getException());
            assertEquals(1,pendingNamespaces(f));
            f.machine.cancelTransaction(first); assertEquals(1,pendingNamespaces(f));
            f.machine.preAppendTransaction(duplicate); duplicate.initLogEntry(1,1);
            f.machine.cancelTransaction(duplicate); assertEquals(1,pendingNamespaces(f));
            f.machine.applyTransaction(duplicate).get(5,TimeUnit.SECONDS); assertEquals(0,pendingNamespaces(f));
            var retry=f.propose(original); assertNull(retry.getException()); assertEquals(0,pendingNamespaces(f)); f.machine.cancelTransaction(retry);
            var fresh=f.propose(ack(f,UUID.randomUUID(),1,1,0)); assertNotNull(fresh.getException());
            assertEquals(1,f.store.estimateSessionMetadata(original).retainedNamespaces()); f.assertReleased();
        }
    }
    @Test void canceledUnsubmittedAckRefundsAndCommittedFullReplaySavesNoEffectConflict() throws Exception {
        try(var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("refund"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            f.store.configureSessionMetadataCapacity(1);
            var canceled=f.propose(ack(f,f.session,1,1,0)); assertNull(canceled.getException());
            f.machine.cancelTransaction(canceled); assertEquals(0,pendingNamespaces(f));
            var admitted=ack(f,UUID.randomUUID(),1,1,0);
            assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(admitted),1));
            var denied=ack(f,UUID.randomUUID(),1,1,0);
            assertNotNull(f.propose(denied).getException());
            // A committed record bypasses volatile admission; native replay stores a stable rejection.
            assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.apply(new ApplyPosition(1,2),denied)).status());
            assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.apply(new ApplyPosition(1,3),denied)).status());
            assertEquals(1,f.store.estimateSessionMetadata(denied).retainedNamespaces()); f.assertReleased();
        }
    }
    private int pendingNamespaces(RatisRetentionAdmissionTest.Fixture f) throws Exception {
        var field=RatisRangeStateMachine.class.getDeclaredField("pendingSessionNamespaces"); field.setAccessible(true);
        return ((java.util.Map<?,?>)field.get(f.machine)).size();
    }
}
