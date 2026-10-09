package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native HOME retirement; DATA fence provenance is exercised separately by adapter fixtures. */
class NativeHomeRetirementTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    void configure(NativeHomeRecoveryTest.Fixture f) { var initial=helper.initial(0); f.store.configureHomeRecoveryReservations(initial.clusterId(),initial.databaseId(),helper.column); }
    HomeIdentity identity(TransactionStatus status) { return new HomeIdentity(helper.group,1,new byte[32],helper.column,status); }
    ParticipantFenceProof dataFence(TransactionStatus status,long floor) { return new ParticipantFenceProof(new ParticipantIdentity(helper.group,1,helper.participant,status),floor,new ApplyPosition(2,20)); }
    TransactionStatus replacement(TransactionStatus before) {
        byte[] plan=new byte[32]; plan[0]=1;
        return TransactionStatus.begin(before.clusterId(),before.databaseId(),before.domainId(),before.transactionId(),new OperationId(new UUID(0,900),2,0),before.schemaEpoch(),before.schemaDigest(),plan,before.asInitial().participants());
    }
    record Finished(TransactionStatus initial,TransactionStatus consumed,HomeRecovery recovery,HomeRecoveryReceipt receipt,HomeRetirement retirement,int index,DistributedCommand registration) {}
    Finished finish(NativeHomeRecoveryTest.Fixture f,boolean commit) throws Exception {
        configure(f); var initial=helper.initial(0); var registration=helper.cas(new OperationId(helper.user,2,0),null,initial); helper.apply(f,1,registration);
        int index=1; TransactionStatus terminal; HomeRecovery first; HomeRecoveryReceipt receipt;
        if (commit) {
            var voted=initial.prepared(helper.participant,new byte[32],new ApplyPosition(1,10)); helper.apply(f,++index,helper.cas(new OperationId(helper.user,2,1),initial,voted));
            terminal=voted.decide(TransactionStatus.Phase.COMMIT); helper.apply(f,++index,helper.cas(new OperationId(helper.user,2,2),voted,terminal)); first=null; receipt=null;
        } else {
            first=helper.abort(initial,0); receipt=f.store.applyHomeRecovery(new ApplyPosition(1,++index),first).receipt(); terminal=first.next();
        }
        HomeRecovery.Proof proof;
        if (commit) {
            var finalized=new ParticipantFinalization(helper.group,1,helper.participant,new OperationId(new UUID(0,901),0,0),terminal);
            proof=HomeRecovery.Proof.from(new ParticipantOutcome(finalized,new ApplyPosition(1,11),ParticipantVote.prepared(new ApplyPosition(1,10)),new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
        } else proof=HomeRecovery.Proof.from(dataFence(initial,2));
        var acknowledged=terminal.finalized(helper.participant); var ack=new HomeRecovery(HomeRecovery.Action.ACK,helper.cas(new OperationId(helper.recoverySession,0,1),terminal,acknowledged),proof);
        var ackReceipt=f.store.applyHomeRecovery(new ApplyPosition(1,++index),ack).receipt(); if (first==null) { first=ack; receipt=ackReceipt; }
        var consumed=acknowledged.consume(); helper.apply(f,++index,helper.cas(new OperationId(helper.user,2,3),acknowledged,consumed));
        helper.apply(f,++index,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2));
        return new Finished(initial,consumed,first,receipt,new HomeRetirement(identity(initial),consumed,List.of(dataFence(initial,2))),index,registration);
    }
    @Test void consumedCommitAndAbortRetireAtomicallyWhilePinsAndOrdinaryReceiptsRemainHistorical() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var f=helper.new Fixture(root.resolve("consumed-"+commit))) {
            f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); var done=finish(f,commit); var scope=new TransactionScope(List.of(TransactionScope.Span.point(helper.column,helper.key(done.initial()))));
            var usage=f.store.receiptUsage(); assertTrue(f.store.estimateHomeRetirement(done.retirement())); assertFalse(f.store.homeRetired(done.retirement())); assertNull(f.store.homeFence(done.retirement().identity()));
            try (var old=f.store.capture(); var tracked=f.store.captureTracked(scope)) {
                assertEquals(done.receipt(),old.homeRecoveryResult(done.recovery()).receipt()); f.store.applyHomeRetirement(new ApplyPosition(1,done.index()+1),done.retirement());
                assertEquals(0,f.store.homeRecoveryCapacity().effectiveCount()); assertEquals(0,f.store.homeRecoveryCapacity().effectiveBytes()); assertEquals(usage.retainedCount(),f.store.receiptUsage().retainedCount()); assertEquals(usage.retainedBytes(),f.store.receiptUsage().retainedBytes());
                assertTrue(f.store.homeRetired(done.retirement())); assertFalse(f.store.estimateHomeRetirement(done.retirement())); assertEquals(done.consumed(),TransactionStatus.decode(old.get(helper.column,helper.key(done.initial())))); assertFalse(old.homeRetired(done.retirement())); assertNull(old.homeFence(done.retirement().identity()));
                var newWrite=new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,2,99),List.of(new Mutation(Mutation.Kind.PUT,helper.column,helper.key(done.initial()),done.consumed().encode())));
                assertThrows(IllegalStateException.class,() -> f.store.planScopedCommit(tracked,newWrite,0,new UUID(0,902)));
                var result=f.store.homeRecoveryResult(done.recovery()); assertTrue(result.retired()); assertNull(result.receipt()); assertEquals(new ApplyPosition(1,done.index()+1),result.retirement().position());
                var replay=f.store.applyHomeRecovery(new ApplyPosition(1,done.index()+2),done.recovery()); assertTrue(replay.retired()); assertNull(replay.receipt()); assertEquals(new ApplyPosition(1,done.index()+2),replay.retirement().position()); assertEquals(0,f.store.homeRecoveryCapacity().count());
                assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,done.index()+3,done.registration()).status());
                var fresh=helper.cas(new OperationId(helper.user,3,0),null,done.initial()); assertEquals(0,f.store.estimateHomeRegistration(fresh).additionalCount()); assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,done.index()+4,fresh).status());
                try (var pin=f.store.capture()) { assertNull(pin.get(helper.column,helper.key(done.initial()))); assertTrue(pin.homeRetired(done.retirement())); assertTrue(pin.homeRecoveryResult(done.recovery()).retired()); }
                assertEquals(done.receipt(),old.homeRecoveryResult(done.recovery()).receipt());
            }
        }
    }
    @Test void retiredHomeFloorAndAbsenceSurviveReopenAndPortableInstall() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("home-retired.image"); Finished done;
        try (var f=helper.new Fixture(source)) {
            done=finish(f,true); f.store.applyHomeRetirement(new ApplyPosition(1,done.index()+1),done.retirement());
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
        }
        NativeRangeImage.inspect(image,helper.limits);
        for (boolean install:new boolean[]{false,true}) try (var f=helper.new Fixture(install ? root.resolve("target") : source)) {
            if (install) { f.store.fence(); f.store.installSnapshot(image,helper.limits); f.store.activate(); }
            configure(f); assertTrue(f.store.homeRetired(done.retirement())); assertEquals(2,f.store.homeFence(done.retirement().identity()).minimumEpoch()); assertTrue(f.store.homeRecoveryResult(done.recovery()).retired());
            f.store.applyHomeRetirement(new ApplyPosition(2,done.index()+2),done.retirement()); assertEquals(0,f.store.homeRecoveryCapacity().count());
            assertTrue(f.store.applyHomeRecovery(new ApplyPosition(2,done.index()+3),done.recovery()).retired()); try (var pin=f.store.capture()) { assertNull(pin.get(helper.column,helper.key(done.initial()))); }
        }
    }
    @Test void all1024OrphanConflictsReclaimTheirBoundedCapacityWithoutAnyDataProofOrDecision() throws Exception {
        try (var f=helper.new Fixture(root.resolve("orphan-capacity"))) {
            configure(f); var requests=new ArrayList<HomeRecovery>(); int index=0;
            for (int n=0;n<ReplicatedRangeStore.MAX_HOME_RECOVERIES;n++) { var recovery=helper.abort(helper.initial(n),n); requests.add(recovery); assertEquals(CommandResult.Status.CONFLICT,f.store.applyHomeRecovery(new ApplyPosition(1,++index),recovery).receipt().result().status()); }
            assertEquals(1024,f.store.homeRecoveryCapacity().count()); helper.apply(f,++index,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2));
            var first=requests.getFirst(); var retirement=new HomeRetirement(identity(first.before()),null,List.of()); assertNotNull(f.store.homeFence(retirement.identity())); assertFalse(f.store.homeRetired(retirement));
            try (var old=f.store.capture()) {
                for (var request:requests) { var reclaim=new HomeRetirement(identity(request.before()),null,List.of()); assertTrue(f.store.estimateHomeRetirement(reclaim)); f.store.applyHomeRetirement(new ApplyPosition(1,++index),reclaim); }
                assertEquals(0,f.store.homeRecoveryCapacity().count()); assertEquals(0,f.store.homeRecoveryCapacity().bytes()); assertTrue(f.store.homeRetired(retirement)); assertFalse(old.homeRetired(retirement)); assertEquals(CommandResult.Status.CONFLICT,old.homeRecoveryResult(first).receipt().result().status()); assertTrue(f.store.homeRecoveryResult(first).retired());
            }
            var next=helper.abort(replacement(helper.initial(0)),2000); assertEquals(CommandResult.Status.CONFLICT,f.store.applyHomeRecovery(new ApplyPosition(1,++index),next).receipt().result().status()); assertEquals(1,f.store.homeRecoveryCapacity().count());
        }
    }
    @Test void oldReplaysAndFreshControlsDoNotTouchNewBusinessAtTheSameUuid() throws Exception {
        try (var f=helper.new Fixture(root.resolve("uuid-reuse"))) {
            configure(f); var old=helper.initial(0); var request=helper.abort(old,0); f.store.applyHomeRecovery(new ApplyPosition(1,1),request);
            helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2)); var next=replacement(old); helper.apply(f,3,helper.cas(new OperationId(helper.user,3,0),null,next));
            var retirement=new HomeRetirement(identity(old),null,List.of()); assertNotNull(f.store.homeFence(identity(old))); f.store.applyHomeRetirement(new ApplyPosition(1,4),retirement); assertEquals(2,f.store.homeRecoveryCapacity().reservedCount());
            assertTrue(f.store.applyHomeRecovery(new ApplyPosition(1,5),request).retired()); f.store.applyHomeRetirement(new ApplyPosition(1,6),retirement); assertTrue(f.store.applyHomeRecovery(new ApplyPosition(1,1),request).retired());
            assertEquals(CommandResult.Status.SESSION_EXPIRED,helper.apply(f,7,helper.cas(new OperationId(helper.user,3,1),null,old)).status());
            try (var pin=f.store.capture()) { assertEquals(next,TransactionStatus.decode(pin.get(helper.column,helper.key(old)))); }
            var changed=TransactionStatus.begin(next.clusterId(),next.databaseId(),next.domainId(),next.transactionId(),next.operation(),next.schemaEpoch(),next.schemaDigest(),new byte[32],next.participants());
            assertThrows(IllegalArgumentException.class,() -> f.store.homeFence(identity(changed)));
            helper.apply(f,8,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,1),3)); assertNull(f.store.homeFence(identity(next))); assertEquals(2,f.store.homeRecoveryCapacity().reservedCount());
            var abort=helper.abort(next,1); assertFalse(f.store.applyHomeRecovery(new ApplyPosition(1,9),abort).retired()); assertEquals(1,f.store.homeRecoveryCapacity().reservedCount());
        }
    }
    @Test void unconsumedHomeOrInsufficientFloorCannotBeCollected() throws Exception {
        try (var f=helper.new Fixture(root.resolve("guards"))) {
            configure(f); var initial=helper.initial(0); helper.apply(f,1,helper.cas(new OperationId(helper.user,2,0),null,initial)); var absentMode=new HomeRetirement(identity(initial),null,List.of());
            assertThrows(IllegalStateException.class,() -> f.store.estimateHomeRetirement(absentMode)); helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateHomeRetirement(absentMode)); assertThrows(IllegalArgumentException.class,() -> f.store.applyHomeRetirement(new ApplyPosition(1,3),absentMode)); assertEquals(new ApplyPosition(1,2),f.store.applied());
            var abort=helper.abort(initial,0); assertFalse(f.store.applyHomeRecovery(new ApplyPosition(1,3),abort).retired()); assertNull(f.store.homeFence(identity(initial)));
        }
    }
    @SuppressWarnings("unchecked") ColumnFamilyHandle userColumn(NativeHomeRecoveryTest.Fixture f) throws Exception { var field=ReplicatedRangeStore.class.getDeclaredField("columns"); field.setAccessible(true); return ((Map<UUID,ColumnFamilyHandle>)field.get(f.store)).get(helper.column); }
    RocksDB db(NativeHomeRecoveryTest.Fixture f) throws Exception { var field=ReplicatedRangeStore.class.getDeclaredField("db"); field.setAccessible(true); return (RocksDB)field.get(f.store); }
    @Test void missingOrReplacedUserCannotTurnSuccessReceiptsIntoRetirementProofs() throws Exception {
        for (boolean replace:new boolean[]{false,true}) try (var f=helper.new Fixture(root.resolve("success-orphan-"+replace))) {
            var done=finish(f,false); var key=helper.key(done.initial());
            if (replace) db(f).put(userColumn(f),key,replacement(done.initial()).decide(TransactionStatus.Phase.ABORT).finalized(helper.participant).consume().encode()); else db(f).delete(userColumn(f),key);
            assertThrows(RuntimeException.class,() -> f.store.homeRecoveryResult(done.recovery())); assertThrows(RuntimeException.class,() -> f.store.homeFence(done.retirement().identity())); assertThrows(RuntimeException.class,() -> f.store.estimateHomeRetirement(done.retirement()));
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-success-"+replace+".image"),pin,f.store.ownership().interval(),helper.limits)); }
            f.store.fence(); assertThrows(RuntimeException.class,f.store::activate);
        }
    }
    @Test void orphanPromisePreventsAbsenceProofGcExportAndActivation() throws Exception {
        try (var f=helper.new Fixture(root.resolve("credit-orphan"))) {
            configure(f); var initial=helper.initial(0); helper.apply(f,1,helper.cas(new OperationId(helper.user,2,0),null,initial)); helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2)); db(f).delete(userColumn(f),helper.key(initial));
            var retirement=new HomeRetirement(identity(initial),null,List.of()); assertThrows(IllegalStateException.class,() -> f.store.homeFence(identity(initial))); assertThrows(IllegalStateException.class,() -> f.store.estimateHomeRetirement(retirement));
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad-credit.image"),pin,f.store.ownership().interval(),helper.limits)); }
            f.store.fence(); assertThrows(IllegalStateException.class,f.store::activate);
        }
    }
}
