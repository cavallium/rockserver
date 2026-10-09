package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native promises only. Runtime activation still requires adapter registration counters and exclusive home-row ownership. */
class NativeHomeRecoveryReservationsTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    void configure(NativeHomeRecoveryTest.Fixture f) { var initial=helper.initial(0); f.store.configureHomeRecoveryReservations(initial.clusterId(),initial.databaseId(),helper.column); }
    @Test void ordinaryRegistrationVotesDecisionAndAckUpdatePromisesAtomicallyWithoutProtectedReceipts() throws Exception {
        try (var f=helper.new Fixture(root.resolve("ordinary"))) {
            configure(f); configure(f); var initial=helper.initial(0); var registration=helper.cas(new OperationId(helper.user,1,0),null,initial);
            var estimate=f.store.estimateHomeRegistration(registration); assertEquals(2,estimate.additionalCount()); assertTrue(estimate.additionalBytes()>2*ReplicatedRangeStore.HomeRecoveryCredit.TRANSITION_BYTES);
            helper.apply(f,1,registration); var reserved=f.store.homeRecoveryCapacity(); assertEquals(0,reserved.count()); assertEquals(2,reserved.reservedCount()); assertEquals(estimate.additionalBytes(),reserved.reservedBytes());
            var voted=initial.prepared(helper.participant,new byte[32],new ApplyPosition(1,1)); helper.apply(f,2,helper.cas(new OperationId(helper.user,1,1),initial,voted)); assertEquals(reserved.reservedBytes(),f.store.homeRecoveryCapacity().reservedBytes());
            var commit=voted.decide(TransactionStatus.Phase.COMMIT); helper.apply(f,3,helper.cas(new OperationId(helper.user,1,2),voted,commit)); assertEquals(1,f.store.homeRecoveryCapacity().reservedCount());
            var acknowledged=commit.finalized(helper.participant); helper.apply(f,4,helper.cas(new OperationId(helper.user,1,3),commit,acknowledged)); assertEquals(0,f.store.homeRecoveryCapacity().reservedCount()); assertEquals(0,f.store.homeRecoveryCapacity().reservedBytes());
            helper.apply(f,5,helper.cas(new OperationId(helper.user,1,4),acknowledged,acknowledged.consume())); assertEquals(0,f.store.homeRecoveryCapacity().effectiveCount());
            assertThrows(IllegalArgumentException.class,() -> f.store.configureHomeRecoveryReservations(initial.clusterId(),UUID.randomUUID(),helper.column));
        }
    }
    @Test void fullEffectiveCapacityStillFundsMatchingAbortAndAckButNeverAConflictingCas() throws Exception {
        try (var f=helper.new Fixture(root.resolve("full"))) {
            configure(f); var registered=new ArrayList<TransactionStatus>(); int index=0;
            for (int n=0;n<ReplicatedRangeStore.MAX_HOME_RECOVERIES;n++) {
                var initial=helper.initial(n); var command=helper.cas(new OperationId(helper.user,1,n),null,initial);
                try { f.store.estimateHomeRegistration(command); } catch (IllegalStateException full) { break; }
                helper.apply(f,++index,command); registered.add(initial);
            }
            assertTrue(registered.size()>1); assertTrue(registered.size()<ReplicatedRangeStore.MAX_HOME_RECOVERIES);
            var original=registered.getFirst(); var voted=original.prepared(helper.participant,new byte[32],new ApplyPosition(1,1)); helper.apply(f,++index,helper.cas(new OperationId(helper.user,1,5000),original,voted));
            int promises=f.store.homeRecoveryCapacity().reservedCount();
            for (int n=0;n<ReplicatedRangeStore.MAX_HOME_RECOVERIES;n++) {
                var conflict=helper.abort(helper.initial(10000+n),10000+n);
                try { var estimate=f.store.estimateHomeRecovery(conflict); assertFalse(estimate.creditCovered()); } catch (IllegalStateException full) { break; }
                assertEquals(CommandResult.Status.CONFLICT,f.store.applyHomeRecovery(new ApplyPosition(1,++index),conflict).receipt().result().status());
            }
            assertEquals(promises,f.store.homeRecoveryCapacity().reservedCount()); var full=f.store.homeRecoveryCapacity();
            var stale=helper.abort(original,20000); assertThrows(IllegalStateException.class,() -> f.store.estimateHomeRecovery(stale)); final int failedIndex=index+1;
            assertThrows(IllegalStateException.class,() -> f.store.applyHomeRecovery(new ApplyPosition(1,failedIndex),stale).receipt()); assertEquals(full,f.store.homeRecoveryCapacity());
            var abort=helper.abort(voted,20001); var estimate=f.store.estimateHomeRecovery(abort); assertTrue(estimate.creditCovered()); assertEquals(0,estimate.additionalCount()); assertEquals(0,estimate.additionalBytes());
            assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,++index),abort).receipt().result().status()); assertEquals(promises-1,f.store.homeRecoveryCapacity().reservedCount());
            var terminal=voted.decide(TransactionStatus.Phase.ABORT); var finalization=new ParticipantFinalization(helper.group,1,helper.participant,new OperationId(new UUID(0,800),1,0),terminal);
            var outcome=new ParticipantOutcome(finalization,new ApplyPosition(7,50),ParticipantVote.prepared(new ApplyPosition(1,1)),null);
            var ack=new HomeRecovery(HomeRecovery.Action.ACK,helper.cas(new OperationId(helper.recoverySession,0,20002),terminal,terminal.finalized(helper.participant)),HomeRecovery.Proof.from(outcome));
            assertTrue(f.store.estimateHomeRecovery(ack).creditCovered()); assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,++index),ack).receipt().result().status());
            assertEquals(promises-2,f.store.homeRecoveryCapacity().reservedCount()); assertTrue(f.store.homeRecoveryCapacity().effectiveCount()<=ReplicatedRangeStore.MAX_HOME_RECOVERIES); assertTrue(f.store.homeRecoveryCapacity().effectiveBytes()<=ReplicatedRangeStore.MAX_HOME_RECOVERY_BYTES);
        }
    }
    @Test void promisesSurviveFloorsReopenAndImageWithoutAnActivationMarker() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("credits.image"); var initial=helper.initial(0);
        try (var f=helper.new Fixture(source)) {
            configure(f); helper.apply(f,1,helper.cas(new OperationId(helper.user,1,0),null,initial));
            helper.apply(f,2,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,5,0),5)); assertEquals(2,f.store.homeRecoveryCapacity().reservedCount());
            try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
        }
        NativeRangeImage.inspect(image,helper.limits);
        try (var f=helper.new Fixture(source)) {
            assertEquals(2,f.store.homeRecoveryCapacity().reservedCount()); assertTrue(f.store.estimateHomeRecovery(helper.abort(initial,0)).creditCovered());
            var vote=initial.prepared(helper.participant,new byte[32],new ApplyPosition(1,1));
            assertThrows(IllegalStateException.class,() -> helper.apply(f,3,helper.cas(new OperationId(helper.user,5,0),initial,vote))); assertEquals(new ApplyPosition(1,2),f.store.applied());
            f.store.applyHomeRecovery(new ApplyPosition(1,3),helper.abort(initial,0)).receipt(); assertEquals(1,f.store.homeRecoveryCapacity().reservedCount());
        }
        try (var f=helper.new Fixture(root.resolve("target"))) {
            f.store.fence(); f.store.installSnapshot(image,helper.limits); f.store.activate(); assertEquals(2,f.store.homeRecoveryCapacity().reservedCount()); configure(f);
            assertThrows(IllegalArgumentException.class,() -> f.store.configureHomeRecoveryReservations(initial.clusterId(),UUID.randomUUID(),helper.column));
        }
    }
    @Test void imageAndActivationRejectPromisesWhichDisagreeWithActualUserHomeRows() throws Exception {
        Path image=root.resolve("valid.image"),corrupt=root.resolve("mismatched.image"); var initial=helper.initial(0);
        try (var f=helper.new Fixture(root.resolve("corrupt-source"))) {
            configure(f); helper.apply(f,1,helper.cas(new OperationId(helper.user,1,0),null,initial)); try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
            byte[] bytes=Files.readAllBytes(image),before=initial.encode(),after=initial.decide(TransactionStatus.Phase.ABORT).encode(); int offset=find(bytes,before); assertTrue(offset>=0); System.arraycopy(after,0,bytes,offset,after.length);
            byte[] hash=MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes,bytes.length-32)); System.arraycopy(hash,0,bytes,bytes.length-32,32); Files.write(corrupt,bytes);
            assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspect(corrupt,helper.limits));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); dbField.setAccessible(true); var db=(RocksDB)dbField.get(f.store);
            var columnsField=ReplicatedRangeStore.class.getDeclaredField("columns"); columnsField.setAccessible(true); @SuppressWarnings("unchecked") var columns=(Map<UUID,ColumnFamilyHandle>)columnsField.get(f.store); db.put(columns.get(helper.column),helper.key(initial),after);
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("invalid-native.image"),pin,f.store.ownership().interval(),helper.limits)); }
            f.store.fence(); assertThrows(IllegalStateException.class,f.store::activate);
        }
        try (var f=helper.new Fixture(root.resolve("corrupt-target"))) { f.store.fence(); assertThrows(java.io.IOException.class,() -> f.store.installSnapshot(corrupt,helper.limits)); assertFalse(f.store.pendingInstallation()); assertEquals(new ApplyPosition(1,0),f.store.applied()); }
    }
    @Test void missingPromisesIncludingAnEntirelyRemovedCreditSectionAreRejectedBeforeInstall() throws Exception {
        for (boolean partial:new boolean[]{false,true}) {
            Path image=root.resolve("complete-promises-"+partial+".image"),bad=root.resolve("missing-promises-"+partial+".image"); var initial=helper.initial(0);
            try (var f=helper.new Fixture(root.resolve("missing-source-"+partial))) {
                configure(f); helper.apply(f,1,helper.cas(new OperationId(helper.user,1,0),null,initial));
                if (partial) helper.apply(f,2,helper.cas(new OperationId(helper.user,1,1),null,helper.initial(1)));
                try (var pin=f.store.capture()) { NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits); }
                var credit=new ReplicatedRangeStore.HomeRecoveryCredit(helper.group,1,new byte[32],initial.clusterId(),initial.databaseId(),helper.column,initial,initial,new ApplyPosition(1,1));
                byte[] creditKey=java.nio.ByteBuffer.allocate(33).putLong(helper.group.getMostSignificantBits()).putLong(helper.group.getLeastSignificantBits()).put((byte)0x26).put(helper.key(initial)).array();
                byte[] value=credit.encode(),row=java.nio.ByteBuffer.allocate(9+creditKey.length+value.length).put((byte)2).putInt(creditKey.length).putInt(value.length).put(creditKey).put(value).array();
                byte[] valid=Files.readAllBytes(image); int offset=find(valid,row); assertTrue(offset>=0); byte[] bytes=new byte[valid.length-row.length];
                System.arraycopy(valid,0,bytes,0,offset); System.arraycopy(valid,offset+row.length,bytes,offset,valid.length-offset-row.length);
                java.nio.ByteBuffer.wrap(bytes).putLong(bytes.length-40,java.nio.ByteBuffer.wrap(valid).getLong(valid.length-40)-1);
                byte[] checksum=MessageDigest.getInstance("SHA-256").digest(Arrays.copyOf(bytes,bytes.length-32)); System.arraycopy(checksum,0,bytes,bytes.length-32,32); Files.write(bad,bytes);
                assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspectHomeRecoveryReservations(bad,helper.limits,initial.clusterId(),initial.databaseId(),helper.column));
                if (partial) assertThrows(java.io.IOException.class,() -> NativeRangeImage.inspect(bad,helper.limits));
                var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); dbField.setAccessible(true); var db=(RocksDB)dbField.get(f.store);
                var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); stateField.setAccessible(true); db.delete((ColumnFamilyHandle)stateField.get(f.store),creditKey);
                assertThrows(IllegalStateException.class,() -> configure(f));
                try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("native-missing-"+partial+".image"),pin,f.store.ownership().interval(),helper.limits)); }
                f.store.fence(); assertThrows(IllegalStateException.class,f.store::activate);
            }
            try (var f=helper.new Fixture(root.resolve("missing-target-"+partial))) {
                configure(f); f.store.fence(); assertThrows(java.io.IOException.class,() -> f.store.installSnapshot(bad,helper.limits)); assertFalse(f.store.pendingInstallation()); assertEquals(new ApplyPosition(1,0),f.store.applied());
            }
        }
    }
    private static int find(byte[] bytes,byte[] value) { for (int i=0;i<=bytes.length-value.length;i++) { int j=0; while (j<value.length && bytes[i+j]==value[j]) j++; if (j==value.length) return i; } return -1; }
}
