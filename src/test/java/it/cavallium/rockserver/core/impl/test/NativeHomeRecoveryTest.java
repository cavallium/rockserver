package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Trusted native protected-home seam, independent of qualified adapter proof admission and reserved future ACK headroom. */
class NativeHomeRecoveryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final UUID group=new UUID(0,1),column=new UUID(0,2),domain=new UUID(0,3),participant=new UUID(0,4);
    final UUID user=new UUID(0,5),recoverySession=new UUID(0,6),admin=new UUID(0,7);
    final NativeRangeImage.Limits limits=new NativeRangeImage.Limits(1024*1024,32L*1024*1024,1);
    final class Fixture implements AutoCloseable {
        final DistributedStoragePool pool;
        final ReplicatedRangeStore store;
        Fixture(Path path) throws Exception {
            pool=DistributedStoragePool.open(path,ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(group,1),new RangeOwnership.Interval(domain,Set.of(column),null,null),new byte[32],Map.of(column,"home"),Set.of());
            store.initializeConfiguration(new byte[]{1}); if (store.applied()==null) store.applyNoop(new ApplyPosition(1,0));
        }
        @Override public void close() throws Exception { pool.close(); }
    }
    TransactionStatus initial(int id) {
        return TransactionStatus.begin(new UUID(0,20),new UUID(0,21),new UUID(0,22),new UUID(0,100+id),new OperationId(new UUID(0,23),1,id),1,new byte[32],new byte[32],
                List.of(new TransactionStatus.Participant(participant,participant,1,new byte[32],new byte[32],null,false)));
    }
    byte[] key(TransactionStatus status) { return ByteBuffer.allocate(16).putLong(status.transactionId().getMostSignificantBits()).putLong(status.transactionId().getLeastSignificantBits()).array(); }
    DistributedCommand cas(OperationId operation,TransactionStatus before,TransactionStatus next) { return new DistributedCommand(group,1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,key(next),next.encode())),List.of(new KeyPredicate(column,key(next),before==null ? null : before.encode()))); }
    HomeRecovery abort(TransactionStatus initial,long sequence) { return new HomeRecovery(HomeRecovery.Action.ABORT,cas(new OperationId(recoverySession,0,sequence),initial,initial.decide(TransactionStatus.Phase.ABORT)),null); }
    CommandResult apply(Fixture f,int index,DistributedCommand command) throws Exception { return CommandResult.decode(f.store.apply(new ApplyPosition(1,index),command)); }
    @Test void protectedReceiptSurvivesEpochFloorsOrdinaryAckGcReopenAndPortableImage() throws Exception {
        Path source=root.resolve("source"),image=root.resolve("home.image"); var initial=initial(0); var recovery=abort(initial,0); HomeRecoveryReceipt receipt;
        try (var f=new Fixture(source)) {
            assertEquals(CommandResult.Status.SUCCESS,apply(f,1,cas(new OperationId(user,1,0),null,initial)).status());
            try (var before=f.store.capture()) {
                apply(f,2,DistributedCommand.advanceSessionEpochFloor(group,1,new byte[32],new OperationId(admin,5,0),5));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,apply(f,3,recovery.command()).status());
                var usage=f.store.receiptUsage(); receipt=f.store.applyHomeRecovery(new ApplyPosition(1,4),recovery).receipt();
                assertEquals(CommandResult.Status.SUCCESS,receipt.result().status()); assertEquals(new ApplyPosition(1,4),receipt.position()); assertEquals(usage.retainedCount(),f.store.receiptUsage().retainedCount());
                assertNull(before.homeRecoveryResult(recovery)); assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,5),recovery.command()));
                apply(f,5,DistributedCommand.advanceSessionEpochFloor(group,1,new byte[32],new OperationId(admin,6,0),6));
                apply(f,6,new DistributedCommand(group,1,new byte[32],new OperationId(admin,6,1),List.of(),List.of(),0));
                assertEquals(receipt,f.store.applyHomeRecovery(new ApplyPosition(2,7),recovery).receipt()); assertEquals(new ApplyPosition(2,7),f.store.applied()); assertEquals(1,f.store.homeRecoveryCapacity().count());
                try (var pin=f.store.capture()) { assertEquals(receipt,pin.homeRecoveryResult(recovery).receipt()); NativeRangeImage.write(image,pin,f.store.ownership().interval(),limits); }
            }
            try (var pin=f.store.captureReadable(new TransactionScope(List.of(TransactionScope.Span.point(column,key(initial)))))) { assertEquals(initial.decide(TransactionStatus.Phase.ABORT),TransactionStatus.decode(pin.get(column,key(initial)))); }
        }
        NativeRangeImage.inspect(image,limits);
        try (var f=new Fixture(source)) { assertEquals(receipt,f.store.homeRecoveryResult(recovery).receipt()); assertEquals(receipt,f.store.applyHomeRecovery(new ApplyPosition(2,8),recovery).receipt()); assertEquals(1,f.store.homeRecoveryCapacity().count()); }
        try (var f=new Fixture(root.resolve("target"))) {
            f.store.fence(); f.store.installSnapshot(image,limits); assertEquals(receipt,f.store.homeRecoveryResult(recovery).receipt()); f.store.activate();
            assertEquals(1,f.store.homeRecoveryCapacity().count()); assertEquals(ReplicatedRangeStore.homeRecoveryBytes(recovery),f.store.homeRecoveryCapacity().bytes()); assertEquals(receipt,f.store.applyHomeRecovery(new ApplyPosition(2,8),recovery).receipt());
        }
    }
    @Test void protectedAckPublishesOnlyLocalFinalizationAndLateReceiptsNeverResetConsumedHome() throws Exception {
        try (var f=new Fixture(root.resolve("ack"))) {
            var initial=initial(9); apply(f,1,cas(new OperationId(user,1,0),null,initial)); var abort=abort(initial,0);
            var abortReceipt=f.store.applyHomeRecovery(new ApplyPosition(1,2),abort).receipt(); var terminal=initial.decide(TransactionStatus.Phase.ABORT);
            var finalized=new ParticipantFinalization(group,1,participant,new OperationId(new UUID(0,800),1,0),terminal);
            var nativeOutcome=new ParticipantOutcome(finalized,new ApplyPosition(7,50),null,null);
            var ack=new HomeRecovery(HomeRecovery.Action.ACK,cas(new OperationId(recoverySession,0,1),terminal,terminal.finalized(participant)),HomeRecovery.Proof.from(nativeOutcome)); ack.requireOutcome(nativeOutcome);
            var receipt=f.store.applyHomeRecovery(new ApplyPosition(1,3),ack).receipt(); assertEquals(CommandResult.Status.SUCCESS,receipt.result().status()); assertEquals(2,f.store.homeRecoveryCapacity().count()); assertEquals(1,f.store.receiptUsage().retainedCount());
            var acknowledged=terminal.finalized(participant); apply(f,4,cas(new OperationId(user,1,1),acknowledged,acknowledged.consume()));
            assertEquals(receipt,f.store.applyHomeRecovery(new ApplyPosition(2,5),ack).receipt()); assertEquals(abortReceipt,f.store.applyHomeRecovery(new ApplyPosition(2,6),abort).receipt());
            try (var pin=f.store.capture()) { assertTrue(TransactionStatus.decode(pin.get(column,key(initial))).consumed()); assertEquals(receipt,pin.homeRecoveryResult(ack).receipt()); }
        }
    }
    @Test void staleCasHasPermanentConflictAndChangedDigestCannotReuseItsControl() throws Exception {
        try (var f=new Fixture(root.resolve("conflict"))) {
            var initial=initial(1); apply(f,1,cas(new OperationId(user,1,0),null,initial));
            var voted=initial.prepared(participant,new byte[32],new ApplyPosition(1,1)); apply(f,2,cas(new OperationId(user,1,1),initial,voted));
            var stale=abort(initial,0); var receipt=f.store.applyHomeRecovery(new ApplyPosition(1,3),stale).receipt(); assertEquals(CommandResult.Status.CONFLICT,receipt.result().status());
            assertEquals(receipt,f.store.applyHomeRecovery(new ApplyPosition(2,4),stale).receipt());
            var changed=abort(voted,0); assertThrows(IllegalArgumentException.class,() -> f.store.estimateHomeRecovery(changed)); assertThrows(IllegalArgumentException.class,() -> f.store.homeRecoveryResult(changed).receipt());
            assertThrows(IllegalArgumentException.class,() -> f.store.applyHomeRecovery(new ApplyPosition(2,5),changed).receipt()); assertEquals(new ApplyPosition(2,4),f.store.applied());
        }
    }
    @Test void protectedCapacityIsFiniteIndependentAndExactDuplicatesStillAdvanceAtCapacity() throws Exception {
        try (var f=new Fixture(root.resolve("capacity"))) {
            HomeRecovery first=null; HomeRecoveryReceipt original=null;
            for (int n=0;n<ReplicatedRangeStore.MAX_HOME_RECOVERIES;n++) {
                var recovery=abort(initial(n),n); var receipt=f.store.applyHomeRecovery(new ApplyPosition(1,n+1),recovery).receipt(); assertEquals(CommandResult.Status.CONFLICT,receipt.result().status());
                if (n==0) { first=recovery; original=receipt; }
            }
            assertEquals(0,f.store.receiptUsage().retainedCount()); assertEquals(ReplicatedRangeStore.MAX_HOME_RECOVERIES,f.store.homeRecoveryCapacity().count());
            var extra=abort(initial(2000),2000); var before=f.store.homeRecoveryCapacity(); assertThrows(IllegalStateException.class,() -> f.store.estimateHomeRecovery(extra));
            assertThrows(IllegalStateException.class,() -> f.store.applyHomeRecovery(new ApplyPosition(1,ReplicatedRangeStore.MAX_HOME_RECOVERIES+1),extra).receipt()); assertEquals(before,f.store.homeRecoveryCapacity());
            assertEquals(original,f.store.applyHomeRecovery(new ApplyPosition(2,ReplicatedRangeStore.MAX_HOME_RECOVERIES+1),first).receipt()); assertEquals(before.count(),f.store.homeRecoveryCapacity().count());
        }
    }
    @Test void retainedHomeControlAndOrdinaryOrGateOwnershipCannotOverlapInEitherDirection() throws Exception {
        try (var f=new Fixture(root.resolve("identities"))) {
            var initial=initial(2); apply(f,1,cas(new OperationId(user,1,0),null,initial)); var occupied=abort(initial,0);
            var ordinary=new DistributedCommand(group,1,new byte[32],occupied.operation(),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[16],new byte[]{1}))); apply(f,2,ordinary);
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateHomeRecovery(occupied));
            var recovery=abort(initial,1); f.store.applyHomeRecovery(new ApplyPosition(1,3),recovery).receipt();
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateReceiptRetention(recovery.command())); assertThrows(IllegalArgumentException.class,() -> f.store.apply(new ApplyPosition(1,4),recovery.command()));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[16])));
            var gate=new SnapshotFence(new UUID(0,20),new UUID(0,21),domain,new UUID(0,300),new UUID(0,301),1,0,1,new byte[32],List.of(new SnapshotFence.Participant(group,group,1,new byte[32],recovery.operation(),new OperationId(new UUID(0,302),0,0),scope)));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateGateInstall(gate));
            var business=new DistributedCommand(group,1,new byte[32],new OperationId(new UUID(0,700),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[16],new byte[]{9})));
            var accepted=DistributedCommand.acceptedTransaction(business,new TransactionAcceptance(f.store.applied(),0,new UUID(0,701),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            var registration=TransactionStatus.begin(new UUID(0,20),new UUID(0,21),domain,new UUID(0,702),business.operationId(),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(group,group,1,new byte[32],accepted.payloadDigest(),null,false)));
            var preparation=new PreparedParticipant.Preparation(new UUID(0,703),1,recovery.operation(),registration,accepted);
            assertThrows(IllegalArgumentException.class,() -> f.store.estimatePreparation(preparation)); assertThrows(IllegalArgumentException.class,() -> f.store.estimateRejection(preparation));
            var finalization=new ParticipantFinalization(new UUID(0,703),1,group,recovery.operation(),registration.decide(TransactionStatus.Phase.ABORT));
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateFinalization(finalization));
            assertEquals(new ApplyPosition(1,3),f.store.applied());
        }
        try (var f=new Fixture(root.resolve("gate-first"))) {
            var recovery=abort(initial(3),0); var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[16])));
            var gate=new SnapshotFence(new UUID(0,20),new UUID(0,21),domain,new UUID(0,300),new UUID(0,301),1,0,1,new byte[32],List.of(new SnapshotFence.Participant(group,group,1,new byte[32],recovery.operation(),new OperationId(new UUID(0,302),0,0),scope)));
            f.store.applyGateInstall(new ApplyPosition(1,1),gate); assertThrows(IllegalArgumentException.class,() -> f.store.estimateHomeRecovery(recovery)); assertEquals(0,f.store.homeRecoveryCapacity().count());
        }
    }
    @Test void existingNativePreparedControlCannotBecomeProtectedHomeRecovery() throws Exception {
        try (var f=new Fixture(root.resolve("prepared-first"))) {
            f.store.enableScopedHistory(RangeMutationHistory.Limits.defaults()); var recovery=abort(initial(8),0); var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[16])));
            var business=new DistributedCommand(group,1,new byte[32],new OperationId(new UUID(0,704),1,0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[16],new byte[]{9})));
            DistributedCommand accepted; try (var pin=f.store.captureTracked(scope)) { accepted=f.store.planScopedCommit(pin,business,0,new UUID(0,705)); }
            var registration=TransactionStatus.begin(new UUID(0,20),new UUID(0,21),domain,new UUID(0,706),business.operationId(),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(group,group,1,new byte[32],accepted.payloadDigest(),null,false)));
            var preparation=new PreparedParticipant.Preparation(new UUID(0,703),1,recovery.operation(),registration,accepted); assertTrue(f.store.applyPrepared(new ApplyPosition(1,1),preparation).prepared());
            assertThrows(IllegalArgumentException.class,() -> f.store.estimateHomeRecovery(recovery)); assertThrows(IllegalArgumentException.class,() -> f.store.applyHomeRecovery(new ApplyPosition(1,2),recovery).receipt()); assertEquals(0,f.store.homeRecoveryCapacity().count());
        }
    }
    @Test void equalIndexWrongTermReceiptIsRejectedByLookupImageAndActivation() throws Exception {
        try (var f=new Fixture(root.resolve("cut"))) {
            var recovery=abort(initial(4),0); f.store.applyHomeRecovery(new ApplyPosition(2,1),recovery).receipt();
            var bad=new HomeRecoveryReceipt(recovery,new ApplyPosition(1,1),new CommandResult(CommandResult.Status.CONFLICT,List.of()));
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); dbField.setAccessible(true); var db=(RocksDB)dbField.get(f.store);
            var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); stateField.setAccessible(true); var state=(ColumnFamilyHandle)stateField.get(f.store);
            byte[] key=ByteBuffer.allocate(49).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x25).putLong(recoverySession.getMostSignificantBits()).putLong(recoverySession.getLeastSignificantBits()).putLong(0).putLong(0).array(); db.put(state,key,bad.encode());
            assertThrows(IllegalArgumentException.class,() -> f.store.homeRecoveryResult(recovery).receipt());
            try (var pin=f.store.capture()) { assertThrows(java.io.IOException.class,() -> NativeRangeImage.write(root.resolve("bad.image"),pin,f.store.ownership().interval(),limits)); }
            f.store.fence(); assertThrows(IllegalArgumentException.class,f.store::activate);
        }
    }
}
