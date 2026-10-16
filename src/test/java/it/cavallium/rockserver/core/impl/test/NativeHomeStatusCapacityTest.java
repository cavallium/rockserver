package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(120)
class NativeHomeStatusCapacityTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest helper=new NativeHomeRecoveryTest();
    void configure(NativeHomeRecoveryTest.Fixture f){var initial=helper.initial(0);f.store.configureHomeRecoveryReservations(initial.clusterId(),initial.databaseId(),helper.column);}
    DistributedCommand cas(long sequence,TransactionStatus before,TransactionStatus next){return helper.cas(new OperationId(helper.user,1,sequence),before,next);}
    int finish(NativeHomeRecoveryTest.Fixture f,int index,int id) throws Exception {
        var initial=helper.initial(id);var abort=initial.decide(TransactionStatus.Phase.ABORT);var terminal=abort.finalized(helper.participant);
        for(var command:List.of(cas(4L*id,null,initial),cas(4L*id+1,initial,abort),cas(4L*id+2,abort,terminal),cas(4L*id+3,terminal,terminal.consume())))assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,command).status());return index;
    }
    @Test void consumedRowsSaturateRacingAdmissionButPromisedCompletionAndRetirementRemainUsable() throws Exception {
        var path=root.resolve("source");var image=root.resolve("home.image");int index=0;var pending=helper.initial(2000);var raced=helper.initial(2001);var registration=cas(9000,null,pending);var rejected=cas(9001,null,raced);TransactionStatus consumed;
        try(var f=helper.new Fixture(path)) {
            configure(f);for(int n=0;n<ReplicatedRangeStore.MAX_HOME_STATUS_ROWS-1;n++)index=finish(f,index,n);
            assertEquals(1023,f.store.homeStatusCapacity().rows());assertEquals(0,f.store.homeRecoveryCapacity().effectiveCount());
            f.store.validate(registration);f.store.validate(rejected);assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,registration).status());
            var capacity=f.store.homeStatusCapacity();assertEquals(1024,capacity.rows());assertEquals(1024L*ReplicatedRangeStore.HOME_TRANSACTION_STATUS_BYTES,capacity.reservedBytes());
            assertThrows(IllegalStateException.class,()->f.store.estimateHomeRegistration(rejected));
            assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,rejected).status());assertNull(f.store.readCurrentPoint(helper.column,helper.key(raced)));
            assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,rejected).status());assertEquals(capacity.reservedBytes(),f.store.homeStatusCapacity().reservedBytes());
            var abort=helper.abort(pending,9000);assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,++index),abort).receipt().result().status());
            var fixture=new NativeHomeRetirementTest();var proof=fixture.dataFence(pending,2);var acknowledged=abort.next().finalized(helper.participant);
            var ack=new HomeRecovery(HomeRecovery.Action.ACK,helper.cas(new OperationId(helper.recoverySession,0,9001),abort.next(),acknowledged),HomeRecovery.Proof.from(proof));
            assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,++index),ack).receipt().result().status());consumed=acknowledged.consume();
            var consume=new HomeRecovery(HomeRecovery.Action.CONSUME,helper.cas(new OperationId(helper.recoverySession,0,9002),acknowledged,consumed),null);
            assertEquals(CommandResult.Status.SUCCESS,f.store.applyHomeRecovery(new ApplyPosition(1,++index),consume).receipt().result().status());assertEquals(1024,f.store.homeStatusCapacity().rows());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits);}
        }
        var first=helper.initial(0);NativeRangeImage.inspectHomeRecoveryReservations(image,helper.limits,first.clusterId(),first.databaseId(),helper.column);
        try(var f=helper.new Fixture(path)) {
            configure(f);assertEquals(1024,f.store.homeStatusCapacity().rows());
            helper.apply(f,++index,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),2));
            var fixture=new NativeHomeRetirementTest();var retirement=new HomeRetirement(fixture.identity(first),first.decide(TransactionStatus.Phase.ABORT).finalized(helper.participant).consume(),List.of(fixture.dataFence(first,2)));
            f.store.applyHomeRetirement(new ApplyPosition(1,++index),retirement);assertEquals(1023,f.store.homeStatusCapacity().rows());
            var replacement=TransactionStatus.begin(first.clusterId(), first.databaseId(), new UUID(0,50000), new OperationId(helper.user,3,1), first.domains(), new byte[32], first.participants());
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,helper.cas(new OperationId(helper.user,3,2),null,replacement)).status());assertEquals(1024,f.store.homeStatusCapacity().rows());
        }
        try(var f=helper.new Fixture(root.resolve("image-target"))){configure(f);f.store.fence();f.store.installSnapshot(image,helper.limits);f.store.activate();assertEquals(1024,f.store.homeStatusCapacity().rows());assertEquals(consumed,TransactionStatus.decode(f.store.readCurrentPoint(helper.column,helper.key(pending))));}
    }
    @Test void unknownNativeWriteReconstructsActualReservationOnColdRetry() throws Exception {
        var path=root.resolve("unknown");var initial=helper.initial(0);var command=cas(0,null,initial);
        try(var f=helper.new Fixture(path)){configure(f);assertEquals(0,f.store.homeStatusCapacity().rows());f.store.homeStatusWriteObserver(()->{throw new org.rocksdb.RocksDBException("unknown after actual write");});assertThrows(org.rocksdb.RocksDBException.class,()->helper.apply(f,1,command));}
        try(var f=helper.new Fixture(path)){configure(f);assertEquals(new ApplyPosition(1,1),f.store.applied());assertEquals(1,f.store.homeStatusCapacity().rows());assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,2,command).status());assertEquals(1,f.store.homeStatusCapacity().rows());}
    }
    SnapshotFence largeFence(int id,long epoch) {
        byte[] key=new byte[900_000];
        return new SnapshotFence(new UUID(0,20), new UUID(0,21), new UUID(0,10000+id), helper.group, 1, new OperationId(new UUID(0,23),epoch,id), List.of(new DomainBinding(new UUID(0,22), 1, new byte[32], 1, new byte[32])), List.of(
            new SnapshotFence.Participant(new UUID(0,22), new UUID(0,31), helper.participant, 1, new byte[32], new OperationId(new UUID(0,32),epoch,2L*id), new OperationId(new UUID(0,32),epoch,2L*id+1), new TransactionScope(List.of(TransactionScope.Span.point(new UUID(0,33),key))))));
    }
    @Test void snapshotBytePromisesIncludeCaptureGrowthAndConsumedRowsUntilRetirement() throws Exception {
        var snapshotHelper=new NativeSnapshotHomeRetirementTest();var path=root.resolve("snapshots");var image=root.resolve("snapshot.image");int index=0;SnapshotFence last=null;int rows=0;long reserved;
        try(var f=helper.new Fixture(path)) {
            snapshotHelper.configure(f);
            while(true) {
                var fence=largeFence(rows,2);long charge=ReplicatedRangeStore.homeSnapshotStatusBytes(fence);var capacity=f.store.homeStatusCapacity();
                if(capacity.reservedBytes()+charge>ReplicatedRangeStore.MAX_HOME_STATUS_BYTES)break;
                var initial=SnapshotFenceStatus.begin(fence);
                assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,snapshotHelper.cas(index,null,initial)).status());
                if(capacity.reservedBytes()+2*charge<=ReplicatedRangeStore.MAX_HOME_STATUS_BYTES){var abort=initial.abort();helper.apply(f,++index,snapshotHelper.cas(index,initial,abort));helper.apply(f,++index,snapshotHelper.cas(index,abort,abort.consume()));}
                last=fence;rows++;
            }
            assertTrue(rows>1 && rows<ReplicatedRangeStore.MAX_HOME_STATUS_ROWS);reserved=f.store.homeStatusCapacity().reservedBytes();
            var excess=SnapshotFenceStatus.begin(largeFence(rows,2));var command=snapshotHelper.cas(10000,null,excess);
            assertThrows(IllegalStateException.class,()->f.store.validate(command));assertEquals(CommandResult.Status.CONFLICT,helper.apply(f,++index,command).status());assertEquals(rows,f.store.homeStatusCapacity().rows());
            var initial=SnapshotFenceStatus.begin(last);var ready=initial.ready(helper.participant,new ApplyPosition(1,1));
            var captured=ready.capture(helper.participant,new SnapshotFenceStatus.Capture(new UUID(0,40),new UUID(0,41),new ApplyPosition(1,2)));
            var complete=captured.complete();var consumed=complete.consume();
            for(var pair:List.of(List.of(initial,ready),List.of(ready,captured),List.of(captured,complete),List.of(complete,consumed)))assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,snapshotHelper.cas(index,pair.get(0),pair.get(1))).status());
            assertEquals(reserved,f.store.homeStatusCapacity().reservedBytes());assertEquals(rows,f.store.homeStatusCapacity().rows());
            try(var pin=f.store.capture()){NativeRangeImage.write(image,pin,f.store.ownership().interval(),helper.limits);}
        }
        NativeRangeImage.inspectSnapshotHome(image,helper.limits,snapshotHelper.cluster,snapshotHelper.database,helper.column);
        try(var f=helper.new Fixture(path)) {
            snapshotHelper.configure(f);assertEquals(rows,f.store.homeStatusCapacity().rows());assertEquals(reserved,f.store.homeStatusCapacity().reservedBytes());
            helper.apply(f,++index,DistributedCommand.advanceSessionEpochFloor(helper.group,1,new byte[32],new OperationId(helper.admin,3,0),3));
            var original=largeFence(0,2);var consumed=SnapshotFenceStatus.begin(original).abort().consume();
            var retirement=new SnapshotHomeRetirement(snapshotHelper.identity(original),consumed,List.of(new SnapshotFenceProof(original,helper.participant,3,new ApplyPosition(2,20))));
            f.store.applySnapshotHomeRetirement(new ApplyPosition(1,++index),retirement);assertEquals(rows-1,f.store.homeStatusCapacity().rows());
            var replacement=SnapshotFenceStatus.begin(largeFence(100,4));var command=new DistributedCommand(helper.group,1,new byte[32],new OperationId(helper.user,4,0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,snapshotHelper.key(replacement.fence()),replacement.encode())),List.of(new KeyPredicate(helper.column,snapshotHelper.key(replacement.fence()),null)));
            assertEquals(CommandResult.Status.SUCCESS,helper.apply(f,++index,command).status());assertEquals(rows,f.store.homeStatusCapacity().rows());
        }
        try(var f=helper.new Fixture(root.resolve("snapshot-target"))){snapshotHelper.configure(f);f.store.fence();f.store.installSnapshot(image,helper.limits);f.store.activate();assertEquals(rows,f.store.homeStatusCapacity().rows());assertEquals(reserved,f.store.homeStatusCapacity().reservedBytes());}
    }

}
