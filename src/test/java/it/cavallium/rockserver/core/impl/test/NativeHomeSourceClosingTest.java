package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.RocksDBException;
import static org.junit.jupiter.api.Assertions.*;

/** Genuine native source CLOSE and HOME force/replay; adapter authority is tested separately. */
class NativeHomeSourceClosingTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeSourceClosingTest source=new NativeSourceClosingTest();
    final NativeSourceOutcomePublicationTest outcomes=source.outcomes;
    final NativeHomeRecoveryTest home=outcomes.home;
    NativeHomeSourceClosing.Request request(NativeHomeRecoveryTest.Fixture h,NativeSourceClosing closing)throws Exception {var cfg=h.store.configuration();return new NativeHomeSourceClosing.Request(home.group,1,new byte[32],cfg.term(),cfg.index(),cfg.data(),closing);}
    OperationRerouteOrigin another(NativeSourceOutcomeCredit.Request original,DistributedCommand command){var o=original.origin();return new OperationRerouteOrigin(o.clusterId(),o.databaseId(),o.domainId(),o.source(),command.operationId(),command.receiptDigest(),o.absenceCommitment(),o.absenceCut(),o.targetDirectoryDigest(),o.planDigest(),o.targetParticipantCount(),new OperationId(home.user,5,512));}
    @Test void preparedBeforeFenceCannotCreateReservedAfterFenceButOriginalIdentitySettlesWithoutFakeSourcePublication()throws Exception {
        Path path=root.resolve("home");NativeHomeSourceClosing closing;TransactionStatus terminal;DistributedCommand queued;HomeRecovery originalAbort;HomeRecoveryReceipt originalReceipt;
        try(var f=source.topology.new Fixture(root.resolve("source"));var h=home.new Fixture(path)){
            f.freeze();f.seed();var original=outcomes.reserve(f,h,1,1);var reserved=original.home().definition();
            var late=another(original,f.write(f.parent,2,8));var pending=TransactionStatus.reserve(late,reserved.domains().getFirst());queued=home.cas(late.firstControl(),null,pending);
            h.store.estimateHomeRegistration(queued); // PREPARE observed source ABSENT before either closing force.
            var sourceClose=f.parent.applySourceClosing(new ApplyPosition(1,2),source.request(f));
            closing=h.store.applyHomeSourceClosing(new ApplyPosition(1,2),request(h,sourceClose));
            assertEquals(closing,NativeHomeSourceClosing.decode(closing.encode()));
            assertEquals(CommandResult.Status.CONFLICT,home.apply(h,3,queued).status());assertNull(h.store.readCurrentPoint(home.column,home.key(pending)));
            assertEquals(CommandResult.Status.SUCCESS,home.apply(h,4,home.cas(reserved.origin().firstControl(),null,reserved)).status());
            assertEquals(reserved,h.store.homeSourceReferences(closing,null,16).transaction());
            f.parent.requireSourceClosedNoCredit(sourceClose,reserved.origin());terminal=reserved.abortReservation(TransactionStatus.NoEffectOutcome.SOURCE_CLOSED);
            var abort=new HomeRecovery(HomeRecovery.Action.ABORT,home.cas(new OperationId(home.recoverySession,0,10),reserved,terminal),null);originalAbort=abort;
            var receipt=h.store.applyHomeRecovery(new ApplyPosition(1,5),abort).receipt();originalReceipt=receipt;assertEquals(CommandResult.Status.SUCCESS,receipt.result().status());assertEquals(CommandResult.Status.CONFLICT,terminal.terminalBusinessResult().status());assertNull(f.parent.operationResult(original.original().operationId(),original.original().receiptDigest()));
            assertEquals(receipt,h.store.applyHomeRecovery(new ApplyPosition(1,6),abort).receipt());
            var consume=new HomeRecovery(HomeRecovery.Action.CONSUME,home.cas(new OperationId(home.recoverySession,0,11),terminal,terminal.consume()),null);h.store.applyHomeRecovery(new ApplyPosition(1,7),consume);
            assertNull(h.store.homeSourceReferences(closing,null,16).transaction());
            assertThrows(IllegalArgumentException.class,()->h.store.applyHomeSourceClosing(new ApplyPosition(1,8),new NativeHomeSourceClosing.Request(UUID.randomUUID(),1,new byte[32],0,-1,new byte[]{1},sourceClose)));
        }
        try(var h=home.new Fixture(path)){
            h.store.configureHomeRecoveryReservations(terminal.clusterId(),terminal.databaseId(),home.column);assertEquals(closing,h.store.homeSourceClosing(closing.request().source().request().group(),closing.request().source().request().header().participant(TopologyChange.Stage.FREEZE,closing.request().source().request().group()).rangeEpoch()));
            assertEquals(CommandResult.Status.CONFLICT,home.apply(h,8,queued).status());assertEquals(closing,h.store.applyHomeSourceClosing(new ApplyPosition(1,9),closing.request()));assertEquals(originalReceipt,h.store.homeRecoveryResult(originalAbort).receipt());assertEquals(originalReceipt,h.store.applyHomeRecovery(new ApplyPosition(1,10),originalAbort).receipt());assertEquals(terminal.consume(),TransactionStatus.decode(h.store.readCurrentPoint(home.column,home.key(terminal))));
        }
    }
    @Test void unknownForceBeforeAndAfterWriteRequiresExactRecipeAndSurvivesColdReopen()throws Exception {
        for(boolean persisted:List.of(false,true)){
            Path path=root.resolve("home-"+persisted);NativeHomeSourceClosing.Request recipe;TransactionStatus reserved;
            try(var f=source.topology.new Fixture(root.resolve("source-"+persisted));var h=home.new Fixture(path)){
                f.freeze();f.seed();var original=outcomes.reserve(f,h,1,1);reserved=original.home().definition();var closed=f.parent.applySourceClosing(new ApplyPosition(1,2),source.request(f));recipe=request(h,closed);
                var once=new AtomicBoolean();h.store.homeSourceClosingForceObserver(after->{if(after==persisted && once.compareAndSet(false,true))throw new RocksDBException("Lost HOME closing force ACK");});
                assertThrows(RocksDBException.class,()->h.store.applyHomeSourceClosing(new ApplyPosition(1,2),recipe));assertNotNull(h.store.homeSourceClosingAttempt(closed.request().group(),original.origin().source().rangeEpoch()));assertEquals(persisted?new ApplyPosition(1,2):new ApplyPosition(1,1),h.store.applied());
                assertThrows(IllegalArgumentException.class,()->h.store.applyHomeSourceClosing(new ApplyPosition(1,3),new NativeHomeSourceClosing.Request(home.group,1,new byte[32],0,-1,new byte[]{2},closed)));
            }
            try(var h=home.new Fixture(path)){
                h.store.configureHomeRecoveryReservations(reserved.clusterId(),reserved.databaseId(),home.column);var closed=h.store.applyHomeSourceClosing(new ApplyPosition(1,persisted?3:2),recipe);assertEquals(new ApplyPosition(1,2),closed.position());
                var consume=new HomeRecovery(HomeRecovery.Action.ABORT,home.cas(new OperationId(home.recoverySession,0,20),reserved,reserved.abortReservation(TransactionStatus.NoEffectOutcome.SOURCE_CLOSED)),null);long next=h.store.applied().index()+1;assertEquals(persisted?4:3,next,"Pre-write failure did not consume a native index");assertEquals(CommandResult.Status.SUCCESS,h.store.applyHomeRecovery(new ApplyPosition(1,next),consume).receipt().result().status());
            }
        }
    }
    @Test void creditedOriginalCannotBeMisrepresentedAsNoCreditAndOrdinaryTransactionAndSnapshotReferencesAreFenced()throws Exception {
        try(var f=source.topology.new Fixture(root.resolve("source-credit"));var h=home.new Fixture(root.resolve("tx"));var snapshots=home.new Fixture(root.resolve("snap"))){
            f.freeze();f.seed();f.parent.configureSourceOutcomeCapacity(2,1024);var original=outcomes.reserve(f,h,1,1);f.parent.applySourceOutcomeCredit(new ApplyPosition(1,2),original);var closed=f.parent.applySourceClosing(new ApplyPosition(1,3),source.request(f));assertThrows(IllegalArgumentException.class,()->f.parent.requireSourceClosedNoCredit(closed,original.origin()));
            h.store.applyHomeSourceClosing(new ApplyPosition(1,2),request(h,closed));var directory=f.definition.sourceDirectory();var range=f.definition.parents().getFirst();
            var tx=TransactionStatus.begin(directory.clusterId(),directory.databaseId(),new UUID(0,900),new OperationId(home.user,1,9),List.of(DomainBinding.of(directory)),new byte[32],List.of(new TransactionStatus.Participant(directory.domainId(),range.rangeId(),range.groupId(),range.rangeEpoch(),directory.schemaDigest(),new byte[32],null,false)));
            assertEquals(CommandResult.Status.CONFLICT,home.apply(h,3,home.cas(new OperationId(home.user,1,8),null,tx)).status());assertEquals(1,f.parent.sourceOutcomeCredits().size());
            snapshots.store.configureSnapshotHome(directory.clusterId(),directory.databaseId(),home.column);snapshots.store.applyHomeSourceClosing(new ApplyPosition(1,1),request(snapshots,closed));
            var fence=new SnapshotFence(directory.clusterId(),directory.databaseId(),new UUID(0,901),home.group,1,new OperationId(home.user,1,10),List.of(DomainBinding.of(directory)),List.of(new SnapshotFence.Participant(directory.domainId(),range.rangeId(),range.groupId(),range.rangeEpoch(),directory.schemaDigest(),new OperationId(home.admin,1,10),new OperationId(home.admin,1,11),new TransactionScope(List.of(TransactionScope.Span.point(directory.columns().iterator().next(),new byte[]{1}))))));
            var key=ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();var command=new DistributedCommand(home.group,1,new byte[32],new OperationId(home.user,1,11),List.of(new Mutation(Mutation.Kind.PUT,home.column,key,SnapshotFenceStatus.begin(fence).encode())),List.of(new KeyPredicate(home.column,key,null)));
            assertEquals(CommandResult.Status.CONFLICT,home.apply(snapshots,2,command).status());assertNull(snapshots.store.readCurrentPoint(home.column,key));
        }
    }
    @Test void portableHomeImageRetainsFenceAndPreFenceImageCannotResurrectReferences()throws Exception {
        Path before=root.resolve("before.image"),after=root.resolve("after.image");NativeHomeSourceClosing closing;TransactionStatus reserved;
        try(var f=source.topology.new Fixture(root.resolve("source-image"));var h=home.new Fixture(root.resolve("home-image"))){
            f.freeze();f.seed();var original=outcomes.reserve(f,h,1,1);reserved=original.home().definition();try(var pin=h.store.capture()){NativeRangeImage.write(before,pin,pin.interval(),home.limits);}
            var sourceClose=f.parent.applySourceClosing(new ApplyPosition(1,2),source.request(f));closing=h.store.applyHomeSourceClosing(new ApplyPosition(1,2),request(h,sourceClose));
            try(var pin=h.store.capture()){NativeRangeImage.write(after,pin,pin.interval(),home.limits);}h.store.fence();assertThrows(java.io.IOException.class,()->h.store.installSnapshot(before,home.limits));h.store.activate();assertNotNull(h.store.homeSourceClosing(sourceClose.request().group(),original.origin().source().rangeEpoch()));
        }
        NativeRangeImage.inspect(after,home.limits);
        try(var h=home.new Fixture(root.resolve("home-image-target"))){h.store.configureHomeRecoveryReservations(reserved.clusterId(),reserved.databaseId(),home.column);h.store.fence();h.store.installSnapshot(after,home.limits);h.store.activate();assertEquals(closing,h.store.homeSourceClosing(closing.request().source().request().group(),reserved.origin().source().rangeEpoch()));assertEquals(reserved,h.store.homeSourceReferences(closing,null,16).transaction());}
    }

}
