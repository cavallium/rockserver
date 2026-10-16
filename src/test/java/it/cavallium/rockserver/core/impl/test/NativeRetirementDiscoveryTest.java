package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeRetirementDiscoveryTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativeHomeRecoveryTest tx=new NativeHomeRecoveryTest();
    final NativeSnapshotHomeRetirementTest snapshots=new NativeSnapshotHomeRetirementTest();
    void configure(NativeHomeRecoveryTest.Fixture f) { f.store.configureHomeRecoveryReservations(new UUID(0,20),new UUID(0,21),tx.column); }
    @Test void userCursorFindsLateConsumedTransactionsAndUsesUnsignedOrderWithoutRequiringHomeFloor() throws Exception {
        try (var f=tx.new Fixture(root.resolve("transactions"))) {
            configure(f); int index=0;
            for (int n=0;n<18;n++) tx.apply(f,++index,tx.cas(new OperationId(tx.user,1,n),null,tx.initial(n)));
            var initial=tx.initial(18); var consumed=initial.decide(TransactionStatus.Phase.ABORT).finalized(tx.participant).consume();
            tx.apply(f,++index,tx.cas(new OperationId(tx.user,1,18),null,initial));
            var terminal=initial.decide(TransactionStatus.Phase.ABORT); var acknowledged=terminal.finalized(tx.participant);
            tx.apply(f,++index,tx.cas(new OperationId(tx.user,1,19),initial,terminal));
            tx.apply(f,++index,tx.cas(new OperationId(tx.user,1,20),terminal,acknowledged));
            tx.apply(f,++index,tx.cas(new OperationId(tx.user,1,21),acknowledged,consumed));
            var first=f.store.retirementCandidates(false,null,16); assertNull(first.candidate()); assertTrue(first.more()); assertArrayEquals(tx.key(tx.initial(15)),first.next());
            var second=f.store.retirementCandidates(false,first.next(),16); assertEquals(consumed,second.candidate().transaction()); assertFalse(second.more()); assertEquals(0,second.minimumEpoch()); assertEquals(f.store.applied(),second.position());
            byte[] cursor=second.next(); second.next()[0]=99; assertArrayEquals(tx.key(initial),cursor);
            var eof=f.store.retirementCandidates(false,cursor,1); assertNull(eof.next()); assertNull(eof.candidate()); assertFalse(eof.more()); assertNotNull(f.store.retirementCandidates(false,null,16).next());
            assertThrows(IllegalArgumentException.class,() -> f.store.retirementCandidates(false,new byte[15],1)); assertThrows(IllegalArgumentException.class,() -> f.store.retirementCandidates(false,null,17));
        }
    }
    @Test void snapshotCursorAdvancesAcrossLargeRowsAtByteBudgetThenReturnsConsumedSnapshot() throws Exception {
        try (var f=tx.new Fixture(root.resolve("snapshots"))) {
            snapshots.configure(f); var small=snapshots.fence(1); var local=small.participant(snapshots.dataGroup); var scope=new TransactionScope(List.of(TransactionScope.Span.point(new UUID(0,33),new byte[1100*1024])));
            SnapshotFenceStatus last=null;
            for (int n=0;n<3;n++) {
                var fence=new SnapshotFence(small.clusterId(), small.databaseId(), new UUID(n==2 ? Long.MIN_VALUE : 0,100+n), small.homeGroup(), small.homeEpoch(), new OperationId(new UUID(0,23),1,n), small.domains(), List.of(new SnapshotFence.Participant(local.domainId(), local.rangeId(), local.groupId(), local.rangeEpoch(), local.schemaDigest(), local.installOperation(), local.releaseOperation(), n==2 ? local.scope() : scope)));
                var status=SnapshotFenceStatus.begin(fence); assertEquals(CommandResult.Status.SUCCESS,tx.apply(f,n+1,snapshots.cas(n,null,status)).status()); last=status;
                if (n==2) { last=status.abort().consume(); assertEquals(CommandResult.Status.SUCCESS,tx.apply(f,4,snapshots.cas(3,status,last)).status()); }
            }
            var first=f.store.retirementCandidates(false,null,16); assertNull(first.candidate()); assertTrue(first.more()); assertArrayEquals(ByteBuffer.allocate(16).putLong(0).putLong(100).array(),first.next());
            var second=f.store.retirementCandidates(false,first.next(),1); assertNull(second.candidate()); assertTrue(second.more());
            var third=f.store.retirementCandidates(false,second.next(),16); assertEquals(last,third.candidate().snapshot()); assertFalse(third.more()); assertEquals(Long.MIN_VALUE,ByteBuffer.wrap(third.next()).getLong());
            assertThrows(IllegalStateException.class,() -> f.store.retirementCandidates(true,null,1));
        }
    }
    @Test void orphanCursorExcludesRetainedFamiliesUntilOriginalBusinessIsFencedAndCarriesCurrentFloor() throws Exception {
        try (var f=tx.new Fixture(root.resolve("orphans"))) {
            configure(f); var initial=tx.initial(1); tx.apply(f,1,tx.cas(new OperationId(tx.user,1,0),null,initial)); tx.apply(f,2,tx.cas(new OperationId(tx.user,1,1),initial,initial.decide(TransactionStatus.Phase.ABORT)));
            var retained=tx.abort(initial,0); var orphan=tx.abort(tx.initial(2),1); f.store.applyHomeRecovery(new ApplyPosition(1,3),retained); f.store.applyHomeRecovery(new ApplyPosition(1,4),orphan);
            assertNull(f.store.retirementCandidates(true,null,16).candidate());
            try (var before=f.store.capture()) {
                tx.apply(f,5,DistributedCommand.advanceSessionEpochFloor(tx.group,1,new byte[32],new OperationId(tx.admin,2,0),2));
                var skipped=f.store.retirementCandidates(true,null,1); assertNull(skipped.candidate()); assertTrue(skipped.more()); assertEquals(32,skipped.next().length);
                var found=f.store.retirementCandidates(true,skipped.next(),1); assertTrue(found.candidate().orphan()); assertEquals(orphan.before().asInitial(),found.candidate().transaction()); assertFalse(found.more()); assertEquals(2,found.minimumEpoch());
                assertEquals(0,before.stateView().minimumSessionEpoch()); assertEquals(before.stateView(),RangeStateView.decode(before.stateView().encode()));
                try (var now=f.store.capture()) { assertEquals(2,now.stateView().minimumSessionEpoch()); assertEquals(now.stateView(),RangeStateView.decode(now.stateView().encode())); }
            }
        }
    }
}
