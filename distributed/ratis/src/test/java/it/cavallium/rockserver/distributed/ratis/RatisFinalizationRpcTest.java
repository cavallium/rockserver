package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native participant effects with controlled DATA and HOME RPCs; no local finalization authority callback. */
class RatisFinalizationRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    final class Fixture implements AutoCloseable {
        final RatisParticipantPrepareTest.Node home,data;
        final TransactionStatus initial,decision;
        final RatisRoutedClient.PreparedParticipantOperation preparation;
        final RatisRoutedClient.PreparedFinalizationOperation operation;
        Fixture(String name,boolean commit) throws Exception {
            helper.root=root; home=helper.new Node(name+"-home",true); data=helper.new Node(name+"-data",false);
            try (var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
                initial=helper.register(home,data,pin,data.f.put(2),0);
                TransactionStatus status=initial;
                try (var ready=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,initial.transactionId())) {
                    preparation=data.routed.prepareParticipant(ready.preparation());
                    if (commit) {
                        var vote=data.routed.executeParticipant(preparation); status=initial.prepared(data.f.groupId,preparation.preparation().business().payloadDigest(),vote.position());
                        home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(30,1),1,0),initial,status));
                    }
                }
                decision=status.decide(commit ? TransactionStatus.Phase.COMMIT : TransactionStatus.Phase.ABORT);
                home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(30,1),1,1),status,decision));
                operation=data.routed.prepareFinalization(new ParticipantFinalization(home.f.groupId,1,data.f.groupId,new OperationId(new UUID(30,2),0,0),decision));
            }
        }
        void released() throws Exception {
            helper.released(data); assertEquals(0,data.f.machine.protectedFinalizationSlots()); assertEquals(0,data.f.machine.reservedFinalizationCount());
        }
        public void close() throws Exception { data.close(); home.close(); }
    }
    @Test void remoteCommitAndAbortResolveLostRepliesWithoutAnotherHomeReadOrAppendAfterFloorsAndReload() throws Exception {
        for (boolean commit:new boolean[]{false,true}) try (var f=new Fixture("retry-"+commit,commit)) {
            var reads=new java.util.concurrent.atomic.AtomicInteger(); f.home.wire.afterRead=reads::incrementAndGet;
            f.data.wire.lose=true; assertThrows(IOException.class,() -> f.data.routed.executeFinalization(f.operation));
            var actual=f.data.routed.finalizedParticipant(f.data.f.groupId,f.initial.transactionId()); assertNotNull(actual); assertEquals(f.operation.finalization(),actual.finalization());
            assertEquals(1,reads.get()); long index=f.data.wire.index;
            f.home.wire.afterRead=() -> { throw new AssertionError("Retained native proof must not reread HOME"); };
            assertEquals(actual.position(),f.data.routed.executeFinalization(f.operation)); assertEquals(index,f.data.wire.index);
            if (!commit) assertThrows(IOException.class,() -> f.data.routed.executeParticipant(f.preparation),"ABORT cannot grant a late PREPARE fresh acceptance");
            f.data.wire.command(f.data.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(31,1),5,0),5));
            f.home.wire.command(f.home.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(f.home.f.groupId,1,new byte[32],new OperationId(new UUID(31,2),5,0),5));
            var old=f.operation.descriptor(); var changed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
            f.data.routed.refresh(new RangeDirectory(helper.cluster,helper.database,f.data.directory.domainId(),1,new byte[32],1,f.data.directory.columns(),0,List.of(changed)));
            var restored=f.data.routed.restoreFinalization(f.operation.finalization(),old); assertEquals(0,restored.descriptor().configurationVersion());
            f.data.f.machine.takeSnapshot(); f.data.f.machine.pause(); f.data.f.machine.reinitialize(); index=f.data.wire.index;
            assertEquals(actual.position(),f.data.routed.executeFinalization(restored)); assertEquals(index,f.data.wire.index); f.released();
            assertArrayEquals(commit ? new byte[]{7} : null,f.data.routed.read(f.data.f.column,new byte[]{2}));
        }
    }
    @Test void frozenDecisionAndHomeBindingMustMatchFreshConfiguredAuthority() throws Exception {
        try (var f=new Fixture("binding",false)) {
            var foreign=new ParticipantFinalization(new UUID(32,1),1,f.data.f.groupId,f.operation.finalization().operation(),f.decision);
            assertThrows(IOException.class,() -> f.data.routed.executeFinalization(f.data.routed.prepareFinalization(foreign))); f.released(); assertEquals(0,f.data.wire.index);
            var fakeCommit=f.initial.prepared(f.data.f.groupId,f.preparation.preparation().business().payloadDigest(),new ApplyPosition(1,1)).decide(TransactionStatus.Phase.COMMIT);
            var changed=new ParticipantFinalization(f.home.f.groupId,1,f.data.f.groupId,f.operation.finalization().operation(),fakeCommit);
            assertThrows(IOException.class,() -> f.data.routed.executeFinalization(f.data.routed.prepareFinalization(changed))); f.released(); assertEquals(0,f.data.wire.index);
            assertNotNull(f.data.routed.executeFinalization(f.operation)); f.released();
        }
    }
    @Test void cancellationReleasesReadyAuthorityButNotSubmittedUnknownOwnership() throws Exception {
        try (var f=new Fixture("cancel",false)) {
            f.data.routed.authorizeFinalization(f.operation); assertEquals(1,f.data.f.machine.protectedFinalizationSlots());
            f.data.routed.cancelFinalization(f.operation); f.released(); assertNotNull(f.data.wire.propose(f.operation.bytes()).getException());
            f.data.routed.authorizeFinalization(f.operation); var pending=f.data.wire.propose(f.operation.bytes()); assertNull(pending.getException());
            f.data.f.machine.preAppendTransaction(pending); pending.initLogEntry(1,++f.data.wire.index); f.data.f.machine.cancelTransaction(pending);
            f.data.routed.cancelFinalization(f.operation); assertEquals(1,f.data.f.machine.reservedFinalizationCount()); assertEquals(1,f.data.f.counter("protectedAdmissions"));
            var receipt=RatisParticipantRecord.finalizationResult(f.data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray());
            assertEquals(receipt,f.data.routed.executeFinalization(f.operation)); assertEquals(1,f.data.wire.index); f.released();
        }
    }
    @Test void blockedHomeReadKeepsPreparingCapacityUntilActualReturnAndAllQueriesRequireDataAndFreshBarrier() throws Exception {
        try (var f=new Fixture("blocked",false); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            for (int kind:List.of(ScopedTransactionRpc.FINALIZATION_PREPARE,ScopedTransactionRpc.FINALIZATION_CANCEL)) {
                var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(kind,f.operation.bytes())));
                assertThrows(ExecutionException.class,() -> f.data.f.machine.query(request).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> f.home.f.machine.queryLinearizable(request).get(5,TimeUnit.SECONDS));
            }
            assertThrows(IllegalStateException.class,() -> f.data.f.machine.transactionHomes(f.home.statuses));
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Test HOME read timed out"); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var preparing=executor.submit(() -> { f.data.routed.authorizeFinalization(f.operation); return null; });
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(1,f.data.f.machine.protectedFinalizationSlots());
                assertThrows(IOException.class,() -> f.data.routed.cancelFinalization(f.operation)); assertEquals(1,f.data.f.machine.protectedFinalizationSlots());
                f.data.f.machine.notifyNotLeader(List.of()); assertEquals(1,f.data.f.machine.protectedFinalizationSlots());
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> preparing.get(5,TimeUnit.SECONDS)); f.released();
        }
    }
    @Test void retainedProofAtCompletedIndexMustHaveTheExactNativeTerm() throws Exception {
        try (var f=new Fixture("corrupt-cut",false)) {
            f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.data.f.machine.notifyTermIndexUpdated(2,1); f.data.wire.index=1;
            var position=f.data.routed.executeFinalization(f.operation); assertEquals(new ApplyPosition(2,2),position);
            var dbField=ReplicatedRangeStore.class.getDeclaredField("db"); dbField.setAccessible(true); var db=(RocksDB)dbField.get(f.data.f.store);
            var stateField=ReplicatedRangeStore.class.getDeclaredField("state"); stateField.setAccessible(true); var state=(ColumnFamilyHandle)stateField.get(f.data.f.store);
            var key=ByteBuffer.allocate(33).putLong(f.data.f.groupId.getMostSignificantBits()).putLong(f.data.f.groupId.getLeastSignificantBits()).put((byte)0x21).putLong(f.initial.transactionId().getMostSignificantBits()).putLong(f.initial.transactionId().getLeastSignificantBits()).array();
            var valid=new AbortedParticipant(f.operation.finalization(),position); db.put(state,key,new AbortedParticipant(f.operation.finalization(),new ApplyPosition(1,2)).encode());
            try { assertThrows(IOException.class,() -> f.data.routed.executeFinalization(f.operation)); assertEquals(2,f.data.wire.index); }
            finally { db.put(state,key,valid.encode()); }
            f.released();
        }
    }
}
