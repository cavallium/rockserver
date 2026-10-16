package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Durable coordinator files with controlled RPC/native apply, not power-loss or quorum acceptance. */
class RatisDurableTransactionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    OperationId homeOperation(long n) { return new OperationId(new UUID(0,4),1,n); }
    record Plan(TransactionStatus initial,RatisRoutedClient.PreparedCommand registration,RatisDurableTransaction.Intent intent) {}
    Plan plan(RatisParticipantPrepareTest.Node home,RatisParticipantPrepareTest.Node data,it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore.ScopedHistoryPin pin) throws Exception {
        var accepted=data.f.machine.participantIntent(pin,data.f.put(2));
        var participant=new TransactionStatus.Participant(data.directory.domainId(), data.f.groupId, data.f.groupId, 1, new byte[32], accepted.payloadDigest(), null, false);
        var intent=new RatisDurableTransaction.Intent(data.directory.ranges().getFirst(),accepted,helper.control(0),new OperationId(new UUID(0,1501),3,0));
        var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,1500), accepted.operationId(), List.of(new DomainBinding(data.directory.domainId(), data.directory.generation(), data.directory.payloadDigest(), 1, new byte[32])), RatisDurableTransaction.planDigest(List.of(intent)), List.of(participant));
        return new Plan(initial,home.statuses.prepare(homeOperation(0),null,initial),intent);
    }
    RatisDurableTransaction create(Path path,RatisParticipantPrepareTest.Node home,RatisParticipantPrepareTest.Node data,Plan plan) throws Exception {
        return RatisDurableTransaction.create(path,home.statuses,data.routed,plan.initial(),plan.registration(),List.of(plan.intent()));
    }
    @Test void deniedPersistenceWorkspaceDoesNotEncodeRetainedMetadata()throws Exception {
        helper.root=root;
        try(var home=helper.new Node("count-home",true);var data=helper.new Node("count-data",false);var pin=data.f.store.captureTracked(helper.scope(data,1,4));
                var budget=org.mockito.Mockito.spy(new RatisRequestBudget(RemoteMutationOwnerLimits.defaults()))) {
            var plan=plan(home,data,pin);var homes=org.mockito.Mockito.spy(home.statuses);
            var retained=new java.util.concurrent.atomic.AtomicReference<CommandResult>();
            org.mockito.Mockito.doAnswer(call->{var result=org.mockito.Mockito.spy(home.statuses.execute(call.getArgument(0)));retained.set(result);return result;})
                    .when(homes).execute(org.mockito.ArgumentMatchers.any(RatisRoutedClient.PreparedCommand.class));
            try(var credit=budget.request(RemoteMutationToken.Kind.RANGE,0);
                    var journal=RatisDurableTransaction.createAccounted(root.resolve("count.bin"),homes,data.routed,plan.initial(),plan.registration(),List.of(plan.intent()),null,budget,credit)) {
                org.mockito.Mockito.doThrow(new IllegalStateException("Workspace denied before metadata encoding")).when(budget).work(org.mockito.ArgumentMatchers.anyLong());
                assertThrows(IOException.class,journal::executeHome);
                assertNotNull(retained.get());org.mockito.Mockito.verify(retained.get(),org.mockito.Mockito.never()).encode();
                assertEquals(plan.initial(),home.statuses.read(plan.initial().transactionId()));
            }
        }
    }
    @Test void lostRegistrationPrepareAndFinalizeRepliesRecoverFromExactForcedRequestsAndNativeProofs() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("home",true); var data=helper.new Node("data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("transaction.bin"); var journal=create(path,home,data,plan);
            try {
                home.wire.lose=true; assertThrows(IOException.class,journal::executeHome); assertEquals(plan.initial(),home.statuses.read(plan.initial().transactionId()));
                assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,data.routed));
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                assertEquals(plan.initial(),journal.refreshHome()); assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status());
                try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,plan.initial().transactionId())) {
                    journal.freezePreparation(data.routed.prepareParticipant(preparation.preparation())); data.wire.lose=true;
                    var owned=journal; assertThrows(IOException.class,() -> owned.executePreparation(data.f.groupId));
                }
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                var prepared=journal.resolveParticipant(data.f.groupId); assertTrue(prepared.vote().prepared());
                var ready=plan.initial().prepared(data.f.groupId,plan.intent().acceptedBusiness().payloadDigest(),prepared.vote().position());
                journal.queueHome(home.statuses.prepare(homeOperation(1),plan.initial(),ready)); assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status());
                var commit=ready.decide(TransactionStatus.Phase.COMMIT); journal.queueHome(home.statuses.prepare(homeOperation(2),ready,commit)); journal.executeHome();
                try (var finalization=data.f.machine.prepareFinalization(home.statuses,plan.initial().transactionId(),plan.intent().finalizeControl())) {
                    journal.freezeFinalization(data.routed.prepareFinalization(finalization.finalization())); data.wire.lose=true;
                    var owned=journal; assertThrows(IOException.class,() -> owned.executeFinalization(data.f.groupId));
                }
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                var outcome=journal.resolveParticipant(data.f.groupId).outcome(); assertNotNull(outcome); assertEquals(TransactionStatus.Phase.COMMIT,outcome.finalization().decision().phase());
                assertArrayEquals(plan.intent().acceptedBusiness().acceptance().result(),outcome.result()); assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2}));
                var ack=commit.finalized(data.f.groupId); journal.queueRecovery(ack); journal.executeHome();
                var consumed=ack.consume(); journal.queueRecovery(consumed); journal.executeHome(); assertTrue(journal.snapshot().observedHome().gcEligible());
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                assertTrue(journal.snapshot().observedHome().gcEligible()); assertEquals(outcome,journal.snapshot().participant(data.f.groupId).outcome()); helper.released(data);
            } finally { journal.close(); }
        }
    }
    @Test void protectedAbortAndAckKeepExactRequestsReceiptsAndIndependentHighWaterAcrossReopenAndHomeFloor() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("protected-home",true); var data=helper.new Node("protected-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("protected.bin"); var journal=create(path,home,data,plan);
            try {
                journal.executeHome(); var ordinary=journal.snapshot().lastHome(); var abort=plan.initial().decide(TransactionStatus.Phase.ABORT);
                var initiallyOwned=journal; assertThrows(IllegalArgumentException.class,() -> initiallyOwned.queueHome(home.statuses.prepare(homeOperation(1),plan.initial(),abort)));
                journal.queueRecovery(abort); var frozen=journal.snapshot().pendingRecovery();
                assertEquals(0,frozen.recovery().operation().epoch()); assertEquals(0,frozen.recovery().operation().sequence()); assertEquals(ordinary,journal.snapshot().lastHome());
                home.wire.command(home.directory.ranges().getFirst(),DistributedCommand.advanceSessionEpochFloor(home.f.groupId,1,new byte[32],new OperationId(home.f.admin,2,0),2));
                home.wire.lose=true; var owned=journal; assertThrows(IOException.class,owned::executeHome);
                var nativeAbort=home.statuses.recoveryResult(frozen); assertNotNull(nativeAbort); assertNull(journal.snapshot().recoveryReceipt());
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                assertArrayEquals(frozen.bytes(),journal.snapshot().pendingRecovery().bytes()); assertEquals(frozen.descriptor(),journal.snapshot().pendingRecovery().descriptor());
                assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status()); assertEquals(nativeAbort,journal.snapshot().recoveryReceipt()); assertEquals(ordinary,journal.snapshot().lastHome());
                var ack=abort.finalized(data.f.groupId); owned=journal; var noProof=owned; assertThrows(IllegalArgumentException.class,() -> noProof.queueRecovery(ack));
                pin.close();
                try (var authorization=data.f.machine.prepareFinalization(home.statuses,plan.initial().transactionId(),plan.intent().finalizeControl())) {
                    journal.freezeFinalization(data.routed.prepareFinalization(authorization.finalization())); journal.executeFinalization(data.f.groupId);
                }
                journal.queueRecovery(ack); var frozenAck=journal.snapshot().pendingRecovery();
                assertEquals(frozen.recovery().operation().sessionId(),frozenAck.recovery().operation().sessionId()); assertEquals(1,frozenAck.recovery().operation().sequence());
                frozenAck.recovery().requireOutcome(journal.snapshot().participant(data.f.groupId).outcome()); home.wire.lose=true; owned=journal; assertThrows(IOException.class,owned::executeHome);
                var nativeAck=home.statuses.recoveryResult(frozenAck); journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);
                assertArrayEquals(frozenAck.bytes(),journal.snapshot().pendingRecovery().bytes()); journal.executeHome(); assertEquals(nativeAck,journal.snapshot().recoveryReceipt());
                long index=home.wire.index; journal.executeHome(); assertEquals(index,home.wire.index); assertEquals(CommandResult.Status.CONFLICT,journal.result().status()); assertFalse(journal.snapshot().observedHome().consumed());
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed); assertEquals(nativeAck,journal.snapshot().recoveryReceipt()); assertEquals(ordinary,journal.snapshot().lastHome());
                journal.queueRecovery(ack.consume());var frozenConsume=journal.snapshot().pendingRecovery();assertEquals(HomeRecovery.Action.CONSUME,frozenConsume.recovery().action());assertEquals(frozenAck.recovery().operation().sessionId(),frozenConsume.recovery().operation().sessionId());assertEquals(2,frozenConsume.recovery().operation().sequence());journal.executeHome();
                assertEquals(ordinary,journal.snapshot().lastHome());assertEquals(frozenConsume.recovery(),journal.snapshot().recoveryReceipt().recovery());journal.close();journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed);assertArrayEquals(frozenConsume.bytes(),journal.snapshot().pendingRecovery().bytes());assertTrue(journal.snapshot().observedHome().consumed());
            } finally { journal.close(); }
        }
    }

    @Test void anOldSuccessfulRegistrationReceiptDoesNotRegressAnObservedAbort() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("old-home",true); var data=helper.new Node("old-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin);
            try (var journal=create(root.resolve("old.bin"),home,data,plan)) {
                home.wire.lose=true; assertThrows(IOException.class,journal::executeHome);
                var abort=plan.initial().decide(TransactionStatus.Phase.ABORT); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,1502),2,0),plan.initial(),abort));
                assertEquals(abort,journal.refreshHome()); assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status()); assertEquals(abort,journal.snapshot().observedHome());
                var ack=abort.finalized(data.f.groupId); assertThrows(IllegalArgumentException.class,() -> journal.queueHome(home.statuses.prepare(homeOperation(1),abort,ack)));
                assertEquals(abort,home.statuses.read(plan.initial().transactionId())); assertNull(data.routed.finalizedParticipant(data.f.groupId,plan.initial().transactionId()));
            }
        }
    }
    @Test void fabricatedVoteCannotBeReportedAndAnUncertainHomeRequestCannotBeReplaced() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("proof-home",true); var data=helper.new Node("proof-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin);
            try (var journal=create(root.resolve("proof.bin"),home,data,plan)) {
                var abort=plan.initial().decide(TransactionStatus.Phase.ABORT);
                assertThrows(IllegalStateException.class,() -> journal.queueHome(home.statuses.prepare(homeOperation(1),plan.initial(),abort)));
                journal.executeHome(); var fake=plan.initial().prepared(data.f.groupId,plan.intent().acceptedBusiness().payloadDigest(),new ApplyPosition(1,1));
                assertThrows(IllegalArgumentException.class,() -> journal.queueHome(home.statuses.prepare(homeOperation(1),plan.initial(),fake)));
                assertEquals(plan.initial(),home.statuses.read(plan.initial().transactionId()));
            }
        }
    }
    @Test void persistenceFailureFencesBeforeParticipantRpcAndExistingStateReopens() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("write-home",true); var data=helper.new Node("write-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("write.bin"); var journal=create(path,home,data,plan);
            try {
                journal.executeHome();
                try (var preparation=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,plan.initial().transactionId())) {
                    Files.createDirectory(path.resolveSibling(path.getFileName()+".pending")); var operation=data.routed.prepareParticipant(preparation.preparation()); var owned=journal;
                    assertThrows(IOException.class,() -> owned.freezePreparation(operation)); assertThrows(IOException.class,() -> owned.executePreparation(data.f.groupId)); assertEquals(0,data.wire.index);
                }
                journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,data.routed); assertEquals(plan.initial(),journal.snapshot().observedHome()); assertNull(journal.snapshot().participant(data.f.groupId).preparation()); helper.released(data);
            } finally { journal.close(); }
        }
    }
    @Test void blockedRpcRetainsFileOwnershipAndCloseRemainsRetryableAfterReturn() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("busy-home",true); var data=helper.new Node("busy-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4)); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var plan=plan(home,data,pin); Path path=root.resolve("busy.bin"); var journal=create(path,home,data,plan);
            try {
                journal.executeHome(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
                home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException e) { throw new AssertionError(e); } };
                var pending=executor.submit(journal::refreshHome);
                try { assertTrue(entered.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,journal::close); assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,data.routed)); }
                finally { release.countDown(); }
                assertEquals(plan.initial(),pending.get(5,TimeUnit.SECONDS));
            } finally { journal.close(); }
            try (var reopened=RatisDurableTransaction.reopen(path,home.statuses,data.routed)) { assertEquals(plan.initial(),reopened.snapshot().observedHome()); }
        }
    }
    @Test void exclusiveChecksumAndDatabaseBindingsRejectInvalidReopenWithoutRpc() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("bound-home",true); var data=helper.new Node("bound-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("bound.bin");
            try (var journal=create(path,home,data,plan)) { assertThrows(IOException.class,() -> create(path,home,data,plan)); }
            var source=data.directory; var wrong=new RangeDirectory(source.clusterId(),source.databaseId(),new UUID(0,1510),source.schemaEpoch(),source.schemaDigest(),source.generation(),source.columns(),source.routingPrefixBytes(),source.ranges());
            try (var other=new RatisRoutedClient(wrong,data.wire)) { assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,other)); }
            byte[] bytes=Files.readAllBytes(path); bytes[bytes.length-1]^=1; Files.write(path,bytes);
            assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,data.routed)); assertEquals(0,home.wire.index); assertEquals(0,data.wire.index);
        }
    }
    @Test void laterParticipantVotesAndRefreshedHomeRoutesRemainFrozenAcrossReopen() throws Exception {
        helper.root=root; UUID column=new UUID(0,1600),domain=new UUID(0,1601); byte[] split={(byte)128};
        var li=new RangeOwnership.Interval(domain,Set.of(column),null,split); var ri=new RangeOwnership.Interval(domain,Set.of(column),split,null);
        try (var home=helper.new Node("multi-home",true); var left=helper.new Node("left",false,li); var right=helper.new Node("right",false,ri)) {
            var lr=left.directory.ranges().getFirst(); var rr=right.directory.ranges().getFirst();
            var l=new RangeDescriptor(lr.rangeId(),lr.groupId(),1,0,null,split,RangeDescriptor.TopologyState.ACTIVE,lr.replicas()); var r=new RangeDescriptor(rr.rangeId(),rr.groupId(),1,0,split,null,RangeDescriptor.TopologyState.ACTIVE,rr.replicas());
            var directory=new RangeDirectory(helper.cluster,helper.database,domain,1,new byte[32],0,Set.of(column),0,List.of(l,r));
            var transport=new RatisRoutedClient.Transport() {
                RatisParticipantPrepareTest.Wire wire(RangeDescriptor route) { return route.groupId().equals(l.groupId()) ? left.wire : right.wire; }
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { return wire(route).read(route,col,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return wire(route).command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return wire(route).participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return wire(route).transaction(route,bytes,fresh); }
            };
            var operation=new OperationId(new UUID(0,1602),1,0); var lb=new DistributedCommand(l.groupId(),1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));
            var rb=new DistributedCommand(r.groupId(),1,new byte[32],operation,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)130},new byte[]{8})));
            try (var lp=left.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})))); var rp=right.f.store.captureTracked(new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)130})))); var routed=new RatisRoutedClient(directory,transport)) {
                var la=left.f.machine.participantIntent(lp,lb); var ra=right.f.machine.participantIntent(rp,rb);
                var intents=List.of(new RatisDurableTransaction.Intent(l,la,helper.control(0),new OperationId(new UUID(0,1604),3,0),List.of()),new RatisDurableTransaction.Intent(r,ra,helper.control(1),new OperationId(new UUID(0,1605),3,0),List.of()));
                var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,1603), operation, List.of(new DomainBinding(domain, 0, new byte[32], 1, new byte[32])), RatisDurableTransaction.planDigest(intents), List.of(new TransactionStatus.Participant(domain, l.rangeId(), l.groupId(), 1, new byte[32], la.payloadDigest(), null, false),new TransactionStatus.Participant(domain, r.rangeId(), r.groupId(), 1, new byte[32], ra.payloadDigest(), null, false)));
                Path path=root.resolve("multi.bin"); var journal=RatisDurableTransaction.create(path,home.statuses,routed,initial,home.statuses.prepare(homeOperation(0),null,initial),intents);
                try {
                    journal.executeHome();
                    try (var prepare=left.f.machine.prepareParticipant(lp,lb,helper.control(0),home.statuses,initial.transactionId())) { journal.freezePreparation(routed.prepareParticipant(prepare.preparation())); journal.executePreparation(l.groupId()); }
                    var leftVote=journal.snapshot().participant(l.groupId()).vote(); var one=initial.prepared(l.groupId(),la.payloadDigest(),leftVote.position()); journal.queueHome(home.statuses.prepare(homeOperation(1),initial,one)); journal.executeHome();
                    try (var prepare=right.f.machine.prepareParticipant(rp,rb,helper.control(1),home.statuses,initial.transactionId())) { assertEquals(one,prepare.preparation().registration()); journal.freezePreparation(routed.prepareParticipant(prepare.preparation())); right.wire.lose=true; var owned=journal; assertThrows(IOException.class,() -> owned.executePreparation(r.groupId())); }
                    journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,routed); assertEquals(one,journal.snapshot().participant(r.groupId()).preparation().preparation().registration());
                    var rightVote=journal.resolveParticipant(r.groupId()).vote(); var ready=one.prepared(r.groupId(),ra.payloadDigest(),rightVote.position());
                    var old=home.directory.ranges().getFirst(); var refreshed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas()); home.routed.refresh(new RangeDirectory(helper.cluster,helper.database,home.directory.domainId(),1,new byte[32],1,home.directory.columns(),0,List.of(refreshed)));
                    journal.queueHome(home.statuses.prepare(homeOperation(2),one,ready)); home.wire.lose=true; var owned=journal; assertThrows(IOException.class,owned::executeHome);
                    journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,routed); assertEquals(1,journal.snapshot().pendingHome().descriptor().configurationVersion()); journal.refreshHome(); journal.executeHome();
                    var commit=ready.decide(TransactionStatus.Phase.COMMIT); journal.queueHome(home.statuses.prepare(homeOperation(3),ready,commit)); journal.executeHome();
                    try (var finalize=left.f.machine.prepareFinalization(home.statuses,initial.transactionId(),intents.get(0).finalizeControl())) { journal.freezeFinalization(routed.prepareFinalization(finalize.finalization())); journal.executeFinalization(l.groupId()); }
                    var ack=commit.finalized(l.groupId()); journal.queueRecovery(ack); journal.executeHome();
                    try (var finalize=right.f.machine.prepareFinalization(home.statuses,initial.transactionId(),intents.get(1).finalizeControl())) { journal.freezeFinalization(routed.prepareFinalization(finalize.finalization())); journal.executeFinalization(r.groupId()); }
                    assertArrayEquals(new byte[]{7},routed.read(column,new byte[]{1})); assertArrayEquals(new byte[]{8},routed.read(column,new byte[]{(byte)130})); helper.released(left); helper.released(right);
                } finally { journal.close(); }
            }
        }
    }
    @Test void aPersistedAbortOutcomeCanGainItsLateNegativePrepareVote() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("late-home",true); var data=helper.new Node("late-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("late.bin");
            try (var journal=create(path,home,data,plan)) {
                journal.executeHome();
                try (var prepare=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,plan.initial().transactionId())) {
                    var original=data.routed.prepareParticipant(prepare.preparation()); journal.freezePreparation(original);
                    var pending=data.wire.propose(original.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending); data.f.machine.cancelTransaction(pending);
                    var abort=plan.initial().decide(TransactionStatus.Phase.ABORT); journal.queueRecovery(abort); journal.executeHome();
                    try (var finalize=data.f.machine.prepareFinalization(home.statuses,plan.initial().transactionId(),plan.intent().finalizeControl())) { journal.freezeFinalization(data.routed.prepareFinalization(finalize.finalization())); journal.executeFinalization(data.f.groupId); }
                    assertNull(journal.snapshot().participant(data.f.groupId).outcome().vote()); pending.initLogEntry(1,++data.wire.index);
                    assertEquals(ParticipantVote.negative(),RatisParticipantRecord.result(data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray()));
                    assertEquals(ParticipantVote.negative(),journal.resolveParticipant(data.f.groupId).outcome().vote()); helper.released(data);
                }
            }
            try (var reopened=RatisDurableTransaction.reopen(path,home.statuses,data.routed)) { assertEquals(ParticipantVote.negative(),reopened.snapshot().participant(data.f.groupId).outcome().vote()); }
        }
    }

    @Test void persistedFinalizationReceiptRetriesProofAfterFailureNullAndMismatchedPosition() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("receipt-home",true); var data=helper.new Node("receipt-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var mode=new java.util.concurrent.atomic.AtomicInteger(); var queries=new java.util.concurrent.atomic.AtomicInteger();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return data.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return data.wire.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return data.wire.participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    if (ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.PARTICIPANT_OUTCOME && mode.get()!=0) {
                        queries.incrementAndGet();
                        if (mode.get()==1) throw new IOException("Lost terminal proof reply");
                        if (mode.get()==2) return ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_OUTCOME,java.nio.ByteBuffer.allocate(4).putInt(-1).array());
                        if (mode.get()==3) { var actual=data.f.store.finalizedParticipant(new UUID(0,1500)); var wrong=new ParticipantOutcome(actual.finalization(),new ApplyPosition(1,actual.position().index()+1),actual.vote(),actual.result()); byte[] bytes=wrong.encode(); var body=java.nio.ByteBuffer.allocate(4+bytes.length); ScopedTransactionRpc.putBlob(body,bytes); return ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_OUTCOME,body.array()); }
                    }
                    return data.wire.transaction(route,request,fresh);
                }
            };
            var plan=plan(home,data,pin); Path path=root.resolve("receipt.bin");
            try (var routed=new RatisRoutedClient(data.directory,transport)) {
                var journal=RatisDurableTransaction.create(path,home.statuses,routed,plan.initial(),plan.registration(),List.of(plan.intent()));
                try {
                    journal.executeHome();
                    try (var prepare=data.f.machine.prepareParticipant(pin,data.f.put(2),helper.control(0),home.statuses,plan.initial().transactionId())) { journal.freezePreparation(routed.prepareParticipant(prepare.preparation())); journal.executePreparation(data.f.groupId); }
                    var vote=journal.snapshot().participant(data.f.groupId).vote(); var ready=plan.initial().prepared(data.f.groupId,plan.intent().acceptedBusiness().payloadDigest(),vote.position()); journal.queueHome(home.statuses.prepare(homeOperation(1),plan.initial(),ready)); journal.executeHome(); var commit=ready.decide(TransactionStatus.Phase.COMMIT); journal.queueHome(home.statuses.prepare(homeOperation(2),ready,commit)); journal.executeHome();
                    try (var finalize=data.f.machine.prepareFinalization(home.statuses,plan.initial().transactionId(),plan.intent().finalizeControl())) { journal.freezeFinalization(routed.prepareFinalization(finalize.finalization())); mode.set(1); var owned=journal; assertThrows(IOException.class,() -> owned.executeFinalization(data.f.groupId)); }
                    assertNotNull(journal.snapshot().participant(data.f.groupId).finalizationReceipt()); assertNull(journal.snapshot().participant(data.f.groupId).outcome()); long index=data.wire.index;
                    journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,routed);
                    mode.set(2); var owned=journal; assertThrows(IOException.class,() -> owned.executeFinalization(data.f.groupId)); mode.set(3); assertThrows(IOException.class,() -> owned.executeFinalization(data.f.groupId));
                    mode.set(4); assertEquals(new ApplyPosition(1,2),journal.executeFinalization(data.f.groupId)); assertEquals(index,data.wire.index); assertEquals(4,queries.get()); assertNotNull(journal.snapshot().participant(data.f.groupId).outcome());
                } finally { journal.close(); }
            }
        }
    }
    @Test void aggregateIntentCapacityRejectsBeforeHomeRegistrationCanHaveEffects() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("budget-home",true); var data=helper.new Node("budget-data",false)) {
            var ranges=new ArrayList<RangeDescriptor>(); var intents=new ArrayList<RatisDurableTransaction.Intent>(); var participants=new ArrayList<TransactionStatus.Participant>(); var business=new OperationId(new UUID(0,1700),1,0);
            for (int n=0;n<9;n++) {
                var group=new UUID(0,1710+n); var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"budget-"+n,"127.0.0.1:"+(15000+n),ReplicaDescriptor.Role.VOTER);
                var route=new RangeDescriptor(group,group,1,0,n==0 ? null : new byte[]{(byte)(n*28)},n==8 ? null : new byte[]{(byte)((n+1)*28)},RangeDescriptor.TopologyState.ACTIVE,List.of(replica)); ranges.add(route);
                byte[] key={(byte)(n*28+1)}; var raw=new DistributedCommand(group,1,new byte[32],business,List.of(new Mutation(Mutation.Kind.PUT,data.f.column,key,new byte[1_900_000])));
                var scope=new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,key))); var accepted=DistributedCommand.acceptedTransaction(raw,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,1701),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
                intents.add(new RatisDurableTransaction.Intent(route,accepted,new OperationId(new UUID(0,1702),2,n),new OperationId(new UUID(0,1703),3,n),List.of())); participants.add(new TransactionStatus.Participant(data.directory.domainId(), group, group, 1, new byte[32], accepted.payloadDigest(), null, false));
            }
            var directory=new RangeDirectory(helper.cluster,helper.database,data.directory.domainId(),1,new byte[32],0,Set.of(data.f.column),0,ranges);
            var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,1704), business, List.of(new DomainBinding(data.directory.domainId(), data.directory.generation(), data.directory.payloadDigest(), 1, new byte[32])), RatisDurableTransaction.planDigest(intents), participants);
            try (var routed=new RatisRoutedClient(directory,data.wire)) {
                Path path=root.resolve("budget.bin"); assertThrows(IOException.class,() -> RatisDurableTransaction.create(path,home.statuses,routed,initial,home.statuses.prepare(homeOperation(0),null,initial),intents));
                assertFalse(Files.exists(path)); assertEquals(0,home.wire.index); assertEquals(0,data.wire.index);
            }
        }
    }

    @Test void definitelyExpiredHomeControlCanBeReplacedWhileOriginalBusinessRemainsLive() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("expired-home",true); var data=helper.new Node("expired-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("expired.bin");
            try (var journal=create(path,home,data,plan)) {
                home.wire.command(home.directory.ranges().getFirst(),new DistributedCommand(home.f.groupId,1,new byte[32],homeOperation(1),List.of(),List.of(),0));
                assertEquals(CommandResult.Status.SESSION_EXPIRED,journal.executeHome().status()); assertNull(journal.refreshHome());
                var recovery=new OperationId(new UUID(0,1800),3,0); journal.queueHome(home.statuses.prepare(recovery,null,plan.initial())); assertEquals(CommandResult.Status.SUCCESS,journal.executeHome().status());
                assertEquals(plan.initial(),journal.snapshot().observedHome()); assertEquals(plan.intent().acceptedBusiness().operationId(),journal.snapshot().initial().operation()); assertEquals(0,data.wire.index);
            }
            try (var reopened=RatisDurableTransaction.reopen(path,home.statuses,data.routed)) { assertEquals(plan.initial(),reopened.snapshot().observedHome()); assertEquals(3,reopened.snapshot().pendingHome().command().operationId().epoch()); }
        }
    }

    @Test void aHomeReadFromAnotherRangeCannotRetireAnUncertainRequest() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("moved-home",true); var data=helper.new Node("moved-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("moved.bin");
            try (var journal=create(path,home,data,plan)) {
                home.wire.lose=true; assertThrows(IOException.class,journal::executeHome);
                var original=home.directory.ranges().getFirst(); var different=new RangeDescriptor(new UUID(0,1990),new UUID(0,1991),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,original.replicas());
                home.routed.refresh(new RangeDirectory(helper.cluster,helper.database,home.directory.domainId(),1,new byte[32],1,home.directory.columns(),0,List.of(different)));
                assertThrows(IOException.class,journal::refreshHome); assertNull(journal.snapshot().observedHome()); assertFalse(journal.resolveHomeFromObserved()); assertFalse(journal.snapshot().homeSuperseded()); assertEquals(1,home.wire.index);
                home.routed.refresh(new RangeDirectory(helper.cluster,helper.database,home.directory.domainId(),1,new byte[32],2,home.directory.columns(),0,List.of(original)));
                assertEquals(plan.initial(),journal.refreshHome()); assertTrue(journal.resolveHomeFromObserved()); assertNull(journal.snapshot().homeResult());
            }
            try (var reopened=RatisDurableTransaction.reopen(path,home.statuses,data.routed)) { assertTrue(reopened.snapshot().homeSuperseded()); assertThrows(IllegalStateException.class,reopened::executeHome); }
        }
    }

    @Test void unchangedFreshObservationsDoNotRewriteTheJournalButNewIntentsStillMustForce() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("unchanged-home",true); var data=helper.new Node("unchanged-data",false); var pin=data.f.store.captureTracked(helper.scope(data,1,4))) {
            var plan=plan(home,data,pin); Path path=root.resolve("unchanged.bin");
            try (var journal=create(path,home,data,plan)) {
                journal.executeHome(); Path blocked=path.resolveSibling(path.getFileName()+".pending"); Files.createDirectory(blocked);
                assertEquals(plan.initial(),journal.refreshHome()); assertNull(journal.resolveParticipant(data.f.groupId).outcome()); assertTrue(Files.isDirectory(blocked),"Unchanged observations must not enter atomic snapshot replacement");
                var abort=plan.initial().decide(TransactionStatus.Phase.ABORT); assertThrows(IOException.class,() -> journal.queueRecovery(abort)); assertEquals(plan.initial(),home.statuses.read(plan.initial().transactionId()));
                assertThrows(IOException.class,journal::executeHome); assertEquals(0,data.wire.index);
            }
            try (var reopened=RatisDurableTransaction.reopen(path,home.statuses,data.routed)) { assertEquals(plan.initial(),reopened.snapshot().observedHome()); }
        }
    }

    @Test void globalResultBytesSlotsAndDigestAreRejectedBeforeAnyHomeOrParticipantRpc() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("result-budget-home",true); var data=helper.new Node("result-budget-data",false)) {
            for (String invalid:List.of("bytes","duplicate","gap","digest")) {
                var routes=new ArrayList<RangeDescriptor>(); var intents=new ArrayList<RatisDurableTransaction.Intent>(); var participants=new ArrayList<TransactionStatus.Participant>();
                var business=new OperationId(new UUID(0,1720),1,0);
                for (int n=0;n<2;n++) {
                    UUID group=new UUID(0,1730+n); byte[] key={(byte)(n==0 ? 1 : 129)};
                    var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"result-"+n,"127.0.0.1:"+(16000+n),ReplicaDescriptor.Role.VOTER);
                    var route=new RangeDescriptor(group,group,1,0,n==0 ? null : new byte[]{(byte)128},n==0 ? new byte[]{(byte)128} : null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica)); routes.add(route);
                    var raw=new DistributedCommand(group,1,new byte[32],business,List.of(new Mutation(Mutation.Kind.PUT,data.f.column,key,new byte[]{7},Mutation.ReturnMode.PREVIOUS)));
                    byte[] value=new byte[invalid.equals("bytes") ? 1_100_000 : 1];
                    var accepted=DistributedCommand.acceptedTransaction(raw,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,1721),new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,key))),new CommandResult(CommandResult.Status.SUCCESS,List.of(value)).encode()));
                    int slot=invalid.equals("duplicate") ? 0 : invalid.equals("gap") && n==1 ? 2 : n;
                    intents.add(new RatisDurableTransaction.Intent(route,accepted,new OperationId(new UUID(0,1722),2,n),new OperationId(new UUID(0,1723),3,n),List.of(slot)));
                    participants.add(new TransactionStatus.Participant(data.directory.domainId(), group, group, 1, new byte[32], accepted.payloadDigest(), null, false));
                    assertThrows(IllegalArgumentException.class,() -> new RatisDurableTransaction.Intent(route,accepted,new OperationId(new UUID(0,1722),2,20),new OperationId(new UUID(0,1723),3,20),List.of()));
                    assertThrows(IllegalArgumentException.class,() -> new RatisDurableTransaction.Intent(route,accepted,new OperationId(new UUID(0,1722),2,20),new OperationId(new UUID(0,1723),3,20),List.of(256)));
                }
                var directory=new RangeDirectory(helper.cluster,helper.database,data.directory.domainId(),1,new byte[32],0,Set.of(data.f.column),0,routes);
                byte[] digest=RatisDurableTransaction.planDigest(intents); if (invalid.equals("digest")) digest[0]^=1;
                var initial=TransactionStatus.begin(helper.cluster, helper.database, new UUID(0,1740+List.of("bytes","duplicate","gap","digest").indexOf(invalid)), business, List.of(new DomainBinding(data.directory.domainId(), data.directory.generation(), data.directory.payloadDigest(), 1, new byte[32])), digest, participants);
                try (var routed=new RatisRoutedClient(directory,data.wire)) {
                    Path path=root.resolve("global-result-"+invalid+".bin");
                    assertThrows(IOException.class,() -> RatisDurableTransaction.create(path,home.statuses,routed,initial,home.statuses.prepare(homeOperation(0),null,initial),intents));
                    assertFalse(Files.exists(path)); assertEquals(0,home.wire.index); assertEquals(0,data.wire.index);
                }
            }
        }
    }

}
