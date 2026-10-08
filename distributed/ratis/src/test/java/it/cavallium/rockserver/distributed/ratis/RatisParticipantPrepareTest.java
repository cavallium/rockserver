package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Consumer;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Controlled group/append/native handoff. Does not establish process, network or independent quorum acceptance. */
class RatisParticipantPrepareTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(0,1),database=new UUID(0,2),controlSession=new UUID(0,3);
    RatisTransactionStatuses transactionHomes;
    RatisSnapshotReadinessHelper.Recovery snapshotRecovery;
    final Map<UUID,Node> nodes=new ConcurrentHashMap<>();
    OperationId control(long n) { return new OperationId(controlSession,1,n); }
    final class Node implements AutoCloseable {
        final it.cavallium.rockserver.core.impl.RWScheduler scheduler;
        final RatisSnapshotHelpService snapshotHelp;
        final RatisRetentionAdmissionTest.Fixture f; final Wire wire; final RangeDirectory directory; final RatisRoutedClient routed; final RatisTransactionStatuses statuses;
        Node(String name,boolean home) throws Exception { this(name,home,null); }
        Node(String name,boolean home,RangeOwnership.Interval interval) throws Exception { this(name,home,interval,null,System::nanoTime); }
        Node(String name,boolean home,RangeOwnership.Interval interval,RatisSnapshotFences snapshotHomes,java.util.function.LongSupplier nanos) throws Exception {
            var recoveryProvider=home ? null : snapshotRecovery;
            scheduler=recoveryProvider==null ? null : it.cavallium.rockserver.core.impl.RWScheduler.forTesting(1,1,1,4,1,"snapshot-help-fixture");
            snapshotHelp=scheduler==null ? null : new RatisSnapshotHelpService(scheduler);
            var validator=new java.util.concurrent.atomic.AtomicReference<Consumer<DistributedCommand>>(command -> {});
            var ownedInterval=home && interval==null ? new RangeOwnership.Interval(UUID.randomUUID(),Set.of(UUID.randomUUID()),null,null) : interval;
            try {
            f=new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> validator.get().accept(command),nanos,ownedInterval,machine -> {
                if (snapshotHomes!=null) machine.snapshotHomes(snapshotHomes);
                if (!home && transactionHomes!=null) machine.transactionHomes(transactionHomes);
                if (!home && recoveryProvider!=null) machine.snapshotRecovery(recoveryProvider,snapshotHelp);
                if (home) {
                    machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value) -> RatisTransactionStatuses.validateRecord(cluster,database,key,value));
                    machine.homeRecoveryReservations(cluster,database,ownedInterval.columns().iterator().next());
                    machine.homeRecoveryProofs(recovery -> {
                        var node=nodes.get(recovery.proof().participant());
                        if (node==null) throw new IOException("Unconfigured controlled DATA proof group");
                        return node.routed.finalizedParticipant(recovery.proof().participant(),recovery.before().transactionId());
                    });
                }
            });
            } catch (Exception | Error failure) {
                if (snapshotHelp!=null) try { snapshotHelp.stop().get(5,TimeUnit.SECONDS); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                if (scheduler!=null) try { scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(5)); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
            var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:12345",ReplicaDescriptor.Role.VOTER);
            var range=new RangeDescriptor(f.groupId,f.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
            directory=new RangeDirectory(cluster,database,f.store.ownership().interval().domainId(),1,new byte[32],0,Set.of(f.column),0,List.of(range));
            nodes.put(f.groupId,this); wire=new Wire(f); routed=new RatisRoutedClient(directory,wire); statuses=new RatisTransactionStatuses(routed,f.column);
            if (home) { transactionHomes=statuses; validator.set(RatisTransactionStatuses.validator(cluster,database,f.groupId,1,new byte[32],f.column)); }
        }
        @Override public void close() throws Exception { if (snapshotHelp!=null) snapshotHelp.stop().get(5,TimeUnit.SECONDS); nodes.remove(f.groupId,this); routed.close(); f.close(); if (scheduler!=null) scheduler.disposeGracefully().block(java.time.Duration.ofSeconds(5)); }
    }
    static final class Wire implements RatisRoutedClient.Transport {
        final RatisRetentionAdmissionTest.Fixture f; long index; boolean lose; Runnable afterRead,beforeFinalization; IOException readFailure;
        Wire(RatisRetentionAdmissionTest.Fixture f) { this.f=f; }
        TransactionContext propose(byte[] bytes) throws Exception {
            return f.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(f.peer.getId()).setGroupId(f.group.getGroupId()).setCallId(1)
                    .setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build());
        }
        byte[] append(byte[] bytes) throws IOException {
            try {
                var context=propose(bytes); if (context.getException()!=null) throw new IOException("Admission rejected",context.getException());
                f.machine.preAppendTransaction(context); context.initLogEntry(f.leadership.get().term(),++index);
                byte[] result=f.machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                if (lose) { lose=false; throw new IOException("Lost applied participant reply"); } return result;
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return CommandResult.decode(append(DistributedCommandCodec.encode(command))); }
        public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return append(record); }
        public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
            try { if (beforeFinalization!=null && ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.FINALIZATION_PREPARE) beforeFinalization.run(); var message=Message.valueOf(ByteString.copyFrom(request)); return (fresh ? f.machine.queryLinearizable(message) : f.machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
            catch (Exception failure) { throw new IOException(failure); }
        }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException {
            if (readFailure!=null) throw readFailure;
            byte[] value=ScopedTransactionRpc.pointValue(transaction(route,ScopedTransactionRpc.pointRequest(column,key),true),2*1024*1024);
            if (afterRead!=null) afterRead.run(); return value;
        }
    }
    TransactionScope scope(Node node,int lower,int upper) { return new TransactionScope(List.of(TransactionScope.Span.range(node.f.column,new byte[]{(byte)lower},new byte[]{(byte)upper}))); }
    TransactionStatus register(Node home,Node data,ReplicatedRangeStore.ScopedHistoryPin pin,DistributedCommand business,long n) throws Exception {
        byte[] digest=data.f.machine.participantPlanDigest(pin,business);
        var definition=new TransactionStatus.Participant(data.f.groupId,data.f.groupId,1,new byte[32],digest,null,false);
        var registration=TransactionStatus.begin(cluster,database,data.directory.domainId(),new UUID(0,100+n),business.operationId(),1,new byte[32],new byte[32],List.of(definition));
        assertEquals(CommandResult.Status.SUCCESS,home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,n),null,registration)).status()); return registration;
    }
    void released(Node data) throws Exception { assertEquals(0,data.f.machine.reservedParticipantCount()); assertEquals(0,data.f.machine.reservedParticipantBytes()); data.f.assertReleased(); }
    @Test void acceptedPrepareHandsOffScopeAndBothIdentitiesThenLostReplyRetriesAndQueriesItsStableVote() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                assertEquals(1,data.f.machine.reservedParticipantCount()); assertEquals(1,data.f.machine.proposalFootprintStats().candidates());
                assertNotNull(data.f.propose(data.f.put(1)).getException());
                var wrongBusiness=new DistributedCommand(data.f.groupId,1,new byte[32],business.operationId(),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{5},new byte[]{5})));
                var wrongControl=new DistributedCommand(data.f.groupId,1,new byte[32],control(0),wrongBusiness.mutations());
                assertNotNull(data.f.propose(wrongBusiness).getException()); assertNotNull(data.f.propose(wrongControl).getException());
                data.wire.command(data.directory.ranges().getFirst(),data.f.put(4));
                var operation=data.routed.prepareParticipant(plan.preparation()); assertArrayEquals(plan.bytes(),operation.bytes());
                data.wire.lose=true; assertThrows(IOException.class,() -> data.routed.executeParticipant(operation));
                assertEquals(1,data.f.store.preparedParticipantCount()); assertEquals(0,data.f.machine.proposalFootprintStats().candidates()); released(data);
                var vote=data.routed.executeParticipant(operation).position(); assertEquals(new ApplyPosition(1,2),vote);
                var durable=data.routed.preparedParticipant(data.f.groupId,registration.transactionId()); assertEquals(vote,durable.position()); assertEquals(plan.preparation(),durable.preparation());
                var queryBody=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(queryBody,registration.transactionId());
                var query=ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_STATE,queryBody.array());
                assertThrows(IOException.class,() -> data.wire.transaction(operation.descriptor(),query,false));
                assertThrows(ExecutionException.class,() -> data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(query)),0).get(5,TimeUnit.SECONDS));
                var oldRoute=operation.descriptor(); var changed=new RangeDescriptor(oldRoute.rangeId(),oldRoute.groupId(),oldRoute.rangeEpoch(),1,null,null,RangeDescriptor.TopologyState.ACTIVE,oldRoute.replicas());
                data.routed.refresh(new RangeDirectory(cluster,database,data.directory.domainId(),1,new byte[32],1,data.directory.columns(),0,List.of(changed)));
                var restored=data.routed.restoreParticipant(operation.preparation(),oldRoute); assertEquals(0,restored.descriptor().configurationVersion()); assertArrayEquals(operation.bytes(),restored.bytes());
                assertThrows(IOException.class,() -> data.routed.read(data.f.column,new byte[]{2})); assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{4}));
                try (var raw=data.f.store.capture()) { assertNull(raw.get(data.f.column,new byte[]{2})); }
                var voted=registration.prepared(data.f.groupId,PreparedParticipant.Preparation.planDigest(plan.preparation().business()),vote);
                assertEquals(CommandResult.Status.SUCCESS,home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,voted)).status());
                var floor=DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,2,0),2); data.wire.command(data.directory.ranges().getFirst(),floor);
                assertThrows(IOException.class,() -> data.routed.executeParticipant(operation)); assertEquals(vote,data.routed.preparedParticipant(data.f.groupId,registration.transactionId()).position());
                released(data);
            }
        }
    }
    @Test void ownerlessAdmissionIsReplacedAfterNativeCompletionBeforeTheOriginalApplyCallback() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                var original=data.wire.propose(plan.bytes()); assertNull(original.getException()); data.f.machine.preAppendTransaction(original); original.initLogEntry(1,1);
                data.f.machine.cancelTransaction(original); assertEquals(1,data.f.machine.reservedParticipantCount());
                data.f.store.applyPrepared(new ApplyPosition(1,1),plan.preparation()); data.wire.index=1; // Controlled native publication before the adapter callback.
                var vote=data.routed.executeParticipant(data.routed.prepareParticipant(plan.preparation())).position(); assertEquals(new ApplyPosition(1,1),vote); released(data);
                assertEquals(vote,RatisParticipantRecord.result(data.f.machine.applyTransaction(original).get(5,TimeUnit.SECONDS).getContent().toByteArray()).position()); released(data);
            }
        }
    }
    @Test void homeRoutingRefreshCannotChangeTheAuthorityAttachedToTheCapturedRegistration() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0); var old=home.directory.ranges().getFirst();
            var next=new RangeDescriptor(UUID.randomUUID(),UUID.randomUUID(),2,0,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
            home.wire.afterRead=() -> { home.routed.refresh(new RangeDirectory(cluster,database,home.directory.domainId(),1,new byte[32],1,home.directory.columns(),0,List.of(next))); home.wire.afterRead=null; };
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                assertEquals(old.groupId(),plan.preparation().homeGroup()); assertEquals(old.rangeEpoch(),plan.preparation().homeEpoch()); assertEquals(next.groupId(),home.routed.directory().ranges().getFirst().groupId());
            }
            released(data);
        }
    }
    @Test void frozenPlanRejectsReopenedCutsButAcceptsItsOriginalCutAfterDisjointProgress() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var original=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,original,business,0); data.wire.command(data.directory.ranges().getFirst(),data.f.put(9));
            try (var reopened=data.f.store.captureTracked(scope(data,1,4))) { assertThrows(IllegalArgumentException.class,() -> data.f.machine.prepareParticipant(reopened,business,control(0),home.statuses,registration.transactionId())); }
            released(data);
            try (var plan=data.f.machine.prepareParticipant(original,business,control(0),home.statuses,registration.transactionId())) { assertEquals(new ApplyPosition(1,0),plan.preparation().business().acceptance().snapshot()); }
            released(data);
        }
    }
    @Test void invalidatedBlockedPlannerRetainsCapacityUntilActualHomeReadReturns() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4)); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            home.wire.afterRead=() -> { entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException e) { throw new AssertionError(e); } };
            var pending=executor.submit(() -> data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId()));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); data.f.machine.notifyNotLeader(List.of()); assertEquals(1,data.f.machine.reservedParticipantCount()); assertTrue(data.f.machine.reservedParticipantBytes()>0); assertFalse(pending.isDone());
            } finally { release.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); released(data);
        }
    }
    @Test void freshTerminalQueriesRecoverCommitAndBothAbortOrdersAfterAllOriginalGrantsExpire() throws Exception {
        for (int mode=0;mode<3;mode++) try (var home=new Node("outcome-home-"+mode,true); var data=new Node("outcome-data-"+mode,false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            assertNull(data.routed.finalizedParticipant(data.f.groupId,registration.transactionId()));
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                var operation=data.routed.prepareParticipant(plan.preparation()); var status=registration;
                if (mode!=2) {
                    var vote=data.routed.executeParticipant(operation); assertTrue(vote.prepared());
                    if (mode==0) { status=registration.prepared(data.f.groupId,plan.preparation().business().payloadDigest(),vote.position()); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,status)); }
                }
                var decision=status.decide(mode==0 ? TransactionStatus.Phase.COMMIT : TransactionStatus.Phase.ABORT);
                home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,2),status,decision));
                var finalization=home.statuses.readFinalization(registration.transactionId(),data.f.groupId,new OperationId(new UUID(0,778),3,0));
                var finalPosition=new ApplyPosition(1,++data.wire.index);
                if (mode==0) data.f.store.applyCommit(finalPosition,finalization); else data.f.store.applyAbort(finalPosition,finalization);
                var outcome=data.routed.finalizedParticipant(data.f.groupId,registration.transactionId()); assertEquals(finalization,outcome.finalization()); assertEquals(finalPosition,outcome.position());
                if (mode==0) { assertArrayEquals(plan.preparation().business().acceptance().result(),outcome.result()); assertArrayEquals(new byte[]{7},data.routed.read(data.f.column,new byte[]{2})); }
                else { assertNull(outcome.result()); assertNull(data.routed.read(data.f.column,new byte[]{2})); }
                if (mode==2) {
                    assertNull(outcome.vote()); data.wire.lose=true; assertThrows(IOException.class,() -> data.routed.executeParticipant(operation));
                    assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(operation)); outcome=data.routed.finalizedParticipant(data.f.groupId,registration.transactionId()); assertEquals(ParticipantVote.negative(),outcome.vote());
                } else assertTrue(outcome.vote().prepared());
                var body=ByteBuffer.allocate(16); ScopedTransactionRpc.uuid(body,registration.transactionId()); var query=ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_OUTCOME,body.array());
                assertThrows(IOException.class,() -> data.wire.transaction(operation.descriptor(),query,false));
                assertThrows(ExecutionException.class,() -> data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(query)),0).get(5,TimeUnit.SECONDS));
                var floor=DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,4,0),4); data.wire.command(operation.descriptor(),floor);
                assertThrows(IOException.class,() -> data.routed.executeParticipant(operation)); assertEquals(outcome,data.routed.finalizedParticipant(data.f.groupId,registration.transactionId()));
                data.f.machine.takeSnapshot(); data.f.machine.pause(); data.f.machine.reinitialize(); assertEquals(outcome,data.routed.finalizedParticipant(data.f.groupId,registration.transactionId())); released(data);
            }
        }
    }
    @Test void routedTerminalQueriesRejectWrongTransactionAndTrailingReplyData() throws Exception {
        try (var home=new Node("invalid-home",true); var data=new Node("invalid-data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0); var decision=registration.decide(TransactionStatus.Phase.ABORT);
            home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,decision));
            var finalization=home.statuses.readFinalization(registration.transactionId(),data.f.groupId,new OperationId(new UUID(0,779),2,0)); data.f.store.applyAbort(new ApplyPosition(1,1),finalization); data.wire.index=1;
            var outcome=data.f.store.finalizedParticipant(registration.transactionId()); var trailing=new java.util.concurrent.atomic.AtomicBoolean();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return data.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return data.wire.command(route,command); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) {
                    byte[] bytes=outcome.encode(); var output=ByteBuffer.allocate(4+bytes.length+(trailing.get() ? 1 : 0)); ScopedTransactionRpc.putBlob(output,bytes);
                    return ScopedTransactionRpc.frame(ScopedTransactionRpc.PARTICIPANT_OUTCOME,output.array());
                }
            };
            try (var routed=new RatisRoutedClient(data.directory,transport)) {
                assertThrows(IOException.class,() -> routed.finalizedParticipant(data.f.groupId,new UUID(0,999))); trailing.set(true);
                assertThrows(IOException.class,() -> routed.finalizedParticipant(data.f.groupId,registration.transactionId()));
            }
        }
    }
    @Test void prepareVoteCodecDistinguishesNegativeVotesFromMalformedOrTransientOutcomes() {
        assertEquals(ParticipantVote.negative(),RatisParticipantRecord.result(RatisParticipantRecord.result(ParticipantVote.negative())));
        assertEquals(ParticipantVote.prepared(new ApplyPosition(1,1)),RatisParticipantRecord.result(RatisParticipantRecord.result(new ApplyPosition(1,1))));
        for (var status:List.of(CommandResult.Status.SESSION_EXPIRED,CommandResult.Status.RESULT_TOO_LARGE)) assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.result(new CommandResult(status,List.of()).encode()));
        assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.result(new CommandResult(CommandResult.Status.CONFLICT,List.of(new byte[]{1})).encode()));
        assertThrows(IllegalArgumentException.class,() -> new ParticipantVote(false,new ApplyPosition(1,1)));
    }
    @Test void abortFenceBeforeAdmittedPrepareProducesStableNegativeVoteAndSettlesOwnership() throws Exception {
        try (var home=new Node("abort-home",true); var data=new Node("abort-data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                var pending=data.wire.propose(plan.bytes()); assertNull(pending.getException()); data.f.machine.preAppendTransaction(pending);
                var abort=registration.decide(TransactionStatus.Phase.ABORT); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,abort));
                var intent=home.statuses.readFinalization(registration.transactionId(),data.f.groupId,new OperationId(new UUID(0,777),2,0));
                data.f.store.applyAbort(new ApplyPosition(1,1),intent); data.wire.index=1; pending.initLogEntry(1,2);
                assertEquals(ParticipantVote.negative(),RatisParticipantRecord.result(data.f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray())); data.wire.index=2; released(data);
                var operation=data.routed.prepareParticipant(plan.preparation()); data.wire.lose=true;
                assertThrows(IOException.class,() -> data.routed.executeParticipant(operation)); assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(operation)); released(data);
                assertNull(data.routed.preparedParticipant(data.f.groupId,registration.transactionId())); assertNull(data.routed.read(data.f.column,new byte[]{2}));
                data.f.machine.takeSnapshot(); data.f.machine.pause(); data.f.machine.reinitialize();
                assertEquals(ParticipantVote.negative(),data.routed.executeParticipant(operation)); released(data);
                assertEquals(CommandResult.Status.SUCCESS,data.wire.command(operation.descriptor(),data.f.put(3)).status());
            }
        }
    }
    @Test void durableCapacityAndPendingHoldsShareOneBoundAcrossScopeHandoff() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false)) {
            for (int n=0;n<63;n++) {
                var point=new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{(byte)n})));
                try (var pin=data.f.store.captureTracked(point)) {
                    var business=data.f.put(n); var registration=register(home,data,pin,business,n);
                    try (var plan=data.f.machine.prepareParticipant(pin,business,control(n),home.statuses,registration.transactionId())) { data.routed.executeParticipant(data.routed.prepareParticipant(plan.preparation())); }
                }
            }
            assertEquals(63,data.f.store.preparedParticipantCount()); released(data);
            var firstScope=new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{63})));
            var secondScope=new TransactionScope(List.of(TransactionScope.Span.point(data.f.column,new byte[]{64})));
            try (var first=data.f.store.captureTracked(firstScope); var second=data.f.store.captureTracked(secondScope)) {
                var one=data.f.put(63); var firstHome=register(home,data,first,one,63);
                try (var held=data.f.machine.prepareParticipant(first,one,control(63),home.statuses,firstHome.transactionId())) {
                    var two=data.f.put(64); var secondHome=register(home,data,second,two,64);
                    assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipant(second,two,control(64),home.statuses,secondHome.transactionId()));
                    assertEquals(1,data.f.machine.reservedParticipantCount()); assertEquals(63,data.f.store.preparedParticipantCount());
                    held.close();
                    try (var retry=data.f.machine.prepareParticipant(second,two,control(64),home.statuses,secondHome.transactionId())) { data.routed.executeParticipant(data.routed.prepareParticipant(retry.preparation())); }
                    assertEquals(64,data.f.store.preparedParticipantCount()); released(data);
                }
            }
        }
    }
    @Test void twoDataGroupsProduceRealPreparedVotesBeforeTheHomeCanChooseCommit() throws Exception {
        UUID domain=new UUID(0,77),column=new UUID(0,88);
        var leftInterval=new RangeOwnership.Interval(domain,Set.of(column),null,new byte[]{(byte)128});
        var rightInterval=new RangeOwnership.Interval(domain,Set.of(column),new byte[]{(byte)128},null);
        try (var home=new Node("home",true); var left=new Node("left",false,leftInterval); var right=new Node("right",false,rightInterval)) {
            var l=left.directory.ranges().getFirst(); var r=right.directory.ranges().getFirst();
            var ranges=List.of(new RangeDescriptor(l.rangeId(),l.groupId(),1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,l.replicas()),
                    new RangeDescriptor(r.rangeId(),r.groupId(),1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,r.replicas()));
            var directory=new RangeDirectory(cluster,database,domain,1,new byte[32],0,Set.of(column),0,ranges);
            var transport=new RatisRoutedClient.Transport() {
                Wire wire(RangeDescriptor route) { return route.groupId().equals(left.f.groupId) ? left.wire : right.wire; }
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { return wire(route).read(route,col,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return wire(route).command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return wire(route).participant(route,record); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException { return wire(route).transaction(route,request,fresh); }
            };
            var businessId=new OperationId(new UUID(0,500),1,0);
            var lb=new DistributedCommand(left.f.groupId,1,new byte[32],businessId,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));
            var rb=new DistributedCommand(right.f.groupId,1,new byte[32],businessId,List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)130},new byte[]{8})));
            var leftScope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1})));
            var rightScope=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)130})));
            try (var lp=left.f.store.captureTracked(leftScope); var rp=right.f.store.captureTracked(rightScope); var routed=new RatisRoutedClient(directory,transport)) {
                var ld=left.f.machine.participantPlanDigest(lp,lb); var rd=right.f.machine.participantPlanDigest(rp,rb);
                var registration=TransactionStatus.begin(cluster,database,domain,new UUID(0,800),businessId,1,new byte[32],new byte[32],List.of(
                        new TransactionStatus.Participant(l.rangeId(),l.groupId(),1,new byte[32],ld,null,false),new TransactionStatus.Participant(r.rangeId(),r.groupId(),1,new byte[32],rd,null,false)));
                home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,0),null,registration));
                try (var lplan=left.f.machine.prepareParticipant(lp,lb,control(0),home.statuses,registration.transactionId()); var rplan=right.f.machine.prepareParticipant(rp,rb,control(1),home.statuses,registration.transactionId())) {
                    var lv=routed.executeParticipant(routed.prepareParticipant(lplan.preparation())).position(); var rv=routed.executeParticipant(routed.prepareParticipant(rplan.preparation())).position();
                    var voted=registration.prepared(l.groupId(),ld,lv); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,1),registration,voted));
                    assertThrows(IllegalArgumentException.class,() -> voted.decide(TransactionStatus.Phase.COMMIT));
                    var ready=voted.prepared(r.groupId(),rd,rv); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,2),voted,ready));
                    var commit=ready.decide(TransactionStatus.Phase.COMMIT); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,3),ready,commit));
                    assertEquals(TransactionStatus.Phase.COMMIT,home.statuses.read(registration.transactionId()).phase()); assertFalse(commit.fullyFinalized());
                    assertEquals(lv,routed.preparedParticipant(l.groupId(),registration.transactionId()).position()); assertEquals(rv,routed.preparedParticipant(r.groupId(),registration.transactionId()).position());
                    assertThrows(IOException.class,() -> routed.read(column,new byte[]{1})); assertThrows(IOException.class,() -> routed.read(column,new byte[]{(byte)130}));
                    try (var raw=left.f.store.capture()) { assertNull(raw.get(column,new byte[]{1})); }
                    try (var raw=right.f.store.capture()) { assertNull(raw.get(column,new byte[]{(byte)130})); }
                    try (var lf=left.f.machine.prepareFinalization(home.statuses,registration.transactionId(),new OperationId(new UUID(0,901),2,0)); var rf=right.f.machine.prepareFinalization(home.statuses,registration.transactionId(),new OperationId(new UUID(0,902),2,0))) {
                        routed.executeFinalization(routed.prepareFinalization(lf.finalization())); assertArrayEquals(new byte[]{7},routed.read(column,new byte[]{1}));
                        assertThrows(IOException.class,() -> routed.read(column,new byte[]{(byte)130}));
                        routed.executeFinalization(routed.prepareFinalization(rf.finalization())); assertArrayEquals(new byte[]{8},routed.read(column,new byte[]{(byte)130}));
                        assertNotNull(routed.finalizedParticipant(l.groupId(),registration.transactionId())); assertNotNull(routed.finalizedParticipant(r.groupId(),registration.transactionId()));
                        var ackLeft=commit.finalized(l.groupId()); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,4),commit,ackLeft));
                        var ackBoth=ackLeft.finalized(r.groupId()); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,5),ackLeft,ackBoth)); assertTrue(ackBoth.fullyFinalized());
                        var consumed=ackBoth.consume(); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,900),1,6),ackBoth,consumed)); assertTrue(home.statuses.read(registration.transactionId()).gcEligible());
                    }
                    released(left); released(right);
                }
            }
        }
    }
    @Test void snapshotCoveredPrepareSettlesCapacityBeforeItsDeferredDuplicateSuffix() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                var first=data.wire.propose(plan.bytes()); var duplicate=data.wire.propose(plan.bytes());
                data.f.machine.preAppendTransaction(first); first.initLogEntry(1,1); data.f.machine.preAppendTransaction(duplicate); duplicate.initLogEntry(1,2);
                data.f.store.applyPrepared(new ApplyPosition(1,1),plan.preparation()); assertEquals(1,data.f.machine.takeSnapshot());
                data.f.machine.pause(); var covered=data.f.machine.applyTransaction(first); var suffix=data.f.machine.applyTransaction(duplicate);
                assertFalse(covered.isDone()); assertFalse(suffix.isDone()); data.f.machine.reinitialize();
                assertEquals(new ApplyPosition(1,1),RatisParticipantRecord.result(covered.get(5,TimeUnit.SECONDS).getContent().toByteArray()).position());
                assertFalse(suffix.isDone()); assertEquals(0,data.f.machine.reservedParticipantCount()); assertEquals(0,data.f.machine.proposalFootprintStats().candidates());
                assertEquals(new ApplyPosition(1,1),RatisParticipantRecord.result(data.f.machine.applyTransaction(duplicate).get(5,TimeUnit.SECONDS).getContent().toByteArray()).position());
                assertEquals(new ApplyPosition(1,1),RatisParticipantRecord.result(suffix.get(5,TimeUnit.SECONDS).getContent().toByteArray()).position()); released(data);
                assertEquals(new ApplyPosition(1,1),data.routed.preparedParticipant(data.f.groupId,registration.transactionId()).position());
            }
        }
    }
    @Test void absentAbortedAndClosedAcceptanceNeverAdmitANewPreparation() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2);
            assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,new UUID(0,999)));
            released(data); var registration=register(home,data,pin,business,0); byte[] bytes;
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) { bytes=plan.bytes(); }
            released(data); assertThrows(IOException.class,() -> data.wire.participant(data.directory.ranges().getFirst(),bytes)); assertEquals(0,data.f.store.preparedParticipantCount());
            byte[] invalid=bytes.clone(); ByteBuffer.wrap(invalid).putInt(4,99); assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.decode(invalid));
            assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.result(new CommandResult(CommandResult.Status.SUCCESS,Collections.singletonList(null)).encode()));
            var abort=registration.decide(TransactionStatus.Phase.ABORT); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),registration,abort));
            try (var next=data.f.store.captureTracked(scope(data,1,4))) { assertThrows(IllegalStateException.class,() -> data.f.machine.prepareParticipant(next,business,control(0),home.statuses,registration.transactionId())); }
            released(data);
        }
    }
    @Test void submittedUncertaintySurvivesSameTermInvalidationUntilANewerCompletedTerm() throws Exception {
        try (var home=new Node("home",true); var data=new Node("data",false); var pin=data.f.store.captureTracked(scope(data,1,4))) {
            var business=data.f.put(2); var registration=register(home,data,pin,business,0);
            try (var plan=data.f.machine.prepareParticipant(pin,business,control(0),home.statuses,registration.transactionId())) {
                var uncertain=data.wire.propose(plan.bytes()); data.f.machine.preAppendTransaction(uncertain); uncertain.initLogEntry(1,1); data.f.machine.cancelTransaction(uncertain);
                data.f.machine.notifyNotLeader(List.of()); assertEquals(1,data.f.machine.reservedParticipantCount()); assertNotNull(data.f.propose(data.f.put(1)).getException());
                data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); data.f.machine.notifyTermIndexUpdated(2,1);
                var disjoint=data.f.propose(data.f.put(5)); assertNull(disjoint.getException()); data.f.machine.cancelTransaction(disjoint); released(data);
                assertEquals(0,data.f.store.preparedParticipantCount());
            }
        }
    }
}
