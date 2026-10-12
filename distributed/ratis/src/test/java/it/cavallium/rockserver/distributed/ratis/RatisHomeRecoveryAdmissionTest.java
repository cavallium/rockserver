package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.protocol.*;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native HOME state and controlled qualified DATA replies; no server or network. */
class RatisHomeRecoveryAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID cluster=new UUID(10,1),database=new UUID(10,2),domain=new UUID(10,3),participant=new UUID(10,4),column=new UUID(10,5);
    final AtomicReference<ParticipantOutcome> outcome=new AtomicReference<>();
    final class ProofReader implements AutoCloseable {
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        volatile boolean block;
        final RatisRoutedClient client;
        ProofReader() {
            var range=new RangeDescriptor(participant,participant,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(new ReplicaDescriptor(new UUID(11,1),new UUID(11,2),new UUID(11,3),"data","localhost:12345",ReplicaDescriptor.Role.VOTER)));
            var directory=new RangeDirectory(cluster,database,domain,0,new byte[32],0,Set.of(new UUID(11,4)),0,List.of(range));
            client=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    assertTrue(fresh); var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES); assertEquals(ScopedTransactionRpc.PARTICIPANT_OUTCOME,packet.kind());
                    entered.countDown(); if (block) try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Test proof read timed out"); } catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IOException(e); }
                    var actual=outcome.get(); byte[] encoded=actual==null ? null : actual.encode(); var body=ByteBuffer.allocate(4+(encoded==null ? 0 : encoded.length)); ScopedTransactionRpc.putBlob(body,encoded);
                    return ScopedTransactionRpc.frame(packet.kind(),body.array());
                }
            });
        }
        public void close() throws IOException { release.countDown(); client.close(); }
    }
    RatisRetentionAdmissionTest.Fixture fixture(String name,ProofReader reader,long receipts) throws Exception {
        return fixture(name,reader,receipts,false);
    }
    RatisRetentionAdmissionTest.Fixture fixture(String name,ProofReader reader,long receipts,boolean reservations) throws Exception {
        return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),new RatisRangeStateMachine.RetentionLimits(receipts,receipts*92,1,92),command -> {},System::nanoTime,
                new RangeOwnership.Interval(domain,Set.of(column),null,null),machine -> {
                    machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value) -> RatisTransactionStatuses.validateRecord(cluster,database,key,value));
                    if (reader!=null) machine.homeRecoveryData(reader.client);
                    if (reservations) machine.homeRecoveryReservations(cluster,database,column);
                });
    }
    TransactionStatus initial(int id) {
        return TransactionStatus.begin(cluster,database,domain,new UUID(12,id),new OperationId(new UUID(12,0),1,id),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(participant,participant,1,new byte[32],new byte[32],null,false)));
    }
    DistributedCommand cas(RatisRetentionAdmissionTest.Fixture f,long sequence,TransactionStatus before,TransactionStatus next,boolean protectedControl) {
        byte[] key=ByteBuffer.allocate(16).putLong(next.transactionId().getMostSignificantBits()).putLong(next.transactionId().getLeastSignificantBits()).array();
        return new DistributedCommand(f.groupId,1,new byte[32],new OperationId(protectedControl ? f.admin : f.session,protectedControl ? 0 : 1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,before==null ? null : before.encode())));
    }
    HomeRecovery abort(RatisRetentionAdmissionTest.Fixture f,long sequence,TransactionStatus initial) { return new HomeRecovery(HomeRecovery.Action.ABORT,cas(f,sequence,initial,initial.decide(TransactionStatus.Phase.ABORT),true),null); }
    HomeRecovery ack(RatisRetentionAdmissionTest.Fixture f,long sequence,TransactionStatus terminal) {
        return new HomeRecovery(HomeRecovery.Action.ACK,cas(f,sequence,terminal,terminal.finalized(participant),true),HomeRecovery.Proof.from(outcome.get()));
    }
    TransactionContext propose(RatisRetentionAdmissionTest.Fixture f,HomeRecovery recovery) throws Exception {
        return f.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(f.peer.getId()).setGroupId(f.group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(recovery.encode()))).build());
    }
    HomeRecoveryReceipt apply(RatisRetentionAdmissionTest.Fixture f,TransactionContext pending,long index) throws Exception {
        assertNull(pending.getException()); f.machine.preAppendTransaction(pending); pending.initLogEntry(1,index);
        return HomeRecoveryResult.decode(f.machine.applyTransaction(pending).get(5,TimeUnit.SECONDS).getContent().toByteArray()).receipt();
    }
    void released(RatisRetentionAdmissionTest.Fixture f) throws Exception {
        assertEquals(0,f.machine.homeRecoveryInputBytes()); assertEquals(0,f.machine.homeRecoverySlots()); assertEquals(0,f.machine.reservedHomeRecoveryCount()); assertEquals(0,f.machine.proposalFootprintStats().candidates()); f.assertReleased();
    }
    @Test void dataRoleRejectsProtectedHomeRecordsAtPreparationAndAppend() throws Exception {
        try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("data-role"),RatisRangeStateMachine.RetentionLimits.defaults())) {
            var initial=initial(1); var key=ByteBuffer.allocate(16).putLong(initial.transactionId().getMostSignificantBits()).putLong(initial.transactionId().getLeastSignificantBits()).array();
            var command=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.admin,0,0),List.of(new Mutation(Mutation.Kind.PUT,f.column,key,initial.decide(TransactionStatus.Phase.ABORT).encode())),List.of(new KeyPredicate(f.column,key,initial.encode())));
            var recovery=new HomeRecovery(HomeRecovery.Action.ABORT,command,null);
            assertThrows(IllegalArgumentException.class,() -> f.machine.prepareHomeRecovery(recovery)); assertNotNull(propose(f,recovery).getException()); released(f);
        }
    }
    @Test void abortBypassesFullOrdinaryRetentionButRequiresPrivilegedPreparationAndKeepsOriginalReceipt() throws Exception {
        try (var f=fixture("abort",null,1)) {
            var initial=initial(1); assertEquals(CommandResult.Status.SUCCESS,f.apply(f.propose(cas(f,0,null,initial,false)),1));
            var recovery=abort(f,0,initial); assertNotNull(propose(f,recovery).getException());
            try (var prepared=f.machine.prepareHomeRecovery(recovery)) {
                assertArrayEquals(recovery.encode(),prepared.bytes()); assertEquals(1,f.machine.homeRecoverySlots());
                try (var borrowed=f.machine.prepareHomeRecovery(recovery)) { assertArrayEquals(prepared.bytes(),borrowed.bytes()); }
                assertEquals(1,f.machine.reservedHomeRecoveryCount());
                var first=propose(f,recovery); var duplicate=propose(f,recovery); assertNull(first.getException()); assertNull(duplicate.getException());
                var receipt=apply(f,first,2); assertEquals(CommandResult.Status.SUCCESS,receipt.result().status());
                assertEquals(recovery.encode().length,f.machine.homeRecoveryInputBytes()); assertEquals(0,f.machine.reservedHomeRecoveryCount());
                f.machine.cancelTransaction(duplicate); released(f);
                assertEquals(receipt,apply(f,propose(f,recovery),3)); released(f);
            }
            assertEquals(1,f.store.receiptUsage().retainedCount());
        }
    }
    @Test void ackUsesFreshQualifiedOutcomeAndRejectsMissingOrChangedProof() throws Exception {
        try (var reader=new ProofReader(); var f=fixture("ack",reader,4)) {
            var initial=initial(2); f.apply(f.propose(cas(f,0,null,initial,false)),1); var terminal=initial.decide(TransactionStatus.Phase.ABORT);
            try (var prepared=f.machine.prepareHomeRecovery(abort(f,0,initial))) { apply(f,propose(f,prepared.recovery()),2); }
            var actual=new ParticipantOutcome(new ParticipantFinalization(f.groupId,1,participant,new OperationId(new UUID(13,1),0,0),terminal),new ApplyPosition(7,50),null,null);
            outcome.set(actual); var recovery=ack(f,1,terminal); outcome.set(null);
            assertThrows(IllegalArgumentException.class,() -> f.machine.prepareHomeRecovery(recovery)); released(f);
            outcome.set(new ParticipantOutcome(actual.finalization(),new ApplyPosition(7,51),null,actual.result()));
            assertThrows(IllegalArgumentException.class,() -> f.machine.prepareHomeRecovery(recovery)); released(f);
            outcome.set(actual);
            try (var prepared=f.machine.prepareHomeRecovery(recovery)) { assertEquals(CommandResult.Status.SUCCESS,apply(f,propose(f,prepared.recovery()),3).result().status()); }
            released(f); outcome.set(null);
            var acknowledged=terminal.finalized(participant); var consume=f.propose(cas(f,1,acknowledged,acknowledged.consume(),false)); assertNull(consume.getException());
            try (var retry=f.machine.prepareHomeRecovery(recovery)) { assertEquals(new ApplyPosition(1,3),apply(f,propose(f,retry.recovery()),4).position()); }
            assertEquals(CommandResult.Status.SUCCESS,f.apply(consume,5)); released(f);
        }
    }
    @Test void invalidatedBlockedProofRetainsInputAndIdentityUntilActualReturn() throws Exception {
        try (var reader=new ProofReader(); var f=fixture("blocked",reader,4)) {
            var initial=initial(3); f.apply(f.propose(cas(f,0,null,initial,false)),1); var terminal=initial.decide(TransactionStatus.Phase.ABORT);
            try (var prepared=f.machine.prepareHomeRecovery(abort(f,0,initial))) { apply(f,propose(f,prepared.recovery()),2); }
            outcome.set(new ParticipantOutcome(new ParticipantFinalization(f.groupId,1,participant,new OperationId(new UUID(13,1),0,0),terminal),new ApplyPosition(7,50),null,null));
            var recovery=ack(f,1,terminal); reader.block=true; var executor=Executors.newSingleThreadExecutor();
            try {
                var pending=executor.submit(() -> f.machine.prepareHomeRecovery(recovery)); assertTrue(reader.entered.await(5,TimeUnit.SECONDS));
                assertEquals(1,f.machine.homeRecoverySlots()); assertEquals(recovery.encode().length,f.machine.homeRecoveryInputBytes());
                assertNotNull(f.propose(cas(f,1,terminal,terminal.finalized(participant),false)).getException(),"Exclusive HOME row prevents proof/CAS interposition");
                f.machine.notifyNotLeader(List.of()); assertEquals(1,f.machine.homeRecoverySlots()); assertEquals(recovery.encode().length,f.machine.homeRecoveryInputBytes());
                assertThrows(IllegalStateException.class,() -> f.machine.prepareHomeRecovery(recovery));
                reader.release.countDown(); assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); released(f);
            } finally { reader.release.countDown(); executor.shutdownNow(); assertTrue(executor.awaitTermination(5,TimeUnit.SECONDS)); }
        }
    }
    @Test void concurrentRegistrationsReserveFutureCreditsAndFullCapacityStillFundsAbortAckAndConsume() throws Exception {
        try (var reader=new ProofReader(); var f=fixture("credits",reader,4096,true)) {
            var pending=new ArrayList<TransactionContext>(); var statuses=new ArrayList<TransactionStatus>();
            for (int i=1;i<=1024;i++) {
                var status=initial(i); var request=f.propose(cas(f,i,null,status,false));
                if (request.getException()!=null) break;
                pending.add(request); statuses.add(status);
            }
            assertFalse(pending.isEmpty()); assertTrue(pending.size()<1024,"Future credit envelope limits registrations before ordinary queue saturation");
            assertTrue(f.counter("reservedHomeCount")>0); long index=0;
            for (var request:pending) assertEquals(CommandResult.Status.SUCCESS,f.apply(request,++index));
            assertEquals(0,f.counter("reservedHomeCount")); assertEquals(3*statuses.size(),f.store.homeRecoveryCapacity().reservedCount());
            assertNotNull(f.propose(cas(f,2000,null,initial(2000),false)).getException());
            var initial=statuses.getFirst(); var recovery=abort(f,0,initial);
            try (var prepared=f.machine.prepareHomeRecovery(recovery)) {
                assertEquals(0,f.machine.reservedHomeRecoveryCount(),"Native registration already promised this transition");
                assertEquals(CommandResult.Status.SUCCESS,apply(f,propose(f,prepared.recovery()),++index).result().status());
            }
            var terminal=initial.decide(TransactionStatus.Phase.ABORT);
            outcome.set(new ParticipantOutcome(new ParticipantFinalization(f.groupId,1,participant,new OperationId(new UUID(13,1),0,0),terminal),new ApplyPosition(7,50),null,null));
            try (var prepared=f.machine.prepareHomeRecovery(ack(f,1,terminal))) { assertEquals(CommandResult.Status.SUCCESS,apply(f,propose(f,prepared.recovery()),++index).result().status()); }
            released(f); assertEquals(3*statuses.size()-2,f.store.homeRecoveryCapacity().reservedCount());
            var finalized=terminal.finalized(participant);
            var consume=new HomeRecovery(HomeRecovery.Action.CONSUME,cas(f,2,finalized,finalized.consume(),true),null);
            try(var prepared=f.machine.prepareHomeRecovery(consume)) {
                assertEquals(0,f.machine.reservedHomeRecoveryCount(),"Consumption uses its original future credit at full capacity");
                assertEquals(CommandResult.Status.SUCCESS,apply(f,propose(f,prepared.recovery()),++index).result().status());
            }
            released(f);assertEquals(3*statuses.size()-3,f.store.homeRecoveryCapacity().reservedCount());
            var key=consume.command().mutations().getFirst().key();
            assertTrue(TransactionStatus.decode(f.store.readCurrentPoint(column,key)).consumed());
        }
    }
    @Test void submittedUnknownRetainsExclusiveRowUntilNewerCompletedNativeTerm() throws Exception {
        try (var f=fixture("unknown",null,4)) {
            var initial=initial(4); f.apply(f.propose(cas(f,0,null,initial,false)),1); var recovery=abort(f,0,initial);
            try (var prepared=f.machine.prepareHomeRecovery(recovery)) {
                var pending=propose(f,recovery); assertNull(pending.getException()); f.machine.preAppendTransaction(pending); pending.initLogEntry(1,2); f.machine.cancelTransaction(pending);
                f.machine.notifyNotLeader(List.of()); assertEquals(1,f.machine.proposalFootprintStats().candidates()); assertEquals(recovery.encode().length,f.machine.homeRecoveryInputBytes());
                assertNotNull(f.propose(cas(f,1,initial,initial.decide(TransactionStatus.Phase.ABORT),false)).getException());
                f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,2);
                try (var next=f.machine.prepareHomeRecovery(recovery)) { assertEquals(1,f.machine.homeRecoverySlots()); }
                released(f);
            }
        }
    }
}
