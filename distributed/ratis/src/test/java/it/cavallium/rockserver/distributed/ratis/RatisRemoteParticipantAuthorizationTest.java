package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisRemoteParticipantAuthorizationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    TransactionStatus register(RatisParticipantPrepareTest.Node home,RatisParticipantPrepareTest.Node data,DistributedCommand accepted) throws Exception {
        var initial=TransactionStatus.begin(helper.cluster,helper.database,data.directory.domainId(),new UUID(0,48000),accepted.operationId(),1,new byte[32],new byte[32],List.of(new TransactionStatus.Participant(data.f.groupId,data.f.groupId,1,new byte[32],accepted.payloadDigest(),null,false)));
        home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,0),null,initial)); return initial;
    }
    @Test void remoteReadParticipantPreparesOriginalScopeAndCommitsWithoutUserEffects() throws Exception {
        helper.root=root;
        try (var home=helper.new Node("read-home",true); var data=helper.new Node("read-data",false);
             var handle=data.routed.openTransaction(helper.scope(data,1,4),Duration.ofSeconds(30))) {
            assertNull(handle.get(data.f.column,new byte[]{2}));
            var accepted=handle.readCertification(new OperationId(data.f.session,1,10));
            var initial=register(home,data,accepted);
            var operation=handle.authorizeParticipant(accepted,helper.control(0),initial.transactionId());
            assertTrue(operation.preparation().business().readCertification());
            assertEquals(handle.snapshot(),operation.preparation().business().acceptance().snapshot());
            var vote=data.routed.executeParticipant(operation);
            assertTrue(vote.prepared());
            assertNotNull(data.f.propose(data.f.put(2)).getException());
            var prepared=initial.prepared(data.f.groupId,accepted.payloadDigest(),vote.position());
            home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),initial,prepared));
            var decision=prepared.decide(TransactionStatus.Phase.COMMIT);
            home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,2),prepared,decision));
            data.routed.executeFinalization(data.routed.prepareFinalization(new ParticipantFinalization(home.f.groupId,1,data.f.groupId,helper.control(1),decision)));
            var result=CommandResult.decode(data.routed.finalizedParticipant(data.f.groupId,initial.transactionId()).result());
            assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(0,result.valueCount());
            assertNull(data.wire.read(handle.descriptor(),data.f.column,new byte[]{2}));
        }
    }
    @Test void remoteReadCertificationRejectsPhantomsAndCannotDiscardStagedWrites() throws Exception {
        helper.root=root;
        for (boolean staged:new boolean[]{false,true}) try (var home=helper.new Node("read-conflict-home-"+staged,true); var data=helper.new Node("read-conflict-data-"+staged,false);
             var handle=data.routed.openTransaction(helper.scope(data,1,4),Duration.ofSeconds(30))) {
            assertNull(handle.get(data.f.column,new byte[]{2}));
            if (staged) handle.stage(data.f.put(2).mutations());
            var accepted=handle.readCertification(new OperationId(data.f.session,1,10));
            var initial=register(home,data,accepted);
            if (!staged) data.wire.command(handle.descriptor(),data.f.put(2));
            if (staged) assertThrows(IOException.class,() -> handle.authorizeParticipant(accepted,helper.control(0),initial.transactionId()));
            else assertThrows(RatisPreparationUnavailableException.class,() -> handle.authorizeParticipant(accepted,helper.control(0),initial.transactionId()));
            assertNull(data.routed.preparedParticipant(data.f.groupId,initial.transactionId()));
        }
    }
    @Test void remoteOriginalHandleAuthorizesCommitAndAbortAndLostAuthorizationReplyRetriesExactBytes() throws Exception {
        helper.root=root;
        for (boolean commit:new boolean[]{false,true}) try (var home=helper.new Node("home-"+commit,true); var data=helper.new Node("data-"+commit,false)) {
            var lose=new AtomicBoolean(true); var observed=new AtomicReference<byte[]>();
            try (var client=new RatisRoutedClient(data.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return data.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return data.wire.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return data.wire.participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    byte[] result=data.wire.transaction(route,bytes,fresh);
                    if (ScopedTransactionRpc.kind(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES)==ScopedTransactionRpc.PARTICIPANT_AUTHORIZE) { assertTrue(fresh); observed.set(bytes.clone()); if (lose.getAndSet(false)) throw new IOException("Lost authorization reply"); }
                    return result;
                }
            }); var handle=client.openTransaction(helper.scope(data,1,4),Duration.ofSeconds(30))) {
                var business=data.f.put(2); var staged=handle.stage(business.mutations(),business.predicates()); var accepted=handle.participantIntent(business,staged); var initial=register(home,data,accepted);
                assertThrows(IOException.class,() -> handle.authorizeParticipant(accepted,helper.control(0),initial.transactionId()));
                var intent=new RatisDurableTransaction.Intent(handle.descriptor(),accepted,helper.control(0),helper.control(1),List.of(),handle.captureId());
                var restored=client.restoreTransaction(intent); assertEquals(handle.captureId(),restored.captureId());
                var operation=restored.authorizeParticipant(accepted,helper.control(0),initial.transactionId()); assertArrayEquals(DistributedCommandCodec.encode(accepted),DistributedCommandCodec.encode(operation.preparation().business())); assertFalse(operation.negative());
                assertThrows(IOException.class,() -> data.wire.transaction(operation.descriptor(),observed.get(),false)); assertThrows(IOException.class,() -> home.wire.transaction(home.directory.ranges().getFirst(),observed.get(),true));
                var vote=client.executeParticipant(operation); assertTrue(vote.prepared()); var status=initial;
                if (commit) { status=initial.prepared(data.f.groupId,accepted.payloadDigest(),vote.position()); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,1),initial,status)); }
                var decision=status.decide(commit ? TransactionStatus.Phase.COMMIT : TransactionStatus.Phase.ABORT); home.statuses.execute(home.statuses.prepare(new OperationId(new UUID(0,4),1,2),status,decision));
                client.executeFinalization(client.prepareFinalization(new ParticipantFinalization(home.f.groupId,1,data.f.groupId,helper.control(1),decision)));
                assertNotNull(client.finalizedParticipant(data.f.groupId,initial.transactionId()));
            }
        }
    }
    @Test void changedStagedPrefixIsRejectedAndBlockedHomeCloseRetainsOwnershipUntilReturn() throws Exception {
        helper.root=root; var entered=new CountDownLatch(1); var resume=new CountDownLatch(1);
        try (var home=helper.new Node("blocked-home",true); var data=helper.new Node("blocked-data",false); var handle=data.routed.openTransaction(helper.scope(data,1,4),Duration.ofSeconds(30))) {
            var business=data.f.put(2); var staged=handle.stage(business.mutations()); var accepted=handle.participantIntent(business,staged); var initial=register(home,data,accepted);
            var changed=data.f.put(3); var wrong=handle.participantIntent(changed,staged); assertThrows(IOException.class,() -> handle.authorizeParticipant(wrong,helper.control(0),initial.transactionId()));
            home.wire.afterRead=() -> { entered.countDown(); try { if (!resume.await(5,TimeUnit.SECONDS)) throw new IllegalStateException("HOME barrier timed out"); } catch (InterruptedException interrupted) { throw new RuntimeException(interrupted); } };
            var pending=CompletableFuture.supplyAsync(() -> { try { return handle.authorizeParticipant(accepted,helper.control(0),initial.transactionId()); } catch (IOException failure) { throw new CompletionException(failure); } });
            try { assertTrue(entered.await(5,TimeUnit.SECONDS)); handle.close(); assertFalse(pending.isDone()); assertTrue(data.f.machine.reservedParticipantCount()>0); }
            finally { resume.countDown(); }
            assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertEquals(0,data.f.machine.reservedParticipantCount());
        } finally { resume.countDown(); }
    }
    @Test void expiredOriginalHandleIsDefinitivelyUnavailableWithoutOpeningANewCut() throws Exception {
        helper.root=root; var clock=new AtomicLong();
        try (var home=helper.new Node("expired-home",true); var data=helper.new Node("expired-data",false,null,null,clock::get); var handle=data.routed.openTransaction(helper.scope(data,1,4),Duration.ofSeconds(1))) {
            var business=data.f.put(2); var staged=handle.stage(business.mutations()); var accepted=handle.participantIntent(business,staged); var initial=register(home,data,accepted);
            var intent=new RatisDurableTransaction.Intent(handle.descriptor(),accepted,helper.control(0),helper.control(1),List.of(),handle.captureId()); clock.set(TimeUnit.SECONDS.toNanos(2));
            try (var restored=data.routed.restoreTransaction(intent)) { assertThrows(RatisPreparationUnavailableException.class,() -> restored.authorizeParticipant(accepted,helper.control(0),initial.transactionId())); }
            assertNull(data.routed.preparedParticipant(data.f.groupId,initial.transactionId())); assertEquals(initial,home.statuses.read(initial.transactionId()));
        }
    }

    @Test void closeFromAnotherIncarnationCannotRetireCurrentLeaseAndExpiredCloseIsIdempotent() throws Exception {
        helper.root=root; var clock=new AtomicLong();
        try (var home=helper.new Node("close-owner-home",true); var data=helper.new Node("close-owner-data",false); var registry=new RatisScopedTransactions(data.f.machine,RatisScopedTransactions.Limits.defaults(),clock::get)) {
            var handle=registry.open(UUID.randomUUID(),helper.scope(data,1,4),Duration.ofSeconds(1));
            var old=new ScopedTransactionRpc.Handle(UUID.randomUUID(),handle.id(),handle.generation(),handle.position());
            registry.release(old); assertEquals(1,registry.chargedHandles()); assertNull(registry.get(handle,data.f.column,new byte[]{2}));
            clock.set(TimeUnit.SECONDS.toNanos(2)); registry.expire(); assertEquals(0,registry.chargedHandles());
            registry.release(handle); registry.release(old); assertEquals(0,registry.chargedHandles());
        }
    }

}
