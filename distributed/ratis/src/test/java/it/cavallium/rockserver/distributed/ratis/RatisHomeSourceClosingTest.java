package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual JNI and adapter append/apply; catalogue authority is controlled, no sockets or quorum claim. */
class RatisHomeSourceClosingTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSourceClosingTest source=new RatisSourceClosingTest();
    final java.util.concurrent.atomic.AtomicBoolean sourceAvailable=new java.util.concurrent.atomic.AtomicBoolean(true);
    final java.util.concurrent.atomic.AtomicInteger sourceProofCalls=new java.util.concurrent.atomic.AtomicInteger();
    static Message message(byte[] bytes){return RatisSourceOutcomeTest.message(bytes);}
    RatisRetentionAdmissionTest.Fixture home(RatisSourceOutcomeTest.Fixture source,Path path,UUID column,UUID domain)throws Exception {
        var origin=source.request.origin();return new RatisRetentionAdmissionTest.Fixture(path,RatisRangeStateMachine.RetentionLimits.defaults(),command->{},System::nanoTime,new RangeOwnership.Interval(domain,Set.of(column),null,null),machine->{
            machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value)->RatisTransactionStatuses.validateRecord(origin.clusterId(),origin.databaseId(),key,value));machine.homeRecoveryReservations(origin.clusterId(),origin.databaseId(),column);
            machine.homeSourceClosingAuthority(request->sourceProof(source,request.source(),null),(closing,original)->sourceProof(source,closing,original));
        });
    }
    void sourceProof(RatisSourceOutcomeTest.Fixture source,NativeSourceClosing closing,OperationRerouteOrigin origin)throws IOException {
        sourceProofCalls.incrementAndGet();if(!sourceAvailable.get())throw new IOException("Original source125 is unavailable");
        try{var reply=source.machine.queryLinearizable(message(RatisHomeSourceClosingRpc.source(closing,origin))).get(5,TimeUnit.SECONDS);RatisHomeSourceClosingRpc.requireSourceAck(reply.getContent().toByteArray(),closing,origin);}catch(Exception failure){throw new IOException(failure);}
    }
    NativeHomeSourceClosing close(RatisRetentionAdmissionTest.Fixture home,NativeSourceClosing closed,long index)throws Exception {
        var cfg=home.store.configuration();var request=new NativeHomeSourceClosing.Request(home.groupId,1,new byte[32],cfg.term(),cfg.index(),cfg.data(),closed);var record=new RatisHomeSourceClosingRecord(request);home.machine.prepareHomeSourceClosing(record);
        var tx=home.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(home.peer.getId()).setGroupId(home.group.getGroupId()).setCallId(901).setType(RaftClientRequest.writeRequestType()).setMessage(message(record.encode())).build());assertNull(tx.getException());home.machine.preAppendTransaction(tx);tx.initLogEntry(1,index);return NativeHomeSourceClosing.decode(home.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray());
    }
    @Test void actualSourceProofAndHomeFenceOrderQueuedRegistrationWithoutEffects()throws Exception {
        try(var s=source.helper.new Fixture(root.resolve("source"),false)){
            var closed=source.close(s,source.record(s));var origin=s.request.origin();var column=new UUID(0,990);var domain=new UUID(0,991);
            try(var home=home(s,root.resolve("home"),column,domain)){
                var definition=s.home.source.definition;var route=definition.parents().getFirst();var directory=definition.sourceDirectory();
                var initial=TransactionStatus.begin(origin.clusterId(),origin.databaseId(),new UUID(0,992),new OperationId(home.session,1,9),List.of(DomainBinding.of(directory)),new byte[32],List.of(new TransactionStatus.Participant(directory.domainId(),route.rangeId(),route.groupId(),route.rangeEpoch(),directory.schemaDigest(),new byte[32],null,false)));
                byte[] key=java.nio.ByteBuffer.allocate(16).putLong(initial.transactionId().getMostSignificantBits()).putLong(initial.transactionId().getLeastSignificantBits()).array();var command=new DistributedCommand(home.groupId,1,new byte[32],new OperationId(home.session,1,0),List.of(new Mutation(Mutation.Kind.PUT,column,key,initial.encode())),List.of(new KeyPredicate(column,key,null)));
                var queued=home.propose(command);assertNull(queued.getException());home.machine.preAppendTransaction(queued);
                var cfg=home.store.configuration();var request=new NativeHomeSourceClosing.Request(home.groupId,1,new byte[32],cfg.term(),cfg.index(),cfg.data(),closed);var record=new RatisHomeSourceClosingRecord(request);
                var old=closed.request();var foreign=new NativeSourceClosing(new NativeSourceClosing.Request(old.header(),old.group(),UUID.randomUUID(),old.catalogueCut(),old.configurationTerm(),old.configurationIndex(),old.configuration(),old.recoveryObligationsDigest()),closed.position());
                var wrong=new NativeHomeSourceClosing.Request(home.groupId,1,new byte[32],cfg.term(),cfg.index(),cfg.data(),foreign);
                assertThrows(java.util.concurrent.ExecutionException.class,()->home.machine.queryLinearizable(message(RatisHomeSourceClosingRpc.request(RatisHomeSourceClosingRpc.PREPARE,wrong))).get(5,TimeUnit.SECONDS));assertNull(home.store.homeSourceClosing(old.group(),route.rangeEpoch()));
                assertEquals(record,RatisHomeSourceClosingRecord.decode(record.encode()));assertArrayEquals(request.digest(),home.machine.queryLinearizable(message(RatisHomeSourceClosingRpc.request(RatisHomeSourceClosingRpc.PREPARE,request))).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                var tx=home.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(home.peer.getId()).setGroupId(home.group.getGroupId()).setCallId(900).setType(RaftClientRequest.writeRequestType()).setMessage(message(record.encode())).build());assertNull(tx.getException());home.machine.preAppendTransaction(tx);tx.initLogEntry(1,1);
                var actual=NativeHomeSourceClosing.decode(home.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray());assertEquals(request,actual.request());
                assertEquals(CommandResult.Status.CONFLICT,home.apply(queued,2));assertNull(home.store.readCurrentPoint(column,key));assertEquals(new ApplyPosition(1,2),home.store.applied());

                assertThrows(IllegalArgumentException.class,()->RatisHomeSourceClosingRecord.decode(Arrays.copyOf(record.encode(),record.encode().length-1)));
                var query=RatisHomeSourceClosingRpc.source(closed,origin);var decoded=RatisHomeSourceClosingRpc.sourceQuery(ScopedTransactionRpc.parse(query,RatisHomeSourceClosingRpc.MAX_BYTES).body());RatisHomeSourceClosingRpc.requireSourceAck(RatisHomeSourceClosingRpc.sourceAck(decoded),closed,origin);var forged=RatisHomeSourceClosingRpc.sourceAck(decoded);forged[0]^=1;assertThrows(IllegalArgumentException.class,()->RatisHomeSourceClosingRpc.requireSourceAck(forged,closed,origin));
            }
        }
    }
    @Test void reservedNoEffectAbortUsesActualClosedNoCreditAndItsOriginalProtectedReceipt()throws Exception {
        var column=new UUID(0,993);try(var s=source.helper.new Fixture(root.resolve("source-abort"),false);var home=home(s,root.resolve("home-abort"),column,new UUID(0,994))){
            var reserved=s.base.status();byte[] key=java.nio.ByteBuffer.allocate(16).putLong(reserved.transactionId().getMostSignificantBits()).putLong(reserved.transactionId().getLeastSignificantBits()).array();
            var registration=new DistributedCommand(home.groupId,1,new byte[32],reserved.origin().firstControl(),List.of(new Mutation(Mutation.Kind.PUT,column,key,reserved.encode())),List.of(new KeyPredicate(column,key,null)));
            // Actual original native RESERVED, before CLOSE; no copied accepted participant effects.
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(home.store.apply(new ApplyPosition(1,1),registration)).status());home.machine.notifyTermIndexUpdated(1,1);
            var closed=source.close(s,source.record(s));var fence=close(home,closed,2);assertEquals(reserved,home.store.homeSourceReferences(fence,null,16).transaction());
            var lookup=RatisHomeSourceClosingRpc.lookup(new HomeIdentity(home.groupId,1,new byte[32],column,reserved));sourceAvailable.set(false);
            assertThrows(java.util.concurrent.ExecutionException.class,()->home.machine.queryLinearizable(message(lookup)).get(5,TimeUnit.SECONDS),"First abort still needs actual source CLOSED/no-credit");sourceAvailable.set(true);
            assertEquals(fence,RatisHomeSourceClosingRpc.ClosedReservation.decode(home.machine.queryLinearizable(message(lookup)).get(5,TimeUnit.SECONDS).getContent().toByteArray()).closing());
            var terminal=reserved.abortReservation(TransactionStatus.NoEffectOutcome.SOURCE_CLOSED);var command=new DistributedCommand(home.groupId,1,new byte[32],new OperationId(home.admin,0,99),List.of(new Mutation(Mutation.Kind.PUT,column,key,terminal.encode())),List.of(new KeyPredicate(column,key,reserved.encode())));
            var recovery=new HomeRecovery(HomeRecovery.Action.ABORT,command,null);try(var prepared=home.machine.prepareHomeRecovery(recovery)){
                var tx=home.machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(home.peer.getId()).setGroupId(home.group.getGroupId()).setCallId(902).setType(RaftClientRequest.writeRequestType()).setMessage(message(recovery.encode())).build());assertNull(tx.getException());home.machine.preAppendTransaction(tx);tx.initLogEntry(1,3);var actual=HomeRecoveryResult.decode(home.machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray()).receipt();assertEquals(CommandResult.Status.SUCCESS,actual.result().status());assertEquals(TransactionStatus.NoEffectOutcome.SOURCE_CLOSED,TransactionStatus.decode(home.store.readCurrentPoint(column,key)).noEffectOutcome());
                assertEquals(actual,home.store.homeRecoveryResult(recovery).receipt());assertNull(s.home.source.store.operationResult(s.request.original().operationId(),s.request.original().receiptDigest()));
                var cut=home.store.applied();int calls=sourceProofCalls.get();sourceAvailable.set(false);
                assertEquals(fence,RatisHomeSourceClosingRpc.ClosedReservation.decode(home.machine.queryLinearizable(message(lookup)).get(5,TimeUnit.SECONDS).getContent().toByteArray()).closing());
                assertEquals(fence,RatisHomeSourceClosingRpc.ClosedReservation.decode(home.machine.queryLinearizable(message(lookup)).get(5,TimeUnit.SECONDS).getContent().toByteArray()).closing());
                assertEquals(calls,sourceProofCalls.get(),"Exact forced terminal lookup must not contact unavailable source125");assertEquals(cut,home.store.applied());assertEquals(terminal,TransactionStatus.decode(home.store.readCurrentPoint(column,key)));assertEquals(actual,home.store.homeReservationAbortReceipt(new HomeIdentity(home.groupId,1,new byte[32],column,reserved)));assertNull(s.home.source.store.operationResult(s.request.original().operationId(),s.request.original().receiptDigest()));
            }
        }
    }

}
