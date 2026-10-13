package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Full native/controlled-dispatch workflow; the barrier hook is simulated, not a quorum/network acceptance test. */
class RatisTransactionRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final AtomicLong nanos=new AtomicLong(100);
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception { return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command -> {},nanos::get); }
    TransactionScope scope(RatisRetentionAdmissionTest.Fixture f,int key) { return new TransactionScope(List.of(TransactionScope.Span.point(f.column,new byte[]{(byte)key}))); }
    RangeDirectory directory(RatisRetentionAdmissionTest.Fixture f) {
        var peer=new ReplicaDescriptor(new UUID(0,11),new UUID(0,12),new UUID(0,13),"fixture","127.0.0.1:12345",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(0,14),f.groupId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        return new RangeDirectory(new UUID(0,1),new UUID(0,2),new UUID(0,3),1,new byte[32],0,Set.of(f.column),0,List.of(range));
    }
    final class Controlled implements RatisRoutedClient.Transport {
        final RatisRetentionAdmissionTest.Fixture f; long index; boolean lose; Path journal; byte[] openRequest; int fresh,owned; final java.util.List<Integer> freshMetadataKinds=new java.util.ArrayList<>();
        Controlled(RatisRetentionAdmissionTest.Fixture f) { this.f=f; }
        void requireForcedCommand(DistributedCommand command)throws IOException {
            if(journal==null)return;var body=SessionJournalFiles.read(journal,SessionWindowJournal.MAX_BYTES);var input=ByteBuffer.wrap(body);input.position(117);int entries=input.getInt(),directories=input.getInt();
            for(int i=0;i<directories;i++){int size=input.getInt();input.position(input.position()+size);}
            boolean found=false;byte[] expected=DistributedCommandCodec.encode(command);
            for(int i=0;i<entries;i++){int size=input.getInt();byte[] encoded=new byte[size];input.get(encoded);found|=Arrays.equals(expected,encoded);input.position(input.position()+48);int result=input.getInt();if(result>=0)input.position(input.position()+result);input.get();input.get();}
            assertFalse(input.hasRemaining());assertTrue(found,"Exact pending command must be forced in the current session-window layout before dispatch");
        }
        void observeQuery(byte[] request,boolean barrier) {
            if(!barrier){owned++;return;}int kind=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
            if(kind==ScopedTransactionRpc.OPEN){fresh++;openRequest=request.clone();}else freshMetadataKinds.add(kind);
        }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
        public byte[] transaction(RangeDescriptor route,byte[] request,boolean barrier) throws IOException {
            assertEquals(f.groupId,route.groupId());
            observeQuery(request,barrier);
            try { var message=Message.valueOf(ByteString.copyFrom(request)); return (barrier ? f.machine.queryLinearizable(message) : f.machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
            catch (Exception failure) { throw new IOException("Controlled query failed",failure); }
        }
        public java.util.concurrent.CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] request,boolean barrier)throws IOException {
            assertEquals(f.groupId,route.groupId());observeQuery(request,barrier);
            var input=Message.valueOf(ByteString.copyFrom(request));return (barrier?f.machine.queryLinearizable(input):f.machine.query(input)).thenApply(reply->reply.getContent().toByteArray());
        }
        public java.util.concurrent.CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID column,byte[] key)throws IOException{return transactionAsync(route,ScopedTransactionRpc.pointRequest(column,key),true).thenApply(reply->ScopedTransactionRpc.pointValue(reply,SnapshotFenceStatus.MAX_BYTES));}
        private java.util.concurrent.CompletableFuture<Void> commandTail=java.util.concurrent.CompletableFuture.completedFuture(null);
        public java.util.concurrent.CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException {
            var result=new java.util.concurrent.CompletableFuture<CommandResult>();final java.util.concurrent.CompletableFuture<Void> prior;
            synchronized(this){prior=commandTail;commandTail=result.handle((value,failure)->null);}
            prior.whenComplete((ignored,previousFailure)->{
                try {
                    requireForcedCommand(command);
                    var transaction=f.propose(command);if(transaction.getException()!=null)throw new IOException("Admission failed",transaction.getException());
                    f.machine.preAppendTransaction(transaction);transaction.initLogEntry(1,++index);
                    f.machine.applyTransaction(transaction).whenComplete((reply,failure)->{if(failure!=null){result.completeExceptionally(failure);return;}try{var decoded=CommandResult.decode(reply.getContent().toByteArray());if(lose){lose=false;throw new IOException("Lost applied reply");}result.complete(decoded);}catch(Throwable failed){result.completeExceptionally(failed);}});
                }catch(Throwable failed){result.completeExceptionally(failed);}
            });return result;
        }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
            try {
                requireForcedCommand(command);
                var transaction=f.propose(command); if (transaction.getException()!=null) throw new IOException("Admission failed",transaction.getException());
                f.machine.preAppendTransaction(transaction); transaction.initLogEntry(1,++index);
                var result=CommandResult.decode(f.machine.applyTransaction(transaction).get(5,TimeUnit.SECONDS).getContent().toByteArray());
                if (lose) { lose=false; throw new IOException("Lost applied reply"); } return result;
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
        }
    }
    @Test void nativeOpenReadPlanForcedAcceptanceLostReplyAndReopenResolveExactlyOnce() throws Exception {
        try (var f=fixture("workflow")) {
            var transport=new Controlled(f); var directory=directory(f); Path journal=root.resolve("window.bin"); transport.journal=journal;
            var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),10).issue(f.session,100); var grant=ledger.grants().getFirst();
            try (var routed=new RatisRoutedClient(directory,transport)) {
                byte[] frozen;
                try (var client=RatisDurableSessionClient.create(journal,routed,ledger,grant,RatisSessionClient.Limits.defaults()); var tx=routed.openTransaction(scope(f,0),Duration.ofSeconds(30))) {
                    assertNull(tx.get(f.column,new byte[]{0}));
                    var intent=client.prepareTransactionIntent(List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{7},Mutation.ReturnMode.PREVIOUS)));
                    assertThrows(IllegalStateException.class,() -> client.execute(intent));
                    var accepted=tx.plan(intent.command()); frozen=DistributedCommandCodec.encode(accepted);
                    var mismatched=new DistributedCommand(f.groupId,1,new byte[32],intent.command().operationId(),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{9},Mutation.ReturnMode.PREVIOUS)));
                    assertThrows(IOException.class,() -> tx.plan(mismatched));
                    assertArrayEquals(frozen,DistributedCommandCodec.encode(tx.plan(intent.command()))); assertEquals(0,f.store.activeTrackedSnapshotPins());
                    client.acceptTransaction(intent,accepted); transport.lose=true; assertThrows(IOException.class,() -> client.execute(intent)); assertNull(intent.savedResult());
                    assertEquals(0,f.machine.proposalFootprintStats().candidates());
                }
                try (var client=RatisDurableSessionClient.reopen(journal,routed)) {
                    var pending=client.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(pending.command()));
                    assertNull(client.execute(pending).value(0)); client.acknowledge(pending); assertEquals(0,client.acknowledgedThrough());
                }
                assertEquals(1,transport.fresh); assertEquals(java.util.List.of(RatisTopologyOperationOutcomeRpc.KIND),transport.freshMetadataKinds,"Lost WRITE reply performs one separate fresh outcome lookup, never another OPEN"); assertTrue(transport.owned>=4); f.assertReleased();
                try (var pin=f.store.capture()) { assertArrayEquals(new byte[]{7},pin.get(f.column,new byte[]{0})); }
            }
        }
    }
    @Test void openNeedsFreshHookRetryNonceIsIdempotentAndStaleDispatchCannotOpen() throws Exception {
        try (var f=fixture("barrier")) {
            var transport=new Controlled(f);
            try (var routed=new RatisRoutedClient(directory(f),transport); var tx=routed.openTransaction(scope(f,0),Duration.ofSeconds(1))) {
                byte[] request=transport.openRequest;
                assertThrows(ExecutionException.class,() -> f.machine.query(Message.valueOf(ByteString.copyFrom(request))).get(5,TimeUnit.SECONDS));
                assertThrows(ExecutionException.class,() -> f.machine.queryStale(Message.valueOf(ByteString.copyFrom(request)),0).get(5,TimeUnit.SECONDS));
                byte[] retry=transport.transaction(tx.descriptor(),request,true); var reply=ByteBuffer.wrap(ScopedTransactionRpc.parse(retry,ScopedTransactionRpc.MAX_REPLY_BYTES).body());
                ScopedTransactionRpc.uuid(reply); var handle=ScopedTransactionRpc.readHandle(reply);
                assertEquals(tx.snapshot(),handle.position()); assertEquals(1,f.store.activeTrackedSnapshotPins()); assertNull(tx.get(f.column,new byte[]{0}));
            }
            assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
    @Test void snapshotReadsStayAtTheirCutConflictRetiresAndAbsoluteTtlDoesNotRenew() throws Exception {
        try (var f=fixture("expiry")) {
            var transport=new Controlled(f);
            try (var routed=new RatisRoutedClient(directory(f),transport)) {
                try (var tx=routed.openTransaction(scope(f,0),Duration.ofMillis(1))) {
                    assertNull(tx.get(f.column,new byte[]{0})); f.apply(f.propose(f.put(0)),1); transport.index=1;
                    var changed=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{9})));
                    assertNull(tx.get(f.column,new byte[]{0})); assertThrows(IOException.class,() -> tx.plan(changed)); assertEquals(0,f.store.activeTrackedSnapshotPins());
                }
                try (var tx=routed.openTransaction(scope(f,2),Duration.ofMillis(1))) {
                    nanos.addAndGet(TimeUnit.MICROSECONDS.toNanos(900)); assertNull(tx.get(f.column,new byte[]{2}));
                    nanos.addAndGet(TimeUnit.MICROSECONDS.toNanos(200)); f.machine.expireTransactions(); assertEquals(0,f.store.activeTrackedSnapshotPins());
                    assertThrows(IOException.class,() -> tx.get(f.column,new byte[]{2}));
                }
            }
        }
    }
    @Test void physicalPagesUseOneDeclaredCutAndGenerationLossInvalidatesOwnedHandle() throws Exception {
        try (var f=fixture("pages")) {
            f.apply(f.propose(f.put(1)),1); f.apply(f.propose(f.put(2)),2);
            var range=new TransactionScope(List.of(TransactionScope.Span.range(f.column,new byte[]{0},new byte[]{4})));
            try (var routed=new RatisRoutedClient(directory(f),new Controlled(f)); var tx=routed.openTransaction(range,Duration.ofSeconds(1))) {
                var first=tx.page(f.column,new byte[]{0},new byte[]{4},false,null,1,1024); assertTrue(first.hasMore()); assertArrayEquals(new byte[]{1},first.rows().getFirst().key());
                f.apply(f.propose(f.put(3)),3);
                var second=tx.page(f.column,new byte[]{0},new byte[]{4},false,first.resumeAfter(),2,1024); assertFalse(second.hasMore()); assertEquals(1,second.rows().size()); assertArrayEquals(new byte[]{2},second.rows().getFirst().key());
                f.machine.notifyNotLeader(List.of()); assertThrows(IOException.class,() -> tx.get(f.column,new byte[]{1})); f.machine.expireTransactions(); assertEquals(0,f.store.activeTrackedSnapshotPins());
            }
        }
    }
    @Test void definitivePlanConflictCanBeAbandonedDurablyWithoutLeavingAnAckHole() throws Exception {
        try (var f=fixture("abandon")) {
            var transport=new Controlled(f); var directory=directory(f); var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),10).issue(f.session,100);
            Path journal=root.resolve("abandoned-window.bin"); transport.journal=journal;
            try (var routed=new RatisRoutedClient(directory,transport)) {
                try (var client=RatisDurableSessionClient.create(journal,routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults())) {
                    var mutation=new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{9},Mutation.ReturnMode.PREVIOUS);
                    var intent=client.prepareTransactionIntent(List.of(mutation));
                    try (var tx=routed.openTransaction(scope(f,0),Duration.ofSeconds(1))) {
                        var concurrent=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(new UUID(0,99),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{7})));
                        f.apply(f.propose(concurrent),1); transport.index=1;
                        assertThrows(IOException.class,() -> tx.plan(intent.command()));
                        assertEquals(CommandResult.Status.CONFLICT,client.abandonTransactionIntent(intent).status()); assertTrue(intent.abandoned());
                        assertNull(f.store.operationResult(intent.command().operationId(),intent.command().payloadDigest())); client.acknowledge(intent); assertEquals(0,client.acknowledgedThrough());
                    }
                    var next=client.prepareTransactionIntent(List.of(mutation)); assertEquals(1,next.command().operationId().sequence()); assertEquals(0,next.command().acknowledgedThrough());
                    try (var tx=routed.openTransaction(scope(f,0),Duration.ofSeconds(1))) {
                        client.acceptTransaction(next,tx.plan(next.command())); assertArrayEquals(new byte[]{7},client.execute(next).value(0)); client.acknowledge(next);
                    }
                }
                try (var recovered=RatisDurableSessionClient.reopen(journal,routed)) { assertEquals(1,recovered.acknowledgedThrough()); assertTrue(recovered.pending().isEmpty()); }
                f.assertReleased(); assertEquals(0,f.store.activeTrackedSnapshotPins());
            }
        }
    }
    @Test void legalLargePhysicalContinuationCanBeSentBackWithoutTruncatingIt() throws Exception {
        try (var f=fixture("continuation")) {
            var delegate=new Controlled(f); byte[] key=new byte[3*1024*1024]; Arrays.fill(key,(byte)7);
            var visits=new java.util.concurrent.atomic.AtomicInteger();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] physical) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    var packet=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if (packet.kind()!=ScopedTransactionRpc.PAGE) return delegate.transaction(route,request,fresh);
                    if (visits.getAndIncrement()==0) return ScopedTransactionRpc.frame(ScopedTransactionRpc.PAGE,ScopedTransactionRpc.pageBytes(new PhysicalRangePage(List.of(new PhysicalRangePage.Row(key,new byte[0])),true)));
                    assertTrue(request.length>2*1024*1024+64*1024); var input=ByteBuffer.wrap(packet.body());
                    ScopedTransactionRpc.readHandle(input); ScopedTransactionRpc.uuid(input); ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); input.get();
                    assertArrayEquals(key,ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,true));
                    return ScopedTransactionRpc.frame(ScopedTransactionRpc.PAGE,ScopedTransactionRpc.pageBytes(new PhysicalRangePage(List.of(),false)));
                }
            };
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(f.column,null,null)));
            try (var routed=new RatisRoutedClient(directory(f),transport); var tx=routed.openTransaction(scope,Duration.ofSeconds(1))) {
                var first=tx.page(f.column,null,null,false,null,1,PhysicalRangePage.MAX_BYTES); assertTrue(first.hasMore());
                var second=tx.page(f.column,null,null,false,first.resumeAfter(),1,PhysicalRangePage.MAX_BYTES); assertFalse(second.hasMore()); assertTrue(second.rows().isEmpty());
            }
        }
    }
    @Test void pointAndStateQueriesUseTheCurrentFraming() throws Exception {
        try (var f=fixture("current-reads")) {
            f.apply(f.propose(f.put(0)),1);
            var request=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(f.column,new byte[]{0})));
            assertArrayEquals(new byte[]{7},ScopedTransactionRpc.pointValue(f.machine.queryLinearizable(request).get(5,TimeUnit.SECONDS).getContent().toByteArray(),2*1024*1024));
            var stateRequest=Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0])));
            var state=ScopedTransactionRpc.parse(f.machine.queryLinearizable(stateRequest).get(5,TimeUnit.SECONDS).getContent().toByteArray(),ScopedTransactionRpc.MAX_REPLY_BYTES);
            assertEquals(new ApplyPosition(1,1),RangeStateView.decode(state.body()).applied());
            assertThrows(IllegalStateException.class,() -> f.machine.transactionClock(nanos::get));
        }
    }
    @Test void expiredAcceptedAuthorityResolvesDurablyAfterLostReplyAndReopen() throws Exception {
        try (var f=fixture("expired-accepted")) {
            var transport=new Controlled(f); var directory=directory(f); Path journal=root.resolve("expired-accepted.bin"); transport.journal=journal;
            var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),10).issue(f.session,100); byte[] original;
            try (var routed=new RatisRoutedClient(directory,transport)) {
                try (var client=RatisDurableSessionClient.create(journal,routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults()); var tx=routed.openTransaction(scope(f,0),Duration.ofMillis(1))) {
                    var pending=client.prepareTransactionIntent(List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{0},new byte[]{7},Mutation.ReturnMode.PREVIOUS)));
                    client.acceptTransaction(pending,tx.plan(pending.command())); original=DistributedCommandCodec.encode(pending.command());
                    nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2)); f.machine.expireTransactions(); assertEquals(0,f.machine.proposalFootprintStats().candidates());
                    assertThrows(IllegalStateException.class,() -> client.abandonTransactionIntent(pending));
                    transport.lose=true; assertThrows(IOException.class,() -> client.execute(pending)); assertNull(pending.savedResult());
                }
                try (var client=RatisDurableSessionClient.reopen(journal,routed)) {
                    var pending=client.pending().getFirst(); assertArrayEquals(original,DistributedCommandCodec.encode(pending.command()));
                    assertEquals(CommandResult.Status.CONFLICT,client.execute(pending).status());
                    assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.operationResult(pending.command().operationId(),pending.command().payloadDigest())).status());
                    try (var pin=f.store.capture()) { assertNull(pin.get(f.column,new byte[]{0})); }
                    client.acknowledge(pending); assertEquals(0,client.acknowledgedThrough()); f.assertReleased();
                }
            }
        }
    }
    @Test void delayedExpiryCleanupCannotAuthorizeALateFirstSubmission() throws Exception {
        try (var f=fixture("late-submission"); var routed=new RatisRoutedClient(directory(f),new Controlled(f)); var tx=routed.openTransaction(scope(f,0),Duration.ofMillis(1))) {
            var accepted=tx.plan(f.put(0)); nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
            assertEquals(1,f.machine.proposalFootprintStats().candidates(),"No cleanup quantum has run");
            var transaction=f.propose(accepted);
            try {
                assertNull(transaction.getException());
                assertTrue(DistributedCommandCodec.decode(transaction.getStateMachineLogEntry().getLogData().toByteArray()).transactionRejection());
                assertEquals(CommandResult.Status.CONFLICT,f.apply(transaction,1)); try (var pin=f.store.capture()) { assertNull(pin.get(f.column,new byte[]{0})); }
            } finally { f.machine.cancelTransaction(transaction); }
            f.assertReleased();
        }
    }
}
