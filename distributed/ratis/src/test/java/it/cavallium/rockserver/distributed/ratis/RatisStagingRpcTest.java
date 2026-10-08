package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Current framed staging through controlled dispatch/native apply; no server process or quorum claim. */
class RatisStagingRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisTransactionRpcTest helper() { var helper=new RatisTransactionRpcTest(); helper.root=root; return helper; }
    Mutation put(RatisRetentionAdmissionTest.Fixture f,int key,int value,Mutation.ReturnMode mode) {
        return new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)key},new byte[]{(byte)value},mode);
    }
    @Test void lostStageReplyPrefixExtensionAndDurableCommitRetryPreserveReadYourWrites() throws Exception {
        var helper=helper();
        try (var f=helper.fixture("staged-workflow")) {
            var delegate=helper.new Controlled(f); var loseStage=new AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return delegate.command(route,command); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    byte[] reply=delegate.transaction(route,request,fresh);
                    if (ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.STAGE && loseStage.getAndSet(false)) throw new IOException("Lost staged reply");
                    return reply;
                }
            };
            var directory=helper.directory(f); var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),10).issue(f.session,100);
            Path journal=root.resolve("staged-window.bin"); delegate.journal=journal; byte[] frozen;
            try (var routed=new RatisRoutedClient(directory,transport)) {
                try (var client=RatisDurableSessionClient.create(journal,routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
                     var tx=routed.openTransaction(helper.scope(f,0),Duration.ofSeconds(1))) {
                    var first=List.of(put(f,0,7,Mutation.ReturnMode.PREVIOUS));
                    assertThrows(IOException.class,() -> tx.stage(first)); assertArrayEquals(new byte[]{7},tx.get(f.column,new byte[]{0}));
                    assertNull(tx.stage(first).value(0));
                    var extended=List.of(first.getFirst(),put(f,0,9,Mutation.ReturnMode.PREVIOUS)); var staged=tx.stage(extended);
                    assertNull(staged.value(0)); assertArrayEquals(new byte[]{7},staged.value(1)); assertArrayEquals(staged.encode(),tx.stage(extended).encode());
                    assertThrows(IOException.class,() -> tx.stage(first)); assertArrayEquals(new byte[]{9},tx.get(f.column,new byte[]{0}));
                    var wrong=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,0),first);
                    assertThrows(IOException.class,() -> tx.plan(wrong)); assertArrayEquals(new byte[]{9},tx.get(f.column,new byte[]{0}));
                    var intent=client.prepareTransactionIntent(extended); var accepted=tx.plan(intent.command());
                    assertArrayEquals(staged.encode(),accepted.acceptance().result()); frozen=DistributedCommandCodec.encode(accepted);
                    assertEquals(0,f.store.activeSnapshotPins(),"PLAN releases the staged native owner along with its capture");
                    client.acceptTransaction(intent,accepted); delegate.lose=true; assertThrows(IOException.class,() -> client.execute(intent));
                }
                try (var recovered=RatisDurableSessionClient.reopen(journal,routed)) {
                    var pending=recovered.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(pending.command()));
                    var result=recovered.execute(pending); assertNull(result.value(0)); assertArrayEquals(new byte[]{7},result.value(1)); recovered.acknowledge(pending);
                }
                try (var pin=f.store.capture()) { assertArrayEquals(new byte[]{9},pin.get(f.column,new byte[]{0})); }
                f.assertReleased(); assertEquals(1,delegate.fresh);
            }
        }
    }
    @Test void rangeStagingUsesOrderedPointReturnsAndDurableAcceptedRetryAfterLostReply() throws Exception {
        var helper=helper();
        try (var f=helper.fixture("range-staging")) {
            for (int n:new int[]{1,2,4}) f.apply(f.propose(f.put(n)),f.store.applied().index()+1);
            var transport=helper.new Controlled(f); transport.index=3; var directory=helper.directory(f);
            var ledger=SessionEpochLedger.initial(directory.clusterId(),directory.databaseId(),10).issue(f.session,100);
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(f.column,new byte[]{0},new byte[]{6})));
            var mutations=List.of(put(f,1,8,Mutation.ReturnMode.PREVIOUS),new Mutation(Mutation.Kind.DELETE_RANGE,f.column,new byte[]{1},new byte[]{4}),put(f,2,9,Mutation.ReturnMode.PREVIOUS));
            Path path=root.resolve("range-window.bin"); transport.journal=path; byte[] frozen;
            try (var routed=new RatisRoutedClient(directory,transport)) {
                try (var window=RatisDurableSessionClient.create(path,routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults()); var tx=routed.openTransaction(scope,Duration.ofSeconds(5))) {
                    var preview=tx.stage(mutations); assertArrayEquals(new byte[]{7},preview.value(0)); assertNull(preview.value(1));
                    assertNull(tx.get(f.column,new byte[]{1})); assertArrayEquals(new byte[]{9},tx.get(f.column,new byte[]{2}));
                    var page=tx.page(f.column,new byte[]{0},new byte[]{6},false,null,4,1024); assertEquals(2,page.rows().size()); assertFalse(page.hasMore());
                    assertArrayEquals(new byte[]{2},page.rows().getFirst().key()); assertArrayEquals(new byte[]{4},page.rows().getLast().key());
                    var intent=window.prepareTransactionIntent(mutations); var accepted=tx.plan(intent.command()); assertArrayEquals(preview.encode(),accepted.acceptance().result());
                    frozen=DistributedCommandCodec.encode(accepted); window.acceptTransaction(intent,accepted); transport.lose=true; assertThrows(IOException.class,() -> window.execute(intent));
                }
                try (var window=RatisDurableSessionClient.reopen(path,routed)) {
                    var pending=window.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(pending.command()));
                    var result=window.execute(pending); assertArrayEquals(new byte[]{7},result.value(0)); assertNull(result.value(1)); window.acknowledge(pending);
                }
                try (var current=f.store.capture()) { assertNull(current.get(f.column,new byte[]{1})); assertArrayEquals(new byte[]{9},current.get(f.column,new byte[]{2})); assertArrayEquals(new byte[]{7},current.get(f.column,new byte[]{4})); }
                assertEquals(0,f.store.activeSnapshotPins()); f.assertReleased();
            }
        }
    }
    @Test void malformedStagingResultsDoNotBecomeSuccessfulRepliesAndCanBeRetried() throws Exception {
        var helper=helper();
        try (var f=helper.fixture("stage-reply-shape")) {
            var delegate=helper.new Controlled(f); var corrupt=new AtomicBoolean(true);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    byte[] reply=delegate.transaction(route,request,fresh);
                    if (ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.STAGE && corrupt.getAndSet(false)) {
                        var packet=ScopedTransactionRpc.parse(reply,ScopedTransactionRpc.MAX_REPLY_BYTES);
                        byte[] invalid=Arrays.copyOf(packet.body(),40); // Retain the correct prefix digest, remove the required return value.
                        java.nio.ByteBuffer.wrap(invalid).putInt(32,0).putInt(36,0);
                        return ScopedTransactionRpc.frame(ScopedTransactionRpc.STAGE,invalid);
                    }
                    return reply;
                }
            };
            try (var routed=new RatisRoutedClient(helper.directory(f),transport); var tx=routed.openTransaction(helper.scope(f,0),Duration.ofSeconds(1))) {
                var mutations=List.of(put(f,0,7,Mutation.ReturnMode.PREVIOUS));
                assertThrows(IOException.class,() -> tx.stage(mutations)); assertNull(tx.stage(mutations).value(0));
                assertArrayEquals(new byte[]{7},tx.get(f.column,new byte[]{0}));
            }
            assertEquals(0,f.store.activeSnapshotPins()); f.assertReleased();
        }
    }
    @Test void stagedPagesCombineInsertDeleteAndSnapshotRowsThenConflictOnConcurrentScopeChanges() throws Exception {
        var helper=helper();
        try (var f=helper.fixture("staged-pages")) {
            f.apply(f.propose(f.put(1)),1); f.apply(f.propose(f.put(2)),2);
            var transport=helper.new Controlled(f); transport.index=2;
            var scope=new TransactionScope(List.of(TransactionScope.Span.range(f.column,new byte[]{0},new byte[]{4})));
            try (var routed=new RatisRoutedClient(helper.directory(f),transport); var tx=routed.openTransaction(scope,Duration.ofSeconds(1))) {
                var mutations=List.of(put(f,0,8,Mutation.ReturnMode.NONE),new Mutation(Mutation.Kind.DELETE,f.column,new byte[]{1},null),put(f,3,9,Mutation.ReturnMode.NONE));
                tx.stage(mutations); f.apply(f.propose(new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,3),List.of(put(f,2,99,Mutation.ReturnMode.NONE)))),3);
                var first=tx.page(f.column,new byte[]{0},new byte[]{4},false,null,1,1024); assertArrayEquals(new byte[]{0},first.rows().getFirst().key()); assertTrue(first.hasMore());
                var rest=tx.page(f.column,new byte[]{0},new byte[]{4},false,first.resumeAfter(),4,1024); assertEquals(2,rest.rows().size()); assertFalse(rest.hasMore());
                assertArrayEquals(new byte[]{7},rest.rows().getFirst().value()); assertArrayEquals(new byte[]{3},rest.rows().getLast().key()); assertNull(tx.get(f.column,new byte[]{1}));
                var reverse=tx.page(f.column,new byte[]{0},new byte[]{4},true,null,4,1024); assertEquals(3,reverse.rows().size()); assertArrayEquals(new byte[]{3},reverse.rows().getFirst().key());
                var intent=new DistributedCommand(f.groupId,1,new byte[32],new OperationId(f.session,1,4),mutations);
                assertThrows(IOException.class,() -> tx.plan(intent)); assertEquals(0,f.store.activeSnapshotPins()); f.assertReleased();
            }
        }
    }
    @Test void failedStagingExtensionPreservesTheOldViewAndTtlAndLeadershipLossReleaseIt() throws Exception {
        for (boolean loseLeader:new boolean[]{false,true}) {
            var helper=helper();
            try (var f=helper.fixture("stage-loss-"+loseLeader); var routed=new RatisRoutedClient(helper.directory(f),helper.new Controlled(f));
                 var tx=routed.openTransaction(helper.scope(f,0),Duration.ofMillis(1))) {
                var first=List.of(put(f,0,7,Mutation.ReturnMode.NONE)); tx.stage(first);
                assertThrows(IOException.class,() -> tx.stage(first,List.of(new KeyPredicate(f.column,new byte[]{0},new byte[]{99}))));
                assertArrayEquals(new byte[]{7},tx.get(f.column,new byte[]{0})); assertEquals(1,f.store.activeSnapshotPins());
                if (loseLeader) f.machine.notifyNotLeader(List.of()); else helper.nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(2));
                f.machine.expireTransactions(); assertThrows(IOException.class,() -> tx.get(f.column,new byte[]{0})); assertEquals(0,f.store.activeSnapshotPins()); f.assertReleased();
            }
        }
    }
}
