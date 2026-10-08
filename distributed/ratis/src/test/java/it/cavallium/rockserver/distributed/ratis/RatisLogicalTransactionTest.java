package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Durable logical adapter against native stores and controlled Ratis dispatch; no network/quorum claims. */
class RatisLogicalTransactionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisDurableLogicalSessionTest helper() { var h=new RatisDurableLogicalSessionTest(); h.root=root; return h; }
    TransactionScope scope(DistributedSchema schema,int key) {
        var column=schema.column(71).columnId(); var physical=schema.encodeLogicalKey(column,new Keys(Buf.wrap(new byte[]{(byte)key}))).physicalKey();
        return new TransactionScope(List.of(TransactionScope.Span.point(column,physical)));
    }
    List<RatisLogicalClient.Change> changes(Keys key,int value) {
        return List.of(RatisLogicalClient.Change.put(71,key,Buf.wrap(new byte[]{(byte)value}),Mutation.ReturnMode.PREVIOUS));
    }
    @Test void stagedLogicalCommitForcesAcceptedBytesBeforeWriteAndLostReplyReopensExactly() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            Path path=Files.createDirectory(root.resolve("logical-transaction")); byte[] accepted;
            try (var session=h.create(path,f,schema,1000)) {
                session.advance(); var seed=session.preparePut(71,h.key(6),Buf.wrap(new byte[]{6}),Mutation.ReturnMode.NONE);
                session.ensureBinding(seed); session.execute(seed); session.acknowledge(seed);
                try (var tx=session.openTransaction(scope(schema,7),Duration.ofSeconds(5))) {
                    assertNull(tx.get(71,h.key(7))); var first=changes(h.key(7),7); assertNull(tx.stage(first).value(0));
                    assertArrayEquals(new byte[]{7},tx.get(71,h.key(7)).toByteArray());
                    var full=List.of(first.getFirst(),RatisLogicalClient.Change.put(71,h.key(7),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.PREVIOUS));
                    var preview=tx.stage(full); assertArrayEquals(new byte[]{7},preview.value(1));
                    var writes=new AtomicInteger();
                    f.beforeCommand=command -> {
                        if (command.acceptance()!=null) {
                            writes.incrementAndGet(); f.lost.add(command.groupId());
                            try {
                                byte[] journal=SessionJournalFiles.read(path.resolve("window.bin"),SessionWindowJournal.MAX_BYTES);
                                int length=ByteBuffer.wrap(journal).getInt(121);
                                assertArrayEquals(DistributedCommandCodec.encode(command),Arrays.copyOfRange(journal,125,125+length));
                            } catch (IOException failure) { throw new AssertionError(failure); }
                        }
                    };
                    assertThrows(IOException.class,() -> tx.commit(full));
                    var pending=tx.pendingCommit(); assertFalse(pending.awaitingAcceptance()); assertNotNull(pending.command().acceptance()); assertNull(pending.savedResult());
                    accepted=DistributedCommandCodec.encode(pending.command()); assertEquals(1,writes.get());
                    assertThrows(IllegalArgumentException.class,() -> tx.commit(first)); assertEquals(1,writes.get());
                    assertThrows(IOException.class,() -> tx.stage(full)); assertThrows(IOException.class,() -> tx.get(71,h.key(7)));
                    f.beforeCommand=command -> {};
                }
            }
            long catalogueCommands=f.catalogue.next.get();
            try (var session=h.reopen(path,f,schema)) {
                var pending=session.pending().getFirst(); assertArrayEquals(accepted,DistributedCommandCodec.encode(pending.command()));
                var result=session.execute(pending); assertNull(result.value(0)); assertArrayEquals(new byte[]{7},result.value(1)); session.acknowledge(pending);
                assertEquals(1,session.acknowledgedThrough()); assertArrayEquals(new byte[]{9},session.get(71,h.key(7)).toByteArray());
                assertEquals(catalogueCommands,f.catalogue.next.get());
            }
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void conflictedCommitIntentReopensAsUnacceptedAndCanBeAbandonedAndAcked() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            Path path=Files.createDirectory(root.resolve("conflict")); OperationId identity;
            try (var session=h.create(path,f,schema,1000)) {
                session.advance();
                try (var tx=session.openTransaction(scope(schema,7),Duration.ofSeconds(5))) {
                    var changes=changes(h.key(7),9); tx.stage(changes);
                    var concurrent=session.preparePut(71,h.key(7),Buf.wrap(new byte[]{8}),Mutation.ReturnMode.NONE);
                    session.ensureBinding(concurrent); session.execute(concurrent); session.acknowledge(concurrent);
                    assertArrayEquals(new byte[]{9},tx.get(71,h.key(7)).toByteArray()); assertThrows(IOException.class,() -> tx.commit(changes));
                    var pending=tx.pendingCommit(); assertTrue(pending.awaitingAcceptance()); identity=pending.operationId();
                    assertThrows(IllegalStateException.class,() -> session.execute(pending));
                }
            }
            try (var session=h.reopen(path,f,schema)) {
                var pending=session.pending().getFirst(); assertEquals(identity,pending.operationId()); assertTrue(pending.awaitingAcceptance());
                assertEquals(CommandResult.Status.CONFLICT,session.abandonTransactionIntent(pending).status()); assertTrue(pending.abandoned()); session.acknowledge(pending);
                assertArrayEquals(new byte[]{8},session.get(71,h.key(7)).toByteArray()); assertTrue(session.pending().isEmpty());
            }
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void lostPlanReplyRetriesTheSameIntentAndReturnsTheOriginalCommitResult() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            var plans=new AtomicInteger();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return f.routed.read(column,key); }
                public RangeStateView stateView(RangeDescriptor route) throws IOException { return f.routed.stateView(route.groupId()); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return f.routed.execute(command.sessionControl()!=null ? f.routed.restoreSessionControl(command,route) : f.routed.restoreSessionCommand(command,route)); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    try {
                        var machine=f.machines.get(route.groupId()); var message=Message.valueOf(ByteString.copyFrom(request));
                        byte[] reply=(fresh ? machine.queryLinearizable(message) : machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                        if (ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.PLAN && plans.getAndIncrement()==0) throw new IOException("Lost PLAN reply");
                        return reply;
                    } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
                }
            };
            Path path=Files.createDirectory(root.resolve("plan-retry"));
            try (var routed=new RatisRoutedClient(f.directory,transport); var session=RatisDurableLogicalSession.create(path,schema,f.catalogue.epochs,routed,1000,RatisSessionClient.Limits.defaults())) {
                session.advance();
                try (var tx=session.openTransaction(scope(schema,7),Duration.ofSeconds(5))) {
                    var changes=changes(h.key(7),7); tx.stage(changes); assertThrows(IOException.class,() -> tx.commit(changes));
                    var original=tx.pendingCommit(); assertTrue(original.awaitingAcceptance()); assertEquals(1,session.pending().size());
                    assertNull(tx.commit(changes).value(0)); assertSame(original,tx.pendingCommit()); assertEquals(2,plans.get());
                    assertNull(tx.commit(changes).value(0)); assertEquals(2,plans.get()); assertEquals(1,session.pending().size());
                    assertThrows(IllegalStateException.class,() -> session.abandonTransactionIntent(original)); session.acknowledge(original);
                }
            }
        }
    }
    @Test void logicalStagingAndCommitDistinguishCollisionMembersAndKeyOnlyPresence() throws Exception {
        var h=helper(); var buckets=new RatisBucketSessionTest();
        for (boolean values:new boolean[]{true,false}) {
            var schema=buckets.schema(values); var column=schema.column(71).columnId();
            Path instance=Files.createDirectory(root.resolve("buckets-"+values));
            try (var f=new RatisSessionPropagationTest.Fixture(instance,true,schema.digest(),schema)) {
                Path path=Files.createDirectory(instance.resolve("logical-session"));
                try (var session=h.create(path,f,schema,1000)) {
                    session.advance(); var encoded=schema.encodeLogicalKey(column,buckets.key(1));
                    var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,encoded.physicalKey())));
                    try (var tx=session.openTransaction(scope,Duration.ofSeconds(5))) {
                        var first=List.of(RatisLogicalClient.Change.put(71,buckets.key(1),values ? Buf.wrap(new byte[]{1}) : null,Mutation.ReturnMode.PREVIOUS),
                                RatisLogicalClient.Change.put(71,buckets.key(2),values ? Buf.wrap(new byte[]{2}) : null,Mutation.ReturnMode.CURRENT));
                        assertNull(tx.stage(first).value(0)); assertNull(tx.get(71,buckets.key(3)));
                        assertArrayEquals(values ? new byte[]{1} : new byte[0],tx.get(71,buckets.key(1)).toByteArray());
                        var full=new ArrayList<>(first); full.add(RatisLogicalClient.Change.delete(71,buckets.key(1),Mutation.ReturnMode.PREVIOUS));
                        full.add(RatisLogicalClient.Change.put(71,buckets.key(1),values ? Buf.wrap(new byte[]{9}) : null,Mutation.ReturnMode.CURRENT));
                        var preview=tx.stage(full); assertArrayEquals(values ? new byte[]{9} : new byte[0],tx.get(71,buckets.key(1)).toByteArray());
                        assertArrayEquals(values ? new byte[]{2} : new byte[0],tx.get(71,buckets.key(2)).toByteArray());
                        assertArrayEquals(preview.encode(),tx.commit(full).encode()); session.acknowledge(tx.pendingCommit());
                    }
                    assertArrayEquals(values ? new byte[]{9} : new byte[0],session.get(71,buckets.key(1)).toByteArray());
                    assertArrayEquals(values ? new byte[]{2} : new byte[0],session.get(71,buckets.key(2)).toByteArray());
                }
                for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
            }
        }
    }
    @Test void liveReadOnlyHandlesRetainRootOwnershipAndTheirBudgetUntilClosed() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            Path path=Files.createDirectory(root.resolve("handle-budget"));
            try (var session=RatisDurableLogicalSession.create(path,schema,f.catalogue.epochs,f.routed,1000,new RatisSessionClient.Limits(1,1024*1024,1024*1024))) {
                session.advance();
                try (var tx=session.openTransaction(scope(schema,7),Duration.ofSeconds(5))) {
                    assertThrows(IOException.class,session::close); assertThrows(IOException.class,() -> h.reopen(path,f,schema));
                    assertThrows(IOException.class,() -> session.openTransaction(scope(schema,6),Duration.ofSeconds(5)));
                    assertThrows(IllegalArgumentException.class,() -> tx.commit(changes(h.key(8),8)));
                    assertNull(tx.pendingCommit()); assertNull(tx.get(71,h.key(7))); assertTrue(session.pending().isEmpty()); tx.close(); tx.close();
                    assertThrows(IOException.class,() -> tx.get(71,h.key(7)));
                }
                try (var replacement=session.openTransaction(scope(schema,6),Duration.ofSeconds(5))) { assertNull(replacement.get(71,h.key(6))); }
            }
            try (var session=h.reopen(path,f,schema)) { assertTrue(session.pending().isEmpty()); }
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
}
