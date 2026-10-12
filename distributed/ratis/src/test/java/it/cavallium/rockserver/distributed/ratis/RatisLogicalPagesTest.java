package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Logical page/cursor parity on owned native cuts; the quantum fixture isolates client-side bounded work. */
class RatisLogicalPagesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    RatisDurableLogicalSessionTest helper() { var h=new RatisDurableLogicalSessionTest(); h.root=root; return h; }
    TransactionScope scope(DistributedSchema schema,byte[] lower,byte[] upper) { return new TransactionScope(List.of(TransactionScope.Span.range(schema.column(71).columnId(),lower,upper))); }
    RatisLogicalClient.Change put(Keys key,int value) { return RatisLogicalClient.Change.put(71,key,Buf.wrap(new byte[]{(byte)value}),Mutation.ReturnMode.NONE); }
    List<Keys> keys(RangePage<KV> page) { return page.items().stream().map(KV::keys).toList(); }
    @Test void fixedPagesAndBorrowedCursorKeepBoundsBudgetContinuationAndStagedSnapshotValues() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            Path path=Files.createDirectory(root.resolve("fixed-pages"));
            try (var session=h.create(path,f,schema,1000)) {
                session.advance(); var seed=session.prepare(List.of(put(h.key(1),1),put(h.key(3),3),put(h.key(5),5)),List.of());
                session.ensureBinding(seed); session.execute(seed); session.acknowledge(seed);
                try (var tx=session.openTransaction(scope(schema,new byte[]{1},new byte[]{6}),Duration.ofSeconds(5))) {
                    tx.stage(List.of(put(h.key(2),2),RatisLogicalClient.Change.delete(71,h.key(3),Mutation.ReturnMode.NONE)));
                    var page=tx.getRangePage(71,h.key(1),h.key(6),false,null,new RangeBudget(2,48)); assertEquals(List.of(h.key(1),h.key(2)),keys(page)); assertTrue(page.hasMore());
                    var next=tx.getRangePage(71,h.key(1),h.key(6),false,page.resumeAfter(),new RangeBudget(2,48)); assertEquals(List.of(h.key(5)),keys(next)); assertFalse(next.hasMore());
                    var reverse=tx.getRangePage(71,h.key(1),h.key(6),true,null,new RangeBudget(2,48)); assertEquals(List.of(h.key(5),h.key(2)),keys(reverse)); assertTrue(reverse.hasMore());
                    assertEquals(List.of(h.key(1)),keys(tx.getRangePage(71,h.key(1),h.key(6),true,reverse.resumeAfter(),new RangeBudget(2,48))));
                    var tooSmall=assertThrows(RocksDBException.class,() -> tx.getRangePage(71,h.key(1),h.key(6),false,null,new RangeBudget(1,1)));
                    assertEquals(RocksDBException.RocksDBErrorType.RANGE_ITEM_TOO_LARGE,tooSmall.getErrorUniqueId());
                    assertThrows(IllegalArgumentException.class,() -> tx.getRangePage(71,h.key(1),h.key(6),false,h.key(6),RangeBudget.DEFAULT));
                    assertThrows(IllegalArgumentException.class,() -> tx.getRangePage(71,null,null,false,null,RangeBudget.DEFAULT));
                    assertThrows(IllegalArgumentException.class,() -> tx.getRangePage(71,h.key(1),h.key(6),false,null,new RangeBudget(4097,1024)));
                    assertThrows(IllegalArgumentException.class,() -> tx.openCursor(71,null,null,false));
                    assertThrows(IllegalArgumentException.class,() -> tx.openCursor(71,h.key(6),h.key(1),false));
                    Keys lower=h.key(1);
                    try (var cursor=tx.openCursor(71,lower,h.key(6),false)) {
                        lower.keys()[0]=Buf.wrap(new byte[]{5}); var first=cursor.nextPage(new RangeBudget(1,24)); assertEquals(List.of(h.key(1)),keys(first));
                        first.resumeAfter().keys()[0]=Buf.wrap(new byte[]{99}); // Returned buffers cannot corrupt the cursor's private continuation.
                        var concurrent=session.preparePut(71,h.key(5),Buf.wrap(new byte[]{99}),Mutation.ReturnMode.NONE); session.execute(concurrent); session.acknowledge(concurrent);
                        assertEquals(List.of(h.key(2)),keys(cursor.nextPage(new RangeBudget(1,24))));
                        var last=cursor.nextPage(new RangeBudget(1,24)); assertEquals(List.of(h.key(5)),keys(last)); assertArrayEquals(new byte[]{5},last.items().getFirst().value().toByteArray()); assertFalse(last.hasMore());
                        assertTrue(cursor.nextPage(RangeBudget.DEFAULT).items().isEmpty());
                    }
                    try (var cursor=tx.openCursor(71,h.key(1),h.key(6),false)) {
                        assertEquals(1,cursor.nextPage(new RangeBudget(1,24)).items().size()); tx.close();
                        assertThrows(IOException.class,() -> cursor.nextPage(RangeBudget.DEFAULT)); cursor.close(); assertTrue(cursor.nextPage(RangeBudget.DEFAULT).items().isEmpty());
                    }
                }
                assertArrayEquals(new byte[]{99},session.get(71,h.key(5)).toByteArray()); assertTrue(session.pending().isEmpty());
            }
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void bucketPagesSortOriginalMembersAndResumeWithinTheBucketInEitherDirection() throws Exception {
        var h=helper(); var b=new RatisBucketSessionTest();
        for (boolean values:new boolean[]{true,false}) {
            var schema=b.schema(values); Path instance=Files.createDirectory(root.resolve("buckets-"+values));
            try (var f=new RatisSessionPropagationTest.Fixture(instance,true,schema.digest(),schema);
                 var session=h.create(Files.createDirectory(instance.resolve("session")),f,schema,1000)) {
                session.advance(); Keys empty=new Keys(Buf.wrap(new byte[]{6}),Buf.wrap(new byte[]{1})); Keys row8=new Keys(Buf.wrap(new byte[]{8}),Buf.wrap(new byte[]{1}));
                var changes=new ArrayList<RatisLogicalClient.Change>();
                for (int n:new int[]{3,1,2}) changes.add(RatisLogicalClient.Change.put(71,b.key(n),values ? Buf.wrap(new byte[]{(byte)n}) : null,Mutation.ReturnMode.NONE));
                changes.add(RatisLogicalClient.Change.put(71,row8,values ? Buf.wrap(new byte[]{8}) : null,Mutation.ReturnMode.NONE));
                changes.add(RatisLogicalClient.Change.delete(71,empty,Mutation.ReturnMode.NONE));
                var seed=session.prepare(changes,List.of()); session.ensureBinding(seed); session.execute(seed); session.acknowledge(seed);
                Keys lower=new Keys(Buf.wrap(new byte[]{6}),Buf.wrap(new byte[]{0})), upper=new Keys(Buf.wrap(new byte[]{9}),Buf.wrap(new byte[]{0}));
                try (var tx=session.openTransaction(scope(schema,new byte[]{6,0},new byte[]{9,1}),Duration.ofSeconds(5))) {
                    var first=tx.getRangePage(71,lower,upper,false,null,new RangeBudget(2,1024)); assertEquals(List.of(b.key(1),b.key(2)),keys(first)); assertTrue(first.hasMore());
                    var rest=tx.getRangePage(71,lower,upper,false,first.resumeAfter(),new RangeBudget(2,1024)); assertEquals(List.of(b.key(3),row8),keys(rest)); assertFalse(rest.hasMore());
                    var reverse=tx.getRangePage(71,lower,upper,true,null,new RangeBudget(2,1024)); assertEquals(List.of(row8,b.key(3)),keys(reverse));
                    var prior=tx.getRangePage(71,lower,upper,true,reverse.resumeAfter(),new RangeBudget(2,1024)); assertEquals(List.of(b.key(2),b.key(1)),keys(prior)); assertFalse(prior.hasMore());
                    Keys between=new Keys(Buf.wrap(new byte[]{7}),Buf.wrap(new byte[]{2,1}));
                    assertEquals(List.of(b.key(3),row8),keys(tx.getRangePage(71,lower,upper,false,between,new RangeBudget(2,1024))));
                    tx.stage(List.of(RatisLogicalClient.Change.put(71,b.key(0),values ? Buf.wrap(new byte[]{9}) : null,Mutation.ReturnMode.NONE)));
                    try (var cursor=tx.openCursor(71,lower,upper,false)) {
                        assertEquals(List.of(b.key(0)),keys(cursor.nextPage(new RangeBudget(1,values ? 31 : 30))));
                        assertEquals(List.of(b.key(1),b.key(2),b.key(3),row8),keys(cursor.nextPage(RangeBudget.DEFAULT)));
                    }
                }
                assertArrayEquals(values ? new byte[]{1} : new byte[0],session.get(71,b.key(1)).toByteArray());
                for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
            }
        }
    }
    @Test void cursorOwnerLossFailsInsteadOfReopeningAtCurrentState() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema);
             var session=h.create(Files.createDirectory(root.resolve("loss")),f,schema,1000)) {
            session.advance(); var seed=session.prepare(List.of(put(h.key(1),1),put(h.key(2),2)),List.of()); session.ensureBinding(seed); session.execute(seed); session.acknowledge(seed);
            try (var tx=session.openTransaction(scope(schema,new byte[]{1},new byte[]{3}),Duration.ofSeconds(5)); var cursor=tx.openCursor(71,h.key(1),h.key(3),false)) {
                assertTrue(cursor.nextPage(new RangeBudget(1,24)).hasMore()); var machine=f.machines.get(tx.descriptor().groupId()); machine.notifyNotLeader(List.of()); machine.expireTransactions();
                assertThrows(IOException.class,() -> cursor.nextPage(RangeBudget.DEFAULT));
                assertEquals(0,f.stores.get(tx.descriptor().groupId()).activeSnapshotPins());
            }
        }
    }
    @Test void anUnboundedLogicalRequestCannotTurnAShardBoundaryIntoEndOfData() throws Exception {
        var h=helper(); var schema=h.schema();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema);
             var session=h.create(Files.createDirectory(root.resolve("shard-boundary")),f,schema,1000)) {
            session.advance();
            for (int n:new int[]{1,130}) {
                var seed=session.preparePut(71,h.key(n),Buf.wrap(new byte[]{(byte)n}),Mutation.ReturnMode.NONE);
                session.ensureBinding(seed); session.execute(seed); session.acknowledge(seed);
            }
            try (var tx=session.openTransaction(scope(schema,null,new byte[]{(byte)128}),Duration.ofSeconds(5))) {
                assertThrows(IllegalArgumentException.class,() -> tx.getRangePage(71,null,null,false,null,RangeBudget.DEFAULT));
                assertThrows(IllegalArgumentException.class,() -> tx.openCursor(71,null,null,true));
                var bounded=tx.getRangePage(71,null,h.key(128),false,null,RangeBudget.DEFAULT);
                assertEquals(List.of(h.key(1)),keys(bounded)); assertFalse(bounded.hasMore());
            }
            assertArrayEquals(new byte[]{(byte)130},session.get(71,h.key(130)).toByteArray());
        }
    }
    @Test void emptyPhysicalRowsHaveABoundedQuantumAndNeverReturnAnEmptyContinuingPage() throws Exception {
        var h=helper(); var o=new RatisSessionEpochsTest();
        var schema=new DistributedSchema(o.cluster,o.database,new UUID(6,1),1,0,List.of(new DistributedSchema.Column(71,o.data,"bucket",
                ColumnSchema.of(IntArrayList.of(2),ObjectArrayList.of(ColumnHashType.ALLSAME8),true),null)));
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            var calls=new AtomicInteger(); var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException {
                    var packet=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if (packet.kind()==ScopedTransactionRpc.PAGE) {
                        var input=ByteBuffer.wrap(packet.body()); ScopedTransactionRpc.readHandle(input); ScopedTransactionRpc.uuid(input);
                        ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); ScopedTransactionRpc.blob(input,TransactionScope.MAX_BOUND_BYTES,true); input.get();
                        byte[] resume=ScopedTransactionRpc.blob(input,PhysicalRangePage.MAX_BYTES,true); int count=input.getInt(); int start=resume==null ? 0 : ((resume[0]&255)<<8 | resume[1]&255)+1;
                        calls.incrementAndGet(); var rows=new ArrayList<PhysicalRangePage.Row>();
                        for (int n=start;n<start+count;n++) rows.add(new PhysicalRangePage.Row(new byte[]{(byte)(n>>>8),(byte)n,0},new byte[0]));
                        return ScopedTransactionRpc.frame(ScopedTransactionRpc.PAGE,ScopedTransactionRpc.pageBytes(new PhysicalRangePage(rows,true)));
                    }
                    try { var machine=f.machines.get(route.groupId()); var message=Message.valueOf(ByteString.copyFrom(request)); return (fresh ? machine.queryLinearizable(message) : machine.query(message)).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
                    catch (Exception failure) { throw new IOException(failure); }
                }
            };
            try (var routed=new RatisRoutedClient(f.directory,transport);
                 var session=RatisDurableLogicalSession.create(Files.createDirectory(root.resolve("quantum")),schema,f.catalogue.epochs,routed,f.logicalHomes(),f.logicalScheduler(),f.logicalDirectories(),f.logicalAcknowledgements(),1000,RatisSessionClient.Limits.defaults())) {
                session.advance();
                try (var tx=session.openTransaction(scope(schema,new byte[]{0,0,0},new byte[]{32,0,1}),Duration.ofSeconds(5))) {
                    var low=new Keys(Buf.wrap(new byte[]{0,0}),Buf.wrap(new byte[]{0})); var high=new Keys(Buf.wrap(new byte[]{32,0}),Buf.wrap(new byte[]{0}));
                    assertThrows(IOException.class,() -> tx.getRangePage(71,low,high,false,null,RangeBudget.DEFAULT)); assertEquals(32,calls.get()); assertTrue(session.pending().isEmpty());
                }
            }
        }
    }
}
