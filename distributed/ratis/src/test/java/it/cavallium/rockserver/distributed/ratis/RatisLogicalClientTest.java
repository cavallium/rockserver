package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Native logical-key workflow and immutable retry evidence, without sockets or quorum claims. */
class RatisLogicalClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSessionEpochsTest outer=new RatisSessionEpochsTest();
    final UUID domain=new UUID(6,1), keyOnly=new UUID(2,4), bucket=new UUID(2,5), operated=new UUID(2,6);
    ColumnSchema fixed(boolean values) { return ColumnSchema.of(IntArrayList.of(8,1),new ObjectArrayList<>(),values); }
    DistributedSchema schema() {
        var op=new DistributedSchema.Operator("sum",1,"example.Sum",new byte[32],1,1,"pure-sum",1);
        return new DistributedSchema(outer.cluster,outer.database,domain,1,0,List.of(
                new DistributedSchema.Column(71,outer.data,"values",fixed(true),null),
                new DistributedSchema.Column(91,keyOnly,"keys",fixed(false),null),
                new DistributedSchema.Column(101,bucket,"buckets",ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(ColumnHashType.ALLSAME8),true),null),
                new DistributedSchema.Column(111,operated,"sum",ColumnSchema.of(IntArrayList.of(8,1),new ObjectArrayList<>(),true,"sum",1L,"example.Sum"),op)));
    }
    Keys key(long id) { return new Keys(Buf.wrap(ByteBuffer.allocate(8).putLong(id).array()),Buf.wrap(new byte[]{7})); }
    @Test void logicalAtomicWriteLostReplyExactRetryExplicitConsumptionAndSignedRouting() throws Exception {
        try (var f=new Fixture(root)) {
            var change=RatisLogicalClient.Change.put(71,key(4),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.PREVIOUS);
            var pending=f.logical.prepare(List.of(change,RatisLogicalClient.Change.put(91,key(4),null,Mutation.ReturnMode.CURRENT)),
                    List.of(new RatisLogicalClient.Condition(71,key(4),null),new RatisLogicalClient.Condition(91,key(4),null)));
            assertEquals(f.left,pending.descriptor().groupId()); f.bind(pending); int catalogueReads=f.catalogue.reads.get();
            f.lose=true; byte[] frozen=DistributedCommandCodec.encode(pending.command());
            assertThrows(IOException.class,() -> f.logical.execute(pending));
            assertThrows(IllegalStateException.class,() -> f.logical.acknowledge(pending));
            assertEquals(1,f.logical.outstanding());
            var outcome=f.logical.execute(pending); assertEquals(CommandResult.Status.SUCCESS,outcome.status());
            assertNull(outcome.value(0)); assertArrayEquals(new byte[0],outcome.value(1));
            assertArrayEquals(frozen,f.sent.get(f.sent.size()-1)); assertArrayEquals(frozen,f.sent.get(f.sent.size()-2));
            assertEquals(-1,f.logical.acknowledgedThrough()); f.logical.acknowledge(pending); assertEquals(0,f.logical.outstanding());
            assertArrayEquals(new byte[]{9},f.logical.get(71,key(4)).toByteArray()); assertTrue(f.logical.get(91,key(4)).isEmpty());
            assertNull(f.logical.get(91,key(5))); assertEquals(catalogueReads,f.catalogue.reads.get(),"Ordinary operations use the cached schema and route");
            var negative=f.logical.preparePut(71,key(-4),Buf.wrap(new byte[]{8}),Mutation.ReturnMode.NONE);
            assertEquals(f.right,negative.descriptor().groupId()); assertEquals(0,negative.command().acknowledgedThrough()); f.bind(negative);
            assertEquals(CommandResult.Status.SUCCESS,f.logical.execute(negative).status()); f.logical.acknowledge(negative);
            assertArrayEquals(new byte[]{8},f.logical.get(71,key(-4)).toByteArray());
            var deletion=f.logical.prepareDelete(71,key(-4),Mutation.ReturnMode.PREVIOUS);
            assertArrayEquals(new byte[]{8},f.logical.execute(deletion).value(0)); f.logical.acknowledge(deletion); assertNull(f.logical.get(71,key(-4)));
            var ack=f.logical.prepareAcknowledgement(71,key(4)); assertEquals(2,ack.command().acknowledgedThrough());
            f.logical.execute(ack); f.logical.acknowledge(ack);
            assertEquals(2,f.stores.get(f.left).receiptUsage().retainedCount(),"One binding receipt and the final user ACK receipt remain");
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void unsupportedSchemasInvalidInputsAndCrossRangeAtomicPlansNeverSubmitOrConsumeIdentity() throws Exception {
        try (var f=new Fixture(root)) {
            int sent=f.sent.size(),reads=f.reads;
            assertThrows(IllegalArgumentException.class,() -> f.logical.get(101,new Keys(Buf.wrap(new byte[8]))));
            assertEquals(Mutation.Kind.PUT,RatisLogicalClient.plan(f.schema,List.of(RatisLogicalClient.Change.put(111,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE)),List.of()).mutations().getFirst().kind());
            assertThrows(IllegalArgumentException.class,() -> f.logical.get(999,key(1)));
            assertThrows(IllegalArgumentException.class,() -> f.logical.get(71,new Keys(Buf.wrap(new byte[8]))));
            assertThrows(IllegalArgumentException.class,() -> f.logical.preparePut(71,key(1),null,Mutation.ReturnMode.NONE));
            assertThrows(IllegalArgumentException.class,() -> f.logical.preparePut(91,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE));
            assertThrows(IllegalArgumentException.class,() -> f.logical.prepare(List.of(
                    RatisLogicalClient.Change.put(71,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE),
                    RatisLogicalClient.Change.delete(91,key(-1),Mutation.ReturnMode.NONE)),List.of()));
            assertEquals(sent,f.sent.size()); assertEquals(reads,f.reads); assertEquals(0,f.logical.outstanding());
            assertEquals(0,f.logical.preparePut(71,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE).operationId().sequence());
        }
    }
    @Test void inputCopiesSchemaReopenStableAliasesAndSavedPredicateConflict() throws Exception {
        try (var f=new Fixture(root)) {
            assertEquals(71,f.logical.getColumnId("values"));
            var restored=DistributedSchemaCodec.decode(DistributedSchemaCodec.encode(f.schema)); assertEquals(71,restored.column("values").apiId());
            var exposed=f.logical.getColumnSchema(71); assertThrows(UnsupportedOperationException.class,() -> exposed.keys().set(0,1)); assertEquals(8,f.logical.getColumnSchema(71).key(0));
            assertThrows(UnsupportedOperationException.class,() -> f.logical.getAllColumnDefinitions().clear());
            var keys=key(6); var value=Buf.wrap(new byte[]{2}); var change=RatisLogicalClient.Change.put(71,keys,value,Mutation.ReturnMode.NONE);
            keys.keys()[0].setByte(0,(byte)0x80); value.setByte(0,(byte)9);
            var pending=f.logical.prepare(List.of(change),List.of(new RatisLogicalClient.Condition(71,key(6),Buf.wrap(new byte[]{4}))));
            f.bind(pending); assertEquals(f.left,pending.descriptor().groupId());
            assertEquals(CommandResult.Status.CONFLICT,f.logical.execute(pending).status()); f.logical.acknowledge(pending); assertNull(f.logical.get(71,key(6)));
            var next=f.logical.prepare(List.of(change),List.of()); assertEquals(1,next.operationId().sequence());
            f.logical.execute(next); f.logical.acknowledge(next); assertArrayEquals(new byte[]{2},f.logical.get(71,key(6)).toByteArray());
            var wrong=new RangeDirectory(outer.cluster,outer.database,domain,1,new byte[32],0,f.schema.columnIds(),0,f.directory.ranges());
            try (var routed=new RatisRoutedClient(wrong,f.transport)) {
                assertThrows(IllegalArgumentException.class,() -> new RatisLogicalClient(f.schema,routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults()));
            }
        }
    }
    final class Fixture implements AutoCloseable {
        final DistributedSchema schema=schema(); final UUID left=new UUID(3,1),right=new UUID(3,2);
        final RatisSessionEpochsTest.Fixture catalogue; final SessionEpochLedger ledger;
        final RangeDirectory directory; final Map<UUID,ReplicatedRangeStore> stores=new LinkedHashMap<>();
        final Map<UUID,Long> positions=new HashMap<>(); final List<byte[]> sent=new ArrayList<>();
        final RatisRoutedClient.Transport transport; final RatisRoutedClient routed; final RatisLogicalClient logical;
        boolean lose; int reads; long maintenance;
        Fixture(Path path) throws Exception {
            catalogue=outer.new Fixture(path.resolve("catalogue")); var initial=SessionEpochLedger.initial(outer.cluster,outer.database,10);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(0),null,initial)); ledger=initial.issue(outer.a,100);
            catalogue.epochs.execute(catalogue.epochs.prepare(outer.operation(1),initial,ledger));
            var replica=new ReplicaDescriptor(new UUID(5,1),new UUID(5,2),new UUID(5,3),"n","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
            directory=new RangeDirectory(outer.cluster,outer.database,domain,1,schema.digest(),0,schema.columnIds(),0,List.of(
                    new RangeDescriptor(left,left,1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(replica)),
                    new RangeDescriptor(right,right,1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
            for (var range:directory.ranges()) {
                var store=catalogue.pool.openRange(new RangeOwnership.Owner(range.groupId(),1),new RangeOwnership.Interval(domain,schema.columnIds(),range.lowerInclusive(),range.upperExclusive()),
                        schema.digest(),Map.of(outer.data,"data",keyOnly,"keys",bucket,"buckets",operated,"sum"),Set.of());
                store.enableSessionControls(); store.enableLogicalSchema(schema); stores.put(range.groupId(),store); positions.put(range.groupId(),0L);
            }
            transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor range,UUID column,byte[] key) throws IOException {
                    reads++; try (var pin=stores.get(range.groupId()).capture()) { return pin.get(column,key); }
                    catch (org.rocksdb.RocksDBException failure) { throw new IOException(failure); }
                }
                public CommandResult command(RangeDescriptor range,DistributedCommand command) throws IOException {
                    sent.add(DistributedCommandCodec.encode(command));
                    try {
                        long index=positions.compute(range.groupId(),(id,n) -> n+1)-1;
                        var result=CommandResult.decode(stores.get(range.groupId()).apply(new ApplyPosition(1,index),command));
                        if (lose && command.sessionControl()==null) { lose=false; throw new IOException("Lost native reply"); }
                        return result;
                    } catch (org.rocksdb.RocksDBException failure) { throw new IOException(failure); }
                }
            };
            routed=new RatisRoutedClient(directory,transport);
            logical=new RatisLogicalClient(schema,routed,ledger,ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
        }
        void bind(RatisLogicalClient.Pending pending) throws IOException {
            routed.execute(logical.prepareBinding(catalogue.epochs,new OperationId(outer.maintenance,ledger.minimumEpoch(),maintenance++),pending,-1));
        }
        public void close() throws Exception { routed.close(); catalogue.close(); }
    }
}
