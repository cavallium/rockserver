package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RatisSessionClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID cluster=UUID.randomUUID(), database=UUID.randomUUID(), column=UUID.randomUUID(), session=UUID.randomUUID();
    final UUID left=UUID.randomUUID(), right=UUID.randomUUID();
    final SessionEpochLedger ledger=SessionEpochLedger.initial(cluster,database,10).issue(session,1000);
    RangeDirectory directory() {
        var replica=new ReplicaDescriptor(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),"fixture","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
        return new RangeDirectory(cluster,database,UUID.randomUUID(),1,new byte[32],0,Set.of(column),0,List.of(
                new RangeDescriptor(UUID.randomUUID(),left,1,0,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(replica)),
                new RangeDescriptor(UUID.randomUUID(),right,1,0,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
    }
    Mutation put(int key) { return new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)key},new byte[]{7}); }
    RatisSessionClient session(RatisRoutedClient routed,int count) {
        return new RatisSessionClient(routed,ledger,ledger.grants().getFirst(),new RatisSessionClient.Limits(count,16*1024*1024,16*1024*1024));
    }
    @Test void lostNativeReplyKeepsIdentityAndAckCannotPassAnUnconsumedGapAcrossRanges() throws Exception {
        try (var pool=DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()))) {
            var stores=new HashMap<UUID,ReplicatedRangeStore>(); var positions=new HashMap<UUID,Long>();
            for (UUID group:List.of(left,right)) stores.put(group,pool.openRange(new RangeOwnership.Owner(group,1),
                    new RangeOwnership.Interval(left,Set.of(column),group.equals(left) ? null : new byte[]{(byte)128},group.equals(left) ? new byte[]{(byte)128} : null),new byte[32],Map.of(column,"data"),Set.of()));
            var sent=new ArrayList<byte[]>(); var lost=new AtomicInteger(1);
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor descriptor,UUID col,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                    sent.add(DistributedCommandCodec.encode(command));
                    try {
                        long position=positions.compute(descriptor.groupId(),(group,previous) -> previous==null ? 0L : previous+1);
                        var result=CommandResult.decode(stores.get(descriptor.groupId()).apply(new ApplyPosition(1,position),command));
                        if (lost.getAndDecrement()>0) throw new IOException("Reply lost after native apply");
                        return result;
                    } catch (org.rocksdb.RocksDBException failure) { throw new IOException(failure); }
                }
            };
            try (var routed=new RatisRoutedClient(directory(),transport)) {
                var client=session(routed,2);
                var first=client.prepare(List.of(put(1))); var second=client.prepare(List.of(put(200)));
                byte[] original=DistributedCommandCodec.encode(first.command());
                assertThrows(IOException.class,() -> client.execute(first));
                assertEquals(CommandResult.Status.SUCCESS,client.execute(second).status()); client.acknowledge(second);
                assertEquals(-1,client.acknowledgedThrough()); assertEquals(2,client.outstanding());
                assertThrows(IllegalStateException.class,() -> client.acknowledge(first));
                assertThrows(IllegalStateException.class,() -> client.prepare(List.of(put(2))));
                assertEquals(CommandResult.Status.SUCCESS,client.execute(first).status());
                assertArrayEquals(original,sent.get(2)); client.acknowledge(first);
                assertEquals(1,client.acknowledgedThrough()); assertEquals(0,client.outstanding());
                assertEquals(1,stores.get(left).receiptUsage().retainedCount()); assertEquals(1,stores.get(right).receiptUsage().retainedCount());
                var third=client.prepare(List.of(put(2))); assertEquals(2,third.command().operationId().sequence());
                assertEquals(1,third.command().acknowledgedThrough()); client.execute(third); client.acknowledge(third);
                var ack=client.prepareAcknowledgement(column,new byte[]{(byte)200});
                assertEquals(2,ack.command().acknowledgedThrough()); client.execute(ack); client.acknowledge(ack);
                assertEquals(1,stores.get(left).receiptUsage().retainedCount()); assertEquals(1,stores.get(right).receiptUsage().retainedCount());
                int calls=sent.size(); assertEquals(CommandResult.Status.SUCCESS,client.execute(first).status());
                client.acknowledge(first); assertEquals(calls,sent.size()); assertEquals(3,client.acknowledgedThrough());
            }
        }
    }
    @Test void failedPreparationAndByteLimitDoNotAllocateSequencesAndResultsNeedExplicitConsumption() throws Exception {
        var success=new CommandResult(CommandResult.Status.CONFLICT,List.of());
        var calls=new AtomicInteger();
        try (var routed=new RatisRoutedClient(directory(),new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor descriptor,UUID col,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { calls.incrementAndGet(); return success; }
        })) {
            var client=new RatisSessionClient(routed,ledger,ledger.grants().getFirst(),new RatisSessionClient.Limits(2,1024,8));
            assertThrows(IllegalArgumentException.class,() -> client.prepare(List.of(put(1),put(200))));
            assertThrows(IllegalStateException.class,() -> client.prepare(List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[1024]))));
            var returning=new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7},Mutation.ReturnMode.PREVIOUS);
            assertThrows(IllegalStateException.class,() -> client.prepare(List.of(returning)));
            assertEquals(0,client.outstanding());
            var first=client.prepare(List.of(put(1))); assertEquals(0,first.command().operationId().sequence());
            assertThrows(IllegalStateException.class,() -> client.prepare(List.of(put(2))));
            assertSame(success,client.execute(first)); assertEquals(-1,client.acknowledgedThrough());
            assertSame(success,client.execute(first)); assertEquals(1,calls.get());
            client.acknowledge(first); var next=client.prepare(List.of(put(2)));
            assertEquals(1,next.command().operationId().sequence()); assertEquals(0,next.command().acknowledgedThrough());
        }
    }
    @Test void duplicateExecutionIsBoundedAndOtherOperationsRemainIndependent() throws Exception {
        var started=new CountDownLatch(1); var release=new CountDownLatch(1);
        try (var routed=new RatisRoutedClient(directory(),new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor descriptor,UUID col,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) throws IOException {
                if (command.operationId().sequence()==0) {
                    started.countDown();
                    try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                }
                return new CommandResult(CommandResult.Status.SUCCESS,List.of());
            }
        }); var executor=Executors.newSingleThreadExecutor()) {
            var client=session(routed,2); var first=client.prepare(List.of(put(1))); var second=client.prepare(List.of(put(2)));
            var executing=executor.submit(() -> client.execute(first));
            try {
                assertTrue(started.await(5,TimeUnit.SECONDS));
                assertThrows(IOException.class,() -> client.execute(first));
                client.execute(second); client.acknowledge(second); assertEquals(-1,client.acknowledgedThrough());
            } finally { release.countDown(); }
            executing.get(5,TimeUnit.SECONDS); client.acknowledge(first); assertEquals(1,client.acknowledgedThrough());
        }
    }
    @Test void wrongGrantAndForeignPendingCannotCrossSessionOwnership() throws Exception {
        try (var routed=new RatisRoutedClient(directory(),new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor descriptor,UUID col,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError(); }
        })) {
            var client=session(routed,2); var pending=client.prepare(List.of(put(1)));
            var otherLedger=ledger.issue(UUID.randomUUID(),1000);
            var other=new RatisSessionClient(routed,otherLedger,otherLedger.grants().getLast(),RatisSessionClient.Limits.defaults());
            assertThrows(IllegalArgumentException.class,() -> other.execute(pending));
            assertThrows(IllegalArgumentException.class,() -> other.acknowledge(pending));
            assertThrows(IllegalArgumentException.class,() -> new RatisSessionClient(routed,ledger.advanceTime(1000),ledger.grants().getFirst(),RatisSessionClient.Limits.defaults()));
            assertThrows(IllegalArgumentException.class,() -> new RatisSessionClient(routed,ledger,
                    new SessionEpochLedger.Grant(1,session,10,999),RatisSessionClient.Limits.defaults()));
        }
    }
}
