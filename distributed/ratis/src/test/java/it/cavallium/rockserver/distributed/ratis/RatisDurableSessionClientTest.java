package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** Durable user-window/native composition; no server process, network or power-loss acceptance. */
class RatisDurableSessionClientTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void preparedReopenLostNativeReplyConsumedHoleAndDurableAckNeverResequence() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            Path path=root.resolve("window.bin"); byte[] frozen;
            try (var client=RatisDurableSessionClient.create(path,f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults())) {
                var first=client.prepare(List.of(new Mutation(Mutation.Kind.PUT,f.outer.data,new byte[]{7},new byte[]{9},Mutation.ReturnMode.PREVIOUS)));
                frozen=DistributedCommandCodec.encode(first.command()); assertNull(first.savedResult()); assertEquals(0,first.command().operationId().sequence());
                assertThrows(IOException.class,() -> RatisDurableSessionClient.reopen(path,f.routed));
            }
            try (var client=RatisDurableSessionClient.reopen(path,f.routed)) {
                var first=client.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(first.command()));
                var second=client.prepare(List.of(new Mutation(Mutation.Kind.PUT,f.outer.data,new byte[]{(byte)200},new byte[]{8})));
                f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,100),first,-1));
                f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,101),second,-1));
                f.lost.add(first.descriptor().groupId()); assertThrows(IOException.class,() -> client.execute(first));
                assertNull(first.savedResult()); client.execute(second); client.acknowledge(second);
                assertTrue(second.consumed()); assertEquals(-1,client.acknowledgedThrough()); assertEquals(2,client.pending().size());
            }
            try (var client=RatisDurableSessionClient.reopen(path,f.routed)) {
                var first=client.pending().getFirst(); var second=client.pending().getLast();
                assertFalse(first.consumed()); assertNull(first.savedResult()); assertTrue(second.consumed()); assertNotNull(second.savedResult());
                int sent=f.submissions; assertEquals(CommandResult.Status.SUCCESS,client.execute(second).status()); assertEquals(sent,f.submissions);
                assertArrayEquals(frozen,DistributedCommandCodec.encode(first.command())); assertNull(client.execute(first).value(0),"Retry returns saved pre-write absence");
                client.acknowledge(first); assertEquals(1,client.acknowledgedThrough()); assertTrue(client.pending().isEmpty());
            }
            try (var client=RatisDurableSessionClient.reopen(path,f.routed)) {
                assertTrue(client.pending().isEmpty()); assertEquals(1,client.acknowledgedThrough());
                var next=client.prepare(List.of(new Mutation(Mutation.Kind.DELETE,f.outer.data,new byte[]{7},null)));
                assertEquals(2,next.command().operationId().sequence()); assertEquals(1,next.command().acknowledgedThrough());
                client.execute(next); client.acknowledge(next);
            }
        }
    }
    @Test void completionWriteFailureFencesBeforeOutcomeExposureAndReopenRetriesNativeIdentity() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            Path path=root.resolve("failure.bin"), temporary=root.resolve("failure.bin.pending"); byte[] frozen;
            try (var client=RatisDurableSessionClient.create(path,f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults())) {
                var pending=client.prepare(List.of(new Mutation(Mutation.Kind.PUT,f.outer.data,new byte[]{7},new byte[]{9},Mutation.ReturnMode.PREVIOUS)));
                frozen=DistributedCommandCodec.encode(pending.command());
                f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,100),pending,-1));
                Files.createDirectory(temporary); Files.writeString(temporary.resolve("block"),"fixture");
                assertThrows(IOException.class,() -> client.execute(pending)); assertNull(pending.savedResult()); int sent=f.submissions;
                assertThrows(IOException.class,() -> client.execute(pending)); assertThrows(IOException.class,() -> client.acknowledge(pending));
                assertThrows(IOException.class,() -> client.prepare(List.of(new Mutation(Mutation.Kind.DELETE,f.outer.data,new byte[]{7},null))));
                assertEquals(sent,f.submissions);
            }
            Files.delete(temporary.resolve("block")); Files.delete(temporary);
            try (var client=RatisDurableSessionClient.reopen(path,f.routed)) {
                var pending=client.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(pending.command()));
                assertNull(client.execute(pending).value(0)); client.acknowledge(pending);
            }
        }
    }
    @Test void disjointRpcOwnersPipelineWhileCloseAndSameIdentityRemainBounded() throws Exception {
        var outer=new RatisSessionClientTest(); var began=new CountDownLatch(1); var release=new CountDownLatch(1); var calls=new AtomicInteger();
        try (var routed=new RatisRoutedClient(outer.directory(),new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor range,DistributedCommand command) throws IOException {
                calls.incrementAndGet(); if (command.operationId().sequence()==0) {
                    began.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new IOException(interrupted); }
                }
                return new CommandResult(CommandResult.Status.SUCCESS,List.of());
            }
        }); var executor=Executors.newSingleThreadExecutor();
             var client=RatisDurableSessionClient.create(root.resolve("pipeline.bin"),routed,outer.ledger,outer.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults())) {
            var first=client.prepare(List.of(outer.put(1))); var second=client.prepare(List.of(outer.put(200)));
            var active=executor.submit(() -> client.execute(first));
            try {
                assertTrue(began.await(5,TimeUnit.SECONDS)); assertThrows(IOException.class,client::close);
                assertThrows(IOException.class,() -> client.execute(first)); client.execute(second); client.acknowledge(second);
                assertTrue(second.consumed()); assertEquals(2,calls.get()); assertEquals(-1,client.acknowledgedThrough());
            } finally { release.countDown(); }
            active.get(5,TimeUnit.SECONDS); client.acknowledge(first); assertEquals(1,client.acknowledgedThrough());
        }
    }
    @Test void exhaustedSequenceRestoresFinalIdentityAndMalformedWindowsCannotInventAllocation() throws Exception {
        var outer=new RatisSessionClientTest();
        try (var routed=new RatisRoutedClient(outer.directory(),new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor range,DistributedCommand command) { return new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of()); }
        })) {
            var grant=outer.ledger.grants().getFirst(); var limits=RatisSessionClient.Limits.defaults();
            var prepared=routed.prepare(new OperationId(grant.sessionId(),grant.epoch(),Long.MAX_VALUE),List.of(outer.put(1)),List.of(),Long.MAX_VALUE-1);
            var entry=new RatisSessionClient.WindowEntry(prepared,null,false);
            var state=new RatisSessionClient.WindowState(Long.MAX_VALUE,Long.MAX_VALUE-1,true,List.of(entry));
            Path path=root.resolve("exhausted.bin");
            try (var journal=SessionWindowJournal.open(path,routed,true)) { journal.write(grant,limits,state); }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                assertEquals(CommandResult.Status.SESSION_EXPIRED,client.execute(client.pending().getFirst()).status()); client.acknowledge(client.pending().getFirst());
                assertEquals(Long.MAX_VALUE,client.acknowledgedThrough()); assertTrue(client.pending().isEmpty());
                assertThrows(IllegalStateException.class,() -> client.prepare(List.of(outer.put(2))));
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) { assertEquals(Long.MAX_VALUE,client.acknowledgedThrough()); assertTrue(client.pending().isEmpty()); }
            assertThrows(IllegalArgumentException.class,() -> RatisSessionClient.restore(routed,grant,limits,new RatisSessionClient.WindowState(3,-1,false,List.of(entry))));
            assertThrows(IllegalArgumentException.class,() -> RatisSessionClient.restore(routed,grant,limits,new RatisSessionClient.WindowState(Long.MAX_VALUE,Long.MAX_VALUE-1,true,List.of(new RatisSessionClient.WindowEntry(prepared,null,true)))));
        }
    }
    @Test void frozenRestoreValidatesEveryPredicateAndCompleteRangeDeletionProjection() throws Exception {
        var outer=new RatisSessionClientTest(); var directory=outer.directory();
        var transport=new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError(); }
        };
        try (var routed=new RatisRoutedClient(directory,transport)) {
            var range=directory.ranges().getFirst(); var id=new OperationId(outer.session,1,0);
            var valid=new DistributedCommand(range.groupId(),1,directory.schemaDigest(),id,
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.column,new byte[]{1},new byte[]{(byte)128})));
            assertDoesNotThrow(() -> routed.restoreSessionCommand(valid,range));
            var crossing=new DistributedCommand(range.groupId(),1,directory.schemaDigest(),id,
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.column,new byte[]{1},new byte[]{(byte)129})));
            assertThrows(IllegalArgumentException.class,() -> routed.restoreSessionCommand(crossing,range));
            var predicate=new DistributedCommand(range.groupId(),1,directory.schemaDigest(),id,List.of(outer.put(1)),
                    List.of(new KeyPredicate(outer.column,new byte[]{(byte)200},null)));
            assertThrows(IllegalArgumentException.class,() -> routed.restoreSessionCommand(predicate,range));
        }
        var source=directory.ranges().getFirst();
        var full=new RangeDescriptor(source.rangeId(),source.groupId(),1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,source.replicas());
        var prefix=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),1,directory.schemaDigest(),0,directory.columns(),2,List.of(full));
        try (var routed=new RatisRoutedClient(prefix,transport)) {
            var command=new DistributedCommand(full.groupId(),1,prefix.schemaDigest(),new OperationId(outer.session,1,0),
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.column,new byte[]{1,0},new byte[]{2})));
            assertThrows(IllegalArgumentException.class,() -> routed.restoreSessionCommand(command,full));
        }
        var prefixRanges=directory.ranges();
        var oneBytePrefix=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),1,directory.schemaDigest(),0,directory.columns(),1,prefixRanges);
        try (var routed=new RatisRoutedClient(oneBytePrefix,transport)) {
            var range=prefixRanges.getFirst();
            var command=new DistributedCommand(range.groupId(),1,oneBytePrefix.schemaDigest(),new OperationId(outer.session,1,0),
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE,outer.column,new byte[]{1,0},new byte[]{(byte)128,0})));
            assertThrows(IllegalArgumentException.class,() -> routed.restoreSessionCommand(command,range));
        }
    }

}
