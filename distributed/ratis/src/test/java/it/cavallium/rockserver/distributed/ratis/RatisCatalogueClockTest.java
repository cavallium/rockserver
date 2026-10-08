package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** Durable journal/native CAS composition; no transport, quorum or power-loss acceptance claim. */
class RatisCatalogueClockTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void rollbackLostReplyAndReopenPreservePendingIdentityAndBoundClockReceipts() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            bootstrap(outer,f); Path journal=root.resolve("clock.bin"); byte[] pending; UUID session;
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertEquals(RatisCatalogueClock.Outcome.OBSERVED,clock.advance(-100).outcome());
                assertEquals(10,clock.observed().logicalTimeMillis());
                assertEquals(1,f.next.get(),"No-op observation consumes no catalogue identity or receipt");
                f.writeOverride=new byte[] {1};
                assertThrows(IOException.class,() -> clock.advance(20));
                pending=DistributedCommandCodec.encode(clock.pendingCommand()); session=clock.pendingCommand().operationId().sessionId();
                assertEquals(new OperationId(session,0,0),clock.pendingCommand().operationId());
                assertEquals(-1,clock.pendingCommand().acknowledgedThrough());
                assertEquals(10,clock.observed().logicalTimeMillis());
                assertEquals(20,f.epochs.read().logicalTimeMillis());
            }
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertArrayEquals(pending,DistributedCommandCodec.encode(clock.pendingCommand()));
                int reads=f.reads.get(); f.writeOverride=null;
                var tick=clock.advance(9999);
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,tick.outcome()); assertEquals(20,tick.authority().logicalTimeMillis());
                assertEquals(reads,f.reads.get(),"Pending retry does not read a replacement source cut");
                assertNull(clock.pendingCommand());
                for (int time=21;time<=40;time++) {
                    assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(time).outcome());
                    assertEquals(2,f.store.receiptUsage().retainedCount(),"Bootstrap plus one clock receipt; prior clock prefix is ACKed");
                }
                f.writeOverride=new byte[] {1}; assertThrows(IOException.class,() -> clock.advance(41));
                assertEquals(session,clock.pendingCommand().operationId().sessionId());
                assertEquals(21,clock.pendingCommand().operationId().sequence()); assertEquals(20,clock.pendingCommand().acknowledgedThrough());
                assertTrue(Files.size(journal)<=2*SessionEpochLedger.MAX_BYTES+4096);
            }
            f.writeOverride=null;
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(0).outcome());
                assertEquals(41,clock.observed().logicalTimeMillis());
                assertEquals(RatisCatalogueClock.Outcome.OBSERVED,clock.advance(41).outcome());
                assertEquals(2,f.store.receiptUsage().retainedCount());
            }
        }
    }

    @Test void conflictConsumesTheFrozenCommandAndFreshTickObservesTheNewCut() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            bootstrap(outer,f); var writable=new AtomicBoolean(false);
            var epochs=wrapped(outer,f,() -> { if (!writable.get()) throw new IOException("Unsubmitted write"); });
            try (var clock=new RatisCatalogueClock(root.resolve("clock.bin"),epochs)) {
                assertThrows(IOException.class,() -> clock.advance(30));
                var operation=clock.pendingCommand().operationId(); byte[] frozen=DistributedCommandCodec.encode(clock.pendingCommand());
                var before=f.epochs.read();
                f.epochs.execute(f.epochs.prepare(outer.operation(1),before,before.advanceTime(20)));
                int reads=f.reads.get(); writable.set(true);
                var tick=clock.advance(100);
                assertEquals(RatisCatalogueClock.Outcome.CONFLICT,tick.outcome()); assertEquals(10,tick.authority().logicalTimeMillis());
                assertEquals(reads,f.reads.get()); assertNull(clock.pendingCommand());
                assertEquals(CommandResult.Status.CONFLICT,CommandResult.decode(f.store.operationResult(operation,
                        DistributedCommandCodec.decode(frozen).payloadDigest())).status());
                writable.set(false); assertThrows(IOException.class,() -> clock.advance(40));
                assertEquals(operation.sessionId(),clock.pendingCommand().operationId().sessionId());
                assertEquals(operation.sequence()+1,clock.pendingCommand().operationId().sequence());
                assertEquals(operation.sequence(),clock.pendingCommand().acknowledgedThrough());
                assertEquals(20,clock.observed().logicalTimeMillis());
                writable.set(true); assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(0).outcome());
                assertEquals(40,clock.observed().logicalTimeMillis());
            }
        }
    }

    @Test void expiryIsTerminalAndDoesNotAdvertiseTheUnappliedReplacement() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            bootstrap(outer,f); var writable=new AtomicBoolean(false);
            var epochs=wrapped(outer,f,() -> { if (!writable.get()) throw new IOException("Unsubmitted write"); });
            try (var clock=new RatisCatalogueClock(root.resolve("clock.bin"),epochs)) {
                assertThrows(IOException.class,() -> clock.advance(30));
                var floor=DistributedCommand.advanceSessionEpochFloor(outer.catalogue,1,new byte[32],new OperationId(outer.maintenance,1,0),1);
                f.store.apply(new ApplyPosition(1,f.next.getAndIncrement()),floor);
                writable.set(true); int reads=f.reads.get();
                var tick=clock.advance(50);
                assertEquals(RatisCatalogueClock.Outcome.EXPIRED,tick.outcome()); assertEquals(10,tick.authority().logicalTimeMillis());
                assertEquals(reads,f.reads.get()); assertNull(clock.pendingCommand());
                assertEquals(10,f.epochs.read().logicalTimeMillis());
            }
        }
    }

    @Test void journalFailureFencesAndReopenResolvesAppliedPendingWhileCorruptionAndOtherOwnersFail() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            bootstrap(outer,f); Path journal=root.resolve("clock.bin"), temporary=root.resolve("clock.bin.pending");
            var failPersist=new AtomicBoolean(true);
            var epochs=wrapped(outer,f,() -> { if (failPersist.getAndSet(false)) Files.createDirectory(temporary); });
            byte[] pending;
            try (var clock=new RatisCatalogueClock(journal,epochs)) {
                assertThrows(IOException.class,() -> new RatisCatalogueClock(journal,epochs));
                assertThrows(IOException.class,() -> clock.advance(20));
                pending=DistributedCommandCodec.encode(clock.pendingCommand());
                assertThrows(IOException.class,() -> clock.advance(30));
                assertEquals(20,f.epochs.read().logicalTimeMillis());
            }
            Files.deleteIfExists(temporary);
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertArrayEquals(pending,DistributedCommandCodec.encode(clock.pendingCommand()));
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(30).outcome());
                assertEquals(20,clock.observed().logicalTimeMillis());
            }
            byte[] valid=Files.readAllBytes(journal), damaged=valid.clone(); damaged[damaged.length-1]^=1; Files.write(journal,damaged);
            assertThrows(IOException.class,() -> new RatisCatalogueClock(journal,f.epochs));
            Files.write(journal,valid);
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(30).outcome());
            }
        }
    }

    @Test void recoveredPendingMustBeAnExactClockAdvanceBoundToThisCatalogue() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            bootstrap(outer,f); Path journal=root.resolve("clock.bin");
            var epochs=wrapped(outer,f,() -> { throw new IOException("Unsubmitted write"); });
            DistributedCommand pending;
            try (var clock=new RatisCatalogueClock(journal,epochs)) {
                assertThrows(IOException.class,() -> clock.advance(20)); pending=clock.pendingCommand();
            }
            byte[] valid=Files.readAllBytes(journal), body=SessionJournalFiles.read(journal,2*SessionEpochLedger.MAX_BYTES+4096);
            var expected=SessionEpochLedger.decode(pending.predicates().getFirst().expectedValue());
            var issuance=f.epochs.prepare(pending.operationId(),expected,expected.issue(outer.a,100));
            var input=java.nio.ByteBuffer.wrap(body); input.position(72); int observedBytes=input.getInt();
            input.position(input.position()+observedBytes); int pendingOffset=input.position();
            var replacement=new java.io.ByteArrayOutputStream(); var output=new java.io.DataOutputStream(replacement);
            output.write(body,0,pendingOffset); byte[] wrong=DistributedCommandCodec.encode(issuance);
            output.writeInt(wrong.length); output.write(wrong); output.flush();
            SessionJournalFiles.write(journal,replacement.toByteArray(),2*SessionEpochLedger.MAX_BYTES+4096);
            assertThrows(IOException.class,() -> new RatisCatalogueClock(journal,f.epochs),"A valid grant CAS is not a valid clock journal command");
            Files.write(journal,valid);
            var foreign=new RatisSessionEpochs(f.client,UUID.randomUUID(),outer.database,outer.catalogue,1,new byte[32],outer.sessions);
            assertThrows(IOException.class,() -> new RatisCatalogueClock(journal,foreign));
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertArrayEquals(DistributedCommandCodec.encode(pending),DistributedCommandCodec.encode(clock.pendingCommand()));
                assertEquals(RatisCatalogueClock.Outcome.APPLIED,clock.advance(100).outcome());
            }
        }
    }

    @Test void closeDuringRpcRetainsOwnershipAndMissingCatalogueIsNeverBootstrapped() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root)) {
            Path journal=root.resolve("clock.bin");
            try (var clock=new RatisCatalogueClock(journal,f.epochs)) {
                assertThrows(IOException.class,() -> clock.advance(20)); assertNull(clock.pendingCommand()); assertNull(clock.observed());
                assertEquals(0,f.next.get());
            }
            bootstrap(outer,f);
            var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            var epochs=wrapped(outer,f,() -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("Timed out gate"); });
            var clock=new RatisCatalogueClock(journal,epochs); var executor=Executors.newSingleThreadExecutor();
            try {
                var future=executor.submit(() -> clock.advance(20)); assertTrue(entered.await(5,TimeUnit.SECONDS));
                assertNotNull(clock.pendingCommand(),"RPC does not hold the journal monitor");
                assertThrows(IOException.class,clock::close);
                assertThrows(IOException.class,() -> new RatisCatalogueClock(journal,epochs));
                release.countDown(); assertEquals(RatisCatalogueClock.Outcome.APPLIED,future.get(5,TimeUnit.SECONDS).outcome());
                clock.close(); assertThrows(IOException.class,() -> clock.advance(30));
            } finally { release.countDown(); executor.shutdownNow(); executor.awaitTermination(5,TimeUnit.SECONDS); clock.close(); }
        }
    }

    private static void bootstrap(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f) throws Exception {
        f.epochs.execute(f.epochs.prepare(outer.operation(0),null,SessionEpochLedger.initial(outer.cluster,outer.database,10)));
    }
    @FunctionalInterface private interface WriteGate { void run() throws Exception; }
    private static RatisSessionEpochs wrapped(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f,WriteGate gate) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
            if (method.getName().equals("send")) gate.run();
            try { return method.invoke(f.client.io(),args); }
            catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
        });
        var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
            case "getGroupId" -> f.client.getGroupId();
            case "io" -> io;
            default -> throw new AssertionError("Unexpected borrowed client call "+method);
        });
        return new RatisSessionEpochs(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.sessions);
    }
}
