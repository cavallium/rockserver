package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import static org.junit.jupiter.api.Assertions.*;

/** Native catalogue and durable allocation intent, without transport/quorum/power-loss claims. */
class RatisGrantIssuerTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static void bootstrap(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f) throws Exception {
        f.epochs.execute(f.epochs.prepare(outer.operation(0),null,SessionEpochLedger.initial(outer.cluster,outer.database,10)));
    }
    private static SessionEpochLedger replacement(DistributedCommand command) { return SessionEpochLedger.decode(command.mutations().getFirst().value()); }

    @Test void lostAppliedReplyReopensExactAttemptAndRetainsOriginalIssuedImageAfterClockAdvance() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path path=root.resolve("issuer.bin");
        try (var f=outer.new Fixture(root.resolve("native"))) {
            bootstrap(outer,f); byte[] frozen; SessionEpochLedger expectedIssuance;
            try (var issuer=RatisGrantIssuer.create(path,f.epochs,100)) {
                assertEquals(RatisGrantIssuer.Phase.IDLE,issuer.phase()); assertEquals(100,issuer.expiresAtMillis());
                f.writeOverride=new byte[4]; assertThrows(IOException.class,issuer::step);
                assertEquals(RatisGrantIssuer.Phase.PENDING,issuer.phase()); assertNull(issuer.issued());
                frozen=DistributedCommandCodec.encode(issuer.preparedCommand()); expectedIssuance=replacement(issuer.preparedCommand());
                assertEquals(0,issuer.preparedCommand().operationId().epoch()); assertEquals(0,issuer.preparedCommand().operationId().sequence());
                assertNotEquals(expectedIssuance.grants().getLast().sessionId(),issuer.preparedCommand().operationId().sessionId());
            }
            f.writeOverride=null; var current=f.epochs.read(); f.epochs.execute(f.epochs.prepare(outer.operation(1),current,current.advanceTime(20)));
            try (var issuer=RatisGrantIssuer.reopen(path,f.epochs)) {
                assertArrayEquals(frozen,DistributedCommandCodec.encode(issuer.preparedCommand())); int reads=f.reads.get();
                assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step()); assertEquals(reads,f.reads.get());
                assertEquals(expectedIssuance,issuer.issued().issuance()); assertEquals(expectedIssuance.grants().getLast(),issuer.issued().grant());
                assertNull(issuer.preparedCommand()); long calls=f.next.get(); assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step()); assertEquals(calls,f.next.get());
            }
            current=f.epochs.read(); f.epochs.execute(f.epochs.prepare(outer.operation(2),current,current.advanceTime(1000)));
            try (var issuer=RatisGrantIssuer.reopen(path,f.epochs)) {
                int reads=f.reads.get(); long calls=f.next.get(); assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step());
                assertEquals(reads,f.reads.get()); assertEquals(calls,f.next.get()); assertEquals(expectedIssuance,issuer.issued().issuance());
            }
            assertEquals(0,f.closes.get());
        }
    }

    @Test void racingConflictConsumesAttemptAndNextStepUsesSameUserNewSequenceAndAck() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path path=root.resolve("issuer.bin");
        try (var f=outer.new Fixture(root.resolve("conflict"))) {
            bootstrap(outer,f); var writable=new AtomicBoolean(false);
            var epochs=wrapped(outer,f,() -> { if (!writable.get()) throw new IOException("Unsubmitted write"); });
            try (var issuer=RatisGrantIssuer.create(path,epochs,100)) {
                assertThrows(IOException.class,issuer::step); var first=issuer.preparedCommand(); UUID user=replacement(first).grants().getLast().sessionId();
                var source=f.epochs.read(); var winner=f.epochs.prepareGrant(outer.operation(1),source,outer.a,80); f.epochs.execute(winner.command());
                writable.set(true); int reads=f.reads.get(); long calls=f.next.get();
                assertEquals(RatisGrantIssuer.Phase.RETRY,issuer.step()); assertEquals(calls+1,f.next.get()); assertEquals(reads,f.reads.get());
                assertNull(issuer.preparedCommand()); assertNull(issuer.issued());
                writable.set(false); assertThrows(IOException.class,issuer::step); var second=issuer.preparedCommand();
                assertEquals(first.operationId().sessionId(),second.operationId().sessionId()); assertEquals(1,second.operationId().sequence()); assertEquals(0,second.acknowledgedThrough());
                assertEquals(user,replacement(second).grants().getLast().sessionId()); assertEquals(2,replacement(second).grants().getLast().epoch());
                writable.set(true); assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step()); assertEquals(user,issuer.issued().grant().sessionId());
                assertEquals(3,f.store.receiptUsage().retainedCount(),"Bootstrap, competing user, and latest issuer receipt; conflicted attempt is ACKed");
            }
        }
    }

    @Test void expiryHasNoCasAndInvalidLifetimeDoesNotAllocateAnAttemptOrBootstrap() throws Exception {
        var outer=new RatisSessionEpochsTest();
        try (var f=outer.new Fixture(root.resolve("expiry"))) {
            try (var issuer=RatisGrantIssuer.create(root.resolve("missing.bin"),f.epochs,100)) {
                assertThrows(IOException.class,issuer::step); assertNull(issuer.preparedCommand()); assertEquals(0,f.next.get());
            }
            bootstrap(outer,f); Path expired=root.resolve("expired.bin");
            try (var issuer=RatisGrantIssuer.create(expired,f.epochs,10)) {
                long calls=f.next.get(); assertEquals(RatisGrantIssuer.Phase.EXPIRED,issuer.step()); assertEquals(calls,f.next.get()); assertNull(issuer.issued());
            }
            try (var issuer=RatisGrantIssuer.reopen(expired,f.epochs)) {
                int reads=f.reads.get(); assertEquals(RatisGrantIssuer.Phase.EXPIRED,issuer.step()); assertEquals(reads,f.reads.get());
            }
            try (var issuer=RatisGrantIssuer.create(root.resolve("lifetime.bin"),f.epochs,11+SessionEpochLedger.MAX_LIFETIME_MILLIS)) {
                long calls=f.next.get(); assertThrows(IllegalArgumentException.class,issuer::step);
                assertEquals(RatisGrantIssuer.Phase.IDLE,issuer.phase()); assertNull(issuer.preparedCommand()); assertEquals(calls,f.next.get());
            }
            assertThrows(IllegalArgumentException.class,() -> RatisGrantIssuer.create(root.resolve("zero.bin"),f.epochs,0));
        }
    }

    @Test void lastSequenceConflictBecomesDurableExhaustionWithoutNonceReplacement() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path path=root.resolve("last.bin");
        try (var f=outer.new Fixture(root.resolve("last"))) {
            bootstrap(outer,f); var writable=new AtomicBoolean(false); DistributedCommand first;
            var epochs=wrapped(outer,f,() -> { if (!writable.get()) throw new IOException("Unsubmitted write"); });
            try (var issuer=RatisGrantIssuer.create(path,epochs,100)) { assertThrows(IOException.class,issuer::step); first=issuer.preparedCommand(); }
            byte[] body=SessionJournalFiles.read(path,2*SessionEpochLedger.MAX_BYTES+4096);
            var expected=SessionEpochLedger.decode(first.predicates().getFirst().expectedValue());
            var last=f.epochs.prepare(new OperationId(first.operationId().sessionId(),0,Long.MAX_VALUE),expected,replacement(first),Long.MAX_VALUE-1);
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes); output.write(body,0,76);
            output.writeLong(Long.MAX_VALUE); output.writeLong(Long.MAX_VALUE-1); output.writeByte(1);
            byte[] command=DistributedCommandCodec.encode(last); output.writeInt(command.length); output.write(command); output.writeInt(-1); output.flush();
            SessionJournalFiles.write(path,bytes.toByteArray(),2*SessionEpochLedger.MAX_BYTES+4096);
            var source=f.epochs.read(); f.epochs.execute(f.epochs.prepare(outer.operation(1),source,source.advanceTime(20))); writable.set(true);
            try (var issuer=RatisGrantIssuer.reopen(path,epochs)) {
                assertEquals(Long.MAX_VALUE,issuer.preparedCommand().operationId().sequence()); assertEquals(RatisGrantIssuer.Phase.EXHAUSTED,issuer.step());
                assertNull(issuer.preparedCommand()); assertNull(issuer.issued()); long calls=f.next.get(); assertEquals(RatisGrantIssuer.Phase.EXHAUSTED,issuer.step()); assertEquals(calls,f.next.get());
            }
            try (var issuer=RatisGrantIssuer.reopen(path,f.epochs)) { assertEquals(RatisGrantIssuer.Phase.EXHAUSTED,issuer.phase()); }
        }
    }

    @Test void journalLockIdentityCorruptionAndFailedCompletionFenceWithoutExposingGrant() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path path=root.resolve("issuer.bin");
        try (var f=outer.new Fixture(root.resolve("journal"))) {
            bootstrap(outer,f); var blocked=new AtomicBoolean(true); var pending=new java.util.concurrent.atomic.AtomicReference<byte[]>();
            var epochs=wrapped(outer,f,() -> {
                if (blocked.getAndSet(false)) { pending.set(Files.readAllBytes(path)); Files.delete(path); Files.createDirectory(path); Files.writeString(path.resolve("block"),"fixture"); }
            });
            try (var issuer=RatisGrantIssuer.create(path,epochs,100)) {
                assertThrows(IOException.class,() -> RatisGrantIssuer.create(path,epochs,100)); assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(path,epochs));
                // The wrapper gate runs only after pending persistence; completion cannot be journaled.
                assertThrows(IOException.class,issuer::step); assertEquals(RatisGrantIssuer.Phase.PENDING,issuer.phase()); assertNull(issuer.issued());
                assertThrows(IOException.class,issuer::step);
            }
            Files.delete(path.resolve("block")); Files.delete(path); Files.write(path,pending.get());
            try (var issuer=RatisGrantIssuer.reopen(path,f.epochs)) { assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.step()); }
            byte[] valid=Files.readAllBytes(path); assertThrows(IOException.class,() -> RatisGrantIssuer.create(path,f.epochs,100));
            var foreign=new RatisSessionEpochs(f.client,UUID.randomUUID(),outer.database,outer.catalogue,1,new byte[32],outer.sessions);
            assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(path,foreign));
            byte[] corrupted=valid.clone(); corrupted[corrupted.length/2]^=1; Files.write(path,corrupted);
            assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(path,f.epochs));
            Files.write(path,Arrays.copyOf(valid,32)); assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(path,f.epochs));
            Files.write(path,valid); try (var issuer=RatisGrantIssuer.reopen(path,f.epochs)) { assertEquals(RatisGrantIssuer.Phase.ISSUED,issuer.phase()); }
            assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(root.resolve("absent.bin"),f.epochs));
            assertEquals(0,f.closes.get());
        }
    }

    @Test void activeRpcKeepsOwnershipAndNeverExposesAnUnpersistedIssuance() throws Exception {
        var outer=new RatisSessionEpochsTest(); Path path=root.resolve("active.bin");
        try (var f=outer.new Fixture(root.resolve("active"))) {
            bootstrap(outer,f); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            var epochs=wrapped(outer,f,() -> { entered.countDown(); if (!release.await(5,TimeUnit.SECONDS)) throw new IOException("fixture timeout"); });
            try (var issuer=RatisGrantIssuer.create(path,epochs,100); var executor=Executors.newSingleThreadExecutor()) {
                var pending=executor.submit(issuer::step);
                try {
                    assertTrue(entered.await(5,TimeUnit.SECONDS)); assertEquals(RatisGrantIssuer.Phase.PENDING,issuer.phase()); assertNull(issuer.issued());
                    assertThrows(IOException.class,issuer::close); assertThrows(IllegalStateException.class,issuer::step);
                    assertThrows(IOException.class,() -> RatisGrantIssuer.reopen(path,f.epochs));
                } finally { release.countDown(); }
                assertEquals(RatisGrantIssuer.Phase.ISSUED,pending.get(5,TimeUnit.SECONDS)); assertNotNull(issuer.issued());
            }
        }
    }
    @FunctionalInterface private interface Gate { void run() throws Exception; }
    private static RatisSessionEpochs wrapped(RatisSessionEpochsTest outer,RatisSessionEpochsTest.Fixture f,Gate gate) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args) -> {
            if (method.getName().equals("send")) gate.run();
            try { return method.invoke(f.client.io(),args); } catch (InvocationTargetException wrapped) { throw wrapped.getCause(); }
        });
        var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args) -> switch(method.getName()) {
            case "getGroupId" -> f.client.getGroupId(); case "io" -> io; default -> throw new AssertionError(method);
        });
        return new RatisSessionEpochs(client,outer.cluster,outer.database,outer.catalogue,1,new byte[32],outer.sessions);
    }
}
