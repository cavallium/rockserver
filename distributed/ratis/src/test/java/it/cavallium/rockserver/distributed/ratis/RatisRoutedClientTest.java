package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Client routing/ownership boundaries; actual replicated catalogue and data acceptance are separate tests. */
class RatisRoutedClientTest {
    private static final UUID CLUSTER = new UUID(1, 1), DB = new UUID(1, 2), DOMAIN = new UUID(1, 3);
    private static final UUID A = new UUID(2, 1), B = new UUID(2, 2);
    private static final OperationId OPERATION = new OperationId(new UUID(3, 1), 4, 5);
    private final CommandResult success = new CommandResult(CommandResult.Status.SUCCESS, List.of());

    @Test
    void releasedGrpcStatusFailureBecomesIOExceptionWithoutHidingArgumentErrors() throws Exception {
        var status = org.apache.ratis.thirdparty.io.grpc.Status.UNAVAILABLE.withDescription("peer stopped").asRuntimeException();
        IOException normalized = assertThrows(IOException.class, () -> RatisRoutedClient.rpc(() -> { throw status; }));
        assertSame(status, normalized.getCause());
        var argument = new IllegalArgumentException("invalid descriptor");
        assertSame(argument, assertThrows(IllegalArgumentException.class,
                () -> RatisRoutedClient.rpc(() -> { throw argument; })));
        assertEquals("success", RatisRoutedClient.rpc(() -> "success"));
        // A resource close failure is part of the same transport boundary after an otherwise valid call.
        assertThrows(IOException.class, () -> RatisRoutedClient.rpc(() -> {
            try (AutoCloseable resource = () -> { throw status; }) { return "reply"; }
            catch (IOException | RuntimeException failure) { throw failure; }
            catch (Exception unexpected) { throw new AssertionError(unexpected); }
        }));
    }

    private byte[] key(int n) { return new byte[]{(byte) n}; }
    private RangeDescriptor range(int n, byte[] lower, byte[] upper) {
        var voter = new ReplicaDescriptor(new UUID(4, 1), new UUID(5, 1), new UUID(6, n), "n1", "127.0.0.1:8001", ReplicaDescriptor.Role.VOTER);
        return new RangeDescriptor(new UUID(7, n), new UUID(8, n), 9, 10, lower, upper, RangeDescriptor.TopologyState.ACTIVE, List.of(voter));
    }
    private RangeDirectory directory(long generation, int firstId) {
        return new RangeDirectory(CLUSTER, DB, DOMAIN, 11, new byte[32], generation, Set.of(A, B), 0,
                List.of(range(firstId, null, key(0x80)), range(firstId + 1, key(0x80), null)));
    }
    private Mutation put(UUID column, int key) { return new Mutation(Mutation.Kind.PUT, column, key(key), new byte[]{1}); }

    @Test
    void pointReadsAndAtomicSameRangeCommandsKeepExactIdentityAndOperandOrder() throws Exception {
        var transport = new RecordingTransport();
        try (var client = new RatisRoutedClient(directory(1, 1), transport)) {
            byte[] query = key(1);
            assertArrayEquals(new byte[]{7}, client.read(A, query));
            assertArrayEquals(key(1), query);
            assertEquals(new UUID(8, 1), transport.lastDescriptor.groupId());
            var mutations = List.of(put(A, 1), new Mutation(Mutation.Kind.MERGE, A, key(1), key(2)), put(B, 2));
            var predicates = List.of(new KeyPredicate(A, key(1), null));
            var prepared = client.prepare(OPERATION, mutations, predicates);
            assertEquals(OPERATION, prepared.command().operationId());
            assertEquals(new UUID(8, 1), prepared.command().groupId());
            assertEquals(9, prepared.command().rangeEpoch());
            assertEquals(mutations, prepared.command().mutations());
            assertEquals(predicates, prepared.command().predicates());
            assertSame(success, client.execute(prepared));
            assertSame(success, client.execute(prepared));
            assertEquals(2, transport.commands.size());
            assertSame(prepared.command(), transport.commands.get(0));
            assertSame(transport.commands.get(0), transport.commands.get(1));
        }
        assertEquals(1, transport.closes.get());
    }

    @Test
    void refreshChangesFreshRoutingButNeverRebindsPreparedRetry() throws Exception {
        var transport = new RecordingTransport();
        try (var client = new RatisRoutedClient(directory(1, 1), transport)) {
            var prepared = client.prepare(OPERATION, List.of(put(A, 1)));
            byte[] original = DistributedCommandCodec.encode(prepared.command());
            client.refresh(directory(2, 3));
            client.read(A, key(1));
            assertEquals(new UUID(8, 3), transport.lastDescriptor.groupId());
            client.execute(prepared);
            assertEquals(new UUID(8, 1), transport.lastDescriptor.groupId());
            assertArrayEquals(original, DistributedCommandCodec.encode(transport.commands.getFirst()));
            assertEquals(2, client.directory().generation());
        }
    }

    @Test
    void refreshRejectsForeignSchemaStaleAndEqualGenerationConflicts() throws Exception {
        var initial = directory(1, 1);
        try (var client = new RatisRoutedClient(initial, new RecordingTransport())) {
            client.refresh(RangeDirectoryCodec.decode(RangeDirectoryCodec.encode(initial)));
            assertThrows(IllegalArgumentException.class, () -> client.refresh(directory(0, 1)));
            assertThrows(IllegalArgumentException.class, () -> client.refresh(directory(1, 3)));
            for (int variant = 0; variant < 4; variant++) {
                byte[] schema = new byte[32]; if (variant == 3) schema[0] = 1;
                var changed = new RangeDirectory(variant == 0 ? UUID.randomUUID() : CLUSTER,
                        variant == 1 ? UUID.randomUUID() : DB, variant == 2 ? UUID.randomUUID() : DOMAIN,
                        11, schema, 2, Set.of(A, B), 0, initial.ranges());
                assertThrows(IllegalArgumentException.class, () -> client.refresh(changed));
            }
            var changedEpoch = new RangeDirectory(CLUSTER, DB, DOMAIN, 12, new byte[32], 2, Set.of(A, B), 0, initial.ranges());
            assertThrows(IllegalArgumentException.class, () -> client.refresh(changedEpoch));
        }
        var foreign = new RangeDirectory(UUID.randomUUID(), DB, DOMAIN, 11, new byte[32], 1, Set.of(A, B), 0, initial.ranges());
        try (var one = new RatisRoutedClient(initial, new RecordingTransport());
             var other = new RatisRoutedClient(foreign, new RecordingTransport())) {
            var prepared = one.prepare(OPERATION, List.of(put(A, 1)));
            assertThrows(IllegalArgumentException.class, () -> other.execute(prepared));
        }
    }

    @Test
    void validatesPredicatesAndWholeDeleteRangeBeforeAnyTransportEffects() throws Exception {
        var transport = new RecordingTransport();
        try (var client = new RatisRoutedClient(directory(1, 1), transport)) {
            assertThrows(IllegalArgumentException.class, () -> client.prepare(OPERATION, List.of(put(A, 1), put(B, 0x80))));
            assertThrows(IllegalArgumentException.class, () -> client.prepare(OPERATION, List.of(put(A, 1)),
                    List.of(new KeyPredicate(B, key(0x80), null))));
            assertThrows(IllegalArgumentException.class, () -> client.prepare(OPERATION,
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE, A, key(1), key(0x81)))));
            assertDoesNotThrow(() -> client.prepare(OPERATION,
                    List.of(new Mutation(Mutation.Kind.DELETE_RANGE, A, key(1), key(0x80)))));
            assertThrows(IllegalArgumentException.class, () -> client.prepare(OPERATION, List.of(put(UUID.randomUUID(), 1))));
            assertThrows(IllegalArgumentException.class, () -> client.prepare(OPERATION, List.of(put(A, 1)),
                    List.of(new KeyPredicate(UUID.randomUUID(), key(1), null))));
            assertTrue(transport.commands.isEmpty());
        }
    }

    @Test
    void transportFailureHasNoAutomaticRetryAndMissingResultIsAnError() throws Exception {
        var attempts = new AtomicInteger();
        var observedCalls = new ArrayList<DistributedCommand>();
        var transport = new RecordingTransport() {
            @Override public CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException {
                observedCalls.add(command);
                if (attempts.incrementAndGet() == 1) throw new IOException("reply lost");
                return null;
            }
        };
        try (var client = new RatisRoutedClient(directory(1, 1), transport)) {
            var prepared = client.prepare(OPERATION, List.of(put(A, 1)));
            assertThrows(IOException.class, () -> client.execute(prepared));
            assertEquals(1, attempts.get());
            assertThrows(IOException.class, () -> client.execute(prepared));
            assertEquals(2, attempts.get());
            assertSame(observedCalls.get(0), observedCalls.get(1));
            assertEquals(OPERATION, observedCalls.getFirst().operationId());
        }
    }

    @Test
    void closeDrainsCallBeforeTransportReleaseWithoutHoldingNetworkLock() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var transport = new RecordingTransport() {
            @Override public byte[] read(RangeDescriptor descriptor, UUID column, byte[] key) throws IOException {
                entered.countDown();
                try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException interrupted) { throw new IOException(interrupted); }
                return key;
            }
        };
        var client = new RatisRoutedClient(directory(1, 1), transport);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var read = executor.submit(() -> client.read(A, key(1)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            var started = new CountDownLatch(1);
            var close = executor.submit(() -> { started.countDown(); client.close(); return null; });
            assertTrue(started.await(5, TimeUnit.SECONDS));
            try {
                // Metadata access remains responsive while close waits for the remote call.
                assertEquals(1, executor.submit(() -> client.directory().generation()).get(1, TimeUnit.SECONDS));
                assertFalse(close.isDone());
                assertEquals(0, transport.closes.get());
            } finally { release.countDown(); }
            assertArrayEquals(key(1), read.get(5, TimeUnit.SECONDS));
            close.get(5, TimeUnit.SECONDS);
        }
        client.close();
        assertEquals(1, transport.closes.get());
        assertThrows(IllegalStateException.class, () -> client.read(A, key(1)));
        assertThrows(IllegalStateException.class, () -> client.prepare(OPERATION, List.of(put(A, 1))));
        assertThrows(IllegalStateException.class, () -> client.refresh(directory(2, 1)));
    }

    @Test
    void failedTransportCloseCanBeRetriedWithoutReopeningAdmission() throws Exception {
        var transport = new RecordingTransport() {
            @Override public void close() throws IOException {
                if (closes.incrementAndGet() == 1) throw new IOException("retained resource");
            }
        };
        var client = new RatisRoutedClient(directory(1, 1), transport);
        assertThrows(IOException.class, client::close);
        assertThrows(IllegalStateException.class, () -> client.read(A, key(1)));
        client.close(); client.close();
        assertEquals(2, transport.closes.get());
    }

    @Test void explicitAcknowledgementPreservesOwnSessionFloorAndOriginalPreparedRoute() throws Exception {
        var transport = new RecordingTransport();
        try (var client = new RatisRoutedClient(directory(1,1),transport)) {
            var ack = client.prepareAcknowledgement(OPERATION,A,key(1),4);
            assertEquals(3,ack.command().formatVersion()); assertEquals(4,ack.command().acknowledgedThrough());
            assertEquals(OPERATION,ack.command().operationId()); assertTrue(ack.command().mutations().isEmpty());
            var original = DistributedCommandCodec.encode(ack.command());
            client.refresh(directory(2,3)); client.execute(ack);
            assertEquals(new UUID(8,1),transport.lastDescriptor.groupId());
            assertArrayEquals(original,DistributedCommandCodec.encode(transport.commands.getFirst()));
            assertEquals(1,transport.commands.size());
            var piggyback = client.prepare(OPERATION,List.of(put(A,1)),List.of(),4);
            assertEquals(new UUID(8,3),piggyback.command().groupId());
            assertEquals(4,piggyback.command().acknowledgedThrough());
            assertEquals(2,client.prepare(OPERATION,List.of(put(A,1))).command().formatVersion());
            assertThrows(IllegalArgumentException.class,() -> client.prepareAcknowledgement(OPERATION,A,key(1),5));
            assertThrows(IllegalArgumentException.class,() -> client.prepareAcknowledgement(OPERATION,UUID.randomUUID(),key(1),4));
            assertThrows(IllegalArgumentException.class,() -> client.prepare(OPERATION,List.of(put(A,1),put(B,0x80)),List.of(),4));
        }
    }

    private class RecordingTransport implements RatisRoutedClient.Transport {
        final List<DistributedCommand> commands = new ArrayList<>();
        final AtomicInteger closes = new AtomicInteger();
        RangeDescriptor lastDescriptor;
        @Override public byte[] read(RangeDescriptor descriptor, UUID column, byte[] key) throws IOException {
            lastDescriptor = descriptor; key[0] = 99; return new byte[]{7};
        }
        @Override public CommandResult command(RangeDescriptor descriptor, DistributedCommand command) throws IOException {
            lastDescriptor = descriptor; commands.add(command); return success;
        }
        @Override public void close() throws IOException { closes.incrementAndGet(); }
    }
}
