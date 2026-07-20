package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionPointReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteSubmitter;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterBoundary;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterClientResolver;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterFactory;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;

import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class RatisAdapterClientResolverTest {

    @Test
    void resolvesWriteAndReadClientsByRoutePartition() {
        var descriptor = descriptor("7");
        var factory = new RecordingAdapterFactory();
        var target = target(new ShardId(7));

        try (var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor))) {
            var writeClient = ((PartitionWriteSubmitter.LogClientResolver) resolver).resolve(target);
            var pointReadClient = ((PartitionPointReadSubmitter.RatisClientResolver) resolver).resolve(target);
            var rangeReadClient = ((PartitionRangeReadSubmitter.RatisClientResolver) resolver).resolve(target);

            assertEquals(List.of(descriptor), factory.openedDescriptors);
            assertSame(writeClient, pointReadClient);
            assertSame(writeClient, rangeReadClient);
        }

        assertTrue(factory.openedClients.getFirst().closed);
        assertTrue(factory.closed);
    }

    @Test
    void rejectsMissingRoutePartition() {
        try (var resolver = new RatisAdapterClientResolver(new RecordingAdapterFactory(), List.of(descriptor("7")))) {
            assertThrows(IllegalArgumentException.class, () -> resolver.resolve(target(new ShardId(8))));
        }
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var descriptor = descriptor("7");
        var partitionsWithNullEntry = new ArrayList<RatisPartitionDescriptor>();
        partitionsWithNullEntry.add(descriptor);
        partitionsWithNullEntry.add(null);
        var nullFactory = assertRejectedResolverConstruction(null, List.of(descriptor));
        var nullLaunchPlan = assertThrows(IllegalArgumentException.class,
                () -> RatisAdapterClientResolver.fromLaunchPlan(new RecordingAdapterFactory(), null));
        var nullPartitions = assertRejectedResolverConstruction(new RecordingAdapterFactory(), null);
        var nullPartitionEntry = assertRejectedResolverConstruction(new RecordingAdapterFactory(), partitionsWithNullEntry);
        IllegalArgumentException nullTarget;
        IllegalArgumentException nullRefreshPartitions;
        try (var resolver = new RatisAdapterClientResolver(new RecordingAdapterFactory(), List.of(descriptor))) {
            nullTarget = assertThrows(IllegalArgumentException.class, () -> resolver.resolve(null));
            nullRefreshPartitions = assertThrows(IllegalArgumentException.class,
                    () -> resolver.updatePartitions(null));
        }

        assertEquals("adapterFactory must not be null", nullFactory.getMessage());
        assertEquals("launchPlan must not be null", nullLaunchPlan.getMessage());
        assertEquals("partitions must not be null", nullPartitions.getMessage());
        assertEquals("partitions entry must not be null", nullPartitionEntry.getMessage());
        assertEquals("target must not be null", nullTarget.getMessage());
        assertEquals("partitions must not be null", nullRefreshPartitions.getMessage());
    }

    @Test
    void closesAlreadyOpenedClientWhenDuplicateDescriptorIsRejected() {
        try (var factory = new RecordingAdapterFactory()) {

            assertThrows(IllegalArgumentException.class, () ->
                    new RatisAdapterClientResolver(factory, List.of(descriptor("7"), descriptor("7"))));

            assertEquals(0, factory.openedClients.size());
            assertTrue(factory.closed);
        }
    }

    @Test
    void constructorClosesStagedClientsAndFactoryWhenOpenThrowsError() {
        try (var factory = new RecordingAdapterFactory()) {
            factory.errorOpeningPartitionId = "8";

            var failure = assertThrows(AssertionError.class, () ->
                    new RatisAdapterClientResolver(factory, List.of(descriptor("7"), descriptor("8"))));

            assertEquals("open error for 8", failure.getMessage());
            assertTrue(factory.openedClients.getFirst().closed);
            assertTrue(factory.closed);
        }
    }

    @Test
    void constructorRetriesTransientStagedClientCleanupFailureBeforeDiscardingResolver() {
        try (var factory = new RecordingAdapterFactory()) {
            factory.nextCloseFailureOnce = new IllegalStateException("cleanup failed");
            factory.failOpeningPartitionId = "8";

            var failure = assertThrows(IllegalStateException.class, () ->
                    new RatisAdapterClientResolver(factory, List.of(descriptor("7"), descriptor("8"))));

            assertEquals("open failed for 8", failure.getMessage());
            assertEquals(1, failure.getSuppressed().length);
            assertEquals("cleanup failed", failure.getSuppressed()[0].getMessage());
            assertEquals(2, factory.openedClients.getFirst().closeCalls);
            assertTrue(factory.openedClients.getFirst().closed);
            assertEquals(1, factory.closeCalls);
            assertTrue(factory.closed);
        }
    }

    @Test
    void refreshKeepsUnchangedClientsAndOpensNewPartitions() {
        var firstDescriptor = descriptor("7");
        var secondDescriptor = descriptor("8");
        var factory = new RecordingAdapterFactory();
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(firstDescriptor))) {
            var firstClient = resolver.resolve(target(new ShardId(7)));

            resolver.updatePartitions(List.of(firstDescriptor, secondDescriptor));

            assertSame(firstClient, resolver.resolve(target(new ShardId(7))));
            assertSame(factory.openedClients.get(1), resolver.resolve(target(new ShardId(8))));
            assertEquals(List.of(firstDescriptor, secondDescriptor), factory.openedDescriptors);
            assertFalse(factory.openedClients.getFirst().closed);
            assertFalse(factory.openedClients.get(1).closed);
        }
    }

    @Test
    void refreshClosesRemovedPartitions() {
        var firstDescriptor = descriptor("7");
        var secondDescriptor = descriptor("8");
        var factory = new RecordingAdapterFactory();
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(firstDescriptor))) {

            resolver.updatePartitions(List.of(secondDescriptor));

            assertThrows(IllegalArgumentException.class, () -> resolver.resolve(target(new ShardId(7))));
            assertSame(factory.openedClients.get(1), resolver.resolve(target(new ShardId(8))));
            assertTrue(factory.openedClients.getFirst().closed);
            assertFalse(factory.openedClients.get(1).closed);
        }
    }

    @Test
    void refreshReopensChangedPartitionDescriptor() {
        var initialDescriptor = descriptor("7", "group-7");
        var changedDescriptor = descriptor("7", "group-7b");
        var factory = new RecordingAdapterFactory();
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(initialDescriptor))) {

            resolver.updatePartitions(List.of(changedDescriptor));

            assertSame(factory.openedClients.get(1), resolver.resolve(target(new ShardId(7))));
            assertTrue(factory.openedClients.getFirst().closed);
            assertFalse(factory.openedClients.get(1).closed);
            assertEquals(List.of(initialDescriptor, changedDescriptor), factory.openedDescriptors);
        }
    }

    @Test
    void failedRefreshLeavesExistingClientsUsable() {
        var descriptor = descriptor("7");
        var factory = new RecordingAdapterFactory();
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor))) {
            var client = resolver.resolve(target(new ShardId(7)));

            assertThrows(IllegalArgumentException.class, () ->
                    resolver.updatePartitions(List.of(descriptor("8"), descriptor("8"))));

            assertSame(client, resolver.resolve(target(new ShardId(7))));
            assertFalse(factory.openedClients.getFirst().closed);
            assertEquals(List.of(descriptor), factory.openedDescriptors);
        }
    }

    @Test
    void failedRefreshPreservesOpenFailureWhenNewClientCleanupFails() {
        var descriptor = descriptor("7");
        var newDescriptor = descriptor("8");
        var failingDescriptor = descriptor("9");
        var factory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor));
        var existingClient = resolver.resolve(target(new ShardId(7)));
        factory.nextCloseFailure = new IllegalStateException("cleanup failed");
        factory.failOpeningPartitionId = "9";

        var failure = assertThrows(IllegalStateException.class, () ->
                resolver.updatePartitions(List.of(descriptor, newDescriptor, failingDescriptor)));

        assertEquals("open failed for 9", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("cleanup failed", failure.getSuppressed()[0].getMessage());
        assertSame(existingClient, resolver.resolve(target(new ShardId(7))));
        assertFalse(factory.openedClients.getFirst().closed);
        assertTrue(factory.openedClients.get(1).closed);
        assertEquals(List.of(descriptor, newDescriptor, failingDescriptor), factory.openedDescriptors);

        factory.openedClients.get(1).closeFailure = null;
        resolver.close();
        assertEquals(2, factory.openedClients.get(1).closeCalls);
    }

    @Test
    void failedRefreshPreservesOpenErrorWhenNewClientCleanupThrowsError() {
        var descriptor = descriptor("7");
        var newDescriptor = descriptor("8");
        var failingDescriptor = descriptor("9");
        var factory = new RecordingAdapterFactory();
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor));
        var existingClient = resolver.resolve(target(new ShardId(7)));
        factory.nextCloseFailure = new AssertionError("cleanup error");
        factory.errorOpeningPartitionId = "9";

        var failure = assertThrows(AssertionError.class, () ->
                resolver.updatePartitions(List.of(descriptor, newDescriptor, failingDescriptor)));

        assertEquals("open error for 9", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("cleanup error", failure.getSuppressed()[0].getMessage());
        assertSame(existingClient, resolver.resolve(target(new ShardId(7))));
        assertFalse(factory.openedClients.getFirst().closed);
        assertTrue(factory.openedClients.get(1).closed);
        assertEquals(List.of(descriptor, newDescriptor, failingDescriptor), factory.openedDescriptors);
    }

    @Test
    void refreshKeepsPublishedClientsUsableWhenStaleClientCleanupFails() {
        var firstDescriptor = descriptor("7");
        var secondDescriptor = descriptor("8");
        var factory = new RecordingAdapterFactory();
        factory.nextCloseFailure = new IllegalStateException("cleanup failed");
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(firstDescriptor))) {

            resolver.updatePartitions(List.of(secondDescriptor));

            assertThrows(IllegalArgumentException.class, () -> resolver.resolve(target(new ShardId(7))));
            assertSame(factory.openedClients.get(1), resolver.resolve(target(new ShardId(8))));
            assertTrue(factory.openedClients.getFirst().closed);
            assertFalse(factory.openedClients.get(1).closed);

            factory.openedClients.getFirst().closeFailure = null;
            resolver.close();
            assertEquals(2, factory.openedClients.getFirst().closeCalls);
        }
    }

    @Test
    void refreshKeepsPublishedClientsUsableWhenStaleClientCleanupThrowsError() {
        var firstDescriptor = descriptor("7");
        var secondDescriptor = descriptor("8");
        var factory = new RecordingAdapterFactory();
        factory.nextCloseFailure = new AssertionError("cleanup error");
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(firstDescriptor))) {

            resolver.updatePartitions(List.of(secondDescriptor));

            assertThrows(IllegalArgumentException.class, () -> resolver.resolve(target(new ShardId(7))));
            assertSame(factory.openedClients.get(1), resolver.resolve(target(new ShardId(8))));
            assertTrue(factory.openedClients.getFirst().closed);
            assertFalse(factory.openedClients.get(1).closed);

            factory.openedClients.getFirst().closeFailure = null;
            resolver.close();
            assertEquals(2, factory.openedClients.getFirst().closeCalls);
        }
    }

    @Test
    void failedRefreshClosesAlreadyOpenedNewClientsWhenFactoryReturnsNull() {
        var descriptor = descriptor("7");
        var newDescriptor = descriptor("8");
        var nullDescriptor = descriptor("9");
        var factory = new RecordingAdapterFactory();
        try (var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor))) {
            var existingClient = resolver.resolve(target(new ShardId(7)));
            factory.returnNullOpeningPartitionId = "9";

            var failure = assertThrows(IllegalStateException.class, () ->
                    resolver.updatePartitions(List.of(descriptor, newDescriptor, nullDescriptor)));

            assertEquals("adapterFactory.openPartition returned null for partition 9", failure.getMessage());
            assertSame(existingClient, resolver.resolve(target(new ShardId(7))));
            assertThrows(IllegalArgumentException.class, () -> resolver.resolve(target(new ShardId(8))));
            assertFalse(factory.openedClients.getFirst().closed);
            assertTrue(factory.openedClients.get(1).closed);
            assertEquals(List.of(descriptor, newDescriptor, nullDescriptor), factory.openedDescriptors);
        }
    }

    @Test
    void closeStillClosesFactoryWhenPartitionClientCleanupFails() {
        var factory = new RecordingAdapterFactory();
        factory.nextCloseFailure = new IllegalStateException("cleanup failed");
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor("7")));

        var failure = assertThrows(IllegalStateException.class, resolver::close);

        assertEquals("cleanup failed", failure.getMessage());
        assertTrue(factory.openedClients.getFirst().closed);
        assertTrue(factory.closed);
    }

    @Test
    void closeStillClosesRemainingClientsAndFactoryWhenPartitionClientCleanupThrowsError() {
        var factory = new RecordingAdapterFactory();
        factory.nextCloseFailure = new AssertionError("cleanup error");
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor("7"), descriptor("8")));

        var failure = assertThrows(AssertionError.class, resolver::close);

        assertEquals("cleanup error", failure.getMessage());
        assertTrue(factory.openedClients.getFirst().closed);
        assertTrue(factory.openedClients.get(1).closed);
        assertTrue(factory.closed);
    }

    @Test
    void closeRetriesPartitionClientCleanupFailure() {
        var factory = new RecordingAdapterFactory();
        factory.nextCloseFailure = new IllegalStateException("cleanup failed");
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor("7")));

        assertThrows(IllegalStateException.class, resolver::close);

        var client = factory.openedClients.getFirst();
        assertEquals(1, client.closeCalls);
        assertEquals(1, factory.closeCalls);

        client.closeFailure = null;
        resolver.close();

        assertEquals(2, client.closeCalls);
        assertEquals(1, factory.closeCalls);
    }

    @Test
    void closeRetriesFactoryCleanupFailure() {
        var factory = new RecordingAdapterFactory();
        factory.closeFailure = new IllegalStateException("factory cleanup failed");
        var resolver = new RatisAdapterClientResolver(factory, List.of(descriptor("7")));

        assertThrows(IllegalStateException.class, resolver::close);

        assertEquals(1, factory.openedClients.getFirst().closeCalls);
        assertEquals(1, factory.closeCalls);

        factory.closeFailure = null;
        resolver.close();

        assertEquals(1, factory.openedClients.getFirst().closeCalls);
        assertEquals(2, factory.closeCalls);
    }

    private static RouteTarget target(ShardId shardId) {
        return new RouteTarget(
                shardId,
                new ReplicaMember(
                        NodeId.of("n1"),
                        URI.create("grpc://127.0.0.1:19001"),
                        ReplicaRole.VOTER,
                        0,
                        0,
                        true
                ),
                true,
                ReplicatedCommandKind.REPLICATED_WRITE,
                1
        );
    }

    private static IllegalArgumentException assertRejectedResolverConstruction(
            RatisAdapterFactory factory,
            List<RatisPartitionDescriptor> partitions) {
        return assertThrows(IllegalArgumentException.class, () -> {
            try (var ignored = new RatisAdapterClientResolver(factory, partitions)) {
                fail("resolver construction unexpectedly succeeded");
            }
        });
    }

    private static RatisPartitionDescriptor descriptor(String partitionId) {
        return descriptor(partitionId, "group-" + partitionId);
    }

    private static RatisPartitionDescriptor descriptor(String partitionId, String groupId) {
        return new RatisPartitionDescriptor(
                partitionId,
                groupId,
                "n1",
                List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:19001", "rack-a")),
                Path.of("/tmp/rockserver-partition-" + partitionId)
        );
    }

    private static final class RecordingAdapterFactory implements RatisAdapterFactory {

        private final ArrayList<RatisPartitionDescriptor> openedDescriptors = new ArrayList<>();
        private final ArrayList<RecordingPartitionClient> openedClients = new ArrayList<>();
        private Throwable nextCloseFailure;
        private Throwable nextCloseFailureOnce;
        private String failOpeningPartitionId;
        private String errorOpeningPartitionId;
        private String returnNullOpeningPartitionId;
        private Throwable closeFailure;
        private int closeCalls;
        private boolean closed;

        @Override
        public RatisAdapterBoundary boundary() {
            return RatisAdapterBoundary.externalProcess("test");
        }

        @Override
        public RatisPartitionClient openPartition(RatisPartitionDescriptor descriptor) {
            openedDescriptors.add(descriptor);
            if (descriptor.partitionId().equals(failOpeningPartitionId)) {
                throw new IllegalStateException("open failed for " + descriptor.partitionId());
            }
            if (descriptor.partitionId().equals(errorOpeningPartitionId)) {
                throw new AssertionError("open error for " + descriptor.partitionId());
            }
            if (descriptor.partitionId().equals(returnNullOpeningPartitionId)) {
                return null;
            }
            var client = new RecordingPartitionClient(descriptor, nextCloseFailure, nextCloseFailureOnce);
            nextCloseFailure = null;
            nextCloseFailureOnce = null;
            openedClients.add(client);
            return client;
        }

        @Override
        public void close() {
            closeCalls++;
            closed = true;
            if (closeFailure != null) {
                throwUnchecked(closeFailure);
            }
        }

        private static void throwUnchecked(Throwable failure) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("Unexpected checked test cleanup failure", failure);
        }
    }

    private static final class RecordingPartitionClient implements RatisPartitionClient {

        private final RatisPartitionDescriptor descriptor;
        private Throwable closeFailure;
        private Throwable closeFailureOnce;
        private int closeCalls;
        private boolean closed;

        private RecordingPartitionClient(RatisPartitionDescriptor descriptor,
                                         Throwable closeFailure,
                                         Throwable closeFailureOnce) {
            this.descriptor = descriptor;
            this.closeFailure = closeFailure;
            this.closeFailureOnce = closeFailureOnce;
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return descriptor;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(1, true));
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return CompletableFuture.completedFuture(request);
        }

        @Override
        public void close() {
            closeCalls++;
            closed = true;
            if (closeFailureOnce != null) {
                var failure = closeFailureOnce;
                closeFailureOnce = null;
                throwUnchecked(failure);
            }
            if (closeFailure != null) {
                throwUnchecked(closeFailure);
            }
        }

        private static void throwUnchecked(Throwable failure) {
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
            throw new AssertionError("Unexpected checked test cleanup failure", failure);
        }
    }
}
