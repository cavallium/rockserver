package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteSubmitter;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class PartitionWriteSubmitterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId PARTITION = new ShardId(7);
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var resolver = new RecordingResolver();
        var cache = new PartitionRouteCache(metadata(7, N1));

        assertIllegalArgument("routeCache must not be null",
                () -> new PartitionWriteSubmitter(null, resolver, 1, () -> 0));
        assertIllegalArgument("logClientResolver must not be null",
                () -> new PartitionWriteSubmitter(cache, null, 1, () -> 0));
        assertIllegalArgument("currentTimeMillis must not be null",
                () -> new PartitionWriteSubmitter(cache, resolver, 1, null));

        var submitter = new PartitionWriteSubmitter(cache, resolver, 1, () -> 0);
        assertIllegalArgument("command must not be null", () -> submitter.submit(null, 7, 1000));
    }

    @Test
    void submitsStablePayloadToOwningLeader() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.completedFuture(new ReplicatedLogCommit(11, true)));
        var submitter = submitter(metadata(7, N1), resolver, 2, 0);
        var command = command();

        var commit = submitter.submit(command, 7, 1000);

        assertEquals(11, commit.index());
        assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
        var proposedCommand = PartitionWriteCommandCodec.decode(resolver.payloads.getFirst());
        assertEquals(command.requestId(), proposedCommand.requestId());
        assertNotNull(proposedCommand.seal());
        assertEquals(PARTITION, proposedCommand.seal().partitionId());
        assertEquals(7, proposedCommand.seal().metadataEpoch());
        assertEquals(buf(0x18), proposedCommand.mutations().getFirst().requiredCalculatedKey());
    }

    @Test
    void notLeaderRefreshesMetadataAndRetriesSamePayload() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.failedFuture(PartitionWriteRouteException.notLeader(metadata(8, N2))));
        resolver.responses.add(CompletableFuture.completedFuture(new ReplicatedLogCommit(12, true)));
        var submitter = submitter(metadata(7, N1), resolver, 3, 0);
        var command = command();

        var commit = submitter.submit(command, 7, 1000);

        assertEquals(12, commit.index());
        assertEquals(List.of(N1, N2), resolver.targets.stream().map(target -> target.replica().nodeId()).toList());
        assertArrayEquals(resolver.payloads.getFirst(), resolver.payloads.get(1));
        assertEquals(requestId(), command.requestId());
    }

    @Test
    void synchronousNotLeaderProposalFailureRefreshesMetadataAndRetriesSamePayload() {
        var resolver = new RecordingResolver();
        resolver.proposalFailures.add(PartitionWriteRouteException.notLeader(metadata(8, N2)));
        resolver.responses.add(CompletableFuture.completedFuture(new ReplicatedLogCommit(14, true)));
        var submitter = submitter(metadata(7, N1), resolver, 3, 0);
        var command = command();

        var commit = submitter.submit(command, 7, 1000);

        assertEquals(14, commit.index());
        assertEquals(List.of(N1, N2), resolver.targets.stream().map(target -> target.replica().nodeId()).toList());
        assertArrayEquals(resolver.payloads.getFirst(), resolver.payloads.get(1));
        assertEquals(requestId(), command.requestId());
    }

    @Test
    void rejectedNotLeaderRefreshMetadataStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.failedFuture(PartitionWriteRouteException.notLeader(metadata(6, N2))));
        var submitter = submitter(metadata(7, N1), resolver, 3, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, error.kind());
        assertTrue(error.getMessage().contains("not-leader refreshed metadata was rejected"));
        assertInstanceOf(IllegalArgumentException.class, error.getCause());
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.payloads.size());
    }

    @Test
    void staleExpectedEpochRetriesAgainstCurrentRouteCacheEpoch() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.completedFuture(new ReplicatedLogCommit(13, true)));
        var submitter = submitter(metadata(7, N1), resolver, 2, 0);

        var commit = submitter.submit(command(), 6, 1000);

        assertEquals(13, commit.index());
        assertEquals(1, resolver.targets.size());
        assertEquals(7, resolver.targets.getFirst().metadataEpoch());
    }

    @Test
    void futureExpectedEpochFailsBeforeSealingOrResolvingRoute() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 2, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 8, 1000));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertTrue(error.getMessage().contains("future route metadata epoch 8; current epoch is 7"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.payloads.size());
    }

    @Test
    void noKnownLeaderFailsBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, null), resolver, 2, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.ROUTE_FAILED, error.kind());
        assertEquals("write route failed: no known leader for shard " + PARTITION, error.getMessage());
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.payloads.size());
    }

    @Test
    void timeoutIsReportedWithoutChangingPayloadSemantics() {
        var resolver = new RecordingResolver();
        var response = new CompletableFuture<ReplicatedLogCommit>();
        resolver.responses.add(response);
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1));

        assertEquals(PartitionWriteFailureKind.TIMEOUT, error.kind());
        assertTrue(response.isCancelled());
        assertEquals(1, resolver.payloads.size());
        assertNotNull(PartitionWriteCommandCodec.decode(resolver.payloads.getFirst()).seal());
    }

    @Test
    void timeoutRetainsProposalCancellationFailureAsSuppressedDiagnostic() {
        var resolver = new RecordingResolver();
        var cancellationFailure = new AssertionError("cancellation failed");
        var response = new CompletableFuture<ReplicatedLogCommit>() {
            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                super.cancel(mayInterruptIfRunning);
                throw cancellationFailure;
            }
        };
        resolver.responses.add(response);
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1));

        assertEquals(PartitionWriteFailureKind.TIMEOUT, error.kind());
        assertTrue(response.isCancelled());
        assertSame(cancellationFailure, error.getCause().getSuppressed()[0]);
    }

    @Test
    void interruptionCancelsProposalAndRestoresInterruptFlag() {
        var resolver = new RecordingResolver();
        var response = new CompletableFuture<ReplicatedLogCommit>();
        resolver.responses.add(response);
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        Thread.currentThread().interrupt();
        try {
            var error = assertThrows(PartitionWriteRouteException.class,
                    () -> submitter.submit(command(), 7, 1000));

            assertEquals(PartitionWriteFailureKind.TIMEOUT, error.kind());
            assertTrue(response.isCancelled());
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void retryableFailuresStopAtRetryBudgetWithoutChangingPayloadSemantics() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.failedFuture(new PartitionWriteRouteException(
                PartitionWriteFailureKind.TIMEOUT,
                "first timeout"
        )));
        resolver.responses.add(CompletableFuture.failedFuture(new PartitionWriteRouteException(
                PartitionWriteFailureKind.TIMEOUT,
                "second timeout"
        )));
        var submitter = submitter(metadata(7, N1), resolver, 2, 0);
        var command = command();

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command, 7, 1000));

        assertEquals(PartitionWriteFailureKind.TIMEOUT, error.kind());
        assertEquals(2, resolver.targets.size());
        assertEquals(List.of(N1, N1), resolver.targets.stream().map(target -> target.replica().nodeId()).toList());
        assertArrayEquals(resolver.payloads.getFirst(), resolver.payloads.get(1));
        assertEquals(requestId(), command.requestId());
    }

    @Test
    void runtimeProposalFailuresStayTyped() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new IllegalStateException("async proposal failed");
        asyncResolver.responses.add(CompletableFuture.failedFuture(asyncFailure));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 1, 0);

        var asyncError = assertThrows(PartitionWriteRouteException.class,
                () -> asyncSubmitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, asyncError.kind());
        assertEquals("replicated write proposal failed", asyncError.getMessage());
        assertSame(asyncFailure, asyncError.getCause());
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.payloads.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new IllegalStateException("sync proposal failed");
        syncResolver.proposalFailures.add(syncFailure);
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 1, 0);

        var syncError = assertThrows(PartitionWriteRouteException.class,
                () -> syncSubmitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, syncError.kind());
        assertEquals("replicated write proposal failed", syncError.getMessage());
        assertSame(syncFailure, syncError.getCause());
        assertEquals(1, syncResolver.targets.size());
        assertEquals(1, syncResolver.payloads.size());
    }

    @Test
    void fatalProposalFailureRetainsSeverity() {
        var resolver = new RecordingResolver();
        var fatalFailure = new AssertionError("fatal proposal failed");
        resolver.responses.add(CompletableFuture.failedFuture(fatalFailure));
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(AssertionError.class,
                () -> submitter.submit(command(), 7, 1000));

        assertSame(fatalFailure, error);
        assertEquals(1, resolver.targets.size());
    }

    @Test
    void wrappedFatalProposalFailuresRetainSeverity() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new AssertionError("wrapped async fatal proposal failed");
        asyncResolver.responses.add(CompletableFuture.failedFuture(new CompletionException(asyncFailure)));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 1, 0);

        var asyncError = assertThrows(AssertionError.class,
                () -> asyncSubmitter.submit(command(), 7, 1000));

        assertSame(asyncFailure, asyncError);
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.payloads.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new AssertionError("wrapped sync fatal proposal failed");
        syncResolver.proposalFailures.add(new CompletionException(syncFailure));
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 1, 0);

        var syncError = assertThrows(AssertionError.class,
                () -> syncSubmitter.submit(command(), 7, 1000));

        assertSame(syncFailure, syncError);
        assertEquals(1, syncResolver.targets.size());
        assertEquals(1, syncResolver.payloads.size());
    }

    @Test
    void adapterRpcProposalFailuresStayTyped() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new RatisProcessAdapterRpcException("ADAPTER_ERROR", "adapter proposal failed");
        asyncResolver.responses.add(CompletableFuture.failedFuture(asyncFailure));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 1, 0);

        var asyncError = assertThrows(PartitionWriteRouteException.class,
                () -> asyncSubmitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, asyncError.kind());
        assertSame(asyncFailure, asyncError.getCause());
        assertTrue(asyncError.getMessage().contains(
                "replicated write proposal failed with adapter error ADAPTER_ERROR: adapter proposal failed"));
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.payloads.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new RatisProcessAdapterRpcException("UNSUPPORTED", "proposal disabled");
        syncResolver.proposalFailures.add(syncFailure);
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 1, 0);

        var syncError = assertThrows(PartitionWriteRouteException.class,
                () -> syncSubmitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, syncError.kind());
        assertSame(syncFailure, syncError.getCause());
        assertTrue(syncError.getMessage().contains(
                "replicated write proposal failed with adapter error UNSUPPORTED: proposal disabled"));
        assertEquals(1, syncResolver.targets.size());
        assertEquals(1, syncResolver.payloads.size());
    }

    @Test
    void nullLogClientStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.returnNullClient = true;
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, error.kind());
        assertTrue(error.getMessage().contains("resolver returned null log client"));
        assertEquals(1, resolver.targets.size());
        assertEquals(0, resolver.payloads.size());
    }

    @Test
    void resolverFailuresStayTyped() {
        var resolver = new RecordingResolver();
        var resolverFailure = new IllegalStateException("resolver failed");
        resolver.resolveFailure = resolverFailure;
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, error.kind());
        assertEquals("replicated write proposal failed", error.getMessage());
        assertSame(resolverFailure, error.getCause());
        assertEquals(1, resolver.targets.size());
        assertEquals(0, resolver.payloads.size());
    }

    @Test
    void nullProposalFutureStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.returnNullProposal = true;
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, error.kind());
        assertTrue(error.getMessage().contains("proposal returned null future"));
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.payloads.size());
    }

    @Test
    void nullProposalCommitStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.responses.add(CompletableFuture.completedFuture(null));
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.RETRY_EXHAUSTED, error.kind());
        assertEquals("replicated write proposal returned null commit", error.getMessage());
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.payloads.size());
    }

    @Test
    void synchronousFatalProposalFailureRetainsSeverity() {
        var resolver = new RecordingResolver();
        var fatalFailure = new AssertionError("fatal sync proposal failed");
        resolver.proposalFailures.add(fatalFailure);
        var submitter = submitter(metadata(7, N1), resolver, 1, 0);

        var error = assertThrows(AssertionError.class,
                () -> submitter.submit(command(), 7, 1000));

        assertSame(fatalFailure, error);
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.payloads.size());
    }

    @Test
    void expiredDeadlineFailsBeforeResolvingRoute() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 2, 10);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(command(), 7, 10));

        assertEquals(PartitionWriteFailureKind.DEADLINE_EXPIRED, error.kind());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void rejectsCrossPartitionMultiMutationWriteBeforeResolvingRoute() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 2, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> submitter.submit(crossPartitionCommand(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.INVALID_REQUEST, error.kind());
        assertTrue(error.getMessage().contains("outside the target partition"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.payloads.size());
    }

    private static PartitionWriteSubmitter submitter(ClusterMetadata metadata,
                                                     RecordingResolver resolver,
                                                     int maxAttempts,
                                                     long nowMillis) {
        return new PartitionWriteSubmitter(new PartitionRouteCache(metadata), resolver, maxAttempts, () -> nowMillis);
    }

    private static PartitionWriteCommand command() {
        return PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("value")));
    }

    private static PartitionWriteCommand crossPartitionCommand() {
        return new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("left")),
                ReplicatedMutation.put(4, new Keys(buf(0x28)), text("right"))
        ));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    private static ClusterMetadata metadata(long epoch, NodeId leader) {
        var group = new ReplicaGroupMetadata(PARTITION, 3, leader, peers(), 3);
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        var partition = new PartitionDescriptor(
                PARTITION,
                buf(0x10),
                buf(0x20),
                3,
                "group-7",
                peers(),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
        return new ClusterMetadata(
                CLUSTER_ID,
                epoch,
                Map.of(PARTITION, group),
                Map.of(4L, column),
                Map.of(PARTITION, partition)
        );
    }

    private static List<ReplicaMember> peers() {
        return List.of(
                new ReplicaMember(N1, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 25, 25, true),
                new ReplicaMember(N2, URI.create("http://127.0.0.1:10002"), ReplicaRole.VOTER, 24, 24, true)
        );
    }

    private static ClientRequestId requestId() {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), 7);
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes);
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    private static final class RecordingResolver implements PartitionWriteSubmitter.LogClientResolver {

        private final Queue<CompletableFuture<ReplicatedLogCommit>> responses = new ArrayDeque<>();
        private final Queue<Throwable> proposalFailures = new ArrayDeque<>();
        private final List<RouteTarget> targets = new ArrayList<>();
        private final List<byte[]> payloads = new ArrayList<>();
        private boolean returnNullClient;
        private boolean returnNullProposal;
        private RuntimeException resolveFailure;

        @Override
        public ReplicatedLogClient resolve(RouteTarget target) {
            targets.add(target);
            if (resolveFailure != null) {
                throw resolveFailure;
            }
            if (returnNullClient) {
                return null;
            }
            return payload -> {
                payloads.add(payload.clone());
                if (returnNullProposal) {
                    return null;
                }
                if (!proposalFailures.isEmpty()) {
                    var failure = proposalFailures.remove();
                    if (failure instanceof RuntimeException runtimeException) {
                        throw runtimeException;
                    }
                    if (failure instanceof Error error) {
                        throw error;
                    }
                    throw new AssertionError("unsupported proposal failure type", failure);
                }
                return responses.remove();
            };
        }
    }
}
