package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursor;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursorCodec;
import it.cavallium.rockserver.core.cluster.client.PartitionReadFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionReadRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

class PartitionRangeReadSubmitterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId PARTITION = new ShardId(7);
    private static final ShardId NEXT_PARTITION = new ShardId(8);
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var resolver = new RecordingResolver();
        var cache = new PartitionRouteCache(metadata(7, N1));
        var limits = new PartitionRangeReadSubmitter.PartitionRangeScanLimits(2, 1024);

        assertIllegalArgument("routeCache must not be null",
                () -> new PartitionRangeReadSubmitter(null, resolver, () -> 0));
        assertIllegalArgument("ratisClientResolver must not be null",
                () -> new PartitionRangeReadSubmitter(cache, null, () -> 0));
        assertIllegalArgument("currentTimeMillis must not be null",
                () -> new PartitionRangeReadSubmitter(cache, resolver, null));
        assertIllegalArgument("scanLimits must not be null",
                () -> new PartitionRangeReadSubmitter(cache, resolver, () -> 0, null));

        var submitter = new PartitionRangeReadSubmitter(cache, resolver, () -> 0, limits);
        assertIllegalArgument("command must not be null",
                () -> submitter.scanLinearizable(null, 7, 1000));
        assertIllegalArgument("command must not be null",
                () -> submitter.scanLinearizableAcrossPartitions(null, 7, 1000));
        assertIllegalArgument("command must not be null", () -> submitter.openCursor(null, 7));
        assertIllegalArgument("cursor must not be null", () -> submitter.scanLinearizablePage(null, 1, 1000));

        var routeTarget = routeTarget();
        var command = command(0x18, 0x1f, false, 128, 3);
        assertIllegalArgument("target must not be null",
                () -> new PartitionRangeReadSubmitter.PartitionRangeScanResult(null, command, null));
        assertIllegalArgument("command must not be null",
                () -> new PartitionRangeReadSubmitter.PartitionRangeScanResult(routeTarget, null, null));
        assertIllegalArgument("results must not be null",
                () -> new PartitionRangeReadSubmitter.PartitionRangeScanPage(null, null));
        var results = new ArrayList<PartitionRangeReadSubmitter.PartitionRangeScanResult>();
        results.add(null);
        assertIllegalArgument("results[0] must not be null",
                () -> new PartitionRangeReadSubmitter.PartitionRangeScanPage(results, null));
    }

    @Test
    void linearizableRangeScanRoutesToLeaderAndUsesRatisReadPath() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("batch")));
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var command = command(0x18, 0x1f, false, 128, 3);

        var result = submitter.scanLinearizable(command, 7, 1000);

        assertArrayEquals(bytes("batch"), result);
        assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
        assertArrayEquals(PartitionRangeReadCommandCodec.encode(command), resolver.client.queries.getFirst());
        assertEquals(command, PartitionRangeReadCommandCodec.decode(resolver.client.queries.getFirst()));
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void rangeEndingAtPartitionBoundaryRoutesToOnlyOwningPartition() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("boundary-batch")));
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var result = submitter.scanLinearizable(command(0x18, 0x20, true, 16, 3), 7, 1000);

        assertArrayEquals(bytes("boundary-batch"), result);
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
        assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void multiPartitionRangeScanDispatchesSegmentsInCalculatedKeyOrder() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("first-partition")));
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("second-partition")));
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var command = command(0x18, 0x28, false, 128, 3);

        var results = submitter.scanLinearizableAcrossPartitions(command, 7, 1000);

        assertEquals(2, results.size());
        assertEquals(PARTITION, results.getFirst().target().shardId());
        assertEquals(NEXT_PARTITION, results.getLast().target().shardId());
        assertArrayEquals(bytes("first-partition"), results.getFirst().response());
        assertArrayEquals(bytes("second-partition"), results.getLast().response());
        var firstSegment = PartitionRangeReadCommandCodec.decode(resolver.client.queries.getFirst());
        var secondSegment = PartitionRangeReadCommandCodec.decode(resolver.client.queries.getLast());
        assertEquals(new Keys(buf(0x18)), firstSegment.startKeysInclusive());
        assertNull(firstSegment.endKeysExclusive());
        assertNull(secondSegment.startKeysInclusive());
        assertEquals(new Keys(buf(0x28)), secondSegment.endKeysExclusive());
        assertEquals(128, firstSegment.maxEntries());
        assertEquals(128, secondSegment.maxEntries());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void reverseMultiPartitionRangeScanDispatchesLastPartitionFirst() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("second-partition")));
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("first-partition")));
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var command = command(0x18, 0x28, true, 128, 3);

        var results = submitter.scanLinearizableAcrossPartitions(command, 7, 1000);

        assertEquals(2, results.size());
        assertEquals(NEXT_PARTITION, results.getFirst().target().shardId());
        assertEquals(PARTITION, results.getLast().target().shardId());
        var secondSegment = PartitionRangeReadCommandCodec.decode(resolver.client.queries.getFirst());
        var firstSegment = PartitionRangeReadCommandCodec.decode(resolver.client.queries.getLast());
        assertNull(secondSegment.startKeysInclusive());
        assertEquals(new Keys(buf(0x28)), secondSegment.endKeysExclusive());
        assertTrue(secondSegment.reverse());
        assertEquals(new Keys(buf(0x18)), firstSegment.startKeysInclusive());
        assertNull(firstSegment.endKeysExclusive());
        assertTrue(firstSegment.reverse());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void multiPartitionRangeScanRejectsMetadataGapsBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(gappedMetadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x38, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("calculated key range crosses non-contiguous partitions"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void multiPartitionRangeScanRejectsStaleMetadataBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x28, false, 128, 3), 6, 1000));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void multiPartitionRangeScanRejectsFutureMetadataBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x28, false, 128, 3), 8, 1000));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertTrue(error.getMessage().contains("future range read route metadata epoch 8; current epoch is 7"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void singlePartitionRangeScanRejectsSplittingPartitionBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1, PartitionSplitState.SPLIT_SOURCE, PartitionSplitState.ACTIVE),
                resolver,
                0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("is not active for range scans"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void singlePartitionRangeScanWithoutServingLeaderStaysTyped() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadataWithFirstLeaderNotServing(), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertEquals("range read route failed: leader cannot satisfy read policy for shard " + PARTITION,
                error.getMessage());
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void multiPartitionRangeScanRejectsSplittingPartitionBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1, PartitionSplitState.ACTIVE, PartitionSplitState.SPLIT_TARGET),
                resolver,
                0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x28, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("is not active for range scans"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void cursorTokenRoundTripsWithoutResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var cursor = submitter.openCursor(command(0x18, 0x28, false, 128, 3), 7);

        var encoded = PartitionRangeScanCursorCodec.encode(cursor);
        var decoded = PartitionRangeScanCursorCodec.decode(encoded);

        assertEquals(cursor, decoded);
        assertEquals(0, resolver.targets.size());
        var corrupted = encoded.clone();
        corrupted[corrupted.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> PartitionRangeScanCursorCodec.decode(corrupted));
    }

    @Test
    void cursorPageResumesAtNextPartitionSegment() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("first-page")));
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var cursor = submitter.openCursor(command(0x18, 0x28, false, 128, 3), 7);

        var firstPage = submitter.scanLinearizablePage(cursor, 1, 1000);

        assertEquals(1, firstPage.results().size());
        assertArrayEquals(bytes("first-page"), firstPage.results().getFirst().response());
        assertNotNull(firstPage.nextCursor());
        assertEquals(1, firstPage.nextCursor().nextSegmentIndex());
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("second-page")));

        var secondPage = submitter.scanLinearizablePage(firstPage.nextCursor(), 1, 1000);

        assertEquals(1, secondPage.results().size());
        assertArrayEquals(bytes("second-page"), secondPage.results().getFirst().response());
        assertNull(secondPage.nextCursor());
        assertEquals(PARTITION, resolver.targets.getFirst().shardId());
        assertEquals(NEXT_PARTITION, resolver.targets.getLast().shardId());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void reverseCursorPageResumesInReversePartitionOrder() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("reverse-first")));
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("reverse-second")));
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var cursor = submitter.openCursor(command(0x18, 0x28, true, 128, 3), 7);

        var firstPage = submitter.scanLinearizablePage(cursor, 1, 1000);
        var secondPage = submitter.scanLinearizablePage(firstPage.nextCursor(), 1, 1000);

        assertEquals(NEXT_PARTITION, firstPage.results().getFirst().target().shardId());
        assertEquals(PARTITION, secondPage.results().getFirst().target().shardId());
        assertNull(secondPage.nextCursor());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void cursorPageClampsRequestedSegmentsToConfiguredFanoutLimit() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("first-page")));
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("second-page")));
        var limits = new PartitionRangeReadSubmitter.PartitionRangeScanLimits(1, 1024);
        var submitter = submitter(metadata(7, N1), resolver, 0, limits);
        var cursor = submitter.openCursor(command(0x18, 0x28, false, 128, 3), 7);

        var firstPage = submitter.scanLinearizablePage(cursor, 10, 1000);

        assertEquals(1, firstPage.results().size());
        assertNotNull(firstPage.nextCursor());
        assertEquals(1, firstPage.nextCursor().nextSegmentIndex());
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void wideScanWithoutCursorRejectsFanoutAboveConfiguredLimit() {
        var resolver = new RecordingResolver();
        var limits = new PartitionRangeReadSubmitter.PartitionRangeScanLimits(1, 1024);
        var submitter = submitter(metadata(7, N1), resolver, 0, limits);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x28, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.RESOURCE_LIMIT_EXCEEDED, error.kind());
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void oversizedSegmentResponseIsRejected() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("too-large")));
        var limits = new PartitionRangeReadSubmitter.PartitionRangeScanLimits(4, 4);
        var submitter = submitter(metadata(7, N1), resolver, 0, limits);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.RESOURCE_LIMIT_EXCEEDED, error.kind());
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void cursorRejectsStaleMetadataBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var staleCursor = new PartitionRangeScanCursor(6, command(0x18, 0x28, false, 128, 3), 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizablePage(staleCursor, 1, 1000));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void cursorRejectsInvalidSegmentIndexBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var invalidCursor = new PartitionRangeScanCursor(7, command(0x18, 0x28, false, 128, 3), 3);

        assertThrows(IllegalArgumentException.class,
                () -> submitter.scanLinearizablePage(invalidCursor, 1, 1000));

        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void cursorRejectsSplittingPartitionBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1, PartitionSplitState.ACTIVE, PartitionSplitState.SPLIT_FINALIZING),
                resolver,
                0);
        var cursor = new PartitionRangeScanCursor(7, command(0x18, 0x28, false, 128, 3), 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizablePage(cursor, 1, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("is not active for range scans"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void cursorRejectsMetadataGapBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(gappedMetadata(7, N1), resolver, 0);
        var cursor = new PartitionRangeScanCursor(7, command(0x18, 0x38, false, 128, 3), 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizablePage(cursor, 1, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("calculated key range crosses non-contiguous partitions"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void crossingPartitionRangeFailsBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x28, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("calculated key range spans multiple partitions"));
        assertEquals(0, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void openEndedRangeAcrossPartitionsFailsBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var command = new PartitionRangeReadCommand(3, 4, new Keys(buf(0x18)), null, false, 128);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command, 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("calculated key range spans multiple partitions"));
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void malformedRangeBoundsStayRequestValidationFailures() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);
        var invalidRange = new PartitionRangeReadCommand(3, 4, new Keys(buf(0x1f)), new Keys(buf(0x18)),
                false, 128);

        var error = assertThrows(IllegalArgumentException.class,
                () -> submitter.scanLinearizable(invalidRange, 7, 1000));

        assertEquals("range start key must be lower than end key", error.getMessage());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void staleMetadataEpochFailsBeforeResolvingClient() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 6, 1000));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void expiredDeadlineFailsBeforeRoutingCommand() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 10);
        var invalidIfRouted = new PartitionRangeReadCommand(3, 404, new Keys(buf(0x18)), new Keys(buf(0x1f)),
                false, 128);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(invalidIfRouted, 7, 10));

        assertEquals(PartitionReadFailureKind.DEADLINE_EXPIRED, error.kind());
        assertEquals("linearizable range scan deadline expired before routing", error.getMessage());
        assertEquals(0, resolver.targets.size());
    }

    @Test
    void multiPartitionDeadlineExpiresBeforeNextSegmentDispatch() {
        var resolver = new RecordingResolver();
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("first-partition")));
        resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("second-partition")));
        var clockReads = new AtomicInteger();
        var submitter = new PartitionRangeReadSubmitter(
                new PartitionRouteCache(metadata(7, N1)),
                resolver,
                () -> clockReads.getAndIncrement() < 3 ? 0 : 3
        );

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizableAcrossPartitions(command(0x18, 0x28, false, 128, 3), 7, 3));

        assertEquals(PartitionReadFailureKind.DEADLINE_EXPIRED, error.kind());
        assertEquals("linearizable range scan deadline expired before segment dispatch", error.getMessage());
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.client.queries.size());
        assertEquals(0, resolver.client.proposals.size());
    }

    @Test
    void timeoutCancelsLinearizableQuery() {
        var resolver = new RecordingResolver();
        var response = new CompletableFuture<byte[]>();
        resolver.client.queryResponses.add(response);
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1));

        assertEquals(PartitionReadFailureKind.TIMEOUT, error.kind());
        assertTrue(response.isCancelled());
    }

    @Test
    void interruptionCancelsLinearizableQueryAndRestoresInterruptFlag() {
        var resolver = new RecordingResolver();
        var response = new CompletableFuture<byte[]>();
        resolver.client.queryResponses.add(response);
        var submitter = submitter(metadata(7, N1), resolver, 0);

        Thread.currentThread().interrupt();
        try {
            var error = assertThrows(PartitionReadRouteException.class,
                    () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

            assertEquals(PartitionReadFailureKind.TIMEOUT, error.kind());
            assertTrue(response.isCancelled());
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptedStatus();
        }
    }

    @Test
    void checkedRangeReadFailureUsesQueryFailedKind() {
        var resolver = new RecordingResolver();
        var checkedFailure = new Exception("checked range read failed");
        resolver.client.queryResponses.add(CompletableFuture.failedFuture(checkedFailure));
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertSame(checkedFailure, error.getCause());
        assertEquals(1, resolver.targets.size());
    }

    @Test
    void runtimeRangeReadFailuresUseQueryFailedKind() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new IllegalStateException("async range read failed");
        asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(asyncFailure));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

        var asyncError = assertThrows(PartitionReadRouteException.class,
                () -> asyncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, asyncError.kind());
        assertEquals("linearizable range scan failed", asyncError.getMessage());
        assertSame(asyncFailure, asyncError.getCause());
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.client.queries.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new IllegalStateException("sync range read failed");
        syncResolver.client.queryFailure = syncFailure;
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

        var syncError = assertThrows(PartitionReadRouteException.class,
                () -> syncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, syncError.kind());
        assertEquals("linearizable range scan failed", syncError.getMessage());
        assertSame(syncFailure, syncError.getCause());
        assertEquals(1, syncResolver.targets.size());
        assertEquals(0, syncResolver.client.queries.size());
    }

    @Test
    void adapterRpcRangeReadFailuresUseQueryFailedKind() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new RatisProcessAdapterRpcException("ADAPTER_ERROR", "adapter range failed");
        asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(asyncFailure));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

        var asyncError = assertThrows(PartitionReadRouteException.class,
                () -> asyncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, asyncError.kind());
        assertSame(asyncFailure, asyncError.getCause());
        assertTrue(asyncError.getMessage().contains(
                "linearizable range scan failed with adapter error ADAPTER_ERROR: adapter range failed"));
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.client.queries.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new RatisProcessAdapterRpcException("UNSUPPORTED", "range disabled");
        syncResolver.client.queryFailure = syncFailure;
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

        var syncError = assertThrows(PartitionReadRouteException.class,
                () -> syncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, syncError.kind());
        assertSame(syncFailure, syncError.getCause());
        assertTrue(syncError.getMessage().contains(
                "linearizable range scan failed with adapter error UNSUPPORTED: range disabled"));
        assertEquals(1, syncResolver.targets.size());
        assertEquals(0, syncResolver.client.queries.size());
    }

    @Test
    void nullRangeReadClientStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.returnNullClient = true;
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("resolver returned null Ratis client"));
        assertEquals(1, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void rangeReadResolverFailuresStayTyped() {
        var resolver = new RecordingResolver();
        var resolverFailure = new IllegalStateException("resolver failed");
        resolver.resolveFailure = resolverFailure;
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertEquals("linearizable range scan failed", error.getMessage());
        assertSame(resolverFailure, error.getCause());
        assertEquals(1, resolver.targets.size());
        assertEquals(0, resolver.client.queries.size());
    }

    @Test
    void nullRangeReadQueryFutureStaysTyped() {
        var resolver = new RecordingResolver();
        resolver.client.returnNullQuery = true;
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertTrue(error.getMessage().contains("returned null query future"));
        assertEquals(1, resolver.targets.size());
        assertEquals(1, resolver.client.queries.size());
    }

    @Test
    void unsupportedRangeReadFailuresStayTyped() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new RatisFeatureUnavailableException("async unsupported");
        asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(asyncFailure));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

        var asyncError = assertThrows(PartitionReadRouteException.class,
                () -> asyncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, asyncError.kind());
        assertEquals("linearizable range scan is unsupported", asyncError.getMessage());
        assertSame(asyncFailure, asyncError.getCause());
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.client.queries.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new RatisFeatureUnavailableException("sync unsupported");
        syncResolver.client.queryFailure = syncFailure;
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

        var syncError = assertThrows(PartitionReadRouteException.class,
                () -> syncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, syncError.kind());
        assertEquals("linearizable range scan is unsupported", syncError.getMessage());
        assertSame(syncFailure, syncError.getCause());
        assertEquals(1, syncResolver.targets.size());
        assertEquals(0, syncResolver.client.queries.size());
    }

    @Test
    void fatalRangeReadFailureRetainsSeverity() {
        var resolver = new RecordingResolver();
        var fatalFailure = new AssertionError("fatal range read failed");
        resolver.client.queryResponses.add(CompletableFuture.failedFuture(fatalFailure));
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(AssertionError.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertSame(fatalFailure, error);
        assertEquals(1, resolver.targets.size());
    }

    @Test
    void wrappedFatalRangeReadFailuresRetainSeverity() {
        var asyncResolver = new RecordingResolver();
        var asyncFailure = new AssertionError("wrapped async fatal range read failed");
        asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(new CompletionException(asyncFailure)));
        var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

        var asyncError = assertThrows(AssertionError.class,
                () -> asyncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertSame(asyncFailure, asyncError);
        assertEquals(1, asyncResolver.targets.size());
        assertEquals(1, asyncResolver.client.queries.size());

        var syncResolver = new RecordingResolver();
        var syncFailure = new AssertionError("wrapped sync fatal range read failed");
        syncResolver.client.queryFailure = new CompletionException(syncFailure);
        var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

        var syncError = assertThrows(AssertionError.class,
                () -> syncSubmitter.scanLinearizable(command(0x18, 0x1f, false, 128, 3), 7, 1000));

        assertSame(syncFailure, syncError);
        assertEquals(1, syncResolver.targets.size());
        assertEquals(0, syncResolver.client.queries.size());
    }

    @Test
    void rejectsSchemaEpochMismatchBeforeRatisQuery() {
        var resolver = new RecordingResolver();
        var submitter = submitter(metadata(7, N1), resolver, 0);

        var error = assertThrows(PartitionReadRouteException.class,
                () -> submitter.scanLinearizable(command(0x18, 0x1f, false, 128, 2), 7, 1000));

        assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
        assertEquals("range read route failed: range read schema epoch 2 does not match column schema epoch 3",
                error.getMessage());
        assertSame(IllegalArgumentException.class, error.getCause().getClass());
        assertEquals(0, resolver.targets.size());
    }

    private static PartitionRangeReadSubmitter submitter(ClusterMetadata metadata,
                                                         RecordingResolver resolver,
                                                         long nowMillis) {
        return new PartitionRangeReadSubmitter(new PartitionRouteCache(metadata), resolver, () -> nowMillis);
    }

    @SuppressWarnings("SameParameterValue")
    private static PartitionRangeReadSubmitter submitter(ClusterMetadata metadata,
                                                         RecordingResolver resolver,
                                                         long nowMillis,
                                                         PartitionRangeReadSubmitter.PartitionRangeScanLimits limits) {
        return new PartitionRangeReadSubmitter(new PartitionRouteCache(metadata), resolver, () -> nowMillis, limits);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptedStatus() {
        Thread.interrupted();
    }

    @SuppressWarnings("SameParameterValue")
    private static PartitionRangeReadCommand command(int start,
                                                     int end,
                                                     boolean reverse,
                                                     int maxEntries,
                                                     long schemaEpoch) {
        return new PartitionRangeReadCommand(schemaEpoch,
                4,
                new Keys(buf(start)),
                new Keys(buf(end)),
                reverse,
                maxEntries);
    }

    @SuppressWarnings("SameParameterValue")
    private static ClusterMetadata metadata(long epoch, NodeId firstPartitionLeader) {
        return metadata(epoch, firstPartitionLeader, PartitionSplitState.ACTIVE, PartitionSplitState.ACTIVE);
    }

    @SuppressWarnings("SameParameterValue")
    private static ClusterMetadata metadata(long epoch,
                                            NodeId firstPartitionLeader,
                                            PartitionSplitState firstPartitionState,
                                            PartitionSplitState secondPartitionState) {
        var firstGroup = new ReplicaGroupMetadata(PARTITION, 3, firstPartitionLeader, peers(), 3);
        var secondGroup = new ReplicaGroupMetadata(NEXT_PARTITION, 3, N2, peers(), 3);
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        var firstPartition = partition(PARTITION, buf(0x10), buf(0x20), firstPartitionLeader, firstPartitionState);
        var secondPartition = partition(NEXT_PARTITION, buf(0x20), buf(0x30), N2, secondPartitionState);
        return new ClusterMetadata(
                CLUSTER_ID,
                epoch,
                Map.of(PARTITION, firstGroup, NEXT_PARTITION, secondGroup),
                Map.of(4L, column),
                Map.of(PARTITION, firstPartition, NEXT_PARTITION, secondPartition)
        );
    }

    private static ClusterMetadata metadataWithFirstLeaderNotServing() {
        var peers = peers(false);
        var firstGroup = new ReplicaGroupMetadata(PARTITION, 3, N1, peers, 3);
        var secondGroup = new ReplicaGroupMetadata(NEXT_PARTITION, 3, N2, peers, 3);
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        var firstPartition = partition(PARTITION, buf(0x10), buf(0x20), N1, peers);
        var secondPartition = partition(NEXT_PARTITION, buf(0x20), buf(0x30), N2, peers);
        return new ClusterMetadata(
                CLUSTER_ID,
                7,
                Map.of(PARTITION, firstGroup, NEXT_PARTITION, secondGroup),
                Map.of(4L, column),
                Map.of(PARTITION, firstPartition, NEXT_PARTITION, secondPartition)
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static ClusterMetadata gappedMetadata(long epoch, NodeId firstPartitionLeader) {
        var firstGroup = new ReplicaGroupMetadata(PARTITION, 3, firstPartitionLeader, peers(), 3);
        var secondGroup = new ReplicaGroupMetadata(NEXT_PARTITION, 3, N2, peers(), 3);
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        var firstPartition = partition(PARTITION, buf(0x10), buf(0x20), firstPartitionLeader);
        var secondPartition = partition(NEXT_PARTITION, buf(0x30), null, N2);
        return new ClusterMetadata(
                CLUSTER_ID,
                epoch,
                Map.of(PARTITION, firstGroup, NEXT_PARTITION, secondGroup),
                Map.of(4L, column),
                Map.of(PARTITION, firstPartition, NEXT_PARTITION, secondPartition)
        );
    }

    private static PartitionDescriptor partition(ShardId partition,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 NodeId leader) {
        return partition(partition, startKeyInclusive, endKeyExclusive, leader, PartitionSplitState.ACTIVE);
    }

    private static PartitionDescriptor partition(ShardId partition,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 NodeId leader,
                                                 PartitionSplitState splitState) {
        return partition(partition, startKeyInclusive, endKeyExclusive, leader, peers(), splitState);
    }

    private static PartitionDescriptor partition(ShardId partition,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 NodeId leader,
                                                 List<ReplicaMember> peers) {
        return partition(partition, startKeyInclusive, endKeyExclusive, leader, peers, PartitionSplitState.ACTIVE);
    }

    private static PartitionDescriptor partition(ShardId partition,
                                                 Buf startKeyInclusive,
                                                 Buf endKeyExclusive,
                                                 NodeId leader,
                                                 List<ReplicaMember> peers,
                                                 PartitionSplitState splitState) {
        return new PartitionDescriptor(
                partition,
                startKeyInclusive,
                endKeyExclusive,
                3,
                "group-" + partition.value(),
                peers,
                leader,
                Map.of("rack", "rack-a"),
                splitState
        );
    }

    private static RouteTarget routeTarget() {
        return new RouteTarget(PARTITION, peers().getFirst(), true, ReplicatedCommandKind.ROUTED_READ, 7);
    }

    private static List<ReplicaMember> peers() {
        return peers(true);
    }

    private static List<ReplicaMember> peers(boolean firstServingReads) {
        return List.of(
                new ReplicaMember(N1, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 25, 25,
                        firstServingReads),
                new ReplicaMember(N2, URI.create("grpc://127.0.0.1:10002"), ReplicaRole.VOTER, 24, 24, true)
        );
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

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class RecordingResolver implements PartitionRangeReadSubmitter.RatisClientResolver {

        private final RecordingRatisClient client = new RecordingRatisClient();
        private final List<RouteTarget> targets = new ArrayList<>();
        private boolean returnNullClient;
        private RuntimeException resolveFailure;

        @Override
        public RatisPartitionClient resolve(RouteTarget target) {
            targets.add(target);
            if (resolveFailure != null) {
                throw resolveFailure;
            }
            if (returnNullClient) {
                return null;
            }
            return client;
        }
    }

    private static final class RecordingRatisClient implements RatisPartitionClient {

        private final Queue<CompletableFuture<byte[]>> queryResponses = new ArrayDeque<>();
        private final List<byte[]> queries = new ArrayList<>();
        private final List<byte[]> proposals = new ArrayList<>();
        private RuntimeException queryFailure;
        private boolean returnNullQuery;

        @Override
        public RatisPartitionDescriptor descriptor() {
            return new RatisPartitionDescriptor(
                    "7",
                    "group-7",
                    "n1",
                    List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:10001", "rack-a")),
                    Path.of("/tmp/partition-range-read-test")
            );
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            if (queryFailure != null) {
                throw queryFailure;
            }
            queries.add(request.clone());
            if (returnNullQuery) {
                return null;
            }
            return queryResponses.remove();
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            proposals.add(payload.clone());
            return CompletableFuture.completedFuture(new ReplicatedLogCommit(proposals.size(), true));
        }

        @Override
        public void close() {
        }
    }
}
