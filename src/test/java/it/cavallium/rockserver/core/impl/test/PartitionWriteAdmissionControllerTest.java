package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import it.cavallium.rockserver.core.cluster.client.PartitionWriteAdmissionController;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteAdmissionQueue;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteRouteException;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

class PartitionWriteAdmissionControllerTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId FIRST_PARTITION = new ShardId(7);
    private static final ShardId SECOND_PARTITION = new ShardId(8);
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var cache = new PartitionRouteCache(metadata(7, N1));

        assertIllegalArgument("routeCache must not be null",
                () -> new PartitionWriteAdmissionController(null, 1, 1, () -> 0));
        assertIllegalArgument("currentTimeMillis must not be null",
                () -> new PartitionWriteAdmissionController(cache, 1, 1, null));

        var controller = new PartitionWriteAdmissionController(cache, 10, 10, () -> 0);
        assertIllegalArgument("command must not be null", () -> controller.admit(null, 7, 1000));
        assertIllegalArgument("partitionId must not be null", () -> controller.drainBatch(null));
        assertIllegalArgument("partitionId must not be null", () -> controller.queued(null));

        assertIllegalArgument("partitionId must not be null",
                () -> new PartitionWriteAdmissionQueue(null, 1, 1, () -> 0));
        assertIllegalArgument("currentTimeMillis must not be null",
                () -> new PartitionWriteAdmissionQueue(FIRST_PARTITION, 1, 1, null));

        var queue = new PartitionWriteAdmissionQueue(FIRST_PARTITION, 1, 1, () -> 0);
        assertIllegalArgument("command must not be null", () -> queue.admit(null, 1000));

        var sealedCommand = controller.admit(command(0x18, 1), 7, 1000).command();
        assertIllegalArgument("partitionId must not be null",
                () -> new PartitionWriteAdmissionQueue.AdmittedPartitionWrite(null, sealedCommand, 1000));
        assertIllegalArgument("command must not be null",
                () -> new PartitionWriteAdmissionQueue.AdmittedPartitionWrite(FIRST_PARTITION, null, 1000));
    }

    @Test
    void queuesAreBoundedPerPartition() {
        var controller = controller(1, 10, 0);
        controller.admit(command(0x18, 1), 7, 1000);
        controller.admit(command(0x28, 2), 7, 1000);

        assertEquals(1, controller.queued(FIRST_PARTITION));
        assertEquals(1, controller.queued(SECOND_PARTITION));
        assertEquals(Set.of(FIRST_PARTITION, SECOND_PARTITION), controller.queuedPartitions());
    }

    @Test
    void backpressureAppliesOnlyToFullPartitionQueue() {
        var controller = controller(1, 10, 0);
        controller.admit(command(0x18, 1), 7, 1000);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(command(0x19, 2), 7, 1000));

        assertEquals(PartitionWriteFailureKind.BACKPRESSURE, error.kind());
        controller.admit(command(0x28, 3), 7, 1000);
        assertEquals(1, controller.queued(SECOND_PARTITION));
    }

    @Test
    void expiredQueuedWritesDoNotHoldPartitionCapacity() {
        var now = new AtomicLong();
        var controller = new PartitionWriteAdmissionController(
                new PartitionRouteCache(metadata(7, N1)),
                1,
                10,
                now::get
        );
        controller.admit(command(0x18, 1), 7, 10);
        now.set(10);

        var admitted = controller.admit(command(0x19, 2), 7, 100);
        var batch = controller.drainBatch(FIRST_PARTITION);

        assertEquals(requestId(2), admitted.command().requestId());
        assertEquals(List.of(requestId(2)), batch.stream().map(entry -> entry.command().requestId()).toList());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void queuedPartitionAccountingPrunesExpiredWrites() {
        var now = new AtomicLong();
        var controller = new PartitionWriteAdmissionController(
                new PartitionRouteCache(metadata(7, N1)),
                2,
                10,
                now::get
        );
        controller.admit(command(0x18, 1), 7, 10);
        controller.admit(command(0x28, 2), 7, 100);
        now.set(10);

        assertEquals(0, controller.queued(FIRST_PARTITION));
        assertEquals(Set.of(SECOND_PARTITION), controller.queuedPartitions());
    }

    @Test
    void drainsBatchesAtConfiguredSizeInFifoOrder() {
        var controller = controller(3, 2, 0);
        var firstAdmitted = controller.admit(command(0x18, 1), 7, 1000);
        var returnedPayload = firstAdmitted.payload();
        returnedPayload[0] ^= (byte) 0xff;
        controller.admit(command(0x19, 2), 7, 1000);
        controller.admit(command(0x1a, 3), 7, 1000);

        var firstBatch = controller.drainBatch(FIRST_PARTITION);
        assertEquals(Set.of(FIRST_PARTITION), controller.queuedPartitions());
        var secondBatch = controller.drainBatch(FIRST_PARTITION);

        assertEquals(List.of(requestId(1), requestId(2)),
                firstBatch.stream().map(admitted -> admitted.command().requestId()).toList());
        assertEquals(1000, firstBatch.getFirst().deadlineEpochMillis());
        assertArrayEquals(PartitionWriteCommandCodec.encode(firstBatch.getFirst().command()),
                firstBatch.getFirst().payload());
        assertEquals(List.of(requestId(3)),
                secondBatch.stream().map(admitted -> admitted.command().requestId()).toList());
        assertEquals(0, controller.queued(FIRST_PARTITION));
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void expiredDeadlineFailsBeforeCommandRouting() {
        var controller = controller(1, 10, 10);
        var invalidIfRouted = PartitionWriteCommand.single(requestId(1), 3,
                ReplicatedMutation.put(404, new Keys(buf(0x18)), text("value")));

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(invalidIfRouted, 7, 10));

        assertEquals(PartitionWriteFailureKind.DEADLINE_EXPIRED, error.kind());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void deadlineExpiringAfterRoutingDoesNotCreateEmptyQueue() {
        var clockReads = new AtomicInteger();
        var controller = new PartitionWriteAdmissionController(
                new PartitionRouteCache(metadata(7, N1)),
                1,
                10,
                () -> clockReads.getAndIncrement() == 0 ? 0 : 10
        );

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(command(0x18, 1), 7, 10));

        assertEquals(PartitionWriteFailureKind.DEADLINE_EXPIRED, error.kind());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void staleMetadataEpochFailsBeforeQueueing() {
        var controller = controller(1, 10, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(command(0x18, 1), 6, 1000));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertEquals(0, controller.queued(FIRST_PARTITION));
    }

    @Test
    void staleMetadataEpochFailsBeforeCommandSealing() {
        var controller = controller(1, 10, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(crossPartitionCommand(), 6, 1000));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void futureMetadataEpochFailsBeforeCommandSealing() {
        var controller = controller(1, 10, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(crossPartitionCommand(), 8, 1000));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertEquals("future route metadata epoch 8; current epoch is 7", error.getMessage());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void crossPartitionMultiMutationWriteFailsBeforeQueueing() {
        var controller = controller(1, 10, 0);

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> controller.admit(crossPartitionCommand(), 7, 1000));

        assertEquals(PartitionWriteFailureKind.INVALID_REQUEST, error.kind());
        assertEquals(Set.of(), controller.queuedPartitions());
    }

    @Test
    void queueRejectsDirectUnsealedCommands() {
        var queue = new PartitionWriteAdmissionQueue(FIRST_PARTITION, 1, 1, () -> 0);

        var error = assertThrows(IllegalArgumentException.class,
                () -> queue.admit(command(0x18, 1), 1000));

        assertEquals("partition write admission queue requires a sealed command", error.getMessage());
    }

    private static PartitionWriteAdmissionController controller(int capacityPerPartition,
                                                                int maxBatchSize,
                                                                long nowMillis) {
        return new PartitionWriteAdmissionController(
                new PartitionRouteCache(metadata(7, N1)),
                capacityPerPartition,
                maxBatchSize,
                () -> nowMillis
        );
    }

    @SuppressWarnings("SameParameterValue")
    private static ClusterMetadata metadata(long epoch, NodeId firstLeader) {
        var firstGroup = group(FIRST_PARTITION, firstLeader);
        var secondGroup = group(SECOND_PARTITION, N2);
        var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        return new ClusterMetadata(
                CLUSTER_ID,
                epoch,
                Map.of(FIRST_PARTITION, firstGroup, SECOND_PARTITION, secondGroup),
                Map.of(4L, column),
                Map.of(FIRST_PARTITION, firstPartition(firstLeader), SECOND_PARTITION, secondPartition())
        );
    }

    private static ReplicaGroupMetadata group(ShardId partitionId, NodeId leader) {
        return new ReplicaGroupMetadata(partitionId, 3, leader, peers(), 3);
    }

    private static PartitionDescriptor firstPartition(NodeId leader) {
        return new PartitionDescriptor(
                FIRST_PARTITION,
                buf(0x10),
                buf(0x20),
                3,
                "group-7",
                peers(),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static PartitionDescriptor secondPartition() {
        return new PartitionDescriptor(
                SECOND_PARTITION,
                buf(0x20),
                buf(0x30),
                3,
                "group-8",
                peers(),
                N2,
                Map.of("rack", "rack-b"),
                PartitionSplitState.ACTIVE
        );
    }

    private static List<ReplicaMember> peers() {
        return List.of(
                new ReplicaMember(N1, URI.create("http://127.0.0.1:10001"), ReplicaRole.VOTER, 25, 25, true),
                new ReplicaMember(N2, URI.create("http://127.0.0.1:10002"), ReplicaRole.VOTER, 24, 24, true)
        );
    }

    private static PartitionWriteCommand command(int key, long requestId) {
        return PartitionWriteCommand.single(requestId(requestId), 3,
                ReplicatedMutation.put(4, new Keys(buf(key)), text("value")));
    }

    private static PartitionWriteCommand crossPartitionCommand() {
        return new PartitionWriteCommand(requestId(1), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("left")),
                ReplicatedMutation.put(4, new Keys(buf(0x28)), text("right"))
        ));
    }

    private static ClientRequestId requestId(long requestId) {
        return new ClientRequestId(new ClientId(new UUID(0, 1)), requestId);
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
}
