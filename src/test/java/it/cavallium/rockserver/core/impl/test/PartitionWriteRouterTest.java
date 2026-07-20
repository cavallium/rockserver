package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import it.cavallium.rockserver.core.cluster.client.PartitionWriteRouter;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionWriteRouterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId FIRST_PARTITION = new ShardId(7);
    private static final ShardId SECOND_PARTITION = new ShardId(8);
    private static final NodeId N1 = NodeId.of("n1");
    private static final NodeId N2 = NodeId.of("n2");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("routeCache must not be null", () -> new PartitionWriteRouter(null));

        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));
        assertIllegalArgument("command must not be null", () -> router.routeWriteAtEpoch(7, null));
    }

    @Test
    void routesWriteToLeaderOfPartitionOwningCalculatedKey() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));
        var command = command();

        var target = router.routeWrite(command);

        assertEquals(FIRST_PARTITION, target.shardId());
        assertEquals(N1, target.replica().nodeId());
        assertTrue(target.leader());
        assertEquals(7, target.metadataEpoch());
    }

    @Test
    void metadataUpdateRoutesWriteToNewLeader() {
        var cache = new PartitionRouteCache(metadata(7, N1));
        var router = new PartitionWriteRouter(cache);
        router.routeWrite(command());

        cache.updateMetadata(metadata(8, N2));
        var target = router.routeWriteAtEpoch(8, command());

        assertEquals(FIRST_PARTITION, target.shardId());
        assertEquals(N2, target.replica().nodeId());
        assertEquals(8, target.metadataEpoch());
    }

    @Test
    void staleExpectedMetadataEpochStaysTyped() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> router.routeWriteAtEpoch(6, command()));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertEquals("stale route metadata epoch 6; current epoch is 7", error.getMessage());
    }

    @Test
    void futureExpectedMetadataEpochStaysTyped() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));

        var error = assertThrows(PartitionWriteRouteException.class,
                () -> router.routeWriteAtEpoch(8, command()));

        assertEquals(PartitionWriteFailureKind.STALE_ROUTE, error.kind());
        assertEquals("future route metadata epoch 8; current epoch is 7", error.getMessage());
    }

    @Test
    void noKnownLeaderStaysTyped() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, null)));

        var error = assertThrows(PartitionWriteRouteException.class, () -> router.routeWrite(command()));

        assertEquals(PartitionWriteFailureKind.ROUTE_FAILED, error.kind());
        assertEquals("write route failed: no known leader for shard " + FIRST_PARTITION, error.getMessage());
    }

    @Test
    void writeOutsidePartitionRangeStaysTyped() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));

        var error = assertThrows(PartitionWriteRouteException.class, () -> router.routeWrite(command(0x40)));

        assertEquals(PartitionWriteFailureKind.ROUTE_FAILED, error.kind());
        assertEquals("write route failed: no partition owns calculated key", error.getMessage());
    }

    @Test
    void rejectsCrossPartitionWriteBeforeRoutingToLeader() {
        var router = new PartitionWriteRouter(new PartitionRouteCache(metadata(7, N1)));
        var command = new PartitionWriteCommand(requestId(), 3, List.of(
                ReplicatedMutation.put(4, new Keys(buf(0x18)), text("first")),
                ReplicatedMutation.put(4, new Keys(buf(0x28)), text("second"))
        ));

        assertThrows(IllegalArgumentException.class, () -> router.routeWrite(command));
    }

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

    private static PartitionWriteCommand command() {
        return command(0x18);
    }

    private static PartitionWriteCommand command(int key) {
        return PartitionWriteCommand.single(requestId(), 3,
                ReplicatedMutation.put(4, new Keys(buf(key)), text("value")));
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
}
