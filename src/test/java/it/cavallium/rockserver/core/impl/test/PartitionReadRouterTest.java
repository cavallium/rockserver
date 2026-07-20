package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionPointReadRouter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadRouter;
import it.cavallium.rockserver.core.cluster.client.PartitionReadFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionReadRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class PartitionReadRouterTest {

    private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ShardId PARTITION = new ShardId(7);
    private static final NodeId N1 = NodeId.of("n1");
	private static final NodeId N2 = NodeId.of("n2");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("routeCache must not be null", () -> new PartitionPointReadRouter(null));
        assertIllegalArgument("routeCache must not be null", () -> new PartitionRangeReadRouter(null));

        var cache = new PartitionRouteCache(metadata());
        var pointRouter = new PartitionPointReadRouter(cache);
        var rangeRouter = new PartitionRangeReadRouter(cache);

        assertIllegalArgument("command must not be null",
                () -> pointRouter.routeLinearizableReadAtEpoch(7, null));
        assertIllegalArgument("policy must not be null",
                () -> pointRouter.routeReadAtEpoch(7, pointCommand(), null));
        assertIllegalArgument("command must not be null",
                () -> rangeRouter.routeLinearizableScanAtEpoch(7, null));
        assertIllegalArgument("command must not be null",
                () -> rangeRouter.routeLinearizableScanSegmentsAtEpoch(7, null));
    }

	@Test
	void pointReadRoutesToOwningPartitionLeader() {
		var router = new PartitionPointReadRouter(new PartitionRouteCache(metadata()));

		var target = router.routeLinearizableRead(pointCommand());

		assertEquals(PARTITION, target.shardId());
		assertEquals(N1, target.replica().nodeId());
		assertEquals(7, target.metadataEpoch());
	}

	@Test
	void pointReadStaleExpectedMetadataEpochStaysTyped() {
		var router = new PartitionPointReadRouter(new PartitionRouteCache(metadata()));

        var error = assertThrows(PartitionReadRouteException.class,
                () -> router.routeLinearizableReadAtEpoch(6, pointCommand()));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals("stale read route metadata epoch 6; current epoch is 7", error.getMessage());
    }

	@Test
	void pointReadFutureExpectedMetadataEpochStaysTyped() {
		var router = new PartitionPointReadRouter(new PartitionRouteCache(metadata()));

        var error = assertThrows(PartitionReadRouteException.class,
                () -> router.routeLinearizableReadAtEpoch(8, pointCommand()));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals("future read route metadata epoch 8; current epoch is 7", error.getMessage());
    }

	@Test
	void rangeReadStaleExpectedMetadataEpochStaysTyped() {
		var router = new PartitionRangeReadRouter(new PartitionRouteCache(metadata()));

        var error = assertThrows(PartitionReadRouteException.class,
                () -> router.routeLinearizableScanAtEpoch(6, rangeCommand()));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
        assertEquals("stale range read route metadata epoch 6; current epoch is 7", error.getMessage());
    }

	@Test
	void rangeReadFutureExpectedMetadataEpochStaysTyped() {
		var router = new PartitionRangeReadRouter(new PartitionRouteCache(metadata()));

        var error = assertThrows(PartitionReadRouteException.class,
                () -> router.routeLinearizableScanAtEpoch(8, rangeCommand()));

        assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
		assertEquals("future range read route metadata epoch 8; current epoch is 7", error.getMessage());
	}

	@Test
	void rangeReadRoutesToOwningPartitionLeader() {
		var router = new PartitionRangeReadRouter(new PartitionRouteCache(metadata()));

		var target = router.routeLinearizableScan(rangeCommand());

		assertEquals(PARTITION, target.shardId());
		assertEquals(N1, target.replica().nodeId());
		assertEquals(7, target.metadataEpoch());
	}

    private static PartitionPointReadCommand pointCommand() {
        return new PartitionPointReadCommand(3, 4, new Keys(buf(0x18)));
    }

    private static PartitionRangeReadCommand rangeCommand() {
        return new PartitionRangeReadCommand(
                3,
                4,
                new Keys(buf(0x18)),
                new Keys(buf(0x1f)),
                false,
                128
        );
    }

	private static ClusterMetadata metadata() {
		var group = new ReplicaGroupMetadata(PARTITION, 3, N1, peers(), 3);
		var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
        var partition = new PartitionDescriptor(
                PARTITION,
                buf(0x10),
                buf(0x20),
                3,
                "group-7",
                peers(),
                N1,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
		return new ClusterMetadata(CLUSTER_ID, 7, Map.of(PARTITION, group), Map.of(4L, column),
				Map.of(PARTITION, partition));
	}

    private static List<ReplicaMember> peers() {
        return List.of(
                new ReplicaMember(N1, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 25, 25, true),
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
}
