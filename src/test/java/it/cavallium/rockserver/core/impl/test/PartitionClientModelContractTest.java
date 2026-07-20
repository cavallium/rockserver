package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadRoute;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursor;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursorCodec;
import it.cavallium.rockserver.core.cluster.client.PartitionReadRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteRouteException;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.client.RoutingPolicy;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.common.Keys;

import java.net.URI;

import org.junit.jupiter.api.Test;

class PartitionClientModelContractTest {

    private static final ShardId SHARD = new ShardId(7);
    private static final NodeId NODE = NodeId.of("n1");

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var command = command();
        var target = routeTarget();

        assertIllegalArgument("kind must not be null",
                () -> new PartitionReadRouteException(null, "read failed"));
        assertIllegalArgument("kind must not be null",
                () -> new PartitionWriteRouteException(null, "write failed"));
        assertIllegalArgument("refreshedMetadata must not be null",
                () -> PartitionWriteRouteException.notLeader(null));
        assertIllegalArgument("consistency must not be null",
                () -> new RoutingPolicy(null, 0, false));
        assertIllegalArgument("shardId must not be null",
                () -> new RouteTarget(null, replica(), true, ReplicatedCommandKind.ROUTED_READ, 7));
        assertIllegalArgument("replica must not be null",
                () -> new RouteTarget(SHARD, null, true, ReplicatedCommandKind.ROUTED_READ, 7));
        assertIllegalArgument("commandKind must not be null",
                () -> new RouteTarget(SHARD, replica(), true, null, 7));
        assertIllegalArgument("command must not be null",
                () -> new PartitionRangeScanCursor(7, null, 0));
        assertIllegalArgument("target must not be null",
                () -> new PartitionRangeReadRoute(null, command));
        assertIllegalArgument("command must not be null",
                () -> new PartitionRangeReadRoute(target, null));
        assertIllegalArgument("cursor must not be null",
                () -> PartitionRangeScanCursorCodec.encode(null));
        assertIllegalArgument("payload must not be null",
                () -> PartitionRangeScanCursorCodec.decode(null));
    }

    private static RouteTarget routeTarget() {
        return new RouteTarget(SHARD, replica(), true, ReplicatedCommandKind.ROUTED_READ, 7);
    }

    private static ReplicaMember replica() {
        return new ReplicaMember(
                NODE,
                URI.create("grpc://127.0.0.1:10001"),
                ReplicaRole.VOTER,
                25,
                25,
                true
        );
    }

    private static PartitionRangeReadCommand command() {
        return new PartitionRangeReadCommand(3, 4, new Keys(buf(0x18)), null, false, 10);
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
