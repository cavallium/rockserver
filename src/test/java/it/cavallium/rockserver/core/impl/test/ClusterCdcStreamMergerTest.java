package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ClusterCdcStreamMerger;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcEvent;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationOp;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class ClusterCdcStreamMergerTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var partitionId = new ShardId(1);
        var event = event(partitionId, 10, 0, "p1-10");
        var merger = new ClusterCdcStreamMerger();

        assertIllegalArgument("partitionEvents must not be null",
                () -> merger.mergePage(null, 10));

        var nullPartitionIdEvents = new HashMap<ShardId, List<ReplicatedCdcEvent>>();
        nullPartitionIdEvents.put(null, List.of(event));
        assertIllegalArgument("partitionId must not be null",
                () -> merger.mergePage(nullPartitionIdEvents, 10));

        var nullEvents = new HashMap<ShardId, List<ReplicatedCdcEvent>>();
        nullEvents.put(partitionId, null);
        assertIllegalArgument("events must not be null",
                () -> merger.mergePage(nullEvents, 10));

        assertIllegalArgument("event must not be null",
                () -> merger.mergePage(Map.of(partitionId, Collections.singletonList(null)), 10));
        assertIllegalArgument("events must not be null",
                () -> new ClusterCdcStreamMerger.ClusterCdcPage(null, Map.of()));
        assertIllegalArgument("events[0] must not be null",
                () -> new ClusterCdcStreamMerger.ClusterCdcPage(Collections.singletonList(null), Map.of()));
        assertIllegalArgument("nextPartitionTokens must not be null",
                () -> new ClusterCdcStreamMerger.ClusterCdcPage(List.of(event), null));

        var nullTokenPartitionId = new HashMap<ShardId, ReplicatedCdcToken>();
        nullTokenPartitionId.put(null, event.token());
        assertIllegalArgument("partitionId must not be null",
                () -> new ClusterCdcStreamMerger.ClusterCdcPage(List.of(event), nullTokenPartitionId));

        var nullToken = new HashMap<ShardId, ReplicatedCdcToken>();
        nullToken.put(partitionId, null);
        assertIllegalArgument("nextPartitionToken must not be null",
                () -> new ClusterCdcStreamMerger.ClusterCdcPage(List.of(event), nullToken));
    }

    @Test
    void mergesPartitionLocalStreamsWithoutGlobalOrdering() {
        var p1 = new ShardId(1);
        var p2 = new ShardId(2);
        var p1Event10 = event(p1, 10, 0, "p1-10");
        var p1Event11 = event(p1, 11, 0, "p1-11");
        var p2Event3 = event(p2, 3, 0, "p2-3");

        var page = new ClusterCdcStreamMerger().mergePage(Map.of(
                p1, List.of(p1Event10, p1Event11),
                p2, List.of(p2Event3)
        ), 3);

        assertEquals(List.of(p1Event10, p2Event3, p1Event11), page.events());
        assertEquals(List.of(p1, p2), page.partitionsWithProgress());
        assertEquals(p1Event11.token(), page.nextPartitionTokens().get(p1));
        assertEquals(p2Event3.token(), page.nextPartitionTokens().get(p2));
    }

    @Test
    void limitsPageAndReturnsOnlyAdvancedPartitionTokens() {
        var p1 = new ShardId(1);
        var p2 = new ShardId(2);
        var p1Event = event(p1, 10, 0, "p1-10");
        var p2Event = event(p2, 3, 0, "p2-3");

        var page = new ClusterCdcStreamMerger().mergePage(Map.of(
                p1, List.of(p1Event),
                p2, List.of(p2Event)
        ), 1);

        assertEquals(List.of(p1Event), page.events());
        assertEquals(Map.of(p1, p1Event.token()), page.nextPartitionTokens());
    }

    @Test
    void rejectsPartitionMismatchAndOutOfOrderEvents() {
        var p1 = new ShardId(1);
        var p2 = new ShardId(2);
        var merger = new ClusterCdcStreamMerger();

        assertThrows(IllegalArgumentException.class, () -> merger.mergePage(Map.of(
                p1, List.of(event(p2, 1, 0, "wrong"))
        ), 10));
        assertThrows(IllegalArgumentException.class, () -> merger.mergePage(Map.of(
                p1, List.of(event(p1, 2, 0, "later"), event(p1, 1, 0, "earlier"))
        ), 10));
        assertThrows(IllegalArgumentException.class, () -> merger.mergePage(Map.of(
                p1, List.of(event(p1, 1, 1, "later"), event(p1, 1, 0, "earlier"))
        ), 10));
    }

    @Test
    void rejectsInvalidPageSizeBeforeAllocatingResultList() {
        var merger = new ClusterCdcStreamMerger();

        assertEquals("maxEvents must be positive",
                assertThrows(IllegalArgumentException.class, () -> merger.mergePage(Map.of(), 0))
                        .getMessage());
        assertEquals("maxEvents must be <= " + ClusterCdcStreamMerger.MAX_EVENTS_PER_PAGE
                        + ": " + Integer.MAX_VALUE,
                assertThrows(IllegalArgumentException.class,
                        () -> merger.mergePage(Map.of(), Integer.MAX_VALUE))
                        .getMessage());
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }

    private static ReplicatedCdcEvent event(ShardId partitionId, long appliedIndex, int eventOrdinal, String value) {
        return new ReplicatedCdcEvent(
                new ReplicatedCdcToken(7, partitionId, 3, appliedIndex, eventOrdinal),
                11,
                4,
                text(value + "-key"),
                text(value),
                ReplicatedMutationOp.PUT
        );
    }

    private static Buf text(String value) {
        return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
    }
}
