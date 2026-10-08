package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.Path;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

/** Leadership callback invalidation must not fence ordinary follower apply. */
class RatisScopedHistoryTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void leaderLossInvalidatesTrackedPinsWhileFollowerMetadataContinues() throws Exception {
        try (var f=new RatisSessionPropagationTest.Fixture(root)) {
            var group=f.groups.getFirst(); var store=f.stores.get(group); var machine=f.machines.get(group); store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(f.outer.data,new byte[]{7})));
            try (var pin=store.captureTracked(scope)) {
                machine.notifyNotLeader(List.of()); assertEquals(RangeMutationHistory.Status.INVALIDATED,pin.status()); assertEquals(0,store.activeSnapshotPins());
                assertEquals(RangeOwnership.State.ACTIVE,store.ownership().state()); machine.notifyTermIndexUpdated(1,1);
                assertEquals(new ApplyPosition(1,1),store.applied()); assertEquals(RangeMutationHistory.Status.INVALIDATED,pin.validate());
            }
        }
    }
}
