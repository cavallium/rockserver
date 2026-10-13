package it.cavallium.rockserver.distributed.ratis;

import java.nio.file.Path;
import java.util.List;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;

/** Safety settings are explicit rather than inherited from library defaults. */
public final class RatisDurability {
    private RatisDurability() {}
    public static RaftProperties properties(Path logDirectory) {
        var properties = new RaftProperties();
        RaftServerConfigKeys.setStorageDir(properties, List.of(logDirectory.toFile()));
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(RatisRangeStateMachine.COMMITTED_SEGMENT_BYTES));
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
        RaftServerConfigKeys.Log.setQueueElementLimit(properties, 1024);
        RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf("16MB"));
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, false);
        RaftServerConfigKeys.Read.setLeaderHeartbeatCheckEnabled(properties, true);
        RaftServerConfigKeys.Read.ReadIndex.setType(properties, RaftServerConfigKeys.Read.ReadIndex.Type.COMMIT_INDEX);
        // Until portable snapshot publication is implemented, keep every log entry.
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
        return properties;
    }
}
