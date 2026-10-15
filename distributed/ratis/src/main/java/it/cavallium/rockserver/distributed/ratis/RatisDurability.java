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
        // Cold division loading shares one bounded log-reader workspace across this node.
        RaftServerConfigKeys.ThreadPool.setProxyCached(properties,false);
        RaftServerConfigKeys.ThreadPool.setProxySize(properties,1);
        RaftServerConfigKeys.setStorageDir(properties, List.of(logDirectory.toFile()));
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        // Framing streams large legal entries through this small, lifetime-owned buffer.
        RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(64L * 1024));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(
                it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES + 4096L));
        // Peer replication must carry the admitted batch plus Ratis's required framing headroom.
        org.apache.ratis.grpc.GrpcConfigKeys.setMessageSizeMax(properties,SizeInBytes.valueOf(Math.max(
                RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSize()+SizeInBytes.ONE_MB.getSize(),
                RatisTopologySourceSnapshotRpc.MAX_REPLY_BYTES+4096L)));
        RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(RatisRangeStateMachine.COMMITTED_SEGMENT_BYTES));
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
        RaftServerConfigKeys.Log.setQueueElementLimit(properties, 1024);
        // Each new row reserves its data and future metadata IO tasks before append.
        RaftServerConfigKeys.Log.Appender.setBufferElementLimit(properties,128);
        RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf(it.cavallium.rockserver.core.common.distributed.NativeSstImportManifest.MAX_FILE_BYTES+8192L));
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, false);
        RaftServerConfigKeys.Read.setLeaderHeartbeatCheckEnabled(properties, true);
        RaftServerConfigKeys.Read.ReadIndex.setType(properties, RaftServerConfigKeys.Read.ReadIndex.Type.COMMIT_INDEX);
        // Running recovery snapshots are requested locally; the updater owns publication and log purge.
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
        return properties;
    }
}
