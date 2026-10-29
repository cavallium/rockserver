package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

/** Ratis settings. Safety-relevant values are set explicitly instead of relying on library defaults. */
final class RaftSettings {

	/** Upper bound of one encoded log entry; larger writes are split by the caller. */
	static final int MAX_ENTRY_BYTES = 16 * 1024 * 1024;

	private RaftSettings() {}

	static RaftProperties server(NodeConfig config) {
		var properties = new RaftProperties();
		RaftServerConfigKeys.setStorageDir(properties, List.of(config.raftDir().toFile()));
		var self = config.self();
		GrpcConfigKeys.Server.setPort(properties, self.raftPort());
		if (config.raftBindHost() != null) {
			GrpcConfigKeys.Server.setHost(properties, config.raftBindHost());
		}

		// Durability: an entry counts toward a quorum only after it is fsynced to the local Raft log.
		RaftServerConfigKeys.Log.setUseMemory(properties, false);
		RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
		RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
		// Commit metadata entries only speed up restarts; without them every log entry is a data or
		// configuration entry, which keeps the applied-index bookkeeping simple.
		RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, false);
		RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf("64MB"));
		RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (1 << 20)));
		RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf(4L * MAX_ENTRY_BYTES));
		GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));

		// Reads use ReadBarrier, not Ratis ReadIndex (see ReadBarrier). Keep Ratis' own read path strict anyway.
		RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
		RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, false);

		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, duration(config.electionTimeoutMin()));
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, duration(config.electionTimeoutMax()));
		RaftServerConfigKeys.Rpc.setRequestTimeout(properties, duration(config.electionTimeoutMax()));
		RaftServerConfigKeys.LeaderElection.setPreVote(properties, true);

		// Snapshots are RocksDB checkpoints; the log is purged up to the latest one.
		RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
		RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, config.snapshotEveryEntries());
		RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
		RaftServerConfigKeys.Log.setPurgeGap(properties, 1024);
		RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
		RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(properties, SizeInBytes.valueOf("8MB"));
		return properties;
	}

	static RaftProperties client(NodeConfig config) {
		var properties = new RaftProperties();
		RaftClientConfigKeys.Rpc.setRequestTimeout(properties, duration(config.electionTimeoutMax()));
		GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));
		return properties;
	}

	static TimeDuration duration(Duration value) {
		return TimeDuration.valueOf(value.toMillis(), TimeUnit.MILLISECONDS);
	}
}
