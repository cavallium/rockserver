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
		// The open segment is preallocated with zeros, so after a power loss in the middle of a group commit its tail
		// reads back as a bad checksum or terminator rather than as its end, and the default policy (EXCEPTION) keeps
		// the node from starting. Those entries were never fsynced, so they never counted toward a quorum or an
		// acknowledgement: drop them and start. Closed segments are still checked and fail loudly.
		RaftServerConfigKeys.Log.setCorruptionPolicy(properties, RaftServerConfigKeys.Log.CorruptionPolicy.WARN_AND_RETURN);
		RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (1 << 20)));
		RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));
		RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf(4L * MAX_ENTRY_BYTES));
		// Group commit: entries that queue up during an fsync are written and synced together by the next one. Raise
		// the cap on one such group (default 128) and on the queue (default 4096): a full queue blocks appends while
		// holding the server lock. Async flush stays off: in Ratis 3.3.1 it marks as flushed the entries written while
		// the fsync ran, which it did not cover.
		RaftServerConfigKeys.Log.setForceSyncNum(properties, 4096);
		RaftServerConfigKeys.Log.setQueueElementLimit(properties, 65536);
		// Pending client requests on the leader (defaults 4096 and 64MB): with thousands of writers in flight, a full
		// queue fails writes with ResourceUnavailable instead of letting them join the next group commit.
		RaftServerConfigKeys.Write.setElementLimit(properties, 65536);
		RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes.valueOf(64L * MAX_ENTRY_BYTES));
		// Every resend carries a new call id and Rockserver deduplicates requests itself (see Request), so the Ratis
		// retry cache only holds replies nobody asks for again: keep them briefly instead of 60s.
		RaftServerConfigKeys.RetryCache.setExpiryTime(properties, TimeDuration.valueOf(5, TimeUnit.SECONDS));
		GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));
		// AppendEntries calls in flight to each follower (default 8). A follower fsyncs once per call it processes, so
		// with many calls of one entry each its fsync rate caps the commit rate; with fewer calls in flight, the next
		// call carries every entry appended meanwhile, and they cost one fsync together.
		GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, 2);

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
		// Not at every stop or removal (the defaults): a graceful stop flushes the database, and a restart resumes from
		// its own applied position, so a snapshot then would only delay the exit.
		RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
		RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
		// Purge only what every member already committed: a follower that is restarting or installing a snapshot
		// can then catch up from the log afterwards, instead of needing a new snapshot install forever. A member
		// that is gone for good holds the log back until it is removed from the configuration.
		RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, false);
		RaftServerConfigKeys.Log.setPurgeGap(properties, 1024);
		// Keep a margin of log behind the snapshot, so that a member restarted during a deploy catches up from the
		// log instead of downloading the whole database.
		RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, config.snapshotEveryEntries());
		// Allow the snapshot requested right after a seeded genesis, whatever the gap since the last one.
		RaftServerConfigKeys.Snapshot.setCreationGap(properties, 1);
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
