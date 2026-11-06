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
	/**
	 * Largest log entry Ratis reads back from a segment (it uses the appender's buffer limit as its maximum entry
	 * size); {@link RaftLogRepair} reads with the same limit.
	 */
	static final SizeInBytes LOG_ENTRY_SIZE_MAX = SizeInBytes.valueOf(MAX_ENTRY_BYTES + (1 << 20));
	/** The last and longest step of {@link #APPENDER_RETRY_POLICY}. */
	private static final Duration APPENDER_RETRY_LAST_STEP = Duration.ofSeconds(2);
	/**
	 * What the leader's appender sleeps after failures to reach a follower: pairs of a step and how many failures use
	 * it, in the format of Ratis' MultipleLinearRandomRetry. Ratis' default, {@code 1ms,10, 1s,20, 5s,1000}, can leave a
	 * follower that stopped for a moment without a leader for 7.5 s, and {@link ApiGate} must wait out that sleep.
	 *
	 * <p>The count of the last step is that high on purpose: a follower must not reach the end of the policy, where Ratis
	 * no longer sleeps and retries a dead follower with only the heartbeat wait between attempts. With its default
	 * policy that happens to a follower down for about 40 minutes: a failed try counted about twice in a test run.</p>
	 */
	static final String APPENDER_RETRY_POLICY = "1ms,10, 1s,20, " + APPENDER_RETRY_LAST_STEP.toSeconds() + "s,1000000000";
	/** Ratis multiplies each step by a random number from 0.5 to 1.5, so a sleep can reach 1.5 times the step. */
	static final Duration APPENDER_RETRY_LONGEST_SLEEP = APPENDER_RETRY_LAST_STEP.multipliedBy(3).dividedBy(2);

	/**
	 * The timeout of a snapshot chunk is 8 times this ({@code install_snapshot.request.element-limit}), and Ratis keeps
	 * every chunk it sends on the heap, in the timer of that timeout, whether or not its reply came. So a leader that
	 * sends a snapshot holds what it sends in that time: with Ratis' default of 3 s, 24 s of sending, more than 1 GiB
	 * of a 1 GiB snapshot. At 4 s that is 800 MB at 200 MB/s. A follower that does not answer a chunk for 4 s makes the
	 * leader start the snapshot over. See "Heap" in the design document.
	 */
	private static final Duration INSTALL_SNAPSHOT_REQUEST_TIMEOUT = Duration.ofMillis(500);

	/**
	 * Nodes whose attempts in flight together fit in {@link #PENDING_BYTES_LIMIT}, the smallest limit; a node started
	 * with a longer peer list sizes its limit for that list ({@link #nodesUnderPendingLimit}).
	 */
	static final int NODES_UNDER_PENDING_LIMIT = 4;
	/** Ratis' pending-request count limit on the leader: its default. */
	static final int PENDING_REQUESTS_LIMIT = 4096;
	/**
	 * Tests only: when this system property is true, a node purges its log up to its latest snapshot even if a member
	 * has not committed that far, so that a member that lags gets a snapshot install. Never set in production.
	 */
	static final String PURGE_UP_TO_SNAPSHOT = "rockserver.test.purgeUpToSnapshot";
	/** Ratis' pending-request byte limit on the leader (default 64 MB) of a cluster of up to 4 nodes, see {@link #server}. */
	static final SizeInBytes PENDING_BYTES_LIMIT = pendingBytesLimit(NODES_UNDER_PENDING_LIMIT);

	/**
	 * Nodes whose attempts the leader's pending-request byte limit holds: those of its peer list, at least
	 * {@link #NODES_UNDER_PENDING_LIMIT}. Ratis reads the limit once, at the start, so a cluster that grew past it by
	 * {@code add-peer} keeps the smaller limit until each node restarts with the longer list; the leader warns meanwhile
	 * ({@code ReplicatedNode.warnAbovePendingLimit}).
	 */
	static int nodesUnderPendingLimit(NodeConfig config) {
		return Math.max(NODES_UNDER_PENDING_LIMIT, config.peers().size());
	}

	/** Every node's attempts in flight plus the leader's own transaction commits, which have permits of their own. */
	static SizeInBytes pendingBytesLimit(int nodes) {
		return SizeInBytes.valueOf((nodes + 1) * Proposer.ATTEMPT_BYTES_IN_FLIGHT);
	}

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
		RaftServerConfigKeys.Log.setSegmentSizeMax(properties, SizeInBytes.valueOf(config.logSegmentBytes()));
		// The corruption policy stays the default, EXCEPTION. WARN_AND_RETURN would let a node start after a power loss
		// tore the unsynced tail of its open segment, but in Ratis 3.3.1 it applies to closed segments too, which it
		// truncates: committed, acknowledged entries could disappear. A torn open segment is repaired offline instead,
		// by "ReplicatedServerMain repair-log" (RaftLogRepair), which never touches closed segments.
		RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, LOG_ENTRY_SIZE_MAX);
		RaftServerConfigKeys.Log.setWriteBufferSize(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));
		RaftServerConfigKeys.Log.setQueueByteLimit(properties, SizeInBytes.valueOf(4L * MAX_ENTRY_BYTES));
		// Group commit: entries that queue up during an fsync are written and synced together by the next one. Raise
		// the cap on one such group (default 128) and on the queue (default 4096): a full queue blocks appends while
		// holding the server lock. Async flush stays off: in Ratis 3.3.1 it marks as flushed the entries written while
		// the fsync ran, which it did not cover.
		RaftServerConfigKeys.Log.setForceSyncNum(properties, 4096);
		RaftServerConfigKeys.Log.setQueueElementLimit(properties, 65536);
		// Pending client requests on the leader. Ratis refuses more before the append with a ResourceUnavailableException
		// that Proposer recognizes by its message (pinned by RatisContractTest) and resends, as a refusal before the
		// append. Each node's proposer keeps at most 2 × Proposer.MAX_BATCHES_IN_FLIGHT batches and
		// Proposer.ATTEMPT_BYTES_IN_FLIGHT bytes of attempts in flight per lane (its own permits), and its writers are
		// batched: the element limit stays Ratis' default, and the byte limit covers the data writes of every node of the
		// peer list (at least NODES_UNDER_PENDING_LIMIT) plus the leader's own transaction commits, 160 MiB for up to 4
		// nodes instead of the 1 GiB this used to be. A remote attempt's permit is given back when its node stops waiting, so a long
		// commit stall can still fill it. The heap of writes is bounded on each node by Proposer's byte admission.
		RaftServerConfigKeys.Write.setElementLimit(properties, PENDING_REQUESTS_LIMIT);
		RaftServerConfigKeys.Write.setByteLimit(properties, pendingBytesLimit(nodesUnderPendingLimit(config)));
		// Every attempt at one proposer batch is the same Ratis invocation (see Proposer), so the retry cache answers a
		// resend while the first copy is still pending instead of appending it again. Every replica caches every
		// reply, with no size bound, so keep them only about two attempt timeouts, which covers the usual resend of a
		// copy still pending: Rockserver deduplicates requests itself (see Request), so a resend the cache misses is
		// only appended again. Ratis 3.3.1 offers no size bound for this cache, only the expiry.
		RaftServerConfigKeys.RetryCache.setExpiryTime(properties, TimeDuration.valueOf(
				Math.max(6_000, 4 * config.electionTimeoutMax().toMillis()), TimeUnit.MILLISECONDS));
		GrpcConfigKeys.setMessageSizeMax(properties, SizeInBytes.valueOf(MAX_ENTRY_BYTES + (2 << 20)));
		// AppendEntries calls in flight to each follower (default 8). A follower fsyncs once per call it processes, so
		// with many calls of one entry each its fsync rate caps the commit rate; with fewer calls in flight, the next
		// call carries every entry appended meanwhile, and they cost one fsync together.
		GrpcConfigKeys.Server.setLeaderOutstandingAppendsMax(properties, 2);

		// Reads use ReadBarrier, not Ratis ReadIndex (see ReadBarrier). Keep Ratis' own read path strict anyway. Its
		// heartbeat check (the default, set here so that it stays) is what CommitHeartbeats triggers: a read on the leader
		// makes the appenders send a heartbeat round, with the commit index, at once.
		RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
		RaftServerConfigKeys.Read.setLeaderHeartbeatCheckEnabled(properties, true);
		RaftServerConfigKeys.Read.setLeaderLeaseEnabled(properties, false);

		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, duration(config.electionTimeoutMin()));
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, duration(config.electionTimeoutMax()));
		RaftServerConfigKeys.Rpc.setRequestTimeout(properties, duration(config.electionTimeoutMax()));
		RaftServerConfigKeys.LeaderElection.setPreVote(properties, true);
		// A leader that lost its majority starts no election for this long (default 10 s). With one node of three down and
		// the other one restarting it may be the only node that can win, and pre-votes keep it from disturbing anyone.
		RaftServerConfigKeys.LeaderElection.setLeaderStepDownWaitTime(properties, duration(config.electionTimeoutMax()));
		// After a JVM pause longer than this Ratis closes the server. ReplicatedNode then stops the node: see "Long pauses"
		// in the design document.
		RaftServerConfigKeys.setCloseThreshold(properties, duration(config.jvmPauseLimit()));

		// Snapshots are RocksDB checkpoints; the log is purged up to the latest one.
		RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
		RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(properties, config.snapshotEveryEntries());
		// Not at every stop or removal (the defaults): a graceful stop flushes the database, and a restart resumes from
		// its own applied position, so a snapshot then would only delay the exit.
		RaftServerConfigKeys.Snapshot.setTriggerWhenStopEnabled(properties, false);
		RaftServerConfigKeys.Snapshot.setTriggerWhenRemoveEnabled(properties, false);
		// Purge only what every member already committed: a follower that is restarting or installing a snapshot
		// can then catch up from the log afterwards, instead of needing a new snapshot install forever. A member
		// that is gone for good holds the log back until it is removed from the configuration. Followers too: each
		// learns every member's commit index from the leader, so none purges past a member that lags. Tests that need a
		// member to lag past the purged log (an install to an existing member) turn this off, see PURGE_UP_TO_SNAPSHOT.
		RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, Boolean.getBoolean(PURGE_UP_TO_SNAPSHOT));
		RaftServerConfigKeys.Log.setPurgeGap(properties, 1024);
		// Keep a margin of log behind the snapshot, so that a member restarted during a deploy catches up from the
		// log instead of downloading the whole database.
		RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, config.snapshotEveryEntries());
		// Allow the snapshot requested right after a seeded genesis, whatever the gap since the last one.
		RaftServerConfigKeys.Snapshot.setCreationGap(properties, 1);
		RaftServerConfigKeys.Log.Appender.setRetryPolicy(properties, APPENDER_RETRY_POLICY);
		RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, true);
		RaftServerConfigKeys.Log.Appender.setSnapshotChunkSizeMax(properties, SizeInBytes.valueOf("8MB"));
		GrpcConfigKeys.Server.setInstallSnapshotRequestTimeout(properties, duration(INSTALL_SNAPSHOT_REQUEST_TIMEOUT));
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
