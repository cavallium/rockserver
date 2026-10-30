package it.cavallium.rockserver.distributed;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * Configuration of one replicated node.
 *
 * @param nodeId id of this node, must appear in {@code peers}
 * @param peers initial voting members, identical on every node
 * @param dataDir node data directory: {@code dataDir/db} holds the RocksDB database, {@code dataDir/raft} the Raft log
 *                and snapshots. Both must be on the same filesystem (checkpoints use hard links)
 * @param rockserverConfig Rockserver database config file, identical on every node; null for defaults
 * @param raftBindHost host the Raft server binds to; null binds all interfaces
 * @param apiBindHost host the Rockserver API binds to; null uses this node's {@link PeerConfig#apiHost()}
 * @param groupId Raft group id (a UUID) shared by the whole cluster
 * @param electionTimeoutMin minimum Raft election timeout
 * @param electionTimeoutMax maximum Raft election timeout
 * @param snapshotEveryEntries take a checkpoint snapshot after this many applied log entries (each holds a batch
 *                             of requests); a snapshot flushes the memtables and briefly stops the apply, and
 *                             the log is kept for up to twice this many entries
 * @param requestTimeout how long a node waits for a leader or a Raft reply before failing a request as retryable
 * @param maxTransactionLease longest lease of a transaction opened on the leader: a longer requested lease is
 *                            shortened to this, since an open transaction keeps the leader's write history
 * @param logSegmentBytes size at which a segment of the Raft log is closed and the next one started
 * @param pipelinedUpdates on the leader, pass a key to its next {@code getForUpdate} update as soon as the commit of
 *                         the previous one is admitted to the log, instead of after its apply. The next update then
 *                         reads the value from leader memory, provisionally: see the design document. It changes
 *                         nothing but this node's behavior as a leader, so nodes may differ. Off by default
 */
public record NodeConfig(String nodeId,
		List<PeerConfig> peers,
		Path dataDir,
		@Nullable Path rockserverConfig,
		@Nullable String raftBindHost,
		@Nullable String apiBindHost,
		String groupId,
		Duration electionTimeoutMin,
		Duration electionTimeoutMax,
		long snapshotEveryEntries,
		Duration requestTimeout,
		Duration maxTransactionLease,
		long logSegmentBytes,
		boolean pipelinedUpdates) {

	public static final String DEFAULT_GROUP_ID = "5e3b8c1a-7a52-4b8e-9d2c-0c9f6a3d4e71";
	/** One snapshot every minute or two at a few thousand entries per second. */
	public static final long DEFAULT_SNAPSHOT_EVERY_ENTRIES = 500_000;
	public static final Duration DEFAULT_MAX_TRANSACTION_LEASE = Duration.ofSeconds(60);
	public static final long DEFAULT_LOG_SEGMENT_BYTES = 64L << 20;

	public NodeConfig {
		Objects.requireNonNull(nodeId, "nodeId");
		peers = List.copyOf(peers);
		Objects.requireNonNull(dataDir, "dataDir");
		Objects.requireNonNull(groupId, "groupId");
		if (peers.stream().noneMatch(p -> p.id().equals(nodeId))) {
			throw new IllegalArgumentException("Node " + nodeId + " is not in the peer list");
		}
		if (peers.stream().map(PeerConfig::id).distinct().count() != peers.size()) {
			throw new IllegalArgumentException("Duplicate peer ids");
		}
		if (electionTimeoutMin.compareTo(electionTimeoutMax) >= 0) {
			throw new IllegalArgumentException("electionTimeoutMin must be lower than electionTimeoutMax");
		}
		if (snapshotEveryEntries <= 0) {
			throw new IllegalArgumentException("snapshotEveryEntries must be positive");
		}
		if (maxTransactionLease.isNegative() || maxTransactionLease.isZero()) {
			throw new IllegalArgumentException("maxTransactionLease must be positive");
		}
		if (logSegmentBytes <= 0) {
			throw new IllegalArgumentException("logSegmentBytes must be positive");
		}
	}

	public static NodeConfig defaults(String nodeId, List<PeerConfig> peers, Path dataDir) {
		return new NodeConfig(nodeId, peers, dataDir, null, null, null, DEFAULT_GROUP_ID,
				Duration.ofMillis(1000), Duration.ofMillis(2000), DEFAULT_SNAPSHOT_EVERY_ENTRIES, Duration.ofSeconds(10),
				DEFAULT_MAX_TRANSACTION_LEASE, DEFAULT_LOG_SEGMENT_BYTES, false);
	}

	public PeerConfig self() {
		return peer(nodeId);
	}

	public PeerConfig peer(String id) {
		return peers.stream().filter(p -> p.id().equals(id)).findFirst()
				.orElseThrow(() -> new IllegalArgumentException("Unknown peer " + id));
	}

	public Path dbDir() {
		return dataDir.resolve("db");
	}

	public Path raftDir() {
		return dataDir.resolve("raft");
	}

	public NodeConfig withRockserverConfig(@Nullable Path value) {
		return new NodeConfig(nodeId, peers, dataDir, value, raftBindHost, apiBindHost, groupId,
				electionTimeoutMin, electionTimeoutMax, snapshotEveryEntries, requestTimeout, maxTransactionLease,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withBindHosts(@Nullable String raft, @Nullable String api) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raft, api, groupId,
				electionTimeoutMin, electionTimeoutMax, snapshotEveryEntries, requestTimeout, maxTransactionLease,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withSnapshotEveryEntries(long value) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raftBindHost, apiBindHost, groupId,
				electionTimeoutMin, electionTimeoutMax, value, requestTimeout, maxTransactionLease,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withElectionTimeouts(Duration min, Duration max) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raftBindHost, apiBindHost, groupId,
				min, max, snapshotEveryEntries, requestTimeout, maxTransactionLease,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withRequestTimeout(Duration value) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raftBindHost, apiBindHost, groupId,
				electionTimeoutMin, electionTimeoutMax, snapshotEveryEntries, value, maxTransactionLease,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withMaxTransactionLease(Duration value) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raftBindHost, apiBindHost, groupId,
				electionTimeoutMin, electionTimeoutMax, snapshotEveryEntries, requestTimeout, value,
				logSegmentBytes, pipelinedUpdates);
	}

	public NodeConfig withPipelinedUpdates(boolean value) {
		return new NodeConfig(nodeId, peers, dataDir, rockserverConfig, raftBindHost, apiBindHost, groupId,
				electionTimeoutMin, electionTimeoutMax, snapshotEveryEntries, requestTimeout, maxTransactionLease,
				logSegmentBytes, value);
	}
}
