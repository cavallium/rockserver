package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One member of a replicated Rockserver cluster: the local database, the Raft server and the Rockserver gRPC API
 * (bound to this node's {@link PeerConfig#apiPort()}). Any member serves the full API; requests that need the
 * leader are forwarded to it.
 */
public final class ReplicatedNode implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(ReplicatedNode.class);
	private static final long LEADERSHIP_TRANSFER_TIMEOUT_MS = 5_000;

	private final NodeConfig config;
	private final RaftGroup group;
	private final RockserverStateMachine stateMachine;
	private final RaftServer server;
	private final ReadBarrier barrier;
	private final Proposer proposer;
	private final LeaderForwarder forwarder;
	private final ReplicatedConnection connection;
	private final GrpcServer apiServer;
	private final ScheduledExecutorService maintenance;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final CompletableFuture<@Nullable Throwable> stopped = new CompletableFuture<>();
	private final NodeMetrics metrics;

	private ReplicatedNode(NodeConfig config) throws IOException {
		this.config = config;
		Files.createDirectories(config.dbDir().getParent());
		Files.createDirectories(config.raftDir());
		var groupId = RaftGroupId.valueOf(UUID.fromString(config.groupId()));
		this.group = RaftGroup.valueOf(groupId, raftPeers(config));
		this.metrics = new NodeMetrics(config.nodeId());
		this.stateMachine = new RockserverStateMachine(config, this::onReplicaFailure, metrics);

		var parameters = new Parameters();
		var apiAddress = new HostAndPort(config.self().apiHost(), config.self().apiPort());
		GrpcConfigKeys.Server.setServicesCustomizer(parameters, TermService.customizer(this::raftServer, apiAddress));
		boolean existing = Files.isDirectory(config.raftDir().resolve(groupId.getUuid().toString()));
		if (!existing) {
			requireNoClusterData(config);
		}
		this.server = RaftServer.newBuilder()
				.setServerId(RaftPeerId.valueOf(config.nodeId()))
				.setGroup(group)
				.setStateMachine(stateMachine)
				.setProperties(RaftSettings.server(config))
				.setParameters(parameters)
				.setOption(existing ? RaftStorage.StartupOption.RECOVER : RaftStorage.StartupOption.FORMAT)
				.build();

		var started = new ArrayList<Closeable>();
		try {
			server.start();
			started.add(server);
			this.barrier = new ReadBarrier(server, groupId, stateMachine.appliedIndex(), config.requestTimeout());
			started.add(barrier);
			this.proposer = new Proposer(group, RaftSettings.client(config), config, this::raftServer, metrics);
			started.add(proposer);
			this.forwarder = new LeaderForwarder(config);
			started.add(forwarder);
			this.connection = new ReplicatedConnection(config, stateMachine, proposer, barrier, forwarder, metrics);
			var self = config.self();
			var apiHost = config.apiBindHost() != null ? config.apiBindHost() : self.apiHost();
			this.apiServer = new GrpcServer(connection, new InetSocketAddress(apiHost, self.apiPort()));
			apiServer.start();
			started.add(apiServer);
			this.maintenance = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-maintenance").factory());
			maintenance.scheduleWithFixedDelay(this::initializeCluster, 0, 200, TimeUnit.MILLISECONDS);
			var appendLag = metrics.amount("lag.uncommitted");
			var applyLag = metrics.amount("lag.unapplied");
			var flushLag = metrics.amount("lag.unflushed");
			metrics.sampleEveryMillisecond(() -> {
				RaftLog log;
				try {
					log = server.getDivision(group.getGroupId()).getRaftLog();
				} catch (IOException e) {
					return;
				}
				var lastEntry = log.getLastEntryTermIndex();
				long last = lastEntry == null ? 0 : lastEntry.getIndex();
				long committed = log.getLastCommittedIndex();
				appendLag.record(last - committed);
				flushLag.record(last - log.getFlushIndex());
				applyLag.record(committed - stateMachine.appliedIndex().get());
			});
		} catch (IOException | RuntimeException ex) {
			for (int i = started.size() - 1; i >= 0; i--) {
				try {
					started.get(i).close();
				} catch (IOException | RuntimeException closeError) {
					ex.addSuppressed(closeError);
				}
			}
			throw ex;
		}
		LOG.info("{}: started, Raft {}, API {}:{}", config.nodeId(), config.self().raftAddress(),
				config.self().apiHost(), config.self().apiPort());
	}

	/** Starts the node; it joins the group and serves the API. */
	public static ReplicatedNode start(NodeConfig config) throws IOException {
		return new ReplicatedNode(config);
	}

	private static List<RaftPeer> raftPeers(NodeConfig config) {
		return config.peers().stream()
				.map(peer -> RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build())
				.toList();
	}

	private RaftServer raftServer() {
		return server;
	}

	/**
	 * Proposes the {@link LogEntry.Genesis} entry once, from the first ready leader, then stops. On a cluster
	 * seeded from an existing database it also takes a snapshot right away, so that members added later receive
	 * the data as a snapshot rather than by replaying a log that does not contain it.
	 */
	private void initializeCluster() {
		try {
			var store = stateMachine.store();
			if (store.clusterId() != null) {
				maintenance.shutdown();
				return;
			}
			var division = stateMachine.division();
			if (division == null || !division.getInfo().isLeader() || !division.getInfo().isLeaderReady()) {
				return;
			}
			boolean seeded = store.hasUserColumns();
			LOG.info("{}: initializing cluster {} (seeded: {})", config.nodeId(), config.groupId(), seeded);
			proposer.propose(new LogEntry.Genesis(config.groupId(), seeded)).get(config.requestTimeout().toMillis(),
					TimeUnit.MILLISECONDS);
			if (seeded) {
				proposer.client().getSnapshotManagementApi(RaftPeerId.valueOf(config.nodeId()))
						.create(config.requestTimeout().toMillis());
			}
		} catch (Exception ex) {
			LOG.warn("{}: cluster initialization attempt failed, retrying", config.nodeId(), ex);
		}
	}

	/**
	 * A node without Raft state starts a fresh member (term 0, no vote). Its database must then not belong to the
	 * cluster already: a member that lost its Raft state but kept its id could vote twice in one term. Such a node
	 * must be replaced: wipe it and add it back under a new id.
	 */
	private static void requireNoClusterData(NodeConfig config) throws IOException {
		if (!Files.isDirectory(config.dbDir())) {
			return;
		}
		try (var store = NodeStore.open(config.dbDir(), config.rockserverConfig(), config.nodeId())) {
			if (store.clusterId() != null) {
				throw new IOException("Node " + config.nodeId() + " has the database of cluster " + store.clusterId()
						+ " but no Raft state in " + config.raftDir() + ": it cannot rejoin under the same id."
						+ " Remove it from the cluster, wipe its data directory and add it back under a new id");
			}
		}
	}

	/** Completes when the node stopped: normally with null, or with the failure that stopped its replica. */
	public CompletableFuture<@Nullable Throwable> stopped() {
		return stopped;
	}

	private void onReplicaFailure(Throwable error) {
		// Leave the group: a replica that cannot apply the log must not vote or lead.
		Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-failure").start(() -> {
			try {
				shutdown(true, error);
			} catch (IOException | RuntimeException ex) {
				LOG.error("{}: failed to stop after a replica failure", config.nodeId(), ex);
			}
		});
	}

	public NodeConfig config() {
		return config;
	}

	/** In-process view of the cluster API through this node, with the same routing as the gRPC API. */
	public RocksDBConnection connection() {
		return connection;
	}

	public boolean isLeader() {
		var division = stateMachine.division();
		return division != null && division.getInfo().isLeader();
	}

	public @Nullable String leaderId() {
		var division = stateMachine.division();
		var leader = division != null ? division.getInfo().getLeaderId() : null;
		return leader != null ? leader.toString() : null;
	}

	public long appliedIndex() {
		return stateMachine.appliedIndex().get();
	}

	/** The local replica, for tests that inspect each node's own database. */
	RockserverStateMachine stateMachine() {
		return stateMachine;
	}

	/** True once this replica failed to apply the log and stopped. */
	public boolean isFailed() {
		return stateMachine.isFailed();
	}

	/** Graceful stop: hands leadership to another member first, then closes everything. */
	@Override
	public void close() throws IOException {
		if (closed.get()) {
			return;
		}
		if (isLeader()) {
			transferLeadership();
		}
		shutdown(false, null);
	}

	/** Abrupt stop for tests: no leadership hand-off. */
	public void halt() throws IOException {
		shutdown(false, null);
	}

	/**
	 * Hands leadership to one follower, chosen among those that answered recently: the one with the highest commit
	 * index. Only one attempt is made: while a transfer is pending, Ratis refuses writes and refuses transfers to
	 * any other peer, and a failed attempt can stay pending until its timeout. The request goes straight to the
	 * local Raft server, not through a client that may still route it to a stopped former leader.
	 */
	private void transferLeadership() {
		var transferee = transferee();
		if (transferee == null) {
			LOG.warn("{}: no follower answered recently, stopping as leader without a hand-off", config.nodeId());
			return;
		}
		try {
			var request = new TransferLeadershipRequest(ClientId.randomId(), server.getId(), group.getGroupId(),
					0, transferee, LEADERSHIP_TRANSFER_TIMEOUT_MS);
			var reply = server.transferLeadership(request);
			if (reply.isSuccess()) {
				LOG.info("{}: leadership transferred to {}", config.nodeId(), transferee);
				return;
			}
			LOG.warn("{}: leadership transfer to {} failed", config.nodeId(), transferee, reply.getException());
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: leadership transfer to {} failed", config.nodeId(), transferee, ex);
		}
		LOG.warn("{}: stopping as leader without a hand-off", config.nodeId());
	}

	/** The most up-to-date follower that answered this leader within an election timeout, null if none. */
	private @Nullable RaftPeerId transferee() {
		var division = stateMachine.division();
		if (division == null || !division.getInfo().isLeader()) {
			return null;
		}
		var commitIndices = new HashMap<RaftPeerId, Long>();
		for (var info : division.getCommitInfos()) {
			commitIndices.put(RaftPeerId.valueOf(info.getServer().getId()), info.getCommitIndex());
		}
		long recentMillis = config.electionTimeoutMin().toMillis();
		return division.getInfo().getRoleInfoProto().getLeaderInfo().getFollowerInfoList().stream()
				.filter(follower -> follower.getLastRpcElapsedTimeMs() < recentMillis)
				.map(follower -> RaftPeerId.valueOf(follower.getId().getId()))
				.max(Comparator.comparingLong(id -> commitIndices.getOrDefault(id, -1L)))
				.orElse(null);
	}

	/**
	 * Normally stops taking requests first, then consensus, then the database (closed by the state machine). After a
	 * replica failure consensus stops first, so that a failed leader stops leading at once.
	 */
	private void shutdown(boolean consensusFirst, @Nullable Throwable cause) throws IOException {
		if (!closed.compareAndSet(false, true)) {
			return;
		}
		if (maintenance != null) {
			maintenance.shutdownNow();
		}
		metrics.close();
		IOException failure = null;
		// A replica can fail while the constructor is still starting the rest: skip what does not exist yet.
		var order = (consensusFirst
				? java.util.stream.Stream.<Closeable>of(server, apiServer, proposer, barrier, forwarder)
				: java.util.stream.Stream.<Closeable>of(apiServer, proposer, barrier, forwarder, server))
				.filter(java.util.Objects::nonNull)
				.toList();
		try {
			failure = closeAll(order);
		} finally {
			stopped.complete(cause);
		}
		LOG.info("{}: stopped", config.nodeId());
		if (failure != null) {
			throw failure;
		}
	}

	private @Nullable IOException closeAll(List<Closeable> resources) {
		IOException failure = null;
		for (Closeable resource : resources) {
			try {
				resource.close();
			} catch (IOException | RuntimeException ex) {
				if (failure == null) {
					failure = new IOException("Failed to stop node " + config.nodeId(), ex);
				} else {
					failure.addSuppressed(ex);
				}
			}
		}
		return failure;
	}
}
