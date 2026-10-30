package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
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
	private static final long MAINTENANCE_INTERVAL_MS = 200;
	private static final Duration DATABASE_CLOSE_TIMEOUT = Duration.ofMinutes(1);

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
	/** Only used on the maintenance thread. */
	private final ApiGate apiGate;
	/** Only used on the maintenance thread. */
	private boolean clusterInitialized;
	/** Only used on the maintenance thread. */
	private final LeaderStall leaderStall;
	private final NodeMetrics.Series leaderStallStepDowns;
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
		this.leaderStall = LeaderStall.forElectionTimeout(config.electionTimeoutMax());
		this.leaderStallStepDowns = metrics.amount("leader.stall.stepdowns");
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
				// With no peers at all Ratis starts the node without an election timer, until a leader's first request
				// that is not part of bringing the node up to date. Its own log or snapshot overrides this when it has a
				// configuration, and nothing about this start stays behind for a later start without the option.
				.setGroup(config.join() ? RaftGroup.valueOf(groupId) : group)
				.setStateMachine(stateMachine)
				.setProperties(RaftSettings.server(config))
				.setParameters(parameters)
				.setOption(existing ? RaftStorage.StartupOption.RECOVER : RaftStorage.StartupOption.FORMAT)
				.build();

		var started = new ArrayList<Closeable>();
		try {
			server.start();
			started.add(server);
			this.barrier = new ReadBarrier(server, groupId, stateMachine.appliedIndex(), config.requestTimeout(),
					stateMachine::leaderReadyIndex);
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
			this.apiGate = ApiGate.forElectionTimeout(config.electionTimeoutMax(), hadLeaderBeforeStart(), System.nanoTime());
			maintenance.scheduleWithFixedDelay(this::initializeCluster, 0, MAINTENANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
			maintenance.scheduleWithFixedDelay(this::gateApi, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			maintenance.scheduleWithFixedDelay(this::watchRaft, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
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
		if (clusterInitialized) {
			return;
		}
		try {
			var store = stateMachine.store();
			if (store.clusterId() != null) {
				clusterInitialized = true;
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
	 * Whether this node holds what a leader left: only a leader writes the Raft log, and it starts its term with an
	 * entry. The cluster id alone does not tell: a node killed soon after genesis can lose it from its database (the
	 * WAL is buffered), and gets it back from the log only once it knows a leader. The log alone does not either: it can
	 * be empty after a purge, on a node that holds a snapshot.
	 */
	private boolean hadLeaderBeforeStart() {
		var division = stateMachine.division();
		return stateMachine.store().clusterId() != null
				|| (division != null && division.getRaftLog().getNextIndex() > 0);
	}

	/** Stops serving clients while the node is cut off from a leader (see {@link ApiGate}). */
	private void gateApi() {
		try {
			if (apiGate.shouldServe(knowsLeader(), System.nanoTime())) {
				apiServer.resume();
			} else {
				apiServer.pause();
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to pause or resume the API, retrying", config.nodeId(), ex);
		}
	}

	/**
	 * A former leader that stepped down may keep its own id as leader: it does not count. Neither does what a closed
	 * division remembers.
	 */
	boolean knowsLeader() {
		var division = stateMachine.division();
		if (division == null || !division.getInfo().isAlive()) {
			return false;
		}
		var info = division.getInfo();
		var leader = info.getLeaderId();
		return leader != null && (info.isLeader() || !leader.toString().equals(config.nodeId()));
	}

	/** Stops the node if Raft closed, and starts a new election if this leader is stalled (see {@link LeaderStall}). */
	private void watchRaft() {
		try {
			if (closed.get()) {
				return;
			}
			var division = stateMachine.division();
			if (division == null || !division.getInfo().isAlive()) {
				stopAfterRaftClosed();
				return;
			}
			var log = division.getRaftLog();
			long commitIndex = log.getLastCommittedIndex();
			long term = division.getInfo().getCurrentTerm();
			if (leaderStall.shouldStepDown(waitsForReplyOfSentEntry(division), term, commitIndex, System.nanoTime())) {
				LOG.warn("{}: leader of term {}: no entry of this term committed, commit index {} did not move and the log"
						+ " up to {} went to a majority, stepping down to restart replication", config.nodeId(), term,
						commitIndex, log.getNextIndex() - 1);
				leaderStallStepDowns.record(1);
				// With no new leader Ratis only steps down, which needs no ready leader.
				var request = new TransferLeadershipRequest(ClientId.randomId(), server.getId(), group.getGroupId(), 0, null,
						LEADERSHIP_TRANSFER_TIMEOUT_MS);
				server.transferLeadershipAsync(request).whenComplete((reply, error) -> {
					if (error != null || !reply.isSuccess()) {
						LOG.warn("{}: failed to step down", config.nodeId(), error != null ? error : reply.getException());
					}
				});
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to check Raft, retrying", config.nodeId(), ex);
		}
	}

	/**
	 * This node leads, no entry of its term is committed, and a majority of members has been sent all of its log and
	 * still answers: see {@link LeaderStall}.
	 */
	private boolean waitsForReplyOfSentEntry(RaftServer.Division division) {
		var info = division.getInfo();
		var log = division.getRaftLog();
		if (!info.isLeader()) {
			return false;
		}
		var committed = log.getTermIndex(log.getLastCommittedIndex());
		if (committed != null && committed.getTerm() == info.getCurrentTerm()) {
			return false;
		}
		var nextIndices = info.getFollowerNextIndices();
		var replies = info.getRoleInfoProto().getLeaderInfo().getFollowerInfoList();
		if (nextIndices == null || nextIndices.length != replies.size()) {
			return false;
		}
		var followers = new ArrayList<LeaderStall.Follower>();
		for (int i = 0; i < nextIndices.length; i++) {
			var reply = replies.get(i);
			followers.add(new LeaderStall.Follower(RaftPeerId.valueOf(reply.getId().getId()).toString(), nextIndices[i],
					reply.getLastRpcElapsedTimeMs()));
		}
		var conf = division.getRaftConf();
		return LeaderStall.sentToMajority(followers, log.getNextIndex(), config.electionTimeoutMax().toMillis(),
				config.nodeId(), peerIds(conf.getCurrentPeers()), peerIds(conf.getPreviousPeers()));
	}

	private static List<String> peerIds(Collection<RaftPeer> peers) {
		return peers.stream().map(peer -> peer.getId().toString()).toList();
	}

	/**
	 * Ratis closed this node's Raft server by itself: after a JVM pause longer than {@link NodeConfig#jvmPauseLimit()},
	 * or after a leader told a node outside its configuration to go away. The process would stay up, with an API that
	 * only forwards to a leader it can no longer follow. Stop it like a failed replica, so that a supervisor restarts it.
	 */
	private void stopAfterRaftClosed() {
		onReplicaFailure(new IOException("Raft closed on node " + config.nodeId() + " while the node was running"));
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

	/** Completes when the node stopped: normally with null, or with the failure that stopped it. */
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

	/** This node's Raft server, for tests. */
	RaftServer raftServerForTests() {
		return server;
	}

	/** This node's metrics, for tests. */
	NodeMetrics metrics() {
		return metrics;
	}

	/** This node's proposer, for tests that propose what no client could. */
	Proposer proposer() {
		return proposer;
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
			awaitDatabaseClosed();
		} finally {
			stopped.complete(cause);
		}
		LOG.info("{}: stopped", config.nodeId());
		if (failure != null) {
			throw failure;
		}
	}

	/**
	 * If Ratis closed by itself the database can still be closing on one of its threads, and closing Raft again does not
	 * wait for that. A JVM that exits meanwhile does not end: its exit runs the destructors of RocksDB, which wait for
	 * background threads that wait for the JVM to detach them.
	 */
	private void awaitDatabaseClosed() {
		try {
			if (!stateMachine.awaitClosed(DATABASE_CLOSE_TIMEOUT)) {
				LOG.error("{}: the database did not close within {}", config.nodeId(), DATABASE_CLOSE_TIMEOUT);
			}
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
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
