package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
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
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.util.LifeCycle;
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
	static final Duration DATABASE_CLOSE_TIMEOUT = Duration.ofMinutes(1);
	/** How long a node that handed over its leadership waits for its successor's first commit. */
	private static final Duration SUCCESSOR_COMMIT_WAIT = Duration.ofSeconds(3);
	private static final long SUCCESSOR_COMMIT_POLL_MS = 10;
	/** Maximum election timeouts in the initializing state before a node says so, and again every as many. */
	private static final int ELECTION_TIMEOUTS_TO_WARN = 15;
	/** Ratis keeps here the peer list of a start with one, until the node has a configuration from a log. */
	private static final String BOOTSTRAP_FILE = "raft-meta.bootstrap";
	/** Ratis keeps here the configuration from the log. */
	private static final String CONFIGURATION_FILE = "raft-meta.conf";
	/** In the Raft directory, once a configuration from the log made this node a member: see {@link #requireNotRemoved}. */
	private static final String MEMBER_FILE = "member";

	private final NodeConfig config;
	private final RaftGroup group;
	private final RockserverStateMachine stateMachine;
	private final RaftServer server;
	private final ReadBarrier barrier;
	private final FollowerReads followerReads;
	/** Sends the commit index to the followers when one of them asks for a read index. */
	final CommitHeartbeats commitHeartbeats;
	private final Proposer proposer;
	private final LeaderForwarder forwarder;
	private final ReplicatedConnection connection;
	private final GrpcServer apiServer;
	/** Runs {@link #gateApi} and {@link #watchRaft}, which must keep running while the cluster is initialized. */
	private final ScheduledExecutorService maintenance;
	/**
	 * Runs {@link #initializeCluster}, whose genesis proposal blocks for up to the request timeout: on a thread of its
	 * own, so that it does not hold back {@link #gateApi} and {@link #watchRaft} (and {@link LeaderStall}).
	 */
	private final ScheduledExecutorService initialization;
	/**
	 * Runs {@link #discardFailedInstalls}, which lists, walks and renames in the data directory: on a thread of its own,
	 * so that a hung disk or Ratis threads holding the install lock never hold back {@link #gateApi} and
	 * {@link #watchRaft}, which must notice exactly that disk (see "Hung disk" in the design document).
	 */
	private final ScheduledExecutorService installCleanup;
	/** The node's control plane and local command threads, see {@link NodeThreads}. */
	private final NodeThreads threads;
	/** Only used on the maintenance thread. */
	private final ApiGate apiGate;
	/** The id this node proposes, if it is the one that starts the cluster. Only used on the initialization thread. */
	private final String newClusterId = UUID.randomUUID().toString();
	/** The cluster of this node, once it belongs to one. Only used on the initialization thread. */
	private @Nullable String clusterId;
	/** Only used on the initialization thread. */
	private boolean snapshotSettled;
	private volatile boolean takingSeedSnapshot;
	/** Only used on the initialization thread. */
	private final ClusterCheck clusterCheck;
	/** Finds a hung disk, which pauses the API like a missing leader does; see {@link #gateApi}. */
	private final DiskProbe diskProbe;
	/** Whether {@link #gateApi} last found the disk hung. Maintenance thread only. */
	private boolean diskHung;
	/** Only used on the maintenance thread. */
	private final LeaderStall leaderStall;
	private final NodeMetrics.Series leaderStallStepDowns;
	private final NodeMetrics.Series leaderLogStepDowns;
	/** When this node last entered or warned about the initializing state, 0 if out of it. Maintenance thread only. */
	private long initializingSinceNanos;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicLong snapshotCalls = new AtomicLong();
	private final CompletableFuture<@Nullable Throwable> stopped = new CompletableFuture<>();
	private final NodeMetrics metrics;
	/** Whether {@link #MEMBER_FILE} exists. Maintenance thread only. */
	private boolean memberRecorded;
	/** The gauges and alarms of this node; only refreshed on the maintenance thread. */
	private NodeMonitor monitor;

	private ReplicatedNode(NodeConfig config) throws IOException {
		this.config = config;
		Files.createDirectories(config.dbDir().getParent());
		Files.createDirectories(config.raftDir());
		requireSameFileSystem(config.raftDir(), config.dbDir().getParent());
		var groupId = RaftGroupId.valueOf(UUID.fromString(config.groupId()));
		this.group = RaftGroup.valueOf(groupId, raftPeers(config));
		this.metrics = new NodeMetrics(config.nodeId());
		this.commitHeartbeats = new CommitHeartbeats(this::raftServer, groupId);
		this.leaderStall = LeaderStall.forElectionTimeout(config.electionTimeoutMax());
		var configDigest = NodeStore.configDigest(config.rockserverConfig());
		this.clusterCheck = new ClusterCheck(config.nodeId(), groupId, config.electionTimeoutMax(), configDigest);
		this.diskProbe = DiskProbe.forDirectory(config.nodeId(), config.dataDir());
		this.leaderStallStepDowns = metrics.amount("leader.stall.stepdowns");
		this.leaderLogStepDowns = metrics.amount("leader.log.stepdowns");
		this.stateMachine = new RockserverStateMachine(config, this::onReplicaFailure, metrics);
		this.threads = new NodeThreads(config.nodeId());

		var parameters = new Parameters();
		var apiAddress = new HostAndPort(config.self().apiHost(), config.self().apiPort());
		GrpcConfigKeys.Server.setServicesCustomizer(parameters, TermService.customizer(this::raftServer, apiAddress,
				this::nodeState, this::readIndex, configDigest, this::answerOps));
		boolean existing = Files.isDirectory(config.raftDir().resolve(groupId.getUuid().toString()));
		if (!existing) {
			// Before anything opens the database: copies of a seed are the same only until then.
			NodeStore.recordSeedDigest(config.dbDir());
			requireNoClusterData(config);
			NodeStore.noteSeedOpened(config.dbDir());
		} else if (config.join() && startedOutsideJoin()) {
			throw new IOException("Node " + config.nodeId() + " was started without --join: "
					+ raftMetaFile(BOOTSTRAP_FILE) + " holds the peer list of that start, and Ratis uses it whatever this start"
					+ " says, so --join would change nothing. Wipe the data directory " + config.dataDir()
					+ " and start the node again with --join. Do not do it if an add-peer already reached this node: it is then"
					+ " a member, see \"Replacing a node\" in the design document");
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
			try {
				server.start();
			} catch (IOException | RuntimeException ex) {
				// Ratis wraps what the state machine's initialization threw.
				for (Throwable cause = ex; cause != null; cause = cause.getCause()) {
					if (cause instanceof NewerVersionException newer) {
						server.close();
						throw newer;
					}
				}
				throw ex;
			}
			started.add(server);
			requireNotRemoved(existing);
			this.barrier = new ReadBarrier(server, groupId, stateMachine.appliedIndex(), config.requestTimeout(),
					stateMachine::leaderReadyIndex, threads.control());
			// Every node of a cluster should have follower reads on or off alike: see "Follower reads" in the design.
			barrier.followerReads = Router.followerReadsConfigured();
			started.add(barrier);
			// A leader that cannot confirm its leadership within an election timeout is likely losing it: the read is
			// forwarded instead, and waits for the next leader there.
			this.followerReads = new FollowerReads(stateMachine, groupId, config.nodeId(), config.electionTimeoutMax());
			started.add(followerReads);
			this.proposer = new Proposer(group, RaftSettings.client(config), config, this::raftServer, metrics,
					threads.control());
			started.add(proposer);
			stateMachine.ops().proposer(proposer);
			this.forwarder = new LeaderForwarder(config);
			started.add(forwarder);
			stateMachine.onLeaderChange(forwarder::leaderChanged);
			this.connection = new ReplicatedConnection(config, stateMachine, proposer, barrier, followerReads, forwarder,
					metrics, threads.local());
			var self = config.self();
			var apiHost = config.apiBindHost() != null ? config.apiBindHost() : self.apiHost();
			this.apiServer = new GrpcServer(connection, new InetSocketAddress(apiHost, self.apiPort()));
			apiServer.start();
			started.add(apiServer);
			this.maintenance = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-maintenance").factory());
			this.initialization = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-initialization").factory());
			this.installCleanup = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-install-cleanup").factory());
			this.apiGate = ApiGate.forElectionTimeout(config.electionTimeoutMax(), hadLeaderBeforeStart(), System.nanoTime());
			diskProbe.start();
			initialization.scheduleWithFixedDelay(this::initializeCluster, 0, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			maintenance.scheduleWithFixedDelay(this::gateApi, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			maintenance.scheduleWithFixedDelay(this::watchRaft, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			installCleanup.scheduleWithFixedDelay(this::discardFailedInstalls, MAINTENANCE_INTERVAL_MS,
					MAINTENANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
			this.monitor = new NodeMonitor(config, stateMachine::division, stateMachine,
					config.raftDir().resolve(groupId.getUuid().toString()).resolve("current"), metrics);
			maintenance.scheduleWithFixedDelay(this::refreshMonitor, 0, NodeMonitor.REFRESH_MILLIS, TimeUnit.MILLISECONDS);
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
			threads.close();
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
	 * Proposes the {@link LogEntry.Genesis} entry from the first ready leader. On a cluster seeded from an existing
	 * database every replica then takes a snapshot: a member added later gets its data from the snapshot of whichever
	 * node leads by then, and a log that does not contain the seed is useless to it. Any leader tells its followers which
	 * cluster they follow.
	 */
	private void initializeCluster() {
		try {
			if (clusterId == null) {
				clusterId = stateMachine.store().clusterId();
			}
			if (clusterId == null) {
				proposeGenesis();
				return;
			}
			var division = stateMachine.division();
			if (division != null) {
				takeSeedSnapshot(division);
				checkFollowers(division);
				stateMachine.ops().onLeader(division, group.getGroupId());
			}
		} catch (Exception ex) {
			LOG.warn("{}: cluster initialization attempt failed, retrying", config.nodeId(), ex);
		}
	}

	/**
	 * Everything from earlier leaders is applied first: if one of them got an entry of this kind into the log, the
	 * cluster has its id. This node proposes the same id at every attempt, so that a cluster gets one id.
	 */
	private void proposeGenesis() throws Exception {
		var division = stateMachine.division();
		if (division == null || !division.getInfo().isLeader() || !division.getInfo().isLeaderReady()
				|| stateMachine.appliedIndex().get() < stateMachine.leaderReadyIndex(division.getInfo().getCurrentTerm())) {
			return;
		}
		var store = stateMachine.store();
		if (store.clusterId() != null) {
			return;
		}
		boolean seeded = store.hasUserColumns();
		// Every replica of a seeded cluster checks that it started from the same seed as this leader.
		var seedDigest = seeded ? store.seedDigest() : null;
		LOG.info("{}: initializing cluster {} (seeded: {}, seed digest {})", config.nodeId(), newClusterId, seeded,
				seedDigest);
		proposer.propose(new LogEntry.Genesis(newClusterId, seeded, seedDigest)).get(config.requestTimeout().toMillis(),
				TimeUnit.MILLISECONDS);
	}

	/** A replica of a seeded cluster takes a snapshot as soon as it has none, on every start until it has one. */
	private void takeSeedSnapshot(RaftServer.Division division) throws IOException {
		if (snapshotSettled || takingSeedSnapshot) {
			return;
		}
		if (stateMachine.getLatestSnapshot() != null || !stateMachine.store().seeded()) {
			snapshotSettled = true;
			return;
		}
		takingSeedSnapshot = true;
		long timeoutMs = config.requestTimeout().toMillis();
		var request = SnapshotManagementRequest.newCreate(ClientId.randomId(), server.getId(), group.getGroupId(),
				snapshotCalls.incrementAndGet(), timeoutMs);
		server.snapshotManagementAsync(request).whenComplete((reply, error) -> {
			takingSeedSnapshot = false;
			if (error != null || !reply.isSuccess()) {
				LOG.warn("{}: failed to take the snapshot of the seed, retrying", config.nodeId(),
						error != null ? error : reply.getException());
			}
		});
	}

	private void checkFollowers(RaftServer.Division division) {
		var info = division.getInfo();
		if (!info.isLeader()) {
			return;
		}
		var followers = new HashMap<String, String>();
		for (var follower : info.getRoleInfoProto().getLeaderInfo().getFollowerInfoList()) {
			followers.put(RaftPeerId.valueOf(follower.getId().getId()).toString(), follower.getId().getAddress());
		}
		clusterCheck.check(info.getCurrentTerm(), clusterId, followers, System.nanoTime());
	}

	/** Answers {@link TermService#NODE_STATE}, and stops this node if the caller speaks for another cluster. */
	/** Tests only. */
	ReadBarrier readBarrier() {
		return barrier;
	}

	/** Answers {@link TermService#OPS}: see {@link ReplicaOps#answer}. */
	private CompletableFuture<byte[]> answerOps(RaftGroupId requested, byte[] request) {
		if (!requested.equals(group.getGroupId())) {
			return CompletableFuture.failedFuture(org.apache.ratis.thirdparty.io.grpc.Status.NOT_FOUND
					.withDescription("No group " + requested).asRuntimeException());
		}
		return stateMachine.ops().answer(request);
	}

	private void refreshMonitor() {
		try {
			if (!closed.get()) {
				monitor.refresh();
			}
		} catch (RuntimeException ex) {
			LOG.debug("{}: failed to refresh the monitor", config.nodeId(), ex);
		}
	}

	/** This node's gauges and alarms, for tests. */
	NodeMonitor monitor() {
		return monitor;
	}

	/** Answers a follower's {@link TermService#READ_INDEX}: see {@link ReadBarrier#readIndex()}. */
	private CompletableFuture<ReadBarrier.ReadIndex> readIndex(RaftGroupId requested) {
		// Null until the constructor created it: term calls can arrive as soon as the Raft server started.
		var readBarrier = this.barrier;
		if (readBarrier == null || !requested.equals(group.getGroupId())) {
			return CompletableFuture.failedFuture(new ReadBarrier.NotLeaderException("No read barrier for " + requested));
		}
		// The commit index now: what the heartbeats sent below carry, at least.
		var index = readBarrier.readIndex();
		if (!index.isDone()) {
			// The follower must apply up to the read index, so it must know that it is committed: tell it now, while the
			// barrier confirms this node's leadership, rather than with the next append or heartbeat.
			commitHeartbeats.send();
		}
		return index;
	}

	private @Nullable NodeState nodeState(RaftGroupId requested, @Nullable String callerClusterId) {
		var division = stateMachine.division();
		var store = stateMachine.store();
		if (!requested.equals(group.getGroupId()) || division == null || store == null) {
			return null;
		}
		var own = store.clusterId();
		if (callerClusterId != null && own != null && !own.equals(callerClusterId)) {
			var error = new IOException("Node " + config.nodeId() + " belongs to cluster " + own + ", but a leader of this"
					+ " group leads cluster " + callerClusterId + ". Its Raft log and database are of another cluster: wipe its"
					+ " data directory and start it with --join");
			LOG.error("{}: another cluster reached this node", config.nodeId(), error);
			onReplicaFailure(error);
		}
		return new NodeState(division.getRaftLog().getNextIndex(), store.seeded(), stateMachine.getLatestSnapshot() != null,
				startedOutsideJoin(), own);
	}

	private Path raftMetaFile(String name) {
		return config.raftDir().resolve(group.getGroupId().getUuid().toString()).resolve("current").resolve(name);
	}

	/** The first start of this node had a peer list, and no log gave it a configuration since. */
	private boolean startedOutsideJoin() {
		return Files.exists(raftMetaFile(BOOTSTRAP_FILE)) && !Files.exists(raftMetaFile(CONFIGURATION_FILE));
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
			long now = System.nanoTime();
			boolean serve = apiGate.shouldServe(knowsLeader(), now);
			// A write waits at most the request timeout: a disk that has not ended an fsync for that long fails them all.
			boolean hung = diskProbe.hung(now, config.requestTimeout().toNanos());
			if (hung != diskHung) {
				diskHung = hung;
				if (hung) {
					LOG.warn("{}: an fsync in {} has not ended for more than {} ms: the disk hangs, pausing the API",
							config.nodeId(), config.dataDir(), config.requestTimeout().toMillis());
				} else {
					LOG.info("{}: the disk answers again", config.nodeId());
				}
			}
			if (serve && !hung) {
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
			warnWhileInitializing(division);
			warnAbovePendingLimit(division);
			var log = division.getRaftLog();
			long commitIndex = log.getLastCommittedIndex();
			long term = division.getInfo().getCurrentTerm();
			long now = System.nanoTime();
			boolean waiting = LeaderStall.waitsForReplyOfSentEntry(division, config.nodeId(),
					config.electionTimeoutMax().toMillis());
			if (leaderStall.shouldStepDown(waiting, term, commitIndex, now)) {
				LOG.warn("{}: leader of term {}: no entry of this term committed, commit index {} did not move and the log"
						+ " up to {} went to a majority, stepping down to restart replication", config.nodeId(), term,
						commitIndex, log.getNextIndex() - 1);
				leaderStallStepDowns.record(1);
				stepDown();
			} else if (leaderStall.logStopped(division.getInfo().isLeader(), log.getFlushIndex(), log.getNextIndex() - 1,
					now)) {
				LOG.warn("{}: leader of term {}: its Raft log flushed nothing past {} with entries up to {} waiting, and a"
						+ " leader commits only what it flushed: stepping down", config.nodeId(), term, log.getFlushIndex(),
						log.getNextIndex() - 1);
				leaderLogStepDowns.record(1);
				handOverOrStepDown();
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to check Raft, retrying", config.nodeId(), ex);
		}
		// After the checks above, and on its own: a write that fails (a full disk) must not keep a stalled leader in place.
		try {
			var division = stateMachine.division();
			if (!closed.get() && division != null && division.getInfo().isAlive()) {
				recordMembership(division);
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to record its membership in {}, retrying", config.nodeId(),
					config.raftDir().resolve(MEMBER_FILE), ex);
		}
	}

	/**
	 * Deletes the partial copies of failed snapshot installations also while nothing else reads the snapshot. Install
	 * cleanup thread only: it does disk I/O and takes the install lock, so it must stay off the maintenance thread.
	 */
	private void discardFailedInstalls() {
		try {
			if (!closed.get()) {
				stateMachine.getLatestSnapshot();
			}
		} catch (RuntimeException ex) {
			LOG.warn("{}: failed to discard abandoned snapshot installations, retrying", config.nodeId(), ex);
		}
	}

	/** The member count of the configuration that {@link #warnAbovePendingLimit} last warned about. */
	private int pendingLimitWarnedFor;

	/**
	 * Warns, on the leader, when the configuration has more members than its Ratis pending-request byte limit was sized
	 * for ({@link RaftSettings#nodesUnderPendingLimit}): their attempts in flight can then fill it, and Ratis refuses
	 * the rest before the append, which costs resends, not correctness. Maintenance thread only.
	 */
	private void warnAbovePendingLimit(org.apache.ratis.server.RaftServer.Division division) {
		int members = division.getGroup().getPeers().size();
		int limit = RaftSettings.nodesUnderPendingLimit(config);
		if (members <= limit || !division.getInfo().isLeader()) {
			return;
		}
		if (members != pendingLimitWarnedFor) {
			pendingLimitWarnedFor = members;
			LOG.warn("{}: the cluster has {} members, but this node's Ratis pending-request byte limit holds the attempts of"
					+ " {} nodes: restart the nodes with every member in --peer", config.nodeId(), members, limit);
		}
	}

	private void stepDown() throws IOException {
		// With no new leader Ratis only steps down, which needs no ready leader.
		var request = new TransferLeadershipRequest(ClientId.randomId(), server.getId(), group.getGroupId(), 0, null,
				LEADERSHIP_TRANSFER_TIMEOUT_MS);
		server.transferLeadershipAsync(request).whenComplete((reply, error) -> {
			if (error != null || !reply.isSuccess()) {
				LOG.warn("{}: failed to step down", config.nodeId(), error != null ? error : reply.getException());
			}
		});
	}

	/**
	 * For a leader that cannot flush. It stays a good candidate after a step down: its followers hold what it holds, and
	 * they wait for their election timers. So it hands over if it can: the follower elects itself at once.
	 */
	private void handOverOrStepDown() throws IOException {
		var transferee = transferee();
		if (transferee == null) {
			stepDown();
			return;
		}
		var request = new TransferLeadershipRequest(ClientId.randomId(), server.getId(), group.getGroupId(), 0, transferee,
				config.electionTimeoutMax().multipliedBy(2).toMillis());
		server.transferLeadershipAsync(request).whenComplete((reply, error) -> {
			if (error != null || !reply.isSuccess()) {
				LOG.warn("{}: failed to hand over to {}, stepping down", config.nodeId(), transferee,
						error != null ? error : reply.getException());
				try {
					stepDown();
				} catch (IOException | RuntimeException ex) {
					LOG.warn("{}: failed to step down", config.nodeId(), ex);
				}
			}
		});
	}

	/**
	 * A node in Ratis' initializing state has no election timer, and nothing shows that but the log: it printed READY and
	 * it listens. It leaves that state when a leader sends it a request that is not part of bringing it up to date.
	 */
	private void warnWhileInitializing(RaftServer.Division division) {
		if (division.getInfo().getLifeCycleState() != LifeCycle.State.STARTING) {
			initializingSinceNanos = 0;
			return;
		}
		long now = System.nanoTime();
		if (initializingSinceNanos == 0) {
			initializingSinceNanos = now;
		} else if (now - initializingSinceNanos >= config.electionTimeoutMax().multipliedBy(ELECTION_TIMEOUTS_TO_WARN)
				.toNanos()) {
			initializingSinceNanos = now;
			if (!config.join() || Files.exists(config.raftDir().resolve(MEMBER_FILE))) {
				askMembersWhileOutside(division);
			}
			LOG.warn("{}: this node is outside the cluster, with no election timer, and waits for a leader. {}", config.nodeId(),
					config.join() ? "It was started with --join, so it waits for add-peer. If no node of the cluster started"
							+ " without --join, nothing elects a leader: --join is only for a node that joins a running cluster"
							: "Its configuration does not contain it: it was removed from the cluster, or it never was a member."
							+ " A removed node has nothing to do: stop it. To bring it back, wipe its data directory and add it again");
		}
	}

	/** Whether {@link #askMembersWhileOutside} has a check queued or running. */
	private final AtomicBoolean askingMembers = new AtomicBoolean();

	/**
	 * For a node that was a member and waits outside the cluster: asks the members, on the initialization thread, and
	 * stops the node if a majority of them removed it ({@link RemovalCheck}), which the start-up check could not tell
	 * when they were not reachable then. The process then exits as a removed node.
	 */
	private void askMembersWhileOutside(RaftServer.Division division) {
		if (!askingMembers.compareAndSet(false, true)) {
			return;
		}
		try {
			initialization.execute(() -> {
				try {
					var conf = division.getRaftConf();
					var removedBy = closed.get() ? null : askMembers(conf);
					if (removedBy != null) {
						onReplicaFailure(new RemovedException("Node " + config.nodeId() + " waits outside the cluster, and members "
								+ removedBy + ", a majority of its configuration " + conf + ", hold a newer committed one without"
								+ " it: it was removed from the cluster. " + REMOVED_ADVICE));
					}
				} catch (RuntimeException ex) {
					LOG.warn("{}: failed to ask the members whether it was removed", config.nodeId(), ex);
				} finally {
					askingMembers.set(false);
				}
			});
		} catch (java.util.concurrent.RejectedExecutionException closing) {
			askingMembers.set(false);
		}
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
	 * Fails the start of a node that a configuration from its log left out, which a supervisor started again after its
	 * removal: it would otherwise print {@code READY} and stay up in Ratis' initializing state, with no leader to serve
	 * from. A node counts as removed if the new half of that configuration does not hold it, and either it was started
	 * without {@code --join} (its own peer list holds it, so only a removal can have left it out) or it was a member once
	 * ({@link #MEMBER_FILE}). A removed node usually holds the joint configuration of its removal, with itself in the old
	 * half only: the leader sends the final one to the members that stay. A node started with {@code --join} that never was
	 * a member is not removed: it may hold the configuration of a snapshot from before its {@code add-peer}.
	 *
	 * <p>One case is refused wrongly: a joint configuration that leaves the node out, which the node wrote but the leader
	 * never committed before it lost its leadership, so that the next leader drops it. Only a node that was being removed
	 * gets there, and it can be removed again with {@code remove-peer}.</p>
	 *
	 * <p>The configuration that removes a node commits with the other members alone, so the removed node's log may
	 * never receive it: a node that was stopped, slow or cut off during its removal still holds itself in its log. So a
	 * node that restarts ({@code existing}) and is or was a member also asks the members of its own configuration for
	 * theirs, see {@link RemovalCheck}: it is refused once a majority of them hold a committed configuration, newer than
	 * its own, that leaves it out. A first start, or a {@code --join} node that never was a member, skips this.</p>
	 */
	private void requireNotRemoved(boolean existing) throws IOException {
		var conf = server.getDivision(group.getGroupId()).getRaftConf();
		boolean leftOut = conf.getLogEntryIndex() >= 0
				&& conf.getCurrentPeers().stream().noneMatch(peer -> peer.getId().toString().equals(config.nodeId()));
		boolean memberOnce = !config.join() || Files.exists(config.raftDir().resolve(MEMBER_FILE));
		if (leftOut && memberOnce) {
			throw new RemovedException("Node " + config.nodeId() + " is not in the new configuration of its log, " + conf
					+ ": it was removed from the cluster. " + REMOVED_ADVICE);
		}
		if (leftOut || !existing || !memberOnce) {
			return;
		}
		var removedBy = askMembers(conf);
		if (removedBy != null) {
			throw new RemovedException("Node " + config.nodeId() + " is in the configuration of its log, " + conf
					+ ", but members " + removedBy + ", a majority of it, hold a newer committed one without this node: it was"
					+ " removed from the cluster while its log did not follow. " + REMOVED_ADVICE);
		}
	}

	private static final String REMOVED_ADVICE = "Take it out of its supervisor. To add it again, wipe its data"
			+ " directory, start it with --join and run add-peer";

	/**
	 * Asks the other members of {@code conf} about this node, see {@link RemovalCheck}: the members that say it was
	 * removed, if they are a majority, else null. Takes at most two election timeouts (at least a second).
	 */
	private @Nullable List<String> askMembers(org.apache.ratis.server.RaftConfiguration conf) {
		var addresses = new java.util.HashMap<String, String>();
		for (var peer : config.peers()) {
			addresses.put(peer.id(), peer.raftAddress());
		}
		for (var peer : conf.getAllPeers()) {
			if (peer.getAddress() != null) {
				addresses.put(peer.getId().toString(), peer.getAddress());
			}
		}
		var members = conf.getCurrentPeers().stream().map(peer -> peer.getId().toString()).toList();
		var budget = config.electionTimeoutMax().multipliedBy(2);
		if (budget.compareTo(java.time.Duration.ofSeconds(1)) < 0) {
			budget = java.time.Duration.ofSeconds(1);
		}
		return RemovalCheck.removedBy(config.nodeId(), group.getGroupId(), conf.getLogEntryIndex(), members.size(),
				RemovalCheck.addressesOf(config.nodeId(), members, addresses), budget);
	}

	/** Records, once, that a configuration from the log made this node a member: see {@link #requireNotRemoved}. */
	private void recordMembership(RaftServer.Division division) throws IOException {
		if (memberRecorded) {
			return;
		}
		var conf = division.getRaftConf();
		if (conf.getLogEntryIndex() < 0 || !conf.getPreviousPeers().isEmpty()
				|| conf.getCurrentPeers().stream().noneMatch(peer -> peer.getId().toString().equals(config.nodeId()))) {
			return;
		}
		var file = config.raftDir().resolve(MEMBER_FILE);
		if (!Files.exists(file)) {
			Files.writeString(file, config.nodeId() + "\n");
			NodeStore.syncFile(file);
			NodeStore.syncDirectory(config.raftDir());
		}
		memberRecorded = true;
	}

	/**
	 * Thrown by {@link #start} for a database whose cluster version has capabilities this version lacks (a downgrade, or a
	 * restore of a newer cluster's backup): {@code run} exits with 5.
	 */
	public static final class NewerVersionException extends IOException {
		NewerVersionException(String message) {
			super(message);
		}
	}

	/** Thrown by {@link #start} for a node that was removed from the cluster: see {@link #requireNotRemoved}. */
	public static final class RemovedException extends IOException {
		RemovedException(String message) {
			super(message);
		}
	}

	/**
	 * Fails unless {@code raftDir} and every directory under it are on the file system of {@code dataDir}, which holds the
	 * database directory: a snapshot hard-links the database's table files into the Raft directory, and on another file
	 * system it would copy the whole database instead, every time, while the apply waits.
	 */
	private static void requireSameFileSystem(Path raftDir, Path dataDir) throws IOException {
		var dataStore = Files.getFileStore(dataDir);
		List<Path> directories;
		try (var walk = Files.walk(raftDir)) {
			directories = walk.filter(Files::isDirectory).toList();
		}
		for (Path directory : directories) {
			if (!Files.getFileStore(directory).equals(dataStore)) {
				throw new IOException(directory + " is on a different file system than " + dataDir + ": every snapshot would"
						+ " copy the whole database instead of linking its files, while the apply waits. Keep the Raft"
						+ " directory and the database on one file system: mount the data directory itself");
			}
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
		var division = stateMachine.division();
		if (isLeader() && division != null) {
			long term = division.getInfo().getCurrentTerm();
			if (transferLeadership()) {
				awaitSuccessorCommit(division, term);
			}
		}
		shutdown(false, null);
	}

	/**
	 * Waits until the commit index of this node reaches an entry of a later term, for at most
	 * {@link #SUCCESSOR_COMMIT_WAIT}. With one node of three down this node is the only partner of its successor: if it
	 * closes before the reply to the successor's first entry gets out, that reply is lost and the successor stalls until
	 * {@link LeaderStall} ends it, unless the node took long enough to come back. This node learns of the commit from the
	 * successor's next request, which also says that the reply arrived.
	 */
	private void awaitSuccessorCommit(RaftServer.Division division, long term) {
		var log = division.getRaftLog();
		long deadline = System.nanoTime() + SUCCESSOR_COMMIT_WAIT.toNanos();
		try {
			while (System.nanoTime() - deadline < 0) {
				var committed = log.getTermIndex(log.getLastCommittedIndex());
				if (committed != null && committed.getTerm() > term) {
					return;
				}
				Thread.sleep(SUCCESSOR_COMMIT_POLL_MS);
			}
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return;
		}
		LOG.warn("{}: no commit of the new leader within {}, stopping anyway", config.nodeId(), SUCCESSOR_COMMIT_WAIT);
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
	 *
	 * @return whether another node leads now
	 */
	private boolean transferLeadership() {
		var transferee = transferee();
		if (transferee == null) {
			LOG.warn("{}: no follower answered recently, stopping as leader without a hand-off", config.nodeId());
			return false;
		}
		try {
			var request = new TransferLeadershipRequest(ClientId.randomId(), server.getId(), group.getGroupId(),
					0, transferee, LEADERSHIP_TRANSFER_TIMEOUT_MS);
			var reply = server.transferLeadership(request);
			if (reply.isSuccess()) {
				LOG.info("{}: leadership transferred to {}", config.nodeId(), transferee);
				return true;
			}
			LOG.warn("{}: leadership transfer to {} failed", config.nodeId(), transferee, reply.getException());
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: leadership transfer to {} failed", config.nodeId(), transferee, ex);
		}
		LOG.warn("{}: stopping as leader without a hand-off", config.nodeId());
		return false;
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
		if (diskProbe != null) {
			diskProbe.close();
		}
		if (initialization != null) {
			initialization.shutdownNow();
		}
		if (installCleanup != null) {
			installCleanup.shutdownNow();
		}
		metrics.close();
		IOException failure = null;
		// A replica can fail while the constructor is still starting the rest: skip what does not exist yet.
		var order = (consensusFirst
				? java.util.stream.Stream.<Closeable>of(server, apiServer, proposer, barrier, followerReads, forwarder)
				: java.util.stream.Stream.<Closeable>of(apiServer, proposer, barrier, followerReads, forwarder, server))
				.filter(java.util.Objects::nonNull)
				.toList();
		try {
			failure = closeAll(order);
			// After everything that uses them: their remaining timers and reply handlers have nothing left to answer.
			if (threads != null) {
				threads.close();
			}
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
