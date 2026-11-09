package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.io.grpc.Status;
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
	/** Ratis keeps here its current term and vote. */
	private static final String RAFT_META_FILE = "raft-meta";
	/** Ratis keeps here the configuration from the log. */
	private static final String CONFIGURATION_FILE = "raft-meta.conf";
	/** In the Raft directory, once a leader made this node a voter: see {@link #recordMembership}. */
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
	/**
	 * Runs {@link #gateApi}, and nothing else (round 25, R25-W): every call it makes reads fields or atomics, or pauses
	 * and resumes the API server, and none waits for a disk or for a lock that a thread waiting for a disk can hold (the
	 * snapshot storage monitor, the Raft log's lock, the Raft server's monitor). So the API pauses within
	 * {@link #MAINTENANCE_INTERVAL_MS} of the disk probe finding the disk hung, whatever Ratis and the state machine
	 * wait for. Never schedule anything else here.
	 */
	private final ScheduledExecutorService apiGateThread;
	/**
	 * Runs {@link #watchRaft}: it reads the Raft log, under the log's read lock, which a hung disk can keep away for
	 * good (an append holds the write lock while it waits for room in the queue of a log worker that stopped writing).
	 * Then Ratis cannot step down either, and the API gate, on its own thread, still pauses the API. The step-downs
	 * it decides run on a virtual thread ({@link #startStepDown}), since a hand-over takes the Raft server's monitor.
	 */
	private final ScheduledExecutorService raftWatchThread;
	/**
	 * Runs {@link #refreshMonitor} and {@link #recordMembership}, which touch the disk (file listings, sizes, free
	 * space, a file write and fsyncs) and read Ratis' log: off the threads of {@link #gateApi} and {@link #watchRaft}.
	 */
	private final ScheduledExecutorService monitorThread;
	/** Whether a step-down decided by {@link #watchRaft} is still being started on its virtual thread. */
	private final AtomicBoolean stepDownStarting = new AtomicBoolean();
	/**
	 * Runs {@link #initializeCluster}, whose genesis proposal blocks for up to the request timeout: on a thread of its
	 * own, so that it does not hold back {@link #gateApi} and {@link #watchRaft} (and {@link LeaderStall}).
	 */
	private final ScheduledExecutorService initialization;
	/**
	 * Runs {@link #discardFailedInstalls}, which lists, walks and renames in the data directory: on a thread of its own,
	 * so that a hung disk never holds back {@link #gateApi} and {@link #watchRaft}, which must notice exactly that disk
	 * (see "Hung disk" in the design document). It is the only thread that takes the install lock.
	 */
	private final ScheduledExecutorService installCleanup;
	/** The node's control plane and local command threads, see {@link NodeThreads}. */
	private final NodeThreads threads;
	/** Only used on the API gate thread. */
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
	/**
	 * The channels to the other members' Raft ports for {@link ReplicaOps} (checksum results, capabilities) and the
	 * address lookups of {@link LeaderForwarder}: one cache per node, pruned to the members by {@link #watchRaft}.
	 * {@link ReadBarrier} and {@link FollowerReads} keep their own, see {@link PeerChannels}.
	 */
	private final PeerChannels peerChannels = new PeerChannels();
	/** Finds a hung disk, which pauses the API like a missing leader does; see {@link #gateApi}. */
	private final DiskProbe diskProbe;
	/** Whether {@link #gateApi} last found the disk hung. API gate thread only. */
	private boolean diskHung;
	/** Only used on the Raft watch thread. */
	private final LeaderStall leaderStall;
	private final NodeMetrics.Series leaderStallStepDowns;
	private final NodeMetrics.Series leaderLogStepDowns;
	private final AtomicBoolean closed = new AtomicBoolean();
	private final AtomicLong snapshotCalls = new AtomicLong();
	private final CompletableFuture<@Nullable Throwable> stopped = new CompletableFuture<>();
	private final NodeMetrics metrics;
	/** Whether {@link #MEMBER_FILE} exists. Monitor thread only. */
	private boolean memberRecorded;
	/** When a leader last sent this node a Raft request: see {@link RaftTraffic}. */
	private final RaftTraffic raftTraffic;
	/** The gauges and alarms of this node; only refreshed on the monitor thread. */
	private NodeMonitor monitor;

	private ReplicatedNode(NodeConfig config) throws IOException {
		this.config = config;
		this.metrics = new NodeMetrics(config.nodeId());
		this.raftTraffic = new RaftTraffic(config.nodeId(), config.raftDir(), this::currentTerm,
				this::installFloor, metrics.amount("install.recordFailures"), metrics.amount("install.staleChunks"));
		Files.createDirectories(config.dbDir().getParent());
		Files.createDirectories(config.raftDir());
		requireSameFileSystem(config.raftDir(), config.dbDir().getParent());
		var groupId = RaftGroupId.valueOf(UUID.fromString(config.groupId()));
		this.group = RaftGroup.valueOf(groupId, raftPeers(config));
		this.commitHeartbeats = new CommitHeartbeats(this::raftServer, groupId);
		this.leaderStall = LeaderStall.forElectionTimeout(config.electionTimeoutMax());
		var configDigest = NodeStore.configDigest(config.rockserverConfig());
		this.clusterCheck = new ClusterCheck(config.nodeId(), groupId, config.electionTimeoutMax(), configDigest);
		this.diskProbe = DiskProbe.forDirectory(config.nodeId(), config.dataDir());
		this.leaderStallStepDowns = metrics.amount("leader.stall.stepdowns");
		this.leaderLogStepDowns = metrics.amount("leader.log.stepdowns");
		this.stateMachine = new RockserverStateMachine(config, this::onReplicaFailure, metrics);
		stateMachine.ops().channels(peerChannels);
		this.threads = new NodeThreads(config.nodeId());

		var parameters = new Parameters();
		parameters.put(CommitHeartbeats.PARAMETER, commitHeartbeats, CommitHeartbeats.class);
		var apiAddress = new HostAndPort(config.self().apiHost(), config.self().apiPort());
		GrpcConfigKeys.Server.setServicesCustomizer(parameters, TermService.customizer(this::raftServer, apiAddress,
				this::nodeState, this::readIndex, configDigest, this::answerOps, raftTraffic, this::configurationReport,
				new MembershipGuard(this::raftServer)));
		boolean existing = Files.isDirectory(config.raftDir().resolve(groupId.getUuid().toString()));
		if (!existing) {
			// Before anything opens the database: copies of a seed are the same only until then. It fsyncs the seed's
			// files first, which an operator copied into place and nothing fsynced (R34-1).
			NodeStore.recordSeedDigest(config.dbDir());
			// Durably, before the check's open changes the files: a start killed during that open keeps the seed digest
			// at the next first start (R41-1).
			NodeStore.noteSeedOpening(config.dbDir());
			requireNoClusterData(config);
			NodeStore.noteSeedOpened(config.dbDir());
		} else if (config.join() && startedOutsideJoin()) {
			throw new IOException("Node " + config.nodeId() + " was started without --join: "
					+ raftMetaFile(BOOTSTRAP_FILE) + " holds the peer list of that start, and Ratis uses it whatever this start"
					+ " says, so --join would change nothing. Wipe the data directory " + config.dataDir()
					+ " and start the node again with --join. Do not do it if an add-peer already reached this node: it is then"
					+ " a member, see \"Replacing a node\" in the design document");
		}
		// Before the Raft server can take a snapshot chunk: the floor of the term bounds of installed configurations is
		// the term Ratis persisted, which no chunk it handled before this start exceeds (round 15, R14-1).
		InstalledConfiguration.initialize(config.raftDir(), existing ? persistedTerm() : 0);
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
			checkMembershipAtStart(existing);
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
					threads.control(), threads.submit());
			started.add(proposer);
			stateMachine.ops().proposer(proposer);
			this.forwarder = new LeaderForwarder(config, peerChannels);
			started.add(forwarder);
			stateMachine.onLeaderChange(forwarder::leaderChanged);
			this.connection = new ReplicatedConnection(config, stateMachine, proposer, barrier, followerReads, forwarder,
					metrics, threads.local());
			var self = config.self();
			var apiHost = config.apiBindHost() != null ? config.apiBindHost() : self.apiHost();
			this.apiServer = new GrpcServer(connection, new InetSocketAddress(apiHost, self.apiPort()));
			apiServer.start();
			started.add(apiServer);
			this.apiGateThread = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-api-gate").factory());
			this.raftWatchThread = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-raft-watch").factory());
			this.monitorThread = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-monitor").factory());
			this.initialization = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-initialization").factory());
			this.installCleanup = Executors.newSingleThreadScheduledExecutor(
					Thread.ofPlatform().daemon().name("rockserver-" + config.nodeId() + "-install-cleanup").factory());
			this.apiGate = ApiGate.forElectionTimeout(config.electionTimeoutMax(), hadLeaderBeforeStart(), System.nanoTime());
			diskProbe.start();
			initialization.scheduleWithFixedDelay(this::initializeCluster, 0, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			apiGateThread.scheduleWithFixedDelay(this::gateApi, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			raftWatchThread.scheduleWithFixedDelay(this::watchRaft, MAINTENANCE_INTERVAL_MS, MAINTENANCE_INTERVAL_MS,
					TimeUnit.MILLISECONDS);
			monitorThread.scheduleWithFixedDelay(this::recordMembershipOnce, MAINTENANCE_INTERVAL_MS,
					MAINTENANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
			installCleanup.scheduleWithFixedDelay(this::discardFailedInstalls, MAINTENANCE_INTERVAL_MS,
					MAINTENANCE_INTERVAL_MS, TimeUnit.MILLISECONDS);
			this.monitor = new NodeMonitor(config, stateMachine::division, stateMachine,
					config.raftDir().resolve(groupId.getUuid().toString()).resolve("current"), metrics);
			monitor.membershipState(() -> membershipGauge(membership));
			monitorThread.scheduleWithFixedDelay(this::refreshMonitor, 0, NodeMonitor.REFRESH_MILLIS, TimeUnit.MILLISECONDS);
			var appendLag = metrics.amount("lag.uncommitted");
			var applyLag = metrics.amount("lag.unapplied");
			var flushLag = metrics.amount("lag.unflushed");
			metrics.sampleEveryMillisecond(() -> {
				errorForTesting("sampler");
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
			failForTesting(config.nodeId(), "start", () -> new IllegalStateException("test: " + config.nodeId() + "/start"));
		} catch (Throwable ex) {
			// The background tasks first: the API gate and the Raft watch must not go on against a half-built node, and
			// only the Raft watch would stop them otherwise (when it finds the Raft server closed, as a replica failure).
			closed.set(true);
			try {
				stopBackgroundTasks();
			} catch (Throwable stopError) {
				ex.addSuppressed(stopError);
			}
			for (int i = started.size() - 1; i >= 0; i--) {
				try {
					started.get(i).close();
				} catch (IOException | RuntimeException closeError) {
					ex.addSuppressed(closeError);
				}
			}
			threads.close();
			peerChannels.close();
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
	 * The current term of this node's division of {@code requested}, {@link RaftTraffic#NO_TERM} if the server is not
	 * built yet, or the group is another one or not started: for {@link RaftTraffic}.
	 */
	private long currentTerm(RaftGroupId requested) {
		var raftServer = this.server;
		if (raftServer == null || group == null || !requested.equals(group.getGroupId())) {
			return RaftTraffic.NO_TERM;
		}
		try {
			return raftServer.getDivision(requested).getInfo().getCurrentTerm();
		} catch (IOException | RuntimeException e) {
			return RaftTraffic.NO_TERM;
		}
	}

	/**
	 * The {@link MembershipFacts#installFloor} of this node's division of {@code requested} for a chunk of a leader of
	 * {@code leaderTerm}, {@link RaftTraffic#NO_INDEX} if none is known, the server is not built yet, or the group is
	 * another one or not started: for {@link RaftTraffic} (R16-2). Package-private for {@code InstallStreamTest}.
	 */
	long installFloor(RaftGroupId requested, long leaderTerm) {
		var raftServer = this.server;
		if (raftServer == null || group == null || !requested.equals(group.getGroupId())) {
			return RaftTraffic.NO_INDEX;
		}
		try {
			return MembershipFacts.installFloor(raftServer.getDivision(requested), stateMachine.configurations(),
					leaderTerm);
		} catch (IOException | RuntimeException e) {
			return RaftTraffic.NO_INDEX;
		}
	}

	/** The interceptor of this node's Raft traffic: for {@code InstallStreamTest}. */
	RaftTraffic raftTraffic() {
		return raftTraffic;
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
		} catch (Throwable error) {
			taskError(config.nodeId(), "cluster initialization", error);
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
			return CompletableFuture.failedFuture(Status.NOT_FOUND
					.withDescription("No group " + requested).asRuntimeException());
		}
		return stateMachine.ops().answer(request);
	}

	private void refreshMonitor() {
		try {
			errorForTesting("monitor");
			if (!closed.get()) {
				monitor.refresh();
			}
		} catch (RuntimeException ex) {
			LOG.debug("{}: failed to refresh the monitor", config.nodeId(), ex);
		} catch (Throwable error) {
			taskError(config.nodeId(), "the node monitor's refresh", error);
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

	/**
	 * The term in Ratis' {@code raft-meta} ({@code RaftStorageMetadataFileImpl}: properties {@code term} and
	 * {@code votedFor}), 0 if there is none; {@link Long#MAX_VALUE} if it cannot be read: then, until a start that reads
	 * it, every configuration a chunk installs is bounded by no term, and blocks the removal check
	 * ({@link InstalledConfiguration#initialize}).
	 */
	private long persistedTerm() {
		var file = raftMetaFile(RAFT_META_FILE);
		if (!Files.exists(file)) {
			return 0;
		}
		try (var reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
			var properties = new Properties();
			properties.load(reader);
			return Long.parseLong(Objects.requireNonNull(properties.getProperty("term"), "no term").trim());
		} catch (IOException | RuntimeException e) {
			LOG.warn("{}: cannot read the term in {}; configurations from snapshot installs get no term bound until a"
					+ " start that reads it",
					config.nodeId(), file, e);
			return Long.MAX_VALUE;
		}
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
			errorForTesting("api-gate");
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
		} catch (Throwable error) {
			// An OutOfDirectMemoryError when resume() builds and binds a new Netty server, say.
			taskError(config.nodeId(), "the API gate", error);
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
			errorForTesting("raft-watch");
			if (closed.get()) {
				return;
			}
			var division = stateMachine.division();
			if (division == null || !division.getInfo().isAlive()) {
				stopAfterRaftClosed();
				return;
			}
			checkWhileDetached(division);
			warnAbovePendingLimit(division);
			dropChannelsOfFormerMembers(division);
			var log = division.getRaftLog();
			long commitIndex = log.getLastCommittedIndex();
			long term = division.getInfo().getCurrentTerm();
			long now = System.nanoTime();
			boolean waiting = LeaderStall.waitsForReplyOfSentEntry(division, config.nodeId(),
					config.electionTimeoutMax().toMillis());
			// A decision taken while the previous step-down is still being started could not be acted on, yet would double
			// the next bound and start it again: LeaderStall does not take it.
			boolean canStepDown = !stepDownStarting.get();
			if (leaderStall.shouldStepDown(waiting, term, commitIndex, now, canStepDown)) {
				LOG.warn("{}: leader of term {}: no entry of this term committed, commit index {} did not move and the log"
						+ " up to {} went to a majority, stepping down to restart replication", config.nodeId(), term,
						commitIndex, log.getNextIndex() - 1);
				startStepDown(false, leaderStallStepDowns);
			} else if (leaderStall.logStopped(division.getInfo().isLeader(), log.getFlushIndex(), log.getNextIndex() - 1,
					now, canStepDown)) {
				LOG.warn("{}: leader of term {}: its Raft log flushed nothing past {} with entries up to {} waiting, and a"
						+ " leader commits only what it flushed: stepping down", config.nodeId(), term, log.getFlushIndex(),
						log.getNextIndex() - 1);
				startStepDown(true, leaderLogStepDowns);
			}
		} catch (RuntimeException ex) {
			LOG.warn("{}: failed to check Raft, retrying", config.nodeId(), ex);
		} catch (Throwable error) {
			taskError(config.nodeId(), "the Raft watch", error);
		}
	}

	/**
	 * Steps down ({@link #stepDown}) or hands over ({@link #handOverOrStepDown}) on a virtual thread: a hand-over takes
	 * the Raft server's monitor, which Ratis holds while it waits for its log or fsyncs its metadata, and the Raft watch
	 * thread must still notice that Raft closed meanwhile. At most one is being started at a time: {@link #watchRaft}
	 * asks {@link LeaderStall} for a decision only while none is (only this thread sets {@code stepDownStarting}), so
	 * that a decision nothing can act on neither doubles the next bound nor starts it again.
	 */
	private void startStepDown(boolean handOver, NodeMetrics.Series counter) {
		if (!stepDownStarting.compareAndSet(false, true)) {
			// Not reached: only this thread sets it, after watchRaft found it clear.
			LOG.info("{}: the previous step-down is still being started", config.nodeId());
			return;
		}
		counter.record(1);
		try {
			Thread.ofVirtual().name("rockserver-" + config.nodeId() + "-step-down").start(() -> {
				try {
					if (handOver) {
						handOverOrStepDown();
					} else {
						stepDown();
					}
				} catch (IOException | RuntimeException ex) {
					LOG.warn("{}: failed to step down", config.nodeId(), ex);
				} finally {
					stepDownStarting.set(false);
				}
			});
		} catch (Throwable notStarted) {
			stepDownStarting.set(false);
			LOG.warn("{}: failed to start the step-down, retrying at the next check", config.nodeId(), notStarted);
		}
	}

	/**
	 * {@link #recordMembership}, on the monitor thread: it writes and fsyncs a file. A write that fails (a full disk)
	 * is retried at the next run, and holds nothing else back.
	 */
	private void recordMembershipOnce() {
		try {
			errorForTesting("member");
			var division = stateMachine.division();
			if (!closed.get() && division != null && division.getInfo().isAlive()) {
				recordMembership(division);
			}
		} catch (IOException | RuntimeException ex) {
			LOG.warn("{}: failed to record its membership in {}, retrying", config.nodeId(),
					config.raftDir().resolve(MEMBER_FILE), ex);
		} catch (Throwable error) {
			taskError(config.nodeId(), "the write of raft/" + MEMBER_FILE, error);
		}
	}

	/**
	 * Shuts down the shared channels to addresses that are no longer a member's (members that left, or changed address):
	 * the members of the configuration, the peers the leader is bringing in, and this node's peer list stay.
	 */
	private void dropChannelsOfFormerMembers(RaftServer.Division division) {
		dropChannelsOfFormerMembers(peerChannels, division, config);
	}

	/**
	 * Shuts down the channels of {@code channels} to addresses that are no longer a member's ({@link #memberAddresses}):
	 * the address of a removed member, or the old address of a member that moved.
	 */
	static void dropChannelsOfFormerMembers(PeerChannels channels, RaftServer.Division division, NodeConfig config) {
		if (channels.size() == 0) {
			return;
		}
		channels.retainOnly(memberAddresses(division, config));
	}

	/** Tests only: the node's shared channels to the other members' Raft ports. */
	PeerChannels peerChannels() {
		return peerChannels;
	}

	/** The Raft addresses that {@link #peerChannels} keeps channels to. */
	static Set<@Nullable String> memberAddresses(RaftServer.Division division, NodeConfig config) {
		var addresses = new HashSet<@Nullable String>();
		ReplicaOps.pollPeers(division).values().forEach(peer -> addresses.add(peer.getAddress()));
		config.peers().forEach(peer -> addresses.add(peer.raftAddress()));
		return addresses;
	}

	/**
	 * Deletes the partial copies of failed snapshot installations. Install cleanup thread only: it does disk I/O and
	 * takes the install lock, so it must stay off the threads of {@link #gateApi} and {@link #watchRaft}, and off
	 * Ratis' threads, which read the latest snapshot under the server monitor.
	 */
	private void discardFailedInstalls() {
		runInstallCleanup(config.nodeId(), () -> {
			if (!closed.get()) {
				stateMachine.discardFailedInstalls();
			}
		});
	}

	/**
	 * Logs an Error that a periodic task threw, and returns, so that its next run still happens:
	 * {@code scheduleWithFixedDelay} cancels every later run of a task that throws, silently (R24-1, R25 review). Every
	 * periodic task of the node catches {@link Throwable} for that reason. Guarded: out of memory even for the message,
	 * the next run retries anyway.
	 */
	static void taskError(String nodeId, String task, Throwable error) {
		try {
			LOG.error("{}: {} failed, retrying at its next run", nodeId, task, error);
		} catch (Throwable logFailed) {
			// Nothing left to do: the next run retries.
		}
	}

	/**
	 * Tests only: how many {@link OutOfMemoryError}s the periodic task {@code <node id>/<task>} still throws, one per run
	 * ({@code api-gate}, {@code raft-watch}, {@code monitor}, {@code member}, {@code sampler}); and {@code <node id>/start}
	 * makes the constructor fail once the background tasks run. Empty in production: one lookup of an empty map per run.
	 */
	static final Map<String, Integer> FAILURES_FOR_TESTING = new ConcurrentHashMap<>();

	/** Tests only: throws {@code failure} if {@link #FAILURES_FOR_TESTING} asks for one more for {@code task}. */
	static void failForTesting(String nodeId, String task, java.util.function.Supplier<? extends Throwable> failure) {
		if (FAILURES_FOR_TESTING.isEmpty()) {
			return;
		}
		var fail = new boolean[1];
		FAILURES_FOR_TESTING.computeIfPresent(nodeId + "/" + task, (key, left) -> {
			fail[0] = true;
			return left > 1 ? left - 1 : null;
		});
		if (fail[0]) {
			sneakyThrow(failure.get());
		}
	}

	@SuppressWarnings("unchecked")
	private static <T extends Throwable> void sneakyThrow(Throwable failure) throws T {
		throw (T) failure;
	}

	/** {@link #failForTesting} with an {@link OutOfMemoryError}, for a periodic task of this node. */
	private void errorForTesting(String task) {
		failForTesting(config.nodeId(), task, () -> new OutOfMemoryError("test: " + config.nodeId() + "/" + task));
	}

	/**
	 * One run of the install cleanup task: {@code discard} logged when it fails, so that the next runs still happen.
	 * Errors too: {@code scheduleWithFixedDelay} cancels every later run of a task that throws, silently, and nothing
	 * else deletes replaced or abandoned installations before the next restart. An {@link OutOfMemoryError} in a walk
	 * of a large installation, or when {@code CheckpointStorage.discard} cannot start its virtual thread, is retried
	 * like any other failure; the scan itself waits {@code INSTALL_SCAN_INTERVAL} after the run that failed.
	 */
	static void runInstallCleanup(String nodeId, Runnable discard) {
		try {
			discard.run();
		} catch (Throwable ex) {
			try {
				LOG.error("{}: failed to discard abandoned snapshot installations, retrying", nodeId, ex);
			} catch (Throwable logFailed) {
				// Out of memory even for the log message: the next run retries anyway.
			}
		}
	}

	/** The member count of the configuration that {@link #warnAbovePendingLimit} last warned about. */
	private int pendingLimitWarnedFor;

	/**
	 * Warns, on the leader, when the configuration has more members than its Ratis pending-request byte limit was sized
	 * for ({@link RaftSettings#nodesUnderPendingLimit}): their attempts in flight can then fill it, and Ratis refuses
	 * the rest before the append, which costs resends, not correctness. Raft watch thread only.
	 */
	private void warnAbovePendingLimit(RaftServer.Division division) {
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

	/** When {@link #checkWhileDetached} last asked the members, and last warned. Raft watch thread only. */
	private long lastDetachedCheckNanos;
	private long lastDetachedWarnNanos;
	/** When the node started: until a leader sends to it, it counts as detached from then. */
	private final long startedNanos = System.nanoTime();

	/** The last membership verdict of this node (R11): see {@link #membership()}. */
	private volatile RemovalRule.@Nullable Verdict membership;

	/**
	 * For a node that hears no leader (R11): asks the members whether it was removed ({@link RemovalCheck}), on the
	 * initialization thread, and stops it with exit 4 if it was, which the start-up check could not tell when they did
	 * not answer then, or when it was removed while it ran. "Detached" means that no leader sent this node any Raft
	 * request (append, heartbeat, snapshot chunk, including those that bring it up to date for add-peer) for three
	 * election timeouts ({@link RaftTraffic}), and it does not lead itself. Ratis' initializing state is not
	 * detachment: a node being caught up for add-peer hears its leader and is never checked during its own add. The
	 * check runs every three election timeouts (at least a second) while the node stays detached; the warning, chosen
	 * by the verdict (never by {@code --join}), every {@value #ELECTION_TIMEOUTS_TO_WARN} election timeouts.
	 */
	private void checkWhileDetached(RaftServer.Division division) {
		long now = System.nanoTime();
		long last = raftTraffic.lastLeaderRequestNanos();
		long quiet = config.electionTimeoutMax().multipliedBy(3).toNanos();
		boolean detached = !division.getInfo().isLeader() && now - (last == 0 ? startedNanos : last) >= quiet;
		if (!detached) {
			var verdict = membership;
			if (verdict != null && verdict.outcome() != RemovalRule.Outcome.MEMBER && division.getRaftConf()
					.getCurrentPeers().stream().anyMatch(peer -> peer.getId().toString().equals(config.nodeId()))) {
				// A leader sends to it, and its configuration holds it: a member again (or at last).
				membership = new RemovalRule.Verdict(RemovalRule.Reason.MEMBER, null, List.of(), List.of(), List.of(),
						List.of(), "a leader sends to it");
			}
			return;
		}
		long interval = Math.max(TimeUnit.SECONDS.toNanos(1), quiet);
		if (lastDetachedCheckNanos != 0 && now - lastDetachedCheckNanos < interval) {
			return;
		}
		lastDetachedCheckNanos = now;
		if (!askingMembers.compareAndSet(false, true)) {
			return;
		}
		try {
			initialization.execute(() -> {
				try {
					if (closed.get()) {
						return;
					}
					var verdict = checkMembership(division);
					detachedChecks.incrementAndGet();
					if (verdict.outcome() == RemovalRule.Outcome.REMOVED) {
						onReplicaFailure(RemovedException.of(config.nodeId(), verdict, "while it heard no leader"));
						return;
					}
					long warnEvery = config.electionTimeoutMax().multipliedBy(ELECTION_TIMEOUTS_TO_WARN).toNanos();
					long at = System.nanoTime();
					boolean outside = verdict.outcome() != RemovalRule.Outcome.MEMBER
							|| division.getInfo().getLifeCycleState() == LifeCycle.State.STARTING;
					if (outside && (lastDetachedWarnNanos == 0 || at - lastDetachedWarnNanos >= warnEvery)) {
						lastDetachedWarnNanos = at;
						LOG.warn("{}: hears no leader; {}", config.nodeId(), describeOutside(verdict, division));
					}
				} catch (RuntimeException ex) {
					LOG.warn("{}: failed to ask the members whether it was removed", config.nodeId(), ex);
				} finally {
					askingMembers.set(false);
				}
			});
		} catch (RejectedExecutionException closing) {
			askingMembers.set(false);
		}
	}

	/**
	 * The {@code membership.state} gauge for the verdict {@code verdict} (null: not checked yet): 0 a member, 1 never
	 * added, 2 outside and undecided, 3 removed (reported only if a push of the gauges happens between the verdict and
	 * exit 4: the shutdown starts at once and does not wait for one, and a removal found at the start exits before the
	 * gauges report; do not alert on it).
	 */
	static int membershipGauge(RemovalRule.@Nullable Verdict verdict) {
		return verdict == null ? 0 : switch (verdict.outcome()) {
			case MEMBER -> 0;
			case NOT_ADDED -> 1;
			case UNDECIDED -> 2;
			case REMOVED -> 3;
		};
	}

	/** Whether {@link #checkWhileDetached} has a check queued or running. */
	private final AtomicBoolean askingMembers = new AtomicBoolean();
	/** How many checks {@link #checkWhileDetached} completed (its verdict set in {@link #membership}): tests. */
	private final AtomicLong detachedChecks = new AtomicLong();

	/** See {@link #detachedChecks}: tests. */
	long detachedChecks() {
		return detachedChecks.get();
	}

	/**
	 * The membership verdict of this node's last check (R11), null if none ran: at the start (existing data
	 * directory), and while it hears no leader. Its {@link RemovalRule.Reason} says whether it is a member, not added,
	 * or outside and undecided, and why.
	 */
	public RemovalRule.@Nullable Verdict membership() {
		return membership;
	}

	/** What a node outside the cluster logs, and what the operator does about it (design: "Removal and waiting outside"). */
	private String describeOutside(RemovalRule.Verdict verdict, RaftServer.Division division) {
		var facts = (division.getInfo().getLifeCycleState() == LifeCycle.State.STARTING
				? "this node is outside the cluster, in Ratis' initializing state (no election timer, no vote). " : "")
				+ "Membership " + verdict + ".";
		var action = switch (verdict.reason()) {
			case MEMBER -> division.getInfo().getLifeCycleState() == LifeCycle.State.STARTING
					? "It is a member, in Ratis' initializing state (no election timer, no vote) until a leader sends to it."
							+ " If no node of the cluster started without --join, nothing elects a leader: --join is only for a"
							+ " node that joins a running cluster"
					: "It is a member: it waits for the others to elect a leader";
			case NOT_ADDED -> "It never was a voter (started with --join): run add-peer for it. If its add was abandoned,"
					+ " or it was added and removed long ago, stop it and wipe its data directory: add-peer refuses a node"
					+ " that has a log";
			case NO_COMMITTED_CONFIGURATION -> "No member answered with a committed configuration, and this node applied"
					+ " none. If every node of a new cluster was started with --join, nothing elects a leader: --join is only"
					+ " for a node that joins a running cluster. Otherwise start or repair the pending and unreachable"
					+ " members: this node decides by itself once they answer";
			case NO_MAJORITY -> "Start or repair the pending and unreachable members (also"
					+ " members of a version without the Configuration method count as unreachable): this node decides by"
					+ " itself once a majority of the new half of the newest committed configuration answer. If they are gone"
					+ " for good, check with status on a member: if this node is not in its configuration, stop it, wipe its"
					+ " data directory, start it with --join and run add-peer";
			case NEEDS_TERM -> "Members answered with a version that predates Configuration2, which does not tell terms"
					+ " (its committed flag does not confirm a removal by itself), and the members of this version that"
					+ " answered cannot make a majority of the new half of the newest committed configuration without"
					+ " them: upgrade the named members of the older version, or start more members of this version."
					+ " Until then, judge with status on a member";
			case MEMBER_LATER_CONFIGURATION -> "Normally clears within seconds. If it persists, the blocking members are"
					+ " stuck or were removed themselves: look at their logs and status";
			case OWN_LATER_CONFIGURATION -> "Its own log holds a later configuration that includes it and may still"
					+ " commit: normally an add of this node in flight (let add-peer finish), or a configuration a leader"
					+ " appended just before it lost leadership, which clears once the next leader's configuration reaches"
					+ " this node. If it persists while a leader runs, check status on a member";
			case INCONSISTENT -> "A bug, or a data directory restored from elsewhere: do not wipe anything, collect the"
					+ " logs of every node";
			case HEARD_LEADER -> "A leader sent this node a request during the check: it is checked again if it hears no"
					+ " leader";
			case REMOVED -> REMOVED_ADVICE;
		};
		return facts + " " + action + ".";
	}

	/**
	 * One membership check of this node ({@link RemovalCheck}, at most {@link #removalCheckBudget}): the verdict, also
	 * kept in {@link #membership}. A removal found while a leader sent this node a request is not trusted (the answers
	 * may be older than that leader's configuration): {@link RemovalRule.Reason#HEARD_LEADER}.
	 */
	private RemovalRule.Verdict checkMembership(RaftServer.Division division) {
		long trafficBefore = raftTraffic.lastLeaderRequestNanos();
		var verdict = RemovalCheck.check(() -> ownFacts(division), memberAddresses(division.getRaftConf()),
				group.getGroupId(), removalCheckBudget());
		if (verdict.outcome() == RemovalRule.Outcome.REMOVED && raftTraffic.lastLeaderRequestNanos() != trafficBefore) {
			verdict = new RemovalRule.Verdict(RemovalRule.Reason.HEARD_LEADER, verdict.kStar(), verdict.confirmers(),
					verdict.blockers(), verdict.pending(), verdict.unreachable(), verdict.ownLog());
		}
		membership = verdict;
		return verdict;
	}

	/** This node's own facts for {@link RemovalRule#decide}. */
	private RemovalRule.Own ownFacts(RaftServer.Division division) {
		return MembershipFacts.own(division, stateMachine.configurations(), config.nodeId(), !config.join(),
				Files.exists(config.raftDir().resolve(MEMBER_FILE)), InstalledConfiguration.read(config.raftDir()));
	}

	/** The answer of this node to {@link TermService#CONFIGURATION2}. */
	private RemovalRule.@Nullable Report configurationReport(RaftGroupId groupId, List<TermIndex> probes) {
		var raftServer = this.server;
		if (raftServer == null || !groupId.equals(group.getGroupId())) {
			return null;
		}
		RaftServer.Division division;
		try {
			division = raftServer.getDivision(groupId);
		} catch (IOException e) {
			return null;
		}
		return division == null ? null : MembershipFacts.report(division, stateMachine.configurations(), probes);
	}

	/**
	 * Ratis closed this node's Raft server by itself: after a JVM pause longer than {@link NodeConfig#jvmPauseLimit()},
	 * or after a leader told a node outside its configuration to go away. The process would stay up, with an API that
	 * only forwards to a leader it can no longer follow. Stop it like a failed replica, so that a supervisor restarts it.
	 */
	private void stopAfterRaftClosed() {
		onReplicaFailure(raftClosedFailure());
	}

	private IOException raftClosedFailure() {
		return new IOException("Raft closed on node " + config.nodeId() + " while the node was running");
	}

	/**
	 * The membership check at the start of a node with an existing data directory (R11), after its Raft server started
	 * (it needs its own log, and its own {@link TermService} must answer the others during a cold start of the
	 * cluster): see {@link RemovalCheck} and {@link RemovalRule}. A removed node is refused with
	 * {@link RemovedException} (exit 4). Any other verdict lets it start: a member; a node never added
	 * ({@link RemovalRule.Reason#NOT_ADDED}); or a node outside and undecided (members down or unreachable, a later
	 * configuration that may still commit, ...), which logs why and what the operator does, and runs in whatever Ratis
	 * state its own log gives (Ratis' initializing state for a log whose latest configuration leaves it out of a half:
	 * no election timer, no vote). {@link #checkWhileDetached} decides again while it hears no leader. A first start
	 * has nothing to check.
	 */
	private void checkMembershipAtStart(boolean existing) throws IOException {
		if (!existing) {
			return;
		}
		var division = server.getDivision(group.getGroupId());
		var verdict = checkMembership(division);
		switch (verdict.outcome()) {
			case REMOVED -> throw RemovedException.of(config.nodeId(), verdict, "at its start");
			case MEMBER -> {
			}
			default -> LOG.warn("{}: starting, not known to be a member; {}", config.nodeId(),
					describeOutside(verdict, division));
		}
	}

	private static final String REMOVED_ADVICE = "Take it out of its supervisor. To add it again, wipe its data"
			+ " directory, start it with --join and run add-peer";


	/** Id to Raft address of every peer this node knows of: its startup list, then the peers of {@code conf}. */
	private Map<String, String> memberAddresses(RaftConfiguration conf) {
		var addresses = new HashMap<String, String>();
		for (var peer : config.peers()) {
			addresses.put(peer.id(), peer.raftAddress());
		}
		for (var peer : conf.getAllPeers()) {
			if (peer.getAddress() != null) {
				addresses.put(peer.getId().toString(), peer.getAddress());
			}
		}
		return addresses;
	}

	/** Two election timeouts, at least a second. */
	private Duration removalCheckBudget() {
		var budget = config.electionTimeoutMax().multipliedBy(2);
		return budget.compareTo(Duration.ofSeconds(1)) < 0 ? Duration.ofSeconds(1) : budget;
	}

	/** Whether {@link #MEMBER_FILE} exists: tests. */
	boolean memberFileExists() {
		return Files.exists(config.raftDir().resolve(MEMBER_FILE));
	}

	/**
	 * Records, once, that a leader made this node a voter (R11): Ratis runs it (it left its initializing state, which only
	 * an append that is not part of bringing it up to date does), and its latest configuration holds it in its new half.
	 * {@link RemovalRule.Own#memberFile}: a node started with {@code --join} counts as removed only after this. The
	 * appends that bring a node up to date for add-peer may replay configurations of an earlier membership of the same
	 * id; they arrive while the node is initializing, and do not count. The configuration may not be committed yet: a
	 * node whose add was then dropped is told to wipe its data directory, which add-peer needs anyway. Files of
	 * earlier versions, written from a stable configuration that held the node, are kept.
	 */
	private void recordMembership(RaftServer.Division division) throws IOException {
		if (memberRecorded) {
			return;
		}
		var conf = division.getRaftConf();
		if (conf.getLogEntryIndex() < 0 || division.getInfo().getLifeCycleState() != LifeCycle.State.RUNNING
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

		/** For a cluster version {@code version} of which this node lacks the capabilities {@code unknown}. */
		static NewerVersionException of(long version, long unknown) {
			return new NewerVersionException("Cluster version " + Capabilities.describe(version)
					+ " has capabilities this version lacks (" + Capabilities.describe(unknown) + "): it was written by a newer"
					+ " version. Run the newer version, see \"Upgrades\" in docs/distributed/runbooks.md");
		}

		/** Whether {@code failure}, or one of its causes, is a {@link NewerVersionException}. */
		public static boolean in(@Nullable Throwable failure) {
			for (var cause = failure; cause != null; cause = cause.getCause()) {
				if (cause instanceof NewerVersionException) {
					return true;
				}
			}
			return false;
		}
	}

	/**
	 * Thrown by {@link #start}, or stopping a running node, for a node that was removed from the cluster (R11): see
	 * {@link #checkMembershipAtStart} and {@link #checkWhileDetached}. {@code run} exits with 4.
	 */
	public static final class RemovedException extends IOException {
		private final RemovalRule.@Nullable Verdict verdict;

		RemovedException(String message) {
			this(message, null);
		}

		RemovedException(String message, RemovalRule.@Nullable Verdict verdict) {
			super(message);
			this.verdict = verdict;
		}

		/** The verdict of the check that found the removal. */
		public RemovalRule.@Nullable Verdict verdict() {
			return verdict;
		}

		/** For node {@code nodeId}, removed by {@code verdict}, found {@code when}. */
		static RemovedException of(String nodeId, RemovalRule.Verdict verdict, String when) {
			var kStar = java.util.Objects.requireNonNull(verdict.kStar());
			return new RemovedException("Node " + nodeId + " was removed from the cluster (found " + when + "): the"
					+ " committed configuration " + kStar + " leaves it out of its new half, and members "
					+ verdict.confirmers() + ", a majority of that new half, hold it committed with no later configuration that"
					+ " includes this node; " + verdict.ownLog() + ". " + REMOVED_ADVICE, verdict);
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
	 * Tests only: called inside the start-up check's open of the database of a first start, once it is open; what it
	 * throws stops the start there, as a kill during that open would (the files are changed already, R41-1).
	 */
	static volatile java.util.function.@Nullable Consumer<NodeConfig> startupCheckOpenedForTesting;

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
			var opened = startupCheckOpenedForTesting;
			if (opened != null) {
				opened.accept(config);
			}
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

	/**
	 * Graceful stop: hands leadership to another member first, then closes everything. If Ratis already closed its
	 * server by itself (a log worker that hit a disk error closes the division), this is the failure's stop, not a
	 * graceful one: {@link #watchRaft} notices that only on its next run, and a signal that lands before it would
	 * otherwise end a failed node with the exit status of a normal stop (DD-7h).
	 */
	@Override
	public void close() throws IOException {
		if (closed.get()) {
			return;
		}
		if (raftClosedByItself()) {
			shutdown(true, raftClosedFailure());
			return;
		}
		var division = stateMachine.division();
		if (isLeader() && division != null) {
			long term = division.getInfo().getCurrentTerm();
			if (transferLeadership()) {
				awaitSuccessorCommit(division, term);
			}
		}
		// Raft can close by itself during the hand-off too.
		if (raftClosedByItself()) {
			shutdown(true, raftClosedFailure());
			return;
		}
		shutdown(false, null);
	}

	/** Whether this node's Raft server closed while nothing here closed it. */
	private boolean raftClosedByItself() {
		if (closed.get()) {
			return false;
		}
		var division = stateMachine.division();
		return division == null || !division.getInfo().isAlive();
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
		stopBackgroundTasks();
		IOException failure = null;
		// A replica can fail while the constructor is still starting the rest: skip what does not exist yet.
		var order = (consensusFirst
				? Stream.<Closeable>of(server, apiServer, proposer, barrier, followerReads, forwarder,
						peerChannels)
				: Stream.<Closeable>of(apiServer, proposer, barrier, followerReads, forwarder, server,
						peerChannels))
				.filter(Objects::nonNull)
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
	 * Stops the periodic tasks and their threads, the disk probe and the metrics threads. Also called by a constructor
	 * that fails, so a field can still be null.
	 */
	private void stopBackgroundTasks() {
		for (var executor : Arrays.asList(apiGateThread, raftWatchThread, monitorThread, initialization, installCleanup)) {
			if (executor != null) {
				executor.shutdownNow();
			}
		}
		if (diskProbe != null) {
			diskProbe.close();
		}
		metrics.close();
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
