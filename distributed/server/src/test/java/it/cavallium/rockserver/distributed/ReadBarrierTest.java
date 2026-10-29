package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.distributed.ReadBarrier.NotLeaderException;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Read barrier against three real Ratis servers talking gRPC over localhost. */
class ReadBarrierTest {

	private static final Duration FAST_ELECTION_MIN = Duration.ofMillis(300);
	private static final Duration FAST_ELECTION_MAX = Duration.ofMillis(600);
	private static final Duration ROUND_TIMEOUT = Duration.ofSeconds(1);
	private static final Duration WAIT = Duration.ofSeconds(20);
	private static final String APPEND_ENTRIES = RaftServerProtocolServiceGrpc.getAppendEntriesMethod()
			.getFullMethodName();

	@TempDir
	Path tempDir;

	private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.randomUUID());
	private final List<Node> nodes = new ArrayList<>();
	private final List<ReadBarrier> barriers = new ArrayList<>();
	private final List<RaftClient> clients = new ArrayList<>();

	@AfterEach
	void tearDown() throws IOException {
		barriers.forEach(ReadBarrier::close);
		for (var client : clients) {
			client.close();
		}
		for (var node : nodes) {
			node.stop();
		}
	}

	@Test
	void leaderBarrierCoversEveryCommittedEntry() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var client = client();
		for (int i = 0; i < 10; i++) {
			assertTrue(client.io().send(Message.valueOf("inc")).isSuccess());
		}
		long commitIndex = leader.commitIndex();

		barrier(leader).await().get(10, TimeUnit.SECONDS);

		assertTrue(leader.stateMachine.applied.get() >= commitIndex,
				"applied " + leader.stateMachine.applied.get() + " < commit index " + commitIndex);
		assertEquals(10, leader.stateMachine.writes.get());
	}

	@Test
	void barrierWaitsUntilCommittedWriteIsApplied() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var barrier = barrier(leader);
		var client = client();
		long commitBefore = leader.commitIndex();

		var gate = new CountDownLatch(1);
		leader.stateMachine.applyGate = gate;
		try {
			// Committed by the majority, but the leader cannot apply it yet.
			var write = client.async().send(Message.valueOf("inc"));
			waitUntil(() -> leader.commitIndex() > commitBefore, "write committed");
			long callsBefore = followers.getFirst().termCalls.get();

			var writesSeenByRead = barrier.await().thenApply(_ -> leader.stateMachine.writes.get());
			waitUntil(() -> followers.getFirst().termCalls.get() > callsBefore, "leadership round sent");
			assertFalse(writesSeenByRead.isDone(), "barrier completed before the committed write was applied");

			gate.countDown();
			assertEquals(1, writesSeenByRead.get(10, TimeUnit.SECONDS));
			assertTrue(write.get(10, TimeUnit.SECONDS).isSuccess());
		} finally {
			gate.countDown();
		}
	}

	@Test
	void followersRejectBarrier() throws Exception {
		startCluster();
		var leader = awaitLeader();
		for (var follower : followersOf(leader)) {
			assertNotLeader(barrier(follower).await());
		}
	}

	@Test
	void leaderWithoutFollowersNeverConfirms() throws Exception {
		// A long election timeout keeps node 0 a leader for seconds after it lost its followers.
		startCluster(Duration.ofSeconds(5), Duration.ofSeconds(6));
		var leader = nodes.getFirst();
		makeLeader(leader);
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		for (var follower : followersOf(leader)) {
			follower.stop();
		}
		assertTrue(leader.isLeader(), "precondition: the isolated node still believes it leads");
		for (int attempt = 0; attempt < 3; attempt++) {
			var error = failure(barrier.await());
			assertTrue(error instanceof TimeoutException || error instanceof NotLeaderException, error.toString());
		}
	}

	@Test
	void staleLeaderIsRejectedByHigherTerms() throws Exception {
		startCluster(Duration.ofSeconds(5), Duration.ofSeconds(6));
		var stale = nodes.getFirst();
		makeLeader(stale);
		long staleTerm = stale.term();
		var barrier = barrier(stale);
		barrier.await().get(10, TimeUnit.SECONDS);

		// Partition: the others stop accepting the stale leader's heartbeats, and it hears none of their votes,
		// so it keeps believing it leads while they move on to later terms.
		stale.rejectedMethods = Set.of(RaftServerProtocolServiceGrpc.getRequestVoteMethod().getFullMethodName(),
				RaftServerProtocolServiceGrpc.getStartLeaderElectionMethod().getFullMethodName(), APPEND_ENTRIES);
		var others = followersOf(stale);
		others.forEach(node -> node.rejectedMethods = Set.of(APPEND_ENTRIES));
		waitUntil(() -> others.stream().allMatch(node -> node.term() > staleTerm), "others in a later term");
		assertTrue(stale.isLeader() && stale.term() == staleTerm, "precondition: the stale leader still leads");

		assertNotLeader(barrier.await());
	}

	@Test
	void barrierMovesToNewLeaderAfterLeaderFails() throws Exception {
		startCluster();
		var oldLeader = awaitLeader();
		var oldBarrier = barrier(oldLeader);
		oldBarrier.await().get(10, TimeUnit.SECONDS);

		oldLeader.stop();
		var newLeader = awaitLeader();
		assertTrue(client().io().send(Message.valueOf("inc")).isSuccess());
		long commitIndex = newLeader.commitIndex();

		barrier(newLeader).await().get(10, TimeUnit.SECONDS);
		assertTrue(newLeader.stateMachine.applied.get() >= commitIndex);
		assertNotLeader(oldBarrier.await());
	}

	@Test
	void deposedLeaderRejectsBarrier() throws Exception {
		startCluster();
		var oldLeader = awaitLeader();
		var oldBarrier = barrier(oldLeader);
		oldBarrier.await().get(10, TimeUnit.SECONDS);

		var newLeader = followersOf(oldLeader).getFirst();
		makeLeader(newLeader);

		assertNotLeader(oldBarrier.await());
		barrier(newLeader).await().get(10, TimeUnit.SECONDS);
	}

	@Test
	void barrierFollowsMembershipChange() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var kept = followers.get(0);
		var replaced = followers.get(1);
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		// Replace a follower with a new member on another port, then lose the other old follower: the leader
		// can only confirm its leadership through the new member.
		replaced.stop();
		var added = addNode("n3", RaftGroup.valueOf(groupId));
		var newPeers = List.of(leader.peer, kept.peer, added.peer);
		assertTrue(client().admin().setConfiguration(newPeers).isSuccess());
		waitUntil(() -> {
			var conf = leader.division().getRaftConf();
			return conf.getPreviousPeers().isEmpty() && conf.getCurrentPeers().containsAll(newPeers);
		}, "new configuration committed");
		kept.stop();

		barrier.await().get(10, TimeUnit.SECONDS);
		assertTrue(added.termCalls.get() > 0);
	}

	@Test
	void concurrentCallsShareRounds() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var follower = followersOf(leader).getFirst();
		var barrier = barrier(leader);
		long callsBefore = follower.termCalls.get();

		int calls = 1000;
		var start = new CountDownLatch(1);
		var results = new ArrayList<CompletableFuture<Void>>();
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
			var submitted = new ArrayList<Future<CompletableFuture<Void>>>();
			for (int i = 0; i < calls; i++) {
				submitted.add(executor.submit(() -> {
					start.await();
					return barrier.await();
				}));
			}
			start.countDown();
			for (var future : submitted) {
				results.add(future.get());
			}
		}
		CompletableFuture.allOf(results.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);

		// Each round asks every follower once.
		long rounds = follower.termCalls.get() - callsBefore;
		assertTrue(rounds >= 1 && rounds <= calls / 10, "rounds: " + rounds);
	}

	@Test
	void unansweredRoundTimesOutAndNextRoundWorks() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var barrier = barrier(leader);

		followers.forEach(node -> node.holdTermCalls = true);
		assertInstanceOf(TimeoutException.class, failure(barrier.await()));

		followers.forEach(node -> node.holdTermCalls = false);
		barrier.await().get(10, TimeUnit.SECONDS);
	}

	@Test
	void closeFailsPendingCalls() throws Exception {
		startCluster();
		var leader = awaitLeader();
		followersOf(leader).forEach(node -> node.holdTermCalls = true);
		var barrier = new ReadBarrier(leader.server, groupId, leader.stateMachine.applied, Duration.ofMinutes(1));
		barriers.add(barrier);

		var pending = barrier.await();
		barrier.close();
		assertNotLeader(pending);
		assertNotLeader(barrier.await());
	}

	@Test
	void termServiceAnswersMinusOneForUnknownGroup() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var channel = NettyChannelBuilder.forTarget(leader.peer.getAddress()).usePlaintext().build();
		try {
			long term = TermService.currentTerm(channel, groupId, ROUND_TIMEOUT).get(10, TimeUnit.SECONDS);
			assertEquals(leader.term(), term);
			var unknown = RaftGroupId.valueOf(UUID.randomUUID());
			assertEquals(TermService.NO_GROUP, TermService.currentTerm(channel, unknown, ROUND_TIMEOUT)
					.get(10, TimeUnit.SECONDS));
		} finally {
			channel.shutdownNow();
		}
	}

	// ---- cluster harness ----

	private void startCluster() throws IOException {
		startCluster(FAST_ELECTION_MIN, FAST_ELECTION_MAX);
	}

	/** Starts n0, n1, n2; n0 gets the given election timeouts, the others fast ones. */
	private void startCluster(Duration firstMin, Duration firstMax) throws IOException {
		var peers = new ArrayList<RaftPeer>();
		for (int i = 0; i < 3; i++) {
			peers.add(peer("n" + i));
		}
		var group = RaftGroup.valueOf(groupId, peers);
		for (int i = 0; i < 3; i++) {
			var slow = i == 0;
			nodes.add(startNode(peers.get(i), group, slow ? firstMin : FAST_ELECTION_MIN,
					slow ? firstMax : FAST_ELECTION_MAX));
		}
	}

	/** Starts a node outside the initial group, to be added by a configuration change. */
	private Node addNode(String id, RaftGroup group) throws IOException {
		var node = startNode(peer(id), group, FAST_ELECTION_MIN, FAST_ELECTION_MAX);
		nodes.add(node);
		return node;
	}

	private Node startNode(RaftPeer peer, RaftGroup group, Duration electionMin, Duration electionMax)
			throws IOException {
		var node = new Node(peer);
		var properties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
		var address = peer.getAddress();
		GrpcConfigKeys.Server.setHost(properties, address.substring(0, address.lastIndexOf(':')));
		GrpcConfigKeys.Server.setPort(properties, Integer.parseInt(address.substring(address.lastIndexOf(':') + 1)));
		RaftServerConfigKeys.setStorageDir(properties, List.of(tempDir.resolve(peer.getId().toString()).toFile()));
		var min = TimeDuration.valueOf(electionMin.toMillis(), TimeUnit.MILLISECONDS);
		var max = TimeDuration.valueOf(electionMax.toMillis(), TimeUnit.MILLISECONDS);
		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, min);
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, max);
		RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, min);
		RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, max);

		var parameters = new Parameters();
		var termService = TermService.customizer(node.serverRef::get);
		GrpcConfigKeys.Server.setServicesCustomizer(parameters, (builder, types) -> {
			if (types.contains(GrpcServices.Type.SERVER)) {
				builder.intercept(node.faults());
			}
			return termService.customize(builder, types);
		});
		node.server = RaftServer.newBuilder()
				.setServerId(peer.getId())
				.setGroup(group)
				.setProperties(properties)
				.setParameters(parameters)
				.setStateMachine(node.stateMachine)
				.setOption(RaftStorage.StartupOption.FORMAT)
				.build();
		node.serverRef.set(node.server);
		node.server.start();
		return node;
	}

	private static RaftPeer peer(String id) throws IOException {
		try (var socket = new ServerSocket(0)) {
			return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + socket.getLocalPort()).build();
		}
	}

	private RaftClient client() {
		var properties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
		var peers = nodes.stream().filter(node -> !node.stopped).map(node -> node.peer).toList();
		var client = RaftClient.newBuilder()
				.setProperties(properties)
				.setRaftGroup(RaftGroup.valueOf(groupId, peers))
				.build();
		clients.add(client);
		return client;
	}

	private ReadBarrier barrier(Node node) {
		var barrier = new ReadBarrier(node.server, groupId, node.stateMachine.applied, ROUND_TIMEOUT);
		barriers.add(barrier);
		return barrier;
	}

	private Node awaitLeader() {
		var leader = new AtomicReference<Node>();
		waitUntil(() -> {
			for (var node : nodes) {
				if (!node.stopped && node.isLeader() && node.division().getInfo().isLeaderReady()) {
					leader.set(node);
					return true;
				}
			}
			return false;
		}, "a ready leader");
		return leader.get();
	}

	private void makeLeader(Node target) throws IOException {
		awaitLeader();
		if (!target.isLeader()) {
			assertTrue(client().admin().transferLeadership(target.peer.getId(), 10_000).isSuccess());
		}
		waitUntil(() -> target.isLeader() && target.division().getInfo().isLeaderReady(), target + " leads");
	}

	private List<Node> followersOf(Node leader) {
		return nodes.stream().filter(node -> node != leader && !node.stopped).toList();
	}

	private static Throwable failure(CompletableFuture<Void> future) throws InterruptedException, TimeoutException {
		try {
			future.get(10, TimeUnit.SECONDS);
		} catch (ExecutionException e) {
			return e.getCause();
		}
		return fail("expected the barrier to fail, but it succeeded");
	}

	private static void assertNotLeader(CompletableFuture<Void> future) {
		var error = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
		assertInstanceOf(NotLeaderException.class, error.getCause());
	}

	private static void waitUntil(BooleanSupplier condition, String description) {
		long deadline = System.nanoTime() + WAIT.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() > deadline) {
				fail("Timed out waiting for " + description);
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			}
		}
	}

	/** One Ratis server with its state machine and the faults the test can inject into its gRPC server. */
	private final class Node {

		final RaftPeer peer;
		final CountingStateMachine stateMachine = new CountingStateMachine();
		final AtomicReference<RaftServer> serverRef = new AtomicReference<>();
		/** Term requests received from other nodes' barriers. */
		final AtomicLong termCalls = new AtomicLong();
		/** Leave term requests unanswered. */
		volatile boolean holdTermCalls;
		/** Ratis methods (full names) refused with UNAVAILABLE, to cut Raft traffic into this node. */
		volatile Set<String> rejectedMethods = Set.of();
		RaftServer server;
		boolean stopped;

		Node(RaftPeer peer) {
			this.peer = peer;
		}

		RaftServer.Division division() {
			try {
				return server.getDivision(groupId);
			} catch (IOException e) {
				throw new AssertionError(e);
			}
		}

		boolean isLeader() {
			return division().getInfo().isLeader();
		}

		long term() {
			return division().getInfo().getCurrentTerm();
		}

		long commitIndex() {
			return division().getRaftLog().getLastCommittedIndex();
		}

		void stop() throws IOException {
			if (!stopped) {
				stopped = true;
				stateMachine.applyGate = null;
				server.close();
			}
		}

		ServerInterceptor faults() {
			return new ServerInterceptor() {
				@Override
				public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call,
						Metadata headers,
						ServerCallHandler<Q, R> next) {
					var method = call.getMethodDescriptor();
					if (TermService.SERVICE_NAME.equals(method.getServiceName())) {
						termCalls.incrementAndGet();
						if (holdTermCalls) {
							return new ServerCall.Listener<>() {};
						}
					} else if (rejectedMethods.contains(method.getFullMethodName())) {
						call.close(Status.UNAVAILABLE.withDescription("partitioned by the test"), new Metadata());
						return new ServerCall.Listener<>() {};
					}
					return next.startCall(call, headers);
				}
			};
		}

		@Override
		public String toString() {
			return peer.getId().toString();
		}
	}

	/** Counts applied client writes and tracks the applied index like the real state machine does. */
	private static final class CountingStateMachine extends BaseStateMachine {

		final AppliedIndex applied = new AppliedIndex(-1);
		final AtomicLong writes = new AtomicLong();
		/** When set, applying a client write blocks until it opens. */
		volatile CountDownLatch applyGate;
		private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

		@Override
		public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
			super.initialize(server, groupId, raftStorage);
			storage.init(raftStorage);
		}

		@Override
		public SimpleStateMachineStorage getStateMachineStorage() {
			return storage;
		}

		@Override
		public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
			var gate = applyGate;
			if (gate != null) {
				try {
					gate.await();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					return CompletableFuture.failedFuture(e);
				}
			}
			var entry = trx.getLogEntry();
			writes.incrementAndGet();
			updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
			applied.advance(entry.getIndex());
			return CompletableFuture.completedFuture(Message.valueOf("ok"));
		}

		/** Configuration and leader no-op entries. */
		@Override
		public void notifyTermIndexUpdated(long term, long index) {
			super.notifyTermIndexUpdated(term, index);
			applied.advance(index);
		}
	}
}
