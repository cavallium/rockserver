package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.distributed.ReadBarrier.ApplyLagException;
import it.cavallium.rockserver.distributed.ReadBarrier.NotLeaderException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
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
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
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
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
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
	/** Election timeouts of restarted nodes, long enough that they hear from the leader before campaigning. */
	private static final Duration RESTART_ELECTION_MIN = Duration.ofSeconds(2);
	private static final Duration RESTART_ELECTION_MAX = Duration.ofSeconds(3);
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
	void leadershipWaitsForTheEntriesOfEarlierLeadersToBeApplied() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var client = client();
		long commitBefore = leader.commitIndex();
		var gate = new CountDownLatch(1);
		leader.stateMachine.applyGate = gate;
		try {
			var write = client.async().send(Message.valueOf("inc"));
			waitUntil(() -> leader.commitIndex() > commitBefore, "write committed");
			long readyIndex = leader.commitIndex();
			// As if that entry were an earlier leader's, still waiting for this node's apply thread.
			var barrier = new ReadBarrier(leader.server, groupId, leader.stateMachine.applied, ROUND_TIMEOUT,
					_ -> readyIndex);
			barriers.add(barrier);
			var writesSeenByRead = barrier.awaitLeadership().thenApply(_ -> leader.stateMachine.writes.get());
			Thread.sleep(300);
			assertFalse(writesSeenByRead.isDone(), "leadership confirmed before the earlier entries were applied");
			gate.countDown();
			assertEquals(1, writesSeenByRead.get(10, TimeUnit.SECONDS));
			assertTrue(write.get(10, TimeUnit.SECONDS).isSuccess());
		} finally {
			gate.countDown();
		}
	}

	/**
	 * A leader that cannot apply a committed entry within a round timeout fails the barrier with
	 * {@link ApplyLagException}: it neither serves the read without that entry nor keeps the caller waiting.
	 */
	@Test
	void leaderThatCannotApplyFailsReadsWithApplyLag() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var client = client();
		long commitBefore = leader.commitIndex();
		var gate = new CountDownLatch(1);
		leader.stateMachine.applyGate = gate;
		try {
			var write = client.async().send(Message.valueOf("inc"));
			waitUntil(() -> leader.commitIndex() > commitBefore, "write committed");
			long readyIndex = leader.commitIndex();
			// As if that entry were an earlier leader's, still waiting for this node's apply thread.
			var leadershipBarrier = new ReadBarrier(leader.server, groupId, leader.stateMachine.applied, ROUND_TIMEOUT,
					_ -> readyIndex);
			barriers.add(leadershipBarrier);

			long start = System.nanoTime();
			var read = barrier(leader).await();
			var leadership = leadershipBarrier.awaitLeadership();
			assertInstanceOf(ApplyLagException.class, failure(read));
			assertInstanceOf(ApplyLagException.class, failure(leadership));
			// Leadership was confirmed, then the apply wait ran for a whole round timeout.
			assertTrue(System.nanoTime() - start >= ROUND_TIMEOUT.toNanos(),
					"failed after " + Duration.ofNanos(System.nanoTime() - start).toMillis() + " ms");
			assertEquals(0, leader.stateMachine.writes.get());

			gate.countDown();
			assertTrue(write.get(10, TimeUnit.SECONDS).isSuccess());
			barrier(leader).await().get(10, TimeUnit.SECONDS);
			leadershipBarrier.awaitLeadership().get(10, TimeUnit.SECONDS);
			assertEquals(1, leader.stateMachine.writes.get());
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

		followers.forEach(Node::holdTermCalls);
		assertInstanceOf(TimeoutException.class, failure(barrier.await()));

		followers.forEach(Node::releaseTermCalls);
		barrier.await().get(10, TimeUnit.SECONDS);
	}

	@Test
	void unansweringPeerHasAtMostOneQueryInFlight() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var paused = followers.get(0);
		var other = followers.get(1);
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		// Like a paused process: its connection stays up, but it never answers.
		paused.ignoreTermCalls = true;
		long callsBefore = paused.termCalls.get();
		long start = System.nanoTime();
		int rounds = 0;
		while (System.nanoTime() - start < ROUND_TIMEOUT.multipliedBy(3).toNanos()) {
			// Confirmed by the leader and the other follower.
			barrier.await().get(10, TimeUnit.SECONDS);
			rounds++;
		}
		long timeouts = Math.ceilDiv(System.nanoTime() - start, ROUND_TIMEOUT.toNanos());
		long calls = paused.termCalls.get() - callsBefore;

		assertTrue(rounds >= 50, "rounds: " + rounds);
		// A query lasts until the end of the round that sent it; only a round after that sends the next one.
		assertTrue(calls <= 2 * timeouts + 1, calls + " calls in " + rounds + " rounds and " + timeouts + " timeouts");
		// One, plus at most one that the caller gave up on but whose cancellation the peer did not see yet.
		assertTrue(paused.mostOpenTermCalls.get() <= 2, "open calls: " + paused.mostOpenTermCalls.get());

		// Once the peer answers again it confirms rounds as before, here alone with the leader.
		paused.ignoreTermCalls = false;
		waitUntil(() -> paused.openTermCalls.get() == 0, "the unanswered call given up");
		other.stop();
		barrier.await().get(10, TimeUnit.SECONDS);
	}

	@Test
	void closeFailsPendingCalls() throws Exception {
		startCluster();
		var leader = awaitLeader();
		followersOf(leader).forEach(Node::holdTermCalls);
		var barrier = new ReadBarrier(leader.server, groupId, leader.stateMachine.applied, Duration.ofMinutes(1));
		barriers.add(barrier);

		var pending = barrier.await();
		barrier.close();
		assertNotLeader(pending);
		assertNotLeader(barrier.await());
	}

	@Test
	void roundInFlightFailsWhenLeaderIsDeposed() throws Exception {
		startCluster();
		var oldLeader = awaitLeader();
		var followers = followersOf(oldLeader);
		// A round can only end through a reply here, not through its timeout.
		var barrier = new ReadBarrier(oldLeader.server, groupId, oldLeader.stateMachine.applied, Duration.ofMinutes(1));
		barriers.add(barrier);
		var callsBefore = followers.stream().map(node -> node.termCalls.get()).toList();

		followers.forEach(Node::holdTermCalls);
		var inFlight = barrier.await();
		waitUntil(() -> followers.get(0).termCalls.get() > callsBefore.get(0)
				&& followers.get(1).termCalls.get() > callsBefore.get(1), "round sent");
		var queued = barrier.await();

		makeLeader(followers.getFirst());
		// The followers now answer with the new leader's term.
		followers.forEach(Node::releaseTermCalls);
		assertNotLeader(inFlight);
		assertNotLeader(queued);
	}

	@Test
	void jointConfigurationNeedsBothMajorities() throws Exception {
		// A long election timeout keeps node 0 the leader for seconds after it lost the old majority.
		startCluster(Duration.ofSeconds(5), Duration.ofSeconds(6));
		var leader = nodes.getFirst();
		makeLeader(leader);
		var kept = nodes.get(1);
		var replaced = nodes.get(2);
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		// Replace n2 with n3. Without n1 and n3 the joint configuration (old {n0, n1, n2}, new {n0, n1, n3}) has no
		// majority of the new members, so while they refuse to store it, the leader stays in it.
		var added = addNode("n3", RaftGroup.valueOf(groupId));
		kept.refuseJointConfiguration = true;
		added.refuseJointConfiguration = true;
		var newPeers = List.of(leader.peer, kept.peer, added.peer);
		var admin = client();
		Thread.ofVirtual().start(() -> {
			try {
				admin.admin().setConfiguration(newPeers);
			} catch (IOException e) {
				// Expected: the change cannot complete; the client is closed at the end of the test.
			}
		});
		waitUntil(() -> !leader.division().getRaftConf().getPreviousPeers().isEmpty(), "joint configuration");

		barrier.await().get(10, TimeUnit.SECONDS);

		// Only the old configuration has a majority answering: n0 and n2.
		kept.holdTermCalls();
		added.holdTermCalls();
		assertInstanceOf(TimeoutException.class, failure(barrier.await()));
		kept.releaseTermCalls();
		added.releaseTermCalls();
		barrier.await().get(10, TimeUnit.SECONDS);

		// Only the new configuration has a majority answering: n0 and n3.
		kept.stop();
		replaced.stop();
		assertTrue(leader.isLeader() && !leader.division().getRaftConf().getPreviousPeers().isEmpty(),
				"precondition: still the leader of the joint configuration");
		var error = failure(barrier.await());
		assertTrue(error instanceof TimeoutException || error instanceof NotLeaderException, error.toString());
	}

	@Test
	void barrierFollowsPeerAddressChange() throws Exception {
		// A long election timeout keeps node 0 the leader even if Ratis itself cannot reach the moved peer.
		startCluster(Duration.ofSeconds(5), Duration.ofSeconds(6));
		var leader = nodes.getFirst();
		makeLeader(leader);
		var followers = followersOf(leader);
		var other = followers.get(1);
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		var moved = restart(followers.get(0), freeAddress());
		// Ratis ignores a configuration whose peers differ only by address, so the leader's priority changes too.
		var newPeers = List.of(withPriority(leader.peer, 1), moved.peer, other.peer);
		assertTrue(client().admin().setConfiguration(newPeers).isSuccess());
		waitUntil(() -> {
			var conf = leader.division().getRaftConf();
			return conf.getPreviousPeers().isEmpty() && conf.getCurrentPeers().stream()
					.anyMatch(peer -> peer.getId().equals(moved.peer.getId())
							&& peer.getAddress().equals(moved.peer.getAddress()));
		}, "new address committed");
		other.stop();

		barrier.await().get(10, TimeUnit.SECONDS);
		assertTrue(moved.termCalls.get() > 0);
	}

	@Test
	void followerBackOnItsAddressIsReachedAtOnce() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var restarted = followers.get(0);
		var other = followers.get(1);
		var address = restarted.peer.getAddress();
		var barrier = barrier(leader);
		barrier.await().get(10, TimeUnit.SECONDS);

		restarted.stop();
		try (var refuser = new RefusingListener(address)) {
			// Barriers keep succeeding through the other follower and keep dialing the stopped one, which fails.
			// gRPC alone would wait longer and longer between attempts; the barrier must not let it.
			while (refuser.longestGap().compareTo(Duration.ofSeconds(3)) < 0 && refuser.attempts() < 20) {
				barrier.await().get(10, TimeUnit.SECONDS);
			}
		}
		long commitIndex = leader.commitIndex();
		var back = restart(restarted, address);
		waitUntil(() -> back.commitIndex() >= commitIndex, "restarted follower caught up");
		other.stop();

		// Only the restarted follower can confirm this leadership, on the first try.
		barrier.await().get(10, TimeUnit.SECONDS);
	}

	@Test
	void blockedContinuationDoesNotStallLaterBarriers() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var followers = followersOf(leader);
		var barrier = barrier(leader);
		var release = new CountDownLatch(1);
		try {
			followers.forEach(Node::holdTermCalls);
			var first = barrier.await();
			// Like a slow database read run after the barrier.
			var slowReader = first.handle((_, _) -> awaitUninterruptibly(release));
			var second = barrier.await();
			followers.forEach(Node::releaseTermCalls);

			second.get(10, TimeUnit.SECONDS);
			release.countDown();
			slowReader.get(10, TimeUnit.SECONDS);
		} finally {
			release.countDown();
		}
	}

	@Test
	void blockedContinuationDoesNotStallApply() throws Exception {
		startCluster();
		var leader = awaitLeader();
		var follower = followersOf(leader).getFirst();
		var barrier = barrier(leader);
		var client = client();
		long commitBefore = leader.commitIndex();

		var gate = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		leader.stateMachine.applyGate = gate;
		try {
			var write = client.async().send(Message.valueOf("inc"));
			waitUntil(() -> leader.commitIndex() > commitBefore, "write committed");
			long callsBefore = follower.termCalls.get();
			var read = barrier.await();
			var slowReader = read.thenRun(() -> awaitUninterruptibly(release));
			// Rounds run one at a time: once the next one is sent, the read only waits for the write's apply.
			barrier.await();
			waitUntil(() -> follower.termCalls.get() >= callsBefore + 2, "second round sent");

			gate.countDown();
			assertTrue(write.get(10, TimeUnit.SECONDS).isSuccess());
			// The leader keeps applying while the reader is blocked.
			assertTrue(client.async().send(Message.valueOf("inc")).get(10, TimeUnit.SECONDS).isSuccess());
			assertEquals(2, leader.stateMachine.writes.get());
			release.countDown();
			slowReader.get(10, TimeUnit.SECONDS);
		} finally {
			release.countDown();
			gate.countDown();
		}
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
					slow ? firstMax : FAST_ELECTION_MAX, RaftStorage.StartupOption.FORMAT));
		}
	}

	/** Starts a node outside the initial group, to be added by a configuration change. */
	private Node addNode(String id, RaftGroup group) throws IOException {
		var node = startNode(peer(id), group, FAST_ELECTION_MIN, FAST_ELECTION_MAX, RaftStorage.StartupOption.FORMAT);
		nodes.add(node);
		return node;
	}

	/** Stops {@code node} if needed and starts it again from its storage, listening on {@code address}. */
	private Node restart(Node node, String address) throws IOException {
		node.stop();
		var peer = RaftPeer.newBuilder().setId(node.peer.getId()).setAddress(address).build();
		var restarted = startNode(peer, RaftGroup.valueOf(groupId), RESTART_ELECTION_MIN, RESTART_ELECTION_MAX,
				RaftStorage.StartupOption.RECOVER);
		nodes.add(restarted);
		return restarted;
	}

	private Node startNode(RaftPeer peer,
			RaftGroup group,
			Duration electionMin,
			Duration electionMax,
			RaftStorage.StartupOption startupOption) throws IOException {
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
		var termService = TermService.customizer(node::serverAnsweringTermCalls, new HostAndPort("127.0.0.1", 0));
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
				.setOption(startupOption)
				.build();
		node.serverRef.set(node.server);
		node.server.start();
		return node;
	}

	private static RaftPeer peer(String id) throws IOException {
		return RaftPeer.newBuilder().setId(id).setAddress(freeAddress()).build();
	}

	/** Not a port from the system: see {@link InProcessCluster}. */
	private static String freeAddress() throws IOException {
		return "127.0.0.1:" + ProcessCluster.freePorts(1, List.of()).getFirst();
	}

	private static RaftPeer withPriority(RaftPeer peer, int priority) {
		return RaftPeer.newBuilder(peer).setPriority(priority).build();
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

	private static Void awaitUninterruptibly(CountDownLatch latch) {
		try {
			if (!latch.await(WAIT.toNanos(), TimeUnit.NANOSECONDS)) {
				throw new AssertionError("latch never opened");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError(e);
		}
		return null;
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
		/** While set, term requests are answered only once it opens, with the term at that time. */
		private volatile CountDownLatch termCallGate;
		/** While set, term requests are never answered, and stay open until the caller gives up on them. */
		volatile boolean ignoreTermCalls;
		/** Term requests ignored and not given up yet by their caller. */
		final AtomicLong openTermCalls = new AtomicLong();
		/** The highest {@link #openTermCalls} seen. */
		final AtomicLong mostOpenTermCalls = new AtomicLong();
		/** Ratis methods (full names) refused with UNAVAILABLE, to cut Raft traffic into this node. */
		volatile Set<String> rejectedMethods = Set.of();
		/** Drop the log entries that carry a joint (old and new) configuration, so that it never commits. */
		volatile boolean refuseJointConfiguration;
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

		void holdTermCalls() {
			termCallGate = new CountDownLatch(1);
		}

		void releaseTermCalls() {
			var gate = termCallGate;
			termCallGate = null;
			if (gate != null) {
				gate.countDown();
			}
		}

		/** The server for {@link TermService}, once term calls are no longer held. */
		RaftServer serverAnsweringTermCalls() {
			var gate = termCallGate;
			if (gate != null) {
				awaitUninterruptibly(gate);
			}
			return serverRef.get();
		}

		void stop() throws IOException {
			if (!stopped) {
				stopped = true;
				stateMachine.applyGate = null;
				releaseTermCalls();
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
						if (ignoreTermCalls) {
							return ignore();
						}
					} else if (rejectedMethods.contains(method.getFullMethodName())) {
						call.close(Status.UNAVAILABLE.withDescription("partitioned by the test"), new Metadata());
						return new ServerCall.Listener<>() {};
					} else if (APPEND_ENTRIES.equals(method.getFullMethodName())) {
						return dropJointConfiguration(call, next.startCall(call, headers));
					}
					return next.startCall(call, headers);
				}
			};
		}

		/** Leaves a call open, unanswered, until its caller cancels it. */
		private <Q> ServerCall.Listener<Q> ignore() {
			mostOpenTermCalls.accumulateAndGet(openTermCalls.incrementAndGet(), Math::max);
			return new ServerCall.Listener<>() {
				@Override
				public void onCancel() {
					openTermCalls.decrementAndGet();
				}
			};
		}

		private <Q> ServerCall.Listener<Q> dropJointConfiguration(ServerCall<Q, ?> call, ServerCall.Listener<Q> next) {
			return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next) {
				@Override
				public void onMessage(Q message) {
					if (refuseJointConfiguration && message instanceof AppendEntriesRequestProto request
							&& request.getEntriesList().stream().anyMatch(entry -> entry.hasConfigurationEntry()
									&& entry.getConfigurationEntry().getOldPeersCount() > 0)) {
						// Unanswered, as if lost: the leader sends it again later. Keep the stream flowing.
						call.request(1);
					} else {
						super.onMessage(message);
					}
				}
			};
		}

		@Override
		public String toString() {
			return peer.getId().toString();
		}
	}

	/** Listens on a stopped node's address and resets every connection, recording when each attempt arrived. */
	private static final class RefusingListener implements AutoCloseable {

		private final ServerSocket socket = new ServerSocket();
		private final List<Long> attemptTimes = new CopyOnWriteArrayList<>();
		private final Thread acceptor;

		RefusingListener(String address) throws IOException {
			int separator = address.lastIndexOf(':');
			socket.setReuseAddress(true);
			socket.bind(new InetSocketAddress(address.substring(0, separator),
					Integer.parseInt(address.substring(separator + 1))));
			acceptor = Thread.ofVirtual().start(this::refuseAll);
		}

		int attempts() {
			return attemptTimes.size();
		}

		Duration longestGap() {
			long longest = 0;
			for (int i = 1; i < attemptTimes.size(); i++) {
				longest = Math.max(longest, attemptTimes.get(i) - attemptTimes.get(i - 1));
			}
			return Duration.ofNanos(longest);
		}

		private void refuseAll() {
			while (true) {
				try (var connection = socket.accept()) {
					attemptTimes.add(System.nanoTime());
					// A reset leaves no TIME_WAIT behind, so the restarted node can bind the port again.
					connection.setSoLinger(true, 0);
				} catch (IOException e) {
					return;
				}
			}
		}

		@Override
		public void close() throws IOException, InterruptedException {
			socket.close();
			acceptor.join();
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
