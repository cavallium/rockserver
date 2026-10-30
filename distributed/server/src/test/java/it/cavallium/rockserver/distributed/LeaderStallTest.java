package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.util.CodeInjectionForTesting;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rule of {@link LeaderStall}, then the stall itself on a real group: nodes in this JVM, with Raft going through the
 * links of a {@link Network}, so that a test can lose exactly one reply.
 */
@Timeout(120)
class LeaderStallTest {

	private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofSeconds(2);
	private static final long TICK = TimeUnit.MILLISECONDS.toNanos(200);
	private static final long STALL = LeaderStall.MIN_STALL.toNanos();
	private static final long COMMIT = 100;
	private static final long TERM = 7;
	private static final long LOG_NEXT = 200;
	private static final long RECENT = 100;
	private static final long LONG_AGO = 60_000;
	private static final List<String> THREE = List.of("a", "b", "c");
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;
	private Network network;

	@AfterEach
	void tearDown() throws IOException {
		CodeInjectionForTesting.remove(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST);
		if (cluster != null) {
			cluster.close();
		}
		if (network != null) {
			network.close();
		}
	}

	/** Not zero: the clock of {@link System#nanoTime()} starts anywhere. */
	private long now = -123_456_789L;
	private final LeaderStall stall = LeaderStall.forElectionTimeout(ELECTION_TIMEOUT_MAX);

	/** Observes this every tick for this long, and returns the last answer. */
	private boolean run(boolean waiting, long commit, long nanos) {
		boolean answer = stall.shouldStepDown(waiting, TERM, commit, now);
		for (long elapsed = 0; elapsed < nanos; elapsed += TICK) {
			now += TICK;
			answer = stall.shouldStepDown(waiting, TERM, commit, now);
		}
		return answer;
	}

	@Test
	void aLeaderWaitingForTheWholeBoundStepsDownAndNotBefore() {
		assertFalse(run(true, COMMIT, STALL - TICK));
		assertTrue(run(true, COMMIT, TICK));
	}

	/** The step down may not happen, for example with the event queue of the leader stuck. */
	@Test
	void anotherStepDownTakesAnotherWholeBound() {
		run(true, COMMIT, STALL);
		assertFalse(run(true, COMMIT, STALL - TICK));
		assertTrue(run(true, COMMIT, TICK));
	}

	@Test
	void aMovingCommitIndexIsNoStall() {
		for (long commit = 0; commit < 100; commit++) {
			assertFalse(run(true, commit, STALL - TICK));
		}
	}

	@Test
	void aBreakInTheWaitStartsTheBoundAgain() {
		assertFalse(run(true, COMMIT, STALL - TICK));
		assertFalse(run(false, COMMIT, TICK));
		assertFalse(run(true, COMMIT, STALL - TICK));
		assertTrue(run(true, COMMIT, TICK));
	}

	@Test
	void aNewTermStartsTheBoundAgain() {
		assertFalse(run(true, COMMIT, STALL - TICK));
		now += TICK;
		assertFalse(stall.shouldStepDown(true, TERM + 1, COMMIT, now));
		now += TICK;
		assertFalse(stall.shouldStepDown(true, TERM + 1, COMMIT, now));
	}

	@Test
	void aLongElectionTimeoutLengthensTheBound() {
		var longTimeout = LeaderStall.forElectionTimeout(Duration.ofSeconds(4));
		assertFalse(longTimeout.shouldStepDown(true, TERM, COMMIT, 0));
		assertFalse(longTimeout.shouldStepDown(true, TERM, COMMIT, TimeUnit.SECONDS.toNanos(20) - 1));
		assertTrue(longTimeout.shouldStepDown(true, TERM, COMMIT, TimeUnit.SECONDS.toNanos(20)));
	}

	@Test
	void aShortElectionTimeoutDoesNotShortenTheBound() {
		var shortTimeout = LeaderStall.forElectionTimeout(Duration.ofMillis(800));
		assertFalse(shortTimeout.shouldStepDown(true, TERM, COMMIT, 0));
		assertFalse(shortTimeout.shouldStepDown(true, TERM, COMMIT, STALL - 1));
		assertTrue(shortTimeout.shouldStepDown(true, TERM, COMMIT, STALL));
	}

	private static boolean sentToMajority(List<String> members, List<String> previous, LeaderStall.Follower... followers) {
		return LeaderStall.sentToMajority(List.of(followers), LOG_NEXT, ELECTION_TIMEOUT_MAX.toMillis(), "a", members,
				previous);
	}

	@Test
	void theOnlyPartnerSentEverythingIsAMajority() {
		assertTrue(sentToMajority(THREE, List.of(), new LeaderStall.Follower("b", LOG_NEXT, RECENT),
				new LeaderStall.Follower("c", 5, LONG_AGO)));
	}

	/** It would start a snapshot or a long catch-up from the beginning. */
	@Test
	void aPartnerStillCatchingUpIsNoStall() {
		assertFalse(sentToMajority(THREE, List.of(), new LeaderStall.Follower("b", LOG_NEXT - 1, RECENT),
				new LeaderStall.Follower("c", 5, LONG_AGO)));
	}

	/** A next index stays behind by nothing on a dead follower: the stream failed without saying what it lost. */
	@Test
	void aDeadFollowerDoesNotCount() {
		assertFalse(sentToMajority(THREE, List.of(), new LeaderStall.Follower("b", 5, RECENT),
				new LeaderStall.Follower("c", LOG_NEXT, LONG_AGO)));
	}

	@Test
	void aLeaderWithoutAMajorityIsNoStall() {
		assertFalse(sentToMajority(THREE, List.of(), new LeaderStall.Follower("b", LOG_NEXT, LONG_AGO),
				new LeaderStall.Follower("c", LOG_NEXT, LONG_AGO)));
	}

	@Test
	void aPeerThatIsNotAMemberDoesNotCount() {
		assertFalse(sentToMajority(THREE, List.of(), new LeaderStall.Follower("d", LOG_NEXT, RECENT)));
	}

	@Test
	void bothConfigurationsMustHaveAMajority() {
		var followers = new LeaderStall.Follower[] {new LeaderStall.Follower("b", LOG_NEXT, RECENT)};
		assertTrue(sentToMajority(THREE, List.of("a", "b", "x"), followers));
		assertFalse(sentToMajority(THREE, List.of("a", "x", "y"), followers));
	}

	/**
	 * With one node down, a new leader's first entry reaches the disk of the other node, and the reply is lost when the
	 * stream dies. Writes are refused until that entry commits, and only a new leader sends it again.
	 */
	@Test
	void aReplyLostForTheFirstEntryOfATermEndsWithANewLeader() throws Exception {
		startRoutedCluster();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		long column = oldLeader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var followers = cluster.runningNodes().stream().filter(node -> node != oldLeader).toList();
		var partner = followers.getFirst();
		cluster.halt(followers.getLast().config().nodeId());

		String partnerId = partner.config().nodeId();
		String oldLeaderId = oldLeader.config().nodeId();
		var armed = new CountDownLatch(1);
		CodeInjectionForTesting.put(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST, (localId, _, request) -> {
			if (request[0] instanceof RaftProtos.AppendEntriesRequestProto proto && partnerId.equals(localId.toString())
					&& proto.getEntriesCount() > 0 && oldLeaderId.equals(proto.getServerRequest().getReplyId().toStringUtf8())
					&& armed.getCount() > 0) {
				network.dropReplies(partnerId, oldLeaderId);
				armed.countDown();
			}
			return true;
		});
		// A new leader first sends a heartbeat, then its first entry when the answer comes: that is the request to arm on.
		oldLeader.raftServerForTests().transferLeadershipAsync(transferRequest(oldLeader, partnerId));
		assertTrue(armed.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS), "the partner did not become leader");

		var partnerDivision = partner.stateMachine().division();
		var followerDivision = oldLeader.stateMachine().division();
		long term = partnerDivision.getInfo().getCurrentTerm();
		InProcessCluster.await(TIMEOUT, () -> {
			var last = followerDivision.getRaftLog().getLastEntryTermIndex();
			return last != null && last.getTerm() == term
					&& followerDivision.getRaftLog().getFlushIndex() >= last.getIndex();
		});
		network.blockLink(partnerId, oldLeaderId);
		network.heal();

		// The heartbeats after the reconnection succeed, and nothing commits.
		var log = partnerDivision.getRaftLog();
		long stalledAt = log.getLastCommittedIndex();
		long end = System.nanoTime() + Duration.ofSeconds(4).toNanos();
		while (System.nanoTime() - end < 0) {
			assertTrue(partner.isLeader(), "the partner lost its leadership");
			assertEquals(stalledAt, log.getLastCommittedIndex(), "the entry was committed: its reply was not lost");
			Thread.sleep(20);
		}

		var stepDowns = partner.metrics().amount("leader.stall.stepdowns");
		awaitWrite(column);
		assertEquals(1, stepDowns.count());
	}

	/**
	 * The same loss after the first commit of a term is no stall: writes are accepted, and the reply to the next one moves
	 * the match index past the entry whose reply was lost.
	 */
	@Test
	void aReplyLostAfterTheFirstCommitOfATermHealsWithTheNextWrite() throws Exception {
		startRoutedCluster();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = leader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
		var partner = followers.getFirst();
		cluster.halt(followers.getLast().config().nodeId());

		String leaderId = leader.config().nodeId();
		String partnerId = partner.config().nodeId();
		var armed = new CountDownLatch(1);
		CodeInjectionForTesting.put(GrpcServicesImpl.GRPC_SEND_SERVER_REQUEST, (localId, _, request) -> {
			if (request[0] instanceof RaftProtos.AppendEntriesRequestProto proto && leaderId.equals(localId.toString())
					&& proto.getEntriesCount() > 0 && partnerId.equals(proto.getServerRequest().getReplyId().toStringUtf8())
					&& armed.getCount() > 0) {
				network.dropReplies(leaderId, partnerId);
				armed.countDown();
			}
			return true;
		});
		var log = leader.stateMachine().division().getRaftLog();
		long committed = log.getLastCommittedIndex();
		var lost = leader.connection().getAsyncApi(CONTEXT).putAsync(0, column, key(), Buf.wrap(new byte[] {1}),
				RequestType.none());
		assertTrue(armed.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS), "the write did not go out");
		var partnerLog = partner.stateMachine().division().getRaftLog();
		InProcessCluster.await(TIMEOUT, () -> partnerLog.getFlushIndex() > committed);
		long lostIndex = partnerLog.getFlushIndex();
		network.blockLink(leaderId, partnerId);
		network.heal();

		leader.connection().getSyncApi(CONTEXT).put(0, column, key(), Buf.wrap(new byte[] {2}), RequestType.none());
		assertTrue(log.getLastCommittedIndex() > lostIndex);
		lost.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertTrue(leader.isLeader());
		assertEquals(0, leader.metrics().amount("leader.stall.stepdowns").count());
	}

	/**
	 * A leader behind in applying, even for longer than the bound, keeps leading: its first entry is committed. Its apply
	 * is stopped here, with one follower down, as after a long replay.
	 */
	@Test
	void aLeaderBehindInApplyingKeepsLeading() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		long column = oldLeader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var followers = cluster.runningNodes().stream().filter(node -> node != oldLeader).toList();
		var partner = followers.getFirst();
		cluster.halt(followers.getLast().config().nodeId());

		var stopped = new CountDownLatch(1);
		partner.stateMachine().applyGateForTesting = stopped;
		try {
			oldLeader.connection().getSyncApi(CONTEXT).put(0, column, key(), Buf.wrap(new byte[] {1}), RequestType.none());
			long applied = partner.appliedIndex();
			var reply = oldLeader.raftServerForTests().transferLeadership(transferRequest(oldLeader,
					partner.config().nodeId()));
			assertTrue(reply.isSuccess(), () -> "transfer failed: " + reply);
			var division = partner.stateMachine().division();
			InProcessCluster.await(TIMEOUT, partner::isLeader);
			long term = division.getInfo().getCurrentTerm();
			InProcessCluster.await(TIMEOUT, () -> {
				var log = division.getRaftLog();
				var committed = log.getTermIndex(log.getLastCommittedIndex());
				return committed != null && committed.getTerm() == term;
			});
			Thread.sleep(LeaderStall.MIN_STALL.plusSeconds(2).toMillis());
			assertEquals(applied, partner.appliedIndex(), "the apply was not stopped");
			assertTrue(partner.isLeader());
			assertEquals(term, division.getInfo().getCurrentTerm());
			assertEquals(0, partner.metrics().amount("leader.stall.stepdowns").count());
		} finally {
			partner.stateMachine().applyGateForTesting = null;
			stopped.countDown();
		}
	}

	/** Every node reaches the others through the links of {@link #network}, with its own view of the addresses. */
	private void startRoutedCluster() throws IOException {
		var linksRoot = root.resolve("links");
		cluster = new InProcessCluster(root, 3, config -> {
			if (network == null) {
				var ports = new LinkedHashMap<String, List<Integer>>();
				config.peers().forEach(peer -> ports.put(peer.id(), List.of(peer.raftPort())));
				try {
					network = Network.start(ports, linksRoot);
				} catch (IOException e) {
					throw new java.io.UncheckedIOException(e);
				}
			}
			return config.withPeers(config.peers().stream()
					.map(peer -> peer.id().equals(config.nodeId()) ? peer : new PeerConfig(peer.id(),
							network.linkAddress(config.nodeId(), peer.id()).getHostAddress(), peer.raftPort(), peer.apiHost(),
							peer.apiPort()))
					.toList())
					.withElectionTimeouts(Duration.ofSeconds(1), ELECTION_TIMEOUT_MAX);
		}).startAll();
	}

	private static TransferLeadershipRequest transferRequest(ReplicatedNode from, String to) {
		var groupId = RaftGroupId.valueOf(UUID.fromString(from.config().groupId()));
		return new TransferLeadershipRequest(ClientId.randomId(), from.raftServerForTests().getId(), groupId, 0,
				RaftPeerId.valueOf(to), TIMEOUT.toMillis());
	}

	private void awaitWrite(long column) {
		InProcessCluster.await(TIMEOUT, () -> {
			var node = cluster.runningNodes().stream().filter(ReplicatedNode::isLeader).findFirst();
			if (node.isEmpty()) {
				return false;
			}
			try {
				node.get().connection().getSyncApi(CONTEXT).put(0, column, key(), Buf.wrap(new byte[] {1}), RequestType.none());
				return true;
			} catch (RuntimeException notYet) {
				return false;
			}
		});
	}

	private static Keys key() {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array()));
	}
}
