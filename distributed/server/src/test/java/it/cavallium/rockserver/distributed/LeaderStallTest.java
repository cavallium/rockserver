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
import java.io.UncheckedIOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.grpc.server.GrpcServicesImpl;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.ServerImplUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
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

	private static RaftProtos.ServerRpcProto follower(String id, long lastReplyMillisAgo) {
		return RaftProtos.ServerRpcProto.newBuilder()
				.setId(RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:1").build().getRaftPeerProto())
				.setLastRpcElapsedTimeMs(lastReplyMillisAgo)
				.build();
	}

	/** Ratis gives the next indices without ids: they pair only with follower lists that agree around them. */
	@Test
	void nextIndicesArePairedWithFollowerIdsOnlyWhenTheListDidNotChange() {
		var before = List.of(follower("b", 5), follower("c", 7));
		var after = List.of(follower("b", 1), follower("c", 2));
		var paired = LeaderStall.pairById(before, new long[] {10, 20}, after);
		assertEquals(List.of(new LeaderStall.Follower("b", 10, 1), new LeaderStall.Follower("c", 20, 2)), paired);

		var reordered = List.of(follower("c", 2), follower("b", 1));
		assertEquals(null, LeaderStall.pairById(before, new long[] {10, 20}, reordered));
		var replaced = List.of(follower("b", 1), follower("d", 2));
		assertEquals(null, LeaderStall.pairById(before, new long[] {10, 20}, replaced));
		assertEquals(null, LeaderStall.pairById(before, new long[] {10}, after));
		assertEquals(null, LeaderStall.pairById(before, null, after));
		assertEquals(null, LeaderStall.pairById(before, new long[] {10, 20, 30},
				List.of(follower("b", 1), follower("c", 2), follower("d", 3))));
	}

	@Test
	void aLeaderWaitingForTheWholeBoundStepsDownAndNotBefore() {
		assertFalse(run(true, COMMIT, STALL - TICK));
		assertTrue(run(true, COMMIT, TICK));
	}

	/** The step down may not happen, for example with the event queue of the leader stuck. */
	@Test
	void anotherStepDownTakesTwiceTheBound() {
		run(true, COMMIT, STALL);
		assertFalse(run(true, COMMIT, 2 * STALL - TICK));
		assertTrue(run(true, COMMIT, TICK));
	}

	/** Disks that take longer than the bound to answer: each leader must be given the time to reach its first commit. */
	@Test
	void theBoundGrowsWithEachStepDownWithoutACommit() {
		for (int doublings = 0; doublings <= LeaderStall.MAX_DOUBLINGS; doublings++) {
			assertFalse(run(true, COMMIT, (STALL << doublings) - TICK));
			assertTrue(run(true, COMMIT, TICK));
		}
		assertFalse(run(true, COMMIT, (STALL << LeaderStall.MAX_DOUBLINGS) - TICK));
		assertTrue(run(true, COMMIT, TICK), "the bound did not stop growing");
	}

	/** Under another leader this node sees the commit that the slow disks allowed in the end. */
	@Test
	void aCommitBringsTheBoundBack() {
		run(true, COMMIT, STALL);
		run(true, COMMIT, 2 * STALL);
		assertFalse(run(false, COMMIT, TICK));
		assertFalse(run(false, COMMIT + 1, TICK));
		assertFalse(run(true, COMMIT + 1, STALL - TICK));
		assertTrue(run(true, COMMIT + 1, TICK));
	}

	/** A new term is no progress: the term of the next leader of a slow disk starts with the same commit index. */
	@Test
	void aNewTermAloneDoesNotBringTheBoundBack() {
		run(true, COMMIT, STALL);
		now += TICK;
		assertFalse(stall.shouldStepDown(true, TERM + 1, COMMIT, now));
		for (long elapsed = 0; elapsed < 2 * STALL - TICK; elapsed += TICK) {
			now += TICK;
			assertFalse(stall.shouldStepDown(true, TERM + 1, COMMIT, now));
		}
		now += TICK;
		assertTrue(stall.shouldStepDown(true, TERM + 1, COMMIT, now));
	}

	private static final long LOG_STALL = ELECTION_TIMEOUT_MAX.multipliedBy(LeaderStall.ELECTION_TIMEOUTS_TO_LOG_STALL).toNanos();

	/** Observes this every tick for this long, and returns the last answer. */
	private boolean runLog(boolean leader, long flushIndex, long lastIndex, long nanos) {
		boolean answer = stall.logStopped(leader, flushIndex, lastIndex, now);
		for (long elapsed = 0; elapsed < nanos; elapsed += TICK) {
			now += TICK;
			answer = stall.logStopped(leader, flushIndex, lastIndex, now);
		}
		return answer;
	}

	@Test
	void aLeaderWithEntriesItDoesNotFlushStepsDownAfterTheBound() {
		assertFalse(runLog(true, COMMIT, COMMIT + 1, LOG_STALL - TICK));
		assertTrue(runLog(true, COMMIT, COMMIT + 1, TICK));
	}

	@Test
	void aMovingFlushIndexIsNoStoppedLog() {
		for (long flushIndex = 0; flushIndex < 100; flushIndex++) {
			assertFalse(runLog(true, flushIndex, flushIndex + 1, LOG_STALL - TICK));
		}
	}

	/** Nothing to write: a hung disk would not show, and costs nothing yet. */
	@Test
	void aLeaderWithNothingToFlushIsNoStoppedLog() {
		assertFalse(runLog(true, COMMIT, COMMIT, 2 * LOG_STALL));
	}

	/** Only a leader's flush holds back commits: other members can miss a follower. */
	@Test
	void aFollowerIsNoStoppedLog() {
		assertFalse(runLog(false, COMMIT, COMMIT + 1, 2 * LOG_STALL));
	}

	/** Disks that are all this slow must not end every leader in turn. */
	@Test
	void theLogBoundGrowsLikeTheOtherOne() {
		assertTrue(runLog(true, COMMIT, COMMIT + 1, LOG_STALL));
		assertFalse(runLog(true, COMMIT, COMMIT + 1, 2 * LOG_STALL - TICK));
		assertTrue(runLog(true, COMMIT, COMMIT + 1, TICK));
	}

	/** With rounds that take more than the bound, commits still happen: they say nothing about the disk. */
	@Test
	void aSlowFlushKeepsTheLogBoundLong() {
		assertTrue(runLog(true, COMMIT, COMMIT + 1, LOG_STALL));
		assertFalse(runLog(true, COMMIT, COMMIT + 1, LOG_STALL + TICK));
		assertFalse(runLog(true, COMMIT + 1, COMMIT + 2, 2 * LOG_STALL - TICK));
		assertTrue(runLog(true, COMMIT + 1, COMMIT + 2, TICK));
	}

	@Test
	void aQuickFlushBringsTheLogBoundBack() {
		assertTrue(runLog(true, COMMIT, COMMIT + 1, LOG_STALL));
		assertFalse(runLog(true, COMMIT, COMMIT + 1, TICK));
		assertFalse(runLog(true, COMMIT + 1, COMMIT + 2, LOG_STALL - TICK));
		assertTrue(runLog(true, COMMIT + 1, COMMIT + 2, TICK));
	}

	/** The first tick after a step down has no leader, right after the bound: that is no quick flush. */
	@Test
	void aStepDownIsNoQuickFlush() {
		assertTrue(runLog(true, COMMIT, COMMIT + 1, LOG_STALL));
		assertFalse(runLog(false, COMMIT, COMMIT + 1, TICK));
		assertFalse(runLog(true, COMMIT, COMMIT + 1, 2 * LOG_STALL - TICK));
		assertTrue(runLog(true, COMMIT, COMMIT + 1, TICK));
	}

	@Test
	void aShortElectionTimeoutDoesNotShortenTheLogBound() {
		var shortTimeout = LeaderStall.forElectionTimeout(Duration.ofMillis(800));
		long bound = LeaderStall.MIN_LOG_STALL.toNanos();
		assertFalse(shortTimeout.logStopped(true, COMMIT, COMMIT + 1, 0));
		assertFalse(shortTimeout.logStopped(true, COMMIT, COMMIT + 1, bound - 1));
		assertTrue(shortTimeout.logStopped(true, COMMIT, COMMIT + 1, bound));
	}

	@Test
	void aLongElectionTimeoutLengthensTheLogBound() {
		var longTimeout = LeaderStall.forElectionTimeout(Duration.ofSeconds(4));
		assertFalse(longTimeout.logStopped(true, COMMIT, COMMIT + 1, 0));
		assertFalse(longTimeout.logStopped(true, COMMIT, COMMIT + 1, TimeUnit.SECONDS.toNanos(12) - 1));
		assertTrue(longTimeout.logStopped(true, COMMIT, COMMIT + 1, TimeUnit.SECONDS.toNanos(12)));
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

	/** What a {@link RaftServer.Division} tells, as a stub: only the calls of the rule answer. */
	private static RaftServer.Division division(RaftConfiguration conf, String... followerIds) {
		var follower = (InvocationHandler) (_, method, _) -> switch (method.getName()) {
			case "getLastCommittedIndex" -> COMMIT;
			case "getNextIndex" -> LOG_NEXT;
			case "getTermIndex" -> TermIndex.valueOf(TERM - 1, COMMIT);
			default -> throw new UnsupportedOperationException(method.getName());
		};
		var log = (RaftLog) Proxy.newProxyInstance(RaftLog.class.getClassLoader(), new Class<?>[] {RaftLog.class}, follower);
		var leaderInfo = RaftProtos.LeaderInfoProto.newBuilder();
		for (var id : followerIds) {
			leaderInfo.addFollowerInfo(RaftProtos.ServerRpcProto.newBuilder()
					.setId(RaftProtos.RaftPeerProto.newBuilder().setId(RaftPeerId.valueOf(id).toByteString()))
					.setLastRpcElapsedTimeMs(RECENT));
		}
		var role = RaftProtos.RoleInfoProto.newBuilder().setLeaderInfo(leaderInfo).build();
		var nextIndices = new long[followerIds.length];
		Arrays.fill(nextIndices, LOG_NEXT);
		var info = (DivisionInfo) Proxy.newProxyInstance(DivisionInfo.class.getClassLoader(),
				new Class<?>[] {DivisionInfo.class}, (_, method, _) -> switch (method.getName()) {
					case "isLeader" -> true;
					case "getCurrentTerm" -> TERM;
					case "getFollowerNextIndices" -> nextIndices;
					case "getRoleInfoProto" -> role;
					default -> throw new UnsupportedOperationException(method.getName());
				});
		return (RaftServer.Division) Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),
				new Class<?>[] {RaftServer.Division.class}, (_, method, _) -> switch (method.getName()) {
					case "getInfo" -> info;
					case "getRaftLog" -> log;
					case "getRaftConf" -> conf;
					default -> throw new UnsupportedOperationException(method.getName());
				});
	}

	private static List<RaftPeer> peers(String... ids) {
		return Arrays.stream(ids).map(id -> RaftPeer.newBuilder().setId(id).build()).toList();
	}

	/** During a change of configuration the majority of the old one counts too, and the rule must hear of it. */
	@Test
	void theRuleOfARealDivisionLooksAtThePreviousConfiguration() {
		var joint = ServerImplUtils.newRaftConfiguration(peers("a", "b", "c"), List.of(), 1, peers("a", "x", "y"), List.of());
		var single = ServerImplUtils.newRaftConfiguration(peers("a", "b", "c"), List.of());
		assertFalse(LeaderStall.waitsForReplyOfSentEntry(division(joint, "b"), "a", ELECTION_TIMEOUT_MAX.toMillis()));
		assertTrue(LeaderStall.waitsForReplyOfSentEntry(division(single, "b"), "a", ELECTION_TIMEOUT_MAX.toMillis()));
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
		partner.stateMachine().testHooks().applyGate = stopped;
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
			partner.stateMachine().testHooks().applyGate = null;
			stopped.countDown();
		}
	}

	/**
	 * The node of a graceful stop is the only partner of its successor, with one node down. If it closes before the reply
	 * to the successor's first entry gets out, and comes back right away, nothing sends that entry again.
	 */
	@Test
	void aGracefulStopOfALeaderWithOneNodeDownLosesNoReply() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config.withElectionTimeouts(Duration.ofSeconds(1),
				ELECTION_TIMEOUT_MAX)).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		var oldLeaderId = oldLeader.config().nodeId();
		long column = oldLeader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		var followers = cluster.runningNodes().stream().filter(node -> node != oldLeader).toList();
		var partner = followers.getFirst();
		cluster.halt(followers.getLast().config().nodeId());

		long start = System.nanoTime();
		cluster.stop(oldLeaderId);
		var restarted = cluster.start(oldLeaderId);
		awaitWrite(column);
		long outageMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
		System.out.printf("MEASURE graceful stop with one node down: first write after %d ms%n", outageMillis);

		assertEquals(0, partner.metrics().amount("leader.stall.stepdowns").count()
				+ restarted.metrics().amount("leader.stall.stepdowns").count());
		assertTrue(outageMillis < LeaderStall.MIN_STALL.toMillis(), () -> "first write after " + outageMillis + " ms");
	}

	/**
	 * A leader commits only what it flushed, whatever its followers hold, and its heartbeats go on: the followers have no
	 * reason for an election. A single small write leaves the queue of the log worker far from full, so Ratis would never
	 * block, and no heartbeat would ever stop.
	 */
	@Test
	void aLeaderWhoseLogStoppedStepsDown() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var oldLeader = cluster.awaitLeader(TIMEOUT);
		long column = oldLeader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);

		var disk = new CountDownLatch(1);
		try {
			LogWorkerGate.block(oldLeader.stateMachine().division().getRaftLog(), disk);
			long start = System.nanoTime();
			var stuck = oldLeader.connection().getAsyncApi(CONTEXT).putAsync(0, column, key(), Buf.wrap(new byte[] {1}),
					RequestType.none());
			awaitOtherLeader(oldLeader);
			long failoverMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
			System.out.printf("MEASURE leader with a stopped log: new leader after %d ms%n", failoverMillis);
			assertEquals(1, oldLeader.metrics().amount("leader.log.stepdowns").count());
			assertTrue(failoverMillis < 2 * LeaderStall.MIN_LOG_STALL.toMillis(), () -> "after " + failoverMillis + " ms");
			assertFalse(stuck.isDone(), "the write was committed: the log did not stop");
			var leader = cluster.runningNodes().stream().filter(node -> node != oldLeader && node.isLeader()).findFirst()
					.orElseThrow();
			leader.connection().getSyncApi(CONTEXT).put(0, column, key(), Buf.wrap(new byte[] {2}), RequestType.none());
		} finally {
			disk.countDown();
		}
	}

	private void awaitOtherLeader(ReplicatedNode oldLeader) {
		InProcessCluster.await(Duration.ofSeconds(25), () -> cluster.runningNodes().stream()
				.anyMatch(node -> node != oldLeader && node.isLeader()));
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
					throw new UncheckedIOException(e);
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
