package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3-G2b-4, R10-1, R11: a node whose log ends with a joint configuration of its own removal. That configuration counts
 * only once it is committed: the node is refused (exit 4) only when a majority of the new half of the newest committed
 * configuration confirm it (see {@link RemovalRule}); otherwise it starts, outside the cluster if its log leaves it out.
 */
@Timeout(300)
@Tag("slow")
class JointRemovalStartTest {

	/** Long enough that the leader keeps its leadership (Ratis' step-down wait is the election timeout) while it writes. */
	private static final Duration ELECTION_MIN = Duration.ofSeconds(2);
	private static final Duration ELECTION_MAX = Duration.ofSeconds(4);
	private static final Duration TIMEOUT = Duration.ofSeconds(60);

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static boolean isJoint(ReplicatedNode node) {
		return !Objects.requireNonNull(node.stateMachine().division()).getRaftConf().getPreviousPeers().isEmpty();
	}

	/**
	 * The leader, once it committed the first entry of its term: before that Ratis refuses a configuration change with
	 * {@code LeaderNotReadyException}, and a leader that loses its majority first never gets ready.
	 */
	private ReplicatedNode awaitReadyLeader() {
		var leader = cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> leader.isLeader()
				&& Objects.requireNonNull(leader.stateMachine().division()).getInfo().isLeaderReady());
		return leader;
	}

	/** Whether {@code node}'s configuration is a joint one that holds {@code id} in its old half only. */
	private static boolean jointWithoutIn(ReplicatedNode node, String id) {
		var conf = Objects.requireNonNull(node.stateMachine().division()).getRaftConf();
		return conf.getPreviousPeers().stream().anyMatch(peer -> peer.getId().toString().equals(id))
				&& conf.getCurrentPeers().stream().noneMatch(peer -> peer.getId().toString().equals(id));
	}

	/**
	 * R10-4: waits until {@code node}'s joint configuration is in its log on disk. Ratis takes a configuration from an
	 * append before it writes the entry, and a halt can drop a write still queued: the restarted log would then end with
	 * the configuration before, and the start would not take the path under test.
	 */
	private static void awaitConfigurationOnDisk(ReplicatedNode node) {
		var division = Objects.requireNonNull(node.stateMachine().division());
		long index = division.getRaftConf().getLogEntryIndex();
		InProcessCluster.await(TIMEOUT, () -> division.getRaftLog().getFlushIndex() >= index);
	}

	/** The nodes of {@link #haltAfterAnUncommittedJointRemoval}. */
	private record DroppedRemoval(String leader, String removed, List<String> others) {}

	/**
	 * Five nodes: the leader writes the joint configuration that removes one follower while the other three are down,
	 * so it reaches only that follower and cannot commit. Then the leader and that follower stop too: every node is
	 * down, and the follower holds itself in the old half only, on disk, while the other three never received it.
	 */
	private DroppedRemoval haltAfterAnUncommittedJointRemoval() throws Exception {
		cluster = new InProcessCluster(root, 5, config -> config.withElectionTimeouts(ELECTION_MIN, ELECTION_MAX))
				.startAll();
		var leader = awaitReadyLeader();
		var leaderId = leader.config().nodeId();
		var removed = cluster.anyFollower().config().nodeId();
		var others = cluster.ids().stream().filter(id -> !id.equals(leaderId) && !id.equals(removed)).toList();
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		for (var id : others) {
			cluster.halt(id);
		}

		// The joint configuration reaches the removed follower only: the new half has no majority to commit it.
		var division = Objects.requireNonNull(leader.stateMachine().division());
		var reply = division.getRaftServer().setConfigurationAsync(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leaderId), division.getGroup().getGroupId(), 1, cluster.raftPeers(newHalf)));
		var removedNode = cluster.node(removed);
		InProcessCluster.await(Duration.ofSeconds(10), () -> isJoint(removedNode) || reply.isDone());
		// It cannot commit: a reply means the leader refused the change or lost its leadership first.
		assertFalse(reply.isDone(), () -> "setConfiguration replied: " + reply.handle((r, e) -> e != null ? e : r).join());
		assertTrue(jointWithoutIn(removedNode, removed));
		awaitConfigurationOnDisk(removedNode);
		cluster.halt(removed);
		cluster.halt(leaderId);
		return new DroppedRemoval(leaderId, removed, others);
	}

	/** Waits until {@code node} holds a configuration that is not joint and holds it: a leader cut its log back. */
	private static void awaitMemberAgain(ReplicatedNode node, String id) {
		InProcessCluster.await(TIMEOUT, () -> {
			var conf = Objects.requireNonNull(node.stateMachine().division()).getRaftConf();
			return conf.getPreviousPeers().isEmpty()
					&& conf.getCurrentPeers().stream().anyMatch(peer -> peer.getId().toString().equals(id));
		});
	}

	/**
	 * The other three elect a leader without the joint configuration, which drops it. The members of the new half
	 * still hold the follower in their committed configuration: it is a member, and starts.
	 */
	@Test
	void aNodeWhoseRemovalWasNeverCommittedStarts() throws Exception {
		var nodes = haltAfterAnUncommittedJointRemoval();
		for (var id : nodes.others()) {
			cluster.start(id);
		}
		cluster.awaitLeader(TIMEOUT);

		// Before R3-G2b-4: RemovedException, "is not in the new configuration of its log".
		var restarted = cluster.start(nodes.removed());
		assertEquals(RemovalRule.Reason.MEMBER, Objects.requireNonNull(restarted.membership()).reason());
		awaitMemberAgain(restarted, nodes.removed());
		cluster.awaitConverged(TIMEOUT);
	}

	/**
	 * R10-1: the same, but the follower starts first, in a cold restart of the cluster, while no member of the new half
	 * answers. Not knowing is no evidence that it was removed: it starts outside the cluster (Ratis' initializing state)
	 * and becomes a member again when the others elect a leader that dropped the joint configuration.
	 */
	@Test
	void aNodeWhoseRemovalWasNeverCommittedStartsBeforeTheNewHalf() throws Exception {
		var nodes = haltAfterAnUncommittedJointRemoval();

		// Before R10-1: RemovedException (exit 4, with the advice to wipe it), as no member of the new half answered.
		var restarted = cluster.start(nodes.removed());
		var division = Objects.requireNonNull(restarted.stateMachine().division());
		assertTrue(jointWithoutIn(restarted, nodes.removed()), "the start took the joint-configuration path");
		assertEquals(LifeCycle.State.STARTING, division.getInfo().getLifeCycleState(), "it waits outside the cluster");
		// Its own committed configuration, the one before the joint one, holds it: no member had to answer.
		assertEquals(RemovalRule.Reason.MEMBER, Objects.requireNonNull(restarted.membership()).reason());

		for (var id : nodes.others()) {
			cluster.start(id);
		}
		cluster.awaitLeader(TIMEOUT);
		awaitMemberAgain(restarted, nodes.removed());
		InProcessCluster.await(TIMEOUT, () -> division.getInfo().getLifeCycleState() == LifeCycle.State.RUNNING);
		cluster.awaitConverged(TIMEOUT);
	}

	/**
	 * A node stopped after receiving the joint configuration of its committed removal is still refused: the members of
	 * the new half hold that configuration, or the final one, committed.
	 */
	@Test
	void aNodeWhoseRemovalWasCommittedIsRefused() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = awaitReadyLeader();
		var removed = cluster.anyFollower().config().nodeId();
		var removedNode = cluster.node(removed);
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		var division = Objects.requireNonNull(leader.stateMachine().division());
		var pending = division.getRaftServer().setConfigurationAsync(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leader.config().nodeId()), division.getGroup().getGroupId(), 1,
				cluster.raftPeers(newHalf)));
		// The removal commits without the removed follower: it must hold the joint configuration before it stops, so that
		// its start decides on a log that ends with the joint configuration of its removal (the case under test), not on a
		// log that still holds it in both halves.
		InProcessCluster.await(TIMEOUT, () -> jointWithoutIn(removedNode, removed));
		awaitConfigurationOnDisk(removedNode);
		var reply = pending.get();
		assertTrue(reply.isSuccess(), String.valueOf(reply.getException()));
		assertTrue(jointWithoutIn(removedNode, removed), "the leader sends the final configuration to the members that stay");
		cluster.halt(removed);
		var failure = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removed));
		assertTrue(failure.getMessage().contains(removed), failure.getMessage());
		// R11-8: the verdict, not a message shared by two paths, says why.
		var verdict = Objects.requireNonNull(failure.verdict());
		assertEquals(RemovalRule.Reason.REMOVED, verdict.reason());
		var kStar = Objects.requireNonNull(verdict.kStar());
		assertFalse(kStar.inNew(removed), String.valueOf(kStar));
		assertTrue(verdict.confirmers().size() >= RemovalRule.majority(kStar.newHalf().size()), String.valueOf(verdict));
	}
}
