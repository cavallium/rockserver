package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3-G2b-4: a node whose log ends with a joint configuration of its own removal decides by asking the members of the new
 * half whether that configuration was committed.
 */
@Timeout(300)
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
	 * Five nodes: the leader writes the joint configuration that removes one follower while the other three are down,
	 * so it reaches only that follower and cannot commit. The leader and that follower stop; the other three elect a
	 * leader without the entry, which drops it. The follower then holds itself in the old half only, while the members
	 * of the new half still hold it in their committed configuration: it is a member, and starts.
	 */
	@Test
	void aNodeWhoseRemovalWasNeverCommittedStarts() throws Exception {
		cluster = new InProcessCluster(root, 5, config -> config.withElectionTimeouts(ELECTION_MIN, ELECTION_MAX))
				.startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		var removed = cluster.anyFollower().config().nodeId();
		var others = cluster.ids().stream().filter(id -> !id.equals(leaderId) && !id.equals(removed)).toList();
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		for (var id : others) {
			cluster.halt(id);
		}

		// The joint configuration reaches the removed follower only: the new half has no majority to commit it.
		var division = Objects.requireNonNull(leader.stateMachine().division());
		division.getRaftServer().setConfigurationAsync(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leaderId), division.getGroup().getGroupId(), 1, cluster.raftPeers(newHalf)));
		var removedNode = cluster.node(removed);
		InProcessCluster.await(Duration.ofSeconds(10), () -> isJoint(removedNode));
		cluster.halt(removed);
		cluster.halt(leaderId);

		// The other three elect a leader without the entry.
		for (var id : others) {
			cluster.start(id);
		}
		cluster.awaitLeader(TIMEOUT);

		// Before R3-G2b-4: RemovedException, "is not in the new configuration of its log".
		var restarted = cluster.start(removed);
		InProcessCluster.await(TIMEOUT, () -> {
			var conf = Objects.requireNonNull(restarted.stateMachine().division()).getRaftConf();
			return conf.getPreviousPeers().isEmpty()
					&& conf.getCurrentPeers().stream().anyMatch(peer -> peer.getId().toString().equals(removed));
		});
		cluster.awaitConverged(TIMEOUT);
	}

	/**
	 * A node stopped after receiving the joint configuration of its committed removal is still refused: the members of
	 * the new half hold that configuration, or the final one, committed.
	 */
	@Test
	void aNodeWhoseRemovalWasCommittedIsRefused() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var removed = cluster.anyFollower().config().nodeId();
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		var division = Objects.requireNonNull(leader.stateMachine().division());
		var reply = division.getRaftServer().setConfiguration(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leader.config().nodeId()), division.getGroup().getGroupId(), 1,
				cluster.raftPeers(newHalf)));
		assertTrue(reply.isSuccess(), String.valueOf(reply.getException()));
		cluster.halt(removed);
		var failure = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removed));
		assertTrue(failure.getMessage().contains(removed), failure.getMessage());
	}
}
