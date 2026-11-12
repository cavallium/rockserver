package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitConfigurationOnDisk;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.confEntry;
import static it.cavallium.rockserver.distributed.RemovalScenarios.isAppend;
import static it.cavallium.rockserver.distributed.RemovalScenarios.isVote;
import static it.cavallium.rockserver.distributed.RemovalScenarios.sender;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11-2: a removed node whose log has a longer, uncommitted tail than the configuration that removed it. Comparing
 * configuration indexes across the two logs said "member" (its own configuration, at a higher index, holds it), and
 * the node ran as a leaderless follower forever. Only the term of the removal at its index in the node's own log tells
 * that its tail can never be committed.
 */
@Timeout(300)
@Tag("slow")
class DivergentTailRemovalTest {

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		RaftTraffic.dropForTesting = null;
		if (cluster != null) {
			cluster.close();
		}
	}

	private record Removed(String x, List<String> members, TermIndex tail, TermIndex removal) {}

	/**
	 * Five nodes. X leads, but its appends reach nobody, and only it gets votes: it wins term after term, and each term
	 * starts with a configuration entry that holds X, at the end of its own log only. Then X stops, the others elect a
	 * leader in a higher term and remove X, at an index below X's last configuration.
	 */
	private Removed removeANodeWithALongerTail() throws Exception {
		cluster = new InProcessCluster(root, 5, config -> config).startAll();
		var leader = awaitReadyLeader(cluster);
		var x = cluster.anyFollower().config().nodeId();
		var members = cluster.ids().stream().filter(id -> !id.equals(x)).toList();
		var xNode = cluster.node(x);
		long start = confEntry(xNode).getIndex();

		RaftTraffic.dropForTesting = (receiver, message) -> (isAppend(message) && x.equals(sender(message)))
				|| (isVote(message) && !x.equals(sender(message)));
		var division = Objects.requireNonNull(leader.stateMachine().division());
		division.getRaftServer().transferLeadershipAsync(new TransferLeadershipRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leader.config().nodeId()), division.getGroup().getGroupId(), 1, RaftPeerId.valueOf(x),
				10_000));
		InProcessCluster.await(Duration.ofSeconds(120), () -> confEntry(xNode).getIndex() >= start + 8);
		awaitConfigurationOnDisk(xNode);
		var tail = confEntry(xNode);
		cluster.halt(x);
		RaftTraffic.dropForTesting = null;

		var newLeader = awaitReadyLeader(cluster);
		var removal = setConfiguration(cluster, newLeader, members);
		assertTrue(removal.get(60, TimeUnit.SECONDS).isSuccess(), String.valueOf(removal.get().getException()));
		var removalEntry = confEntry(newLeader);
		// The case under test: the removal is below X's own last configuration, and of a higher term.
		assertTrue(removalEntry.getIndex() < tail.getIndex() && removalEntry.getTerm() > tail.getTerm(),
				() -> "removal " + removalEntry + ", X's tail " + tail);
		return new Removed(x, members, tail, removalEntry);
	}

	@Test
	void aRemovedNodeWithALongerTailIsRefused() throws Exception {
		var removed = removeANodeWithALongerTail();
		// Before R11-2: it started, as a follower that no leader sends to, and never exited.
		var failure = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removed.x()));
		assertTrue(failure.getMessage().contains(removed.x()), failure.getMessage());
	}

	/** The same, started while the members are down: it starts, and stops once they answer. */
	@Test
	void aRemovedNodeWithALongerTailStopsOnceTheMembersAnswer() throws Exception {
		var removed = removeANodeWithALongerTail();
		for (var id : removed.members()) {
			cluster.halt(id);
		}
		var node = cluster.start(removed.x());
		for (var id : removed.members()) {
			cluster.start(id);
		}
		// Not cluster.awaitLeader: X, running, follows no leader.
		InProcessCluster.await(TIMEOUT, () -> removed.members().stream().anyMatch(id -> cluster.node(id).isLeader()));
		// Before R11-2: it never stopped.
		var failure = node.stopped().get(120, TimeUnit.SECONDS);
		assertInstanceOf(ReplicatedNode.RemovedException.class, failure, String.valueOf(failure));
	}
}
