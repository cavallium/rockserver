package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11: a cold restart of the whole cluster, in any order of the nodes, never refuses a member; a removed node that
 * starts among them exits with 4 once a majority of the members are up, whether it starts before them or after.
 */
@Timeout(600)
@Tag("slow")
class ColdRestartOrderTest {

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

	private static List<List<String>> permutations(List<String> ids) {
		if (ids.size() <= 1) {
			return List.of(ids);
		}
		var result = new ArrayList<List<String>>();
		for (var first : ids) {
			var rest = new ArrayList<>(ids);
			rest.remove(first);
			for (var tail : permutations(rest)) {
				var order = new ArrayList<String>();
				order.add(first);
				order.addAll(tail);
				result.add(order);
			}
		}
		return result;
	}

	private void haltAll() throws Exception {
		for (var node : cluster.runningNodes()) {
			cluster.halt(node.config().nodeId());
		}
	}

	@Test
	void everyStartOrderOfThreeMembersStarts() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		awaitReadyLeader(cluster);
		cluster.awaitConverged(TIMEOUT);
		for (var order : permutations(cluster.ids())) {
			haltAll();
			for (var id : order) {
				var node = cluster.start(id);
				var membership = node.membership();
				assertTrue(membership == null || membership.outcome() != RemovalRule.Outcome.REMOVED,
						() -> order + ": " + id + " " + membership);
			}
			awaitReadyLeader(cluster);
			cluster.awaitConverged(TIMEOUT);
		}
	}

	@Test
	void aRemovedNodeExitsOnceAMajorityIsUpInAnyOrder() throws Exception {
		cluster = new InProcessCluster(root, 4, config -> config).startAll();
		var leader = awaitReadyLeader(cluster);
		var removed = cluster.anyFollower().config().nodeId();
		var members = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		assertTrue(setConfiguration(cluster, leader, members).get(60, TimeUnit.SECONDS).isSuccess());
		for (int position = 0; position <= members.size(); position++) {
			haltAll();
			var order = new ArrayList<>(members);
			order.add(position, removed);
			ReplicatedNode removedNode = null;
			int membersUp = 0;
			for (var id : order) {
				if (!id.equals(removed)) {
					var node = cluster.start(id);
					var membership = node.membership();
					assertTrue(membership == null || membership.outcome() != RemovalRule.Outcome.REMOVED,
							() -> order + ": " + id + " " + membership);
					membersUp++;
					continue;
				}
				try {
					removedNode = cluster.start(id);
				} catch (ReplicatedNode.RemovedException refused) {
					assertTrue(membersUp >= 2, () -> order + ": refused with " + refused.verdict());
				}
			}
			if (removedNode != null) {
				// Started before a majority answered: its check while it hears no leader stops it.
				var failure = removedNode.stopped().get(120, TimeUnit.SECONDS);
				assertInstanceOf(ReplicatedNode.RemovedException.class, failure, order + ": " + failure);
				cluster.halt(removed);
			}
			InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(ReplicatedNode::isLeader));
			for (var id : members) {
				assertNotEquals(true, cluster.node(id).stopped().isDone(), id + " stopped");
			}
		}
	}
}
