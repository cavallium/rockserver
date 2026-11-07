package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitConfigurationOnDisk;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.carriesConfiguration;
import static it.cavallium.rockserver.distributed.RemovalScenarios.holdsId;
import static it.cavallium.rockserver.distributed.RemovalScenarios.jointWithIn;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Objects;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11-3: a node started with {@code --join} received the joint configuration of its add, but not the final one, and
 * went down; it was removed while down. Its next start must end with exit 4, not wait outside forever with add-peer
 * advice.
 */
@Timeout(300)
class JoinRemovedWhileDownTest {

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

	@Test
	void aJoinerRemovedWhileDownIsRefused() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var members = cluster.ids();
		var leader = awaitReadyLeader(cluster);
		var y = "j0";
		var yNode = cluster.join(y);
		// Y gets the joint configuration of its add, never the final one.
		RaftTraffic.dropForTesting = (receiver, message) -> receiver.equals(y) && carriesConfiguration(message,
				conf -> conf.getOldPeersCount() == 0 && holdsId(conf.getPeersList(), y));
		var all = new ArrayList<>(members);
		all.add(y);
		var add = setConfiguration(cluster, leader, all);
		InProcessCluster.await(TIMEOUT, () -> jointWithIn(yNode, y));
		assertTrue(add.get().isSuccess(), String.valueOf(add.get().getException()));
		var division = Objects.requireNonNull(yNode.stateMachine().division());
		// A voter of the joint configuration: the leader sends it appends that are not part of its catch-up.
		InProcessCluster.await(TIMEOUT, () -> division.getInfo().getLifeCycleState() == LifeCycle.State.RUNNING);
		awaitConfigurationOnDisk(yNode);
		// A few maintenance rounds, which record that the node was a voter.
		Thread.sleep(1_000);
		cluster.halt(y);
		RaftTraffic.dropForTesting = null;

		var removal = setConfiguration(cluster, awaitReadyLeader(cluster), members);
		assertTrue(removal.get().isSuccess(), String.valueOf(removal.get().getException()));

		// Before R11-3: it started, and waited outside the cluster forever, saying that it waits for add-peer.
		var failure = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(y));
		assertTrue(failure.getMessage().contains(y), failure.getMessage());
	}
}
