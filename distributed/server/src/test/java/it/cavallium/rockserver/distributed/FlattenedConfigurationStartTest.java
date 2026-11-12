package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitConfigurationOnDisk;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.isAppend;
import static it.cavallium.rockserver.distributed.RemovalScenarios.jointWithoutIn;
import static it.cavallium.rockserver.distributed.RemovalScenarios.sender;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static it.cavallium.rockserver.distributed.RemovalScenarios.stableWith;
import static it.cavallium.rockserver.distributed.RemovalScenarios.stableWithout;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11-1: Ratis' term-start entry copies only the new half of a joint configuration (LeaderStateImpl.StartupLogEntry),
 * so a leader re-elected with an uncommitted joint configuration that removes node X writes a stable configuration
 * without X, which can itself be dropped later. A log that ends with such a stable configuration is no evidence of a
 * committed removal: X is still a member and must start.
 */
@Timeout(300)
@Tag("slow")
class FlattenedConfigurationStartTest {

	private static final Duration ELECTION_MIN = Duration.ofSeconds(2);
	private static final Duration ELECTION_MAX = Duration.ofSeconds(4);

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

	/**
	 * Five nodes {L, X, A, B, C}, C down. L's appends reach X only. L appends the joint configuration J that removes X,
	 * cannot commit it, loses its leadership, and A and B elect it again (its log is the longest): its term-start entry
	 * is the stable C_new = {L, A, B, C}, which reaches X only. Then L and X stop, C starts, and A, B, C elect a leader
	 * without J, whose term-start entry at J's index commits and drops J and C_new. X restarts with a log that ends with
	 * C_new, a stable configuration without it that was never committed.
	 */
	@Test
	void aFlattenedRemovalThatWasDroppedDoesNotRefuseTheNode() throws Exception {
		cluster = new InProcessCluster(root, 5, config -> config.withElectionTimeouts(ELECTION_MIN, ELECTION_MAX))
				.startAll();
		var leader = awaitReadyLeader(cluster);
		var l = leader.config().nodeId();
		var followers = cluster.ids().stream().filter(id -> !id.equals(l)).toList();
		var x = followers.get(0);
		var c = followers.get(3);
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(x)).toList();
		// X must not stand for election while L is out: Ratis shuts down a candidate outside the new half of its
		// configuration (LeaderElection, NOT_IN_CONF), and X then never receives L's next term-start entry.
		cluster.halt(x);
		cluster.reconfigure(x, config -> config.withElectionTimeouts(Duration.ofSeconds(60), Duration.ofSeconds(90)));
		cluster.start(x);
		cluster.awaitConverged(TIMEOUT);
		cluster.halt(c);
		var xNode = cluster.node(x);

		RaftTraffic.dropForTesting = (receiver, message) -> isAppend(message) && l.equals(sender(message))
				&& !receiver.equals(x);
		var reply = setConfiguration(cluster, leader, newHalf);
		InProcessCluster.await(TIMEOUT, () -> jointWithoutIn(xNode, x) || reply.isDone());
		// L re-elected: its term-start entry, C_new without X, reaches X.
		InProcessCluster.await(Duration.ofSeconds(120), () -> stableWithout(xNode, x));
		awaitConfigurationOnDisk(xNode);
		assertFalse(reply.isDone() && reply.join().isSuccess(), "the removal must not commit");
		cluster.halt(x);
		cluster.halt(l);
		RaftTraffic.dropForTesting = null;

		cluster.reconfigure(x, config -> config.withElectionTimeouts(ELECTION_MIN, ELECTION_MAX));
		cluster.start(c);
		cluster.awaitLeader(TIMEOUT);
		// Before R11-1: RemovedException at once, "is not in the new configuration of its log".
		var restarted = cluster.start(x);
		InProcessCluster.await(TIMEOUT, () -> stableWith(restarted, x));
		cluster.awaitConverged(TIMEOUT);
	}
}
