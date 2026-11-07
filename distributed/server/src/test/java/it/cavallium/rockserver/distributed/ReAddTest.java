package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitAnyReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.confEntry;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static it.cavallium.rockserver.distributed.RemovalScenarios.stableWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.proto.RaftProtos;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11: nodes that never were voters, or are voters again, are not told they were removed. A node id added again
 * after its removal (wiped, started with --join, add-peer) replays the configurations of its earlier membership while
 * it is brought up to date: they do not make it a voter, so neither its start nor its check while it hears no leader
 * says it was removed.
 */
@Timeout(300)
class ReAddTest {

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
	 * Four nodes; X is removed, wiped, started with --join and added again while one member is down: X is brought up to
	 * date (replaying the configurations of its earlier membership) but the joint configuration of its new add never
	 * reaches it, and cannot commit without it. Neither the checks while it hears no leader nor its restart in the
	 * middle of the add say it was removed; once the member is back, the add completes.
	 */
	@Test
	void aRemovedNodeAddedAgainIsNotRefusedWhileItIsBroughtUpToDate() throws Exception {
		cluster = new InProcessCluster(root, 4, config -> config).startAll();
		var leader = awaitReadyLeader(cluster);
		var followers = cluster.ids().stream().filter(id -> !id.equals(leader.config().nodeId())).toList();
		var x = followers.get(0);
		var down = followers.get(1);
		var members = cluster.ids().stream().filter(id -> !id.equals(x)).toList();
		var removal = setConfiguration(cluster, leader, members);
		assertTrue(removal.get(60, TimeUnit.SECONDS).isSuccess(), String.valueOf(removal.get().getException()));
		long removalIndex = confEntry(leader).getIndex();
		cluster.halt(x);
		NodeStore.deleteRecursively(cluster.config(x).dataDir());
		cluster.reconfigure(x, config -> config.withJoin(true));
		cluster.halt(down);
		var fresh = cluster.start(x);

		// The joint configuration of the new add (X in its new half only) never reaches X.
		RaftTraffic.dropForTesting = (receiver, message) -> receiver.equals(x) && RemovalScenarios.carriesConfiguration(
				message, conf -> conf.getOldPeersCount() > 0 && RemovalScenarios.holdsId(conf.getPeersList(), x)
						&& !RemovalScenarios.holdsId(conf.getOldPeersList(), x));
		var all = cluster.ids();
		var add = setConfiguration(cluster, awaitAnyReadyLeader(cluster), all);
		InProcessCluster.await(TIMEOUT, () -> fresh.appliedIndex() >= removalIndex);
		var history = fresh.stateMachine().configurations();
		assertTrue(history.all().stream().anyMatch(conf -> conf.inNew(x)), "it replayed its earlier membership");
		// Several checks while it hears no leader (now every request to it is dropped): none says removed.
		RaftTraffic.dropForTesting = (receiver, message) -> receiver.equals(x);
		long before = fresh.detachedChecks();
		InProcessCluster.await(checks(fresh, 3), () -> fresh.detachedChecks() >= before + 3);
		assertFalse(fresh.stopped().isDone(), () -> "stopped: " + fresh.stopped().join());
		assertEquals(RemovalRule.Reason.NOT_ADDED, Objects.requireNonNull(fresh.membership()).reason());
		assertFalse(Files.exists(cluster.config(x).raftDir().resolve("member")), "not a voter yet");

		// It crashes in the middle of its add, and starts again.
		cluster.halt(x);
		var restarted = cluster.start(x);
		assertEquals(RemovalRule.Reason.NOT_ADDED, Objects.requireNonNull(restarted.membership()).reason());
		RaftTraffic.dropForTesting = null;
		cluster.start(down);
		if (!add.get(120, TimeUnit.SECONDS).isSuccess()) {
			// The leader lost its majority of the joint configuration while X heard nothing: add-peer again.
			var again = setConfiguration(cluster, awaitAnyReadyLeader(cluster), all).get(120, TimeUnit.SECONDS);
			assertTrue(again.isSuccess(), String.valueOf(again.getException()));
		}
		InProcessCluster.await(TIMEOUT, () -> stableWith(restarted, x));
		cluster.awaitConverged(TIMEOUT);
		assertFalse(restarted.stopped().isDone());
	}

	/**
	 * Enough time for {@code n} checks while a node hears no leader: three election timeouts of quiet, then one check
	 * every three election timeouts (at least a second), with room to spare.
	 */
	private static Duration checks(ReplicatedNode node, int n) {
		var periods = node.config().electionTimeoutMax().multipliedBy(3L * (n + 3));
		return periods.compareTo(TIMEOUT) > 0 ? periods : TIMEOUT;
	}

	/** A fresh --join node waits for add-peer through several checks, and never exits. */
	@Test
	void aFreshJoinerWaitsWithoutExiting() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		awaitReadyLeader(cluster);
		var joiner = cluster.join("j0");
		InProcessCluster.await(checks(joiner, 6), () -> joiner.detachedChecks() >= 6);
		assertFalse(joiner.stopped().isDone(), () -> "stopped: " + joiner.stopped().join());
		var membership = Objects.requireNonNull(joiner.membership(), "checked while it heard no leader");
		assertEquals(RemovalRule.Reason.NOT_ADDED, membership.reason(), String.valueOf(membership));
		// Then it is added, and is a member.
		var all = cluster.ids();
		assertTrue(setConfiguration(cluster, awaitAnyReadyLeader(cluster), all).get(120, TimeUnit.SECONDS).isSuccess());
		InProcessCluster.await(TIMEOUT, () -> stableWith(joiner, "j0"));
		assertFalse(joiner.stopped().isDone());
	}
}
