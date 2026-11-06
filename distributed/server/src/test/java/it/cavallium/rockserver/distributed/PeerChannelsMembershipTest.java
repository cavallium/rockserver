package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R2-RT-2: after a real membership change the node's shared {@link PeerChannels} (forwarding, {@link ReplicaOps}) shut
 * down the channel to a removed member, through {@code ReplicatedNode.watchRaft}, and keep the others'. The removed
 * member joined after the start, so no node's startup peer list holds its address (an address in that list is kept on
 * purpose: see {@code PeerChannelsPruningTest}).
 */
@Timeout(180)
class PeerChannelsMembershipTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

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

	private RaftClient admin(NodeConfig config) {
		var groupId = RaftGroupId.valueOf(UUID.fromString(config.groupId()));
		return RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(groupId, cluster.raftPeers(List.of("n0", "n1", "n2"))))
				.setProperties(RaftSettings.client(config))
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(10, TimeDuration.valueOf(1, TimeUnit.SECONDS)))
				.build();
	}

	@Test
	void theSharedChannelsDropAMemberRemovedAfterItJoined() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		cluster.awaitLeader(TIMEOUT);
		var originals = List.of("n0", "n1", "n2");
		var joined = cluster.join("n3");
		var joinedAddress = joined.config().self().raftAddress();
		var withJoined = new ArrayList<>(originals);
		withJoined.add("n3");
		try (var client = admin(cluster.config("n0"))) {
			assertTrue(client.admin().setConfiguration(cluster.raftPeers(withJoined)).isSuccess());
		}
		cluster.awaitConverged(TIMEOUT);

		// Every original node has channels to every other member, the joined one too (as a forward or a poll would make).
		for (var id : originals) {
			var channels = cluster.node(id).peerChannels();
			for (var other : withJoined) {
				if (!other.equals(id)) {
					channels.get(cluster.config(other).self().raftAddress());
				}
			}
			// The pruning (every 200 ms) keeps a member's channel.
			Thread.sleep(500);
			assertTrue(channels.addresses().contains(joinedAddress), id + " keeps the channel to a member");
		}

		try (var client = admin(cluster.config("n0"))) {
			assertTrue(client.admin().setConfiguration(cluster.raftPeers(originals)).isSuccess());
		}
		for (var id : originals) {
			var channels = cluster.node(id).peerChannels();
			InProcessCluster.await(TIMEOUT, () -> !channels.addresses().contains(joinedAddress));
			for (var other : originals) {
				if (!other.equals(id)) {
					assertTrue(channels.addresses().contains(cluster.config(other).self().raftAddress()),
							id + " keeps the channel to " + other);
				}
			}
			assertFalse(channels.addresses().contains(joinedAddress), id + " dropped the removed member's channel");
		}
	}
}
