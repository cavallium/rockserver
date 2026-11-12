package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Two clusters under one group id: a node that forgot {@code --join} and has only itself in its peer list elects itself
 * and commits under the group id of the real cluster, with the same terms and indices. Each genesis gets its own id, and
 * that id keeps such a node out.
 */
@Timeout(120)
@Tag("slow")
class ClusterIdentityTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final String STRAY = "n3";
	private static final int STRAY_KEYS = 5;

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

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static String clusterId(ReplicatedNode node) {
		return node.stateMachine().store().clusterId();
	}

	/** Nodes n0 to n2 are a cluster, and n3 is one by itself. */
	private List<PeerConfig> startClusterAndStray() throws IOException {
		var allPeers = new ArrayList<PeerConfig>();
		cluster = new InProcessCluster(root, 4, config -> {
			if (allPeers.isEmpty()) {
				allPeers.addAll(config.peers());
			}
			return config.withPeers(config.nodeId().equals(STRAY) ? List.of(config.self()) : config.peers().subList(0, 3));
		}).startAll();
		var stray = cluster.node(STRAY);
		InProcessCluster.await(TIMEOUT, () -> stray.isLeader() && clusterId(stray) != null);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().filter(node -> node != stray)
				.allMatch(node -> clusterId(node) != null));
		return allPeers;
	}

	private ReplicatedNode realLeader() {
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.anyMatch(node -> node.isLeader() && !node.config().nodeId().equals(STRAY)));
		return cluster.runningNodes().stream()
				.filter(node -> node.isLeader() && !node.config().nodeId().equals(STRAY)).findFirst().orElseThrow();
	}

	@Test
	void everyGenesisGetsItsOwnId() throws Exception {
		startClusterAndStray();
		var ids = cluster.runningNodes().stream().filter(node -> !node.config().nodeId().equals(STRAY))
				.map(ClusterIdentityTest::clusterId).distinct().toList();
		assertEquals(1, ids.size(), () -> "ids of one cluster: " + ids);
		assertNotEquals(ids.getFirst(), clusterId(cluster.node(STRAY)));
		assertNotEquals(cluster.node(STRAY).config().groupId(), ids.getFirst());
	}

	/** A batch of a former leader reaches the next one, which proposed its own id meanwhile. */
	@Test
	void aSecondGenesisInTheLogChangesNothing() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node -> clusterId(node) != null));
		var id = clusterId(leader);
		leader.proposer().propose(new LogEntry.Genesis(UUID.randomUUID().toString(), false))
				.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		leader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertEquals(id, clusterId(node));
			assertFalse(node.isFailed());
		}
	}

	/**
	 * The Ratis tools do not ask what {@code admin add-peer} asks. The leader tells a node that it starts to bring up to
	 * date which cluster it leads, and a node of another cluster stops before the configuration changes.
	 */
	@Test
	void aNodeOfAnotherClusterStopsBeforeItJoins() throws Exception {
		var peers = startClusterAndStray();
		var leader = realLeader();
		var stray = cluster.node(STRAY);
		long column = leader.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		long strayColumn = stray.connection().getSyncApi(CONTEXT).createColumn("data", SCHEMA);
		assertEquals(column, strayColumn);
		for (int i = 0; i < STRAY_KEYS; i++) {
			stray.connection().getSyncApi(CONTEXT).put(0, strayColumn, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
		}

		var raftPeers = peers.stream()
				.map(peer -> RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build()).toList();
		var groupId = RaftGroupId.valueOf(UUID.fromString(leader.config().groupId()));
		try (var client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(groupId, raftPeers.subList(0, 3)))
				.setProperties(RaftSettings.client(leader.config()))
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(5,
						TimeDuration.valueOf(1, TimeUnit.SECONDS)))
				.build()) {
			assertThrows(IOException.class, () -> client.admin().setConfiguration(raftPeers));
		}

		var failure = stray.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertNotNull(failure, "the node stopped without a failure");
		assertTrue(failure.getMessage().contains("belongs to cluster"), failure::toString);
		assertEquals(3, leader.stateMachine().division().getRaftConf().getCurrentPeers().size());
		leader.connection().getSyncApi(CONTEXT).put(0, column, key(0), Buf.wrap(new byte[] {2}), RequestType.none());
	}

	@Test
	void aNewNodeMayJoin() {
		assertNull(new NodeState(0, false, false, false, null).refusalToJoin("n", "cluster"));
	}

	@Test
	void aNodeOfTheSameClusterMayJoinAgain() {
		assertNull(new NodeState(100, false, true, false, "cluster").refusalToJoin("n", "cluster"));
	}

	@Test
	void aNodeOfAnotherClusterMayNotJoin() {
		assertNotNull(new NodeState(100, false, false, false, "other").refusalToJoin("n", "cluster"));
	}

	@Test
	void aLogWithoutAClusterMayNotJoin() {
		assertNotNull(new NodeState(1, false, false, false, null).refusalToJoin("n", "cluster"));
	}

	@Test
	void aNodeStartedOutsideJoinMayNotJoin() {
		assertNotNull(new NodeState(0, false, false, true, null).refusalToJoin("n", "cluster"));
	}
}
