package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/** Start-up checks that need a cluster: removed nodes, peer API addresses, seed digests. */
@Timeout(180)
class StartupClusterTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

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

	private RaftClient admin(NodeConfig config) {
		var raftPeers = config.peers().stream()
				.map(peer -> RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build()).toList();
		var groupId = RaftGroupId.valueOf(UUID.fromString(config.groupId()));
		return RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(groupId, raftPeers))
				.setProperties(RaftSettings.client(config))
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(10, TimeDuration.valueOf(1, TimeUnit.SECONDS)))
				.build();
	}

	/** DD-30: a removed node that a supervisor starts again does not print READY and stay up: it refuses to start. */
	@Test
	void aRemovedNodeDoesNotStartAgain() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var removed = cluster.anyFollower();
		var removedId = removed.config().nodeId();
		InProcessCluster.await(TIMEOUT, () -> Files.exists(removed.config().raftDir().resolve("member")));

		// A member restarts as usual.
		cluster.stop(removedId);
		cluster.start(removedId);
		cluster.awaitConverged(TIMEOUT);

		var remaining = leader.config().peers().stream().filter(peer -> !peer.id().equals(removedId))
				.map(peer -> RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build()).toList();
		try (var client = admin(leader.config())) {
			assertTrue(client.admin().setConfiguration(remaining).isSuccess());
		}
		var running = cluster.node(removedId);
		running.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		cluster.halt(removedId);

		var error = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removedId));
		assertTrue(error.getMessage().contains("removed from the cluster"), error.getMessage());
		// Also with --join in its command line, since it was a member once.
		cluster.reconfigure(removedId, config -> config.withJoin(true));
		assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removedId));
	}

	/**
	 * R4-6: the configuration that removes a node commits with the other members alone, so the removed node's log may
	 * never receive it (here it is stopped meanwhile, which a slow or partitioned node does by chance). Its own log still
	 * holds it; the members it asks at start tell it that it was removed.
	 */
	@Test
	void aNodeRemovedWhileStoppedDoesNotStartAgain() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var removed = cluster.anyFollower();
		var removedId = removed.config().nodeId();
		InProcessCluster.await(TIMEOUT, () -> Files.exists(removed.config().raftDir().resolve("member")));
		cluster.stop(removedId);

		var remaining = leader.config().peers().stream().filter(peer -> !peer.id().equals(removedId))
				.map(peer -> RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build()).toList();
		try (var client = admin(leader.config())) {
			assertTrue(client.admin().setConfiguration(remaining).isSuccess());
		}

		var error = assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removedId));
		assertTrue(error.getMessage().contains("removed from the cluster while its log did not follow"),
				error.getMessage());
		// Refused again, also the second time (nothing of the refused start makes it look like a member). A --join
		// restart is not tried here: the raft-meta.bootstrap check may refuse it first, with another error.
		assertThrows(ReplicatedNode.RemovedException.class, () -> cluster.start(removedId));
	}

	/** DD-31: a node given a wrong API port for a peer learns the right one from the peer and forwards there. */
	@Test
	void aWrongApiPortInThePeerListIsCorrectedByThePeer() throws Exception {
		var wrongNode = "n2";
		cluster = new InProcessCluster(root, 3, config -> config.nodeId().equals(wrongNode)
				? config.withPeers(config.peers().stream()
						.map(peer -> peer.id().equals(wrongNode) ? peer
								: new PeerConfig(peer.id(), peer.raftHost(), peer.raftPort(), peer.apiHost(), 1))
						.toList())
				: config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		if (leader.config().nodeId().equals(wrongNode)) {
			try (var client = admin(cluster.config("n0"))) {
				assertTrue(client.admin().transferLeadership(RaftPeerId.valueOf("n0"), 10_000).isSuccess());
			}
			leader = cluster.awaitLeader(TIMEOUT);
		}
		var leaderPeer = cluster.config(leader.config().nodeId()).self();
		var node = cluster.node(wrongNode);
		var forwarderField = ReplicatedNode.class.getDeclaredField("forwarder");
		forwarderField.setAccessible(true);
		var forwarder = (LeaderForwarder) forwarderField.get(node);
		// The listed port 1 refuses connections: without the check, no connection to the leader's API.
		var target = forwarder.target(leader.config().nodeId(), node.stateMachine()).get(10, TimeUnit.SECONDS);
		assertNotNull(target);
		assertEquals(new HostAndPort(leaderPeer.apiHost(), leaderPeer.apiPort()),
				forwarder.apiAddress(leader.config().nodeId()));
	}

	/** DD-18: a replica that started from another copy of the seed than the leader stops at genesis. */
	@Test
	void aReplicaWithAnotherSeedStopsAtGenesis() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config);
		var seed = createSeed(root.resolve("seed"), 100);
		var otherSeed = createSeed(root.resolve("other-seed"), 101);
		copy(seed, cluster.config("n0").dbDir());
		copy(seed, cluster.config("n1").dbDir());
		copy(otherSeed, cluster.config("n2").dbDir());
		cluster.startAll();
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(ReplicatedNode::isFailed));
		var failed = cluster.runningNodes().stream().filter(ReplicatedNode::isFailed).findFirst().orElseThrow();
		var failure = failed.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertNotNull(failure);
		assertTrue(Stream.iterate(failure, cause -> cause != null, Throwable::getCause)
				.anyMatch(cause -> String.valueOf(cause.getMessage()).contains("another seed")), failure::toString);
	}

	/** DD-18: copies of one seed agree, and the cluster starts as seeded. */
	@Test
	void copiesOfOneSeedStartACluster() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config);
		var seed = createSeed(root.resolve("seed"), 100);
		for (var id : cluster.ids()) {
			copy(seed, cluster.config(id).dbDir());
		}
		cluster.startAll();
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().store().clusterId() != null));
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertTrue(node.stateMachine().store().seeded());
			assertTrue(!node.isFailed());
			assertNotNull(node.stateMachine().store().seedDigest());
		}
	}

	private Path createSeed(Path dir, int keys) throws IOException {
		try (var store = NodeStore.open(dir, root.resolve("rockserver.conf"), "seed")) {
			long column = store.db().createColumn("seeded", SCHEMA);
			for (int i = 0; i < keys; i++) {
				store.db().put(0, column, key(i), Buf.wrap(new byte[] {(byte) i}), RequestType.none());
			}
		}
		return dir;
	}

	private static void copy(Path source, Path target) throws IOException {
		try (Stream<Path> walk = Files.walk(source)) {
			for (Path path : walk.toList()) {
				Path destination = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(destination);
				} else {
					Files.copy(path, destination);
				}
			}
		}
	}
}
