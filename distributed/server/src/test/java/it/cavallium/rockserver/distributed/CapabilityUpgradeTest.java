package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.TransferLeadershipRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3-G3-1: the upgrade path of the capability gate, in process. A cluster created before the cluster version (version
 * 0, made here by removing it from every replica's database) runs with one member of an older version: the leader
 * raises the version only to what every member supports, so pipelined updates stay off and clients get no sessions.
 * Once that member is upgraded the leader raises it to everything, sessions are advertised at once, and pipelining
 * starts with the next term, not in the current one.
 */
@Timeout(300)
@Tag("slow")
class CapabilityUpgradeTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	/** The older member's version: checksums and backups, not pipelined updates, sessions or schema digests. */
	private static final long OLDER = Capabilities.CHECKSUM | Capabilities.BACKUP;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		ReplicaOps.SUPPORTED_FOR_TESTING.clear();
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aVersionZeroClusterTurnsCapabilitiesOnOnlyOnceEveryMemberHasThem() throws Exception {
		cluster = new InProcessCluster(temp, 3, config -> config.withPipelinedUpdates(true)).startAll();
		long column = api(cluster.awaitLeader(TIMEOUT)).createColumn("c", SCHEMA);
		api(cluster.awaitLeader(TIMEOUT)).put(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (var id : cluster.ids()) {
			cluster.stop(id);
		}
		// A cluster created before the cluster version existed: none in any replica.
		for (var id : cluster.ids()) {
			var config = cluster.config(id);
			try (var store = NodeStore.open(config.dbDir(), config.rockserverConfig(), "test")) {
				store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_VERSION),
						Buf.wrap(new byte[Long.BYTES]), RequestType.none());
				assertEquals(0, store.clusterVersion());
			}
		}
		var older = "n2";
		ReplicaOps.SUPPORTED_FOR_TESTING.put(older, OLDER);
		for (var id : cluster.ids()) {
			cluster.start(id);
		}

		// The leader raises the version to what every member has, no further.
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().ops().clusterVersion() == OLDER));
		var leader = cluster.awaitLeader(TIMEOUT);
		api(leader).put(0, column, key(2), Buf.wrap(new byte[] {2}), RequestType.none());
		for (var node : cluster.runningNodes()) {
			assertFalse(node.connection().getCapabilities().clientSessions(), node.config().nodeId() + " offers sessions");
			assertEquals(OLDER, node.stateMachine().store().clusterVersion());
		}
		assertFalse(leaderPipelined(leader), "pipelined before every member reads its entries");
		// Still so after another round of the leader's check (every 10 s).
		Thread.sleep(11_000);
		assertEquals(OLDER, cluster.awaitLeader(TIMEOUT).stateMachine().ops().clusterVersion());

		// The older member is upgraded: stopped, and started again with the new version.
		cluster.stop(older);
		ReplicaOps.SUPPORTED_FOR_TESTING.remove(older);
		cluster.start(older);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().ops().clusterVersion() == Capabilities.SUPPORTED));
		leader = cluster.awaitLeader(TIMEOUT);
		api(leader).put(0, column, key(3), Buf.wrap(new byte[] {3}), RequestType.none());
		for (var node : cluster.runningNodes()) {
			assertTrue(node.connection().getCapabilities().clientSessions(), node.config().nodeId() + " offers no sessions");
		}
		// Raised during this term: pipelining waits for the next one.
		assertFalse(leaderPipelined(leader), "pipelined in the term the version was raised in");
		var division = leader.stateMachine().division();
		var target = cluster.runningNodes().stream().filter(node -> !node.isLeader()).findFirst().orElseThrow();
		long term = division.getInfo().getCurrentTerm();
		var transfer = division.getRaftServer().transferLeadershipAsync(new TransferLeadershipRequest(ClientId.randomId(),
				division.getId(), division.getGroup().getGroupId(), 1, target.stateMachine().division().getId(), 10_000));
		assertTrue(transfer.get(30, TimeUnit.SECONDS).isSuccess());
		InProcessCluster.await(TIMEOUT, () -> target.isLeader()
				&& target.stateMachine().division().getInfo().getCurrentTerm() > term);
		var newLeader = cluster.awaitLeader(TIMEOUT);
		api(newLeader).put(0, column, key(4), Buf.wrap(new byte[] {4}), RequestType.none());
		assertTrue(leaderPipelined(newLeader), "pipelined from the next term");
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertEquals(Capabilities.SUPPORTED, node.stateMachine().store().clusterVersion());
		}
	}

	private static boolean leaderPipelined(ReplicatedNode leader) {
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		var session = leader.stateMachine().leaderSession(term);
		assertTrue(session != null, "a leader session of term " + term);
		return session.pipelined();
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
