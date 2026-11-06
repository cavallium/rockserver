package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The cluster operations of {@link ReplicaOps} on a real 3-node cluster: the cluster version at genesis, a checksum that
 * every replica computes alike, a backup that every replica writes at the same position, the restore of that backup
 * into a new cluster, and the gauges of {@link NodeMonitor} exported to each node's registry.
 */
class ClusterOpsTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(60);

	@TempDir
	Path temp;
	private InProcessCluster cluster;
	private InProcessCluster restored;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
		if (restored != null) {
			restored.close();
		}
	}

	@Test
	void versionChecksumBackupAndRestore() throws Exception {
		cluster = new InProcessCluster(Files.createDirectories(temp.resolve("old")), 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (long i = 0; i < 500; i++) {
			api.put(0, column, key(i), value("v" + i), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);

		// A cluster created by this version can use everything at once.
		for (var node : cluster.runningNodes()) {
			assertEquals(Capabilities.GENESIS, node.stateMachine().ops().clusterVersion(), node.config().nodeId());
			assertEquals(Capabilities.GENESIS, node.stateMachine().store().clusterVersion());
			assertTrue(node.connection().getCapabilities().clientSessions());
		}

		// Checksum, proposed through a follower: every replica digests the same data.
		var follower = cluster.anyFollower();
		long checksumIndex = (Long) follower.proposer().propose(new LogEntry.Checksum()).get(20, TimeUnit.SECONDS);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				node.stateMachine().ops().checksumResult(checksumIndex).state() == ReplicaOps.State.DONE));
		var digests = new HashSet<String>();
		for (var node : cluster.runningNodes()) {
			var result = node.stateMachine().ops().checksumResult(checksumIndex);
			assertEquals(500, result.keys());
			digests.add(result.detail());
		}
		assertEquals(1, digests.size(), "digests " + digests);
		// Every replica compared with the others and found them equal.
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				node.metrics().amount("checksum.matches").count() >= 1));
		for (var node : cluster.runningNodes()) {
			assertEquals(0, node.metrics().amount("checksum.mismatches").count());
		}
		// What the Raft port answers about it.
		var answer = ReplicaOps.decodeResult(ReplicaOps.call(leader.config().self().raftAddress(),
				leader.stateMachine().groupId(), ReplicaOps.OP_CHECKSUM_RESULT,
				ByteBuffer.allocate(Long.BYTES).putLong(checksumIndex).array(), Duration.ofSeconds(5)).join());
		assertEquals(digests.iterator().next(), answer.detail());

		// Backup through the Raft port, as admin does it.
		var reply = ReplicaOps.call(leader.config().self().raftAddress(), leader.stateMachine().groupId(),
				ReplicaOps.OP_PROPOSE_BACKUP, "b1".getBytes(StandardCharsets.UTF_8), Duration.ofSeconds(20)).join();
		long backupIndex = ByteBuffer.wrap(reply).getLong();
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node -> {
			var result = node.stateMachine().ops().backupResult("b1");
			return result != null && result.state() == ReplicaOps.State.DONE;
		}));
		for (var node : cluster.runningNodes()) {
			var dir = ReplicaOps.backupsDir(node.config().dataDir()).resolve("b1");
			var properties = ReplicaOps.readBackupProperties(dir);
			assertEquals(String.valueOf(backupIndex), properties.getProperty("index"));
			assertTrue(Files.isDirectory(dir.resolve(ReplicaOps.BACKUP_DB_DIR)));
		}
		// Written after the backup: not in it.
		api.put(0, column, key(1000), value("after"), RequestType.none());

		// Gauges, exported to the registry of each node's database.
		cluster.runningNodes().forEach(node -> node.monitor().refresh());
		long leaders = cluster.runningNodes().stream()
				.filter(node -> node.monitor().role() == NodeMonitor.LEADER).count();
		assertEquals(1, leaders);
		var registry = leader.stateMachine().store().db().getMetricsRegistry();
		var role = registry.find("rockserver.replication.raft.role").tag("node", leader.config().nodeId()).gauge();
		assertNotNull(role);
		assertEquals(NodeMonitor.LEADER, role.value());
		assertTrue(leader.monitor().logBytes() > 0);
		assertNotNull(registry.find("rockserver.replication.duration").tag("name", "apply").functionTimer());
		assertEquals(leader.config().nodeId(), registry.find("rocksdb.operation.timer").timers().iterator().next()
				.getId().getTag("node"), "the core meters carry the node too");

		Path backup = ReplicaOps.backupsDir(follower.config().dataDir()).resolve("b1");
		cluster.close();
		cluster = null;

		// A new cluster, seeded from copies of the backup.
		var newRoot = temp.resolve("new");
		ReplicaOps.restore(backup, newRoot.resolve("n0"), null);
		restored = new InProcessCluster(newRoot, 1, c -> c).startAll();
		var newLeader = restored.awaitLeader(TIMEOUT);
		var newApi = newLeader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		InProcessCluster.await(TIMEOUT, () -> newLeader.stateMachine().store().clusterId() != null);
		assertTrue(newLeader.stateMachine().store().seeded());
		long newColumn = newLeader.stateMachine().store().db().getColumnId("c");
		for (long i = 0; i < 500; i++) {
			assertEquals("v" + i, text(newApi.get(0, newColumn, key(i), RequestType.current())));
		}
		assertNull(newApi.get(0, newColumn, key(1000), RequestType.current()));
	}

	private static Keys key(long i) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array()));
	}

	private static Buf value(String text) {
		return Buf.wrap(text.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}
}
