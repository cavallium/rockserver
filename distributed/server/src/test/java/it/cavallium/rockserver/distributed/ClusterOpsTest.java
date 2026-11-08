package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
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
import java.util.List;
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
		// A second checksum after one more write: every replica lists exactly both, in log order, with its own results.
		api.put(0, column, key(500), value("v500"), RequestType.none());
		long secondIndex = (Long) leader.proposer().propose(new LogEntry.Checksum()).get(20, TimeUnit.SECONDS);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				node.stateMachine().ops().checksumResult(secondIndex).state() == ReplicaOps.State.DONE));
		var listedDigests = new HashSet<List<String>>();
		for (var node : cluster.runningNodes()) {
			var ops = node.stateMachine().ops();
			var listed = ops.checksumResults();
			assertEquals(List.of(checksumIndex, secondIndex), listed.stream().map(ReplicaOps.Result::index).toList(),
					node.config().nodeId());
			assertEquals(List.of(ops.checksumResult(checksumIndex), ops.checksumResult(secondIndex)), listed,
					node.config().nodeId());
			assertEquals(List.of(500L, 501L), listed.stream().map(ReplicaOps.Result::keys).toList(), node.config().nodeId());
			listedDigests.add(listed.stream().map(ReplicaOps.Result::detail).toList());
		}
		assertEquals(1, listedDigests.size(), "listed digests " + listedDigests);
		assertTrue(!listedDigests.iterator().next().get(0).equals(listedDigests.iterator().next().get(1)),
				"the second digest covers one more key");
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
				ReplicaOps.OP_PROPOSE_BACKUP, ReplicaOps.backupPayload(77, "b1"), Duration.ofSeconds(20)).join();
		long backupIndex = ByteBuffer.wrap(reply).getLong();
		awaitBackup("b1", backupIndex, ReplicaOps.State.DONE);
		// What the Raft port answers about it, by name and log index.
		var backupAnswer = ReplicaOps.decodeResult(ReplicaOps.call(leader.config().self().raftAddress(),
				leader.stateMachine().groupId(), ReplicaOps.OP_BACKUP_RESULT, ReplicaOps.backupPayload(backupIndex, "b1"),
				Duration.ofSeconds(5)).join());
		assertEquals(ReplicaOps.State.DONE, backupAnswer.state());
		assertEquals(backupIndex, backupAnswer.index());
		// The same request proposed again (admin retrying after a lost answer): the same backup, done again.
		long again = (Long) follower.proposer().propose(new LogEntry.Backup("b1", 77)).get(20, TimeUnit.SECONDS);
		awaitBackup("b1", again, ReplicaOps.State.DONE);
		// Another request with the name: fails, and the first result stays DONE.
		long reused = (Long) follower.proposer().propose(new LogEntry.Backup("b1", 78)).get(20, TimeUnit.SECONDS);
		awaitBackup("b1", reused, ReplicaOps.State.FAILED);
		awaitBackup("b1", backupIndex, ReplicaOps.State.DONE);
		// Asked about another name at that index, or a position that holds no backup: never an older result.
		assertEquals(ReplicaOps.State.FAILED, leader.stateMachine().ops().backupResult("b2", backupIndex).state());
		assertEquals(ReplicaOps.State.NONE, leader.stateMachine().ops().backupResult("b1", checksumIndex).state());
		assertEquals(ReplicaOps.State.PENDING, leader.stateMachine().ops().backupResult("b1", reused + 1_000_000).state());
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

		// A new cluster, seeded from copies of the backup, into a data directory that held the results and backups of
		// another cluster: they are moved aside, so that they do not answer for the new one.
		var newRoot = temp.resolve("new");
		Path target = newRoot.resolve("n0");
		Files.createDirectories(target.resolve(ReplicaOps.BACKUPS_DIR).resolve("old"));
		Files.writeString(target.resolve("replica-ops.log"), "2026-01-01T00:00:00Z backup 5 DONE b1 /old\n");
		ReplicaOps.restore(backup, target, null);
		assertTrue(Files.notExists(target.resolve("replica-ops.log")));
		assertTrue(Files.notExists(target.resolve(ReplicaOps.BACKUPS_DIR)));
		try (var listed = Files.list(target)) {
			assertEquals(2, listed.filter(p -> p.getFileName().toString().contains(".before-restore-")).count());
		}
		// The other nodes in the two ways the runbook gives. n1: restore-backup from its own copy of the same backup. Each
		// restore opens its database once and leaves files of its own (OPTIONS, the info log): the seed digest must still
		// be the same on every node, or the followers stop at genesis (R26-1).
		Path copy = temp.resolve("copy-n1");
		copyTree(backup, copy);
		ReplicaOps.restore(copy, newRoot.resolve("n1"), null);
		// n2: a copy of the db/ that n0's restore left, stopped, with db.seed-digest. That file holds the backup's digest,
		// which a digest of the restored db/ is not: a copy without it records the db/ digest at its first start, and the
		// node stops at genesis (R26-1 review). The runbook says to copy the file too.
		Path restoredDb = target.resolve("db");
		Path seedDigestFile = target.resolve("db.seed-digest");
		String backupDigest = NodeStore.readSeedDigest(restoredDb);
		assertNotNull(backupDigest);
		Path onlyDb = temp.resolve("only-db").resolve("db");
		copyTree(restoredDb, onlyDb);
		assertNotEquals(backupDigest, NodeStore.recordSeedDigest(onlyDb),
				"a copy of db/ without db.seed-digest records another digest");
		copyTree(restoredDb, newRoot.resolve("n2").resolve("db"));
		Files.copy(seedDigestFile, newRoot.resolve("n2").resolve("db.seed-digest"));
		assertEquals(backupDigest, NodeStore.recordSeedDigest(newRoot.resolve("n2").resolve("db")),
				"a copy of db/ with db.seed-digest keeps the backup's digest");
		restored = new InProcessCluster(newRoot, 3, c -> c).startAll();
		var newLeader = restored.awaitLeader(TIMEOUT);
		var newApi = newLeader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		// Every node applies the seeded genesis; a follower whose seed digest differs stops there instead (exit 3).
		InProcessCluster.await(TIMEOUT, () -> restored.runningNodes().stream().allMatch(node ->
				node.stateMachine().store().clusterId() != null));
		restored.awaitConverged(TIMEOUT);
		var seedDigests = new HashSet<String>();
		for (var node : restored.runningNodes()) {
			assertNotNull(node.stateMachine().store().clusterId(), node.config().nodeId());
			assertTrue(node.stateMachine().store().seeded(), node.config().nodeId());
			seedDigests.add(String.valueOf(node.stateMachine().store().seedDigest()));
		}
		assertEquals(3, restored.runningNodes().size());
		assertEquals(1, seedDigests.size(), "seed digests " + seedDigests);
		long newColumn = newLeader.stateMachine().store().db().getColumnId("c");
		for (long i = 0; i < 500; i++) {
			assertEquals("v" + i, text(newApi.get(0, newColumn, key(i), RequestType.current())));
		}
		assertNull(newApi.get(0, newColumn, key(1000), RequestType.current()));
	}

	private static void copyTree(Path from, Path to) throws java.io.IOException {
		try (var walk = Files.walk(from)) {
			for (Path file : walk.toList()) {
				Path target = to.resolve(from.relativize(file).toString());
				if (Files.isDirectory(file)) {
					Files.createDirectories(target);
				} else {
					Files.copy(file, target);
				}
			}
		}
	}

	private void awaitBackup(String name, long index, ReplicaOps.State state) throws Exception {
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().allMatch(node ->
				node.stateMachine().ops().backupResult(name, index).state() == state));
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
