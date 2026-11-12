package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static it.cavallium.rockserver.distributed.ScenarioSupport.BATCH_CONTEXT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.CLIENT_RETRY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.PHASE_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.READY_TIMEOUT;
import static it.cavallium.rockserver.distributed.ScenarioSupport.SCHEMA;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * R3-G2a-3 (DD-9): the disk of one node hangs under a running 3-node process cluster. With the shim of {@link FaultFs}
 * in "HANG sync" mode, every fsync, fdatasync and sync_file_range under that node's data directory blocks, without
 * failing, until the trigger goes: the Raft log, RocksDB's WAL and the {@link DiskProbe} all wait.
 *
 * <p>Expected: the node does not exit; once the request timeout passed, its probe makes it stop listening on its API
 * port, so that clients move to the other nodes, which keep acknowledging writes; a leader on that disk stops being the
 * leader (its log does not flush: {@link LeaderStall}, and the other members elect another). Once the disk answers, the
 * node listens again, catches up, and every acknowledged write is readable on identical replicas.</p>
 *
 * <p>Multi-process: part of the heavy suites. Skipped without {@code gcc}. Written in round 6 and first run in the
 * review of R62, after R62-2 gave the Raft watch a stop that reads the log writer's stack ({@link LogWriterWatch}): a
 * writer on a hung disk is inside the fsync, not waiting for work, so the node must stay up. It did; a leader on the
 * hung disk was replaced within {@code PHASE_TIMEOUT}, and the teardown was clean. Until then it ran only with
 * {@code -Drockserver.test.hungDisk=true}.</p>
 */
@Timeout(600)
@Tag("slow")
class HungDiskTest {

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;

	@AfterEach
	void tearDown() throws IOException {
		if (cluster != null) {
			// A node blocked in a sync must not outlive the test: end the hang first, then stop everything.
			for (var id : cluster.nodeIds()) {
				FaultFs.clear(trigger(id));
			}
		}
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	private Path trigger(String id) {
		return cluster.nodeDir(id).resolve("disk-fault");
	}

	private Path start() throws Exception {
		var library = FaultFs.build(root.resolve("faultfs"));
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.create(root.resolve("cluster"), 3, 0, List.of(
				"--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "3000"));
		for (var id : cluster.members()) {
			cluster.setEnvironment(id, FaultFs.environment(library, cluster.dataDir(id), trigger(id)));
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		return rockserverConfig;
	}

	@ParameterizedTest(name = "on the {0}")
	@ValueSource(strings = {"leader", "follower"})
	void aHungDiskPausesItsNodeUntilTheDiskAnswers(String role) throws Exception {
		var rockserverConfig = start();
		var support = new ScenarioSupport(cluster);
		long column = client.createColumn("data", SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var workload = new ScenarioSupport.UniqueWrites(cluster.apiAddresses(), column, 4, 256).start();
		try {
			workload.awaitMore(200, PHASE_TIMEOUT, "before the hang");
			var leader = support.awaitLeader(PHASE_TIMEOUT);
			var victim = role.equals("leader") ? leader
					: cluster.members().stream().filter(id -> !id.equals(leader)).findFirst().orElseThrow();
			FaultFs.trigger(trigger(victim), FaultFs.Errno.HANG, FaultFs.Ops.SYNC);
			long hungAt = System.nanoTime();

			ScenarioSupport.await(PHASE_TIMEOUT, () -> !listens(victim), "node " + victim + " still serves its API");
			System.out.println("Node " + victim + " (" + role + ") paused its API "
					+ Duration.ofNanos(System.nanoTime() - hungAt).toMillis() + " ms after its disk hung");
			if (role.equals("leader")) {
				ScenarioSupport.await(PHASE_TIMEOUT, () -> {
					var now = support.leaderOrNull();
					return now != null && !now.equals(victim);
				}, "no other leader while the leader's disk hangs");
			}
			long pausedAt = System.nanoTime();
			ScenarioSupport.await(PHASE_TIMEOUT, () -> workload.lastAcknowledgedNanos() > pausedAt,
					"no write acknowledged while node " + victim + " hangs");
			workload.awaitMore(100, PHASE_TIMEOUT, "while node " + victim + " hangs");
			assertTrue(cluster.isRunning(victim), "a hung disk is no reason to exit\n" + cluster.logTail(victim));
			assertNotEquals(victim, support.awaitLeader(PHASE_TIMEOUT), "the leader is the node whose disk hangs");

			FaultFs.clear(trigger(victim));
			ScenarioSupport.await(PHASE_TIMEOUT, () -> listens(victim), "node " + victim + " does not serve again");
			support.awaitCaughtUp(victim);
			workload.awaitMore(100, PHASE_TIMEOUT, "after node " + victim + "'s disk answered");
		} finally {
			workload.stop();
		}
		support.awaitQuiescent();
		var writes = workload.writes();
		int unknownApplied = ScenarioSupport.checkWrites(client, column, writes);
		System.out.println("Writes " + writes.size() + ", unknown outcomes that applied: " + unknownApplied);
		support.checkReplicasIdentical(rockserverConfig);
	}

	private boolean listens(String id) {
		var address = cluster.apiAddress(id);
		try (var socket = new Socket()) {
			socket.connect(new InetSocketAddress(address.host(), address.port()), 1_000);
			return true;
		} catch (IOException refused) {
			return false;
		}
	}
}
