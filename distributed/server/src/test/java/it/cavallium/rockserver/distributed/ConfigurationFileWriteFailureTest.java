package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a node does when Ratis fails to write {@code raft-meta.conf} while it applies a configuration entry (review of
 * R66, 1).
 *
 * <p>Ratis 3.3.1 writes that file from two threads with no lock between them, through one temporary name: the state
 * machine updater, for every configuration entry it applies (a new leader's first entry is one), and the thread that
 * handles a snapshot chunk whose configuration differs from the node's (design: "Upstream", R66). When the two
 * overlap, the rename of one of them fails. If it is the chunk's, the leader's install request fails and is sent
 * again. If it is the updater's, the updater ends ("caught a Throwable") and closes the division: the case here,
 * which a node of a running cluster can hit when a snapshot reaches it while it still applies entries of its own
 * log.</p>
 *
 * <p>The race itself needs two writes within the same few milliseconds, so the test fails the updater's write in
 * another way, with the same exception type on the same call ({@code RaftStorageImpl.writeRaftConfiguration} throws an
 * {@code IOException}): a directory stands where the temporary file goes. Expected: the node stops as a failed
 * replica, with exit code 3 and "Raft closed" in its log, so that a supervisor starts it again; the other two serve;
 * the node, started again once the write can succeed, applies the entry and serves.</p>
 *
 * <p>Multi-process: part of the heavy suites.</p>
 */
@Timeout(300)
@Tag("slow")
class ConfigurationFileWriteFailureTest {

	private static final List<String> SERVER_ARGS = List.of(
			"--election-timeout-ms", "400:800",
			"--request-timeout-ms", "3000");
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration EXIT_TIMEOUT = Duration.ofSeconds(60);
	private static final RequestContext REQUEST = RequestContext.batch(Duration.ofSeconds(20));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	// Kept on failure: the node logs and data directories are the evidence.
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private ProcessCluster cluster;

	@AfterEach
	void tearDown() {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aNodeWhoseUpdaterCannotWriteTheConfigurationFileStopsAndRestarts() throws Exception {
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, ProcessCluster.pipelined(SERVER_ARGS));
		cluster.waitReady(READY_TIMEOUT);
		var leader = ClientFailoverTest.awaitLeader(cluster);
		var followers = cluster.nodeIds().stream().filter(id -> !id.equals(leader)).toList();
		var victim = followers.get(0);
		var next = followers.get(1);
		long column;
		try (var connection = GrpcConnection.forHostAndPort("before", cluster.apiAddress(leader))) {
			var api = connection.getSyncApi(REQUEST);
			column = api.createColumn("data", SCHEMA);
			api.put(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		}

		// The updater wrote the file for the entries applied so far; its next write is for the next configuration entry.
		Path conf;
		try (var files = Files.walk(cluster.dataDir(victim))) {
			conf = files.filter(file -> file.getFileName().toString().equals("raft-meta.conf")).findFirst()
					.orElseThrow(() -> new AssertionError("no raft-meta.conf under " + cluster.dataDir(victim)));
		}
		var obstacle = conf.resolveSibling("raft-meta.conf.tmp");
		Files.createDirectory(obstacle);
		// A new leader's first entry is a configuration entry, which every node applies.
		long changed = System.nanoTime();
		cluster.admin(cluster.nodeIds(), "transfer-leader", next);

		assertEquals(3, cluster.awaitExit(victim, EXIT_TIMEOUT), () -> cluster.logTail(victim));
		System.out.println("ConfigurationFileWriteFailureTest: node " + victim + " exited with 3 "
				+ Duration.ofNanos(System.nanoTime() - changed).toMillis() + " ms after the leader change was asked for");
		var log = Files.readString(cluster.stderrLog(victim));
		assertTrue(log.contains("caught a Throwable"), () -> cluster.logTail(victim));
		assertTrue(log.contains("raft-meta.conf.tmp"), () -> cluster.logTail(victim));
		assertTrue(log.contains("Raft closed"), () -> cluster.logTail(victim));

		try (var connection = GrpcConnection.forHostAndPort("meanwhile", cluster.apiAddress(next))) {
			connection.getSyncApi(REQUEST).put(0, column, key(2), Buf.wrap(new byte[] {2}), RequestType.none());
		}

		Files.delete(obstacle);
		cluster.start(victim);
		cluster.waitReady(victim, READY_TIMEOUT);
		try (var connection = GrpcConnection.forHostAndPort("after", cluster.apiAddress(victim))) {
			var api = connection.getSyncApi(REQUEST);
			api.put(0, column, key(3), Buf.wrap(new byte[] {3}), RequestType.none());
			for (int i = 1; i <= 3; i++) {
				Buf value = api.get(0, column, key(i), RequestType.current());
				assertEquals(i, value == null ? -1 : value.toByteArray()[0], "key " + i + " read through node " + victim);
			}
		}
		// The restarted node applies what the others committed, the configuration entry it failed on included.
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (true) {
			var status = cluster.adminResult(cluster.nodeIds(), "status");
			if (status.exitCode() == 0 && ColdRestartTest.convergedIndex(cluster.nodeIds(), status.output()) != null) {
				break;
			}
			assertTrue(System.nanoTime() - deadline < 0, () -> "The nodes did not reach the same applied index:\n"
					+ status.output() + cluster.logTail(victim));
			Thread.sleep(100);
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(java.nio.ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
