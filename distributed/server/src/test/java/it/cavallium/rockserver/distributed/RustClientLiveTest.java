package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the Rust client's live test ({@code rust-client/tests/live_server.rs}) against a follower of a three-node
 * in-process cluster, then checks on every node what it wrote. Off unless the system property
 * {@value #RUST_CLIENT_DIR} names a buildable copy of {@code rust-client} (cargo on the PATH, crates available
 * offline; the crate's workspace lists a member missing from the repository, so a copy with an empty member list).
 */
@Timeout(600)
@EnabledIfSystemProperty(named = RustClientLiveTest.RUST_CLIENT_DIR, matches = ".+")
class RustClientLiveTest {

	static final String RUST_CLIENT_DIR = "rockserver.rust-client.dir";
	private static final String GRPC_GRACEFUL_SHUTDOWN =
			"it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms";
	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void theRustClientNumbersItsWritesAgainstALiveCluster() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.runningNodes().stream().filter(node -> node != leader).findFirst().orElseThrow();
		var self = follower.config().self();
		var log = temp.resolve("cargo.log");
		var process = new ProcessBuilder(List.of("cargo", "test", "--offline", "--test", "live_server", "--",
				"--include-ignored", "--nocapture", "--exact", "numbered_writes_against_a_live_server"))
				.directory(Path.of(System.getProperty(RUST_CLIENT_DIR)).toFile())
				.redirectErrorStream(true)
				.redirectOutput(log.toFile());
		process.environment().put("ROCKSERVER_TEST_ADDR", "http://" + self.apiHost() + ":" + self.apiPort());
		var running = process.start();
		if (!running.waitFor(500, TimeUnit.SECONDS)) {
			running.destroyForcibly();
		}
		var output = Files.readString(log);
		assertEquals(0, running.exitValue(), output);
		assertTrue(output.contains("test numbered_writes_against_a_live_server ... ok"), output);

		cluster.awaitConverged(TIMEOUT);
		long column = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT)).getColumnId("rust-live");
		for (var node : cluster.runningNodes()) {
			assertEquals("a,b,c,d", value(node, column, 1), "merged once each, on " + node.config().nodeId());
			assertEquals("m11", value(node, column, 11), "on " + node.config().nodeId());
			assertEquals("p13", value(node, column, 13), "on " + node.config().nodeId());
			assertEquals("u", value(node, column, 30), "on " + node.config().nodeId());
			assertEquals("t", value(node, column, 40), "on " + node.config().nodeId());
			assertEquals("once", value(node, column, 70), "a resent copy merged once, on " + node.config().nodeId());
		}
	}

	/**
	 * RA-2.6b: the Rust client's numbered writes (a merge, a multi merge and a multi put with results) whose calls are
	 * lost with a real leader. The client reaches the leader through a proxy; the writes commit on the followers while
	 * the leader's apply is stopped, so no answer comes; the leader is halted, which drops the calls, and the proxy is
	 * moved to a follower on the same port. The client sends the writes again under their numbers, and they answer from
	 * the record with the first copy's results: applied once on every node, and the client counted resends
	 * ({@code numbered_writes_resent_across_a_leader_loss} in {@code rust-client/tests/live_server.rs}).
	 */
	@Test
	void theRustClientResendsItsNumberedWritesAcrossALeaderLoss() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		var follower = cluster.runningNodes().stream().filter(node -> node != leader).findFirst().orElseThrow();
		var signals = Files.createDirectories(temp.resolve("signals"));
		var proxy = new LinkProxy(new InetSocketAddress("127.0.0.1", 0), apiAddress(leader));
		var port = proxy.listenAddress().getPort();
		LinkProxy moved = null;
		var log = temp.resolve("cargo-failover.log");
		var process = new ProcessBuilder(List.of("cargo", "test", "--offline", "--test", "live_server", "--",
				"--include-ignored", "--nocapture", "--exact", "numbered_writes_resent_across_a_leader_loss"))
				.directory(Path.of(System.getProperty(RUST_CLIENT_DIR)).toFile())
				.redirectErrorStream(true)
				.redirectOutput(log.toFile());
		process.environment().put("ROCKSERVER_TEST_ADDR", "http://127.0.0.1:" + port);
		process.environment().put("ROCKSERVER_TEST_SIGNAL_DIR", signals.toString());
		var graceful = System.getProperty(GRPC_GRACEFUL_SHUTDOWN);
		var gate = new CountDownLatch(1);
		Thread halting = null;
		Process running = null;
		try {
			running = process.start();
			var started = running;
			// Compiling the test may take a while.
			InProcessCluster.await(Duration.ofSeconds(400), () -> Files.exists(signals.resolve("warm")) || !started.isAlive());
			assertTrue(Files.exists(signals.resolve("warm")), () -> "the Rust test did not start: " + read(log));
			cluster.awaitConverged(TIMEOUT);
			long column = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT)).getColumnId("rust-failover");
			leader.stateMachine().testHooks().applyGate = gate;
			Files.writeString(signals.resolve("go"), "");

			var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
			InProcessCluster.await(TIMEOUT, () -> followers.stream().allMatch(node -> "x".equals(value(node, column, 1))
					&& "m4".equals(value(node, column, 4)) && "new6".equals(value(node, column, 6))));

			// The leader's API server drops the calls in progress instead of waiting for them.
			System.setProperty(GRPC_GRACEFUL_SHUTDOWN, "0");
			halting = Thread.ofPlatform().name("halt-" + leaderId).start(() -> {
				try {
					cluster.halt(leaderId);
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			});
			proxy.close();
			moved = new LinkProxy(new InetSocketAddress("127.0.0.1", port), apiAddress(follower));

			if (!running.waitFor(120, TimeUnit.SECONDS)) {
				running.destroyForcibly();
			}
			var output = read(log);
			assertEquals(0, running.exitValue(), output);
			assertTrue(output.contains("test numbered_writes_resent_across_a_leader_loss ... ok"), output);
			gate.countDown();
			halting.join();
			halting = null;

			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				var id = node.config().nodeId();
				assertEquals("x,y", value(node, column, 1), "merged once, on " + id);
				assertEquals("m2", value(node, column, 2), "on " + id);
				assertEquals("m4", value(node, column, 4), "on " + id);
				assertEquals("new5", value(node, column, 5), "on " + id);
				assertEquals("new6", value(node, column, 6), "on " + id);
			}
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
			if (running != null && running.isAlive()) {
				running.destroyForcibly();
			}
			if (halting != null) {
				halting.join();
			}
			proxy.close();
			if (moved != null) {
				moved.close();
			}
			if (graceful == null) {
				System.clearProperty(GRPC_GRACEFUL_SHUTDOWN);
			} else {
				System.setProperty(GRPC_GRACEFUL_SHUTDOWN, graceful);
			}
		}
	}

	private static InetSocketAddress apiAddress(ReplicatedNode node) {
		var self = node.config().self();
		return new InetSocketAddress(self.apiHost(), self.apiPort());
	}

	private static String read(Path log) {
		try {
			return Files.readString(log);
		} catch (IOException ex) {
			return "(no output: " + ex + ")";
		}
	}

	private static String value(ReplicatedNode node, long column, long key) {
		Buf value = node.stateMachine().store().db().get(0, column,
				new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array())), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}
}
