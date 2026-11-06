package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
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
				"--include-ignored", "--nocapture"))
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

	private static String value(ReplicatedNode node, long column, long key) {
		Buf value = node.stateMachine().store().db().get(0, column,
				new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array())), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}
}
