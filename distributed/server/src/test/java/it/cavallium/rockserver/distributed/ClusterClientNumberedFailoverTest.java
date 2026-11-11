package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Review of R62, 5: {@link ClusterClient} against nodes with client sessions, where a write is numbered. The first
 * node answers, then goes down. Its connection is open, so no handshake fails, and the core client sends a numbered
 * write again to that one refused address until the call's deadline, then reports an unknown outcome, which this
 * client does not carry to another node: that failed {@code PublicApiRecoveryTest}'s last check one run in three
 * (R62-5), which then went around this client. The client now finds the port closed before the call and moves on
 * with nothing sent.
 */
@Timeout(180)
class ClusterClientNumberedFailoverTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(30));

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
	void aNumberedWriteFailsOverFromANodeThatWentDownAfterItAnswered() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		// A follower first: the two others stay a majority with their leader when it goes down.
		var first = cluster.runningNodes().stream().filter(node -> node != leader).findFirst().orElseThrow();
		var nodes = new LinkedHashMap<String, HostAndPort>();
		nodes.put(first.config().nodeId(), address(first));
		for (var node : cluster.runningNodes()) {
			nodes.putIfAbsent(node.config().nodeId(), address(node));
		}
		try (var client = new ClusterClient(nodes, Duration.ofSeconds(20), false)) {
			var column = client.createColumn("c", SCHEMA, CONTEXT);
			assertEquals(OpStatus.OK, column.status(), () -> String.valueOf(column.error()));
			assertEquals(first.config().nodeId(), column.node());
			var before = client.put(column.value(), key(1), value(1), CONTEXT);
			assertEquals(OpStatus.OK, before.status(), () -> String.valueOf(before.error()));

			cluster.halt(first.config().nodeId());
			Thread.sleep(1_200);
			long start = System.nanoTime();
			// A schema change and a put: both numbered writes.
			var other = client.createColumn("other", SCHEMA, CONTEXT);
			var put = client.put(column.value(), key(1), value(2), CONTEXT);
			var elapsed = Duration.ofNanos(System.nanoTime() - start);
			assertEquals(OpStatus.OK, other.status(), () -> "after " + elapsed + ": " + other.error());
			assertNotEquals(first.config().nodeId(), other.node());
			assertEquals(2, other.attempts());
			assertEquals(OpStatus.OK, put.status(), () -> String.valueOf(put.error()));
			assertEquals(1, put.attempts(), "the client did not stay on the node that answered");
			assertTrue(elapsed.compareTo(Duration.ofSeconds(10)) < 0, "the writes took " + elapsed);
			assertEquals(value(2), client.get(column.value(), key(1), CONTEXT).valueOrThrow());
		}
	}

	private static HostAndPort address(ReplicatedNode node) {
		return new HostAndPort(node.config().self().apiHost(), node.config().self().apiPort());
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}
}
