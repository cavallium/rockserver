package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@link ClusterClient} against a real single-node Rockserver gRPC server. */
@Timeout(60)
@Tag("slow")
class ClusterClientTest {

	private static final RequestContext CONTEXT = RequestContext.latency(Duration.ofSeconds(5));
	private static final RequestContext BATCH_CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);

	@TempDir
	Path dir;
	private EmbeddedConnection db;
	private GrpcServer server;

	@BeforeEach
	void startServer() throws IOException {
		db = new EmbeddedConnection(dir.resolve("db"), "cluster-client-test", null);
		server = new GrpcServer(db, new InetSocketAddress("127.0.0.1", 0));
		server.start();
	}

	@AfterEach
	void stopServer() throws IOException {
		server.close();
		db.close();
	}

	@Test
	void failsOverFromUnreachableNode() throws IOException {
		var nodes = new LinkedHashMap<String, HostAndPort>();
		nodes.put("down", new HostAndPort("127.0.0.1", freePort()));
		nodes.put("up", serverAddress());
		try (var client = new ClusterClient(nodes, Duration.ofSeconds(10), false)) {
			var column = client.createColumn("data", SCHEMA, BATCH_CONTEXT);
			assertEquals(OpStatus.OK, column.status(), () -> String.valueOf(column.error()));
			assertEquals("up", column.node());
			assertEquals(2, column.attempts());
			long columnId = column.value();

			assertEquals(OpStatus.OK, client.put(columnId, key(1), value("a"), CONTEXT).status());
			assertEquals(value("a"), client.get(columnId, key(1), CONTEXT).valueOrThrow());

			var commit = client.transaction(BATCH_CONTEXT, Duration.ofSeconds(30), (api, tx) ->
					api.put(tx, columnId, key(2), value("b"), RequestType.none()));
			assertEquals(OpStatus.OK, commit.status(), () -> String.valueOf(commit.error()));
			assertTrue(commit.value());
			assertEquals(value("b"), client.get(columnId, key(2), CONTEXT).valueOrThrow());
			assertNull(client.get(columnId, key(3), CONTEXT).valueOrThrow());
		}
	}

	/**
	 * A node that accepts TCP connections but never answers, like a paused (SIGSTOP) process: the connection
	 * handshake must not hold calls for longer than their request timeout.
	 */
	@Test
	void failsOverFromFrozenNodeWithinRequestTimeout() throws Exception {
		long columnId = db.getSyncApi(BATCH_CONTEXT).createColumn("data", SCHEMA);
		var shortContext = RequestContext.latency(Duration.ofMillis(500));
		// Never accepted: the kernel still completes the TCP handshakes, as it does for a stopped process.
		try (var frozen = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
			var nodes = new LinkedHashMap<String, HostAndPort>();
			nodes.put("frozen", new HostAndPort("127.0.0.1", frozen.getLocalPort()));
			nodes.put("up", serverAddress());
			try (var client = new ClusterClient(nodes, Duration.ofSeconds(30), false);
					var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				long start = System.nanoTime();
				// Concurrent calls, both starting on the frozen node: neither waits for the other's handshake.
				var put = executor.submit(() -> client.put(columnId, key(1), value("a"), shortContext));
				var get = executor.submit(() -> client.get(columnId, key(2), shortContext));
				var putResult = put.get(20, TimeUnit.SECONDS);
				var getResult = get.get(20, TimeUnit.SECONDS);
				var elapsed = Duration.ofNanos(System.nanoTime() - start);

				assertEquals(OpStatus.OK, putResult.status(), () -> String.valueOf(putResult.error()));
				assertEquals("up", putResult.node());
				assertEquals(2, putResult.attempts());
				assertEquals(OpStatus.OK, getResult.status(), () -> String.valueOf(getResult.error()));
				assertNull(getResult.value());
				// The core client's own handshake timeout is 10 s; each call should spend about 500 ms on it.
				assertTrue(elapsed.compareTo(Duration.ofSeconds(5)) < 0, "calls took " + elapsed);
				assertEquals(value("a"), client.get(columnId, key(1), shortContext).valueOrThrow());
			}
		}
	}

	@Test
	void writeNeverDeliveredIsFailed() throws IOException {
		var nodes = new LinkedHashMap<String, HostAndPort>();
		nodes.put("a", new HostAndPort("127.0.0.1", freePort()));
		nodes.put("b", new HostAndPort("127.0.0.1", freePort()));
		try (var client = new ClusterClient(nodes, Duration.ofMillis(500), true)) {
			var put = client.put(1, key(1), value("x"), CONTEXT);
			assertEquals(OpStatus.FAILED, put.status());
			assertFalse(put.acknowledged());
			assertTrue(put.attempts() > 2, "retried until the deadline: " + put.attempts());
		}
	}

	@Test
	void writeOnBrokenConnectionIsUnknownAndReadIsFailed() throws IOException {
		long columnId = db.getSyncApi(BATCH_CONTEXT).createColumn("data", SCHEMA);
		try (var proxy = new LinkProxy(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0),
				new InetSocketAddress("127.0.0.1", server.getPort()))) {
			try (var client = new ClusterClient(nodes(proxy), Duration.ofSeconds(2), false)) {
				assertEquals(OpStatus.OK, client.put(columnId, key(1), value("a"), CONTEXT).status());

				// The connection is reset after it was established: the request may have reached the server.
				proxy.block();
				var put = client.put(columnId, key(1), value("b"), CONTEXT);
				assertEquals(OpStatus.UNKNOWN, put.status(), () -> String.valueOf(put.error()));
				assertEquals(1, put.attempts(), "an ambiguous write is not retried");
				var read = client.get(columnId, key(1), RequestContext.latency(Duration.ofMillis(500)));
				assertEquals(OpStatus.FAILED, read.status());
			}
			proxy.unblock();
			// A fresh client: the old channel may wait out its reconnect backoff.
			try (var client = new ClusterClient(nodes(proxy), Duration.ofSeconds(10), false)) {
				assertEquals(value("a"), client.get(columnId, key(1), CONTEXT).valueOrThrow());
			}
		}
	}

	private static Map<String, HostAndPort> nodes(LinkProxy proxy) {
		return Map.of("proxied", new HostAndPort("127.0.0.1", proxy.listenAddress().getPort()));
	}

	private HostAndPort serverAddress() {
		return new HostAndPort("127.0.0.1", server.getPort());
	}

	private static Keys key(int id) {
		return new Keys(Buf.wrap(new byte[] {0, 0, 0, (byte) id}));
	}

	private static Buf value(String text) {
		return Buf.wrap(text.getBytes(StandardCharsets.UTF_8));
	}

	private static int freePort() throws IOException {
		try (var socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
			return socket.getLocalPort();
		}
	}
}
