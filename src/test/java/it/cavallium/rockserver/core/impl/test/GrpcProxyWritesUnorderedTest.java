package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Put;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Writes a client sends without awaiting the previous one have no relative order (R23-A): a gRPC server in front of
 * another connection, as a replicated node's is, runs each call on a thread of its own, so a later put to the same key
 * reaches the connection, and completes, while an earlier one is still running. A caller that needs order awaits.
 */
class GrpcProxyWritesUnorderedTest {

	private static final Keys KEY = new Keys(Buf.wrap(new byte[] {1}));

	/** Records when each put starts and ends; the put of "A" waits for {@link #releaseA}. */
	private static final class RecordingConnection implements RocksDBConnection, RocksDBAPI {

		final List<String> events = new CopyOnWriteArrayList<>();
		final CountDownLatch releaseA = new CountDownLatch(1);

		@Override
		public URI getUrl() {
			return URI.create("recording://writes");
		}

		@Override
		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return this;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return this;
		}

		@Override
		public void close() {}

		@Override
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
			put(request);
			return null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
			return (RA) CompletableFuture.runAsync(() -> put(request), Thread::startVirtualThread);
		}

		private void put(RocksDBAPICommand<?, ?, ?> request) {
			if (!(request instanceof Put<?> put)) {
				throw new IllegalStateException("Unexpected " + request);
			}
			String value = new String(put.value().toByteArray(), StandardCharsets.UTF_8);
			events.add(value + "-start");
			if (value.equals("A")) {
				try {
					releaseA.await();
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
			events.add(value + "-end");
		}
	}

	private RecordingConnection connection;
	private GrpcServer server;
	private GrpcConnection client;

	@BeforeEach
	void setUp() throws Exception {
		connection = new RecordingConnection();
		server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0));
		server.start();
		client = GrpcConnection.forHostAndPort("unordered-writes", new HostAndPort("127.0.0.1", server.getPort()));
	}

	@AfterEach
	void tearDown() throws Exception {
		connection.releaseA.countDown();
		if (client != null) {
			client.close();
		}
		if (server != null) {
			server.close();
		}
	}

	@Test
	void aLaterPutToTheSameKeyCompletesBeforeAnEarlierOneThatWasNotAwaited() throws Exception {
		var api = client.getAsyncApi(RequestContext.batch(Duration.ofSeconds(30)));
		CompletableFuture<Void> first = api.putAsync(0, 1, KEY, text("A"), RequestType.none());
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (!connection.events.contains("A-start") && System.nanoTime() < deadline) {
			Thread.sleep(5);
		}
		assertTrue(connection.events.contains("A-start"), "the first put did not reach the connection");
		CompletableFuture<Void> second = api.putAsync(0, 1, KEY, text("B"), RequestType.none());

		second.get(30, TimeUnit.SECONDS);
		assertFalse(first.isDone(), "the first put completed before it was released");
		connection.releaseA.countDown();
		first.get(30, TimeUnit.SECONDS);
		assertEquals(List.of("A-start", "B-start", "B-end", "A-end"), connection.events);
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}
}
