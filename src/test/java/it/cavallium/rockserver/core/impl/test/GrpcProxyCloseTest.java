package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@code closeFailedUpdate} against a connection that answers late, as a replicated one does for a chained update. */
class GrpcProxyCloseTest {

	/** More than the workers and the queue of the CONTROL scheduler of a proxy server hold together. */
	private static final int CLOSES = 2_000;

	/** Holds every close until a test answers it. */
	private static final class LateConnection implements RocksDBConnection, RocksDBAPI {

		final ConcurrentLinkedQueue<CompletableFuture<Void>> closes = new ConcurrentLinkedQueue<>();
		final AtomicInteger syncCalls = new AtomicInteger();

		@Override
		public URI getUrl() {
			return URI.create("late://close");
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
			syncCalls.incrementAndGet();
			throw new IllegalStateException("A close must not wait on a worker");
		}

		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
			if (!(request instanceof CloseFailedUpdate)) {
				throw new IllegalStateException("Unexpected " + request);
			}
			var close = new CompletableFuture<Void>();
			closes.add(close);
			return (RA) close;
		}
	}

	private LateConnection connection;
	private GrpcServer server;
	private GrpcConnection client;

	@BeforeEach
	void setUp() throws Exception {
		connection = new LateConnection();
		server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0));
		server.start();
		client = GrpcConnection.forHostAndPort("late-close", new HostAndPort("127.0.0.1", server.getPort()));
	}

	@AfterEach
	void tearDown() throws Exception {
		connection.closes.forEach(close -> close.complete(null));
		if (client != null) {
			client.close();
		}
		if (server != null) {
			server.close();
		}
	}

	private void awaitStarted(int closes) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (connection.closes.size() < closes && System.nanoTime() < deadline) {
			Thread.sleep(5);
		}
		assertEquals(closes, connection.closes.size());
	}

	@Test
	void closesThatAnswerLateHoldNoWorkerAndAreNotRefused() throws Exception {
		var api = client.getAsyncApi(RequestContext.batch());
		var answers = new ArrayList<CompletableFuture<Void>>();
		for (int id = 1; id <= CLOSES; id++) {
			answers.add(api.closeFailedUpdateAsync(id));
		}
		awaitStarted(CLOSES);
		assertEquals(0, connection.syncCalls.get());
		assertFalse(answers.stream().anyMatch(CompletableFuture::isDone));
		connection.closes.forEach(close -> close.complete(null));
		for (var answer : answers) {
			answer.get(30, TimeUnit.SECONDS);
		}
	}

	@Test
	void aRetryReachesTheClient() throws Exception {
		var answer = client.getAsyncApi(RequestContext.batch()).closeFailedUpdateAsync(5);
		awaitStarted(1);
		connection.closes.poll().completeExceptionally(new RocksDBRetryException());
		var error = assertThrows(ExecutionException.class, () -> answer.get(30, TimeUnit.SECONDS));
		assertInstanceOf(RocksDBRetryException.class, error.getCause());
	}

	/** A caller that went away cancels nothing: the close started must run to its end. */
	@Test
	void aCancelledCloseGoesOn() throws Exception {
		var answer = client.getAsyncApi(RequestContext.batch()).closeFailedUpdateAsync(5);
		awaitStarted(1);
		var close = connection.closes.peek();
		answer.cancel(true);
		Thread.sleep(200);
		assertFalse(close.isDone());
		close.complete(null);
	}

	@Test
	void shutdownWaitsForAStartedClose() throws Exception {
		client.getAsyncApi(RequestContext.batch()).closeFailedUpdateAsync(5);
		awaitStarted(1);
		var close = connection.closes.peek();
		var shutdown = CompletableFuture.runAsync(() -> {
			try {
				server.close();
			} catch (java.io.IOException ex) {
				throw new java.io.UncheckedIOException(ex);
			}
		});
		Thread.sleep(300);
		assertFalse(shutdown.isDone(), "the close still runs");
		close.complete(null);
		shutdown.get(30, TimeUnit.SECONDS);
		server = null;
	}
}
