package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.BindableService;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle.Get;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** {@code closeFailedUpdate} against a connection that answers late, as a replicated one does for a chained update. */
class GrpcProxyCloseTest {

	/** More than the workers and the queue of the CONTROL scheduler of a proxy server hold together. */
	private static final int CLOSES = 2_000;

	private static final String CLOSE_FAILED_UPDATE = "closeFailedUpdate";
	private static final String CLOSE_ABANDONED_UPDATE = "closeAbandonedUpdate";
	private static final long UPDATE_ID = 7;

	/** Holds every close and every {@code getForUpdate} until a test answers it. */
	private static final class LateConnection implements RocksDBConnection, RocksDBAPI {

		final ConcurrentLinkedQueue<CompletableFuture<Void>> closes = new ConcurrentLinkedQueue<>();
		final ConcurrentLinkedQueue<Long> closedIds = new ConcurrentLinkedQueue<>();
		final ConcurrentLinkedQueue<CompletableFuture<UpdateContext<Buf>>> updates = new ConcurrentLinkedQueue<>();
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
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
			if (request instanceof Get<?>) {
				// As a replicated node does: opening an update waits for the key on a worker.
				var update = new CompletableFuture<UpdateContext<Buf>>();
				updates.add(update);
				return (RS) update.join();
			}
			syncCalls.incrementAndGet();
			throw new IllegalStateException("A close must not wait on a worker");
		}

		@Override
		@SuppressWarnings("unchecked")
		public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
			if (!(request instanceof CloseFailedUpdate failedUpdate)) {
				throw new IllegalStateException("Unexpected " + request);
			}
			closedIds.add(failedUpdate.updateId());
			var close = new CompletableFuture<Void>();
			closes.add(close);
			return (RA) close;
		}
	}

	private LateConnection connection;
	private GrpcServer server;
	private GrpcConnection client;
	private io.grpc.Server observedServer;
	private GrpcConnection observedClient;

	@BeforeEach
	void setUp() throws Exception {
		connection = new LateConnection();
		server = new GrpcServer(connection, new InetSocketAddress("127.0.0.1", 0));
		server.start();
		client = GrpcConnection.forHostAndPort("late-close", new HostAndPort("127.0.0.1", server.getPort()));
	}

	@AfterEach
	void tearDown() throws Exception {
		connection.updates.forEach(update -> update.complete(new UpdateContext<>(null, UPDATE_ID)));
		connection.closes.forEach(close -> close.complete(null));
		if (observedClient != null) {
			observedClient.close();
		}
		if (observedServer != null) {
			observedServer.shutdownNow();
			assertTrue(observedServer.awaitTermination(5, TimeUnit.SECONDS));
		}
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

	private static void await(BooleanSupplier condition, String description) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5);
		}
		assertTrue(condition.getAsBoolean(), description);
	}

	/** A close whose caller went away, and the server knows. */
	private CompletableFuture<Void> closeOfAGoneCaller() throws Exception {
		long cancelled = server.getCancelledMustCompleteOperationCountForTesting();
		int started = connection.closes.size();
		var answer = client.getAsyncApi(RequestContext.batch()).closeFailedUpdateAsync(5);
		awaitStarted(started + 1);
		CompletableFuture<Void> close = null;
		for (var candidate : connection.closes) {
			close = candidate;
		}
		answer.cancel(true);
		await(() -> server.getCancelledMustCompleteOperationCountForTesting() == cancelled + 1,
				"the server did not see the cancellation");
		return close;
	}

	/**
	 * A {@code getForUpdate} that the connection answers once its caller is gone, and the server knows. The cancellation
	 * of a request that is not a must-complete one has no counter, so this goes through a second server that sees it.
	 */
	private void openUpdateOfAGoneCaller() throws Exception {
		var field = GrpcServer.class.getDeclaredField("grpc");
		field.setAccessible(true);
		var cancelled = new CountDownLatch(1);
		observedServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
				.addService((BindableService) field.get(server))
				.intercept(new ServerInterceptor() {
					@Override
					public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
							Metadata headers, ServerCallHandler<ReqT, RespT> next) {
						return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(
								next.startCall(call, headers)) {
							@Override
							public void onCancel() {
								super.onCancel();
								cancelled.countDown();
							}
						};
					}
				}).build().start();
		observedClient = GrpcConnection.forHostAndPort("late-update",
				new HostAndPort("127.0.0.1", observedServer.getPort()));
		var answer = observedClient.getAsyncApi(RequestContext.batch())
				.getAsync(0, 1, new Keys(Buf.wrap(new byte[] {1})), RequestType.<Buf>forUpdate());
		await(() -> !connection.updates.isEmpty(), "the update did not reach the connection");
		answer.cancel(true);
		assertTrue(cancelled.await(30, TimeUnit.SECONDS), "the server did not see the cancellation");
		connection.updates.poll().complete(new UpdateContext<>(null, UPDATE_ID));
		awaitStarted(1);
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

	/** A retry tells a caller to run its update again: with no caller, it is no failure. */
	@Test
	void aRetryOfACloseWithoutACallerIsNoFailure() throws Exception {
		closeOfAGoneCaller().completeExceptionally(new RocksDBRetryException());
		assertEquals(0, server.getLateProtectedOperationFailureCountForTesting(CLOSE_FAILED_UPDATE));
		closeOfAGoneCaller().completeExceptionally(new IllegalStateException("broken"));
		assertEquals(1, server.getLateProtectedOperationFailureCountForTesting(CLOSE_FAILED_UPDATE));
	}

	/** No caller can learn the update's id, so the close of it takes no worker either, however late it answers. */
	@Test
	void anUpdateOpenedForAGoneCallerIsClosedWithoutAWorker() throws Exception {
		openUpdateOfAGoneCaller();
		assertEquals(UPDATE_ID, connection.closedIds.peek());
		assertEquals(0, connection.syncCalls.get());
		assertFalse(connection.closes.peek().isDone());
		connection.closes.peek().complete(null);
		assertEquals(0, server.getLateProtectedOperationFailureCountForTesting(CLOSE_ABANDONED_UPDATE));
	}

	@Test
	void aRetryOfTheCloseOfAnUpdateNobodyHeardOfIsNoFailure() throws Exception {
		openUpdateOfAGoneCaller();
		connection.closes.peek().completeExceptionally(new RocksDBRetryException());
		assertEquals(0, server.getLateProtectedOperationFailureCountForTesting(CLOSE_ABANDONED_UPDATE));
	}

	@Test
	void anErrorOfTheCloseOfAnUpdateNobodyHeardOfIsAFailure() throws Exception {
		openUpdateOfAGoneCaller();
		connection.closes.peek().completeExceptionally(new IllegalStateException("broken"));
		assertEquals(1, server.getLateProtectedOperationFailureCountForTesting(CLOSE_ABANDONED_UPDATE));
	}

	/**
	 * The caller is gone, so gRPC has no call left to wait for: only the server's own count of what it must finish keeps
	 * shutdown from going on while the close still runs.
	 */
	@Test
	void shutdownWaitsForAStartedCloseWhoseCallerIsGone() throws Exception {
		var close = closeOfAGoneCaller();
		assertEquals(1, server.getAcceptedMustCompleteOperationCountForTesting());
		var shutdown = CompletableFuture.runAsync(() -> {
			try {
				server.close();
			} catch (java.io.IOException ex) {
				throw new java.io.UncheckedIOException(ex);
			}
		});
		assertThrows(TimeoutException.class, () -> shutdown.get(2, TimeUnit.SECONDS), "the close still runs");
		close.complete(null);
		assertNull(shutdown.get(30, TimeUnit.SECONDS));
		server = null;
	}
}
