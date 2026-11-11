package it.cavallium.rockserver.core.impl.test;

import io.grpc.ServerBuilder;
import io.grpc.Status;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ClientBuilder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.protobuf.Empty;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionResponse;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import java.io.IOException;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

class DeferredRockserverConnectionTest {

	private static final URI URL = URI.create("http://localhost:1234");

	private static RocksDBConnection deferred(URI url, Callable<RocksDBConnection> factory)
			throws Exception {
		return deferred(url, factory, System::nanoTime);
	}

	private static RocksDBConnection deferred(URI url, Callable<RocksDBConnection> factory,
			LongSupplier clock) throws Exception {
		var constructor = Class.forName("it.cavallium.rockserver.core.client.DeferredRockserverConnection")
				.getDeclaredConstructor(URI.class, Callable.class, LongSupplier.class);
		constructor.setAccessible(true);
		return (RocksDBConnection) constructor.newInstance(url, factory, clock);
	}

	@Test
	void builderDefersRemoteConstructionAndCloseWithoutUse() throws Exception {
		var builder = new ClientBuilder();
		builder.setDeferred(true);
		builder.setHttpAddress(new Utils.HostAndPort("localhost", 1));
		try (var client = builder.build()) {
			assertEquals(URI.create("http://localhost:1"), client.getUrl());
			assertNotNull(client.getAsyncApi(RequestContext.analytical()));
			builder.setHttpAddress(new Utils.HostAndPort("localhost", 2));
			assertEquals(URI.create("http://localhost:1"), client.getUrl());
		}
		builder.setHttpAddress(null);
		builder.setUnixSocket(UnixDomainSocketAddress.of("/tmp/rockserver-deferred-unused.sock"));
		try (var client = builder.build()) {
			assertEquals(URI.create("unix:///tmp/rockserver-deferred-unused.sock"), client.getUrl());
		}
	}

	@Test
	void builderValidatesCapabilitiesOnceAndReusesThenReopensRemoteConnection() throws Exception {
		var handshakes = new AtomicInteger();
		var operations = new AtomicInteger();
		var service = new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<CapabilitiesResponse> getCapabilities(
					CapabilitiesRequest request) {
				handshakes.incrementAndGet();
				return Mono.just(CapabilitiesResponse.newBuilder()
						.setWorkloadContractVersion(3).build());
			}

			@Override
			public Mono<GetColumnIdResponse> getColumnId(
					GetColumnIdRequest request) {
				assertTrue(handshakes.get() > 0);
				operations.incrementAndGet();
				return Mono.just(GetColumnIdResponse.newBuilder()
						.setColumnId(11).build());
			}
		};
		var server = ServerBuilder.forPort(0).addService(service).build().start();
		try {
			var builder = new ClientBuilder();
			builder.setDeferred(true);
			builder.setHttpAddress(new Utils.HostAndPort("127.0.0.1", server.getPort()));
			for (int opened = 0; opened < 2; opened++) {
				try (var client = builder.build()) {
					assertEquals(opened, handshakes.get());
					var api = client.getAsyncApi(RequestContext.analytical(Duration.ofSeconds(5)));
					assertEquals(11L, api.getColumnIdAsync("first").get(5, TimeUnit.SECONDS));
					assertEquals(11L, api.getColumnIdAsync("second").get(5, TimeUnit.SECONDS));
					assertEquals(opened + 1, handshakes.get());
				}
			}
			assertEquals(4, operations.get());
		} finally {
			server.shutdownNow();
			assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
		}
	}

	@Test
	void synchronousAccessOnNonBlockingThreadRejectsBeforeConstruction() throws Exception {
		var calls = new AtomicInteger();
		try (var deferred = deferred(URL, () -> { calls.incrementAndGet(); return client(); })) {
			assertThrows(IllegalStateException.class, () -> Mono.fromCallable(deferred::getCapabilities)
					.subscribeOn(Schedulers.parallel()).block());
			assertEquals(0, calls.get());
		}
	}

	private static RocksDBConnection client() {
		var client = mock(RocksDBConnection.class);
		when(client.getCapabilities()).thenReturn(RockserverCapabilities.CURRENT);
		var api = mock(RocksDBAsyncAPI.class);
		when(api.requestAsync(any())).thenAnswer(invocation -> CompletableFuture.completedFuture(11L));
		when(client.getAsyncApi(any())).thenReturn(api);
		return client;
	}

	@Test
	void viewConstructionDoesNotConnectAndFailedHandshakeRetries() throws Exception {
		var calls = new AtomicInteger();
		var client = client();
		try (var deferred = deferred(URL, () -> {
			if (calls.incrementAndGet() == 1) throw new IOException("offline");
			return client;
		})) {
			var api = deferred.getAsyncApi(RequestContext.latency(Duration.ofSeconds(5)));
			assertEquals(0, calls.get());
			assertThrows(Exception.class, () -> api.getColumnIdAsync("first").get(5, TimeUnit.SECONDS));
			assertEquals(11L, api.getColumnIdAsync("retry").get(5, TimeUnit.SECONDS));
			assertEquals(11L, api.getColumnIdAsync("cached").get(5, TimeUnit.SECONDS));
			assertEquals(2, calls.get());
		}
		verify(client).close();
	}

	@Test
	void incompatibleBackendIsClosedAndDoesNotPoisonRetry() throws Exception {
		var incompatible = client();
		when(incompatible.getCapabilities()).thenReturn(new RockserverCapabilities(2));
		var compatible = client();
		var calls = new AtomicInteger();
		try (var deferred = deferred(URL,
				() -> calls.incrementAndGet() == 1 ? incompatible : compatible)) {
			var api = deferred.getAsyncApi(RequestContext.analytical());
			assertThrows(Exception.class, () -> api.getColumnIdAsync("bad").get(5, TimeUnit.SECONDS));
			verify(incompatible).close();
			assertEquals(11L, api.getColumnIdAsync("good").get(5, TimeUnit.SECONDS));
		}
	}

	@Test
	void forwardsWalBarrierWithRemainingBudgetWithoutFullFlush() throws Exception {
		var clock = new AtomicLong();
		var client = client();
		var api = mock(RocksDBAsyncAPI.class);
		when(api.flushWalAsync()).thenReturn(CompletableFuture.completedFuture(null));
		when(client.getAsyncApi(any())).thenReturn(api);
		try (var deferred = deferred(URL, () -> {
			clock.set(Duration.ofSeconds(1).toNanos());
			return client;
		}, clock::get)) {
			deferred.getAsyncApi(RequestContext.batch(Duration.ofSeconds(2)))
					.flushWalAsync().get(5, TimeUnit.SECONDS);
			verify(api).flushWalAsync();
			verify(api, never()).flushAsync();
			verify(api, never()).requestAsync(any());
			verify(client).getAsyncApi(argThat(context -> context.profile()
					== WorkloadProfile.BATCH
					&& context.timeoutNanos() == Duration.ofSeconds(1).toNanos()));
		}
	}

	/** A deferred connection to a server with client sessions opens them through the real connection (CC-4.8). */
	@Test
	void forwardsClientSessionOpeningWithRemainingBudget() throws Exception {
		var clock = new AtomicLong();
		var client = client();
		when(client.openClientSessionAsync(any())).thenReturn(CompletableFuture.completedFuture(42L));
		try (var deferred = deferred(URL, () -> {
			clock.set(Duration.ofSeconds(1).toNanos());
			return client;
		}, clock::get)) {
			assertEquals(42L, deferred.openClientSessionAsync(RequestContext.batch(Duration.ofSeconds(3)))
					.get(5, TimeUnit.SECONDS));
			verify(client).openClientSessionAsync(argThat(context -> context.timeoutNanos() == Duration.ofSeconds(2).toNanos()));
		}
	}

	@Test
	void concurrentWaitersShareConstructionAndOneCancellationDoesNotDiscardIt() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var calls = new AtomicInteger();
		var requester = Thread.currentThread();
		try (var deferred = deferred(URL, () -> {
			assertNotSame(requester, Thread.currentThread());
			calls.incrementAndGet();
			entered.countDown();
			assertTrue(release.await(5, TimeUnit.SECONDS));
			return client();
		})) {
			var api = deferred.getAsyncApi(RequestContext.analytical());
			var first = api.getColumnIdAsync("first");
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			var second = api.getColumnIdAsync("second");
			first.cancel(true);
			release.countDown();
			assertEquals(11L, second.get(5, TimeUnit.SECONDS));
			assertEquals(1, calls.get());
		} finally {
			release.countDown();
		}
	}

	@Test
	void cancelledConstructionClosesLateResultAndCanRetry() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var disposed = new CountDownLatch(1);
		var late = client();
		doAnswer(invocation -> { disposed.countDown(); return null; }).when(late).close();
		var calls = new AtomicInteger();
		try (var deferred = deferred(URL, () -> {
			if (calls.incrementAndGet() > 1) return client();
			entered.countDown();
			assertTrue(release.await(5, TimeUnit.SECONDS));
			return late;
		})) {
			var api = deferred.getAsyncApi(RequestContext.analytical());
			var pending = api.getColumnIdAsync("cancel");
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			pending.cancel(true);
			var retry = api.getColumnIdAsync("retry-before-cleanup");
			assertFalse(retry.isDone());
			assertEquals(1, calls.get());
			release.countDown();
			assertTrue(disposed.await(5, TimeUnit.SECONDS));
			assertEquals(11L, retry.get(5, TimeUnit.SECONDS));
		} finally {
			release.countDown();
		}
	}

	@Test
	void timedOutConstructionClosesLateClientBeforeRetry() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var late = client();
		var calls = new AtomicInteger();
		try (var deferred = deferred(URL, () -> {
			if (calls.incrementAndGet() > 1) {
				verify(late).close();
				return client();
			}
			entered.countDown();
			assertTrue(release.await(5, TimeUnit.SECONDS));
			return late;
		})) {
			var pending = deferred.getAsyncApi(RequestContext.latency(Duration.ofSeconds(1)))
					.getColumnIdAsync("timeout");
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			var failure = assertThrows(ExecutionException.class,
					() -> pending.get(5, TimeUnit.SECONDS));
			assertNotSent(RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure);
			verify(late, never()).getAsyncApi(any());
			var retry = deferred.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("retry");
			assertFalse(retry.isDone());
			assertEquals(1, calls.get());
			release.countDown();
			assertEquals(11L, retry.get(5, TimeUnit.SECONDS));
			assertEquals(2, calls.get());
		} finally {
			release.countDown();
		}
	}

	@Test
	void connectionWaitConsumesFiniteSingleAndStreamBudgets() throws Exception {
		for (int operation = 0; operation < 2; operation++) {
			var clock = new AtomicLong();
			var client = client();
			var api = mock(RocksDBAsyncAPI.class);
			when(api.requestAsync(any())).thenAnswer(invocation -> invocation.getArgument(0)
					instanceof RocksDBAPICommand.RocksDBAPICommandStream<?>
					? Flux.empty() : CompletableFuture.completedFuture(11L));
			when(client.getAsyncApi(any())).thenReturn(api);
			try (var deferred = deferred(URL, () -> {
				clock.set(Duration.ofMillis(750).toNanos());
				return client;
			}, clock::get)) {
				perform(operation, deferred.getAsyncApi(RequestContext.analytical(Duration.ofSeconds(1))))
						.get(5, TimeUnit.SECONDS);
				verify(client).getAsyncApi(RequestContext.analytical(Duration.ofMillis(250)));
			}
		}
	}

	@Test
	void expiredSingleAndStreamBudgetsDoNotDispatch() throws Exception {
		for (int operation = 0; operation < 2; operation++) {
			int selectedOperation = operation;
			var clock = new AtomicLong();
			var client = client();
			try (var deferred = deferred(URL, () -> {
				clock.set(Duration.ofSeconds(2).toNanos());
				return client;
			}, clock::get)) {
				var failure = assertThrows(ExecutionException.class,
						() -> perform(selectedOperation, deferred.getAsyncApi(RequestContext.analytical(Duration.ofSeconds(1))))
								.get(5, TimeUnit.SECONDS));
				assertNotSent(RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure);
				verify(client, never()).getAsyncApi(any());
			}
		}
	}

	/**
	 * R48-4: a call that fails before any connection exists was never sent, and says so as the direct client does
	 * (GrpcConnection's expired deadline, SessionWrites' closed client): the deadline that came while connecting is a
	 * not-applied READ_DEADLINE_EXCEEDED, and a closed client a not-applied CLIENT_CLOSED (R61-7; SERVER_OVERLOADED before), for a single call, a
	 * stream, a WAL flush, a session opening and the synchronous API alike. A raw gRPC DEADLINE_EXCEEDED or an
	 * IllegalStateException read as an unknown outcome. A failure of the connection itself (the factory, the
	 * handshake) keeps its own exception: it is a transport failure, which callers classify by its gRPC status.
	 */
	@Test
	void failuresBeforeAnyConnectionAreFlaggedNotApplied() throws Exception {
		var closed = deferred(URL, DeferredRockserverConnectionTest::client);
		closed.close();
		for (int operation = 0; operation < 4; operation++) {
			int selected = operation;
			var failure = assertThrows(ExecutionException.class,
					() -> performAny(selected, closed).get(5, TimeUnit.SECONDS));
			assertClosedNotSent(failure);
		}
		assertClosedNotSent(
				assertThrows(RuntimeException.class, () -> closed.getSyncApi(RequestContext.analytical())));

		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var closing = deferred(URL, () -> {
			entered.countDown();
			release.await(5, TimeUnit.SECONDS);
			return client();
		});
		try {
			var pending = closing.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("pending");
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			closing.close();
			assertClosedNotSent(
					assertThrows(ExecutionException.class, () -> pending.get(5, TimeUnit.SECONDS)));
		} finally {
			release.countDown();
			closing.close();
		}

		for (int operation = 0; operation < 4; operation++) {
			var clock = new AtomicLong();
			int selected = operation;
			try (var late = deferred(URL, () -> {
				clock.set(Duration.ofSeconds(2).toNanos());
				return client();
			}, clock::get)) {
				var failure = assertThrows(ExecutionException.class,
						() -> performAny(selected, late, RequestContext.analytical(Duration.ofSeconds(1)))
								.get(5, TimeUnit.SECONDS));
				assertNotSent(RocksDBErrorType.READ_DEADLINE_EXCEEDED, failure);
			}
		}

		var offline = Status.UNAVAILABLE.withDescription("offline").asRuntimeException();
		try (var failing = deferred(URL, () -> { throw offline; })) {
			var failure = assertThrows(ExecutionException.class,
					() -> failing.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("c").get(5, TimeUnit.SECONDS));
			assertEquals(offline, failure.getCause(), "a transport failure keeps its own exception");
		}
	}

	private static CompletableFuture<?> performAny(int operation, RocksDBConnection deferred) {
		return performAny(operation, deferred, RequestContext.analytical());
	}

	/** A single call, a stream, a WAL flush, a session opening. */
	private static CompletableFuture<?> performAny(int operation, RocksDBConnection deferred, RequestContext context) {
		return switch (operation) {
			case 0, 1 -> perform(operation, deferred.getAsyncApi(context));
			case 2 -> deferred.getAsyncApi(context).flushWalAsync();
			case 3 -> deferred.openClientSessionAsync(context);
			default -> throw new AssertionError(operation);
		};
	}

	/**
	 * R61-7: a closed client is not an overload. A caller that waits out {@code SERVER_OVERLOADED} for as long as it
	 * takes would resend for ever a call that can never be sent; the failure has a type of its own, still flagged not
	 * applied.
	 */
	private static void assertClosedNotSent(Throwable failure) {
		var rocks = rocks(failure);
		assertEquals(RocksDBErrorType.CLIENT_CLOSED, rocks.getErrorUniqueId(), rocks.toString());
		assertTrue(rocks.isNotApplied(), "flagged not applied: " + rocks);
	}

	private static void assertNotSent(RocksDBErrorType type, Throwable failure) {
		var rocks = rocks(failure);
		assertEquals(type, rocks.getErrorUniqueId(), rocks.toString());
		assertTrue(rocks.isNotApplied(), "flagged not applied: " + rocks);
	}

	private static CompletableFuture<?> perform(int operation, RocksDBAsyncAPI api) {
		return switch (operation) {
			case 0 -> api.getColumnIdAsync("column");
			case 1 -> Flux.from(api.requestAsync(new RocksDBAPICommand
					.RocksDBAPICommandStream.ScanRaw(1, 0, 1))).collectList().toFuture();
			default -> throw new AssertionError(operation);
		};
	}

	@Test
	void closeRejectsPendingAndLateConstruction() throws Exception {
		var entered = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var disposed = new CountDownLatch(1);
		var late = client();
		doAnswer(invocation -> { disposed.countDown(); return null; }).when(late).close();
		var deferred = deferred(URL, () -> {
			entered.countDown();
			// A factory that finishes although it is interrupted: its result reaches a closed connection.
			boolean interrupted = false;
			while (true) {
				try {
					assertTrue(release.await(5, TimeUnit.SECONDS));
					break;
				} catch (InterruptedException e) {
					interrupted = true;
				}
			}
			if (interrupted) Thread.currentThread().interrupt();
			return late;
		});
		try {
			var api = deferred.getAsyncApi(RequestContext.analytical());
			var pending = api.getColumnIdAsync("pending");
			assertTrue(entered.await(5, TimeUnit.SECONDS));
			deferred.close();
			assertThrows(Exception.class, () -> pending.get(5, TimeUnit.SECONDS));
			release.countDown();
			assertTrue(disposed.await(5, TimeUnit.SECONDS));
			assertThrows(Exception.class, () -> api.getColumnIdAsync("closed").get(5, TimeUnit.SECONDS));
		} finally {
			release.countDown();
			deferred.close();
		}
	}

	/** A handshake can wait for 30 s and more, holding a thread and connecting: close ends it. */
	@Test
	void closeInterruptsConstructionInProgress() throws Exception {
		var entered = new CountDownLatch(1);
		var interrupted = new CountDownLatch(1);
		var deferred = deferred(URL, () -> {
			entered.countDown();
			try {
				Thread.sleep(Duration.ofSeconds(30));
			} catch (InterruptedException e) {
				interrupted.countDown();
				throw e;
			}
			return client();
		});
		var pending = deferred.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("pending");
		assertTrue(entered.await(5, TimeUnit.SECONDS));
		deferred.close();
		assertThrows(Exception.class, () -> pending.get(5, TimeUnit.SECONDS));
		assertTrue(interrupted.await(5, TimeUnit.SECONDS));
	}

	/**
	 * R47-2: through the deferred client Yotsuba builds by default, a numbered write cancelled after a copy was sent
	 * ends OUTCOME_UNKNOWN and its cancel returns false, as without the wrapper (SessionWrites' CallResult): the copy
	 * may have been applied. The wrapper's future ({@code Mono.toFuture()}) answered true and ended cancelled, which
	 * reads "not applied", so a caller that sends it again can apply a merge twice.
	 */
	@Test
	void numberedWriteCancelledAfterACopyWasSentEndsUnknownThroughTheBuilder() throws Exception {
		var sent = new CountDownLatch(1);
		var serverCancelled = new CountDownLatch(1);
		var sessions = new AtomicInteger();
		var service = new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
				return Mono.just(CapabilitiesResponse.newBuilder().setWorkloadContractVersion(3)
						.setClientSessions(true).build());
			}

			@Override
			public Mono<OpenClientSessionResponse> openClientSession(OpenClientSessionRequest request) {
				sessions.incrementAndGet();
				return Mono.just(OpenClientSessionResponse.newBuilder().setSessionId(5).build());
			}

			@Override
			public Mono<Empty> put(PutRequest request) {
				assertTrue(request.getContext().hasRequestId(), "the write is numbered");
				sent.countDown();
				return Mono.<Empty>never().doOnCancel(serverCancelled::countDown);
			}
		};
		var server = ServerBuilder.forPort(0).addService(service).build().start();
		try {
			var builder = new ClientBuilder();
			builder.setDeferred(true);
			builder.setHttpAddress(new Utils.HostAndPort("127.0.0.1", server.getPort()));
			try (var client = builder.build()) {
				var write = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(30)))
						.putAsync(0, 1, new Keys(Buf.wrap(new byte[] {1})), Buf.wrap(new byte[] {2}), RequestType.none());
				assertTrue(sent.await(5, TimeUnit.SECONDS), "a copy was sent");
				assertFalse(write.cancel(true), "not cancelled: a copy was sent, so it may have been applied");
				assertFalse(write.isCancelled());
				var error = assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
				assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
				assertTrue(serverCancelled.await(5, TimeUnit.SECONDS), "the copy in flight is cancelled");
				assertEquals(1, sessions.get());
			}
		} finally {
			server.shutdownNow();
			assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
		}
	}

	/** R47-2: through the builder, a cancel after the answer returns false and the answer comes through. */
	@Test
	void cancelAfterTheAnswerReturnsFalseThroughTheBuilder() throws Exception {
		var service = new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
				return Mono.just(CapabilitiesResponse.newBuilder().setWorkloadContractVersion(3).build());
			}

			@Override
			public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
				return Mono.just(GetColumnIdResponse.newBuilder().setColumnId(11).build());
			}
		};
		var server = ServerBuilder.forPort(0).addService(service).build().start();
		try {
			var builder = new ClientBuilder();
			builder.setDeferred(true);
			builder.setHttpAddress(new Utils.HostAndPort("127.0.0.1", server.getPort()));
			try (var client = builder.build()) {
				var answer = client.getAsyncApi(RequestContext.latency(Duration.ofSeconds(5))).getColumnIdAsync("c");
				assertEquals(11L, answer.get(5, TimeUnit.SECONDS));
				assertFalse(answer.cancel(true));
				assertFalse(answer.isCancelled());
				assertEquals(11L, answer.get());
			}
		} finally {
			server.shutdownNow();
			assertTrue(server.awaitTermination(5, TimeUnit.SECONDS));
		}
	}

	/**
	 * R47-2: the inner call has ended but its future has not completed yet (R46-4's CancelPropagatingFuture whose source
	 * answered, a numbered write that ends unknown): its cancel returns false, and so does the wrapper's, whose
	 * outcome is the inner one, never "cancelled".
	 */
	@Test
	void cancelRefusedByTheCallReturnsFalseAndKeepsItsOutcome() throws Exception {
		for (var outcome : new Object[] {"answer", RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost")}) {
			var inner = new CompletableFuture<Object>() {
				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					return false;
				}
			};
			var client = client();
			var api = mock(RocksDBAsyncAPI.class);
			when(api.requestAsync(any())).thenAnswer(invocation -> inner);
			when(client.getAsyncApi(any())).thenReturn(api);
			try (var deferred = deferred(URL, () -> client)) {
				CompletableFuture<?> call = deferred.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("c");
				verify(api, timeout(5_000)).requestAsync(any());
				assertFalse(call.cancel(true), "the call refused the cancel");
				assertFalse(call.isCancelled());
				if (outcome instanceof Throwable failure) {
					inner.completeExceptionally(failure);
					var error = assertThrows(ExecutionException.class, () -> call.get(5, TimeUnit.SECONDS));
					assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
				} else {
					inner.complete(outcome);
					assertEquals(outcome, call.get(5, TimeUnit.SECONDS));
				}
			}
		}
	}

	/**
	 * R47-2: a cancel that reaches the inner call cancels it and the wrapper; one before the connection exists cancels
	 * the wrapper and sends nothing. The same for a single call, a WAL flush and a session opening.
	 */
	@Test
	void cancelReachesTheCallOrStopsItBeforeItIsSent() throws Exception {
		for (int operation = 0; operation < 3; operation++) {
			var inner = new CompletableFuture<Object>();
			var client = client();
			var api = mock(RocksDBAsyncAPI.class);
			when(api.requestAsync(any())).thenAnswer(invocation -> inner);
			doReturn(inner).when(api).flushWalAsync();
			doReturn(inner).when(client).openClientSessionAsync(any());
			when(client.getAsyncApi(any())).thenReturn(api);
			try (var deferred = deferred(URL, () -> client)) {
				var call = start(operation, deferred);
				awaitStarted(operation, client, api);
				assertTrue(call.cancel(true), "operation " + operation);
				assertTrue(call.isCancelled());
				assertTrue(inner.isCancelled(), "the cancel reached the call");
				assertThrows(CancellationException.class, call::get);
			}

			var release = new CountDownLatch(1);
			var late = client();
			var lateApi = mock(RocksDBAsyncAPI.class);
			when(late.getAsyncApi(any())).thenReturn(lateApi);
			try (var deferred = deferred(URL, () -> {
				assertTrue(release.await(5, TimeUnit.SECONDS));
				return late;
			})) {
				var call = start(operation, deferred);
				assertTrue(call.cancel(true));
				assertTrue(call.isCancelled());
				release.countDown();
				Thread.sleep(200);
				verify(lateApi, never()).requestAsync(any());
				verify(lateApi, never()).flushWalAsync();
				verify(late, never()).openClientSessionAsync(any());
			} finally {
				release.countDown();
			}
		}
	}

	/**
	 * Review of R47, 1: a cancel made on the thread that is making the call, while it is being made (a dependent stage
	 * of another future that the call completes synchronously), is not a cancel "before the call": the call is made,
	 * the cancel is passed to it once it exists, and this future ends as the call does. A call that refuses the cancel
	 * (a numbered write that sent a copy) keeps its outcome; one that accepts it ends cancelled.
	 */
	@Test
	void cancelWhileTheCallIsBeingMadeOnTheSameThreadReachesTheCall() throws Exception {
		for (boolean refuses : new boolean[] {true, false}) {
			var inner = new CompletableFuture<Object>() {
				@Override
				public boolean cancel(boolean mayInterruptIfRunning) {
					return !refuses && super.cancel(mayInterruptIfRunning);
				}
			};
			var holder = new java.util.concurrent.atomic.AtomicReference<CompletableFuture<?>>();
			var nestedCancel = new java.util.concurrent.atomic.AtomicReference<Boolean>();
			var client = client();
			var api = mock(RocksDBAsyncAPI.class);
			when(api.requestAsync(any())).thenAnswer(invocation -> {
				nestedCancel.set(holder.get().cancel(true));
				return inner;
			});
			when(client.getAsyncApi(any())).thenReturn(api);
			var release = new CountDownLatch(1);
			try (var deferred = deferred(URL, () -> {
				assertTrue(release.await(5, TimeUnit.SECONDS));
				return client;
			})) {
				CompletableFuture<?> call = deferred.getAsyncApi(RequestContext.analytical()).getColumnIdAsync("c");
				holder.set(call);
				release.countDown();
				verify(api, timeout(5_000)).requestAsync(any());
				assertEquals(Boolean.FALSE, nestedCancel.get(), "the call was being made: the cancel cannot be granted yet");
				if (refuses) {
					assertFalse(inner.isCancelled());
					assertFalse(call.isCancelled(), "the call refused the cancel");
					inner.completeExceptionally(RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost"));
					var error = assertThrows(ExecutionException.class, () -> call.get(5, TimeUnit.SECONDS));
					assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, rocks(error).getErrorUniqueId());
				} else {
					assertTrue(inner.isCancelled(), "the cancel reached the call made after it");
					assertThrows(CancellationException.class, () -> call.get(5, TimeUnit.SECONDS));
				}
			} finally {
				release.countDown();
			}
		}
	}

	private static CompletableFuture<?> start(int operation, RocksDBConnection deferred) {
		var api = deferred.getAsyncApi(RequestContext.analytical());
		return switch (operation) {
			case 0 -> api.getColumnIdAsync("c");
			case 1 -> api.flushWalAsync();
			case 2 -> deferred.openClientSessionAsync(RequestContext.analytical());
			default -> throw new AssertionError(operation);
		};
	}

	private static void awaitStarted(int operation, RocksDBConnection client, RocksDBAsyncAPI api) {
		switch (operation) {
			case 0 -> verify(api, timeout(5_000)).requestAsync(any());
			case 1 -> verify(api, timeout(5_000)).flushWalAsync();
			case 2 -> verify(client, timeout(5_000)).openClientSessionAsync(any());
			default -> throw new AssertionError(operation);
		}
	}

	private static RocksDBException rocks(Throwable error) {
		Throwable cause = error;
		while (cause.getCause() != null && !(cause instanceof RocksDBException)) {
			cause = cause.getCause();
		}
		return assertInstanceOf(RocksDBException.class, cause);
	}
}
