package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ClientBuilder;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class DeferredRockserverConnectionTest {

	private static final URI URL = URI.create("http://localhost:1234");

	private static RocksDBConnection deferred(URI url, java.util.concurrent.Callable<RocksDBConnection> factory)
			throws Exception {
		return deferred(url, factory, System::nanoTime);
	}

	private static RocksDBConnection deferred(URI url, java.util.concurrent.Callable<RocksDBConnection> factory,
			java.util.function.LongSupplier clock) throws Exception {
		var constructor = Class.forName("it.cavallium.rockserver.core.client.DeferredRockserverConnection")
				.getDeclaredConstructor(URI.class, java.util.concurrent.Callable.class, java.util.function.LongSupplier.class);
		constructor.setAccessible(true);
		return (RocksDBConnection) constructor.newInstance(url, factory, clock);
	}

	@Test
	void builderDefersRemoteConstructionAndCloseWithoutUse() throws Exception {
		var builder = new ClientBuilder();
		builder.setDeferred(true);
		builder.setHttpAddress(new it.cavallium.rockserver.core.common.Utils.HostAndPort("localhost", 1));
		try (var client = builder.build()) {
			assertEquals(URI.create("http://localhost:1"), client.getUrl());
			assertNotNull(client.getAsyncApi(RequestContext.analytical()));
			builder.setHttpAddress(new it.cavallium.rockserver.core.common.Utils.HostAndPort("localhost", 2));
			assertEquals(URI.create("http://localhost:1"), client.getUrl());
		}
		builder.setHttpAddress(null);
		builder.setUnixSocket(java.net.UnixDomainSocketAddress.of("/tmp/rockserver-deferred-unused.sock"));
		try (var client = builder.build()) {
			assertEquals(URI.create("unix:///tmp/rockserver-deferred-unused.sock"), client.getUrl());
		}
	}

	@Test
	void builderValidatesCapabilitiesOnceAndReusesThenReopensRemoteConnection() throws Exception {
		var handshakes = new AtomicInteger();
		var operations = new AtomicInteger();
		var service = new it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse> getCapabilities(
					it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest request) {
				handshakes.incrementAndGet();
				return Mono.just(it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse.newBuilder()
						.setWorkloadContractVersion(3).build());
			}

			@Override
			public Mono<it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse> getColumnId(
					it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest request) {
				assertTrue(handshakes.get() > 0);
				operations.incrementAndGet();
				return Mono.just(it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse.newBuilder()
						.setColumnId(11).build());
			}
		};
		var server = io.grpc.ServerBuilder.forPort(0).addService(service).build().start();
		try {
			var builder = new ClientBuilder();
			builder.setDeferred(true);
			builder.setHttpAddress(new it.cavallium.rockserver.core.common.Utils.HostAndPort("127.0.0.1", server.getPort()));
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
					.subscribeOn(reactor.core.scheduler.Schedulers.parallel()).block());
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
					== it.cavallium.rockserver.core.common.WorkloadProfile.BATCH
					&& context.timeoutNanos() == Duration.ofSeconds(1).toNanos()));
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
			var failure = assertThrows(java.util.concurrent.ExecutionException.class,
					() -> pending.get(5, TimeUnit.SECONDS));
			assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED,
					io.grpc.Status.fromThrowable(failure.getCause()).getCode());
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
	void connectionWaitConsumesFiniteSingleStreamAndCdcBudgets() throws Exception {
		for (int operation = 0; operation < 3; operation++) {
			var clock = new AtomicLong();
			var client = client();
			var api = mock(RocksDBAsyncAPI.class);
			when(api.requestAsync(any())).thenAnswer(invocation -> invocation.getArgument(0)
					instanceof it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream<?>
					? Flux.empty() : CompletableFuture.completedFuture(11L));
			when(api.cdcPollBatchAsync(anyString(), any(), anyLong())).thenReturn(Mono.empty());
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
	void expiredSingleStreamAndCdcBudgetsDoNotDispatch() throws Exception {
		for (int operation = 0; operation < 3; operation++) {
			int selectedOperation = operation;
			var clock = new AtomicLong();
			var client = client();
			try (var deferred = deferred(URL, () -> {
				clock.set(Duration.ofSeconds(2).toNanos());
				return client;
			}, clock::get)) {
				var failure = assertThrows(java.util.concurrent.ExecutionException.class,
						() -> perform(selectedOperation, deferred.getAsyncApi(RequestContext.analytical(Duration.ofSeconds(1))))
								.get(5, TimeUnit.SECONDS));
				assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, io.grpc.Status.fromThrowable(failure.getCause()).getCode());
				verify(client, never()).getAsyncApi(any());
			}
		}
	}

	private static CompletableFuture<?> perform(int operation, RocksDBAsyncAPI api) {
		return switch (operation) {
			case 0 -> api.getColumnIdAsync("column");
			case 1 -> Flux.from(api.requestAsync(new it.cavallium.rockserver.core.common.RocksDBAPICommand
					.RocksDBAPICommandStream.ScanRaw(1, 0, 1))).collectList().toFuture();
			case 2 -> api.cdcPollBatchAsync("cdc", null, 1).toFuture();
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
			assertTrue(release.await(5, TimeUnit.SECONDS));
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
}
