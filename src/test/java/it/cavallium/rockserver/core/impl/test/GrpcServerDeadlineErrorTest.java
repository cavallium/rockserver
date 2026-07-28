package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Flux;

@Timeout(30)
class GrpcServerDeadlineErrorTest {

	@Test
	void expiredRequestContextFailsLocallyBeforeGrpcTransport() throws Exception {
		var backend = new CountingBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-expired-context",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var expired = new it.cavallium.rockserver.core.common.RequestContext(
						it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, 1L);
				var error = assertThrows(RocksDBException.class,
						() -> client.getAsyncApi(expired).getColumnIdAsync("never-sent"));
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
				assertEquals("Request deadline already expired", error.getMessage());
				assertEquals(0, backend.operations.get(),
						"an expired contextual call must fail before transport reaches the backend");
			}
		}
	}

	@Test
	void genericGrpcTransportDeadlineMapsToRocksDbDeadlineError() throws Exception {
		var backend = new GenericBlockingBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-generic-transport-deadline",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				try {
					var response = client.getAsyncApi(
							it.cavallium.rockserver.core.common.RequestContext.batch(Instant.now().plusSeconds(2)))
							.getColumnIdAsync("blocked");
					assertTrue(backend.entered.await(5, TimeUnit.SECONDS));

					var failure = assertThrows(ExecutionException.class,
							() -> response.get(5, TimeUnit.SECONDS));
					var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
					assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							rocksFailure.getErrorUniqueId());
					assertEquals("Deadline exceeded", rocksFailure.getMessage());
				} finally {
					backend.release.countDown();
				}
			}
		}
	}

	@Test
	void expiredCallerDeadlineDoesNotApplyToProtectedGrpcOperations() throws Exception {
		var backend = new ProtectedOperationBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-protected-deadline",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var expired = new it.cavallium.rockserver.core.common.RequestContext(
						it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, 1L);
				var api = client.getSyncApi(expired);
				assertTrue(api.closeTransaction(11L, false));
				assertEquals(it.cavallium.rockserver.core.common.RequestContext.NO_DEADLINE,
						backend.rollbackContextDeadline.get());
				api.closeFailedUpdate(12L);
				api.closeIterator(13L);
				api.cdcCommit("protected", 14L);
			}
		}
		assertTrue(backend.rollback.get());
		assertEquals(12L, backend.closedUpdate.get());
		assertEquals(13L, backend.closedIterator.get());
		assertEquals(14L, backend.committedCdcSequence.get());
	}

	@Test
	void protectedGrpcWorkloadProfilesAreRejectedByServer() throws Exception {
		var backend = new ProtectedOperationBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = io.grpc.ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			try {
				for (var profile : List.of(
						it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.CONTROL,
						it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.CDC,
						it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.PHYSICAL_MAINTENANCE)) {
					var request = it.cavallium.rockserver.core.common.api.proto.PutRequest.newBuilder()
							.setColumnId(1L)
							.setData(it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
									.addKeys(com.google.protobuf.ByteString.copyFrom(new byte[] {1}))
									.setValue(com.google.protobuf.ByteString.copyFrom(new byte[] {2})))
							.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
									.setProfile(profile)
									.setDeadlineEpochMillis(Long.MAX_VALUE))
							.build();
					var error = assertThrows(io.grpc.StatusRuntimeException.class,
							() -> it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc
									.newBlockingStub(channel)
									.put(request));
					assertEquals(io.grpc.Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
				}
			} finally {
				channel.shutdownNow();
			}
		}
	}

	@Test
	void readDeadlineExceededSurvivesARealGrpcServerRoundTrip() throws Exception {
		var backend = new DeadlineBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var error = assertThrows(RocksDBException.class, () -> {
					try (var range = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getRange(0,
							0,
							null,
							null,
							false,
							RequestType.allInRangeNoCache(),
							1_000)) {
						range.toList();
					}
				});

				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
				assertEquals("Deadline exceeded", error.getMessage());
			}
		}
	}

	@Test
	void cancelledUnaryCallDoesNotDropLateNativeDeadlineError() throws Exception {
		GrpcServer.clearLateReadDeadlineLogStatesForTesting();
		var backend = new BlockingDeadlineBackendConnection();
		var droppedError = new AtomicReference<Throwable>();
		var lateWarning = new AtomicReference<LogCall>();
		var lateWarningLogged = new CountDownLatch(1);
		Logger recordingLogger = recordingLogger(lateWarning, lateWarningLogged);
		Logger originalLogger = swapGrpcServerLogger(recordingLogger);
		Hooks.onErrorDropped(error -> droppedError.compareAndSet(null, error));
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-cancelled-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).reduceRangeAsync(0,
						0,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						1_000);

				assertTrue(backend.entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
				assertTrue(response.cancel(true));
				assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS),
						"the server did not observe RPC cancellation before native completion");
				backend.release.countDown();
				assertTrue(backend.finished.await(5, java.util.concurrent.TimeUnit.SECONDS));

				assertTrue(lateWarningLogged.await(5, TimeUnit.SECONDS),
						"the late native failure never reached the request-scoped logger");
				assertNull(droppedError.get(), "the server must not emit a terminal error after RPC cancellation");
				var warning = lateWarning.get();
				assertEquals("Late gRPC request failure after call termination: operation={}, requestType={}, "
						+ "request={}, errorType={}, grpcStatus={}, message={}", warning.message());
				assertEquals("reduceRangeFirstAndLast", warning.arguments().get(0));
				assertTrue(warning.arguments().get(1).toString().endsWith(".GetRangeRequest"));
				assertTrue(warning.arguments().get(2).toString().contains("timeoutMs=1000"));
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, warning.arguments().get(3));
				assertEquals(io.grpc.Status.Code.DEADLINE_EXCEEDED, warning.arguments().get(4));
				assertEquals("Deadline exceeded", warning.arguments().get(5));
			}
		} finally {
			backend.release.countDown();
			Hooks.resetOnErrorDropped();
			swapGrpcServerLogger(originalLogger);
		}
	}

	@Test
	void uncancelledUnaryDeadlineErrorRemainsVisible() throws Exception {
		var backend = new BlockingDeadlineBackendConnection();
		backend.release.countDown();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-visible-deadline-error",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var error = assertThrows(RocksDBException.class, () -> client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).reduceRange(0,
						0,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						1_000));

				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, error.getErrorUniqueId());
			}
		}
	}

	@Test
	void transportDeadlineClampsTheNativeReadBudget() throws Exception {
		var backend = new RecordingTimeoutBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = io.grpc.ManagedChannelBuilder
					.forAddress("127.0.0.1", server.getPort())
					.directExecutor()
					.usePlaintext()
					.build();
			try {
				var request = it.cavallium.rockserver.core.common.api.proto.GetRangeRequest.newBuilder()
						.setTimeoutMs(10_000)
						.setContext(wireBatchContext())
						.build();
				it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.newFutureStub(channel)
						.withDeadlineAfter(2, TimeUnit.SECONDS)
						.reduceRangeFirstAndLast(request)
						.get(5, TimeUnit.SECONDS);

				assertTrue(backend.observedTimeoutMs.get() >= 0);
				assertTrue(backend.observedTimeoutMs.get() <= 2_000,
						"the server must not grant the payload a fresh timeout beyond the gRPC deadline");
			} finally {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void transportDeadlineClampsTheNativeStreamingReadBudget() throws Exception {
		var backend = new RecordingTimeoutBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = io.grpc.ManagedChannelBuilder
					.forAddress("127.0.0.1", server.getPort())
					.directExecutor()
					.usePlaintext()
					.build();
			try {
				var request = it.cavallium.rockserver.core.common.api.proto.GetRangeRequest.newBuilder()
						.setTimeoutMs(10_000)
						.setContext(wireBatchContext())
						.build();
				it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc
						.newReactorStub(channel)
						.withDeadlineAfter(2, TimeUnit.SECONDS)
						.getAllInRange(request)
						.collectList()
						.block(java.time.Duration.ofSeconds(5));

				assertTrue(backend.observedTimeoutMs.get() >= 0);
				assertTrue(backend.observedTimeoutMs.get() <= 2_000,
						"the server must cap a streaming RocksDB read to the gRPC deadline");
			} finally {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
		}
	}

	@Test
	void clientReadTimeoutIsAlsoTheGrpcCallDeadline() throws Exception {
		var backend = new CancellableNeverCompletingBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-call-deadline",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).reduceRangeAsync(0,
						0,
						null,
						null,
						false,
						RequestType.firstAndLast(),
						1_000);
				assertTrue(backend.entered.await(5, TimeUnit.SECONDS));

				var failure = assertThrows(ExecutionException.class,
						() -> response.get(5, TimeUnit.SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						rocksFailure.getErrorUniqueId());
				assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS));
				assertTrue(backend.observedTimeoutMs.get() >= 0);
				assertTrue(backend.observedTimeoutMs.get() <= 1_000);
			}
		}
	}

	@Test
	void existsMultiUsesTheSameEndToEndReadDeadline() throws Exception {
		var backend = new CancellableNeverCompletingBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-exists-multi-deadline",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var response = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).existsMultiAsync(
						0, 0, List.of(new it.cavallium.rockserver.core.common.Keys()), 1_000);
				assertTrue(backend.entered.await(5, TimeUnit.SECONDS));

				var failure = assertThrows(ExecutionException.class,
						() -> response.get(5, TimeUnit.SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						rocksFailure.getErrorUniqueId());
				assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS));
				assertTrue(backend.observedTimeoutMs.get() >= 0);
				assertTrue(backend.observedTimeoutMs.get() <= 1_000);
			}
		}
	}

	@Test
	void requestContextDeadlineWinsOverLongerMethodTimeout() throws Exception {
		var backend = new CancellableNeverCompletingBackendConnection();
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-context-deadline",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var response = client.getAsyncApi(
						it.cavallium.rockserver.core.common.RequestContext.batch(
								Instant.now().plusMillis(500)))
						.reduceRangeAsync(0, 0, null, null, false,
								RequestType.firstAndLast(), 10_000);
				assertTrue(backend.entered.await(5, TimeUnit.SECONDS));

				var failure = assertThrows(ExecutionException.class,
						() -> response.get(5, TimeUnit.SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						rocksFailure.getErrorUniqueId());
				assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS));
				assertTrue(backend.observedTimeoutMs.get() >= 0);
				assertTrue(backend.observedTimeoutMs.get() <= 500,
						"the server-side operation budget must be capped by the caller context");
			}
		}
	}

	private static it.cavallium.rockserver.core.common.api.proto.RequestContext wireBatchContext() {
		return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
				.setDeadlineEpochMillis(it.cavallium.rockserver.core.common.RequestContext.NO_DEADLINE)
				.build();
	}

	private static final class DeadlineBackendConnection implements RocksDBConnection {

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?>) {
					return (RA) Flux.error(RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"Deadline exceeded"));
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://deadline-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}

	private static final class CountingBackendConnection implements RocksDBConnection {

		private final AtomicInteger operations = new AtomicInteger();
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			public long getColumnId(String name) {
				operations.incrementAndGet();
				return 1L;
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://counting-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public void close() {
		}
	}

	private static final class BlockingDeadlineBackendConnection implements RocksDBConnection {

		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch cancelObserved = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch finished = new CountDownLatch(1);
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (!(request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?>)) {
					throw new UnsupportedOperationException("Unexpected request: " + request);
				}
				var result = new NonCancellableFuture<>(cancelObserved);
				Thread.startVirtualThread(() -> {
					entered.countDown();
					while (true) {
						try {
							release.await();
							break;
						} catch (InterruptedException ignored) {
							// Model a native RocksDB call that does not return until its own deadline.
						}
					}
					finished.countDown();
					result.completeExceptionally(RocksDBException.of(
							RocksDBErrorType.READ_DEADLINE_EXCEEDED,
							"Deadline exceeded"));
				});
				return (RA) result;
			}
		};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};

		@Override
		public URI getUrl() {
			return URI.create("test://blocking-deadline-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}

	private static final class RecordingTimeoutBackendConnection implements RocksDBConnection {

		private final AtomicLong observedTimeoutMs = new AtomicLong(-1);
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> range) {
					observedTimeoutMs.set(range.timeoutMs());
					return (RA) CompletableFuture.completedFuture(
							new it.cavallium.rockserver.core.common.FirstAndLast<>(null, null));
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> range) {
					observedTimeoutMs.set(range.timeoutMs());
					return (RA) Flux.empty();
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://recording-timeout-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBSyncAPI() {};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}

	private static final class CancellableNeverCompletingBackendConnection implements RocksDBConnection {

		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch cancelObserved = new CountDownLatch(1);
		private final AtomicLong observedTimeoutMs = new AtomicLong(-1);
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				long timeoutMs;
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> range) {
					timeoutMs = range.timeoutMs();
				} else if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti existsMulti) {
					timeoutMs = existsMulti.timeoutMs();
				} else {
					throw new UnsupportedOperationException("Unexpected request: " + request);
				}
				observedTimeoutMs.set(timeoutMs);
				entered.countDown();
				return (RA) new CompletableFuture<>() {
					@Override
					public boolean cancel(boolean mayInterruptIfRunning) {
						cancelObserved.countDown();
						return super.cancel(mayInterruptIfRunning);
					}
				};
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://never-completing-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBSyncAPI() {};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return asyncApi;
		}

		@Override
		public void close() {
		}
	}

	private static final class ProtectedOperationBackendConnection implements RocksDBConnection {

		private final AtomicBoolean rollback = new AtomicBoolean();
		private final AtomicLong rollbackContextDeadline = new AtomicLong(-1L);
		private final AtomicLong closedUpdate = new AtomicLong(-1L);
		private final AtomicLong closedIterator = new AtomicLong(-1L);
		private final AtomicLong committedCdcSequence = new AtomicLong(-1L);

		@Override
		public URI getUrl() {
			return URI.create("test://protected-operation-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBSyncAPI() {
				@Override
				public boolean closeTransaction(long transactionId, boolean commit) {
					rollbackContextDeadline.set(context.deadlineEpochMillis());
					rollback.set(transactionId == 11L && !commit);
					return true;
				}

				@Override
				public void closeFailedUpdate(long updateId) {
					closedUpdate.set(updateId);
				}

				@Override
				public void closeIterator(long iteratorId) {
					closedIterator.set(iteratorId);
				}

				@Override
				public void cdcCommit(String id, long seq) {
					committedCdcSequence.set(seq);
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public void close() {
		}
	}

	private static final class GenericBlockingBackendConnection implements RocksDBConnection {

		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			public long getColumnId(String name) {
				entered.countDown();
				while (true) {
					try {
						release.await();
						return 1L;
					} catch (InterruptedException ignored) {
						// Model a native call that outlives transport cancellation.
					}
				}
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://generic-blocking-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public void close() {
		}
	}

	/** Models JNI work that keeps running after the RPC cancellation signal. */
	private static final class NonCancellableFuture<T> extends CompletableFuture<T> {

		private final CountDownLatch cancelObserved;

		private NonCancellableFuture(CountDownLatch cancelObserved) {
			this.cancelObserved = cancelObserved;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			cancelObserved.countDown();
			return false;
		}
	}

	private record LogCall(String message, List<Object> arguments) {
	}

	private static Logger recordingLogger(AtomicReference<LogCall> lateWarning,
			CountDownLatch lateWarningLogged) {
		return (Logger) Proxy.newProxyInstance(Logger.class.getClassLoader(), new Class<?>[] {Logger.class},
				(_, method, arguments) -> {
					if (method.getName().equals("getName")) {
						return GrpcServer.class.getName();
					}
					if (method.getName().startsWith("is") && method.getReturnType() == boolean.class) {
						return true;
					}
					if (method.getName().equals("warn") && arguments != null && arguments.length >= 2
							&& arguments[0] instanceof String message
							&& message.startsWith("Late gRPC request failure after call termination:")) {
						Object[] logArguments = arguments.length == 2 && arguments[1] instanceof Object[] values
								? values
								: Arrays.copyOfRange(arguments, 1, arguments.length);
						lateWarning.compareAndSet(null, new LogCall(message, List.of(logArguments)));
						lateWarningLogged.countDown();
					}
					if (method.getReturnType() == boolean.class) {
						return false;
					}
					return null;
				});
	}

	/** Swap the private static logger without changing production code just for this assertion. */
	private static Logger swapGrpcServerLogger(Logger replacement) throws ReflectiveOperationException {
		Class.forName(GrpcServer.class.getName(), true, GrpcServer.class.getClassLoader());
		Field loggerField = GrpcServer.class.getDeclaredField("LOG");
		Class<?> unsafeClass = Class.forName("sun.misc.Unsafe");
		Field theUnsafeField = unsafeClass.getDeclaredField("theUnsafe");
		theUnsafeField.setAccessible(true);
		Object unsafe = theUnsafeField.get(null);
		Object fieldBase = unsafeClass.getMethod("staticFieldBase", Field.class).invoke(unsafe, loggerField);
		long fieldOffset = (long) unsafeClass.getMethod("staticFieldOffset", Field.class).invoke(unsafe, loggerField);
		var getObject = unsafeClass.getMethod("getObjectVolatile", Object.class, long.class);
		var putObject = unsafeClass.getMethod("putObjectVolatile", Object.class, long.class, Object.class);
		Logger previous = (Logger) getObject.invoke(unsafe, fieldBase, fieldOffset);
		putObject.invoke(unsafe, fieldBase, fieldOffset, replacement);
		return previous;
	}
}
