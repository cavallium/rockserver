package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.SubsequentRequest;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.test.StepVerifier;

@Timeout(60)
class GrpcIteratorLeaseTest {

	private static final int ITERATOR_COUNT = 48;

	@TempDir
	Path tempDir;

	@Test
	void expiredAndFailedIteratorOperationsDoNotRetainServerLeases() throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-iterator-leases", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-iterator-leases",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var api = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				long columnId = api.createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				List<Long> iteratorIds = new ArrayList<>(ITERATOR_COUNT);
				try {
					for (int i = 0; i < ITERATOR_COUNT; i++) {
						iteratorIds.add(api.openIterator(0, columnId, new Keys(), null, false, java.time.Duration.ofMillis( 25)));
					}

					assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
							"idle or abandoned iterators must not be retained as operation leases");

					Thread.sleep(50);
					for (long iteratorId : iteratorIds) {
						assertThrows(RocksDBException.class,
								() -> api.subsequent(iteratorId, 0, 1, RequestType.exists()));
						assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
								"an error must release its transient operation lease");
					}

					for (int i = 0; i < ITERATOR_COUNT; i++) {
						long missingIteratorId = Long.MAX_VALUE - i;
						assertThrows(RocksDBException.class,
								() -> api.subsequent(missingIteratorId, 0, 1, RequestType.exists()));
					}
					assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
							"failed operations on unknown iterators must not accumulate leases");
					assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
					assertEquals(0L, embedded.getInternalDB().getPendingOpsCount());
				} finally {
					for (long iteratorId : iteratorIds) {
						api.closeIterator(iteratorId);
					}
				}
			}
		}
	}

	@Test
	void cancellationKeepsLeaseUntilRunningNativeIteratorCallReturns() throws Exception {
		var backend = new BlockingSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-running-iterator-lease",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var asyncApi = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				var syncApi = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				long iteratorId = syncApi.openIterator(0, 0, new Keys(), null, false, java.time.Duration.ofMillis( 30_000));
				var running = asyncApi.subsequentAsync(iteratorId, 4_097, 1, RequestType.exists());

				try {
					assertTrue(backend.entered.await(5, TimeUnit.SECONDS));
					assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting());
					assertTrue(running.cancel(true));
					assertTrue(running.isCancelled(),
							"cancelling the client future establishes RPC cancellation");

					var concurrent = assertThrows(RocksDBException.class,
							() -> syncApi.subsequent(iteratorId, 0, 1, RequestType.exists()));
					assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
							concurrent.getErrorUniqueId());
					assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting(),
							"RPC cancellation must not release a lease while JNI is still running");
				} finally {
					backend.release.countDown();
				}

				assertTrue(backend.finished.await(5, TimeUnit.SECONDS));
				awaitLeaseCount(server, 0);
				assertTrue(syncApi.subsequent(iteratorId, 0, 1, RequestType.exists()),
						"the iterator must accept a new operation after the cancelled task really terminates");
				awaitLeaseCount(server, 0);
			}
		}
	}

	@Test
	void longGrpcMultiContinuationStreamsPagesFromOneCooperativeLogicalRequest() throws Exception {
		var backend = new RecordingAsyncSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			backend.queueForegroundAfterFirstMulti.set(true);
			var scheduler = backend.scheduler;
			var before = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			var observed = new AtomicInteger();
			var leaseCountAtCompletion = new AtomicInteger(-1);
			var request = SubsequentRequest.newBuilder()
					.setIterationId(RecordingAsyncSubsequentConnection.ITERATOR_ID)
					.setSkipCount(4_097L)
					.setTakeCount(128L)
					.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
							.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
							.setWorkloadContractVersion(3)
				.setTimeoutNanos(Long.MAX_VALUE))
					.build();

			var values = grpcService(server).subsequentMultiGet(request)
					.doOnNext(value -> assertEquals((byte) observed.getAndIncrement(),
							value.getValue().byteAt(0)))
					.doOnComplete(() -> leaseCountAtCompletion.set(
							server.getActiveIteratorOperationLeaseCountForTesting()));
			StepVerifier.create(values, 0)
					.then(() -> {
						assertEquals(0, backend.syncSubsequentCalls.get(),
								"zero downstream demand must park before native iterator work");
						assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting());
					})
					.thenRequest(64)
					.expectNextCount(64)
					.then(() -> {
						backend.awaitForeground();
						assertEquals(List.of(4_096L, 1L), backend.syncExistsTakeCounts);
						assertEquals(List.of(64L), backend.syncMultiTakeCounts,
								"the second native page must wait for downstream demand");
					})
					.thenRequest(64)
					.expectNextCount(64)
					.expectComplete()
					.verify(Duration.ofSeconds(5));

			awaitLeaseCount(server, 0);
			var after = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(128, observed.get());
			assertEquals(0, leaseCountAtCompletion.get(),
					"the iterator lease must be released before stream completion is published");
			assertEquals(List.of(4_096L, 1L), backend.syncExistsTakeCounts);
			assertEquals(List.of(64L, 64L), backend.syncMultiTakeCounts);
			assertEquals(4, backend.syncSubsequentCalls.get());
			assertEquals(0, backend.asyncSubsequentCalls.get(),
					"gRPC must not materialize the full iterator result through a future");
			assertFalse(backend.secondPageBeforeForeground.get(),
					"foreground work must run before the second native value page");
			assertEquals(2L, after.acceptedTasks() - before.acceptedTasks(),
					"one retained request plus one foreground task must be admitted");
			assertEquals(2L, after.startedTasks() - before.startedTasks());
			assertEquals(2L, after.completedTasks() - before.completedTasks());
			assertEquals(1.0d, backend.counter(
					"rockserver.workload.admission", "batch", "range_page", "result", "accepted"));
			assertEquals(1L, backend.timerCount(
					"rockserver.workload.queue.wait", "batch", "range_page"));
			assertEquals(1L, backend.timerCount(
					"rockserver.workload.execution", "batch", "range_page"));
			assertTrue(backend.counter(
					"rockserver.workload.quantums", "batch", "range_page") >= 2.0d,
					"demand parking must redispatch the same logical scheduler node");
		}
	}

	@Test
	void cooperativeGrpcFailureIsSchedulerAuthoritativeAndPrecedesErrorPublication() throws Exception {
		var backend = new RecordingAsyncSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			var nativeFailure = RocksDBException.of(RocksDBException.RocksDBErrorType.GET_1,
					"forced gRPC iterator page failure");
			backend.firstMultiFailure.set(nativeFailure);
			var before = backend.scheduler.poolSnapshot(RWScheduler.Pool.READ);
			var leaseCountAtFailure = new AtomicInteger(-1);
			var request = SubsequentRequest.newBuilder()
					.setIterationId(RecordingAsyncSubsequentConnection.ITERATOR_ID)
					.setTakeCount(4_097L)
					.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
							.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
							.setWorkloadContractVersion(3)
				.setTimeoutNanos(Long.MAX_VALUE))
					.build();

			StepVerifier.create(grpcService(server).subsequentMultiGet(request)
						.doOnError(_ -> leaseCountAtFailure.set(
								server.getActiveIteratorOperationLeaseCountForTesting())), 0)
					.thenRequest(1)
					.expectError()
					.verify(Duration.ofSeconds(5));

			var after = backend.scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, leaseCountAtFailure.get(),
					"the iterator lease must be released before stream failure is published");
			assertEquals(1L,
					after.outcomes().get(RWScheduler.TerminalOutcome.FAILURE)
							- before.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
			assertEquals(0L,
					after.outcomes().get(RWScheduler.TerminalOutcome.RUN)
							- before.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertEquals(List.of(64L), backend.syncMultiTakeCounts);
			assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting());
		}
	}

	@Test
	void cancellationKeepsCooperativeMultiLeaseUntilNativePageReturns() throws Exception {
		var backend = new RecordingAsyncSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			backend.blockFirstMulti.set(true);
			var request = SubsequentRequest.newBuilder()
					.setIterationId(RecordingAsyncSubsequentConnection.ITERATOR_ID)
					.setTakeCount(4_097L)
					.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
							.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
							.setWorkloadContractVersion(3)
				.setTimeoutNanos(Long.MAX_VALUE))
					.build();

			StepVerifier.create(grpcService(server).subsequentMultiGet(request), 0)
					.thenRequest(1)
					.then(backend::awaitMultiEntered)
					.thenCancel()
					.verify(Duration.ofSeconds(5));

			try {
				assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting(),
						"cancellation must retain the operation gate until the native call returns");
				assertEquals(List.of(64L), backend.syncMultiTakeCounts);
			} finally {
				backend.releaseMulti.countDown();
			}
			assertTrue(backend.multiReturned.await(5, TimeUnit.SECONDS));
			awaitLeaseCount(server, 0);
			var snapshot = backend.scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, snapshot.queuedTasks());
			assertEquals(0, snapshot.activeTasks());
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
		}
	}

	private static ReactorRocksDBServiceGrpc.RocksDBServiceImplBase grpcService(GrpcServer server)
			throws ReflectiveOperationException {
		Field serviceField = GrpcServer.class.getDeclaredField("grpc");
		serviceField.setAccessible(true);
		return (ReactorRocksDBServiceGrpc.RocksDBServiceImplBase) serviceField.get(server);
	}

	@Test
	void nonCooperativeAndLatencyGrpcRequestsKeepOriginalChunkedPath() throws Exception {
		var backend = new RecordingAsyncSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-chunked-iterator-fallback",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var batchApi = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				var latencyApi = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.latency(
						java.time.Duration.ofSeconds(30)));

				batchApi.subsequent(RecordingAsyncSubsequentConnection.ITERATOR_ID,
						0, 1, RequestType.none());
				assertTrue(batchApi.subsequent(RecordingAsyncSubsequentConnection.ITERATOR_ID,
						0, 1, RequestType.exists()));
				assertEquals(65, batchApi.subsequent(RecordingAsyncSubsequentConnection.ITERATOR_ID,
						0, 65, RequestType.multi()).size());
				assertEquals(4_096, latencyApi.subsequent(RecordingAsyncSubsequentConnection.ITERATOR_ID,
						0, 4_096, RequestType.multi()).size());

				assertEquals(0, backend.asyncSubsequentCalls.get(),
						"bounded and LATENCY requests must not use the cooperative whole-result route");
				assertEquals(List.of(1L, 1L), backend.syncExistsTakeCounts);
				assertEquals(List.of(64L, 1L), backend.syncMultiTakeCounts.subList(0, 2),
						"bounded BATCH MULTI must retain 64-value paging");
				assertEquals(66, backend.syncMultiTakeCounts.size());
				assertTrue(backend.syncMultiTakeCounts.subList(2, 66).stream()
						.allMatch(take -> take == 64L),
						"the largest admitted LATENCY MULTI must retain 64-value paging");
				assertEquals(68, backend.syncSubsequentCalls.get());
				awaitLeaseCount(server, 0);
			}
		}
	}

	@Test
	void rejectedLateOpenCleanupFallsBackInline() throws Exception {
		var backend = new BlockingOpenIteratorConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-rejected-open-cleanup",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var open = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).openIteratorAsync(0, 0, new Keys(), null, false, java.time.Duration.ofMillis( 30_000));
				CompletableFuture<Void> schedulerShutdown = null;
				try {
					assertTrue(backend.entered.await(5, TimeUnit.SECONDS));
					assertTrue(open.cancel(true));
					assertTrue(open.isCancelled(),
							"cancelling the client future establishes RPC cancellation");
					schedulerShutdown = CompletableFuture.runAsync(backend.scheduler::disposeNow);
					awaitPoolShutdown(backend.scheduler, RWScheduler.Pool.CONTROL);
				} finally {
					backend.release.countDown();
				}

				assertTrue(backend.closed.await(5, TimeUnit.SECONDS),
						"a rejected control-lane cleanup must close the late iterator inline");
				assertFalse(backend.iteratorOpen.get());
				if (schedulerShutdown != null) {
					schedulerShutdown.get(5, TimeUnit.SECONDS);
				}
			}
		}
	}

	private static void awaitLeaseCount(GrpcServer server, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (server.getActiveIteratorOperationLeaseCountForTesting() == expected) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		assertEquals(expected, server.getActiveIteratorOperationLeaseCountForTesting());
	}

	private static void awaitPoolShutdown(RWScheduler scheduler,
			RWScheduler.Pool pool) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (scheduler.poolSnapshot(pool).shutdown()) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		assertTrue(scheduler.poolSnapshot(pool).shutdown(), pool + " did not enter shutdown");
	}

	private static final class BlockingSubsequentConnection implements RocksDBConnection, InternalConnection {
		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() { return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT; }

		private static final long ITERATOR_ID = 17;

		private final RWScheduler scheduler = RWScheduler.forTesting(
				2, 1, 1, 16, 16, "grpc-blocking-subsequent");
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch finished = new CountDownLatch(1);
		private final AtomicInteger subsequentCalls = new AtomicInteger();
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {

			public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException("CDC polling is not used by this test double"));
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> CompletableFuture<T> subsequentAsync(long iterationId,
					long skipCount,
					long takeCount,
					RequestType.RequestIterate<? super Buf, T> requestType) {
				if (subsequentCalls.getAndIncrement() != 0) {
					return CompletableFuture.completedFuture((T) Boolean.TRUE);
				}
				var cancellationRequested = new AtomicBoolean();
				CompletableFuture<T> result = new CompletableFuture<>() {
					@Override
					public boolean cancel(boolean mayInterruptIfRunning) {
						return !isDone() && cancellationRequested.compareAndSet(false, true);
					}
				};
				Thread.ofPlatform().name("grpc-blocking-subsequent-native").start(() -> {
					entered.countDown();
					try {
						awaitIgnoringInterrupts(release);
						if (cancellationRequested.get()) {
							result.completeExceptionally(new CancellationException());
						} else {
							result.complete((T) Boolean.TRUE);
						}
					} finally {
						finished.countDown();
					}
				});
				return result;
			}
		};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator) {
					return (RS) Long.valueOf(ITERATOR_ID);
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator) {
					return null;
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> subsequent
						&& subsequent.requestType() instanceof RequestType.RequestExists<?>) {
					return (RS) Boolean.TRUE;
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-blocking-subsequent");
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
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			release.countDown();
			scheduler.dispose();
		}
	}

	private static final class RecordingAsyncSubsequentConnection
			implements RocksDBConnection, InternalConnection {
		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() { return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT; }

		private static final long ITERATOR_ID = 31L;
		private static final String METRIC_DATABASE = "grpc-recording-subsequent";

		private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
		private final RWScheduler scheduler = RWScheduler.forTesting(
				1, 1, 1, 16, 16, "grpc-recording-subsequent", registry, METRIC_DATABASE);
		private final AtomicInteger asyncSubsequentCalls = new AtomicInteger();
		private final AtomicInteger syncSubsequentCalls = new AtomicInteger();
		private final List<Long> syncExistsTakeCounts = new CopyOnWriteArrayList<>();
		private final List<Long> syncMultiTakeCounts = new CopyOnWriteArrayList<>();
		private final AtomicInteger nextMultiValue = new AtomicInteger();
		private final AtomicBoolean queueForegroundAfterFirstMulti = new AtomicBoolean();
		private final AtomicBoolean blockFirstMulti = new AtomicBoolean();
		private final AtomicReference<RuntimeException> firstMultiFailure = new AtomicReference<>();
		private final CountDownLatch foregroundRan = new CountDownLatch(1);
		private final CountDownLatch multiEntered = new CountDownLatch(1);
		private final CountDownLatch releaseMulti = new CountDownLatch(1);
		private final CountDownLatch multiReturned = new CountDownLatch(1);
		private final AtomicBoolean secondPageBeforeForeground = new AtomicBoolean();
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {

			public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException("CDC polling is not used by this test double"));
			}

			@Override
			@SuppressWarnings("unchecked")
			public <T> CompletableFuture<T> subsequentAsync(long iterationId,
					long requestedSkipCount,
					long requestedTakeCount,
					RequestType.RequestIterate<? super Buf, T> requestType) {
				assertEquals(ITERATOR_ID, iterationId);
				assertTrue(requestType instanceof RequestType.RequestMulti<?>);
				asyncSubsequentCalls.incrementAndGet();
				var values = new ArrayList<Buf>(Math.toIntExact(requestedTakeCount));
				for (int index = 0; index < requestedTakeCount; index++) {
					values.add(Buf.wrap(new byte[] {(byte) index}));
				}
				return CompletableFuture.completedFuture((T) values);
			}
		};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?> subsequent) {
					assertEquals(ITERATOR_ID, subsequent.iterationId());
					syncSubsequentCalls.incrementAndGet();
					if (subsequent.requestType() instanceof RequestType.RequestExists<?>) {
						syncExistsTakeCounts.add(subsequent.takeCount());
						return (RS) Boolean.TRUE;
					}
					if (subsequent.requestType() instanceof RequestType.RequestMulti<?>) {
						syncMultiTakeCounts.add(subsequent.takeCount());
						var forcedFailure = firstMultiFailure.getAndSet(null);
						if (forcedFailure != null) {
							throw forcedFailure;
						}
						int pageNumber = syncMultiTakeCounts.size();
						if (pageNumber == 1 && blockFirstMulti.get()) {
							multiEntered.countDown();
							try {
								awaitIgnoringInterrupts(releaseMulti);
							} finally {
								multiReturned.countDown();
							}
						}
						if (pageNumber == 1 && queueForegroundAfterFirstMulti.get()) {
							scheduler.executor(WorkloadProfile.LATENCY,
									OperationFamily.POINT_LOOKUP,
									Long.MAX_VALUE).execute(foregroundRan::countDown, 0L);
						} else if (pageNumber > 1 && foregroundRan.getCount() != 0L) {
							secondPageBeforeForeground.set(true);
						}
						var values = new ArrayList<Buf>(Math.toIntExact(subsequent.takeCount()));
						for (int index = 0; index < subsequent.takeCount(); index++) {
							values.add(Buf.wrap(new byte[] {(byte) nextMultiValue.getAndIncrement()}));
						}
						return (RS) values;
					}
				}
				throw new UnsupportedOperationException("Unexpected synchronous request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-recording-subsequent");
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
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			releaseMulti.countDown();
			scheduler.dispose();
			registry.close();
		}

		private void awaitForeground() {
			try {
				assertTrue(foregroundRan.await(5, TimeUnit.SECONDS),
						"foreground task did not run while the retained stream was parked");
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw new AssertionError("Interrupted while waiting for foreground progress", interrupted);
			}
		}

		private void awaitMultiEntered() {
			try {
				assertTrue(multiEntered.await(5, TimeUnit.SECONDS),
						"cooperative iterator task did not enter its native value page");
			} catch (InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw new AssertionError("Interrupted while waiting for the native value page", interrupted);
			}
		}

		private double counter(String name,
				String profile,
				String operation,
				String... extraTags) {
			var tags = new ArrayList<String>();
			tags.add("database");
			tags.add(METRIC_DATABASE);
			tags.add("resource");
			tags.add("read");
			tags.add("profile");
			tags.add(profile);
			tags.add("operation");
			tags.add(operation);
			tags.addAll(List.of(extraTags));
			return registry.get(name).tags(tags.toArray(String[]::new)).counter().count();
		}

		private long timerCount(String name, String profile, String operation) {
			return registry.get(name)
					.tags("database", METRIC_DATABASE,
							"resource", "read",
							"profile", profile,
							"operation", operation)
					.timer()
					.count();
		}
	}

	private static final class BlockingOpenIteratorConnection implements RocksDBConnection, InternalConnection {
		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() { return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT; }

		private static final long ITERATOR_ID = 23;

		private final RWScheduler scheduler = RWScheduler.forTesting(
				1, 1, 1, 16, 16, "grpc-blocking-open");
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch closed = new CountDownLatch(1);
		private final AtomicBoolean iteratorOpen = new AtomicBoolean();
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {

			public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException("CDC polling is not used by this test double"));
			}
};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator) {
					entered.countDown();
					awaitIgnoringInterrupts(release);
					iteratorOpen.set(true);
					return (RS) Long.valueOf(ITERATOR_ID);
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator) {
					iteratorOpen.set(false);
					closed.countDown();
					return null;
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-blocking-open");
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
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			release.countDown();
			scheduler.dispose();
		}
	}

	private static void awaitIgnoringInterrupts(CountDownLatch release) {
		while (true) {
			try {
				release.await();
				return;
			} catch (InterruptedException ignored) {
				// Model an uninterruptible native call. Cancellation alone cannot release its lease.
			}
		}
	}
}
