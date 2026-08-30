package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.time.Duration;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.layered.TFramedTransport;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Timeout(30)
class ThriftAsyncDispatchRegressionTest {
	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}
	@Test
	void thriftSlotContentionConsumesTheAlreadyBoundBudget() throws Exception {
		String previousPoolSize = System.getProperty("rockserver.thrift.client.connections");
		System.setProperty("rockserver.thrift.client.connections", "1");
		var backend = new SlotBlockingConnection();
		int port = freePort();
		try (var server = new ThriftServer(backend, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("thrift-slot-deadline", "127.0.0.1", port)) {
				var first = CompletableFuture.supplyAsync(() -> client.getSyncApi(RequestContext.batch())
						.getColumnId("block"));
				assertTrue(backend.entered.await(5, TimeUnit.SECONDS));
				long started = System.nanoTime();
				var failure = assertThrows(RocksDBException.class,
						() -> client.getSyncApi(RequestContext.batch(Duration.ofMillis(100)))
								.getColumnId("must-not-send"));
				long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
				assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						failure.getErrorUniqueId());
				assertTrue(elapsedMillis >= 75L && elapsedMillis < 2_000L,
						"slot wait ignored the request budget: " + elapsedMillis + " ms");
				assertEquals(0, backend.secondCalls.get());
				backend.release.countDown();
				assertEquals(1L, first.get(5, TimeUnit.SECONDS));
			}
		} finally {
			backend.release.countDown();
			if (previousPoolSize == null) System.clearProperty("rockserver.thrift.client.connections");
			else System.setProperty("rockserver.thrift.client.connections", previousPoolSize);
		}
	}

	@TempDir
	Path tempDir;

	@Test
	void hybridBridgeKeepsPointOperationsDirectAndDispatchesBoundedOrLaneSpecificWork() {
		var connection = new TrackingConnection();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);
		var expected = Buf.wrap(new byte[] {1, 2, 3});
		connection.pointReadResult = expected;

		assertEquals(expected, api.get(0, 1, key(1), RequestType.current()));
		api.put(0, 1, key(1), expected, RequestType.none());
		assertEquals(41L, api.openTransaction( java.time.Duration.ofMillis(10_000)));
		assertEquals(3, connection.syncRequests.size());
		assertEquals(0, connection.asyncRequests.size());

		api.put(0, 1, key(2), expected, RequestType.none());
		assertEquals(42L, api.createColumn("classified", ColumnSchema.of(
				IntList.of(Integer.BYTES), ObjectList.of(), true)));
		api.deleteRange(1, key(1), key(2));
		assertEquals(List.of(false), api.existsMulti(0, 1, List.of(key(1))));
		api.mergeBatch(1, Flux.empty(), MergeBatchMode.MERGE_WRITE_BATCH);

		assertEquals(4, connection.syncRequests.size(),
				"ordinary point work must retain the direct sync path");
		assertEquals(List.of(
				RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch.class),
				connection.asyncRequests);
	}

	@Test
	void prefixlessCdcWorkUsesTheAsyncBridgeForTheDedicatedCdcLane() {
		var connection = new TrackingConnection();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);

		assertEquals(it.cavallium.rockserver.core.common.OperationFamily.WAL_PAGE,
				new RocksDBAPICommand.CdcGetEarliestAvailableSequence().operationFamily());
		assertEquals(37L, api.cdcGetEarliestAvailableSequence());
		assertEquals(41L, api.cdcCreate("prefixless", 0L, null, false, java.util.OptionalLong.empty()));

		assertTrue(connection.syncRequests.isEmpty());
		assertEquals(List.of(
				RocksDBAPICommand.CdcGetEarliestAvailableSequence.class,
				RocksDBAPICommand.CdcCreate.class), connection.asyncRequests);
	}

	@Test
	void transactionCleanupAndCommitUseTheAsyncSchedulerBridge() {
		var connection = new TrackingConnection();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);

		api.closeFailedUpdate(71L);
		assertTrue(api.closeTransaction(72L, false));
		assertTrue(api.closeTransaction(73L, true));

		assertTrue(connection.syncRequests.isEmpty());
		assertEquals(List.of(
				RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction.class),
				connection.asyncRequests);
	}

	@Test
	void expiredThriftContextFailsLocallyButRollbackUsesProtectedBackendContext() throws Exception {
		var connection = new TrackingConnection();
		int port = freePort();
		try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("thrift-deadline-contract", "127.0.0.1", port)) {
				var expired = new it.cavallium.rockserver.core.common.RequestContext(
						it.cavallium.rockserver.core.common.WorkloadProfile.BATCH, 1L);
				var error = assertThrows(RocksDBException.class,
						() -> client.getSyncApi(expired).getColumnId("never-sent"));
				assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						error.getErrorUniqueId());

				var finite = it.cavallium.rockserver.core.common.RequestContext.batch(
						java.time.Duration.ofSeconds(30));
				long transactionId = client.getSyncApi(finite).openTransaction( java.time.Duration.ofMillis(10_000L));
				assertTrue(connection.lastApiContextDeadline.get() > 0L);
				assertTrue(connection.lastApiContextDeadline.get() <= finite.timeoutNanos());
				assertTrue(client.getSyncApi(expired).closeTransaction(transactionId, false));
				assertEquals(Long.MAX_VALUE,
						connection.lastApiContextDeadline.get());
			}
		}
	}

	@Test
	void v2ThriftRollbackIsRejectedBeforeProtectedBackendDispatch() throws Exception {
		var connection = new TrackingConnection();
		int port = freePort();
		try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
			server.start();
			var configuration = TConfiguration.custom().build();
			try (var transport = new TFramedTransport(
					new TSocket(configuration, "127.0.0.1", port))) {
				transport.open();
				var rawClient = new it.cavallium.rockserver.core.common.api.RocksDB.Client(
						new TBinaryProtocol(transport));
				var v2Context = new it.cavallium.rockserver.core.common.api.RequestContext(
						it.cavallium.rockserver.core.common.api.WorkloadProfile.BATCH,
						2,
						Long.MAX_VALUE);
				var failure = assertThrows(
						it.cavallium.rockserver.core.common.api.RocksDBThriftException.class,
						() -> rawClient.closeTransaction(72L, false, v2Context));
				assertEquals(it.cavallium.rockserver.core.common.api.RocksDBErrorType.PUT_INVALID_REQUEST,
						failure.getErrorType());
				assertTrue(connection.asyncRequests.isEmpty(),
						"v2 rollback reached the protected backend");
			}
		}
	}

	@Test
	void thriftBindsTheWireEpochOnceBeforeAHostClockJump() throws Exception {
		var clock = new MutableClock(System.currentTimeMillis(), 0L);
		var scheduler = scheduler(clock, "thrift-monotonic-boundary");
		var connection = new BoundaryJumpConnection(scheduler, clock);
		int port = freePort();
		try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("thrift-monotonic-boundary", "127.0.0.1", port)) {
				var context = it.cavallium.rockserver.core.common.RequestContext.batch(
						Duration.ofSeconds(10));

				var failure = assertThrows(RocksDBException.class,
						() -> client.getSyncApi(context).getColumnId("deadline-probe"));

				assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						failure.getErrorUniqueId());
				assertFalse(connection.scheduledTaskRan.get(),
						"a backward wall jump after Thrift decoding must not grant a fresh scheduler budget");
			}
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void cdcCommitUsesTheAsyncBridgeAndSurvivesWorkerInterruption() throws Exception {
		var connection = new TrackingConnection();
		connection.cdcCommitFuture = new CompletableFuture<>();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);

		var interrupted = runInterrupted(
				() -> api.cdcCommit("progress", 42L), connection.cdcCommitRequested);

		assertInstanceOf(RocksDBException.class, interrupted.failure());
		assertTrue(interrupted.interruptRestored());
		assertFalse(connection.cdcCommitFuture.isCancelled(),
				"an interrupted CDC checkpoint must remain live after the Thrift worker returns");
		assertEquals(List.of(RocksDBAPICommand.CdcCommit.class), connection.asyncRequests);
		connection.cdcCommitFuture.complete(null);
		connection.cdcCommitFuture.get(1, SECONDS);
	}

	@Test
	void interruptedQueuedOpenIteratorClosesTheHandleAfterItEventuallyOpens() throws Exception {
		assertInterruptedOpenIteratorCleanup(false);
	}

	@Test
	void interruptedRunningOpenIteratorClosesTheHandleAfterNativeCompletion() throws Exception {
		assertInterruptedOpenIteratorCleanup(true);
	}

	@Test
	@ResourceLock(Resources.SYSTEM_PROPERTIES)
	void runningAsyncThriftResourceCreationCannotLoseItsEventualHandleToCancellation() throws Exception {
		String property = "rockserver.thrift.client.connections";
		String previous = System.getProperty(property);
		System.setProperty(property, "1");
		var connection = new TrackingConnection();
		int port = freePort();
		try (var server = new ThriftServer(connection, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("thrift-running-cancel", "127.0.0.1", port)) {
				var api = client.getAsyncApi(
						it.cavallium.rockserver.core.common.RequestContext.batch());
				var opened = api.openIteratorAsync(0, 1, key(0), null, false, java.time.Duration.ofMillis( 10_000));
				boolean cancelled;
				try {
					assertTrue(connection.openIteratorRequested.await(5, SECONDS));
					cancelled = opened.cancel(true);
					var queued = api.openTransactionAsync( java.time.Duration.ofMillis(10_000));
					assertTrue(queued.cancel(true),
							"a queued transport call must be removable before it acquires a socket");
				} finally {
					connection.openIteratorFuture.complete(77L);
				}
				assertFalse(cancelled, "a running transport call must retain its eventual resource handle");
				assertEquals(77L, opened.get(5, SECONDS));
				assertTrue(connection.syncRequests.isEmpty(),
						"the cancelled queued transaction creation must never reach the server");
				api.closeIteratorAsync(77L).get(5, SECONDS);
				assertEquals(List.of(77L), connection.closedIteratorIds);
			}
		} finally {
			connection.openIteratorFuture.completeExceptionally(
					new IllegalStateException("test cleanup"));
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
	}

	@Test
	void interruptedQueuedCloseIteratorRemainsLiveUntilTheReleaseCompletes() throws Exception {
		var connection = new TrackingConnection();
		connection.closeIteratorFuture = new CompletableFuture<>();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);

		var interrupted = runInterrupted(() -> api.closeIterator(91L), connection.closeIteratorRequested);

		assertInstanceOf(RocksDBException.class, interrupted.failure());
		assertTrue(interrupted.interruptRestored());
		assertFalse(connection.closeIteratorFuture.isCancelled(),
				"an interrupted queued close must not be suppressed");
		assertEquals(List.of(91L), connection.closedIteratorIds);
		connection.closeIteratorFuture.complete(null);
		connection.closeIteratorFuture.get(1, SECONDS);
	}

	@Test
	void concurrentMergeBatchesThroughAsyncThriftBridgeDoNotSelfDeadlockAtWriteCapacity() throws Exception {
		var config = tempDir.resolve("single-writer.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: {
				    enable-fast-get: false
				    ingest-behind: false
				    optimistic: false
				    fallback-column-options: {
				      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
				    }
				  }
				}
				""");
		int port = freePort();
		try (var backend = new EmbeddedConnection(tempDir.resolve("db"), "thrift-single-writer", config);
				var server = new ThriftServer(backend, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("thrift-single-writer", "127.0.0.1", port)) {
				long columnId = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
					var completions = new ArrayList<CompletableFuture<Void>>();
					for (int i = 0; i < 3; i++) {
						var key = key(i);
						var value = Buf.wrap(("value-" + i).getBytes(StandardCharsets.UTF_8));
						completions.add(CompletableFuture.runAsync(() -> client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).mergeBatch(
								columnId,
								Flux.just(new KVBatchRef(List.of(key), List.of(value))),
								MergeBatchMode.MERGE_WRITE_BATCH), executor));
					}
					CompletableFuture.allOf(completions.toArray(CompletableFuture[]::new)).get(5, SECONDS);
				}

				for (int i = 0; i < 3; i++) {
					assertEquals(Buf.wrap(("value-" + i).getBytes(StandardCharsets.UTF_8)),
							client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch())
									.get(0, columnId, key(i), RequestType.current()));
				}
			}
		}
	}

	@Test
	void concurrentAsyncCallsUseIndependentFramedTransports() throws Exception {
		var config = tempDir.resolve("concurrent-thrift.conf");
		Files.writeString(config, """
				database: {
				  parallelism: { read: 4, write: 3 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		int port = freePort();
		try (var backend = new EmbeddedConnection(tempDir.resolve("concurrent-db"), "concurrent-thrift", config);
				var server = new ThriftServer(backend, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("concurrent-thrift", "127.0.0.1", port)) {
				var sync = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
				long columnId = sync.createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				for (int i = 0; i < 32; i++) {
					sync.put(0, columnId, key(i), intValue(i), RequestType.none());
				}

				var reads = new ArrayList<CompletableFuture<Buf>>();
				for (int i = 0; i < 256; i++) {
					int value = i % 32;
					reads.add(client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getAsync(0,
							columnId,
							key(value),
							RequestType.current()));
				}
				CompletableFuture.allOf(reads.toArray(CompletableFuture[]::new)).get(10, SECONDS);
				for (int i = 0; i < reads.size(); i++) {
					assertEquals(intValue(i % 32), reads.get(i).join());
				}
			}
		}
	}

	private void assertInterruptedOpenIteratorCleanup(boolean running) throws Exception {
		var connection = new TrackingConnection();
		var api = ThriftServer.createDispatchingSyncApiForTesting(connection);
		var request = new RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator(
				0, 1, key(0), null, false, Duration.ofSeconds(10));

		var invocation = new AtomicReference<InterruptedInvocation>();
		var thread = Thread.ofPlatform().name("interrupted-thrift-open-iterator").unstarted(() ->
				invocation.set(invokeAndCaptureInterrupt(() -> api.requestSync(request))));
		thread.start();
		assertTrue(connection.openIteratorRequested.await(5, SECONDS));
		if (running) {
			connection.openIteratorRunning.set(true);
		}
		thread.interrupt();
		thread.join(5_000);
		assertFalse(thread.isAlive(), "interrupted Thrift invocation did not return");

		assertInstanceOf(RocksDBException.class, invocation.get().failure());
		assertTrue(invocation.get().interruptRestored());
		assertEquals(running, connection.openIteratorRunning.get());
		assertFalse(connection.openIteratorFuture.isCancelled(),
				"resource-producing work must retain its eventual id for cleanup");

		if (!running) {
			// Model a task that was still queued when the worker was interrupted and only
			// entered the native call afterwards.
			connection.openIteratorRunning.set(true);
		}
		connection.openIteratorFuture.complete(77L);
		assertTrue(connection.closeIteratorRequested.await(5, SECONDS));
		assertEquals(List.of(77L), connection.closedIteratorIds);
	}

	private static InterruptedInvocation runInterrupted(ThrowingRunnable invocation,
			CountDownLatch requestDispatched) throws Exception {
		var result = new AtomicReference<InterruptedInvocation>();
		var thread = Thread.ofPlatform().name("interrupted-thrift-request").unstarted(() ->
				result.set(invokeAndCaptureInterrupt(invocation)));
		thread.start();
		assertTrue(requestDispatched.await(5, SECONDS));
		thread.interrupt();
		thread.join(5_000);
		assertFalse(thread.isAlive(), "interrupted Thrift invocation did not return");
		return result.get();
	}

	private static InterruptedInvocation invokeAndCaptureInterrupt(ThrowingRunnable invocation) {
		try {
			invocation.run();
			return new InterruptedInvocation(new AssertionError("request unexpectedly completed"),
					Thread.currentThread().isInterrupted());
		} catch (Throwable failure) {
			return new InterruptedInvocation(failure, Thread.currentThread().isInterrupted());
		}
	}

	private static int freePort() throws Exception {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static RWScheduler scheduler(MutableClock clock, String name) {
		try {
			Method factory = RWScheduler.class.getDeclaredMethod("forTesting",
					int.class,
					int.class,
					int.class,
					int.class,
					int.class,
					String.class,
					LongSupplier.class);
			factory.setAccessible(true);
			return (RWScheduler) factory.invoke(null,
					1,
					1,
					1,
					8,
					8,
					name,
					(LongSupplier) clock::nanoTime);
		} catch (ReflectiveOperationException reflectionFailure) {
			throw new AssertionError(reflectionFailure);
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private record InterruptedInvocation(Throwable failure, boolean interruptRestored) {
	}

	private static final class MutableClock {

		private final AtomicLong epochMillis;
		private final AtomicLong nanoTime;

		private MutableClock(long epochMillis, long nanoTime) {
			this.epochMillis = new AtomicLong(epochMillis);
			this.nanoTime = new AtomicLong(nanoTime);
		}

		private long epochMillis() {
			return epochMillis.get();
		}

		private long nanoTime() {
			return nanoTime.get();
		}
	}

	private static final class BoundaryJumpConnection implements RocksDBConnection, InternalConnection {
		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() { return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT; }

		private final RWScheduler scheduler;
		private final MutableClock clock;
		private final AtomicBoolean scheduledTaskRan = new AtomicBoolean();

		private BoundaryJumpConnection(RWScheduler scheduler, MutableClock clock) {
			this.scheduler = scheduler;
			this.clock = clock;
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public URI getUrl() {
			return URI.create("memory://thrift-monotonic-boundary");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBSyncAPI() {
				@Override
				public long getColumnId(String name) {
					clock.epochMillis.addAndGet(-30_000L);
					clock.nanoTime.addAndGet(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(10_001L));
					scheduler.executor(WorkloadProfile.BATCH, OperationFamily.METADATA, context)
							.execute(() -> scheduledTaskRan.set(true));
					return 1L;
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {

			public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException("CDC polling is not used by this test double"));
			}
};
		}

		@Override
		public void close() {
		}
	}

	private static final class SlotBlockingConnection implements RocksDBConnection {
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final AtomicInteger secondCalls = new AtomicInteger();

		@Override
		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public URI getUrl() { return URI.create("test://thrift-slot"); }

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return new RocksDBSyncAPI() {
				@Override
				public long getColumnId(String name) {
					if (name.equals("block")) {
						entered.countDown();
						awaitUninterruptibly(release);
						return 1L;
					}
					secondCalls.incrementAndGet();
					return 2L;
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return new RocksDBAsyncAPI() {
				@Override
				public Mono<CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
					return Mono.error(new UnsupportedOperationException());
				}
			};
		}

		@Override public void close() {}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {

		void run() throws Exception;
	}

	private static final class TrackingConnection implements RocksDBConnection {

		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() {
			return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT;
		}


		private final List<Class<?>> syncRequests = new CopyOnWriteArrayList<>();
		private final List<Class<?>> asyncRequests = new CopyOnWriteArrayList<>();
		private final List<Long> closedIteratorIds = new CopyOnWriteArrayList<>();
		private final CountDownLatch openIteratorRequested = new CountDownLatch(1);
		private final CountDownLatch closeIteratorRequested = new CountDownLatch(1);
		private final CountDownLatch cdcCommitRequested = new CountDownLatch(1);
		private final AtomicBoolean openIteratorRunning = new AtomicBoolean();
		private final AtomicLong lastApiContextDeadline = new AtomicLong(-1L);
		private final CompletableFuture<Long> openIteratorFuture = new CompletableFuture<>();
		private volatile CompletableFuture<Void> closeIteratorFuture = CompletableFuture.completedFuture(null);
		private volatile CompletableFuture<Void> cdcCommitFuture = CompletableFuture.completedFuture(null);
		private volatile Buf pointReadResult;

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				syncRequests.add(request.getClass());
				Object result = switch (request) {
					case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> _ -> pointReadResult;
					case RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction _ -> 41L;
					default -> null;
				};
				return (RS) result;
			}
		};

		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {

			public reactor.core.publisher.Mono<it.cavallium.rockserver.core.common.cdc.CdcBatch> cdcPollBatchAsync(String id, Long fromSeq, long maxEvents) {
				return reactor.core.publisher.Mono.error(new UnsupportedOperationException("CDC polling is not used by this test double"));
			}

			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				asyncRequests.add(request.getClass());
				Object result = switch (request) {
					case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> _ ->
							CompletableFuture.completedFuture(null);
					case RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn _ ->
							CompletableFuture.completedFuture(42L);
					case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteRange _ ->
							CompletableFuture.completedFuture(null);
					case RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti _ ->
							CompletableFuture.completedFuture(List.of(false));
					case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch _ ->
							CompletableFuture.completedFuture(null);
					case RocksDBAPICommand.CdcGetEarliestAvailableSequence _ ->
							CompletableFuture.completedFuture(37L);
					case RocksDBAPICommand.CdcCreate _ -> CompletableFuture.completedFuture(41L);
					case RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate _ ->
							CompletableFuture.completedFuture(null);
					case RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction _ ->
							CompletableFuture.completedFuture(true);
					case RocksDBAPICommand.CdcCommit _ -> {
						cdcCommitRequested.countDown();
						yield cdcCommitFuture;
					}
					case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator _ -> {
						openIteratorRequested.countDown();
						yield openIteratorFuture;
					}
					case RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator close -> {
						closedIteratorIds.add(close.iteratorId());
						closeIteratorRequested.countDown();
						yield closeIteratorFuture;
					}
					default -> CompletableFuture.failedFuture(
							new UnsupportedOperationException("Unexpected async request: " + request.getClass().getName()));
				};
				return (RA) result;
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("memory://thrift-dispatch-test");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			lastApiContextDeadline.set(context.timeoutNanos());
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			lastApiContextDeadline.set(context.timeoutNanos());
			return asyncApi;
		}

		@Override
		public void close() throws IOException {
		}
	}
}
