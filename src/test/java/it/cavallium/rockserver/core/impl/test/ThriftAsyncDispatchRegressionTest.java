package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.ServerSocket;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

@Timeout(30)
class ThriftAsyncDispatchRegressionTest {

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
		assertEquals(41L, api.openTransaction(10_000));
		assertEquals(3, connection.syncRequests.size());
		assertEquals(0, connection.asyncRequests.size());

		assertEquals(List.of(false), api.existsMulti(0, 1, List.of(key(1)), 10_000));
		api.mergeBatch(1, Flux.empty(), MergeBatchMode.MERGE_WRITE_BATCH);

		assertEquals(3, connection.syncRequests.size(),
				"bounded and lane-specific work must not fall back to the direct sync path");
		assertEquals(List.of(
				RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti.class,
				RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch.class),
				connection.asyncRequests);
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
	void mergeBatchThroughAsyncThriftBridgeDoesNotSelfDeadlockWithOneWriteWorker() throws Exception {
		var config = tempDir.resolve("single-writer.conf");
		Files.writeString(config, """
				database: {
				  parallelism: { read: 1, write: 1 }
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
				long columnId = client.getSyncApi().createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				var key = key(1);
				var value = Buf.wrap("first".getBytes(StandardCharsets.UTF_8));
				try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
					var completion = CompletableFuture.runAsync(() -> client.getSyncApi().mergeBatch(
							columnId,
							Flux.just(new KVBatchRef(List.of(key), List.of(value))),
							MergeBatchMode.MERGE_WRITE_BATCH), executor);
					completion.get(5, SECONDS);
				}

				assertEquals(value, client.getSyncApi().get(0, columnId, key, RequestType.current()));
			}
		}
	}

	@Test
	void concurrentAsyncCallsUseIndependentFramedTransports() throws Exception {
		var config = tempDir.resolve("concurrent-thrift.conf");
		Files.writeString(config, """
				database: {
				  parallelism: { read: 4, write: 2 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		int port = freePort();
		try (var backend = new EmbeddedConnection(tempDir.resolve("concurrent-db"), "concurrent-thrift", config);
				var server = new ThriftServer(backend, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("concurrent-thrift", "127.0.0.1", port)) {
				var sync = client.getSyncApi();
				long columnId = sync.createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				for (int i = 0; i < 32; i++) {
					sync.put(0, columnId, key(i), intValue(i), RequestType.none());
				}

				var reads = new ArrayList<CompletableFuture<Buf>>();
				for (int i = 0; i < 256; i++) {
					int value = i % 32;
					reads.add(client.getAsyncApi().getAsync(0,
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
				0, 1, key(0), null, false, 10_000);

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

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private record InterruptedInvocation(Throwable failure, boolean interruptRestored) {
	}

	@FunctionalInterface
	private interface ThrowingRunnable {

		void run() throws Exception;
	}

	private static final class TrackingConnection implements RocksDBConnection {

		private final List<Class<?>> syncRequests = new CopyOnWriteArrayList<>();
		private final List<Class<?>> asyncRequests = new CopyOnWriteArrayList<>();
		private final List<Long> closedIteratorIds = new CopyOnWriteArrayList<>();
		private final CountDownLatch openIteratorRequested = new CountDownLatch(1);
		private final CountDownLatch closeIteratorRequested = new CountDownLatch(1);
		private final AtomicBoolean openIteratorRunning = new AtomicBoolean();
		private final CompletableFuture<Long> openIteratorFuture = new CompletableFuture<>();
		private volatile CompletableFuture<Void> closeIteratorFuture = CompletableFuture.completedFuture(null);
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
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				asyncRequests.add(request.getClass());
				Object result = switch (request) {
					case RocksDBAPICommand.RocksDBAPICommandSingle.ExistsMulti _ ->
							CompletableFuture.completedFuture(List.of(false));
					case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch _ ->
							CompletableFuture.completedFuture(null);
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
		public RocksDBSyncAPI getSyncApi() {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi() {
			return asyncApi;
		}

		@Override
		public void close() throws IOException {
		}
	}
}
