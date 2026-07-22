package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.LoggingClient;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import reactor.core.publisher.Flux;
import sun.misc.Unsafe;

@Timeout(30)
class EmbeddedConnectionAsyncRegressionTest {

	private static final int ITERATOR_CHUNK_SIZE = 4_096;

	@TempDir
	Path tempDir;

	@Test
	void completedAsyncSeekReleasesAdmissionBeforeCompletionIsVisible() throws Exception {
		try (var connection = singleThreadedConnection("seek-completion-order")) {
			var syncApi = connection.getSyncApi();
			var asyncApi = connection.getAsyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			syncApi.put(0, columnId, key(1), value(1), RequestType.none());

			long iteratorId = syncApi.openIterator(0, columnId, new Keys(), null, false, 10_000);
			try {
				for (int attempt = 0; attempt < 1_000; attempt++) {
					asyncApi.seekToAsync(iteratorId, key(1)).join();
					asyncApi.subsequentAsync(iteratorId, 0, 0, RequestType.multi()).join();
				}
			} finally {
				syncApi.closeIterator(iteratorId);
			}
			assertEquals(0, connection.getInternalDB().getOpenIteratorsCount());
			assertEquals(0, connection.getInternalDB().getPendingOpsCount());
		}
	}

	@Test
	void asyncIteratorAdmissionRemainsClaimedBetweenContinuationChunksAndThroughClose() throws Exception {
		try (var connection = singleThreadedConnection("iterator-admission")) {
			var syncApi = connection.getSyncApi();
			var asyncApi = connection.getAsyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = new ArrayList<Keys>(ITERATOR_CHUNK_SIZE + 1);
			var values = new ArrayList<Buf>(ITERATOR_CHUNK_SIZE + 1);
			for (int i = 0; i <= ITERATOR_CHUNK_SIZE; i++) {
				keys.add(key(i));
				values.add(value(i));
			}
			syncApi.putBatch(columnId,
					Flux.just(new KVBatchRef(keys, values)),
					PutBatchMode.WRITE_BATCH_NO_WAL);

			long iteratorId = syncApi.openIterator(0, columnId, new Keys(), null, false, 10_000);
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var initialWorkerEntered = new CountDownLatch(1);
			var releaseInitialWorker = new CountDownLatch(1);
			var betweenChunksEntered = new CountDownLatch(1);
			var releaseBetweenChunks = new CountDownLatch(1);
			CompletableFuture<Void> close = null;
			try {
				readExecutor.execute(() -> {
					initialWorkerEntered.countDown();
					awaitUninterruptibly(releaseInitialWorker);
				});
				assertTrue(initialWorkerEntered.await(5, SECONDS));

				var paging = asyncApi.subsequentAsync(
						iteratorId, 0, ITERATOR_CHUNK_SIZE + 1L, RequestType.none());
				readExecutor.execute(() -> {
					betweenChunksEntered.countDown();
					awaitUninterruptibly(releaseBetweenChunks);
				});
				releaseInitialWorker.countDown();
				assertTrue(betweenChunksEntered.await(5, SECONDS),
						"the test gate never ran between the two bounded iterator steps");
				assertFalse(paging.isDone(), "the second iterator chunk must still be pending");

				assertConcurrentIteratorOperation(asyncApi.seekToAsync(iteratorId, key(0)));
				assertConcurrentIteratorOperation(asyncApi.subsequentAsync(
						iteratorId, 0, 1, RequestType.exists()));

				close = asyncApi.closeIteratorAsync(iteratorId);
				assertFalse(close.isDone(), "close must wait for the complete logical paging operation");
				assertConcurrentIteratorOperation(asyncApi.seekToAsync(iteratorId, key(0)));

				releaseBetweenChunks.countDown();
				paging.get(5, SECONDS);
				close.get(5, SECONDS);
				assertEquals(0, connection.getInternalDB().getOpenIteratorsCount());
				assertEquals(0, connection.getInternalDB().getPendingOpsCount());
			} finally {
				releaseInitialWorker.countDown();
				releaseBetweenChunks.countDown();
				if (close == null || !close.isDone()) {
					syncApi.closeIterator(iteratorId);
				}
			}
		}
	}

	@Test
	void queuedReadCancellationRemovesTheTaskWithoutExecutingNativeWork() throws Exception {
		try (var connection = singleThreadedConnection("queued-cancel")) {
			var syncApi = connection.getSyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var workerEntered = new CountDownLatch(1);
			var releaseWorker = new CountDownLatch(1);
			var nativeChunks = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeChunks::incrementAndGet);
			try {
				readExecutor.execute(() -> {
					workerEntered.countDown();
					awaitUninterruptibly(releaseWorker);
				});
				assertTrue(workerEntered.await(5, SECONDS));

				var queued = connection.getAsyncApi().existsMultiAsync(
						0, columnId, List.of(key(1)), 10_000);
				assertTrue(awaitQueueSize(readExecutor, 1), "the read never entered the executor queue");
				assertTrue(queued.cancel(true));
				assertTrue(awaitQueueSize(readExecutor, 0),
						"cancelling queued work must remove it from the bounded executor queue");

				releaseWorker.countDown();
				assertTrue(awaitCompletedTasks(readExecutor, 1));
				assertEquals(0, nativeChunks.get(), "cancelled queued work reached RocksDB");
				assertTrue(queued.isCancelled());
			} finally {
				releaseWorker.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellingBetweenMultiGetChunksStopsContinuationAndReleasesLogicalState() throws Exception {
		try (var connection = singleThreadedConnection("between-chunks-cancel")) {
			var syncApi = connection.getSyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = new ArrayList<Keys>(ITERATOR_CHUNK_SIZE + 1);
			for (int i = 0; i <= ITERATOR_CHUNK_SIZE; i++) {
				keys.add(key(i));
			}
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var nativeChunks = new AtomicInteger();
			var snapshotAcquisitions = new AtomicInteger();
			var betweenChunksEntered = new CountDownLatch(1);
			var releaseBetweenChunks = new CountDownLatch(1);
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				if (nativeChunks.incrementAndGet() == 1) {
					readExecutor.execute(() -> {
						betweenChunksEntered.countDown();
						awaitUninterruptibly(releaseBetweenChunks);
					});
				}
			});
			try {
				var request = connection.getAsyncApi().existsMultiAsync(
						0, columnId, keys, 10_000);
				assertTrue(betweenChunksEntered.await(5, SECONDS));
				assertTrue(awaitQueueSize(readExecutor, 1),
						"the second native chunk never entered the composite queue");

				assertFalse(request.cancel(true),
						"a started logical read must retain its terminal completion");
				assertThrows(CancellationException.class, () -> request.get(5, SECONDS));
				assertTrue(request.isCancelled());
				assertTrue(awaitQueueSize(readExecutor, 0),
						"cancellation did not remove the queued continuation");
				assertEquals(0, connection.getInternalDB().getPendingOpsCount(),
						"cancellation did not release the retained logical operation");
				assertEquals(1, nativeChunks.get(), "a native chunk ran after cancellation");
				assertEquals(1, snapshotAcquisitions.get());
			} finally {
				releaseBetweenChunks.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
				connection.getInternalDB().setExistsMultiSnapshotObserverForTesting(null);
			}
		}
	}

	@Test
	void forcedShutdownCancelsQueuedMultiGetContinuationAndReleasesLogicalState() throws Exception {
		String timeoutProperty = "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
		String previousTimeout = System.getProperty(timeoutProperty);
		System.setProperty(timeoutProperty, "1");
		var releaseBetweenChunks = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		CompletableFuture<Void> close = null;
		try {
			connection = singleThreadedConnection("between-chunks-shutdown");
			var syncApi = connection.getSyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = new ArrayList<Keys>(ITERATOR_CHUNK_SIZE + 1);
			for (int i = 0; i <= ITERATOR_CHUNK_SIZE; i++) {
				keys.add(key(i));
			}
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var nativeChunks = new AtomicInteger();
			var betweenChunksEntered = new CountDownLatch(1);
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				if (nativeChunks.incrementAndGet() == 1) {
					readExecutor.execute(() -> {
						betweenChunksEntered.countDown();
						awaitUninterruptibly(releaseBetweenChunks);
					});
				}
			});

			var request = connection.getAsyncApi().existsMultiAsync(
					0, columnId, keys, 10_000);
			assertTrue(betweenChunksEntered.await(5, SECONDS));
			assertTrue(awaitQueueSize(readExecutor, 1),
					"the second native chunk never entered the composite queue");

			var connectionToClose = connection;
			close = CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception error) {
					throw new CompletionException(error);
				}
			});

			assertThrows(CancellationException.class, () -> request.get(5, SECONDS));
			assertTrue(request.isCancelled());
			assertEquals(0, connection.getInternalDB().getPendingOpsCount(),
					"forced shutdown did not release the retained logical operation");
			assertEquals(1, nativeChunks.get(), "a native chunk ran after shutdown cancellation");
			assertTrue(awaitQueueSize(readExecutor, 0),
					"shutdown cancellation did not remove the queued continuation");

			releaseBetweenChunks.countDown();
			close.get(5, SECONDS);
		} finally {
			releaseBetweenChunks.countDown();
			if (connection != null) {
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
				if (close == null) {
					connection.closeTesting();
				} else if (!close.isDone()) {
					close.get(5, SECONDS);
				}
			}
			if (previousTimeout == null) {
				System.clearProperty(timeoutProperty);
			} else {
				System.setProperty(timeoutProperty, previousTimeout);
			}
		}
	}

	@Test
	void forcedShutdownCancelsInitiallyQueuedMultiGetAndReleasesAdmission() throws Exception {
		String timeoutProperty = "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
		String previousTimeout = System.getProperty(timeoutProperty);
		System.setProperty(timeoutProperty, "1");
		var releaseWorker = new CountDownLatch(1);
		EmbeddedConnection connection = null;
		CompletableFuture<Void> close = null;
		try {
			connection = singleThreadedConnection("initial-queue-shutdown");
			long columnId = connection.getSyncApi().createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var workerEntered = new CountDownLatch(1);
			var nativeChunks = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeChunks::incrementAndGet);
			readExecutor.execute(() -> {
				workerEntered.countDown();
				awaitUninterruptibly(releaseWorker);
			});
			assertTrue(workerEntered.await(5, SECONDS));

			var request = connection.getAsyncApi().existsMultiAsync(
					0, columnId, List.of(key(1)), 10_000);
			assertTrue(awaitQueueSize(readExecutor, 1), "the initial read never entered the executor queue");
			assertEquals(1, connection.getInternalDB().getPendingOpsCount(),
					"initial queue residence must retain one admitted logical operation");

			var connectionToClose = connection;
			close = CompletableFuture.runAsync(() -> {
				try {
					connectionToClose.closeTesting();
				} catch (Exception error) {
					throw new CompletionException(error);
				}
			});

			assertThrows(CancellationException.class, () -> request.get(5, SECONDS));
			assertTrue(request.isCancelled());
			assertEquals(0, connection.getInternalDB().getPendingOpsCount(),
					"forced shutdown did not release the initially queued admission");
			assertEquals(0, nativeChunks.get(), "an initially queued request reached RocksDB during shutdown");
			assertTrue(awaitQueueSize(readExecutor, 0),
					"shutdown cancellation did not remove the initially queued request");

			releaseWorker.countDown();
			close.get(5, SECONDS);
		} finally {
			releaseWorker.countDown();
			if (connection != null) {
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
				if (close == null) {
					connection.closeTesting();
				} else if (!close.isDone()) {
					close.get(5, SECONDS);
				}
			}
			if (previousTimeout == null) {
				System.clearProperty(timeoutProperty);
			} else {
				System.setProperty(timeoutProperty, previousTimeout);
			}
		}
	}

	@Test
	void multiGetDeadlineIncludesInitialSchedulerQueueWait() throws Exception {
		try (var connection = singleThreadedConnection("queued-deadline")) {
			var syncApi = connection.getSyncApi();
			long columnId = syncApi.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var readExecutor = (ThreadPoolExecutor) connection.getScheduler().readExecutor();
			var workerEntered = new CountDownLatch(1);
			var releaseWorker = new CountDownLatch(1);
			var nativeChunks = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeChunks::incrementAndGet);
			try {
				readExecutor.execute(() -> {
					workerEntered.countDown();
					awaitUninterruptibly(releaseWorker);
				});
				assertTrue(workerEntered.await(5, SECONDS));

				var request = connection.getAsyncApi().existsMultiAsync(
						0, columnId, List.of(key(1)), 25);
				assertTrue(awaitQueueSize(readExecutor, 1));
				Thread.sleep(75);
				releaseWorker.countDown();

				var completion = assertThrows(ExecutionException.class, () -> request.get(5, SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, completion.getCause());
				assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocksFailure.getErrorUniqueId());
				assertEquals(0, nativeChunks.get(), "an expired queued request reached RocksDB");
			} finally {
				releaseWorker.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellingARunningReadDoesNotReplaceItsRootFailure() throws Exception {
		try (var connection = singleThreadedConnection("running-cancel")) {
			long columnId = connection.getSyncApi().createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var nativeCallFinished = new CountDownLatch(1);
			var releaseNativeCall = new CountDownLatch(1);
			var rootFailure = new IllegalStateException("synthetic native completion failure");
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				nativeCallFinished.countDown();
				awaitUninterruptibly(releaseNativeCall);
				throw rootFailure;
			});
			try {
				var running = connection.getAsyncApi().existsMultiAsync(
						0, columnId, List.of(key(1)), 10_000);
				assertTrue(nativeCallFinished.await(5, SECONDS));
				assertFalse(running.cancel(true),
						"a running native read must retain its terminal completion");

				releaseNativeCall.countDown();
				var completion = assertThrows(ExecutionException.class, () -> running.get(5, SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, completion.getCause());
				assertEquals(RocksDBErrorType.GET_1, rocksFailure.getErrorUniqueId());
				assertSame(rootFailure, rocksFailure.getCause(),
						"cancellation must not replace the real running-call failure");
			} finally {
				releaseNativeCall.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void traceLoggingReturnsTheDelegateFutureWithoutWrappingIt() throws Exception {
		var delegateFuture = new CompletableFuture<Buf>();
		var delegate = new FixedFutureConnection(delegateFuture);
		var loggingClient = new LoggingClient(delegate);
		setLogger(loggingClient, traceEnabledNoopLogger());
		var request = new RocksDBAPICommand.RocksDBAPICommandSingle.Get<>(
				0, 1, key(1), RequestType.current());

		var actual = loggingClient.getAsyncApi().requestAsync(request);

		assertSame(delegateFuture, actual,
				"TRACE observation must not change cancellation or completion identity");
		delegateFuture.complete(value(1));
	}

	private EmbeddedConnection singleThreadedConnection(String name) throws Exception {
		var config = tempDir.resolve(name + ".conf");
		Files.writeString(config, """
				database: {
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		return new EmbeddedConnection(tempDir.resolve(name + "-db"), name, config);
	}

	private static void assertConcurrentIteratorOperation(CompletableFuture<?> rejected) {
		var completion = assertThrows(ExecutionException.class, () -> rejected.get(1, SECONDS));
		var rocksFailure = assertInstanceOf(RocksDBException.class, completion.getCause());
		assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, rocksFailure.getErrorUniqueId());
		assertTrue(rocksFailure.getMessage().contains("Concurrent operation on iterator"));
	}

	private static boolean awaitQueueSize(ThreadPoolExecutor executor, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		do {
			if (executor.getQueue().size() == expected) {
				return true;
			}
			Thread.sleep(1);
		} while (System.nanoTime() < deadline);
		return executor.getQueue().size() == expected;
	}

	private static boolean awaitCompletedTasks(ThreadPoolExecutor executor, long expected) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		do {
			if (executor.getCompletedTaskCount() >= expected) {
				return true;
			}
			Thread.sleep(1);
		} while (System.nanoTime() < deadline);
		return executor.getCompletedTaskCount() >= expected;
	}

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
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static Logger traceEnabledNoopLogger() {
		return (Logger) Proxy.newProxyInstance(
				Logger.class.getClassLoader(),
				new Class<?>[] {Logger.class},
				(_, method, _) -> switch (method.getName()) {
					case "isEnabledForLevel" -> true;
					case "getName" -> "db.requests";
					default -> method.getReturnType() == boolean.class ? false : null;
				});
	}

	@SuppressWarnings({"deprecation", "removal"})
	private static void setLogger(LoggingClient client, Logger logger) throws Exception {
		Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
		unsafeField.setAccessible(true);
		Unsafe unsafe = (Unsafe) unsafeField.get(null);
		Field loggerField = LoggingClient.class.getDeclaredField("logger");
		unsafe.putObject(client, unsafe.objectFieldOffset(loggerField), logger);
	}

	private static final class FixedFutureConnection implements RocksDBConnection {

		private final CompletableFuture<Buf> future;
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> request) {
				return (RA) future;
			}
		};

		private FixedFutureConnection(CompletableFuture<Buf> future) {
			this.future = future;
		}

		@Override
		public URI getUrl() {
			return URI.create("test://logging-future");
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
		public void close() {
		}
	}
}
