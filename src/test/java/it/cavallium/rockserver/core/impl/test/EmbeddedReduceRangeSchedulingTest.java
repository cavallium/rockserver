package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class EmbeddedReduceRangeSchedulingTest {
	private static final int DATA_WORKERS = 3;

	@TempDir
	Path tempDir;

	@Test
	void firstAndLastIsInteractiveWhileAFullCountRemainsComposite() {
		var firstAndLast = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<FirstAndLast<KV>>(
				0, 1, null, null, false, RequestType.firstAndLast(), 1_000);
		var entriesCount = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<Long>(
				0, 1, null, null, false, RequestType.entriesCount(), 1_000);

		assertEquals(it.cavallium.rockserver.core.common.OperationFamily.BOUNDARY_SEEK,
				firstAndLast.operationFamily());
		assertEquals(it.cavallium.rockserver.core.common.OperationFamily.FULL_SCAN_AGGREGATE,
				entriesCount.operationFamily());
	}

	@Test
	void firstAndLastUsesAsyncIoForManySstSeekFanout() throws Exception {
		var configFile = tempDir.resolve("range-async-io.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("async-io-db"), "range-async-io", configFile)) {
			var api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var observedAsyncIo = new AtomicReference<Boolean>();
			connection.getInternalDB().setReduceRangeAsyncIoObserverForTesting(observedAsyncIo::set);
			try {
				api.reduceRange(0, columnId, null, null, false, RequestType.firstAndLast(), 1_000);
				assertEquals(Boolean.TRUE, observedAsyncIo.get(),
						"endpoint seeks must submit SST child reads concurrently");
			} finally {
				connection.getInternalDB().setReduceRangeAsyncIoObserverForTesting(null);
			}
		}
	}

	@Test
	void firstAndLastDeadlineIncludesTimeWaitingForTheReadWorker() throws Exception {
		var configFile = tempDir.resolve("single-reader.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "range-deadline", configFile)) {
			var api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, intKey(1), intValue(1), RequestType.none());

			var blockerStarted = new CountDownLatch(DATA_WORKERS);
			var releaseBlocker = new CountDownLatch(1);
			occupyWorkers(connection.getInternalDB().getScheduler().readExecutor(),
					blockerStarted,
					releaseBlocker);
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			try {
				var result = connection.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.firstAndLast(), 50);
				Thread.sleep(150);
				releaseBlocker.countDown();

				var failure = assertThrows(ExecutionException.class,
						() -> result.get(5, TimeUnit.SECONDS));
				var rocksFailure = assertInstanceOf(RocksDBException.class, failure.getCause());
				assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
						rocksFailure.getErrorUniqueId());
				assertEquals(0, iteratorOpens.get(),
						"an already-expired queued request must not open a native iterator");
			} finally {
				releaseBlocker.countDown();
				connection.getInternalDB().setRangeIteratorOpenObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellingQueuedFirstAndLastRemovesItsInteractiveWrapper() throws Exception {
		var configFile = tempDir.resolve("single-reader-cancellation.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("cancel-db"), "range-cancellation", configFile)) {
			var api = connection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var scheduler = connection.getInternalDB().getScheduler();
			var readExecutor = scheduler.readExecutor();
			var blockerStarted = new CountDownLatch(DATA_WORKERS);
			var releaseBlocker = new CountDownLatch(1);
			occupyWorkers(readExecutor, blockerStarted, releaseBlocker);
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			try {
				var result = connection.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.firstAndLast(), 5_000);
				awaitQueueSize(scheduler, 1);

				assertTrue(result.cancel(true));
				awaitQueueSize(scheduler, 0);
				assertTrue(result.isCancelled());
			} finally {
				releaseBlocker.countDown();
			}
		}
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException _) {
			Thread.currentThread().interrupt();
		}
	}

	private static void occupyWorkers(java.util.concurrent.Executor executor,
			CountDownLatch entered,
			CountDownLatch release) {
		for (int i = 0; i < DATA_WORKERS; i++) {
			executor.execute(() -> {
				entered.countDown();
				await(release);
			});
		}
	}

	private static void awaitQueueSize(RWScheduler scheduler, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() != expected
				&& System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(expected, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
	}

	private static Keys intKey(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
