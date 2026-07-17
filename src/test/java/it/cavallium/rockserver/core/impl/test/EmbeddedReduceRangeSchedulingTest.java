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
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(30)
class EmbeddedReduceRangeSchedulingTest {

	@TempDir
	Path tempDir;

	@Test
	void firstAndLastIsInteractiveWhileAFullCountRemainsComposite() {
		var firstAndLast = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<FirstAndLast<KV>>(
				0, 1, null, null, false, RequestType.firstAndLast(), 1_000);
		var entriesCount = new RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<Long>(
				0, 1, null, null, false, RequestType.entriesCount(), 1_000);

		assertEquals(RocksDBAPICommand.ReadWorkClass.INTERACTIVE, firstAndLast.readWorkClass());
		assertEquals(RocksDBAPICommand.ReadWorkClass.COMPOSITE, entriesCount.readWorkClass());
	}

	@Test
	void firstAndLastDeadlineIncludesTimeWaitingForTheReadWorker() throws Exception {
		var configFile = tempDir.resolve("single-reader.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "range-deadline", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, intKey(1), intValue(1), RequestType.none());

			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			connection.getInternalDB().getScheduler().read().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			try {
				var result = connection.getAsyncApi().reduceRangeAsync(
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
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("cancel-db"), "range-cancellation", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var readExecutor = assertInstanceOf(ThreadPoolExecutor.class,
					connection.getInternalDB().getScheduler().readExecutor());
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			readExecutor.execute(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			try {
				var result = connection.getAsyncApi().reduceRangeAsync(
						0, columnId, null, null, false, RequestType.firstAndLast(), 5_000);
				awaitQueueSize(readExecutor, 1);

				assertTrue(result.cancel(true));
				awaitQueueSize(readExecutor, 0);
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

	private static void awaitQueueSize(ThreadPoolExecutor executor, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (executor.getQueue().size() != expected && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(expected, executor.getQueue().size());
	}

	private static Keys intKey(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
