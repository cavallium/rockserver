package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

@Timeout(30)
class EmbeddedRangeSchedulingTest {

	@TempDir
	Path tempDir;

	@Test
	void blockedRangeConsumerDoesNotParkTheOnlyReadWorker() throws Exception {
		var configFile = tempDir.resolve("single-reader.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "range-scheduling", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < 128; i++) {
				api.put(0, columnId, intKey(i), intValue(i), RequestType.none());
			}

			var firstItem = new AtomicBoolean(true);
			var firstItemBlocked = new CountDownLatch(1);
			var releaseRange = new CountDownLatch(1);
			var interactiveRan = new CountDownLatch(1);
			var items = new AtomicInteger();
			var completion = Flux.from(connection.getAsyncApi().getRangeAsync(0,
						columnId,
						null,
						null,
						false,
						RequestType.allInRange(),
						10_000))
					.doOnNext(_ -> {
						items.incrementAndGet();
						if (firstItem.compareAndSet(true, false)) {
							firstItemBlocked.countDown();
							await(releaseRange);
						}
					})
					.then()
					.toFuture();

			assertTrue(firstItemBlocked.await(5, TimeUnit.SECONDS));
			try {
				connection.getInternalDB().getScheduler().interactiveRead().schedule(interactiveRan::countDown);
				assertTrue(interactiveRan.await(5, TimeUnit.SECONDS),
						"the only read worker stayed parked behind a blocked range consumer");
				assertTrue(awaitPendingOps(connection, 0, 5, TimeUnit.SECONDS),
						"an idle logical range retained a native read operation after its requested slice completed");
			} finally {
				releaseRange.countDown();
			}

			completion.get(5, TimeUnit.SECONDS);
			assertEquals(128, items.get());
		}
	}

	private static boolean awaitPendingOps(EmbeddedConnection connection,
			long expected,
			long timeout,
			TimeUnit unit) throws InterruptedException {
		long deadline = System.nanoTime() + unit.toNanos(timeout);
		do {
			if (connection.getInternalDB().getPendingOpsCount() == expected) {
				return true;
			}
			Thread.sleep(1);
		} while (System.nanoTime() < deadline);
		return connection.getInternalDB().getPendingOpsCount() == expected;
	}

	@Test
	void largeAsyncIteratorContinuationUsesBoundedSlicesAndStopsAtExhaustion() throws Exception {
		var configFile = tempDir.resolve("single-reader-iterator.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("iterator-db"),
				"iterator-scheduling", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			final int entries = 4_097;
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, intKey(i), intValue(i), RequestType.none());
			}

			long iteratorId = api.openIterator(0, columnId, new Keys(), null, false, 10_000);
			try {
				var readExecutor = (ThreadPoolExecutor) connection.getInternalDB()
						.getScheduler()
						.readExecutor();
				long tasksBefore = readExecutor.getTaskCount();
				var values = connection.getAsyncApi()
						.subsequentAsync(iteratorId, 0, entries, RequestType.<Buf>multi())
						.get(5, TimeUnit.SECONDS);

				assertEquals(entries, values.size());
				assertEquals(intValue(0), values.getFirst());
				assertEquals(intValue(entries - 1), values.getLast());
				long continuationTasks = readExecutor.getTaskCount() - tasksBefore;
				assertTrue(continuationTasks >= 2 && continuationTasks <= 3,
						"large iterator reads should use a few bounded tasks, got " + continuationTasks);

				long exhaustedTasksBefore = readExecutor.getTaskCount();
				connection.getAsyncApi()
						.subsequentAsync(iteratorId, 0, Long.MAX_VALUE, RequestType.none())
						.get(5, TimeUnit.SECONDS);
				assertTrue(readExecutor.getTaskCount() - exhaustedTasksBefore <= 2,
						"an exhausted iterator must not schedule work proportional to the requested count");
			} finally {
				api.closeIterator(iteratorId);
			}
		}
	}

	@Test
	void singleNativeMultiPointReadDoesNotAcquireAnExplicitSnapshot() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("single-multi-db"),
				"single-multi-read", null)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var key = intKey(1);
			api.put(0, columnId, key, intValue(1), RequestType.none());

			var nativeChunks = new AtomicInteger();
			var snapshotAcquisitions = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeChunks::incrementAndGet);
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);

			var existence = connection.getAsyncApi()
					.existsMultiAsync(0, columnId, List.of(key), 10_000)
					.get(5, TimeUnit.SECONDS);

			assertEquals(List.of(true), existence);
			assertEquals(1, nativeChunks.get());
			assertEquals(0, snapshotAcquisitions.get(),
					"one native MultiGet must not acquire and release an explicit RocksDB snapshot");
		}
	}

	@Test
	void multiPointReadUsesOneNativeViewAndOneDeadline() throws Exception {
		var configFile = tempDir.resolve("single-reader-multi.conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { read: 1, write: 1 }
				  global: { enable-fast-get: false, ingest-behind: false, optimistic: false }
				}
				""");
		try (var connection = new EmbeddedConnection(tempDir.resolve("multi-db"),
				"multi-read-scheduling", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			final int existingKeys = 4_200;
			var keys = new java.util.ArrayList<Keys>(existingKeys + 1);
			for (int i = 0; i <= existingKeys; i++) {
				var key = intKey(i);
				keys.add(key);
				if (i < existingKeys) {
					api.put(0, columnId, key, intValue(i), RequestType.none());
				}
			}
			var nativeChunks = new java.util.concurrent.atomic.AtomicInteger();
			var snapshotAcquisitions = new AtomicInteger();
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				if (nativeChunks.incrementAndGet() == 1) {
					api.put(0, columnId, intKey(existingKeys), intValue(existingKeys), RequestType.none());
				}
			});

			var readExecutor = (ThreadPoolExecutor) connection.getInternalDB()
					.getScheduler()
					.readExecutor();
			long tasksBefore = readExecutor.getTaskCount();
			var existence = connection.getAsyncApi()
					.existsMultiAsync(0, columnId, keys, 10_000)
					.get(5, TimeUnit.SECONDS);

			assertEquals(existingKeys + 1, existence.size());
			assertTrue(existence.subList(0, existingKeys).stream().allMatch(Boolean.TRUE::equals));
			assertFalse(existence.getLast(),
					"native chunks must share the snapshot captured before the concurrent insert");
			assertEquals(2, nativeChunks.get());
			assertEquals(1, snapshotAcquisitions.get(),
					"a split logical MultiGet must acquire exactly one shared snapshot");
			long tasks = readExecutor.getTaskCount() - tasksBefore;
			assertEquals(1, tasks,
					"one MultiGet should require one queued read task, got " + tasks);
		}
	}

	@Test
	void multiPointReadBoundsNativeDirectKeyMemoryByBytes() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("multi-byte-db"),
				"multi-read-byte-bound", null)) {
			var api = connection.getSyncApi();
			final int keyBytes = 1_024;
			final int keyCount = 2_050;
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(keyBytes), ObjectList.of(), true));
			var keys = new java.util.ArrayList<Keys>(keyCount);
			for (int i = 0; i < keyCount; i++) {
				var bytes = new byte[keyBytes];
				ByteBuffer.wrap(bytes).putInt(i);
				keys.add(new Keys(Buf.wrap(bytes)));
			}
			var nativeChunks = new AtomicInteger();
			var snapshotAcquisitions = new AtomicInteger();
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				if (nativeChunks.incrementAndGet() == 1) {
					api.put(0, columnId, keys.getLast(), intValue(1), RequestType.none());
				}
			});

			long transactionId = api.openTransaction(10_000);
			java.util.List<Boolean> existence;
			try {
				existence = connection.getAsyncApi()
						.existsMultiAsync(transactionId, columnId, keys, 10_000)
						.get(10, TimeUnit.SECONDS);
			} finally {
				api.closeTransaction(transactionId, false);
			}

			assertEquals(keyCount, existence.size());
			assertTrue(existence.stream().noneMatch(Boolean.TRUE::equals),
					"transactional native chunks must share the request-start snapshot");
			assertEquals(2, nativeChunks.get(),
					"the 2 MiB calculated-key budget must split before the 4096-key limit");
			assertEquals(1, snapshotAcquisitions.get(),
					"transactional native chunks must reuse one explicit request snapshot");
		}
	}

	private static void await(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static Keys intKey(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf intValue(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
