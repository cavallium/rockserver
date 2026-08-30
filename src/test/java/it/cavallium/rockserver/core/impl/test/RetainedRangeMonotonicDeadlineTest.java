package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ReadOptions;
import reactor.core.publisher.Flux;

@Timeout(30)
class RetainedRangeMonotonicDeadlineTest {

	@TempDir
	Path tempDir;

	@Test
	void backwardWallJumpCannotExtendActiveOrPermitWaitingRetainedReads() throws Exception {
		var clock = new MutableClock(System.currentTimeMillis(), 0L);
		var scheduler = scheduler(clock, "retained-monotonic-deadline");
		EmbeddedDB db = embeddedDb(scheduler, "retained-monotonic-backward");
		var firstChunk = new CountDownLatch(1);
		var releaseFirstChunk = new CountDownLatch(1);
		try {
			long columnId = db.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int index = 0; index < 64; index++) {
				db.put(0L, columnId, key(index), value(index), RequestType.none());
			}

			long deadlineEpochMillis = clock.epochMillis() + 30_000L;
			var context = new RequestContext(WorkloadProfile.BATCH, deadlineEpochMillis);
			var observedChunks = new AtomicInteger();
			db.setRangeReadChunkSizeObserverForTesting(_ -> {
				if (observedChunks.getAndIncrement() == 0) {
					firstChunk.countDown();
					awaitUninterruptibly(releaseFirstChunk);
				}
			});
			var first = Flux.from(db.getRangeAsyncInternal(0L,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					120_000L,
					context,
					WorkloadProfile.BATCH)).collectList().toFuture();
			assertTrue(firstChunk.await(5, SECONDS));
			assertTrue(awaitCondition(() -> db.getActiveRangeCursorCount() == 1
					&& db.getRetainedRangePermitCount() == 1, 5_000L));

			assertEquals(MILLISECONDS.toMicros(deadlineEpochMillis), nativeReadDeadlineMicros(db),
					"RocksDB ReadOptions must retain its absolute epoch deadline");

			var second = Flux.from(db.getRangeAsyncInternal(0L,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					120_000L,
					context,
					WorkloadProfile.BATCH)).collectList().toFuture();
			assertTrue(awaitCondition(() -> db.getRetainedRangeWaiterCount() == 1, 5_000L));
			assertFalse(second.isDone());

			clock.epochMillis.addAndGet(-3_600_000L);
			clock.nanoTime.addAndGet(MILLISECONDS.toNanos(30_001L));
			db.cleanupExpiredRangesNow();
			releaseFirstChunk.countDown();

			assertTrue(awaitCondition(() -> db.getActiveRangeCursorCount() == 0
					&& db.getRetainedRangeSnapshotCount() == 0
					&& db.getRetainedRangePermitCount() == 0
					&& db.getRetainedRangeWaiterCount() == 0, 5_000L),
					() -> "retained resources did not drain: cursors=" + db.getActiveRangeCursorCount()
							+ " snapshots=" + db.getRetainedRangeSnapshotCount()
							+ " permits=" + db.getRetainedRangePermitCount()
							+ " waiters=" + db.getRetainedRangeWaiterCount());
			assertDeadlineFailure(first);
			assertDeadlineFailure(second);
		} finally {
			releaseFirstChunk.countDown();
			db.setRangeReadChunkSizeObserverForTesting(null);
			db.closeTesting();
		}
	}

	@Test
	void forwardWallJumpCannotPrematurelyExpireARetainedCursor() throws Exception {
		var clock = new MutableClock(System.currentTimeMillis(), 0L);
		var scheduler = scheduler(clock, "retained-monotonic-forward");
		EmbeddedDB db = embeddedDb(scheduler, "retained-monotonic-forward");
		var firstChunk = new CountDownLatch(1);
		var releaseFirstChunk = new CountDownLatch(1);
		try {
			long columnId = db.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int index = 0; index < 8; index++) {
				db.put(0L, columnId, key(index), value(index), RequestType.none());
			}
			db.setRangeReadChunkSizeObserverForTesting(_ -> {
				firstChunk.countDown();
				awaitUninterruptibly(releaseFirstChunk);
			});
			var context = new RequestContext(WorkloadProfile.BATCH, clock.epochMillis() + 30_000L);
			var range = Flux.from(db.getRangeAsyncInternal(0L,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					120_000L,
					context,
					WorkloadProfile.BATCH)).collectList().toFuture();
			assertTrue(firstChunk.await(5, SECONDS));
			assertTrue(awaitCondition(() -> db.getActiveRangeCursorCount() == 1, 5_000L));

			clock.epochMillis.addAndGet(3_600_000L);
			clock.nanoTime.addAndGet(MILLISECONDS.toNanos(1L));
			db.cleanupExpiredRangesNow();

			assertFalse(range.isDone(), "a forward wall jump must not expire an unelapsed retained deadline");
			assertEquals(1, db.getActiveRangeCursorCount());
			releaseFirstChunk.countDown();
			assertEquals(8, range.get(5, SECONDS).size());
			assertTrue(awaitCondition(() -> db.getActiveRangeCursorCount() == 0
					&& db.getRetainedRangePermitCount() == 0, 5_000L));
		} finally {
			releaseFirstChunk.countDown();
			db.setRangeReadChunkSizeObserverForTesting(null);
			db.closeTesting();
		}
	}

	private EmbeddedDB embeddedDb(RWScheduler scheduler, String name) {
		try {
			Path config = Files.writeString(tempDir.resolve(name + ".conf"), """
					database.parallelism.workload.range-quantum-max-items = 1
					database.parallelism.workload.range-quantum-max-bytes = 1MiB
					database.parallelism.workload.range-quantum-max-duration = PT1S
					database.parallelism.workload.retained-analytical-snapshots = 1
					database.parallelism.workload.retained-snapshot-maximum-age = PT60S
					""");
			Constructor<EmbeddedDB> constructor = EmbeddedDB.class.getDeclaredConstructor(
					Path.class,
					String.class,
					Path.class,
					RWScheduler.class);
			constructor.setAccessible(true);
			return constructor.newInstance(tempDir.resolve(name),
					name,
					config,
					scheduler);
		} catch (Exception reflectionFailure) {
			throw new AssertionError(reflectionFailure);
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
					LongSupplier.class,
					LongSupplier.class);
			factory.setAccessible(true);
			return (RWScheduler) factory.invoke(null,
					1,
					1,
					1,
					8,
					8,
					name,
					(LongSupplier) clock::epochMillis,
					(LongSupplier) clock::nanoTime);
		} catch (ReflectiveOperationException reflectionFailure) {
			throw new AssertionError(reflectionFailure);
		}
	}

	private static long nativeReadDeadlineMicros(EmbeddedDB db) throws ReflectiveOperationException {
		Field activeResourcesField = EmbeddedDB.class.getDeclaredField("activeRangeResources");
		activeResourcesField.setAccessible(true);
		@SuppressWarnings("unchecked")
		var activeResources = (Set<Object>) activeResourcesField.get(db);
		Object retainedTask = activeResources.stream()
				.filter(resource -> resource.getClass().getSimpleName().startsWith("RetainedRange"))
				.findFirst()
				.orElseThrow();
		Field cursorField = retainedTask.getClass().getSuperclass().getDeclaredField("cursor");
		cursorField.setAccessible(true);
		Object cursor = cursorField.get(retainedTask);
		Field readOptionsField = cursor.getClass().getDeclaredField("readOptions");
		readOptionsField.setAccessible(true);
		return ((ReadOptions) readOptionsField.get(cursor)).deadline();
	}

	private static void assertDeadline(Throwable failure) {
		var rocks = assertInstanceOf(RocksDBException.class, failure);
		assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				rocks.getErrorUniqueId());
	}

	private static void assertDeadlineFailure(java.util.concurrent.CompletableFuture<?> future)
			throws Exception {
		var failure = org.junit.jupiter.api.Assertions.assertThrows(ExecutionException.class,
				() -> future.get(5, SECONDS));
		assertDeadline(failure.getCause());
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

	private static boolean awaitCondition(BooleanSupplier condition, long timeoutMillis)
			throws InterruptedException {
		long deadline = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		return condition.getAsBoolean();
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
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

}
