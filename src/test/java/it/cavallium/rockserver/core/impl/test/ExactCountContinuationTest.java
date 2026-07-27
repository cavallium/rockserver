package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class ExactCountContinuationTest {

	private static final int INITIAL_ENTRIES = 4_200;

	@TempDir
	Path tempDir;

	@Test
	void countUsesOneRetainedSnapshotAcrossYieldedQuantums() throws Exception {
		try (var connection = populatedConnection("snapshot-consistency")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstQuantum = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstQuantum.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount(), 30_000);
				assertTrue(firstQuantum.await(10, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				var ingest = connection.getSyncApi(RequestContext.ingest());
				for (int i = INITIAL_ENTRIES; i < INITIAL_ENTRIES + 32; i++) {
					ingest.put(0, columnId, key(i), value(i), RequestType.none());
				}
				release.countDown();
				assertEquals(INITIAL_ENTRIES, count.get(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 5_000));
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void cancellationClosesRetainedIteratorAndSnapshotImmediately() throws Exception {
		try (var connection = populatedConnection("snapshot-cancellation")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstQuantum = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstQuantum.countDown();
					await(release);
				}
			});
			try {
				var count = connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount(), 30_000);
				assertTrue(firstQuantum.await(10, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				assertTrue(count.cancel(false));
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 2_000));
				assertEquals(0, connection.getInternalDB().getActiveRangeCursorCount());
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void configuredMaximumSnapshotAgeClosesAStalledCount() throws Exception {
		var property = "it.cavallium.rockserver.workload.max-retained-snapshot-age-ms";
		var previous = System.getProperty(property);
		System.setProperty(property, "100");
		try (var connection = populatedConnection("snapshot-max-age")) {
			long columnId = connection.getSyncApi(RequestContext.analytical()).getColumnId("entries");
			var firstQuantum = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstQuantum.countDown();
					await(release);
				}
			});
			try {
				connection.getAsyncApi(RequestContext.analytical()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount(), Long.MAX_VALUE);
				assertTrue(firstQuantum.await(10, TimeUnit.SECONDS));
				assertTrue(awaitCondition(
						() -> connection.getInternalDB().getRetainedRangeSnapshotCount() == 0, 2_000));
			} finally {
				release.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
			}
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
	}

	private EmbeddedConnection populatedConnection(String name) throws Exception {
		var connection = new EmbeddedConnection(tempDir.resolve(name), name, null);
		var ingest = connection.getSyncApi(RequestContext.ingest());
		long columnId = ingest.createColumn("entries",
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		for (int i = 0; i < INITIAL_ENTRIES; i++) {
			ingest.put(0, columnId, key(i), value(i), RequestType.none());
		}
		return connection;
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition,
			long timeoutMillis) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		return condition.getAsBoolean();
	}
}
