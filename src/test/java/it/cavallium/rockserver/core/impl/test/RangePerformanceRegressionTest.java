package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Timeout(60)
class RangePerformanceRegressionTest {

	@TempDir
	Path tempDir;

	@Test
	void schedulingSlicesReuseOneIteratorForRangeCountAndEndpoints() throws Exception {
		final int entries = 4_200;
		try (var connection = new EmbeddedConnection(tempDir.resolve("reuse-db"), "range-reuse", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			assertTrue(internal.isExpiredRangeCleanupScheduledForTesting(),
					"stalled range cleanup must be registered on the production leak scheduler");
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, key(i), value(i), RequestType.none());
			}
			var iteratorOpens = new AtomicInteger();
			internal.setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);

			var rows = Flux.from(connection.getAsyncApi().getRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					10_000))
					.collectList()
					.block(Duration.ofSeconds(20));
			assertNotNull(rows);
			assertEquals(entries, rows.size());
			assertEquals(1, iteratorOpens.get(),
					"scheduler slices must not recreate the native range iterator");

			iteratorOpens.set(0);
			long count = connection.getAsyncApi().reduceRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.entriesCount(),
					10_000).get(20, TimeUnit.SECONDS);
			assertEquals(entries, count);
			assertEquals(1, iteratorOpens.get(),
					"exact count must aggregate physical chunks with one iterator");

			iteratorOpens.set(0);
			var endpoints = connection.getAsyncApi().reduceRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.firstAndLast(),
					10_000).get(20, TimeUnit.SECONDS);
			assertEquals(key(0), endpoints.first().keys());
			assertEquals(key(entries - 1), endpoints.last().keys());
			assertEquals(1, iteratorOpens.get(),
					"first and last must be two seeks on one iterator/view");
		}
	}

	@Test
	void cancellingExactCountStopsTheScanAndReleasesTheCursor() throws Exception {
		final int entries = 5_000;
		try (var connection = new EmbeddedConnection(tempDir.resolve("cancel-count-db"),
				"range-cancel-count", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, key(i), value(i), RequestType.none());
			}

			var firstChunk = new CountDownLatch(1);
			var releaseChunk = new CountDownLatch(1);
			var first = new AtomicBoolean(true);
			internal.setRangeCountChunkObserverForTesting(() -> {
				if (first.compareAndSet(true, false)) {
					firstChunk.countDown();
					try {
						releaseChunk.await(10, TimeUnit.SECONDS);
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
						throw new RuntimeException(interrupted);
					}
				}
			});

			var count = connection.getAsyncApi().reduceRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.entriesCount(),
					60_000);
			assertTrue(firstChunk.await(10, TimeUnit.SECONDS));
			assertTrue(count.cancel(true), "the cancellable count future must stop its source");
			releaseChunk.countDown();

			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (internal.getActiveRangeCursorCount() != 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, internal.getActiveRangeCursorCount(),
					"cancellation must not pin a range iterator or snapshot until the read deadline");
		}
	}

	@Test
	void valueRangeChunksHaveABoundedDecodedByteFootprint() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("byte-bounded-range-db"),
				"range-byte-bound", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var largeValue = Buf.wrap(new byte[1_100_000]);
			for (int i = 0; i < 3; i++) {
				api.put(0, columnId, key(i), largeValue, RequestType.none());
			}
			var chunkSizes = new ArrayList<Integer>();
			internal.setRangeReadChunkSizeObserverForTesting(chunkSizes::add);

			var rows = Flux.from(connection.getAsyncApi().getRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					10_000)).collectList().block(Duration.ofSeconds(20));

			assertNotNull(rows);
			assertEquals(3, rows.size());
			assertEquals(List.of(2, 1, 0), chunkSizes,
					"decoded range pages must stop at the 2 MiB soft byte boundary");
		}
	}

	@Test
	void laterDemandKeepsTheOriginalPointInTimeView() throws Exception {
		final int entries = 1_100;
		try (var connection = new EmbeddedConnection(tempDir.resolve("snapshot-db"), "range-snapshot", null)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, key(i * 2), value(i), RequestType.none());
			}
			var internal = connection.getInternalDB();
			var decodedPages = new AtomicInteger();
			internal.setRangeReadChunkSizeObserverForTesting(_ -> decodedPages.incrementAndGet());

			var range = Flux.from(connection.getAsyncApi().getRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					10_000));
			StepVerifier.create(range, 1)
					.assertNext(first -> assertEquals(key(0), first.keys()))
					.thenAwait(Duration.ofMillis(250))
					.then(() -> assertEquals(1, decodedPages.get(),
							"one requested row must not predecode a second 1,024-row page"))
					.then(() -> api.put(0, columnId, key(2_101), value(-1), RequestType.none()))
					.thenRequest(Long.MAX_VALUE)
					.expectNextCount(entries - 1L)
					.verifyComplete();
		}
	}

	@Test
	void longRangeKeepsOneOriginalPointInTimeViewAcrossSchedulingSlices() throws Exception {
		final int entries = 3_000;
		try (var connection = new EmbeddedConnection(tempDir.resolve("refresh-snapshot-db"),
				"range-refresh-snapshot", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, key(i * 2), value(i), RequestType.none());
			}
			var iteratorOpens = new AtomicInteger();
			internal.setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);

			var range = Flux.from(connection.getAsyncApi().getRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					10_000));
			StepVerifier.create(range, 1)
					.assertNext(first -> assertEquals(key(0), first.keys()))
					.then(() -> api.put(0, columnId, key(5_001), value(-1), RequestType.none()))
					.thenRequest(Long.MAX_VALUE)
					.expectNextCount(entries - 1L)
					.verifyComplete();
			assertEquals(1, iteratorOpens.get(),
					"one logical range must retain one native iterator and point-in-time view");
		}
	}

	@Test
	void stalledRangeReleasesItsSnapshotAfterTheReadDeadline() throws Exception {
		// Keep more data than the one in-flight chunk plus the single delivery-side
		// prefetch, so expiry is observed when buffered entries have drained.
		final int entries = 5_000;
		try (var connection = new EmbeddedConnection(tempDir.resolve("expired-snapshot-db"),
				"range-expired-snapshot", null)) {
			var api = connection.getSyncApi();
			var internal = connection.getInternalDB();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			for (int i = 0; i < entries; i++) {
				api.put(0, columnId, key(i), value(i), RequestType.none());
			}

			var range = Flux.from(connection.getAsyncApi().getRangeAsync(0,
					columnId,
					null,
					null,
					false,
					RequestType.allInRange(),
					500));
			StepVerifier.create(range, 1)
					.assertNext(first -> assertEquals(key(0), first.keys()))
					.thenAwait(Duration.ofMillis(600))
					.then(internal::cleanupExpiredRangesNow)
					.then(() -> assertEquals(0, internal.getActiveRangeCursorCount()))
					.thenRequest(Long.MAX_VALUE)
					.thenConsumeWhile(ignored -> true)
					.expectErrorSatisfies(error -> {
						var rocksError = org.junit.jupiter.api.Assertions.assertInstanceOf(
								RocksDBException.class, error);
						assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, rocksError.getErrorUniqueId());
					})
					.verify(Duration.ofSeconds(10));
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
