package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

class ColumnHandleLifecycleTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path tempDir;

	private EmbeddedDB db;
	private ExecutorService executor;

	@BeforeEach
	void setUp() throws Exception {
		db = new EmbeddedDB(tempDir, "column-handle-lifecycle", null);
		executor = Executors.newFixedThreadPool(3);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (db != null) {
			db.setColumnUseAcquiredObserverForTesting(null);
			db.closeTesting();
		}
		if (executor != null) {
			executor.shutdownNow();
			assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
		}
	}

	@Test
	@Timeout(15)
	void deleteWaitsForActivePointUseRejectsNewUsesAndAllowsRecreate() throws Exception {
		String name = "point-use";
		long columnId = db.createColumn(name, SCHEMA);
		var useAcquired = new CountDownLatch(1);
		var releaseUse = new CountDownLatch(1);
		var blockOnce = new AtomicBoolean();
		db.setColumnUseAcquiredObserverForTesting(observedColumnId -> {
			if (observedColumnId == columnId && blockOnce.compareAndSet(false, true)) {
				useAcquired.countDown();
				awaitLatch(releaseUse);
			}
		});

		Future<Long> activeUse = executor.submit(() -> db.estimateNumKeys(columnId));
		assertTrue(useAcquired.await(5, TimeUnit.SECONDS), "point operation did not acquire its column use");
		Future<?> deletion = executor.submit(() -> db.deleteColumn(columnId));

		try {
			awaitRetirement(name);
			assertFalse(deletion.isDone(), "delete completed while a column use was still active");
			assertColumnNotFound(() -> db.estimateNumKeys(columnId));
		} finally {
			releaseUse.countDown();
		}

		activeUse.get(5, TimeUnit.SECONDS);
		deletion.get(5, TimeUnit.SECONDS);
		db.setColumnUseAcquiredObserverForTesting(null);

		long recreatedId = db.createColumn(name, SCHEMA);
		assertEquals(recreatedId, db.getColumnId(name));
		db.estimateNumKeys(recreatedId);
	}

	@Test
	@Timeout(15)
	void deleteWaitsForExplicitIteratorLease() throws Exception {
		String name = "iterator-use";
		long columnId = db.createColumn(name, SCHEMA);
		long iteratorId = db.openIterator(0L, columnId, null, null, false, 30_000L);
		Future<?> deletion = executor.submit(() -> db.deleteColumn(columnId));

		try {
			awaitRetirement(name);
			assertFalse(deletion.isDone(), "delete completed while the iterator retained the column handle");
			assertColumnNotFound(() -> db.estimateNumKeys(columnId));
		} finally {
			db.closeIterator(iteratorId);
		}

		deletion.get(5, TimeUnit.SECONDS);
	}

	@Test
	@Timeout(15)
	void deleteWaitsForReactiveRangeCursorLease() throws Exception {
		String name = "range-use";
		long columnId = db.createColumn(name, SCHEMA);
		db.put(0L, columnId, key(1L), Buf.wrap(new byte[] {1}), RequestType.none());
		db.put(0L, columnId, key(2L), Buf.wrap(new byte[] {2}), RequestType.none());
		var firstItem = new CountDownLatch(1);
		var releaseRange = new CountDownLatch(1);
		var blockOnce = new AtomicBoolean();
		var rangeCompletion = Flux.from(db.getRangeAsyncInternal(0L,
				columnId,
				null,
				null,
				false,
				RequestType.allInRange(),
				30_000L))
				.doOnNext(_ -> {
					if (blockOnce.compareAndSet(false, true)) {
						firstItem.countDown();
						awaitLatch(releaseRange);
					}
				})
				.then()
				.toFuture();
		assertTrue(firstItem.await(5, TimeUnit.SECONDS), "range cursor did not emit its first item");
		Future<?> deletion = executor.submit(() -> db.deleteColumn(columnId));

		try {
			awaitRetirement(name);
			assertFalse(deletion.isDone(), "delete completed while the range cursor retained the column handle");
		} finally {
			releaseRange.countDown();
		}

		rangeCompletion.get(5, TimeUnit.SECONDS);
		deletion.get(5, TimeUnit.SECONDS);
	}

	@Test
	@Timeout(15)
	void interruptedRetirementStillDrainsAndDropsColumn() throws Exception {
		String name = "interrupted-retirement";
		long columnId = db.createColumn(name, SCHEMA);
		var useAcquired = new CountDownLatch(1);
		var releaseUse = new CountDownLatch(1);
		var blockOnce = new AtomicBoolean();
		db.setColumnUseAcquiredObserverForTesting(observedColumnId -> {
			if (observedColumnId == columnId && blockOnce.compareAndSet(false, true)) {
				useAcquired.countDown();
				awaitLatch(releaseUse);
			}
		});
		Future<Long> activeUse = executor.submit(() -> db.estimateNumKeys(columnId));
		assertTrue(useAcquired.await(5, TimeUnit.SECONDS));

		var deletionFailure = new AtomicReference<Throwable>();
		var deletionThread = Thread.ofPlatform().unstarted(() -> {
			try {
				db.deleteColumn(columnId);
			} catch (Throwable error) {
				deletionFailure.set(error);
			}
		});
		deletionThread.start();
		try {
			awaitRetirement(name);
			deletionThread.interrupt();
		} finally {
			releaseUse.countDown();
		}

		activeUse.get(5, TimeUnit.SECONDS);
		deletionThread.join(5_000L);
		assertFalse(deletionThread.isAlive(), "interrupted delete abandoned column retirement");
		assertTrue(deletionThread.isInterrupted(), "delete did not restore its interrupt status after draining");
		assertNull(deletionFailure.get());
		assertColumnNotFound(() -> db.getColumnId(name));
	}

	private void awaitRetirement(String columnName) throws Exception {
		executor.submit(() -> {
			for (;;) {
				try {
					db.getColumnId(columnName);
					Thread.onSpinWait();
				} catch (RocksDBException error) {
					assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
					return;
				}
			}
		}).get(5, TimeUnit.SECONDS);
	}

	private static void assertColumnNotFound(ThrowingOperation operation) {
		var error = assertThrows(RocksDBException.class, operation::run);
		assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
	}

	private static void awaitLatch(CountDownLatch latch) {
		try {
			if (!latch.await(10, TimeUnit.SECONDS)) {
				throw new AssertionError("timed out waiting to release the column use");
			}
		} catch (InterruptedException error) {
			Thread.currentThread().interrupt();
			throw new AssertionError("interrupted while retaining the column use", error);
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	@FunctionalInterface
	private interface ThrowingOperation {

		void run() throws Exception;
	}
}
