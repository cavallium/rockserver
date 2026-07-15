package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.time.Duration;
import java.util.AbstractList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;

@Timeout(10)
class BatchWriteLifecycleTest {

	@TempDir
	Path tempDir;

	@Test
	void putBatchProcessingFailureCancelsUpstreamAndReleasesNativeState() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("put-failure"), "put-failure", null)) {
			long columnId = db.createColumn("data", fixedColumnSchema());
			var subscribed = new CountDownLatch(1);
			var cancelled = new CountDownLatch(1);
			var source = Flux.just(mismatchedBatch(false))
					.concatWith(Flux.never())
					.doOnSubscribe(ignored -> subscribed.countDown())
					.doOnCancel(cancelled::countDown);

			var future = db.getAsyncApi().putBatchAsync(columnId, source, PutBatchMode.WRITE_BATCH);
			assertTrue(subscribed.await(2, SECONDS));

			var failure = assertThrows(ExecutionException.class, () -> future.get(2, SECONDS));
			assertBatchMismatch(failure.getCause());
			assertTrue(cancelled.await(2, SECONDS), "processing errors must cancel a non-terminating source");
			awaitNoPendingOperations(db);
		}
	}

	@Test
	void mergeBatchProcessingFailureRollsBackItsBucketTransaction() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("merge-failure"), "merge-failure", null)) {
			long columnId = db.createColumn("bucketed", bucketedColumnSchema());
			var subscribed = new CountDownLatch(1);
			var cancelled = new CountDownLatch(1);
			var source = Flux.just(mismatchedBatch(true))
					.concatWith(Flux.never())
					.doOnSubscribe(ignored -> subscribed.countDown())
					.doOnCancel(cancelled::countDown);

			var future = db.getAsyncApi().mergeBatchAsync(columnId, source, MergeBatchMode.MERGE_WRITE_BATCH);
			assertTrue(subscribed.await(2, SECONDS));

			var failure = assertThrows(ExecutionException.class, () -> future.get(2, SECONDS));
			assertBatchMismatch(failure.getCause());
			assertTrue(cancelled.await(2, SECONDS), "processing errors must cancel the merge source");
			awaitNoPendingOperations(db);
			assertEquals(0, db.getInternalDB().getOpenTransactionsCount());
		}
	}

	@Test
	void cancellingTheReturnedFutureCancelsTheSourceAndReleasesTheWriter() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("put-cancel"), "put-cancel", null)) {
			long columnId = db.createColumn("data", fixedColumnSchema());
			var subscribed = new CountDownLatch(1);
			var cancelled = new CountDownLatch(1);
			var source = Flux.<KVBatch>never()
					.doOnSubscribe(ignored -> subscribed.countDown())
					.doOnCancel(cancelled::countDown);

			var future = db.getAsyncApi().putBatchAsync(columnId, source, PutBatchMode.WRITE_BATCH);
			assertTrue(subscribed.await(2, SECONDS));
			assertTrue(future.cancel(false));

			assertTrue(cancelled.await(2, SECONDS));
			awaitNoPendingOperations(db);
		}
	}

	@Test
	void cancellingWhilePutCallbackIsActiveDefersNativeCloseUntilCallbackReturns() throws Exception {
		var callbackEntered = new CountDownLatch(1);
		var releaseCallback = new CountDownLatch(1);
		var sourceCancelled = new CountDownLatch(1);
		var droppedError = new AtomicReference<Throwable>();
		Hooks.onErrorDropped(error -> droppedError.compareAndSet(null, error));
		try (var db = new EmbeddedConnection(tempDir.resolve("put-active-cancel"), "put-active-cancel", null)) {
			long columnId = db.createColumn("data", fixedColumnSchema());
			var key = new Keys(new Buf[] {Buf.wrap(new byte[] {0, 0, 0, 7})});
			var value = Buf.wrap(new byte[] {7});
			var source = Flux.just(blockingBatch(key, value, callbackEntered, releaseCallback))
					.concatWith(Flux.never())
					.doOnCancel(sourceCancelled::countDown);

			var future = db.getAsyncApi().putBatchAsync(columnId, source, PutBatchMode.WRITE_BATCH);
			assertTrue(callbackEntered.await(2, SECONDS), "the write callback did not become active");
			assertEquals(1, db.getInternalDB().getPendingOpsCount());

			assertTrue(future.cancel(false));
			assertTrue(sourceCancelled.await(2, SECONDS), "future cancellation must cancel the source");
			assertEquals(1, db.getInternalDB().getPendingOpsCount(),
					"an active writer must remain counted until its callback exits");

			releaseCallback.countDown();
			awaitNoPendingOperations(db);

			assertTrue(future.isCancelled());
			assertNull(droppedError.get(), "closing an active writer must not cause a dropped native error");
			assertFalse(db.get(0, columnId, key, it.cavallium.rockserver.core.common.RequestType.exists()),
					"a cancelled write batch must not publish its pending write");
		} finally {
			releaseCallback.countDown();
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	void cancellingWhileBucketedMergeCallbackIsActiveRollsBackAfterCallbackReturns() throws Exception {
		var callbackEntered = new CountDownLatch(1);
		var releaseCallback = new CountDownLatch(1);
		var sourceCancelled = new CountDownLatch(1);
		var droppedError = new AtomicReference<Throwable>();
		Hooks.onErrorDropped(error -> droppedError.compareAndSet(null, error));
		try (var db = new EmbeddedConnection(tempDir.resolve("merge-active-cancel"), "merge-active-cancel", null)) {
			long columnId = db.createColumn("bucketed", bucketedColumnSchema());
			var key = new Keys(new Buf[] {
					Buf.wrap(new byte[] {0, 0, 0, 8}),
					Buf.wrap(new byte[] {8})
			});
			var value = Buf.wrap(new byte[] {8});
			var source = Flux.just(blockingBatch(key, value, callbackEntered, releaseCallback))
					.concatWith(Flux.never())
					.doOnCancel(sourceCancelled::countDown);

			var future = db.getAsyncApi().mergeBatchAsync(
					columnId, source, MergeBatchMode.MERGE_WRITE_BATCH);
			assertTrue(callbackEntered.await(2, SECONDS), "the merge callback did not become active");
			assertEquals(2, db.getInternalDB().getPendingOpsCount(),
					"the batch state and its private transaction must both be counted");

			assertTrue(future.cancel(true));
			assertTrue(sourceCancelled.await(2, SECONDS), "future cancellation must cancel the merge source");
			assertEquals(2, db.getInternalDB().getPendingOpsCount(),
					"an active merge and its transaction must remain alive until the callback exits");

			releaseCallback.countDown();
			awaitNoPendingOperations(db);

			assertTrue(future.isCancelled());
			assertEquals(0, db.getInternalDB().getOpenTransactionsCount());
			assertNull(droppedError.get(), "deferred rollback must not race the active merge callback");
			assertFalse(db.get(0, columnId, key, it.cavallium.rockserver.core.common.RequestType.exists()),
					"the private merge transaction must be rolled back on cancellation");
		} finally {
			releaseCallback.countDown();
			Hooks.resetOnErrorDropped();
		}
	}

	@Test
	void asynchronousInitializationFailureCompletesTheFutureAndBalancesOperations() throws Exception {
		try (var db = new EmbeddedConnection(tempDir.resolve("put-init-failure"), "put-init-failure", null)) {
			var future = db.getAsyncApi().putBatchAsync(Long.MAX_VALUE, Flux.never(), PutBatchMode.WRITE_BATCH);

			var failure = assertThrows(ExecutionException.class, () -> future.get(2, SECONDS));
			var rocksError = assertInstanceOf(RocksDBException.class, failure.getCause());
			assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, rocksError.getErrorUniqueId());
			awaitNoPendingOperations(db);
		}
	}

	private static ColumnSchema fixedColumnSchema() {
		return ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
	}

	private static ColumnSchema bucketedColumnSchema() {
		return ColumnSchema.of(
				IntList.of(Integer.BYTES),
				ObjectList.of(ColumnHashType.XXHASH32),
				true,
				null,
				null,
				"it.cavallium.rockserver.core.impl.MyStringAppendOperator"
		);
	}

	private static KVBatch mismatchedBatch(boolean bucketed) {
		var fixedKey = Buf.wrap(new byte[] {0, 0, 0, 1});
		var keys = bucketed
				? new Keys(new Buf[] {fixedKey, Buf.wrap(new byte[] {2})})
				: new Keys(new Buf[] {fixedKey});
		return new KVBatch.KVBatchRef(List.of(keys), List.of());
	}

	private static KVBatch blockingBatch(Keys key,
			Buf value,
			CountDownLatch callbackEntered,
			CountDownLatch releaseCallback) {
		List<Keys> blockingKeys = new AbstractList<>() {
			@Override
			public Keys get(int index) {
				if (index != 0) {
					throw new IndexOutOfBoundsException(index);
				}
				callbackEntered.countDown();
				boolean interrupted = false;
				while (true) {
					try {
						releaseCallback.await();
						break;
					} catch (InterruptedException ignored) {
						// Cancellation may interrupt the scheduler worker. Keep the callback
						// active until the test explicitly releases it so close-vs-active is deterministic.
						interrupted = true;
					}
				}
				if (interrupted) {
					Thread.currentThread().interrupt();
				}
				return key;
			}

			@Override
			public int size() {
				return 1;
			}
		};
		return new KVBatch() {
			@Override
			public List<Keys> keys() {
				return blockingKeys;
			}

			@Override
			public List<Buf> values() {
				return List.of(value);
			}
		};
	}

	private static void assertBatchMismatch(Throwable failure) {
		var rocksError = assertInstanceOf(RocksDBException.class, failure);
		assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, rocksError.getErrorUniqueId());
		assertTrue(rocksError.getMessage().contains("Batch key/value count mismatch"));
	}

	private static void awaitNoPendingOperations(EmbeddedConnection db) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(2).toNanos();
		while (db.getInternalDB().getPendingOpsCount() != 0 && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		assertEquals(0, db.getInternalDB().getPendingOpsCount());
	}
}
