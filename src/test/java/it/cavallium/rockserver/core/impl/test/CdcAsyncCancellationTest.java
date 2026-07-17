package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.WriteBatchIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.WriteBatch;

@Timeout(30)
class CdcAsyncCancellationTest {

	@TempDir
	Path tempDir;

	@Test
	void cancellingQueuedPreparationReleasesItsShutdownLeaseWithoutRunningLater() throws Exception {
		try (var db = new EmbeddedDB(tempDir.resolve("queued"), "cdc-cancel-queued", null)) {
			var cdcStarted = new CountDownLatch(1);
			var releaseCdc = new CountDownLatch(1);
			db.getScheduler().cdc().schedule(() -> {
				cdcStarted.countDown();
				awaitUninterruptibly(releaseCdc);
			});
			assertTrue(cdcStarted.await(5, TimeUnit.SECONDS));

			var delivered = new AtomicBoolean();
			var subscription = db.cdcPollBatchAsyncInternal("not-needed", 1L, 1)
					.subscribe(_ -> delivered.set(true),
							_ -> delivered.set(true),
							() -> delivered.set(true));
			try {
				await(() -> db.getPendingOpsCount() == 1L,
						"queued CDC preparation never acquired its shutdown lease");

				subscription.dispose();
				await(() -> db.getPendingOpsCount() == 0L,
						"cancelling queued CDC preparation leaked its shutdown lease");
			} finally {
				subscription.dispose();
				releaseCdc.countDown();
			}
			var laneDrained = new CountDownLatch(1);
			db.getScheduler().cdc().schedule(laneDrained::countDown);
			assertTrue(laneDrained.await(5, TimeUnit.SECONDS));
			assertEquals(0L, db.getPendingOpsCount(),
					"a cancelled queued task ran later and released the lease twice");
			assertFalse(delivered.get(), "a cancelled CDC poll delivered a terminal signal");
		}
	}

	@Test
	void oneBlockedCleanupDoesNotStallCdcWalPublication() throws Exception {
		try (var db = new EmbeddedDB(tempDir.resolve("control-overlap"), "cdc-control-overlap", null)) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0,
					columnId,
					new Keys(new Buf[] {Buf.wrap(new byte[] {0, 0, 0, 1})}),
					Buf.wrap(new byte[] {1}),
					RequestType.none());

			var cleanupStarted = new CountDownLatch(1);
			var releaseCleanup = new CountDownLatch(1);
			db.getScheduler().control().schedule(() -> {
				cleanupStarted.countDown();
				awaitUninterruptibly(releaseCleanup);
			});
			try {
				assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));
				var batch = db.cdcPollBatchAsyncInternal("sub", startSeq, 100)
						.toFuture()
						.get(5, TimeUnit.SECONDS);
				assertFalse(batch.events().isEmpty(),
						"blocked iterator cleanup stalled the independent CDC publication lane");
			} finally {
				releaseCleanup.countDown();
			}
		}
	}

	@Test
	void cancellingRunningPollKeepsItsShutdownLeaseUntilNativeWorkExits() throws Exception {
		try (var db = new BlockingParserEmbeddedDB(tempDir.resolve("running"), "cdc-cancel-running")) {
			long columnId = db.createColumn("data", ColumnSchema.of(
					IntArrayList.of(Integer.BYTES),
					new ObjectArrayList<>(),
					true,
					null,
					null,
					null));
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0,
					columnId,
					new Keys(new Buf[] {Buf.wrap(new byte[] {0, 0, 0, 1})}),
					Buf.wrap(new byte[] {1}),
					RequestType.none());

			var delivered = new AtomicBoolean();
			var subscription = db.cdcPollBatchAsyncInternal("sub", startSeq, 100)
					.subscribe(_ -> delivered.set(true), _ -> delivered.set(true));
			try {
				assertTrue(db.parserStarted.await(5, TimeUnit.SECONDS), "CDC parser never started");
				await(() -> db.getPendingOpsCount() == 1L,
						"CDC preparation did not release its lease after scheduling the native page");

				subscription.dispose();
				assertEquals(1L, db.getPendingOpsCount(),
						"cancelling a running native step released its shutdown lease too early");
			} finally {
				subscription.dispose();
				db.releaseParser.countDown();
			}
			await(() -> db.getPendingOpsCount() == 0L,
					"running CDC work did not release its shutdown lease on exit");
			assertTrue(db.completedParses.get() >= 1,
					"the in-flight native poll did not finish after cancellation");
			assertFalse(delivered.get(), "a cancelled CDC poll delivered a terminal signal");
		}
	}

	private static void await(BooleanSupplier condition, String failureMessage) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertTrue(condition.getAsBoolean(), failureMessage);
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		while (true) {
			try {
				latch.await();
				return;
			} catch (InterruptedException _) {
				// The test deliberately keeps the simulated native call alive after cancellation.
			}
		}
	}

	private static final class BlockingParserEmbeddedDB extends EmbeddedDB {

		private final CountDownLatch parserStarted = new CountDownLatch(1);
		private final CountDownLatch releaseParser = new CountDownLatch(1);
		private final AtomicInteger completedParses = new AtomicInteger();

		private BlockingParserEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected WriteBatchIterator.Cursor createCdcWriteBatchCursor(WriteBatch writeBatch)
				throws org.rocksdb.RocksDBException {
			parserStarted.countDown();
			awaitUninterruptibly(releaseParser);
			completedParses.incrementAndGet();
			return super.createCdcWriteBatchCursor(writeBatch);
		}
	}
}
