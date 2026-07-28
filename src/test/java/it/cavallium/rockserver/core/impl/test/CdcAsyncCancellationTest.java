package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WriteBatchIterator;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
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

	@Test
	void cancellationDuringWalPublicationKeepsTheNativeLeaseUntilFlushExits() throws Exception {
		try (var db = new BlockingStageEmbeddedDB(tempDir.resolve("publication"), "cdc-cancel-publication")) {
			long columnId = createColumn(db, "data");
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			db.blockPublication.set(true);

			var subscription = db.cdcPollBatchAsyncInternal("sub", startSeq, 10).subscribe();
			try {
				assertTrue(db.stageStarted.await(5, TimeUnit.SECONDS));
				await(() -> db.getPendingOpsCount() == 1L,
						"CDC publication did not retain its shutdown lease");
				assertEquals(1, db.getScheduler().poolSnapshot(RWScheduler.Pool.WRITE)
						.activeByProfile().get(WorkloadProfile.CDC));
				subscription.dispose();
				assertEquals(1L, db.getPendingOpsCount());
			} finally {
				subscription.dispose();
				db.releaseStage.countDown();
			}
			await(() -> db.getPendingOpsCount() == 0L,
					"cancelled WAL publication did not drain");
			assertEquals(0, db.getActiveCdcPollCursorCount());
		}
	}

	@Test
	void writeAfterFlushBoundaryIsDeferredToTheNextFixedTailPoll() throws Exception {
		try (var db = new BlockingStageEmbeddedDB(tempDir.resolve("flush-boundary"), "cdc-flush-boundary")) {
			long columnId = createColumn(db, "data");
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.blockPublication.set(true);

			var firstPoll = db.cdcPollBatchAsyncInternal("sub", startSeq, 10).toFuture();
			assertTrue(db.stageStarted.await(5, TimeUnit.SECONDS));
			db.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			db.releaseStage.countDown();

			var first = firstPoll.get(10, TimeUnit.SECONDS);
			assertTrue(first.events().isEmpty(),
					"a write after the captured flush boundary must not extend the fixed tail");
			var second = db.cdcPollBatchAsyncInternal("sub", startSeq, 10)
					.block(java.time.Duration.ofSeconds(10));
			assertEquals(1, second.events().size());
			assertEquals(0, db.getActiveCdcPollCursorCount());
		}
	}

	@Test
	void cancellationDuringPrefixlessDiscoveryClosesWithoutOpeningAPollCursor() throws Exception {
		try (var db = new BlockingStageEmbeddedDB(tempDir.resolve("discovery"), "cdc-cancel-discovery")) {
			long columnId = createColumn(db, "data");
			db.cdcCreate("sub", null, List.of(columnId), false);
			db.blockDiscovery.set(true);

			var subscription = db.cdcPollBatchAsyncInternal("sub", 0L, 10).subscribe();
			try {
				assertTrue(db.stageStarted.await(5, TimeUnit.SECONDS));
				assertEquals(1, db.getScheduler().poolSnapshot(RWScheduler.Pool.READ)
						.activeByProfile().get(WorkloadProfile.CDC));
				subscription.dispose();
				assertEquals(1L, db.getPendingOpsCount(),
						"running discovery must own its lease until the native probe exits");
			} finally {
				subscription.dispose();
				db.releaseStage.countDown();
			}
			await(() -> db.getPendingOpsCount() == 0L,
					"cancelled prefixless discovery did not drain");
			assertEquals(0, db.getActiveCdcPollCursorCount());
		}
	}

	@Test
	void cancellationDuringLatestValueFanOutClosesCursorAfterNativeResolution() throws Exception {
		try (var db = new EmbeddedDB(tempDir.resolve("latest-values"), "cdc-cancel-latest-values", null)) {
			long columnId = createColumn(db, "data");
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), true);
			db.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			var resolutionStarted = new CountDownLatch(1);
			var releaseResolution = new CountDownLatch(1);
			db.setCdcLatestValueResolutionObserverForTesting(() -> {
				resolutionStarted.countDown();
				awaitUninterruptibly(releaseResolution);
			});

			var subscription = db.cdcPollBatchAsyncInternal("sub", startSeq, 10).subscribe();
			try {
				assertTrue(resolutionStarted.await(5, TimeUnit.SECONDS));
				assertEquals(1, db.getScheduler().poolSnapshot(RWScheduler.Pool.READ)
						.activeByProfile().get(WorkloadProfile.CDC));
				subscription.dispose();
				assertEquals(1L, db.getPendingOpsCount(),
						"latest-value fan-out released its running lease too early");
			} finally {
				subscription.dispose();
				releaseResolution.countDown();
				db.setCdcLatestValueResolutionObserverForTesting(null);
			}
			await(() -> db.getPendingOpsCount() == 0L && db.getActiveCdcPollCursorCount() == 0,
					"cancelled latest-value resolution leaked its cursor or lease");
		}
	}

	@Test
	void cancellationBetweenContinuationsClosesRetainedCursor() throws Exception {
		Path config = tempDir.resolve("continuation.conf");
		Files.writeString(config, """
				database.parallelism.workload.cdc-quantum-max-mutations = 1
				database.parallelism.workload.cdc-quantum-max-duration = PT1S
				""");
		try (var db = new EmbeddedDB(tempDir.resolve("continuation"), "cdc-cancel-continuation", config)) {
			long selected = createColumn(db, "selected");
			long ignored = createColumn(db, "ignored");
			long startSeq = db.cdcCreate("sub", null, List.of(selected), false);
			long tx = db.openTransaction(10_000);
			for (int i = 0; i < 3; i++) {
				db.put(tx, ignored, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
			}
			db.put(tx, selected, key(9), Buf.wrap(new byte[] {9}), RequestType.none());
			assertTrue(db.closeTransaction(tx, true));

			var continuationStarted = new CountDownLatch(1);
			var releaseContinuation = new CountDownLatch(1);
			db.setCdcContinuationObserverForTesting(() -> {
				continuationStarted.countDown();
				awaitUninterruptibly(releaseContinuation);
			});
			var subscription = db.cdcPollBatchAsyncInternal("sub", startSeq, 10).subscribe();
			try {
				assertTrue(continuationStarted.await(5, TimeUnit.SECONDS));
				subscription.dispose();
			} finally {
				subscription.dispose();
				releaseContinuation.countDown();
				db.setCdcContinuationObserverForTesting(null);
			}
			await(() -> db.getPendingOpsCount() == 0L && db.getActiveCdcPollCursorCount() == 0,
					"cancellation between CDC quanta leaked the retained cursor");
		}
	}

	@Test
	void cancelledCommitRemainsMustCompleteAndHasNoCallerDeadline() throws Exception {
		try (var db = new EmbeddedDB(tempDir.resolve("commit"), "cdc-cancel-commit", null)) {
			long columnId = createColumn(db, "data");
			db.cdcCreate("sub", 1L, List.of(columnId), false);
			var commitLoaded = new CountDownLatch(1);
			var releaseCommit = new CountDownLatch(1);
			db.setCdcMetadataLoadedObserverForTesting((operation, id) -> {
				if (operation.equals("commit") && id.equals("sub")) {
					commitLoaded.countDown();
					awaitUninterruptibly(releaseCommit);
				}
			});

			var future = db.cdcCommitAsyncInternal("sub", 42L);
			try {
				assertTrue(commitLoaded.await(5, TimeUnit.SECONDS));
				assertEquals(1, db.getScheduler().poolSnapshot(RWScheduler.Pool.WRITE)
						.activeByProfile().get(WorkloadProfile.CDC));
				assertTrue(future.cancel(true));
				assertTrue(db.getPendingOpsCount() >= 1L,
						"cancelled must-complete commit released shutdown accounting early");
			} finally {
				releaseCommit.countDown();
				db.setCdcMetadataLoadedObserverForTesting(null);
			}
			await(() -> db.getPendingOpsCount() == 0L,
					"must-complete CDC commit did not drain");
			assertEquals(OptionalLong.of(42L), db.cdcGetLastCommittedSequence("sub"));
		}
	}

	@Test
	@ResourceLock(Resources.SYSTEM_PROPERTIES)
	void shutdownClosesRunningCdcCursorAndDrainsAllLeases() throws Exception {
		String timeoutProperty = "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
		String previousTimeout = System.getProperty(timeoutProperty);
		var db = new BlockingParserEmbeddedDB(tempDir.resolve("shutdown"), "cdc-shutdown");
		System.setProperty(timeoutProperty, "0");
		var subscription = new AtomicReference<reactor.core.Disposable>();
		var databaseClosed = new AtomicBoolean();
		try {
			long columnId = createColumn(db, "data");
			long startSeq = db.cdcCreate("sub", null, List.of(columnId), false);
			db.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			subscription.set(db.cdcPollBatchAsyncInternal("sub", startSeq, 10).subscribe());
			assertTrue(db.parserStarted.await(5, TimeUnit.SECONDS));
			var forcedShutdownStarted = new CountDownLatch(1);
			db.setForcedShutdownObserverForTesting(forcedShutdownStarted::countDown);

			var closed = CompletableFuture.runAsync(() -> {
				try {
					db.closeTesting();
					databaseClosed.set(true);
				} catch (IOException error) {
					throw new RuntimeException(error);
				}
			});
			assertTrue(forcedShutdownStarted.await(5, TimeUnit.SECONDS),
					"closing a running CDC cursor must not block before the shutdown timeout");
			db.releaseParser.countDown();
			closed.get(10, TimeUnit.SECONDS);
			assertEquals(0L, db.getPendingOpsCount());
			assertEquals(0, db.getActiveCdcPollCursorCount());
		} finally {
			try {
				db.setForcedShutdownObserverForTesting(null);
				var active = subscription.get();
				if (active != null) {
					active.dispose();
				}
				db.releaseParser.countDown();
				if (!databaseClosed.get()) {
					db.closeTesting();
				}
			} finally {
				if (previousTimeout == null) {
					System.clearProperty(timeoutProperty);
				} else {
					System.setProperty(timeoutProperty, previousTimeout);
				}
			}
		}
	}

	private static long createColumn(EmbeddedDB db, String name) {
		return db.createColumn(name, ColumnSchema.of(
				IntArrayList.of(Integer.BYTES),
				new ObjectArrayList<>(),
				true,
				null,
				null,
				null));
	}

	private static Keys key(int value) {
		return new Keys(new Buf[] {Buf.wrap(new byte[] {
				(byte) (value >>> 24),
				(byte) (value >>> 16),
				(byte) (value >>> 8),
				(byte) value})});
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

	private static final class BlockingStageEmbeddedDB extends EmbeddedDB {

		private final AtomicBoolean blockPublication = new AtomicBoolean();
		private final AtomicBoolean blockDiscovery = new AtomicBoolean();
		private final CountDownLatch stageStarted = new CountDownLatch(1);
		private final CountDownLatch releaseStage = new CountDownLatch(1);

		private BlockingStageEmbeddedDB(Path path, String name) throws IOException {
			super(path, name, null);
		}

		@Override
		protected void flushCdcWalForPrefixlessProbe() throws org.rocksdb.RocksDBException {
			if (blockPublication.get()) {
				stageStarted.countDown();
				awaitUninterruptibly(releaseStage);
			}
			super.flushCdcWalForPrefixlessProbe();
		}

		@Override
		protected OptionalLong probeEarliestAvailableWalSeq() throws org.rocksdb.RocksDBException {
			if (blockDiscovery.get()) {
				stageStarted.countDown();
				awaitUninterruptibly(releaseStage);
			}
			return super.probeEarliestAvailableWalSeq();
		}
	}
}
