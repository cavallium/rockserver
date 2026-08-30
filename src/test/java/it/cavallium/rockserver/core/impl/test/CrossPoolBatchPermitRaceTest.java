package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class CrossPoolBatchPermitRaceTest {

	@Test
	void batchOnlyPermitLossReleasesPoolLockForInspectionCancellationAndSubmission() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 32, 32, "batch-only-permit-race");
		var releaseWriteBatch = new CountDownLatch(1);
		var releasePermitObserver = new CountDownLatch(1);
		var observerEntered = new CountDownLatch(1);
		var writeBatchStarted = new CountDownLatch(1);
		var writeBatchCompleted = new CountDownLatch(1);
		var foregroundCompleted = new CountDownLatch(1);
		var observerFirstEntry = new AtomicBoolean();
		var readBatchRan = new AtomicBoolean();
		var readBatchExecutor = scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				Long.MAX_VALUE);
		Runnable readBatch = () -> readBatchRan.set(true);
		try {
			scheduler.setStoragePressure(true);
			scheduler.setBeforeBatchPermitAcquisitionObserverForTesting(RWScheduler.Pool.READ, () -> {
				if (observerFirstEntry.compareAndSet(false, true)) {
					observerEntered.countDown();
					awaitUninterruptibly(releasePermitObserver);
				}
			});
			readBatchExecutor.execute(readBatch);
			assertTrue(observerEntered.await(5, SECONDS),
					"read BATCH did not reach the permit-acquisition race seam");

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.MUTATION,
					Long.MAX_VALUE).execute(() -> {
				writeBatchStarted.countDown();
				awaitUninterruptibly(releaseWriteBatch);
				writeBatchCompleted.countDown();
			});
			assertTrue(writeBatchStarted.await(5, SECONDS));

			releasePermitObserver.countDown();
			var racedSnapshot = CompletableFuture
					.supplyAsync(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ))
					.get(1, SECONDS);
			assertEquals(1, racedSnapshot.queuedByProfile().get(WorkloadProfile.BATCH));
			assertEquals(0, racedSnapshot.activeByProfile().get(WorkloadProfile.BATCH));
			assertTrue(CompletableFuture
					.supplyAsync(() -> scheduler.removeQueuedTask(readBatchExecutor, readBatch))
					.get(1, SECONDS),
					"queued BATCH cancellation must acquire the pool lock while the winning pool holds its permit");

			CompletableFuture.runAsync(() -> scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE).execute(foregroundCompleted::countDown)).get(1, SECONDS);
			assertTrue(foregroundCompleted.await(1, SECONDS),
					"new foreground submission must dispatch while the other pool still holds the permit");
			assertFalse(readBatchRan.get(), "the cancelled losing BATCH candidate must never execute");
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));

			releaseWriteBatch.countDown();
			assertTrue(writeBatchCompleted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved());
		} finally {
			releasePermitObserver.countDown();
			releaseWriteBatch.countDown();
			scheduler.setStoragePressure(false);
			scheduler.setBeforeBatchPermitAcquisitionObserverForTesting(RWScheduler.Pool.READ, null);
			scheduler.disposeNow();
		}
	}

	@Test
	void losingCrossPoolBatchPermitRaceImmediatelyReselectsQueuedForegroundWork() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 32, 32, "cross-pool-permit-race");
		var releaseReadBlocker = new CountDownLatch(1);
		var releaseWriteBatch = new CountDownLatch(1);
		var releasePermitObserver = new CountDownLatch(1);
		var observerEntered = new CountDownLatch(1);
		var writeBatchStarted = new CountDownLatch(1);
		var readForegroundStarted = new CountDownLatch(1);
		var readWorkCompleted = new CountDownLatch(3);
		var writeBatchCompleted = new CountDownLatch(1);
		var observerFirstEntry = new AtomicBoolean();
		try {
			primeReadDrrCursorAtBatch(scheduler);

			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + SECONDS.toMillis(30)).execute(() -> {
				awaitUninterruptibly(releaseReadBlocker);
				readWorkCompleted.countDown();
			});
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 1);

			scheduler.setStoragePressure(true);
			scheduler.setBeforeBatchPermitAcquisitionObserverForTesting(RWScheduler.Pool.READ, () -> {
				if (observerFirstEntry.compareAndSet(false, true)) {
					observerEntered.countDown();
					awaitUninterruptibly(releasePermitObserver);
				}
			});
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE).execute(readWorkCompleted::countDown);
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE).execute(() -> {
				readForegroundStarted.countDown();
				readWorkCompleted.countDown();
			});

			releaseReadBlocker.countDown();
			assertTrue(observerEntered.await(5, SECONDS),
					"read BATCH did not reach the permit-acquisition race seam");

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.MUTATION,
					Long.MAX_VALUE).execute(() -> {
				writeBatchStarted.countDown();
				awaitUninterruptibly(releaseWriteBatch);
				writeBatchCompleted.countDown();
			});
			assertTrue(writeBatchStarted.await(5, SECONDS),
					"write BATCH did not acquire the shared pressure permit");

			releasePermitObserver.countDown();
			assertTrue(readForegroundStarted.await(1, SECONDS),
					"a lost BATCH permit race must not park a worker while foreground work is queued");
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.queuedByProfile().get(WorkloadProfile.BATCH));
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.WRITE)
					.activeByProfile().get(WorkloadProfile.BATCH));

			releaseWriteBatch.countDown();
			assertTrue(writeBatchCompleted.await(5, SECONDS));
			assertTrue(readWorkCompleted.await(5, SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved());

			// Reset the pacing clock and prove that every permit from the raced dispatches was released.
			scheduler.setStoragePressure(false);
			scheduler.setStoragePressure(true);
			var permitProbeCompleted = new CountDownLatch(1);
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE).execute(permitProbeCompleted::countDown);
			assertTrue(permitProbeCompleted.await(1, SECONDS),
					"a subsequent pressured BATCH must acquire the fully conserved global permit");
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).failedTasks());
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.WRITE).failedTasks());
		} finally {
			releaseReadBlocker.countDown();
			releasePermitObserver.countDown();
			releaseWriteBatch.countDown();
			scheduler.setStoragePressure(false);
			scheduler.setBeforeBatchPermitAcquisitionObserverForTesting(RWScheduler.Pool.READ, null);
			scheduler.disposeNow();
		}
	}

	private static void primeReadDrrCursorAtBatch(RWScheduler scheduler) throws Exception {
		for (var profile : new WorkloadProfile[] {
				WorkloadProfile.INGEST,
				WorkloadProfile.CDC,
				WorkloadProfile.ANALYTICAL
		}) {
			var completed = new CountDownLatch(1);
			var family = profile == WorkloadProfile.CDC
					? OperationFamily.WAL_PAGE
					: OperationFamily.RANGE_PAGE;
			scheduler.executor(profile, family, Long.MAX_VALUE)
					.execute(completed::countDown);
			assertTrue(completed.await(5, SECONDS), "failed to prime DRR through " + profile);
		}
	}

	private static void assertEventually(java.util.function.BooleanSupplier condition) throws Exception {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean());
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
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
}
