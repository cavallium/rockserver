package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class RWSchedulerTest {

	@Test
	void latencyUsesEarliestDeadlineFirst() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 8, 8, "edf-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						await(release);
					});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(20)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("later", order, completed));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(10)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("earlier", order, completed));
			release.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("earlier", "later"), order);
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void guaranteedProfilesAllProgressUnderLatencyLoad() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 64, 64, "fairness-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var guaranteed = new CountDownLatch(4);
		try {
			var latency = scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP);
			latency.execute(() -> {
				blockerStarted.countDown();
				await(release);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			for (int i = 0; i < 32; i++) {
				latency.execute(() -> {});
			}
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(guaranteed::countDown);
			scheduler.executor(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE)
					.execute(guaranteed::countDown);
			scheduler.executor(RequestContext.analytical(), OperationFamily.FULL_SCAN_AGGREGATE)
					.execute(guaranteed::countDown);
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE)
					.execute(guaranteed::countDown);
			release.countDown();
			assertTrue(guaranteed.await(5, TimeUnit.SECONDS));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void controlIsIsolatedFromSaturatedDataWorkers() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 2, 2, "control-test");
		var readStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var controlDone = new CountDownLatch(1);
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				readStarted.countDown();
				await(release);
			});
			assertTrue(readStarted.await(5, TimeUnit.SECONDS));
			scheduler.controlExecutor().execute(controlDone::countDown);
			assertTrue(controlDone.await(2, TimeUnit.SECONDS));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void queuesAreBoundedAndCancelledTasksAreRemoved() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 1, 1, "bounded-test");
		var started = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var queuedRan = new AtomicBoolean();
		try {
			var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			view.execute(() -> {
				started.countDown();
				await(release);
			});
			assertTrue(started.await(5, TimeUnit.SECONDS));
			var queued = (Runnable) () -> queuedRan.set(true);
			view.execute(queued);
			var overloaded = assertThrows(RocksDBException.class, () -> view.execute(() -> {}));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
					overloaded.getErrorUniqueId());
			assertEquals(1, scheduler.queuedTasks(WorkloadProfile.BATCH));
			assertTrue(scheduler.removeQueuedTask(view, queued));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.BATCH));
			release.countDown();
			assertFalse(queuedRan.get());
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void storagePressurePausesMaintenanceButRetainsReservedProfilesAndMinimalBatchProgress() {
		var scheduler = new RWScheduler(1, 1, 1, 4, 4, "pressure-test");
		try {
			scheduler.setStoragePressure(true);
			assertThrows(RocksDBException.class, () -> scheduler.maintenanceExecutor().execute(() -> {}));
			scheduler.interactiveReadExecutor().execute(() -> {});
			scheduler.writeExecutor().execute(() -> {});
			scheduler.cdcExecutor().execute(() -> {});
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {});
			assertThrows(RocksDBException.class,
					() -> scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {}));
			assertTrue(scheduler.admissionSnapshot().storagePressure());
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void shutdownRejectsNewWorkAndDrainsAcceptedWork() throws Exception {
		var scheduler = new RWScheduler(1, 1, "shutdown-test");
		var done = new CountDownLatch(1);
		scheduler.controlExecutor().execute(done::countDown);
		scheduler.dispose();
		assertTrue(done.await(1, TimeUnit.SECONDS));
		assertThrows(java.util.concurrent.RejectedExecutionException.class,
				() -> scheduler.controlExecutor().execute(() -> {}));
	}

	private static void record(String value, List<String> order, CountDownLatch completed) {
		order.add(value);
		completed.countDown();
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
}
