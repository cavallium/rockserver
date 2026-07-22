package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.WriteClass;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.scheduler.Schedulers;

@Timeout(30)
class RWSchedulerTest {

	@Test
	void configuredClassifiedWritesUseDistinctViewsOfOnePool() {
		var scheduler = new RWScheduler(1, 1, "write-class-alias-test");
		try {
			assertSame(scheduler.write(), scheduler.write(WriteClass.FOREGROUND));
			assertNotSame(scheduler.write(), scheduler.write(WriteClass.MAINTENANCE));
			assertSame(scheduler.writeExecutor(), scheduler.writeExecutor(WriteClass.FOREGROUND));
			assertNotSame(scheduler.writeExecutor(), scheduler.writeExecutor(WriteClass.MAINTENANCE));
			assertEquals(1, scheduler.writeWorkerLimit(WriteClass.FOREGROUND));
			assertEquals(1, scheduler.writeWorkerLimit(WriteClass.MAINTENANCE));
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void compatibilityConstructorRetainsWriteLaneAliases() {
		var read = Schedulers.newSingle("compat-read");
		var write = Schedulers.newSingle("compat-write");
		var scheduler = new RWScheduler(read, write);
		try {
			assertSame(scheduler.write(), scheduler.write(WriteClass.FOREGROUND));
			assertSame(scheduler.write(), scheduler.write(WriteClass.MAINTENANCE));
			assertEquals(0, scheduler.writeWorkerLimit(WriteClass.FOREGROUND));
			assertEquals(0, scheduler.writeWorkerLimit(WriteClass.MAINTENANCE));
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void gracefulDisposalTerminatesEveryDedicatedExecutor() {
		var scheduler = new RWScheduler(2, 1, "graceful-disposal-test");
		var readExecutor = (ExecutorService) scheduler.readExecutor();
		var writeExecutor = (ExecutorService) scheduler.writeExecutor();
		var maintenanceExecutor = (ExecutorService) scheduler.maintenanceExecutor();
		var controlExecutor = (ExecutorService) scheduler.controlExecutor();
		var cdcExecutor = (ExecutorService) scheduler.cdcExecutor();
		scheduler.read().schedule(() -> {});
		scheduler.write().schedule(() -> {});
		scheduler.maintenance().schedule(() -> {});
		scheduler.control().schedule(() -> {});
		scheduler.cdc().schedule(() -> {});

		scheduler.disposeGracefully().block(Duration.ofSeconds(10));

		assertTrue(readExecutor.isTerminated());
		assertTrue(writeExecutor.isTerminated());
		assertTrue(maintenanceExecutor.isTerminated());
		assertTrue(controlExecutor.isTerminated());
		assertTrue(cdcExecutor.isTerminated());
	}

	@Test
	void gracefulDisposalDrainsBothClassifiedQueues() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 4, 4, "graceful-write-drain-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var queuedCompleted = new CountDownLatch(2);
		try {
			scheduler.write().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduler.write().schedule(queuedCompleted::countDown);
			scheduler.write(WriteClass.MAINTENANCE).schedule(queuedCompleted::countDown);

			var disposal = scheduler.disposeGracefully().toFuture();
			releaseBlocker.countDown();
			disposal.get(10, TimeUnit.SECONDS);

			assertEquals(0, queuedCompleted.getCount());
			assertTrue(((ExecutorService) scheduler.writeExecutor()).isTerminated());
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void gracefulDisposalHandlesCompatibilityLaneAliasesOnce() {
		var read = Schedulers.newSingle("aliased-read");
		var write = Schedulers.newSingle("aliased-write");
		var scheduler = new RWScheduler(read, write);

		scheduler.disposeGracefully().block(Duration.ofSeconds(10));

		assertTrue(read.isDisposed());
		assertTrue(write.isDisposed());
	}

	@Test
	void constructionDoesNotEagerlyStartConfiguredWorkerThreads() {
		var scheduler = new RWScheduler(16, 8, "lazy-worker-test");
		try {
			assertEquals(0, ((ThreadPoolExecutor) scheduler.readExecutor()).getPoolSize());
			assertEquals(0, scheduler.writeWorkerCount());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.maintenanceExecutor()).getPoolSize());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.controlExecutor()).getPoolSize());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.cdcExecutor()).getPoolSize());
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void combinedAndMaintenanceRunningCapsAreHardLimits() throws Exception {
		var scheduler = new RWScheduler(1, 4, 1, 64, 64, "write-cap-test");
		var firstWaveStarted = new CountDownLatch(4);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(10);
		var active = new AtomicInteger();
		var activeMaintenance = new AtomicInteger();
		var maxActive = new AtomicInteger();
		var maxActiveMaintenance = new AtomicInteger();
		try {
			for (int i = 0; i < 4; i++) {
				scheduler.write(WriteClass.MAINTENANCE).schedule(() -> trackedWrite(
						true, firstWaveStarted, release, completed, active, activeMaintenance,
						maxActive, maxActiveMaintenance));
			}
			for (int i = 0; i < 6; i++) {
				scheduler.write(WriteClass.FOREGROUND).schedule(() -> trackedWrite(
						false, firstWaveStarted, release, completed, active, activeMaintenance,
						maxActive, maxActiveMaintenance));
			}

			assertTrue(firstWaveStarted.await(5, TimeUnit.SECONDS));
			assertEquals(4, maxActive.get());
			assertEquals(1, maxActiveMaintenance.get());
			release.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(4, maxActive.get());
			assertEquals(1, maxActiveMaintenance.get());
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void foregroundOvertakesQueuedMaintenance() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 8, 8, "write-overtake-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.write().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduler.write(WriteClass.MAINTENANCE).schedule(() -> {
				order.add("maintenance");
				completed.countDown();
			});
			scheduler.write(WriteClass.FOREGROUND).schedule(() -> {
				order.add("foreground");
				completed.countDown();
			});

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("foreground", "maintenance"), order);
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void eachWriteClassRetainsFifoOrder() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 8, 8, "write-fifo-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(4);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.write().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduleRecorded(scheduler, WriteClass.MAINTENANCE, "maintenance-1", order, completed);
			scheduleRecorded(scheduler, WriteClass.MAINTENANCE, "maintenance-2", order, completed);
			scheduleRecorded(scheduler, WriteClass.FOREGROUND, "foreground-1", order, completed);
			scheduleRecorded(scheduler, WriteClass.FOREGROUND, "foreground-2", order, completed);

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("foreground-1", "foreground-2", "maintenance-1", "maintenance-2"), order);
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void maintenanceProgressIsBoundedDuringForegroundFlood() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 64, 8, "write-progress-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(34);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.write().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduler.write(WriteClass.MAINTENANCE).schedule(() -> {
				order.add("maintenance");
				completed.countDown();
			});
			for (int i = 0; i < 33; i++) {
				int task = i;
				scheduler.write().schedule(() -> {
					order.add("foreground-" + task);
					completed.countDown();
				});
			}

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(32, order.indexOf("maintenance"));
			assertEquals("foreground-0", order.getFirst());
			assertEquals("foreground-31", order.get(31));
			assertEquals("foreground-32", order.getLast());
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void queuesRejectIndependentlyWithServerOverloaded() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 1, 1, "write-rejection-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		try {
			scheduler.writeExecutor().execute(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			scheduler.writeExecutor().execute(() -> {});
			scheduler.writeExecutor(WriteClass.MAINTENANCE).execute(() -> {});

			var foregroundFailure = assertThrows(RocksDBException.class,
					() -> scheduler.writeExecutor().execute(() -> {}));
			var maintenanceFailure = assertThrows(RocksDBException.class,
					() -> scheduler.writeExecutor(WriteClass.MAINTENANCE).execute(() -> {}));
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, foregroundFailure.getErrorUniqueId());
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, maintenanceFailure.getErrorUniqueId());
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void cancellingScheduledWorkRemovesItsClassifiedWrapper() throws Exception {
		var scheduler = new RWScheduler(1, 1, 1, 4, 4, "write-cancellation-test");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var cancelledTaskRan = new AtomicInteger();
		try {
			scheduler.write().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			var disposable = scheduler.write(WriteClass.MAINTENANCE).schedule(cancelledTaskRan::incrementAndGet);
			assertEquals(1, scheduler.queuedWriteTasks(WriteClass.MAINTENANCE));

			disposable.dispose();
			assertTrue(awaitCondition(
					() -> scheduler.queuedWriteTasks(WriteClass.MAINTENANCE) == 0, Duration.ofSeconds(5)));
			releaseBlocker.countDown();
			assertTrue(awaitCondition(() -> scheduler.activeWriteTasks(WriteClass.FOREGROUND) == 0,
					Duration.ofSeconds(5)));
			assertEquals(0, cancelledTaskRan.get());
		} finally {
			releaseBlocker.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void admissionMetricsReflectBothLanesAndConfiguredLimits() throws Exception {
		var registry = new SimpleMeterRegistry();
		try {
			var scheduler = new RWScheduler(1, 1, 1, 1, 2,
					"write-metrics-test", registry, "metrics-db");
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			try {
				scheduler.write().schedule(() -> {
					blockerStarted.countDown();
					await(releaseBlocker);
				});
				assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
				var cancelled = scheduler.write(WriteClass.FOREGROUND).schedule(() -> {});
				assertThrows(RocksDBException.class, () -> scheduler.writeExecutor().execute(() -> {}));
				cancelled.dispose();
				releaseBlocker.countDown();
				assertTrue(awaitCondition(() -> scheduler.activeWriteTasks(WriteClass.FOREGROUND) == 0,
						Duration.ofSeconds(5)));

				var maintenanceDone = new CountDownLatch(1);
				scheduler.write(WriteClass.MAINTENANCE).schedule(maintenanceDone::countDown);
				assertTrue(maintenanceDone.await(5, TimeUnit.SECONDS));

				assertEquals(0d, gauge(registry, "rockserver.write.admission.queued", "foreground"));
				assertEquals(0d, gauge(registry, "rockserver.write.admission.active", "foreground"));
				assertEquals(1d, gauge(registry, "rockserver.write.admission.worker.limit", "foreground"));
				assertEquals(1d, gauge(registry, "rockserver.write.admission.worker.limit", "maintenance"));
				assertEquals(1d, gauge(registry, "rockserver.write.admission.queue.limit", "foreground"));
				assertEquals(2d, gauge(registry, "rockserver.write.admission.queue.limit", "maintenance"));
				assertEquals(1d, counter(registry, "rockserver.write.admission.cancelled", "foreground"));
				assertEquals(1d, counter(registry, "rockserver.write.admission.rejected", "foreground"));
				assertTrue(counter(registry, "rockserver.write.admission.completed", "foreground") >= 1d);
				assertTrue(counter(registry, "rockserver.write.admission.completed", "maintenance") >= 1d);
				assertTrue(timerCount(registry, "rockserver.write.admission.queue.wait", "foreground") >= 1L);
				assertTrue(timerCount(registry, "rockserver.write.admission.execution", "maintenance") >= 1L);
			} finally {
				releaseBlocker.countDown();
				scheduler.dispose();
			}
		} finally {
			registry.close();
		}
	}

	@Test
	void blockedMaintenanceDoesNotParkWriteOrControlWork() throws Exception {
		var scheduler = new RWScheduler(1, 1, "maintenance-isolation-test");
		var maintenanceStarted = new CountDownLatch(1);
		var releaseMaintenance = new CountDownLatch(1);
		try {
			scheduler.maintenance().schedule(() -> {
				maintenanceStarted.countDown();
				await(releaseMaintenance);
			});
			assertTrue(maintenanceStarted.await(5, TimeUnit.SECONDS));

			var writeCompleted = new CountDownLatch(1);
			var controlCompleted = new CountDownLatch(1);
			scheduler.write().schedule(writeCompleted::countDown);
			scheduler.control().schedule(controlCompleted::countDown);
			assertTrue(writeCompleted.await(5, TimeUnit.SECONDS),
					"maintenance work occupied the write lane");
			assertTrue(controlCompleted.await(5, TimeUnit.SECONDS),
					"maintenance work occupied the control/cleanup lane");
		} finally {
			releaseMaintenance.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void saturatedCleanupPoolDoesNotParkCdcOrWriteWork() throws Exception {
		var scheduler = new RWScheduler(1, 1, "control-overlap-test");
		var cleanupStarted = new CountDownLatch(2);
		var releaseCleanup = new CountDownLatch(1);
		try {
			assertEquals(2, ((ThreadPoolExecutor) scheduler.controlExecutor()).getMaximumPoolSize());
			for (int i = 0; i < 2; i++) {
				scheduler.control().schedule(() -> {
					cleanupStarted.countDown();
					await(releaseCleanup);
				});
			}
			assertTrue(cleanupStarted.await(5, TimeUnit.SECONDS));

			var cdcPublicationCompleted = new CountDownLatch(1);
			var writeCompleted = new CountDownLatch(1);
			scheduler.cdc().schedule(cdcPublicationCompleted::countDown);
			scheduler.write().schedule(writeCompleted::countDown);

			assertTrue(cdcPublicationCompleted.await(5, TimeUnit.SECONDS),
					"blocking iterator closes stalled the independent CDC WAL-publication lane");
			assertTrue(writeCompleted.await(5, TimeUnit.SECONDS),
					"control cleanup occupied the write lane");
		} finally {
			releaseCleanup.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void interactiveReadOvertakesQueuedCompositeRead() throws Exception {
		var scheduler = new RWScheduler(1, 1, "priority-overtake-test");
		try {
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			var completed = new CountDownLatch(2);
			var order = Collections.synchronizedList(new ArrayList<String>());

			scheduler.read().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			scheduler.read().schedule(() -> {
				order.add("composite");
				completed.countDown();
			});
			scheduler.interactiveRead().schedule(() -> {
				order.add("interactive");
				completed.countDown();
			});

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("interactive", "composite"), order);
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void compositeReadProgressesDuringContinuousInteractiveQueue() throws Exception {
		var scheduler = new RWScheduler(1, 1, "weighted-handoff-test");
		try {
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			var completed = new CountDownLatch(11);
			var order = Collections.synchronizedList(new ArrayList<String>());

			scheduler.read().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			scheduler.read().schedule(() -> {
				order.add("composite");
				completed.countDown();
			});
			for (int i = 0; i < 10; i++) {
				int task = i;
				scheduler.interactiveRead().schedule(() -> {
					order.add("interactive-" + task);
					completed.countDown();
				});
			}

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals("composite", order.get(8));
			assertEquals(List.of(
					"interactive-0",
					"interactive-1",
					"interactive-2",
					"interactive-3",
					"interactive-4",
					"interactive-5",
					"interactive-6",
					"interactive-7"), order.subList(0, 8));
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void bothReadClassesShareTheConfiguredRunningTaskCap() throws Exception {
		var scheduler = new RWScheduler(2, 1, "shared-cap-test");
		try {
			var firstTwoStarted = new CountDownLatch(2);
			var release = new CountDownLatch(1);
			var completed = new CountDownLatch(20);
			var active = new AtomicInteger();
			var maximumActive = new AtomicInteger();
			Runnable task = () -> {
				int current = active.incrementAndGet();
				maximumActive.accumulateAndGet(current, Math::max);
				firstTwoStarted.countDown();
				await(release);
				active.decrementAndGet();
				completed.countDown();
			};

			for (int i = 0; i < 10; i++) {
				scheduler.read().schedule(task);
				scheduler.interactiveRead().schedule(task);
			}

			assertTrue(firstTwoStarted.await(5, TimeUnit.SECONDS));
			assertEquals(2, maximumActive.get());
			release.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(2, maximumActive.get());
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void interactiveOnlyTrafficDoesNotLeavePriorityDebtForTheNextCompositeRead() throws Exception {
		var scheduler = new RWScheduler(1, 1, "priority-debt-reset-test");
		try {
			var warmupCompleted = new CountDownLatch(64);
			for (int i = 0; i < 64; i++) {
				scheduler.interactiveRead().schedule(warmupCompleted::countDown);
			}
			assertTrue(warmupCompleted.await(5, TimeUnit.SECONDS));

			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			var completed = new CountDownLatch(2);
			var order = Collections.synchronizedList(new ArrayList<String>());
			scheduler.interactiveRead().schedule(() -> {
				blockerStarted.countDown();
				await(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));

			scheduler.read().schedule(() -> {
				order.add("composite");
				completed.countDown();
			});
			scheduler.interactiveRead().schedule(() -> {
				order.add("interactive");
				completed.countDown();
			});

			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("interactive", "composite"), order);
		} finally {
			scheduler.dispose();
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

	private static void trackedWrite(boolean maintenance,
			CountDownLatch started,
			CountDownLatch release,
			CountDownLatch completed,
			AtomicInteger active,
			AtomicInteger activeMaintenance,
			AtomicInteger maxActive,
			AtomicInteger maxActiveMaintenance) {
		int current = active.incrementAndGet();
		maxActive.accumulateAndGet(current, Math::max);
		if (maintenance) {
			int currentMaintenance = activeMaintenance.incrementAndGet();
			maxActiveMaintenance.accumulateAndGet(currentMaintenance, Math::max);
		}
		started.countDown();
		await(release);
		if (maintenance) {
			activeMaintenance.decrementAndGet();
		}
		active.decrementAndGet();
		completed.countDown();
	}

	private static void scheduleRecorded(RWScheduler scheduler,
			WriteClass writeClass,
			String value,
			List<String> order,
			CountDownLatch completed) {
		scheduler.write(writeClass).schedule(() -> {
			order.add(value);
			completed.countDown();
		});
	}

	private static boolean awaitCondition(BooleanSupplier condition, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(1L);
		}
		return condition.getAsBoolean();
	}

	private static double gauge(SimpleMeterRegistry registry, String name, String lane) {
		return registry.get(name).tag("database", "metrics-db").tag("lane", lane).gauge().value();
	}

	private static double counter(SimpleMeterRegistry registry, String name, String lane) {
		return registry.get(name).tag("database", "metrics-db").tag("lane", lane).counter().count();
	}

	private static long timerCount(SimpleMeterRegistry registry, String name, String lane) {
		return registry.get(name).tag("database", "metrics-db").tag("lane", lane).timer().count();
	}
}
