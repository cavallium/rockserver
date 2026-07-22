package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.scheduler.Schedulers;

@Timeout(30)
class RWSchedulerTest {

	@Test
	void classifiedWritesRemainAliasesOfTheExistingWriteLane() {
		var scheduler = new RWScheduler(1, 1, "write-class-alias-test");
		try {
			assertSame(scheduler.write(), scheduler.write(WriteClass.FOREGROUND));
			assertSame(scheduler.write(), scheduler.write(WriteClass.MAINTENANCE));
			assertSame(scheduler.writeExecutor(), scheduler.writeExecutor(WriteClass.FOREGROUND));
			assertSame(scheduler.writeExecutor(), scheduler.writeExecutor(WriteClass.MAINTENANCE));
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
			assertEquals(0, ((ThreadPoolExecutor) scheduler.writeExecutor()).getPoolSize());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.maintenanceExecutor()).getPoolSize());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.controlExecutor()).getPoolSize());
			assertEquals(0, ((ThreadPoolExecutor) scheduler.cdcExecutor()).getPoolSize());
		} finally {
			scheduler.dispose();
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
}
