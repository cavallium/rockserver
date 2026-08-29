package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

@Timeout(30)
class ProfiledWorkloadDeferredAdmissionTest {

	@Test
	void boundedFifoWaitersCannotBeOvertakenAndPreserveAccounting() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 2, "deferred-fifo");
		var activeRelease = new CountDownLatch(1);
		var activeStarted = new CountDownLatch(1);
		var order = Collections.synchronizedList(new ArrayList<String>());
		var regularOvertakeRejected = new AtomicBoolean();
		try {
			scheduler.readExecutor().execute(() -> {
				activeStarted.countDown();
				awaitUninterruptibly(activeRelease);
			});
			assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
			scheduler.readExecutor().execute(() -> {
				order.add("queued-1");
				try {
					scheduler.readExecutor().execute(() -> order.add("overtake"));
				} catch (RocksDBException expected) {
					regularOvertakeRejected.set(true);
				}
			});
			scheduler.readExecutor().execute(() -> order.add("queued-2"));

			var first = new DeferredProbe(() -> order.add("deferred-1"));
			var second = new DeferredProbe(() -> order.add("deferred-2"));
			executeWhenCapacity(scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)), first);
			executeWhenCapacity(scheduler.read(), second);
			var overflow = new DeferredProbe(() -> order.add("overflow"));
			RocksDBException rejection = assertThrows(RocksDBException.class,
					() -> executeWhenCapacity(scheduler.read(), overflow));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, rejection.getErrorUniqueId());
			assertEquals(3L, scheduler.poolSnapshot(RWScheduler.Pool.READ).submissionAttempts(),
					"bounded pre-admission waiters and overflow are not admitted scheduler attempts");

			activeRelease.countDown();
			assertTrue(first.ran().await(5, TimeUnit.SECONDS));
			assertTrue(second.ran().await(5, TimeUnit.SECONDS));
			assertTrue(regularOvertakeRejected.get(),
					"atomic waiter promotion must occupy each released slot before a later regular submission");
			assertEquals(List.of("queued-1", "queued-2", "deferred-1", "deferred-2"), order);
			assertFalse(overflow.ran().await(20, TimeUnit.MILLISECONDS));
			assertInstanceOf(RocksDBException.class, overflow.failure().join());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			var drained = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(6L, drained.submissionAttempts());
			assertEquals(5L, drained.acceptedTasks());
			assertEquals(6L, drained.terminalOutcomes());
		} finally {
			activeRelease.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellationAfterPromotionRemovesTheExactQueuedWaiter() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "deferred-cancel");
		var activeRelease = new CountDownLatch(1);
		var activeStarted = new CountDownLatch(1);
		var queuedRelease = new CountDownLatch(1);
		var queuedStarted = new CountDownLatch(1);
		try {
			scheduler.readExecutor().execute(() -> {
				activeStarted.countDown();
				awaitUninterruptibly(activeRelease);
			});
			assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
			scheduler.readExecutor().execute(() -> {
				queuedStarted.countDown();
				awaitUninterruptibly(queuedRelease);
			});
			var probe = new DeferredProbe(() -> {});
			var handle = executeWhenCapacity(scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)), probe);

			activeRelease.countDown();
			assertTrue(queuedStarted.await(5, TimeUnit.SECONDS));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			handle.dispose();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			queuedRelease.countDown();

			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertFalse(probe.ran().await(20, TimeUnit.MILLISECONDS));
			assertInstanceOf(CancellationException.class, probe.failure().join());
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			activeRelease.countDown();
			queuedRelease.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellationBeforePromotionRemovesThePreadmissionWaiter() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "deferred-wait-cancel");
		var activeRelease = new CountDownLatch(1);
		var activeStarted = new CountDownLatch(1);
		try {
			scheduler.readExecutor().execute(() -> {
				activeStarted.countDown();
				awaitUninterruptibly(activeRelease);
			});
			assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
			scheduler.readExecutor().execute(() -> {});
			var probe = new DeferredProbe(() -> {});
			var handle = executeWhenCapacity(scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)), probe);
			assertEquals(2L, scheduler.poolSnapshot(RWScheduler.Pool.READ).submissionAttempts());

			handle.dispose();
			activeRelease.countDown();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());

			assertFalse(probe.ran().await(20, TimeUnit.MILLISECONDS));
			assertFalse(probe.failure().isDone(),
					"downstream cancellation must remove a pre-admission waiter without an error signal");
			assertEquals(2L, scheduler.poolSnapshot(RWScheduler.Pool.READ).submissionAttempts());
		} finally {
			activeRelease.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void waitingDeadlineExpiresPromptlyWithoutCapacityAndWinsCancellationExactlyOnce() throws Exception {
		var scheduler = RWScheduler.forTesting(2, 1, 1, 1, 1, "deferred-deadline");
		var activeRelease = new CountDownLatch(1);
		var activeStarted = new CountDownLatch(1);
		var probe = new BlockingRejectionProbe();
		try {
			scheduler.setStoragePressure(true);
			scheduler.readExecutor().execute(() -> {
				activeStarted.countDown();
				awaitUninterruptibly(activeRelease);
			});
			assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
			scheduler.readExecutor().execute(() -> {});
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			var deadlineScheduler = scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					System.currentTimeMillis() + 200L);
			var handle = executeWhenCapacity(deadlineScheduler, probe);
			assertEquals(2L, scheduler.poolSnapshot(RWScheduler.Pool.READ).submissionAttempts(),
					"waiting before capacity is pre-admission and must not disturb conservation");

			assertTrue(probe.rejectionEntered.await(5, TimeUnit.SECONDS),
					"a waiting worker must wake at the deferred deadline while the real queue stays full");
			assertEquals(1, probe.rejectionCount.get());
			handle.dispose();
			probe.releaseRejection.countDown();
			Throwable failure = probe.failure.get(5, TimeUnit.SECONDS);
			RocksDBException deadline = assertInstanceOf(RocksDBException.class, failure);
			assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED, deadline.getErrorUniqueId());
			assertEquals(1, probe.rejectionCount.get(),
					"cancellation racing an already-selected deadline must not deliver a second terminal signal");
			assertFalse(probe.ran.await(20, TimeUnit.MILLISECONDS));
			assertEquals(2L, scheduler.poolSnapshot(RWScheduler.Pool.READ).submissionAttempts(),
					"deadline rejection before promotion must leave admitted-work metrics unchanged");
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks(),
					"the real queue was never released to promote the expired waiter");

			scheduler.setStoragePressure(false);
			activeRelease.countDown();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
		} finally {
			probe.releaseRejection.countDown();
			scheduler.setStoragePressure(false);
			activeRelease.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void gracefulShutdownRejectsWaitingAdmissionWithoutRunningIt() throws Exception {
		assertShutdownRejectsWaitingAdmission(false);
	}

	@Test
	void forcedShutdownRejectsWaitingAdmissionWithoutRunningIt() throws Exception {
		assertShutdownRejectsWaitingAdmission(true);
	}

	private static void assertShutdownRejectsWaitingAdmission(boolean forced) throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "deferred-shutdown-" + forced);
		var activeRelease = new CountDownLatch(1);
		var activeStarted = new CountDownLatch(1);
		try {
			scheduler.readExecutor().execute(() -> {
				activeStarted.countDown();
				awaitUninterruptibly(activeRelease);
			});
			assertTrue(activeStarted.await(5, TimeUnit.SECONDS));
			scheduler.readExecutor().execute(() -> {});
			var probe = new DeferredProbe(() -> {});
			executeWhenCapacity(scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1)), probe);

			CompletableFuture<Void> shutdown = CompletableFuture.runAsync(
					forced ? scheduler::disposeNow : scheduler::dispose);
			assertInstanceOf(RejectedExecutionException.class, probe.failure().get(5, TimeUnit.SECONDS));
			assertFalse(probe.ran().await(20, TimeUnit.MILLISECONDS));
			activeRelease.countDown();
			shutdown.get(15, TimeUnit.SECONDS);
			assertTrue(scheduler.instrumentationSnapshot().pools().values().stream()
					.allMatch(RWScheduler.PoolSnapshot::terminated));
			assertTrue(scheduler.instrumentationSnapshot().pools().values().stream()
					.allMatch(RWScheduler.PoolSnapshot::drainedAndConserved));
		} finally {
			activeRelease.countDown();
			scheduler.disposeNow();
		}
	}

	private static Disposable executeWhenCapacity(Scheduler scheduler, Runnable command) {
		try {
			var method = scheduler.getClass().getDeclaredMethod("executeWhenCapacity", Runnable.class);
			method.setAccessible(true);
			return (Disposable) method.invoke(scheduler, command);
		} catch (InvocationTargetException failure) {
			Throwable cause = failure.getCause();
			if (cause instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			throw new IllegalStateException(cause);
		} catch (ReflectiveOperationException failure) {
			throw new IllegalStateException(failure);
		}
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		while (true) {
			try {
				latch.await();
				return;
			} catch (InterruptedException ignored) {
				// The owner releases the latch during cleanup, including forced shutdown.
			}
		}
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		assertTrue(condition.getAsBoolean(), "condition did not become true before timeout");
	}

	private static final class DeferredProbe implements Runnable, RWScheduler.RejectionAwareTask {

		private final Runnable action;
		private final CountDownLatch ran = new CountDownLatch(1);
		private final CompletableFuture<Throwable> failure = new CompletableFuture<>();

		private DeferredProbe(Runnable action) {
			this.action = action;
		}

		@Override
		public void run() {
			action.run();
			ran.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.complete(failure);
		}

		private CountDownLatch ran() {
			return ran;
		}

		private CompletableFuture<Throwable> failure() {
			return failure;
		}
	}

	private static final class BlockingRejectionProbe implements Runnable, RWScheduler.RejectionAwareTask {

		private final CountDownLatch ran = new CountDownLatch(1);
		private final CountDownLatch rejectionEntered = new CountDownLatch(1);
		private final CountDownLatch releaseRejection = new CountDownLatch(1);
		private final AtomicInteger rejectionCount = new AtomicInteger();
		private final CompletableFuture<Throwable> failure = new CompletableFuture<>();

		@Override
		public void run() {
			ran.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
			rejectionEntered.countDown();
			awaitUninterruptibly(releaseRejection);
			this.failure.complete(failure);
		}
	}
}
