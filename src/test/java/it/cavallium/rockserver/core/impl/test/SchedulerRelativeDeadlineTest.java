package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
class SchedulerRelativeDeadlineTest {

	@Test
	void deadlineExpiryDependsOnlyOnMonotonicProgress() throws Exception {
		var monotonic = new AtomicLong(1_000L);
		var scheduler = scheduler(monotonic::get, "relative-deadline-expiry");
		try {
			long deadline = scheduler.bindTimeoutNanos(100L);
			assertFalse(scheduler.isMonotonicDeadlineExpired(deadline));

			monotonic.addAndGet(100L);
			assertTrue(scheduler.isMonotonicDeadlineExpired(deadline));
			var failure = assertThrows(RocksDBException.class,
					() -> scheduler.executor(WorkloadProfile.LATENCY,
							OperationFamily.POINT_LOOKUP, deadline).execute(() -> {}));
			assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					failure.getErrorUniqueId());
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void edfUsesImmutableMonotonicDeadlines() throws Exception {
		var monotonic = new AtomicLong();
		var scheduler = scheduler(monotonic::get, "relative-deadline-edf");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		List<String> order = new CopyOnWriteArrayList<>();
		try {
			scheduler.executor(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, Long.MAX_VALUE)
					.execute(() -> {
						blockerStarted.countDown();
						await(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, TimeUnit.SECONDS));
			long later = scheduler.bindTimeoutNanos(200L);
			long earlier = scheduler.bindTimeoutNanos(100L);
			scheduler.executor(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, later)
					.execute(() -> { order.add("later"); completed.countDown(); });
			scheduler.executor(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, earlier)
					.execute(() -> { order.add("earlier"); completed.countDown(); });
			releaseBlocker.countDown();
			assertTrue(completed.await(5, TimeUnit.SECONDS));
			assertEquals(List.of("earlier", "later"), order);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void regressingMonotonicSourceFailsFiniteBindingClosed() throws Exception {
		var monotonic = new AtomicLong(1_000L);
		var scheduler = scheduler(monotonic::get, "relative-deadline-regression");
		try {
			monotonic.set(999L);
			long deadline = scheduler.bindTimeoutNanos(100L);
			assertEquals(Long.MAX_VALUE - 1L, deadline);
			assertTrue(scheduler.isMonotonicDeadlineExpired(deadline));
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void finiteOverflowNeverBecomesNoTimeoutSentinel() throws Exception {
		var monotonic = new AtomicLong(100L);
		var scheduler = scheduler(monotonic::get, "relative-deadline-overflow");
		try {
			assertEquals(Long.MAX_VALUE - 1L, scheduler.bindTimeoutNanos(Long.MAX_VALUE - 1L));
			monotonic.set(101L);
			assertEquals(2L, scheduler.bindTimeoutNanos(1L));
			monotonic.set(Long.MAX_VALUE);
			assertEquals(Long.MAX_VALUE - 1L, scheduler.bindTimeoutNanos(100L));
		} finally {
			scheduler.disposeNow();
		}
	}

	private static RWScheduler scheduler(LongSupplier nanoTime, String name) throws Exception {
		Method factory = RWScheduler.class.getDeclaredMethod("forTesting",
				int.class, int.class, int.class, int.class, int.class, String.class, LongSupplier.class);
		factory.setAccessible(true);
		return (RWScheduler) factory.invoke(null, 1, 1, 1, 16, 16, name, nanoTime);
	}

	private static void await(CountDownLatch latch) {
		try {
			latch.await();
		} catch (InterruptedException interrupted) {
			Thread.currentThread().interrupt();
			throw new AssertionError(interrupted);
		}
	}
}
