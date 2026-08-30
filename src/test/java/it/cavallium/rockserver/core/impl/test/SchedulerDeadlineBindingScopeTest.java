package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.management.ThreadMXBean;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class SchedulerDeadlineBindingScopeTest {

	private static final Object MARKER = new Object();
	private static final Supplier<Object> RETURN_MARKER = () -> MARKER;

	@Test
	void nestedBindingsRestoreOuterStateAndClearCrossRequestIdentity() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-binding-nested");
		var outer = new RequestContext(WorkloadProfile.BATCH, 2_000L);
		var inner = new RequestContext(WorkloadProfile.BATCH, 3_000L);
		try {
			Object result = scheduler.withDeadlineBinding(outer, 111L, () -> {
				assertEquals(111L, scheduler.resolveMonotonicDeadline(outer));
				assertThrows(SyntheticScopeFailure.class, () -> scheduler.withDeadlineBinding(inner, 222L, () -> {
					assertEquals(222L, scheduler.resolveMonotonicDeadline(inner));
					throw new SyntheticScopeFailure();
				}));
				assertEquals(111L, scheduler.resolveMonotonicDeadline(outer),
						"failed nested dispatch did not restore its outer binding");
				return MARKER;
			});

			assertSame(MARKER, result);
			assertEquals(1_000_000_000L, scheduler.resolveMonotonicDeadline(outer),
					"the completed request leaked its explicit sidecar into a later lookup");
			assertClearedRetainedSlot(scheduler);

			scheduler.withDeadlineBinding(inner, 333L, () -> {
				assertEquals(333L, scheduler.resolveMonotonicDeadline(inner));
				return MARKER;
			});
			assertEquals(2_000_000_000L, scheduler.resolveMonotonicDeadline(inner));
			assertClearedRetainedSlot(scheduler);
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void directResolutionDoesNotCreateABindingSlot() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-binding-direct");
		var context = new RequestContext(WorkloadProfile.BATCH, 2_000L);
		var observedSlot = new AtomicReference<Object>();
		var threadFailure = new AtomicReference<Throwable>();
		try {
			Thread thread = Thread.startVirtualThread(() -> {
				try {
					assertEquals(1_000_000_000L, scheduler.resolveMonotonicDeadline(context));
					observedSlot.set(bindingThreadLocal(scheduler).get());
				} catch (Throwable failure) {
					threadFailure.set(failure);
				}
			});
			thread.join();
			assertNull(threadFailure.get(), "direct resolution thread failed");
			assertNull(observedSlot.get(), "a direct non-server dispatch allocated a binding slot");
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void warmedBindingScopeAllocatesNoPerDispatchObjects() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-binding-allocation");
		var context = new RequestContext(WorkloadProfile.BATCH, 2_000L);
		var threadMetrics = (ThreadMXBean) ManagementFactory.getThreadMXBean();
		try {
			assertTrue(threadMetrics.isThreadAllocatedMemorySupported());
			if (!threadMetrics.isThreadAllocatedMemoryEnabled()) {
				threadMetrics.setThreadAllocatedMemoryEnabled(true);
			}
			for (int index = 0; index < 100_000; index++) {
				scheduler.withDeadlineBinding(context, 111L, RETURN_MARKER);
			}
			long threadId = Thread.currentThread().threadId();
			var first = allocationWindow(scheduler, context, threadMetrics, threadId);
			var second = allocationWindow(scheduler, context, threadMetrics, threadId);

			assertSame(MARKER, second.result());
			long minimumWindowAllocation = Math.min(first.allocatedBytes(), second.allocatedBytes());
			assertTrue(minimumWindowAllocation <= 256L,
					"one million warmed bindings allocated more than bounded measurement noise: "
							+ minimumWindowAllocation);
			assertClearedRetainedSlot(scheduler);
		} finally {
			scheduler.disposeNow();
		}
	}

	private static AllocationWindow allocationWindow(RWScheduler scheduler,
			RequestContext context,
			ThreadMXBean threadMetrics,
			long threadId) {
		long before = threadMetrics.getThreadAllocatedBytes(threadId);
		Object result = null;
		for (int index = 0; index < 1_000_000; index++) {
			result = scheduler.withDeadlineBinding(context, 111L, RETURN_MARKER);
		}
		return new AllocationWindow(threadMetrics.getThreadAllocatedBytes(threadId) - before, result);
	}

	private static void assertClearedRetainedSlot(RWScheduler scheduler) throws Exception {
		Object slot = bindingThreadLocal(scheduler).get();
		Field context = slot.getClass().getDeclaredField("context");
		context.setAccessible(true);
		Field deadline = slot.getClass().getDeclaredField("localMonotonicDeadlineNanos");
		deadline.setAccessible(true);
		assertNull(context.get(slot));
		assertEquals(Long.MIN_VALUE, deadline.getLong(slot));
	}

	@SuppressWarnings("unchecked")
	private static ThreadLocal<Object> bindingThreadLocal(RWScheduler scheduler)
			throws ReflectiveOperationException {
		Field field = RWScheduler.class.getDeclaredField("localDeadlineBinding");
		field.setAccessible(true);
		return (ThreadLocal<Object>) field.get(scheduler);
	}

	private static RWScheduler scheduler(MutableClock clock, String name) {
		try {
			Method factory = RWScheduler.class.getDeclaredMethod("forTesting",
					int.class, int.class, int.class, int.class, int.class, String.class,
					LongSupplier.class, LongSupplier.class);
			factory.setAccessible(true);
			return (RWScheduler) factory.invoke(null, 1, 1, 1, 8, 8, name,
					(LongSupplier) clock::epochMillis, (LongSupplier) clock::nanoTime);
		} catch (ReflectiveOperationException reflectionFailure) {
			throw new AssertionError(reflectionFailure);
		}
	}

	private static final class MutableClock {

		private final AtomicLong epochMillis;
		private final AtomicLong nanoTime;

		private MutableClock(long epochMillis, long nanoTime) {
			this.epochMillis = new AtomicLong(epochMillis);
			this.nanoTime = new AtomicLong(nanoTime);
		}

		private long epochMillis() {
			return epochMillis.get();
		}

		private long nanoTime() {
			return nanoTime.get();
		}
	}

	private static final class SyntheticScopeFailure extends RuntimeException {
	}

	private record AllocationWindow(long allocatedBytes, Object result) {
	}
}
