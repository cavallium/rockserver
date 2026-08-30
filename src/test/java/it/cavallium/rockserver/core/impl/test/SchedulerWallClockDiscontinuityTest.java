package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(20)
class SchedulerWallClockDiscontinuityTest {

	@Test
	void finiteAdmissionPathsShareOneClockPairAndPreserveBoundSidecars() {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-coherent-admission-sample");
		try {
			clock.resetReadCounts();
			assertThrows(RocksDBException.class, () -> scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					clock.rawEpochMillis()).execute(new TerminalProbe()));
			assertClockReads(clock, 1L, 1L);

			clock.resetReadCounts();
			assertThrows(RocksDBException.class, () -> scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.rawEpochMillis()).executeCooperatively(new CooperativeProbe(), 1L));
			assertClockReads(clock, 1L, 1L);

			clock.resetReadCounts();
			var deferred = scheduler.scheduler(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					clock.rawEpochMillis());
			assertThrows(RocksDBException.class,
					() -> executeWhenCapacity(deferred, new TerminalProbe()));
			assertClockReads(clock, 1L, 1L);

			clock.resetReadCounts();
			assertThrows(RocksDBException.class, () -> scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					2_000L,
					0L).execute(new TerminalProbe()));
			assertClockReads(clock, 0L, 1L);
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void alignedQueuedAdmissionsTakeExactlyOneClockPairEach() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-aligned-clock-calls");
		var release = occupyReadWorker(scheduler);
		try {
			clock.resetReadCounts();
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					2_000L).execute(new TerminalProbe());
			clock.advanceNanos(MILLISECONDS.toNanos(10L));
			clock.jumpWallMillis(10L);
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					3_000L).execute(new TerminalProbe());

			assertClockReads(clock, 2L, 2L);
			assertFalse(readPoolBoolean(scheduler, "latencyExpiryIndexed"));
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void contendedAdmissionRefreshesMonotonicNowWithoutRebindingWallTime() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-contended-admission-refresh");
		var poolLock = readPoolLock(scheduler);
		var failure = new AtomicReference<Throwable>();
		var probe = new TerminalProbe();
		Thread submitter = null;
		poolLock.lock();
		try {
			clock.resetReadCounts();
			submitter = Thread.ofPlatform().start(() -> {
				try {
					scheduler.executor(WorkloadProfile.LATENCY,
							OperationFamily.POINT_LOOKUP,
							clock.rawEpochMillis() + 100L).execute(probe);
					failure.set(new AssertionError("expired contended admission was accepted"));
				} catch (Throwable terminal) {
					failure.set(terminal);
				}
			});
			assertTrue(awaitCondition(() -> clock.epochMillisReads() == 1L
					&& clock.nanoTimeReads() == 1L, 5_000L),
					"the immutable deadline must be bound before waiting for the pool lock");
			clock.advanceNanos(MILLISECONDS.toNanos(101L));
			clock.jumpWallMillis(-SECONDS.toMillis(30L));
		} finally {
			poolLock.unlock();
		}
		try {
			if (submitter != null) {
				submitter.join(5_000L);
				assertFalse(submitter.isAlive());
			}
			assertInstanceOf(RocksDBException.class, failure.get());
			assertClockReads(clock, 1L, 2L);
			assertFalse(probe.ran(), "lock waiting must consume the original monotonic budget");
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void backwardWallJumpCannotExtendAQueuedDeadline() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-backward-queued");
		var release = occupyReadWorker(scheduler);
		var probe = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.epochMillis() + 100L).execute(probe);

			clock.advanceNanos(MILLISECONDS.toNanos(101L));
			clock.jumpWallMillis(-SECONDS.toMillis(30L));
			release.countDown();

			assertDeadline(probe);
			assertFalse(probe.ran(), "a backward wall jump must not grant extra runtime");
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void forwardWallJumpCannotPrematurelyExpireAQueuedDeadline() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-forward-queued");
		var release = occupyReadWorker(scheduler);
		var probe = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.epochMillis() + 100L).execute(probe);

			clock.advanceNanos(MILLISECONDS.toNanos(50L));
			clock.jumpWallMillis(SECONDS.toMillis(30L));
			release.countDown();

			assertTrue(probe.ranSignal.await(5, SECONDS));
			assertTrue(probe.ran());
			assertFalse(probe.failure.isDone(), "elapsed monotonic time is still inside the budget");
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void absoluteEdfAndMonotonicExpiryRemainIndependentAcrossAdmissions() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-independent-orders");
		var release = occupyReadWorker(scheduler);
		var laterEpochEarlierExpiry = new TerminalProbe();
		var earlierEpochLaterExpiry = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					2_000L).execute(laterEpochEarlierExpiry);

			clock.advanceNanos(MILLISECONDS.toNanos(100L));
			clock.jumpWallMillis(-10_000L);
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					1_500L).execute(earlierEpochLaterExpiry);

			clock.advanceNanos(MILLISECONDS.toNanos(1_000L));
			release.countDown();

			assertDeadline(laterEpochEarlierExpiry);
			assertTrue(earlierEpochLaterExpiry.ranSignal.await(5, SECONDS));
			assertTrue(earlierEpochLaterExpiry.ran(),
					"absolute EDF may select the earlier epoch only after monotonic expiry removes its peer");
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void absoluteEdfStillControlsDispatchWhenExpiryOrderDiverges() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-divergent-dispatch-orders");
		var release = occupyReadWorker(scheduler);
		var nextOrder = new AtomicLong();
		var laterEpochEarlierExpiry = new OrderedProbe(nextOrder);
		var earlierEpochLaterExpiry = new OrderedProbe(nextOrder);
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					4_000L).execute(laterEpochEarlierExpiry);

			clock.advanceNanos(MILLISECONDS.toNanos(100L));
			clock.jumpWallMillis(-10_000L);
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					1_500L).execute(earlierEpochLaterExpiry);

			clock.advanceNanos(MILLISECONDS.toNanos(100L));
			release.countDown();

			assertTrue(laterEpochEarlierExpiry.ranSignal.await(5, SECONDS));
			assertTrue(earlierEpochLaterExpiry.ranSignal.await(5, SECONDS));
			assertEquals(0L, earlierEpochLaterExpiry.order,
					"EDF must use the absolute epoch even when that task has the later local expiry");
			assertEquals(1L, laterEpochEarlierExpiry.order);
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void alignedEpochBindingsUseOnlyTheEdfHeapAndResetWhenDrained() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-aligned-single-index");
		var release = occupyReadWorker(scheduler);
		var first = new TerminalProbe();
		var second = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					2_000L).execute(first);
			clock.advanceNanos(MILLISECONDS.toNanos(10L));
			clock.jumpWallMillis(10L);
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					3_000L).execute(second);

			assertTrue(readPoolBoolean(scheduler, "latencyBindingPresent"));
			assertFalse(readPoolBoolean(scheduler, "latencyExpiryIndexed"));
			assertEquals(0, readPoolDeadlineHeapSize(scheduler),
					"aligned LATENCY work must not enter the secondary expiry heap");
			assertEquals(List.of("deadlineEpochMillis", "latencyHeapIndex"),
					queuedLatencyTaskFields(scheduler),
					"ordinary LATENCY task shape must stay at the single-heap baseline");

			release.countDown();
			assertTrue(first.ranSignal.await(5, SECONDS));
			assertTrue(second.ranSignal.await(5, SECONDS));
			assertTrue(awaitCondition(() -> {
				try {
					return !readPoolBoolean(scheduler, "latencyBindingPresent")
							&& !readPoolBoolean(scheduler, "latencyExpiryIndexed");
				} catch (ReflectiveOperationException failure) {
					throw new AssertionError(failure);
				}
			}, 5_000L));
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void equalEpochAcrossClockGenerationsKeepsFifoAndAdaptiveIndexResets() throws Exception {
		var clock = new MutableClock(1_000L, 0L);
		var scheduler = scheduler(clock, "deadline-equal-epoch-adaptive");
		var release = occupyReadWorker(scheduler);
		var nextOrder = new AtomicLong();
		var first = new OrderedProbe(nextOrder);
		var second = new OrderedProbe(nextOrder);
		try {
			long commonEpochDeadline = 20_000L;
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					commonEpochDeadline).execute(first);
			clock.advanceNanos(MILLISECONDS.toNanos(100L));
			clock.jumpWallMillis(-10_000L);
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					commonEpochDeadline).execute(second);

			assertTrue(readPoolBoolean(scheduler, "latencyExpiryIndexed"));
			assertEquals(2, readPoolDeadlineHeapSize(scheduler));
			release.countDown();

			assertTrue(first.ranSignal.await(5, SECONDS));
			assertTrue(second.ranSignal.await(5, SECONDS));
			assertEquals(0L, first.order,
					"equal absolute epochs must remain FIFO even when local expiry generations differ");
			assertEquals(1L, second.order);
			assertTrue(awaitCondition(() -> {
				try {
					return !readPoolBoolean(scheduler, "latencyBindingPresent")
							&& !readPoolBoolean(scheduler, "latencyExpiryIndexed")
							&& readPoolDeadlineHeapSize(scheduler) == 0;
				} catch (ReflectiveOperationException failure) {
					throw new AssertionError(failure);
				}
			}, 5_000L));
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void adaptiveExpiryIndexSurvivesHeapMovementAndIndexedCancellation() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-adaptive-index-stress", 128, 8);
		var release = occupyReadWorker(scheduler);
		var probes = new ArrayList<TerminalProbe>();
		var disposables = new ArrayList<reactor.core.Disposable>();
		try {
			for (int index = 0; index < 96; index++) {
				var probe = new TerminalProbe();
				probes.add(probe);
				long deadline = 2_000_000L + (index * 17L) % 101L;
				disposables.add(scheduler.scheduler(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						deadline).schedule(probe));
				clock.advanceNanos(MILLISECONDS.toNanos(1L));
				clock.jumpWallMillis((index & 1) == 0 ? 10_000L : -10_000L);
			}
			assertTrue(readPoolBoolean(scheduler, "latencyExpiryIndexed"));
			assertEquals(96, readPoolDeadlineHeapSize(scheduler));
			for (int index = 0; index < disposables.size(); index += 3) {
				disposables.get(index).dispose();
			}
			assertTrue(awaitCondition(() -> {
				try {
					return readPoolDeadlineHeapSize(scheduler) == 64;
				} catch (ReflectiveOperationException failure) {
					throw new AssertionError(failure);
				}
			}, 5_000L));

			release.countDown();
			for (int index = 0; index < probes.size(); index++) {
				if (index % 3 == 0) {
					assertFalse(probes.get(index).ranSignal.await(10, MILLISECONDS));
				} else {
					assertTrue(probes.get(index).ranSignal.await(5, SECONDS));
				}
			}
			assertTrue(awaitCondition(() -> {
				try {
					return !readPoolBoolean(scheduler, "latencyExpiryIndexed")
							&& readPoolDeadlineHeapSize(scheduler) == 0;
				} catch (ReflectiveOperationException failure) {
					throw new AssertionError(failure);
				}
			}, 5_000L));
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void backwardWallJumpCannotExtendDeferredAdmission() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-backward-deferred", 1, 1);
		var release = occupyReadWorker(scheduler);
		var queued = new TerminalProbe();
		var deferred = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					Long.MAX_VALUE).execute(queued);
			var indexed = scheduler.scheduler(
					WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					clock.epochMillis() + 100L);
			executeWhenCapacity(indexed, deferred);

			clock.advanceNanos(MILLISECONDS.toNanos(101L));
			clock.jumpWallMillis(-SECONDS.toMillis(30L));
			release.countDown();

			assertTrue(queued.ranSignal.await(5, SECONDS));
			assertDeadline(deferred);
			assertFalse(deferred.ran(), "pre-admission waiting consumes the same immutable budget");
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void activeCooperativeDeadlineUsesElapsedMonotonicTime() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-backward-cooperative");
		var task = new CooperativeProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.epochMillis() + 100L).executeCooperatively(task, 1L);
			assertTrue(task.started.await(5, SECONDS));

			clock.advanceNanos(MILLISECONDS.toNanos(101L));
			clock.jumpWallMillis(-SECONDS.toMillis(30L));
			task.checkDeadline.countDown();

			assertTrue(task.terminal.await(5, SECONDS));
			assertTrue(task.observedTermination,
					"active cooperative work must see the same immutable deadline as queued work");
			assertInstanceOf(RocksDBException.class, task.failure.get());
		} finally {
			task.checkDeadline.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void nanoTimeWrapDoesNotExtendADeadline() throws Exception {
		var clock = new MutableClock(1_000_000L, Long.MAX_VALUE - MILLISECONDS.toNanos(50L));
		var scheduler = scheduler(clock, "deadline-nanotime-wrap");
		var release = occupyReadWorker(scheduler);
		var probe = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.epochMillis() + 100L).execute(probe);

			clock.advanceNanos(MILLISECONDS.toNanos(101L));
			release.countDown();

			assertDeadline(probe);
			assertFalse(probe.ran());
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void hugeEpochBudgetSaturatesInsteadOfOverflowingIntoExpiry() throws Exception {
		var clock = new MutableClock(1L, 0L);
		var scheduler = scheduler(clock, "deadline-epoch-overflow");
		var release = occupyReadWorker(scheduler);
		var probe = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					Long.MAX_VALUE - 1L).execute(probe);

			clock.advanceNanos(1L);
			release.countDown();

			assertTrue(probe.ranSignal.await(5, SECONDS));
			assertTrue(probe.ran(), "millisecond-to-nanosecond overflow must saturate to an infinite local budget");
			assertFalse(probe.failure.isDone());
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void regressingMonotonicSourceFailsClosed() throws Exception {
		var clock = new MutableClock(1_000_000L, 0L);
		var scheduler = scheduler(clock, "deadline-monotonic-regression");
		var release = occupyReadWorker(scheduler);
		var probe = new TerminalProbe();
		try {
			scheduler.executor(WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					clock.epochMillis() + 100L).execute(probe);

			clock.advanceNanos(-1L);
			release.countDown();

			assertDeadline(probe);
			assertFalse(probe.ran(), "a broken monotonic source must not grant unbounded runtime");
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	private static RWScheduler scheduler(MutableClock clock, String name) {
		return scheduler(clock, name, 4, 4);
	}

	private static RWScheduler scheduler(MutableClock clock,
			String name,
			int foregroundQueueCapacity,
			int batchQueueCapacity) {
		try {
			Method factory = RWScheduler.class.getDeclaredMethod("forTesting",
					int.class,
					int.class,
					int.class,
					int.class,
					int.class,
					String.class,
					LongSupplier.class,
					LongSupplier.class);
			factory.setAccessible(true);
			return (RWScheduler) factory.invoke(null,
					1,
					1,
					1,
					foregroundQueueCapacity,
					batchQueueCapacity,
					name,
					(LongSupplier) clock::epochMillis,
					(LongSupplier) clock::nanoTime);
		} catch (ReflectiveOperationException reflectionFailure) {
			throw new AssertionError(reflectionFailure);
		}
	}

	private static void executeWhenCapacity(reactor.core.scheduler.Scheduler scheduler, Runnable command) {
		try {
			var method = scheduler.getClass().getDeclaredMethod("executeWhenCapacity", Runnable.class);
			method.setAccessible(true);
			method.invoke(scheduler, command);
		} catch (InvocationTargetException invocation) {
			if (invocation.getCause() instanceof RuntimeException runtime) {
				throw runtime;
			}
			throw new AssertionError(invocation.getCause());
		} catch (ReflectiveOperationException reflectionFailure) {
			throw new AssertionError(reflectionFailure);
		}
	}

	private static CountDownLatch occupyReadWorker(RWScheduler scheduler) throws Exception {
		var started = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				Long.MAX_VALUE).execute(() -> {
			started.countDown();
			awaitUninterruptibly(release);
		});
		assertTrue(started.await(5, SECONDS));
		return release;
	}

	private static void assertDeadline(TerminalProbe probe) throws Exception {
		Throwable failure = probe.failure.get(5, SECONDS);
		var deadline = assertInstanceOf(RocksDBException.class, failure);
		assertTrue(deadline.getErrorUniqueId()
				== RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED);
	}

	private static boolean readPoolBoolean(RWScheduler scheduler, String fieldName)
			throws ReflectiveOperationException {
		Object readPool = readPool(scheduler);
		Field field = readPool.getClass().getDeclaredField(fieldName);
		field.setAccessible(true);
		return field.getBoolean(readPool);
	}

	private static int readPoolDeadlineHeapSize(RWScheduler scheduler) throws ReflectiveOperationException {
		Object readPool = readPool(scheduler);
		Field queueField = readPool.getClass().getDeclaredField("deadlineQueue");
		queueField.setAccessible(true);
		Object queue = queueField.get(readPool);
		Field size = queue.getClass().getDeclaredField("size");
		size.setAccessible(true);
		return size.getInt(queue);
	}

	private static List<String> queuedLatencyTaskFields(RWScheduler scheduler)
			throws ReflectiveOperationException {
		Object readPool = readPool(scheduler);
		Field queueField = readPool.getClass().getDeclaredField("latencyQueue");
		queueField.setAccessible(true);
		Object queue = queueField.get(readPool);
		Field elementsField = queue.getClass().getDeclaredField("elements");
		elementsField.setAccessible(true);
		Object[] elements = (Object[]) elementsField.get(queue);
		Object task = Arrays.stream(elements).filter(java.util.Objects::nonNull).findFirst().orElseThrow();
		return Arrays.stream(task.getClass().getDeclaredFields()).map(Field::getName).sorted().toList();
	}

	private static Object readPool(RWScheduler scheduler) throws ReflectiveOperationException {
		Field field = RWScheduler.class.getDeclaredField("readPool");
		field.setAccessible(true);
		return field.get(scheduler);
	}

	private static ReentrantLock readPoolLock(RWScheduler scheduler) throws ReflectiveOperationException {
		Object readPool = readPool(scheduler);
		Field field = readPool.getClass().getDeclaredField("lock");
		field.setAccessible(true);
		return (ReentrantLock) field.get(readPool);
	}

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition, long timeoutMillis)
			throws InterruptedException {
		long deadline = System.nanoTime() + MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		return condition.getAsBoolean();
	}

	private static void assertClockReads(MutableClock clock, long expectedEpoch, long expectedNano) {
		assertEquals(expectedEpoch, clock.epochMillisReads(), "unexpected wall-clock sample count");
		assertEquals(expectedNano, clock.nanoTimeReads(), "unexpected monotonic-clock sample count");
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static final class MutableClock {

		private final AtomicLong epochMillis;
		private final AtomicLong nanoTime;
		private final AtomicLong epochMillisReads = new AtomicLong();
		private final AtomicLong nanoTimeReads = new AtomicLong();

		private MutableClock(long epochMillis, long nanoTime) {
			this.epochMillis = new AtomicLong(epochMillis);
			this.nanoTime = new AtomicLong(nanoTime);
		}

		private long epochMillis() {
			epochMillisReads.incrementAndGet();
			return epochMillis.get();
		}

		private long nanoTime() {
			nanoTimeReads.incrementAndGet();
			return nanoTime.get();
		}

		private long rawEpochMillis() {
			return epochMillis.get();
		}

		private void resetReadCounts() {
			epochMillisReads.set(0L);
			nanoTimeReads.set(0L);
		}

		private long epochMillisReads() {
			return epochMillisReads.get();
		}

		private long nanoTimeReads() {
			return nanoTimeReads.get();
		}

		private void jumpWallMillis(long delta) {
			epochMillis.addAndGet(delta);
		}

		private void advanceNanos(long delta) {
			nanoTime.addAndGet(delta);
		}
	}

	private static final class TerminalProbe implements Runnable, RWScheduler.RejectionAwareTask {

		private final CountDownLatch ranSignal = new CountDownLatch(1);
		private final CompletableFuture<Throwable> failure = new CompletableFuture<>();
		private volatile boolean ran;

		@Override
		public void run() {
			ran = true;
			ranSignal.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.complete(failure);
		}

		private boolean ran() {
			return ran;
		}
	}

	private static final class OrderedProbe implements Runnable, RWScheduler.RejectionAwareTask {

		private final AtomicLong nextOrder;
		private final CountDownLatch ranSignal = new CountDownLatch(1);
		private volatile long order = -1L;

		private OrderedProbe(AtomicLong nextOrder) {
			this.nextOrder = nextOrder;
		}

		@Override
		public void run() {
			order = nextOrder.getAndIncrement();
			ranSignal.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			throw new AssertionError("ordered task was rejected", failure);
		}
	}

	private static final class CooperativeProbe implements RWScheduler.CooperativeTask {

		private final CountDownLatch started = new CountDownLatch(1);
		private final CountDownLatch checkDeadline = new CountDownLatch(1);
		private final CountDownLatch terminal = new CountDownLatch(1);
		private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
		private volatile boolean observedTermination;

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			started.countDown();
			awaitUninterruptibly(checkDeadline);
			observedTermination = context.terminationRequested();
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			terminal.countDown();
		}
	}
}
