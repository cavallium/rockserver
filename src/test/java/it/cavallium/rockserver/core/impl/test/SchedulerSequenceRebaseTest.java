package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.scheduler.Scheduler;

/** Regression coverage for the scheduler's lifetime-long FIFO/EDF ordering ticket. */
@Timeout(60)
class SchedulerSequenceRebaseTest {

	@Test
	void everySequenceFieldIsPartOfTheExplicitRebaseInventory() throws Exception {
		var executor = Class.forName("it.cavallium.rockserver.core.impl.ProfiledWorkloadExecutor");
		var classes = new ArrayList<Class<?>>();
		classes.add(executor);
		classes.addAll(Arrays.asList(executor.getDeclaredClasses()));
		var actual = new HashSet<String>();
		for (var type : classes) {
			for (var field : type.getDeclaredFields()) {
				if (!Modifier.isStatic(field.getModifiers())
						&& field.getType() == long.class
						&& field.getName().toLowerCase(java.util.Locale.ROOT).contains("sequence")) {
					actual.add(type.getSimpleName() + '#' + field.getName());
				}
			}
		}
		assertEquals(Set.of(
				"ProfiledWorkloadExecutor#sequence",
				"DeferredAdmission#sequence",
				"WorkloadTask#deadlineSequence",
				"CooperativeWorkloadTask#sequence"), actual,
				"a new order-bearing sequence must be added deliberately to rebaseSequencesUnsafe");
	}

	@Test
	void equalDeadlineEdfRemainsFifoAcrossTheTicketBoundary() throws Exception {
		var scheduler = scheduler("sequence-edf");
		var blocker = blockReadPool(scheduler);
		var observed = new ArrayList<Integer>();
		var completed = new CountDownLatch(3);
		long deadline = System.currentTimeMillis() + SECONDS.toMillis(30);
		try {
			forceNextSequence(scheduler, Long.MAX_VALUE - 1L);
			var latency = scheduler.scheduler(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					deadline);
			for (int id = 0; id < 3; id++) {
				int taskId = id;
				latency.schedule(() -> {
					observed.add(taskId);
					completed.countDown();
				});
			}

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(0, 1, 2), observed,
					"equal-deadline EDF must retain submission order across a ticket rebase");
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void normalAndCooperativeFifoHeadsRemainOrderedAcrossTheTicketBoundary() throws Exception {
		var scheduler = scheduler("sequence-normal-cooperative");
		var blocker = blockReadPool(scheduler);
		var observed = new ArrayList<Integer>();
		var completed = new CountDownLatch(3);
		try {
			forceNextSequence(scheduler, Long.MAX_VALUE - 1L);
			var ingest = scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			ingest.execute(() -> record(0, observed, completed));
			ingest.executeCooperatively(new CompletingCooperativeTask(1, observed, completed), 1L);
			ingest.execute(() -> record(2, observed, completed));

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(0, 1, 2), observed,
					"normal and cooperative heads share one FIFO order across a ticket rebase");
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void repeatedRebasesPreserveCancellationAndRemainingEdfOrder() throws Exception {
		var scheduler = scheduler("sequence-repeated-cancel");
		var blocker = blockReadPool(scheduler);
		var observed = new ArrayList<Integer>();
		var completed = new CountDownLatch(3);
		long deadline = System.currentTimeMillis() + SECONDS.toMillis(30);
		try {
			var latency = scheduler.scheduler(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					deadline);
			forceNextSequence(scheduler, Long.MAX_VALUE);
			latency.schedule(() -> record(0, observed, completed));
			Disposable cancelled = latency.schedule(() -> record(99, observed, completed));
			cancelled.dispose();

			forceNextSequence(scheduler, Long.MAX_VALUE);
			latency.schedule(() -> record(1, observed, completed));
			latency.schedule(() -> record(2, observed, completed));

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(0, 1, 2), observed);
			assertTrue(cancelled.isDisposed());
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void deferredDeadlineOrderSurvivesARebaseWhileTheRealBatchQueueIsFull() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 2, "sequence-deferred");
		var blocker = blockReadPool(scheduler);
		var queuedCompleted = new CountDownLatch(2);
		var rejected = new CountDownLatch(2);
		var rejectedOrder = new ArrayList<Integer>();
		long deadline = System.currentTimeMillis() + 250L;
		try {
			var batch = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			batch.execute(queuedCompleted::countDown);
			batch.execute(queuedCompleted::countDown);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.queuedByProfile().get(WorkloadProfile.BATCH) == 2);

			forceNextSequence(scheduler, Long.MAX_VALUE);
			var deadlineBatch = scheduler.scheduler(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					deadline);
			executeWhenCapacity(deadlineBatch, new DeferredProbe(0, rejectedOrder, rejected));
			executeWhenCapacity(deadlineBatch, new DeferredProbe(1, rejectedOrder, rejected));

			awaitWallDeadline(deadline);
			blocker.release().countDown();
			assertTrue(rejected.await(5, SECONDS));
			assertEquals(List.of(0, 1), rejectedOrder,
					"equal-deadline deferred admission must expire in FIFO order after rebasing");
			assertTrue(queuedCompleted.await(5, SECONDS));
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void concurrentEqualDeadlineSubmissionsCannotOvertakeAnOlderQueuedTaskAtRebase() throws Exception {
		var scheduler = scheduler("sequence-concurrent");
		var blocker = blockReadPool(scheduler);
		int concurrentTasks = 32;
		var observed = Collections.synchronizedList(new ArrayList<Integer>());
		var completed = new CountDownLatch(concurrentTasks + 1);
		var ready = new CountDownLatch(concurrentTasks);
		var start = new CountDownLatch(1);
		long deadline = System.currentTimeMillis() + SECONDS.toMillis(30);
		try (var submitters = Executors.newVirtualThreadPerTaskExecutor()) {
			var latency = scheduler.scheduler(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					deadline);
			latency.schedule(() -> record(-1, observed, completed));
			forceNextSequence(scheduler, Long.MAX_VALUE);
			for (int id = 0; id < concurrentTasks; id++) {
				int taskId = id;
				submitters.submit(() -> {
					ready.countDown();
					awaitUninterruptibly(start);
					latency.schedule(() -> record(taskId, observed, completed));
				});
			}
			assertTrue(ready.await(5, SECONDS));
			start.countDown();
			submitters.shutdown();
			assertTrue(submitters.awaitTermination(5, SECONDS));

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(-1, observed.getFirst(),
					"the task queued before the concurrent rebase wave must remain first");
			assertEquals(concurrentTasks + 1, observed.stream().distinct().count());
			assertEventuallyConserved(scheduler);
		} finally {
			start.countDown();
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void finiteDeadlineCooperativeDispatchPreservesBothTicketsAfterYieldAndRebase() throws Exception {
		var scheduler = scheduler("sequence-cooperative-two-ticket-dispatch");
		var initialBlocker = blockReadPool(scheduler);
		var foregroundStarted = new CountDownLatch(1);
		var releaseForeground = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var observed = new ArrayList<Integer>();
		long deadline = System.currentTimeMillis() + SECONDS.toMillis(30);
		try {
			forceNextSequence(scheduler, Long.MAX_VALUE - 3L);
			var ingest = scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					deadline);
			var cooperative = new YieldOnceTask(0, observed, completed, null, null);
			ingest.executeCooperatively(cooperative, 1L);
			ingest.execute(() -> {
				foregroundStarted.countDown();
				awaitUninterruptibly(releaseForeground);
			});

			initialBlocker.release().countDown();
			assertTrue(foregroundStarted.await(5, SECONDS),
					"the cooperative task must yield before the foreground blocker starts");
			assertEquals(1, cooperative.quantums().get());

			forceNextSequence(scheduler, Long.MAX_VALUE);
			ingest.execute(() -> record(1, observed, completed));
			releaseForeground.countDown();

			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(0, 1), observed,
					"the yielded cooperative dispatch ticket must remain older than a post-rebase task");
			assertEventuallyConserved(scheduler);
		} finally {
			initialBlocker.release().countDown();
			releaseForeground.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void finiteDeadlineCooperativeExpiryPreservesAdmissionTicketAfterYieldAndRebase() throws Exception {
		var scheduler = scheduler("sequence-cooperative-two-ticket-expiry");
		var initialBlocker = blockReadPool(scheduler);
		var foregroundStarted = new CountDownLatch(1);
		var releaseForeground = new CountDownLatch(1);
		var rejected = new CountDownLatch(2);
		var rejectionOrder = new ArrayList<Integer>();
		long deadline = System.currentTimeMillis() + 500L;
		try {
			forceNextSequence(scheduler, Long.MAX_VALUE - 3L);
			var ingest = scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					deadline);
			var cooperative = new YieldOnceTask(0, null, null, rejectionOrder, rejected);
			ingest.executeCooperatively(cooperative, 1L);
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				foregroundStarted.countDown();
				awaitUninterruptibly(releaseForeground);
			});

			initialBlocker.release().countDown();
			assertTrue(foregroundStarted.await(5, SECONDS));
			assertEquals(1, cooperative.quantums().get());
			forceNextSequence(scheduler, Long.MAX_VALUE);
			ingest.execute(new ExpiryProbe(1, rejectionOrder, rejected));

			awaitWallDeadline(deadline);
			releaseForeground.countDown();
			assertTrue(rejected.await(5, SECONDS));
			assertEquals(List.of(0, 1), rejectionOrder,
					"rebasing the yielded dispatch ticket must also preserve its older deadline ticket");
			assertEventuallyConserved(scheduler);
		} finally {
			initialBlocker.release().countDown();
			releaseForeground.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void deterministicBoundaryWindowsPreserveEdfOrderAndCancellation() throws Exception {
		for (int scenario = 0; scenario < 24; scenario++) {
			var scheduler = scheduler("sequence-window-" + scenario);
			var blocker = blockReadPool(scheduler);
			int tasks = 4 + scenario % 7;
			int distance = 1 + scenario % 3;
			var observed = new ArrayList<Integer>();
			int cancelledId = scenario % tasks;
			var completed = new CountDownLatch(tasks - 1);
			long deadline = System.currentTimeMillis() + SECONDS.toMillis(30);
			try {
				forceNextSequence(scheduler, Long.MAX_VALUE - distance);
				var latency = scheduler.scheduler(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						deadline);
				var handles = new Disposable[tasks];
				for (int id = 0; id < tasks; id++) {
					int taskId = id;
					handles[id] = latency.schedule(() -> record(taskId, observed, completed));
				}
				handles[cancelledId].dispose();
				blocker.release().countDown();
				assertTrue(completed.await(5, SECONDS));
				var expected = java.util.stream.IntStream.range(0, tasks)
						.filter(id -> id != cancelledId)
						.boxed()
						.toList();
				assertEquals(expected, observed, "boundary scenario " + scenario);
				assertEventuallyConserved(scheduler);
			} finally {
				blocker.release().countDown();
				scheduler.disposeNow();
			}
		}
	}

	private static RWScheduler scheduler(String name) {
		return RWScheduler.forTesting(1, 1, 1, 64, 64, name);
	}

	private static Blocker blockReadPool(RWScheduler scheduler) throws InterruptedException {
		var started = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				RequestContext.NO_DEADLINE).execute(() -> {
			started.countDown();
			awaitUninterruptibly(release);
		});
		assertTrue(started.await(5, SECONDS));
		return new Blocker(release);
	}

	private static void forceNextSequence(RWScheduler scheduler, long value) throws Exception {
		var poolField = RWScheduler.class.getDeclaredField("readPool");
		poolField.setAccessible(true);
		var pool = poolField.get(scheduler);
		var sequenceField = pool.getClass().getDeclaredField("sequence");
		sequenceField.setAccessible(true);
		sequenceField.setLong(pool, value);
	}

	private static Disposable executeWhenCapacity(Scheduler scheduler, Runnable command) throws Exception {
		var method = scheduler.getClass().getDeclaredMethod("executeWhenCapacity", Runnable.class);
		method.setAccessible(true);
		try {
			return (Disposable) method.invoke(scheduler, command);
		} catch (InvocationTargetException failure) {
			if (failure.getCause() instanceof Exception exception) throw exception;
			if (failure.getCause() instanceof Error error) throw error;
			throw failure;
		}
	}

	private static void awaitWallDeadline(long deadlineEpochMillis) throws InterruptedException {
		while (System.currentTimeMillis() <= deadlineEpochMillis) {
			MILLISECONDS.sleep(Math.max(1L, Math.min(10L, deadlineEpochMillis - System.currentTimeMillis() + 1L)));
		}
	}

	private static void record(int id, List<Integer> observed, CountDownLatch completed) {
		observed.add(id);
		completed.countDown();
	}

	private static void assertEventuallyConserved(RWScheduler scheduler) throws InterruptedException {
		assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
	}

	private static void assertEventually(java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			MILLISECONDS.sleep(1L);
		}
		assertTrue(condition.getAsBoolean());
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		for (;;) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}

	private record Blocker(CountDownLatch release) {
	}

	private record DeferredProbe(int id,
			List<Integer> rejectedOrder,
			CountDownLatch rejected,
			AtomicBoolean ran) implements Runnable, RWScheduler.RejectionAwareTask {

		private DeferredProbe(int id, List<Integer> rejectedOrder, CountDownLatch rejected) {
			this(id, rejectedOrder, rejected, new AtomicBoolean());
		}

		@Override
		public void run() {
			ran.set(true);
		}

		@Override
		public void reject(RuntimeException failure) {
			assertFalse(ran.get());
			rejectedOrder.add(id);
			rejected.countDown();
		}
	}

	private record CompletingCooperativeTask(int id,
			List<Integer> observed,
			CountDownLatch completed) implements RWScheduler.CooperativeCompletionTask {

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			record(id, observed, completed);
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void completeCooperatively() {
		}

		@Override
		public void reject(RuntimeException failure) {
			throw Objects.requireNonNull(failure);
		}
	}

	private record ExpiryProbe(int id,
			List<Integer> rejectionOrder,
			CountDownLatch rejected,
			AtomicBoolean ran) implements Runnable, RWScheduler.RejectionAwareTask {

		private ExpiryProbe(int id, List<Integer> rejectionOrder, CountDownLatch rejected) {
			this(id, rejectionOrder, rejected, new AtomicBoolean());
		}

		@Override
		public void run() {
			ran.set(true);
		}

		@Override
		public void reject(RuntimeException failure) {
			assertFalse(ran.get());
			rejectionOrder.add(id);
			rejected.countDown();
		}
	}

	private static final class YieldOnceTask implements RWScheduler.CooperativeCompletionTask {
		private final int id;
		private final List<Integer> observed;
		private final CountDownLatch completed;
		private final List<Integer> rejectionOrder;
		private final CountDownLatch rejected;
		private final AtomicInteger quantums = new AtomicInteger();

		private YieldOnceTask(int id,
				List<Integer> observed,
				CountDownLatch completed,
				List<Integer> rejectionOrder,
				CountDownLatch rejected) {
			this.id = id;
			this.observed = observed;
			this.completed = completed;
			this.rejectionOrder = rejectionOrder;
			this.rejected = rejected;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			if (quantums.incrementAndGet() == 1) {
				return RWScheduler.CooperativeResult.YIELD;
			}
			record(id, Objects.requireNonNull(observed), Objects.requireNonNull(completed));
			return RWScheduler.CooperativeResult.COMPLETE;
		}

		@Override
		public void completeCooperatively() {
		}

		@Override
		public void reject(RuntimeException failure) {
			Objects.requireNonNull(failure);
			Objects.requireNonNull(rejectionOrder).add(id);
			Objects.requireNonNull(rejected).countDown();
		}

		private AtomicInteger quantums() {
			return quantums;
		}
	}
}
