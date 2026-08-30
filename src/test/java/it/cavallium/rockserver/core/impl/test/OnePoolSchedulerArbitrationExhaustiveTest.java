package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;

/** Exhaustive bounded arbitration checks against simple independent queue models. */
@Timeout(90)
class OnePoolSchedulerArbitrationExhaustiveTest {

	private static final WorkloadProfile[] GUARANTEED = {
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH
	};
	private static final Map<WorkloadProfile, Integer> QUANTA = Map.of(
			WorkloadProfile.INGEST, 4,
			WorkloadProfile.CDC, 4,
			WorkloadProfile.ANALYTICAL, 2,
			WorkloadProfile.BATCH, 1);

	@Test
	void everyThreeTaskEdfPermutationAndCancellationSubsetMatchesDeadlineSequenceOrder()
			throws Exception {
		long scenarios = 0L;
		for (int[] permutation : permutations(new int[] {0, 1, 2})) {
			for (int cancellationMask = 0; cancellationMask < 1 << 3; cancellationMask++) {
				assertEdfScenario(permutation, cancellationMask);
				scenarios++;
			}
		}
		assertEquals(48L, scenarios);
	}

	@Test
	void everySmallGuaranteedProfileAndCostCombinationMatchesIndependentDrr() throws Exception {
		long scenarios = 0L;
		for (int ternary = 1; ternary < 81; ternary++) {
			int value = ternary;
			var tasks = new ArrayList<Task>();
			for (int index = 0; index < GUARANTEED.length; index++) {
				int digit = value % 3;
				value /= 3;
				if (digit != 0) tasks.add(new Task(index, GUARANTEED[index], digit == 1 ? 1 : 5));
			}
			assertDrrScenario(tasks);
			scenarios++;
		}
		assertEquals(80L, scenarios);
	}

	@Test
	void latencyPriorityIsBoundedByTheConfiguredBurstBeforeGuaranteedWorkRuns() throws Exception {
		var scheduler = scheduler("exhaustive-latency-burst");
		var blocker = blockWithBatch(scheduler);
		var observed = new ArrayList<Integer>();
		var completed = new CountDownLatch(10);
		long deadlineBase = System.currentTimeMillis() + SECONDS.toMillis(30);
		try {
			for (int index = 0; index < 9; index++) {
				int id = index;
				scheduler.executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						deadlineBase + index).execute(() -> {
					observed.add(id);
					completed.countDown();
				});
			}
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				observed.add(99);
				completed.countDown();
			});

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 99, 8), observed,
					"LATENCY must retain priority but yield after exactly the configured burst");
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void everyDataProfileRejectsAnAlreadyExpiredDeadlineWithoutExecution() throws Exception {
		var scheduler = scheduler("exhaustive-expired-deadline");
		var tasks = new ArrayList<DeadlineTask>();
		try {
			for (var profile : new WorkloadProfile[] {
					WorkloadProfile.LATENCY,
					WorkloadProfile.INGEST,
					WorkloadProfile.CDC,
					WorkloadProfile.ANALYTICAL,
					WorkloadProfile.BATCH
			}) {
				var task = new DeadlineTask();
				tasks.add(task);
				assertThrows(RuntimeException.class, () -> scheduler.scheduler(profile,
						profile == WorkloadProfile.LATENCY ? OperationFamily.POINT_LOOKUP : family(profile),
						1L).schedule(task));
			}
			for (var task : tasks) {
				assertTrue(task.rejected().await(5, SECONDS));
				assertEquals(1, task.rejectionCount().get());
				assertTrue(!task.ran().get());
			}
			assertEventuallyConserved(scheduler);
			assertEquals(5L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.DEADLINE));
		} finally {
			scheduler.disposeNow();
		}
	}

	private static void assertEdfScenario(int[] permutation, int cancellationMask) throws Exception {
		var scheduler = scheduler("exhaustive-edf-" + Arrays.toString(permutation) + '-' + cancellationMask);
		var blocker = blockWithBatch(scheduler);
		var observed = new ArrayList<Integer>();
		var handles = new Disposable[3];
		long[] deadlines = {10L, 10L, 20L};
		long deadlineBase = System.currentTimeMillis() + SECONDS.toMillis(30);
		int expectedRuns = 3 - Integer.bitCount(cancellationMask);
		int cancelled = cancellationMask;
		var completed = new CountDownLatch(expectedRuns);
		try {
			for (int submission = 0; submission < permutation.length; submission++) {
				int id = permutation[submission];
				handles[id] = scheduler.scheduler(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						deadlineBase + deadlines[id]).schedule(() -> {
					observed.add(id);
					completed.countDown();
				});
			}
			for (int id = 0; id < 3; id++) {
				if ((cancellationMask & 1 << id) != 0) handles[id].dispose();
			}
			var sequence = new int[3];
			for (int index = 0; index < permutation.length; index++) sequence[permutation[index]] = index;
			var expected = Arrays.stream(permutation)
					.boxed()
					.filter(id -> (cancelled & 1 << id) == 0)
					.sorted(Comparator.comparingLong((Integer id) -> deadlines[id])
							.thenComparingInt(id -> sequence[id]))
					.toList();

			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(expected, observed,
					"EDF divergence for permutation=" + Arrays.toString(permutation)
							+ " cancellationMask=" + cancellationMask);
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	private static void assertDrrScenario(List<Task> tasks) throws Exception {
		var scheduler = scheduler("exhaustive-drr-" + tasks);
		var blocker = blockWithBatch(scheduler);
		var observed = new ArrayList<Integer>();
		var completed = new CountDownLatch(tasks.size());
		try {
			for (Task task : tasks) {
				scheduler.executor(task.profile(), family(task.profile()), RequestContext.NO_DEADLINE)
						.execute(() -> {
							observed.add(task.id());
							completed.countDown();
						}, estimatedBytes(task.cost()));
			}
			List<Integer> expected = drrOrder(tasks);
			blocker.release().countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(expected, observed, "DRR divergence for " + tasks);
			assertEventuallyConserved(scheduler);
		} finally {
			blocker.release().countDown();
			scheduler.disposeNow();
		}
	}

	private static List<Integer> drrOrder(List<Task> tasks) {
		var queues = new EnumMap<WorkloadProfile, ArrayDeque<Task>>(WorkloadProfile.class);
		var deficits = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : GUARANTEED) {
			queues.put(profile, new ArrayDeque<>());
			deficits.put(profile, 0);
		}
		for (Task task : tasks) queues.get(task.profile()).addLast(task);
		var result = new ArrayList<Integer>(tasks.size());
		int cursor = 0;
		boolean needsQuantum = true;
		while (result.size() < tasks.size()) {
			var profile = GUARANTEED[cursor];
			var queue = queues.get(profile);
			if (queue.isEmpty()) {
				deficits.put(profile, 0);
				cursor = (cursor + 1) % GUARANTEED.length;
				needsQuantum = true;
				continue;
			}
			if (needsQuantum) {
				deficits.put(profile, Math.min(WorkloadCost.MAX_UNITS,
						deficits.get(profile) + QUANTA.get(profile)));
				needsQuantum = false;
			}
			Task head = queue.getFirst();
			if (deficits.get(profile) < head.cost()) {
				cursor = (cursor + 1) % GUARANTEED.length;
				needsQuantum = true;
				continue;
			}
			queue.removeFirst();
			result.add(head.id());
			deficits.put(profile, deficits.get(profile) - head.cost());
			if (queue.isEmpty()) {
				deficits.put(profile, 0);
				cursor = (cursor + 1) % GUARANTEED.length;
				needsQuantum = true;
			} else if (deficits.get(profile) < queue.getFirst().cost()) {
				cursor = (cursor + 1) % GUARANTEED.length;
				needsQuantum = true;
			}
		}
		return List.copyOf(result);
	}

	private static Blocker blockWithBatch(RWScheduler scheduler) throws InterruptedException {
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

	private static RWScheduler scheduler(String name) {
		return RWScheduler.forTesting(1, 1, 1, 64, 64, name);
	}

	private static OperationFamily family(WorkloadProfile profile) {
		return switch (profile) {
			case INGEST, BATCH -> OperationFamily.RANGE_PAGE;
			case CDC -> OperationFamily.WAL_PAGE;
			case ANALYTICAL -> OperationFamily.FULL_SCAN_AGGREGATE;
			case LATENCY, CONTROL, PHYSICAL_MAINTENANCE ->
					throw new IllegalArgumentException("not a guaranteed profile: " + profile);
		};
	}

	private static long estimatedBytes(int cost) {
		return cost == 1 ? 1L : (cost - 1L) * WorkloadCost.QUANTUM_BYTES + 1L;
	}

	private static List<int[]> permutations(int[] values) {
		var result = new ArrayList<int[]>();
		permute(values.clone(), 0, result);
		return List.copyOf(result);
	}

	private static void permute(int[] values, int index, List<int[]> result) {
		if (index == values.length) {
			result.add(values.clone());
			return;
		}
		for (int next = index; next < values.length; next++) {
			int temporary = values[index];
			values[index] = values[next];
			values[next] = temporary;
			permute(values, index + 1, result);
			temporary = values[index];
			values[index] = values[next];
			values[next] = temporary;
		}
	}

	private static void assertEventuallyConserved(RWScheduler scheduler) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved()) {
			if (System.nanoTime() >= deadline) break;
			Thread.onSpinWait();
		}
		assertTrue(scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
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

	private record Task(int id, WorkloadProfile profile, int cost) {
	}

	private record Blocker(CountDownLatch release) {
	}

	private record DeadlineTask(AtomicBoolean ran,
			AtomicInteger rejectionCount,
			CountDownLatch rejected) implements Runnable, RWScheduler.RejectionAwareTask {

		private DeadlineTask() {
			this(new AtomicBoolean(), new AtomicInteger(), new CountDownLatch(1));
		}

		@Override
		public void run() {
			ran.set(true);
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
			rejected.countDown();
		}
	}
}
