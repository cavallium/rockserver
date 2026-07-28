package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class RWSchedulerTest {

	@Test
	void latencyUsesEarliestDeadlineFirst() throws Exception {
		var scheduler = scheduler(1, "edf-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(release);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(20)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("later", order, completed));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(10)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("earlier", order, completed));
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of("earlier", "later"), order);
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void queuedDeadlineCompletesExceptionallyWithoutRunning() throws Exception {
		var scheduler = scheduler(1, "queued-deadline");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var expired = new ObservableTask(() -> {});
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			var deadline = new RequestContext(WorkloadProfile.LATENCY, System.currentTimeMillis() + 100L);
			scheduler.executor(deadline, OperationFamily.POINT_LOOKUP).execute(expired);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			Thread.sleep(150L);
			release.countDown();

			var completion = assertThrows(ExecutionException.class, () -> expired.get(5, SECONDS));
			var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
			assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					failure.getErrorUniqueId());
			assertFalse(expired.ran());
			assertEquals(1, expired.disposeCount());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.DEADLINE));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void deadlineIsRecheckedAfterPotentiallySlowMetricLookup() throws Exception {
		var registry = new BlockingTimerRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "metric-deadline-test", registry, "metric-db");
		var task = new ObservableTask(() -> {});
		try {
			registry.blockTimers();
			var deadline = new RequestContext(WorkloadProfile.LATENCY, System.currentTimeMillis() + 100L);
			scheduler.executor(deadline, OperationFamily.POINT_LOOKUP).execute(task);
			assertTrue(registry.timerCreationStarted().await(5, SECONDS));
			Thread.sleep(150L);
			registry.releaseTimers();

			var completion = assertThrows(ExecutionException.class, () -> task.get(5, SECONDS));
			var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
			assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
					failure.getErrorUniqueId());
			assertFalse(task.ran());
			assertEquals(1, task.disposeCount());
		} finally {
			registry.releaseTimers();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void latencyBurstIsBoundedBeforeGuaranteedWorkRuns() throws Exception {
		var scheduler = scheduler(1, "latency-burst");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var order = Collections.synchronizedList(new ArrayList<String>());
		var completed = new CountDownLatch(10);
		try {
			var latency = scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)),
					OperationFamily.POINT_LOOKUP);
			latency.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			for (int i = 0; i < 9; i++) {
				latency.execute(() -> record("latency", order, completed));
			}
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("ingest", order, completed));
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			int ingestIndex = order.indexOf("ingest");
			assertTrue(ingestIndex >= 0 && ingestIndex <= 8,
					"guaranteed work must be reconsidered after at most eight LATENCY tasks");
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void reservationsWinTheNextSlotFromBlockingBorrowers() throws Exception {
		var scheduler = scheduler(3, "reservation-test");
		var batchStarted = new CountDownLatch(2);
		var releaseBatch = new CountDownLatch(1);
		var analyticalStarted = new CountDownLatch(1);
		var releaseAnalytical = new CountDownLatch(1);
		var reservedDone = new CountDownLatch(3);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			for (int i = 0; i < 2; i++) {
				batch.execute(() -> {
					batchStarted.countDown();
					awaitUninterruptibly(releaseBatch);
				});
			}
			assertTrue(batchStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.analytical(), OperationFamily.FULL_SCAN_AGGREGATE)
					.execute(() -> {
						analyticalStarted.countDown();
						awaitUninterruptibly(releaseAnalytical);
					});
			assertTrue(analyticalStarted.await(5, SECONDS));

			batch.execute(() -> record("borrowed-batch", order, new CountDownLatch(0)));
			scheduler.executor(RequestContext.latency(Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("latency", order, reservedDone));
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("ingest", order, reservedDone));
			scheduler.executor(WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> record("cdc", order, reservedDone));

			releaseAnalytical.countDown();
			assertTrue(reservedDone.await(5, SECONDS));
			List<String> firstThree;
			synchronized (order) {
				assertTrue(order.size() >= 3);
				firstThree = List.copyOf(order.subList(0, 3));
			}
			assertEquals(new HashSet<>(List.of("latency", "ingest", "cdc")),
					new HashSet<>(firstThree));
		} finally {
			releaseAnalytical.countDown();
			releaseBatch.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void idleReservationsAreBorrowedWithoutExceedingTheHardCapacity() throws Exception {
		var scheduler = scheduler(3, "borrowing-test");
		var started = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		try {
			var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
			for (int i = 0; i < 3; i++) {
				batch.execute(() -> {
					started.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(started.await(5, SECONDS));
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(3, snapshot.workerCount());
			assertEquals(3, snapshot.activeTasks());
			assertEquals(3, snapshot.activeByProfile().get(WorkloadProfile.BATCH));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void drrDeliversConfiguredSharesForUnitCostWork() throws Exception {
		var scheduler = scheduler(1, "drr-shares");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(33);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			enqueue(scheduler, WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, 12, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.CDC, OperationFamily.WAL_PAGE, 12, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.ANALYTICAL, OperationFamily.FULL_SCAN_AGGREGATE,
					6, 0L, order, completed);
			enqueue(scheduler, WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, 3, 0L, order, completed);
			release.countDown();
			assertTrue(completed.await(5, SECONDS));

			var firstRound = frequencies(order.subList(0, 11));
			assertEquals(4, firstRound.get(WorkloadProfile.INGEST));
			assertEquals(4, firstRound.get(WorkloadProfile.CDC));
			assertEquals(2, firstRound.get(WorkloadProfile.ANALYTICAL));
			assertEquals(1, firstRound.get(WorkloadProfile.BATCH));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void drrChargesDeclaredByteCost() throws Exception {
		var scheduler = scheduler(1, "drr-cost");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(8);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			enqueue(scheduler,
					WorkloadProfile.INGEST,
					OperationFamily.POINT_LOOKUP,
					4,
					8L * 1024L * 1024L,
					order,
					completed);
			enqueue(scheduler, WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, 4, 1L, order, completed);
			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH), order.subList(0, 4));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void taskCostUsesTwoMiBQuantaAndCapsAtSixteen() {
		long twoMiB = 2L * 1024L * 1024L;
		assertEquals(1, RWScheduler.taskCost(0L));
		assertEquals(1, RWScheduler.taskCost(1L));
		assertEquals(1, RWScheduler.taskCost(twoMiB));
		assertEquals(2, RWScheduler.taskCost(twoMiB + 1L));
		assertEquals(16, RWScheduler.taskCost(32L * 1024L * 1024L));
		assertEquals(16, RWScheduler.taskCost(Long.MAX_VALUE));
		assertThrows(IllegalArgumentException.class, () -> RWScheduler.taskCost(-1L));
	}

	@Test
	void storagePressureParksPhysicalAndGloballySpacesBatchAfterCompletion() throws Exception {
		var scheduler = scheduler(3, "pressure-test");
		var physical = new ObservableTask(() -> {});
		var firstStarted = new CountDownLatch(1);
		var releaseFirst = new CountDownLatch(1);
		var firstFinishedNanos = new AtomicLong();
		var secondStartedNanos = new AtomicLong();
		var secondStarted = new CountDownLatch(1);
		try {
			scheduler.setStoragePressure(true);
			scheduler.maintenanceExecutor().execute(physical);
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
				firstStarted.countDown();
				awaitUninterruptibly(releaseFirst);
				firstFinishedNanos.set(System.nanoTime());
			});
			assertTrue(firstStarted.await(5, SECONDS));
			scheduler.executor(RequestContext.batch(), OperationFamily.MUTATION).execute(() -> {
				secondStartedNanos.set(System.nanoTime());
				secondStarted.countDown();
			});
			assertEquals(1, scheduler.activeTasks(WorkloadProfile.BATCH));
			assertFalse(physical.ran());

			releaseFirst.countDown();
			assertTrue(secondStarted.await(3, SECONDS));
			assertTrue(secondStartedNanos.get() - firstFinishedNanos.get()
					>= TimeUnit.MILLISECONDS.toNanos(900),
					"pressured BATCH dispatches must be separated by one second after completion");
			assertFalse(physical.ran());

			scheduler.setStoragePressure(false);
			physical.get(5, SECONDS);
			assertTrue(physical.ran());
			assertFalse(scheduler.admissionSnapshot().storagePressure());
		} finally {
			releaseFirst.countDown();
			scheduler.setStoragePressure(false);
			scheduler.dispose();
		}
	}

	@Test
	void cancellationImmediatelyBeforeRunDoesNotConsumeAPressureInterval() throws Exception {
		var scheduler = scheduler(1, "pressure-cancel-test");
		var cancelled = new CancelAtDispatchTask();
		var nextStarted = new CountDownLatch(1);
		try {
			scheduler.setStoragePressure(true);
			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(cancelled);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION) == 1L);

			scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE)
					.execute(nextStarted::countDown);
			assertTrue(nextStarted.await(800L, TimeUnit.MILLISECONDS),
					"a BATCH task that never ran must not start the one-second pressure interval");
		} finally {
			scheduler.setStoragePressure(false);
			scheduler.dispose();
		}
	}

	@Test
	void queueOverloadAndCancellationHaveOneTerminalOutcomeAndMetric() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "terminal-test", registry, "terminal-db");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var queued = new ObservableTask(() -> {});
		var overloaded = new ObservableTask(() -> {});
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(queued);
			var failure = assertThrows(RocksDBException.class, () -> view.execute(overloaded));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, failure.getErrorUniqueId());
			assertTrue(overloaded.isCompletedExceptionally());
			assertEquals(1, overloaded.disposeCount());

			assertTrue(queued.cancel(false));
			assertTrue(scheduler.removeQueuedTask(view, queued));
			assertFalse(scheduler.removeQueuedTask(view, queued));
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(1.0,
					registry.get("rockserver.workload.cancellations")
							.tags("database", "terminal-db",
									"resource", "read",
									"profile", "batch",
									"operation", "range_page")
							.counter()
							.count());
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void taskFailureIsLoggedAndMetricizedOnce() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "failure-test", registry, "failure-db");
		try {
			scheduler.readExecutor().execute(() -> {
				throw new IllegalStateException("expected test failure");
			});
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).completedTasks() == 1L);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.failedTasks());
			assertEquals(1.0,
					registry.get("rockserver.workload.failures")
							.tags("database", "failure-db",
									"resource", "read",
									"profile", "batch",
									"operation", "range_page")
							.counter()
							.count());
		} finally {
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void metricFailureCannotStrandAdmissionOrKillTheWorker() throws Exception {
		var registry = new FailingCounterRegistry();
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "metric-failure-test", registry, "metric-db");
		var first = new CompletableFuture<Void>();
		var second = new CompletableFuture<Void>();
		try {
			registry.failCounters();
			scheduler.readExecutor().execute(() -> first.complete(null));
			first.get(5, SECONDS);
			scheduler.readExecutor().execute(() -> second.complete(null));
			second.get(5, SECONDS);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).completedTasks() == 2L);
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, snapshot.activeTasks());
			assertEquals(2L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertEquals(0L, snapshot.failedTasks());
		} finally {
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void queuedRemovalUsesIdentityEvenWhenTasksCompareEqual() throws Exception {
		var scheduler = scheduler(1, "identity-removal-test");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var first = new EqualTask();
		var second = new EqualTask();
		var view = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(first);
			view.execute(second);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(view, second));
			release.countDown();
			first.get(5, SECONDS);
			assertTrue(first.ran());
			assertFalse(second.ran());
			assertTrue(second.isCompletedExceptionally());
			assertEquals(1, second.disposeCount());
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void forcedShutdownCompletesQueuedWorkDisposesItAndInterruptsRunningWork() throws Exception {
		var scheduler = scheduler(1, "forced-shutdown");
		var started = new CountDownLatch(1);
		var interrupted = new AtomicBoolean();
		var queued = new LifecycleTask(true);
		var reactorRan = new AtomicBoolean();
		scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(() -> {
			started.countDown();
			try {
				new CountDownLatch(1).await();
			} catch (InterruptedException expected) {
				interrupted.set(true);
				Thread.currentThread().interrupt();
			}
		});
		assertTrue(started.await(5, SECONDS));
		scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE).execute(queued);
		var reactorTask = scheduler.scheduler(RequestContext.batch(), OperationFamily.RANGE_PAGE)
				.schedule(() -> reactorRan.set(true));
		assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

		scheduler.disposeNow();

		var completion = assertThrows(ExecutionException.class, () -> queued.get(5, SECONDS));
		assertInstanceOf(RejectedExecutionException.class, completion.getCause());
		assertFalse(queued.ran());
		assertEquals(1, queued.rejectionCount());
		assertEquals(1, queued.disposeCount());
		assertTrue(reactorTask.isDisposed());
		assertFalse(reactorRan.get());
		assertTrue(interrupted.get());
		var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
		assertEquals(2L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
		assertEquals(1L, snapshot.failedTasks());
		assertTrue(snapshot.terminated());
		assertThrows(RejectedExecutionException.class,
				() -> scheduler.readExecutor().execute(() -> {}));
	}

	@Test
	void reentrancyBelongsToTheOwningSchedulerOnly() throws Exception {
		var first = scheduler(1, "reentrant-first");
		var second = scheduler(1, "reentrant-second");
		var result = new CompletableFuture<List<Boolean>>();
		try {
			first.readExecutor().execute(() -> result.complete(List.of(
					first.isExecutingWorkloadTask(),
					second.isExecutingWorkloadTask())));
			assertEquals(List.of(true, false), result.get(5, SECONDS));
			assertFalse(first.isExecutingWorkloadTask());
			assertFalse(second.isExecutingWorkloadTask());
		} finally {
			first.dispose();
			second.dispose();
		}
	}

	@Test
	void oneThreadFactoryProducesUniqueNamesAndNoExtraCdcWorker() throws Exception {
		var scheduler = scheduler(3, "thread-name-test");
		var started = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		try {
			var cdc = scheduler.executor(WorkloadProfile.CDC,
					OperationFamily.WAL_PAGE,
					RequestContext.NO_DEADLINE);
			for (int i = 0; i < 3; i++) {
				cdc.execute(() -> {
					started.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(started.await(5, SECONDS));
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(3, snapshot.workerCount());
			assertEquals(3, snapshot.workerThreadNames().size());
			assertEquals(3, new HashSet<>(snapshot.workerThreadNames()).size());
			assertTrue(snapshot.workerThreadNames().stream()
					.allMatch(name -> name.startsWith("thread-name-test-read-")));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
	}

	@Test
	void controlIsIsolatedAndGracefulShutdownDrainsAcceptedControl() throws Exception {
		var scheduler = scheduler(3, "control-test");
		var dataStarted = new CountDownLatch(3);
		var release = new CountDownLatch(1);
		var controlDone = new CountDownLatch(1);
		try {
			for (int i = 0; i < 3; i++) {
				scheduler.readExecutor().execute(() -> {
					dataStarted.countDown();
					awaitUninterruptibly(release);
				});
			}
			assertTrue(dataStarted.await(5, SECONDS));
			scheduler.controlExecutor().execute(controlDone::countDown);
			assertTrue(controlDone.await(2, SECONDS));
		} finally {
			release.countDown();
			scheduler.dispose();
		}
		assertTrue(scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).terminated());
	}

	@Test
	void productionConstructorsRequireAllThreeReservationsAndExposeNoUnmanagedVariant() {
		assertThrows(IllegalArgumentException.class, () -> new RWScheduler(2, 3, "too-small-read"));
		assertThrows(IllegalArgumentException.class, () -> new RWScheduler(3, 2, "too-small-write"));
		assertTrue(java.util.Arrays.stream(RWScheduler.class.getConstructors())
				.noneMatch(constructor -> java.util.Arrays.stream(constructor.getParameterTypes())
						.anyMatch(type -> type.getName().equals("reactor.core.scheduler.Scheduler"))));
	}

	private static RWScheduler scheduler(int threads, String name) {
		return RWScheduler.forTesting(threads, threads, 1, 128, 128, name);
	}

	private static void enqueue(RWScheduler scheduler,
			WorkloadProfile profile,
			OperationFamily family,
			int count,
			long estimatedBytes,
			List<WorkloadProfile> order,
			CountDownLatch completed) {
		var executor = scheduler.executor(profile, family, RequestContext.NO_DEADLINE);
		for (int i = 0; i < count; i++) {
			executor.execute(() -> {
				order.add(profile);
				completed.countDown();
			}, estimatedBytes);
		}
	}

	private static Map<WorkloadProfile, Integer> frequencies(List<WorkloadProfile> profiles) {
		var result = new HashMap<WorkloadProfile, Integer>();
		for (var profile : profiles) {
			result.merge(profile, 1, Integer::sum);
		}
		return result;
	}

	private static void record(String value, List<String> order, CountDownLatch completed) {
		order.add(value);
		completed.countDown();
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		assertTrue(condition.getAsBoolean());
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

	private static final class ObservableTask extends CompletableFuture<Void> implements Runnable, Disposable {

		private final Runnable action;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger disposeCount = new AtomicInteger();

		private ObservableTask(Runnable action) {
			this.action = action;
		}

		@Override
		public void run() {
			ran.set(true);
			try {
				action.run();
				complete(null);
			} catch (Throwable failure) {
				completeExceptionally(failure);
			}
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		private boolean ran() {
			return ran.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class CancelAtDispatchTask implements Runnable, Disposable {

		private final AtomicInteger disposalChecks = new AtomicInteger();

		@Override
		public void run() {
			throw new AssertionError("cancelled task must not run");
		}

		@Override
		public void dispose() {
			disposalChecks.set(Integer.MAX_VALUE);
		}

		@Override
		public boolean isDisposed() {
			return disposalChecks.incrementAndGet() >= 2;
		}
	}

	private static final class EqualTask extends CompletableFuture<Void> implements Runnable, Disposable {

		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger disposeCount = new AtomicInteger();

		@Override
		public void run() {
			ran.set(true);
			complete(null);
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		@Override
		public boolean equals(Object ignored) {
			return ignored instanceof EqualTask;
		}

		@Override
		public int hashCode() {
			return 1;
		}

		private boolean ran() {
			return ran.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class LifecycleTask extends CompletableFuture<Void>
			implements Runnable, Disposable, RWScheduler.RejectionAwareTask {

		private final boolean failAfterReject;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger rejectionCount = new AtomicInteger();
		private final AtomicInteger disposeCount = new AtomicInteger();

		private LifecycleTask(boolean failAfterReject) {
			this.failAfterReject = failAfterReject;
		}

		@Override
		public void run() {
			ran.set(true);
			complete(null);
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
			completeExceptionally(failure);
			if (failAfterReject) {
				throw new IllegalStateException("expected rejection callback failure");
			}
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		private boolean ran() {
			return ran.get();
		}

		private int rejectionCount() {
			return rejectionCount.get();
		}

		private int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class FailingCounterRegistry extends SimpleMeterRegistry {

		private final AtomicBoolean failCounters = new AtomicBoolean();

		private void failCounters() {
			failCounters.set(true);
		}

		@Override
		protected Counter newCounter(Meter.Id id) {
			if (failCounters.get()) {
				throw new IllegalStateException("expected counter failure");
			}
			return super.newCounter(id);
		}
	}

	private static final class BlockingTimerRegistry extends SimpleMeterRegistry {

		private final AtomicBoolean blockTimers = new AtomicBoolean();
		private final CountDownLatch timerCreationStarted = new CountDownLatch(1);
		private final CountDownLatch releaseTimers = new CountDownLatch(1);

		private void blockTimers() {
			blockTimers.set(true);
		}

		private CountDownLatch timerCreationStarted() {
			return timerCreationStarted;
		}

		private void releaseTimers() {
			releaseTimers.countDown();
		}

		@Override
		protected Timer newTimer(Meter.Id id,
				DistributionStatisticConfig distributionStatisticConfig,
				PauseDetector pauseDetector) {
			if (blockTimers.compareAndSet(true, false)) {
				timerCreationStarted.countDown();
				awaitUninterruptibly(releaseTimers);
			}
			return super.newTimer(id, distributionStatisticConfig, pauseDetector);
		}
	}
}
