package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.scheduler.Schedulers;

class RWSchedulerIndexedQueueTest {

	private static final int LARGE_QUEUE_SIZE = 4_096;

	@Test
	void plainIndexedTasksAvoidEstimatedMetadataWhileEstimatedTasksPreserveIt() {
		var scheduler = scheduler(1, 16, "indexed-metadata-specialization");
		var view = scheduler.scheduler(
				WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE);
		try {
			var plain = view.schedule(() -> {});
			assertFalse(plain instanceof RWScheduler.EstimatedWork,
					"plain scheduled work must retain the compact wrapper fast path");

			var estimated = view.schedule(new EstimatedOrderTask(17L,
					WorkloadProfile.INGEST,
					Collections.synchronizedList(new ArrayList<>()),
					new CountDownLatch(1)));
			assertInstanceOf(RWScheduler.EstimatedWork.class, estimated);
			assertEquals(17L, ((RWScheduler.EstimatedWork) estimated).estimatedBytes());
		} finally {
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void reactorHookCannotEraseEstimatedWorkFromDrrAdmission() throws Exception {
		String hook = "indexed-estimated-work-" + System.identityHashCode(this);
		Schedulers.onScheduleHook(hook, original -> original::run);
		var scheduler = scheduler(1, 16, "indexed-estimated-work");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(8);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			var ingest = scheduler.scheduler(
					WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE);
			var batch = scheduler.scheduler(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 4; i++) {
				ingest.schedule(new EstimatedOrderTask(8L * 1024L * 1024L,
						WorkloadProfile.INGEST, order, completed));
				batch.schedule(new EstimatedOrderTask(1L, WorkloadProfile.BATCH, order, completed));
			}

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of(
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH,
					WorkloadProfile.INGEST,
					WorkloadProfile.BATCH), order.subList(0, 4));
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
			Schedulers.resetOnScheduleHook(hook);
		}
	}

	@Test
	void immediateReactorOverloadRejectsOriginalTaskExactlyOnce() throws Exception {
		var scheduler = scheduler(1, 1, "indexed-reactor-overload");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var queued = new TerminalTask(() -> {});
		var overloaded = new TerminalTask(() -> {});
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.schedule(queued);

			assertThrows(RejectedExecutionException.class, () -> view.schedule(overloaded));
			var completion = assertThrows(ExecutionException.class, () -> overloaded.get(1, SECONDS));
			assertInstanceOf(RocksDBException.class, completion.getCause());
			assertEquals(1, overloaded.rejectionCount());
			assertEquals(0, overloaded.disposeCount(),
					"terminal delegation must mark the wrapper disposed before generic disposal fallback");
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.OVERLOAD));
		} finally {
			releaseBlocker.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void queuedReactorDeadlineRejectsOriginalTaskExactlyOnce() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-reactor-deadline");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		long deadline = System.currentTimeMillis() + 25L;
		var view = scheduler.scheduler(WorkloadProfile.ANALYTICAL,
				OperationFamily.FULL_SCAN_AGGREGATE, deadline);
		var expired = new TerminalTask(() -> {});
		try {
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.schedule(expired);
			while (System.currentTimeMillis() < deadline) Thread.onSpinWait();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {});

			assertDeadlineFailure(expired);
			assertEquals(1, expired.rejectionCount());
			assertEquals(0, expired.disposeCount());
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.DEADLINE));
		} finally {
			releaseBlocker.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void queuedReactorCancellationRejectsOriginalTaskExactlyOnce() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-reactor-cancellation-callback");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var cancelled = new TerminalTask(() -> {});
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			Disposable handle = view.schedule(cancelled);
			handle.dispose();

			assertThrows(CancellationException.class, () -> cancelled.get(1, SECONDS));
			assertEquals(1, cancelled.rejectionCount());
			assertEquals(0, cancelled.disposeCount());
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseBlocker.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void queuedReactorShutdownRejectsOriginalTaskExactlyOnce() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-reactor-shutdown-callback");
		var blockerStarted = new CountDownLatch(1);
		var interrupted = new CountDownLatch(1);
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var shutdown = new TerminalTask(() -> {});
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				try {
					new CountDownLatch(1).await();
				} catch (InterruptedException expected) {
					interrupted.countDown();
					Thread.currentThread().interrupt();
				}
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.schedule(shutdown);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

			scheduler.disposeNow();

			assertTrue(interrupted.await(5, SECONDS));
			var completion = assertThrows(ExecutionException.class, () -> shutdown.get(1, SECONDS));
			assertInstanceOf(RejectedExecutionException.class, completion.getCause());
			assertEquals(1, shutdown.rejectionCount());
			assertEquals(0, shutdown.disposeCount());
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
		} finally {
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void queuedReactorCancellationRacingShutdownPublishesOneOriginalFailure() throws Exception {
		for (int repetition = 0; repetition < 32; repetition++) {
			var scheduler = scheduler(1, 8, "indexed-reactor-terminal-race-" + repetition);
			var blockerStarted = new CountDownLatch(1);
			var releaseRace = new CountDownLatch(1);
			var view = scheduler.scheduler(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			var terminal = new TerminalTask(() -> {});
			try {
				view.schedule(() -> {
					blockerStarted.countDown();
					try {
						new CountDownLatch(1).await();
					} catch (InterruptedException expected) {
						Thread.currentThread().interrupt();
					}
				});
				assertTrue(blockerStarted.await(5, SECONDS));
				Disposable handle = view.schedule(terminal);
				assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

				var cancel = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(releaseRace);
					handle.dispose();
				});
				var shutdown = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(releaseRace);
					scheduler.disposeNow();
				});
				releaseRace.countDown();
				cancel.get(5, SECONDS);
				shutdown.get(5, SECONDS);

				assertEquals(1, terminal.rejectionCount());
				assertEquals(0, terminal.disposeCount());
				assertTrue(terminal.isCompletedExceptionally());
				var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				assertEquals(1L,
						snapshot.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION)
								+ snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
				assertTrue(snapshot.drainedAndConserved());
			} finally {
				releaseRace.countDown();
				view.dispose();
				scheduler.disposeNow();
			}
		}
	}

	@Test
	void queuedReactorTerminalRaceCrossProductSelectsOneCauseOutsideThePoolLock() throws Exception {
		var pairs = List.of(
				new TerminalRacePair(TerminalTrigger.CANCELLATION, TerminalTrigger.DEADLINE),
				new TerminalRacePair(TerminalTrigger.CANCELLATION, TerminalTrigger.SHUTDOWN),
				new TerminalRacePair(TerminalTrigger.DEADLINE, TerminalTrigger.SHUTDOWN));
		for (var pair : pairs) {
			for (int repetition = 0; repetition < 8; repetition++) {
				assertQueuedReactorTerminalRace(pair, repetition);
			}
		}
	}

	private static void assertQueuedReactorTerminalRace(TerminalRacePair pair,
			int repetition) throws Exception {
		String suffix = pair.first() + "-" + pair.second() + "-" + repetition;
		var scheduler = scheduler(1, 8, "indexed-reactor-terminal-matrix-" + suffix);
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var blockerReturned = new CountDownLatch(1);
		long raceMillis = System.currentTimeMillis() + 100L;
		long deadline = pair.has(TerminalTrigger.DEADLINE)
				? raceMillis
				: RequestContext.NO_DEADLINE;
		var view = scheduler.scheduler(WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, deadline);
		var terminal = new LockCheckingTask(scheduler);
		try {
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE).execute(() -> {
				blockerStarted.countDown();
				try {
					releaseBlocker.await();
				} catch (InterruptedException expectedDuringForcedShutdown) {
					Thread.currentThread().interrupt();
				} finally {
					blockerReturned.countDown();
				}
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			Disposable handle = view.schedule(terminal);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

			var raceStart = new CountDownLatch(1);
			var first = CompletableFuture.runAsync(() -> runTerminalTrigger(
					pair.first(), raceStart, raceMillis, handle, scheduler, releaseBlocker));
			var second = CompletableFuture.runAsync(() -> runTerminalTrigger(
					pair.second(), raceStart, raceMillis, handle, scheduler, releaseBlocker));
			raceStart.countDown();
			first.get(15, SECONDS);
			second.get(15, SECONDS);
			releaseBlocker.countDown();
			assertTrue(blockerReturned.await(5, SECONDS), "blocker did not return for " + suffix);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());

			assertEquals(1, terminal.rejectionCount(), "duplicate terminal callback for " + suffix);
			assertEquals(0, terminal.disposeCount(),
					"the indexed wrapper, not the original command, owns disposal for " + suffix);
			assertTrue(terminal.callbackOutsideLock(), "terminal callback held the pool lock for " + suffix);
			assertNull(terminal.callbackFailure(), "terminal callback could not re-enter telemetry for " + suffix);
			var outcomes = scheduler.poolSnapshot(RWScheduler.Pool.READ).outcomes();
			long selected = pair.allowedOutcomes().stream().mapToLong(outcomes::get).sum();
			assertEquals(1L, selected, "terminal cause was not selected exactly once for " + suffix);
		} finally {
			releaseBlocker.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	private static void runTerminalTrigger(TerminalTrigger trigger,
			CountDownLatch raceStart,
			long raceMillis,
			Disposable handle,
			RWScheduler scheduler,
			CountDownLatch releaseBlocker) {
		awaitUninterruptibly(raceStart);
		while (System.currentTimeMillis() < raceMillis) {
			Thread.onSpinWait();
		}
		switch (trigger) {
			case CANCELLATION -> handle.dispose();
			case DEADLINE -> releaseBlocker.countDown();
			case SHUTDOWN -> scheduler.disposeNow();
		}
	}

	@Test
	void largeQueueAdmissionSnapshotsAndGaugeSamplingNeverInspectCommands() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = RWScheduler.forTesting(
				1, 1, 1, 8, LARGE_QUEUE_SIZE, "indexed-large-queue", registry, "indexed-large-db");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var cancellationChecks = new AtomicInteger();
		var view = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			for (int i = 0; i < LARGE_QUEUE_SIZE; i++) {
				view.execute(new InspectionProbeTask(cancellationChecks));
			}

			assertEquals(LARGE_QUEUE_SIZE, scheduler.queuedTasks(WorkloadProfile.BATCH));
			assertEquals(LARGE_QUEUE_SIZE,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(LARGE_QUEUE_SIZE,
					scheduler.admissionSnapshot().queued().get(WorkloadProfile.BATCH));
			assertEquals(LARGE_QUEUE_SIZE,
					scheduler.instrumentationSnapshot()
							.pools()
							.get(RWScheduler.Pool.READ)
							.queuedTasks());

			var sampledGauges = new AtomicInteger();
			registry.getMeters().stream()
					.filter(meter -> meter.getId().getType() == Meter.Type.GAUGE)
					.forEach(meter -> {
						sampledGauges.incrementAndGet();
						meter.measure().forEach(ignored -> {});
					});
			assertTrue(sampledGauges.get() > 0);
			assertEquals(0, cancellationChecks.get(),
					"admission and observability must use indexes and incremental counts, not inspect queued commands");
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
			registry.close();
		}
	}

	@Test
	void latencyUsesDeadlineThenEnqueueSequence() throws Exception {
		var scheduler = scheduler(1, 16, "indexed-edf");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(3);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			long earlierDeadline = System.currentTimeMillis() + SECONDS.toMillis(20);
			long laterDeadline = earlierDeadline + SECONDS.toMillis(10);
			scheduler.executor(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, laterDeadline)
					.execute(() -> record("later", order, completed));
			var earlier = scheduler.executor(
					WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, earlierDeadline);
			earlier.execute(() -> record("earlier-1", order, completed));
			earlier.execute(() -> record("earlier-2", order, completed));

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(List.of("earlier-1", "earlier-2", "later"), order);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void latencyBurstAtIntegerMaximumCannotWrapAndStarveNewGuaranteedWork() throws Exception {
		var scheduler = scheduler(1, 16, "indexed-latency-burst-overflow");
		setReadPoolLatencyBurst(scheduler, Integer.MAX_VALUE);
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			scheduler.executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				order.add(WorkloadProfile.LATENCY);
				completed.countDown();
			});
			scheduler.executor(WorkloadProfile.INGEST,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				order.add(WorkloadProfile.INGEST);
				completed.countDown();
			});

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(WorkloadProfile.INGEST, order.getFirst(),
					"a saturated LATENCY burst must immediately yield to guaranteed work");
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	private static void setReadPoolLatencyBurst(RWScheduler scheduler, int value) throws Exception {
		var poolField = RWScheduler.class.getDeclaredField("readPool");
		poolField.setAccessible(true);
		Object pool = poolField.get(scheduler);
		var burstField = pool.getClass().getDeclaredField("latencyBurst");
		burstField.setAccessible(true);
		burstField.setInt(pool, value);
	}

	@Test
	void latencyDeadlineIndexesResizeAndSupportArbitraryRemoval() throws Exception {
		int taskCount = 64;
		var scheduler = scheduler(1, 128, "indexed-edf-resize-remove");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(taskCount - taskCount / 4);
		var order = Collections.synchronizedList(new ArrayList<Integer>());
		var tasks = new ArrayList<Runnable>(taskCount);
		var expected = new ArrayList<Integer>(taskCount);
		long deadlineBase = System.currentTimeMillis() + SECONDS.toMillis(30);
		var removalView = scheduler.executor(
				WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, deadlineBase);
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			for (int value = 0; value < taskCount; value++) {
				int captured = value;
				var task = (Runnable) () -> {
					order.add(captured);
					completed.countDown();
				};
				tasks.add(task);
				long deadline = deadlineBase + ((value * 37) & (taskCount - 1));
				scheduler.executor(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, deadline)
						.execute(task);
				if ((value & 3) != 0) {
					expected.add(value);
				}
			}

			for (int value = 0; value < taskCount; value += 4) {
				assertTrue(scheduler.removeQueuedTask(removalView, tasks.get(value)));
			}
			expected.sort(Comparator.comparingInt(value -> (value * 37) & (taskCount - 1)));

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(expected, order);
			assertReadPoolDrained(scheduler);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void fifoProfilePreservesInsertionOrderAndDrains() throws Exception {
		var scheduler = scheduler(1, 32, "indexed-fifo");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(16);
		var order = Collections.synchronizedList(new ArrayList<Integer>());
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			var ingest = scheduler.executor(
					WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 16; i++) {
				int value = i;
				ingest.execute(() -> {
					order.add(value);
					completed.countDown();
				});
			}

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEquals(java.util.stream.IntStream.range(0, 16).boxed().toList(), order);
			assertReadPoolDrained(scheduler);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void finiteNonLatencyDeadlineExpiresWhileItsProfileIsIneligible() throws Exception {
		var scheduler = RWScheduler.forTesting(2, 1, 1, 16, 16, "indexed-finite-deadline");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var expired = new TerminalTask(() -> {});
		try {
			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			long deadline = System.currentTimeMillis() + 300L;
			scheduler.executor(
						WorkloadProfile.ANALYTICAL, OperationFamily.FULL_SCAN_AGGREGATE, deadline)
					.execute(expired);

			assertDeadlineFailure(expired);
			assertFalse(expired.ran());
			assertEquals(1, expired.rejectionCount());
			assertEquals(1, expired.disposeCount());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.DEADLINE));
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void latencyAndNonLatencyIndexesShareEarliestDeadlineExpiry() throws Exception {
		var scheduler = scheduler(1, 16, "indexed-mixed-deadline-expiry");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var later = new TerminalTask(() -> {});
		var earlier = new TerminalTask(() -> {});
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			long nowMillis = System.currentTimeMillis();
			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						nowMillis + SECONDS.toMillis(4))
					.execute(later);
			scheduler.executor(
						WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						nowMillis + 250L)
					.execute(earlier);

			Thread.sleep(350L);
			scheduler.executor(
						WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE)
					.execute(() -> {});

			assertDeadlineFailure(earlier);
			assertFalse(earlier.ran());
			assertFalse(later.isDone(), "the later non-latency deadline must remain indexed");
			assertEquals(1, earlier.rejectionCount());
			assertEquals(1, earlier.disposeCount());
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void earlierDeadlineReplacesTheCurrentTimedWaitLeader() throws Exception {
		var scheduler = RWScheduler.forTesting(2, 1, 1, 16, 16, "indexed-deadline-leader");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var late = new TerminalTask(() -> {});
		var early = new TerminalTask(() -> {});
		try {
			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						System.currentTimeMillis() + SECONDS.toMillis(4))
					.execute(late);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			Thread.sleep(50L);

			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						System.currentTimeMillis() + 500L)
					.execute(early);

			var completion = assertThrows(ExecutionException.class, () -> early.get(2, SECONDS));
			assertDeadlineFailure(completion);
			assertFalse(late.isDone(), "the original later deadline must still be pending");
			assertEquals(1, early.rejectionCount());
			assertEquals(1, early.disposeCount());
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void drrRetainsConfiguredShares() throws Exception {
		var scheduler = scheduler(1, 64, "indexed-drr");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var completed = new CountDownLatch(33);
		var order = Collections.synchronizedList(new ArrayList<WorkloadProfile>());
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));

			enqueue(scheduler, WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, 12, order, completed);
			enqueue(scheduler, WorkloadProfile.CDC, OperationFamily.WAL_PAGE, 12, order, completed);
			enqueue(scheduler,
					WorkloadProfile.ANALYTICAL,
					OperationFamily.FULL_SCAN_AGGREGATE,
					6,
					order,
					completed);
			enqueue(scheduler, WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, 3, order, completed);

			releaseBlocker.countDown();
			assertTrue(completed.await(5, SECONDS));
			var firstRound = frequencies(order.subList(0, 11));
			assertEquals(4, firstRound.get(WorkloadProfile.INGEST));
			assertEquals(4, firstRound.get(WorkloadProfile.CDC));
			assertEquals(2, firstRound.get(WorkloadProfile.ANALYTICAL));
			assertEquals(1, firstRound.get(WorkloadProfile.BATCH));
			assertReadPoolDrained(scheduler);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void reservationsPrecedeQueuedBorrowers() throws Exception {
		var scheduler = scheduler(3, 32, "indexed-reservations");
		var batchStarted = new CountDownLatch(2);
		var releaseBatch = new CountDownLatch(1);
		var analyticalStarted = new CountDownLatch(1);
		var releaseAnalytical = new CountDownLatch(1);
		var reservedDone = new CountDownLatch(3);
		var order = Collections.synchronizedList(new ArrayList<String>());
		try {
			var batch = scheduler.executor(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			for (int i = 0; i < 2; i++) {
				batch.execute(() -> {
					batchStarted.countDown();
					awaitUninterruptibly(releaseBatch);
				});
			}
			assertTrue(batchStarted.await(5, SECONDS));
			scheduler.executor(
						WorkloadProfile.ANALYTICAL,
						OperationFamily.FULL_SCAN_AGGREGATE,
						RequestContext.NO_DEADLINE)
					.execute(() -> {
						analyticalStarted.countDown();
						awaitUninterruptibly(releaseAnalytical);
					});
			assertTrue(analyticalStarted.await(5, SECONDS));

			batch.execute(() -> order.add("borrowed-batch"));
			scheduler.executor(
						WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						System.currentTimeMillis() + SECONDS.toMillis(30))
					.execute(() -> record("latency", order, reservedDone));
			scheduler.executor(
						WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE)
					.execute(() -> record("ingest", order, reservedDone));
			scheduler.executor(
						WorkloadProfile.CDC, OperationFamily.WAL_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> record("cdc", order, reservedDone));

			releaseAnalytical.countDown();
			assertTrue(reservedDone.await(5, SECONDS));
			List<String> firstThree;
			synchronized (order) {
				firstThree = List.copyOf(order.subList(0, 3));
			}
			assertEquals(new HashSet<>(List.of("latency", "ingest", "cdc")),
					new HashSet<>(firstThree));
		} finally {
			releaseAnalytical.countDown();
			releaseBatch.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void pressurePacesBatchAndOverloadStillDrainsTheQueue() throws Exception {
		var pressured = scheduler(1, 8, "indexed-pressure");
		var firstStarted = new CountDownLatch(1);
		var releaseFirst = new CountDownLatch(1);
		var firstFinishedNanos = new AtomicLong();
		var secondStartedNanos = new AtomicLong();
		var secondStarted = new CountDownLatch(1);
		try {
			pressured.setStoragePressure(true);
			var batch = pressured.executor(
					WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
			batch.execute(() -> {
				firstStarted.countDown();
				awaitUninterruptibly(releaseFirst);
				firstFinishedNanos.set(System.nanoTime());
			});
			assertTrue(firstStarted.await(5, SECONDS));
			batch.execute(() -> {
				secondStartedNanos.set(System.nanoTime());
				secondStarted.countDown();
			});

			releaseFirst.countDown();
			assertFalse(secondStarted.await(500L, MILLISECONDS));
			assertTrue(secondStarted.await(3, SECONDS));
			assertTrue(secondStartedNanos.get() - firstFinishedNanos.get()
					>= MILLISECONDS.toNanos(900L));
			assertReadPoolDrained(pressured);
		} finally {
			releaseFirst.countDown();
			pressured.setStoragePressure(false);
			pressured.disposeNow();
		}

		var overloadedScheduler = RWScheduler.forTesting(1, 1, 1, 1, 1, "indexed-overload");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var queued = new TerminalTask(() -> {});
		var overloaded = new TerminalTask(() -> {});
		var view = overloadedScheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(queued);

			var failure = assertThrows(RocksDBException.class, () -> view.execute(overloaded));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
					failure.getErrorUniqueId());
			assertEquals(1, overloaded.rejectionCount());
			assertEquals(1, overloaded.disposeCount());
			assertFalse(overloaded.ran());

			releaseBlocker.countDown();
			queued.get(5, SECONDS);
			assertTrue(queued.ran());
			assertReadPoolDrained(overloadedScheduler);
			assertEquals(1L,
					overloadedScheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.OVERLOAD));
		} finally {
			releaseBlocker.countDown();
			overloadedScheduler.disposeNow();
		}
	}

	@Test
	void equalButDistinctCommandsAreRemovedByIdentity() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-equal-identity");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var first = new EqualTerminalTask();
		var second = new EqualTerminalTask();
		var view = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(first);
			view.execute(second);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(view, second));
			assertFalse(scheduler.removeQueuedTask(view, second));
			assertEquals(1, second.rejectionCount());
			assertEquals(1, second.disposeCount());
			assertFalse(first.isDone());

			releaseBlocker.countDown();
			first.get(5, SECONDS);
			assertTrue(first.ran());
			assertFalse(second.ran());
			assertTrue(second.isCompletedExceptionally());
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void repeatedIdenticalCommandUsesAnEnqueueOrderedKeyChain() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-repeated-identity");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var repeated = new RepeatedTask(2);
		var batchRange = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var batchPoint = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE);
		var ingestPoint = scheduler.executor(
				WorkloadProfile.INGEST, OperationFamily.POINT_LOOKUP, RequestContext.NO_DEADLINE);
		try {
			batchRange.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			batchRange.execute(repeated);
			batchRange.execute(repeated);
			batchPoint.execute(repeated);
			ingestPoint.execute(repeated);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 4);

			assertTrue(scheduler.removeQueuedTask(batchRange, repeated));
			assertTrue(scheduler.removeQueuedTask(batchRange, repeated));
			assertFalse(scheduler.removeQueuedTask(batchRange, repeated));
			assertEquals(2, repeated.rejectionCount());
			assertEquals(2, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());

			releaseBlocker.countDown();
			assertTrue(repeated.awaitRuns());
			assertEquals(2, repeated.runCount());
			assertEquals(2, repeated.rejectionCount());
			assertReadPoolDrained(scheduler);
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void repeatedIdenticalCommandRemovesTheOldestSubmission() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-repeated-oldest");
		var repeated = new RepeatedTask(0);
		try {
			scheduler.setStoragePressure(true);
			var late = scheduler.executor(
					WorkloadProfile.PHYSICAL_MAINTENANCE,
					OperationFamily.COMPACTION,
					System.currentTimeMillis() + SECONDS.toMillis(30));
			var early = scheduler.executor(
					WorkloadProfile.PHYSICAL_MAINTENANCE,
					OperationFamily.COMPACTION,
					System.currentTimeMillis() + 500L);
			late.execute(repeated);
			early.execute(repeated);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL).queuedTasks() == 2);

			assertTrue(scheduler.removeQueuedTask(early, repeated));
			assertEquals(1, repeated.rejectionCount());
			assertEquals(1, scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL).queuedTasks());

			assertEventually(() -> repeated.rejectionCount() == 2);
			assertEquals(0, repeated.runCount());
			var outcomes = scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL).outcomes();
			assertEquals(1L, outcomes.get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(1L, outcomes.get(RWScheduler.TerminalOutcome.DEADLINE));
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL).queuedTasks());
		} finally {
			scheduler.setStoragePressure(false);
			scheduler.disposeNow();
		}
	}

	@Test
	void indexedRemovalCompletesExactlyOnceAndRunsCallbacksOutsideTheLock() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-callback-lock");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var task = new LockCheckingTask(scheduler);
		var view = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(task);

			assertTrue(scheduler.removeQueuedTask(view, task));
			assertFalse(scheduler.removeQueuedTask(view, task));
			assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks());
			assertEquals(1, task.rejectionCount());
			assertEquals(1, task.disposeCount());
			assertTrue(task.callbackOutsideLock());
			assertNull(task.callbackFailure());
			assertTrue(task.isCompletedExceptionally());
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void reactorSchedulerDisposalImmediatelyUsesIndexedRemoval() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-reactor-disposal");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var ran = new AtomicBoolean();
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			var queued = view.schedule(() -> ran.set(true));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			queued.dispose();

			assertTrue(queued.isDisposed());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			assertFalse(ran.get());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseBlocker.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellationThatLosesDispatchLeavesRunningOwnershipUnchanged() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-running-cancellation");
		var started = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var completed = new CountDownLatch(1);
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			var running = view.schedule(() -> {
				started.countDown();
				awaitUninterruptibly(release);
				completed.countDown();
			});
			assertTrue(started.await(5, SECONDS));

			running.dispose();
			assertFalse(running.isDisposed(),
					"a cancellation that lost dispatch arbitration must not rewrite running ownership");
			var active = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1, active.activeTasks());
			assertEquals(0L, active.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertEquals(0L, active.outcomes().get(RWScheduler.TerminalOutcome.RUN));

			release.countDown();
			assertTrue(completed.await(5, SECONDS));
			assertEventually(running::isDisposed);
			var terminal = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, terminal.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertEquals(0L, terminal.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
			assertTrue(terminal.drainedAndConserved());
		} finally {
			release.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void reactorWorkerDisposalImmediatelyUnlinksEveryQueuedTask() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-reactor-worker-disposal");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var firstRan = new AtomicBoolean();
		var secondRan = new AtomicBoolean();
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var worker = view.createWorker();
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			var first = worker.schedule(() -> firstRan.set(true));
			worker.schedule(() -> secondRan.set(true));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 2);

			first.dispose();
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);
			worker.dispose();

			assertTrue(first.isDisposed());
			assertTrue(worker.isDisposed());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			assertFalse(firstRan.get());
			assertFalse(secondRan.get());
			assertEquals(2L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseBlocker.countDown();
			worker.dispose();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void parentSchedulerDisposalCancelsRegisteredWorkerTasksAndRejectsLaterScheduling() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-parent-worker-disposal");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var view = scheduler.scheduler(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		var worker = view.createWorker();
		var queued = new TerminalTask(() -> {});
		try {
			view.schedule(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			worker.schedule(queued);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

			view.dispose();

			assertTrue(worker.isDisposed());
			assertThrows(RejectedExecutionException.class, view::createWorker);
			assertThrows(RejectedExecutionException.class, () -> worker.schedule(() -> {}));
			assertThrows(CancellationException.class, () -> queued.get(1, SECONDS));
			assertFalse(queued.ran());
			assertEquals(1, queued.rejectionCount());
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 0);
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));
		} finally {
			releaseBlocker.countDown();
			worker.dispose();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void dispatchCancellationChecksNeverInvokeQueuedCommands() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-dispatch-inspection");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var ran = new CountDownLatch(1);
		var cancellationChecks = new AtomicInteger();
		var task = new InspectionProbeTask(cancellationChecks, ran);
		var view = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(releaseBlocker);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(task);

			releaseBlocker.countDown();
			assertTrue(ran.await(5, SECONDS));
			assertEventually(() -> {
				var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
				return snapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN) == 2L
						&& snapshot.drainedAndConserved();
			});
			assertEquals(0, cancellationChecks.get(),
					"dispatch must read scheduler-owned cancellation state, not invoke queued commands");
			assertEquals(2L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.outcomes()
							.get(RWScheduler.TerminalOutcome.RUN));
		} finally {
			releaseBlocker.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void gracefulShutdownDrainsAcceptedWork() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-graceful-shutdown");
		var blockerStarted = new CountDownLatch(1);
		var releaseBlocker = new CountDownLatch(1);
		var queued = new TerminalTask(() -> {});
		CompletableFuture<Void> shutdown = null;
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						blockerStarted.countDown();
						awaitUninterruptibly(releaseBlocker);
					});
			assertTrue(blockerStarted.await(5, SECONDS));
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(queued);

			shutdown = CompletableFuture.runAsync(scheduler::dispose);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).shutdown());
			assertFalse(shutdown.isDone());
			releaseBlocker.countDown();

			queued.get(5, SECONDS);
			shutdown.get(5, SECONDS);
			assertTrue(queued.ran());
			assertEquals(0, queued.rejectionCount());
			assertEquals(0, queued.disposeCount());
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(0, snapshot.queuedTasks());
			assertEquals(0, snapshot.activeTasks());
			assertTrue(snapshot.terminated());
		} finally {
			releaseBlocker.countDown();
			try {
				if (shutdown != null) {
					shutdown.get(15, SECONDS);
				}
			} finally {
				scheduler.disposeNow();
			}
		}
	}

	@Test
	void terminatedPoolAcceptsZeroAndHugeAwaitTimeoutsWithoutDeadlineOverflow() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-await-boundaries");
		var ran = new CountDownLatch(1);
		scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				RequestContext.NO_DEADLINE).execute(ran::countDown);
		assertTrue(ran.await(5, SECONDS));
		scheduler.disposeNow();

		var poolField = RWScheduler.class.getDeclaredField("readPool");
		poolField.setAccessible(true);
		var pool = (java.util.concurrent.ExecutorService) poolField.get(scheduler);
		assertTrue(pool.awaitTermination(0L, java.util.concurrent.TimeUnit.NANOSECONDS));
		assertTrue(pool.awaitTermination(Long.MAX_VALUE, java.util.concurrent.TimeUnit.DAYS));
	}

	@Test
	void forcedShutdownRejectsQueuedWorkOnceAndInterruptsRunningWork() throws Exception {
		var scheduler = scheduler(1, 8, "indexed-forced-shutdown");
		var started = new CountDownLatch(1);
		var interrupted = new CountDownLatch(1);
		var queued = new TerminalTask(() -> {});
		try {
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(() -> {
						started.countDown();
						try {
							new CountDownLatch(1).await();
						} catch (InterruptedException expected) {
							interrupted.countDown();
							Thread.currentThread().interrupt();
						}
					});
			assertTrue(started.await(5, SECONDS));
			scheduler.executor(
						WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE)
					.execute(queued);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).queuedTasks() == 1);

			scheduler.disposeNow();

			assertTrue(interrupted.await(5, SECONDS));
			assertFalse(queued.ran());
			assertEquals(1, queued.rejectionCount());
			assertEquals(1, queued.disposeCount());
			var completion = assertThrows(ExecutionException.class, () -> queued.get(5, SECONDS));
			assertInstanceOf(RejectedExecutionException.class, completion.getCause());
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(1L, snapshot.outcomes().get(RWScheduler.TerminalOutcome.SHUTDOWN));
			assertTrue(snapshot.terminated());
		} finally {
			scheduler.disposeNow();
		}
	}

	private static RWScheduler scheduler(int threads, int queueCapacity, String name) {
		return RWScheduler.forTesting(
				threads, threads, 1, queueCapacity, queueCapacity, name);
	}

	private static void enqueue(RWScheduler scheduler,
			WorkloadProfile profile,
			OperationFamily family,
			int count,
			List<WorkloadProfile> order,
			CountDownLatch completed) {
		var executor = scheduler.executor(profile, family, RequestContext.NO_DEADLINE);
		for (int i = 0; i < count; i++) {
			executor.execute(() -> {
				order.add(profile);
				completed.countDown();
			});
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

	private static void assertDeadlineFailure(TerminalTask task) {
		var completion = assertThrows(ExecutionException.class, () -> task.get(5, SECONDS));
		assertDeadlineFailure(completion);
	}

	private static void assertDeadlineFailure(ExecutionException completion) {
		var failure = assertInstanceOf(RocksDBException.class, completion.getCause());
		assertEquals(RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				failure.getErrorUniqueId());
	}

	private static void assertReadPoolDrained(RWScheduler scheduler) throws InterruptedException {
		assertEventually(() -> {
			var snapshot = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			return snapshot.queuedTasks() == 0 && snapshot.activeTasks() == 0;
		});
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

	private static final class InspectionProbeTask implements Runnable, Disposable {

		private final AtomicInteger cancellationChecks;
		private final CountDownLatch ran;

		private InspectionProbeTask(AtomicInteger cancellationChecks) {
			this(cancellationChecks, null);
		}

		private InspectionProbeTask(AtomicInteger cancellationChecks, CountDownLatch ran) {
			this.cancellationChecks = cancellationChecks;
			this.ran = ran;
		}

		@Override
		public void run() {
			if (ran != null) {
				ran.countDown();
			}
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			cancellationChecks.incrementAndGet();
			return false;
		}
	}

	private record EstimatedOrderTask(long estimatedBytes,
			WorkloadProfile profile,
			List<WorkloadProfile> order,
			CountDownLatch completed) implements Runnable, RWScheduler.EstimatedWork {

		@Override
		public void run() {
			order.add(profile);
			completed.countDown();
		}
	}

	private static class TerminalTask extends CompletableFuture<Void>
			implements Runnable, Disposable, RWScheduler.RejectionAwareTask {

		private final Runnable action;
		private final AtomicBoolean ran = new AtomicBoolean();
		private final AtomicInteger rejectionCount = new AtomicInteger();
		private final AtomicInteger disposeCount = new AtomicInteger();

		private TerminalTask(Runnable action) {
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
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
			completeExceptionally(failure);
		}

		@Override
		public void dispose() {
			disposeCount.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposeCount.get() > 0;
		}

		final boolean ran() {
			return ran.get();
		}

		final int rejectionCount() {
			return rejectionCount.get();
		}

		final int disposeCount() {
			return disposeCount.get();
		}
	}

	private static final class EqualTerminalTask extends TerminalTask {

		private EqualTerminalTask() {
			super(() -> {});
		}

		@Override
		public boolean equals(Object ignored) {
			return ignored instanceof EqualTerminalTask;
		}

		@Override
		public int hashCode() {
			return 1;
		}
	}

	private static final class RepeatedTask implements Runnable, RWScheduler.RejectionAwareTask {

		private final CountDownLatch runs;
		private final AtomicInteger runCount = new AtomicInteger();
		private final AtomicInteger rejectionCount = new AtomicInteger();

		private RepeatedTask(int expectedRuns) {
			this.runs = new CountDownLatch(expectedRuns);
		}

		@Override
		public void run() {
			runCount.incrementAndGet();
			runs.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			rejectionCount.incrementAndGet();
		}

		private boolean awaitRuns() throws InterruptedException {
			return runs.await(5, SECONDS);
		}

		private int runCount() {
			return runCount.get();
		}

		private int rejectionCount() {
			return rejectionCount.get();
		}
	}

	private static final class LockCheckingTask extends TerminalTask {

		private final RWScheduler scheduler;
		private final AtomicBoolean callbackOutsideLock = new AtomicBoolean();
		private final AtomicReference<Throwable> callbackFailure = new AtomicReference<>();

		private LockCheckingTask(RWScheduler scheduler) {
			super(() -> {});
			this.scheduler = scheduler;
		}

		@Override
		public void reject(RuntimeException terminalFailure) {
			try {
				CompletableFuture.runAsync(
						() -> scheduler.poolSnapshot(RWScheduler.Pool.READ)).get(2, SECONDS);
				callbackOutsideLock.set(true);
			} catch (Throwable failure) {
				callbackFailure.set(failure);
			}
			super.reject(terminalFailure);
		}

		private boolean callbackOutsideLock() {
			return callbackOutsideLock.get();
		}

		private Throwable callbackFailure() {
			return callbackFailure.get();
		}
	}

	private enum TerminalTrigger {
		CANCELLATION,
		DEADLINE,
		SHUTDOWN
	}

	private record TerminalRacePair(TerminalTrigger first, TerminalTrigger second) {

		private boolean has(TerminalTrigger trigger) {
			return first == trigger || second == trigger;
		}

		private Set<RWScheduler.TerminalOutcome> allowedOutcomes() {
			var outcomes = java.util.EnumSet.noneOf(RWScheduler.TerminalOutcome.class);
			for (var trigger : List.of(first, second)) {
				outcomes.add(switch (trigger) {
					case CANCELLATION -> RWScheduler.TerminalOutcome.CANCELLATION;
					case DEADLINE -> RWScheduler.TerminalOutcome.DEADLINE;
					case SHUTDOWN -> RWScheduler.TerminalOutcome.SHUTDOWN;
				});
			}
			return outcomes;
		}
	}

}
