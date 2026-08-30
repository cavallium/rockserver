package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;

@Timeout(90)
class SchedulerStateModelPropertyTest {

	private static final long[] ORDER_SEEDS = {
			0L, 1L, 0x5EEDL, 0x5EED_C0FFEE_2026L, -0x6A09E667F3BCC909L,
			0x243F6A8885A308D3L, 0x13198A2E03707344L, Long.MIN_VALUE, Long.MAX_VALUE
	};
	private static final WorkloadProfile[] DATA_PROFILES = {
			WorkloadProfile.LATENCY,
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH
	};

	@Test
	void generatedQueuedMixMatchesIndependentEdfDrrReservationModel() throws Exception {
		for (long seed : ORDER_SEEDS) {
			try {
				assertGeneratedOrder(seed);
			} catch (Throwable failure) {
				throw new AssertionError("scheduler ordering trace failed; seed=" + seed, failure);
			}
		}
	}

	private static void assertGeneratedOrder(long seed) throws Exception {
		var scheduler = RWScheduler.forTesting(3, 3, 1, 128, 128, "state-model-order-" + seed);
		var settings = WorkloadSettings.testingDefaults(3, 3, 1, 128, 128);
		var model = new SchedulerReferenceModel(modelSettings(settings, true));
		var blockerPermits = new Semaphore(0);
		var blockersStarted = new CountDownLatch(3);
		var batchExecutor = scheduler.executor(
				WorkloadProfile.BATCH, OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			for (int index = 0; index < 3; index++) {
				long id = -1L - index;
				assertEquals(SchedulerReferenceModel.Admission.ACCEPTED,
						model.submit(new SchedulerReferenceModel.Spec(
								id, WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L));
				assertEquals(id, model.dispatch(0L, true).orElseThrow().id());
				batchExecutor.execute(() -> {
					blockersStarted.countDown();
					blockerPermits.acquireUninterruptibly();
				});
			}
			assertTrue(blockersStarted.await(5, SECONDS), "failed to occupy all read workers");

			var random = new SplittableRandom(seed);
			int taskCount = 24 + random.nextInt(25);
			long actualDeadlineBase = System.currentTimeMillis() + SECONDS.toMillis(30);
			var specs = new ArrayList<SchedulerReferenceModel.Spec>(taskCount);
			var cancel = new boolean[taskCount];
			for (int index = 0; index < taskCount; index++) {
				var profile = DATA_PROFILES[random.nextInt(DATA_PROFILES.length)];
				long logicalDeadline = profile == WorkloadProfile.LATENCY
						? 1L + random.nextLong(10_000L)
						: Long.MAX_VALUE;
				int cost = 1 + random.nextInt(WorkloadCost.MAX_UNITS);
				boolean failure = random.nextInt(7) == 0;
				cancel[index] = random.nextInt(8) == 0;
				specs.add(new SchedulerReferenceModel.Spec(index, profile, logicalDeadline, cost, failure));
			}

			var actualOrder = Collections.synchronizedList(new ArrayList<Long>());
			int expectedRuns = Math.toIntExact(specs.stream().filter(spec -> !cancel[(int) spec.id()]).count());
			var completed = new CountDownLatch(expectedRuns);
			var handles = new ArrayList<Disposable>(taskCount);
			for (var spec : specs) {
				assertEquals(SchedulerReferenceModel.Admission.ACCEPTED, model.submit(spec, 0L));
				long actualDeadline = spec.deadline() == Long.MAX_VALUE
						? RequestContext.NO_DEADLINE
						: actualDeadlineBase + spec.deadline();
				var task = new OrderedTask(spec, actualOrder, completed);
				handles.add(scheduler.scheduler(spec.profile(), family(spec.profile()), actualDeadline).schedule(task));
			}
			for (int index = 0; index < taskCount; index++) {
				if (cancel[index]) {
					assertTrue(model.cancel(index));
					handles.get(index).dispose();
				}
			}

			model.complete(-1L);
			List<Long> expectedOrder = model.drainOrder(0L, true);
			blockerPermits.release();
			assertTrue(completed.await(20, SECONDS),
					"generated tasks did not drain; seed=" + seed + ", remaining=" + completed.getCount());
			List<Long> observedOrder = List.copyOf(actualOrder);
			assertEquals(expectedOrder,
					observedOrder,
					"dispatch order mismatch; seed=" + seed + ", actionIndex="
							+ firstDifference(expectedOrder, observedOrder));

			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).terminalOutcomes()
					== model.outcomes());
			var actual = scheduler.poolSnapshot(RWScheduler.Pool.READ);
			assertEquals(model.outcomes(), actual.terminalOutcomes(), "mid-trace conservation mismatch; seed=" + seed);
			assertEquals(model.outcomeCounts().get(SchedulerReferenceModel.Outcome.FAILURE),
					actual.outcomes().get(RWScheduler.TerminalOutcome.FAILURE));
			assertEquals(model.outcomeCounts().get(SchedulerReferenceModel.Outcome.CANCELLATION),
					actual.outcomes().get(RWScheduler.TerminalOutcome.CANCELLATION));

			blockerPermits.release(2);
			model.complete(-2L);
			model.complete(-3L);
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertTrue(model.drainedAndConserved());
		} finally {
			blockerPermits.release(3);
			scheduler.disposeNow();
		}
	}

	@Test
	void boundedAdmissionAndImmediateDeadlineOutcomesMatchModel() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 2, 2, "state-model-admission");
		var settings = WorkloadSettings.testingDefaults(1, 1, 1, 2, 2);
		var model = new SchedulerReferenceModel(modelSettings(settings, true));
		var release = new CountDownLatch(1);
		var started = new CountDownLatch(1);
		try {
			model.submit(new SchedulerReferenceModel.Spec(-1L,
					WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L);
			model.dispatch(0L, true);
			scheduler.readExecutor().execute(() -> {
				started.countDown();
				await(release);
			});
			assertTrue(started.await(5, SECONDS));

			for (long id = 0; id < 2; id++) {
				assertEquals(SchedulerReferenceModel.Admission.ACCEPTED,
						model.submit(new SchedulerReferenceModel.Spec(
								id, WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L));
				scheduler.readExecutor().execute(() -> {});
			}
			assertEquals(SchedulerReferenceModel.Admission.OVERLOAD,
					model.submit(new SchedulerReferenceModel.Spec(
							2L, WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L));
			var overload = assertThrows(RocksDBException.class, () -> scheduler.readExecutor().execute(() -> {}));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, overload.getErrorUniqueId());

			long now = System.currentTimeMillis();
			assertEquals(SchedulerReferenceModel.Admission.DEADLINE,
					model.submit(new SchedulerReferenceModel.Spec(
							3L, WorkloadProfile.LATENCY, 5L, 1, false), 5L));
			assertThrows(RocksDBException.class, () -> scheduler.executor(
					WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, now - 1L).execute(() -> {}));
		} finally {
			release.countDown();
			scheduler.disposeNow();
		}
	}

	@Test
	void cancellationAfterDispatchLosesInBothSubjectAndModel() throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 8, 8, "state-model-active-cancel");
		var settings = WorkloadSettings.testingDefaults(1, 1, 1, 8, 8);
		var model = new SchedulerReferenceModel(modelSettings(settings, true));
		var started = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var ran = new CountDownLatch(1);
		var view = scheduler.scheduler(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE, RequestContext.NO_DEADLINE);
		try {
			model.submit(new SchedulerReferenceModel.Spec(1L,
					WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L);
			model.dispatch(0L, true);
			var handle = view.schedule(() -> {
				started.countDown();
				await(release);
				ran.countDown();
			});
			assertTrue(started.await(5, SECONDS));

			handle.dispose();
			assertFalse(model.cancelNonCooperative(1L));
			assertFalse(handle.isDisposed());
			release.countDown();
			assertTrue(ran.await(5, SECONDS));
			model.complete(1L);
			assertEventually(handle::isDisposed);
			assertEquals(1L, scheduler.poolSnapshot(RWScheduler.Pool.READ)
					.outcomes().get(RWScheduler.TerminalOutcome.RUN));
			assertEquals(1L, model.outcomeCounts().get(SchedulerReferenceModel.Outcome.RUN));
		} finally {
			release.countDown();
			view.dispose();
			scheduler.disposeNow();
		}
	}

	@Test
	void generatedCooperativeYieldParkResumeFailureAndCancellationMatchModel() throws Exception {
		for (long seed : new long[] {3L, 17L, 0xC001D00DL, -91L}) {
			assertCooperativeTrace(seed, false);
			assertCooperativeTrace(seed, true);
		}
	}

	private static void assertCooperativeTrace(long seed, boolean cancelAtFirstPark) throws Exception {
		var scheduler = RWScheduler.forTesting(1, 1, 1, 16, 16, "state-model-coop-" + seed);
		var settings = WorkloadSettings.testingDefaults(1, 1, 1, 16, 16);
		var model = new SchedulerReferenceModel(modelSettings(settings, true));
		var random = new SplittableRandom(seed);
		var actions = new ArrayList<RWScheduler.CooperativeResult>();
		for (int index = 0, count = 2 + random.nextInt(8); index < count; index++) {
			actions.add(random.nextBoolean()
					? RWScheduler.CooperativeResult.YIELD
					: RWScheduler.CooperativeResult.PARK);
		}
		boolean fail = !cancelAtFirstPark && random.nextInt(4) == 0;
		actions.add(RWScheduler.CooperativeResult.COMPLETE);
		var task = new ScriptedCooperativeTask(actions, fail);
		model.submit(new SchedulerReferenceModel.Spec(
				1L, WorkloadProfile.BATCH, Long.MAX_VALUE, 1, fail), 0L);
		var handle = scheduler.readExecutor().executeCooperatively(task, 1L);
		boolean cancelled = false;
		try {
			for (int actionIndex = 0; actionIndex < actions.size(); actionIndex++) {
				assertTrue(task.quantums.tryAcquire(10, SECONDS), trace(seed, actionIndex, "missing quantum"));
				var action = actions.get(actionIndex);
				var dispatched = model.dispatch(0L, true).orElseThrow();
				assertEquals(1L, dispatched.id());
				if (action == RWScheduler.CooperativeResult.YIELD) {
					model.yield(1L);
				} else if (action == RWScheduler.CooperativeResult.PARK) {
					model.park(1L);
					assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).parkedTasks() == 1);
					if (cancelAtFirstPark) {
						assertTrue(model.cancel(1L));
						assertTrue(handle.cancel());
						cancelled = true;
						break;
					}
					assertTrue(model.resume(1L));
					handle.resume();
				} else {
					model.complete(1L);
				}
			}
			assertTrue(task.terminal.await(10, SECONDS), trace(seed, actions.size(), "missing terminal"));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertTrue(model.drainedAndConserved());
			var expected = cancelled
					? SchedulerReferenceModel.Outcome.CANCELLATION
					: fail ? SchedulerReferenceModel.Outcome.FAILURE : SchedulerReferenceModel.Outcome.RUN;
			assertEquals(1L, model.outcomeCounts().get(expected));
			assertEquals(cancelled, task.failure.get() instanceof java.util.concurrent.CancellationException);
		} finally {
			handle.cancel();
			scheduler.disposeNow();
		}
	}

	@Test
	void generatedPressureCompetitionAndShutdownModelTracesConserveOwnership() {
		for (long seed : ORDER_SEEDS) {
			var random = new SplittableRandom(seed);
			var gate = new SchedulerReferenceModel.BatchGate(2, 3, 1, 3L, 2L, 4L);
			var subject = PressureSubject.create();
			var permits = new EnumMap<SchedulerReferenceModel.BatchGate.Pool,
					ArrayList<PermitPair>>(SchedulerReferenceModel.BatchGate.Pool.class);
			for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) permits.put(pool, new ArrayList<>());
			var competitors = new EnumMap<SchedulerReferenceModel.BatchGate.Pool, Boolean>(
					SchedulerReferenceModel.BatchGate.Pool.class);
			for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) competitors.put(pool, false);
			long now = Long.MAX_VALUE;
			for (int actionIndex = 0; actionIndex < 512; actionIndex++) {
				var pool = random.nextBoolean()
						? SchedulerReferenceModel.BatchGate.Pool.READ
						: SchedulerReferenceModel.BatchGate.Pool.WRITE;
				try {
					switch (random.nextInt(8)) {
						case 0 -> {
							boolean value = random.nextBoolean();
							gate.pressure(value);
							subject.pressure(value);
						}
						case 1 -> {
							boolean value = random.nextBoolean();
							gate.queued(pool, value);
							subject.queued(pool, value);
							if (!value) subject.dispatchable(pool, false);
						}
						case 2 -> {
							boolean value = random.nextBoolean();
							gate.dispatchable(pool, value);
							subject.dispatchable(pool, gate.isDispatchable(pool));
						}
						case 3 -> {
							if (gate.isDispatchable(pool)) {
								var expected = gate.start(pool, now);
								long actual = subject.start(pool, now);
								assertEquals(expected.isPresent(), actual != 0L,
										trace(seed, actionIndex, "pressure permit decision"));
								expected.ifPresent(permit -> permits.get(pool).add(new PermitPair(permit, actual)));
							}
						}
						case 4 -> {
							if (!permits.get(pool).isEmpty()) {
								var pair = permits.get(pool).removeFirst();
								gate.finish(pool, pair.model(), now);
								subject.finish(pool, pair.subject());
							}
						}
						case 5 -> {
							gate.competitor(pool, true, now);
							subject.competitor(pool, true);
							competitors.put(pool, true);
						}
						case 6 -> {
							if (competitors.get(pool)) {
								gate.competitor(pool, false, now);
								subject.competitor(pool, false);
								competitors.put(pool, false);
							}
						}
						case 7 -> {
							// Long.MAX_VALUE is the deterministic boundary after every real monotonic deadline.
						}
						default -> throw new AssertionError();
					}
					gate.validate();
					assertPressureEquivalent(gate, subject, now, seed, actionIndex);
				} catch (Throwable failure) {
					throw new AssertionError(trace(seed, actionIndex, "pressure trace"), failure);
				}
			}
			for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) {
				for (var pair : List.copyOf(permits.get(pool))) {
					gate.finish(pool, pair.model(), now);
					subject.finish(pool, pair.subject());
				}
				if (competitors.get(pool)) {
					gate.competitor(pool, false, now);
					subject.competitor(pool, false);
					competitors.put(pool, false);
				}
			}
			gate.validate();
			assertEquals(0, gate.activeTotal());
			assertPressureEquivalent(gate, subject, now, seed, 512);
		}
	}

	private static void assertPressureEquivalent(SchedulerReferenceModel.BatchGate model,
			PressureSubject subject,
			long now,
			long seed,
			int actionIndex) {
		assertEquals(model.activeTotal(), subject.active(), trace(seed, actionIndex, "active permits"));
		for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) {
			assertEquals(model.isDispatchable(pool), subject.isDispatchable(pool),
					trace(seed, actionIndex, "dispatchability " + pool));
			assertEquals(model.fairTurn(pool, now), subject.fairTurn(pool),
					trace(seed, actionIndex, "fair turn " + pool));
			assertEquals(model.canStart(pool, now), subject.allowance(pool, now) > 0,
					trace(seed, actionIndex, "eligibility " + pool) + "; model={" + model.describe()
							+ "}; subject={" + subject.describe() + "}");
		}
	}

	@Test
	void gracefulAndForcedShutdownReferenceTracesAreConservative() {
		var settings = WorkloadSettings.testingDefaults(1, 1, 1, 8, 8);
		var graceful = new SchedulerReferenceModel(modelSettings(settings, true));
		graceful.submit(new SchedulerReferenceModel.Spec(1L,
				WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L);
		graceful.gracefulShutdown();
		assertEquals(SchedulerReferenceModel.Admission.SHUTDOWN,
				graceful.submit(new SchedulerReferenceModel.Spec(2L,
						WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L));
		graceful.drainOrder(0L, true);
		assertTrue(graceful.drainedAndConserved());

		var forced = new SchedulerReferenceModel(modelSettings(settings, true));
		forced.submit(new SchedulerReferenceModel.Spec(1L,
				WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L);
		forced.dispatch(0L, true);
		forced.submit(new SchedulerReferenceModel.Spec(2L,
				WorkloadProfile.BATCH, Long.MAX_VALUE, 1, false), 0L);
		forced.forceShutdown();
		forced.complete(1L);
		assertTrue(forced.drainedAndConserved());
		assertEquals(2L, forced.outcomeCounts().get(SchedulerReferenceModel.Outcome.SHUTDOWN));
	}

	private static SchedulerReferenceModel.Settings modelSettings(WorkloadSettings settings, boolean read) {
		return new SchedulerReferenceModel.Settings(
				read ? settings.readParallelism() : settings.writeParallelism(),
				settings.analyticalActiveLimit(),
				settings.latencyBurst(),
				settings.queueCapacities(),
				read ? settings.readReservations() : settings.writeReservations(),
				settings.drrWeights());
	}

	private static OperationFamily family(WorkloadProfile profile) {
		return switch (profile) {
			case LATENCY -> OperationFamily.POINT_LOOKUP;
			case INGEST, BATCH -> OperationFamily.RANGE_PAGE;
			case CDC -> OperationFamily.WAL_PAGE;
			case ANALYTICAL -> OperationFamily.FULL_SCAN_AGGREGATE;
			case CONTROL, PHYSICAL_MAINTENANCE -> throw new IllegalArgumentException("not a data profile");
		};
	}

	private static long estimatedBytes(int cost) {
		return cost == 1 ? 1L : (cost - 1L) * WorkloadCost.QUANTUM_BYTES + 1L;
	}

	private static int firstDifference(List<?> expected, List<?> actual) {
		int shared = Math.min(expected.size(), actual.size());
		for (int index = 0; index < shared; index++) {
			if (!java.util.Objects.equals(expected.get(index), actual.get(index))) return index;
		}
		return shared;
	}

	private record OrderedTask(SchedulerReferenceModel.Spec spec,
	                           List<Long> order,
	                           CountDownLatch completed) implements Runnable, RWScheduler.EstimatedWork {

		@Override
		public long estimatedBytes() {
			return SchedulerStateModelPropertyTest.estimatedBytes(spec.cost());
		}

		@Override
		public void run() {
			try {
				order.add(spec.id());
				if (spec.failWhenRun()) throw new IllegalStateException("injected failure " + spec.id());
			} finally {
				completed.countDown();
			}
		}
	}

	private record PermitPair(SchedulerReferenceModel.BatchGate.Permit model, long subject) {
	}

	private static final class PressureSubject {

		private final Object controller;
		private final Class<?> type;
		private final AtomicBoolean readDispatchable = new AtomicBoolean();
		private final AtomicBoolean writeDispatchable = new AtomicBoolean();
		private final Method setPressured;
		private final Method setQueued;
		private final Method dispatchabilityLost;
		private final Method setCompetition;
		private final Method start;
		private final Method finish;
		private final Method dispatchable;
		private final Method fairTurn;
		private final Method allowance;

		private PressureSubject(Object controller, Class<?> type) throws ReflectiveOperationException {
			this.controller = controller;
			this.type = type;
			setPressured = accessible(type.getDeclaredMethod("setPressured", boolean.class));
			setQueued = accessible(type.getDeclaredMethod("setBatchQueued", RWScheduler.Pool.class, boolean.class));
			var setDispatchabilitySources = accessible(type.getDeclaredMethod(
					"setBatchDispatchabilitySources",
					java.util.function.BooleanSupplier.class,
					java.util.function.BooleanSupplier.class));
			setDispatchabilitySources.invoke(controller,
					(java.util.function.BooleanSupplier) readDispatchable::get,
					(java.util.function.BooleanSupplier) writeDispatchable::get);
			dispatchabilityLost = accessible(type.getDeclaredMethod(
					"batchDispatchabilityLost", RWScheduler.Pool.class));
			setCompetition = accessible(type.getDeclaredMethod(
					"setPoolCompetition", RWScheduler.Pool.class, boolean.class));
			start = accessible(type.getDeclaredMethod(
					"tryStartBatch", boolean.class, RWScheduler.Pool.class, long.class));
			finish = accessible(type.getDeclaredMethod("finishBatch", long.class, RWScheduler.Pool.class));
			dispatchable = accessible(type.getDeclaredMethod("isBatchDispatchable", RWScheduler.Pool.class));
			fairTurn = accessible(type.getDeclaredMethod("hasFairPressureTurn", RWScheduler.Pool.class));
			allowance = accessible(type.getDeclaredMethod(
					"batchStartAllowance", boolean.class, RWScheduler.Pool.class, long.class));
		}

		static PressureSubject create() {
			try {
				var type = Class.forName("it.cavallium.rockserver.core.impl.WorkloadPressureController");
				var constructor = type.getDeclaredConstructor(
						int.class,
						int.class,
						Duration.class,
						int.class,
						Duration.class,
						Duration.class);
				constructor.setAccessible(true);
				return new PressureSubject(constructor.newInstance(
						3, 1, Duration.ofNanos(2), 2, Duration.ofNanos(4), Duration.ofNanos(3)), type);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("Unable to construct pressure subject", failure);
			}
		}

		void pressure(boolean value) {
			invoke(setPressured, value);
		}

		void queued(SchedulerReferenceModel.BatchGate.Pool pool, boolean value) {
			invoke(setQueued, actual(pool), value);
		}

		void dispatchable(SchedulerReferenceModel.BatchGate.Pool pool, boolean value) {
			(pool == SchedulerReferenceModel.BatchGate.Pool.READ
					? readDispatchable : writeDispatchable).set(value);
			if (!value) invoke(dispatchabilityLost, actual(pool));
		}

		void competitor(SchedulerReferenceModel.BatchGate.Pool pool, boolean value) {
			invoke(setCompetition, actual(pool), value);
		}

		long start(SchedulerReferenceModel.BatchGate.Pool pool, long now) {
			return (long) invoke(start, false, actual(pool), now);
		}

		void finish(SchedulerReferenceModel.BatchGate.Pool pool, long permit) {
			invoke(finish, permit, actual(pool));
		}

		boolean isDispatchable(SchedulerReferenceModel.BatchGate.Pool pool) {
			return (boolean) invoke(dispatchable, actual(pool));
		}

		boolean fairTurn(SchedulerReferenceModel.BatchGate.Pool pool) {
			return (boolean) invoke(fairTurn, actual(pool));
		}

		int allowance(SchedulerReferenceModel.BatchGate.Pool pool, long now) {
			return (int) invoke(allowance, false, actual(pool), now);
		}

		int active() {
			try {
				Field field = type.getDeclaredField("activeBatches");
				field.setAccessible(true);
				return field.getInt(controller);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("Unable to read pressure permit count", failure);
			}
		}

		String describe() {
			return "pressured=" + field("pressured") + ", active=" + field("activeBatches")
					+ ", read=" + field("activeReadBatches") + ", write=" + field("activeWriteBatches")
					+ ", queuedMask=" + field("queuedBatchPoolMask")
					+ ", dispatchableRead=" + readDispatchable.get()
					+ ", dispatchableWrite=" + writeDispatchable.get()
					+ ", competitionMask=" + field("competitionPoolMask")
					+ ", last=" + field("lastCompletedPressuredBatchPoolBit")
					+ ", nextPressure=" + field("nextBatchNanos")
					+ ", nextWrite=" + field("nextCompetingWriteNanos")
					+ ", competitionUntil=" + field("competitionUntilNanos")
					+ ", pressureGeneration=" + field("pressureGeneration")
					+ ", competitionGeneration=" + field("competitionGeneration");
		}

		private Object field(String name) {
			try {
				Field field = type.getDeclaredField(name);
				field.setAccessible(true);
				return field.get(controller);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("Unable to read pressure field " + name, failure);
			}
		}

		private Object invoke(Method method, Object... arguments) {
			try {
				return method.invoke(controller, arguments);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("Pressure subject invocation failed: " + method.getName(), failure);
			}
		}

		private static Method accessible(Method method) {
			method.setAccessible(true);
			return method;
		}

		private static RWScheduler.Pool actual(SchedulerReferenceModel.BatchGate.Pool pool) {
			return pool == SchedulerReferenceModel.BatchGate.Pool.READ
					? RWScheduler.Pool.READ
					: RWScheduler.Pool.WRITE;
		}
	}

	private static final class ScriptedCooperativeTask implements RWScheduler.CooperativeCompletionTask {
		private final List<RWScheduler.CooperativeResult> actions;
		private final boolean fail;
		private final AtomicInteger cursor = new AtomicInteger();
		private final AtomicReference<Throwable> failure = new AtomicReference<>();
		private final CountDownLatch terminal = new CountDownLatch(1);
		private final Semaphore quantums = new Semaphore(0);

		private ScriptedCooperativeTask(List<RWScheduler.CooperativeResult> actions, boolean fail) {
			this.actions = List.copyOf(actions);
			this.fail = fail;
		}

		@Override
		public RWScheduler.CooperativeResult runCooperatively(RWScheduler.CooperativeContext context) {
			int index = cursor.getAndIncrement();
			var action = actions.get(index);
			if (action == RWScheduler.CooperativeResult.COMPLETE && fail) {
				context.fail(new IllegalStateException("injected cooperative failure"));
			}
			quantums.release();
			return action;
		}

		@Override
		public void completeCooperatively() {
			terminal.countDown();
		}

		@Override
		public void reject(RuntimeException failure) {
			this.failure.compareAndSet(null, failure);
			terminal.countDown();
		}
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(10);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(2L);
		assertTrue(condition.getAsBoolean());
	}

	private static void await(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}

	private static String trace(long seed, int actionIndex, String detail) {
		return detail + "; seed=" + seed + ", actionIndex=" + actionIndex;
	}
}
