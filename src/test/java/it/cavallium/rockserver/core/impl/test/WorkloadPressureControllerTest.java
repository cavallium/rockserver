package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;

/**
 * Deterministic state-machine coverage for the shared read/write BATCH controller.
 *
 * <p>Every timestamp-sensitive assertion uses an explicit logical timestamp or a deadline
 * captured from the controller. Wall-clock sleeps are deliberately absent: these tests prove
 * permit conservation and boundary behavior rather than relying on thread scheduling.</p>
 */
class WorkloadPressureControllerTest {

	private static final long ELIGIBLE_TIME = Long.MAX_VALUE;

	@Test
	void dispatchabilitySourcesRemainIndependentAcrossAdversarialTransitions()
			throws Exception {
		var controller = Controller.create(1, Duration.ofNanos(1));

		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, false);
		assertFalse(controller.isDispatchable(RWScheduler.Pool.READ));
		assertTrue(controller.isDispatchable(RWScheduler.Pool.WRITE),
				"publishing READ must not overwrite WRITE's independently owned state");

		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, false);
		assertTrue(controller.isDispatchable(RWScheduler.Pool.READ),
				"publishing WRITE must not overwrite READ's independently owned state");
		assertFalse(controller.isDispatchable(RWScheduler.Pool.WRITE));
	}

	@Test
	void pressureHandsEveryReleasedPermitToTheOtherContinuouslyQueuedPool() throws Exception {
		var controller = Controller.create(1, Duration.ofNanos(1));
		controller.setPressured(true);
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);

		for (int round = 0; round < 32; round++) {
			var read = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
			controller.finish(read, RWScheduler.Pool.READ);
			assertEquals(0L, controller.tryStart(RWScheduler.Pool.READ, ELIGIBLE_TIME),
					"a continuously queued pool must not reacquire the only permit before its peer");

			var write = controller.requireStart(RWScheduler.Pool.WRITE, ELIGIBLE_TIME);
			controller.finish(write, RWScheduler.Pool.WRITE);
			assertEquals(0L, controller.tryStart(RWScheduler.Pool.WRITE, ELIGIBLE_TIME),
					"cross-pool fairness must be symmetric");
		}

		var finalRead = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
		controller.finish(finalRead, RWScheduler.Pool.READ);
	}

	@Test
	void abandonedFairHandoffReopensCapacityAndWakesTheRemainingPoolOutsideTheCallerLock()
			throws Exception {
		var controller = Controller.create(1, Duration.ofNanos(1));
		var deferredWakeups = new AtomicInteger();
		var directBatchWakeups = new AtomicInteger();
		controller.setPressured(true);
		controller.setNotifier(deferredWakeups::incrementAndGet);
		controller.setBatchNotifier(_ -> directBatchWakeups.incrementAndGet());
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);

		var read = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
		controller.finish(read, RWScheduler.Pool.READ);
		assertEquals(1, directBatchWakeups.get(), "completion must directly wake the peer pool");
		assertEquals(0L, controller.tryStart(RWScheduler.Pool.READ, ELIGIBLE_TIME));
		assertEquals(Long.MAX_VALUE, controller.waitNanos(RWScheduler.Pool.READ, ELIGIBLE_TIME),
				"a fair handoff has no timer deadline and must park instead of spin");

		assertTrue(controller.isQueued(RWScheduler.Pool.WRITE));
		assertTrue(controller.isDispatchable(RWScheduler.Pool.WRITE));
		controller.setDispatchable(RWScheduler.Pool.WRITE, false);
		assertTrue(controller.isQueued(RWScheduler.Pool.WRITE),
				"a queued peer can remain backlogged after losing all dispatch capacity");
		assertFalse(controller.isDispatchable(RWScheduler.Pool.WRITE));
		assertEquals(0, deferredWakeups.get(),
				"dispatchability transitions run under an executor lock and must not invoke another pool");
		controller.signalPendingAvailability();
		assertEquals(1, deferredWakeups.get(),
				"an undispatchable handed-off peer must wake the remaining eligible pool after unlock");
		assertEquals(0L, controller.waitNanos(RWScheduler.Pool.READ, ELIGIBLE_TIME));
		controller.setQueued(RWScheduler.Pool.WRITE, false);

		var replacement = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
		controller.finish(replacement, RWScheduler.Pool.READ);
	}

	@Test
	void pressuredIntervalIsClosedBeforeAndOpenExactlyAtItsDeadline() throws Exception {
		var controller = Controller.create(1, Duration.ofSeconds(30));
		controller.setPressured(true);
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);

		var permit = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
		controller.finish(permit, RWScheduler.Pool.READ);
		long deadline = controller.longField("nextBatchNanos");

		assertEquals(0, controller.allowance(RWScheduler.Pool.READ, deadline - 1L));
		assertEquals(0L, controller.tryStart(RWScheduler.Pool.READ, deadline - 1L));
		assertEquals(1, controller.allowance(RWScheduler.Pool.READ, deadline));
		var boundaryPermit = controller.requireStart(RWScheduler.Pool.READ, deadline);
		controller.finish(boundaryPermit, RWScheduler.Pool.READ);
	}

	@Test
	void repeatedEquivalentPressureSignalKeepsTheCurrentEpisodeAndItsPacing() throws Exception {
		var controller = Controller.create(1, Duration.ofSeconds(30));
		controller.setPressured(true);
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		var permit = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);

		controller.setPressured(true);
		controller.finish(permit, RWScheduler.Pool.READ);
		long deadline = controller.longField("nextBatchNanos");
		assertEquals(0, controller.allowance(RWScheduler.Pool.READ, deadline - 1L),
				"polling the same pressure state must not invalidate the active episode");
		assertEquals(1, controller.allowance(RWScheduler.Pool.READ, deadline));
	}

	@Test
	void competingWriteIntervalIsClosedBeforeAndOpenExactlyAtItsDeadline() throws Exception {
		var controller = Controller.create(
				4,
				Duration.ofSeconds(30),
				Duration.ofSeconds(30),
				Duration.ofSeconds(30));
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);
		controller.setCompeting(RWScheduler.Pool.READ, true);
		var permit = controller.requireStart(RWScheduler.Pool.WRITE, ELIGIBLE_TIME);
		controller.finish(permit, RWScheduler.Pool.WRITE);
		long deadline = controller.longField("nextCompetingWriteNanos");

		assertEquals(0, controller.allowance(RWScheduler.Pool.WRITE, deadline - 1L));
		assertEquals(4, controller.allowance(RWScheduler.Pool.WRITE, deadline));
	}

	@Test
	void pressureTransitionsNeitherRetroactivelyPaceOldWorkNorRetainAStaleClock() throws Exception {
		var controller = Controller.create(1, Duration.ofSeconds(30));
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);

		var unpressured = controller.requireStart(RWScheduler.Pool.READ, 1L);
		controller.setPressured(true);
		assertEquals(0L, controller.tryStart(RWScheduler.Pool.WRITE, 1L),
				"pressure onset must account for an already active unpressured permit");
		controller.finish(unpressured, RWScheduler.Pool.READ);
		var firstPressured = controller.requireStart(RWScheduler.Pool.WRITE, 1L,
				"finishing work admitted before pressure must not create a synthetic quiet interval");

		controller.setPressured(false);
		controller.finish(firstPressured, RWScheduler.Pool.WRITE);
		controller.setPressured(true);
		var afterToggle = controller.requireStart(RWScheduler.Pool.READ, 1L,
				"clearing pressure must discard the old pressure episode's pacing deadline");
		controller.finish(afterToggle, RWScheduler.Pool.READ);
	}

	@Test
	void completionFromAnOlderPressureEpisodeCannotThrottleTheCurrentEpisode() throws Exception {
		var controller = Controller.create(1, Duration.ofSeconds(30));
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);
		controller.setPressured(true);
		var staleRead = controller.requireStart(RWScheduler.Pool.READ, 1L);

		controller.setPressured(false);
		controller.setPressured(true);
		controller.finish(staleRead, RWScheduler.Pool.READ);

		var currentWrite = controller.requireStart(RWScheduler.Pool.WRITE, 1L,
				"an old pressure generation must not install a deadline in the current generation");
		controller.finish(currentWrite, RWScheduler.Pool.WRITE);
	}

	@Test
	void completionFromAnExpiredCompetitionEpisodeCannotThrottleTheNextEpisode() throws Exception {
		var controller = Controller.create(
				4,
				Duration.ofSeconds(30),
				Duration.ofSeconds(30),
				Duration.ofNanos(1));
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);
		controller.setCompeting(RWScheduler.Pool.READ, true);
		var staleWrite = controller.requireStart(RWScheduler.Pool.WRITE, 1L);

		controller.setCompeting(RWScheduler.Pool.READ, false);
		controller.allowance(RWScheduler.Pool.WRITE, ELIGIBLE_TIME);
		controller.setCompeting(RWScheduler.Pool.READ, true);
		controller.finish(staleWrite, RWScheduler.Pool.WRITE);

		var currentWrite = controller.requireStart(RWScheduler.Pool.WRITE, 1L,
				"a completion from an expired competition generation must not pace its successor");
		controller.finish(currentWrite, RWScheduler.Pool.WRITE);
	}

	@Test
	void globalPressureCapIsConservedWhileFairnessFillsEveryAvailablePermit() throws Exception {
		var controller = Controller.create(2, Duration.ofNanos(1));
		controller.setPressured(true);
		controller.setQueued(RWScheduler.Pool.READ, true);
		controller.setQueued(RWScheduler.Pool.WRITE, true);
		controller.setDispatchable(RWScheduler.Pool.READ, true);
		controller.setDispatchable(RWScheduler.Pool.WRITE, true);

		var firstRead = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME);
		var secondRead = controller.requireStart(RWScheduler.Pool.READ, ELIGIBLE_TIME,
				"pressure fairness must not serialize unused capacity before any quantum completes");
		assertEquals(0, controller.allowance(RWScheduler.Pool.READ, ELIGIBLE_TIME));
		assertEquals(0, controller.allowance(RWScheduler.Pool.WRITE, ELIGIBLE_TIME));

		controller.finish(firstRead, RWScheduler.Pool.READ);
		assertEquals(0L, controller.tryStart(RWScheduler.Pool.READ, ELIGIBLE_TIME),
				"after a completion, the continuously queued peer owns the released slot");
		var firstWrite = controller.requireStart(RWScheduler.Pool.WRITE, ELIGIBLE_TIME);
		assertEquals(0, controller.allowance(RWScheduler.Pool.READ, ELIGIBLE_TIME));

		controller.finish(secondRead, RWScheduler.Pool.READ);
		controller.finish(firstWrite, RWScheduler.Pool.WRITE);
	}

	private static final class Controller {

		private final Object instance;
		private final Class<?> type;
		private final AtomicBoolean readDispatchable = new AtomicBoolean();
		private final AtomicBoolean writeDispatchable = new AtomicBoolean();
		private final Method setPressured;
		private final Method setQueued;
		private final Method isDispatchable;
		private final Method dispatchabilityLost;
		private final Method setCompeting;
		private final Method setNotifier;
		private final Method setBatchNotifier;
		private final Method signalPendingAvailability;
		private final Method tryStart;
		private final Method finish;
		private final Method allowance;
		private final Method waitNanos;

		private Controller(Object instance, Class<?> type) throws ReflectiveOperationException {
			this.instance = instance;
			this.type = type;
			this.setPressured = accessible(type.getDeclaredMethod("setPressured", boolean.class));
			this.setQueued = accessible(type.getDeclaredMethod(
					"setBatchQueued", RWScheduler.Pool.class, boolean.class));
			var setDispatchabilitySources = accessible(type.getDeclaredMethod(
					"setBatchDispatchabilitySources",
					java.util.function.BooleanSupplier.class,
					java.util.function.BooleanSupplier.class));
			setDispatchabilitySources.invoke(instance,
					(java.util.function.BooleanSupplier) readDispatchable::get,
					(java.util.function.BooleanSupplier) writeDispatchable::get);
			this.isDispatchable = accessible(type.getDeclaredMethod(
					"isBatchDispatchable", RWScheduler.Pool.class));
			this.dispatchabilityLost = accessible(type.getDeclaredMethod(
					"batchDispatchabilityLost", RWScheduler.Pool.class));
			this.setCompeting = accessible(type.getDeclaredMethod(
					"setPoolCompetition", RWScheduler.Pool.class, boolean.class));
			this.setNotifier = accessible(type.getDeclaredMethod("setNotifier", Runnable.class));
			this.setBatchNotifier = accessible(type.getDeclaredMethod("setBatchNotifier", IntConsumer.class));
			this.signalPendingAvailability = accessible(type.getDeclaredMethod("signalPendingAvailability"));
			this.tryStart = accessible(type.getDeclaredMethod(
					"tryStartBatch", boolean.class, RWScheduler.Pool.class, long.class));
			this.finish = accessible(type.getDeclaredMethod(
					"finishBatch", long.class, RWScheduler.Pool.class));
			this.allowance = accessible(type.getDeclaredMethod(
					"batchStartAllowance", boolean.class, RWScheduler.Pool.class, long.class));
			this.waitNanos = accessible(type.getDeclaredMethod(
					"nanosUntilBatchEligible", RWScheduler.Pool.class, long.class));
		}

		static Controller create(int pressuredMaximumActive, Duration pressureInterval)
				throws ReflectiveOperationException {
			return create(
					pressuredMaximumActive,
					pressureInterval,
					Duration.ofSeconds(30),
					Duration.ofSeconds(30));
		}

		static Controller create(int pressuredMaximumActive,
		                         Duration pressureInterval,
		                         Duration competitionInterval,
		                         Duration competitionHold) throws ReflectiveOperationException {
			var type = Class.forName("it.cavallium.rockserver.core.impl.WorkloadPressureController");
			var constructor = type.getDeclaredConstructor(
					int.class,
					int.class,
					Duration.class,
					int.class,
					Duration.class,
					Duration.class);
			constructor.setAccessible(true);
			return new Controller(constructor.newInstance(
					4,
					4,
					competitionInterval,
					pressuredMaximumActive,
					competitionHold,
					pressureInterval), type);
		}

		void setPressured(boolean pressured) throws ReflectiveOperationException {
			setPressured.invoke(instance, pressured);
		}

		void setQueued(RWScheduler.Pool pool, boolean queued) throws ReflectiveOperationException {
			setQueued.invoke(instance, pool, queued);
		}

		void setDispatchable(RWScheduler.Pool pool, boolean dispatchable)
				throws ReflectiveOperationException {
			(pool == RWScheduler.Pool.READ ? readDispatchable : writeDispatchable).set(dispatchable);
			if (!dispatchable) {
				dispatchabilityLost.invoke(instance, pool);
			}
		}

		boolean isDispatchable(RWScheduler.Pool pool) throws ReflectiveOperationException {
			return (boolean) isDispatchable.invoke(instance, pool);
		}

		boolean isQueued(RWScheduler.Pool pool) throws ReflectiveOperationException {
			Field field = type.getDeclaredField("queuedBatchPoolMask");
			field.setAccessible(true);
			return (field.getInt(instance) & 1 << pool.ordinal()) != 0;
		}

		void setCompeting(RWScheduler.Pool pool, boolean competing) throws ReflectiveOperationException {
			setCompeting.invoke(instance, pool, competing);
		}

		void setNotifier(Runnable notifier) throws ReflectiveOperationException {
			setNotifier.invoke(instance, notifier);
		}

		void setBatchNotifier(IntConsumer notifier) throws ReflectiveOperationException {
			setBatchNotifier.invoke(instance, notifier);
		}

		void signalPendingAvailability() throws ReflectiveOperationException {
			signalPendingAvailability.invoke(instance);
		}

		long tryStart(RWScheduler.Pool pool, long nowNanos) throws ReflectiveOperationException {
			return (long) tryStart.invoke(instance, false, pool, nowNanos);
		}

		long requireStart(RWScheduler.Pool pool, long nowNanos) throws ReflectiveOperationException {
			return requireStart(pool, nowNanos, "expected BATCH permit for " + pool);
		}

		long requireStart(RWScheduler.Pool pool, long nowNanos, String message)
				throws ReflectiveOperationException {
			long permit = tryStart(pool, nowNanos);
			assertNotEquals(0L, permit, message);
			return permit;
		}

		void finish(long permit, RWScheduler.Pool pool) throws ReflectiveOperationException {
			finish.invoke(instance, permit, pool);
		}

		int allowance(RWScheduler.Pool pool, long nowNanos) throws ReflectiveOperationException {
			return (int) allowance.invoke(instance, false, pool, nowNanos);
		}

		long waitNanos(RWScheduler.Pool pool, long nowNanos) throws ReflectiveOperationException {
			return (long) waitNanos.invoke(instance, pool, nowNanos);
		}

		long longField(String name) throws ReflectiveOperationException {
			Field field = type.getDeclaredField(name);
			field.setAccessible(true);
			return field.getLong(instance);
		}

		private static Method accessible(Method method) {
			method.setAccessible(true);
			return method;
		}
	}
}
