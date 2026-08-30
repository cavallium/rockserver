package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;

/** Exhaustive semantic-permit-symmetry bisimulation for pressured caps greater than one. */
class WorkloadPressureControllerConcurrentExhaustiveTest {

	@Test
	void everyReachableConcurrentPressureStateMatchesTheIndependentModel() {
		var capOne = explore(1);
		var capTwo = explore(2);
		var capThree = explore(3);

		assertCoverage(capOne);
		assertCoverage(capTwo);
		assertCoverage(capThree);
		assertTrue(capTwo.concurrentSamePoolWitness(), "cap two never filled concurrently");
		assertTrue(capThree.concurrentSamePoolWitness(), "cap three never filled concurrently");
		assertTrue(capTwo.mixedPoolWitness(), "cap two never mixed READ and WRITE permits");
		assertTrue(capThree.mixedPoolWitness(), "cap three never mixed READ and WRITE permits");
		assertTrue(capTwo.saturatedPeerWorkWitness(),
				"cap two never exercised work conservation against a saturated peer");
		assertTrue(capThree.saturatedPeerWorkWitness(),
				"cap three never exercised work conservation against a saturated peer");
		assertTrue(capTwo.extraSlotWitness(), "cap two never preserved a peer slot while using capacity");
		assertTrue(capThree.extraSlotWitness(), "cap three never preserved a peer slot while using capacity");
		assertTrue(capTwo.finalSlotReservationWitness(), "cap two never reserved its final slot");
		assertTrue(capThree.finalSlotReservationWitness(), "cap three never reserved its final slot");

		for (var result : List.of(capOne, capTwo, capThree)) {
			System.out.printf(java.util.Locale.ROOT,
					"Concurrent pressure graph cap=%d: states=%d transitions=%d maxDepth=%d actions=%s%n",
					result.cap(), result.states(), result.transitions(), result.maximumDepth(), result.actions());
		}
	}

	@Test
	void controllerCloneAccessFailsClosedWhenMutableStateShapeChanges() {
		assertEquals(Subject.COPIED_FIELDS, Subject.mutableStateFields(),
				"new controller state must be deliberately included in concurrent exhaustive cloning");
	}

	private static void assertCoverage(Exploration result) {
		assertTrue(result.states() >= 1_000, "cap " + result.cap() + " graph unexpectedly collapsed");
		assertTrue(result.transitions() >= 10_000,
				"cap " + result.cap() + " transition coverage unexpectedly collapsed");
		for (var kind : ConcurrentPressureControllerModel.Kind.values()) {
			assertTrue(result.actions().getOrDefault(kind, 0L) > 0L,
					"cap " + result.cap() + " missed action " + kind);
		}
		assertTrue(result.pressureGenerationWitness(), "cap " + result.cap() + " missed stale pressure");
		assertTrue(result.competitionGenerationWitness(), "cap " + result.cap() + " missed stale competition");
		assertTrue(result.deferredWakeWitness(), "cap " + result.cap() + " missed deferred wakeup");
		assertTrue(result.directWakeWitness(), "cap " + result.cap() + " missed direct peer wakeup");
	}

	private static Exploration explore(int cap) {
		var limits = new ConcurrentPressureControllerModel.Limits(cap, cap, 1);
		var root = Pair.create(limits);
		root.compare("initial state", List.of());
		var visited = new HashSet<ConcurrentPressureControllerModel.Snapshot>();
		visited.add(root.model.snapshot());
		var actionCounts = new EnumMap<ConcurrentPressureControllerModel.Kind, Long>(
				ConcurrentPressureControllerModel.Kind.class);
		var trace = new ArrayList<ConcurrentPressureControllerModel.Action>();
		var accumulator = new Accumulator(cap, visited, actionCounts);
		explore(root, trace, 0, accumulator);
		return accumulator.result();
	}

	private static void explore(Pair pair,
	                            ArrayList<ConcurrentPressureControllerModel.Action> trace,
	                            int depth,
	                            Accumulator accumulator) {
		accumulator.maximumDepth = Math.max(accumulator.maximumDepth, depth);
		for (var action : pair.model.actions()) {
			if (!pair.model.applicable(action)) {
				continue;
			}
			accumulator.transitions++;
			accumulator.actionCounts.merge(action.kind(), 1L, Long::sum);
			var successor = pair.copy();
			trace.add(action);
			try {
				var result = successor.apply(action);
				successor.compare("after " + action, trace);
				accumulator.observe(action,
						successor.model.snapshot(),
						successor.model.eligibility(),
						result);
			} catch (Throwable divergence) {
				throw counterexample(divergence, trace);
			}
			var state = successor.model.snapshot();
			if (accumulator.visited.add(state)) {
				explore(successor, trace, depth + 1, accumulator);
			}
			trace.removeLast();
		}
	}

	private static AssertionError counterexample(Throwable divergence,
			List<ConcurrentPressureControllerModel.Action> trace) {
		return new AssertionError("concurrent pressure-controller/model divergence\ntrace:\n"
				+ formatTrace(trace) + '\n' + divergence.getMessage(), divergence);
	}

	private static String formatTrace(List<ConcurrentPressureControllerModel.Action> trace) {
		var text = new StringBuilder();
		for (int index = 0; index < trace.size(); index++) {
			text.append(index).append(": ").append(trace.get(index)).append('\n');
		}
		return text.toString();
	}

	private record Exploration(int cap,
			int states,
			long transitions,
			int maximumDepth,
			Map<ConcurrentPressureControllerModel.Kind, Long> actions,
			boolean pressureGenerationWitness,
			boolean competitionGenerationWitness,
			boolean concurrentSamePoolWitness,
			boolean mixedPoolWitness,
			boolean saturatedPeerWorkWitness,
			boolean extraSlotWitness,
			boolean finalSlotReservationWitness,
			boolean deferredWakeWitness,
			boolean directWakeWitness) {
	}

	private static final class Accumulator {

		private final int cap;
		private final Set<ConcurrentPressureControllerModel.Snapshot> visited;
		private final EnumMap<ConcurrentPressureControllerModel.Kind, Long> actionCounts;
		private long transitions;
		private int maximumDepth;
		private boolean pressureGenerationWitness;
		private boolean competitionGenerationWitness;
		private boolean concurrentSamePoolWitness;
		private boolean mixedPoolWitness;
		private boolean saturatedPeerWorkWitness;
		private boolean extraSlotWitness;
		private boolean finalSlotReservationWitness;
		private boolean deferredWakeWitness;
		private boolean directWakeWitness;

		private Accumulator(int cap,
		                    Set<ConcurrentPressureControllerModel.Snapshot> visited,
		                    EnumMap<ConcurrentPressureControllerModel.Kind, Long> actionCounts) {
			this.cap = cap;
			this.visited = visited;
			this.actionCounts = actionCounts;
		}

		private void observe(ConcurrentPressureControllerModel.Action action,
		                     ConcurrentPressureControllerModel.Snapshot state,
		                     ConcurrentPressureControllerModel.Eligibility eligibility,
		                     ConcurrentPressureControllerModel.ActionResult result) {
			pressureGenerationWitness |= state.pressureGeneration()
					>= ConcurrentPressureControllerModel.MAX_PRESSURE_TRANSITIONS
					&& state.pressured()
					&& state.permits().stream().anyMatch(permit -> permit.pressured()
					&& permit.pressureGeneration() != state.pressureGeneration());
			competitionGenerationWitness |= state.competitionGeneration() >= 1
					&& state.competitionPhase() != ConcurrentPressureControllerModel.CompetitionPhase.NONE
					&& state.permits().stream().anyMatch(permit -> permit.competing()
					&& permit.competitionGeneration() != state.competitionGeneration());
			long reads = state.permits().stream()
					.filter(permit -> permit.pool() == ConcurrentPressureControllerModel.Pool.READ)
					.count();
			long writes = state.permits().size() - reads;
			boolean allPressured = state.permits().stream()
					.allMatch(ConcurrentPressureControllerModel.Permit::pressured);
			concurrentSamePoolWitness |= state.pressured()
					&& allPressured
					&& state.permits().size() == cap
					&& (reads == cap || writes == cap);
			mixedPoolWitness |= state.pressured() && allPressured && reads > 0 && writes > 0;
			int available = cap - state.permits().size();
			boolean lastRead = state.lastCompleted() == ConcurrentPressureControllerModel.Pool.READ;
			saturatedPeerWorkWitness |= state.pressured()
					&& available == 1
					&& lastRead
					&& writes == 1
					&& state.readDispatchable()
					&& state.writeDispatchable()
					&& eligibility.readLate() > 0
					&& eligibility.writeLate() == 0;
			extraSlotWitness |= state.pressured()
					&& available > 1
					&& state.lastCompleted() != null
					&& state.readDispatchable()
					&& state.writeDispatchable()
					&& (state.lastCompleted() == ConcurrentPressureControllerModel.Pool.READ
					? eligibility.readLate() > 0 : eligibility.writeLate() > 0);
			finalSlotReservationWitness |= state.pressured()
					&& available == 1
					&& state.readDispatchable()
					&& state.writeDispatchable()
					&& state.lastCompleted() != null
					&& (state.lastCompleted() == ConcurrentPressureControllerModel.Pool.READ
					? eligibility.readLate() == 0 && eligibility.writeLate() > 0
					: eligibility.writeLate() == 0 && eligibility.readLate() > 0);
			deferredWakeWitness |= action.kind() == ConcurrentPressureControllerModel.Kind.FLUSH_NOTIFICATIONS
					&& result.notifierCalls() > 0;
			directWakeWitness |= action.kind() == ConcurrentPressureControllerModel.Kind.FINISH
					&& result.batchWakeMask() != 0;
		}

		private Exploration result() {
			return new Exploration(cap,
					visited.size(),
					transitions,
					maximumDepth,
					Map.copyOf(actionCounts),
					pressureGenerationWitness,
					competitionGenerationWitness,
					concurrentSamePoolWitness,
					mixedPoolWitness,
					saturatedPeerWorkWitness,
					extraSlotWitness,
					finalSlotReservationWitness,
					deferredWakeWitness,
					directWakeWitness);
		}
	}

	private static final class Pair {

		private final ConcurrentPressureControllerModel model;
		private final Subject subject;

		private Pair(ConcurrentPressureControllerModel model, Subject subject) {
			this.model = model;
			this.subject = subject;
		}

		static Pair create(ConcurrentPressureControllerModel.Limits limits) {
			return new Pair(new ConcurrentPressureControllerModel(limits), Subject.create(limits));
		}

		Pair copy() {
			return new Pair(model.copy(), subject.copy());
		}

		ConcurrentPressureControllerModel.ActionResult apply(
				ConcurrentPressureControllerModel.Action action) {
			var expected = model.apply(action);
			var actual = subject.apply(action);
			if (!expected.equals(actual)) {
				throw new AssertionError("action result differs: expected=" + expected + " actual=" + actual);
			}
			return expected;
		}

		void compare(String point, List<ConcurrentPressureControllerModel.Action> trace) {
			var expected = model.snapshot();
			var actual = subject.snapshot(expected.pressureTransitions(), expected.competitionChanges());
			if (!expected.equals(actual)) {
				throw new AssertionError(point + " state differs\nexpected=" + expected + "\nactual=" + actual
						+ "\ntrace:\n" + formatTrace(trace));
			}
			var expectedEligibility = model.eligibility();
			var actualEligibility = subject.eligibility();
			if (!expectedEligibility.equals(actualEligibility)) {
				throw new AssertionError(point + " eligibility differs\nexpected=" + expectedEligibility
						+ "\nactual=" + actualEligibility + "\ntrace:\n" + formatTrace(trace));
			}
			model.validate();
			subject.validateConservation();
		}
	}

	private static final class Subject {

		private static final long NO_PERMIT = 0L;
		private static final long PRESSURED_FLAG = 1L << 1;
		private static final long COMPETING_FLAG = 1L << 2;
		private static final int PRESSURE_SHIFT = 3;
		private static final int COMPETITION_SHIFT = 33;
		private static final int GENERATION_MASK = (1 << 30) - 1;
		private static final Set<String> COPIED_FIELDS = Set.of(
				"pressured", "activeBatches", "activeReadBatches", "activeWriteBatches",
				"nextCompetingWriteNanos", "nextBatchNanos", "preemptionPoolMask",
				"competitionPoolMask", "queuedBatchPoolMask", "lastCompletedPressuredBatchPoolBit",
				"pressureGeneration", "competitionGeneration", "competitionUntilNanos",
				"preemptionRequested", "notificationPending");
		private static final Set<String> EXTERNAL_OR_CALLBACK_FIELDS = Set.of(
				"readBatchDispatchable", "writeBatchDispatchable", "notifier", "batchNotifier");

		private record Token(ConcurrentPressureControllerModel.Pool pool, long value) {
		}

		private final ConcurrentPressureControllerModel.Limits limits;
		private final Object controller;
		private final Class<?> type;
		private final AtomicBoolean readDispatchable = new AtomicBoolean();
		private final AtomicBoolean writeDispatchable = new AtomicBoolean();
		private final AtomicInteger notifierCalls = new AtomicInteger();
		private final AtomicInteger batchWakeMask = new AtomicInteger();
		private final ArrayList<Token> permits = new ArrayList<>();
		private final Method setPressured;
		private final Method setQueued;
		private final Method dispatchabilityLost;
		private final Method setCompetition;
		private final Method tryStart;
		private final Method finish;
		private final Method abort;
		private final Method allowance;
		private final Method signalPending;
		private final Method fairTurn;

		private Subject(ConcurrentPressureControllerModel.Limits limits,
		                Object controller,
		                Class<?> type) throws ReflectiveOperationException {
			this.limits = limits;
			this.controller = controller;
			this.type = type;
			setPressured = accessible(type.getDeclaredMethod("setPressured", boolean.class));
			setQueued = accessible(type.getDeclaredMethod(
					"setBatchQueued", RWScheduler.Pool.class, boolean.class));
			dispatchabilityLost = accessible(type.getDeclaredMethod(
					"batchDispatchabilityLost", RWScheduler.Pool.class));
			setCompetition = accessible(type.getDeclaredMethod(
					"setPoolCompetition", RWScheduler.Pool.class, boolean.class));
			tryStart = accessible(type.getDeclaredMethod(
					"tryStartBatch", boolean.class, RWScheduler.Pool.class, long.class));
			finish = accessible(type.getDeclaredMethod(
					"finishBatch", long.class, RWScheduler.Pool.class));
			abort = accessible(type.getDeclaredMethod(
					"abortBatch", long.class, RWScheduler.Pool.class));
			allowance = accessible(type.getDeclaredMethod(
					"batchStartAllowance", boolean.class, RWScheduler.Pool.class, long.class));
			signalPending = accessible(type.getDeclaredMethod("signalPendingAvailability"));
			fairTurn = accessible(type.getDeclaredMethod(
					"hasFairPressureTurn", RWScheduler.Pool.class));
			invoke(accessible(type.getDeclaredMethod("setBatchDispatchabilitySources",
					BooleanSupplier.class, BooleanSupplier.class)),
					(BooleanSupplier) readDispatchable::get,
					(BooleanSupplier) writeDispatchable::get);
			invoke(accessible(type.getDeclaredMethod("setNotifier", Runnable.class)),
					(Runnable) notifierCalls::incrementAndGet);
			invoke(accessible(type.getDeclaredMethod("setBatchNotifier", IntConsumer.class)),
					(IntConsumer) mask -> batchWakeMask.getAndAccumulate(mask, (left, right) -> left | right));
		}

		static Subject create(ConcurrentPressureControllerModel.Limits limits) {
			try {
				var type = Class.forName("it.cavallium.rockserver.core.impl.WorkloadPressureController");
				var constructor = type.getDeclaredConstructor(int.class,
						int.class,
						Duration.class,
						int.class,
						Duration.class,
						Duration.class);
				constructor.setAccessible(true);
				return new Subject(limits,
						constructor.newInstance(limits.competingReadCap(),
								limits.competingWriteCap(),
								Duration.ofDays(1),
								limits.pressureCap(),
								Duration.ofDays(1),
								Duration.ofDays(1)),
						type);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("unable to create concurrent pressure controller", failure);
			}
		}

		Subject copy() {
			var copy = create(limits);
			try {
				for (String name : COPIED_FIELDS) {
					Field source = field(type, name);
					Field target = field(copy.type, name);
					target.set(copy.controller, source.get(controller));
				}
				copy.readDispatchable.set(readDispatchable.get());
				copy.writeDispatchable.set(writeDispatchable.get());
				copy.permits.addAll(permits);
				return copy;
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("unable to clone concurrent pressure controller", failure);
			}
		}

		ConcurrentPressureControllerModel.ActionResult apply(
				ConcurrentPressureControllerModel.Action action) {
			notifierCalls.set(0);
			batchWakeMask.set(0);
			boolean started = false;
			switch (action.kind()) {
				case PRESSURE -> invoke(setPressured, action.value());
				case QUEUED -> invoke(setQueued, actual(action.pool()), action.value());
				case DISPATCHABLE -> {
					dispatchable(action.pool()).set(action.value());
					if (!action.value()) {
						invoke(dispatchabilityLost, actual(action.pool()));
					}
				}
				case COMPETITION -> invoke(setCompetition, actual(action.pool()), action.value());
				case START -> {
					long permit = (long) invoke(tryStart,
							false,
							actual(action.pool()),
							action.time() == ConcurrentPressureControllerModel.Time.EARLY
									? ConcurrentPressureControllerModel.EARLY_TIME
									: ConcurrentPressureControllerModel.LATE_TIME);
					started = permit != NO_PERMIT;
					if (started) {
						permits.add(new Token(action.pool(), permit));
					}
				}
				case FINISH -> {
					var token = remove(action.permit());
					invoke(finish, token.value(), actual(token.pool()));
				}
				case CANCEL -> {
					var token = remove(action.permit());
					invoke(abort, token.value(), actual(token.pool()));
				}
				case EXPIRE -> invoke(allowance,
						false, RWScheduler.Pool.READ, ConcurrentPressureControllerModel.LATE_TIME);
				case FLUSH_NOTIFICATIONS -> invoke(signalPending);
			}
			return new ConcurrentPressureControllerModel.ActionResult(
					started, notifierCalls.get(), batchWakeMask.get());
		}

		ConcurrentPressureControllerModel.Snapshot snapshot(int pressureTransitions,
				int competitionChanges) {
			int competitionMask = intField("competitionPoolMask");
			long competitionUntil = longField("competitionUntilNanos");
			var phase = competitionMask != 0
					? ConcurrentPressureControllerModel.CompetitionPhase.ACTIVE
					: competitionUntil == Long.MIN_VALUE
							? ConcurrentPressureControllerModel.CompetitionPhase.NONE
							: ConcurrentPressureControllerModel.CompetitionPhase.HOLDING;
			int queuedMask = intField("queuedBatchPoolMask");
			int lastBit = intField("lastCompletedPressuredBatchPoolBit");
			var semanticPermits = permits.stream().map(this::decode).sorted(java.util.Comparator
					.comparing(ConcurrentPressureControllerModel.Permit::pool)
					.thenComparing(ConcurrentPressureControllerModel.Permit::pressured)
					.thenComparing(ConcurrentPressureControllerModel.Permit::competing)
					.thenComparingInt(ConcurrentPressureControllerModel.Permit::pressureGeneration)
					.thenComparingInt(ConcurrentPressureControllerModel.Permit::competitionGeneration)).toList();
			return new ConcurrentPressureControllerModel.Snapshot(
					booleanField("pressured"),
					intField("pressureGeneration"),
					intField("competitionGeneration"),
					pressureTransitions,
					competitionChanges,
					(queuedMask & 1) != 0,
					(queuedMask & 2) != 0,
					readDispatchable.get(),
					writeDispatchable.get(),
					(competitionMask & 1) != 0,
					(competitionMask & 2) != 0,
					phase,
					semanticPermits,
					lastBit == 1 ? ConcurrentPressureControllerModel.Pool.READ
							: lastBit == 2 ? ConcurrentPressureControllerModel.Pool.WRITE : null,
					longField("nextBatchNanos") != Long.MIN_VALUE,
					longField("nextCompetingWriteNanos") != Long.MIN_VALUE,
					booleanField("notificationPending"),
					(boolean) invoke(fairTurn, RWScheduler.Pool.READ),
					(boolean) invoke(fairTurn, RWScheduler.Pool.WRITE));
		}

		ConcurrentPressureControllerModel.Eligibility eligibility() {
			var early = copy();
			int readEarly = (int) early.invoke(early.allowance,
					false, RWScheduler.Pool.READ, ConcurrentPressureControllerModel.EARLY_TIME);
			int writeEarly = (int) early.invoke(early.allowance,
					false, RWScheduler.Pool.WRITE, ConcurrentPressureControllerModel.EARLY_TIME);
			var late = copy();
			int readLate = (int) late.invoke(late.allowance,
					false, RWScheduler.Pool.READ, ConcurrentPressureControllerModel.LATE_TIME);
			int writeLate = (int) late.invoke(late.allowance,
					false, RWScheduler.Pool.WRITE, ConcurrentPressureControllerModel.LATE_TIME);
			return new ConcurrentPressureControllerModel.Eligibility(
					readEarly, writeEarly, readLate, writeLate);
		}

		void validateConservation() {
			int active = intField("activeBatches");
			int read = intField("activeReadBatches");
			int write = intField("activeWriteBatches");
			long tokensRead = permits.stream()
					.filter(token -> token.pool() == ConcurrentPressureControllerModel.Pool.READ)
					.count();
			if (active != read + write || active != permits.size()
					|| read != tokensRead || write != permits.size() - tokensRead
					|| active < 0 || read < 0 || write < 0) {
				throw new AssertionError("concurrent subject permit counters are not conserved");
			}
		}

		static Set<String> mutableStateFields() {
			try {
				var result = new HashSet<String>();
				for (Field field : Class.forName(
						"it.cavallium.rockserver.core.impl.WorkloadPressureController").getDeclaredFields()) {
					int modifiers = field.getModifiers();
					if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers)
							|| EXTERNAL_OR_CALLBACK_FIELDS.contains(field.getName())) {
						continue;
					}
					result.add(field.getName());
				}
				return Set.copyOf(result);
			} catch (ClassNotFoundException failure) {
				throw new AssertionError(failure);
			}
		}

		private Token remove(ConcurrentPressureControllerModel.Permit permit) {
			for (int index = 0; index < permits.size(); index++) {
				var token = permits.get(index);
				if (decode(token).equals(permit)) {
					permits.remove(index);
					return token;
				}
			}
			throw new AssertionError("missing subject permit " + permit);
		}

		private ConcurrentPressureControllerModel.Permit decode(Token token) {
			long value = token.value();
			boolean pressured = (value & PRESSURED_FLAG) != 0;
			boolean competing = (value & COMPETING_FLAG) != 0;
			return new ConcurrentPressureControllerModel.Permit(token.pool(),
					pressured,
					competing,
					pressured ? (int) (value >>> PRESSURE_SHIFT) & GENERATION_MASK : 0,
					competing ? (int) (value >>> COMPETITION_SHIFT) & GENERATION_MASK : 0);
		}

		private AtomicBoolean dispatchable(ConcurrentPressureControllerModel.Pool pool) {
			return pool == ConcurrentPressureControllerModel.Pool.READ
					? readDispatchable : writeDispatchable;
		}

		private int intField(String name) {
			try {
				return field(type, name).getInt(controller);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError(failure);
			}
		}

		private long longField(String name) {
			try {
				return field(type, name).getLong(controller);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError(failure);
			}
		}

		private boolean booleanField(String name) {
			try {
				return field(type, name).getBoolean(controller);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError(failure);
			}
		}

		private static Field field(Class<?> type, String name) throws NoSuchFieldException {
			Field field = type.getDeclaredField(name);
			field.setAccessible(true);
			return field;
		}

		private Object invoke(Method method, Object... arguments) {
			try {
				return method.invoke(controller, arguments);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("controller call failed: " + method.getName(), failure);
			}
		}

		private static Method accessible(Method method) {
			method.setAccessible(true);
			return method;
		}

		private static RWScheduler.Pool actual(ConcurrentPressureControllerModel.Pool pool) {
			return pool == ConcurrentPressureControllerModel.Pool.READ
					? RWScheduler.Pool.READ : RWScheduler.Pool.WRITE;
		}
	}
}
