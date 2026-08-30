package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayDeque;
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

/** Exhaustive bisimulation over the bounded two-pool pressure-controller state graph. */
class WorkloadPressureControllerExhaustiveTest {

	private static final int MAX_DEPTH = 20;

	@Test
	void everyReachableBoundedPressureStateMatchesTheIndependentModel() {
		var result = explore();

		assertTrue(result.saturated(), "the configured depth must cover the full bounded graph");
		assertTrue(result.states() >= 1_000, "exhaustive graph unexpectedly collapsed");
		assertTrue(result.transitions() >= 10_000, "exhaustive transition coverage unexpectedly collapsed");
		for (var kind : PressureControllerExhaustiveModel.Kind.values()) {
			assertTrue(result.actions().getOrDefault(kind, 0L) > 0L, "missing action coverage for " + kind);
		}
		assertTrue(result.maximumDepth() >= 8, "multi-episode stale-permit traces were not reached");
		assertTrue(result.pressureGenerationWitness(), "pressure on/off/on generations were not covered");
		assertTrue(result.competitionGenerationWitness(), "competition expiry generation was not covered");
		assertTrue(result.nondispatchablePeerWitness(), "nondispatchable-peer work conservation was not covered");
		assertTrue(result.alternationWitness(), "two-pool fair alternation was not covered");
		System.out.printf(java.util.Locale.ROOT,
				"Exhaustive pressure graph: states=%d transitions=%d maxDepth=%d actions=%s%n",
				result.states(), result.transitions(), result.maximumDepth(), result.actions());
	}

	@Test
	void controllerCloneAccessFailsClosedWhenMutableStateShapeChanges() {
		assertEquals(Subject.COPIED_FIELDS, Subject.mutableStateFields(),
				"new controller state must be deliberately included in exhaustive branch cloning");
	}

	private static Exploration explore() {
		var initial = Pair.create();
		initial.compare("initial state", List.of());
		var queue = new ArrayDeque<Node>();
		var visited = new HashSet<PressureControllerExhaustiveModel.Snapshot>();
		var root = new Node(initial, null, null, 0);
		queue.add(root);
		visited.add(initial.model.snapshot());
		var actionCounts = new EnumMap<PressureControllerExhaustiveModel.Kind, Long>(
				PressureControllerExhaustiveModel.Kind.class);
		long transitions = 0L;
		int maximumDepth = 0;
		boolean saturated = true;
		boolean pressureGenerationWitness = false;
		boolean competitionGenerationWitness = false;
		boolean nondispatchablePeerWitness = false;
		boolean alternationWitness = false;
		while (!queue.isEmpty()) {
			Node node = queue.removeFirst();
			maximumDepth = Math.max(maximumDepth, node.depth());
			for (var action : PressureControllerExhaustiveModel.actions()) {
				if (!node.pair().model.applicable(action)) continue;
				transitions++;
				actionCounts.merge(action.kind(), 1L, Long::sum);
				var successor = node.pair().copy();
				var trace = append(node, action);
				try {
					successor.apply(action);
					successor.compare("after " + action, trace);
				} catch (Throwable divergence) {
					throw counterexample(divergence, trace);
				}
				var state = successor.model.snapshot();
				pressureGenerationWitness |= state.pressureGeneration()
						>= PressureControllerExhaustiveModel.MAX_PRESSURE_TRANSITIONS;
				competitionGenerationWitness |= state.competitionGeneration() >= 1;
				nondispatchablePeerWitness |= state.pressured()
						&& state.activePool() == null
						&& state.readDispatchable() != state.writeDispatchable()
						&& (state.lastCompleted() != null);
				alternationWitness |= state.pressured()
						&& state.activePool() == null
						&& state.readDispatchable()
						&& state.writeDispatchable()
						&& state.lastCompleted() != null
						&& state.readFairTurn() != state.writeFairTurn();
				if (!visited.add(state)) continue;
				if (node.depth() >= MAX_DEPTH) {
					saturated = false;
					throw new AssertionError("bounded graph did not saturate by depth " + MAX_DEPTH
							+ "\nshortest newly discovered trace:\n" + formatTrace(trace));
				}
				queue.addLast(new Node(successor, node, action, node.depth() + 1));
			}
		}
		return new Exploration(visited.size(), transitions, maximumDepth, saturated,
				Map.copyOf(actionCounts), pressureGenerationWitness, competitionGenerationWitness,
				nondispatchablePeerWitness, alternationWitness);
	}

	private static AssertionError counterexample(Throwable divergence,
			List<PressureControllerExhaustiveModel.Action> trace) {
		return new AssertionError("pressure-controller/model divergence\nshortest counterexample trace:\n"
				+ formatTrace(trace) + "\n" + divergence.getMessage(), divergence);
	}

	private static List<PressureControllerExhaustiveModel.Action> append(Node node,
			PressureControllerExhaustiveModel.Action action) {
		var trace = trace(node);
		trace.add(action);
		return List.copyOf(trace);
	}

	private static ArrayList<PressureControllerExhaustiveModel.Action> trace(Node node) {
		var reverse = new ArrayList<PressureControllerExhaustiveModel.Action>();
		for (Node cursor = node; cursor != null && cursor.action() != null; cursor = cursor.previous()) {
			reverse.add(cursor.action());
		}
		java.util.Collections.reverse(reverse);
		return reverse;
	}

	private static String formatTrace(List<PressureControllerExhaustiveModel.Action> trace) {
		var text = new StringBuilder();
		for (int index = 0; index < trace.size(); index++) {
			text.append(index).append(": ").append(trace.get(index)).append('\n');
		}
		return text.toString();
	}

	private record Node(Pair pair,
			Node previous,
			PressureControllerExhaustiveModel.Action action,
			int depth) {
	}

	private record Exploration(int states,
			long transitions,
			int maximumDepth,
			boolean saturated,
			Map<PressureControllerExhaustiveModel.Kind, Long> actions,
			boolean pressureGenerationWitness,
			boolean competitionGenerationWitness,
			boolean nondispatchablePeerWitness,
			boolean alternationWitness) {
	}

	private static final class Pair {

		private final PressureControllerExhaustiveModel model;
		private final Subject subject;

		private Pair(PressureControllerExhaustiveModel model, Subject subject) {
			this.model = model;
			this.subject = subject;
		}

		static Pair create() {
			return new Pair(new PressureControllerExhaustiveModel(), Subject.create());
		}

		Pair copy() {
			return new Pair(model.copy(), subject.copy());
		}

		void apply(PressureControllerExhaustiveModel.Action action) {
			var expected = model.apply(action);
			var actual = subject.apply(action);
			if (!expected.equals(actual)) {
				throw new AssertionError("action result differs: expected=" + expected + " actual=" + actual);
			}
		}

		void compare(String point, List<PressureControllerExhaustiveModel.Action> trace) {
			var expected = model.snapshot();
			var actual = subject.snapshot(expected.pressureTransitions(), expected.competitionChanges());
			if (!expected.equals(actual)) {
				throw new AssertionError(point + " state differs\nexpected=" + expected + "\nactual=" + actual
						+ "\ntrace:\n" + formatTrace(trace));
			}
			model.validate();
			subject.validateConservation();
		}
	}

	private static final class Subject {

		static final Set<String> COPIED_FIELDS = Set.of(
				"pressured", "activeBatches", "activeReadBatches", "activeWriteBatches",
				"nextCompetingWriteNanos", "nextBatchNanos", "preemptionPoolMask",
				"competitionPoolMask", "queuedBatchPoolMask", "lastCompletedPressuredBatchPoolBit",
				"pressureGeneration", "competitionGeneration", "competitionUntilNanos",
				"preemptionRequested", "notificationPending");
		private static final Set<String> EXTERNAL_OR_CALLBACK_FIELDS = Set.of(
				"readBatchDispatchable", "writeBatchDispatchable", "notifier", "batchNotifier");
		private static final long NO_PERMIT = 0L;
		private static final long PRESSURED_FLAG = 1L << 1;
		private static final long COMPETING_FLAG = 1L << 2;
		private static final int PRESSURE_SHIFT = 3;
		private static final int COMPETITION_SHIFT = 33;
		private static final int GENERATION_MASK = (1 << 30) - 1;
		private final Object controller;
		private final Class<?> type;
		private final AtomicBoolean readDispatchable = new AtomicBoolean();
		private final AtomicBoolean writeDispatchable = new AtomicBoolean();
		private final AtomicInteger notifierCalls = new AtomicInteger();
		private final AtomicInteger batchWakeMask = new AtomicInteger();
		private final Method setPressured;
		private final Method setQueued;
		private final Method dispatchabilityLost;
		private final Method setCompetition;
		private final Method setPreemption;
		private final Method tryStart;
		private final Method finish;
		private final Method abort;
		private final Method allowance;
		private final Method signalPending;
		private final Method fairTurn;
		private long activePermit;
		private PressureControllerExhaustiveModel.Pool activePool;

		private Subject(Object controller, Class<?> type) throws ReflectiveOperationException {
			this.controller = controller;
			this.type = type;
			setPressured = accessible(type.getDeclaredMethod("setPressured", boolean.class));
			setQueued = accessible(type.getDeclaredMethod(
					"setBatchQueued", RWScheduler.Pool.class, boolean.class));
			dispatchabilityLost = accessible(type.getDeclaredMethod(
					"batchDispatchabilityLost", RWScheduler.Pool.class));
			setCompetition = accessible(type.getDeclaredMethod(
					"setPoolCompetition", RWScheduler.Pool.class, boolean.class));
			setPreemption = accessible(type.getDeclaredMethod(
					"setPoolPreemption", RWScheduler.Pool.class, boolean.class));
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
			var setSources = accessible(type.getDeclaredMethod("setBatchDispatchabilitySources",
					BooleanSupplier.class, BooleanSupplier.class));
			invoke(setSources, (BooleanSupplier) readDispatchable::get,
					(BooleanSupplier) writeDispatchable::get);
			invoke(accessible(type.getDeclaredMethod("setNotifier", Runnable.class)),
					(Runnable) notifierCalls::incrementAndGet);
			invoke(accessible(type.getDeclaredMethod("setBatchNotifier", IntConsumer.class)),
					(IntConsumer) mask -> batchWakeMask.getAndAccumulate(mask, (left, right) -> left | right));
		}

		static Subject create() {
			try {
				var type = Class.forName("it.cavallium.rockserver.core.impl.WorkloadPressureController");
				var constructor = type.getDeclaredConstructor(int.class,
						int.class,
						Duration.class,
						int.class,
						Duration.class,
						Duration.class);
				constructor.setAccessible(true);
				return new Subject(constructor.newInstance(1,
						1,
						Duration.ofDays(1),
						1,
						Duration.ofDays(1),
						Duration.ofDays(1)), type);
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("unable to create pressure controller", failure);
			}
		}

		Subject copy() {
			var copy = create();
			try {
				for (String name : COPIED_FIELDS) {
					Field source = field(type, name);
					Field target = field(copy.type, name);
					target.set(copy.controller, source.get(controller));
				}
				copy.readDispatchable.set(readDispatchable.get());
				copy.writeDispatchable.set(writeDispatchable.get());
				copy.activePermit = activePermit;
				copy.activePool = activePool;
				return copy;
			} catch (ReflectiveOperationException failure) {
				throw new AssertionError("unable to clone pressure controller state", failure);
			}
		}

		PressureControllerExhaustiveModel.ActionResult apply(
				PressureControllerExhaustiveModel.Action action) {
			notifierCalls.set(0);
			batchWakeMask.set(0);
			boolean started = false;
			int readAllowance = -1;
			int writeAllowance = -1;
			switch (action.kind()) {
				case PRESSURE -> invoke(setPressured, action.value());
				case QUEUED -> invoke(setQueued, actual(action.pool()), action.value());
				case DISPATCHABLE -> {
					dispatchable(action.pool()).set(action.value());
					if (!action.value()) invoke(dispatchabilityLost, actual(action.pool()));
				}
				case COMPETITION -> invoke(setCompetition, actual(action.pool()), action.value());
				case PREEMPTION -> invoke(setPreemption, actual(action.pool()), action.value());
				case START -> {
					long permit = (long) invoke(tryStart,
							false,
							actual(action.pool()),
							action.time() == PressureControllerExhaustiveModel.Time.EARLY
									? PressureControllerExhaustiveModel.EARLY_TIME
									: PressureControllerExhaustiveModel.LATE_TIME);
					started = permit != NO_PERMIT;
					if (started) {
						if (activePermit != NO_PERMIT) throw new AssertionError("subject permit overflow");
						activePermit = permit;
						activePool = action.pool();
					}
				}
				case FINISH -> {
					invoke(finish, activePermit, actual(activePool));
					activePermit = NO_PERMIT;
					activePool = null;
				}
				case CANCEL -> {
					invoke(abort, activePermit, actual(activePool));
					activePermit = NO_PERMIT;
					activePool = null;
				}
				case EXPIRE -> {
					readAllowance = normalizeAllowance((int) invoke(allowance,
							false, RWScheduler.Pool.READ, PressureControllerExhaustiveModel.LATE_TIME));
					writeAllowance = normalizeAllowance((int) invoke(allowance,
							false, RWScheduler.Pool.WRITE, PressureControllerExhaustiveModel.LATE_TIME));
				}
				case FLUSH_NOTIFICATIONS -> invoke(signalPending);
			}
			return new PressureControllerExhaustiveModel.ActionResult(started,
					readAllowance,
					writeAllowance,
					notifierCalls.get(),
					batchWakeMask.get());
		}

		PressureControllerExhaustiveModel.Snapshot snapshot(int pressureTransitions,
				int competitionChanges) {
			int competitionMask = intField("competitionPoolMask");
			long competitionUntil = longField("competitionUntilNanos");
			var phase = competitionMask != 0
					? PressureControllerExhaustiveModel.CompetitionPhase.ACTIVE
					: competitionUntil == Long.MIN_VALUE
							? PressureControllerExhaustiveModel.CompetitionPhase.NONE
							: PressureControllerExhaustiveModel.CompetitionPhase.HOLDING;
			int preemptionMask = intField("preemptionPoolMask");
			int queuedMask = intField("queuedBatchPoolMask");
			int lastBit = intField("lastCompletedPressuredBatchPoolBit");
			return new PressureControllerExhaustiveModel.Snapshot(
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
					(preemptionMask & 1) != 0,
					(preemptionMask & 2) != 0,
					booleanField("preemptionRequested"),
					activePool,
					activePermit != NO_PERMIT && (activePermit & PRESSURED_FLAG) != 0,
					activePermit != NO_PERMIT && (activePermit & COMPETING_FLAG) != 0,
					activePermit == NO_PERMIT ? -1 : (int) (activePermit >>> PRESSURE_SHIFT) & GENERATION_MASK,
					activePermit == NO_PERMIT ? -1 : (int) (activePermit >>> COMPETITION_SHIFT) & GENERATION_MASK,
					lastBit == 1 ? PressureControllerExhaustiveModel.Pool.READ
							: lastBit == 2 ? PressureControllerExhaustiveModel.Pool.WRITE : null,
					longField("nextBatchNanos") != Long.MIN_VALUE,
					longField("nextCompetingWriteNanos") != Long.MIN_VALUE,
					booleanField("notificationPending"),
					(boolean) invoke(fairTurn, RWScheduler.Pool.READ),
					(boolean) invoke(fairTurn, RWScheduler.Pool.WRITE));
		}

		void validateConservation() {
			int active = intField("activeBatches");
			int read = intField("activeReadBatches");
			int write = intField("activeWriteBatches");
			if (active != read + write || active < 0 || read < 0 || write < 0) {
				throw new AssertionError("subject permit counters are not conserved");
			}
			if (active != (activePermit == NO_PERMIT ? 0 : 1)) {
				throw new AssertionError("subject permit token disagrees with active counters");
			}
			if (activePermit != NO_PERMIT
					&& (activePool == PressureControllerExhaustiveModel.Pool.READ ? read : write) != 1) {
				throw new AssertionError("subject permit belongs to the wrong pool");
			}
		}

		static Set<String> mutableStateFields() {
			try {
				var type = Class.forName("it.cavallium.rockserver.core.impl.WorkloadPressureController");
				var result = new HashSet<String>();
				for (Field field : type.getDeclaredFields()) {
					int modifiers = field.getModifiers();
					if (Modifier.isStatic(modifiers) || Modifier.isFinal(modifiers)
							|| EXTERNAL_OR_CALLBACK_FIELDS.contains(field.getName())) continue;
					result.add(field.getName());
				}
				return Set.copyOf(result);
			} catch (ClassNotFoundException failure) {
				throw new AssertionError(failure);
			}
		}

		private AtomicBoolean dispatchable(PressureControllerExhaustiveModel.Pool pool) {
			return pool == PressureControllerExhaustiveModel.Pool.READ
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

		private static int normalizeAllowance(int allowance) {
			return allowance > 0 ? 1 : 0;
		}

		private static RWScheduler.Pool actual(PressureControllerExhaustiveModel.Pool pool) {
			return pool == PressureControllerExhaustiveModel.Pool.READ
					? RWScheduler.Pool.READ : RWScheduler.Pool.WRITE;
		}
	}
}
