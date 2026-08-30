package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;

/** Compact, replayable byte-action driver for the independent scheduler model. */
final class SchedulerStateTraceRunner {

	private static final WorkloadProfile[] DATA_PROFILES = {
			WorkloadProfile.LATENCY,
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH
	};
	private static final int MAX_ACTIONS = 1_024;

	private SchedulerStateTraceRunner() {
	}

	static void run(byte[] input) {
		byte[] actions = input.length <= MAX_ACTIONS
				? input
				: java.util.Arrays.copyOf(input, MAX_ACTIONS);
		var model = new SchedulerReferenceModel(settings());
		var gate = new SchedulerReferenceModel.BatchGate(2, 3, 1, 3L, 2L, 4L);
		var permits = new EnumMap<SchedulerReferenceModel.BatchGate.Pool,
				ArrayList<SchedulerReferenceModel.BatchGate.Permit>>(SchedulerReferenceModel.BatchGate.Pool.class);
		for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) permits.put(pool, new ArrayList<>());
		long now = 0L;
		long nextId = 1L;
		for (int actionIndex = 0; actionIndex < actions.length; actionIndex++) {
			int encoded = Byte.toUnsignedInt(actions[actionIndex]);
			int operation = encoded >>> 4;
			int argument = encoded & 0x0f;
			try {
				switch (operation) {
					case 0 -> {
						var profile = DATA_PROFILES[argument % DATA_PROFILES.length];
						long deadline = (argument & 8) != 0
								? now
								: (argument & 4) != 0 ? saturating(now, 1L + argument) : Long.MAX_VALUE;
						model.submit(new SchedulerReferenceModel.Spec(
								nextId++, profile, deadline, 1 + argument, (argument & 2) != 0), now);
					}
					case 1 -> model.dispatch(now, (argument & 1) == 0);
					case 2 -> select(model.ids(SchedulerReferenceModel.Phase.ACTIVE), argument)
							.ifPresent(model::complete);
					case 3 -> select(nonTerminalIds(model), argument).ifPresent(model::cancel);
					case 4 -> select(model.ids(SchedulerReferenceModel.Phase.ACTIVE), argument)
							.ifPresent(model::yield);
					case 5 -> select(model.ids(SchedulerReferenceModel.Phase.ACTIVE), argument)
							.ifPresent(model::park);
					case 6 -> select(model.ids(SchedulerReferenceModel.Phase.PARKED), argument)
							.ifPresent(model::resume);
					case 7 -> {
						now = saturating(now, 1L + argument);
						model.expire(now);
					}
					case 8 -> gate.pressure((argument & 1) != 0);
					case 9 -> {
						var pool = pool(argument);
						gate.queued(pool, (argument & 2) != 0);
					}
					case 10 -> gate.dispatchable(pool(argument), (argument & 2) != 0);
					case 11 -> {
						var pool = pool(argument);
						if (gate.isDispatchable(pool)) gate.start(pool, now).ifPresent(permits.get(pool)::add);
					}
					case 12 -> {
						var pool = pool(argument);
						if (!permits.get(pool).isEmpty()) gate.finish(pool, permits.get(pool).removeFirst(), now);
					}
					case 13 -> gate.competitor(pool(argument), (argument & 2) != 0, now);
					case 14 -> model.gracefulShutdown();
					case 15 -> model.forceShutdown();
					default -> throw new AssertionError("unreachable action " + operation);
				}
				model.validate();
				gate.validate();
			} catch (Throwable failure) {
				throw new AssertionError("scheduler model trace failed; actionIndex=" + actionIndex
						+ ", action=0x" + HexFormat.of().toHexDigits(actions[actionIndex])
						+ ", trace=" + HexFormat.of().formatHex(actions), failure);
			}
		}

		model.forceShutdown();
		for (long id : List.copyOf(model.ids(SchedulerReferenceModel.Phase.ACTIVE))) model.complete(id);
		if (!model.drainedAndConserved()) {
			throw new AssertionError("scheduler model did not drain; trace=" + HexFormat.of().formatHex(actions));
		}
		for (var pool : SchedulerReferenceModel.BatchGate.Pool.values()) {
			for (var permit : List.copyOf(permits.get(pool))) gate.finish(pool, permit, now);
			gate.competitor(pool, false, now);
		}
		gate.validate();
		if (gate.activeTotal() != 0) {
			throw new AssertionError("pressure permits did not drain; trace=" + HexFormat.of().formatHex(actions));
		}
	}

	private static SchedulerReferenceModel.Settings settings() {
		var capacities = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		var reservations = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		var quanta = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : WorkloadProfile.values()) capacities.put(profile, DATA_PROFILES_LIST.contains(profile) ? 8 : 0);
		reservations.put(WorkloadProfile.LATENCY, 1);
		reservations.put(WorkloadProfile.INGEST, 1);
		reservations.put(WorkloadProfile.CDC, 1);
		quanta.put(WorkloadProfile.INGEST, 4);
		quanta.put(WorkloadProfile.CDC, 4);
		quanta.put(WorkloadProfile.ANALYTICAL, 2);
		quanta.put(WorkloadProfile.BATCH, 1);
		return new SchedulerReferenceModel.Settings(3, 1, 8, capacities, reservations, quanta);
	}

	private static final List<WorkloadProfile> DATA_PROFILES_LIST = List.of(DATA_PROFILES);

	private static List<Long> nonTerminalIds(SchedulerReferenceModel model) {
		var ids = new ArrayList<Long>();
		ids.addAll(model.ids(SchedulerReferenceModel.Phase.QUEUED));
		ids.addAll(model.ids(SchedulerReferenceModel.Phase.ACTIVE));
		ids.addAll(model.ids(SchedulerReferenceModel.Phase.PARKED));
		return ids;
	}

	private static java.util.Optional<Long> select(List<Long> ids, int selector) {
		return ids.isEmpty()
				? java.util.Optional.empty()
				: java.util.Optional.of(ids.get(Math.floorMod(selector, ids.size())));
	}

	private static SchedulerReferenceModel.BatchGate.Pool pool(int argument) {
		return (argument & 1) == 0
				? SchedulerReferenceModel.BatchGate.Pool.READ
				: SchedulerReferenceModel.BatchGate.Pool.WRITE;
	}

	private static long saturating(long value, long increment) {
		return increment >= Long.MAX_VALUE - value ? Long.MAX_VALUE : value + increment;
	}
}
