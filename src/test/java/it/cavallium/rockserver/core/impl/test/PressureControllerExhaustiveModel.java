package it.cavallium.rockserver.core.impl.test;

import java.util.ArrayList;
import java.util.List;

/**
 * Independent finite-state specification for the two-pool pressure controller. It models semantic
 * pacing phases rather than implementation timestamps and deliberately uses plain fields and
 * branch logic instead of the production controller's masks and permit encoding.
 */
final class PressureControllerExhaustiveModel {

	static final int MAX_PRESSURE_TRANSITIONS = 3;
	static final int MAX_COMPETITION_CHANGES = 4;
	static final long EARLY_TIME = 0L;
	static final long LATE_TIME = Long.MAX_VALUE;

	enum Pool {
		READ,
		WRITE;

		Pool other() {
			return this == READ ? WRITE : READ;
		}
	}

	enum Time {
		EARLY,
		LATE
	}

	enum CompetitionPhase {
		NONE,
		ACTIVE,
		HOLDING
	}

	enum Kind {
		PRESSURE,
		QUEUED,
		DISPATCHABLE,
		COMPETITION,
		PREEMPTION,
		START,
		FINISH,
		CANCEL,
		EXPIRE,
		FLUSH_NOTIFICATIONS
	}

	record Action(Kind kind, Pool pool, boolean value, Time time) {

		static Action pressure(boolean value) {
			return new Action(Kind.PRESSURE, null, value, null);
		}

		static Action set(Kind kind, Pool pool, boolean value) {
			return new Action(kind, pool, value, null);
		}

		static Action start(Pool pool, Time time) {
			return new Action(Kind.START, pool, false, time);
		}

		static Action simple(Kind kind) {
			return new Action(kind, null, false, null);
		}

		@Override
		public String toString() {
			return switch (kind) {
				case PRESSURE -> "pressure(" + value + ')';
				case QUEUED, DISPATCHABLE, COMPETITION, PREEMPTION ->
						kind.name().toLowerCase(java.util.Locale.ROOT) + '(' + pool + ',' + value + ')';
				case START -> "start(" + pool + ',' + time + ')';
				case FINISH, CANCEL, EXPIRE, FLUSH_NOTIFICATIONS ->
						kind.name().toLowerCase(java.util.Locale.ROOT);
			};
		}
	}

	record ActionResult(boolean startSucceeded,
			int readAllowance,
			int writeAllowance,
			int notifierCalls,
			int batchWakeMask) {

		static ActionResult empty() {
			return new ActionResult(false, -1, -1, 0, 0);
		}
	}

	record Permit(Pool pool,
			boolean pressured,
			boolean competing,
			int pressureGeneration,
			int competitionGeneration) {
	}

	record Snapshot(boolean pressured,
			int pressureGeneration,
			int competitionGeneration,
			int pressureTransitions,
			int competitionChanges,
			boolean readQueued,
			boolean writeQueued,
			boolean readDispatchable,
			boolean writeDispatchable,
			boolean readCompeting,
			boolean writeCompeting,
			CompetitionPhase competitionPhase,
			boolean readPreempting,
			boolean writePreempting,
			boolean preemptionRequested,
			Pool activePool,
			boolean activePermitPressured,
			boolean activePermitCompeting,
			int activePermitPressureGeneration,
			int activePermitCompetitionGeneration,
			Pool lastCompleted,
			boolean pressurePacing,
			boolean competingWritePacing,
			boolean notificationPending,
			boolean readFairTurn,
			boolean writeFairTurn) {
	}

	private boolean pressured;
	private int pressureGeneration;
	private int competitionGeneration;
	private int pressureTransitions;
	private int competitionChanges;
	private boolean readQueued;
	private boolean writeQueued;
	private boolean readDispatchable;
	private boolean writeDispatchable;
	private boolean readCompeting;
	private boolean writeCompeting;
	private CompetitionPhase competitionPhase = CompetitionPhase.NONE;
	private boolean readPreempting;
	private boolean writePreempting;
	private Permit activePermit;
	private Pool lastCompleted;
	private boolean pressurePacing;
	private boolean competingWritePacing;
	private boolean notificationPending;

	PressureControllerExhaustiveModel copy() {
		var copy = new PressureControllerExhaustiveModel();
		copy.pressured = pressured;
		copy.pressureGeneration = pressureGeneration;
		copy.competitionGeneration = competitionGeneration;
		copy.pressureTransitions = pressureTransitions;
		copy.competitionChanges = competitionChanges;
		copy.readQueued = readQueued;
		copy.writeQueued = writeQueued;
		copy.readDispatchable = readDispatchable;
		copy.writeDispatchable = writeDispatchable;
		copy.readCompeting = readCompeting;
		copy.writeCompeting = writeCompeting;
		copy.competitionPhase = competitionPhase;
		copy.readPreempting = readPreempting;
		copy.writePreempting = writePreempting;
		copy.activePermit = activePermit;
		copy.lastCompleted = lastCompleted;
		copy.pressurePacing = pressurePacing;
		copy.competingWritePacing = competingWritePacing;
		copy.notificationPending = notificationPending;
		return copy;
	}

	boolean applicable(Action action) {
		return switch (action.kind()) {
			case PRESSURE -> action.value() == pressured || pressureTransitions < MAX_PRESSURE_TRANSITIONS;
			case COMPETITION -> action.value() == competing(action.pool())
					|| competitionChanges < MAX_COMPETITION_CHANGES;
			case START -> activePermit == null || pressured;
			case FINISH, CANCEL -> activePermit != null;
			case QUEUED, DISPATCHABLE, PREEMPTION, EXPIRE, FLUSH_NOTIFICATIONS -> true;
		};
	}

	ActionResult apply(Action action) {
		if (!applicable(action)) throw new IllegalArgumentException("inapplicable action " + action);
		int notifierCalls = 0;
		int batchWakeMask = 0;
		boolean started = false;
		int readAllowance = -1;
		int writeAllowance = -1;
		switch (action.kind()) {
			case PRESSURE -> {
				if (pressured != action.value()) {
					pressureTransitions++;
					pressureGeneration++;
				}
				pressured = action.value();
				if (!pressured) {
					pressurePacing = false;
					lastCompleted = null;
				}
				notifierCalls = 1;
			}
			case QUEUED -> {
				Pool pool = action.pool();
				boolean old = queued(pool);
				boolean releasedFairnessWait = !action.value()
						&& pressured
						&& lastCompleted != null
						&& pool != lastCompleted
						&& old
						&& queued(lastCompleted);
				queued(pool, action.value());
				if (releasedFairnessWait) notificationPending = true;
			}
			case DISPATCHABLE -> {
				dispatchable(action.pool(), action.value());
				if (!action.value()
						&& pressured
						&& lastCompleted != null
						&& action.pool() != lastCompleted
						&& dispatchable(lastCompleted)) {
					notificationPending = true;
				}
			}
			case COMPETITION -> {
				boolean oldValue = competing(action.pool());
				boolean wasCompeting = readCompeting || writeCompeting;
				competing(action.pool(), action.value());
				if (oldValue != action.value()) {
					competitionChanges++;
					if (pressured && lastCompleted != null && dispatchable(lastCompleted)) {
						notificationPending = true;
					}
				}
				if (readCompeting || writeCompeting) {
					competitionPhase = CompetitionPhase.ACTIVE;
				} else if (wasCompeting) {
					competitionPhase = CompetitionPhase.HOLDING;
					notificationPending = true;
				}
			}
			case PREEMPTION -> preempting(action.pool(), action.value());
			case START -> {
				if (action.time() == Time.LATE) expireCompetition();
				if (canStart(action.pool(), action.time())) {
					activePermit = new Permit(action.pool(), pressured,
							competitionPhase != CompetitionPhase.NONE,
							pressureGeneration,
							competitionGeneration);
					started = true;
				}
			}
			case FINISH -> {
				Permit permit = activePermit;
				activePermit = null;
				batchWakeMask = queued(permit.pool().other()) ? poolBit(permit.pool().other()) : 0;
				if (permit.pool() == Pool.WRITE
						&& permit.competing()
						&& permit.competitionGeneration() == competitionGeneration
						&& competitionPhase != CompetitionPhase.NONE) {
					competingWritePacing = true;
				}
				if (permit.pressured()
						&& pressured
						&& permit.pressureGeneration() == pressureGeneration) {
					lastCompleted = permit.pool();
					pressurePacing = true;
				}
			}
			case CANCEL -> {
				Pool cancelled = activePermit.pool();
				activePermit = null;
				if (queued(cancelled.other())) notificationPending = true;
			}
			case EXPIRE -> {
				expireCompetition();
				readAllowance = allowance(Pool.READ, Time.LATE);
				writeAllowance = allowance(Pool.WRITE, Time.LATE);
			}
			case FLUSH_NOTIFICATIONS -> {
				if (notificationPending) {
					notificationPending = false;
					notifierCalls = 1;
				}
			}
		}
		validate();
		return new ActionResult(started, readAllowance, writeAllowance, notifierCalls, batchWakeMask);
	}

	Snapshot snapshot() {
		return new Snapshot(pressured,
				pressureGeneration,
				competitionGeneration,
				pressureTransitions,
				competitionChanges,
				readQueued,
				writeQueued,
				readDispatchable,
				writeDispatchable,
				readCompeting,
				writeCompeting,
				competitionPhase,
				readPreempting,
				writePreempting,
				readPreempting || writePreempting,
				activePermit == null ? null : activePermit.pool(),
				activePermit != null && activePermit.pressured(),
				activePermit != null && activePermit.competing(),
				activePermit == null ? -1 : activePermit.pressured() ? activePermit.pressureGeneration() : 0,
				activePermit == null ? -1 : activePermit.competing() ? activePermit.competitionGeneration() : 0,
				lastCompleted,
				pressurePacing,
				competingWritePacing,
				notificationPending,
				fairTurn(Pool.READ, Time.EARLY),
				fairTurn(Pool.WRITE, Time.EARLY));
	}

	void validate() {
		if (pressureTransitions < 0 || pressureTransitions > MAX_PRESSURE_TRANSITIONS) {
			throw new AssertionError("pressure transition bound violated");
		}
		if (competitionChanges < 0 || competitionChanges > MAX_COMPETITION_CHANGES) {
			throw new AssertionError("competition transition bound violated");
		}
		if ((readCompeting || writeCompeting) != (competitionPhase == CompetitionPhase.ACTIVE)) {
			throw new AssertionError("competition phase disagrees with competitors");
		}
		if (activePermit != null && pressured && activeCount() > 1) {
			throw new AssertionError("pressure cap violated");
		}
		if (activeCount() != (activePermit == null ? 0 : 1)) {
			throw new AssertionError("permit conservation violated");
		}
		var late = copy();
		late.expireCompetition();
		if (late.activePermit == null && (late.readDispatchable || late.writeDispatchable)
				&& !late.canStart(Pool.READ, Time.LATE)
				&& !late.canStart(Pool.WRITE, Time.LATE)) {
			throw new AssertionError("dispatchable work reached a dead state");
		}
		if (late.activePermit == null && late.pressured) {
			if (late.readDispatchable && !late.writeDispatchable
					&& !late.canStart(Pool.READ, Time.LATE)) {
				throw new AssertionError("nondispatchable WRITE peer blocked READ");
			}
			if (late.writeDispatchable && !late.readDispatchable
					&& !late.canStart(Pool.WRITE, Time.LATE)) {
				throw new AssertionError("nondispatchable READ peer blocked WRITE");
			}
			if (late.readDispatchable && late.writeDispatchable && late.lastCompleted != null) {
				if (late.canStart(late.lastCompleted, Time.LATE)
						|| !late.canStart(late.lastCompleted.other(), Time.LATE)) {
					throw new AssertionError("bounded cross-pool alternation violated");
				}
			}
		}
	}

	int activeCount() {
		return activePermit == null ? 0 : 1;
	}

	Permit activePermit() {
		return activePermit;
	}

	static List<Action> actions() {
		var actions = new ArrayList<Action>();
		actions.add(Action.pressure(false));
		actions.add(Action.pressure(true));
		for (var pool : Pool.values()) {
			for (boolean value : new boolean[] {false, true}) {
				actions.add(Action.set(Kind.QUEUED, pool, value));
				actions.add(Action.set(Kind.DISPATCHABLE, pool, value));
				actions.add(Action.set(Kind.COMPETITION, pool, value));
				actions.add(Action.set(Kind.PREEMPTION, pool, value));
			}
			for (var time : Time.values()) actions.add(Action.start(pool, time));
		}
		actions.add(Action.simple(Kind.FINISH));
		actions.add(Action.simple(Kind.CANCEL));
		actions.add(Action.simple(Kind.EXPIRE));
		actions.add(Action.simple(Kind.FLUSH_NOTIFICATIONS));
		return List.copyOf(actions);
	}

	private boolean canStart(Pool pool, Time time) {
		if (activePermit != null && pressured) return false;
		if (competitionPhase != CompetitionPhase.NONE) {
			if (activePermit != null && activePermit.pool() == pool) return false;
			if (pool == Pool.WRITE && competingWritePacing && time == Time.EARLY) return false;
		}
		if (!pressured) return activePermit == null;
		return activePermit == null
				&& (time == Time.LATE || !pressurePacing)
				&& fairTurn(pool, time);
	}

	private int allowance(Pool pool, Time time) {
		if (competitionPhase != CompetitionPhase.NONE) {
			if (activePermit != null && activePermit.pool() == pool) return 0;
			if (pool == Pool.WRITE && competingWritePacing && time == Time.EARLY) return 0;
		}
		if (!pressured) return 1;
		return canStart(pool, time) ? 1 : 0;
	}

	private boolean fairTurn(Pool pool, Time time) {
		if (!pressured || lastCompleted == null || lastCompleted != pool
				|| !dispatchable(pool.other())) {
			return true;
		}
		return activePermit == null
				&& pool.other() == Pool.WRITE
				&& competingWritePacing
				&& time == Time.EARLY;
	}

	private void expireCompetition() {
		if (competitionPhase == CompetitionPhase.HOLDING) {
			competitionPhase = CompetitionPhase.NONE;
			competingWritePacing = false;
			competitionGeneration++;
		}
	}

	private boolean queued(Pool pool) {
		return pool == Pool.READ ? readQueued : writeQueued;
	}

	private void queued(Pool pool, boolean value) {
		if (pool == Pool.READ) readQueued = value;
		else writeQueued = value;
	}

	private boolean dispatchable(Pool pool) {
		return pool == Pool.READ ? readDispatchable : writeDispatchable;
	}

	private void dispatchable(Pool pool, boolean value) {
		if (pool == Pool.READ) readDispatchable = value;
		else writeDispatchable = value;
	}

	private boolean competing(Pool pool) {
		return pool == Pool.READ ? readCompeting : writeCompeting;
	}

	private void competing(Pool pool, boolean value) {
		if (pool == Pool.READ) readCompeting = value;
		else writeCompeting = value;
	}

	private void preempting(Pool pool, boolean value) {
		if (pool == Pool.READ) readPreempting = value;
		else writePreempting = value;
	}

	private static int poolBit(Pool pool) {
		return pool == Pool.READ ? 1 : 2;
	}
}
