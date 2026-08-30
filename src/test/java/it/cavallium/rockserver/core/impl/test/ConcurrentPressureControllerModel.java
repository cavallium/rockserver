package it.cavallium.rockserver.core.impl.test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Independent bounded specification for concurrent pressure permits. Permits are represented as
 * an unordered multiset of semantic episode labels; this is a symmetry quotient over identities,
 * not over lifecycle orderings. Every distinct permit class can still finish or cancel next.
 */
final class ConcurrentPressureControllerModel {

	static final int MAX_PRESSURE_TRANSITIONS = 3;
	static final int MAX_COMPETITION_CHANGES = 4;
	static final long EARLY_TIME = 0L;
	static final long LATE_TIME = Long.MAX_VALUE;

	private static final Comparator<Permit> PERMIT_ORDER = Comparator
			.comparing(Permit::pool)
			.thenComparing(Permit::pressured)
			.thenComparing(Permit::competing)
			.thenComparingInt(Permit::pressureGeneration)
			.thenComparingInt(Permit::competitionGeneration);

	record Limits(int pressureCap, int competingReadCap, int competingWriteCap) {

		Limits {
			if (pressureCap < 1 || competingReadCap < 1 || competingWriteCap < 1) {
				throw new IllegalArgumentException("all permit limits must be positive");
			}
		}
	}

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
		START,
		FINISH,
		CANCEL,
		EXPIRE,
		FLUSH_NOTIFICATIONS
	}

	record Permit(Pool pool,
			boolean pressured,
			boolean competing,
			int pressureGeneration,
			int competitionGeneration) {
	}

	record Action(Kind kind, Pool pool, boolean value, Time time, Permit permit) {

		static Action pressure(boolean value) {
			return new Action(Kind.PRESSURE, null, value, null, null);
		}

		static Action set(Kind kind, Pool pool, boolean value) {
			return new Action(kind, pool, value, null, null);
		}

		static Action start(Pool pool, Time time) {
			return new Action(Kind.START, pool, false, time, null);
		}

		static Action permit(Kind kind, Permit permit) {
			return new Action(kind, permit.pool(), false, null, permit);
		}

		static Action simple(Kind kind) {
			return new Action(kind, null, false, null, null);
		}

		@Override
		public String toString() {
			return switch (kind) {
				case PRESSURE -> "pressure(" + value + ')';
				case QUEUED, DISPATCHABLE, COMPETITION ->
						kind.name().toLowerCase(java.util.Locale.ROOT) + '(' + pool + ',' + value + ')';
				case START -> "start(" + pool + ',' + time + ')';
				case FINISH, CANCEL -> kind.name().toLowerCase(java.util.Locale.ROOT) + '(' + permit + ')';
				case EXPIRE, FLUSH_NOTIFICATIONS -> kind.name().toLowerCase(java.util.Locale.ROOT);
			};
		}
	}

	record ActionResult(boolean startSucceeded, int notifierCalls, int batchWakeMask) {
	}

	record Eligibility(int readEarly, int writeEarly, int readLate, int writeLate) {
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
			List<Permit> permits,
			Pool lastCompleted,
			boolean pressurePacing,
			boolean competingWritePacing,
			boolean notificationPending,
			boolean readFairTurn,
			boolean writeFairTurn) {
	}

	private final Limits limits;
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
	private final ArrayList<Permit> permits = new ArrayList<>();
	private Pool lastCompleted;
	private boolean pressurePacing;
	private boolean competingWritePacing;
	private boolean notificationPending;

	ConcurrentPressureControllerModel(Limits limits) {
		this.limits = limits;
	}

	ConcurrentPressureControllerModel copy() {
		var copy = new ConcurrentPressureControllerModel(limits);
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
		copy.permits.addAll(permits);
		copy.lastCompleted = lastCompleted;
		copy.pressurePacing = pressurePacing;
		copy.competingWritePacing = competingWritePacing;
		copy.notificationPending = notificationPending;
		return copy;
	}

	List<Action> actions() {
		var actions = new ArrayList<Action>();
		actions.add(Action.pressure(false));
		actions.add(Action.pressure(true));
		for (var pool : Pool.values()) {
			for (boolean value : new boolean[] {false, true}) {
				actions.add(Action.set(Kind.QUEUED, pool, value));
				actions.add(Action.set(Kind.DISPATCHABLE, pool, value));
				actions.add(Action.set(Kind.COMPETITION, pool, value));
			}
			for (var time : Time.values()) {
				actions.add(Action.start(pool, time));
			}
		}
		for (var permit : new LinkedHashSet<>(canonicalPermits())) {
			actions.add(Action.permit(Kind.FINISH, permit));
			actions.add(Action.permit(Kind.CANCEL, permit));
		}
		actions.add(Action.simple(Kind.EXPIRE));
		actions.add(Action.simple(Kind.FLUSH_NOTIFICATIONS));
		return actions;
	}

	boolean applicable(Action action) {
		return switch (action.kind()) {
			case PRESSURE -> action.value() == pressured
					|| pressureTransitions < MAX_PRESSURE_TRANSITIONS;
			case COMPETITION -> action.value() == competing(action.pool())
					|| competitionChanges < MAX_COMPETITION_CHANGES;
			case START -> permits.size() < limits.pressureCap();
			case FINISH, CANCEL -> permits.contains(action.permit());
			case QUEUED, DISPATCHABLE, EXPIRE, FLUSH_NOTIFICATIONS -> true;
		};
	}

	ActionResult apply(Action action) {
		if (!applicable(action)) {
			throw new IllegalArgumentException("inapplicable action " + action);
		}
		int notifierCalls = 0;
		int batchWakeMask = 0;
		boolean started = false;
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
				var pool = action.pool();
				boolean old = queued(pool);
				boolean released = !action.value()
						&& pressured
						&& lastCompleted != null
						&& pool != lastCompleted
						&& old
						&& queued(lastCompleted);
				queued(pool, action.value());
				if (released) {
					notificationPending = true;
				}
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
				boolean old = competing(action.pool());
				boolean wasActive = readCompeting || writeCompeting;
				competing(action.pool(), action.value());
				if (old != action.value()) {
					competitionChanges++;
					if (pressured && lastCompleted != null && dispatchable(lastCompleted)) {
						notificationPending = true;
					}
				}
				if (readCompeting || writeCompeting) {
					competitionPhase = CompetitionPhase.ACTIVE;
				} else if (wasActive) {
					competitionPhase = CompetitionPhase.HOLDING;
					notificationPending = true;
				}
			}
			case START -> {
				if (allowance(action.pool(), action.time()) > 0) {
					permits.add(new Permit(action.pool(),
							pressured,
							competitionPhase != CompetitionPhase.NONE,
							pressured ? pressureGeneration : 0,
							competitionPhase != CompetitionPhase.NONE ? competitionGeneration : 0));
					started = true;
				}
			}
			case FINISH -> {
				var permit = action.permit();
				remove(permit);
				if (queued(permit.pool().other())) {
					batchWakeMask = poolBit(permit.pool().other());
				}
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
				var permit = action.permit();
				remove(permit);
				if (queued(permit.pool().other())) {
					notificationPending = true;
				}
			}
			case EXPIRE -> expireCompetition();
			case FLUSH_NOTIFICATIONS -> {
				if (notificationPending) {
					notificationPending = false;
					notifierCalls = 1;
				}
			}
		}
		validate();
		return new ActionResult(started, notifierCalls, batchWakeMask);
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
				canonicalPermits(),
				lastCompleted,
				pressurePacing,
				competingWritePacing,
				notificationPending,
				fairTurn(Pool.READ, Time.EARLY),
				fairTurn(Pool.WRITE, Time.EARLY));
	}

	Eligibility eligibility() {
		var early = copy();
		int readEarly = early.allowance(Pool.READ, Time.EARLY);
		int writeEarly = early.allowance(Pool.WRITE, Time.EARLY);
		var late = copy();
		int readLate = late.allowance(Pool.READ, Time.LATE);
		int writeLate = late.allowance(Pool.WRITE, Time.LATE);
		return new Eligibility(readEarly, writeEarly, readLate, writeLate);
	}

	void validate() {
		if (pressureTransitions < 0 || pressureTransitions > MAX_PRESSURE_TRANSITIONS) {
			throw new AssertionError("pressure transition bound violated");
		}
		if (competitionChanges < 0 || competitionChanges > MAX_COMPETITION_CHANGES) {
			throw new AssertionError("competition transition bound violated");
		}
		if ((readCompeting || writeCompeting) != (competitionPhase == CompetitionPhase.ACTIVE)) {
			throw new AssertionError("competition phase disagrees with publishers");
		}
		if (permits.size() > limits.pressureCap()) {
			throw new AssertionError("bounded permit multiset overflow");
		}
		if (pressured && permits.size() > limits.pressureCap()) {
			throw new AssertionError("global pressure cap violated");
		}

		var late = copy();
		late.expireCompetition();
		int available = limits.pressureCap() - late.permits.size();
		if (late.pressured && available > 0) {
			boolean readDemand = late.readDispatchable
					&& late.competitionAllowsStart(Pool.READ, Time.LATE);
			boolean writeDemand = late.writeDispatchable
					&& late.competitionAllowsStart(Pool.WRITE, Time.LATE);
			if ((readDemand || writeDemand)
					&& !(readDemand && late.allowance(Pool.READ, Time.LATE) > 0)
					&& !(writeDemand && late.allowance(Pool.WRITE, Time.LATE) > 0)) {
				throw new AssertionError("controller-reachable demand reached a dead state");
			}
			if (available > 1 && late.lastCompleted != null) {
				if (late.dispatchable(late.lastCompleted)
						&& late.competitionAllowsStart(late.lastCompleted, Time.LATE)
						&& late.allowance(late.lastCompleted, Time.LATE) == 0) {
					throw new AssertionError("fairness serialized capacity before the reserved final slot");
				}
			}
			if (available == 1 && late.lastCompleted != null) {
				var peer = late.lastCompleted.other();
				boolean peerCanConsume = late.dispatchable(peer)
						&& late.competitionAllowsStart(peer, Time.LATE);
				if ((peerCanConsume
						&& late.allowance(late.lastCompleted, Time.LATE) != 0)
						|| (!peerCanConsume
						&& late.dispatchable(late.lastCompleted)
						&& late.competitionAllowsStart(late.lastCompleted, Time.LATE)
						&& late.allowance(late.lastCompleted, Time.LATE) == 0)) {
					throw new AssertionError("final-slot peer reservation violated");
				}
			}
		}
	}

	private int allowance(Pool pool, Time time) {
		if (time == Time.LATE) {
			expireCompetition();
		}
		int allowance = Integer.MAX_VALUE;
		boolean competing = competitionPhase != CompetitionPhase.NONE;
		if (competing) {
			allowance = Math.max(0, competingCap(pool) - active(pool));
			if (pool == Pool.WRITE && competingWritePacing && time == Time.EARLY) {
				return 0;
			}
		}
		if (pressured) {
			if (pressurePacing && time == Time.EARLY) {
				return 0;
			}
			if (!fairTurn(pool, time)) {
				return 0;
			}
			allowance = Math.min(allowance,
					Math.max(0, limits.pressureCap() - permits.size()));
		}
		return allowance;
	}

	private boolean fairTurn(Pool pool, Time time) {
		if (!pressured || lastCompleted == null || lastCompleted != pool) {
			return true;
		}
		int available = limits.pressureCap() - permits.size();
		if (available > 1) {
			return true;
		}
		var peer = pool.other();
		if (!dispatchable(peer)) {
			return true;
		}
		return available == 1 && !competitionAllowsStart(peer, time);
	}

	private boolean competitionAllowsStart(Pool pool, Time time) {
		return competitionPhase == CompetitionPhase.NONE
				|| active(pool) < competingCap(pool)
				&& (pool != Pool.WRITE || !competingWritePacing || time == Time.LATE);
	}

	private void expireCompetition() {
		if (competitionPhase == CompetitionPhase.HOLDING) {
			competitionPhase = CompetitionPhase.NONE;
			competingWritePacing = false;
			competitionGeneration++;
		}
	}

	private List<Permit> canonicalPermits() {
		return permits.stream().sorted(PERMIT_ORDER).toList();
	}

	private void remove(Permit permit) {
		if (!permits.remove(permit)) {
			throw new AssertionError("missing active permit " + permit);
		}
	}

	private int active(Pool pool) {
		int count = 0;
		for (var permit : permits) {
			if (permit.pool() == pool) {
				count++;
			}
		}
		return count;
	}

	private int competingCap(Pool pool) {
		return pool == Pool.READ ? limits.competingReadCap() : limits.competingWriteCap();
	}

	private boolean queued(Pool pool) {
		return pool == Pool.READ ? readQueued : writeQueued;
	}

	private void queued(Pool pool, boolean value) {
		if (pool == Pool.READ) {
			readQueued = value;
		} else {
			writeQueued = value;
		}
	}

	private boolean dispatchable(Pool pool) {
		return pool == Pool.READ ? readDispatchable : writeDispatchable;
	}

	private void dispatchable(Pool pool, boolean value) {
		if (pool == Pool.READ) {
			readDispatchable = value;
		} else {
			writeDispatchable = value;
		}
	}

	private boolean competing(Pool pool) {
		return pool == Pool.READ ? readCompeting : writeCompeting;
	}

	private void competing(Pool pool, boolean value) {
		if (pool == Pool.READ) {
			readCompeting = value;
		} else {
			writeCompeting = value;
		}
	}

	private static int poolBit(Pool pool) {
		return pool == Pool.READ ? 1 : 2;
	}
}
