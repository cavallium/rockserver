package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Small executable specification for workload scheduling.
 *
 * <p>This intentionally shares no scheduler implementation types, queues, counters, or helper
 * algorithms. It favors obvious recomputation over hot-path data structures so generated tests
 * have an independent oracle for ordering, ownership, and conservation.</p>
 */
final class SchedulerReferenceModel {

	enum Phase {
		QUEUED,
		ACTIVE,
		PARKED,
		TERMINAL
	}

	enum Outcome {
		RUN,
		FAILURE,
		CANCELLATION,
		DEADLINE,
		OVERLOAD,
		SHUTDOWN
	}

	enum Admission {
		ACCEPTED,
		DEADLINE,
		OVERLOAD,
		SHUTDOWN
	}

	record Spec(long id,
	            WorkloadProfile profile,
	            long deadline,
	            int cost,
	            boolean failWhenRun) {

		Spec {
			Objects.requireNonNull(profile, "profile");
			if (cost < 1 || cost > 16) throw new IllegalArgumentException("cost must be in [1,16]");
		}
	}

	record Terminal(long id, Outcome outcome) {
	}

	record Settings(int workers,
	                int analyticalLimit,
	                int latencyBurst,
	                Map<WorkloadProfile, Integer> capacities,
	                Map<WorkloadProfile, Integer> reservations,
	                Map<WorkloadProfile, Integer> quanta) {

		Settings {
			if (workers < 1 || analyticalLimit < 1 || analyticalLimit > workers || latencyBurst < 1) {
				throw new IllegalArgumentException("invalid reference settings");
			}
			capacities = Map.copyOf(capacities);
			reservations = Map.copyOf(reservations);
			quanta = Map.copyOf(quanta);
		}
	}

	private static final WorkloadProfile[] GUARANTEED = {
			WorkloadProfile.INGEST,
			WorkloadProfile.CDC,
			WorkloadProfile.ANALYTICAL,
			WorkloadProfile.BATCH
	};
	private static final Comparator<Job> LATENCY_ORDER = Comparator
			.comparingLong((Job job) -> job.spec.deadline())
			.thenComparingLong(job -> job.initialSequence);

	private final Settings settings;
	private final EnumMap<WorkloadProfile, ArrayList<Job>> queues = new EnumMap<>(WorkloadProfile.class);
	private final LinkedHashMap<Long, Job> jobs = new LinkedHashMap<>();
	private final EnumMap<WorkloadProfile, Integer> active = zeroInts();
	private final EnumMap<WorkloadProfile, Integer> deficit = zeroInts();
	private final EnumMap<Outcome, Long> outcomes = zeroLongs();
	private final ArrayList<Terminal> terminalOrder = new ArrayList<>();
	private long nextSequence;
	private long attempts;
	private long accepted;
	private int guaranteedCursor;
	private boolean guaranteedNeedsQuantum = true;
	private int latencyBurst;
	private boolean shutdown;

	SchedulerReferenceModel(Settings settings) {
		this.settings = Objects.requireNonNull(settings, "settings");
		for (var profile : WorkloadProfile.values()) queues.put(profile, new ArrayList<>());
	}

	Admission submit(Spec spec, long now) {
		if (jobs.containsKey(spec.id())) throw new IllegalArgumentException("duplicate id " + spec.id());
		attempts++;
		if (spec.deadline() != Long.MAX_VALUE && now >= spec.deadline()) {
			recordImmediate(spec.id(), Outcome.DEADLINE);
			return Admission.DEADLINE;
		}
		if (shutdown) {
			recordImmediate(spec.id(), Outcome.SHUTDOWN);
			return Admission.SHUTDOWN;
		}
		int capacity = settings.capacities().getOrDefault(spec.profile(), 0);
		if (capacity == 0 || queued(spec.profile()) >= capacity || outstanding(spec.profile()) >= capacity + settings.workers()) {
			recordImmediate(spec.id(), Outcome.OVERLOAD);
			return Admission.OVERLOAD;
		}
		var job = new Job(spec, nextSequence++);
		jobs.put(spec.id(), job);
		queues.get(spec.profile()).add(job);
		accepted++;
		validate();
		return Admission.ACCEPTED;
	}

	void seedActive(long id, WorkloadProfile profile) {
		var spec = new Spec(id, profile, Long.MAX_VALUE, 1, false);
		var job = new Job(spec, nextSequence++);
		job.phase = Phase.ACTIVE;
		jobs.put(id, job);
		active.merge(profile, 1, Integer::sum);
		accepted++;
		attempts++;
		validate();
	}

	boolean cancel(long id) {
		var job = jobs.get(id);
		if (job == null || job.phase == Phase.TERMINAL || job.termination != null) return false;
		return switch (job.phase) {
			case QUEUED -> {
				queues.get(job.spec.profile()).remove(job);
				terminal(job, Outcome.CANCELLATION);
				yield true;
			}
			case PARKED -> {
				terminal(job, Outcome.CANCELLATION);
				yield true;
			}
			case ACTIVE -> {
				job.termination = Outcome.CANCELLATION;
				yield true;
			}
			case TERMINAL -> false;
		};
	}

	boolean cancelNonCooperative(long id) {
		var job = jobs.get(id);
		if (job != null && job.phase == Phase.ACTIVE) return false;
		return cancel(id);
	}

	Optional<Spec> dispatch(long now, boolean batchEligible) {
		expire(now);
		if (activeTotal() >= settings.workers()) return Optional.empty();
		Job selected = select(batchEligible);
		if (selected == null) return Optional.empty();
		queues.get(selected.spec.profile()).remove(selected);
		selected.phase = Phase.ACTIVE;
		active.merge(selected.spec.profile(), 1, Integer::sum);
		commitSelection(selected);
		validate();
		return Optional.of(selected.spec);
	}

	void complete(long id) {
		var job = requirePhase(id, Phase.ACTIVE);
		active.merge(job.spec.profile(), -1, Integer::sum);
		terminal(job, job.termination != null
				? job.termination
				: job.spec.failWhenRun() ? Outcome.FAILURE : Outcome.RUN);
	}

	void yield(long id) {
		var job = requirePhase(id, Phase.ACTIVE);
		active.merge(job.spec.profile(), -1, Integer::sum);
		if (job.termination != null) {
			terminal(job, job.termination);
			return;
		}
		job.phase = Phase.QUEUED;
		job.queueSequence = nextSequence++;
		queues.get(job.spec.profile()).add(job);
		validate();
	}

	void park(long id) {
		var job = requirePhase(id, Phase.ACTIVE);
		active.merge(job.spec.profile(), -1, Integer::sum);
		if (job.termination != null) terminal(job, job.termination);
		else job.phase = Phase.PARKED;
		validate();
	}

	boolean resume(long id) {
		var job = jobs.get(id);
		if (job == null || job.phase != Phase.PARKED || job.termination != null) return false;
		job.phase = Phase.QUEUED;
		job.queueSequence = nextSequence++;
		queues.get(job.spec.profile()).add(job);
		validate();
		return true;
	}

	void gracefulShutdown() {
		shutdown = true;
	}

	void forceShutdown() {
		shutdown = true;
		for (var job : List.copyOf(jobs.values())) {
			switch (job.phase) {
				case QUEUED -> {
					queues.get(job.spec.profile()).remove(job);
					terminal(job, Outcome.SHUTDOWN);
				}
				case PARKED -> terminal(job, Outcome.SHUTDOWN);
				case ACTIVE -> job.termination = Outcome.SHUTDOWN;
				case TERMINAL -> {
				}
			}
		}
		validate();
	}

	void expire(long now) {
		for (var job : List.copyOf(jobs.values())) {
			if (job.phase == Phase.TERMINAL || job.termination != null
					|| job.spec.deadline() == Long.MAX_VALUE || now < job.spec.deadline()) continue;
			switch (job.phase) {
				case QUEUED -> {
					queues.get(job.spec.profile()).remove(job);
					terminal(job, Outcome.DEADLINE);
				}
				case PARKED -> terminal(job, Outcome.DEADLINE);
				case ACTIVE -> job.termination = Outcome.DEADLINE;
				case TERMINAL -> {
				}
			}
		}
		validate();
	}

	List<Long> drainOrder(long now, boolean batchEligible) {
		var result = new ArrayList<Long>();
		while (true) {
			var next = dispatch(now, batchEligible);
			if (next.isEmpty()) break;
			result.add(next.get().id());
			complete(next.get().id());
		}
		return List.copyOf(result);
	}

	Phase phase(long id) {
		var job = jobs.get(id);
		return job == null ? Phase.TERMINAL : job.phase;
	}

	List<Long> ids(Phase phase) {
		return jobs.values().stream()
				.filter(job -> job.phase == phase)
				.map(job -> job.spec.id())
				.toList();
	}

	long attempts() {
		return attempts;
	}

	long accepted() {
		return accepted;
	}

	long outcomes() {
		return outcomes.values().stream().mapToLong(Long::longValue).sum();
	}

	Map<Outcome, Long> outcomeCounts() {
		return Map.copyOf(outcomes);
	}

	List<Terminal> terminalOrder() {
		return List.copyOf(terminalOrder);
	}

	boolean drainedAndConserved() {
		return jobs.values().stream().allMatch(job -> job.phase == Phase.TERMINAL)
				&& activeTotal() == 0
				&& queues.values().stream().allMatch(List::isEmpty)
				&& outcomes() == attempts;
	}

	void validate() {
		for (var profile : WorkloadProfile.values()) {
			long queued = jobs.values().stream()
					.filter(job -> job.spec.profile() == profile && job.phase == Phase.QUEUED)
					.count();
			long activeJobs = jobs.values().stream()
					.filter(job -> job.spec.profile() == profile && job.phase == Phase.ACTIVE)
					.count();
			long parkedJobs = jobs.values().stream()
					.filter(job -> job.spec.profile() == profile && job.phase == Phase.PARKED)
					.count();
			if (queued != queues.get(profile).size() || activeJobs != active.get(profile)) {
				throw new AssertionError("reference ownership mismatch for " + profile);
			}
			long outstanding = queued + activeJobs + parkedJobs;
			if (outstanding != outstanding(profile)) {
				throw new AssertionError("reference outstanding ownership mismatch for " + profile);
			}
			// Queue capacity is an admission bound, not a lifetime bound. Already admitted
			// cooperative tasks may park while other submissions fill the queue, then resume
			// above that admission capacity. The outstanding bound remains absolute.
			long outstandingLimit = (long) settings.capacities().getOrDefault(profile, 0)
					+ settings.workers();
			if (outstanding > outstandingLimit) {
				throw new AssertionError("reference outstanding bound exceeded for " + profile);
			}
		}
		if (activeTotal() > settings.workers()) throw new AssertionError("reference worker bound exceeded");
		if (outcomes() > attempts || accepted > attempts) throw new AssertionError("reference conservation overflow");
	}

	private void expireQueued(long now) {
		expire(now);
	}

	private Job select(boolean batchEligible) {
		boolean reservedLatency = reservationDeficit(WorkloadProfile.LATENCY);
		boolean reservedGuaranteed = false;
		for (var profile : GUARANTEED) {
			reservedGuaranteed |= reservationDeficit(profile) && eligible(profile, batchEligible);
		}
		if (reservedLatency || reservedGuaranteed) {
			if (reservedLatency && (latencyBurst < settings.latencyBurst() || !reservedGuaranteed)) {
				return head(WorkloadProfile.LATENCY);
			}
			var guaranteed = selectGuaranteed(true, batchEligible);
			if (guaranteed != null) return guaranteed;
			if (reservedLatency) return head(WorkloadProfile.LATENCY);
		}

		boolean latencyEligible = eligible(WorkloadProfile.LATENCY, batchEligible);
		boolean guaranteedEligible = false;
		for (var profile : GUARANTEED) guaranteedEligible |= eligible(profile, batchEligible);
		if (latencyEligible && (latencyBurst < settings.latencyBurst() || !guaranteedEligible)) {
			return head(WorkloadProfile.LATENCY);
		}
		var guaranteed = selectGuaranteed(false, batchEligible);
		if (guaranteed != null) return guaranteed;
		return latencyEligible ? head(WorkloadProfile.LATENCY) : null;
	}

	private Job selectGuaranteed(boolean reservationOnly, boolean batchEligible) {
		for (int guard = 0; guard < GUARANTEED.length * 32; guard++) {
			var profile = GUARANTEED[guaranteedCursor];
			if (queues.get(profile).isEmpty()) {
				deficit.put(profile, 0);
				advanceCursor();
				continue;
			}
			if ((reservationOnly && !reservationDeficit(profile)) || !eligible(profile, batchEligible)) {
				advanceCursor();
				continue;
			}
			if (guaranteedNeedsQuantum) {
				deficit.put(profile, Math.min(16,
						deficit.get(profile) + settings.quanta().getOrDefault(profile, 1)));
				guaranteedNeedsQuantum = false;
			}
			var head = head(profile);
			if (deficit.get(profile) < head.spec.cost()) {
				advanceCursor();
				continue;
			}
			return head;
		}
		return null;
	}

	private boolean reservationDeficit(WorkloadProfile profile) {
		return settings.reservations().getOrDefault(profile, 0) > active.get(profile)
				&& !queues.get(profile).isEmpty();
	}

	private boolean eligible(WorkloadProfile profile, boolean batchEligible) {
		if (queues.get(profile).isEmpty() || activeTotal() >= settings.workers()) return false;
		if (profile == WorkloadProfile.BATCH && !batchEligible) return false;
		return profile != WorkloadProfile.ANALYTICAL || active.get(profile) < settings.analyticalLimit();
	}

	private Job head(WorkloadProfile profile) {
		return profile == WorkloadProfile.LATENCY
				? queues.get(profile).stream().min(LATENCY_ORDER).orElseThrow()
				: queues.get(profile).stream()
						.min(Comparator.comparingLong(job -> job.queueSequence))
						.orElseThrow();
	}

	private void commitSelection(Job job) {
		var profile = job.spec.profile();
		if (profile == WorkloadProfile.LATENCY) {
			latencyBurst = Math.min(settings.latencyBurst(), latencyBurst + 1);
			return;
		}
		if (!List.of(GUARANTEED).contains(profile)) return;
		latencyBurst = 0;
		deficit.put(profile, deficit.get(profile) - job.spec.cost());
		if (queues.get(profile).isEmpty() || deficit.get(profile) < head(profile).spec.cost()) advanceCursor();
	}

	private void advanceCursor() {
		guaranteedCursor = (guaranteedCursor + 1) % GUARANTEED.length;
		guaranteedNeedsQuantum = true;
	}

	private Job requirePhase(long id, Phase phase) {
		var job = Objects.requireNonNull(jobs.get(id), "missing job " + id);
		if (job.phase != phase) throw new IllegalStateException("job " + id + " is " + job.phase + ", not " + phase);
		return job;
	}

	private void terminal(Job job, Outcome outcome) {
		job.phase = Phase.TERMINAL;
		job.termination = null;
		outcomes.merge(outcome, 1L, Long::sum);
		terminalOrder.add(new Terminal(job.spec.id(), outcome));
		validate();
	}

	private void recordImmediate(long id, Outcome outcome) {
		outcomes.merge(outcome, 1L, Long::sum);
		terminalOrder.add(new Terminal(id, outcome));
	}

	private int queued(WorkloadProfile profile) {
		return queues.get(profile).size();
	}

	private int outstanding(WorkloadProfile profile) {
		return Math.toIntExact(jobs.values().stream()
				.filter(job -> job.spec.profile() == profile && job.phase != Phase.TERMINAL)
				.count());
	}

	private int activeTotal() {
		return active.values().stream().mapToInt(Integer::intValue).sum();
	}

	private static EnumMap<WorkloadProfile, Integer> zeroInts() {
		var result = new EnumMap<WorkloadProfile, Integer>(WorkloadProfile.class);
		for (var profile : WorkloadProfile.values()) result.put(profile, 0);
		return result;
	}

	private static EnumMap<Outcome, Long> zeroLongs() {
		var result = new EnumMap<Outcome, Long>(Outcome.class);
		for (var outcome : Outcome.values()) result.put(outcome, 0L);
		return result;
	}

	private static final class Job {
		private final Spec spec;
		private final long initialSequence;
		private long queueSequence;
		private Phase phase = Phase.QUEUED;
		private Outcome termination;

		private Job(Spec spec, long sequence) {
			this.spec = spec;
			this.initialSequence = sequence;
			this.queueSequence = sequence;
		}
	}

	/** Pure two-pool BATCH admission model with logical time. */
	static final class BatchGate {

		enum Pool { READ, WRITE }

		record Permit(boolean pressured, boolean competing, long pressureEpisode, long competitionEpisode) {
		}

		private final int pressureMaximum;
		private final int competingReadMaximum;
		private final int competingWriteMaximum;
		private final long pressureInterval;
		private final long competingWriteInterval;
		private final long competitionHold;
		private final EnumMap<Pool, Boolean> queued = new EnumMap<>(Pool.class);
		private final EnumMap<Pool, Boolean> dispatchable = new EnumMap<>(Pool.class);
		private final EnumMap<Pool, Integer> active = new EnumMap<>(Pool.class);
		private final EnumMap<Pool, Boolean> competitors = new EnumMap<>(Pool.class);
		private boolean pressured;
		private int activeTotal;
		private Pool lastCompleted;
		private long pressureEpisode;
		private long competitionEpisode;
		private long competitionUntil = Long.MIN_VALUE;
		private long nextPressureStart = Long.MIN_VALUE;
		private long nextCompetingWrite = Long.MIN_VALUE;

		BatchGate(int pressureMaximum,
		          int competingReadMaximum,
		          int competingWriteMaximum,
		          long pressureInterval,
		          long competingWriteInterval,
		          long competitionHold) {
			this.pressureMaximum = pressureMaximum;
			this.competingReadMaximum = competingReadMaximum;
			this.competingWriteMaximum = competingWriteMaximum;
			this.pressureInterval = pressureInterval;
			this.competingWriteInterval = competingWriteInterval;
			this.competitionHold = competitionHold;
			for (var pool : Pool.values()) {
				queued.put(pool, false);
				dispatchable.put(pool, false);
				active.put(pool, 0);
				competitors.put(pool, false);
			}
		}

		void pressure(boolean value) {
			if (pressured != value) pressureEpisode++;
			pressured = value;
			if (!value) {
				nextPressureStart = Long.MIN_VALUE;
				lastCompleted = null;
			}
		}

		void queued(Pool pool, boolean value) {
			queued.put(pool, value);
			if (!value) dispatchable.put(pool, false);
		}

		void dispatchable(Pool pool, boolean value) {
			dispatchable.put(pool, queued.get(pool) && value);
		}

		void competitor(Pool pool, boolean present, long now) {
			expireCompetition(now);
			boolean wasCompeting = competitionCount() > 0;
			competitors.put(pool, present);
			if (competitionCount() > 0) competitionUntil = Long.MAX_VALUE;
			else if (wasCompeting) competitionUntil = saturating(now, competitionHold);
		}

		Optional<Permit> start(Pool pool, long now) {
			if (!canStart(pool, now)) return Optional.empty();
			boolean competing = competitionActive(now);
			activeTotal++;
			active.merge(pool, 1, Integer::sum);
			return Optional.of(new Permit(pressured, competing, pressureEpisode, competitionEpisode));
		}

		void finish(Pool pool, Permit permit, long now) {
			if (active.get(pool) <= 0 || activeTotal <= 0) throw new IllegalStateException("permit underflow");
			active.merge(pool, -1, Integer::sum);
			activeTotal--;
			if (pool == Pool.WRITE && permit.competing() && permit.competitionEpisode() == competitionEpisode
					&& competitionActive(now)) nextCompetingWrite = saturating(now, competingWriteInterval);
			if (permit.pressured() && pressured && permit.pressureEpisode() == pressureEpisode) {
				lastCompleted = pool;
				nextPressureStart = saturating(now, pressureInterval);
			}
		}

		boolean fairTurn(Pool pool) {
			return !pressured || lastCompleted == null || lastCompleted != pool || !dispatchable.get(other(pool));
		}

		boolean isDispatchable(Pool pool) {
			return dispatchable.get(pool);
		}

		boolean canStart(Pool pool, long now) {
			boolean competing = competitionActive(now);
			if (competing && (active.get(pool) >= competingMaximum(pool)
					|| pool == Pool.WRITE && now < nextCompetingWrite)) return false;
			return !pressured
					|| activeTotal < pressureMaximum && now >= nextPressureStart && fairTurn(pool);
		}

		int activeTotal() {
			return activeTotal;
		}

		String describe() {
			return "pressured=" + pressured + ", active=" + active + ", activeTotal=" + activeTotal
					+ ", queued=" + queued + ", dispatchable=" + dispatchable + ", competitors=" + competitors
					+ ", lastCompleted=" + lastCompleted + ", nextPressure=" + nextPressureStart
					+ ", nextWrite=" + nextCompetingWrite + ", competitionUntil=" + competitionUntil
					+ ", pressureEpisode=" + pressureEpisode + ", competitionEpisode=" + competitionEpisode;
		}

		void validate() {
			int sum = active.values().stream().mapToInt(Integer::intValue).sum();
			if (sum != activeTotal || activeTotal < 0) throw new AssertionError("BATCH permit conservation failed");
			for (var pool : Pool.values()) {
				if (dispatchable.get(pool) && !queued.get(pool)) throw new AssertionError("dispatchability without queue");
			}
		}

		private boolean competitionActive(long now) {
			expireCompetition(now);
			return competitionCount() > 0 || now < competitionUntil;
		}

		private void expireCompetition(long now) {
			if (competitionCount() == 0 && competitionUntil != Long.MIN_VALUE && now >= competitionUntil) {
				competitionUntil = Long.MIN_VALUE;
				nextCompetingWrite = Long.MIN_VALUE;
				competitionEpisode++;
			}
		}

		private int competitionCount() {
			return (int) competitors.values().stream().filter(Boolean::booleanValue).count();
		}

		private int competingMaximum(Pool pool) {
			return pool == Pool.READ ? competingReadMaximum : competingWriteMaximum;
		}

		private static Pool other(Pool pool) {
			return pool == Pool.READ ? Pool.WRITE : Pool.READ;
		}

		private static long saturating(long now, long delay) {
			return delay >= Long.MAX_VALUE - now ? Long.MAX_VALUE : now + delay;
		}
	}
}
