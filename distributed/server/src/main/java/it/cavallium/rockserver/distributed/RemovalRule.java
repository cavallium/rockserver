package it.cavallium.rockserver.distributed;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.LongUnaryOperator;
import org.jetbrains.annotations.Nullable;

/**
 * The removal rule (R11): from a node's own log and its members' answers, whether the node was removed from the
 * cluster. Pure: {@link #decide} reads only its arguments. See "Removal and waiting outside" in the design document,
 * which has the proof sketches.
 *
 * <p>A node concludes that it was removed (exit 4) only when all of these hold:</p>
 * <ul>
 *   <li>a committed configuration K* leaves it out of its new half;</li>
 *   <li>K* is the newest committed configuration that anyone reports, the node's own included;</li>
 *   <li>a majority of K*'s new half each report K* committed in their own log, and none of them holds a later
 *   configuration that includes the node and may still commit; a member that predates {@code Configuration2} counts
 *   only when the node itself or a member of this version reports K* committed (R12-1);</li>
 *   <li>the node's own log holds no later configuration that includes it and may still commit;</li>
 *   <li>the node was a voter once ({@link Own#seeded} or {@link Own#memberFile}).</li>
 * </ul>
 *
 * <p>Only committed configurations are compared by index: committed entries form one log in every node. An entry
 * that is not committed only ever blocks a decision. Such an entry is known never to commit when it cannot be in a log
 * that holds K*: its term is lower than K*'s (terms never decrease along a log), or the log that holds it has another
 * term at K*'s index, or a probe of K*'s (term, index) answers {@link Probe#ABSENT} there.</p>
 */
final class RemovalRule {

	/** A term this side does not know. Ratis' log entries all have a term of 1 or more. */
	static final long UNKNOWN_TERM = 0;
	/** {@link Own#termAt}: the index is in the snapshot, at or below this node's commit index. */
	static final long PURGED = -2;
	/** {@link Own#termAt}: the index is past the end of the log. */
	static final long BEYOND = -3;
	/** The first {@link Report#version} whose committed configuration is a committed fact ({@code Configuration2}). */
	static final int SOUND_VERSION = 2;

	private RemovalRule() {}

	/**
	 * A configuration entry: its term ({@link #UNKNOWN_TERM} if not known), its log index (-1: the configuration a
	 * node starts with, from no entry), its new half, and its old half (empty for a stable configuration, null if not
	 * known: an answer of a member that predates {@code Configuration2}).
	 */
	record Conf(long term, long index, List<String> newHalf, @Nullable List<String> oldHalf) {
		Conf {
			newHalf = List.copyOf(newHalf);
			oldHalf = oldHalf == null ? null : List.copyOf(oldHalf);
		}

		boolean termKnown() {
			return term > UNKNOWN_TERM;
		}

		boolean inNew(String id) {
			return newHalf.contains(id);
		}

		/** Whether either half holds {@code id}. */
		boolean holds(String id) {
			return newHalf.contains(id) || (oldHalf != null && oldHalf.contains(id));
		}

		@Override
		public String toString() {
			return "(term " + (termKnown() ? term : "?") + ", index " + index + ", " + newHalf
					+ (oldHalf == null ? ", old half unknown" : oldHalf.isEmpty() ? "" : ", old half " + oldHalf) + ")";
		}
	}

	/** What a member's log says about a probed (term, index). */
	enum Probe {
		/** The entry is in its log, at or below its commit index: committed. */
		COMMITTED,
		/** The entry is in its log, past its commit index. */
		PRESENT,
		/** Its log has another term at that index, or ends before it, or its history recorded another configuration there. */
		ABSENT,
		/** Cannot tell: in its snapshot with no record in its history, or a member that predates the probe. */
		UNKNOWN
	}

	/**
	 * A member's answer to {@code Configuration2} (or, mapped, to {@code Configuration}): its committed configuration
	 * (null if it applied none), its latest one, and the answers to the asker's probes of its own latest configuration
	 * ({@code ownProbe}) and of the K* it knew when it asked ({@code kStarProbe} at {@code probedTerm},
	 * {@code probedIndex}).
	 */
	record Report(int version, boolean running, boolean leaderKnown, long currentTerm, long commitIndex,
			@Nullable Conf committed, @Nullable Conf latest, Probe ownProbe, long probedTerm, long probedIndex,
			Probe kStarProbe, Map<String, String> addresses) {
		Report {
			addresses = Map.copyOf(addresses);
		}

		/** The answer to the probe of {@code kStar}, if this report probed exactly it; else {@link Probe#UNKNOWN}. */
		Probe probeOf(Conf kStar) {
			return kStar.termKnown() && probedTerm == kStar.term() && probedIndex == kStar.index() ? kStarProbe
					: Probe.UNKNOWN;
		}
	}

	/** What one member answered. */
	record Answer(Kind kind, @Nullable Report report) {
		enum Kind {
			/** A {@link Report}. */
			REPORT,
			/** It runs, but has no group yet (not started, or still opening). */
			PENDING,
			/** No answer: unreachable, an error, or a node that predates both methods. */
			NONE
		}

		static final Answer PENDING = new Answer(Kind.PENDING, null);
		static final Answer NONE = new Answer(Kind.NONE, null);

		static Answer of(Report report) {
			return new Answer(Kind.REPORT, report);
		}
	}

	/**
	 * The node's own facts: its id, its committed configuration (the newest it applied), its latest configuration,
	 * the term at an index of its own log ({@link #PURGED}, {@link #BEYOND}), and whether it was ever a voter: it was
	 * started without {@code --join} ({@code seeded}: its own peer list holds it), or it recorded that a leader made it
	 * a voter ({@code memberFile}). {@code latestStale}: its log holds another entry at the index of its latest
	 * configuration, a stale configuration of Ratis ({@link MembershipFacts.Entry#OTHER}).
	 */
	record Own(String self, @Nullable Conf committed, @Nullable Conf latest, LongUnaryOperator termAt, boolean seeded,
			boolean memberFile, boolean latestStale) {
		Own(String self, @Nullable Conf committed, @Nullable Conf latest, LongUnaryOperator termAt, boolean seeded,
				boolean memberFile) {
			this(self, committed, latest, termAt, seeded, memberFile, false);
		}
	}

	/** What a verdict means for the node. */
	enum Outcome {
		MEMBER, NOT_ADDED, REMOVED, UNDECIDED
	}

	/** Why: the verdict's reason code. */
	enum Reason {
		/** The newest committed configuration holds the node in its new half. */
		MEMBER(Outcome.MEMBER),
		/** The node never was a voter: a fresh {@code --join} node, before or during its add. */
		NOT_ADDED(Outcome.NOT_ADDED),
		/** Removed: see the class comment. */
		REMOVED(Outcome.REMOVED),
		/** Two answers disagree about a committed configuration: a bug, or a data directory restored from elsewhere. */
		INCONSISTENT(Outcome.UNDECIDED),
		/** No committed configuration known: nobody answered, and this node applied none. */
		NO_COMMITTED_CONFIGURATION(Outcome.UNDECIDED),
		/**
		 * The node's own log has a later configuration that includes it and may still commit: normally an add in flight,
		 * or a configuration a leader appended just before it lost leadership.
		 */
		OWN_LATER_CONFIGURATION(Outcome.UNDECIDED),
		/**
		 * K* known only from members that predate {@code Configuration2}: they tell no terms, and their committed flag
		 * cannot confirm K* by itself (R12-1). Either the node's own log has a later configuration that includes it and
		 * K*'s term is unknown, or only such members report K* committed.
		 */
		NEEDS_TERM(Outcome.UNDECIDED),
		/** A member holds a later configuration that includes the node and may still commit. */
		MEMBER_LATER_CONFIGURATION(Outcome.UNDECIDED),
		/** Fewer than a majority of K*'s new half confirmed K*: members down, unreachable, behind, or older. */
		NO_MAJORITY(Outcome.UNDECIDED),
		/**
		 * A removal found while a leader sent this node a Raft request: not trusted, the answers may be older than that
		 * leader's configuration (set by {@link ReplicatedNode}, never by {@link #decide}).
		 */
		HEARD_LEADER(Outcome.UNDECIDED);

		final Outcome outcome;

		Reason(Outcome outcome) {
			this.outcome = outcome;
		}
	}

	/**
	 * A decision: the reason, K* (null if none), and the members by role: those that confirmed K*, those that block
	 * (they hold a later configuration with the node), those still starting, and those that did not answer.
	 * {@code ownLog} says, for a removal, what the node's own log holds at K*'s index.
	 */
	record Verdict(Reason reason, @Nullable Conf kStar, List<String> confirmers, List<String> blockers,
			List<String> pending, List<String> unreachable, String ownLog) {
		Verdict {
			confirmers = List.copyOf(confirmers);
			blockers = List.copyOf(blockers);
			pending = List.copyOf(pending);
			unreachable = List.copyOf(unreachable);
		}

		Outcome outcome() {
			return reason.outcome;
		}

		@Override
		public String toString() {
			return reason + " (K* " + kStar + ", confirmed by " + confirmers + ", blocked by " + blockers + ", pending "
					+ pending + ", unreachable " + unreachable + (ownLog.isEmpty() ? "" : ", " + ownLog) + ")";
		}
	}

	/** The majority of a half of {@code size} members. */
	static int majority(int size) {
		return size / 2 + 1;
	}

	/** Decides for {@code own} from {@code answers} (member id to answer, the node itself left out). */
	static Verdict decide(Own own, Map<String, Answer> answers) {
		var self = own.self();
		var pending = new ArrayList<String>();
		var unreachable = new ArrayList<String>();
		var reports = new LinkedHashMap<String, Report>();
		answers.forEach((id, answer) -> {
			switch (answer.kind()) {
				case REPORT -> reports.put(id, Objects.requireNonNull(answer.report()));
				case PENDING -> pending.add(id);
				case NONE -> unreachable.add(id);
			}
		});

		// 1. The committed configurations that anyone reports. "sound": the indexes of those that this version's facts
		// report (the node's own, a member's of this version, the node's own latest probed committed). A version-1
		// committed flag compares Ratis' configuration with the commit index, which is wrong for a stale configuration
		// of Ratis (round-11 review, finding 1): it raises K* or blocks, but does not confirm by itself (R12-1).
		var byIndex = new TreeMap<Long, List<Conf>>();
		var sound = new HashSet<Long>();
		if (own.committed() != null && own.committed().index() >= 0) {
			byIndex.computeIfAbsent(own.committed().index(), _ -> new ArrayList<>()).add(own.committed());
			sound.add(own.committed().index());
		}
		for (var report : reports.values()) {
			if (report.committed() != null && report.committed().index() >= 0) {
				byIndex.computeIfAbsent(report.committed().index(), _ -> new ArrayList<>()).add(report.committed());
				if (report.version() >= SOUND_VERSION) {
					sound.add(report.committed().index());
				}
			}
		}
		var ownLatest = own.latest();
		if (ownLatest != null && ownLatest.termKnown() && ownLatest.index() >= 0
				&& reports.values().stream().anyMatch(report -> report.ownProbe() == Probe.COMMITTED)) {
			byIndex.computeIfAbsent(ownLatest.index(), _ -> new ArrayList<>()).add(ownLatest);
			sound.add(ownLatest.index());
		}
		if (byIndex.isEmpty()) {
			return new Verdict(Reason.NO_COMMITTED_CONFIGURATION, null, List.of(), List.of(), pending, unreachable, "");
		}
		Conf kStar = null;
		for (var entry : byIndex.entrySet()) {
			var merged = merge(entry.getValue());
			if (merged == null) {
				return new Verdict(Reason.INCONSISTENT, entry.getValue().getFirst(), List.of(), List.of(), pending,
						unreachable, "committed configurations at index " + entry.getKey() + " disagree: " + entry.getValue());
			}
			kStar = merged;
		}
		Objects.requireNonNull(kStar);
		long iStar = kStar.index();

		// 2. A member.
		if (kStar.inNew(self)) {
			return new Verdict(Reason.MEMBER, kStar, List.of(), List.of(), pending, unreachable, "");
		}
		// 3. Never a voter: never removed either.
		if (!own.seeded() && !own.memberFile()) {
			return new Verdict(Reason.NOT_ADDED, kStar, List.of(), List.of(), pending, unreachable, "");
		}
		// 4. Its own log: a later configuration that includes it, and may still commit. A latest configuration with no
		// known term that is not in its own log at all (past its end: Ratis took it from a snapshot install's
		// raft-meta.conf; or another entry at its index: stale, finding 1) does not block by itself (R12-3): it can
		// commit only through a majority of K*'s new half, which step 5 sees.
		long termAtKStar = own.termAt().applyAsLong(iStar);
		boolean outsideOwnLog = ownLatest != null && !ownLatest.termKnown()
				&& (own.latestStale() || own.termAt().applyAsLong(ownLatest.index()) == BEYOND);
		if (ownLatest != null && ownLatest.index() > iStar && ownLatest.holds(self) && !outsideOwnLog) {
			boolean otherBranch = kStar.termKnown() && termAtKStar > 0 && termAtKStar != kStar.term();
			boolean olderTerm = kStar.termKnown() && ownLatest.termKnown() && ownLatest.term() < kStar.term();
			if (!otherBranch && !olderTerm) {
				var reason = kStar.termKnown() || termAtKStar == PURGED ? Reason.OWN_LATER_CONFIGURATION
						: Reason.NEEDS_TERM;
				return new Verdict(reason, kStar, List.of(), List.of(), pending, unreachable,
						"its own log has " + ownLatest + " after K*");
			}
		}
		// 5. Members that hold a later configuration that includes it, and may still commit.
		var blockers = new ArrayList<String>();
		for (var entry : reports.entrySet()) {
			var latest = entry.getValue().latest();
			if (latest != null && latest.index() > iStar && latest.holds(self)
					&& !(kStar.termKnown() && latest.termKnown() && latest.term() < kStar.term())
					&& entry.getValue().probeOf(kStar) != Probe.ABSENT) {
				blockers.add(entry.getKey());
			}
		}
		// 6. Members of K*'s new half that hold K* committed. A version-1 member only once K* is sound: its committed
		// entry at iStar is then the cluster's committed entry there, K* itself.
		var confirmers = new ArrayList<String>();
		boolean kStarSound = sound.contains(iStar);
		for (var entry : reports.entrySet()) {
			var report = entry.getValue();
			if (kStar.inNew(entry.getKey()) && !blockers.contains(entry.getKey())
					&& ((committedAt(report, iStar) && (report.version() >= SOUND_VERSION || kStarSound))
					|| report.probeOf(kStar) == Probe.COMMITTED)) {
				confirmers.add(entry.getKey());
			}
		}
		if (!blockers.isEmpty()) {
			return new Verdict(Reason.MEMBER_LATER_CONFIGURATION, kStar, confirmers, blockers, pending, unreachable, "");
		}
		if (confirmers.size() >= majority(kStar.newHalf().size())) {
			return new Verdict(Reason.REMOVED, kStar, confirmers, List.of(), pending, unreachable,
					ownLog(kStar, termAtKStar, ownLatest));
		}
		boolean onlyOldConfirm = false;
		for (var entry : reports.entrySet()) {
			onlyOldConfirm |= !kStarSound && kStar.inNew(entry.getKey()) && entry.getValue().version() < SOUND_VERSION
					&& committedAt(entry.getValue(), iStar);
		}
		if (onlyOldConfirm) {
			return new Verdict(Reason.NEEDS_TERM, kStar, confirmers, List.of(), pending, unreachable,
					"only members that predate Configuration2 report K* committed");
		}
		return new Verdict(Reason.NO_MAJORITY, kStar, confirmers, List.of(), pending, unreachable, "");
	}

	private static boolean committedAt(Report report, long index) {
		return report.committed() != null && report.committed().index() == index;
	}

	/** What the node's own log holds at K*'s index, for the message of a removal. */
	private static String ownLog(Conf kStar, long termAtKStar, @Nullable Conf ownLatest) {
		if (termAtKStar == BEYOND) {
			return "its own log ends before index " + kStar.index();
		}
		if (termAtKStar > 0 && kStar.termKnown() && termAtKStar != kStar.term()) {
			return "its own log diverged at index " + kStar.index() + " (term " + termAtKStar + " there)";
		}
		return "its own log holds no configuration after it that includes this node"
				+ (ownLatest == null ? "" : " (its latest is " + ownLatest + ")");
	}

	/**
	 * One configuration from the committed ones reported at the same index, with every field that one of them knows;
	 * null if two of them disagree on a field both know (committed entries are the same entry in every log).
	 */
	static @Nullable Conf merge(List<Conf> same) {
		long term = UNKNOWN_TERM;
		List<String> newHalf = null;
		List<String> oldHalf = null;
		for (var conf : same) {
			if (conf.termKnown()) {
				if (term != UNKNOWN_TERM && term != conf.term()) {
					return null;
				}
				term = conf.term();
			}
			if (newHalf != null && !new HashSet<>(newHalf).equals(new HashSet<>(conf.newHalf()))) {
				return null;
			}
			newHalf = conf.newHalf();
			if (conf.oldHalf() != null) {
				if (oldHalf != null && !new HashSet<>(oldHalf).equals(new HashSet<>(conf.oldHalf()))) {
					return null;
				}
				oldHalf = conf.oldHalf();
			}
		}
		return new Conf(term, same.getFirst().index(), Objects.requireNonNull(newHalf), oldHalf);
	}
}
