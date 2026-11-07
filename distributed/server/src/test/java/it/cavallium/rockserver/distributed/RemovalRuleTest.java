package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.RemovalRule.BEYOND;
import static it.cavallium.rockserver.distributed.RemovalRule.PURGED;
import static it.cavallium.rockserver.distributed.RemovalRule.UNKNOWN_TERM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.RemovalRule.Answer;
import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Own;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import it.cavallium.rockserver.distributed.RemovalRule.Reason;
import it.cavallium.rockserver.distributed.RemovalRule.Report;
import it.cavallium.rockserver.distributed.RemovalRule.Verdict;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import org.junit.jupiter.api.Test;

/** R11: the removal rule, one case per scenario of "Removal and waiting outside" in the design document. */
class RemovalRuleTest {

	private static final String X = "x";

	private static Conf stable(long term, long index, String... ids) {
		return new Conf(term, index, List.of(ids), List.of());
	}

	private static Conf joint(long term, long index, List<String> newHalf, List<String> oldHalf) {
		return new Conf(term, index, newHalf, oldHalf);
	}

	private static Answer report(Conf committed, Conf latest) {
		return report(committed, latest, Probe.UNKNOWN, null, Probe.UNKNOWN);
	}

	private static Answer report(Conf committed, Conf latest, Probe own, Conf probed, Probe kStar) {
		return Answer.of(new Report(2, true, true, 9, committed == null ? -1 : committed.index(), committed, latest, own,
				probed == null ? UNKNOWN_TERM : probed.term(), probed == null ? -1 : probed.index(), kStar, Map.of()));
	}

	/** A log with term {@code term} up to index {@code last}, purged up to {@code purged}. */
	private static LongUnaryOperator log(long term, long purged, long last) {
		return index -> index <= purged ? PURGED : index > last ? BEYOND : term;
	}

	private static Own seeded(Conf committed, Conf latest, LongUnaryOperator termAt) {
		return new Own(X, committed, latest, termAt, true, false);
	}

	private static Verdict decide(Own own, Object... idAndAnswer) {
		var answers = new LinkedHashMap<String, Answer>();
		for (int i = 0; i < idAndAnswer.length; i += 2) {
			answers.put((String) idAndAnswer[i], (Answer) idAndAnswer[i + 1]);
		}
		return RemovalRule.decide(own, answers);
	}

	@Test
	void aMemberRestartingInAHealthyClusterIsAMember() {
		var c = stable(2, 10, X, "a", "b");
		var verdict = decide(seeded(c, c, log(2, -1, 12)), "a", report(c, c), "b", report(c, c));
		assertEquals(Reason.MEMBER, verdict.reason());
	}

	@Test
	void aColdRestartWithNoAnswerDecidesOnTheNodesOwnCommittedConfiguration() {
		var c = stable(2, 10, X, "a", "b");
		assertEquals(Reason.MEMBER, decide(seeded(c, c, log(2, -1, 12)), "a", Answer.NONE, "b", Answer.PENDING).reason());
		var verdict = decide(seeded(null, c, log(2, -1, 12)), "a", Answer.NONE, "b", Answer.PENDING);
		assertEquals(Reason.NO_COMMITTED_CONFIGURATION, verdict.reason());
		assertEquals(List.of("b"), verdict.pending());
		assertEquals(List.of("a"), verdict.unreachable());
	}

	/** R4-6: the node's log missed its removal. */
	@Test
	void aRemovedNodeWhoseLogMissedItsRemovalIsRemoved() {
		var own = stable(1, 5, X, "a", "b", "c");
		var removal = stable(3, 9, "a", "b", "c");
		var verdict = decide(seeded(own, own, log(1, -1, 6)), "a", report(removal, removal), "b", report(removal, removal),
				"c", Answer.NONE);
		assertEquals(Reason.REMOVED, verdict.reason());
		assertEquals(List.of("a", "b"), verdict.confirmers());
		assertTrue(verdict.ownLog().contains("ends before index 9"), verdict.ownLog());
	}

	/**
	 * R11-2: a longer, uncommitted tail with a configuration that holds the node, of a lower term than the removal. The
	 * rule of 4010d83 compared the indexes (48 below 50) and said member.
	 */
	@Test
	void aRemovedNodeWithALongerTailOfALowerTermIsRemoved() {
		var tail = stable(5, 50, X, "a", "b", "c", "d");
		var before = stable(4, 45, X, "a", "b", "c", "d");
		var removal = stable(6, 48, "a", "b", "c", "d");
		LongUnaryOperator termAt = index -> index <= 45 ? 4 : index <= 300 ? 5 : BEYOND;
		var verdict = decide(seeded(before, tail, termAt), "a", report(removal, removal), "b", report(removal, removal),
				"c", Answer.NONE, "d", report(removal, removal));
		assertEquals(Reason.REMOVED, verdict.reason());
		assertTrue(verdict.ownLog().contains("diverged at index 48"), verdict.ownLog());
	}

	/** R11-1: the log ends with a stable configuration without the node that a term-start entry flattened, then dropped. */
	@Test
	void aFlattenedRemovalThatWasDroppedIsNoRemoval() {
		var committed = stable(1, 9, X, "l", "a", "b", "c");
		var flattened = stable(2, 11, "l", "a", "b", "c");
		var newLeaderStart = stable(3, 10, X, "l", "a", "b", "c");
		var verdict = decide(seeded(committed, flattened, log(2, -1, 11)), "a", report(newLeaderStart, newLeaderStart),
				"b", report(newLeaderStart, newLeaderStart), "c", report(newLeaderStart, newLeaderStart), "l", Answer.NONE);
		assertEquals(Reason.MEMBER, verdict.reason());
		// The flattened configuration was committed after all: removed.
		var removedNow = decide(seeded(committed, flattened, log(2, -1, 11)), "a", report(flattened, flattened), "b",
				report(flattened, flattened), "c", report(flattened, flattened), "l", Answer.NONE);
		assertEquals(Reason.REMOVED, removedNow.reason());
	}

	@Test
	void aCommittedJointRemovalRemovesTheNode() {
		var before = stable(1, 9, X, "a", "b");
		var removal = joint(1, 10, List.of("a", "b"), List.of(X, "a", "b"));
		var verdict = decide(seeded(before, removal, log(1, -1, 10)), "a", report(removal, removal), "b",
				report(stable(1, 11, "a", "b"), stable(1, 11, "a", "b")));
		// K* is the final configuration at 11, which only b reports: one of two is no majority yet.
		assertEquals(Reason.NO_MAJORITY, verdict.reason());
		assertEquals(11, verdict.kStar().index());
		var final11 = stable(1, 11, "a", "b");
		assertEquals(Reason.REMOVED, decide(seeded(before, removal, log(1, -1, 10)), "a", report(final11, final11), "b",
				report(final11, final11)).reason());
	}

	/** R3-G2b-4: the joint configuration of its removal was dropped by the next leader. */
	@Test
	void aDroppedJointRemovalIsNoRemoval() {
		var before = stable(1, 9, X, "a", "b", "c", "d");
		var dropped = joint(1, 10, List.of("a", "b", "c", "d"), List.of(X, "a", "b", "c", "d"));
		var start = stable(2, 10, X, "a", "b", "c", "d");
		assertEquals(Reason.MEMBER, decide(seeded(before, dropped, log(1, -1, 10)), "b", report(start, start), "c",
				report(start, start), "d", report(start, start)).reason());
	}

	/** R10-1: the removal is in flight while the node restarts. */
	@Test
	void aRemovalInFlightIsNoRemovalYet() {
		var before = stable(1, 9, X, "a", "b");
		var joint = joint(1, 10, List.of("a", "b"), List.of(X, "a", "b"));
		assertEquals(Reason.MEMBER, decide(seeded(before, joint, log(1, -1, 10)), "a", report(before, joint), "b",
				report(before, joint)).reason());
	}

	/** Figure 8: a member's later configuration of a term at least K*'s blocks until it is shown off K*'s branch. */
	@Test
	void aMembersLaterConfigurationBlocksUnlessItCannotCommit() {
		var own = stable(1, 5, X, "a", "b", "c");
		var removal = stable(3, 9, "a", "b", "c");
		var later = stable(4, 12, X, "a", "b", "c");
		var blocking = decide(seeded(own, own, log(1, -1, 5)), "a", report(removal, removal), "b", report(removal, removal),
				"c", report(stable(2, 7, X, "a", "b", "c"), later, Probe.UNKNOWN, removal, Probe.PRESENT));
		assertEquals(Reason.MEMBER_LATER_CONFIGURATION, blocking.reason());
		assertEquals(List.of("c"), blocking.blockers());
		// Not on K*'s branch: its log has no (3, 9).
		var offBranch = decide(seeded(own, own, log(1, -1, 5)), "a", report(removal, removal), "b",
				report(removal, removal), "c", report(stable(2, 7, X, "a", "b", "c"), later, Probe.UNKNOWN, removal,
						Probe.ABSENT));
		assertEquals(Reason.REMOVED, offBranch.reason());
		// A probe of another K* says nothing about this one: still blocking.
		var otherProbe = decide(seeded(own, own, log(1, -1, 5)), "a", report(removal, removal), "b",
				report(removal, removal), "c", report(stable(2, 7, X, "a", "b", "c"), later, Probe.UNKNOWN,
						stable(3, 8, "a"), Probe.ABSENT));
		assertEquals(Reason.MEMBER_LATER_CONFIGURATION, otherProbe.reason());
		// A lower term than K*: never committed.
		var lowerTerm = stable(2, 12, X, "a", "b", "c");
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(removal, removal), "b",
				report(removal, removal), "c", report(stable(2, 7, X, "a", "b", "c"), lowerTerm)).reason());
	}

	@Test
	void aMajorityOfKStarsNewHalfDecides() {
		var own = stable(1, 5, X, "a", "b", "c", "d");
		var four = stable(3, 9, "a", "b", "c", "d");
		assertEquals(Reason.NO_MAJORITY, decide(seeded(own, own, log(1, -1, 5)), "a", report(four, four), "b",
				report(four, four), "c", Answer.NONE, "d", Answer.NONE).reason());
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(four, four), "b",
				report(four, four), "c", report(four, four), "d", Answer.NONE).reason());
		var five = stable(3, 9, "a", "b", "c", "d", "e");
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(five, five), "b",
				report(five, five), "c", report(five, five)).reason());
		// A joint K*: a majority of its new half.
		var jointKStar = joint(3, 9, List.of("a", "b", "c"), List.of(X, "a", "b", "c"));
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(jointKStar, jointKStar), "b",
				report(jointKStar, jointKStar)).reason());
		// A member behind K* does not confirm it.
		assertEquals(Reason.NO_MAJORITY, decide(seeded(own, own, log(1, -1, 5)), "a", report(five, five), "b",
				report(five, five), "c", report(own, own)).reason());
		// ... unless its probe of K* says it holds it committed.
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(five, five), "b",
				report(five, five), "c", report(own, own, Probe.UNKNOWN, five, Probe.COMMITTED)).reason());
	}

	@Test
	void aNodeThatNeverWasAVoterIsNotAdded() {
		var removal = stable(3, 9, "a", "b", "c");
		var replayed = stable(2, 6, X, "a", "b", "c");
		// A fresh --join node, or one replaying an earlier membership of its id while it is brought up to date.
		var fresh = new Own(X, removal, removal, log(3, -1, 9), false, false);
		assertEquals(Reason.NOT_ADDED, decide(fresh, "a", report(removal, removal), "b", report(removal, removal)).reason());
		var midReplay = new Own(X, replayed, removal, log(3, -1, 9), false, false);
		assertEquals(Reason.NOT_ADDED, decide(midReplay, "a", report(removal, removal), "b", report(removal, removal))
				.reason());
		// R11-3: a --join node that a leader made a voter (the joint configuration of its add), removed while down.
		var addJoint = joint(2, 20, List.of("a", "b", "c", X), List.of("a", "b", "c"));
		var removed = stable(3, 30, "a", "b", "c");
		var voter = new Own(X, stable(1, 10, "a", "b", "c"), addJoint, log(2, -1, 20), false, true);
		assertEquals(Reason.REMOVED, decide(voter, "a", report(removed, removed), "b", report(removed, removed)).reason());
	}

	@Test
	void aLaterConfigurationOfTheNodesOwnThatMayCommitWaits() {
		var removal = stable(3, 9, "a", "b");
		var readd = stable(4, 12, X, "a", "b");
		assertEquals(Reason.OWN_LATER_CONFIGURATION, decide(seeded(removal, readd, log(3, -1, 12)), "a",
				report(removal, removal), "b", report(removal, removal)).reason());
		assertEquals(Reason.OWN_LATER_CONFIGURATION, decide(seeded(removal, readd, log(4, 10, 12)), "a",
				report(removal, removal), "b", report(removal, removal)).reason());
		// K* from members that do not tell terms: cannot tell.
		var v1 = new Conf(UNKNOWN_TERM, 9, List.of("a", "b"), null);
		assertEquals(Reason.NEEDS_TERM, decide(seeded(null, readd, log(4, -1, 12)), "a", report(v1, v1), "b",
				report(v1, v1)).reason());
	}

	/** A committed answer of the node's own latest configuration (probe 0) makes it a committed fact. */
	@Test
	void theNodesOwnLatestConfigurationCountsOnceAMemberHoldsItCommitted() {
		var removal = stable(3, 30, "a", "b");
		var readd = stable(4, 40, X, "a", "b");
		assertEquals(Reason.MEMBER, decide(seeded(removal, readd, log(4, -1, 40)), "a",
				report(removal, removal, Probe.COMMITTED, null, Probe.UNKNOWN), "b", Answer.NONE).reason());
	}

	@Test
	void versionOneAnswersDecideWhatNeedsNoTerm() {
		var own = stable(1, 5, X, "a", "b", "c");
		var removal = RemovalCheck.fromVersion1(new RemovalCheck.PeerConfiguration(9, true, List.of("a", "b", "c")));
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", Answer.of(removal), "b",
				Answer.of(removal)).reason());
		// Not committed: no fact.
		var pending = RemovalCheck.fromVersion1(new RemovalCheck.PeerConfiguration(9, false, List.of("a", "b", "c")));
		assertEquals(Reason.MEMBER, decide(seeded(own, own, log(1, -1, 5)), "a", Answer.of(pending), "b",
				Answer.of(pending)).reason());
	}

	@Test
	void committedConfigurationsAtOneIndexMustAgreeOnWhatBothKnow() {
		var own = stable(1, 5, X, "a", "b", "c");
		var v2 = joint(3, 9, List.of("a", "b"), List.of(X, "a", "b"));
		var v1 = new Conf(UNKNOWN_TERM, 9, List.of("b", "a"), null);
		// The version-1 answer knows neither the term nor the old half: no conflict.
		assertEquals(Reason.REMOVED, decide(seeded(own, own, log(1, -1, 5)), "a", report(v2, v2), "b", report(v1, v1))
				.reason());
		var otherTerm = joint(4, 9, List.of("a", "b"), List.of(X, "a", "b"));
		assertEquals(Reason.INCONSISTENT, decide(seeded(own, own, log(1, -1, 5)), "a", report(v2, v2), "b",
				report(otherTerm, otherTerm)).reason());
		var otherMembers = stable(3, 9, "a", "c");
		assertEquals(Reason.INCONSISTENT, decide(seeded(own, own, log(1, -1, 5)), "a", report(v2, v2), "b",
				report(otherMembers, otherMembers)).reason());
	}

	/** Two removed nodes with stale tails: each one's later configuration is of a lower term than the removal. */
	@Test
	void anotherRemovedNodeDoesNotBlock() {
		var own = stable(2, 50, X, "y", "a", "b", "c");
		var removal = stable(3, 48, "a", "b", "c");
		LongUnaryOperator termAt = index -> index <= 45 ? 1 : index <= 50 ? 2 : BEYOND;
		assertEquals(Reason.REMOVED, decide(seeded(stable(1, 45, X, "y", "a", "b", "c"), own, termAt), "y",
				report(stable(1, 45, X, "y", "a", "b", "c"), own), "a", report(removal, removal), "b",
				report(removal, removal)).reason());
	}

	@Test
	void theMembersToAskAndTheProbesFollowTheNodesConfigurationsAndKStar() {
		var latest = joint(2, 20, List.of("a", "b"), List.of(X, "a", "c"));
		var own = new Own(X, stable(1, 10, X, "d"), latest, log(2, -1, 20), true, false);
		var kStar = stable(3, 30, "e", "a");
		assertEquals(List.of("a", "b", "c", "d", "e"), List.copyOf(RemovalCheck.members(own, kStar, List.of())));
		assertEquals(List.of("a", "b", "c", "d", "e", "f"), List.copyOf(RemovalCheck.members(own, kStar, List.of("x", "f"))));
		var probes = RemovalCheck.probes(own, kStar);
		assertEquals(20, probes.get(0).getIndex());
		assertEquals(30, probes.get(1).getIndex());
		assertEquals(10, RemovalCheck.probes(own, null).get(1).getIndex());
	}
}
