package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.distributed.RemovalRule.Reason;
import it.cavallium.rockserver.distributed.RemovalRule.Verdict;
import java.util.List;
import org.junit.jupiter.api.Test;

/** R12-4: the {@code membership.state} gauge; REMOVED no longer reads like a member. */
class MembershipGaugeTest {

	private static Verdict verdict(Reason reason) {
		return new Verdict(reason, null, List.of(), List.of(), List.of(), List.of(), "");
	}

	@Test
	void eachOutcomeHasItsOwnValue() {
		assertEquals(0, ReplicatedNode.membershipGauge(null), "not checked yet");
		assertEquals(0, ReplicatedNode.membershipGauge(verdict(Reason.MEMBER)));
		assertEquals(1, ReplicatedNode.membershipGauge(verdict(Reason.NOT_ADDED)));
		for (var undecided : List.of(Reason.NO_MAJORITY, Reason.NEEDS_TERM, Reason.OWN_LATER_CONFIGURATION,
				Reason.MEMBER_LATER_CONFIGURATION, Reason.NO_COMMITTED_CONFIGURATION, Reason.INCONSISTENT,
				Reason.HEARD_LEADER)) {
			assertEquals(2, ReplicatedNode.membershipGauge(verdict(undecided)), undecided.toString());
		}
		assertEquals(3, ReplicatedNode.membershipGauge(verdict(Reason.REMOVED)), "removed (reported only if a push happens before exit 4)");
	}
}
