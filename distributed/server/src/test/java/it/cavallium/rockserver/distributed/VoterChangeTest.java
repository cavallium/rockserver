package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

/**
 * Round 14: what {@link VoterChange} counts as a change of voters (ids of the server list, compared by id), and what
 * its refusal says.
 */
class VoterChangeTest {

	private static RaftPeer peer(String id) {
		return RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:" + (9000 + id.hashCode() % 100)).build();
	}

	private static List<RaftPeer> peers(String... ids) {
		return Arrays.stream(ids).map(VoterChangeTest::peer).toList();
	}

	private static void assertChange(List<RaftPeer> current, List<RaftPeer> target, Set<String> added,
			Set<String> removed) {
		var change = VoterChange.between(current, target);
		assertEquals(added, change.added(), "added");
		assertEquals(removed, change.removed(), "removed");
		assertEquals(added.size() + removed.size(), change.size());
	}

	@Test
	void oneVoterAddedOrRemovedIsOneChange() {
		assertChange(peers("1", "2", "3"), peers("1", "2", "3", "4"), Set.of("4"), Set.of());
		assertChange(peers("1", "2", "3"), peers("1", "3"), Set.of(), Set.of("2"));
	}

	@Test
	void aReplacementInOneStepIsTwoChanges() {
		// The documented one-step replacement of round 13: --peer lists the survivors and the new node.
		assertChange(peers("1", "2", "3"), peers("1", "3", "4"), Set.of("4"), Set.of("2"));
	}

	@Test
	void aStalePeerListWithRemovePeerReAddsAnOldMember() {
		// The configuration is {1, 3, 4} (2 was replaced); --peer still lists 1, 2, 3 and removes 3.
		assertChange(peers("1", "3", "4"), peers("1", "2"), Set.of("2"), Set.of("3", "4"));
	}

	@Test
	void addressAndPriorityChangesCountNothing() {
		var moved = RaftPeer.newBuilder(peer("2")).setAddress("localhost:1").build();
		var prioritized = RaftPeer.newBuilder(peer("3")).setPriority(5).build();
		assertChange(peers("1", "2", "3"), List.of(peer("1"), moved, prioritized), Set.of(), Set.of());
	}

	@Test
	void anEqualSetInAnotherOrderCountsNothing() {
		assertChange(peers("1", "2", "3"), peers("3", "1", "2"), Set.of(), Set.of());
	}

	@Test
	void listenersAreNotVoters() {
		// A listener added or removed is no voter change: VoterChange looks at server lists only. A voter that becomes
		// a listener leaves the server list (one removal), a listener that becomes a voter enters it (one addition).
		assertChange(peers("1", "2", "3"), peers("1", "2", "3"), Set.of(), Set.of());
		assertChange(peers("1", "2", "3"), peers("1", "2"), Set.of(), Set.of("3"));
		assertChange(peers("1", "2"), peers("1", "2", "3"), Set.of("3"), Set.of());
	}

	@Test
	void theRefusalSaysWhatChangesAndHowToDoItInTwoSteps() {
		var current = peers("1", "2", "3");
		var change = VoterChange.between(current, peers("1", "3", "4"));
		var message = VoterChange.refusal(change, current);
		assertTrue(message.startsWith(VoterChange.MULTI_VOTER_REFUSAL), message);
		assertTrue(message.contains("2 voters"), message);
		assertTrue(message.contains("[4]") && message.contains("[2]"), message);
		assertTrue(message.contains("term-start"), message);
		assertTrue(message.contains("remove-peer 2") && message.contains("add-peer 4"), message);
		assertTrue(message.contains("Replacing a node"), message);
	}

	/**
	 * R51-1: the address of a member that stays, in the new voters, against its address in the configuration now. A
	 * member added or removed is no address change (its id is what changes), and nor are listeners.
	 */
	@Test
	void anAddressChangeOfAStayingMemberIsNamed() {
		var current = peers("1", "2", "3");
		var moved = RaftPeer.newBuilder(peer("2")).setAddress("10.9.9.9:1").build();
		var changes = VoterChange.addressChanges(current, List.of(peer("1"), moved, peer("4")));
		assertEquals(List.of("2 (at " + peer("2").getAddress() + " in the configuration, 10.9.9.9:1 in --peer)"),
				changes);
		assertEquals(List.of(), VoterChange.addressChanges(current, peers("1", "3", "4")));
		assertEquals(List.of(), VoterChange.addressChanges(current, peers("3", "1", "2")));
		var prioritized = RaftPeer.newBuilder(peer("3")).setPriority(5).build();
		assertEquals(List.of(), VoterChange.addressChanges(current, List.of(peer("1"), peer("2"), prioritized)));
	}

	/**
	 * R59-1: the voters whose id differs from a {@code remove-peer} id only by case, which {@code admin} refuses as a
	 * likely typo (Ratis compares ids case-sensitively). An exact voter has none, even when another voter differs from
	 * it only by case; an id that matches no voter in any case has none either (the re-run of a removal that committed).
	 */
	@Test
	void theVotersThatDifferOnlyByCaseAreNamed() {
		var current = peers("n1", "n2", "n3");
		assertEquals(Set.of("n1"), VoterChange.caseVariants("N1", current));
		assertEquals(Set.of(), VoterChange.caseVariants("n1", current));
		assertEquals(Set.of(), VoterChange.caseVariants("n9", current));
		assertEquals(Set.of(), VoterChange.caseVariants("N1", peers("n1", "N1", "n2")));
		assertEquals(List.of("AB", "Ab"), List.copyOf(VoterChange.caseVariants("ab", peers("Ab", "AB", "abc"))));
	}
}
