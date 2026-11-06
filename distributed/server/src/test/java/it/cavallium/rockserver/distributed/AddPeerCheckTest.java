package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

/** CR-19: what {@code add-peer} waits for among the members before it changes the configuration. */
class AddPeerCheckTest {

	private static final NodeState READY = new NodeState(10, false, false, false, "cluster");

	@Test
	void aMajorityOfTheMembersMustAnswer() {
		var none = ReplicatedServerMain.memberNotReady(List.of(), 3);
		assertNotNull(none, "no member answered: nothing is known");
		assertTrue(none.contains("0 of 3"), none);
		assertNotNull(ReplicatedServerMain.memberNotReady(List.of(READY), 3));
		assertNull(ReplicatedServerMain.memberNotReady(List.of(READY, READY), 3));
		assertNotNull(ReplicatedServerMain.memberNotReady(List.of(READY), 2));
		assertNull(ReplicatedServerMain.memberNotReady(List.of(READY, READY), 2));
		assertNull(ReplicatedServerMain.memberNotReady(List.of(READY), 1));
	}

	@Test
	void theMembersThatAnswerMustBeReady() {
		var notInitialized = new NodeState(0, false, false, false, null);
		assertNotNull(ReplicatedServerMain.memberNotReady(List.of(READY, notInitialized), 3));
		var seededWithoutSnapshot = new NodeState(10, true, false, false, "cluster");
		assertNotNull(ReplicatedServerMain.memberNotReady(List.of(READY, seededWithoutSnapshot), 3));
	}
}
