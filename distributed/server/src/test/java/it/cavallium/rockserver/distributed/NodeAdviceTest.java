package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

/**
 * The advice a node logs about itself agrees with the runbooks: R53-5, a removed id comes back only at its old address,
 * and a member moves under a new id (R52-2, "An id keeps its address" in runbooks.md).
 */
class NodeAdviceTest {

	@Test
	void theRemovedNodeAdviceSaysAnIdComesBackOnlyAtItsOldAddress() {
		assertReusedIdRule(ReplicatedNode.REMOVED_ADVICE);
	}

	@Test
	void theNoMajorityAdviceSaysAnIdComesBackOnlyAtItsOldAddress() {
		assertReusedIdRule(ReplicatedNode.NO_MAJORITY_ADVICE);
	}

	/** R53-5 review: the NOT_ADDED advice covers re-adding a removed id too ("added and removed long ago"). */
	@Test
	void theNotAddedAdviceSaysAnIdComesBackOnlyAtItsOldAddress() {
		assertReusedIdRule(ReplicatedNode.NOT_ADDED_ADVICE);
	}

	private static void assertReusedIdRule(String advice) {
		assertTrue(advice.contains("at its old address"), advice);
		assertTrue(advice.contains("under a new id"), advice);
	}
}
