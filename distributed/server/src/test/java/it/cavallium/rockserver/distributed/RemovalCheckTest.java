package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.RemovalCheck.Answer;
import it.cavallium.rockserver.distributed.RemovalCheck.PeerConfiguration;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;

/** R4-6: how a node reads its members' configurations to tell whether it was removed. */
class RemovalCheckTest {

	@Test
	void onlyANewerCommittedConfigurationWithoutTheNodeSaysRemoved() {
		assertEquals(Answer.REMOVED, RemovalCheck.judge(new PeerConfiguration(9, true, List.of("n0", "n2")), "n1", 5));
		assertEquals(Answer.MEMBER, RemovalCheck.judge(new PeerConfiguration(9, true, List.of("n0", "n1")), "n1", 5));
		// Not newer than its own: the node knows better (it was added again since).
		assertEquals(Answer.MEMBER, RemovalCheck.judge(new PeerConfiguration(5, true, List.of("n0", "n2")), "n1", 5));
		// Not committed: the next leader may drop it. Asked again.
		assertEquals(Answer.PENDING, RemovalCheck.judge(new PeerConfiguration(9, false, List.of("n0", "n2")), "n1", 5));
		// A member that has no group yet.
		assertEquals(Answer.PENDING, RemovalCheck.judge(null, "n1", 5));
	}

	/**
	 * R3-G2b-4: for a node whose own configuration is a joint one that leaves it out, a committed answer without it at
	 * that index or later says removed, one that holds it says member, and one from before says nothing.
	 */
	@Test
	void aJointConfigurationThatLeavesTheNodeOutIsJudgedByWhetherItWasCommitted() {
		// The same joint configuration, committed: its removal goes on.
		assertEquals(Answer.REMOVED, RemovalCheck.judge(new PeerConfiguration(7, true, List.of("n0", "n2")), "n1", 7, true));
		assertEquals(Answer.REMOVED, RemovalCheck.judge(new PeerConfiguration(9, true, List.of("n0", "n2")), "n1", 7, true));
		// The joint configuration was dropped: the member's committed one, older, still holds the node.
		assertEquals(Answer.MEMBER, RemovalCheck.judge(new PeerConfiguration(3, true, List.of("n0", "n1", "n2")), "n1", 7,
				true));
		// A member behind this node's log, from before it was added.
		assertEquals(Answer.NONE, RemovalCheck.judge(new PeerConfiguration(3, true, List.of("n0", "n2")), "n1", 7, true));
		assertEquals(Answer.PENDING, RemovalCheck.judge(new PeerConfiguration(7, false, List.of("n0", "n2")), "n1", 7, true));
		// Without the joint case, the same committed answer at the node's own index says member.
		assertEquals(Answer.MEMBER, RemovalCheck.judge(new PeerConfiguration(7, true, List.of("n0", "n2")), "n1", 7));
	}

	/** Without answers, a node whose own joint configuration leaves it out is not kept: it is refused as before. */
	@Test
	void unreachableMembersDoNotKeepANodeOutOfItsNewHalf() throws Exception {
		int port;
		try (var socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		assertNull(RemovalCheck.keptBy("n1", RaftGroupId.valueOf(UUID.randomUUID()), 7, 2,
				Map.of("n0", "127.0.0.1:" + port, "n2", "127.0.0.1:" + port), Duration.ofSeconds(5)));
	}

	@Test
	void aMajorityOfTheKnownConfigurationDecides() {
		assertTrue(RemovalCheck.decide(3, 2, 0));
		assertNull(RemovalCheck.decide(3, 1, 1), "one more may say so");
		assertFalse(RemovalCheck.decide(3, 1, 0), "one of three is not a majority");
		assertFalse(RemovalCheck.decide(3, 0, 1));
		assertTrue(RemovalCheck.decide(5, 3, 1));
		assertFalse(RemovalCheck.decide(5, 2, 0));
	}

	/** Members that cannot be reached do not count, and do not hold the start up for the whole budget. */
	@Test
	void unreachableMembersLetTheNodeStartAtOnce() throws Exception {
		int port;
		try (var socket = new ServerSocket(0)) {
			port = socket.getLocalPort();
		}
		long start = System.nanoTime();
		var removedBy = RemovalCheck.removedBy("n1", RaftGroupId.valueOf(UUID.randomUUID()), 5, 3,
				Map.of("n0", "127.0.0.1:" + port, "n2", "127.0.0.1:" + port), Duration.ofSeconds(20));
		assertNull(removedBy);
		assertTrue(System.nanoTime() - start < Duration.ofSeconds(10).toNanos());
	}
}
