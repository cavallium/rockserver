package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.ReplicatedServerMain.MemberAnswer;
import java.util.List;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

/**
 * R16-1: which leader's configuration {@code admin} settles on ({@link ReplicatedServerMain#settle}), from the answers
 * of every {@code --peer}. A deposed leader that has not stepped down yet (Ratis' leader checks its leadership about
 * once an election timeout) still answers LEADER with the configuration of its term: {@code admin} must not take it
 * because it comes first in {@code --peer}, or a valid change fails at once and the voters printed (which the
 * {@code add-peer} advice to wipe a node relies on) are stale.
 */
class LeaderConfigurationTest {

	private static RaftProtos.RaftConfigurationProto conf(List<String> voters, List<String> oldVoters) {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		for (var id : voters) {
			conf.addPeers(peer(id));
		}
		for (var id : oldVoters) {
			conf.addOldPeers(peer(id));
		}
		return conf.build();
	}

	private static RaftProtos.RaftPeerProto peer(String id) {
		return RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)).setAddress("127.0.0.1:1").build();
	}

	private static MemberAnswer leader(String id, long term, List<String> voters) {
		return new MemberAnswer(id, RaftPeerRole.LEADER, term, term, conf(voters, List.of()));
	}

	private static MemberAnswer follower(String id, long lastEntryTerm, List<String> voters) {
		return new MemberAnswer(id, RaftPeerRole.FOLLOWER, 0, lastEntryTerm, conf(voters, List.of()));
	}

	@Test
	void theLeaderOfTheHighestTermIsTakenWhateverThePeerOrder() {
		var settled = ReplicatedServerMain.settle(List.of(
				leader("n0", 3, List.of("n0", "n1", "n2")),
				leader("n1", 4, List.of("n0", "n1", "n2", "n3")),
				follower("n2", 4, List.of("n0", "n1", "n2", "n3"))));
		var membership = settled.membership();
		assertNotNull(membership, settled.waitingFor());
		assertEquals("n1", membership.leader());
		assertEquals(4, membership.term());
		assertEquals(List.of("n0", "n1", "n2", "n3"), List.copyOf(VoterChange.ids(membership.servers())));
	}

	/** The new leader did not answer, but a member's log shows its term: the deposed leader's answer is not settled. */
	@Test
	void aLeaderBelowATermAnotherMemberShowsIsNotTaken() {
		var settled = ReplicatedServerMain.settle(List.of(
				leader("n0", 3, List.of("n0", "n1", "n2")),
				follower("n2", 4, List.of("n0", "n1", "n2", "n3"))));
		assertNull(settled.membership(), () -> "settled on " + settled.membership());
		assertTrue(settled.waitingFor().contains("term 4"), settled.waitingFor());
	}

	/** The leader of the highest term is in a change: waited for, not replaced by the deposed one's settled answer. */
	@Test
	void aChangeUnderWayOnTheLeaderOfTheHighestTermIsWaitedFor() {
		var settled = ReplicatedServerMain.settle(List.of(
				leader("n0", 3, List.of("n0", "n1", "n2")),
				new MemberAnswer("n1", RaftPeerRole.LEADER, 4, 4, conf(List.of("n0", "n1", "n3"), List.of("n0", "n1",
						"n2")))));
		assertNull(settled.membership(), () -> "settled on " + settled.membership());
		assertTrue(settled.waitingFor().contains("change under way on leader n1"), settled.waitingFor());
	}

	@Test
	void aSingleLeaderIsTaken() {
		var settled = ReplicatedServerMain.settle(List.of(
				follower("n0", 2, List.of("n0", "n1", "n2")),
				leader("n1", 2, List.of("n0", "n1", "n2"))));
		assertNotNull(settled.membership(), settled.waitingFor());
		assertEquals("n1", settled.membership().leader());
	}

	@Test
	void noLeaderIsWaitedFor() {
		var settled = ReplicatedServerMain.settle(List.of(follower("n0", 2, List.of("n0", "n1", "n2"))));
		assertNull(settled.membership());
		assertTrue(settled.waitingFor().contains("a leader"), settled.waitingFor());
	}
}
