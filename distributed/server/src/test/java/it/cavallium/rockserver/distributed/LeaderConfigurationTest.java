package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.ReplicatedServerMain.MemberAnswer;
import it.cavallium.rockserver.distributed.ReplicatedServerMain.Membership;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftPeer;
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

	private static Membership membership(String leader, long term, List<String> voters) {
		return new Membership(leader, term, voters.stream().map(id -> RaftPeer.newBuilder().setId(id)
				.setAddress("127.0.0.1:1").build()).toList(), List.of());
	}

	/**
	 * R16-1 in {@code changeVoters}: after a refusal it fails at once only if the same leader, in the same term, answers
	 * the same members again, and then says to run the command again if the leader changes. Against another leader, or
	 * the same one in another term (the answer that hid the real leader, the original symptom), or after the members
	 * changed (a lost race), it goes on.
	 */
	@Test
	void aRefusalStandsOnlyForTheSameLeaderInTheSameTerm() {
		var refusedOn = membership("n0", 3, List.of("n0", "n1", "n2"));
		var standing = ReplicatedServerMain.standingRefusal(refusedOn, "changes a majority set",
				membership("n0", 3, List.of("n2", "n1", "n0")));
		assertNotNull(standing);
		assertTrue(standing.contains("the leader n0 (term 3) refused") && standing.contains("changes a majority set")
				&& standing.endsWith("If the leader changes, run the command again"), standing);

		assertNull(ReplicatedServerMain.standingRefusal(refusedOn, "changes a majority set",
				membership("n1", 4, List.of("n0", "n1", "n2"))), "another leader");
		assertNull(ReplicatedServerMain.standingRefusal(refusedOn, "changes a majority set",
				membership("n1", 3, List.of("n0", "n1", "n2"))), "another leader in the same term");
		assertNull(ReplicatedServerMain.standingRefusal(refusedOn, "changes a majority set",
				membership("n0", 5, List.of("n0", "n1", "n2"))), "the same leader in a later term");
		assertNull(ReplicatedServerMain.standingRefusal(refusedOn, "changes a majority set",
				membership("n0", 3, List.of("n0", "n1", "n2", "n3"))), "the members changed");
		assertNull(ReplicatedServerMain.standingRefusal(null, null, refusedOn), "no refusal");
	}

	/**
	 * The nit of the R16 review: the members are asked at once, so a member that does not answer before its request
	 * timeout delays a round by one timeout, not one per member. Every ask here waits until all are running; asked one
	 * after another, the first would never return. Answers keep the {@code --peer} order, without the null and failed ones.
	 */
	@Test
	void theMembersAreAskedAtOnce() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2),
				new PeerConfig("n1", "127.0.0.1", 3, "127.0.0.1", 4), new PeerConfig("n2", "127.0.0.1", 5, "127.0.0.1", 6),
				new PeerConfig("n3", "127.0.0.1", 7, "127.0.0.1", 8));
		var running = new CountDownLatch(peers.size());
		var answers = ReplicatedServerMain.askAll(peers, peer -> {
			running.countDown();
			try {
				if (!running.await(10, TimeUnit.SECONDS)) {
					throw new IllegalStateException("asked one after another");
				}
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
			return switch (peer.id()) {
				case "n1" -> null;
				case "n2" -> throw new IllegalStateException("down");
				default -> peer.id();
			};
		});
		assertEquals(List.of("n0", "n3"), answers);
	}
}
