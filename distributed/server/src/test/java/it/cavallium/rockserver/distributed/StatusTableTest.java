package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.ReplicatedServerMain.MemberStatus;
import it.cavallium.rockserver.distributed.ReplicatedServerMain.Style;
import java.util.List;
import org.junit.jupiter.api.Test;

/** The table that {@code admin status} prints on a terminal. */
class StatusTableTest {

	private static PeerConfig peer(int n) {
		return new PeerConfig("n" + n, "10.2.0." + (146 + n), 9001, "10.2.0." + (146 + n), 5333);
	}

	private static MemberStatus member(int n, String role, long applied, long commit) {
		return new MemberStatus(peer(n), role, applied, commit, "", null);
	}

	@Test
	void aHealthyClusterIsOneVerdictAndOneRowPerMember() {
		var table = ReplicatedServerMain.statusTable(List.of(member(1, "FOLLOWER", 48903076, 48903601),
				member(2, "LEADER", 48903606, 48903606), member(3, "FOLLOWER", 48902798, 48903603)), new Style(false));
		assertEquals("""
				HEALTHY  leader n2, 3 of 3 members answer

				NODE  ROLE         APPLIED      COMMIT  BEHIND  RAFT ADDRESS
				n1    follower  48,903,076  48,903,601     530  10.2.0.147:9001
				n2    leader    48,903,606  48,903,606       0  10.2.0.148:9001
				n3    follower  48,902,798  48,903,603     808  10.2.0.149:9001
				""", table);
	}

	@Test
	void aMemberThatDoesNotAnswerIsNamedWithItsReason() {
		var table = ReplicatedServerMain.statusTable(List.of(member(1, "LEADER", 10, 10), member(2, "FOLLOWER", 9, 10),
				MemberStatus.unreachable(peer(3), "no answer within 3000 ms")), new Style(false));
		assertTrue(table.startsWith("DEGRADED  leader n1, only 2 of 3 members answer\n"), table);
		assertTrue(table.contains("n3    unreachable"), table);
		assertTrue(table.endsWith("n3: no answer within 3000 ms\n"), table);
	}

	@Test
	void withoutAMajorityTheVerdictSaysThatNothingIsWritten() {
		var table = ReplicatedServerMain.statusTable(List.of(member(1, "CANDIDATE", 10, 10),
				MemberStatus.unreachable(peer(2), "down"), MemberStatus.unreachable(peer(3), "down")), new Style(false));
		assertTrue(table.startsWith("NO QUORUM  only 1 of 3 members answer: the cluster accepts no write\n"), table);
	}

	@Test
	void colorsAreOnlyEscapesAroundTheSameText() {
		var members = List.of(member(1, "LEADER", 10, 10), member(2, "FOLLOWER", 10, 10), member(3, "FOLLOWER", 10, 10));
		var plain = ReplicatedServerMain.statusTable(members, new Style(false));
		var colored = ReplicatedServerMain.statusTable(members, new Style(true));
		assertFalse(plain.contains("\u001b"), plain);
		assertEquals(plain, colored.replaceAll("\u001b\\[[0-9;]*m", ""));
	}

	@Test
	void theHelpNamesEveryAdminCommand() {
		var help = ReplicatedServerMain.adminHelp(new Style(false));
		for (var command : List.of("status", "transfer-leader ID", "snapshot ID", "checksum", "backup NAME",
				"backup-status NAME INDEX", "add-peer ID=RAFT/API", "remove-peer ID", "help")) {
			assertTrue(help.contains("\n  " + command + " "), command + " in\n" + help);
		}
		assertFalse(help.contains("\u001b"), help);
	}

	@Test
	void theOutcomeOfALineIsMarkedOnATerminal() {
		ReplicatedServerMain.Ui.start(true, false, new Style(false));
		try {
			assertTrue(ReplicatedServerMain.Ui.decorate("OK").startsWith("✓ OK  ("), ReplicatedServerMain.Ui.decorate("OK"));
			assertEquals("✗ FAILED: n2 differs", ReplicatedServerMain.Ui.decorate("FAILED: n2 differs"));
			assertEquals("! WARNING: two voters change", ReplicatedServerMain.Ui.decorate("WARNING: two voters change"));
			assertEquals("· checksum at log index 7", ReplicatedServerMain.Ui.decorate("checksum at log index 7"));
		} finally {
			ReplicatedServerMain.Ui.start(false, false, new Style(false));
		}
	}

	@Test
	void elapsedTimeReadsLikeAClock() {
		assertEquals("250 ms", ReplicatedServerMain.Ui.elapsed(250_000_000L));
		assertEquals("42 s", ReplicatedServerMain.Ui.elapsed(42_000_000_000L));
		assertEquals("49 min 12 s", ReplicatedServerMain.Ui.elapsed(2_952_000_000_000L));
		assertEquals("2 h 5 min", ReplicatedServerMain.Ui.elapsed(7_500_000_000_000L));
	}
}
