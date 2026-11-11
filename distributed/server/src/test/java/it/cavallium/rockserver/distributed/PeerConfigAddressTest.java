package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** R61-15: two different members that share a Raft address or an API address are refused. */
class PeerConfigAddressTest {

	@Test
	void membersWithTheirOwnAddressesPass() {
		var peers = List.of(PeerConfig.parse("n1=h1:1/h1:2"), PeerConfig.parse("n2=h1:3/h1:4"),
				PeerConfig.parse("n3=h2:1/h2:2"));
		assertNull(PeerConfig.sharedAddress(peers));
		// a member's own Raft and API addresses are not compared with each other, nor one's Raft with another's API
		assertNull(PeerConfig.sharedAddress(List.of(PeerConfig.parse("n1=h1:1/h1:2"), PeerConfig.parse("n2=h1:2/h1:1"))));
		NodeConfig.defaults("n1", peers, Path.of("."));
	}

	@Test
	void aSharedRaftAddressNamesBothIds() {
		var peers = List.of(PeerConfig.parse("n1=h1:1/h1:2"), PeerConfig.parse("n2=H1:1/h1:4"));
		var shared = PeerConfig.sharedAddress(peers);
		assertTrue(shared.contains("n1") && shared.contains("n2") && shared.contains("Raft"), shared);
		var ex = assertThrows(IllegalArgumentException.class, () -> NodeConfig.defaults("n1", peers, Path.of(".")));
		assertTrue(ex.getMessage().contains("n1") && ex.getMessage().contains("n2"), ex.getMessage());
	}

	@Test
	void aSharedApiAddressNamesBothIds() {
		var peers = List.of(PeerConfig.parse("n1=h1:1/h1:2"), PeerConfig.parse("n2=h1:3/h1:2"));
		var shared = PeerConfig.sharedAddress(peers);
		assertTrue(shared.contains("n1") && shared.contains("n2") && shared.contains("API"), shared);
		assertThrows(IllegalArgumentException.class, () -> NodeConfig.defaults("n1", peers, Path.of(".")));
	}

	@Test
	void withDistinctAddressesKeepsTheFirstOfASharedAddress() {
		var peers = List.of(PeerConfig.parse("n1=h1:1/h1:2"), PeerConfig.parse("n2=h1:1/h1:4"),
				PeerConfig.parse("n3=h1:5/h1:6"));
		assertEquals(List.of("n1", "n3"), PeerConfig.withDistinctAddresses(peers).stream().map(PeerConfig::id).toList());
	}
}
