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

	/**
	 * Review of R65: one IP address has several spellings (an IPv6 address written out or shortened, with or without
	 * brackets), and they are one address. Host names are compared as written: nothing is resolved.
	 */
	@Test
	void spellingsOfOneIpAddressAreOneAddress() {
		var raft = PeerConfig.sharedAddress(List.of(PeerConfig.parse("n1=[::1]:1/h1:2"),
				PeerConfig.parse("n2=[0:0:0:0:0:0:0:1]:1/h1:4")));
		assertTrue(raft != null && raft.contains("n1") && raft.contains("n2") && raft.contains("Raft"), String.valueOf(raft));
		var api = PeerConfig.sharedAddress(List.of(PeerConfig.parse("n1=h1:1/[2001:DB8::10]:2"),
				PeerConfig.parse("n2=h1:3/[2001:db8:0:0:0:0:0:10]:2")));
		assertTrue(api != null && api.contains("API"), String.valueOf(api));
		assertEquals(List.of("n1"), PeerConfig.withDistinctAddresses(List.of(PeerConfig.parse("n1=[::1]:1/h1:2"),
				PeerConfig.parse("n2=[0:0:0:0:0:0:0:1]:1/h1:4"))).stream().map(PeerConfig::id).toList());
		// Different addresses, and names that only look alike, stay different.
		assertNull(PeerConfig.sharedAddress(List.of(PeerConfig.parse("n1=[::1]:1/h1:2"), PeerConfig.parse("n2=[::2]:1/h1:4"),
				PeerConfig.parse("n3=h1:1/h3:2"), PeerConfig.parse("n4=h1.example:1/h4:2"))));
	}
}
