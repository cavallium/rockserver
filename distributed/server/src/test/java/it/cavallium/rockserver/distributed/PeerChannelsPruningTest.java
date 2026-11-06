package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;

/** {@link ReplicatedNode#dropChannelsOfFormerMembers}: which channels of the node's shared cache survive a pruning. */
class PeerChannelsPruningTest {

	private static RaftPeer peer(String id, int port) {
		return RaftPeer.newBuilder().setId(id).setAddress("localhost:" + port).build();
	}

	@Test
	void aRemovedMemberLosesItsChannelAndEveryoneElseKeepsTheirs() {
		var config = NodeConfig.defaults("a", List.of(new PeerConfig("a", "localhost", 1001, "localhost", 2001),
				new PeerConfig("b", "localhost", 1002, "localhost", 2002),
				new PeerConfig("c", "localhost", 1003, "localhost", 2003)), Path.of("data"));
		var raft = new StubRaft();
		// c was removed after the start (still in the startup peer list), d added later, f removed later; e is staging.
		raft.currentPeers = List.of(peer("a", 1001), peer("b", 1002), peer("d", 1004));
		raft.previousPeers = List.of(peer("b", 1002), peer("g", 1007));
		raft.stagingFollowers = List.of(peer("e", 1005));

		assertEquals(Set.of("localhost:1001", "localhost:1002", "localhost:1003", "localhost:1004", "localhost:1005",
				"localhost:1007"), ReplicatedNode.memberAddresses(raft.division(), config));

		try (var channels = new PeerChannels()) {
			var kept = List.of("localhost:1001", "localhost:1002", "localhost:1003", "localhost:1004", "localhost:1005",
					"localhost:1007");
			var keptChannels = kept.stream().map(channels::get).toList();
			var removed = channels.get("localhost:1006");
			var moved = channels.get("127.0.0.1:1002");

			ReplicatedNode.dropChannelsOfFormerMembers(channels, raft.division(), config);

			assertTrue(removed.isShutdown(), "the removed member's channel is shut down");
			assertTrue(moved.isShutdown(), "an address no member has is shut down");
			assertEquals(kept.size(), channels.size());
			for (int i = 0; i < kept.size(); i++) {
				assertFalse(keptChannels.get(i).isShutdown(), kept.get(i));
				assertSame(keptChannels.get(i), channels.get(kept.get(i)), kept.get(i));
			}

			// A second pruning with the same members changes nothing (it runs every 200 ms).
			ReplicatedNode.dropChannelsOfFormerMembers(channels, raft.division(), config);
			for (int i = 0; i < kept.size(); i++) {
				assertSame(keptChannels.get(i), channels.get(kept.get(i)), kept.get(i));
			}
		}
	}

}
