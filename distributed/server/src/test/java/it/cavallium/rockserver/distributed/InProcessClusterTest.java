package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** R9-9: the configuration of a node that joins, without starting any node. */
class InProcessClusterTest {

	@TempDir
	Path root;

	@Test
	void aSecondJoinListsTheFirstJoinedNodeAndTakesNoConfiguredPort() throws Exception {
		var cluster = new InProcessCluster(root, 3, config -> config);
		// No node runs, as when they are stopped: none holds its ports. Several rounds, the ports being chosen at random.
		for (int round = 0; round < 20; round++) {
			cluster.addJoining("j" + round);
		}
		var ids = cluster.ids();
		var ports = new HashSet<Integer>();
		for (var id : ids) {
			var self = cluster.config(id).self();
			assertTrue(ports.add(self.raftPort()), id + "'s Raft port is its own");
			assertTrue(ports.add(self.apiPort()), id + "'s API port is its own");
		}
		var second = cluster.config("j1");
		assertEquals(List.of("n0", "n1", "n2", "j0", "j1"), second.peers().stream().map(PeerConfig::id).toList());
		var last = cluster.config("j19");
		assertEquals(ids, last.peers().stream().map(PeerConfig::id).toList());
		for (var peer : last.peers()) {
			assertEquals(cluster.config(peer.id()).self(), peer, "the address of " + peer.id());
		}
	}
}
