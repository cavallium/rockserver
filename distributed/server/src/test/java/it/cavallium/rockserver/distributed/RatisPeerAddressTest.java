package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Closeable;
import java.io.IOException;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.util.PeerProxyMap;
import org.junit.jupiter.api.Test;

/**
 * R52-2: pins the Ratis 3.3.1 behaviour that rules out moving a member to a new address under the same id while the
 * other nodes run. A Raft server keeps one proxy per peer id ({@link PeerProxyMap}, which {@code GrpcServicesImpl}
 * and so the log appenders use). A peer of a later configuration is added with {@code computeIfAbsent}, so the
 * address first seen for an id (the node's {@code --peer} list at its start, then its configuration) stays until the
 * server closes, and a reconnect after a failure ({@code resetProxy}) uses that same address again. If this test fails
 * after a Ratis upgrade, the runbook's rule "an id keeps its address" ("One voter per change") may be relaxed.
 */
class RatisPeerAddressTest {

	@Test
	void aPeerProxyKeepsTheFirstAddressOfItsIdUntilTheMapCloses() throws IOException {
		var id = RaftPeerId.valueOf("n3");
		try (var proxies = new PeerProxyMap<AddressProxy>("test", peer -> new AddressProxy(peer.getAddress()))) {
			proxies.addRaftPeers(java.util.List.of(RaftPeer.newBuilder().setId(id).setAddress("10.0.0.3:9001").build()));
			assertEquals("10.0.0.3:9001", proxies.getProxy(id).address());

			// The configuration that re-adds n3 at its new address.
			proxies.addRaftPeers(java.util.List.of(RaftPeer.newBuilder().setId(id).setAddress("10.0.0.9:9001").build()));
			assertEquals("10.0.0.3:9001", proxies.getProxy(id).address());

			// What a failed append does before it tries again.
			proxies.resetProxy(id);
			assertEquals("10.0.0.3:9001", proxies.getProxy(id).address());
		}
	}

	private record AddressProxy(String address) implements Closeable {

		@Override
		public void close() {
		}
	}
}
