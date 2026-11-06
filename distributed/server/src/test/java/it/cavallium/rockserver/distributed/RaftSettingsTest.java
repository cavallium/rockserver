package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.junit.jupiter.api.Test;

class RaftSettingsTest {

	/**
	 * Ratis keeps every chunk of a snapshot it sends on the heap for this long, and an out of memory error there ends the
	 * catch-up of a new member as if it had finished.
	 */
	@Test
	void aLeaderKeepsTheChunksOfASnapshotOnTheHeapForAtMost4Seconds() {
		var peer = new PeerConfig("a", "127.0.0.1", 1, "127.0.0.1", 2);
		var properties = RaftSettings.server(NodeConfig.defaults("a", List.of(peer), Path.of("unused")));
		long requestMillis = GrpcConfigKeys.Server.installSnapshotRequestTimeout(properties).toLong(TimeUnit.MILLISECONDS);
		long chunkMillis = requestMillis * GrpcConfigKeys.Server.installSnapshotRequestElementLimit(properties);
		assertTrue(chunkMillis <= 4_000, () -> "chunks stay for " + chunkMillis + " ms");
	}
}
