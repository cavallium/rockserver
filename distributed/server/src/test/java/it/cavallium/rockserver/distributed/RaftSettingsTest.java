package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.junit.jupiter.api.Test;

class RaftSettingsTest {

	private static RaftProperties server() {
		var peer = new PeerConfig("a", "127.0.0.1", 1, "127.0.0.1", 2);
		return RaftSettings.server(NodeConfig.defaults("a", List.of(peer), Path.of("unused")));
	}

	/** CR2-3: CommitHeartbeats triggers heartbeats through Ratis' linearizable read path with its heartbeat check. */
	@Test
	void theReadPathThatCommitHeartbeatsTriggersIsOn() {
		var properties = server();
		assertEquals(RaftServerConfigKeys.Read.Option.LINEARIZABLE, RaftServerConfigKeys.Read.option(properties));
		assertTrue(RaftServerConfigKeys.Read.leaderHeartbeatCheckEnabled(properties));
		assertTrue(properties.get(RaftServerConfigKeys.Read.LEADER_HEARTBEAT_CHECK_ENABLED_KEY) != null,
				"set explicitly, not left to Ratis' default");
	}

	/**
	 * Ratis keeps every chunk of a snapshot it sends on the heap for this long, and an out of memory error there ends the
	 * catch-up of a new member as if it had finished.
	 */
	@Test
	void aLeaderKeepsTheChunksOfASnapshotOnTheHeapForAtMost4Seconds() {
		var properties = server();
		long requestMillis = GrpcConfigKeys.Server.installSnapshotRequestTimeout(properties).toLong(TimeUnit.MILLISECONDS);
		long chunkMillis = requestMillis * GrpcConfigKeys.Server.installSnapshotRequestElementLimit(properties);
		assertTrue(chunkMillis <= 4_000, () -> "chunks stay for " + chunkMillis + " ms");
	}

	/**
	 * Ratis' refusal for a full pending-request queue is taken as an unknown outcome, so the attempts that every node
	 * keeps in flight must fit in it; and it is no longer what bounds the heap (Proposer's admission is).
	 */
	@Test
	void thePendingRequestLimitsHoldTheAttemptsOfEveryNodeAndStayNearRatisDefaults() {
		var properties = server();
		long bytes = RaftServerConfigKeys.Write.byteLimit(properties).getSize();
		// Every node's data writes, and the leader's own commits, which have permits of their own.
		assertTrue(bytes >= (RaftSettings.NODES_UNDER_PENDING_LIMIT + 1) * Proposer.ATTEMPT_BYTES_IN_FLIGHT,
				() -> bytes + " bytes");
		assertTrue(RaftSettings.NODES_UNDER_PENDING_LIMIT >= 3, "a cluster of 3 never reaches it");
		assertTrue(bytes <= 3 * RaftServerConfigKeys.Write.BYTE_LIMIT_DEFAULT.getSize(), () -> bytes + " bytes");

		int elements = RaftServerConfigKeys.Write.elementLimit(properties);
		assertEquals(RaftServerConfigKeys.Write.ELEMENT_LIMIT_DEFAULT, elements);
		// Each node has at most this many batches in flight: one lane of writes, one of commits.
		assertTrue(elements >= RaftSettings.NODES_UNDER_PENDING_LIMIT * 2 * Proposer.MAX_BATCHES_IN_FLIGHT);
	}

	/** CR-10: a cluster of more than 4 nodes gets a byte limit that holds the attempts of every node of its peer list. */
	@Test
	void thePendingByteLimitGrowsWithThePeerList() {
		var peers = new ArrayList<PeerConfig>();
		for (int i = 0; i < 7; i++) {
			peers.add(new PeerConfig("n" + i, "127.0.0.1", 9000 + 2 * i, "127.0.0.1", 9001 + 2 * i));
		}
		var config = NodeConfig.defaults("n0", peers, Path.of("unused"));
		assertEquals(7, RaftSettings.nodesUnderPendingLimit(config));
		long bytes = RaftServerConfigKeys.Write.byteLimit(RaftSettings.server(config)).getSize();
		assertEquals(8 * Proposer.ATTEMPT_BYTES_IN_FLIGHT, bytes);
		var three = NodeConfig.defaults("n0", peers.subList(0, 3), Path.of("unused"));
		assertEquals(RaftSettings.NODES_UNDER_PENDING_LIMIT, RaftSettings.nodesUnderPendingLimit(three));
		assertEquals(RaftSettings.PENDING_BYTES_LIMIT.getSize(),
				RaftServerConfigKeys.Write.byteLimit(RaftSettings.server(three)).getSize());
	}

	@Test
	void oneEntryOfTheLargestSizeFitsEveryLimitOnItsWay() {
		var properties = server();
		long entry = RaftSettings.MAX_ENTRY_BYTES;
		assertTrue(GrpcConfigKeys.messageSizeMax(properties, _ -> {}).getSize() > entry);
		assertTrue(RaftServerConfigKeys.Log.Appender.bufferByteLimit(properties).getSize() > entry);
		assertTrue(RaftServerConfigKeys.Log.writeBufferSize(properties).getSize() > entry);
		assertTrue(RaftSettings.PENDING_BYTES_LIMIT.getSize() > entry);
	}
}
