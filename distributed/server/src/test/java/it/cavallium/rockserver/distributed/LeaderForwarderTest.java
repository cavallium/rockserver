package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Leader lookup of the forwarder, without a Raft server. */
class LeaderForwarderTest {

	@TempDir
	Path temp;

	@Test
	void closingFailsTheCallsStillWaitingForALeader() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		try {
			// Not started: no Raft division, so no leader is known.
			var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
			var forwarder = new LeaderForwarder(config);
			var pending = forwarder.leader(stateMachine);
			assertFalse(pending.isDone());
			forwarder.close();
			var error = assertThrows(ExecutionException.class, () -> pending.get(1, TimeUnit.SECONDS));
			var cause = assertInstanceOf(RocksDBException.class, error.getCause());
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, cause.getErrorUniqueId());

			var late = forwarder.leader(stateMachine);
			assertThrows(ExecutionException.class, () -> late.get(1, TimeUnit.SECONDS), "no wait after close");
		} finally {
			metrics.close();
		}
	}
}
