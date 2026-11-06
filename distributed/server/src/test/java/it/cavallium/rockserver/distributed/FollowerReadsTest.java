package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Read index requests of a follower, without a Raft server. */
class FollowerReadsTest {

	@TempDir
	Path temp;

	@Test
	void burstOfReadsWithoutALeaderFailsThemAllAndLeavesNothingInFlight() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		try (var reads = new FollowerReads(new RockserverStateMachine(config, _ -> {}, metrics),
				RaftGroupId.valueOf(UUID.randomUUID()), "n0", Duration.ofSeconds(1))) {
			// No Raft division: every request fails at once, while more reads keep arriving. This used to recurse once
			// per batch, deep enough to overflow the stack and leave a request in flight forever.
			var futures = new ArrayList<CompletableFuture<Long>>();
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				var submitted = new ArrayList<java.util.concurrent.Future<CompletableFuture<Long>>>();
				for (int i = 0; i < 20_000; i++) {
					submitted.add(executor.submit(reads::readIndex));
				}
				for (var future : submitted) {
					futures.add(future.get());
				}
			}
			for (var future : futures) {
				var error = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
				assertInstanceOf(FollowerReads.NoLeaderException.class, error.getCause());
			}
			assertFalse(reads.inFlightForTesting());
			// And the next read is sent, and fails, at once.
			var late = reads.readIndex();
			assertThrows(ExecutionException.class, () -> late.get(5, TimeUnit.SECONDS));
		} finally {
			metrics.close();
		}
	}
}
