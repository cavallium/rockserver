package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
			var futures = new ArrayList<CompletableFuture<ReadBarrier.ReadIndex>>();
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				var submitted = new ArrayList<java.util.concurrent.Future<CompletableFuture<ReadBarrier.ReadIndex>>>();
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

	/**
	 * Deterministic version of the case above. A first request is in flight to a leader that never answers, and a
	 * second read waits for the next request. The leader is then forgotten, so every later request fails at once, and
	 * every read is made from the completion of the previous one, while the reader is still finishing its batch: the
	 * requests chain 50,000 deep. The loop of {@code send} handles them on one stack frame; the recursion it replaced
	 * overflowed the stack here, which ended the chain with a request left in flight.
	 */
	@Test
	void aChainOfReadsEachMadeWhenThePreviousFailsDoesNotGrowTheStack() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		var raft = new StubRaft();
		raft.installInto(stateMachine);
		// Accepts connections and never answers: the first request waits for its timeout.
		try (var silent = new java.net.ServerSocket(0, 50, java.net.InetAddress.getLoopbackAddress());
				var reads = new FollowerReads(stateMachine, RaftGroupId.valueOf(UUID.randomUUID()), "n0",
						Duration.ofSeconds(2))) {
			raft.leaderId = "n1";
			raft.currentPeers = List.of(org.apache.ratis.protocol.RaftPeer.newBuilder().setId("n1")
					.setAddress("127.0.0.1:" + silent.getLocalPort()).build());
			var first = reads.readIndex();
			var second = reads.readIndex();
			assertFalse(first.isDone());
			assertFalse(second.isDone());
			raft.leaderId = null;

			int depth = 50_000;
			var completed = new java.util.concurrent.atomic.AtomicInteger();
			var done = new CompletableFuture<Void>();
			var chain = new java.util.function.Consumer<CompletableFuture<ReadBarrier.ReadIndex>>() {
				@Override
				public void accept(CompletableFuture<ReadBarrier.ReadIndex> read) {
					read.whenComplete((_, error) -> {
						if (!(error instanceof FollowerReads.NoLeaderException)) {
							done.completeExceptionally(new AssertionError("unexpected end of read " + completed.get(), error));
							return;
						}
						if (completed.incrementAndGet() == depth) {
							done.complete(null);
						} else {
							// From inside the completion: the reader is still finishing the batch of this read.
							accept(reads.readIndex());
						}
					});
				}
			};
			chain.accept(second);
			done.get(30, TimeUnit.SECONDS);
			assertEquals(depth, completed.get());
			assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
			assertFalse(reads.inFlightForTesting());
		} finally {
			metrics.close();
		}
	}
}
