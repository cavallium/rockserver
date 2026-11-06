package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.junit.jupiter.api.Test;

/** CR2-3: commit heartbeat triggers are coalesced, at most one in flight per commit index and a few in all. */
class CommitHeartbeatsTest {

	private final AtomicLong commit = new AtomicLong(5);
	private final List<CompletableFuture<RaftClientReply>> submitted = new ArrayList<>();
	private final CommitHeartbeats heartbeats = new CommitHeartbeats(commit::get, request -> {
		var reply = new CompletableFuture<RaftClientReply>();
		submitted.add(reply);
		return reply;
	}, RaftGroupId.valueOf(UUID.randomUUID()));

	@Test
	void oneTriggerInFlightPerCommitIndex() {
		heartbeats.send();
		heartbeats.send();
		heartbeats.send();
		assertEquals(1, submitted.size(), "the same commit index: one trigger");
		commit.set(6);
		heartbeats.send();
		assertEquals(2, submitted.size(), "a higher commit index needs a heartbeat of its own");
		// The older one ends: the newer still covers 6.
		submitted.get(0).completeExceptionally(new RuntimeException("not leader"));
		heartbeats.send();
		assertEquals(2, submitted.size());
		// Once it ended, the same commit index may trigger again.
		submitted.get(1).complete(null);
		heartbeats.send();
		assertEquals(3, submitted.size());
		assertEquals(3, heartbeats.sent());
		assertEquals(3, heartbeats.skipped());
	}

	/** Dummy reads wait for the leader's apply: under an apply lag they would pile up without a bound. */
	@Test
	void atMostAFewTriggersInFlight() {
		for (int i = 0; i < 20; i++) {
			commit.incrementAndGet();
			heartbeats.send();
		}
		assertEquals(CommitHeartbeats.MAX_IN_FLIGHT, submitted.size());
		submitted.getFirst().complete(null);
		commit.incrementAndGet();
		heartbeats.send();
		assertEquals(CommitHeartbeats.MAX_IN_FLIGHT + 1, submitted.size());
	}

	@Test
	void nothingBeforeACommitIndexOrWhenDisabled() {
		commit.set(-1);
		heartbeats.send();
		commit.set(9);
		heartbeats.enabled = false;
		heartbeats.send();
		assertEquals(0, submitted.size());
	}
}
