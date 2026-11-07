package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.ResourceUnavailableException;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Transaction commits on a leader whose room for attempts is full, through the real {@link Proposer} against a
 * {@link StubRaft} leader that answers each submission as the test says. A commit refused before its append for lack of
 * room, by Ratis' pending-request limits or by this node's own attempt permits, goes back to the queue and is sent again
 * ({@code retryLater}): it succeeds once there is room, and fails only at its caller's deadline, as not applied.
 */
class ProposerCommitRetryTest {

	@TempDir
	Path temp;
	private final StubRaft raft = new StubRaft();
	private NodeMetrics metrics;
	private NodeThreads threads;
	private Proposer proposer;

	@BeforeEach
	void setUp() {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(Duration.ofSeconds(10));
		metrics = new NodeMetrics("n0");
		threads = new NodeThreads("n0");
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		raft.leaderId = "n0";
		raft.leader = true;
		raft.leaderReady = true;
		proposer = new Proposer(group, RaftSettings.client(config), config, raft.serverSupplier(), metrics,
				threads.control());
	}

	@AfterEach
	void tearDown() throws Exception {
		proposer.close();
		threads.close();
		metrics.close();
	}

	private static LogEntry.Guarded commit(long key, int valueBytes) {
		var keys = new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
		return new LogEntry.Guarded(1, 0, new long[0], new long[0],
				List.of(new LogEntry.Put(1, keys, Buf.wrap(new byte[valueBytes]), RequestTypeId.NOTHING)), false);
	}

	/** The reply of a batch whose every commit applied. */
	private static RaftClientReply applied(RaftClientRequest request) {
		var results = new ArrayList<ApplyResult>();
		int count = RockserverStateMachine.decodeBatch(request.getMessage().getContent()).size();
		for (int i = 0; i < count; i++) {
			results.add(new ApplyResult.Ok(true));
		}
		return RaftClientReply.newBuilder()
				.setRequest(request)
				.setSuccess()
				.setMessage(Message.valueOf(UnsafeByteOperations.unsafeWrap(ResultCodec.encodeAll(results))))
				.build();
	}

	private static CompletableFuture<RaftClientReply> pendingLimitFull() {
		return CompletableFuture.failedFuture(new ResourceUnavailableException(
				"Failed to " + Proposer.RATIS_PENDING_LIMIT_MESSAGE + " (test)"));
	}

	/** WRu-5: Ratis' pending queue stays full (as behind a blocked apply): requeued until the deadline, not applied. */
	@Test
	void aCommitRefusedByAFullPendingQueueIsRequeuedAndFailsAtItsDeadlineAsNotApplied() throws Exception {
		var submissions = new AtomicInteger();
		raft.submit = _ -> {
			submissions.incrementAndGet();
			return pendingLimitFull();
		};
		long start = System.nanoTime();
		var result = proposer.proposeOnce(commit(1, 16), TimeUnit.MILLISECONDS.toNanos(800), null);
		var error = assertThrows(ExecutionException.class, () -> result.get(10, TimeUnit.SECONDS));
		long tookMillis = (System.nanoTime() - start) / 1_000_000;
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, cause.getErrorUniqueId(), cause.getMessage());
		assertTrue(cause.getMessage().contains("not applied"), cause.getMessage());
		assertTrue(submissions.get() >= 3, "sent " + submissions.get() + " times: not requeued");
		assertTrue(tookMillis >= 600, "failed after " + tookMillis + " ms, before its deadline");
	}

	/** WRu-5: once the queue has room again, the requeued commit is appended and answered. */
	@Test
	void aCommitRefusedByAFullPendingQueueSucceedsOnceThereIsRoom() throws Exception {
		var submissions = new AtomicInteger();
		raft.submit = request -> submissions.incrementAndGet() <= 3
				? pendingLimitFull()
				: CompletableFuture.completedFuture(applied(request));
		var result = proposer.proposeOnce(commit(2, 16), TimeUnit.SECONDS.toNanos(5), null);
		assertEquals(true, result.get(10, TimeUnit.SECONDS));
		assertEquals(4, submissions.get());
	}

	/**
	 * WRu-4: commits fill the commit lane's attempt permits ({@link Proposer#ATTEMPT_BYTES_IN_FLIGHT}) and Ratis has not
	 * answered them yet. The next commit gets no permit, goes back to the queue, is not sent while there is no room,
	 * and succeeds once the earlier ones are answered; every commit succeeds.
	 */
	@Test
	void aCommitWithoutAPermitWaitsInTheQueueAndSucceedsOnceTheLaneHasRoom() throws Exception {
		int valueBytes = 6 << 20;
		int fitting = (int) (Proposer.ATTEMPT_BYTES_IN_FLIGHT / valueBytes);
		var held = new ConcurrentLinkedQueue<CompletableFuture<RaftClientReply>>();
		var heldRequests = new ConcurrentLinkedQueue<RaftClientRequest>();
		var submissions = new AtomicInteger();
		var release = new CompletableFuture<Void>();
		raft.submit = request -> {
			submissions.incrementAndGet();
			if (release.isDone()) {
				return CompletableFuture.completedFuture(applied(request));
			}
			var reply = new CompletableFuture<RaftClientReply>();
			held.add(reply);
			heldRequests.add(request);
			return reply;
		};
		var first = new ArrayList<CompletableFuture<Object>>();
		for (int i = 0; i < fitting; i++) {
			first.add(proposer.proposeOnce(commit(10 + i, valueBytes), TimeUnit.SECONDS.toNanos(20), null));
		}
		InProcessCluster.await(Duration.ofSeconds(10), () -> held.size() == fitting);
		var waiting = proposer.proposeOnce(commit(100, valueBytes), TimeUnit.SECONDS.toNanos(20), null);
		Thread.sleep(500);
		assertEquals(fitting, submissions.get(), "a commit was sent without a permit");
		assertFalse(waiting.isDone(), "the commit without a permit ended: " + waiting);

		release.complete(null);
		var requests = new ArrayList<>(heldRequests);
		var replies = new ArrayList<>(held);
		for (int i = 0; i < replies.size(); i++) {
			replies.get(i).complete(applied(requests.get(i)));
		}
		for (var commit : first) {
			assertEquals(true, commit.get(10, TimeUnit.SECONDS));
		}
		assertEquals(true, waiting.get(10, TimeUnit.SECONDS));
		assertEquals(fitting + 1, submissions.get());
	}

	/**
	 * R21-1: a commit sent at once has no expiry timer of its own. Refused before its append for lack of room, it goes
	 * back to the queue 50 ms later, when every commit slot ({@link Proposer#MAX_BATCHES_IN_FLIGHT}) is again taken by
	 * commits that Ratis does not answer. No slot frees up to look at the queue, yet it still fails at its caller's
	 * deadline, as not applied, instead of waiting past it for an answer the caller no longer hears.
	 */
	@Test
	void aRequeuedCommitFailsAtItsDeadlineAsNotAppliedWhileNoSlotFreesUp() throws Exception {
		var refusal = new CompletableFuture<RaftClientReply>();
		var held = new ConcurrentLinkedQueue<CompletableFuture<RaftClientReply>>();
		var heldRequests = new ConcurrentLinkedQueue<RaftClientRequest>();
		var submissions = new AtomicInteger();
		raft.submit = request -> {
			if (submissions.incrementAndGet() == 1) {
				return refusal;
			}
			var reply = new CompletableFuture<RaftClientReply>();
			held.add(reply);
			heldRequests.add(request);
			return reply;
		};
		try {
			long start = System.nanoTime();
			long timeLeftNanos = TimeUnit.MILLISECONDS.toNanos(1000);
			var refused = proposer.proposeOnce(commit(1, 16), timeLeftNanos, null);
			InProcessCluster.await(Duration.ofSeconds(10), () -> submissions.get() == 1);
			var others = new ArrayList<CompletableFuture<Object>>();
			for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
				others.add(proposer.proposeOnce(commit(10 + i, 16), TimeUnit.SECONDS.toNanos(30), null));
			}
			// Every slot but the refused commit's is taken; one more commit waits in the queue.
			InProcessCluster.await(Duration.ofSeconds(10), () -> held.size() == Proposer.MAX_BATCHES_IN_FLIGHT - 1);
			refusal.completeExceptionally(new ResourceUnavailableException(
					"Failed to " + Proposer.RATIS_PENDING_LIMIT_MESSAGE + " (test)"));
			// Its slot goes to the waiting commit: all slots are taken again when it comes back to the queue.
			InProcessCluster.await(Duration.ofSeconds(10), () -> held.size() == Proposer.MAX_BATCHES_IN_FLIGHT);

			var error = assertThrows(ExecutionException.class, () -> refused.get(5, TimeUnit.SECONDS));
			long tookNanos = System.nanoTime() - start;
			var cause = assertInstanceOf(RocksDBException.class, error.getCause());
			assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, cause.getErrorUniqueId(), cause.getMessage());
			assertTrue(cause.getMessage().contains("not applied"), cause.getMessage());
			assertTrue(tookNanos < timeLeftNanos + TimeUnit.MILLISECONDS.toNanos(500),
					"failed after " + TimeUnit.NANOSECONDS.toMillis(tookNanos) + " ms");
			assertEquals(Proposer.MAX_BATCHES_IN_FLIGHT + 1, submissions.get(), "the refused commit was sent again");
			for (var other : others) {
				assertFalse(other.isDone(), "a held commit ended: " + other);
			}
		} finally {
			var requests = new ArrayList<>(heldRequests);
			var replies = new ArrayList<>(held);
			for (int i = 0; i < replies.size(); i++) {
				replies.get(i).complete(applied(requests.get(i)));
			}
		}
	}
}
