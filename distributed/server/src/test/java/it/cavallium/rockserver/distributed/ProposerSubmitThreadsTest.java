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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 * R22: which threads submit to this node's own Raft server. In Ratis 3.3.1 {@code submitClientRequestAsync} waits
 * ({@code supplyAsync(...).join()}) until the entry is in the log queue, which a disk stall fills: a submission can
 * then block for as long as the stall lasts. Through the real {@link Proposer} against a {@link StubRaft} leader whose
 * submissions block as the test says, such a submission holds neither the control plane nor the proposing caller.
 */
class ProposerSubmitThreadsTest {

	@TempDir
	Path temp;
	private final StubRaft raft = new StubRaft();
	private final CountDownLatch release = new CountDownLatch(1);
	private NodeMetrics metrics;
	private NodeThreads threads;
	private Proposer proposer;

	@BeforeEach
	void setUp() {
		metrics = new NodeMetrics("n0");
		threads = new NodeThreads("n0");
		raft.leaderId = "n0";
		raft.leader = true;
		raft.leaderReady = true;
		proposer = newProposer(Duration.ofSeconds(10));
	}

	private Proposer newProposer(Duration requestTimeout) {
		return newProposer(requestTimeout, threads.submit());
	}

	private Proposer newProposer(Duration requestTimeout, Executor submitter) {
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(requestTimeout);
		return new Proposer(group, RaftSettings.client(config), config, raft.serverSupplier(), metrics,
				threads.control(), submitter);
	}

	@AfterEach
	void tearDown() throws Exception {
		release.countDown();
		proposer.close();
		threads.close();
		metrics.close();
	}

	private static LogEntry.Put put(long key) {
		var keys = new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
		return new LogEntry.Put(1, keys, Buf.wrap(new byte[16]), RequestTypeId.NOTHING);
	}

	/** The reply of a batch whose every request applied. */
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

	/** Blocks the submitting thread until the test ends, as Ratis' append behind a full log queue does. */
	private void blockInRaft() {
		try {
			release.await(30, TimeUnit.SECONDS);
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * F1: as many batches as the control plane has threads are each refused once before the append for lack of room,
	 * and their resend then blocks inside Ratis' submit. A write that Ratis answers at once is still answered: its
	 * reply handler does not wait behind the blocked submissions, because no control thread submits to Ratis.
	 */
	@Test
	void aWriteRaftAnswersIsAnsweredWhileOtherSubmissionsBlockInRaft() throws Exception {
		int blockers = NodeThreads.CONTROL_THREADS;
		Set<Long> blockerCalls = ConcurrentHashMap.newKeySet();
		var answerNewCalls = new AtomicBoolean();
		var blocked = new AtomicInteger();
		raft.submit = request -> {
			long callId = request.getCallId();
			if (answerNewCalls.get() && !blockerCalls.contains(callId)) {
				return CompletableFuture.completedFuture(applied(request));
			}
			if (blockerCalls.add(callId)) {
				// The first attempt: refused before the append, resent 50 ms later.
				return pendingLimitFull();
			}
			blocked.incrementAndGet();
			blockInRaft();
			return pendingLimitFull();
		};
		var stuck = new ArrayList<CompletableFuture<Object>>();
		for (int i = 0; i < blockers; i++) {
			stuck.add(proposer.propose(put(i)));
		}
		InProcessCluster.await(Duration.ofSeconds(10), () -> blocked.get() == blockers);
		answerNewCalls.set(true);

		var answered = proposer.propose(put(1000));
		assertEquals(true, answered.get(2, TimeUnit.SECONDS), "the reply handler waited behind blocked submissions");
		for (var write : stuck) {
			assertFalse(write.isDone(), "a blocked write ended: " + write);
		}
	}

	/**
	 * F1b: a write whose submission blocks inside Ratis does not hold its caller: {@code propose} returns its future at
	 * once, so that the caller's own deadline (an {@code orTimeout} on that future) can still end the call.
	 */
	@Test
	void proposeReturnsWhileRaftSubmitBlocks() throws Exception {
		var blocked = new AtomicInteger();
		raft.submit = _ -> {
			blocked.incrementAndGet();
			blockInRaft();
			return pendingLimitFull();
		};
		var returned = new CompletableFuture<CompletableFuture<Object>>();
		Thread.ofVirtual().start(() -> returned.complete(proposer.propose(put(1), TimeUnit.SECONDS.toNanos(1))));
		var write = returned.get(500, TimeUnit.MILLISECONDS);
		InProcessCluster.await(Duration.ofSeconds(10), () -> blocked.get() == 1);
		assertFalse(write.isDone(), "ended while its submission is still inside Ratis: " + write);
	}

	/**
	 * F3: {@code take()} drops a request past its deadline while its expiry timer abandons it at the same moment. The
	 * whole weight of the request goes back to the byte admission exactly once: none of it stays counted for good.
	 * Deterministic: the request timeout (50 ms) is below the least time left to be sent (100 ms), so every request is
	 * dropped by the first {@code take()}, and the test's hook abandons it there as the timer would.
	 */
	@Test
	void aRequestDroppedByTakeWhileItsTimerAbandonsItGivesBackItsWholeWeight() throws Exception {
		proposer.close();
		proposer = newProposer(Duration.ofMillis(50));
		var submissions = new AtomicInteger();
		raft.submit = request -> {
			submissions.incrementAndGet();
			return CompletableFuture.completedFuture(applied(request));
		};
		var dropped = new AtomicInteger();
		proposer.onTakeDropsForTesting = pending -> {
			dropped.incrementAndGet();
			pending.abandon(RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
					"expired by the test, as by its timer"));
		};
		var write = proposer.propose(put(1));
		var error = assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertTrue(cause.isNotApplied(), cause.getMessage());
		InProcessCluster.await(Duration.ofSeconds(5), () -> dropped.get() == 1);
		long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
		while (proposer.admittedBytes() != 0 && System.nanoTime() - until < 0) {
			Thread.sleep(10);
		}
		assertEquals(0, proposer.admittedBytes(), "bytes left counted in the write admission");
		assertEquals(0, submissions.get(), "a write past its deadline was sent");
	}

	/**
	 * R22-2: on a node that does not lead, an attempt only hands the batch to the sender of remote attempts, which
	 * cannot block in Ratis' submit: once the node has seen that it does not lead, its batches and their resends no
	 * longer pass through the submit threads on their way to the sender. Here no leader is known, so each attempt is
	 * refused before the append and resent every 50 ms until the write's 1 s deadline: about 18 attempts.
	 */
	@Test
	void aFollowerSendsWithoutPassingThroughTheSubmitThreads() throws Exception {
		proposer.close();
		var submitted = new AtomicInteger();
		var submit = threads.submit();
		proposer = newProposer(Duration.ofSeconds(10), task -> {
			submitted.incrementAndGet();
			submit.execute(task);
		});
		raft.leader = false;
		raft.leaderReady = false;
		raft.leaderId = null;
		var localSubmissions = new AtomicInteger();
		raft.submit = request -> {
			localSubmissions.incrementAndGet();
			return CompletableFuture.completedFuture(applied(request));
		};
		var write = proposer.propose(put(1), TimeUnit.SECONDS.toNanos(1));
		var error = assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS));
		assertInstanceOf(RocksDBException.class, error.getCause());
		var second = proposer.propose(put(2), TimeUnit.MILLISECONDS.toNanos(500));
		assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS));
		assertEquals(0, localSubmissions.get(), "a follower submitted to its own Raft server");
		assertTrue(submitted.get() <= 1, "batches and resends went through the submit threads: " + submitted.get());
	}

	/**
	 * R22-2: a node that last saw it does not lead, and then leads, hands its batch to the sender first; that attempt
	 * finds it leading and submits on a submit thread, never on the sender's virtual threads, since a local submission
	 * can block in Ratis.
	 */
	@Test
	void aBatchSentAfterBecomingLeaderIsSubmittedOnASubmitThread() throws Exception {
		raft.leader = false;
		raft.leaderReady = false;
		raft.leaderId = null;
		raft.submit = request -> CompletableFuture.completedFuture(applied(request));
		var first = proposer.propose(put(1), TimeUnit.MILLISECONDS.toNanos(500));
		assertThrows(ExecutionException.class, () -> first.get(5, TimeUnit.SECONDS));
		var submitThreads = new ArrayList<Thread>();
		raft.submit = request -> {
			synchronized (submitThreads) {
				submitThreads.add(Thread.currentThread());
			}
			return CompletableFuture.completedFuture(applied(request));
		};
		raft.leaderId = "n0";
		raft.leader = true;
		raft.leaderReady = true;
		assertEquals(true, proposer.propose(put(2)).get(5, TimeUnit.SECONDS));
		assertEquals(true, proposer.propose(put(3)).get(5, TimeUnit.SECONDS));
		synchronized (submitThreads) {
			assertEquals(2, submitThreads.size(), submitThreads.toString());
			for (var thread : submitThreads) {
				assertFalse(thread.isVirtual(), "submitted on " + thread);
				assertTrue(thread.getName().startsWith("rockserver-n0-submit-"), "submitted on " + thread);
			}
		}
	}

	/**
	 * R22-3: an {@link Error} while a batch is encoded (an out-of-memory error on a large batch) ends the batch, as a
	 * runtime failure does: its writes are answered and its slot given back, not left waiting until the node stops.
	 */
	@Test
	void anErrorWhileEncodingFailsTheBatch() throws Exception {
		raft.submit = request -> CompletableFuture.completedFuture(applied(request));
		var thrown = new AtomicBoolean();
		proposer.beforeEncodeForTesting = () -> {
			if (thrown.compareAndSet(false, true)) {
				throw new OutOfMemoryError("thrown by the test while encoding");
			}
		};
		var write = proposer.propose(put(1));
		var error = assertThrows(ExecutionException.class, () -> write.get(2, TimeUnit.SECONDS),
				"the write was not answered after an error while encoding");
		assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(true, proposer.propose(put(2)).get(2, TimeUnit.SECONDS));
		long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
		while (proposer.admittedBytes() != 0 && System.nanoTime() - until < 0) {
			Thread.sleep(10);
		}
		assertEquals(0, proposer.admittedBytes(), "bytes left counted in the write admission");
	}

	/**
	 * R61-6: an {@link Error} of the hand-off itself (the submit pool cannot start a thread:
	 * {@code OutOfMemoryError: unable to create native thread}, which {@code -XX:+ExitOnOutOfMemoryError} does not turn
	 * into an exit) ends the batch as a refused hand-off does: its writes are answered, its slot and its bytes given
	 * back. Only {@code RejectedExecutionException} was caught: the error went up to the proposing caller, the batch
	 * stayed counted in flight (32 per lane) and its writes stayed sent, unanswered.
	 */
	@Test
	void anErrorHandingABatchToASubmitThreadFailsTheBatch() throws Exception {
		proposer.close();
		var submit = threads.submit();
		var failing = new AtomicBoolean(true);
		proposer = newProposer(Duration.ofSeconds(10), task -> {
			if (failing.get()) {
				throw new OutOfMemoryError("unable to create native thread (thrown by the test)");
			}
			submit.execute(task);
		});
		raft.submit = request -> CompletableFuture.completedFuture(applied(request));
		CompletableFuture<Object> write;
		try {
			write = proposer.propose(put(1));
		} catch (OutOfMemoryError leaked) {
			throw new AssertionError("the hand-off's error reached the proposing caller", leaked);
		}
		var error = assertThrows(ExecutionException.class, () -> write.get(2, TimeUnit.SECONDS),
				"the write was not answered after the hand-off failed");
		assertInstanceOf(RocksDBException.class, error.getCause());
		assertEquals(0, proposer.batchesInFlight(), "the batch kept its slot");
		failing.set(false);
		assertEquals(true, proposer.propose(put(2)).get(2, TimeUnit.SECONDS));
		long until = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(500);
		while (proposer.admittedBytes() != 0 && System.nanoTime() - until < 0) {
			Thread.sleep(10);
		}
		assertEquals(0, proposer.admittedBytes(), "bytes left counted in the write admission");
	}

	/**
	 * R61-6, the resend: a batch refused before the append (Ratis' pending limit) is handed to a submit thread again by
	 * a timer on the control plane. An {@link Error} of that hand-off ended the timer's task without a trace: the batch
	 * kept its slot and its writes stayed unanswered. It ends as when the hand-off is refused.
	 */
	@Test
	void anErrorHandingAResendToASubmitThreadEndsTheBatch() throws Exception {
		proposer.close();
		var submit = threads.submit();
		var failing = new AtomicBoolean();
		var failed = new AtomicInteger();
		proposer = newProposer(Duration.ofSeconds(10), task -> {
			if (failing.get()) {
				failed.incrementAndGet();
				throw new OutOfMemoryError("unable to create native thread (thrown by the test)");
			}
			submit.execute(task);
		});
		raft.submit = request -> {
			// Refused before the append: the batch is resent, through a hand-off that now fails.
			failing.set(true);
			return pendingLimitFull();
		};
		// Review of R61, 6: the Error is logged, with its text; the write's own failure does not carry it.
		var originalErr = System.err;
		var captured = new java.io.ByteArrayOutputStream();
		System.setErr(new java.io.PrintStream(new java.io.OutputStream() {
			@Override
			public void write(int b) {
				originalErr.write(b);
				synchronized (captured) {
					captured.write(b);
				}
			}

			@Override
			public void write(byte[] bytes, int offset, int length) {
				originalErr.write(bytes, offset, length);
				synchronized (captured) {
					captured.write(bytes, offset, length);
				}
			}
		}, true));
		try {
			var write = proposer.propose(put(1));
			var error = assertThrows(ExecutionException.class, () -> write.get(5, TimeUnit.SECONDS),
					"the write was not answered after the resend's hand-off failed");
			assertInstanceOf(RocksDBException.class, error.getCause());
		} finally {
			System.setErr(originalErr);
		}
		assertTrue(failed.get() >= 1, "precondition: the resend's hand-off never failed");
		String logged;
		synchronized (captured) {
			logged = captured.toString();
		}
		assertTrue(logged.lines().anyMatch(line -> line.contains("could not be handed to a thread")), logged);
		assertTrue(logged.contains("unable to create native thread (thrown by the test)"), logged);
		long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (proposer.batchesInFlight() != 0 && System.nanoTime() - until < 0) {
			Thread.sleep(10);
		}
		assertEquals(0, proposer.batchesInFlight(), "the batch kept its slot");
		failing.set(false);
		raft.submit = request -> CompletableFuture.completedFuture(applied(request));
		assertEquals(true, proposer.propose(put(2)).get(2, TimeUnit.SECONDS));
	}

	/**
	 * R23-A: two writes proposed by one thread without awaiting the first have no submission order. Batch A, taken
	 * first, is held while it encodes; batch B, taken after it, reaches Ratis and is answered first. A is still applied,
	 * not refused as a late copy: B carries a {@code completedBelow} computed when B encoded, which never passes A's
	 * sequence while A is in flight.
	 */
	@Test
	void aBatchOvertakenByALaterOneIsStillApplied() throws Exception {
		var submitted = new LinkedBlockingQueue<Request>();
		raft.submit = request -> {
			submitted.addAll(RockserverStateMachine.decodeBatch(request.getMessage().getContent()));
			return CompletableFuture.completedFuture(applied(request));
		};
		var encoding = new CountDownLatch(1);
		var releaseA = new CountDownLatch(1);
		var first = new AtomicBoolean(true);
		proposer.beforeEncodeForTesting = () -> {
			if (first.compareAndSet(true, false)) {
				encoding.countDown();
				try {
					releaseA.await(10, TimeUnit.SECONDS);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			}
		};
		var a = proposer.propose(put(1));
		assertTrue(encoding.await(5, TimeUnit.SECONDS), "batch A did not start encoding");
		var b = proposer.propose(put(1));
		assertEquals(true, b.get(5, TimeUnit.SECONDS));
		assertFalse(a.isDone(), "A was answered while it was held");
		var requestB = submitted.poll(5, TimeUnit.SECONDS);
		releaseA.countDown();
		assertEquals(true, a.get(5, TimeUnit.SECONDS));
		var requestA = submitted.poll(5, TimeUnit.SECONDS);
		assertTrue(requestA != null && requestB != null);
		assertTrue(requestA.sequence() < requestB.sequence(), "A was not taken first");
		assertTrue(requestB.completedBelow() <= requestA.sequence(),
				"B told the replicas that A completed: " + requestB + ", " + requestA);
	}

	/**
	 * R22-4: a batch taken just before the proposer starts to close, but not yet sent, is not submitted to the Raft
	 * server once it is closing. {@code close()} marks the proposer closed, then closes its client (which can wait
	 * seconds for a connection's calls to end), and only then answers the writes in flight: the test stops it between
	 * the two, as a submit thread reaching the batch meanwhile would see it. Its write is answered as stopping, since
	 * nothing of it was sent. (Once {@code close()} has answered a write, its batch no longer encodes: its sequence is
	 * below {@code completedBelow}.)
	 */
	@Test
	void aBatchTakenBeforeCloseIsNotSubmittedWhileItCloses() throws Exception {
		proposer.close();
		var held = new LinkedBlockingQueue<Runnable>();
		proposer = newProposer(Duration.ofSeconds(10), held::add);
		var submissions = new AtomicInteger();
		raft.submit = request -> {
			submissions.incrementAndGet();
			return CompletableFuture.completedFuture(applied(request));
		};
		var write = proposer.propose(put(1));
		var batch = held.poll(5, TimeUnit.SECONDS);
		assertTrue(batch != null, "no batch was handed to the submit threads");
		var closed = Proposer.class.getDeclaredField("closed");
		closed.setAccessible(true);
		closed.setBoolean(proposer, true);
		batch.run();
		assertEquals(0, submissions.get(), "a batch was submitted while the proposer was closing");
		var error = assertThrows(ExecutionException.class, () -> write.get(2, TimeUnit.SECONDS));
		var cause = assertInstanceOf(RocksDBException.class, error.getCause());
		assertTrue(cause.getMessage().contains("The node is stopping"), cause.getMessage());
	}
}
