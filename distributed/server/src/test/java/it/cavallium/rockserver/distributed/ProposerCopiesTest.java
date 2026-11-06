package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Copies of a batch the proposer sends: once its batch is encoded, a write's entry is the batch's copy, so whatever
 * encodes it again reads the batch's bytes, never its caller's buffers (R2-LC-1); a resend of a batch whose reply was late is answered by
 * the leader's retry cache, and applies once (R2-LC-1); a caller that gives up while its batch is being encoded is
 * answered only once the encoding is done (R2-LC-2).
 */
@Timeout(120)
class ProposerCopiesTest {

	@TempDir
	Path temp;

	/**
	 * Once its batch is encoded, a write's entry is its copy in the batch (slices of the batch's immutable bytes), not
	 * its caller's: whatever later encodes it again (a commit going back to the queue after another's conflict, a copy
	 * resent alone, a write refused before the append and sent in a new batch) reads that copy. Its caller changes its
	 * buffer (to another size) after the first encoding: the entry still holds the bytes sent, and still encodes to
	 * its size.
	 */
	@Test
	void aWriteTakenIntoABatchKeepsTheBatchCopyNotItsCallersBuffer() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(Duration.ofSeconds(3));
		var metrics = new NodeMetrics("n0");
		var threads = new NodeThreads("n0");
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		var proposer = new Proposer(group, RaftSettings.client(config), config, () -> null, metrics, threads.control());
		try {
			var changing = Buf.create(8);
			for (int i = 0; i < 4; i++) {
				changing.add((byte) i);
			}
			var original = new LogEntry.Put(1, new Keys(Buf.wrap(new byte[] {2})), changing, RequestTypeId.NOTHING);
			var write = proposer.propose(original);
			var pending = livePending(proposer);
			long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			while ((pending.entry == original || pending.isEncoding()) && System.nanoTime() < until) {
				Thread.sleep(5);
			}
			var copy = assertInstanceOf(LogEntry.Put.class, pending.entry);
			assertTrue(copy != original, "the entry was replaced by its batch copy");
			changing.add((byte) 4);
			changing.set(0, (byte) 9);
			assertEquals(4, copy.value().size(), "the copy keeps the bytes sent");
			assertEquals(0, copy.value().getByte(0));
			assertEquals(pending.bodySize, LogEntryCodec.encodeEntryBody(copy).length, "and encodes again to its size");
			var error = assertThrows(ExecutionException.class, () -> write.get(10, TimeUnit.SECONDS));
			var type = assertInstanceOf(RocksDBException.class, error.getCause()).getErrorUniqueId();
			assertTrue(type == RocksDBErrorType.OUTCOME_UNKNOWN || type == RocksDBErrorType.SERVER_OVERLOADED,
					type::toString);
		} finally {
			proposer.close();
			threads.close();
			metrics.close();
		}
	}

	/** The only request {@code proposer} holds. */
	private static Proposer.Pending livePending(Proposer proposer) throws Exception {
		var field = Proposer.class.getDeclaredField("live");
		field.setAccessible(true);
		var live = (java.util.Set<?>) field.get(proposer);
		assertEquals(1, live.size());
		return (Proposer.Pending) live.iterator().next();
	}

	/**
	 * A write through a follower's proposer, whose reply the leader holds back (its apply is gated) for longer than an
	 * attempt: the proposer sends the same batch again, under the same Ratis invocation, and the leader's retry cache
	 * answers it with the first copy's reply once that comes. The merge applies once on every node.
	 */
	@Test
	void aResendOfALateBatchIsAnsweredByTheRetryCacheAndAppliesOnce() throws Exception {
		try (var cluster = new InProcessCluster(temp, 3, c -> c).startAll()) {
			var leader = cluster.awaitLeader(Duration.ofSeconds(30));
			var follower = cluster.runningNodes().stream().filter(node -> node != leader).findFirst().orElseThrow();
			var api = follower.connection().getAsyncApi(RequestContext.batch(Duration.ofSeconds(30)));
			long column = api.createColumnAsync("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true))
					.get(30, TimeUnit.SECONDS);
			cluster.awaitConverged(Duration.ofSeconds(30));
			var resends = follower.metrics().amount("writes.resends");
			long resendsBefore = resends.count();
			long indexBefore = leader.stateMachine().getLastAppliedTermIndex().getIndex();

			var gate = new CountDownLatch(1);
			leader.stateMachine().testHooks().applyGate = gate;
			CompletableFuture<?> merge;
			try {
				merge = api.mergeAsync(0, column, key(1), text("x"), RequestType.merged());
				// Past an attempt timeout (at least 3 s): the proposer sent the batch again.
				long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
				while (resends.count() == resendsBefore && System.nanoTime() < until) {
					Thread.sleep(50);
				}
				assertTrue(resends.count() > resendsBefore, "the batch was sent again");
				assertFalse(merge.isDone(), "precondition: the leader has not answered");
			} finally {
				gate.countDown();
				leader.stateMachine().testHooks().applyGate = null;
			}
			assertEquals("x", ((Buf) merge.get(30, TimeUnit.SECONDS)).toString(StandardCharsets.UTF_8));
			cluster.awaitConverged(Duration.ofSeconds(30));
			for (var node : cluster.runningNodes()) {
				Buf value = node.stateMachine().store().db().get(0, column, key(1), RequestType.current());
				assertEquals("x", value.toString(StandardCharsets.UTF_8), "merged once on " + node.config().nodeId());
			}
			assertEquals(indexBefore + 1, leader.stateMachine().getLastAppliedTermIndex().getIndex(),
					"one log entry: the copy was answered from the retry cache, not appended again");
		}
	}

	/**
	 * R2-LC-2: a caller that gives up on a write while its batch is being encoded waits for the encoding to end
	 * before it is answered; with no encoding in progress it does not wait, and the wait is bounded.
	 */
	@Test
	void aCallerGivingUpDuringTheEncodingWaitsForIt() throws Exception {
		var entry = new LogEntry.DeleteColumn(1);
		var pending = new Proposer.Pending(1, null, entry, (int) LogEntryCodec.entrySize(entry), true,
				System.nanoTime() + 1_000_000_000L, false, 0, -1);
		long start = System.nanoTime();
		pending.awaitEncoded(TimeUnit.SECONDS.toNanos(5));
		assertTrue(System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(100), "no encoding: no wait");

		pending.encodingStarted();
		assertTrue(pending.markSent());
		var waiter = Thread.ofVirtual().start(() -> pending.awaitEncoded(TimeUnit.SECONDS.toNanos(5)));
		Thread.sleep(200);
		assertTrue(waiter.isAlive(), "waits while the batch reads the entry");
		pending.encodingEnded();
		assertTrue(waiter.join(Duration.ofSeconds(1)), "goes on once the encoding ended");
		assertFalse(pending.isEncoding());

		pending.encodingStarted();
		start = System.nanoTime();
		pending.awaitEncoded(TimeUnit.MILLISECONDS.toNanos(200));
		assertTrue(System.nanoTime() - start >= TimeUnit.MILLISECONDS.toNanos(200), "bounded wait");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}
}
