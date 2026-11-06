package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The proposer's queue and answers, without a Raft server: every attempt fails as if no leader were known. */
class ProposerQueueTest {

	@TempDir
	Path temp;

	/**
	 * A request whose caller's deadline comes while it waits for a batch slot leaves the admission at once, except its
	 * small overhead, and its body is dropped although it stays in the queue until a slot frees up.
	 */
	@Test
	void anAbandonedRequestGivesBackItsBodyAtOnceAndTheRestWhenItLeavesTheQueue() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(Duration.ofSeconds(3));
		var metrics = new NodeMetrics("n0");
		var threads = new NodeThreads("n0");
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		var proposer = new Proposer(group, RaftSettings.client(config), config, () -> null, metrics, threads.control());
		try {
			var entry = new LogEntry.VerifyColumn("x".repeat(4096), 1);
			long weight = LogEntryCodec.encodeEntryBody(entry).length + Proposer.REQUEST_OVERHEAD_BYTES;
			// Every batch slot is taken by a batch resent until the node's request timeout.
			var stuck = new ArrayList<CompletableFuture<Object>>();
			for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
				stuck.add(proposer.propose(entry));
			}
			assertEquals(Proposer.MAX_BATCHES_IN_FLIGHT * weight, proposer.admittedBytes());
			var queued = proposer.propose(entry, TimeUnit.MILLISECONDS.toNanos(300));
			var error = assertThrows(ExecutionException.class, () -> queued.get(5, TimeUnit.SECONDS));
			var cause = assertInstanceOf(RocksDBException.class, error.getCause());
			assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, cause.getErrorUniqueId());
			// Its body is gone; its overhead stays counted while it is still in the queue.
			assertEquals(Proposer.MAX_BATCHES_IN_FLIGHT * weight + Proposer.REQUEST_OVERHEAD_BYTES,
					proposer.admittedBytes());
			for (var write : stuck) {
				assertThrows(ExecutionException.class, () -> write.get(10, TimeUnit.SECONDS));
			}
			// The slots freed up, and the next batch took the abandoned request out of the queue.
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (proposer.admittedBytes() != 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, proposer.admittedBytes());
		} finally {
			proposer.close();
			threads.close();
			metrics.close();
		}
	}

	/**
	 * A caller that changes a queued write's buffers makes its batch fail to encode: nothing of it is sent, that write
	 * fails as invalid, and the others of the batch go back to the queue, are sent, and end as usual. Every batch slot
	 * and every admitted byte is given back.
	 */
	@Test
	void aWriteWhoseBuffersChangeWhileQueuedFailsAloneAsInvalid() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(Duration.ofSeconds(3));
		var metrics = new NodeMetrics("n0");
		var threads = new NodeThreads("n0");
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		var proposer = new Proposer(group, RaftSettings.client(config), config, () -> null, metrics, threads.control());
		try {
			// Every batch slot is taken, for about a second.
			var stuck = new ArrayList<CompletableFuture<Object>>();
			for (int i = 0; i < Proposer.MAX_BATCHES_IN_FLIGHT; i++) {
				stuck.add(proposer.propose(new LogEntry.VerifyColumn("x", 1), TimeUnit.SECONDS.toNanos(1)));
			}
			var changing = Buf.create(8);
			for (int i = 0; i < 4; i++) {
				changing.add((byte) i);
			}
			var changed = proposer.propose(new LogEntry.Put(1, new Keys(Buf.wrap(new byte[] {1})), changing,
					RequestTypeId.NOTHING));
			var unchanged = proposer.propose(new LogEntry.Put(1, new Keys(Buf.wrap(new byte[] {2})),
					Buf.wrap(new byte[] {1, 2, 3, 4}), RequestTypeId.NOTHING));
			// Changed while both wait in the queue, before the batch that takes them is encoded.
			changing.add((byte) 4);

			var error = assertThrows(ExecutionException.class, () -> changed.get(10, TimeUnit.SECONDS));
			assertEquals(RocksDBErrorType.INVALID_REQUEST,
					assertInstanceOf(RocksDBException.class, error.getCause()).getErrorUniqueId());
			// The other one was sent: with no Raft server here, its attempts end without an answer, not as invalid.
			error = assertThrows(ExecutionException.class, () -> unchanged.get(10, TimeUnit.SECONDS));
			var type = assertInstanceOf(RocksDBException.class, error.getCause()).getErrorUniqueId();
			assertTrue(type == RocksDBErrorType.OUTCOME_UNKNOWN || type == RocksDBErrorType.SERVER_OVERLOADED,
					type::toString);
			for (var write : stuck) {
				assertThrows(ExecutionException.class, () -> write.get(10, TimeUnit.SECONDS));
			}
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while ((proposer.batchesInFlight() != 0 || proposer.admittedBytes() != 0) && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			assertEquals(0, proposer.batchesInFlight());
			assertEquals(0, proposer.admittedBytes());
		} finally {
			proposer.close();
			threads.close();
			metrics.close();
		}
	}

	/** Only the leader stamps: a proposal that carries a stamp would make an entry that followers cannot decode. */
	@Test
	void theLeaderRefusesAProposalThatAlreadyCarriesAStamp() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		try {
			var batch = LogEntryCodec.encodeBatch(1, 0, new long[] {0},
					List.of(LogEntryCodec.encodeEntryBody(new LogEntry.VerifyColumn("x", 1))));
			var stamped = new byte[LogEntryCodec.stamp(0).length + batch.length];
			System.arraycopy(LogEntryCodec.stamp(42), 0, stamped, 0, LogEntryCodec.stamp(42).length);
			System.arraycopy(batch, 0, stamped, LogEntryCodec.stamp(42).length, batch.length);
			var request = org.apache.ratis.protocol.RaftClientRequest.newBuilder()
					.setClientId(org.apache.ratis.protocol.ClientId.randomId())
					.setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf("n0"))
					.setGroupId(RaftGroupId.valueOf(UUID.randomUUID()))
					.setCallId(1)
					.setMessage(org.apache.ratis.protocol.Message.valueOf(
							org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(stamped)))
					.setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType())
					.build();
			var transaction = stateMachine.startTransaction(request);
			assertTrue(transaction.getException() != null
					&& transaction.getException().getMessage().contains("leader stamp"),
					() -> String.valueOf(transaction.getException()));
		} finally {
			stateMachine.close();
			metrics.close();
		}
	}

	/**
	 * The leader starts a batch its own proposer offered from the offered requests, only for the very content they
	 * were encoded into, and takes the offer out either way; without an offer, or after a withdrawal, it decodes.
	 */
	@Test
	void theLeaderTakesAnOfferedBatchOnlyForItsOwnContent() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		var metrics = new NodeMetrics("n0");
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		try {
			var clientId = org.apache.ratis.protocol.ClientId.randomId();
			// Genesis needs no store, which this state machine has not opened.
			var entry = new LogEntry.Genesis("cluster", false);
			var encoded = LogEntryCodec.encodeBatch(1, 0, new long[] {0}, null, List.of(entry),
					LogEntryCodec.entrySize(entry), true);
			var content = org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations.unsafeWrap(
					encoded.bytes());

			stateMachine.offerLocal(clientId, 1, content, encoded.requests());
			assertNull(stateMachine.startTransaction(request(clientId, 1, content)).getException());
			assertEquals(1, stateMachine.localProposalsTaken());
			assertEquals(0, stateMachine.localProposalsOffered());

			// Equal bytes, but not the content offered: decoded, and the offer is gone.
			stateMachine.offerLocal(clientId, 2, content, encoded.requests());
			var copy = org.apache.ratis.thirdparty.com.google.protobuf.ByteString.copyFrom(encoded.bytes());
			assertNull(stateMachine.startTransaction(request(clientId, 2, copy)).getException());
			assertEquals(1, stateMachine.localProposalsTaken());
			assertEquals(0, stateMachine.localProposalsOffered());

			// Withdrawn before Ratis started it (a refusal, a retry-cache answer): decoded.
			stateMachine.offerLocal(clientId, 3, content, encoded.requests()).withdraw();
			assertEquals(0, stateMachine.localProposalsOffered());
			assertNull(stateMachine.startTransaction(request(clientId, 3, content)).getException());
			assertEquals(1, stateMachine.localProposalsTaken());

			// Another invocation does not take it; withdrawing the offer leaves nothing behind.
			var offered = stateMachine.offerLocal(clientId, 4, content, encoded.requests());
			assertNull(stateMachine.startTransaction(request(clientId, 5, content)).getException());
			assertEquals(1, stateMachine.localProposalsOffered());
			offered.withdraw();
			assertEquals(0, stateMachine.localProposalsOffered());
			assertEquals(1, stateMachine.localProposalsTaken());
		} finally {
			stateMachine.close();
			metrics.close();
		}
	}

	private static org.apache.ratis.protocol.RaftClientRequest request(org.apache.ratis.protocol.ClientId clientId,
			long callId, org.apache.ratis.thirdparty.com.google.protobuf.ByteString content) {
		return org.apache.ratis.protocol.RaftClientRequest.newBuilder()
				.setClientId(clientId)
				.setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf("n0"))
				.setGroupId(RaftGroupId.valueOf(UUID.randomUUID()))
				.setCallId(callId)
				.setMessage(org.apache.ratis.protocol.Message.valueOf(content))
				.setType(org.apache.ratis.protocol.RaftClientRequest.writeRequestType())
				.build();
	}

	private static Proposer.Pending pending() {
		var entry = new LogEntry.DeleteColumn(1);
		return new Proposer.Pending(1, null, entry, (int) LogEntryCodec.entrySize(entry), true,
				System.nanoTime() + 1_000_000_000L, false, 0, -1);
	}

	private static RocksDBException exception(Proposer.Pending pending) {
		var error = assertThrows(ExecutionException.class, () -> pending.result.get(1, TimeUnit.SECONDS));
		return assertInstanceOf(RocksDBException.class, error.getCause());
	}

	private static RocksDBErrorType failure(Proposer.Pending pending) {
		var error = assertThrows(ExecutionException.class, () -> pending.result.get(1, TimeUnit.SECONDS));
		return assertInstanceOf(RocksDBException.class, error.getCause()).getErrorUniqueId();
	}

	/**
	 * A copy refused because its session expired says nothing about an earlier copy, which may have been applied: the
	 * caller must not take it as "not applied" and write again.
	 */
	@Test
	void anExpiredSessionAfterAnEarlierCopyIsAnUnknownOutcome() {
		var only = pending();
		only.complete(Applier.SESSION_GONE, false);
		assertEquals(RocksDBErrorType.SESSION_EXPIRED, failure(only));
		assertTrue(exception(only).isNotApplied(), "the only copy was refused before it ran");

		var afterAmbiguousAttempt = pending();
		afterAmbiguousAttempt.complete(Applier.SESSION_GONE, true);
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, failure(afterAmbiguousAttempt));
		assertFalse(exception(afterAmbiguousAttempt).isNotApplied());

		var afterAppendedBatch = pending();
		afterAppendedBatch.maybeAppended = true;
		afterAppendedBatch.complete(Applier.SESSION_GONE, false);
		assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, failure(afterAppendedBatch));

		// It took effect: its result is lost, not its write.
		var resentAlone = pending();
		resentAlone.resentAlone = true;
		resentAlone.complete(Applier.SESSION_GONE, false);
		var type = failure(resentAlone);
		assertTrue(type == RocksDBErrorType.INVALID_REQUEST, type::toString);
	}
}
