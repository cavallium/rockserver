package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R50-1, through the real {@link Proposer} against a {@link StubRaft} leader that holds its answers: the data lane's
 * attempt permits ({@link Proposer#ATTEMPT_BYTES_IN_FLIGHT}) are full, and a large write is refused, then smaller
 * ones. Each answer frees the bytes of one small write. The large write's resends reserve its bytes, so it is sent
 * before the small ones refused after it, instead of losing every freed slice of the lane to one of them.
 */
class ProposerPermitFairnessTest {

	private static final int SMALL = 7 << 20;
	private static final int LARGE = 12 << 20;

	@TempDir
	Path temp;
	private final StubRaft raft = new StubRaft();
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
		var group = RaftGroup.valueOf(RaftGroupId.valueOf(UUID.randomUUID()),
				RaftPeer.newBuilder().setId("n0").setAddress("127.0.0.1:1").build());
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp).withRequestTimeout(Duration.ofSeconds(30));
		proposer = new Proposer(group, RaftSettings.client(config), config, raft.serverSupplier(), metrics,
				threads.control(), threads.submit());
	}

	@AfterEach
	void tearDown() throws Exception {
		proposer.close();
		threads.close();
		metrics.close();
	}

	private static LogEntry.Put put(long key, int valueBytes) {
		var keys = new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(key).array()));
		return new LogEntry.Put(1, keys, Buf.wrap(new byte[valueBytes]), RequestTypeId.NOTHING);
	}

	private static RaftClientReply applied(RaftClientRequest request) {
		var results = new ArrayList<ApplyResult>();
		int count = RockserverStateMachine.decodeBatch(request.getMessage().getContent()).size();
		for (int i = 0; i < count; i++) {
			results.add(new ApplyResult.Ok(null));
		}
		return RaftClientReply.newBuilder()
				.setRequest(request)
				.setSuccess()
				.setMessage(Message.valueOf(UnsafeByteOperations.unsafeWrap(ResultCodec.encodeAll(results))))
				.build();
	}

	private record Held(RaftClientRequest request, CompletableFuture<RaftClientReply> reply) {}

	@Test
	void aLargeWriteRefusedFirstIsSentBeforeTheSmallOnesRefusedAfterIt() throws Exception {
		var held = new ConcurrentLinkedQueue<Held>();
		// Sizes of the batches sent, in order.
		var sent = new ConcurrentLinkedQueue<Integer>();
		raft.submit = request -> {
			sent.add(request.getMessage().size());
			var reply = new CompletableFuture<RaftClientReply>();
			held.add(new Held(request, reply));
			return reply;
		};
		var results = new ArrayList<CompletableFuture<Object>>();
		for (int i = 0; i < 4; i++) {
			results.add(proposer.propose(put(i, SMALL)));
		}
		InProcessCluster.await(Duration.ofSeconds(10), () -> held.size() == 4);
		results.add(proposer.propose(put(100, LARGE)));
		// Refused at least once (the lane holds 4 small writes), so it asks again every 50 ms.
		Thread.sleep(200);
		for (int i = 0; i < 8; i++) {
			results.add(proposer.propose(put(10 + i, SMALL)));
		}
		Thread.sleep(200);
		assertEquals(4, sent.size(), "a write was sent without a permit");

		boolean largeSent = false;
		for (int round = 0; round < 12 && !largeSent; round++) {
			var first = held.poll();
			first.reply.complete(applied(first.request));
			// Every waiting write asks again meanwhile.
			Thread.sleep(250);
			largeSent = sent.stream().anyMatch(size -> size > LARGE);
		}
		var order = List.copyOf(sent);
		int largeAt = -1;
		for (int i = 0; i < order.size(); i++) {
			if (order.get(i) > LARGE) {
				largeAt = i;
				break;
			}
		}
		assertTrue(largeAt >= 0, "the large write was never sent: " + order);
		assertTrue(largeAt <= 5, "the large write was sent after " + (largeAt - 4)
				+ " small writes refused after it: " + order);

		// Everything else is answered at once from now on.
		raft.submit = request -> CompletableFuture.completedFuture(applied(request));
		var all = CompletableFuture.allOf(results.toArray(CompletableFuture[]::new));
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (!all.isDone() && System.nanoTime() < deadline) {
			Held next;
			while ((next = held.poll()) != null) {
				next.reply.complete(applied(next.request));
			}
			Thread.sleep(20);
		}
		for (var result : results) {
			result.get(20, TimeUnit.SECONDS);
		}
	}
}
