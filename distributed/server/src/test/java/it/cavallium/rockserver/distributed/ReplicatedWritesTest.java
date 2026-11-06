package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
import reactor.core.publisher.Flux;

/**
 * {@link ReplicatedWrites} over a real {@link Proposer} and a {@link StubRaft} leader that records every entry it is
 * given and answers with the results the test chooses: column creation and its check, and how batches are split,
 * refused when numbered and too large, and reported when partly applied.
 */
class ReplicatedWritesTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));

	@TempDir
	Path temp;
	private final StubRaft raft = new StubRaft();
	/** Every entry submitted, in order. */
	private final List<LogEntry> submitted = new CopyOnWriteArrayList<>();
	/** The result of each submitted entry. */
	private volatile Function<LogEntry, ApplyResult> results = _ -> ApplyResult.OK_NULL;
	private NodeMetrics metrics;
	private NodeThreads threads;
	private Proposer proposer;
	private ReplicatedWrites writes;

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
		raft.submit = request -> CompletableFuture.completedFuture(answer(request));
		proposer = new Proposer(group, RaftSettings.client(config), config, raft.serverSupplier(), metrics,
				threads.control());
		writes = new ReplicatedWrites(proposer);
	}

	@AfterEach
	void tearDown() throws Exception {
		proposer.close();
		threads.close();
		metrics.close();
	}

	private RaftClientReply answer(RaftClientRequest request) {
		var answers = new ArrayList<ApplyResult>();
		for (var decoded : RockserverStateMachine.decodeBatch(request.getMessage().getContent())) {
			submitted.add(decoded.entry());
			answers.add(results.apply(decoded.entry()));
		}
		return RaftClientReply.newBuilder()
				.setRequest(request)
				.setSuccess()
				.setMessage(Message.valueOf(UnsafeByteOperations.unsafeWrap(ResultCodec.encodeAll(answers))))
				.build();
	}

	private static RocksDBException rocksFailure(CompletableFuture<?> future) {
		var error = assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
		return assertInstanceOf(RocksDBException.class, error.getCause());
	}

	@Test
	void aColumnIsCreatedThenItsIdIsCheckedOnEveryReplica() throws Exception {
		results = entry -> entry instanceof LogEntry.CreateColumn ? new ApplyResult.Ok(7L) : ApplyResult.OK_NULL;
		assertEquals(7L, writes.createColumn(CONTEXT, System.nanoTime(), "c", SCHEMA).get(10, TimeUnit.SECONDS));
		assertEquals(2, submitted.size(), submitted::toString);
		assertInstanceOf(LogEntry.CreateColumn.class, submitted.get(0));
		assertEquals(new LogEntry.VerifyColumn("c", 7L), submitted.get(1));
	}

	@Test
	void theInternalColumnNameIsRefusedWithoutAProposal() {
		var error = rocksFailure(writes.createColumn(CONTEXT, System.nanoTime(), NodeStore.META_COLUMN, SCHEMA));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId());
		assertEquals(List.of(), submitted);
	}

	/** Items of a quarter chunk: four fill a chunk, so {@code count} items make {@code ceil(count / 4)} chunks. */
	private static KVBatch items(int first, int count) {
		var keys = new ArrayList<Keys>();
		var values = new ArrayList<Buf>();
		for (int i = first; i < first + count; i++) {
			keys.add(new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())));
			values.add(Buf.wrap(new byte[(int) (KVBatchChunks.CHUNK_BYTES / 4) - 64]));
		}
		return new KVBatch.KVBatchRef(keys, values);
	}

	@Test
	void anUnnumberedBatchIsProposedChunkByChunkInOrder() throws Exception {
		writes.batch(CONTEXT, 1, Flux.just(items(0, 6), items(6, 3)), true).get(10, TimeUnit.SECONDS);
		assertTrue(submitted.size() >= 3, submitted.size() + " chunks");
		long previous = -1;
		int keys = 0;
		for (var entry : submitted) {
			var merge = assertInstanceOf(LogEntry.MergeMulti.class, entry);
			for (var key : merge.keys()) {
				long value = ByteBuffer.wrap(key.keys()[0].toByteArray()).getLong();
				assertEquals(previous + 1, value, "chunks out of order");
				previous = value;
				keys++;
			}
		}
		assertEquals(9, keys);
	}

	@Test
	void aNumberedBatchOfMoreThanOneChunkIsRefusedWithNothingProposed() {
		var numbered = CONTEXT.withRequestId(new ClientRequestId(3, 1, 0));
		var error = rocksFailure(writes.batch(numbered, 1, Flux.just(items(0, 8)), false));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId());
		assertTrue(error.getMessage().contains("nothing was applied"), error.getMessage());
		assertEquals(List.of(), submitted);
	}

	@Test
	void aNumberedBatchOfOneChunkIsOneEntry() throws Exception {
		var numbered = CONTEXT.withRequestId(new ClientRequestId(3, 1, 0));
		writes.batch(numbered, 1, Flux.just(items(0, 2)), false).get(10, TimeUnit.SECONDS);
		assertEquals(1, submitted.size());
		assertInstanceOf(LogEntry.PutMulti.class, submitted.getFirst());
	}

	@Test
	void aLaterChunkThatFailsReportsThePartOfTheBatchThatWasApplied() {
		results = entry -> entry instanceof LogEntry.PutMulti put
				&& ByteBuffer.wrap(put.keys().getFirst().keys()[0].toByteArray()).getLong() >= 4
				? new ApplyResult.Failed(RocksDBErrorType.COLUMN_NOT_FOUND, "gone")
				: ApplyResult.OK_NULL;
		var error = rocksFailure(writes.batch(CONTEXT, 1, Flux.just(items(0, 12)), false));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, error.getErrorUniqueId());
		assertTrue(error.getMessage().contains("partly applied: 1 replicated chunks (4 keys)"), error.getMessage());
		// Nothing after the failed chunk was proposed.
		assertEquals(2, submitted.size());
	}

	@Test
	void aFirstChunkThatFailsReportsItsOwnError() {
		results = _ -> new ApplyResult.Failed(RocksDBErrorType.COLUMN_NOT_FOUND, "gone");
		var error = rocksFailure(writes.batch(CONTEXT, 1, Flux.just(items(0, 12)), false));
		assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, error.getErrorUniqueId());
		assertEquals(1, submitted.size());
	}
}
