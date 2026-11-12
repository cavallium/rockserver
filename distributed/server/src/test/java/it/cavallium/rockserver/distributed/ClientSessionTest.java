package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.LongFunction;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Exactly-once client writes: a write whose outcome the client cannot know, because the leader it went to died after
 * the write was committed and before it answered, is sent again by the client library with its session request id,
 * and takes effect once.
 */
@Timeout(180)
@Tag("slow")
class ClientSessionTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final String GRPC_GRACEFUL_SHUTDOWN =
			"it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms";

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static @Nullable String localValue(ReplicatedNode node, long column, long key) {
		Buf value = node.stateMachine().store().db().get(0, column, key(key), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	private static HostAndPort apiAddress(ReplicatedNode node) {
		var self = node.config().self();
		return new HostAndPort(self.apiHost(), self.apiPort());
	}

	/**
	 * The leader commits a merge and a transaction commit, its followers apply them, and it dies before it answers: the
	 * client's calls to it are lost. The client sends both again, with the same request ids, to the next node of its
	 * list. The merge is not applied twice, and the commit, whose transaction went away with the leader, answers
	 * {@code true} from its record instead of "run the transaction again".
	 */
	@Test
	void writesLostWithTheLeaderAreSentAgainAndApplyOnce() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		var addresses = new ArrayList<HostAndPort>();
		addresses.add(apiAddress(leader));
		for (var node : cluster.runningNodes()) {
			if (node != leader) {
				addresses.add(apiAddress(node));
			}
		}
		var graceful = System.getProperty(GRPC_GRACEFUL_SHUTDOWN);
		var gate = new CountDownLatch(1);
		Thread halting = null;
		try (var connection = GrpcConnection.forHostsAndPorts("session-client", addresses)) {
			assertTrue(connection.getCapabilities().clientSessions());
			var api = connection.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("c", SCHEMA).join();
			// Opens the session while the leader still answers.
			api.putAsync(0, column, key(2), text("warm"), RequestType.none()).join();
			long tx = api.openTransactionAsync(Duration.ofSeconds(60)).join();
			api.mergeAsync(tx, column, key(3), text("t"), RequestType.none()).join();
			cluster.awaitConverged(TIMEOUT);

			leader.stateMachine().testHooks().applyGate = gate;
			var merge = api.mergeAsync(0, column, key(1), text("x"), RequestType.merged());
			var commit = api.closeTransactionAsync(tx, true);
			var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
			InProcessCluster.await(TIMEOUT, () -> followers.stream().allMatch(node ->
					"x".equals(localValue(node, column, 1)) && "t".equals(localValue(node, column, 3))));
			assertTrue(!merge.isDone() && !commit.isDone(), "precondition: the leader cannot answer");

			// The leader's API server drops the calls in progress instead of waiting for them.
			System.setProperty(GRPC_GRACEFUL_SHUTDOWN, "0");
			halting = Thread.ofPlatform().name("halt-" + leaderId).start(() -> {
				try {
					cluster.halt(leaderId);
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			});

			assertEquals("x", merge.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS).toString(StandardCharsets.UTF_8));
			assertTrue(commit.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS), "the commit was applied");
			gate.countDown();
			halting.join();
			halting = null;

			// A later write of the same session still works, on the new leader.
			assertEquals("x,y", api.mergeAsync(0, column, key(1), text("y"), RequestType.merged()).join()
					.toString(StandardCharsets.UTF_8));
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				assertEquals("x,y", localValue(node, column, 1), "on " + node.config().nodeId());
				assertEquals("t", localValue(node, column, 3), "on " + node.config().nodeId());
			}
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
			if (halting != null) {
				halting.join();
			}
			if (graceful == null) {
				System.clearProperty(GRPC_GRACEFUL_SHUTDOWN);
			} else {
				System.setProperty(GRPC_GRACEFUL_SHUTDOWN, graceful);
			}
		}
	}

	/**
	 * Copies of a numbered commit sent straight to the leader: a copy that arrives while the first is in flight answers
	 * "unknown" and leaves the transaction alone; once it applied, a copy answers {@code true} from the record, and the
	 * mark is gone. A commit of a transaction the leader never had answers "run it again". A write staged in a
	 * transaction cannot carry a request id.
	 */
	@Test
	void copiesOfACommitAnswerUnknownWhileInFlightThenFromTheRecord() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var connection = (ReplicatedConnection) leader.connection();
		var context = RequestContext.batch(TIMEOUT);
		var api = connection.getAsyncApi(context);
		long session = connection.openClientSessionAsync(context).join();
		long column = api.createColumnAsync("c", SCHEMA).join();
		long tx = api.openTransactionAsync(Duration.ofSeconds(60)).join();
		api.mergeAsync(tx, column, key(3), text("t"), RequestType.none()).join();
		var staged = connection.getAsyncApi(context.withRequestId(new ClientRequestId(session, 0, 0)))
				.mergeAsync(tx, column, key(3), text("again"), RequestType.none());
		var refused = assertThrows(CompletionException.class, staged::join);
		assertEquals(RocksDBErrorType.INVALID_REQUEST, ((RocksDBException) refused.getCause()).getErrorUniqueId());

		cluster.awaitConverged(TIMEOUT);
		var numbered = connection.getAsyncApi(context.withRequestId(new ClientRequestId(session, 1, 1)));
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		CompletableFuture<Boolean> first;
		try {
			first = numbered.closeTransactionAsync(tx, true);
			var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
			InProcessCluster.await(TIMEOUT, () -> followers.stream().allMatch(node ->
					"t".equals(localValue(node, column, 3))));
			var second = assertThrows(CompletionException.class,
					() -> numbered.closeTransactionAsync(tx, true).join());
			assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, ((RocksDBException) second.getCause()).getErrorUniqueId());
			assertTrue(connection.outcomes.unsettledCommitsForTesting() == 1);
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
		}
		assertTrue(first.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS));
		assertTrue(numbered.closeTransactionAsync(tx, true).get(TIMEOUT.toSeconds(), TimeUnit.SECONDS),
				"a copy after the commit answers from its record");
		assertEquals(0, connection.outcomes.unsettledCommitsForTesting());
		assertEquals("t", localValue(leader, column, 3));

		var never = connection.getAsyncApi(context.withRequestId(new ClientRequestId(session, 2, 2)))
				.closeTransactionAsync(123_456, true);
		var retry = assertThrows(CompletionException.class, never::join);
		assertInstanceOf(RocksDBRetryException.class, retry.getCause());
	}
	private static KVBatch kv(long firstKey, String... values) {
		var keys = new ArrayList<Keys>();
		var bufs = new ArrayList<Buf>();
		for (int i = 0; i < values.length; i++) {
			keys.add(key(firstKey + i));
			bufs.add(text(values[i]));
		}
		return new KVBatch.KVBatchRef(keys, bufs);
	}

	/** Values of a size that puts exactly two items with an 8-byte key into one chunk ({@link KVBatchChunks}). */
	private static final int BOUNDARY_VALUE_BYTES =
			(int) (KVBatchChunks.CHUNK_BYTES / 2 - Long.BYTES - 2 * KVBatchChunks.ITEM_OVERHEAD_BYTES);

	private static String[] boundaryValues(char fill, int count) {
		var values = new String[count];
		Arrays.fill(values, String.valueOf(fill).repeat(BOUNDARY_VALUE_BYTES));
		return values;
	}

	/** The id of the newest client session in {@code node}'s database (ids are log indexes: the last opened). */
	private static long newestSession(ReplicatedNode node) {
		var store = node.stateMachine().store();
		try (var sessions = store.db().getRange(0L, store.metaColumnId(), NodeStore.sessionKey(0),
				NodeStore.sessionKey(Long.MAX_VALUE), false, RequestType.<KV>allInRange())) {
			return sessions.mapToLong(kv -> NodeStore.sessionId(kv.keys())).max().orElseThrow();
		}
	}

	private static RocksDBErrorType errorOf(CompletableFuture<?> future) {
		var error = assertThrows(CompletionException.class, future::join);
		return ((RocksDBException) error.getCause()).getErrorUniqueId();
	}

	/**
	 * The write kinds numbered since round 2, sent straight to the leader with request ids: a numbered batch chunk, a
	 * column deletion, a range deletion and the write that ends an update each take effect once, and a copy answers with the first
	 * result; a numbered batch of more than one chunk is refused before anything is applied. Then the client library
	 * sends a batch of several batches as numbered chunks.
	 */
	@Test
	void batchesSchemaChangesAndUpdateEndsAreNumbered() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var connection = (ReplicatedConnection) leader.connection();
		var context = RequestContext.batch(TIMEOUT);
		var api = connection.getAsyncApi(context);
		long session = connection.openClientSessionAsync(context).join();
		long column = api.createColumnAsync("c", SCHEMA).join();
		LongFunction<RocksDBAsyncAPI> numbered = sequence ->
				connection.getAsyncApi(context.withRequestId(new ClientRequestId(session, sequence, 0)));

		// A merge batch chunk sent twice is merged once.
		numbered.apply(0).mergeBatchAsync(column, Flux.just(kv(1, "a", "b")), MergeBatchMode.MERGE_WRITE_BATCH).join();
		numbered.apply(0).mergeBatchAsync(column, Flux.just(kv(1, "a", "b")), MergeBatchMode.MERGE_WRITE_BATCH).join();
		assertEquals("a", localValue(leader, column, 1));
		assertEquals("b", localValue(leader, column, 2));

		// More than one chunk under one number: refused, nothing applied.
		var bigValue = "v".repeat((int) (KVBatchChunks.CHUNK_BYTES / 2));
		var tooBig = numbered.apply(1).putBatchAsync(column, Flux.just(kv(10, bigValue, bigValue, bigValue)),
				PutBatchMode.WRITE_BATCH);
		assertEquals(RocksDBErrorType.INVALID_REQUEST, errorOf(tooBig));
		assertEquals(null, localValue(leader, column, 10));

		// A column deletion sent twice answers as the first time, not "not found".
		long doomed = api.createColumnAsync("d", SCHEMA).join();
		assertTrue(numbered.apply(2).deleteColumnIfExistsAsync("d").join());
		assertTrue(numbered.apply(2).deleteColumnIfExistsAsync("d").join(), "the copy answers from the record");
		long doomed2 = api.createColumnAsync("e", SCHEMA).join();
		numbered.apply(3).deleteColumnAsync(doomed2).join();
		numbered.apply(3).deleteColumnAsync(doomed2).join();
		assertTrue(doomed != doomed2);

		// The write that ends an update: a copy while the first is in flight answers "unknown", then from the record.
		cluster.awaitConverged(TIMEOUT);
		var update = (UpdateContext<?>) api.getAsync(0, column, key(20), RequestType.forUpdate()).join();
		var gate = new CountDownLatch(1);
		leader.stateMachine().testHooks().applyGate = gate;
		CompletableFuture<?> first;
		try {
			first = numbered.apply(4).mergeAsync(update.updateId(), column, key(20), text("u"), RequestType.none());
			var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
			InProcessCluster.await(TIMEOUT, () -> followers.stream().allMatch(node ->
					"u".equals(localValue(node, column, 20))));
			assertEquals(RocksDBErrorType.OUTCOME_UNKNOWN, errorOf(numbered.apply(4)
					.mergeAsync(update.updateId(), column, key(20), text("u"), RequestType.none())));
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
		}
		first.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		numbered.apply(4).mergeAsync(update.updateId(), column, key(20), text("u"), RequestType.none())
				.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		assertEquals("u", localValue(leader, column, 20), "merged once");
		assertEquals(0, connection.outcomes.unsettledCommitsForTesting());

		// A copy of a range deletion does not delete what was written into the range after the first.
		numbered.apply(5).deleteRangeAsync(column, key(40), key(50)).join();
		api.putAsync(0, column, key(41), text("w"), RequestType.none()).join();
		numbered.apply(5).deleteRangeAsync(column, key(40), key(50)).join();
		assertEquals("w", localValue(leader, column, 41));

		// Through the client library: every batch goes as numbered chunks, split by the client as the server splits them
		// again after gRPC encoding (a mismatch would be refused as more than one chunk). Two batches of one chunk each,
		// then one of three chunks whose items fill a chunk to the byte: 5 numbers. The put after them is number 5.
		var addresses = cluster.runningNodes().stream().map(ClientSessionTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("batch-client", addresses)) {
			assertTrue(client.getCapabilities().numberedBatches());
			var clientApi = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			clientApi.mergeBatchAsync(column, Flux.just(kv(30, "p", "q"), kv(32, "r")), MergeBatchMode.MERGE_WRITE_BATCH)
					.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			clientApi.putBatchAsync(column, Flux.just(kv(60, boundaryValues('b', 5))), PutBatchMode.WRITE_BATCH)
					.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			clientApi.putAsync(0, column, key(70), text("after"), RequestType.none()).get(TIMEOUT.toSeconds(),
					TimeUnit.SECONDS);
		}
		cluster.awaitConverged(TIMEOUT);
		long clientSession = newestSession(leader);
		assertTrue(clientSession != session);
		assertTrue(leader.stateMachine().recordedResult(new ClientRequestId(clientSession, 5, 0)) != null,
				"the put after the batches is number 5: one number per chunk");
		assertEquals(null, leader.stateMachine().recordedResult(new ClientRequestId(clientSession, 6, 0)));
		for (var node : cluster.runningNodes()) {
			for (int i = 0; i < 5; i++) {
				assertEquals(BOUNDARY_VALUE_BYTES, localValue(node, column, 60 + i).length());
			}
		}
		for (var node : cluster.runningNodes()) {
			assertEquals("p", localValue(node, column, 30));
			assertEquals("q", localValue(node, column, 31));
			assertEquals("r", localValue(node, column, 32));
			assertEquals("a", localValue(node, column, 1));
			assertEquals("u", localValue(node, column, 20));
			assertEquals("w", localValue(node, column, 41));
		}
	}

	/**
	 * The leader dies while a client batch of three chunks is in flight: it committed the first chunk (its followers
	 * applied it) and never answered. The client sends that chunk again, with its number, to the next node, then the
	 * rest. Every key is merged exactly once: a second merge would double its value.
	 */
	@Test
	void aClientBatchSpanningALeaderKillAppliesEveryChunkOnce() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		var addresses = new ArrayList<HostAndPort>();
		addresses.add(apiAddress(leader));
		for (var node : cluster.runningNodes()) {
			if (node != leader) {
				addresses.add(apiAddress(node));
			}
		}
		var graceful = System.getProperty(GRPC_GRACEFUL_SHUTDOWN);
		var gate = new CountDownLatch(1);
		Thread halting = null;
		try (var connection = GrpcConnection.forHostsAndPorts("batch-kill-client", addresses)) {
			var api = connection.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("c", SCHEMA).join();
			api.putAsync(0, column, key(1), text("warm"), RequestType.none()).join();
			cluster.awaitConverged(TIMEOUT);

			leader.stateMachine().testHooks().applyGate = gate;
			// Chunks of keys 100-101, 102-103, 104.
			var batch = api.mergeBatchAsync(column, Flux.just(kv(100, boundaryValues('m', 5))),
					MergeBatchMode.MERGE_WRITE_BATCH);
			var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
			InProcessCluster.await(TIMEOUT, () -> followers.stream().allMatch(node ->
					localValue(node, column, 101) != null));
			assertTrue(!batch.isDone(), "precondition: the leader cannot answer the first chunk");

			System.setProperty(GRPC_GRACEFUL_SHUTDOWN, "0");
			halting = Thread.ofPlatform().name("halt-" + leaderId).start(() -> {
				try {
					cluster.halt(leaderId);
				} catch (IOException ex) {
					throw new UncheckedIOException(ex);
				}
			});
			batch.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			gate.countDown();
			halting.join();
			halting = null;

			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				for (int i = 0; i < 5; i++) {
					var value = localValue(node, column, 100 + i);
					assertEquals(BOUNDARY_VALUE_BYTES, value == null ? -1 : value.length(),
							"key " + (100 + i) + " merged once on " + node.config().nodeId());
				}
			}
		} finally {
			gate.countDown();
			leader.stateMachine().testHooks().applyGate = null;
			if (halting != null) {
				halting.join();
			}
			if (graceful == null) {
				System.clearProperty(GRPC_GRACEFUL_SHUTDOWN);
			} else {
				System.setProperty(GRPC_GRACEFUL_SHUTDOWN, graceful);
			}
		}
	}
}
