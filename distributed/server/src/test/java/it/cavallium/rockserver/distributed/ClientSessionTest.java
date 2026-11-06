package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ClientRequestId;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
		var refused = assertThrows(java.util.concurrent.CompletionException.class, staged::join);
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
			var second = assertThrows(java.util.concurrent.CompletionException.class,
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
		var retry = assertThrows(java.util.concurrent.CompletionException.class, never::join);
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

	private static RocksDBErrorType errorOf(java.util.concurrent.CompletableFuture<?> future) {
		var error = assertThrows(java.util.concurrent.CompletionException.class, future::join);
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
		java.util.function.LongFunction<it.cavallium.rockserver.core.common.RocksDBAsyncAPI> numbered = sequence ->
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

		// Through the client library: every batch goes as numbered chunks.
		var addresses = cluster.runningNodes().stream().map(ClientSessionTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("batch-client", addresses)) {
			var clientApi = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			clientApi.mergeBatchAsync(column, Flux.just(kv(30, "p", "q"), kv(32, "r")), MergeBatchMode.MERGE_WRITE_BATCH)
					.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertEquals("p", localValue(node, column, 30));
			assertEquals("q", localValue(node, column, 31));
			assertEquals("r", localValue(node, column, 32));
			assertEquals("a", localValue(node, column, 1));
			assertEquals("u", localValue(node, column, 20));
			assertEquals("w", localValue(node, column, 41));
		}
	}
}
