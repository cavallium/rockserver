package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ClientRequestId;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatchChunks;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongFunction;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/**
 * Numbered writes the earlier session tests left out: streamed multi writes of a non-LATENCY caller, copies of column
 * creation and merge operator uploads, batches against a server that does not number them, the refusal of a long
 * numbered batch stream, and the "not applied" trailer of a write refused by the leader behind a follower's forward.
 */
@Timeout(180)
class NumberedWritesTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	/** On every node's classpath (InProcessCluster's merge configuration uses it): an empty jar loads it. */
	private static final String MERGE_OPERATOR_CLASS = "it.cavallium.rockserver.core.impl.MyStringAppendOperator";

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

	private static RocksDBException rocks(Throwable error) {
		while (error instanceof CompletionException && error.getCause() != null) {
			error = error.getCause();
		}
		return (RocksDBException) error;
	}

	private static byte[] jar(String entry) throws Exception {
		var out = new ByteArrayOutputStream();
		try (var jar = new JarOutputStream(out, new Manifest())) {
			if (entry != null) {
				jar.putNextEntry(new ZipEntry(entry));
				jar.write(entry.getBytes(StandardCharsets.UTF_8));
				jar.closeEntry();
			}
		}
		return out.toByteArray();
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

	/** The id of the newest client session in {@code node}'s database (ids are log indexes: the last opened). */
	private static long newestSession(ReplicatedNode node) {
		var store = node.stateMachine().store();
		try (var sessions = store.db().getRange(0L, store.metaColumnId(), NodeStore.sessionKey(0),
				NodeStore.sessionKey(Long.MAX_VALUE), false,
				RequestType.<KV>allInRange())) {
			return sessions.mapToLong(kv -> NodeStore.sessionId(kv.keys())).max().orElseThrow();
		}
	}

	/**
	 * A streamed multi write of a BATCH caller with sessions on: the server used to apply it item by item, each item
	 * under the call's one number, so a replicated server applied the first item and answered the others from its
	 * record without applying them. Now such a call is collected and applied as one numbered write: every item is
	 * applied, on every node, and the results of a multi write with results are the items' own.
	 */
	@Test
	void numberedStreamedMultiWritesOfABatchCallerApplyEveryItem() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var addresses = cluster.runningNodes().stream().map(NumberedWritesTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("multi-client", addresses)) {
			assertTrue(client.getCapabilities().clientSessions());
			var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("m", SCHEMA).get(30, TimeUnit.SECONDS);
			api.mergeMultiAsync(0, column, List.of(key(1), key(2), key(3)), List.of(text("a"), text("b"), text("c")),
					RequestType.none()).get(30, TimeUnit.SECONDS);
			List<@Nullable Buf> previous = api.putMultiAsync(0, column, List.of(key(3), key(4)),
					List.of(text("C"), text("d")), RequestType.previous()).get(30, TimeUnit.SECONDS);
			assertEquals(2, previous.size());
			assertEquals("c", previous.get(0).toString(StandardCharsets.UTF_8));
			assertNull(previous.get(1));
			api.deleteMultiAsync(0, column, List.of(key(1), key(2)), RequestType.none()).get(30, TimeUnit.SECONDS);
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				assertNull(localValue(node, column, 1), "on " + node.config().nodeId());
				assertNull(localValue(node, column, 2), "on " + node.config().nodeId());
				assertEquals("C", localValue(node, column, 3), "on " + node.config().nodeId());
				assertEquals("d", localValue(node, column, 4), "on " + node.config().nodeId());
			}
		}
	}

	/**
	 * R6-3: the server refuses a numbered streamed multi write over 16 MiB counted as invalid, nothing applied. The Java
	 * client used to number every streamed putMulti, deleteMulti and mergeMulti, so such a call of a BATCH caller,
	 * applied item by item before numbering, failed. Past 4 MiB counted the client now sends it unnumbered, as the Rust
	 * client does: it is applied item by item, with its results.
	 */
	@Test
	void aStreamedMultiWriteOverTheNumberingLimitIsSentUnnumberedAndApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var addresses = cluster.runningNodes().stream().map(NumberedWritesTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("large-multi", addresses)) {
			assertTrue(client.getCapabilities().clientSessions());
			var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("m", SCHEMA).get(30, TimeUnit.SECONDS);
			// 20 values of 1 MiB: 20 MiB counted, over the server's 16 MiB for one numbered multi write.
			var keys = new ArrayList<Keys>();
			var values = new ArrayList<Buf>();
			for (int i = 0; i < 20; i++) {
				keys.add(key(i));
				var value = new byte[1024 * 1024];
				value[0] = (byte) i;
				values.add(Buf.wrap(value));
			}
			List<@Nullable Buf> previous = api.putMultiAsync(0, column, keys, values, RequestType.previous())
					.get(60, TimeUnit.SECONDS);
			assertEquals(20, previous.size());
			assertTrue(previous.stream().allMatch(p -> p == null), "no previous values");
			api.deleteMultiAsync(0, column, keys.subList(0, 10), RequestType.none()).get(30, TimeUnit.SECONDS);
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				var db = node.stateMachine().store().db();
				assertNull(db.get(0, column, key(0), RequestType.current()), "on " + node.config().nodeId());
				Buf last = db.get(0, column, key(19), RequestType.current());
				assertNotNull(last, "on " + node.config().nodeId());
				assertEquals(1024 * 1024, last.size());
				assertEquals(19, last.getByte(0));
			}
		}
	}

	/**
	 * R6-3 review: a multi write that ends an update (its transaction or update id is one a {@code getForUpdate} of
	 * this client opened) and goes unnumbered over the numbering limit still makes the client forget the update once it
	 * ended, as a numbered one does: otherwise a transaction that later gets the same id would have its staged writes
	 * numbered and refused.
	 */
	@Test
	void anUnnumberedMultiWriteThatEndsAnUpdateForgetsIt() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var addresses = cluster.runningNodes().stream().map(NumberedWritesTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("large-update-end", addresses)) {
			assertTrue(client.getCapabilities().clientSessions());
			var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("u", SCHEMA).get(30, TimeUnit.SECONDS);
			var update = api.getAsync(0, column, key(0), RequestType.forUpdate()).get(30, TimeUnit.SECONDS);
			long updateId = update.updateId();
			assertTrue(updateId != 0);
			var writes = sessionWritesOf(client);
			assertTrue(numbers(writes, new RocksDBAPICommandSingle.Put<>(updateId, column, key(0), Buf.wrap(new byte[1]),
					RequestType.none())), "the update is remembered");
			// The update's key with a 5 MiB value and its previous value: over 4 MiB counted, streamed, unnumbered.
			var keys = List.of(key(0));
			var values = List.of(Buf.wrap(new byte[5 * 1024 * 1024]));
			var ending = new RocksDBAPICommandSingle.PutMulti<>(updateId, column, keys, values, RequestType.previous());
			assertFalse(numbers(writes, ending), "over the numbering limit");
			List<@Nullable Buf> previous = api.putMultiAsync(updateId, column, keys, values, RequestType.previous())
					.get(60, TimeUnit.SECONDS);
			assertEquals(1, previous.size());
			assertNull(previous.getFirst(), "no previous value");
			assertFalse(numbers(writes, new RocksDBAPICommandSingle.Put<>(updateId, column, key(0), Buf.wrap(new byte[1]),
					RequestType.none())), "the update is forgotten once the write that ended it ended");
		}
	}

	private static Object sessionWritesOf(GrpcConnection client) throws Exception {
		Field field = GrpcConnection.class.getDeclaredField("sessionWrites");
		field.setAccessible(true);
		return field.get(client);
	}

	private static boolean numbers(Object writes, RocksDBAPICommand<?, ?, ?> command) throws Exception {
		Method method = writes.getClass().getDeclaredMethod("numbers", RocksDBAPICommand.class);
		method.setAccessible(true);
		return (Boolean) method.invoke(writes, command);
	}

	/**
	 * A numbered streamed multi write of a BATCH caller is held on the heap while the server collects it, before the
	 * node's write admission sees it: it takes its counted bytes (keys, values and per-item costs) from the server's
	 * collect budget. A write that does not fit is refused as SERVER_OVERLOADED flagged not applied, and applies
	 * nothing; the bytes go back when a collection ends, so later writes that fit one at a time all succeed.
	 */
	@Test
	void numberedStreamedMultiWritesTakeTheServersCollectBudget() throws Exception {
		System.setProperty("rockserver.grpc.numbered-multi-collect-bytes", Long.toString(64 * 1024));
		try {
			cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		} finally {
			System.clearProperty("rockserver.grpc.numbered-multi-collect-bytes");
		}
		cluster.awaitLeader(TIMEOUT);
		var addresses = cluster.runningNodes().stream().map(NumberedWritesTest::apiAddress).toList();
		try (var client = GrpcConnection.forHostsAndPorts("collect-budget", addresses)) {
			assertTrue(client.getCapabilities().clientSessions());
			long column = client.getAsyncApi(RequestContext.batch(TIMEOUT)).createColumnAsync("m", SCHEMA)
					.get(30, TimeUnit.SECONDS);
			// 1000 items of an 8-byte key and a 1-byte value count about 130 KiB: over the 64 KiB budget.
			var tooLarge = new ArrayList<Keys>();
			var tooLargeValues = new ArrayList<Buf>();
			for (int i = 0; i < 1000; i++) {
				tooLarge.add(key(i));
				tooLargeValues.add(text("x"));
			}
			var refused = assertThrows(Exception.class, () -> client.getAsyncApi(RequestContext.batch(Duration.ofSeconds(3)))
					.mergeMultiAsync(0, column, tooLarge, tooLargeValues, RequestType.none())
					.get(30, TimeUnit.SECONDS));
			var cause = refused instanceof ExecutionException ? refused.getCause() : refused;
			assertEquals(RocksDBErrorType.SERVER_OVERLOADED, rocks(cause).getErrorUniqueId(), String.valueOf(cause));
			// 20 writes of 100 items (about 13 KiB each, 260 KiB in all): fit only if each one's bytes went back.
			var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			for (int round = 0; round < 20; round++) {
				var keys = new ArrayList<Keys>();
				var values = new ArrayList<Buf>();
				for (int i = 0; i < 100; i++) {
					keys.add(key(10_000 + round * 100L + i));
					values.add(text("v"));
				}
				api.mergeMultiAsync(0, column, keys, values, RequestType.none()).get(30, TimeUnit.SECONDS);
			}
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				assertNull(localValue(node, column, 0), "refused write applied nothing, on " + node.config().nodeId());
				assertNull(localValue(node, column, 999), "on " + node.config().nodeId());
				assertEquals("v", localValue(node, column, 10_000), "on " + node.config().nodeId());
				assertEquals("v", localValue(node, column, 11_999), "on " + node.config().nodeId());
			}
		}
	}

	/**
	 * R2-SC-3: numbered column creation and merge operator upload. A copy answers from the record with the first
	 * result: a copy of a creation sent after the column was deleted does not create it again, and a copy of an upload
	 * sent after another version was uploaded does not add a version.
	 */
	@Test
	void copiesOfACreationAndOfAnUploadAnswerFromTheRecord() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var connection = (ReplicatedConnection) leader.connection();
		var context = RequestContext.batch(TIMEOUT);
		var api = connection.getAsyncApi(context);
		long session = connection.openClientSessionAsync(context).join();
		LongFunction<RocksDBAsyncAPI> numbered = sequence ->
				connection.getAsyncApi(context.withRequestId(new ClientRequestId(session, sequence, 0)));

		long created = numbered.apply(0).createColumnAsync("x", SCHEMA).join();
		assertEquals(created, (long) numbered.apply(0).createColumnAsync("x", SCHEMA).join());
		api.deleteColumnAsync(created).join();
		assertEquals(created, (long) numbered.apply(0).createColumnAsync("x", SCHEMA).join(),
				"a copy answers with the first result");
		var gone = assertThrows(CompletionException.class, () -> api.getColumnIdAsync("x").join());
		assertEquals(RocksDBErrorType.COLUMN_NOT_FOUND, rocks(gone).getErrorUniqueId(), "not created again");

		long first = numbered.apply(1).uploadMergeOperatorAsync("op", MERGE_OPERATOR_CLASS, jar(null)).join();
		assertEquals(first, (long) numbered.apply(1).uploadMergeOperatorAsync("op", MERGE_OPERATOR_CLASS, jar(null))
				.join());
		long second = api.uploadMergeOperatorAsync("op", MERGE_OPERATOR_CLASS, jar("other")).join();
		assertTrue(second > first);
		assertEquals(first, (long) numbered.apply(1).uploadMergeOperatorAsync("op", MERGE_OPERATOR_CLASS, jar(null))
				.join(), "a copy answers with the first version");
		long third = api.uploadMergeOperatorAsync("op", MERGE_OPERATOR_CLASS, jar("third")).join();
		assertEquals(second + 1, third, "the copy added no version");
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertNotNull(node.stateMachine().recordedResult(new ClientRequestId(session, 0, 0)));
			assertNotNull(node.stateMachine().recordedResult(new ClientRequestId(session, 1, 0)));
		}
	}

	/**
	 * Review of R58, 1: a numbered write of a direct client whose context has no timeout (RequestContext.ingest(), the
	 * profile a SessionWrites caller may use) and whose call has no gRPC deadline. The server's GrpcRequestContexts
	 * used to serve such a context from its cached no-deadline API, whose context has no request id, so the write was
	 * proposed unnumbered: no result recorded, and a copy sent after a lost answer applied again.
	 */
	@Test
	void aNumberedWriteWithNoTimeoutAndNoDeadlineKeepsItsRequestId() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var connection = (ReplicatedConnection) leader.connection();
		var context = RequestContext.batch(TIMEOUT);
		long column = connection.getAsyncApi(context).createColumnAsync("n", SCHEMA).join();
		long session = connection.openClientSessionAsync(context).join();
		var address = apiAddress(leader);
		var channel = io.grpc.netty.NettyChannelBuilder.forAddress(address.host(), address.port()).usePlaintext()
				.build();
		try {
			var stub = it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.newBlockingStub(channel);
			var wireContext = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.INGEST)
					.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
					.setTimeoutNanos(RequestContext.NO_TIMEOUT)
					.setRequestId(it.cavallium.rockserver.core.common.api.proto.ClientRequestId.newBuilder()
							.setSessionId(session).setSequence(0).setCompletedBelow(0));
			var put = it.cavallium.rockserver.core.common.api.proto.PutRequest.newBuilder()
					.setTransactionOrUpdateId(0).setColumnId(column)
					.setData(it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
							.addKeys(com.google.protobuf.ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(1)
									.array()))
							.setValue(com.google.protobuf.ByteString.copyFromUtf8("v")))
					.setContext(wireContext).build();
			stub.put(put);
			assertEquals("v", localValue(leader, column, 1));
			assertNotNull(leader.stateMachine().recordedResult(new ClientRequestId(session, 0, 0)),
					"the write was proposed without its request id");
		} finally {
			channel.shutdownNow();
		}
	}

	/**
	 * R2-SC-4: a client of a server that offers sessions but does not number batches sends a batch of several chunks
	 * unnumbered, as one stream, once: it uses no number of the session (the put after it is number 0), and the server
	 * applies every chunk.
	 */
	@Test
	void aServerThatDoesNotNumberBatchesGetsThemUnnumbered() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var real = leader.connection();
		var withoutNumberedBatches = (RocksDBConnection) Proxy.newProxyInstance(
				RocksDBConnection.class.getClassLoader(), new Class<?>[] {RocksDBConnection.class}, (_, method, args) -> {
					if (method.getName().equals("getCapabilities") && method.getParameterCount() == 0) {
						return new RockserverCapabilities(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION, true,
								false);
					}
					if (method.getName().equals("close") && method.getParameterCount() == 0) {
						// The node closes its own connection.
						return null;
					}
					try {
						return method.invoke(real, args);
					} catch (InvocationTargetException ex) {
						throw ex.getCause();
					}
				});
		try (var server = new GrpcServer(withoutNumberedBatches, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostsAndPorts("unnumbered-batch-client",
					List.of(new HostAndPort("127.0.0.1", server.getPort())))) {
				assertTrue(client.getCapabilities().clientSessions());
				assertTrue(!client.getCapabilities().numberedBatches());
				var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
				long column = api.createColumnAsync("b", SCHEMA).get(30, TimeUnit.SECONDS);
				long session = newestSession(leader);
				// Three chunks: values of half a chunk each.
				var half = "h".repeat((int) (KVBatchChunks.CHUNK_BYTES / 2));
				api.putBatchAsync(column, Flux.just(kv(10, half, half, half, half, half)), PutBatchMode.WRITE_BATCH)
						.get(30, TimeUnit.SECONDS);
				api.putAsync(0, column, key(20), text("after"), RequestType.none()).get(30, TimeUnit.SECONDS);
				assertNotNull(leader.stateMachine().recordedResult(new ClientRequestId(session, 1, 0)),
						"the put after the batch is number 1, after the column creation");
				assertNull(leader.stateMachine().recordedResult(new ClientRequestId(session, 2, 0)),
						"the batch took no number");
			}
		}
		cluster.awaitConverged(TIMEOUT);
		var column = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT)).getColumnId("b");
		for (var node : cluster.runningNodes()) {
			for (int i = 0; i < 5; i++) {
				var value = localValue(node, column, 10 + i);
				assertEquals(KVBatchChunks.CHUNK_BYTES / 2,
						value == null ? -1 : value.length(), "key " + (10 + i) + " on " + node.config().nodeId());
			}
			assertEquals("after", localValue(node, column, 20));
		}
	}

	/**
	 * R2-SC-4: a numbered batch whose stream is long (here endless) is refused as soon as its second chunk arrives,
	 * and the rest of the stream is not read: only a few batches are ever produced, and nothing is applied.
	 */
	@Test
	void aLongNumberedBatchStreamIsRefusedWithoutReadingIt() throws Exception {
		cluster = new InProcessCluster(temp, 1, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var connection = (ReplicatedConnection) leader.connection();
		var context = RequestContext.batch(TIMEOUT);
		long column = connection.getAsyncApi(context).createColumnAsync("l", SCHEMA).join();
		long session = connection.openClientSessionAsync(context).join();
		var produced = new AtomicLong();
		// Every batch is a chunk of its own (chunks never span batches).
		var endless = Flux.<KVBatch>generate(sink -> sink.next(kv(produced.getAndIncrement() * 2, "v")));
		var refused = assertThrows(CompletionException.class, () -> connection
				.getAsyncApi(context.withRequestId(new ClientRequestId(session, 0, 0)))
				.putBatchAsync(column, endless, PutBatchMode.WRITE_BATCH).join());
		assertEquals(RocksDBErrorType.INVALID_REQUEST, rocks(refused).getErrorUniqueId());
		assertTrue(produced.get() <= 4, "the stream was cancelled after its second chunk: " + produced.get()
				+ " batches produced");
		assertNull(localValue(leader, column, 0), "nothing applied");
	}

	/**
	 * R3-G1-1: a commit sent to a follower is forwarded to the leader, whose byte admission is full: the leader refuses
	 * it before the append, flagged "not applied", and the flag crosses the forward back to the client (the trailer of
	 * the follower's answer). Nothing was applied; once the leader has room, the transaction run again commits.
	 */
	@Test
	void aWriteRefusedByTheLeaderBehindAForwardIsFlaggedNotApplied() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.runningNodes().stream().filter(node -> node != leader).findFirst().orElseThrow();
		try (var client = GrpcConnection.forHostsAndPorts("forward-client", List.of(apiAddress(follower)))) {
			var api = client.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("f", SCHEMA).get(30, TimeUnit.SECONDS);
			long tx = api.openTransactionAsync(Duration.ofSeconds(60)).get(30, TimeUnit.SECONDS);
			api.putAsync(tx, column, key(1), text("t"), RequestType.none()).get(30, TimeUnit.SECONDS);

			var admission = leader.proposer().admissionForTesting();
			long filled = admission.limit() - admission.used();
			assertTrue(admission.tryAcquire(filled));
			try {
				var refused = assertThrows(ExecutionException.class,
						() -> api.closeTransactionAsync(tx, true).get(30, TimeUnit.SECONDS));
				var error = (RocksDBException) refused.getCause();
				assertEquals(RocksDBErrorType.SERVER_OVERLOADED, error.getErrorUniqueId(), error.getMessage());
				assertTrue(error.isNotApplied(), "flagged not applied through the forward: " + error.getMessage());
			} finally {
				admission.release(filled);
			}
			assertNull(localValue(leader, column, 1), "not applied");
			// The refused commit ended the transaction: the caller runs it again, as after any failed commit.
			long again = api.openTransactionAsync(Duration.ofSeconds(60)).get(30, TimeUnit.SECONDS);
			api.putAsync(again, column, key(1), text("t"), RequestType.none()).get(30, TimeUnit.SECONDS);
			assertTrue(api.closeTransactionAsync(again, true).get(30, TimeUnit.SECONDS));
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				assertEquals("t", localValue(node, column, 1), "on " + node.config().nodeId());
			}
		}
	}
}
