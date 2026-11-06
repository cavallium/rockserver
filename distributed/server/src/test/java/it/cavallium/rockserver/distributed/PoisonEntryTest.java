package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A log entry whose apply fails in the same way on every replica (a merge operator that throws, a bug in the apply
 * code) is a result, not a local failure: every replica records the same failure and the log moves on. Only a failure
 * of the local storage stops a replica.
 */
class PoisonEntryTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final ColumnSchema BUCKETED = ColumnSchema.of(IntList.of(Long.BYTES),
			ObjectList.of(ColumnHashType.XXHASH32), true);

	/** Appends operands with commas; throws on a {@code boom} operand and returns no value for a {@code void} one. */
	public static final class PoisonOperator extends FFMByteArrayMergeOperator {
		public PoisonOperator() {
			super("PoisonOperator");
		}

		@Override
		public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
			var out = new ByteArrayOutputStream();
			if (existingValue != null) {
				out.writeBytes(existingValue);
			}
			for (byte[] operand : operands) {
				String text = new String(operand, StandardCharsets.UTF_8);
				if (text.equals("boom")) {
					throw new IllegalStateException("boom");
				}
				if (text.equals("void")) {
					return null;
				}
				if (text.equals("oom")) {
					// A failure of the JVM, wrapped by the operator: local, whatever wraps it.
					throw new IllegalStateException(new OutOfMemoryError("simulated"));
				}
				if (text.equals("addr")) {
					// Text that differs between processes, as an FFM operator's out-of-bounds access gives.
					throw new IndexOutOfBoundsException("Out of bound access on segment MemorySegment{ address: 0x"
							+ Long.toHexString(System.nanoTime()) + ", byteSize: 3 } of loader @"
							+ Integer.toHexString(System.identityHashCode(new Object())) + " [in thread \""
							+ Thread.currentThread().getName() + "\"]");
				}
				if (out.size() > 0) {
					out.write(',');
				}
				out.writeBytes(operand);
			}
			return out.toByteArray();
		}

		@Override
		protected void onMergeError(Throwable t, boolean partial) {
			// Expected here: not worth a stack trace in the test log.
		}
	}

	/** An operator whose constructor throws an Error, as one whose JAR lacks a class it needs. */
	public static final class UnloadableOperator extends FFMByteArrayMergeOperator {
		public UnloadableOperator() {
			super("UnloadableOperator");
			throw new NoClassDefFoundError("com/example/MissingDependency");
		}

		@Override
		public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
			return null;
		}
	}

	/** A JAR with no classes: the operator classes come from the test class path, the JAR's parent loader. */
	private static byte[] emptyJar() throws IOException {
		var out = new ByteArrayOutputStream();
		try (var jar = new java.util.jar.JarOutputStream(out, new java.util.jar.Manifest())) {
			// Nothing.
		}
		return out.toByteArray();
	}

	static final String POISON_CONFIG = """
			database: {
			  global: {
			    fallback-column-options: {
			      merge-operator-class: "%s"
			    }
			  }
			}
			""".formatted(PoisonOperator.class.getName());

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

	private static String text(Buf value) {
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	private Path config() throws IOException {
		var config = temp.resolve("poison.conf");
		if (!Files.exists(config)) {
			Files.writeString(config, POISON_CONFIG);
		}
		return config;
	}

	private NodeStore open() throws Exception {
		return NodeStore.open(temp.resolve("db"), config(), "poison-test");
	}

	private static void assertMergeFailure(ApplyResult result) {
		var failed = assertInstanceOf(ApplyResult.Failed.class, result);
		assertEquals(RocksDBErrorType.MERGE_OPERATOR_FAILED, failed.type(), failed.message());
	}

	private static ApplyResult merged(Applier applier, long sequence, long column, Keys keys, String value) {
		return applier.apply(new Request(1, sequence, 0, new LogEntry.Merge(column, keys, text(value),
				RequestTypeId.MERGED)), TermIndex.valueOf(1, sequence + 1));
	}

	@Test
	void aFailingMergeOperatorIsTheResultAndTheLogMovesOn() throws Exception {
		long column;
		ApplyResult failure;
		try (var store = open()) {
			var applier = new Applier(store);
			column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			failure = merged(applier, 1, column, key(1), "boom");
			assertMergeFailure(failure);
			assertTrue(((ApplyResult.Failed) failure).message().contains("boom"), failure.toString());
			// A rejected entry has no effects, and the next one applies.
			assertEquals("a", text(store.db().get(0, column, key(1), RequestType.current())));
			assertMergeFailure(merged(applier, 2, column, key(1), "void"));
			assertEquals("a,b", text((Buf) ((ApplyResult.Ok) merged(applier, 3, column, key(1), "b")).value()));
			assertEquals(TermIndex.valueOf(1, 4), store.applied());

			// In a batch, and in a group, it fails alone.
			applier.beginGroup();
			var batch = applier.applyBatch(List.of(
					new Request(1, 4, 0, new LogEntry.Put(column, key(2), text("x"), RequestTypeId.NOTHING)),
					new Request(1, 5, 0, new LogEntry.Merge(column, key(1), text("boom"), RequestTypeId.MERGED)),
					new Request(1, 6, 0, new LogEntry.Put(column, key(3), text("y"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 5));
			applier.applyBatch(List.of(new Request(1, 7, 0, new LogEntry.Put(column, key(4), text("z"),
					RequestTypeId.NOTHING))), TermIndex.valueOf(1, 6));
			applier.endGroup();
			assertInstanceOf(ApplyResult.Ok.class, batch.get(0));
			assertMergeFailure(batch.get(1));
			assertInstanceOf(ApplyResult.Ok.class, batch.get(2));
			assertEquals("x", text(store.db().get(0, column, key(2), RequestType.current())));
			assertEquals("z", text(store.db().get(0, column, key(4), RequestType.current())));
			assertEquals("a,b", text(store.db().get(0, column, key(1), RequestType.current())));
			assertEquals(TermIndex.valueOf(1, 6), store.applied());
		}
		// The failure is recorded: a copy of the request after a restart gets it, and does not run again.
		try (var store = open()) {
			var applier = new Applier(store);
			assertEquals(failure, merged(applier, 1, column, key(1), "boom"));
			assertEquals("a,b", text(store.db().get(0, column, key(1), RequestType.current())));
		}
	}

	@Test
	void aFailingMergeOperatorOnABucketedColumnIsTheResult() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("b", BUCKETED);
			var element = new Keys(key(1).keys()[0], Buf.wrap(new byte[] {1}));
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, element, text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			// Bucketed columns merge in Java, inside the apply's write itself.
			assertMergeFailure(applier.apply(new Request(1, 1, 0, new LogEntry.Merge(column, element, text("boom"),
					RequestTypeId.NOTHING)), TermIndex.valueOf(1, 2)));
			assertEquals("a", text(store.db().get(0, column, element, RequestType.current())));
			assertInstanceOf(ApplyResult.Ok.class, applier.apply(new Request(1, 2, 0, new LogEntry.Merge(column, element,
					text("b"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 3)));
			assertEquals("a,b", text(store.db().get(0, column, element, RequestType.current())));
		}
	}

	@Test
	void aMergeThatReturnsNothingIsCheckedAtApplySoNoFlushCanFail() throws Exception {
		long column;
		try (var store = open()) {
			var applier = new Applier(store);
			column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			// Its result is not the merged value, but the operand is merged at once: it is refused, not stored.
			assertMergeFailure(applier.apply(new Request(1, 1, 0, new LogEntry.Merge(column, key(1), text("boom"),
					RequestTypeId.NOTHING)), TermIndex.valueOf(1, 2)));
			assertMergeFailure(applier.apply(new Request(1, 2, 0, new LogEntry.Merge(column, key(9), text("void"),
					RequestTypeId.NOTHING)), TermIndex.valueOf(1, 3)));
			// In a group, a multi-merge with one bad operand fails whole, and the entries around it apply.
			applier.beginGroup();
			var batch = applier.applyBatch(List.of(
					new Request(1, 3, 0, new LogEntry.Merge(column, key(1), text("b"), RequestTypeId.NOTHING)),
					new Request(1, 4, 0, new LogEntry.MergeMulti(column, List.of(key(2), key(1)),
							List.of(text("x"), text("boom")), RequestTypeId.NOTHING)),
					new Request(1, 5, 0, new LogEntry.Merge(column, key(3), text("c"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 4));
			applier.endGroup();
			assertInstanceOf(ApplyResult.Ok.class, batch.get(0));
			assertMergeFailure(batch.get(1));
			assertInstanceOf(ApplyResult.Ok.class, batch.get(2));
			assertEquals("a,b", text(store.db().get(0, column, key(1), RequestType.current())));
			assertNull(store.db().get(0, column, key(2), RequestType.current()));
			assertNull(store.db().get(0, column, key(9), RequestType.current()));
			// No unmergeable operand reached the memtable: the flush, which merges the key, succeeds.
			store.db().flush();
			assertEquals(TermIndex.valueOf(1, 4), store.applied());
		}
		try (var store = open()) {
			assertEquals(TermIndex.valueOf(1, 4), store.applied());
			assertEquals("a,b", text(store.db().get(0, column, key(1), RequestType.current())));
			assertEquals("c", text(store.db().get(0, column, key(3), RequestType.current())));
		}
	}

	/** The same in lanes (partitioned apply): the failing entry runs again on the serial path, the others in lanes. */
	@Test
	void aFailingMergeOperatorInALaneIsTheResultAndTheGroupGoesOn() throws Exception {
		var lanes = new ApplyLanes("poison-test", 3);
		// R2-PA-4: each failure is logged once, by the serial redo that records it, not also by the lane that cut.
		var originalErr = System.err;
		var captured = new java.io.ByteArrayOutputStream();
		try (var store = open()) {
			var applier = new Applier(store, null, lanes);
			long column = store.db().createColumn("c", SCHEMA);
			System.setErr(new java.io.PrintStream(new java.io.OutputStream() {
				@Override
				public void write(int b) {
					originalErr.write(b);
					captured.write(b);
				}

				@Override
				public void write(byte[] bytes, int offset, int length) {
					originalErr.write(bytes, offset, length);
					captured.write(bytes, offset, length);
				}
			}, true));
			applier.beginGroup();
			var before = applier.applyBatch(List.of(
					new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					new Request(1, 1, 0, new LogEntry.Put(column, key(5), text("e"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 1));
			var batch = applier.applyBatch(List.of(
					new Request(1, 2, 0, new LogEntry.Merge(column, key(2), text("x"), RequestTypeId.NOTHING)),
					new Request(1, 3, 0, new LogEntry.Merge(column, key(1), text("boom"), RequestTypeId.MERGED)),
					new Request(1, 4, 0, new LogEntry.Merge(column, key(3), text("y"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 2));
			var after = applier.applyBatch(List.of(
					new Request(1, 5, 0, new LogEntry.Merge(column, key(1), text("b"), RequestTypeId.MERGED)),
					new Request(1, 6, 0, new LogEntry.Merge(column, key(5), text("void"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 3));
			var last = applier.applyBatch(List.of(
					new Request(1, 7, 0, new LogEntry.Put(column, key(4), text("z"), RequestTypeId.NOTHING))),
					TermIndex.valueOf(1, 4));
			applier.endGroup();
			System.setErr(originalErr);
			long logged = captured.toString().lines().filter(line -> line.contains("Applier - Merge operator failed")).count();
			assertEquals(2, logged, captured::toString);
			assertEquals(List.of(ApplyResult.OK_NULL, ApplyResult.OK_NULL), List.copyOf(before));
			assertInstanceOf(ApplyResult.Ok.class, batch.get(0));
			assertMergeFailure(batch.get(1));
			assertInstanceOf(ApplyResult.Ok.class, batch.get(2));
			assertEquals("a,b", text((Buf) ((ApplyResult.Ok) after.get(0)).value()));
			assertMergeFailure(after.get(1));
			assertInstanceOf(ApplyResult.Ok.class, last.getFirst());
			assertEquals("a,b", text(store.db().get(0, column, key(1), RequestType.current())));
			assertEquals("x", text(store.db().get(0, column, key(2), RequestType.current())));
			assertEquals("y", text(store.db().get(0, column, key(3), RequestType.current())));
			assertEquals("z", text(store.db().get(0, column, key(4), RequestType.current())));
			assertEquals("e", text(store.db().get(0, column, key(5), RequestType.current())));
			store.db().flush();
			assertEquals(TermIndex.valueOf(1, 4), store.applied());
			assertTrue(applier.segmentCuts >= 2, "both rejected entries cut their segment: " + applier.segmentCuts);
		} finally {
			System.setErr(originalErr);
			lanes.close();
		}
	}

	@Test
	void onlyLocalFailuresStopTheReplica() {
		var ioError = new org.rocksdb.RocksDBException("disk",
				new org.rocksdb.Status(org.rocksdb.Status.Code.IOError, org.rocksdb.Status.SubCode.None, "disk"));
		var noSpace = new org.rocksdb.RocksDBException("full",
				new org.rocksdb.Status(org.rocksdb.Status.Code.IOError, org.rocksdb.Status.SubCode.NoSpace, "full"));
		var corruption = new org.rocksdb.RocksDBException("bad block",
				new org.rocksdb.Status(org.rocksdb.Status.Code.Corruption, org.rocksdb.Status.SubCode.None, "bad"));
		var invalid = new org.rocksdb.RocksDBException("key too large",
				new org.rocksdb.Status(org.rocksdb.Status.Code.InvalidArgument, org.rocksdb.Status.SubCode.None, "k"));
		assertTrue(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_1, ioError)));
		assertTrue(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, noSpace)));
		assertTrue(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_2, "read failed", corruption)));
		assertTrue(Applier.isLocalFailure(new org.rocksdb.RocksDBException("no status")));
		assertTrue(Applier.isLocalFailure(new UncheckedIOException(new IOException("disk"))));
		assertTrue(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR,
				new OutOfMemoryError())));
		assertTrue(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.TRANSACTION_NOT_FOUND, "gone")));
		assertFalse(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_1, invalid)));
		assertFalse(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR,
				new NullPointerException("bug"))));
		assertFalse(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "bug")));
		assertFalse(Applier.isLocalFailure(new IllegalStateException("bug")));
		// RocksDB reports a failed merge as a corruption: the operator's failure is named above it.
		var merge = it.cavallium.rockserver.core.impl.EmbeddedDB.mergeOperatorFailed(new IllegalStateException("boom"));
		merge.addSuppressed(corruption);
		assertFalse(Applier.isLocalFailure(RocksDBException.of(RocksDBErrorType.PUT_UNKNOWN_ERROR, merge)));
	}

	/** Every request record of a node, by key: replicas that applied the same log hold the same ones. */
	private static TreeMap<String, ApplyResult> records(ReplicatedNode node) {
		var store = node.stateMachine().store();
		var records = new TreeMap<String, ApplyResult>();
		try (var range = store.db().getRange(0, store.metaColumnId(), NodeStore.key(NodeStore.REQUEST),
				NodeStore.key(NodeStore.FLOOR), false, RequestType.<KV>allInRange())) {
			range.forEach(kv -> records.put(Arrays.toString(kv.keys().keys()[0].toByteArray()),
					ResultCodec.decode(kv.value().toByteArray())));
		}
		return records;
	}

	@Test
	void aFailingMergeOperatorFailsTheWriteOnEveryReplicaAndTheClusterServes() throws Exception {
		var poison = config();
		cluster = new InProcessCluster(temp, 3, c -> c.withRockserverConfig(poison)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("a"), RequestType.none());
		var failure = assertThrows(RocksDBException.class,
				() -> api.merge(0, column, key(1), text("boom"), RequestType.merged()));
		assertEquals(RocksDBErrorType.MERGE_OPERATOR_FAILED, failure.getErrorUniqueId(), failure.getMessage());
		// The cluster goes on serving writes, through any node.
		var follower = cluster.anyFollower();
		var followerApi = follower.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		assertEquals("a,b", text(followerApi.merge(0, column, key(1), text("b"), RequestType.merged())));
		cluster.awaitConverged(TIMEOUT);
		var expected = records(leader);
		assertTrue(expected.values().stream().anyMatch(result -> result instanceof ApplyResult.Failed failed
				&& failed.type() == RocksDBErrorType.MERGE_OPERATOR_FAILED), expected.toString());
		for (var node : cluster.runningNodes()) {
			assertFalse(node.isFailed(), node.config().nodeId());
			assertEquals("a,b", text(node.stateMachine().store().db().get(0, column, key(1), RequestType.current())));
			assertEquals(expected, records(node), node.config().nodeId() + " recorded other results");
		}
		// A replica that replays the entry after a restart gets the same failure again, and goes on.
		var restarted = follower.config().nodeId();
		cluster.halt(restarted);
		api.put(0, column, key(2), text("c"), RequestType.none());
		var node = cluster.start(restarted);
		cluster.awaitConverged(TIMEOUT);
		assertFalse(node.isFailed());
		assertEquals("c", text(node.stateMachine().store().db().get(0, column, key(2), RequestType.current())));
		assertNull(node.stopped().getNow(null));
	}

	@Test
	void aFailingMergeThatReturnsNothingLeavesEveryReplicaServingAndFlushable() throws Exception {
		var poison = config();
		cluster = new InProcessCluster(temp, 3, c -> c.withRockserverConfig(poison)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("a"), RequestType.none());
		var failure = assertThrows(RocksDBException.class,
				() -> api.merge(0, column, key(1), text("boom"), RequestType.none()));
		assertEquals(RocksDBErrorType.MERGE_OPERATOR_FAILED, failure.getErrorUniqueId(), failure.getMessage());
		api.merge(0, column, key(1), text("b"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertFalse(node.isFailed(), node.config().nodeId());
			// The flush merges the key: it would fail had the bad operand been stored.
			node.stateMachine().store().db().flush();
			assertEquals("a,b", text(node.stateMachine().store().db().get(0, column, key(1), RequestType.current())));
		}
		var restarted = cluster.anyFollower().config().nodeId();
		cluster.stop(restarted);
		api.put(0, column, key(2), text("c"), RequestType.none());
		var node = cluster.start(restarted);
		cluster.awaitConverged(TIMEOUT);
		assertFalse(node.isFailed());
		assertEquals("a,b", text(node.stateMachine().store().db().get(0, column, key(1), RequestType.current())));
		assertEquals("c", text(node.stateMachine().store().db().get(0, column, key(2), RequestType.current())));
	}

	@Test
	void anOperatorWhoseConstructorThrowsAnErrorIsARejection() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			var upload = new LogEntry.UploadMergeOperator("unloadable", UnloadableOperator.class.getName(), emptyJar());
			var result = assertInstanceOf(ApplyResult.Failed.class,
					applier.apply(new Request(1, 0, 0, upload), TermIndex.valueOf(1, 1)));
			assertEquals(RocksDBErrorType.INVALID_REQUEST, result.type(), result.message());
			assertTrue(result.message().contains("java.lang.NoClassDefFoundError: com/example/MissingDependency"),
					result.message());
			assertEquals(TermIndex.valueOf(1, 1), store.applied());
		}
		// Replayed after a restart: the same rejection, no crash.
		try (var store = open()) {
			var applier = new Applier(store);
			var upload = new LogEntry.UploadMergeOperator("unloadable", UnloadableOperator.class.getName(), emptyJar());
			assertInstanceOf(ApplyResult.Failed.class, applier.apply(new Request(2, 0, 0, upload), TermIndex.valueOf(1, 2)));
		}
	}

	@Test
	void aFailureOfTheJvmInsideTheOperatorStopsTheReplicaEvenWhenWrapped() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			long bucketed = store.db().createColumn("b", BUCKETED);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			assertThrows(Applier.ReplicaFailedException.class, () -> merged(applier, 1, column, key(1), "oom"));
			var element = new Keys(key(1).keys()[0], Buf.wrap(new byte[] {1}));
			applier.apply(new Request(1, 2, 0, new LogEntry.Put(bucketed, element, text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 3));
			assertThrows(Applier.ReplicaFailedException.class, () -> applier.apply(new Request(1, 3, 0,
					new LogEntry.Merge(bucketed, element, text("oom"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 4)));
		}
		var wrapped = it.cavallium.rockserver.core.impl.EmbeddedDB.mergeOperatorFailed(
				new RuntimeException(new StackOverflowError()));
		assertTrue(Applier.isLocalFailure(wrapped));
		assertTrue(Applier.isLocalFailure(new NoClassDefFoundError("Could not initialize class X")
				.initCause(new ExceptionInInitializerError("Exception java.lang.OutOfMemoryError: Java heap space"))));
	}

	@Test
	void aRecordedFailureHoldsNoProcessDetails() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			var failed = (ApplyResult.Failed) merged(applier, 1, column, key(1), "addr");
			assertEquals(RocksDBErrorType.MERGE_OPERATOR_FAILED, failed.type(), failed.message());
			assertEquals("Merge operator failed: java.lang.IndexOutOfBoundsException: Out of bound access on segment"
					+ " MemorySegment{ address: 0x?, byteSize: 3 } of loader @? [in thread ?]", failed.message());
			// What is recorded is that same text.
			assertEquals(failed, applier.recordedResult(new Request(1, 1, 0, new LogEntry.Genesis("x", false))));
		}
	}

	@Test
	void anOperatorThatCannotBeInstantiatedLeavesEveryReplicaServing() throws Exception {
		var poison = config();
		cluster = new InProcessCluster(temp, 3, c -> c.withRockserverConfig(poison)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		var failure = assertThrows(RocksDBException.class,
				() -> api.uploadMergeOperator("unloadable", UnloadableOperator.class.getName(), emptyJar()));
		assertEquals(RocksDBErrorType.INVALID_REQUEST, failure.getErrorUniqueId(), failure.getMessage());
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("a"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertFalse(node.isFailed(), node.config().nodeId());
		}
		// A replica that restarts replays the upload (unflushed) or starts past it: either way it serves.
		var restarted = cluster.anyFollower().config().nodeId();
		cluster.halt(restarted);
		api.put(0, column, key(2), text("b"), RequestType.none());
		var node = cluster.start(restarted);
		cluster.awaitConverged(TIMEOUT);
		assertFalse(node.isFailed());
		assertEquals("b", text(node.stateMachine().store().db().get(0, column, key(2), RequestType.current())));
	}
}
