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
	void anOperandThatCannotBeMergedFailsTheWritesThatReadIt() throws Exception {
		try (var store = open()) {
			var applier = new Applier(store);
			long column = store.db().createColumn("c", SCHEMA);
			applier.apply(new Request(1, 0, 0, new LogEntry.Put(column, key(1), text("a"), RequestTypeId.NOTHING)),
					TermIndex.valueOf(1, 1));
			// Nothing reads the merged value: the operand is stored, unmerged.
			assertInstanceOf(ApplyResult.Ok.class, applier.apply(new Request(1, 1, 0, new LogEntry.Merge(column, key(1),
					text("boom"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 2)));
			// Writes that read the key run the operator.
			assertMergeFailure(applier.apply(new Request(1, 2, 0, new LogEntry.Put(column, key(1), text("p"),
					RequestTypeId.PREVIOUS)), TermIndex.valueOf(1, 3)));
			var guarded = new LogEntry.Guarded(1, 0, new long[0], new long[0],
					List.of(new LogEntry.Put(column, key(1), text("g"), RequestTypeId.NOTHING)), true, 99,
					LogEntry.Guard.of(column, key(1), text("a")));
			assertMergeFailure(applier.apply(new Request(1, 3, 0, guarded), TermIndex.valueOf(1, 4)));
			// A write that does not read it replaces it.
			assertInstanceOf(ApplyResult.Ok.class, applier.apply(new Request(1, 4, 0, new LogEntry.Put(column, key(1),
					text("q"), RequestTypeId.NOTHING)), TermIndex.valueOf(1, 5)));
			assertEquals("q", text(store.db().get(0, column, key(1), RequestType.current())));
			assertEquals(TermIndex.valueOf(1, 5), store.applied());
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
}
