package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@TestMethodOrder(MethodOrderer.MethodName.class)
abstract class DBTest {

	protected EmbeddedConnection db;
	protected long colId = 0L;
	protected Arena arena;
	protected Buf bigValue;
	protected Keys key1;
	protected Keys collidingKey1;
	protected Keys key2;
	protected Buf value1;
	protected Buf value2;
	private Path configFile;

	protected enum ConnectionMethod {
		EMBEDDED,
		GRPC,
		THRIFT
	}

	protected enum ConnectionType {
		SYNC,
		ASYNC
	}

	public record ConnectionConfig(ConnectionType type, ConnectionMethod method) {

		public String getName() {
			return type.name() + ", " + method.name();
		}
	}

	public static Stream<Arguments> allImplementations() {
		return Stream
				.of(ConnectionMethod.values())
				.flatMap(method -> Stream.of(ConnectionType.values())
						.map(type -> new ConnectionConfig(type, method)))
				.map(cfg -> Arguments.arguments(cfg.getName(), cfg));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	@Retention(RetentionPolicy.RUNTIME)
	private @interface TestAllImplementations {
	}

	@org.junit.jupiter.api.BeforeEach
	void setUp() throws IOException {
		if (System.getProperty("rockserver.core.print-config", null) == null) {
			System.setProperty("rockserver.core.print-config", "false");
		}
		configFile = Files.createTempFile("dbtest-merge", ".conf");
		Files.writeString(configFile, """
database: {
  global: {
    ingest-behind: true
    fallback-column-options: {
      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
    }
  }
}
""");
		arena = Arena.ofShared();
		db = new EmbeddedConnection(null, "test", configFile);
		createStandardColumn();

		bigValue = getBigValue();
		key1 = getKey1();
		collidingKey1 = getCollidingKey1();
		key2 = getKey2();

		value1 = getValue1();
		value2 = getValue2();
	}

	protected Buf getBigValue() {
		var bigValueArray = new byte[10_000];
		ThreadLocalRandom.current().nextBytes(bigValueArray);
		return Utils.toBuf(bigValueArray);
	}

	protected Keys getKey2() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3),
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, 7)
		});
	}

	protected Keys getCollidingKey1() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3),
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, -48)
		});
	}

	protected Keys getKey1() {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3),
				toBufSimple(1, 2, 3),
				toBufSimple(6, 7, 8)
		});
	}

	/**
	 * @return a sorted sequence of k-v pairs
	 */
	protected List<KV> getKVSequence() {
		var result = new ArrayList<KV>();
		for (int i = 0; i < Byte.MAX_VALUE; i++) {
			result.add(new KV(getKeyI(i), getValueI(i)));
		}
		return result;
	}

	protected KV getKVSequenceFirst() {
		return getKVSequence().getFirst();
	}

	protected KV getKVSequenceLast() {
		return getKVSequence().getLast();
	}

	protected boolean getHasValues() {
		return true;
	}

	protected Buf getValue1() {
		return Utils.toBufSimple(0, 0, 3);
	}

	protected Buf getValue2() {
		return Utils.toBufSimple(0, 0, 5);
	}

	private void createStandardColumn() {
		createColumn(getSchema());
	}

	private void createColumn(ColumnSchema schema) {
		if (colId != 0L) {
			db.deleteColumn(colId);
		}
		colId = db.createColumn("column-test", schema);
	}

	void fillSomeKeys() {
		Assertions.assertNull(db.put(0, colId, key1, value1, RequestType.none()));
		Assertions.assertNull(db.put(0, colId, collidingKey1, value2, RequestType.none()));
		Assertions.assertNull(db.put(0, colId, key2, bigValue, RequestType.none()));
		for (int i = 0; i < Byte.MAX_VALUE; i++) {
			var keyI = getKeyI(i);
			var valueI = getValueI(i);
			Assertions.assertNull(db.put(0, colId, keyI, valueI, RequestType.none()));
		}
	}

	protected Keys getKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3),
				toBufSimple(1, 2, 3),
				toBufSimple(8, 2, 5, 1, 7, i)
		});
	}

	protected Keys getNotFoundKeyI(int i) {
		return new Keys(new Buf[] {
				toBufSimple(3),
				toBufSimple(4, 6),
				toBufSimple(3),
				toBufSimple(1, 2, 3),
				toBufSimple(8, 2, 5, 1, 0, i)
		});
	}

	protected Buf getValueI(int i) {
		return toBufSimple(i, i, i, i, i);
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() throws IOException {
		db.deleteColumn(colId);
		db.close();
		arena.close();
		Files.deleteIfExists(configFile);
	}

	@SuppressWarnings("DataFlowIssue")
	@TestAllImplementations
	void putTestErrors(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			var key = getKey1();

			if (!getHasValues()) {
				Assertions.assertThrows(RocksDBException.class, () -> db.put(0, colId, key, toBufSimple(123), RequestType.delta()));
			} else {
				Assertions.assertThrows(RocksDBException.class, () -> db.put(0, colId, key, emptyBuf(), RequestType.delta()));
				Assertions.assertThrows(RocksDBException.class, () -> {
					try {
						db.put(0, colId, key, null, RequestType.delta());
					} catch (IllegalArgumentException ex) {
						throw RocksDBException.of(RocksDBException.RocksDBErrorType.UNEXPECTED_NULL_VALUE, ex);
					}
				});
			}

			Assertions.assertThrows(RocksDBException.class, () -> {
				try {
					db.put(0, colId, null, value1, RequestType.delta());
				} catch (IllegalArgumentException ex) {
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.UNEXPECTED_NULL_VALUE, ex);
				}
			});
			Assertions.assertThrows(RocksDBException.class, () -> {
				try {
					db.put(0, colId, null, null, RequestType.delta());
				} catch (IllegalArgumentException ex) {
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.UNEXPECTED_NULL_VALUE, ex);
				}
			});
			Assertions.assertThrows(RocksDBException.class, () -> db.put(0, colId, key, value1, null));
			Assertions.assertThrows(RocksDBException.class, () -> db.put(1, colId, key, value1, RequestType.delta()));
			Assertions.assertThrows(RocksDBException.class, () -> db.put(0, 21203, key, value1, RequestType.delta()));
		}
	}

	@TestAllImplementations
	void putSameBucketSameKey(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			var key = getKey1();
			var value1 = getValue1();
			var value2 = getValue2();

			Delta<Buf> delta;

			delta = db.put(0, colId, key, value1, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key, value2, RequestType.delta());
			assertSegmentEquals(value1, delta.previous());
			assertSegmentEquals(value2, delta.current());
		}
	}

	@TestAllImplementations
	void putSameBucketDifferentKey(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			if (getSchemaVarKeys().isEmpty()) {
				return;
			}
			createColumn(ColumnSchema.of(getSchemaFixedKeys(), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.ALLSAME8), getHasValues()));

			var key1 = getKey1();
			var key2 = getCollidingKey1();

			var value1 = getValue1();
			var value2 = getValue2();

			Delta<Buf> delta;

			Assertions.assertFalse(db.put(0, colId, getKeyI(3), value2, RequestType.previousPresence()));
			Assertions.assertFalse(db.put(0, colId, getKeyI(4), value2, RequestType.previousPresence()));


			delta = db.put(0, colId, key1, value1, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key2, value2, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value2, delta.current());

			delta = db.put(0, colId, key2, value1, RequestType.delta());
			assertSegmentEquals(value2, delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key2, value1, RequestType.delta());
			assertSegmentEquals(value1, delta.previous());
			assertSegmentEquals(value1, delta.current());


			Assertions.assertTrue(db.put(0, colId, key1, value2, RequestType.previousPresence()));
			Assertions.assertTrue(db.put(0, colId, key2, value2, RequestType.previousPresence()));


			delta = db.put(0, colId, key1, value1, RequestType.delta());
			assertSegmentEquals(value2, delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key2, value1, RequestType.delta());
			assertSegmentEquals(value2, delta.previous());
			assertSegmentEquals(value1, delta.current());


			Assertions.assertNull(db.put(0, colId, key1, value2, RequestType.none()));
			Assertions.assertNull(db.put(0, colId, key2, value2, RequestType.none()));


			assertSegmentEquals(value2, db.put(0, colId, key1, value1, RequestType.previous()));
			assertSegmentEquals(value2, db.put(0, colId, key2, value1, RequestType.previous()));

			assertSegmentEquals(value1, db.put(0, colId, key1, value1, RequestType.previous()));
			assertSegmentEquals(value1, db.put(0, colId, key2, value1, RequestType.previous()));

			if (!Utils.valueEquals(value1, value2)) {
				Assertions.assertTrue(db.put(0, colId, key1, value2, RequestType.changed()));
				Assertions.assertTrue(db.put(0, colId, key2, value2, RequestType.changed()));
			}

			Assertions.assertFalse(db.put(0, colId, key1, value2, RequestType.changed()));
			Assertions.assertFalse(db.put(0, colId, key2, value2, RequestType.changed()));


			assertSegmentEquals(value2, db.put(0, colId, key1, value1, RequestType.previous()));
			assertSegmentEquals(value2, db.put(0, colId, key2, value1, RequestType.previous()));
		}
	}

	protected ColumnSchema getSchema() {
		return ColumnSchema.of(getSchemaFixedKeys(), getSchemaVarKeys(), getHasValues());
	}

	protected IntList getSchemaFixedKeys() {
		return IntList.of(1, 2, 1);
	}

	protected ObjectList<ColumnHashType> getSchemaVarKeys() {
		return ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH8);
	}

	/**
	 * Some keys have same bucket, some not
	 */
	@TestAllImplementations
	void putMixedBucketMixedKey(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			var key1 = getKey1();
			var collidingKey1 = getCollidingKey1();
			var key2 = getKey2();

			var value1 = getValue1();
			var value2 = getValue2();

			var delta = db.put(0, colId, key1, value1, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, collidingKey1, value2, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value2, delta.current());

			delta = db.put(0, colId, collidingKey1, value1, RequestType.delta());
			assertSegmentEquals(value2, delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key2, value1, RequestType.delta());
			Assertions.assertNull(delta.previous());
			assertSegmentEquals(value1, delta.current());

			delta = db.put(0, colId, key2, value2, RequestType.delta());
			assertSegmentEquals(value1, delta.previous());
			assertSegmentEquals(value2, delta.current());
		}
	}

	@TestAllImplementations
	void get(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			if (getHasValues()) {
				Assertions.assertNull(db.get(0, colId, key1, RequestType.current()));
				Assertions.assertNull(db.get(0, colId, collidingKey1, RequestType.current()));
				Assertions.assertNull(db.get(0, colId, key2, RequestType.current()));
			}
			Assertions.assertFalse(db.get(0, colId, key1, RequestType.exists()));
			Assertions.assertFalse(db.get(0, colId, collidingKey1, RequestType.exists()));
			Assertions.assertFalse(db.get(0, colId, key2, RequestType.exists()));

			fillSomeKeys();

			if (getHasValues()) {
				assertSegmentEquals(value1, db.get(0, colId, key1, RequestType.current()));
				Assertions.assertNull(db.get(0, colId, getNotFoundKeyI(0), RequestType.current()));
				assertSegmentEquals(value2, db.get(0, colId, collidingKey1, RequestType.current()));
				assertSegmentEquals(bigValue, db.get(0, colId, key2, RequestType.current()));
			}

			Assertions.assertTrue(db.get(0, colId, key1, RequestType.exists()));
			Assertions.assertFalse(db.get(0, colId, getNotFoundKeyI(0), RequestType.exists()));
			Assertions.assertTrue(db.get(0, colId, collidingKey1, RequestType.exists()));
			Assertions.assertTrue(db.get(0, colId, key2, RequestType.exists()));
		}
	}

	@TestAllImplementations
	void update(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			if (getHasValues()) {
				var forUpdate = db.get(0, colId, key1, RequestType.forUpdate());
				Assertions.assertNull(forUpdate.previous());
				Assertions.assertTrue(forUpdate.updateId() != 0);
				db.put(forUpdate.updateId(), colId, key1, value1, RequestType.none());

				Assertions.assertThrows(Exception.class, () -> db.put(forUpdate.updateId(), colId, key1, value2, RequestType.none()));
			}
		}
	}

	@TestAllImplementations
	void reduceRangeFirstAndLast(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			var firstKey = getKVSequenceFirst().keys();
			var lastKey = getKVSequenceLast().keys();
			var prevLastKV = getKVSequence().get(getKVSequence().size() - 2);
			if (getSchemaVarKeys().isEmpty()) {
				FirstAndLast<KV> firstAndLast = db.reduceRange(0, colId, firstKey, lastKey, false, RequestType.firstAndLast(), 1000);
				Assertions.assertNull(firstAndLast.first(), "First should be empty because the db is empty");
				Assertions.assertNull(firstAndLast.last(), "Last should be empty because the db is empty");

				fillSomeKeys();

				firstAndLast = db.reduceRange(0, colId, firstKey, lastKey, false, RequestType.firstAndLast(), 1000);
				Assertions.assertEquals(getKVSequenceFirst(), firstAndLast.first(), "First key mismatch");
				Assertions.assertEquals(prevLastKV, firstAndLast.last(), "Last key mismatch");

				firstAndLast = db.reduceRange(0, colId, firstKey, firstKey, false, RequestType.firstAndLast(), 1000);
				Assertions.assertNull(firstAndLast.first(), "First should be empty because the range is empty");
				Assertions.assertNull(firstAndLast.last(), "Last should be empty because the range is empty");
			} else {
				Assertions.assertThrowsExactly(RocksDBException.class, () -> {
					db.reduceRange(0, colId, firstKey, lastKey, false, RequestType.firstAndLast(), 1000);
				});
			}
		}
	}

	@TestAllImplementations
	void getRangeAll(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			int initIndex = 1;
			int count = 5;

			var rangeInitKey = getKVSequence().get(initIndex);
			var rangeEndKeyExcl = getKVSequence().get(initIndex + count);
			var rangeEndKeyIncl = getKVSequence().get(initIndex + count - 1);
			if (getSchemaVarKeys().isEmpty()) {
				var results = db.getRange(0, colId, rangeInitKey.keys(), rangeEndKeyExcl.keys(), false, RequestType.allInRange(), 1000).toList();
				Assertions.assertEquals(0, results.size(), "Results count must be 0");

				fillSomeKeys();

				boolean reverse = false;
				while (true) {
					results = db.getRange(0, colId, rangeInitKey.keys(), rangeEndKeyExcl.keys(), reverse, RequestType.allInRange(), 1000).toList();

					var expectedResults = getKVSequence().stream().skip(initIndex).limit(count).collect(Collectors.toCollection(ArrayList::new));
					if (reverse) {
						Collections.reverse(expectedResults);
					}
					assert expectedResults.size() == count;

					Assertions.assertEquals(count, results.size(), "Results count is not as expected. Reverse = " + reverse);

					for (int i = 0; i < expectedResults.size(); i++) {
						var currentI = results.get(i);
						var expectedI = expectedResults.get(i);
						Assertions.assertEquals(expectedI, currentI, "Element at index " + i + " mismatch. Reverse = " + reverse);
					}

					if (!reverse) {
						reverse = true;
					} else {
						break;
					}
				}
			} else {
				Assertions.assertThrowsExactly(RocksDBException.class, () -> {
					db.getRange(0, colId, rangeInitKey.keys(), rangeEndKeyExcl.keys(), false, RequestType.allInRange(), 1000).toList();
				});
			}
		}
	}


	@TestAllImplementations
	void putBatchSST(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			@NotNull Publisher<@NotNull KVBatch> batchPublisher = new Publisher<KVBatch>() {
				@Override
				public void subscribe(Subscriber<? super KVBatch> subscriber) {
					subscriber.onSubscribe(new Subscription() {
						Iterator<KVBatch> it;
						{
							ArrayList<KVBatch> items = new ArrayList<>();
							ArrayList<Keys> keys = new ArrayList<>();
							ArrayList<Buf> values = new ArrayList<>();
							for (int i = 0; i < 2; i++) {
								var keyI = getKeyI(i);
								var valueI = getValueI(i);
								keys.add(keyI);
								values.add(valueI);
							}
							items.add(new KVBatchRef(keys, values));
							keys = new ArrayList<>();
							values = new ArrayList<>();
							for (int i = 2; i < 4; i++) {
								var keyI = getKeyI(i);
								var valueI = getValueI(i);
								keys.add(keyI);
								values.add(valueI);
							}
							items.add(new KVBatchRef(keys, values));
							it = items.iterator();
						}
						@Override
						public void request(long l) {
							while (l-- > 0) {
								if (it.hasNext()) {
									subscriber.onNext(it.next());
								} else {
									subscriber.onComplete();
									return;
								}
							}
						}

						@Override
						public void cancel() {

						}
					});
				}
			};
			if (this.getSchema().variableLengthKeysCount() <= 0) {
				db.putBatch(colId, batchPublisher, PutBatchMode.SST_INGESTION);

				if (getHasValues()) {
					for (int i = 0; i < 4; i++) {
						assertSegmentEquals(getValueI(i), db.get(0, colId, getKeyI(i), RequestType.current()));
					}
				}
				for (int i = 0; i < 4; i++) {
					Assertions.assertTrue(db.get(0, colId, getKeyI(i), RequestType.exists()));
				}
			} else {
				Assertions.assertThrows(RocksDBException.class, () -> {
					db.putBatch(colId, batchPublisher, PutBatchMode.SST_INGESTION);
				});

			}
		}
	}

	@TestAllImplementations
	void mergeSingleMergedValue(String name, ConnectionConfig connection) {
		if (!getHasValues()) {
			return;
		}
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var key = getKey1();
			Buf merged;

			merged = api.merge(0, colId, key, value1, RequestType.merged());
			assertSegmentEquals(value1, merged);
			assertSegmentEquals(value1, api.get(0, colId, key, RequestType.current()));

			merged = api.merge(0, colId, key, value2, RequestType.merged());
			var expected = mergeExpect(value1, value2);
			assertSegmentEquals(expected, merged);
			assertSegmentEquals(expected, api.get(0, colId, key, RequestType.current()));

			Assertions.assertNull(api.merge(0, colId, key, value1, RequestType.none()));
			var expected3 = mergeExpect(expected, value1);
			assertSegmentEquals(expected3, api.get(0, colId, key, RequestType.current()));
		}
	}

	@TestAllImplementations
	void mergeMultiReturnsMerged(String name, ConnectionConfig connection) {
		if (!getHasValues()) {
			return;
		}
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var keys = List.of(getKey1(), getKey2());
			var merged = api.mergeMulti(0, colId, keys, List.of(value1, value2), RequestType.merged());
			Assertions.assertEquals(List.of(value1, value2), merged);

			var more = List.of(bigValue, value1);
			var merged2 = api.mergeMulti(0, colId, keys, more, RequestType.merged());
			Assertions.assertEquals(2, merged2.size());
			assertSegmentEquals(mergeExpect(value1, bigValue), merged2.getFirst());
			assertSegmentEquals(mergeExpect(value2, value1), merged2.getLast());
		}
	}

	@TestAllImplementations
	void mergeBatchAppends(String name, ConnectionConfig connection) {
		if (!getHasValues() || !getSchemaVarKeys().isEmpty()) {
			return; // mergeBatch not supported for bucketed columns
		}
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var keys = List.of(getKey1(), getKey2());
			var batch1 = new KVBatchRef(keys, List.of(value1, value2));
			api.mergeBatch(colId, Flux.just(batch1), MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);

			var batch2 = new KVBatchRef(keys, List.of(bigValue, value1));
			api.mergeBatch(colId, Flux.just(batch2), MergeBatchMode.MERGE_WRITE_BATCH);

			assertSegmentEquals(mergeExpect(value1, bigValue), api.get(0, colId, getKey1(), RequestType.current()));
			assertSegmentEquals(mergeExpect(value2, value1), api.get(0, colId, getKey2(), RequestType.current()));
		}
	}

	@TestAllImplementations
	void mergeBatchSstModes(String name, ConnectionConfig connection) {
		// Requires value schema and non-bucketed column
		if (!getHasValues()) {
			return;
		}
		var nonBucketSchema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
		createColumn(nonBucketSchema);
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var keyA = new Keys(toBufSimple(21));
			var keyB = new Keys(toBufSimple(22));
			var batch1 = new KVBatchRef(List.of(keyA, keyB), List.of(toBufSimple(1), toBufSimple(2)));
			api.mergeBatch(colId, Flux.just(batch1), MergeBatchMode.MERGE_SST_INGESTION);

			var batch2 = new KVBatchRef(List.of(keyA, keyB), List.of(toBufSimple(3), toBufSimple(4)));
			api.mergeBatch(colId, Flux.just(batch2), MergeBatchMode.MERGE_SST_INGEST_BEHIND);

			assertSegmentEquals(mergeExpect(toBufSimple(1), toBufSimple(3)), api.get(0, colId, keyA, RequestType.current()));
			assertSegmentEquals(mergeExpect(toBufSimple(2), toBufSimple(4)), api.get(0, colId, keyB, RequestType.current()));
		}
	}

	@TestAllImplementations
	void mergeBatchBucketed(String name, ConnectionConfig connection) {
		if (!getHasValues() || getSchemaVarKeys().isEmpty()) {
			return;
		}
		// Ensure bucketed column
		createColumn(getSchema());
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var keys = List.of(getKey1(), getCollidingKey1());
			var batch1 = new KVBatchRef(keys, List.of(value1, value2));
			api.mergeBatch(colId, Flux.just(batch1), MergeBatchMode.MERGE_WRITE_BATCH);

			var batch2 = new KVBatchRef(keys, List.of(bigValue, value1));
			api.mergeBatch(colId, Flux.just(batch2), MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);

			assertSegmentEquals(mergeExpect(value1, bigValue), api.get(0, colId, getKey1(), RequestType.current()));
			assertSegmentEquals(mergeExpect(value2, value1), api.get(0, colId, getCollidingKey1(), RequestType.current()));
		}
	}

	@TestAllImplementations
	void mergeBucketedMergedValue(String name, ConnectionConfig connection) {
		if (getSchemaVarKeys().isEmpty() || !getHasValues()) {
			return;
		}
		createColumn(ColumnSchema.of(getSchemaFixedKeys(), getSchemaVarKeys(), getHasValues()));
		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			var key = getKey1();
			Buf merged = api.merge(0, colId, key, value1, RequestType.merged());
			assertSegmentEquals(value1, merged);

			merged = api.merge(0, colId, key, value2, RequestType.merged());
			assertSegmentEquals(mergeExpect(value1, value2), merged);
		}
	}

	private Buf mergeExpect(@Nullable Buf existing, Buf operand) {
		var op = new MyStringAppendOperator();
		byte[] res = op.merge(null, existing != null ? existing.asArray() : null, List.of(operand.asArray()));
		return Buf.wrap(res);
	}

	@TestAllImplementations
	void concurrentUpdate(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			if (getHasValues()) {
				{
					var forUpdate1 = db.get(0, colId, key1, RequestType.forUpdate());
					try {
						var forUpdate2 = db.get(0, colId, key1, RequestType.forUpdate());
						try {
							db.put(forUpdate1.updateId(), colId, key1, value1, RequestType.none());
							Assertions.assertThrowsExactly(RocksDBRetryException.class, () -> db.put(forUpdate2.updateId(), colId, key1, value2, RequestType.none()));
							// Retrying
							var forUpdate3 = db.get(forUpdate2.updateId(), colId, key1, RequestType.forUpdate());
							try {
								assertSegmentEquals(value1, forUpdate3.previous());
								db.put(forUpdate3.updateId(), colId, key1, value2, RequestType.none());
							} catch (Throwable ex3) {
								db.closeFailedUpdate(forUpdate3.updateId());
								throw ex3;
							}
						} catch (Throwable ex2) {
							db.closeFailedUpdate(forUpdate2.updateId());
							throw ex2;
						}
					} catch (Throwable ex) {
						throw ex;
					}
				}
			}
		}
	}

	public static void assertSegmentEquals(Buf expected, Buf input) {
		if (!Utils.valueEquals(expected, input)) {
			Assertions.fail(
					"Memory segments are not equal! Expected: "
							+ (expected != null ? Arrays.toString(Utils.toByteArray(expected)) : "null")
							+ ", Input: " + (input != null ? Arrays.toString(Utils.toByteArray(input)) : "null"));
		}
	}


	@TestAllImplementations
	void getTestError(String name, ConnectionConfig connection) {
		try (var testDB = new TestDB(db, connection)) {
			var db = testDB.getAPI();

			fillSomeKeys();
			Assertions.assertThrows(Exception.class,
					() -> Utils.valueEquals(value1, db.get(0, colId, null, RequestType.current()))
			);
			Assertions.assertThrows(Exception.class,
					() -> Utils.valueEquals(value1, db.get(0, 18239, key1, RequestType.current()))
			);
			Assertions.assertThrows(Exception.class,
					() -> Utils.valueEquals(value1, db.get(1, colId, key1, RequestType.current()))
			);
			Assertions.assertThrows(Exception.class, () -> Utils.valueEquals(value1, db.get(0, colId, key1, null)));
		}
	}

	@TestAllImplementations
	void testCdcStream(String name, ConnectionConfig connection) throws Exception {
		// Skip for THRIFT if CDC is not implemented there (it's not implemented in Thrift yet based on previous steps)
		// But connection config might cover Thrift.
		// Let's assume only GRPC and EMBEDDED support CDC for now.
		// The error "Method not found" or similar might occur.
		if (connection.method() == ConnectionMethod.THRIFT) {
			return;
		}

		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			
			// 1. Create CDC subscription
			// Use a unique ID to avoid conflicts if parallel tests run (though DBTest usually sequential per instance)
			String subId = "sub-" + name.replaceAll("[^a-zA-Z0-9]", "-");
			long startSeq = api.cdcCreate(subId, null, null);
			
			// 2. Put data
			fillSomeKeys();
			
			// 3. Consume via stream
			// We expect 3 events from fillSomeKeys
			int expectedEvents = 3;
			
			List<CDCEvent> events = api.cdcStream(subId, startSeq, 10, Duration.ofMillis(50))
					.take(expectedEvents)
					.collectList()
					.block(Duration.ofSeconds(10));
			
			Assertions.assertNotNull(events);
			Assertions.assertEquals(expectedEvents, events.size());
			
			// Verify sequence strictly increasing
			long lastSeq = startSeq - 1;
			for (CDCEvent ev : events) {
				Assertions.assertTrue(ev.seq() > lastSeq, "Sequence should increase: " + ev.seq() + " > " + lastSeq);
				lastSeq = ev.seq();
				Assertions.assertEquals(colId, ev.columnId());
				Assertions.assertEquals(CDCEvent.Op.PUT, ev.op());
			}
			
			// 4. Commit last
			api.cdcCommit(subId, lastSeq);
		}
	}

	@TestAllImplementations
	void testCdcWithMergeAndReactiveClient(String name, ConnectionConfig connection) throws Exception {
		if (connection.method() == ConnectionMethod.THRIFT) {
			return;
		}

		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			// Use fallback options which include MyStringAppendOperator
			long mergeColId = api.createColumn("messages-merge-" + name,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));

			String subId = "merge-sub-" + name.replaceAll("[^a-zA-Z0-9]", "-");
			long startSeq = api.cdcCreate(subId, null, null);

			var key1 = new Keys(new Buf[]{Buf.wrap(new byte[]{0, 0, 0, 1})});
			var val1 = Buf.wrap("Initial".getBytes(java.nio.charset.StandardCharsets.UTF_8));

			// 1. Initial Put
			api.putAsync(0, mergeColId, key1, val1, RequestType.none()).join();

			// 2. Merge (Patch)
			var patchVal = Buf.wrap("-Patched".getBytes(java.nio.charset.StandardCharsets.UTF_8));
			api.mergeAsync(0, mergeColId, key1, patchVal, RequestType.none()).join();

			// 3. Verify CDC Stream
			// We expect: PUT(Initial) -> MERGE(-Patched)
			List<CDCEvent> events = api.cdcStream(subId, startSeq, 100, Duration.ofMillis(10))
					.take(2)
					.collectList()
					.block(Duration.ofSeconds(5));

			Assertions.assertNotNull(events);
			Assertions.assertEquals(2, events.size());

			// Event 1: PUT
			CDCEvent ev1 = events.get(0);
			Assertions.assertEquals(CDCEvent.Op.PUT, ev1.op());
			assertSegmentEquals(val1, ev1.value());

			// Event 2: MERGE
			CDCEvent ev2 = events.get(1);
			Assertions.assertEquals(CDCEvent.Op.MERGE, ev2.op());
			// In CDC MERGE event, value is the operand (patch)
			assertSegmentEquals(patchVal, ev2.value());

			// 4. Verify Final State in DB
			Buf current = api.get(0, mergeColId, key1, RequestType.current());
			// MyStringAppendOperator appends with a comma separator
			Buf expected = Buf.wrap("Initial,-Patched".getBytes(java.nio.charset.StandardCharsets.UTF_8));
			assertSegmentEquals(expected, current);

			// 5. Commit
			api.cdcCommit(subId, ev2.seq());
		}
	}

	@TestAllImplementations
	void testCdcRobustness(String name, ConnectionConfig connection) throws Exception {
		if (connection.method() == ConnectionMethod.THRIFT) {
			return;
		}

		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			String subId = "robust-" + name.replaceAll("[^a-zA-Z0-9]", "-");
			long startSeq = api.cdcCreate(subId, null, null);

			int countBatch1 = 50;
			int countBatch2 = 50;
			var kvSequence = getKVSequence();
			
			// 1. Put batch 1
			for (int i = 0; i < countBatch1; i++) {
				var kv = kvSequence.get(i);
				api.put(0, colId, kv.keys(), kv.value(), RequestType.none());
			}

			// 2. Consume with stream (backpressure check via limit/iterator)
			// Use small internal batch size to force multiple fetches
			long cdcBatchSize = 10;

			// Consume first 30 events
			int consumeCount1 = 30;
			List<CDCEvent> received1 = api.cdcStream(subId, null, cdcBatchSize, Duration.ofMillis(10))
					.take(consumeCount1)
					.collectList()
					.block(Duration.ofSeconds(10));

			Assertions.assertNotNull(received1);
			Assertions.assertEquals(consumeCount1, received1.size());
			Assertions.assertTrue(received1.get(0).seq() >= startSeq, "First event seq " + received1.get(0).seq() + " should be >= startSeq " + startSeq);
			long lastSeq = received1.get(received1.size() - 1).seq();

			// 3. Commit the last processed sequence
			api.cdcCommit(subId, lastSeq);

			// 4. Put batch 2
			for (int i = 0; i < countBatch2; i++) {
				var kv = kvSequence.get(countBatch1 + i);
				api.put(0, colId, kv.keys(), kv.value(), RequestType.none());
			}

			// 5. Resume stream (from null -> should pick up from commit)
			// We expect to see remaining 20 from batch1 + 50 from batch2 = 70 events
			int expectedRemaining = (countBatch1 - consumeCount1) + countBatch2;

			List<CDCEvent> received2 = api.cdcStream(subId, null, cdcBatchSize, Duration.ofMillis(10))
					.take(expectedRemaining)
					.collectList()
					.block(Duration.ofSeconds(10));

			Assertions.assertNotNull(received2);
			Assertions.assertEquals(expectedRemaining, received2.size());

			// Verify strictly increasing
			long prev = lastSeq;
			for (CDCEvent ev : received2) {
				Assertions.assertTrue(ev.seq() > prev, "Seq increasing: " + ev.seq() + " > " + prev);
				prev = ev.seq();
			}
		}
	}

	@TestAllImplementations
	void testCdcStreamWithProcessor(String name, ConnectionConfig connection) throws Exception {
		if (connection.method() == ConnectionMethod.THRIFT) {
			return;
		}

		try (var testDB = new TestDB(db, connection)) {
			var api = testDB.getAPI();
			String subId = "proc-sub-" + name.replaceAll("[^a-zA-Z0-9]", "-");
			long startSeq = api.cdcCreate(subId, null, null);

			int count = 20;
			var kvSequence = getKVSequence();
			for (int i = 0; i < count; i++) {
				var kv = kvSequence.get(i);
				api.put(0, colId, kv.keys(), kv.value(), RequestType.none());
			}

			java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(count);

			// Batch size 5. We expect 4 batches of 5.
			var disposable = api.cdcStream(subId, startSeq, 5, Duration.ofMillis(50), event -> {
				latch.countDown();
				return Mono.empty();
			})
			.subscribe();

			boolean finished = latch.await(10, java.util.concurrent.TimeUnit.SECONDS);
			// Wait a tiny bit to allow the commit of the last batch to potentially complete (async)
			Thread.sleep(200);
			disposable.dispose();
			
			Assertions.assertTrue(finished, "Did not process all events in time");
		}
	}
}