package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.emptyBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.foreign.Arena;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;

import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.util.stream.Collectors;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

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
		arena = Arena.ofShared();
		db = new EmbeddedConnection(null, "test", null);
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
}