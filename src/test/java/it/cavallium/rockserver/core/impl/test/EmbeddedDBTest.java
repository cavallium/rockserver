package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegmentSimple;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.Utils;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import org.junit.jupiter.api.Test;

abstract class EmbeddedDBTest {

	protected EmbeddedConnection db;
	protected long colId = 0L;
	protected Arena arena;
	protected MemorySegment bigValue;
	protected Keys key1;
	protected Keys collidingKey1;
	protected Keys key2;
	protected MemorySegment value1;
	protected MemorySegment value2;

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

	protected MemorySegment getBigValue() {
		var bigValueArray = new byte[10_000];
		ThreadLocalRandom.current().nextBytes(bigValueArray);
		return Utils.toMemorySegment(arena, bigValueArray);
	}

	protected Keys getKey2() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 7)
		});
	}

	protected Keys getCollidingKey1() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, -48)
		});
	}

	protected Keys getKey1() {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 6, 7, 8)
		});
	}

	protected boolean getHasValues() {
		return true;
	}

	protected MemorySegment getValue1() {
		return Utils.toMemorySegmentSimple(arena, 0, 0, 3);
	}

	protected MemorySegment getValue2() {
		return Utils.toMemorySegmentSimple(arena, 0, 0, 5);
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
		Assertions.assertNull(db.put(arena, 0, colId, key1, value1, RequestType.none()));
		Assertions.assertNull(db.put(arena, 0, colId, collidingKey1, value2, RequestType.none()));
		Assertions.assertNull(db.put(arena, 0, colId, key2, bigValue, RequestType.none()));
		for (int i = 0; i < Byte.MAX_VALUE; i++) {
			var keyI = getKeyI(i);
			var valueI = getValueI(i);
			Assertions.assertNull(db.put(arena, 0, colId, keyI, valueI, RequestType.none()));
		}
	}

	protected Keys getKeyI(int i) {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 7, i)
		});
	}

	protected Keys getNotFoundKeyI(int i) {
		return new Keys(new MemorySegment[] {
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 4, 6),
				toMemorySegmentSimple(arena, 3),
				toMemorySegmentSimple(arena, 1, 2, 3),
				toMemorySegmentSimple(arena, 8, 2, 5, 1, 0, i)
		});
	}

	protected MemorySegment getValueI(int i) {
		return toMemorySegmentSimple(arena, i, i, i, i, i);
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() throws IOException {
		db.deleteColumn(colId);
		db.close();
		arena.close();
	}

	@SuppressWarnings("DataFlowIssue")
	@Test
	void putTestErrors() {
		var key = getKey1();

		if (!getHasValues()) {
			Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, key, toMemorySegmentSimple(arena, 123), RequestType.delta()));
		} else {
			Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, key, MemorySegment.NULL, RequestType.delta()));
		}

		Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, key, null, RequestType.delta()));
		Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, null, value1, RequestType.delta()));
		Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, null, null, RequestType.delta()));
		Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, colId, key, value1, null));
		Assertions.assertThrows(Exception.class, () -> db.put(arena, 1, colId, key, value1, RequestType.delta()));
		Assertions.assertThrows(Exception.class, () -> db.put(arena, 0, 21203, key, value1, RequestType.delta()));
	}

	@Test
	void putSameBucketSameKey() {
		var key = getKey1();
		var value1 = getValue1();
		var value2 = getValue2();

		Delta<MemorySegment> delta;

		delta = db.put(arena, 0, colId, key, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key, value2, RequestType.delta());
		assertSegmentEquals(value1, delta.previous());
		assertSegmentEquals(value2, delta.current());
	}

	@Test
	void putSameBucketDifferentKey() {
		if (getSchemaVarKeys().isEmpty()) {
			return;
		}
		createColumn(ColumnSchema.of(getSchemaFixedKeys(), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.ALLSAME8), getHasValues()));

		var key1 = getKey1();
		var key2 = getCollidingKey1();

		var value1 = getValue1();
		var value2 = getValue2();

		Delta<MemorySegment> delta;

		Assertions.assertFalse(db.put(arena, 0, colId, getKeyI(3), value2, RequestType.previousPresence()));
		Assertions.assertFalse(db.put(arena, 0, colId, getKeyI(4), value2, RequestType.previousPresence()));


		delta = db.put(arena, 0, colId, key1, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key2, value2, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value2, delta.current());

		delta = db.put(arena, 0, colId, key2, value1, RequestType.delta());
		assertSegmentEquals(value2, delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key2, value1, RequestType.delta());
		assertSegmentEquals(value1, delta.previous());
		assertSegmentEquals(value1, delta.current());


		Assertions.assertTrue(db.put(arena, 0, colId, key1, value2, RequestType.previousPresence()));
		Assertions.assertTrue(db.put(arena, 0, colId, key2, value2, RequestType.previousPresence()));


		delta = db.put(arena, 0, colId, key1, value1, RequestType.delta());
		assertSegmentEquals(value2, delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key2, value1, RequestType.delta());
		assertSegmentEquals(value2, delta.previous());
		assertSegmentEquals(value1, delta.current());


		Assertions.assertNull(db.put(arena, 0, colId, key1, value2, RequestType.none()));
		Assertions.assertNull(db.put(arena, 0, colId, key2, value2, RequestType.none()));


		assertSegmentEquals(value2, db.put(arena, 0, colId, key1, value1, RequestType.previous()));
		assertSegmentEquals(value2, db.put(arena, 0, colId, key2, value1, RequestType.previous()));

		assertSegmentEquals(value1, db.put(arena, 0, colId, key1, value1, RequestType.previous()));
		assertSegmentEquals(value1, db.put(arena, 0, colId, key2, value1, RequestType.previous()));

		if (!Utils.valueEquals(value1, value2)) {
			Assertions.assertTrue(db.put(arena, 0, colId, key1, value2, RequestType.changed()));
			Assertions.assertTrue(db.put(arena, 0, colId, key2, value2, RequestType.changed()));
		}

		Assertions.assertFalse(db.put(arena, 0, colId, key1, value2, RequestType.changed()));
		Assertions.assertFalse(db.put(arena, 0, colId, key2, value2, RequestType.changed()));


		assertSegmentEquals(value2, db.put(arena, 0, colId, key1, value1, RequestType.previous()));
		assertSegmentEquals(value2, db.put(arena, 0, colId, key2, value1, RequestType.previous()));
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
	@Test
	void putMixedBucketMixedKey() {
		var key1 = getKey1();
		var collidingKey1 = getCollidingKey1();
		var key2 = getKey2();

		var value1 = getValue1();
		var value2 = getValue2();

		var delta = db.put(arena, 0, colId, key1, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, collidingKey1, value2, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value2, delta.current());

		delta = db.put(arena, 0, colId, collidingKey1, value1, RequestType.delta());
		assertSegmentEquals(value2, delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key2, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertSegmentEquals(value1, delta.current());

		delta = db.put(arena, 0, colId, key2, value2, RequestType.delta());
		assertSegmentEquals(value1, delta.previous());
		assertSegmentEquals(value2, delta.current());
	}

	@Test
	void get() {
		if (getHasValues()) {
			Assertions.assertNull(db.get(arena, 0, colId, key1, RequestType.current()));
			Assertions.assertNull(db.get(arena, 0, colId, collidingKey1, RequestType.current()));
			Assertions.assertNull(db.get(arena, 0, colId, key2, RequestType.current()));
		}
		Assertions.assertFalse(db.get(arena, 0, colId, key1, RequestType.exists()));
		Assertions.assertFalse(db.get(arena, 0, colId, collidingKey1, RequestType.exists()));
		Assertions.assertFalse(db.get(arena, 0, colId, key2, RequestType.exists()));

		fillSomeKeys();

		if (getHasValues()) {
			assertSegmentEquals(value1, db.get(arena, 0, colId, key1, RequestType.current()));
			Assertions.assertNull(db.get(arena, 0, colId, getNotFoundKeyI(0), RequestType.current()));
			assertSegmentEquals(value2, db.get(arena, 0, colId, collidingKey1, RequestType.current()));
			assertSegmentEquals(bigValue, db.get(arena, 0, colId, key2, RequestType.current()));
		}

		Assertions.assertTrue(db.get(arena, 0, colId, key1, RequestType.exists()));
		Assertions.assertFalse(db.get(arena, 0, colId, getNotFoundKeyI(0), RequestType.exists()));
		Assertions.assertTrue(db.get(arena, 0, colId, collidingKey1, RequestType.exists()));
		Assertions.assertTrue(db.get(arena, 0, colId, key2, RequestType.exists()));
	}

	@Test
	void update() {
		if (getHasValues()) {
			var forUpdate = db.get(arena, 0, colId, key1, RequestType.forUpdate());
			Assertions.assertNull(forUpdate.previous());
			Assertions.assertTrue(forUpdate.updateId() != 0);
			db.put(arena, forUpdate.updateId(), colId, key1, value1, RequestType.none());

			Assertions.assertThrows(Exception.class, () -> db.put(arena, forUpdate.updateId(), colId, key1, value2, RequestType.none()));
		}
	}

	@Test
	void concurrentUpdate() {
		if (getHasValues()) {
			{
				var forUpdate1 = db.get(arena, 0, colId, key1, RequestType.forUpdate());
				try {
					var forUpdate2 = db.get(arena, 0, colId, key1, RequestType.forUpdate());
					try {
						db.put(arena, forUpdate1.updateId(), colId, key1, value1, RequestType.none());
						Assertions.assertThrowsExactly(RocksDBRetryException.class, () -> db.put(arena, forUpdate2.updateId(), colId, key1, value2, RequestType.none()));
						// Retrying
						var forUpdate3 = db.get(arena, forUpdate2.updateId(), colId, key1, RequestType.forUpdate());
						try {
							assertSegmentEquals(value1, forUpdate3.previous());
							db.put(arena, forUpdate3.updateId(), colId, key1, value2, RequestType.none());
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

	public static void assertSegmentEquals(MemorySegment expected, MemorySegment input) {
		if (!Utils.valueEquals(expected, input)) {
			Assertions.fail(
					"Memory segments are not equal! Expected: "
							+ (expected != null ? Arrays.toString(Utils.toByteArray(expected)) : "null")
							+ ", Input: " + (input != null ? Arrays.toString(Utils.toByteArray(input)) : "null"));
		}
	}

	@SuppressWarnings("DataFlowIssue")
	@Test
	void getTestError() {
		fillSomeKeys();
		Assertions.assertThrows(Exception.class, () -> Utils.valueEquals(value1, db.get(arena, 0, colId, null, RequestType.current())));
		Assertions.assertThrows(Exception.class, () -> Utils.valueEquals(value1, db.get(arena, 0, 18239, key1, RequestType.current())));
		Assertions.assertThrows(Exception.class, () -> Utils.valueEquals(value1, db.get(arena, 1, colId, key1, RequestType.current())));
		Assertions.assertThrows(Exception.class, () -> Utils.valueEquals(value1, db.get(arena, 0, colId, key1, null)));
	}
}