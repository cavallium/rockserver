package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegmentSimple;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Callback;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Utils;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.lang.foreign.MemorySegment;

class EmbeddedDBTest {

	private EmbeddedConnection db;
	private long colId = 0L;
	private Arena arena;
	private MemorySegment bigValue;
	private MemorySegment[] key1;
	private MemorySegment[] collidingKey1;
	private MemorySegment[] key2;
	private MemorySegment value1;
	private MemorySegment value2;

	@org.junit.jupiter.api.BeforeEach
	void setUp() throws IOException {
		if (System.getProperty("rockserver.core.print-config", null) == null) {
			System.setProperty("rockserver.core.print-config", "false");
		}
		arena = Arena.ofShared();
		db = new EmbeddedConnection(null, "test", null);
		createStandardColumn();


		var bigValueArray = new byte[10_000];
		ThreadLocalRandom.current().nextBytes(bigValueArray);
		bigValue = MemorySegment.ofArray(bigValueArray);
		key1 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				toMemorySegmentSimple(6, 7, 8)
		};
		collidingKey1 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				toMemorySegmentSimple(6, 7, -48)
		};
		key2 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				toMemorySegmentSimple(6, 7, 7)
		};

		value1 = MemorySegment.ofArray(new byte[] {0, 0, 3});
		value2 = MemorySegment.ofArray(new byte[] {0, 0, 5});
	}

	private void createStandardColumn() {
		createColumn(ColumnSchema.of(IntList.of(1, 2, 1),
				ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32),
				true
		));
	}

	private void createColumn(ColumnSchema schema) {
		if (colId != 0L) {
			db.deleteColumn(colId);
		}
		colId = db.createColumn("column-test", schema);
	}

	void fillSomeKeys() {
		Assertions.assertNull(db.put(arena, 0, colId, key1, value1, Callback.none()));
		Assertions.assertNull(db.put(arena, 0, colId, collidingKey1, value2, Callback.none()));
		Assertions.assertNull(db.put(arena, 0, colId, key2, bigValue, Callback.none()));
		for (int i = 0; i < Byte.MAX_VALUE; i++) {
			var keyI = new MemorySegment[] {
					toMemorySegmentSimple(3),
					toMemorySegmentSimple(4, 6),
					toMemorySegmentSimple(3),
					toMemorySegmentSimple(1, 2, 3),
					toMemorySegmentSimple(8, 2, 5, 1, 7, i)
			};
			var valueI = toMemorySegmentSimple(i, i, i, i, i);
			Assertions.assertNull(db.put(arena, 0, colId, keyI, valueI, Callback.none()));
		}
	}

	@org.junit.jupiter.api.AfterEach
	void tearDown() throws IOException {
		db.deleteColumn(colId);
		db.close();
		arena.close();
	}

	@org.junit.jupiter.api.Test
	void putSameBucketSameKey() {
		var key = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				toMemorySegmentSimple(0, 0, 3, 6, 7, 8)
		};
		var value1 = MemorySegment.ofArray(new byte[] {0, 0, 3});
		var value2 = MemorySegment.ofArray(new byte[] {0, 0, 5});

		var delta = db.put(arena, 0, colId, key, value1, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(arena, 0, colId, key, value2, Callback.delta());
		Assertions.assertTrue(Utils.valueEquals(delta.previous(), value1));
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value2));
	}

	@org.junit.jupiter.api.Test
	void putSameBucketDifferentKey() {
		createColumn(ColumnSchema.of(IntList.of(1, 2, 1), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.ALLSAME8), true));

		var lastKey1 = toMemorySegmentSimple(6, 7, 8);
		var lastKey2 = toMemorySegmentSimple(6, 7, -48);

		var key1 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				lastKey1
		};
		var key2 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				lastKey2
		};

		var value1 = MemorySegment.ofArray(new byte[] {0, 0, 3});
		var value2 = MemorySegment.ofArray(new byte[] {0, 0, 5});

		var delta = db.put(arena, 0, colId, key1, value1, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(arena, 0, colId, key2, value2, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value2));

		delta = db.put(arena, 0, colId, key2, value1, Callback.delta());
		Assertions.assertTrue(Utils.valueEquals(delta.previous(), value2));
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));
	}

	/**
	 * Some keys have same bucket, some not
	 */
	@org.junit.jupiter.api.Test
	void putMixedBucketMixedKey() {
		createColumn(ColumnSchema.of(IntList.of(1, 2, 1), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH8), true));

		var lastKey1 = toMemorySegmentSimple(6, 7, 8);
		var collidingLastKey1 = toMemorySegmentSimple(6, 7, -48);
		var lastKey2 = toMemorySegmentSimple(6, 7, 7);

		var key1 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				lastKey1
		};
		var collidingKey1 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				collidingLastKey1
		};
		var key2 = new MemorySegment[] {
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(4, 6),
				toMemorySegmentSimple(3),
				toMemorySegmentSimple(1, 2, 3),
				lastKey2
		};

		var value1 = MemorySegment.ofArray(new byte[] {0, 0, 3});
		var value2 = MemorySegment.ofArray(new byte[] {0, 0, 5});

		var delta = db.put(arena, 0, colId, key1, value1, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(arena, 0, colId, collidingKey1, value2, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value2));

		delta = db.put(arena, 0, colId, collidingKey1, value1, Callback.delta());
		Assertions.assertTrue(Utils.valueEquals(delta.previous(), value2));
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(arena, 0, colId, key2, value1, Callback.delta());
		Assertions.assertNull(delta.previous());
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value1));

		delta = db.put(arena, 0, colId, key2, value2, Callback.delta());
		Assertions.assertTrue(Utils.valueEquals(delta.previous(), value1));
		Assertions.assertTrue(Utils.valueEquals(delta.current(), value2));
	}

	@org.junit.jupiter.api.Test
	void get() {
		fillSomeKeys();
		Assertions.assertTrue(Utils.valueEquals(value1, db.get(arena, 0, colId, key1, Callback.current())));
		Assertions.assertTrue(Utils.valueEquals(value2, db.get(arena, 0, colId, collidingKey1, Callback.current())));
		Assertions.assertTrue(Utils.valueEquals(bigValue, db.get(arena, 0, colId, key2, Callback.current())));
	}
}