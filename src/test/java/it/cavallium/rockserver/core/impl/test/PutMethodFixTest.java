package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.*;

/**
 * Tests to verify the correctness of the new put() implementation from possible-fix.md.
 *
 * Key changes tested:
 * 1. On owned-tx retry, a fresh transaction is opened instead of reusing the failed one
 *    (prevents C++ WriteBatch growth / native memory leak).
 * 2. On updateId != 0 conflict, rollbackToSavePoint + single undoGetForUpdate is used
 *    instead of the old undosCount logic that could double-undo.
 * 3. All put callback types (none, previous, previousPresence, changed, delta) work correctly.
 * 4. Concurrent puts with callbacks that require transactions are safe under contention.
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class PutMethodFixTest {

	private EmbeddedConnection db;
	private long colId;
	private long colIdNoBuckets;
	private Path configFile;

	// Column with buckets (var keys with hash)
	private static final ColumnSchema SCHEMA_WITH_BUCKETS = ColumnSchema.of(
			IntList.of(1, 2, 1),
			ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH8),
			true
	);

	// Column without buckets (fixed keys only)
	private static final ColumnSchema SCHEMA_NO_BUCKETS = ColumnSchema.of(
			IntList.of(1, 2, 1),
			ObjectList.of(),
			true
	);

	private Keys makeKey(int a, int b, int c) {
		return new Keys(new Buf[]{
				toBufSimple(a),
				toBufSimple(b, b),
				toBufSimple(c)
		});
	}

	private Keys makeKeyWithVar(int a, int b, int c, int... var1) {
		return new Keys(new Buf[]{
				toBufSimple(a),
				toBufSimple(b, b),
				toBufSimple(c),
				Buf.wrap(toBytes(var1)),
				Buf.wrap(new byte[]{(byte) (var1.length > 0 ? var1[0] : 0)})
		});
	}

	private static byte[] toBytes(int[] ints) {
		byte[] bytes = new byte[ints.length];
		for (int i = 0; i < ints.length; i++) bytes[i] = (byte) ints[i];
		return bytes;
	}

	@BeforeEach
	void setUp() throws IOException {
		if (System.getProperty("rockserver.core.print-config", null) == null) {
			System.setProperty("rockserver.core.print-config", "false");
		}
		configFile = Files.createTempFile("putfix-test", ".conf");
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
		db = new EmbeddedConnection(null, "test", configFile);
		colId = db.createColumn("col-buckets", SCHEMA_WITH_BUCKETS);
		colIdNoBuckets = db.createColumn("col-nobuckets", SCHEMA_NO_BUCKETS);
	}

	@AfterEach
	void tearDown() throws IOException {
		db.deleteColumn(colId);
		db.deleteColumn(colIdNoBuckets);
		db.close();
		Files.deleteIfExists(configFile);
	}

	// ==================== Basic put correctness with all callback types ====================

	@Test
	void putNone_noBuckets() {
		var key = makeKey(1, 2, 3);
		var value = toBufSimple(10, 20, 30);
		Assertions.assertNull(db.put(0, colIdNoBuckets, key, value, RequestType.none()));
	}

	@Test
	void putPrevious_noBuckets_returnsNullOnFirstInsert() {
		var key = makeKey(1, 2, 3);
		var value = toBufSimple(10, 20, 30);
		Buf prev = db.put(0, colIdNoBuckets, key, value, RequestType.previous());
		Assertions.assertNull(prev);
	}

	@Test
	void putPrevious_noBuckets_returnsPreviousOnUpdate() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);
		db.put(0, colIdNoBuckets, key, value1, RequestType.none());
		Buf prev = db.put(0, colIdNoBuckets, key, value2, RequestType.previous());
		assertBufEquals(value1, prev);
	}

	@Test
	void putPreviousPresence_noBuckets() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);

		Boolean present = db.put(0, colIdNoBuckets, key, value1, RequestType.previousPresence());
		Assertions.assertFalse(present);

		present = db.put(0, colIdNoBuckets, key, value2, RequestType.previousPresence());
		Assertions.assertTrue(present);
	}

	@Test
	void putChanged_noBuckets() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);

		// First insert: previous is null, value is non-null => changed
		Boolean changed = db.put(0, colIdNoBuckets, key, value1, RequestType.changed());
		Assertions.assertTrue(changed);

		// Same value => not changed
		changed = db.put(0, colIdNoBuckets, key, value1, RequestType.changed());
		Assertions.assertFalse(changed);

		// Different value => changed
		changed = db.put(0, colIdNoBuckets, key, value2, RequestType.changed());
		Assertions.assertTrue(changed);
	}

	@Test
	void putDelta_noBuckets() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);

		Delta<Buf> delta = db.put(0, colIdNoBuckets, key, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertBufEquals(value1, delta.current());

		delta = db.put(0, colIdNoBuckets, key, value2, RequestType.delta());
		assertBufEquals(value1, delta.previous());
		assertBufEquals(value2, delta.current());
	}

	// ==================== Bucketed column put correctness ====================

	@Test
	void putPrevious_bucketed_returnsNullOnFirstInsert() {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		var value = toBufSimple(100, 200);
		Buf prev = db.put(0, colId, key, value, RequestType.previous());
		Assertions.assertNull(prev);
	}

	@Test
	void putPrevious_bucketed_returnsPreviousOnUpdate() {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		var value1 = toBufSimple(100, 200);
		var value2 = toBufSimple(150, 250);
		db.put(0, colId, key, value1, RequestType.none());
		Buf prev = db.put(0, colId, key, value2, RequestType.previous());
		assertBufEquals(value1, prev);
	}

	@Test
	void putDelta_bucketed() {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		var value1 = toBufSimple(100, 200);
		var value2 = toBufSimple(150, 250);

		Delta<Buf> delta = db.put(0, colId, key, value1, RequestType.delta());
		Assertions.assertNull(delta.previous());
		assertBufEquals(value1, delta.current());

		delta = db.put(0, colId, key, value2, RequestType.delta());
		assertBufEquals(value1, delta.previous());
		assertBufEquals(value2, delta.current());
	}

	@Test
	void putChanged_bucketed() {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		var value1 = toBufSimple(100, 200);

		Boolean changed = db.put(0, colId, key, value1, RequestType.changed());
		Assertions.assertTrue(changed);

		changed = db.put(0, colId, key, value1, RequestType.changed());
		Assertions.assertFalse(changed);
	}

	// ==================== Concurrent update with updateId (tests rollbackToSavePoint fix) ====================

	@Test
	void concurrentUpdate_retrySucceeds() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);
		var value3 = toBufSimple(70, 80, 90);

		// First writer gets forUpdate lock
		var forUpdate1 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		Assertions.assertNull(forUpdate1.previous());

		// Second writer gets forUpdate lock on same key
		var forUpdate2 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		Assertions.assertNull(forUpdate2.previous());

		// First writer commits successfully
		db.put(forUpdate1.updateId(), colIdNoBuckets, key, value1, RequestType.none());

		// Second writer should fail with RocksDBRetryException
		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.put(forUpdate2.updateId(), colIdNoBuckets, key, value2, RequestType.none()));

		// Second writer retries with a new forUpdate on the same transaction
		var forUpdate3 = db.get(forUpdate2.updateId(), colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(value1, forUpdate3.previous());

		// Now the retry should succeed
		db.put(forUpdate3.updateId(), colIdNoBuckets, key, value3, RequestType.none());

		// Verify final value
		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(value3, result);
	}

	@Test
	void concurrentUpdate_retrySucceeds_bucketed() {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		var value1 = toBufSimple(100, 200);
		var value2 = toBufSimple(150, 250);
		var value3 = toBufSimple(170, 270);

		var forUpdate1 = db.get(0, colId, key, RequestType.forUpdate());
		Assertions.assertNull(forUpdate1.previous());

		var forUpdate2 = db.get(0, colId, key, RequestType.forUpdate());
		Assertions.assertNull(forUpdate2.previous());

		db.put(forUpdate1.updateId(), colId, key, value1, RequestType.none());

		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.put(forUpdate2.updateId(), colId, key, value2, RequestType.none()));

		var forUpdate3 = db.get(forUpdate2.updateId(), colId, key, RequestType.forUpdate());
		assertBufEquals(value1, forUpdate3.previous());

		db.put(forUpdate3.updateId(), colId, key, value3, RequestType.none());

		Buf result = db.get(0, colId, key, RequestType.current());
		assertBufEquals(value3, result);
	}

	@Test
	void concurrentUpdate_closeFailedUpdateAfterRetryException() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);
		var value2 = toBufSimple(40, 50, 60);

		var forUpdate1 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		var forUpdate2 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());

		db.put(forUpdate1.updateId(), colIdNoBuckets, key, value1, RequestType.none());

		try {
			db.put(forUpdate2.updateId(), colIdNoBuckets, key, value2, RequestType.none());
			Assertions.fail("Should have thrown RocksDBRetryException");
		} catch (RocksDBRetryException e) {
			// Close the failed update cleanly
			db.closeFailedUpdate(forUpdate2.updateId());
		}

		// Verify the first write persisted
		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(value1, result);
	}

	// ==================== Multi-threaded stress tests (tests fresh-tx-on-retry fix) ====================

	@Test
	void concurrentPutWithPrevious_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(1, 2, 3);
		int numThreads = 8;
		int putsPerThread = 50;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		// Seed initial value
		db.put(0, colIdNoBuckets, key, toBufSimple(0), RequestType.none());

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var value = toBufSimple(threadId, i);
						// RequestType.previous() forces a transaction with getForUpdate internally
						db.put(0, colIdNoBuckets, key, value, RequestType.previous());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent put: " + errors);

		// Verify key is readable
		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		Assertions.assertNotNull(result);
	}

	@Test
	void concurrentPutWithDelta_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(2, 3, 4);
		int numThreads = 8;
		int putsPerThread = 50;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var value = toBufSimple(threadId, i);
						Delta<Buf> delta = db.put(0, colIdNoBuckets, key, value, RequestType.delta());
						Assertions.assertNotNull(delta);
						assertBufEquals(value, delta.current());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent put with delta: " + errors);

		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		Assertions.assertNotNull(result);
	}

	@Test
	void concurrentPutWithChanged_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(3, 4, 5);
		int numThreads = 8;
		int putsPerThread = 50;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var value = toBufSimple(threadId, i);
						db.put(0, colIdNoBuckets, key, value, RequestType.changed());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent put with changed: " + errors);
	}

	@Test
	void concurrentPutWithPreviousPresence_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(4, 5, 6);
		int numThreads = 8;
		int putsPerThread = 50;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var value = toBufSimple(threadId, i);
						db.put(0, colIdNoBuckets, key, value, RequestType.previousPresence());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent put with previousPresence: " + errors);
	}

	@Test
	void concurrentPutWithPrevious_bucketed_multiThreaded() throws Exception {
		var key = makeKeyWithVar(1, 2, 3, 10, 20);
		int numThreads = 8;
		int putsPerThread = 50;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var value = toBufSimple(threadId, i);
						db.put(0, colId, key, value, RequestType.previous());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent bucketed put: " + errors);

		Buf result = db.get(0, colId, key, RequestType.current());
		Assertions.assertNotNull(result);
	}

	@Test
	void concurrentPutDifferentKeys_noBuckets_multiThreaded() throws Exception {
		int numThreads = 8;
		int putsPerThread = 100;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < putsPerThread; i++) {
						var key = makeKey(threadId, i, 0);
						var value = toBufSimple(threadId, i);
						Delta<Buf> delta = db.put(0, colIdNoBuckets, key, value, RequestType.delta());
						Assertions.assertNull(delta.previous());
						assertBufEquals(value, delta.current());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(),
				"Errors during concurrent different-key put: " + errors);

		// Verify all keys are present
		for (int t = 0; t < numThreads; t++) {
			for (int i = 0; i < putsPerThread; i++) {
				var key = makeKey(t, i, 0);
				Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
				assertBufEquals(toBufSimple(t, i), result);
			}
		}
	}

	// ==================== Delete methods tests ====================

	@Test
	void deleteNone_noBuckets() {
		var key = makeKey(1, 1, 1);
		var value = toBufSimple(10);
		db.put(0, colIdNoBuckets, key, value, RequestType.none());

		db.delete(0, colIdNoBuckets, key, RequestType.none());
		Assertions.assertNull(db.get(0, colIdNoBuckets, key, RequestType.current()));
	}

	@Test
	void deletePrevious_noBuckets() {
		var key = makeKey(1, 1, 1);
		var value = toBufSimple(10);
		db.put(0, colIdNoBuckets, key, value, RequestType.none());

		Buf previous = db.delete(0, colIdNoBuckets, key, RequestType.previous());
		assertBufEquals(value, previous);
		Assertions.assertNull(db.get(0, colIdNoBuckets, key, RequestType.current()));
	}

	@Test
	void deletePreviousPresence_noBuckets() {
		var key = makeKey(1, 1, 1);
		var value = toBufSimple(10);
		db.put(0, colIdNoBuckets, key, value, RequestType.none());

		Boolean present = db.delete(0, colIdNoBuckets, key, RequestType.previousPresence());
		Assertions.assertTrue(present);
		Assertions.assertNull(db.get(0, colIdNoBuckets, key, RequestType.current()));

		present = db.delete(0, colIdNoBuckets, key, RequestType.previousPresence());
		Assertions.assertFalse(present);
	}

	@Test
	void deleteConcurrentUpdate_retrySucceeds() {
		var key = makeKey(1, 2, 3);
		var value1 = toBufSimple(10, 20, 30);

		db.put(0, colIdNoBuckets, key, value1, RequestType.none());

		// First writer gets forUpdate lock
		var forUpdate1 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(value1, forUpdate1.previous());

		// Second writer gets forUpdate lock on same key
		var forUpdate2 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(value1, forUpdate2.previous());

		// First writer commits delete successfully
		db.delete(forUpdate1.updateId(), colIdNoBuckets, key, RequestType.none());

		// Second writer should fail with RocksDBRetryException
		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.delete(forUpdate2.updateId(), colIdNoBuckets, key, RequestType.none()));

		// Second writer retries with a new forUpdate
		var forUpdate3 = db.get(forUpdate2.updateId(), colIdNoBuckets, key, RequestType.forUpdate());
		Assertions.assertNull(forUpdate3.previous());

		// Now the retry should succeed (deleting what's already deleted is a no-op but should succeed)
		db.delete(forUpdate3.updateId(), colIdNoBuckets, key, RequestType.none());

		// Verify final state
		Assertions.assertNull(db.get(0, colIdNoBuckets, key, RequestType.current()));
	}

	// ==================== Merge methods tests ====================

	@Test
	void mergeNone_noBuckets() {
		var key = makeKey(2, 2, 2);
		db.put(0, colIdNoBuckets, key, Buf.wrap("A".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		db.merge(0, colIdNoBuckets, key, Buf.wrap("B".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(Buf.wrap("A,B".getBytes(StandardCharsets.UTF_8)), result);
	}

	@Test
	void mergeMerged_noBuckets() {
		var key = makeKey(2, 2, 2);
		db.put(0, colIdNoBuckets, key, Buf.wrap("A".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		Buf merged = db.merge(0, colIdNoBuckets, key, Buf.wrap("B".getBytes(StandardCharsets.UTF_8)), RequestType.merged());
		assertBufEquals(Buf.wrap("A,B".getBytes(StandardCharsets.UTF_8)), merged);
	}

	@Test
	void mergeConcurrentUpdate_retrySucceeds() {
		var key = makeKey(3, 3, 3);
		db.put(0, colIdNoBuckets, key, Buf.wrap("A".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		var fu1 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		var fu2 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());

		// Writer 1 merges B
		db.merge(fu1.updateId(), colIdNoBuckets, key, Buf.wrap("B".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		// Writer 2 tries to merge C, should fail
		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.merge(fu2.updateId(), colIdNoBuckets, key, Buf.wrap("C".getBytes(StandardCharsets.UTF_8)), RequestType.none()));

		// Writer 2 retries
		var fu3 = db.get(fu2.updateId(), colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(Buf.wrap("A,B".getBytes(StandardCharsets.UTF_8)), fu3.previous());
		db.merge(fu3.updateId(), colIdNoBuckets, key, Buf.wrap("C".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		// Final value A,B,C
		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(Buf.wrap("A,B,C".getBytes(StandardCharsets.UTF_8)), result);
	}

	// ==================== Multi-operation tests ====================

	@Test
	void putMulti_success() {
		var keys = List.of(makeKey(10, 1, 1), makeKey(10, 1, 2));
		var values = List.of(toBufSimple(101), toBufSimple(102));

		db.putMulti(0, colIdNoBuckets, keys, values, RequestType.none());

		assertBufEquals(values.get(0), db.get(0, colIdNoBuckets, keys.get(0), RequestType.current()));
		assertBufEquals(values.get(1), db.get(0, colIdNoBuckets, keys.get(1), RequestType.current()));
	}

	@Test
	void putMultiConcurrentUpdate_retrySucceeds() {
		var key1 = makeKey(11, 1, 1);
		var key2 = makeKey(11, 1, 2);
		var v1_initial = toBufSimple(1);
		var v2_initial = toBufSimple(2);

		db.put(0, colIdNoBuckets, key1, v1_initial, RequestType.none());
		db.put(0, colIdNoBuckets, key2, v2_initial, RequestType.none());

		var fu1 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		var fu2 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());

		// Writer 1 updates key 1
		db.put(fu1.updateId(), colIdNoBuckets, key1, toBufSimple(10), RequestType.none());

		// Writer 2 tries putMulti on key 1 and key 2, should fail due to key 1 conflict
		try {
			db.putMulti(fu2.updateId(), colIdNoBuckets, List.of(key1, key2), List.of(toBufSimple(11), toBufSimple(22)), RequestType.none());
			Assertions.fail("Should have thrown RocksDBRetryException");
		} catch (RocksDBRetryException e) {
			// Expected
		}

		// Writer 2 retries - since the previous updateId was closed on failure, we need a fresh one.
		var fu3 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		db.putMulti(fu3.updateId(), colIdNoBuckets, List.of(key1, key2), List.of(toBufSimple(111), toBufSimple(222)), RequestType.none());

		assertBufEquals(toBufSimple(111), db.get(0, colIdNoBuckets, key1, RequestType.current()));
		assertBufEquals(toBufSimple(222), db.get(0, colIdNoBuckets, key2, RequestType.current()));
	}

	@Test
	void deleteMulti_success() {
		var keys = List.of(makeKey(12, 1, 1), makeKey(12, 1, 2));
		db.put(0, colIdNoBuckets, keys.get(0), toBufSimple(1), RequestType.none());
		db.put(0, colIdNoBuckets, keys.get(1), toBufSimple(2), RequestType.none());

		db.deleteMulti(0, colIdNoBuckets, keys, RequestType.none());

		Assertions.assertNull(db.get(0, colIdNoBuckets, keys.get(0), RequestType.current()));
		Assertions.assertNull(db.get(0, colIdNoBuckets, keys.get(1), RequestType.current()));
	}

	@Test
	void deleteMultiConcurrentUpdate_retrySucceeds() {
		var key1 = makeKey(13, 1, 1);
		var key2 = makeKey(13, 1, 2);
		db.put(0, colIdNoBuckets, key1, toBufSimple(1), RequestType.none());
		db.put(0, colIdNoBuckets, key2, toBufSimple(2), RequestType.none());

		var fu1 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		var fu2 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());

		// Writer 1 updates key 1
		db.put(fu1.updateId(), colIdNoBuckets, key1, toBufSimple(10), RequestType.none());

		// Writer 2 tries deleteMulti, should fail
		try {
			db.deleteMulti(fu2.updateId(), colIdNoBuckets, List.of(key1, key2), RequestType.none());
			Assertions.fail("Should have thrown RocksDBRetryException");
		} catch (RocksDBRetryException e) {
			// Expected
		}

		// Writer 2 retries - since the previous updateId was closed on failure, we need a fresh one.
		var fu3 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		db.deleteMulti(fu3.updateId(), colIdNoBuckets, List.of(key1, key2), RequestType.none());

		Assertions.assertNull(db.get(0, colIdNoBuckets, key1, RequestType.current()));
		Assertions.assertNull(db.get(0, colIdNoBuckets, key2, RequestType.current()));
	}

	@Test
	void mergeMulti_success() {
		var keys = List.of(makeKey(14, 1, 1), makeKey(14, 1, 2));
		db.put(0, colIdNoBuckets, keys.get(0), Buf.wrap("A1".getBytes(StandardCharsets.UTF_8)), RequestType.none());
		db.put(0, colIdNoBuckets, keys.get(1), Buf.wrap("B1".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		db.mergeMulti(0, colIdNoBuckets, keys, List.of(Buf.wrap("A2".getBytes(StandardCharsets.UTF_8)), Buf.wrap("B2".getBytes(StandardCharsets.UTF_8))), RequestType.none());

		assertBufEquals(Buf.wrap("A1,A2".getBytes(StandardCharsets.UTF_8)), db.get(0, colIdNoBuckets, keys.get(0), RequestType.current()));
		assertBufEquals(Buf.wrap("B1,B2".getBytes(StandardCharsets.UTF_8)), db.get(0, colIdNoBuckets, keys.get(1), RequestType.current()));
	}

	@Test
	void mergeMultiConcurrentUpdate_retrySucceeds() {
		var key1 = makeKey(15, 1, 1);
		var key2 = makeKey(15, 1, 2);
		db.put(0, colIdNoBuckets, key1, Buf.wrap("A".getBytes(StandardCharsets.UTF_8)), RequestType.none());
		db.put(0, colIdNoBuckets, key2, Buf.wrap("B".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		var fu1 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		var fu2 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());

		// Writer 1 updates key 1
		db.put(fu1.updateId(), colIdNoBuckets, key1, Buf.wrap("A_new".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		// Writer 2 tries mergeMulti, should fail
		try {
			db.mergeMulti(fu2.updateId(), colIdNoBuckets, List.of(key1, key2), List.of(Buf.wrap("A2".getBytes(StandardCharsets.UTF_8)), Buf.wrap("B2".getBytes(StandardCharsets.UTF_8))), RequestType.none());
			Assertions.fail("Should have thrown RocksDBRetryException");
		} catch (RocksDBRetryException e) {
			// Expected
		}

		// Writer 2 retries - since the previous updateId was closed on failure, we need a fresh one.
		var fu3 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		db.mergeMulti(fu3.updateId(), colIdNoBuckets, List.of(key1, key2), List.of(Buf.wrap("A3".getBytes(StandardCharsets.UTF_8)), Buf.wrap("B3".getBytes(StandardCharsets.UTF_8))), RequestType.none());

		assertBufEquals(Buf.wrap("A_new,A3".getBytes(StandardCharsets.UTF_8)), db.get(0, colIdNoBuckets, key1, RequestType.current()));
		assertBufEquals(Buf.wrap("B,B3".getBytes(StandardCharsets.UTF_8)), db.get(0, colIdNoBuckets, key2, RequestType.current()));
	}

	@Test
	void concurrentDeleteWithPrevious_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(10, 10, 10);
		int numThreads = 4;
		int opsPerThread = 20;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < opsPerThread; i++) {
						var threadKey = makeKey(10, 10, 10 + threadId * opsPerThread + i);
						db.put(0, colIdNoBuckets, threadKey, toBufSimple(1), RequestType.none());
						db.delete(0, colIdNoBuckets, threadKey, RequestType.previous());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(), "Errors during concurrent delete: " + errors);
	}

	@Test
	void concurrentMerge_noBuckets_multiThreaded() throws Exception {
		var key = makeKey(11, 11, 11);
		int numThreads = 4;
		int opsPerThread = 20;
		var executor = Executors.newFixedThreadPool(numThreads);
		var latch = new CountDownLatch(1);
		var errors = new ConcurrentLinkedQueue<Throwable>();

		db.put(0, colIdNoBuckets, key, Buf.wrap("initial".getBytes(StandardCharsets.UTF_8)), RequestType.none());

		var futures = new ArrayList<Future<?>>();
		for (int t = 0; t < numThreads; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				try {
					latch.await();
					for (int i = 0; i < opsPerThread; i++) {
						db.merge(0, colIdNoBuckets, key, Buf.wrap(("v-" + threadId + "-" + i).getBytes(StandardCharsets.UTF_8)), RequestType.none());
					}
				} catch (Throwable e) {
					errors.add(e);
				}
			}));
		}

		latch.countDown();
		for (var f : futures) f.get(30, TimeUnit.SECONDS);
		executor.shutdown();

		Assertions.assertTrue(errors.isEmpty(), "Errors during concurrent merge: " + errors);

		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		String s = new String(Utils.toByteArray(result), StandardCharsets.UTF_8);
		String[] parts = s.split(",");
		Assertions.assertEquals(numThreads * opsPerThread + 1, parts.length, "Total merged elements mismatch");
	}

	// ==================== Sequential correctness after multiple overwrites ====================

	@Test
	void sequentialOverwrites_noBuckets_deltaConsistency() {
		var key = makeKey(5, 6, 7);
		Buf lastValue = null;

		for (int i = 0; i < 100; i++) {
			var newValue = toBufSimple(i, i + 1, i + 2);
			Delta<Buf> delta = db.put(0, colIdNoBuckets, key, newValue, RequestType.delta());
			if (lastValue == null) {
				Assertions.assertNull(delta.previous());
			} else {
				assertBufEquals(lastValue, delta.previous());
			}
			assertBufEquals(newValue, delta.current());
			lastValue = newValue;
		}

		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(lastValue, result);
	}

	@Test
	void sequentialOverwrites_bucketed_deltaConsistency() {
		var key = makeKeyWithVar(5, 6, 7, 10, 20);
		Buf lastValue = null;

		for (int i = 0; i < 100; i++) {
			var newValue = toBufSimple(i, i + 1, i + 2);
			Delta<Buf> delta = db.put(0, colId, key, newValue, RequestType.delta());
			if (lastValue == null) {
				Assertions.assertNull(delta.previous());
			} else {
				assertBufEquals(lastValue, delta.previous());
			}
			assertBufEquals(newValue, delta.current());
			lastValue = newValue;
		}

		Buf result = db.get(0, colId, key, RequestType.current());
		assertBufEquals(lastValue, result);
	}

	// ==================== Put with none callback (no tx needed) ====================

	@Test
	void putNone_noBuckets_noTransactionNeeded() {
		// put with RequestType.none() and no buckets should not need a transaction at all
		var key = makeKey(9, 8, 7);
		var value = toBufSimple(1, 2, 3);
		Assertions.assertNull(db.put(0, colIdNoBuckets, key, value, RequestType.none()));

		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(value, result);
	}

	@Test
	void putNone_bucketed_usesTransaction() {
		// put with RequestType.none() on bucketed column still needs a transaction
		var key = makeKeyWithVar(9, 8, 7, 1, 2);
		var value = toBufSimple(1, 2, 3);
		Assertions.assertNull(db.put(0, colId, key, value, RequestType.none()));

		Buf result = db.get(0, colId, key, RequestType.current());
		assertBufEquals(value, result);
	}

	// ==================== Multiple concurrent forUpdate conflicts ====================

	@Test
	void multipleConflicts_allResolve() {
		var key = makeKey(7, 7, 7);
		var values = new Buf[]{
				toBufSimple(1), toBufSimple(2), toBufSimple(3)
		};

		// Three concurrent forUpdate holders
		var fu1 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		var fu2 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		var fu3 = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());

		// First commits
		db.put(fu1.updateId(), colIdNoBuckets, key, values[0], RequestType.none());

		// Second fails
		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.put(fu2.updateId(), colIdNoBuckets, key, values[1], RequestType.none()));

		// Third also fails
		Assertions.assertThrowsExactly(RocksDBRetryException.class,
				() -> db.put(fu3.updateId(), colIdNoBuckets, key, values[2], RequestType.none()));

		// Second retries and succeeds
		var fu2retry = db.get(fu2.updateId(), colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(values[0], fu2retry.previous());
		db.put(fu2retry.updateId(), colIdNoBuckets, key, values[1], RequestType.none());

		// Third retries and succeeds
		var fu3retry = db.get(fu3.updateId(), colIdNoBuckets, key, RequestType.forUpdate());
		assertBufEquals(values[1], fu3retry.previous());
		db.put(fu3retry.updateId(), colIdNoBuckets, key, values[2], RequestType.none());

		// Final value should be the last committed
		Buf result = db.get(0, colIdNoBuckets, key, RequestType.current());
		assertBufEquals(values[2], result);
	}

	// ==================== Helpers ====================

	private static void assertBufEquals(Buf expected, Buf actual) {
		if (!Utils.valueEquals(expected, actual)) {
			Assertions.fail("Buf mismatch! Expected: "
					+ (expected != null ? Arrays.toString(Utils.toByteArray(expected)) : "null")
					+ ", Actual: " + (actual != null ? Arrays.toString(Utils.toByteArray(actual)) : "null"));
		}
	}
}
