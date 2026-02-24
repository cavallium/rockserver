package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
class TransactionTest {

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

	@BeforeEach
	void setUp() throws IOException {
		System.setProperty("rockserver.core.print-config", "false");
		configFile = Files.createTempFile("tx-test", ".conf");
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
		db = new EmbeddedConnection(null, "tx-test-db", configFile);
		colId = db.createColumn("test-column", SCHEMA_WITH_BUCKETS);
		colIdNoBuckets = db.createColumn("test-column-no-buckets", SCHEMA_NO_BUCKETS);
	}

	@AfterEach
	void tearDown() throws IOException {
		db.close();
		Files.deleteIfExists(configFile);
	}

	private static Buf toBufString(String s) {
		return Buf.wrap(s.getBytes());
	}

	@Test
	@Order(1)
	void testMultiplePutsInOneTransaction() {
		long txId = db.openTransaction(10_000);
		
		var key1 = makeKey(1, 1, 1);
		var value1 = toBufSimple(100);
		var key2 = makeKey(2, 2, 2);
		var value2 = toBufSimple(200);

		// These should NOT commit yet
		db.put(txId, colIdNoBuckets, key1, value1, RequestType.none());
		db.put(txId, colIdNoBuckets, key2, value2, RequestType.none());

		// Verify they are NOT visible to other transactions (0L = no tx)
		Assertions.assertNull(db.get(0, colIdNoBuckets, key1, RequestType.current()));
		Assertions.assertNull(db.get(0, colIdNoBuckets, key2, RequestType.current()));

		// Commit
		boolean committed = db.closeTransaction(txId, true);
		Assertions.assertTrue(committed);

		// Verify they ARE visible now
		assertBufEquals(value1, db.get(0, colIdNoBuckets, key1, RequestType.current()));
		assertBufEquals(value2, db.get(0, colIdNoBuckets, key2, RequestType.current()));
	}

	@Test
	@Order(2)
	void testPutAndMergeInOneTransaction() {
		var key = makeKey(3, 3, 3);
		db.put(0, colIdNoBuckets, key, toBufString("A"), RequestType.none());

		long txId = db.openTransaction(10_000);
		
		db.put(txId, colIdNoBuckets, key, toBufString("B"), RequestType.none());
		db.merge(txId, colIdNoBuckets, key, toBufString("C"), RequestType.none());

		// MyStringAppendOperator uses ',' as separator.
		// B merge C -> B,C
		assertBufEquals(toBufString("B,C"), db.get(txId, colIdNoBuckets, key, RequestType.current()));
		// Verify NOT visible outside
		assertBufEquals(toBufString("A"), db.get(0, colIdNoBuckets, key, RequestType.current()));

		db.closeTransaction(txId, true);

		assertBufEquals(toBufString("B,C"), db.get(0, colIdNoBuckets, key, RequestType.current()));
	}

	@Test
	@Order(3)
	void testMergeWithGetForUpdateUpdateId() {
		var key = makeKey(4, 4, 4);
		db.put(0, colIdNoBuckets, key, toBufString("Initial"), RequestType.none());

		// getForUpdate returns an updateId
		var updateContext = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		long updateId = updateContext.updateId();
		assertBufEquals(toBufString("Initial"), updateContext.previous());

		// Merge using updateId
		// Now it SHOULD auto-commit because we fixed the merge implementation
		db.merge(updateId, colIdNoBuckets, key, toBufString("Merged"), RequestType.none());

		// Outside view should be "Initial,Merged"
		assertBufEquals(toBufString("Initial,Merged"), db.get(0, colIdNoBuckets, key, RequestType.current()));

		// Trying to commit again should FAIL with TX_NOT_FOUND
		Assertions.assertThrowsExactly(RocksDBException.class, () -> db.closeTransaction(updateId, true));
	}

	@Test
	@Order(4)
	void testPutWithGetForUpdateUpdateId_AutoCommits() {
		var key = makeKey(5, 5, 5);
		db.put(0, colIdNoBuckets, key, toBufString("Initial"), RequestType.none());

		var updateContext = db.get(0, colIdNoBuckets, key, RequestType.forUpdate());
		long updateId = updateContext.updateId();

		// Put using updateId
		// Based on analysis, this SHOULD commit automatically because it's a getForUpdate tx
		db.put(updateId, colIdNoBuckets, key, toBufString("NewValue"), RequestType.none());

		// Outside view should be "NewValue" because it auto-committed
		assertBufEquals(toBufString("NewValue"), db.get(0, colIdNoBuckets, key, RequestType.current()));

		// Trying to commit again should FAIL with TX_NOT_FOUND
		Assertions.assertThrowsExactly(RocksDBException.class, () -> db.closeTransaction(updateId, true));
	}

	private static void assertBufEquals(Buf expected, Buf actual) {
		if (!Utils.valueEquals(expected, actual)) {
			Assertions.fail("Buf mismatch! Expected: "
					+ (expected != null ? Arrays.toString(Utils.toByteArray(expected)) : "null")
					+ ", Actual: " + (actual != null ? Arrays.toString(Utils.toByteArray(actual)) : "null"));
		}
	}
}
