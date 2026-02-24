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
import java.util.List;
import org.junit.jupiter.api.*;

@TestMethodOrder(MethodOrderer.MethodName.class)
class MultiOpTransactionTest {

	private EmbeddedConnection db;
	private long colIdNoBuckets;
	private Path configFile;

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
		configFile = Files.createTempFile("multi-tx-test", ".conf");
		Files.writeString(configFile, """
database: {
  global: {
    ingest-behind: true
  }
}
""");
		db = new EmbeddedConnection(null, "multi-tx-test-db", configFile);
		colIdNoBuckets = db.createColumn("test-column-no-buckets", SCHEMA_NO_BUCKETS);
	}

	@AfterEach
	void tearDown() throws IOException {
		db.close();
		Files.deleteIfExists(configFile);
	}

	@Test
	void testPutMultiWithUpdateId_Conflict() {
		var key1 = makeKey(1, 1, 1);
		var key2 = makeKey(1, 1, 2);
		var value1 = toBufSimple(10);
		var value2 = toBufSimple(20);

		// First, populate key2 to have a conflict later
		db.put(0, colIdNoBuckets, key2, toBufSimple(0), RequestType.none());

		// Holder 1 gets forUpdate on key1 and key2
		var fu1_k1 = db.get(0, colIdNoBuckets, key1, RequestType.forUpdate());
		var fu1_k2 = db.get(fu1_k1.updateId(), colIdNoBuckets, key2, RequestType.forUpdate());
		long updateId1 = fu1_k1.updateId();

		// Holder 2 gets forUpdate on key2
		var fu2_k2 = db.get(0, colIdNoBuckets, key2, RequestType.forUpdate());
		long updateId2 = fu2_k2.updateId();

		// Holder 2 commits key2
		db.put(updateId2, colIdNoBuckets, key2, toBufSimple(100), RequestType.none());

		// Holder 1 tries putMulti on key1 and key2.
		// It should fail on key2 because holder 2 committed.
		try {
			db.putMulti(updateId1, colIdNoBuckets, List.of(key1, key2), List.of(value1, value2), RequestType.none());
			Assertions.fail("Should have thrown RocksDBRetryException");
		} catch (RocksDBRetryException e) {
			// Expected
		}

		// Check if key1 was persisted.
		Buf val1 = db.get(0, colIdNoBuckets, key1, RequestType.current());
		if (val1 != null) {
			System.out.println("Value 1 after failed putMulti: " + Arrays.toString(Utils.toByteArray(val1)));
		} else {
			System.out.println("Value 1 after failed putMulti: null");
		}
	}

	private static void assertBufEquals(Buf expected, Buf actual) {
		if (!Utils.valueEquals(expected, actual)) {
			Assertions.fail("Buf mismatch! Expected: "
					+ (expected != null ? Arrays.toString(Utils.toByteArray(expected)) : "null")
					+ ", Actual: " + (actual != null ? Arrays.toString(Utils.toByteArray(actual)) : "null"));
		}
	}
}
