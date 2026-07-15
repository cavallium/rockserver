package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class RangeReductionConsistencyTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void entriesCountMatchesTheVisibleRangeAcrossStorageAndTransactionStates(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve(name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "range-count", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			var key1 = new Keys(toBufSimple(1));
			var key2 = new Keys(toBufSimple(2));
			var key3 = new Keys(toBufSimple(3));
			var key4 = new Keys(toBufSimple(4));

			assertCountMatchesRange(api, 0, columnId, null, null, 0, name + ", empty database");
			assertCountMatchesRange(api, 0, columnId, key2, key2, 0, name + ", empty bounded range");

			api.put(0, columnId, key1, toBufSimple(11), RequestType.none());
			assertCountMatchesRange(api, 0, columnId, null, null, 1, name + ", pure memtable");
			api.put(0, columnId, key2, toBufSimple(22), RequestType.none());
			api.flush();
			assertCountMatchesRange(api, 0, columnId, null, null, 2, name + ", flushed baseline");

			long transactionId = api.openTransaction(30_000);
			assertTrue(transactionId < 0, name + ", transaction IDs must use the reserved negative range");
			try {
				api.put(transactionId, columnId, key3, toBufSimple(33), RequestType.none());
				assertCountMatchesRange(api, transactionId, columnId, null, null, 3,
						name + ", transaction-local insert");
				assertCountMatchesRange(api, transactionId, columnId, key2, key4, 2,
						name + ", bounded transaction-local insert");

				api.delete(transactionId, columnId, key1, RequestType.none());
				assertCountMatchesRange(api, transactionId, columnId, null, null, 2,
						name + ", transaction-local delete");
				assertCountMatchesRange(api, transactionId, columnId, key1, key3, 1,
						name + ", bounded transaction-local delete");
				assertCountMatchesRange(api, 0, columnId, null, null, 2,
						name + ", committed view while transaction is open");
			} finally {
				assertTrue(api.closeTransaction(transactionId, false), name + ", transaction rollback");
			}

			api.put(0, columnId, key3, toBufSimple(33), RequestType.none());
			assertCountMatchesRange(api, 0, columnId, null, null, 3, name + ", unflushed insert");

			api.put(0, columnId, key1, toBufSimple(111), RequestType.none());
			api.delete(0, columnId, key2, RequestType.none());
			assertCountMatchesRange(api, 0, columnId, null, null, 2,
					name + ", unflushed overwrite and delete");

			api.flush();
			assertCountMatchesRange(api, 0, columnId, null, null, 2,
					name + ", multiple SST generations");
			assertCountMatchesRange(api, 0, columnId, key1, key4, 2, name + ", bounded range");

			api.compact();
			assertCountMatchesRange(api, 0, columnId, null, null, 2, name + ", compacted state");
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void bucketedEntriesCountIncludesCollidingTransactionMutations(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve("bucketed-" + name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "bucketed-range-count", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("bucketed-entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
			var key1 = new Keys(toBufSimple(7), toBufSimple(1));
			var key2 = new Keys(toBufSimple(7), toBufSimple(2));
			var key3 = new Keys(toBufSimple(7), toBufSimple(3));

			api.put(0, columnId, key1, toBufSimple(11), RequestType.none());
			api.put(0, columnId, key2, toBufSimple(22), RequestType.none());
			api.flush();
			assertCountMatchesRange(api, 0, columnId, null, null, 2,
					name + ", two colliding bucket entries");

			long transactionId = api.openTransaction(30_000);
			assertTrue(transactionId < 0, name + ", transaction IDs must use the reserved negative range");
			try {
				api.put(transactionId, columnId, key3, toBufSimple(33), RequestType.none());
				assertCountMatchesRange(api, transactionId, columnId, null, null, 3,
						name + ", colliding transaction-local insert");

				api.delete(transactionId, columnId, key1, RequestType.none());
				assertCountMatchesRange(api, transactionId, columnId, null, null, 2,
						name + ", colliding transaction-local delete");
				assertCountMatchesRange(api, 0, columnId, null, null, 2,
						name + ", committed bucket while transaction is open");

				api.delete(transactionId, columnId, key2, RequestType.none());
				api.delete(transactionId, columnId, key3, RequestType.none());
				assertCountMatchesRange(api, transactionId, columnId, null, null, 0,
						name + ", transaction-local empty bucket");
			} finally {
				assertTrue(api.closeTransaction(transactionId, false), name + ", bucket transaction rollback");
			}

			assertCountMatchesRange(api, 0, columnId, null, null, 2,
					name + ", bucket rollback restores committed view");
		}
	}

	private static void assertCountMatchesRange(RocksDBAPI api,
			long transactionId,
			long columnId,
			@Nullable Keys startInclusive,
			@Nullable Keys endExclusive,
			long expected,
			String context) {
		long scanned;
		try (var range = api.getRange(transactionId,
				columnId,
				startInclusive,
				endExclusive,
				false,
				RequestType.allInRange(),
				10_000)) {
			scanned = range.count();
		}
		assertEquals(expected, scanned, context + ", scanned entries");
		assertEquals(expected, api.reduceRange(transactionId,
				columnId,
				startInclusive,
				endExclusive,
				false,
				RequestType.entriesCount(),
				10_000), context + ", forward reduction");
		assertEquals(expected, api.reduceRange(transactionId,
				columnId,
				startInclusive,
				endExclusive,
				true,
				RequestType.entriesCount(),
				10_000), context + ", reverse reduction");
	}
}
