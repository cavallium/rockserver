package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class IteratorContractTest {

	private static final int[] ENTRIES = {10, 20, 30, 40, 50, 60};
	private static final long ITERATOR_TIMEOUT_MS = 10_000;

	@TempDir
	Path tempDir;

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void boundedIteratorContractIsConsistentAcrossEveryAdapter(String name, ConnectionConfig connection)
			throws Exception {
		Path databasePath = tempDir.resolve(name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "iterator-contract", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			EmbeddedDB internal = embedded.getInternalDB();
			long columnId = api.createColumn(
					"entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			for (int entry : ENTRIES) {
				api.put(0, columnId, key(entry), value(entry), RequestType.none());
			}

			assertAll(name,
					() -> assertForwardMultiPaging(api, columnId),
					() -> assertNoneSkipsAndTakesWithoutReturningValues(api, columnId),
					() -> assertExistsReportsExhaustion(api, columnId),
					() -> assertExactAndMissingSeek(api, columnId),
					() -> assertReverseBoundsAndOrder(api, columnId),
					() -> assertZeroTakeDoesNotAdvance(api, columnId),
					() -> assertInvalidCountsDoNotAdvance(api, columnId),
					() -> assertMaximumTimeoutDoesNotOverflow(api, columnId),
					() -> assertRepeatedCloseIsIdempotent(api, internal, columnId),
					() -> assertIteratorResourcesReleased(internal));
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void transactionIteratorUsesTheNegativeTransactionIdAndSeesItsOwnWrites(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve("transaction-" + name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "transaction-iterator", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("entries", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
			api.put(0, columnId, key(10), value(10), RequestType.none());
			api.put(0, columnId, key(20), value(20), RequestType.none());

			long transactionId = api.openTransaction(30_000);
			assertTrue(transactionId < 0, name + ", transaction IDs must be negative");
			try {
				api.put(transactionId, columnId, key(15), value(15), RequestType.none());
				api.delete(transactionId, columnId, key(20), RequestType.none());

				assertIteratorValues(api, transactionId, columnId, values(10, 15));
				assertIteratorValues(api, 0, columnId, values(10, 20));
			} finally {
				assertTrue(api.closeTransaction(transactionId, false), name + ", rollback must succeed");
			}
			assertIteratorResourcesReleased(embedded.getInternalDB());
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void bucketCollisionsPageAndSeekByLogicalEntryAcrossEveryAdapter(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve("bucket-" + name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "bucket-iterator", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
			var first = bucketKey(7, 1);
			var second = bucketKey(7, 2);
			var third = bucketKey(7, 3);
			api.put(0, columnId, first, value(10), RequestType.none());
			api.put(0, columnId, second, value(20), RequestType.none());
			api.put(0, columnId, third, value(30), RequestType.none());

			long forward = api.openIterator(0, columnId, new Keys(), null, false, ITERATOR_TIMEOUT_MS);
			try {
				assertIterableEquals(values(10, 20, 30), api.subsequent(forward, 0, 10, RequestType.multi()));
				api.seekTo(forward, second);
				assertIterableEquals(values(20, 30), api.subsequent(forward, 0, 10, RequestType.multi()));
			} finally {
				api.closeIterator(forward);
			}

			long reverse = api.openIterator(0, columnId, new Keys(), null, true, ITERATOR_TIMEOUT_MS);
			try {
				assertIterableEquals(values(30, 20, 10), api.subsequent(reverse, 0, 10, RequestType.multi()));
				api.seekTo(reverse, second);
				assertIterableEquals(values(20, 10), api.subsequent(reverse, 0, 10, RequestType.multi()));
			} finally {
				api.closeIterator(reverse);
			}
			assertIteratorResourcesReleased(embedded.getInternalDB());
		}
	}

	private static void assertForwardMultiPaging(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			assertIterableEquals(values(20, 30), api.subsequent(iteratorId, 0, 2, RequestType.multi()));
			assertIterableEquals(values(40), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
			assertIterableEquals(values(50), api.subsequent(iteratorId, 0, 8, RequestType.multi()));
			assertIterableEquals(List.of(), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertNoneSkipsAndTakesWithoutReturningValues(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			assertNull(api.subsequent(iteratorId, 1, 2, RequestType.none()));
			assertIterableEquals(values(50), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
			assertNull(api.subsequent(iteratorId, 0, 1, RequestType.none()));
			assertFalse(api.subsequent(iteratorId, 0, 1, RequestType.exists()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertExistsReportsExhaustion(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			assertTrue(api.subsequent(iteratorId, 0, 1, RequestType.exists()));
			assertTrue(api.subsequent(iteratorId, 2, 1, RequestType.exists()));
			assertFalse(api.subsequent(iteratorId, 0, 1, RequestType.exists()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertExactAndMissingSeek(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			api.seekTo(iteratorId, key(40));
			assertIterableEquals(values(40), api.subsequent(iteratorId, 0, 1, RequestType.multi()));

			api.seekTo(iteratorId, key(35));
			assertIterableEquals(values(40), api.subsequent(iteratorId, 0, 1, RequestType.multi()));

			api.seekTo(iteratorId, key(60));
			assertFalse(api.subsequent(iteratorId, 0, 1, RequestType.exists()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertReverseBoundsAndOrder(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, true);
		try {
			assertIterableEquals(values(50, 40, 30, 20),
					api.subsequent(iteratorId, 0, 10, RequestType.multi()));
			assertIterableEquals(List.of(), api.subsequent(iteratorId, 0, 1, RequestType.multi()));

			api.seekTo(iteratorId, key(35));
			assertIterableEquals(values(30), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
			api.seekTo(iteratorId, key(40));
			assertIterableEquals(values(40), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
			api.seekTo(iteratorId, key(60));
			assertIterableEquals(values(50), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertZeroTakeDoesNotAdvance(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			assertIterableEquals(List.of(), api.subsequent(iteratorId, 0, 0, RequestType.multi()));
			assertNull(api.subsequent(iteratorId, 0, 0, RequestType.none()));
			assertIterableEquals(values(20), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertInvalidCountsDoNotAdvance(RocksDBAPI api, long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		try {
			assertThrows(RocksDBException.class,
					() -> api.subsequent(iteratorId, -1, 1, RequestType.multi()));
			assertThrows(RocksDBException.class,
					() -> api.subsequent(iteratorId, 0, -1, RequestType.multi()));
			assertIterableEquals(values(20), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertMaximumTimeoutDoesNotOverflow(RocksDBAPI api, long columnId) {
		long iteratorId = api.openIterator(0, columnId, key(20), key(60), false, Long.MAX_VALUE);
		try {
			assertIterableEquals(values(20), api.subsequent(iteratorId, 0, 1, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertIteratorValues(RocksDBAPI api,
			long transactionId,
			long columnId,
			List<Buf> expected) {
		long iteratorId = api.openIterator(transactionId, columnId, new Keys(), null, false, ITERATOR_TIMEOUT_MS);
		try {
			assertIterableEquals(expected, api.subsequent(iteratorId, 0, 10, RequestType.multi()));
		} finally {
			api.closeIterator(iteratorId);
		}
	}

	private static void assertRepeatedCloseIsIdempotent(RocksDBAPI api,
			EmbeddedDB internal,
			long columnId) {
		long iteratorId = openBounded(api, columnId, false);
		assertEquals(1, internal.getOpenIteratorsCount(), "one iterator must be registered");
		assertEquals(1, internal.getPendingOpsCount(), "an open iterator must hold one lifetime operation");

		api.closeIterator(iteratorId);
		api.closeIterator(iteratorId);

		assertIteratorResourcesReleased(internal);
	}

	private static long openBounded(RocksDBAPI api, long columnId, boolean reverse) {
		return api.openIterator(0, columnId, key(20), key(60), reverse, ITERATOR_TIMEOUT_MS);
	}

	private static void assertIteratorResourcesReleased(EmbeddedDB internal) {
		assertEquals(0, internal.getOpenIteratorsCount(), "all iterator registrations must be released");
		assertEquals(0, internal.getPendingOpsCount(), "all iterator lifetime operations must be balanced");
	}

	private static Keys key(int value) {
		return new Keys(toBufSimple(value));
	}

	private static Keys bucketKey(int fixed, int variable) {
		return new Keys(toBufSimple(fixed), toBufSimple(variable));
	}

	private static Buf value(int value) {
		return toBufSimple(value);
	}

	private static List<Buf> values(int... entries) {
		return java.util.Arrays.stream(entries).mapToObj(IteratorContractTest::value).toList();
	}
}
