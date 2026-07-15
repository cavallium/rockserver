package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class BucketedFirstAndLastReductionTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void firstAndLastSkipsEmptyPhysicalBuckets(String name, ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve(name.replaceAll("[^a-zA-Z0-9]", "-"));
		try (var embedded = new EmbeddedConnection(databasePath, "bucketed-first-last", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("bucketed-first-last",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));

			KV first = entry(1, "first", 11);
			KV last = entry(2, "last", 22);
			KV deletedTail = entry(3, "deleted-tail", 33);
			Keys afterDeletedTail = keys(4, "range-end");

			put(api, columnId, first);
			put(api, columnId, last);
			put(api, columnId, deletedTail);
			api.flush();

			api.delete(0, columnId, deletedTail.keys(), RequestType.none());
			api.flush();

			assertEndpoints(first, last,
					api.reduceRange(0, columnId, null, null, false, RequestType.firstAndLast(), 10_000),
					name + ", forward full range");
			assertEndpoints(last, first,
					api.reduceRange(0, columnId, null, null, true, RequestType.firstAndLast(), 10_000),
					name + ", reverse full range");

			assertEmpty(api.reduceRange(0,
					columnId,
					deletedTail.keys(),
					afterDeletedTail,
					false,
					RequestType.firstAndLast(),
					10_000), name + ", bounded empty physical bucket");

			api.delete(0, columnId, first.keys(), RequestType.none());
			api.delete(0, columnId, last.keys(), RequestType.none());
			api.flush();

			assertEmpty(api.reduceRange(0,
					columnId,
					null,
					null,
					false,
					RequestType.firstAndLast(),
					10_000), name + ", logically empty full range");
		}
	}

	private static void put(RocksDBAPI api, long columnId, KV entry) {
		api.put(0, columnId, entry.keys(), entry.value(), RequestType.none());
	}

	private static KV entry(int fixedKey, String variableKey, int value) {
		return new KV(keys(fixedKey, variableKey), toBufSimple(value));
	}

	private static Keys keys(int fixedKey, String variableKey) {
		return new Keys(toBufSimple(fixedKey), Buf.wrap(variableKey.getBytes(StandardCharsets.UTF_8)));
	}

	private static void assertEndpoints(KV expectedFirst,
			KV expectedLast,
			FirstAndLast<KV> actual,
			String context) {
		assertEquals(expectedFirst, actual.first(), context + ", first endpoint");
		assertEquals(expectedLast, actual.last(), context + ", last endpoint");
	}

	private static void assertEmpty(FirstAndLast<KV> actual, String context) {
		assertNull(actual.first(), context + ", first endpoint");
		assertNull(actual.last(), context + ", last endpoint");
	}
}
