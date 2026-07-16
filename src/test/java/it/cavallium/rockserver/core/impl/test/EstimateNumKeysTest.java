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
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(120)
class EstimateNumKeysTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void estimatesUnboundedPhysicalKeysWithoutChangingExactCount(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve("estimate-" + safeName(name));
		try (var embedded = new EmbeddedConnection(databasePath, "estimate-num-keys", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(), true));

			assertEquals(0L, api.estimateNumKeys(columnId), name + ", empty estimate");
			for (int i = 0; i < 100; i++) {
				api.put(0,
						columnId,
						new Keys(toBufSimple(i)),
						toBufSimple(i),
						RequestType.none());
			}

			assertExactCount(api, columnId, 100, name + ", memtable exact count");
			assertPositiveEstimate(api, columnId, name + ", memtable estimate");

			api.flush();
			assertExactCount(api, columnId, 100, name + ", flushed exact count");
			long syncEstimate = api.estimateNumKeys(columnId);
			long asyncEstimate = api.estimateNumKeysAsync(columnId).join();
			assertTrue(syncEstimate > 0, name + ", flushed sync estimate");
			assertEquals(syncEstimate, asyncEstimate, name + ", sync/async transport parity");
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void bucketedEstimateCountsPhysicalBucketsWhileExactCountCountsLogicalEntries(String name,
			ConnectionConfig connection) throws Exception {
		Path databasePath = tempDir.resolve("bucketed-estimate-" + safeName(name));
		try (var embedded = new EmbeddedConnection(databasePath, "bucketed-estimate-num-keys", null);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			long columnId = api.createColumn("bucketed-entries",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
			for (int i = 0; i < 5; i++) {
				api.put(0,
						columnId,
						new Keys(toBufSimple(7), toBufSimple(i)),
						toBufSimple(i),
						RequestType.none());
			}
			api.flush();

			assertExactCount(api, columnId, 5, name + ", bucketed exact count");
			assertEquals(1L, api.estimateNumKeys(columnId), name + ", one physical bucket");
		}
	}

	private static void assertPositiveEstimate(RocksDBAPI api, long columnId, String context) {
		assertTrue(api.estimateNumKeys(columnId) > 0, context);
	}

	private static void assertExactCount(RocksDBAPI api, long columnId, long expected, String context) {
		assertEquals(expected, api.reduceRange(0,
				columnId,
				null,
				null,
				false,
				RequestType.entriesCount(),
				10_000), context);
	}

	private static String safeName(String name) {
		return name.replaceAll("[^a-zA-Z0-9]", "-");
	}
}
