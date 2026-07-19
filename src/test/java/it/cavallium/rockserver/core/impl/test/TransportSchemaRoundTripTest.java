package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertIterableEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.impl.MyStringAppendOperator;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionMethod;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Timeout(60)
class TransportSchemaRoundTripTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> transportImplementations() {
		return Stream.of(ConnectionMethod.GRPC, ConnectionMethod.THRIFT)
				.flatMap(method -> Stream.of(ConnectionType.SYNC, ConnectionType.ASYNC)
						.map(type -> new ConnectionConfig(type, method)))
				.map(config -> Arguments.arguments(config.getName(), config));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("transportImplementations")
	void schemasSurviveCreateListDeleteAndRecreate(String name, ConnectionConfig connection) throws Exception {
		Path configFile = tempDir.resolve("schema-round-trip-" + safeName(name) + ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		Path databasePath = tempDir.resolve("db-" + safeName(name));

		try (var embedded = new EmbeddedConnection(databasePath, "schema-round-trip", configFile);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			Map<String, ColumnSchema> expected = expectedSchemas();

			Map<String, Long> firstIds = createAll(api, expected);
			assertSnapshot(api, expected, firstIds, name + ", first creation");

			for (String columnName : expected.keySet()) {
				assertTrue(api.deleteColumnIfExists(columnName), name + ", first deletion of " + columnName);
				assertFalse(api.deleteColumnIfExists(columnName), name + ", repeated deletion of " + columnName);
			}
			assertTrue(api.getAllColumnDefinitions().isEmpty(), name + ", definitions after deletion");
			for (String columnName : expected.keySet()) {
				assertThrows(RocksDBException.class, () -> api.getColumnId(columnName),
						name + ", deleted column still resolves: " + columnName);
			}

			Map<String, Long> recreatedIds = createAll(api, expected);
			assertSnapshot(api, expected, recreatedIds, name + ", recreation");
		}
	}

	private static Map<String, ColumnSchema> expectedSchemas() {
		var schemas = new LinkedHashMap<String, ColumnSchema>();
		schemas.put("hashed-values", ColumnSchema.of(
				IntList.of(3, 7),
				ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH8,
						ColumnHashType.ALLSAME8, ColumnHashType.FIXEDINTEGER32),
				true,
				null,
				null,
				MyStringAppendOperator.class.getName()));
		schemas.put("hashed-set", ColumnSchema.of(
				IntList.of(2, 5, 9),
				ObjectList.of(ColumnHashType.FIXEDINTEGER32, ColumnHashType.ALLSAME8,
						ColumnHashType.XXHASH8, ColumnHashType.XXHASH32),
				false));
		return schemas;
	}

	private static Map<String, Long> createAll(RocksDBAPI api, Map<String, ColumnSchema> schemas) {
		var ids = new LinkedHashMap<String, Long>();
		schemas.forEach((name, schema) -> ids.put(name, api.createColumn(name, schema)));
		return ids;
	}

	private static void assertSnapshot(RocksDBAPI api,
			Map<String, ColumnSchema> expected,
			Map<String, Long> expectedIds,
			String context) {
		Map<String, ColumnSchema> actual = api.getAllColumnDefinitions();
		assertEquals(expected.keySet(), actual.keySet(), context + ", column names");
		for (var entry : expected.entrySet()) {
			String columnName = entry.getKey();
			assertEquals(expectedIds.get(columnName).longValue(), api.getColumnId(columnName),
					context + ", column id for " + columnName);
			assertSchemaEquals(entry.getValue(), actual.get(columnName), context + ", schema for " + columnName);
		}
	}

	private static void assertSchemaEquals(ColumnSchema expected, ColumnSchema actual, String context) {
		assertIterableEquals(expected.keys(), actual.keys(), context + ", logical key widths");
		assertIterableEquals(expected.variableTailKeys(), actual.variableTailKeys(), context + ", variable hash types");
		assertEquals(expected.fixedLengthKeysCount(), actual.fixedLengthKeysCount(), context + ", fixed-key count");
		assertEquals(expected.hasValue(), actual.hasValue(), context + ", hasValue");
		assertEquals(expected.mergeOperatorName(), actual.mergeOperatorName(), context + ", merge operator name");
		assertEquals(expected.mergeOperatorVersion(), actual.mergeOperatorVersion(), context + ", merge operator version");
		assertEquals(expected.mergeOperatorClass(), actual.mergeOperatorClass(), context + ", merge operator class");
	}

	private static String safeName(String name) {
		return name.replaceAll("[^a-zA-Z0-9]", "-");
	}
}
