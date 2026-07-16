package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ExistsMultiTest {

	static Stream<Arguments> allImplementations() {
		return DBTest.allImplementations();
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("allImplementations")
	void preservesPresenceOrderTransactionsAndLogicalBuckets(String name, ConnectionConfig connection) throws Exception {
		Path configFile = Files.createTempFile("exists-multi", ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		try (var embedded = new EmbeddedConnection(null, "exists-multi-" + name, configFile);
				var testDB = new TestDB(embedded, connection)) {
			var api = testDB.getAPI();
			long fixedColumn = api.createColumn("fixed",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var key1 = fixedKey(1);
			var key2 = fixedKey(2);
			var key3 = fixedKey(3);
			var missing = fixedKey(4);
			var largeValue = Buf.wrap(new byte[128 * 1024]);
			Arrays.fill(largeValue.asArray(), (byte) 7);
			api.put(0, fixedColumn, key1, largeValue, RequestType.none());
			api.put(0, fixedColumn, key3, value(3), RequestType.none());

			var query = List.of(key3, missing, key1, key3, missing);
			var expected = List.of(true, false, true, true, false);
			assertEquals(expected, api.existsMulti(0, fixedColumn, query, 5_000));
			assertEquals(expected, api.existsMultiAsync(0, fixedColumn, query, 5_000).join());
			assertEquals(List.of(), api.existsMulti(0, fixedColumn, List.of(), 5_000));

			long transactionId = api.openTransaction(10_000);
			try {
				api.delete(transactionId, fixedColumn, key1, RequestType.none());
				api.put(transactionId, fixedColumn, key2, value(2), RequestType.none());
				assertEquals(List.of(false, true, true, false),
						api.existsMulti(transactionId, fixedColumn, List.of(key1, key2, key3, missing), 5_000));
				assertEquals(List.of(true, false, true, false),
						api.existsMulti(0, fixedColumn, List.of(key1, key2, key3, missing), 5_000));
			} finally {
				api.closeTransaction(transactionId, false);
			}

			long bucketedColumn = api.createColumn("bucketed",
					ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
			var bucketKey1 = bucketedKey(1);
			var bucketKey2 = bucketedKey(2);
			var missingBucketKey = bucketedKey(3);
			api.put(0, bucketedColumn, bucketKey1, value(11), RequestType.none());
			api.put(0, bucketedColumn, bucketKey2, value(12), RequestType.none());
			assertEquals(List.of(true, true, false, true),
					api.existsMulti(0,
							bucketedColumn,
							List.of(bucketKey1, bucketKey2, missingBucketKey, bucketKey1),
							5_000));

			var invalidTimeout = assertThrows(RocksDBException.class,
					() -> api.existsMulti(0, fixedColumn, List.of(key1), -1));
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					invalidTimeout.getErrorUniqueId());
		} finally {
			Files.deleteIfExists(configFile);
		}
	}

	private static Keys fixedKey(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Keys bucketedKey(int variableValue) {
		return new Keys(Buf.wrap(new byte[] {42}), Buf.wrap(new byte[] {(byte) variableValue}));
	}

	private static Buf value(int value) {
		return Utils.toBufSimple(value);
	}
}
