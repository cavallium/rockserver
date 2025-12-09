package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBuf;
import static it.cavallium.rockserver.core.common.Utils.toBufSimple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class MergeCommandIntegrationTest {

	private Path configFile;
	private EmbeddedConnection embedded;

	private static Buf s(String value) {
		return toBuf(value.getBytes(StandardCharsets.UTF_8));
	}

	@BeforeEach
	void setUp() throws IOException {
		configFile = Files.createTempFile("merge-config", ".conf");
		Files.writeString(configFile, """
database: {
  global: {
    fallback-column-options: {
      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
    }
  }
}
""");
		embedded = new EmbeddedConnection(null, "merge-test", configFile);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (embedded != null) {
			embedded.close();
		}
		if (configFile != null) {
			Files.deleteIfExists(configFile);
		}
	}

	@Test
	void mergeSingleReturnsMergedValue() {
		var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
		long colId = embedded.createColumn("c1", schema);
		var api = embedded.getSyncApi();
		var key = new Keys(toBufSimple(1));
		Buf merged;

		merged = api.merge(0, colId, key, s("A"), RequestType.merged());
		assertEquals(s("A"), merged);
		assertEquals(s("A"), api.get(0, colId, key, RequestType.current()));

		merged = api.merge(0, colId, key, s("B"), RequestType.merged());
		assertEquals(s("A,B"), merged);
		assertEquals(s("A,B"), api.get(0, colId, key, RequestType.current()));

		assertNull(api.merge(0, colId, key, s("C"), RequestType.none()));
		assertEquals(s("A,B,C"), api.get(0, colId, key, RequestType.current()));
	}

	@Test
	void mergeMultiReturnsMergedValues() {
		var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
		long colId = embedded.createColumn("c2", schema);
		var api = embedded.getSyncApi();
		var keys = List.of(new Keys(toBufSimple(2)), new Keys(toBufSimple(3)));
		var values = List.of(s("X"), s("Y"));

		var mergedList = api.mergeMulti(0, colId, keys, values, RequestType.merged());
		assertEquals(List.of(s("X"), s("Y")), mergedList);

		var moreValues = List.of(s("Z"), s("W"));
		mergedList = api.mergeMulti(0, colId, keys, moreValues, RequestType.merged());
		assertEquals(List.of(s("X,Z"), s("Y,W")), mergedList);
	}

	@Test
	void mergeBatchAppendsValues() throws RocksDBException {
		var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
		long colId = embedded.createColumn("c3", schema);
		var api = embedded.getSyncApi();

		var batchKeys = List.of(new Keys(toBufSimple(4)), new Keys(toBufSimple(5)));
		var batchValues = List.of(s("M"), s("N"));
		api.mergeBatch(colId, Flux.just(new KVBatchRef(batchKeys, batchValues)), MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);

		var batchValues2 = List.of(s("O"), s("P"));
		api.mergeBatch(colId, Flux.just(new KVBatchRef(batchKeys, batchValues2)), MergeBatchMode.MERGE_WRITE_BATCH);

		assertEquals(s("M,O"), api.get(0, colId, batchKeys.getFirst(), RequestType.current()));
		assertEquals(s("N,P"), api.get(0, colId, batchKeys.getLast(), RequestType.current()));
	}

	@Test
	void bucketedMergeUsesOperatorAndReturnsMerged() {
		var schema = ColumnSchema.of(IntList.of(1, 1), ObjectList.of(ColumnHashType.XXHASH32), true);
		long colId = embedded.createColumn("c4", schema);
		var api = embedded.getSyncApi();
		var key = new Keys(toBufSimple(7), toBufSimple(8), toBufSimple(9));

		Buf merged = api.merge(0, colId, key, s("a"), RequestType.merged());
		assertEquals(s("a"), merged);

		merged = api.merge(0, colId, key, s("b"), RequestType.merged());
		assertEquals(s("a,b"), merged);
	}

	@Test
	void grpcMergeReturnsMergedValue() throws Exception {
		var schema = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
		long colId = embedded.createColumn("c5", schema);
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 8129));
		server.start();
		try (var client = GrpcConnection.forHostAndPort("test-client", new it.cavallium.rockserver.core.common.Utils.HostAndPort("127.0.0.1", 8129))) {
			var api = client.getSyncApi();
			var key = new Keys(toBufSimple(11));
			Buf merged = api.merge(0, colId, key, s("q"), RequestType.merged());
			assertEquals(s("q"), merged);
			merged = api.merge(0, colId, key, s("r"), RequestType.merged());
			assertEquals(s("q,r"), merged);
		} finally {
			server.close();
		}
	}
}
