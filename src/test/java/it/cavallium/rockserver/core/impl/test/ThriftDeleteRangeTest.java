package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ThriftDeleteRangeTest {

	private Path dbDir;
	private Path configFile;
	private EmbeddedConnection embeddedConnection;
	private ThriftServer thriftServer;
	private ThriftConnection client;
	private long colId;

	@BeforeEach
	void setUp() throws IOException, TException {
		dbDir = Files.createTempDirectory("rockserver-thrift-delete-range-test");
		configFile = Files.createTempFile("rockserver-config", ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		embeddedConnection = new EmbeddedConnection(dbDir, "thrift-delete-range-test", configFile);
		var port = findFreePort();
		thriftServer = new ThriftServer(embeddedConnection, "127.0.0.1", port);
		thriftServer.start();
		client = new ThriftConnection("thrift-client", "127.0.0.1", port);
		colId = client.getSyncApi().createColumn("test-col", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
	}

	@AfterEach
	void tearDown() throws IOException {
		if (client != null) {
			client.close();
		}
		if (thriftServer != null) {
			thriftServer.close();
		}
		if (embeddedConnection != null) {
			embeddedConnection.close();
		}
		if (dbDir != null) {
			Utils.deleteDirectory(dbDir.toString());
		}
		if (configFile != null) {
			Files.deleteIfExists(configFile);
		}
	}

	@Test
	void deleteRangeOverThriftRemovesOnlyKeysInsideBounds() {
		var key1 = key(11);
		var key2 = key(12);
		var key3 = key(13);
		var key4 = key(14);

		client.getSyncApi().put(0, colId, key1, value(110), RequestType.none());
		client.getSyncApi().put(0, colId, key2, value(120), RequestType.none());
		client.getSyncApi().put(0, colId, key3, value(130), RequestType.none());
		client.getSyncApi().put(0, colId, key4, value(140), RequestType.none());

		client.getSyncApi().deleteRange(colId, key2, key4);

		assertTrue(client.getSyncApi().get(0, colId, key1, RequestType.exists()));
		assertFalse(client.getSyncApi().get(0, colId, key2, RequestType.exists()));
		assertFalse(client.getSyncApi().get(0, colId, key3, RequestType.exists()));
		assertTrue(client.getSyncApi().get(0, colId, key4, RequestType.exists()));
	}

	@Test
	void noCacheRangeReadOverThriftMatchesNormalRangeRead() {
		var key1 = key(11);
		var key2 = key(12);
		var key3 = key(13);
		var key4 = key(14);

		client.getSyncApi().put(0, colId, key1, value(110), RequestType.none());
		client.getSyncApi().put(0, colId, key2, value(120), RequestType.none());
		client.getSyncApi().put(0, colId, key3, value(130), RequestType.none());
		client.getSyncApi().put(0, colId, key4, value(140), RequestType.none());

		var normal = client.getSyncApi()
				.getRange(0, colId, key2, key4, false, RequestType.allInRange(), 1_000)
				.toList();
		var noCache = client.getSyncApi()
				.getRange(0, colId, key2, key4, false, RequestType.allInRangeNoCache(), 1_000)
				.toList();

		assertEquals(List.of(new KV(key2, value(120)), new KV(key3, value(130))), noCache);
		assertEquals(normal, noCache);
	}

	private static int findFreePort() throws IOException {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}

	private static Buf value(int value) {
		return Utils.toBufSimple(value);
	}
}
