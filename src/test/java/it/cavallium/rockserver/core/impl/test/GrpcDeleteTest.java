package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WriteClass;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class GrpcDeleteTest {

	private Path dbDir;
	private Path configFile;
	private EmbeddedConnection embeddedConnection;
	private GrpcServer grpcServer;
	private GrpcConnection client;
	private long colId;

	@BeforeEach
	void setUp() throws IOException {
		dbDir = Files.createTempDirectory("rockserver-grpc-delete-test");
		configFile = Files.createTempFile("rockserver-config", ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		embeddedConnection = new EmbeddedConnection(dbDir, "grpc-delete-test", configFile);
		grpcServer = new GrpcServer(embeddedConnection, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
		client = GrpcConnection.forHostAndPort("grpc-client", new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
		colId = client.getSyncApi().createColumn("test-col", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
	}

	@AfterEach
	void tearDown() throws IOException {
		if (client != null) {
			client.close();
		}
		if (grpcServer != null) {
			grpcServer.close();
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
	void deleteNoneOverGrpcRemovesValue() {
		var key = key(1);
		var value = value(10);

		client.getSyncApi().put(0, colId, key, value, RequestType.none());
		client.getSyncApi().delete(0, colId, key, RequestType.none());

		assertFalse(client.getSyncApi().get(0, colId, key, RequestType.exists()));
	}

	@Test
	void explicitMaintenancePropagatesOverGrpc() {
		var key = key(101);
		client.getSyncApi().put(0, colId, key, value(101), RequestType.none(), WriteClass.MAINTENANCE);

		assertTrue(client.getSyncApi().get(0, colId, key, RequestType.exists()));

		client.getSyncApi().delete(0, colId, key, RequestType.none(), WriteClass.MAINTENANCE);
		assertFalse(client.getSyncApi().get(0, colId, key, RequestType.exists()));
	}

	@Test
	void explicitMaintenanceUsesDirectEmbeddedPointPath() {
		var key = key(103);
		embeddedConnection.getSyncApi().put(
				0, colId, key, value(103), RequestType.none(), WriteClass.MAINTENANCE);
		assertTrue(embeddedConnection.getSyncApi().get(0, colId, key, RequestType.exists()));

		embeddedConnection.getSyncApi().delete(
				0, colId, key, RequestType.none(), WriteClass.MAINTENANCE);
		assertFalse(embeddedConnection.getSyncApi().get(0, colId, key, RequestType.exists()));
	}

	@Test
	void unknownGrpcWriteClassIsInvalidArgument() {
		var channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
				.usePlaintext()
				.build();
		try {
			var request = PutRequest.newBuilder()
					.setColumnId(colId)
					.setData(it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
							.addKeys(ByteString.copyFrom(ByteBuffer.allocate(Long.BYTES).putLong(102).array()))
							.setValue(ByteString.copyFrom(new byte[] {1})))
					.setWriteClassValue(99)
					.build();
			var error = assertThrows(StatusRuntimeException.class,
					() -> RocksDBServiceGrpc.newBlockingStub(channel).put(request));
			assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		} finally {
			channel.shutdownNow();
		}
	}

	@Test
	void unknownStreamingGrpcWriteClassIsInvalidArgument() {
		var channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
				.usePlaintext()
				.build();
		try {
			var initial = PutBatchRequest.newBuilder()
					.setInitialRequest(PutBatchInitialRequest.newBuilder()
							.setColumnId(colId)
							.setWriteClassValue(99))
					.build();
			var error = assertThrows(StatusRuntimeException.class,
					() -> ReactorRocksDBServiceGrpc.newReactorStub(channel).putBatch(Flux.just(initial)).block());
			assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		} finally {
			channel.shutdownNow();
		}
	}

	@Test
	void deletePreviousOverGrpcReturnsPreviousValue() {
		var key = key(2);
		var value = value(20);

		client.getSyncApi().put(0, colId, key, value, RequestType.none());
		var previous = client.getSyncApi().delete(0, colId, key, RequestType.previous());
		var missingPrevious = client.getSyncApi().delete(0, colId, key, RequestType.previous());

		assertEquals(value, previous);
		assertNull(missingPrevious);
	}

	@Test
	void deletePreviousPresenceOverGrpcReturnsPresence() {
		var key = key(3);

		client.getSyncApi().put(0, colId, key, value(30), RequestType.none());
		var existed = client.getSyncApi().delete(0, colId, key, RequestType.previousPresence());
		var existedAgain = client.getSyncApi().delete(0, colId, key, RequestType.previousPresence());

		assertTrue(existed);
		assertFalse(existedAgain);
	}

	@Test
	void deleteMultiOverGrpcSupportsPreviousPresence() {
		var key1 = key(4);
		var key2 = key(5);
		var key3 = key(6);

		client.getSyncApi().put(0, colId, key1, value(40), RequestType.none());
		client.getSyncApi().put(0, colId, key3, value(60), RequestType.none());
		var result = client.getSyncApi().deleteMulti(0, colId, List.of(key1, key2, key3), RequestType.previousPresence());

		assertEquals(List.of(true, false, true), result);
		assertFalse(client.getSyncApi().get(0, colId, key1, RequestType.exists()));
		assertFalse(client.getSyncApi().get(0, colId, key3, RequestType.exists()));
	}

	@Test
	void deleteRangeOverGrpcRemovesOnlyKeysInsideBounds() {
		var key1 = key(7);
		var key2 = key(8);
		var key3 = key(9);
		var key4 = key(10);

		client.getSyncApi().put(0, colId, key1, value(70), RequestType.none());
		client.getSyncApi().put(0, colId, key2, value(80), RequestType.none());
		client.getSyncApi().put(0, colId, key3, value(90), RequestType.none());
		client.getSyncApi().put(0, colId, key4, value(100), RequestType.none());

		client.getSyncApi().deleteRange(colId, key2, key4);

		assertTrue(client.getSyncApi().get(0, colId, key1, RequestType.exists()));
		assertFalse(client.getSyncApi().get(0, colId, key2, RequestType.exists()));
		assertFalse(client.getSyncApi().get(0, colId, key3, RequestType.exists()));
		assertTrue(client.getSyncApi().get(0, colId, key4, RequestType.exists()));
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}

	private static Buf value(int value) {
		return Utils.toBufSimple(value);
	}
}
