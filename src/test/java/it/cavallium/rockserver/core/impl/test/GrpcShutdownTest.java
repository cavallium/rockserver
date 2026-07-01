package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GrpcShutdownTest {

	private static final String CLIENT_MAX_RETRY_ATTEMPTS
			= "it.cavallium.rockserver.grpc.client.max-retry-attempts";
	private static final String CLIENT_INITIAL_BACKOFF
			= "it.cavallium.rockserver.grpc.client.initial-retry-backoff";
	private static final String CLIENT_MAX_BACKOFF
			= "it.cavallium.rockserver.grpc.client.max-retry-backoff";
	private static final String CLIENT_BACKOFF_MULTIPLIER
			= "it.cavallium.rockserver.grpc.client.retry-backoff-multiplier";
	private static final String SERVER_GRACEFUL_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms";
	private static final String SERVER_FORCED_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms";
	private static final String DB_PENDING_OPS_TIMEOUT
			= "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
	private static final List<String> TEST_PROPERTIES = List.of(
			CLIENT_MAX_RETRY_ATTEMPTS,
			CLIENT_INITIAL_BACKOFF,
			CLIENT_MAX_BACKOFF,
			CLIENT_BACKOFF_MULTIPLIER,
			SERVER_GRACEFUL_TIMEOUT,
			SERVER_FORCED_TIMEOUT,
			DB_PENDING_OPS_TIMEOUT
	);

	private final List<String> previousProperties = new ArrayList<>();
	private final List<GrpcConnection> clients = new ArrayList<>();
	private Path dbDir;
	private Path configFile;
	private EmbeddedConnection embeddedConnection;
	private GrpcServer grpcServer;

	@BeforeEach
	void setUp() throws IOException {
		for (String property : TEST_PROPERTIES) {
			previousProperties.add(System.getProperty(property));
		}
		System.setProperty(CLIENT_MAX_RETRY_ATTEMPTS, "2");
		System.setProperty(CLIENT_INITIAL_BACKOFF, "0.01s");
		System.setProperty(CLIENT_MAX_BACKOFF, "0.01s");
		System.setProperty(CLIENT_BACKOFF_MULTIPLIER, "1.0");
		System.setProperty(SERVER_GRACEFUL_TIMEOUT, "5000");
		System.setProperty(SERVER_FORCED_TIMEOUT, "5000");
		System.setProperty(DB_PENDING_OPS_TIMEOUT, "100");

		dbDir = Files.createTempDirectory("rockserver-grpc-shutdown-test");
		configFile = Files.createTempFile("rockserver-config", ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		embeddedConnection = new EmbeddedConnection(dbDir, "grpc-shutdown-test", configFile);
		grpcServer = new GrpcServer(embeddedConnection, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
	}

	@AfterEach
	void tearDown() throws IOException {
		for (GrpcConnection client : clients) {
			client.close();
		}
		clients.clear();
		if (grpcServer != null) {
			grpcServer.close();
			grpcServer = null;
		}
		if (embeddedConnection != null) {
			embeddedConnection.closeTesting();
			embeddedConnection = null;
		}
		if (dbDir != null) {
			Utils.deleteDirectory(dbDir.toString());
			dbDir = null;
		}
		if (configFile != null) {
			Files.deleteIfExists(configFile);
			configFile = null;
		}
		for (int i = 0; i < TEST_PROPERTIES.size(); i++) {
			var previous = previousProperties.get(i);
			var property = TEST_PROPERTIES.get(i);
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
		previousProperties.clear();
	}

	@Test
	void serverShutdownWithIdleConnectedClientsDoesNotHang() {
		var client1 = newClient();
		var client2 = newClient();
		var colId = client1.getSyncApi().createColumn("idle-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		assertEquals(colId, client2.getSyncApi().getColumnId("idle-col"));

		assertTimeoutPreemptively(Duration.ofSeconds(10), this::closeGrpcServer);
	}

	@Test
	void requestAfterServerShutdownFailsPromptly() throws Exception {
		var client = newClient();
		client.getSyncApi().createColumn("closed-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		closeGrpcServer();

		var request = client.getAsyncApi().getColumnIdAsync("closed-col");
		assertThrows(ExecutionException.class, () -> request.get(3, TimeUnit.SECONDS));
	}

	@Test
	void closingGrpcServerDoesNotDisposeEmbeddedScheduler() throws Exception {
		var client = newClient();
		var colId = client.getSyncApi().createColumn("scheduler-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		closeGrpcServer();

		assertEquals(colId, embeddedConnection.getAsyncApi().getColumnIdAsync("scheduler-col")
				.get(3, TimeUnit.SECONDS));
	}

	@Test
	void databaseShutdownWithOpenRemoteTransactionIsBounded() throws Exception {
		var client = newClient();
		var colId = client.getSyncApi().createColumn("tx-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		var txId = client.getSyncApi().openTransaction(TimeUnit.MINUTES.toMillis(5));
		client.getSyncApi().put(txId,
				colId,
				key(1),
				Buf.wrap(new byte[] {1}),
				RequestType.none());

		closeGrpcServer();

		assertTimeoutPreemptively(Duration.ofSeconds(10), () -> {
			embeddedConnection.closeTesting();
			embeddedConnection = null;
		});
	}

	private GrpcConnection newClient() {
		var client = GrpcConnection.forHostAndPort("grpc-shutdown-client",
				new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
		clients.add(client);
		return client;
	}

	private void closeGrpcServer() throws IOException {
		if (grpcServer != null) {
			grpcServer.close();
			grpcServer = null;
		}
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}
}
