package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GrpcShutdownTest {
	private static final int DATA_WORKERS = 3;

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
	private static final String SERVER_SCHEDULER_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms";
	private static final String DB_PENDING_OPS_TIMEOUT
			= "it.cavallium.rockserver.db.shutdown-pending-ops-timeout-ms";
	private static final List<String> TEST_PROPERTIES = List.of(
			CLIENT_MAX_RETRY_ATTEMPTS,
			CLIENT_INITIAL_BACKOFF,
			CLIENT_MAX_BACKOFF,
			CLIENT_BACKOFF_MULTIPLIER,
			SERVER_GRACEFUL_TIMEOUT,
			SERVER_FORCED_TIMEOUT,
			SERVER_SCHEDULER_TIMEOUT,
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
		Files.writeString(configFile, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { ingest-behind: false, optimistic: false }
				}
				""");
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
		var colId = client1.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("idle-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		assertEquals(colId, client2.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnId("idle-col"));

		assertTimeoutPreemptively(Duration.ofSeconds(10), this::closeGrpcServer);
	}

	@Test
	void maximumConfiguredShutdownDurationsDoNotOverflowMonotonicDeadlines() throws Exception {
		System.setProperty(SERVER_GRACEFUL_TIMEOUT, Long.toString(Long.MAX_VALUE));
		System.setProperty(SERVER_FORCED_TIMEOUT, Long.toString(Long.MAX_VALUE));
		System.setProperty(SERVER_SCHEDULER_TIMEOUT, Long.toString(Long.MAX_VALUE));
		try {
			assertTimeoutPreemptively(Duration.ofSeconds(10),
					() -> assertDoesNotThrow(this::closeGrpcServer));
		} finally {
			System.setProperty(SERVER_GRACEFUL_TIMEOUT, "5000");
			System.setProperty(SERVER_FORCED_TIMEOUT, "5000");
			System.setProperty(SERVER_SCHEDULER_TIMEOUT, "5000");
			closeGrpcServer();
		}
	}

	@Test
	void requestAfterServerShutdownFailsPromptly() throws Exception {
		var client = newClient();
		client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("closed-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		closeGrpcServer();

		var request = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnIdAsync("closed-col");
		assertThrows(ExecutionException.class, () -> request.get(3, TimeUnit.SECONDS));
	}

	@Test
	void closingGrpcServerDoesNotDisposeEmbeddedScheduler() throws Exception {
		var client = newClient();
		var colId = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("scheduler-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));

		closeGrpcServer();

		assertEquals(colId, embeddedConnection.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).getColumnIdAsync("scheduler-col")
				.get(3, TimeUnit.SECONDS));
	}

	@Test
	void databaseShutdownWithOpenRemoteTransactionIsBounded() throws Exception {
		// An idle transaction is a resource to roll back after active requests drain;
		// it must not consume this operation timeout during shutdown.
		System.setProperty(DB_PENDING_OPS_TIMEOUT, "5000");
		var client = newClient();
		var colId = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("tx-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		var txId = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).openTransaction( java.time.Duration.ofMillis(TimeUnit.MINUTES.toMillis(5)));
		client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).put(txId,
				colId,
				key(1),
				Buf.wrap(new byte[] {1}),
				RequestType.none());

		closeGrpcServer();
		assertEquals(1, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertEquals(1, embeddedConnection.getInternalDB().getPendingOpsCount());

		assertTimeoutPreemptively(Duration.ofSeconds(3), () -> {
			embeddedConnection.closeTesting();
			embeddedConnection = null;
		});
	}

	@Test
	void blockedMaintenanceDoesNotDelayRemoteIteratorClose() throws Exception {
		var client = newClient();
		var api = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
		var syncApi = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
		var colId = syncApi.createColumn("maintenance-close-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		syncApi.put(0,
				colId,
				key(1),
				Buf.wrap(new byte[] {1}),
				RequestType.none());
		long iteratorId = api.openIteratorAsync(0,
				colId,
				new Keys(),
				null,
				false, java.time.Duration.ofMillis(
				TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);

		var maintenanceStarted = new CountDownLatch(1);
		var releaseMaintenance = new CountDownLatch(1);
		embeddedConnection.getScheduler().scheduler(it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE, it.cavallium.rockserver.core.common.OperationFamily.COMPACTION, Long.MAX_VALUE).schedule(() -> {
			maintenanceStarted.countDown();
			try {
				releaseMaintenance.await();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
		});
		try {
			assertTrue(maintenanceStarted.await(5, TimeUnit.SECONDS));
			api.closeIteratorAsync(iteratorId).get(5, TimeUnit.SECONDS);
			assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
			assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
		} finally {
			releaseMaintenance.countDown();
		}
	}

	@Test
	void remoteCleanupBypassesASaturatedWriteLane() throws Exception {
		var client = newClient();
		var api = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
		client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).cdcCreate("progress", 1L, null, false, java.util.OptionalLong.empty());
		var writeStarted = new CountDownLatch(DATA_WORKERS);
		var releaseWrite = new CountDownLatch(1);
		for (int i = 0; i < DATA_WORKERS; i++) {
			embeddedConnection.getScheduler().executor(it.cavallium.rockserver.core.common.WorkloadProfile.INGEST, it.cavallium.rockserver.core.common.OperationFamily.MUTATION, Long.MAX_VALUE).execute(() -> {
				writeStarted.countDown();
				try {
					releaseWrite.await();
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			});
		}
		try {
			assertTrue(writeStarted.await(5, TimeUnit.SECONDS));
			api.closeFailedUpdateAsync(Long.MAX_VALUE).get(5, TimeUnit.SECONDS);
			assertTrue(api.closeTransactionAsync(Long.MAX_VALUE, false).get(5, TimeUnit.SECONDS));
			var cdcCommit = api.cdcCommitAsync("progress", 42L);
			assertThrows(TimeoutException.class, () -> cdcCommit.get(200, TimeUnit.MILLISECONDS),
					"remote cdcCommit must remain queued as must-complete CDC mutation work");

			var commit = api.closeTransactionAsync(Long.MAX_VALUE, true);
			assertThrows(TimeoutException.class, () -> commit.get(200, TimeUnit.MILLISECONDS),
					"remote commit=true must remain queued on the saturated write lane");
			releaseWrite.countDown();
			cdcCommit.get(5, TimeUnit.SECONDS);
			assertEquals(java.util.OptionalLong.of(42L),
					embeddedConnection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).cdcGetLastCommittedSequence("progress"));
			commit.handle((_, _) -> null).get(5, TimeUnit.SECONDS);
			assertTrue(commit.isCompletedExceptionally());
		} finally {
			releaseWrite.countDown();
		}
	}

	@Test
	void cancelledQueuedRemoteIteratorCloseStillCompletes() throws Exception {
		var client = newClient();
		var api = client.getAsyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
		var colId = client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("cancelled-close-col",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		client.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).put(0,
				colId,
				key(1),
				Buf.wrap(new byte[] {1}),
				RequestType.none());

		var controlStarted = new CountDownLatch(2);
		var releaseControl = new CountDownLatch(1);
		var scheduler = embeddedConnection.getScheduler();
		for (int i = 0; i < 2; i++) {
			embeddedConnection.getScheduler().scheduler(it.cavallium.rockserver.core.common.WorkloadProfile.CONTROL, it.cavallium.rockserver.core.common.OperationFamily.CONTROL, Long.MAX_VALUE).schedule(() -> {
				controlStarted.countDown();
				try {
					releaseControl.await();
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
			});
		}

		long iteratorId = 0;
		try {
			assertTrue(controlStarted.await(5, TimeUnit.SECONDS));
			iteratorId = api.openIteratorAsync(0,
					colId,
					new Keys(),
					null,
					false, java.time.Duration.ofMillis(
					TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);
			assertEquals(1, embeddedConnection.getInternalDB().getOpenIteratorsCount());
			assertEquals(1, embeddedConnection.getInternalDB().getPendingOpsCount());

			var close = api.closeIteratorAsync(iteratorId);
			awaitQueuedTask(scheduler, WorkloadProfile.CONTROL);
			assertTrue(close.cancel(true));
			assertEquals(1, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
			assertEquals(1, embeddedConnection.getInternalDB().getOpenIteratorsCount());
			assertEquals(1, embeddedConnection.getInternalDB().getPendingOpsCount());
		} finally {
			releaseControl.countDown();
		}

		awaitOpenIteratorCount(0);
		assertEquals(0, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
		assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		assertEquals(0, embeddedConnection.getInternalDB().getPendingOpsCount());
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

	private static void awaitQueuedTask(RWScheduler scheduler, WorkloadProfile profile) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (scheduler.queuedTasks(profile) > 0) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		throw new AssertionError("remote iterator close was not queued on the blocked control lane");
	}

	private void awaitOpenIteratorCount(int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (embeddedConnection.getInternalDB().getOpenIteratorsCount() == expected) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		throw new AssertionError("open iterator count did not reach " + expected);
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}
}
