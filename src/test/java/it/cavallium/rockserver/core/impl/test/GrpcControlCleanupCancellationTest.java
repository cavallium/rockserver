package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GrpcControlCleanupCancellationTest {

	private static final String CLIENT_MAX_RETRY_ATTEMPTS
			= "it.cavallium.rockserver.grpc.client.max-retry-attempts";
	private static final String SERVER_GRACEFUL_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms";
	private static final String SERVER_FORCED_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms";
	private static final String SERVER_SCHEDULER_TIMEOUT
			= "it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms";
	private static final List<String> TEST_PROPERTIES = List.of(
			CLIENT_MAX_RETRY_ATTEMPTS,
			SERVER_GRACEFUL_TIMEOUT,
			SERVER_FORCED_TIMEOUT,
			SERVER_SCHEDULER_TIMEOUT);

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
		System.setProperty(CLIENT_MAX_RETRY_ATTEMPTS, "0");
		System.setProperty(SERVER_GRACEFUL_TIMEOUT, "2000");
		System.setProperty(SERVER_FORCED_TIMEOUT, "2000");
		System.setProperty(SERVER_SCHEDULER_TIMEOUT, "5000");

		dbDir = Files.createTempDirectory("rockserver-grpc-control-cancellation");
		configFile = Files.createTempFile("rockserver-grpc-control-cancellation", ".conf");
		Files.writeString(configFile, """
				database: {
				  parallelism: { read: 3, write: 3 }
				  global: { ingest-behind: false, optimistic: false }
				}
				""");
		embeddedConnection = new EmbeddedConnection(dbDir,
				"grpc-control-cancellation",
				configFile);
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
	void cancellationCannotRemoveAcceptedControlCleanup() throws Exception {
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		long columnId = sync.createColumn("protected-cleanup",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		sync.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		long transactionId = sync.openTransaction(TimeUnit.MINUTES.toMillis(1));
		long iteratorId = async.openIteratorAsync(0,
				columnId,
				new Keys(),
				null,
				false,
				TimeUnit.MINUTES.toMillis(1)).get(5, TimeUnit.SECONDS);
		long updateId = sync.get(0, columnId, key(1), RequestType.forUpdate()).updateId();
		var scheduler = embeddedConnection.getScheduler();
		var controlExecutor = scheduler.executor(WorkloadProfile.CONTROL,
				OperationFamily.CONTROL,
				RequestContext.NO_DEADLINE);
		var blockedControl = blockLane(2, controlExecutor::execute);
		try {
			var rollback = async.closeTransactionAsync(transactionId, false);
			var failedUpdateClose = async.closeFailedUpdateAsync(updateId);
			var iteratorClose = async.closeIteratorAsync(iteratorId);
			awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.CONTROL) >= 3,
					"protected cleanup operations were not queued");

			assertTrue(rollback.cancel(true));
			assertTrue(failedUpdateClose.cancel(true));
			assertTrue(iteratorClose.cancel(true));
			assertEquals(3, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
			assertEquals(1, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		} finally {
			blockedControl.release();
		}

		awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 0,
				"protected cleanup operations did not terminate");
		assertEquals(0, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	@Test
	void transactionCommitRemainsRemovableBeforeExecution() throws Exception {
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		long transactionId = sync.openTransaction(TimeUnit.MINUTES.toMillis(1));
		var scheduler = embeddedConnection.getScheduler();
		var ingestWriteExecutor = scheduler.executor(RequestContext.ingest(), OperationFamily.MUTATION);
		var blockedWrite = blockLane(3, ingestWriteExecutor::execute);
		try {
			var commit = async.closeTransactionAsync(transactionId, true);
			awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.BATCH) >= 1,
					"transaction commit was not queued");
			assertTrue(commit.cancel(true));
			awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.BATCH) == 0,
					"cancelled transaction commit remained queued");
			assertEquals(0, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
		} finally {
			blockedWrite.release();
		}

		assertEquals(1, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertTrue(async.closeTransactionAsync(transactionId, false).get(5, TimeUnit.SECONDS));
		assertEquals(0, embeddedConnection.getInternalDB().getOpenTransactionsCount());
	}

	@Test
	void cancelledCdcAcknowledgementCompletesAndLateFailureIsCountedOnce() throws Exception {
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		sync.cdcCreate("progress", 1L, null, false);
		var blockedWrite = blockCdcWriteLane();
		try {
			var successfulCommit = async.cdcCommitAsync("progress", 42L);
			var failingCommit = async.cdcCommitAsync("missing", 99L);
			awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 2,
					"CDC acknowledgements were not accepted");
			assertFalse(successfulCommit.isDone());
			assertFalse(failingCommit.isDone());
			assertTrue(successfulCommit.cancel(true));
			assertTrue(failingCommit.cancel(true));
			assertEquals(2, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
		} finally {
			blockedWrite.release();
		}

		awaitCondition(() -> sync.cdcGetLastCommittedSequence("progress").orElse(-1L) == 42L,
				"cancelled CDC acknowledgement did not persist progress");
		awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 0,
				"cancelled CDC acknowledgements did not terminate");
		assertEquals(1L, grpcServer.getLateProtectedOperationFailureCountForTesting("cdcCommit"));
	}

	@Test
	void shutdownWaitsForCancelledAcceptedControlCleanup() throws Exception {
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		long columnId = sync.createColumn("shutdown-cleanup",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		long iteratorId = async.openIteratorAsync(0,
				columnId,
				new Keys(),
				null,
				false,
				TimeUnit.MINUTES.toMillis(1)).get(5, TimeUnit.SECONDS);
		var scheduler = embeddedConnection.getScheduler();
		var controlExecutor = scheduler.executor(WorkloadProfile.CONTROL,
				OperationFamily.CONTROL,
				RequestContext.NO_DEADLINE);
		var blockedControl = blockLane(2, controlExecutor::execute);
		var close = async.closeIteratorAsync(iteratorId);
		awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.CONTROL) >= 1,
				"iterator close was not queued");
		assertTrue(close.cancel(true));
		awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 1,
				"iterator close was not tracked as accepted");

		var serverToClose = grpcServer;
		var closeFailure = new AtomicReference<Throwable>();
		var closeThread = Thread.startVirtualThread(() -> {
			try {
				serverToClose.close();
			} catch (Throwable error) {
				closeFailure.set(error);
			}
		});
		try {
			Thread.sleep(Duration.ofMillis(250));
			assertTrue(closeThread.isAlive(),
					"server shutdown must wait for accepted CONTROL cleanup");
		} finally {
			blockedControl.release();
		}
		closeThread.join(TimeUnit.SECONDS.toMillis(10));
		assertFalse(closeThread.isAlive(), "server shutdown did not finish after CONTROL cleanup");
		assertNull(closeFailure.get());
		grpcServer = null;
		assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	private GrpcConnection newClient() {
		var client = GrpcConnection.forHostAndPort("grpc-control-cancellation-client",
				new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
		clients.add(client);
		return client;
	}

	private static BlockedLane blockLane(int taskCount, Consumer<Runnable> submit) throws InterruptedException {
		var started = new CountDownLatch(taskCount);
		var release = new CountDownLatch(1);
		for (int i = 0; i < taskCount; i++) {
			submit.accept(() -> {
				started.countDown();
				try {
					release.await();
				} catch (InterruptedException error) {
					Thread.currentThread().interrupt();
				}
			});
		}
		assertTrue(started.await(5, TimeUnit.SECONDS), "scheduler lane did not saturate");
		return new BlockedLane(release);
	}

	private BlockedLane blockCdcWriteLane() throws InterruptedException {
		var scheduler = embeddedConnection.getScheduler();
		var release = new CountDownLatch(1);
		Runnable blocker = () -> {
			try {
				release.await();
			} catch (InterruptedException error) {
				Thread.currentThread().interrupt();
			}
		};
		var cdcWrite = scheduler.executor(WorkloadProfile.CDC,
				OperationFamily.MUTATION,
				RequestContext.NO_DEADLINE);
		var ingestWrite = scheduler.executor(RequestContext.ingest(), OperationFamily.MUTATION);
		for (int i = 0; i < 8; i++) {
			ingestWrite.execute(blocker);
			cdcWrite.execute(blocker);
		}
		awaitCondition(() -> scheduler.activeTasks(WorkloadProfile.INGEST)
					+ scheduler.activeTasks(WorkloadProfile.CDC) >= 3
					&& scheduler.queuedTasks(WorkloadProfile.INGEST) >= 1
					&& scheduler.queuedTasks(WorkloadProfile.CDC) >= 1,
				"CDC write lane did not saturate");
		return new BlockedLane(release);
	}

	private static void awaitCondition(BooleanSupplier condition, String failureMessage)
			throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (condition.getAsBoolean()) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		throw new AssertionError(failureMessage);
	}

	private static Keys key(long id) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(id).array()));
	}

	private record BlockedLane(CountDownLatch releaseLatch) {

		private void release() {
			releaseLatch.countDown();
		}
	}
}
