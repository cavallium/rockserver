package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.BindableService;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.NettyServerBuilder;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.URI;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
	private io.grpc.Server observedServer;

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
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
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
		if (observedServer != null) {
			observedServer.shutdownNow();
			try {
				assertTrue(observedServer.awaitTermination(5, TimeUnit.SECONDS));
			} catch (InterruptedException error) {
				Thread.currentThread().interrupt();
				throw new IOException(error);
			}
			observedServer = null;
		}
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
		long transactionId = sync.openTransaction( java.time.Duration.ofMillis(TimeUnit.MINUTES.toMillis(1)));
		long iteratorId = async.openIteratorAsync(0,
				columnId,
				new Keys(),
				null,
				false, java.time.Duration.ofMillis(
				TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);
		long updateId = sync.get(0, columnId, key(1), RequestType.forUpdate()).updateId();
		var scheduler = embeddedConnection.getScheduler();
		var controlExecutor = scheduler.executor(WorkloadProfile.CONTROL,
				OperationFamily.CONTROL,
				Long.MAX_VALUE);
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
			awaitCondition(() -> grpcServer.getCancelledMustCompleteOperationCountForTesting() == 3,
					"protected cleanup cancellation signals did not reach the server");
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
		long transactionId = sync.openTransaction( java.time.Duration.ofMillis(TimeUnit.MINUTES.toMillis(1)));
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
	void rollbackUsesProtectedBackendContextWithoutCallerDeadline() throws Exception {
		var recordingBackend = new RecordingRollbackConnection(embeddedConnection);
		replaceServer(recordingBackend);
		var client = newClient();
		long transactionId = client.getSyncApi(RequestContext.batch())
				.openTransaction( java.time.Duration.ofMillis(TimeUnit.MINUTES.toMillis(1)));
		var expiredCallerContext = RequestContext.batch(java.time.Duration.ofNanos(1));

		assertTrue(client.getAsyncApi(expiredCallerContext)
				.closeTransactionAsync(transactionId, false)
				.get(5, TimeUnit.SECONDS));
		var backendContext = recordingBackend.rollbackContext.get();
		assertNotNull(backendContext);
		assertEquals(WorkloadProfile.BATCH, backendContext.profile());
		assertEquals(Long.MAX_VALUE, backendContext.timeoutNanos());
		assertEquals(0, embeddedConnection.getInternalDB().getOpenTransactionsCount());
	}

	@Test
	void cancelledCdcAcknowledgementCompletesAndLateFailureIsCountedOnce() throws Exception {
		var exportedRegistry = new SimpleMeterRegistry();
		((CompositeMeterRegistry) embeddedConnection.getInternalDB().getMetricsRegistry())
				.add(exportedRegistry);
		var blockingBackend = new BlockingCdcCommitConnection(embeddedConnection);
		replaceServer(blockingBackend);
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		sync.cdcCreate("progress", 1L, null, false, java.util.OptionalLong.empty());
		var successfulCommit = async.cdcCommitAsync("progress", 42L);
		var failingCommit = async.cdcCommitAsync("missing", 99L);
		try {
			assertTrue(blockingBackend.awaitCommitsStarted(5, TimeUnit.SECONDS),
					"CDC acknowledgements did not begin running");
			awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 2,
					"CDC acknowledgements were not accepted");
			assertFalse(successfulCommit.isDone());
			assertFalse(failingCommit.isDone());
			assertTrue(successfulCommit.cancel(true));
			assertTrue(failingCommit.cancel(true));
			awaitCondition(() -> grpcServer.getCancelledMustCompleteOperationCountForTesting() == 2,
					"CDC cancellation signals did not reach the server");
			assertEquals(2, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
		} finally {
			blockingBackend.releaseCommits();
		}

		awaitCondition(() -> sync.cdcGetLastCommittedSequence("progress").orElse(-1L) == 42L,
				"cancelled CDC acknowledgement did not persist progress");
		awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 0,
				"cancelled CDC acknowledgements did not terminate");
		assertEquals(1L, grpcServer.getLateProtectedOperationFailureCountForTesting("cdcCommit"));
		var exportedCounter = exportedRegistry.find("rockserver.grpc.protected.late.failures")
				.tag("operation", "cdcCommit")
				.counter();
		assertNotNull(exportedCounter, "late protected-operation failure metric was not exported");
		assertEquals(1.0, exportedCounter.count(),
				"late protected-operation failure metric must be incremented exactly once");
	}

	@Test
	void runningCancellationSurvivesInterruptedOwnedSchedulerShutdown() throws Exception {
		var blockingBackend = new BlockingCloseIteratorConnection(embeddedConnection);
		replaceServer(blockingBackend);
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		long columnId = sync.createColumn("running-shutdown-cleanup",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		long iteratorId = async.openIteratorAsync(0,
				columnId,
				new Keys(),
				null,
				false, java.time.Duration.ofMillis(
				TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);
		var close = async.closeIteratorAsync(iteratorId);
		Thread closeThread = null;
		try {
			assertTrue(blockingBackend.awaitCloseStarted(5, TimeUnit.SECONDS),
					"protected close did not begin running");
			assertTrue(close.cancel(true));
			awaitCondition(() -> grpcServer.getCancelledMustCompleteOperationCountForTesting() == 1,
					"running close cancellation did not reach the server");
			assertEquals(1, grpcServer.getAcceptedMustCompleteOperationCountForTesting());

			var serverToClose = grpcServer;
			var closeFailure = new AtomicReference<Throwable>();
			var closePreservedInterrupt = new AtomicBoolean();
			closeThread = Thread.startVirtualThread(() -> {
				try {
					serverToClose.close();
				} catch (Throwable error) {
					closeFailure.set(error);
				} finally {
					closePreservedInterrupt.set(Thread.currentThread().isInterrupted());
				}
			});
			Thread.sleep(Duration.ofMillis(100));
			closeThread.interrupt();
			Thread.sleep(Duration.ofMillis(150));
			assertTrue(closeThread.isAlive(),
					"interruption must not bypass the accepted protected-operation drain");

			blockingBackend.releaseClose();
			closeThread.join(TimeUnit.SECONDS.toMillis(10));
			assertFalse(closeThread.isAlive(), "owned-scheduler shutdown did not finish after cleanup");
			assertNull(closeFailure.get());
			assertTrue(closePreservedInterrupt.get(), "shutdown must restore the caller's interrupt status");
		} finally {
			blockingBackend.releaseClose();
			if (closeThread != null) {
				closeThread.join(TimeUnit.SECONDS.toMillis(10));
			}
		}
		grpcServer = null;
		assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	@Test
	void protectedDrainTimeoutDoesNotTerminateOwnedScheduler() throws Exception {
		System.setProperty(SERVER_SCHEDULER_TIMEOUT, "50");
		var blockingBackend = new BlockingCloseIteratorConnection(embeddedConnection);
		replaceServer(blockingBackend);
		var client = newClient();
		var sync = client.getSyncApi(RequestContext.batch());
		var async = client.getAsyncApi(RequestContext.batch());
		long columnId = sync.createColumn("timeout-shutdown-cleanup",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		long iteratorId = async.openIteratorAsync(0,
				columnId,
				new Keys(),
				null,
				false, java.time.Duration.ofMillis(
				TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);
		var close = async.closeIteratorAsync(iteratorId);
		assertTrue(blockingBackend.awaitCloseStarted(5, TimeUnit.SECONDS),
				"protected close did not begin running");
		assertTrue(close.cancel(true));
		awaitCondition(() -> grpcServer.getCancelledMustCompleteOperationCountForTesting() == 1,
				"running close cancellation did not reach the server");

		try {
			var failure = assertThrows(IOException.class, grpcServer::close);
			assertTrue(failure.getMessage().contains("accepted protected gRPC operations"));
			assertEquals(1, grpcServer.getAcceptedMustCompleteOperationCountForTesting());
			var schedulerStillRunning = new CountDownLatch(1);
			grpcServer.getSchedulerForTesting().scheduler(WorkloadProfile.CONTROL,
					OperationFamily.CONTROL, Long.MAX_VALUE).schedule(schedulerStillRunning::countDown);
			assertTrue(schedulerStillRunning.await(5, TimeUnit.SECONDS),
					"owned scheduler was terminated before protected work drained");
		} finally {
			blockingBackend.releaseClose();
			System.setProperty(SERVER_SCHEDULER_TIMEOUT, "5000");
		}
		awaitCondition(() -> grpcServer.getAcceptedMustCompleteOperationCountForTesting() == 0,
				"protected close did not finish after the timeout");
		assertEquals(0, embeddedConnection.getInternalDB().getOpenIteratorsCount());
		grpcServer.close();
		grpcServer = null;
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
				false, java.time.Duration.ofMillis(
				TimeUnit.MINUTES.toMillis(1))).get(5, TimeUnit.SECONDS);
		var scheduler = embeddedConnection.getScheduler();
		var controlExecutor = scheduler.executor(WorkloadProfile.CONTROL,
				OperationFamily.CONTROL,
				Long.MAX_VALUE);
		var blockedControl = blockLane(2, controlExecutor::execute);
		var close = async.closeIteratorAsync(iteratorId);
		awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.CONTROL) >= 1,
				"iterator close was not queued");
		assertTrue(close.cancel(true));
		awaitCondition(() -> grpcServer.getCancelledMustCompleteOperationCountForTesting() == 1,
				"queued close cancellation did not reach the server");
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

	@Test
	void cancelledRunningNewForUpdateClosesItsLateAllocationExactlyOnce() throws Exception {
		var backend = new BlockingForUpdateConnection(embeddedConnection);
		var cancelled = new CountDownLatch(1);
		var client = observedClient(backend, cancelled);
		long column = newForUpdateColumn();
		var scheduler = embeddedConnection.getScheduler();
		var blockedControl = blockLane(2, scheduler.executor(WorkloadProfile.CONTROL,
				OperationFamily.CONTROL, Long.MAX_VALUE)::execute);
		try {
			var request = client.getAsyncApi(RequestContext.ingest()).getAsync(0, column, key(1), RequestType.forUpdate());
			assertTrue(backend.allocated.await(5, TimeUnit.SECONDS));
			assertEquals(1, embeddedConnection.getInternalDB().getOpenTransactionsCount());
			assertEquals(1L, embeddedConnection.getInternalDB().getPendingOpsCount());
			assertTrue(request.cancel(true));
			assertTrue(cancelled.await(5, TimeUnit.SECONDS));
			backend.release.countDown();
			awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.CONTROL) == 1,
					"late update cleanup was not submitted to CONTROL");
			assertEquals(0, backend.cleanups.get());
		} finally {
			backend.release.countDown();
			blockedControl.release();
		}
		awaitCondition(() -> embeddedConnection.getInternalDB().getOpenTransactionsCount() == 0
				&& embeddedConnection.getInternalDB().getPendingOpsCount() == 0L,
				"late new update allocation was not released before its TTL");
		awaitCondition(() -> scheduler.poolSnapshot(RWScheduler.Pool.CONTROL).drainedAndConserved(),
				"protected late cleanup did not finish");
		assertEquals(1, backend.cleanups.get());
		assertEquals(RequestContext.batch(), backend.cleanupContext.get());
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void cancelledRunningForUpdatePreservesExistingOwner(boolean explicitTransaction) throws Exception {
		long column = newForUpdateColumn();
		var owner = embeddedConnection.getSyncApi(RequestContext.ingest());
		long id = explicitTransaction ? owner.openTransaction(Duration.ofMinutes(1))
				: owner.get(0, column, key(1), RequestType.forUpdate()).updateId();
		var backend = new BlockingForUpdateConnection(embeddedConnection);
		var cancelled = new CountDownLatch(1);
		var client = observedClient(backend, cancelled);
		try {
			var request = client.getAsyncApi(RequestContext.ingest()).getAsync(id, column, key(1), RequestType.forUpdate());
			assertTrue(backend.allocated.await(5, TimeUnit.SECONDS));
			assertTrue(request.cancel(true));
			assertTrue(cancelled.await(5, TimeUnit.SECONDS));
		} finally {
			backend.release.countDown();
		}
		awaitCondition(() -> embeddedConnection.getScheduler().poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
				"cancelled existing-owner operation did not finish");
		awaitCondition(() -> embeddedConnection.getScheduler().poolSnapshot(RWScheduler.Pool.CONTROL).drainedAndConserved(),
				"late response handling did not finish");
		assertEquals(0, backend.cleanups.get());
		assertEquals(1, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		owner.put(id, column, key(1), Buf.wrap(new byte[] {2}), RequestType.none());
		if (explicitTransaction) assertTrue(owner.closeTransaction(id, true));
		assertEquals(Buf.wrap(new byte[] {2}), owner.get(0, column, key(1), RequestType.current()));
		assertEquals(0, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	@Test
	void successfulForUpdateTransfersNewAllocationToCallerWithoutCleanup() throws Exception {
		var backend = new BlockingForUpdateConnection(embeddedConnection);
		backend.release.countDown();
		var client = observedClient(backend, new CountDownLatch(1));
		long column = newForUpdateColumn();
		var response = client.getAsyncApi(RequestContext.ingest()).getAsync(0, column, key(1), RequestType.forUpdate())
				.get(5, TimeUnit.SECONDS);
		assertEquals(Buf.wrap(new byte[] {1}), response.previous());
		assertEquals(1, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertEquals(0, backend.cleanups.get());
		embeddedConnection.getSyncApi(RequestContext.ingest()).closeFailedUpdate(response.updateId());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	@Test
	void cancelledQueuedForUpdateAllocatesNoUpdate() throws Exception {
		var backend = new BlockingForUpdateConnection(embeddedConnection);
		backend.release.countDown();
		var cancelled = new CountDownLatch(1);
		var client = observedClient(backend, cancelled);
		long column = newForUpdateColumn();
		var scheduler = embeddedConnection.getScheduler();
		var blockedWrite = blockLane(3, scheduler.executor(RequestContext.ingest(), OperationFamily.MUTATION)::execute);
		try {
			var request = client.getAsyncApi(RequestContext.ingest()).getAsync(0, column, key(1), RequestType.forUpdate());
			awaitCondition(() -> scheduler.queuedTasks(WorkloadProfile.INGEST) == 1, "for-update was not queued");
			assertTrue(request.cancel(true));
			assertTrue(cancelled.await(5, TimeUnit.SECONDS));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.INGEST));
		} finally {
			blockedWrite.release();
		}
		awaitCondition(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
				"write lane did not drain after queued cancellation");
		assertEquals(1, backend.allocated.getCount());
		assertEquals(0, backend.cleanups.get());
		assertEquals(0, embeddedConnection.getInternalDB().getOpenTransactionsCount());
		assertEquals(0L, embeddedConnection.getInternalDB().getPendingOpsCount());
	}

	private long newForUpdateColumn() {
		var api = embeddedConnection.getSyncApi(RequestContext.batch());
		long column = api.createColumn("for-update", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		api.put(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		return column;
	}

	private GrpcConnection observedClient(RocksDBConnection backend, CountDownLatch cancelled) throws Exception {
		grpcServer.close();
		grpcServer = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0));
		var grpcField = GrpcServer.class.getDeclaredField("grpc");
		grpcField.setAccessible(true);
		var service = (BindableService) grpcField.get(grpcServer);
		observedServer = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
				.addService(service)
				.intercept(new ServerInterceptor() {
					@Override
					public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
							Metadata headers, ServerCallHandler<ReqT, RespT> next) {
						var delegate = next.startCall(call, headers);
						return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(delegate) {
							@Override
							public void onCancel() {
								super.onCancel();
								cancelled.countDown();
							}
						};
					}
				}).build().start();
		var client = GrpcConnection.forHostAndPort("grpc-for-update-cancellation-client",
				new Utils.HostAndPort("127.0.0.1", observedServer.getPort()));
		clients.add(client);
		return client;
	}

	private static final class BlockingForUpdateConnection implements RocksDBConnection, InternalConnection {
		private final EmbeddedConnection delegate;
		private final CountDownLatch allocated = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final AtomicInteger cleanups = new AtomicInteger();
		private final AtomicReference<RequestContext> cleanupContext = new AtomicReference<>();

		private BlockingForUpdateConnection(EmbeddedConnection delegate) {
			this.delegate = delegate;
		}

		@Override
		public URI getUrl() {
			return delegate.getUrl();
		}

		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() {
			return delegate.getCapabilities();
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			var api = delegate.getSyncApi(context);
			return new RocksDBSyncAPI() {
				@Override
				public <I, S, A> S requestSync(RocksDBAPICommand<I, S, A> command) {
					if (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate) {
						cleanups.incrementAndGet();
						cleanupContext.set(context);
					}
					var result = api.requestSync(command);
					if (command instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get
							&& get.requestType().getRequestTypeId() == RequestType.RequestTypeId.FOR_UPDATE) {
						allocated.countDown();
						awaitUninterruptibly(release);
					}
					return result;
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return delegate.getAsyncApi(context);
		}

		@Override
		public RWScheduler getScheduler() {
			return delegate.getScheduler();
		}

		@Override
		public EmbeddedDB getEmbeddedDB() {
			return delegate.getEmbeddedDB();
		}

		@Override
		public void close() {}
	}

	private GrpcConnection newClient() {
		var client = GrpcConnection.forHostAndPort("grpc-control-cancellation-client",
				new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
		clients.add(client);
		return client;
	}

	private void replaceServer(RocksDBConnection backend) throws IOException {
		grpcServer.close();
		grpcServer = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
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

	private static final class BlockingCloseIteratorConnection implements RocksDBConnection {

		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() {
			return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT;
		}


		private final RocksDBConnection delegate;
		private final CountDownLatch closeStarted = new CountDownLatch(1);
		private final CountDownLatch releaseClose = new CountDownLatch(1);

		private BlockingCloseIteratorConnection(RocksDBConnection delegate) {
			this.delegate = delegate;
		}

		@Override
		public URI getUrl() {
			return delegate.getUrl();
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			var delegateApi = delegate.getSyncApi(context);
			return new RocksDBSyncAPI() {
				@Override
				public <RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> SYNC_RESULT requestSync(
						RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> request) {
					if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator) {
						closeStarted.countDown();
						awaitReleaseUninterruptibly();
					}
					return delegateApi.requestSync(request);
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return delegate.getAsyncApi(context);
		}

		@Override
		public void close() {
			// The test owns the embedded delegate separately.
		}

		private boolean awaitCloseStarted(long timeout, TimeUnit unit) throws InterruptedException {
			return closeStarted.await(timeout, unit);
		}

		private void releaseClose() {
			releaseClose.countDown();
		}

		private void awaitReleaseUninterruptibly() {
			boolean interrupted = false;
			for (;;) {
				try {
					releaseClose.await();
					break;
				} catch (InterruptedException _) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private static final class RecordingRollbackConnection implements RocksDBConnection {

		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() {
			return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT;
		}


		private final RocksDBConnection delegate;
		private final AtomicReference<RequestContext> rollbackContext = new AtomicReference<>();

		private RecordingRollbackConnection(RocksDBConnection delegate) {
			this.delegate = delegate;
		}

		@Override
		public URI getUrl() {
			return delegate.getUrl();
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			var delegateApi = delegate.getSyncApi(context);
			return new RocksDBSyncAPI() {
				@Override
				public <RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> SYNC_RESULT requestSync(
						RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> request) {
					if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction closeTransaction
							&& !closeTransaction.commit()) {
						rollbackContext.set(context);
					}
					return delegateApi.requestSync(request);
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return delegate.getAsyncApi(context);
		}

		@Override
		public void close() {
			// The test owns the embedded delegate separately.
		}
	}

	private static final class BlockingCdcCommitConnection implements RocksDBConnection, InternalConnection {
		@Override
		public it.cavallium.rockserver.core.common.RockserverCapabilities getCapabilities() { return it.cavallium.rockserver.core.common.RockserverCapabilities.CURRENT; }

		private final EmbeddedConnection delegate;
		private final CountDownLatch commitsStarted = new CountDownLatch(2);
		private final CountDownLatch releaseCommits = new CountDownLatch(1);

		private BlockingCdcCommitConnection(EmbeddedConnection delegate) {
			this.delegate = delegate;
		}

		@Override
		public URI getUrl() {
			return delegate.getUrl();
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			var delegateApi = delegate.getSyncApi(context);
			return new RocksDBSyncAPI() {
				@Override
				public <RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> SYNC_RESULT requestSync(
						RocksDBAPICommand<RESULT_ITEM_TYPE, SYNC_RESULT, ASYNC_RESULT> request) {
					if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CdcCommit) {
						commitsStarted.countDown();
						awaitUninterruptibly(releaseCommits);
					}
					return delegateApi.requestSync(request);
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return delegate.getAsyncApi(context);
		}

		@Override
		public RWScheduler getScheduler() {
			return delegate.getScheduler();
		}

		@Override
		public EmbeddedDB getEmbeddedDB() {
			return delegate.getEmbeddedDB();
		}

		@Override
		public void close() {
			// The test owns the embedded delegate separately.
		}

		private boolean awaitCommitsStarted(long timeout, TimeUnit unit) throws InterruptedException {
			return commitsStarted.await(timeout, unit);
		}

		private void releaseCommits() {
			releaseCommits.countDown();
		}
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		for (;;) {
			try {
				latch.await();
				break;
			} catch (InterruptedException _) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}
}
