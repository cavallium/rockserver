package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(45)
class WorkloadGroupsTest {
	@TempDir Path tempDir;
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
	private static final Keys KEY = new Keys(Buf.wrap(new byte[] {1}));
	private static final Buf VALUE = Buf.wrap(new byte[] {42});

	@Test
	void settingsInheritAndRejectInvalidNamesReferencesAndBounds() throws Exception {
		assertTrue(WorkloadSettings.resolveGroups(ConfigParser.parseDefault()).isEmpty());
		var config = ConfigParser.parse(config("valid", ""));
		var settings = WorkloadSettings.resolveGroups(config).get("slow");
		assertEquals(4, settings.readParallelism());
		assertEquals(2, settings.latencyQueueCapacity());
		assertEquals(WorkloadSettings.resolve(config).latencyBurst(), settings.latencyBurst());
		var printed = it.cavallium.rockserver.core.config.ConfigPrinter.stringify(config);
		assertTrue(printed.contains("\"workload-groups\""));
		assertTrue(printed.contains("\"workload-group\": \"slow\""));
		for (String extra : new String[] {
				"database.parallelism.workload-groups: [{name: default}]",
				"database.parallelism.workload-groups: [{name: slow}, {name: slow}]",
				"database.global.fallback-column-options.workload-group: missing",
				"database.parallelism.workload-groups: [{name: slow, read: 2}]",
				"database.parallelism.workload-groups: [{name: slow, workload: {latency-queue-capacity: 0}}]",
				"database.parallelism.workload-groups: [{name: slow, workload: {range-quantum-max-items: 12}}]"}) {
			assertThrows(RocksDBException.class, () -> ConfigParser.parse(config("invalid", extra)), extra);
		}
	}

	@Test
	void slowSaturationCannotConsumeHotAdmissionAndRootOwnsCancellationAndShutdown() throws Exception {
		var release = new CountDownLatch(1);
		RWScheduler slow;
		try (var connection = new EmbeddedConnection(null, "group-isolation", config("isolation", ""))) {
			var api = connection.getSyncApi(RequestContext.batch());
			long slowId = api.createColumn("slow-column", SCHEMA);
			long hotId = api.createColumn("hot-column", SCHEMA);
			slow = connection.getInternalDB().getSchedulerForColumn(slowId);
			var root = connection.getScheduler();
			assertNotSame(root, slow);
			assertSame(root, connection.getInternalDB().getSchedulerForColumn(hotId));
			try {
				blockWorkers(slow, OperationFamily.POINT_LOOKUP, release);
				var async = connection.getAsyncApi(RequestContext.latency(Duration.ofSeconds(10)));
				var cancelled = async.getAsync(0, slowId, KEY, RequestType.current());
				var queued = async.getAsync(0, slowId, KEY, RequestType.current());
				var rejected = async.getAsync(0, slowId, KEY, RequestType.current());
				var failure = assertThrows(java.util.concurrent.ExecutionException.class, () -> rejected.get(5, SECONDS));
				assertInstanceOf(RocksDBException.class, failure.getCause());
				assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						((RocksDBException) failure.getCause()).getErrorUniqueId());
				var compactionTelemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
				root.copyCompactionReadTelemetry(compactionTelemetry, new long[RWScheduler.POOL_TELEMETRY_LENGTH]);
				assertEquals(4, compactionTelemetry[RWScheduler.POOL_TELEMETRY_WORKER_COUNT]);
				assertEquals(4, compactionTelemetry[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS]);
				assertEquals(2, compactionTelemetry[RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.LATENCY.ordinal()]);
				assertTrue(cancelled.cancel(false));
				assertEquals(1, slow.queuedTasks(WorkloadProfile.LATENCY));
				var readTelemetry = new long[RWScheduler.POOL_TELEMETRY_LENGTH];
				slow.copyPoolTelemetry(RWScheduler.Pool.READ, readTelemetry);
				var readSnapshot = slow.poolSnapshot(RWScheduler.Pool.READ);
				long nonRun = readSnapshot.terminalOutcomes() - readSnapshot.outcomes().get(RWScheduler.TerminalOutcome.RUN);
				assertEquals(nonRun, readTelemetry[RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES]);
				assertTrue(nonRun >= 2, "queued cancellation and overload are exact non-RUN outcomes");
				var legacyTelemetry = new long[RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES];
				slow.copyPoolTelemetry(RWScheduler.Pool.READ, legacyTelemetry);
				assertEquals(1, legacyTelemetry[RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.LATENCY.ordinal()]);
				assertNull(async.getAsync(0, hotId, KEY, RequestType.current()).get(5, SECONDS));
				assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
					while (root.poolSnapshot(RWScheduler.Pool.READ).outstandingTasks() != 0) Thread.sleep(10);
				});
				long completed = root.poolSnapshot(RWScheduler.Pool.READ).completedTasks()
						+ slow.poolSnapshot(RWScheduler.Pool.READ).completedTasks();
				for (int targetLength : new int[]{RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES, RWScheduler.POOL_TELEMETRY_LENGTH}) {
					for (int scratchLength : new int[]{RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES, RWScheduler.POOL_TELEMETRY_LENGTH}) {
						var selected = new long[targetLength]; var scratch = new long[scratchLength];
						root.copyCompactionReadTelemetry(selected, scratch);
						assertEquals(4, selected[RWScheduler.POOL_TELEMETRY_ACTIVE_TASKS]);
						assertEquals(1, selected[RWScheduler.POOL_TELEMETRY_QUEUED_BY_PROFILE + WorkloadProfile.LATENCY.ordinal()]);
						assertEquals(completed, selected[RWScheduler.POOL_TELEMETRY_COMPLETED_TASKS]);
						if (targetLength == RWScheduler.POOL_TELEMETRY_LENGTH) {
							assertEquals(scratchLength == RWScheduler.POOL_TELEMETRY_LENGTH ? nonRun : -1,
									selected[RWScheduler.POOL_TELEMETRY_NON_RUN_OUTCOMES],
									"selected legacy group telemetry must not retain the default pool's optional counter");
						}
					}
				}
				assertFalse(queued.isDone());
				release.countDown();
				assertNull(queued.get(5, SECONDS));
				assertFalse(root.isStoragePressure());
				slow.setStoragePressure(true);
				assertFalse(root.isStoragePressure());
				root.dispose();
				for (var pool : slow.instrumentationSnapshot().pools().values()) {
					assertTrue(pool.terminated());
					assertTrue(pool.drainedAndConserved());
				}
			} finally { release.countDown(); }
		}
	}

    @Test
    void realGroupedSamplerKeepsTopologyWhenSelectedPressureMoves() throws Exception {
        var cfg = config("grouped-sampler", "database.parallelism.read=8\n"
                + "database.parallelism.workload-groups=[{name:slow,read:3,write:3,workload:{latency-queue-capacity:32,competing-batch-read-maximum-active:3}}]\n"
                + "database.parallelism.workload.latency-queue-capacity=32\n"
                + "database.global.adaptive-compaction-io=true\n");
        var release = new CountDownLatch(1);
        try (var connection = new EmbeddedConnection(tempDir.resolve("grouped-sampler-db"), "grouped-sampler", cfg)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long slowId = api.createColumn("slow-column", SCHEMA), hotId = api.createColumn("hot-column", SCHEMA);
            for (long id : new long[]{slowId, hotId}) api.put(0, id, KEY, VALUE, RequestType.none());
            api.flush();
            for (int i = 0; i < 64; i++) {
                for (long id : new long[]{slowId, hotId}) {
                    assertEquals(VALUE, api.get(0, id, KEY, RequestType.current()));
                    assertEquals(java.util.List.of(true), api.existsMulti(0, id, java.util.List.of(KEY)));
                }
            }
            var internal = connection.getInternalDB();
            var controllerField = internal.getClass().getDeclaredField("compactionIoController"); controllerField.setAccessible(true);
            var controller = controllerField.get(internal);
            var limiterField = controller.getClass().getDeclaredField("limiter"); limiterField.setAccessible(true);
            var limiter = (org.rocksdb.RateLimiter) limiterField.get(controller);
            var sampleMethod = internal.getClass().getDeclaredMethod("sampleCompactionIo", org.rocksdb.RateLimiter.class);
            sampleMethod.setAccessible(true);
            var executorField = controller.getClass().getDeclaredField("executor"); executorField.setAccessible(true);
            var sampler = (java.util.concurrent.ScheduledExecutorService) executorField.get(controller);
            java.util.concurrent.Callable<it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample> sampleCall =
                    () -> (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) sampleMethod.invoke(internal, limiter);
            var budgetField = controller.getClass().getDeclaredField("budget"); budgetField.setAccessible(true);
            var budget = budgetField.get(controller);
            var previousField = budget.getClass().getDeclaredField("previous"); previousField.setAccessible(true);
            var initial = sampler.submit(() -> (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) previousField.get(budget)).get(5, SECONDS);
            assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
                while (true) {
                    var polled = sampler.submit(() -> (it.cavallium.rockserver.core.impl.CompactionIoBudget.Sample) previousField.get(budget)).get(5, SECONDS);
                    if (polled != null && (initial == null || polled.nanos() > initial.nanos())) {
                        assertEquals(11, polled.readTopologyWorkers()); break;
                    }
                    Thread.sleep(10);
                }
            });
            var root = connection.getScheduler(); var slow = internal.getSchedulerForColumn(slowId);
            try {
                for (var scheduler : new RWScheduler[]{root, slow}) {
                    var executor = scheduler.executor(WorkloadProfile.LATENCY, OperationFamily.POINT_LOOKUP, Long.MAX_VALUE);
                    int workers = scheduler.poolSnapshot(RWScheduler.Pool.READ).workerCount();
                    var started = new CountDownLatch(workers);
                    for (int i = 0; i < workers; i++) executor.execute(() -> {
                        started.countDown();
                        try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    });
                    assertTrue(started.await(5, SECONDS));
                }
                var async = connection.getAsyncApi(RequestContext.latency(Duration.ofSeconds(20)));
                var hot = new java.util.ArrayList<java.util.concurrent.CompletableFuture<Buf>>();
                var cold = new java.util.ArrayList<java.util.concurrent.CompletableFuture<Buf>>();
                for (int i = 0; i < 17; i++) hot.add(async.getAsync(0, hotId, KEY, RequestType.current()));
                for (int i = 0; i < 16; i++) cold.add(async.getAsync(0, slowId, KEY, RequestType.current()));
                assertEquals(17, root.poolSnapshot(RWScheduler.Pool.READ).queuedByProfile().get(WorkloadProfile.LATENCY));
                assertEquals(16, slow.poolSnapshot(RWScheduler.Pool.READ).queuedByProfile().get(WorkloadProfile.LATENCY));
                var first = sampler.submit(sampleCall).get(5, SECONDS);
                assertEquals(11, first.readTopologyWorkers()); assertEquals(8, first.readWorkers());
                assertEquals(8, first.readActive()); assertEquals(17, first.latencyReadQueued());
                assertEquals(-1, first.readFailures()); assertTrue(first.nativePointCompletions() >= 128);
                assertTrue(first.nativeBulkCompletions() >= 128); assertTrue(first.bulkKeys() >= 128);
                assertTrue(hot.removeLast().cancel(false));
                cold.add(async.getAsync(0, slowId, KEY, RequestType.current()));
                assertEquals(16, root.poolSnapshot(RWScheduler.Pool.READ).queuedByProfile().get(WorkloadProfile.LATENCY));
                assertEquals(17, slow.poolSnapshot(RWScheduler.Pool.READ).queuedByProfile().get(WorkloadProfile.LATENCY));
                var second = sampler.submit(sampleCall).get(5, SECONDS);
                assertEquals(11, second.readTopologyWorkers()); assertEquals(3, second.readWorkers());
                assertEquals(3, second.readActive()); assertEquals(17, second.latencyReadQueued());
                release.countDown();
                for (var future : hot) assertEquals(VALUE, future.get(5, SECONDS));
                for (var future : cold) assertEquals(VALUE, future.get(5, SECONDS));
                var progressed = sampler.submit(sampleCall).get(5, SECONDS);
                assertTrue(progressed.nativePointCompletions() > second.nativePointCompletions());
                assertTrue(progressed.readCompletions() > second.readCompletions());
            } finally { release.countDown(); }
        }
    }

	@Test
	@org.junit.jupiter.api.parallel.ResourceLock(org.junit.jupiter.api.parallel.Resources.SYSTEM_PROPERTIES)
	void grpcLegacyAndFastReadsAndStreamedMutationsUseColumnGroups() throws Exception {
		String previous = System.getProperty("rockserver.grpc.fast-get.strategy");
		try {
			for (String strategy : new String[] {"legacy", "automatic"}) {
				System.setProperty("rockserver.grpc.fast-get.strategy", strategy);
				try (var connection = new EmbeddedConnection(null, "grpc-groups-" + strategy, config(strategy, ""));
						var server = new it.cavallium.rockserver.core.server.GrpcServer(connection,
								new java.net.InetSocketAddress("127.0.0.1", 0))) {
					var api = connection.getSyncApi(RequestContext.batch());
					long slowId = api.createColumn("slow-column", SCHEMA);
					long hotId = api.createColumn("hot-column", SCHEMA);
					server.start();
					var channel = io.grpc.ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
					var release = new CountDownLatch(1);
					try {
						var slow = connection.getInternalDB().getSchedulerForColumn(slowId);
						blockWorkers(slow, OperationFamily.POINT_LOOKUP, release);
						blockWorkers(slow, OperationFamily.MUTATION, release);
						var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
								.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY)
								.setWorkloadContractVersion(3).setTimeoutNanos(Duration.ofSeconds(10).toNanos()).build();
						var get = it.cavallium.rockserver.core.common.api.proto.GetRequest.newBuilder()
								.setColumnId(slowId).addKeys(com.google.protobuf.ByteString.copyFrom(new byte[] {1})).setContext(context);
						var stub = it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.newFutureStub(channel);
						var slowGet = stub.get(get.build());
						awaitQueued(slow, WorkloadProfile.LATENCY, 1);
						assertFalse(slowGet.isDone());
						assertFalse(stub.get(get.setColumnId(hotId).build()).get(5, SECONDS).hasValue());
						var data = it.cavallium.rockserver.core.common.api.proto.KV.newBuilder()
								.addKeys(com.google.protobuf.ByteString.copyFrom(new byte[] {1}))
								.setValue(com.google.protobuf.ByteString.copyFrom(new byte[] {42})).build();
						var put = it.cavallium.rockserver.core.common.api.proto.PutRequest.newBuilder()
								.setColumnId(slowId).setData(data).setContext(context);
						var slowPut = stub.put(put.build());
						awaitQueued(slow, WorkloadProfile.LATENCY, 2);
						assertFalse(slowPut.isDone());
						stub.put(put.setColumnId(hotId).build()).get(5, SECONDS);
						var batchContext = context.toBuilder()
								.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
								.setTimeoutNanos(Long.MAX_VALUE).build();
						var reactorStub = it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc.newReactorStub(channel);
						var slowStream = reactorStub.putMulti(putMulti(slowId, batchContext, data)).toFuture();
						awaitQueued(slow, WorkloadProfile.BATCH, 1);
						assertFalse(slowStream.isDone());
						reactorStub.putMulti(putMulti(hotId, batchContext, data)).block(Duration.ofSeconds(5));
						release.countDown();
						slowGet.get(5, SECONDS);
						slowPut.get(5, SECONDS);
						slowStream.get(5, SECONDS);
						assertEquals(VALUE, api.get(0, slowId, KEY, RequestType.current()));
					} finally {
						release.countDown();
						channel.shutdownNow();
						assertTrue(channel.awaitTermination(5, SECONDS));
					}
				}
			}
		} finally {
			if (previous == null) System.clearProperty("rockserver.grpc.fast-get.strategy");
			else System.setProperty("rockserver.grpc.fast-get.strategy", previous);
		}
	}

	private static reactor.core.publisher.Flux<it.cavallium.rockserver.core.common.api.proto.PutMultiRequest> putMulti(
			long columnId, it.cavallium.rockserver.core.common.api.proto.RequestContext context,
			it.cavallium.rockserver.core.common.api.proto.KV data) {
		return reactor.core.publisher.Flux.just(
				it.cavallium.rockserver.core.common.api.proto.PutMultiRequest.newBuilder().setInitialRequest(
						it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest.newBuilder()
								.setColumnId(columnId).setContext(context)).build(),
				it.cavallium.rockserver.core.common.api.proto.PutMultiRequest.newBuilder().setData(data).build());
	}

	private static void blockWorkers(RWScheduler scheduler, OperationFamily family, CountDownLatch release) throws Exception {
		var executor = scheduler.executor(WorkloadProfile.LATENCY, family, Long.MAX_VALUE);
		for (int i = 0; i < 4; i++) {
			var started = new CountDownLatch(1);
			executor.execute(() -> {
				started.countDown();
				try { release.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
			});
			assertTrue(started.await(5, SECONDS));
		}
	}

	private static void awaitQueued(RWScheduler scheduler, WorkloadProfile profile, int expected) throws Exception {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (scheduler.queuedTasks(profile) != expected && System.nanoTime() < deadline) Thread.sleep(1);
		assertEquals(expected, scheduler.queuedTasks(profile));
	}

	@Test
	void columnsTransactionsAndIteratorContinuationsRetainRoutingAcrossReopen() throws Exception {
		Path config = config("reopen", "");
		Path dbPath = tempDir.resolve("database");
		for (int open = 0; open < 2; open++) {
			try (var connection = new EmbeddedConnection(dbPath, "group-reopen", config)) {
				var batch = connection.getSyncApi(RequestContext.batch());
				long slowId = batch.createColumn("slow-column", SCHEMA);
				long hotId = batch.createColumn("hot-column", SCHEMA);
				if (open == 0) {
					long tx = batch.openTransaction(Duration.ofSeconds(5));
					batch.put(tx, slowId, KEY, VALUE, RequestType.none());
					batch.put(tx, hotId, KEY, VALUE, RequestType.none());
					assertTrue(batch.closeTransaction(tx, true));
				}
				assertEquals(VALUE, batch.get(0, slowId, KEY, RequestType.current()));
				assertEquals(VALUE, batch.get(0, hotId, KEY, RequestType.current()));
				long iterator = batch.openIterator(0, slowId, new Keys(), null, false, Duration.ofSeconds(5));
				assertSame(connection.getInternalDB().getSchedulerForColumn(slowId),
						connection.getInternalDB().getSchedulerForIterator(iterator));
				connection.getAsyncApi(RequestContext.batch()).subsequentAsync(iterator, 0, 1, RequestType.multi()).get(5, SECONDS);
				batch.closeIterator(iterator);
				batch.flush();
			}
		}
	}

	private Path config(String name, String extra) throws Exception {
		Path path = tempDir.resolve(name + ".conf");
		Files.writeString(path, """
			database.parallelism.workload-groups: [{
			  name: slow
			  read: 4
			  write: 4
			  workload: { latency-queue-capacity: 2 }
			}]
			database.global.column-options: [{name: slow-column, workload-group: slow}]
			""" + extra);
		return path;
	}
}
