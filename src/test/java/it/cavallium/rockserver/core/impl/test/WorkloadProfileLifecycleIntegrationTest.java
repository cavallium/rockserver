package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.config.WorkloadSettings;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class WorkloadProfileLifecycleIntegrationTest {

	@TempDir
	Path tempDir;

	@Test
	void sevenProfilesRemainBoundedAndDrainAcrossPressureAndRecovery() throws Exception {
		var registry = new SimpleMeterRegistry();
		var settings = WorkloadSettings.resolve(ConfigParser.parse(writeConfig("scheduler")));
		var scheduler = new RWScheduler(settings, "profile-lifecycle", registry, "profile-lifecycle");
		try {
			proveReservationsAndBoundedQueues(scheduler);
			assertSchedulerDrained(scheduler);
			provePressureProgressAndRecovery(scheduler);
			assertSchedulerDrained(scheduler);
		} finally {
			scheduler.setStoragePressure(false);
			scheduler.dispose();
			registry.close();
		}

		var stopped = scheduler.instrumentationSnapshot();
		for (var pool : RWScheduler.Pool.values()) {
			var snapshot = stopped.pools().get(pool);
			assertEquals(0, snapshot.queuedTasks(), pool + " queue leaked after shutdown");
			assertEquals(0, snapshot.activeTasks(), pool + " work leaked after shutdown");
			assertTrue(snapshot.terminated(), pool + " did not terminate");
		}
	}

	@Test
	void completedEmbeddedLifecycleDrainsEveryLogicalResource() throws Exception {
		Path config = writeConfig("embedded");
		try (var connection = new EmbeddedConnection(tempDir.resolve("embedded-db"),
				"embedded-profile-lifecycle", config)) {
			var batch = connection.getSyncApi(RequestContext.batch());
			var ingest = connection.getSyncApi(RequestContext.ingest());
			var latency = connection.getSyncApi(RequestContext.latency(java.time.Duration.ofSeconds(5)));
			var analytical = connection.getSyncApi(RequestContext.analytical());

			long columnId = batch.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			batch.cdcCreate("lifecycle", null, null, null, java.util.OptionalLong.empty());
			ingest.put(0, columnId, key(1), value(1), RequestType.none());
			assertEquals(value(1), latency.get(0, columnId, key(1), RequestType.current()));
			assertEquals(1L, analytical.reduceRange(0,
					columnId,
					null,
					null,
					false,
					RequestType.entriesCount()));

			long transactionId = batch.openTransaction( java.time.Duration.ofMillis(5_000));
			batch.closeTransaction(transactionId, false);
			long iteratorId = batch.openIterator(0, columnId, new Keys(), null, false, java.time.Duration.ofMillis( 5_000));
			batch.closeIterator(iteratorId);

			List<CDCEvent> events;
			try (var poll = batch.cdcPoll("lifecycle", null, 16)) {
				events = poll.toList();
			}
			if (!events.isEmpty()) {
				batch.cdcCommit("lifecycle", events.getLast().seq());
			}
			batch.cdcDelete("lifecycle");
			batch.flush();

			assertTrue(awaitCondition(() -> embeddedResourcesDrained(connection)),
					"embedded operations did not release every logical resource");
			assertSchedulerDrained(connection.getScheduler());
		}
	}

	private static void proveReservationsAndBoundedQueues(RWScheduler scheduler) throws Exception {
		var batchStarted = new CountDownLatch(2);
		var releaseBatch = new CountDownLatch(1);
		var analyticalStarted = new CountDownLatch(1);
		var releaseAnalytical = new CountDownLatch(1);
		var reservedDone = new CountDownLatch(3);
		var batchBacklogDone = new CountDownLatch(2);
		var order = Collections.synchronizedList(new ArrayList<String>());
		var batch = scheduler.executor(RequestContext.batch(), OperationFamily.RANGE_PAGE);
		try {
			for (int i = 0; i < 2; i++) {
				batch.execute(() -> {
					batchStarted.countDown();
					awaitUninterruptibly(releaseBatch);
				});
			}
			assertTrue(batchStarted.await(5, SECONDS));

			scheduler.executor(RequestContext.analytical(), OperationFamily.FULL_SCAN_AGGREGATE)
					.execute(() -> {
						analyticalStarted.countDown();
						awaitUninterruptibly(releaseAnalytical);
					});
			assertTrue(analyticalStarted.await(5, SECONDS));

			batch.execute(() -> record("borrowed-batch", order, batchBacklogDone));
			batch.execute(batchBacklogDone::countDown);
			assertEquals(2,
					scheduler.poolSnapshot(RWScheduler.Pool.READ)
							.queuedByProfile()
							.get(WorkloadProfile.BATCH));
			var overload = assertThrows(RocksDBException.class, () -> batch.execute(() -> {}));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
					overload.getErrorUniqueId());

			scheduler.executor(RequestContext.latency(java.time.Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("latency", order, reservedDone));
			scheduler.executor(RequestContext.ingest(), OperationFamily.POINT_LOOKUP)
					.execute(() -> record("ingest", order, reservedDone));
			scheduler.executor(WorkloadProfile.CDC,
					OperationFamily.WAL_PAGE,
					Long.MAX_VALUE)
					.execute(() -> record("cdc", order, reservedDone));

			releaseAnalytical.countDown();
			assertTrue(reservedDone.await(5, SECONDS),
					"LATENCY, INGEST, and CDC reservations did not recover borrowed capacity");
			List<String> firstThree;
			synchronized (order) {
				firstThree = List.copyOf(order.subList(0, 3));
			}
			assertEquals(new HashSet<>(List.of("latency", "ingest", "cdc")),
					new HashSet<>(firstThree),
					"reserved profiles must run before queued work borrows the reclaimed slot");
		} finally {
			releaseAnalytical.countDown();
			releaseBatch.countDown();
		}
		assertTrue(batchBacklogDone.await(5, SECONDS));
	}

	private static void provePressureProgressAndRecovery(RWScheduler scheduler) throws Exception {
		var firstBatchStarted = new CountDownLatch(1);
		var releaseFirstBatch = new CountDownLatch(1);
		var secondBatchStarted = new CountDownLatch(1);
		var releaseSecondBatch = new CountDownLatch(1);
		var secondBatchFinished = new CountDownLatch(1);
		var recoveredBatchDone = new CountDownLatch(1);
		var dataProgress = new CountDownLatch(4);
		var controlDone = new CountDownLatch(1);
		var physicalDone = new CountDownLatch(1);
		var batch = scheduler.executor(RequestContext.batch(), OperationFamily.MUTATION);
		try {
			scheduler.setStoragePressure(true);
			scheduler.executor(WorkloadProfile.PHYSICAL_MAINTENANCE,
					OperationFamily.FLUSH,
					Long.MAX_VALUE)
					.execute(physicalDone::countDown);
			batch.execute(() -> {
				firstBatchStarted.countDown();
				awaitUninterruptibly(releaseFirstBatch);
			});
			assertTrue(firstBatchStarted.await(5, SECONDS));
			batch.execute(() -> {
				secondBatchStarted.countDown();
				awaitUninterruptibly(releaseSecondBatch);
				secondBatchFinished.countDown();
			});
			batch.execute(recoveredBatchDone::countDown);

			scheduler.executor(RequestContext.latency(java.time.Duration.ofSeconds(30)), OperationFamily.POINT_LOOKUP)
					.execute(dataProgress::countDown);
			scheduler.executor(RequestContext.ingest(), OperationFamily.MUTATION)
					.execute(dataProgress::countDown);
			scheduler.executor(WorkloadProfile.CDC,
					OperationFamily.WAL_PAGE,
					Long.MAX_VALUE)
					.execute(dataProgress::countDown);
			scheduler.executor(RequestContext.analytical(), OperationFamily.FULL_SCAN_AGGREGATE)
					.execute(dataProgress::countDown);
			scheduler.executor(WorkloadProfile.CONTROL,
					OperationFamily.CONTROL,
					Long.MAX_VALUE)
					.execute(controlDone::countDown);

			assertTrue(dataProgress.await(5, SECONDS),
					"LATENCY, INGEST, CDC, and ANALYTICAL must progress during pressure");
			assertTrue(controlDone.await(2, SECONDS),
					"CONTROL must complete independently of pressured data and physical work");
			assertFalse(physicalDone.await(100, MILLISECONDS),
					"physical maintenance must remain parked during storage pressure");
			assertEquals(1, scheduler.activeTasks(WorkloadProfile.BATCH),
					"pressure must cap active BATCH work");

			releaseFirstBatch.countDown();
			assertTrue(secondBatchStarted.await(5, SECONDS),
					"BATCH must retain minimal progress while pressure remains active");
			assertTrue(scheduler.admissionSnapshot().storagePressure());
			assertFalse(physicalDone.await(100, MILLISECONDS));

			releaseSecondBatch.countDown();
			assertTrue(secondBatchFinished.await(5, SECONDS));
			scheduler.setStoragePressure(false);
			assertTrue(recoveredBatchDone.await(5, SECONDS),
					"queued BATCH work did not recover after pressure cleared");
			assertTrue(physicalDone.await(5, SECONDS),
					"physical maintenance did not resume after pressure cleared");
			assertFalse(scheduler.admissionSnapshot().storagePressure());
		} finally {
			releaseFirstBatch.countDown();
			releaseSecondBatch.countDown();
			scheduler.setStoragePressure(false);
		}
	}

	private Path writeConfig(String name) throws Exception {
		Path config = tempDir.resolve(name + ".conf");
		Files.writeString(config, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.latency-queue-capacity = 2
				database.parallelism.workload.ingest-queue-capacity = 2
				database.parallelism.workload.cdc-queue-capacity = 2
				database.parallelism.workload.analytical-queue-capacity = 2
				database.parallelism.workload.batch-queue-capacity = 2
				database.parallelism.workload.control-queue-capacity = 2
				database.parallelism.workload.physical-maintenance-queue-capacity = 2
				database.parallelism.workload.control-threads = 1
				database.parallelism.workload.physical-concurrency = 1
				database.parallelism.workload.analytical-active-limit = 1
				database.parallelism.workload.competing-batch-read-maximum-active = 3
				database.parallelism.workload.competing-batch-write-maximum-active = 3
				database.parallelism.workload.pressured-batch-maximum-active = 1
				database.parallelism.workload.pressured-batch-interval = PT0.2S
				database.global.enable-fast-get = false
				database.global.ingest-behind = false
				database.global.optimistic = false
				""");
		return config;
	}

	private static boolean embeddedResourcesDrained(EmbeddedConnection connection) {
		var db = connection.getInternalDB();
		return db.getPendingOpsCount() == 0L
				&& db.getOpenTransactionsCount() == 0
				&& db.getOpenIteratorsCount() == 0
				&& db.getActiveRangeCursorCount() == 0
				&& db.getRetainedRangeSnapshotCount() == 0
				&& db.getRetainedRangePermitCount() == 0
				&& db.getRetainedRangeWaiterCount() == 0
				&& db.getActiveCdcPollCursorCount() == 0;
	}

	private static void assertSchedulerDrained(RWScheduler scheduler) throws InterruptedException {
		assertTrue(awaitCondition(() -> {
			var snapshot = scheduler.instrumentationSnapshot();
			return snapshot.pools().values().stream()
					.allMatch(pool -> pool.queuedTasks() == 0 && pool.activeTasks() == 0);
		}), "scheduler queues or active work did not drain");
		for (var profile : WorkloadProfile.values()) {
			assertEquals(0, scheduler.queuedTasks(profile), profile + " queue did not drain");
			assertEquals(0, scheduler.activeTasks(profile), profile + " work did not drain");
		}
	}

	private static boolean awaitCondition(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		do {
			if (condition.getAsBoolean()) {
				return true;
			}
			Thread.sleep(5);
		} while (System.nanoTime() < deadline);
		return condition.getAsBoolean();
	}

	private static void record(String value, List<String> order, CountDownLatch completed) {
		order.add(value);
		completed.countDown();
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Buf value(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}
}
