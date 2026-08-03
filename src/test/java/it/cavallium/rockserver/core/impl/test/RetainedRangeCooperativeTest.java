package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Timeout(60)
class RetainedRangeCooperativeTest {

	@TempDir
	Path tempDir;

	@Test
	void batchCountUsesOneLogicalTaskAndYieldsAfterOneNativeQuantum() throws Exception {
		String databaseName = "retained-count-batch";
		try (var connection = populatedConnection(databaseName, 128, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				database.parallelism.workload.range-quantum-max-items = 8
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			var releaseBlockers = new CountDownLatch(1);
			var blockersStarted = new CountDownLatch(2);
			occupyReadWorkers(scheduler.readExecutor(), 2, blockersStarted, releaseBlockers);
			assertTrue(blockersStarted.await(5, TimeUnit.SECONDS));

			var firstChunk = new CountDownLatch(1);
			var releaseFirstChunk = new CountDownLatch(1);
			var foregroundRan = new CountDownLatch(1);
			var chunks = new AtomicInteger();
			var foregroundBeforeSecondChunk = new AtomicBoolean();
			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			connection.getInternalDB().setRangeCountChunkObserverForTesting(() -> {
				int chunk = chunks.incrementAndGet();
				if (chunk == 1) {
					firstChunk.countDown();
					await(releaseFirstChunk);
				} else if (chunk == 2) {
					foregroundBeforeSecondChunk.set(foregroundRan.getCount() == 0L);
				}
			});

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.FULL_SCAN_AGGREGATE,
					RequestContext.NO_DEADLINE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "full_scan_aggregate")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			try {
				var count = connection.getAsyncApi(RequestContext.batch()).reduceRangeAsync(
						0, columnId, null, null, false, RequestType.entriesCount(), 30_000);
				assertTrue(firstChunk.await(5, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getActiveRangeCursorCount());
				assertEquals(1, connection.getInternalDB().getRetainedRangeSnapshotCount());
				assertEquals(1, connection.getInternalDB().getRetainedRangePermitCount());

				scheduler.executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						System.currentTimeMillis() + 5_000L).execute(foregroundRan::countDown);
				releaseFirstChunk.countDown();
				assertTrue(foregroundRan.await(5, TimeUnit.SECONDS));
				assertEquals(128L, count.get(10, TimeUnit.SECONDS));

				assertTrue(foregroundBeforeSecondChunk.get(),
						"queued LATENCY work must run before a second count quantum");
				assertEquals(1, iteratorOpens.get());
				assertEquals(2L,
						scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore,
						"one retained count plus one foreground task are two logical submissions");
				assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore >= 2.0d, 5_000),
						"contention must redispatch the same count task for a later scheduler quantum");
				assertRetainedResourcesDrained(connection);
			} finally {
				releaseFirstChunk.countDown();
				releaseBlockers.countDown();
				connection.getInternalDB().setRangeCountChunkObserverForTesting(null);
				connection.getInternalDB().setRangeIteratorOpenObserverForTesting(null);
			}
		}
	}

	@Test
	void batchStreamParksAndResumesOneLogicalSchedulerNode() throws Exception {
		String databaseName = "retained-stream-batch";
		try (var connection = populatedConnection(databaseName, 65, """
				database.parallelism.workload.range-quantum-max-items = 16
				database.parallelism.workload.range-quantum-max-bytes = 1MiB
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(RequestContext.batch()).getColumnId("entries");
			var scheduler = connection.getScheduler();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "range_page")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			var iteratorOpens = new AtomicInteger();
			var chunkSizes = Collections.synchronizedList(new ArrayList<Integer>());
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);
			connection.getInternalDB().setRangeReadChunkSizeObserverForTesting(chunkSizes::add);

			var range = Flux.from(connection.getAsyncApi(RequestContext.batch()).getRangeAsync(
					0, columnId, null, null, false, RequestType.allInRange(), 30_000));
			StepVerifier.create(range, 1)
					.assertNext(first -> assertEquals(key(0), first.keys()))
					.thenAwait(Duration.ofMillis(200))
					.then(() -> {
						assertEquals(1L,
								scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
						assertEquals(0, scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks());
						assertEquals(1, connection.getInternalDB().getActiveRangeCursorCount());
						assertEquals(1, connection.getInternalDB().getRetainedRangePermitCount());
						assertTrue(chunkSizes.size() <= 2,
								"one delivery-side prefetch may decode at most one extra chunk");
					})
					.thenRequest(Long.MAX_VALUE)
					.expectNextCount(64)
					.verifyComplete();

			assertEquals(1, iteratorOpens.get());
			assertTrue(chunkSizes.size() >= 5);
			assertTrue(chunkSizes.stream().allMatch(size -> size > 0 && size <= 16));
			assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore > 1.0d, 5_000));
			assertRetainedResourcesDrained(connection);
		}
	}

	@Test
	void analyticalCountUsesTheCooperativeRuntimeAfterSchedulerIntegration() throws Exception {
		assertUncontendedCountProfile(WorkloadProfile.ANALYTICAL, RequestContext.analytical());
	}

	@Test
	void ingestCountUsesTheCooperativeRuntimeAfterSchedulerIntegration() throws Exception {
		assertUncontendedCountProfile(WorkloadProfile.INGEST, RequestContext.ingest());
	}

	private void assertUncontendedCountProfile(WorkloadProfile profile, RequestContext context) throws Exception {
		String databaseName = "retained-count-" + profile.name().toLowerCase(java.util.Locale.ROOT);
		try (var connection = populatedConnection(databaseName, 33, """
				database.parallelism.workload.range-quantum-max-items = 4
				database.parallelism.workload.range-quantum-max-duration = PT1S
				""")) {
			long columnId = connection.getSyncApi(context).getColumnId("entries");
			var scheduler = connection.getScheduler();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			var iteratorOpens = new AtomicInteger();
			connection.getInternalDB().setRangeIteratorOpenObserverForTesting(iteratorOpens::incrementAndGet);

			assertEquals(33L, connection.getAsyncApi(context).reduceRangeAsync(
					0, columnId, null, null, false, RequestType.entriesCount(), 30_000)
					.get(10, TimeUnit.SECONDS));
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
			assertEquals(1, iteratorOpens.get());
			assertRetainedResourcesDrained(connection);
		}
	}

	private EmbeddedConnection populatedConnection(String databaseName,
			int entries,
			String configuration) throws Exception {
		Path config = configuration.isBlank()
				? null
				: Files.writeString(tempDir.resolve(databaseName + ".conf"), configuration);
		var connection = new EmbeddedConnection(tempDir.resolve(databaseName), databaseName, config);
		var api = connection.getSyncApi(RequestContext.batch());
		long columnId = api.createColumn("entries",
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		for (int i = 0; i < entries; i++) {
			api.put(0, columnId, key(i), value(i), RequestType.none());
		}
		return connection;
	}

	private static void occupyReadWorkers(java.util.concurrent.Executor executor,
			int workers,
			CountDownLatch started,
			CountDownLatch release) {
		for (int i = 0; i < workers; i++) {
			executor.execute(() -> {
				started.countDown();
				await(release);
			});
		}
	}

	private static void assertRetainedResourcesDrained(EmbeddedConnection connection) throws Exception {
		assertTrue(awaitCondition(() -> connection.getInternalDB().getActiveRangeCursorCount() == 0
				&& connection.getInternalDB().getRetainedRangeSnapshotCount() == 0
				&& connection.getInternalDB().getRetainedRangePermitCount() == 0
				&& connection.getInternalDB().getRetainedRangeWaiterCount() == 0, 5_000));
	}

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition,
			long timeoutMillis) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		return condition.getAsBoolean();
	}

	private static void await(CountDownLatch latch) {
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
