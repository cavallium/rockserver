package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class ExistsMultiCooperativeTest {

	@TempDir
	Path tempDir;

	@Test
	void batchMultiCallRequestIsOneLogicalTaskAndYieldsBetweenNativeCalls() throws Exception {
		String databaseName = "exists-cooperative-batch";
		try (var connection = openConnection(databaseName, """
				database.parallelism.read = 3
				database.parallelism.write = 3
				""")) {
			var api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = integerKeys(4_097);
			api.put(0, columnId, keys.get(0), value(0), RequestType.none());
			api.put(0, columnId, keys.get(2_048), value(2_048), RequestType.none());
			api.put(0, columnId, keys.getLast(), value(4_096), RequestType.none());

			var scheduler = connection.getScheduler();
			var releaseBlockers = new CountDownLatch(1);
			var blockersStarted = new CountDownLatch(2);
			occupyReadWorkers(scheduler.readExecutor(), 2, blockersStarted, releaseBlockers);
			assertTrue(blockersStarted.await(5, TimeUnit.SECONDS));

			var firstNativeCall = new CountDownLatch(1);
			var releaseFirstNativeCall = new CountDownLatch(1);
			var foregroundRan = new CountDownLatch(1);
			var nativeCalls = new AtomicInteger();
			var foregroundBeforeSecondCall = new AtomicBoolean();
			var snapshotAcquisitions = new AtomicInteger();
			var arenaDepth = new AtomicInteger();
			var arenaTransitions = Collections.synchronizedList(new ArrayList<Boolean>());
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);
			connection.getInternalDB().setExistsMultiArenaObserverForTesting(open -> {
				arenaTransitions.add(open);
				if (open) {
					assertEquals(1, arenaDepth.incrementAndGet(),
							"thread-confined native arenas must never overlap across yields");
				} else {
					assertEquals(0, arenaDepth.decrementAndGet());
				}
			});
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				int call = nativeCalls.incrementAndGet();
				if (call == 1) {
					firstNativeCall.countDown();
					await(releaseFirstNativeCall);
				} else if (call == 2) {
					foregroundBeforeSecondCall.set(foregroundRan.getCount() == 0L);
				}
			});

			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.BOUNDED_FAN_OUT,
					RequestContext.NO_DEADLINE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "bounded_fan_out")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();
			try {
				var request = connection.getAsyncApi(RequestContext.batch())
						.existsMultiAsync(0, columnId, keys, 30_000);
				assertTrue(firstNativeCall.await(5, TimeUnit.SECONDS));
				assertEquals(1, connection.getInternalDB().getActiveExistsMultiRequestCount());

				scheduler.executor(WorkloadProfile.LATENCY,
						OperationFamily.POINT_LOOKUP,
						System.currentTimeMillis() + 5_000L).execute(foregroundRan::countDown);
				releaseFirstNativeCall.countDown();
				assertTrue(foregroundRan.await(5, TimeUnit.SECONDS));
				var result = request.get(10, TimeUnit.SECONDS);

				assertEquals(keys.size(), result.size());
				assertTrue(result.get(0));
				assertTrue(result.get(2_048));
				assertTrue(result.getLast());
				assertFalse(result.get(1));
				assertEquals(2, nativeCalls.get());
				assertEquals(1, snapshotAcquisitions.get());
				assertTrue(foregroundBeforeSecondCall.get(),
						"foreground work must run after at most one indivisible MultiGet");
				assertEquals(List.of(true, false, true, false), List.copyOf(arenaTransitions));
				assertEquals(0, arenaDepth.get());
				assertEquals(2L,
						scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore,
						"one split existsMulti plus one competitor are two logical tasks");
				assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore >= 2.0d, 5_000));
				assertExistsResourcesDrained(connection);
			} finally {
				releaseFirstNativeCall.countDown();
				releaseBlockers.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
				connection.getInternalDB().setExistsMultiSnapshotObserverForTesting(null);
				connection.getInternalDB().setExistsMultiArenaObserverForTesting(null);
			}
		}
	}

	@Test
	void batchByteSplitRunsAllNativeCallsInOneUncontendedDispatch() throws Exception {
		String databaseName = "exists-cooperative-bytes";
		try (var connection = openConnection(databaseName, "")) {
			var api = connection.getSyncApi(RequestContext.batch());
			final int keyBytes = 1_024;
			final int keyCount = 2_050;
			long columnId = api.createColumn("entries",
					ColumnSchema.of(IntList.of(keyBytes), ObjectList.of(), true));
			var keys = sizedKeys(keyCount, keyBytes);
			var nativeCalls = new AtomicInteger();
			var snapshotAcquisitions = new AtomicInteger();
			var arenaDepth = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeCalls::incrementAndGet);
			connection.getInternalDB()
					.setExistsMultiSnapshotObserverForTesting(snapshotAcquisitions::incrementAndGet);
			connection.getInternalDB().setExistsMultiArenaObserverForTesting(open -> {
				if (open) {
					assertEquals(1, arenaDepth.incrementAndGet());
				} else {
					assertEquals(0, arenaDepth.decrementAndGet());
				}
			});

			var scheduler = connection.getScheduler();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.BOUNDED_FAN_OUT,
					RequestContext.NO_DEADLINE);
			var quantumCounter = connection.getInternalDB().getMetricsRegistry()
					.get("rockserver.workload.quantums")
					.tags("database", databaseName,
							"resource", "read",
							"profile", "batch",
							"operation", "bounded_fan_out")
					.counter();
			double quantumsBefore = quantumCounter.count();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();

			var result = connection.getAsyncApi(RequestContext.batch())
					.existsMultiAsync(0, columnId, keys, 30_000)
					.get(10, TimeUnit.SECONDS);
			assertEquals(keyCount, result.size());
			assertTrue(result.stream().noneMatch(Boolean.TRUE::equals));
			assertEquals(2, nativeCalls.get());
			assertEquals(1, snapshotAcquisitions.get());
			assertEquals(0, arenaDepth.get());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
			assertTrue(awaitCondition(() -> quantumCounter.count() - quantumsBefore == 1.0d, 5_000),
					"uncontended native calls must stay inside the first scheduler dispatch");
			assertExistsResourcesDrained(connection);
		}
	}

	@Test
	void latencyRequestKeepsTheOrdinarySingleCallPath() throws Exception {
		String databaseName = "exists-latency-ordinary";
		try (var connection = openConnection(databaseName, "")) {
			var batch = connection.getSyncApi(RequestContext.batch());
			final int keyBytes = 8;
			final int keyCount = 256;
			long columnId = batch.createColumn("entries",
					ColumnSchema.of(IntList.of(keyBytes), ObjectList.of(), true));
			var keys = sizedKeys(keyCount, keyBytes);
			var nativeCalls = new AtomicInteger();
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(nativeCalls::incrementAndGet);
			var scheduler = connection.getScheduler();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();

			var result = connection.getAsyncApi(RequestContext.latency(java.time.Duration.ofSeconds(30)))
					.existsMultiAsync(0, columnId, keys, 30_000)
					.get(10, TimeUnit.SECONDS);
			assertEquals(keyCount, result.size());
			assertEquals(1, nativeCalls.get());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore,
					"an admitted LATENCY fan-out remains one ordinary scheduler task");
			assertExistsResourcesDrained(connection);
		}
	}

	@Test
	void analyticalMultiCallUsesTheCooperativeRuntimeAfterSchedulerIntegration() throws Exception {
		assertCooperativeProfile(WorkloadProfile.ANALYTICAL, RequestContext.analytical());
	}

	@Test
	void ingestMultiCallUsesTheCooperativeRuntimeAfterSchedulerIntegration() throws Exception {
		assertCooperativeProfile(WorkloadProfile.INGEST, RequestContext.ingest());
	}

	private void assertCooperativeProfile(WorkloadProfile profile, RequestContext context) throws Exception {
		String databaseName = "exists-" + profile.name().toLowerCase(java.util.Locale.ROOT);
		try (var connection = openConnection(databaseName, "")) {
			var batch = connection.getSyncApi(RequestContext.batch());
			long columnId = batch.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = integerKeys(4_097);
			var scheduler = connection.getScheduler();
			long acceptedBefore = scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks();

			var result = connection.getAsyncApi(context).existsMultiAsync(0, columnId, keys, 30_000)
					.get(10, TimeUnit.SECONDS);
			assertEquals(keys.size(), result.size());
			assertEquals(1L,
					scheduler.poolSnapshot(RWScheduler.Pool.READ).acceptedTasks() - acceptedBefore);
			assertExistsResourcesDrained(connection);
		}
	}

	private EmbeddedConnection openConnection(String databaseName, String configuration) throws Exception {
		Path config = configuration.isBlank()
				? null
				: Files.writeString(tempDir.resolve(databaseName + ".conf"), configuration);
		return new EmbeddedConnection(tempDir.resolve(databaseName), databaseName, config);
	}

	private static List<Keys> integerKeys(int count) {
		var keys = new ArrayList<Keys>(count);
		for (int i = 0; i < count; i++) {
			keys.add(key(i));
		}
		return List.copyOf(keys);
	}

	private static List<Keys> sizedKeys(int count, int keyBytes) {
		var keys = new ArrayList<Keys>(count);
		for (int i = 0; i < count; i++) {
			var bytes = new byte[keyBytes];
			ByteBuffer.wrap(bytes).putInt(i);
			keys.add(new Keys(Buf.wrap(bytes)));
		}
		return List.copyOf(keys);
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

	private static void assertExistsResourcesDrained(EmbeddedConnection connection) throws Exception {
		assertTrue(awaitCondition(() -> connection.getInternalDB().getActiveExistsMultiRequestCount() == 0
				&& connection.getInternalDB().getPendingOpsCount() == 0, 5_000));
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
