package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.impl.StoragePressureSignal;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.util.SizeUnit;

@Timeout(30)
class EmbeddedStoragePressureTest {

	private static final long PENDING_COMPACTION_THRESHOLD = 64L * SizeUnit.GB;
	private static final ColumnSchema TEST_SCHEMA = ColumnSchema.of(
			IntList.of(Integer.BYTES),
			ObjectList.of(),
			true);

	@TempDir
	Path tempDir;

	@Test
	void productionStallDebtIsNotSummedAgainstOneColumnLimit() {
		long senderIndexPending = 55_348_529_560L;
		long messagesV2Pending = 27_874_362_756L;
		long overlayPending = 11_311_192_896L;
		assertTrue(BigInteger.valueOf(senderIndexPending)
				.add(BigInteger.valueOf(messagesV2Pending))
				.add(BigInteger.valueOf(overlayPending))
				.compareTo(BigInteger.valueOf(PENDING_COMPACTION_THRESHOLD)) > 0,
				"fixture must reproduce the old aggregate false positive");

		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, senderIndexPending, PENDING_COMPACTION_THRESHOLD);
		signal.observeColumn(2L, messagesV2Pending, PENDING_COMPACTION_THRESHOLD);
		signal.observeColumn(3L, overlayPending, PENDING_COMPACTION_THRESHOLD);

		assertFalse(signal.pressured(),
				"pending-compaction limits are per column family, not database-wide sums");
	}

	@Test
	void oneColumnAtOrAboveItsLimitActivatesPressure() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(7L, PENDING_COMPACTION_THRESHOLD, PENDING_COMPACTION_THRESHOLD);

		assertTrue(signal.pressured());
		assertTrue(signal.hasReason(StoragePressureSignal.REASON_PENDING_COMPACTION));
		assertEquals(7L, signal.triggeringColumnId());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionBytes());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void writeStopActivatesPressureWithoutCompactionDebt() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 0L);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_WRITE_STOPPED, signal.reasonMask());
	}

	@Test
	void unsignedNativePropertyOverflowFailsClosed() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(9L, Long.MIN_VALUE, PENDING_COMPACTION_THRESHOLD);

		assertTrue(signal.pressured());
		assertEquals(Long.MIN_VALUE, signal.maximumPendingCompactionBytes());
	}

	@Test
	void actualDelayedWriteRateIsAuthoritativeWithoutCompactionDebt() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 1L);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_DELAYED_WRITE, signal.reasonMask());
		assertEquals(1L, signal.actualDelayedWriteRate());
	}

	@Test
	void configuredDisabledLimitsDoNotCreateProactivePressure() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, Long.MIN_VALUE, 0L);
		signal.observeColumn(2L, Long.MIN_VALUE, Long.MAX_VALUE);

		assertFalse(signal.pressured());
	}

	@Test
	void explicitOverrideStillAppliesWhenConfiguredSlowdownIsDisabled() {
		var signal = new StoragePressureSignal(PENDING_COMPACTION_THRESHOLD);
		signal.reset(0L, 0L);
		signal.observeColumn(11L, PENDING_COMPACTION_THRESHOLD, Long.MAX_VALUE);

		assertTrue(signal.pressured());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.pendingCompactionLimitOverride());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void invalidEffectiveLimitFailsClosed() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, 0L, Long.MIN_VALUE);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_SIGNAL_FAILURE, signal.reasonMask());
	}

	@Test
	void resetClearsEveryPreviousReasonAndTrigger() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 5L);
		signal.observeColumn(23L, PENDING_COMPACTION_THRESHOLD, PENDING_COMPACTION_THRESHOLD);
		signal.markSignalFailure();
		assertTrue(signal.pressured());

		signal.reset(0L, 0L);

		assertFalse(signal.pressured());
		assertEquals(0, signal.reasonMask());
		assertEquals(-1L, signal.triggeringColumnId());
		assertEquals(0L, signal.triggeringPendingCompactionBytes());
		assertEquals(0L, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void signalFailureComposesWithAuthoritativeReasons() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 2L);
		signal.markSignalFailure();

		assertEquals(StoragePressureSignal.REASON_WRITE_STOPPED
					| StoragePressureSignal.REASON_DELAYED_WRITE
					| StoragePressureSignal.REASON_SIGNAL_FAILURE,
				signal.reasonMask());
	}

	@Test
	void runtimeColumnCreateAndDeleteUpdateThePolledSnapshotExactlyOnce() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("column-lifecycle"),
				"pressure-column-lifecycle",
				null)) {
			var db = connection.getInternalDB();
			var api = connection.getSyncApi(RequestContext.batch());
			int initialColumns = db.getStoragePressureColumnCountForTesting();
			assertTrue(initialColumns >= 4, "default and internal columns must be polled after initialization");

			long columnId = api.createColumn("runtime-pressure-column", TEST_SCHEMA);
			assertEquals(initialColumns + 1, db.getStoragePressureColumnCountForTesting());
			assertTrue(db.hasStoragePressureColumnForTesting(columnId));

			assertEquals(columnId, api.createColumn("runtime-pressure-column", TEST_SCHEMA));
			assertEquals(initialColumns + 1, db.getStoragePressureColumnCountForTesting(),
					"idempotent logical creation must not duplicate the pressure entry");

			api.deleteColumn(columnId);
			assertEquals(initialColumns, db.getStoragePressureColumnCountForTesting());
			assertFalse(db.hasStoragePressureColumnForTesting(columnId));
		}
	}

	@Test
	void configuredDisabledSlowdownPublishesDisabledEffectiveLimit() throws Exception {
		Path config = tempDir.resolve("disabled-slowdown.conf");
		Files.writeString(config, "database.global.disable-write-slowdown = true\n");
		try (var connection = new EmbeddedConnection(tempDir.resolve("disabled-slowdown"),
				"pressure-disabled-slowdown",
				config)) {
			var db = connection.getInternalDB();
			long columnId = connection.getSyncApi(RequestContext.batch())
					.createColumn("disabled-pressure-column", TEST_SCHEMA);

			assertEquals(Long.MAX_VALUE, db.getStoragePressureColumnLimitForTesting(columnId));
			db.refreshStoragePressureForTesting();
			assertFalse(connection.getScheduler().isStoragePressure());
		}
	}

	@Test
	void deleteRacingACapturedPollSnapshotNeverUsesTheRetiredHandle() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("delete-race"),
				"pressure-delete-race",
				null)) {
			var db = connection.getInternalDB();
			var api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn("delete-race-column", TEST_SCHEMA);
			var pollAdmitted = new CountDownLatch(1);
			var releasePoll = new CountDownLatch(1);
			db.setStoragePressurePollAdmittedObserverForTesting(() -> {
				pollAdmitted.countDown();
				awaitUninterruptibly(releasePoll);
			});
			CompletableFuture<Void> poll = CompletableFuture.runAsync(db::refreshStoragePressureForTesting);
			try {
				assertTrue(pollAdmitted.await(5, TimeUnit.SECONDS));
				CompletableFuture.runAsync(() -> api.deleteColumn(columnId)).get(5, TimeUnit.SECONDS);
				assertFalse(db.hasStoragePressureColumnForTesting(columnId));
			} finally {
				db.setStoragePressurePollAdmittedObserverForTesting(null);
				releasePoll.countDown();
			}
			poll.get(5, TimeUnit.SECONDS);
			assertFalse(connection.getScheduler().isStoragePressure());
		}
	}

	@Test
	void failureMetricsAndSchedulerStateRecoverOnTheNextSuccessfulPoll() throws Exception {
		var exported = new SimpleMeterRegistry();
		try (var connection = new EmbeddedConnection(tempDir.resolve("failure-recovery"),
				"pressure-failure-recovery",
				null)) {
			var db = connection.getInternalDB();
			((CompositeMeterRegistry) db.getMetricsRegistry()).add(exported);

			db.failStoragePressureRefreshForTesting(new IllegalStateException("synthetic property failure"));
			assertTrue(connection.getScheduler().isStoragePressure());
			assertEquals(1.0d, reasonGauge(exported, "signal_failure"));
			var failureCounter = exported.find("rockserver.workload.storage.pressure.refresh.failures")
					.tag("database", "pressure-failure-recovery")
					.counter();
			assertNotNull(failureCounter);
			assertEquals(1.0d, failureCounter.count());

			db.refreshStoragePressureForTesting();
			assertFalse(connection.getScheduler().isStoragePressure());
			assertEquals(0.0d, reasonGauge(exported, "signal_failure"));
			assertEquals(4, exported.find("rockserver.workload.storage.pressure.reason").gauges().size());
			var delayedRateGauge = exported
					.find("rockserver.workload.storage.pressure.actual.delayed.write.rate")
					.gauge();
			assertNotNull(delayedRateGauge);
			assertEquals("bytes", delayedRateGauge.getId().getBaseUnit());
		} finally {
			exported.close();
		}
	}

	@Test
	void closeWaitsForAnAdmittedPressurePollBeforeClosingNativeHandles() throws Exception {
		var connection = new EmbeddedConnection(tempDir.resolve("close-race"),
				"pressure-close-race",
				null);
		var db = connection.getInternalDB();
		var pollAdmitted = new CountDownLatch(1);
		var releasePoll = new CountDownLatch(1);
		var closeStarted = new CountDownLatch(1);
		db.setStoragePressurePollAdmittedObserverForTesting(() -> {
			pollAdmitted.countDown();
			awaitUninterruptibly(releasePoll);
		});

		CompletableFuture<Void> poll = CompletableFuture.runAsync(db::refreshStoragePressureForTesting);
		CompletableFuture<Void> close = null;
		try {
			assertTrue(pollAdmitted.await(5, TimeUnit.SECONDS));
			close = CompletableFuture.runAsync(() -> {
				closeStarted.countDown();
				try {
					connection.closeTesting();
				} catch (Exception failure) {
					throw new RuntimeException(failure);
				}
			});
			assertTrue(closeStarted.await(5, TimeUnit.SECONDS));
			assertTrue(awaitCondition(() -> !db.isAcceptingOperationsForTesting()),
					"close did not close operation admission");
			assertFalse(close.isDone(), "close must retain DB/CF handles while the native poll owns an operation");

			releasePoll.countDown();
			poll.get(5, TimeUnit.SECONDS);
			close.get(5, TimeUnit.SECONDS);
		} finally {
			db.setStoragePressurePollAdmittedObserverForTesting(null);
			releasePoll.countDown();
			if (close != null) {
				try {
					close.get(5, TimeUnit.SECONDS);
				} catch (Exception ignored) {
					// Retry the idempotent test close below to avoid leaking native state.
				}
			}
			if (close == null || close.isCompletedExceptionally()) {
				connection.closeTesting();
			}
		}
	}

	private static double reasonGauge(SimpleMeterRegistry registry, String reason) {
		var gauge = registry.find("rockserver.workload.storage.pressure.reason")
				.tag("database", "pressure-failure-recovery")
				.tag("reason", reason)
				.gauge();
		assertNotNull(gauge);
		return gauge.value();
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

	private static boolean awaitCondition(java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (condition.getAsBoolean()) {
				return true;
			}
			Thread.sleep(5L);
		} while (System.nanoTime() < deadline);
		return condition.getAsBoolean();
	}
}
