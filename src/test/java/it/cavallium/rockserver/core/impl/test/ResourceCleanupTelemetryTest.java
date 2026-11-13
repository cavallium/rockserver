package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Method;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.rocksdb.REntry;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;
import it.cavallium.rockserver.core.impl.rocksdb.Tx;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.rocksdb.RocksIterator;
import org.rocksdb.Transaction;

@Timeout(20)
class ResourceCleanupTelemetryTest {

	@Test
	void deadlineBoundaryEmptyRecoveryAndScanFreshnessAreDistinct() throws Exception {
		var clock = new AtomicLong(1_000_000_000L);
		var telemetry = new Telemetry(clock::get);
		assertTrue(Double.isNaN(telemetry.overdueCount()));
		assertTrue(Double.isNaN(telemetry.scanAgeSeconds()));
		var scan = new Scan(10_000L);
		scan.deadline(10_001L);
		scan.deadline(10_000L);
		scan.deadline(8_500L);
		telemetry.record(scan);
		assertEquals(2.0, telemetry.overdueCount());
		assertEquals(1.5, telemetry.oldestOverdueSeconds());
		clock.addAndGet(3_000_000_000L);
		assertEquals(3.0, telemetry.scanAgeSeconds());
		assertEquals(1.5, telemetry.oldestOverdueSeconds(), "cached deadline sample does not invent live ownership");
		telemetry.record(new Scan(20_000L));
		assertEquals(0.0, telemetry.overdueCount());
		assertEquals(0.0, telemetry.oldestOverdueSeconds());
		assertEquals(0.0, telemetry.scanAgeSeconds());
	}

	@Test
	void scanSaturatesAnOverflowingDeadlineOverrun() throws Exception {
		var telemetry = new Telemetry(System::nanoTime);
		var scan = new Scan(Long.MAX_VALUE);
		scan.deadline(Long.MIN_VALUE);
		telemetry.record(scan);
		assertEquals(1.0, telemetry.overdueCount());
		assertEquals(Long.MAX_VALUE / 1_000.0, telemetry.oldestOverdueSeconds());
	}

	@Test
	void failureCounterSurvivesLaterHealthyScans() throws Exception {
		var registry = new SimpleMeterRegistry();
		try {
			var telemetry = new Telemetry(System::nanoTime);
			telemetry.register(registry, new ArrayList<Meter>(), "test", "iterator");
			telemetry.closeFailed();
			telemetry.record(new Scan(0L));
			telemetry.record(new Scan(1L));
			assertEquals(1.0, registry.get("rockserver.resource.cleanup.failures").functionCounter().count());
			assertEquals(0.0, registry.get("rockserver.resource.overdue.count").gauge().value());
		}
		finally { registry.close(); }
	}

	/**
	 * The cleaner observes against the instant it expired entries at: a deadline that passes while the pass runs was
	 * not attempted, so it is not overdue for that scan, however late the scan ends.
	 */
	@Test
	void overdueIsMeasuredAtTheScanInstantNotWhenTheScanEnds() throws Exception {
		var overdue = RWScheduler.class.getDeclaredMethod("monotonicDeadlineOverdueNanos", long.class, long.class);
		overdue.setAccessible(true);
		long scanInstant = 1_000L;
		assertEquals(-1L, overdue.invoke(null, scanInstant + 1, scanInstant), "expired during the pass");
		assertEquals(0L, overdue.invoke(null, scanInstant, scanInstant));
		assertEquals(400L, overdue.invoke(null, 600L, scanInstant));
		assertEquals(-1L, overdue.invoke(null, Long.MAX_VALUE, scanInstant), "unlimited");
		assertEquals(Long.MAX_VALUE, overdue.invoke(null, Long.MIN_VALUE + 5, scanInstant), "saturated");

		var scan = new Scan(scanInstant);
		scan.deadline(scanInstant + 1);
		var telemetry = new Telemetry(() -> 0L);
		telemetry.record(scan);
		assertEquals(0.0, telemetry.overdueCount());
	}

	@Test
	void rangeOverdueUsesTheBoundClockAndExcludesUnlimitedDeadlines() throws Exception {
		var clock = new AtomicLong(0L);
		var factory = RWScheduler.class.getDeclaredMethod("forTesting", int.class, int.class,
				int.class, int.class, int.class, String.class, LongSupplier.class);
		factory.setAccessible(true);
		var scheduler = (RWScheduler) factory.invoke(null, 1, 1, 1, 8, 8, "cleanup-clock", (LongSupplier) clock::get);
		try {
			clock.set(100L);
			assertEquals(-1L, invoke(scheduler, "monotonicDeadlineOverdueNanos", new Class<?>[] {long.class}, 101L));
			assertEquals(0L, invoke(scheduler, "monotonicDeadlineOverdueNanos", new Class<?>[] {long.class}, 100L));
			assertEquals(-1L, invoke(scheduler, "monotonicDeadlineOverdueNanos", new Class<?>[] {long.class}, Long.MAX_VALUE));
			clock.set(2_000_100L);
			assertEquals(2_000_000L, invoke(scheduler, "monotonicDeadlineOverdueNanos", new Class<?>[] {long.class}, 100L));
		} finally {
			scheduler.disposeNow();
		}
	}

	@Test
	void iteratorFailureIsCountedAfterOwnershipWasRemovedAndLeaseReleased() throws Exception {
		try (var connection = new EmbeddedConnection(null, "iterator-cleanup-telemetry", null)) {
			var db = connection.getInternalDB();
			var iterator = mock(RocksIterator.class);
			var refs = new RocksDBObjects(() -> { throw new IllegalStateException("close failed"); });
			invoke(db, "retainResourceLease");
			iterators(db).put(123L, new REntry<>(iterator, 0L, refs, WorkloadProfile.BATCH));
			invoke(db, "cleanupExpiredIteratorsNow");
			assertEquals(0, db.getOpenIteratorsCount());
			assertEquals(0, db.getPendingOpsCount());
			assertGauge(db, "iterator", "rockserver.resource.overdue.count", 0.0);
			assertEquals(1.0, db.getMetricsRegistry().get("rockserver.resource.cleanup.failures")
					.tag("resource", "iterator").functionCounter().count());
			invoke(db, "cleanupExpiredIteratorsNow");
			verify(iterator, times(1)).close();
			assertEquals(1.0, db.getMetricsRegistry().get("rockserver.resource.cleanup.failures")
					.tag("resource", "iterator").functionCounter().count());
		}
	}

	@Test
	void unexpiredAndUnlimitedIteratorsAreNotCalledLeaksAndExplicitCloseDrainsThem() throws Exception {
		try (var connection = new EmbeddedConnection(null, "iterator-deadline-telemetry", null)) {
			var db = connection.getInternalDB();
			for (long id : new long[] {1L, 2L}) {
				invoke(db, "retainResourceLease");
				iterators(db).put(id, new REntry<>(mock(RocksIterator.class), id == 1L ? Long.MAX_VALUE : null,
						new RocksDBObjects(), WorkloadProfile.BATCH));
			}
			invoke(db, "cleanupExpiredIteratorsNow");
			assertEquals(2, db.getOpenIteratorsCount());
			assertGauge(db, "iterator", "rockserver.resource.overdue.count", 0.0);
			assertEquals(2.0, db.getMetricsRegistry().get("rockserver.resource.leases").gauge().value());
			assertEquals(0.0, db.getMetricsRegistry().get("rockserver.active.native.ops").gauge().value());
			db.closeIterator(1L);
			db.closeIterator(2L);
			assertEquals(0, db.getPendingOpsCount());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void transactionRollbackFailureIsVisibleEvenAfterMapRemoval() throws Exception {
		try (var connection = new EmbeddedConnection(null, "tx-cleanup-telemetry", null)) {
			var db = connection.getInternalDB();
			var tx = mock(Transaction.class);
			when(tx.isOwningHandle()).thenReturn(true);
			doThrow(new org.rocksdb.RocksDBException("rollback failed")).when(tx).rollback();
			var field = EmbeddedDB.class.getDeclaredField("txs");
			field.setAccessible(true);
			var txs = (Map<Long, Tx>) field.get(db);
			invoke(db, "retainResourceLease");
			txs.put(42L, new Tx(tx, false, 0L, new RocksDBObjects(), WorkloadProfile.BATCH));
			invoke(db, "cleanupExpiredTransactionsNow");
			assertEquals(0, db.getOpenTransactionsCount());
			assertEquals(0, db.getPendingOpsCount());
			assertGauge(db, "transaction", "rockserver.resource.overdue.count", 0.0);
			assertEquals(1.0, db.getMetricsRegistry().get("rockserver.resource.cleanup.failures")
					.tag("resource", "transaction").functionCounter().count());
			verify(tx, times(1)).close();
		}
	}

	private static void assertGauge(EmbeddedDB db, String resource, String name, double expected) {
		assertEquals(expected, db.getMetricsRegistry().get(name).tag("resource", resource).gauge().value());
	}
	private static Object invoke(Object target, String name, Class<?>[] types, Object... args) throws Exception {
		Method method = target.getClass().getDeclaredMethod(name, types);
		method.setAccessible(true);
		return method.invoke(target, args);
	}

	private static void invoke(Object target, String name) throws Exception {
		invoke(target, name, new Class<?>[0]);
	}

	@SuppressWarnings("unchecked")
	private static Map<Long, REntry<RocksIterator>> iterators(EmbeddedDB db) throws Exception {
		var field = EmbeddedDB.class.getDeclaredField("its");
		field.setAccessible(true);
		return (Map<Long, REntry<RocksIterator>>) field.get(db);
	}

	private static final class Scan {
		final Object value;
		Scan(long now) throws Exception {
			var type = Class.forName("it.cavallium.rockserver.core.impl.ResourceCleanupTelemetry$Scan");
			var ctor = type.getDeclaredConstructor(long.class);
			ctor.setAccessible(true);
			value = ctor.newInstance(now);
		}
		void deadline(long time) throws Exception { invoke(value, "deadline", new Class<?>[] {long.class}, time); }
	}

	private static final class Telemetry {
		final Object value;
		Telemetry(LongSupplier clock) throws Exception {
			var type = Class.forName("it.cavallium.rockserver.core.impl.ResourceCleanupTelemetry");
			var ctor = type.getDeclaredConstructor(LongSupplier.class);
			ctor.setAccessible(true);
			value = ctor.newInstance(clock);
		}
		void record(Scan scan) throws Exception { invoke(value, "record", new Class<?>[] {scan.value.getClass()}, scan.value); }
		double overdueCount() throws Exception { return (double) invoke(value, "overdueCount", new Class<?>[0]); }
		double oldestOverdueSeconds() throws Exception { return (double) invoke(value, "oldestOverdueSeconds", new Class<?>[0]); }
		double scanAgeSeconds() throws Exception { return (double) invoke(value, "scanAgeSeconds", new Class<?>[0]); }
		void closeFailed() throws Exception { invoke(value, "closeFailed"); }
		void register(SimpleMeterRegistry registry, ArrayList<Meter> meters, String db, String resource) throws Exception {
			invoke(value, "register", new Class<?>[] {io.micrometer.core.instrument.MeterRegistry.class, java.util.List.class, String.class, String.class}, registry, meters, db, resource);
		}
	}

}
