package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R60-3: {@code snapshot.age_seconds} has a finite value on a node without a snapshot. It was NaN, which the InfluxDB
 * registry does not write, so {@code rockserver-snapshot-old} ({@code noDataState: OK}) never fired on a node whose
 * snapshots all fail, the case its summary describes. Without a snapshot (or without the time it was taken) the gauge
 * is the seconds since the node started: no snapshot taken in all that time.
 */
class NodeMonitorTest {

	/** A database without a metrics backend: JMX off, and InfluxDB off as by default. */
	private static final String NO_METRICS_BACKEND = """
			database: {
			  metrics: {
			    jmx: {
			      enabled: false
			    }
			  }
			}
			""";

	/**
	 * R61-8: {@code ALARM write-stopped} and {@code ALARM write-delayed} are raised on a node whose database has no
	 * metrics backend (JMX off or failed, InfluxDB off or failed) too. The monitor read the two reasons from gauges of
	 * the database's composite meter registry, whose gauges answer 0 while the registry has no backend: the alarms the
	 * node "can tell by itself" were never logged there.
	 */
	@Test
	void theWriteStallAlarmsNeedNoMetricsBackend(@TempDir Path temp) throws Exception {
		var config = temp.resolve("rockserver.conf");
		Files.writeString(config, NO_METRICS_BACKEND);
		try (var store = NodeStore.open(temp.resolve("db"), config, "test")) {
			var db = store.db();
			assertFalse(NodeMonitor.writeStopped(db));
			assertFalse(NodeMonitor.writeDelayed(db));
			// The database polls RocksDB once a second and sets the signal again: set and read between two polls.
			assertTrue(holdsRightAfter(() -> db.setStoragePressureForTesting(1, 0), () -> NodeMonitor.writeStopped(db)),
					"write-stopped is not seen without a metrics backend");
			assertTrue(holdsRightAfter(() -> db.setStoragePressureForTesting(0, 4096), () -> NodeMonitor.writeDelayed(db)),
					"write-delayed is not seen without a metrics backend");
			assertTrue(holdsRightAfter(() -> db.setStoragePressureForTesting(0, 4096), () -> !NodeMonitor.writeStopped(db)));
		}
	}

	/** Whether {@code condition} holds right after {@code set}, in one of a few tries (a poll may come in between). */
	private static boolean holdsRightAfter(Runnable set, BooleanSupplier condition) {
		for (int i = 0; i < 20; i++) {
			set.run();
			if (condition.getAsBoolean()) {
				return true;
			}
		}
		return false;
	}

	private static final long STARTED = 1_000_000;
	private static final long DAY_MILLIS = 86_400_000;

	@Test
	void theAgeOfASnapshotIsTheTimeSinceItWasTaken() {
		assertEquals(90, NodeMonitor.snapshotAgeSeconds(STARTED + DAY_MILLIS, true, STARTED + DAY_MILLIS - 90_000,
				STARTED));
		// A clock that went back: never negative.
		assertEquals(0, NodeMonitor.snapshotAgeSeconds(STARTED, true, STARTED + 5_000, STARTED));
	}

	@Test
	void withoutASnapshotTheAgeIsTheTimeSinceTheNodeStarted() {
		assertEquals(2 * 86_400, NodeMonitor.snapshotAgeSeconds(STARTED + 2 * DAY_MILLIS, false, 0, STARTED));
		// A snapshot whose time cannot be read (its directory's modification time) counts the same way.
		assertEquals(86_400, NodeMonitor.snapshotAgeSeconds(STARTED + DAY_MILLIS, true, 0, STARTED));
		assertEquals(0, NodeMonitor.snapshotAgeSeconds(STARTED, false, 0, STARTED));
	}
}
