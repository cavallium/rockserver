package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * R60-3: {@code snapshot.age_seconds} has a finite value on a node without a snapshot. It was NaN, which the InfluxDB
 * registry does not write, so {@code rockserver-snapshot-old} ({@code noDataState: OK}) never fired on a node whose
 * snapshots all fail, the case its summary describes. Without a snapshot (or without the time it was taken) the gauge
 * is the seconds since the node started: no snapshot taken in all that time.
 */
class NodeMonitorTest {

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
