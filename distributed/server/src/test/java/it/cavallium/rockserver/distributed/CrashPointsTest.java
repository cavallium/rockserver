package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.distributed.CrashPoints.Point;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** The arming rules of {@link CrashPoints}, with the halt replaced by a recorder. */
class CrashPointsTest {

	private final List<Integer> halts = new ArrayList<>();
	private IntConsumer realHalt;

	@BeforeEach
	void setUp() {
		realHalt = CrashPoints.halt;
		CrashPoints.halt = halts::add;
	}

	@AfterEach
	void tearDown() {
		CrashPoints.disarm();
		CrashPoints.halt = realHalt;
	}

	@Test
	void nothingArmedNeverHalts() {
		for (var point : Point.values()) {
			CrashPoints.reach(point);
		}
		assertEquals(List.of(), halts);
	}

	@Test
	void anArmedPointHaltsAfterItsSkippedPassesOnceAndOtherPointsNever() {
		CrashPoints.arm(Point.BEFORE_APPLY, 2);
		CrashPoints.reach(Point.AFTER_APPLY_BEFORE_REPLY);
		CrashPoints.reach(Point.BEFORE_APPLY);
		CrashPoints.reach(Point.BEFORE_APPLY);
		assertEquals(List.of(), halts);
		CrashPoints.reach(Point.BEFORE_APPLY);
		assertEquals(List.of(CrashPoints.EXIT_CODE), halts);
		// Disarmed by the halt (which a real halt would not return from).
		CrashPoints.reach(Point.BEFORE_APPLY);
		assertEquals(1, halts.size());
	}

	@Test
	void armingAgainReplacesAndDisarmingClears() {
		CrashPoints.arm(Point.ELECTED, 0);
		CrashPoints.arm(Point.SNAPSHOT_TAKEN, 0);
		CrashPoints.reach(Point.ELECTED);
		assertEquals(List.of(), halts);
		CrashPoints.disarm();
		CrashPoints.reach(Point.SNAPSHOT_TAKEN);
		assertEquals(List.of(), halts);
		assertThrows(IllegalArgumentException.class, () -> CrashPoints.arm(Point.ELECTED, -1));
	}
}
