package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The byte admission of {@link Proposer}, and how it is sized. */
class ByteAdmissionTest {

	@Test
	void refusesWhatDoesNotFitAndTakesNothing() {
		var admission = new ByteAdmission(100);
		assertTrue(admission.tryAcquire(60));
		assertFalse(admission.tryAcquire(41));
		assertEquals(60, admission.used());
		assertTrue(admission.tryAcquire(40));
		assertFalse(admission.tryAcquire(1));
		admission.release(60);
		assertTrue(admission.tryAcquire(60));
		admission.release(60);
		admission.release(40);
		assertEquals(0, admission.used());
	}

	@Test
	void neverExceedsTheLimitUnderContention() throws InterruptedException {
		var admission = new ByteAdmission(1000);
		var maxSeen = new AtomicLong();
		var done = new CountDownLatch(8);
		for (int t = 0; t < 8; t++) {
			Thread.ofPlatform().start(() -> {
				for (int i = 0; i < 20_000; i++) {
					if (admission.tryAcquire(300)) {
						maxSeen.accumulateAndGet(admission.used(), Math::max);
						admission.release(300);
					}
				}
				done.countDown();
			});
		}
		done.await();
		assertTrue(maxSeen.get() <= 1000, () -> "used " + maxSeen.get());
		assertEquals(0, admission.used());
	}

	@Test
	void theDefaultIsAnEighthOfTheHeapAndAlwaysFitsTheLargestEntryTwice() {
		long gib = 1L << 30;
		assertEquals(gib / 2, Proposer.writeAdmissionBytes(null, 4 * gib));
		assertEquals(gib / 2, Proposer.writeAdmissionBytes(" ", 4 * gib));
		long floor = 2 * (Proposer.MAX_ENTRY_BODY_BYTES + Proposer.REQUEST_OVERHEAD_BYTES);
		assertEquals(floor, Proposer.writeAdmissionBytes(null, 64L << 20));
		assertEquals(3 * gib, Proposer.writeAdmissionBytes(String.valueOf(3 * gib), 4 * gib));
		assertEquals(floor, Proposer.writeAdmissionBytes("1", 4 * gib));
	}

	@Test
	void anAttemptOfTheLargestEntryAlwaysGetsAPermit() {
		assertTrue(Proposer.ATTEMPT_BYTES_IN_FLIGHT >= RaftSettings.MAX_ENTRY_BYTES);
		assertTrue(new ByteAdmission(Proposer.ATTEMPT_BYTES_IN_FLIGHT).tryAcquire(RaftSettings.MAX_ENTRY_BYTES));
	}

	/**
	 * R50-1: a saturated lane, as {@code Proposer} sees it: every release is followed at once by a newly dispatched
	 * batch taking the bytes, and a refused attempt of the largest kind asks again only between them. Without a
	 * reservation the freed bytes always go to the newcomer, and the large attempt is refused until its deadline.
	 */
	@Test
	void aRefusedLargeAttemptGetsInAheadOfNewSmallOnes() {
		var admission = new ByteAdmission(32);
		for (int i = 0; i < 4; i++) {
			assertTrue(admission.tryAcquire(8, new Object()));
		}
		var large = new Object();
		int rounds = 0;
		boolean admitted = false;
		while (rounds < 20 && !admitted) {
			rounds++;
			admitted = admission.tryAcquire(12, large);
			if (!admitted) {
				admission.release(8);
				// The batch dispatched by the finished one: in Proposer it goes straight to tryAcquire.
				admission.tryAcquire(8, new Object());
			}
			assertTrue(admission.used() <= 32, () -> "used " + admission.used());
		}
		assertTrue(admitted, "the large attempt was refused for 20 rounds while small ones kept getting in");
		assertTrue(rounds <= 3, "admitted only after " + rounds + " rounds");
	}

	/** R50-1: while an attempt waits, those that fit beside it still get in; it gets in once there is room for it. */
	@Test
	void aWaitingAttemptLeavesRoomForThoseThatStillFit() {
		var admission = new ByteAdmission(32);
		for (int i = 0; i < 3; i++) {
			assertTrue(admission.tryAcquire(8, new Object()));
		}
		var large = new Object();
		assertFalse(admission.tryAcquire(12, large));
		assertFalse(admission.tryAcquire(4, new Object()), "it would leave no room for the waiting attempt");
		assertEquals(24, admission.used());
		admission.release(16);
		assertTrue(admission.tryAcquire(4, new Object()), "it fits beside the waiting attempt");
		assertTrue(admission.tryAcquire(12, large));
		// Admitted: the reservation is gone, and the rest of the limit is free for anyone.
		assertTrue(admission.tryAcquire(8, new Object()));
		assertEquals(32, admission.used());
	}

	/** R50-1: only the oldest refused attempt reserves; the next one reserves once it got in. */
	@Test
	void onlyTheOldestRefusedAttemptReserves() {
		var admission = new ByteAdmission(32);
		assertTrue(admission.tryAcquire(24, new Object()));
		var first = new Object();
		var second = new Object();
		assertFalse(admission.tryAcquire(12, first));
		assertFalse(admission.tryAcquire(12, second));
		admission.release(24);
		assertTrue(admission.tryAcquire(12, first));
		assertTrue(admission.tryAcquire(16, new Object()));
		assertFalse(admission.tryAcquire(12, second));
		assertFalse(admission.tryAcquire(4, new Object()), "the second one reserves now");
		admission.release(16);
		assertTrue(admission.tryAcquire(12, second));
	}

	/** R50-1: a reservation whose waiter stops asking lapses, and one it gives up is gone at once. */
	@Test
	void aReservationLapsesOrIsCancelled() {
		var now = new AtomicLong();
		var admission = new ByteAdmission(32, now::get);
		assertTrue(admission.tryAcquire(24, new Object()));
		var gone = new Object();
		assertFalse(admission.tryAcquire(12, gone));
		assertFalse(admission.tryAcquire(4, new Object()), "the reservation holds");
		now.addAndGet(ByteAdmission.RESERVATION_NANOS / 2);
		assertFalse(admission.tryAcquire(12, gone), "its waiter asks again, which renews it");
		now.addAndGet(ByteAdmission.RESERVATION_NANOS - 1);
		assertFalse(admission.tryAcquire(4, new Object()), "renewed: it still holds");
		now.addAndGet(1);
		assertTrue(admission.tryAcquire(4, new Object()), "the waiter stopped asking: lapsed");
		admission.release(4);
		admission.release(24);

		var cancelled = new Object();
		assertTrue(admission.tryAcquire(16, new Object()));
		assertFalse(admission.tryAcquire(20, cancelled));
		assertFalse(admission.tryAcquire(8, new Object()));
		admission.cancel(cancelled);
		assertTrue(admission.tryAcquire(8, new Object()), "cancelled: the room is free again");
	}

	/**
	 * Review of R50, 2: two refused commit batches came back merged, the later one first, so the batch's first commit
	 * is not the one that holds the reservation. It asks under the holder, and the holder's bytes are not counted twice.
	 */
	@Test
	void aMergedBatchAsksUnderTheMemberThatHoldsTheReservation() {
		var admission = new ByteAdmission(32);
		assertTrue(admission.tryAcquire(24, new Object()));
		var older = new Object();
		var newer = new Object();
		assertFalse(admission.tryAcquire(12, older));
		assertFalse(admission.tryAcquire(12, newer));
		admission.release(24);
		var waiter = admission.waiterAmong(List.of(newer, older));
		assertTrue(admission.tryAcquire(24, waiter), "24 of 32 fit, with the reserved 12 among them");
		assertSame(older, waiter);
		assertTrue(admission.tryAcquire(8, new Object()), "the holder got in: no reservation is left");
		assertSame(newer, admission.waiterAmong(List.of(newer, older)), "with none held, the first");
	}
}
